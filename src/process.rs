//! Event processing and reconciliation module
//!
//! This module handles processing pending raw emails and reconciling them
//! into the orders database using event-sourcing principles.

use crate::db::Database;
use crate::images::{image_id_for_url, ImageProcessor};
use crate::models::{ItemStatus, OrderStatus, WalmartOrder};
use crate::parsing::{EmailType, WalmartEmailParser};
use crate::tracking::create_tracking_cache_entry;
use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use sqlx::{Row, Sqlite};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time::Duration;

/// Signal from orchestrator indicating whether all email sync tasks have completed.
/// - `None`: Legacy mode — exit processing loop when queue is empty.
/// - `Some(flag)`: Concurrent mode — poll with brief sleep while `flag` is `false`,
///   only exit when queue is empty AND flag is `true`.
pub type IngestionDoneFlag = Option<Arc<AtomicBool>>;

/// Tracking info to create after transaction commits
struct PendingTrackingEntry {
    order_id: String,
    tracking_number: String,
    carrier: String,
}

/// Determine optimal parallelism for CPU-bound email parsing based on available CPUs
///
/// Uses 3x CPU count for CPU-bound work to maximize throughput while clamping between
/// reasonable bounds to prevent memory exhaustion on high-core systems.
fn max_concurrent_parsing() -> usize {
    use std::sync::OnceLock;
    static MAX_PARSING: OnceLock<usize> = OnceLock::new();
    *MAX_PARSING.get_or_init(|| {
        // Use 3x CPU count for CPU-bound work (HTML parsing + regex)
        let cpu_count = num_cpus::get();
        let target = cpu_count * 3;

        // Clamp between reasonable bounds
        target.clamp(32, 200)
    })
}

/// Number of emails to process in a single batch (limits memory usage and lock duration)
const BATCH_SIZE: i64 = 500;

/// Fetch the next non-empty batch of pending emails, waiting if ingestion is still running.
///
/// Returns `Ok(Some(emails))` when a batch is available, or `Ok(None)` when it's time to exit:
/// - Legacy mode (`None` flag): exits immediately when queue is empty.
/// - Concurrent mode: polls with 100ms sleep while ingestion is running, exits only
///   when queue is empty AND ingestion is done.
async fn next_batch(
    db: &Database,
    limit: i64,
    ingestion_done: &IngestionDoneFlag,
) -> Result<Option<Vec<RawEmail>>> {
    loop {
        let emails = fetch_pending_emails(db, limit).await?;
        if !emails.is_empty() {
            return Ok(Some(emails));
        }

        match ingestion_done {
            None => return Ok(None), // Legacy: exit on empty
            Some(flag) => {
                if flag.load(Ordering::Acquire) {
                    // Ingestion done. Brief yield for any in-flight DB commits to land.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let final_emails = fetch_pending_emails(db, limit).await?;
                    return if final_emails.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(final_emails))
                    };
                }
                // Ingestion still running — wait briefly and re-poll
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Statistics from processing operation
#[derive(Debug, Default)]
pub struct ProcessStats {
    /// Total pending emails found
    pub total_pending: usize,
    /// Successfully processed
    pub processed: usize,
    /// Failed to parse
    pub failed: usize,
    /// Skipped (unknown type or irrelevant)
    pub skipped: usize,
    /// Total duration in milliseconds
    pub total_duration_ms: u128,
    /// Fetch duration in milliseconds
    pub fetch_duration_ms: u128,
    /// Parse duration in milliseconds
    pub parse_duration_ms: u128,
    /// Categorize duration in milliseconds
    pub categorize_duration_ms: u128,
    /// Confirmation transaction duration in milliseconds
    pub confirm_duration_ms: u128,
    /// Update transaction duration in milliseconds
    pub update_duration_ms: u128,
    /// Tracking cache duration in milliseconds
    pub tracking_duration_ms: u128,
    /// Status update duration in milliseconds
    pub status_duration_ms: u128,
}

impl ProcessStats {
    pub fn summary(&self) -> String {
        format!(
            "Processing complete: {} pending, {} processed, {} skipped, {} failed in {}ms",
            self.total_pending, self.processed, self.skipped, self.failed, self.total_duration_ms
        )
    }

    pub fn performance_summary(&self) -> String {
        format!(
            "Performance breakdown: fetch={}ms, parse={}ms, categorize={}ms, confirm={}ms, update={}ms, tracking={}ms, status={}ms",
            self.fetch_duration_ms,
            self.parse_duration_ms,
            self.categorize_duration_ms,
            self.confirm_duration_ms,
            self.update_duration_ms,
            self.tracking_duration_ms,
            self.status_duration_ms
        )
    }

    fn add(&mut self, other: &ProcessStats) {
        self.total_pending += other.total_pending;
        self.processed += other.processed;
        self.failed += other.failed;
        self.skipped += other.skipped;
        self.total_duration_ms += other.total_duration_ms;
        self.fetch_duration_ms += other.fetch_duration_ms;
        self.parse_duration_ms += other.parse_duration_ms;
        self.categorize_duration_ms += other.categorize_duration_ms;
        self.confirm_duration_ms += other.confirm_duration_ms;
        self.update_duration_ms += other.update_duration_ms;
        self.tracking_duration_ms += other.tracking_duration_ms;
        self.status_duration_ms += other.status_duration_ms;
    }
}

/// Raw email row from database
#[derive(Debug)]
struct RawEmail {
    id: i64,
    gmail_id: String,
    subject: Option<String>,
    raw_body: String,
    gmail_date: Option<String>,
    recipient: Option<String>,
    account_id: Option<i64>,
}

/// Parsed email result ready for database writes
struct ParsedEmail {
    id: i64,
    gmail_id: String,
    email_type: EmailType,
    order: Option<WalmartOrder>,
    raw_email: RawEmail,
}

/// Categorize emails into confirmations (create orders) vs updates (modify orders)
fn categorize_emails(parsed_emails: Vec<ParsedEmail>) -> (Vec<ParsedEmail>, Vec<ParsedEmail>) {
    parsed_emails
        .into_iter()
        .partition(|email| matches!(email.email_type, EmailType::Confirmation))
}

/// Process all pending emails and reconcile into orders.
///
/// This is the standard entry point that exits when the queue is empty.
/// For concurrent use with ongoing ingestion, use [`process_pending_events_concurrent`].
pub async fn process_pending_events(db: &Database) -> Result<ProcessStats> {
    process_pending_events_concurrent(db, None).await
}

/// Process pending emails with awareness of concurrent ingestion.
///
/// When `ingestion_done` is `Some(flag)`:
/// - If the pending queue is empty but `flag` is `false`, polls every 100ms.
/// - If the pending queue is empty and `flag` is `true`, does one final drain then exits.
///
/// When `ingestion_done` is `None`:
/// - Behaves identically to the original `process_pending_events` (exits on empty).
pub async fn process_pending_events_concurrent(
    db: &Database,
    ingestion_done: IngestionDoneFlag,
) -> Result<ProcessStats> {
    let total_start = std::time::Instant::now();
    let mut total_stats = ProcessStats::default();
    let mut batch_count = 0;
    let parser = Arc::new(WalmartEmailParser::new());
    // Track deferred emails (missing order — confirmation may arrive in a later batch).
    // During main pass: mark as 'deferred' in DB (body preserved, excluded from subsequent fetches).
    // After all batches: reset to 'pending' and do one final retry pass.
    let mut total_deferred: u64 = 0;
    let mut is_final_retry = false;

    loop {
        let mut batch_stats = ProcessStats::default();

        // 1. WAIT FOR NEXT BATCH (may poll while ingestion is still running)
        let wait_start = std::time::Instant::now();
        let pending_emails = match next_batch(db, BATCH_SIZE, &ingestion_done).await? {
            Some(emails) => emails,
            None => {
                if !is_final_retry && total_deferred > 0 {
                    // All regular batches done — reset deferred emails for final retry
                    tracing::info!(
                        "=== Final retry: resetting {} deferred emails to pending ===",
                        total_deferred
                    );
                    reset_deferred_to_pending(db).await?;
                    is_final_retry = true;
                    continue; // Re-enter loop — fetch_pending_emails will find the reset emails
                }
                if total_stats.total_pending == 0 {
                    tracing::info!("No pending emails to process");
                }
                break;
            }
        };
        let wait_elapsed = wait_start.elapsed();

        // Start batch timer AFTER emails are available (excludes polling wait)
        let batch_start = std::time::Instant::now();
        batch_stats.total_pending = pending_emails.len();
        batch_stats.fetch_duration_ms = wait_elapsed.as_millis();

        batch_count += 1;

        // Log wait time if processor was idle waiting for ingestion
        if wait_elapsed.as_millis() > 200 {
            tracing::info!(
                wait_ms = wait_elapsed.as_millis(),
                "Processor waited {}ms for emails to arrive (idle while sync ran)",
                wait_elapsed.as_millis()
            );
        }

        tracing::info!("Processing batch {} of {} pending emails", batch_count, pending_emails.len());

        // 2. PARSE EMAILS IN PARALLEL (CPU-bound)
        let parse_start = std::time::Instant::now();
        let parsed_emails: Vec<Result<ParsedEmail, (i64, String, String)>> = stream::iter(pending_emails)
            .map(|email| {
                let parser = Arc::clone(&parser);
                async move {
                    // Offload synchronous parsing to blocking thread pool for TRUE parallelism
                    tokio::task::spawn_blocking(move || {
                        parse_single_email(&parser, email)
                    })
                    .await
                    .unwrap_or_else(|e| Err((0, String::new(), format!("Task panicked: {}", e))))
                }
            })
            .buffer_unordered(max_concurrent_parsing())
            .collect()
            .await;
        let parse_elapsed = parse_start.elapsed();
        batch_stats.parse_duration_ms = parse_elapsed.as_millis();

        // Separate successes from failures
        // Pre-allocate with expected capacity to avoid reallocations
        let mut to_process: Vec<ParsedEmail> = Vec::with_capacity(BATCH_SIZE as usize);
        let mut parse_failures: Vec<(i64, String)> = Vec::with_capacity(50); // ~10% typical failure rate
        let mut skipped_ids: Vec<i64> = Vec::with_capacity(50); // ~10% typical skip rate
        let mut deferred_batch_ids: Vec<i64> = Vec::new();

        let mut unknown_sample_logged = false;
        for result in parsed_emails {
            match result {
                Ok(parsed) => {
                    if parsed.email_type == EmailType::Unknown {
                        if !unknown_sample_logged {
                            let body_snippet: String = parsed.raw_email.raw_body.chars().take(300).collect();
                            tracing::warn!(
                                "Sample Unknown email: gmail_id={}, subject={:?}, body_len={}, body_start={:?}",
                                parsed.gmail_id,
                                parsed.raw_email.subject,
                                parsed.raw_email.raw_body.len(),
                                body_snippet,
                            );
                            unknown_sample_logged = true;
                        }
                        skipped_ids.push(parsed.id);
                        batch_stats.skipped += 1;
                    } else {
                        to_process.push(parsed);
                    }
                }
                Err((id, _gmail_id, error)) => {
                    parse_failures.push((id, error));
                    batch_stats.failed += 1;
                }
            }
        }

        // PHASE 2: Write to database in TWO PASSES (confirmations first, then updates)
        // Pre-allocate with expected capacity to avoid reallocations
        let mut processed_ids: Vec<i64> = Vec::with_capacity(BATCH_SIZE as usize); // Most emails succeed
        let mut db_failed_entries: Vec<(i64, String)> = Vec::with_capacity(50); // ~10% typical failure rate
        let mut pending_tracking: Vec<PendingTrackingEntry> = Vec::with_capacity(50); // Some orders have tracking

        // 3. CATEGORIZE EMAILS
        let categorize_start = std::time::Instant::now();
        let (confirmations, updates) = categorize_emails(to_process);
        let categorize_elapsed = categorize_start.elapsed();
        batch_stats.categorize_duration_ms = categorize_elapsed.as_millis();

        tracing::info!(
            "Two-pass processing: {} confirmations, {} updates",
            confirmations.len(),
            updates.len()
        );

        // 4. PASS 1: Process confirmations (create orders)
        let confirm_start = std::time::Instant::now();
        let tx_begin_start = std::time::Instant::now();
        let mut tx = db.pool().begin().await.context("Failed to begin transaction for confirmations")?;
        let tx_begin_elapsed = tx_begin_start.elapsed();

        let confirm_process_start = std::time::Instant::now();
        for parsed in &confirmations {
            let result = apply_parsed_email_to_db_tx(&mut tx, parsed, &mut pending_tracking).await;
            match result {
                Ok(ProcessResult::Processed) => {
                    batch_stats.processed += 1;
                    processed_ids.push(parsed.id);
                    tracing::debug!("Processed confirmation email {}", parsed.gmail_id);
                }
                Ok(ProcessResult::Skipped(reason)) | Ok(ProcessResult::SkippedMissingOrder(reason)) => {
                    skipped_ids.push(parsed.id);
                    batch_stats.skipped += 1;
                    tracing::warn!(
                        "Skipped confirmation email {}: {}. Subject: {:?}. Body len: {}",
                        parsed.gmail_id,
                        reason,
                        parsed.raw_email.subject,
                        parsed.raw_email.raw_body.len()
                    );
                }
                Err(e) => {
                    let error_msg = format!("{:#}", e);
                    if is_db_lock_error(&error_msg) {
                        deferred_batch_ids.push(parsed.id);
                        batch_stats.failed += 1;
                        tracing::warn!(
                            "Database locked processing confirmation {}, deferring for retry",
                            parsed.gmail_id
                        );
                    } else {
                        batch_stats.failed += 1;
                        db_failed_entries.push((parsed.id, error_msg.clone()));
                        tracing::warn!("Failed to process confirmation email {}: {}", parsed.gmail_id, error_msg);
                    }
                }
            }
        }
        let confirm_process_elapsed = confirm_process_start.elapsed();

        // Commit confirmations before processing updates
        let confirm_commit_start = std::time::Instant::now();
        tx.commit().await.context("Failed to commit confirmations")?;
        let confirm_commit_elapsed = confirm_commit_start.elapsed();

        let confirm_elapsed = confirm_start.elapsed();
        batch_stats.confirm_duration_ms = confirm_elapsed.as_millis();

        if !confirmations.is_empty() {
            tracing::debug!(
                confirmation_count = confirmations.len(),
                total_ms = confirm_elapsed.as_millis(),
                tx_begin_ms = tx_begin_elapsed.as_millis(),
                process_ms = confirm_process_elapsed.as_millis(),
                commit_ms = confirm_commit_elapsed.as_millis(),
                "Confirmation transaction: {} emails in {}ms (begin={}ms, process={}ms, commit={}ms)",
                confirmations.len(),
                confirm_elapsed.as_millis(),
                tx_begin_elapsed.as_millis(),
                confirm_process_elapsed.as_millis(),
                confirm_commit_elapsed.as_millis()
            );
            tracing::info!("✓ Pass 1 complete: Committed {} confirmations in {}ms", confirmations.len(), confirm_elapsed.as_millis());
        }

        // 5. PASS 2: Process updates (shipping, delivery, cancellation)
        let update_start = std::time::Instant::now();
        let update_tx_begin_start = std::time::Instant::now();
        let mut tx = db.pool().begin().await.context("Failed to begin transaction for updates")?;
        let update_tx_begin_elapsed = update_tx_begin_start.elapsed();

        let update_process_start = std::time::Instant::now();
        for parsed in &updates {
            let result = apply_parsed_email_to_db_tx(&mut tx, parsed, &mut pending_tracking).await;
            match result {
                Ok(ProcessResult::Processed) => {
                    batch_stats.processed += 1;
                    processed_ids.push(parsed.id);
                    tracing::debug!("Processed update email {}", parsed.gmail_id);
                }
                Ok(ProcessResult::Skipped(reason)) => {
                    skipped_ids.push(parsed.id);
                    batch_stats.skipped += 1;
                    tracing::warn!(
                        "Skipped update email {}: {}. Subject: {:?}. Body len: {}",
                        parsed.gmail_id,
                        reason,
                        parsed.raw_email.subject,
                        parsed.raw_email.raw_body.len()
                    );
                }
                Ok(ProcessResult::SkippedMissingOrder(reason)) => {
                    if is_final_retry {
                        // Final retry — order truly doesn't exist, permanently skip
                        skipped_ids.push(parsed.id);
                        batch_stats.skipped += 1;
                        tracing::warn!(
                            "Permanently skipping email {} (order not found after final retry): {}",
                            parsed.gmail_id,
                            reason
                        );
                    } else {
                        // Defer for final retry — mark as 'deferred' in DB (body preserved)
                        deferred_batch_ids.push(parsed.id);
                        batch_stats.skipped += 1;
                        tracing::info!(
                            "Deferred email {} for final retry: {}",
                            parsed.gmail_id,
                            reason
                        );
                    }
                }
                Err(e) => {
                    let error_msg = format!("{:#}", e);
                    if is_db_lock_error(&error_msg) {
                        deferred_batch_ids.push(parsed.id);
                        batch_stats.failed += 1;
                        tracing::warn!(
                            "Database locked processing update {}, deferring for retry",
                            parsed.gmail_id
                        );
                    } else {
                        batch_stats.failed += 1;
                        db_failed_entries.push((parsed.id, error_msg.clone()));
                        tracing::warn!("Failed to process update email {}: {}", parsed.gmail_id, error_msg);
                    }
                }
            }
        }
        let update_process_elapsed = update_process_start.elapsed();

        // Commit updates
        let update_commit_start = std::time::Instant::now();
        tx.commit().await.context("Failed to commit updates")?;
        let update_commit_elapsed = update_commit_start.elapsed();

        let update_elapsed = update_start.elapsed();
        batch_stats.update_duration_ms = update_elapsed.as_millis();

        if !updates.is_empty() {
            tracing::debug!(
                update_count = updates.len(),
                total_ms = update_elapsed.as_millis(),
                tx_begin_ms = update_tx_begin_elapsed.as_millis(),
                process_ms = update_process_elapsed.as_millis(),
                commit_ms = update_commit_elapsed.as_millis(),
                "Update transaction: {} emails in {}ms (begin={}ms, process={}ms, commit={}ms)",
                updates.len(),
                update_elapsed.as_millis(),
                update_tx_begin_elapsed.as_millis(),
                update_process_elapsed.as_millis(),
                update_commit_elapsed.as_millis()
            );
            tracing::info!("✓ Pass 2 complete: Committed {} updates in {}ms", updates.len(), update_elapsed.as_millis());
        }

        // 6. CREATE TRACKING CACHE (non-critical)
        let tracking_start = std::time::Instant::now();
        for entry in pending_tracking {
            if let Err(e) = create_tracking_cache_entry(db, &entry.order_id, &entry.tracking_number, &entry.carrier).await {
                tracing::warn!("Failed to create tracking cache entry for {}: {}", entry.tracking_number, e);
            }
        }
        let tracking_elapsed = tracking_start.elapsed();
        batch_stats.tracking_duration_ms = tracking_elapsed.as_millis();

        // Combine parse failures with DB failures
        let mut all_failed: Vec<(i64, String)> = parse_failures;
        all_failed.extend(db_failed_entries);

        // 7. BATCH UPDATE EMAIL STATUSES
        let status_start = std::time::Instant::now();
        if !processed_ids.is_empty() {
            batch_mark_emails_processed(db, &processed_ids).await?;
        }
        if !skipped_ids.is_empty() {
            batch_mark_emails_skipped(db, &skipped_ids).await?;
        }
        if !all_failed.is_empty() {
            batch_mark_emails_failed(db, &all_failed).await?;
        }
        if !deferred_batch_ids.is_empty() {
            batch_mark_emails_deferred(db, &deferred_batch_ids).await?;
            total_deferred += deferred_batch_ids.len() as u64;
        }
        let status_elapsed = status_start.elapsed();
        batch_stats.status_duration_ms = status_elapsed.as_millis();

        // Calculate total batch duration
        let batch_elapsed = batch_start.elapsed();
        batch_stats.total_duration_ms = batch_elapsed.as_millis();

        // Log batch performance summary
        tracing::debug!(
            batch_size = batch_stats.total_pending,
            total_ms = batch_elapsed.as_millis(),
            wait_ms = wait_elapsed.as_millis(),
            parse_ms = parse_elapsed.as_millis(),
            parse_avg_ms = if batch_stats.total_pending > 0 {
                parse_elapsed.as_millis() / batch_stats.total_pending as u128
            } else {
                0
            },
            categorize_ms = categorize_elapsed.as_millis(),
            confirm_ms = confirm_elapsed.as_millis(),
            update_ms = update_elapsed.as_millis(),
            tracking_ms = tracking_elapsed.as_millis(),
            status_ms = status_elapsed.as_millis(),
            "Batch {} completed in {}ms (wait={}ms): parse={}ms (avg {:.1}ms/email), categorize={}ms, confirm={}ms, update={}ms, tracking={}ms, status={}ms",
            batch_count,
            batch_elapsed.as_millis(),
            wait_elapsed.as_millis(),
            parse_elapsed.as_millis(),
            if batch_stats.total_pending > 0 {
                parse_elapsed.as_millis() as f64 / batch_stats.total_pending as f64
            } else {
                0.0
            },
            categorize_elapsed.as_millis(),
            confirm_elapsed.as_millis(),
            update_elapsed.as_millis(),
            tracking_elapsed.as_millis(),
            status_elapsed.as_millis()
        );

        tracing::info!(
            "Batch {} complete: {} processed, {} skipped, {} failed in {}ms",
            batch_count,
            batch_stats.processed,
            batch_stats.skipped,
            batch_stats.failed,
            batch_elapsed.as_millis()
        );

        total_stats.add(&batch_stats);
    }

    let total_elapsed = total_start.elapsed();
    total_stats.total_duration_ms = total_elapsed.as_millis();

    tracing::info!("{}", total_stats.summary());
    tracing::debug!("{}", total_stats.performance_summary());
    tracing::info!(
        "Processing complete: {} batches in {}ms",
        batch_count,
        total_elapsed.as_millis()
    );

    // Diagnostic: verify orders are actually persisted in the database
    match sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM orders")
        .fetch_one(db.pool())
        .await
    {
        Ok((order_count,)) => tracing::info!("Orders in database after processing: {}", order_count),
        Err(err) => tracing::error!("Failed to count orders after processing: {}", err),
    }

    Ok(total_stats)
}

enum ProcessResult {
    Processed,
    Skipped(String),
    /// Skipped because the order doesn't exist yet (confirmation may arrive in a later batch).
    /// Contains the skip reason message.
    SkippedMissingOrder(String),
}

/// Fetch all pending raw emails from the database
#[tracing::instrument(skip_all, fields(limit))]
async fn fetch_pending_emails(db: &Database, limit: i64) -> Result<Vec<RawEmail>> {
    // Order by date, but prioritize confirmations first within the same date
    // This ensures orders are created from confirmations before other events update them
    let rows: Vec<(i64, String, Option<String>, String, Option<String>, Option<String>, Option<i64>)> = sqlx::query_as(
        r#"
        SELECT id, gmail_id, subject, raw_body, gmail_date, recipient, account_id
        FROM raw_emails
        WHERE processing_status = 'pending'
        ORDER BY gmail_date ASC,
                 CASE event_type
                     WHEN 'confirmation' THEN 0
                     ELSE 1
                 END ASC
        LIMIT ?
        "#
    )
    .bind(limit)
    .fetch_all(db.pool())
    .await
    .context("Failed to fetch pending emails")?;

    Ok(rows.into_iter().map(|(id, gmail_id, subject, raw_body, gmail_date, recipient, account_id)| {
        RawEmail {
            id,
            gmail_id,
            subject,
            raw_body,
            gmail_date,
            recipient,
            account_id,
        }
    }).collect())
}

/// Decode quoted-printable encoding in email content
/// Handles soft line breaks and =XX hex codes
fn decode_quoted_printable(input: &str) -> String {
    // Use the quoted_printable crate for proper decoding
    match quoted_printable::decode(input.as_bytes(), quoted_printable::ParseMode::Robust) {
        Ok(bytes) => String::from_utf8_lossy(&bytes).to_string(),
        Err(_) => {
            // Fallback: manual decode of common patterns
            let mut result = input.to_string();
            // Remove soft line breaks (=\r\n or =\n)
            result = result.replace("=\r\n", "").replace("=\n", "");
            // Decode common hex codes
            result = result.replace("=20", " ");
            result = result.replace("=3D", "=");
            result = result.replace("=0A", "\n");
            result = result.replace("=0D", "\r");
            result = result.replace("=22", "\"");
            result = result.replace("=27", "'");
            result
        }
    }
}

fn normalize_item_name(name: &str) -> String {
    let mut tokens: Vec<String> = Vec::new();
    let mut current = String::new();

    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            current.push(ch.to_ascii_lowercase());
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens.retain(|token| token != "and");
    tokens.join("")
}

fn build_normalized_name_map(names: &[String]) -> HashMap<String, Vec<String>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for name in names {
        let key = normalize_item_name(name);
        map.entry(key).or_default().push(name.clone());
    }
    map
}

/// Parse a single email without writing to DB (CPU-bound, parallelizable)
fn parse_single_email(
    parser: &WalmartEmailParser,
    mut email: RawEmail,
) -> Result<ParsedEmail, (i64, String, String)> {
    let _span = tracing::info_span!("parse_email", gmail_id = %email.gmail_id, body_len = email.raw_body.len()).entered();

    // Decode quoted-printable encoding if present.
    // Soft line breaks (= followed by newline) are the definitive QP marker.
    // Avoid checking for =20 or =3D which can appear in decoded HTML (URLs, CSS).
    let needs_decode = email.raw_body.contains("=\r\n") || email.raw_body.contains("=\n");
    let html = if needs_decode {
        let _qp_span = tracing::debug_span!("qp_decode").entered();
        decode_quoted_printable(&email.raw_body)
    } else {
        // Move the string instead of cloning — email.raw_body is consumed anyway
        std::mem::take(&mut email.raw_body)
    };

    // Detect email type: prefer subject line (high-confidence, short text),
    // fall back to full HTML body scan (lower confidence due to footer/boilerplate text)
    let email_type = {
        let _detect_span = tracing::debug_span!("detect_type").entered();
        email.subject.as_deref()
            .and_then(|sub| parser.detect_email_type_from_subject(sub))
            .unwrap_or_else(|| parser.detect_email_type_raw(&html))
    };

    // Convert gmail_date (millis string) to DateTime for date fallback
    let fallback_date = email.gmail_date.as_ref()
        .and_then(|date_str| date_str.parse::<i64>().ok())
        .and_then(chrono::DateTime::from_timestamp_millis);

    // Parse order if it's a known type
    let order = if email_type != EmailType::Unknown {
        let _order_span = tracing::debug_span!("parse_order", email_type = ?email_type).entered();

        // Fast path: shipping and delivery emails can often be parsed with
        // pure regex/string extraction, avoiding expensive DOM construction.
        // Falls back to full parse_order() on None.
        let fast_result = match email_type {
            EmailType::Shipping => parser.parse_shipping_fast(&html, fallback_date),
            EmailType::Delivery => parser.parse_delivery_fast(&html, fallback_date),
            EmailType::Cancellation => parser.parse_cancellation_fast(&html, fallback_date),
            _ => None,
        };

        let parse_result = if let Some(order) = fast_result {
            Ok(order)
        } else {
            parser.parse_order(&html, fallback_date)
        };

        match parse_result {
            Ok(mut order) => {
                order.account_id = email.account_id;
                order.recipient = email.recipient.clone();
                if email_type == EmailType::Confirmation && order.total_cost.is_none() {
                    tracing::warn!(
                        order_id = %order.id,
                        gmail_id = %email.gmail_id,
                        body_len = html.len(),
                        "Confirmation email parsed but total_cost is None"
                    );
                }
                Some(order)
            }
            Err(err) => {
                return Err((email.id, email.gmail_id.clone(), format!("{:#}", err)));
            }
        }
    } else {
        None
    };

    Ok(ParsedEmail {
        id: email.id,
        gmail_id: email.gmail_id.clone(),
        email_type,
        order,
        raw_email: email,
    })
}


/// Apply a parsed email to the database within a transaction
#[tracing::instrument(skip_all, fields(gmail_id = %parsed.gmail_id, email_type = ?parsed.email_type))]
async fn apply_parsed_email_to_db_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    parsed: &ParsedEmail,
    pending_tracking: &mut Vec<PendingTrackingEntry>,
) -> Result<ProcessResult> {
    let order = parsed.order.as_ref()
        .ok_or_else(|| anyhow::anyhow!("No order data for non-unknown email type"))?;

    match parsed.email_type {
        EmailType::Unknown => {
            return Ok(ProcessResult::Skipped("Unknown email type".to_string()));
        }
        EmailType::Confirmation => {
            apply_confirmation_tx(tx, order, &parsed.raw_email).await?;
        }
        EmailType::Cancellation => {
            apply_cancellation_tx(tx, order, &parsed.raw_email).await?;
        }
        EmailType::Shipping => {
            if !order_exists_tx(tx, &order.id).await? {
                return Ok(ProcessResult::SkippedMissingOrder(
                    format!("No confirmation for order {} — skipping shipping event", order.id)
                ));
            }
            apply_shipping_tx(tx, order, &parsed.raw_email, pending_tracking).await?;
        }
        EmailType::Delivery => {
            if !order_exists_tx(tx, &order.id).await? {
                tracing::warn!(
                    "⚠ Skipping delivery email for order '{}' - no matching confirmed order found. Email subject: '{}'",
                    order.id,
                    parsed.raw_email.subject.as_deref().unwrap_or("<no subject>")
                );
                return Ok(ProcessResult::SkippedMissingOrder(
                    format!("No confirmation for order {} — skipping delivery event", order.id)
                ));
            }
            tracing::info!("✓ Processing delivery email for order '{}'", order.id);
            apply_delivery_tx(tx, order, &parsed.raw_email).await?;
        }
    }

    Ok(ProcessResult::Processed)
}






// ============================================================================
// Transaction-aware apply functions for batched processing
// ============================================================================

/// Apply a confirmation to the database within a transaction
async fn apply_confirmation_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order: &WalmartOrder,
    email: &RawEmail,
) -> Result<()> {
    insert_order_tx(tx, order).await?;
    // Update total_cost (INSERT OR IGNORE doesn't set this on existing rows)
    if order.total_cost.is_some() {
        sqlx::query("UPDATE orders SET total_cost = ? WHERE id = ?")
            .bind(order.total_cost)
            .bind(&order.id)
            .execute(&mut **tx)
            .await?;
    }
    record_email_event_tx(tx, &order.id, "confirmation", email).await?;
    Ok(())
}

/// Apply a cancellation to the database within a transaction
async fn apply_cancellation_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order: &WalmartOrder,
    email: &RawEmail,
) -> Result<()> {
    insert_order_tx(tx, order).await?;

    if order.items.is_empty() {
        update_order_status_tx(tx, &order.id, OrderStatus::Canceled).await?;
        cancel_all_items_tx(tx, &order.id).await?;
        tracing::info!("Order {} fully canceled", order.id);
    } else {
        let item_names: Vec<&str> = order.items.iter().map(|i| i.name.as_str()).collect();
        batch_cancel_items_by_name_tx(tx, &order.id, &item_names).await?;

        let all_canceled = check_all_items_canceled_tx(tx, &order.id).await?;
        if all_canceled {
            update_order_status_tx(tx, &order.id, OrderStatus::Canceled).await?;
        } else {
            update_order_status_tx(tx, &order.id, OrderStatus::PartiallyCanceled).await?;
        }
        tracing::info!("Order {} partially canceled ({} items)", order.id, item_names.len());
    }

    // Store cancel reason if extracted from email
    if let Some(ref reason) = order.cancel_reason {
        sqlx::query("UPDATE orders SET cancel_reason = ? WHERE id = ?")
            .bind(reason)
            .bind(&order.id)
            .execute(&mut **tx)
            .await?;
    }

    record_email_event_tx(tx, &order.id, "cancellation", email).await?;
    Ok(())
}

/// Apply a shipping notification to the database within a transaction
async fn apply_shipping_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order: &WalmartOrder,
    email: &RawEmail,
    pending_tracking: &mut Vec<PendingTrackingEntry>,
) -> Result<()> {
    insert_order_row_tx(tx, order).await?;

    // Single UPDATE for all shipping fields with status transition guard
    let shipped_date_formatted = email.gmail_date.as_ref().map(|d| millis_to_rfc3339(d));
    sqlx::query(
        r#"UPDATE orders SET
            status = CASE
                WHEN status IN ('canceled', 'delivered') THEN status
                ELSE ?
            END,
            total_cost = CASE
                WHEN (total_cost IS NULL OR total_cost = 0) AND ? IS NOT NULL THEN ?
                ELSE total_cost
            END,
            shipped_date = CASE
                WHEN shipped_date IS NULL AND ? IS NOT NULL THEN ?
                ELSE shipped_date
            END,
            tracking_number = COALESCE(?, tracking_number),
            carrier = COALESCE(?, carrier)
        WHERE id = ?"#
    )
    .bind(OrderStatus::Shipped.as_str())
    .bind(order.total_cost)
    .bind(order.total_cost)
    .bind(&shipped_date_formatted)
    .bind(&shipped_date_formatted)
    .bind(&order.tracking_number)
    .bind(&order.carrier)
    .bind(&order.id)
    .execute(&mut **tx)
    .await?;

    if let (Some(tracking), Some(carrier)) = (&order.tracking_number, &order.carrier) {
        // Queue tracking cache creation for after transaction commits
        pending_tracking.push(PendingTrackingEntry {
            order_id: order.id.clone(),
            tracking_number: tracking.clone(),
            carrier: carrier.clone(),
        });

        tracing::info!("Order {} shipped via {} - tracking: {}", order.id, carrier, tracking);
    } else {
        tracing::info!("Order {} marked as shipped (no tracking number)", order.id);
    }

    upsert_items_for_event_tx(tx, &order.id, &order.items, ItemStatus::Shipped).await?;

    record_email_event_tx(tx, &order.id, "shipping", email).await?;
    Ok(())
}

/// Apply a delivery notification to the database within a transaction
async fn apply_delivery_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order: &WalmartOrder,
    email: &RawEmail,
) -> Result<()> {
    insert_order_row_tx(tx, order).await?;

    // Single UPDATE for status + total_cost with status transition guard
    sqlx::query(
        r#"UPDATE orders SET
            status = CASE
                WHEN status = 'canceled' THEN status
                ELSE ?
            END,
            total_cost = CASE
                WHEN (total_cost IS NULL OR total_cost = 0) AND ? IS NOT NULL THEN ?
                ELSE total_cost
            END
        WHERE id = ?"#
    )
    .bind(OrderStatus::Delivered.as_str())
    .bind(order.total_cost)
    .bind(order.total_cost)
    .bind(&order.id)
    .execute(&mut **tx)
    .await?;

    upsert_items_for_event_tx(tx, &order.id, &order.items, ItemStatus::Delivered).await?;

    record_email_event_tx(tx, &order.id, "delivery", email).await?;
    tracing::info!("Order {} marked as delivered", order.id);
    Ok(())
}

// ============================================================================
// Database helper functions
// ============================================================================


/// Result of processing missing product images.
#[derive(Debug, Clone)]
pub struct ImageProcessingResult {
    /// Number of images processed
    pub processed: usize,
    /// If ONNX failed, contains the error message suggesting VC++ installation
    pub onnx_error: Option<String>,
}

/// Process product images that haven't been downloaded/cached yet.
/// Uses local rembg-rs for background removal (model auto-downloaded on first use).
/// Separated from `process_pending_events` to allow concurrent execution with tracking.
/// Returns ImageProcessingResult with onnx_error set if VC++ Redistributable is needed.
pub async fn process_missing_product_images(db: &Database, models_dir: &std::path::Path) -> Result<ImageProcessingResult> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT image_url FROM line_items WHERE image_url IS NOT NULL AND image_url != ''"
    )
    .fetch_all(db.pool())
    .await?;

    if rows.is_empty() {
        return Ok(ImageProcessingResult {
            processed: 0,
            onnx_error: None,
        });
    }

    let mut url_ids: Vec<(String, String)> = Vec::with_capacity(rows.len());
    for (url,) in rows {
        let id = image_id_for_url(&url);
        url_ids.push((url, id));
    }

    let mut existing: HashSet<String> = HashSet::new();
    let ids: Vec<String> = url_ids.iter().map(|(_, id)| id.clone()).collect();
    const CHUNK_SIZE: usize = 500;
    for chunk in ids.chunks(CHUNK_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT id FROM images WHERE id IN ({})",
            placeholders.join(", ")
        );
        let mut query = sqlx::query(&sql);
        for id in chunk {
            query = query.bind(id);
        }
        let rows = query.fetch_all(db.pool()).await?;
        for row in rows {
            let id: String = row.get("id");
            existing.insert(id);
        }
    }

    let missing: Vec<String> = url_ids
        .into_iter()
        .filter_map(|(url, id)| if existing.contains(&id) { None } else { Some(url) })
        .collect();

    if missing.is_empty() {
        let processor = ImageProcessor::new_noop(db.pool().clone()).await?;
        let created = processor.process_missing_thumbnails().await?;
        if created > 0 {
            tracing::info!("Backfilled {} image thumbnails", created);
        }
        return Ok(ImageProcessingResult {
            processed: 0,
            onnx_error: None,
        });
    }

    tracing::info!(
        "Processing {} product images via local rembg",
        missing.len()
    );

    let missing_count = missing.len();
    let (processor, onnx_status) = ImageProcessor::new(db.pool().clone(), models_dir).await?;

    // Extract error message if ONNX failed
    let onnx_error = match onnx_status {
        Some(crate::images::OnnxStatus::NeedsVcRedist(err)) => {
            tracing::warn!("ONNX unavailable, images will not have transparent backgrounds: {}", err);
            Some("Background removal unavailable. Install Visual C++ Redistributable for transparent product images.".to_string())
        }
        _ => None,
    };

    let _ = processor.process_batch(missing).await?;
    let created = processor.process_missing_thumbnails().await?;
    if created > 0 {
        tracing::info!("Backfilled {} image thumbnails", created);
    }

    Ok(ImageProcessingResult {
        processed: missing_count,
        onnx_error,
    })
}

// ============================================================================
// Transaction-aware database helper functions
// ============================================================================

#[tracing::instrument(skip_all, fields(order_id = %order.id))]
async fn insert_order_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order: &WalmartOrder,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT OR IGNORE INTO orders (id, order_date, total_cost, status, tracking_number, carrier, account_id, recipient)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        "#
    )
    .bind(&order.id)
    .bind(order.order_date.to_rfc3339())
    .bind(order.total_cost)
    .bind(order.status.as_str())
    .bind(&order.tracking_number)
    .bind(&order.carrier)
    .bind(order.account_id)
    .bind(&order.recipient)
    .execute(&mut **tx)
    .await
    .context("Failed to insert order")?;

    insert_line_items_tx(tx, &order.id, &order.items).await?;

    Ok(())
}

/// Check if an order already exists in the database (within a transaction).
/// Used to gate shipping/delivery processing on having a confirmation email.
async fn order_exists_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order_id: &str,
) -> Result<bool> {
    let result: Option<(i64,)> = sqlx::query_as("SELECT 1 FROM orders WHERE id = ?")
        .bind(order_id)
        .fetch_optional(&mut **tx)
        .await?;
    Ok(result.is_some())
}

async fn insert_order_row_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order: &WalmartOrder,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT OR IGNORE INTO orders (id, order_date, total_cost, status, tracking_number, carrier, account_id, recipient)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        "#
    )
    .bind(&order.id)
    .bind(order.order_date.to_rfc3339())
    .bind(order.total_cost)
    .bind(order.status.as_str())
    .bind(&order.tracking_number)
    .bind(&order.carrier)
    .bind(order.account_id)
    .bind(&order.recipient)
    .execute(&mut **tx)
    .await
    .context("Failed to insert order")?;

    Ok(())
}

async fn fetch_line_item_names_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order_id: &str,
) -> Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT name FROM line_items WHERE order_id = ?"
    )
    .bind(order_id)
    .fetch_all(&mut **tx)
    .await?;

    Ok(rows.into_iter().map(|(name,)| name).collect())
}

#[tracing::instrument(skip_all, fields(order_id, item_count = items.len()))]
async fn insert_line_items_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order_id: &str,
    items: &[crate::models::LineItem],
) -> Result<()> {
    if items.is_empty() {
        return Ok(());
    }

    // 6 columns per item, SQLite limit 999 → max 166 rows; use 150 conservatively
    const MAX_ITEMS: usize = 150;

    for chunk in items.chunks(MAX_ITEMS) {
        let row_placeholder = "(?,?,?,?,?,?)";
        let placeholders: Vec<&str> = chunk.iter().map(|_| row_placeholder).collect();
        let sql = format!(
            "INSERT INTO line_items (order_id, name, quantity, price, image_url, status) \
             VALUES {} \
             ON CONFLICT(order_id, name) DO UPDATE SET \
                 image_url = COALESCE(line_items.image_url, excluded.image_url), \
                 price = COALESCE(line_items.price, excluded.price)",
            placeholders.join(", ")
        );
        let mut query = sqlx::query(&sql);
        for item in chunk {
            query = query
                .bind(order_id)
                .bind(&item.name)
                .bind(item.quantity as i32)
                .bind(item.price)
                .bind(&item.image_url)
                .bind(item.status.as_str());
        }
        query.execute(&mut **tx).await.context("Failed to batch insert line items")?;
    }

    Ok(())
}

#[tracing::instrument(skip_all, fields(order_id, item_count = items.len(), status = ?status))]
async fn upsert_items_for_event_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order_id: &str,
    items: &[crate::models::LineItem],
    status: ItemStatus,
) -> Result<()> {
    if items.is_empty() {
        return Ok(());
    }

    let existing_names = fetch_line_item_names_tx(tx, order_id).await?;
    if existing_names.is_empty() {
        return insert_line_items_tx(tx, order_id, items).await;
    }

    let existing_map = build_normalized_name_map(&existing_names);
    let mut names_to_update: Vec<String> = Vec::new();
    let mut items_to_insert: Vec<crate::models::LineItem> = Vec::new();

    for item in items {
        let key = normalize_item_name(&item.name);
        if let Some(names) = existing_map.get(&key) {
            for name in names {
                names_to_update.push(name.clone());
            }
        } else {
            items_to_insert.push(item.clone());
        }
    }

    if !names_to_update.is_empty() {
        names_to_update.sort();
        names_to_update.dedup();
        let name_refs: Vec<&str> = names_to_update.iter().map(|name| name.as_str()).collect();
        batch_update_item_status_tx(tx, order_id, &name_refs, status).await?;
    }

    if !items_to_insert.is_empty() {
        insert_line_items_tx(tx, order_id, &items_to_insert).await?;
    }

    Ok(())
}


/// Check whether a status transition is valid.
/// Terminal states (Canceled) cannot be overridden.
/// Delivered can only be overridden by Canceled.
fn is_valid_status_transition(current: &str, new: &str) -> bool {
    match current {
        "canceled" => false,
        "delivered" => new == "canceled",
        _ => true,
    }
}

async fn update_order_status_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order_id: &str,
    status: OrderStatus,
) -> Result<()> {
    let current: Option<(String,)> =
        sqlx::query_as("SELECT status FROM orders WHERE id = ?")
            .bind(order_id)
            .fetch_optional(&mut **tx)
            .await?;

    if let Some((current_status,)) = current {
        if !is_valid_status_transition(&current_status, status.as_str()) {
            tracing::warn!(
                "Blocked status transition for order {}: {} → {}",
                order_id,
                current_status,
                status.as_str()
            );
            return Ok(());
        }
    }

    sqlx::query("UPDATE orders SET status = ? WHERE id = ?")
        .bind(status.as_str())
        .bind(order_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

fn millis_to_rfc3339(millis_str: &str) -> String {
    millis_str
        .parse::<i64>()
        .ok()
        .and_then(|ms| chrono::DateTime::from_timestamp_millis(ms))
        .map(|dt: chrono::DateTime<chrono::Utc>| dt.to_rfc3339())
        .unwrap_or_else(|| millis_str.to_string())
}

async fn cancel_all_items_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order_id: &str,
) -> Result<()> {
    sqlx::query("UPDATE line_items SET status = 'canceled' WHERE order_id = ?")
        .bind(order_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn batch_update_item_status_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order_id: &str,
    item_names: &[&str],
    status: ItemStatus,
) -> Result<()> {
    if item_names.is_empty() {
        return Ok(());
    }

    let placeholders: Vec<&str> = item_names.iter().map(|_| "?").collect();
    let query = format!(
        "UPDATE line_items SET status = ? WHERE order_id = ? AND name IN ({})",
        placeholders.join(", ")
    );

    let mut q = sqlx::query(&query)
        .bind(status.as_str())
        .bind(order_id);
    for name in item_names {
        q = q.bind(*name);
    }
    q.execute(&mut **tx).await?;
    Ok(())
}

async fn batch_cancel_items_by_name_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order_id: &str,
    item_names: &[&str],
) -> Result<()> {
    if item_names.is_empty() {
        return Ok(());
    }

    let placeholders: Vec<&str> = item_names.iter().map(|_| "?").collect();
    let query = format!(
        "UPDATE line_items SET status = 'canceled' WHERE order_id = ? AND name IN ({})",
        placeholders.join(", ")
    );

    let mut q = sqlx::query(&query).bind(order_id);
    for name in item_names {
        q = q.bind(*name);
    }
    q.execute(&mut **tx).await?;
    Ok(())
}

async fn check_all_items_canceled_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order_id: &str,
) -> Result<bool> {
    let non_canceled: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM line_items WHERE order_id = ? AND status != 'canceled'"
    )
    .bind(order_id)
    .fetch_one(&mut **tx)
    .await?;

    Ok(non_canceled.0 == 0)
}

async fn record_email_event_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order_id: &str,
    event_type: &str,
    email: &RawEmail,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO email_events (order_id, event_type, email_subject, email_date, raw_html)
        VALUES (?, ?, ?, ?, NULL)
        "#
    )
    .bind(order_id)
    .bind(event_type)
    .bind(&email.subject)
    .bind(&email.gmail_date)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

// ============================================================================
// Batch status update functions for performance
// ============================================================================


/// Batch mark multiple emails as processed
#[tracing::instrument(skip_all, fields(count = ids.len()))]
async fn batch_mark_emails_processed(db: &Database, ids: &[i64]) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }

    // SQLite has a limit on variables, process in chunks
    const CHUNK_SIZE: usize = 500;
    for chunk in ids.chunks(CHUNK_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let query = format!(
            "UPDATE raw_emails SET processing_status = 'processed', processed_at = datetime('now'), raw_body = '' WHERE id IN ({})",
            placeholders.join(", ")
        );

        let mut q = sqlx::query(&query);
        for id in chunk {
            q = q.bind(*id);
        }
        q.execute(db.pool()).await?;
    }
    Ok(())
}

/// Batch mark multiple emails as skipped
#[tracing::instrument(skip_all, fields(count = ids.len()))]
async fn batch_mark_emails_skipped(db: &Database, ids: &[i64]) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }

    const CHUNK_SIZE: usize = 500;
    for chunk in ids.chunks(CHUNK_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let query = format!(
            "UPDATE raw_emails SET processing_status = 'skipped', processed_at = datetime('now'), raw_body = '' WHERE id IN ({})",
            placeholders.join(", ")
        );

        let mut q = sqlx::query(&query);
        for id in chunk {
            q = q.bind(*id);
        }
        q.execute(db.pool()).await?;
    }
    Ok(())
}

/// Mark emails as deferred (missing order, will retry after all batches).
/// Unlike batch_mark_emails_skipped, this does NOT clear raw_body.
#[tracing::instrument(skip_all, fields(count = ids.len()))]
async fn batch_mark_emails_deferred(db: &Database, ids: &[i64]) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    const CHUNK_SIZE: usize = 500;
    for chunk in ids.chunks(CHUNK_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let query = format!(
            "UPDATE raw_emails SET processing_status = 'deferred' WHERE id IN ({})",
            placeholders.join(", ")
        );
        let mut q = sqlx::query(&query);
        for id in chunk {
            q = q.bind(*id);
        }
        q.execute(db.pool()).await?;
    }
    Ok(())
}

/// Reset all deferred emails back to pending for final retry pass.
async fn reset_deferred_to_pending(db: &Database) -> Result<u64> {
    let result = sqlx::query(
        "UPDATE raw_emails SET processing_status = 'pending' WHERE processing_status = 'deferred'"
    )
    .execute(db.pool())
    .await?;
    Ok(result.rows_affected())
}

/// Batch mark multiple emails as failed with their error messages
#[tracing::instrument(skip_all, fields(count = entries.len()))]
async fn batch_mark_emails_failed(db: &Database, entries: &[(i64, String)]) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }

    // For failed emails, we need individual updates due to different error messages
    // But we can do them in a transaction for efficiency
    let mut tx = db.pool().begin().await?;
    for (id, error) in entries {
        sqlx::query(
            "UPDATE raw_emails SET processing_status = 'failed', error_message = ?, processed_at = datetime('now'), raw_body = '' WHERE id = ?"
        )
        .bind(error)
        .bind(*id)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Check if an error message indicates a transient SQLite lock contention.
/// These should be deferred for retry rather than permanently failed.
fn is_db_lock_error(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    lower.contains("database is locked") || lower.contains("database is busy")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_item_name_variants() {
        let a = "Pokemon Scarlet & Violet 8.5 Prismatic Evolutions Poster Collection";
        let b = "Pokemon Scarlet Violet 8 5 Prismatic Evolutions Poster Collection";
        let c = "Pokemon Trading Card Games Scarlett & Violet 8.5 Prismatic Evolutions Surprise Box";
        let d = "Pokemon Trading Card Games Scarlett Violet 8 5 Prismatic Evolutions Surprise Box";

        assert_eq!(normalize_item_name(a), normalize_item_name(b));
        assert_eq!(normalize_item_name(c), normalize_item_name(d));
        assert_eq!(normalize_item_name("Scarlet & Violet"), normalize_item_name("Scarlet and Violet"));
    }

    #[tokio::test]
    async fn test_process_empty_queue() {
        let db = Database::in_memory().await.expect("Should create DB");
        db.run_migrations().await.expect("Should run migrations");

        let stats = process_pending_events(&db).await.expect("Should process");
        assert_eq!(stats.total_pending, 0);
        assert_eq!(stats.processed, 0);
    }

    /// Tests the full parse_single_email pipeline with a real .eml file.
    /// Tests both the properly-decoded path and the raw-MIME path (legacy).
    /// Verifies that the double-decode guard doesn't corrupt the HTML.
    #[test]
    fn test_parse_single_email_real_eml() {
        use crate::ingestion::gmail::find_html_part;

        let eml_path = std::path::Path::new("emails/200013348923251-order-confirmation.eml");
        if !eml_path.exists() {
            eprintln!("Skipping test: .eml fixture not found");
            return;
        }

        let eml_bytes = std::fs::read(eml_path).expect("Should read .eml file");
        let parsed_mail = mailparse::parse_mail(&eml_bytes).expect("Should parse MIME");
        let html = find_html_part(&parsed_mail).expect("Should find HTML part");

        // Check if the double-decode guard would trigger
        let has_eq20 = html.contains("=20");
        let has_eq3d = html.contains("=3D");
        eprintln!(
            "Decoded HTML: len={}, contains '=20': {}, contains '=3D': {}",
            html.len(), has_eq20, has_eq3d
        );

        // Simulate the parse_single_email logic
        let needs_decode = has_eq20 || has_eq3d;
        let processed_html = if needs_decode {
            eprintln!("WARNING: Double-decode guard TRIGGERED on already-decoded HTML");
            decode_quoted_printable(&html)
        } else {
            html.clone()
        };

        let parser = crate::parsing::WalmartEmailParser::new();
        let email_type = parser.detect_email_type(&processed_html);
        assert_eq!(
            email_type,
            crate::parsing::EmailType::Confirmation,
            "Should detect as Confirmation after processing"
        );

        let order = parser.parse_order(&processed_html, None)
            .expect("Should parse order");
        assert_eq!(order.id, "200013348923251");
        assert!(
            order.total_cost.is_some(),
            "Order should have total_cost (double-decode guard triggered: {})",
            needs_decode
        );
        assert!(
            (order.total_cost.unwrap() - 818.42).abs() < 0.01,
            "Order total should be $818.42, got: {:?}",
            order.total_cost
        );
    }

    /// Verifies that delivery/shipping emails for orders without a confirmation
    /// are skipped (not inserted into the database).
    #[tokio::test]
    async fn test_skip_delivery_without_confirmation() {
        use crate::parsing::EmailType;
        use crate::models::{WalmartOrder, OrderStatus};
        use chrono::Utc;

        let db = Database::in_memory().await.expect("Should create DB");
        db.run_migrations().await.expect("Should run migrations");

        // Create a parsed delivery email for an order that has no confirmation
        let raw_email = RawEmail {
            id: 1,
            gmail_id: "test_gmail_id".to_string(),
            subject: Some("Your order has been delivered".to_string()),
            raw_body: String::new(),
            gmail_date: Some("1692300000000".to_string()),
            recipient: None,
            account_id: None,
        };

        let order = WalmartOrder::new("200013348923251", Utc::now(), OrderStatus::Delivered);

        let parsed = ParsedEmail {
            id: 1,
            gmail_id: "test_gmail_id".to_string(),
            email_type: EmailType::Delivery,
            order: Some(order),
            raw_email,
        };

        let mut tx = db.pool().begin().await.expect("Should begin tx");
        let mut pending_tracking = Vec::new();

        let result = apply_parsed_email_to_db_tx(&mut tx, &parsed, &mut pending_tracking)
            .await
            .expect("Should not error");

        tx.commit().await.expect("Should commit");

        match result {
            ProcessResult::SkippedMissingOrder(reason) => {
                assert!(reason.contains("No confirmation"), "Expected skip reason, got: {}", reason);
            }
            ProcessResult::Skipped(reason) => {
                assert!(reason.contains("No confirmation"), "Expected skip reason, got: {}", reason);
            }
            ProcessResult::Processed => {
                panic!("Delivery email without confirmation should be skipped, not processed");
            }
        }

        // Verify order was NOT created
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM orders WHERE id = '200013348923251'")
            .fetch_one(db.pool())
            .await
            .expect("Should query");
        assert_eq!(count.0, 0, "Order should not exist in DB");
    }

    /// Simulates the legacy path where raw_body is the full MIME content
    /// (base64-decoded but NOT MIME-parsed). This was the old `fetch_email` behavior.
    #[test]
    fn test_parse_single_email_raw_mime_legacy() {
        let eml_path = std::path::Path::new("emails/200013348923251-order-confirmation.eml");
        if !eml_path.exists() {
            eprintln!("Skipping test: .eml fixture not found");
            return;
        }

        // Read the raw .eml content (this is what the old fetch_email would store)
        let raw_mime = std::fs::read_to_string(eml_path).expect("Should read .eml file");

        // Simulate parse_single_email double-decode check
        let needs_decode = raw_mime.contains("=20") || raw_mime.contains("=3D");
        eprintln!(
            "Raw MIME: len={}, needs_decode: {}",
            raw_mime.len(), needs_decode
        );

        let processed = if needs_decode {
            decode_quoted_printable(&raw_mime)
        } else {
            raw_mime.clone()
        };

        let parser = crate::parsing::WalmartEmailParser::new();
        let email_type = parser.detect_email_type(&processed);
        eprintln!("Email type from raw MIME: {:?}", email_type);

        if email_type != crate::parsing::EmailType::Unknown {
            let order = parser.parse_order(&processed, None)
                .expect("Should parse order from raw MIME");
            eprintln!(
                "Order from raw MIME: id={}, total={:?}, items={}",
                order.id, order.total_cost, order.items.len()
            );
            assert_eq!(order.id, "200013348923251");
            assert!(
                order.total_cost.is_some(),
                "Order from raw MIME should have total_cost"
            );
        } else {
            panic!("Email type is Unknown from raw MIME — this is likely the bug");
        }
    }
}
