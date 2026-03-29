//! Email ingestion module
//!
//! Handles syncing emails from Gmail to the local database.
//! This is the first step in the pipeline - we fetch and store raw emails
//! before parsing them into orders.

pub mod gmail;

use crate::auth::{AccountAuth, GmailClient, get_gmail_client_for_account};
use crate::db::{Database, EmailData};
use crate::web::fetch_pending_email_count_filtered;
use anyhow::Result;
use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::env;
use std::time::Duration;
use tokio::sync::Semaphore;

pub use gmail::{FetchedEmail, GmailFetcher, MessageRef};

/// Configuration for sync performance
const DEFAULT_CONCURRENT_FETCHES: usize = 48;
const MIN_CONCURRENT_FETCHES: usize = 4;
const MAX_CONCURRENT_FETCHES: usize = 64;
const BATCH_INSERT_SIZE: usize = 50;

/// Retry configuration for rate-limited fetches
const MAX_RETRY_ROUNDS: usize = 3;
const RETRY_COOLDOWN_BASE_SECS: u64 = 15;
const MIN_RETRY_CONCURRENCY: usize = 4;

pub fn max_concurrent_fetches() -> usize {
    let value = env::var("WALMART_GMAIL_FETCH_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_CONCURRENT_FETCHES);

    value.clamp(MIN_CONCURRENT_FETCHES, MAX_CONCURRENT_FETCHES)
}

/// Statistics from a sync operation
#[derive(Debug, Default)]
pub struct SyncStats {
    /// Total emails found in Gmail
    pub total_found: usize,
    /// Total emails fetched from Gmail API
    pub total_fetched: usize,
    /// Emails that were already in the database
    pub skipped: usize,
    /// New emails successfully synced
    pub synced: usize,
    /// Alias for synced (for CLI compatibility)
    pub new_emails: usize,
    /// Emails that failed to sync
    pub failed: usize,
    /// Emails successfully fetched via retry rounds
    pub retried: usize,
}

impl SyncStats {
    pub fn summary(&self) -> String {
        if self.retried > 0 {
            format!(
                "Sync complete: {} found, {} synced ({} retried), {} skipped, {} failed",
                self.total_found, self.synced, self.retried, self.skipped, self.failed
            )
        } else {
            format!(
                "Sync complete: {} found, {} synced, {} skipped, {} failed",
                self.total_found, self.synced, self.skipped, self.failed
            )
        }
    }
}

/// Sync Walmart emails from Gmail to the database with a day limit
///
/// # Arguments
/// * `db` - Database connection
/// * `gmail_client` - Authenticated Gmail API client
/// * `days` - Only fetch emails from the last N days
///
/// # Returns
/// Statistics about the sync operation
pub async fn sync_emails_with_days(
    db: &Database,
    gmail_client: GmailClient,
    days: u32,
) -> Result<SyncStats> {
    let fetcher = GmailFetcher::new(gmail_client);
    let mut stats = SyncStats::default();

    // List Walmart emails with date filter
    tracing::info!("Fetching Walmart emails from the last {} days...", days);
    let messages = fetcher.list_walmart_emails(Some(days)).await?;
    stats.total_found = messages.len();

    if messages.is_empty() {
        tracing::info!("No Walmart emails found in the last {} days", days);
        return Ok(stats);
    }

    tracing::info!("Found {} Walmart-related emails", messages.len());

    // Process emails
    process_messages(&fetcher, db, messages, &mut stats).await?;

    Ok(stats)
}

/// Sync with a custom search query
pub async fn sync_emails_with_query(
    db: &Database,
    gmail_client: GmailClient,
    query: &str,
) -> Result<SyncStats> {
    let fetcher = GmailFetcher::new(gmail_client);
    let mut stats = SyncStats::default();

    tracing::info!("Fetching emails with query: {}", query);
    let messages = fetcher.list_emails_with_query(query).await?;
    stats.total_found = messages.len();

    if messages.is_empty() {
        tracing::info!("No matching emails found");
        return Ok(stats);
    }

    process_messages(&fetcher, db, messages, &mut stats).await?;

    Ok(stats)
}

/// Fetch emails from Gmail with automatic retry rounds for rate-limited failures.
///
/// After the initial fetch, any emails that failed with retryable errors (429, timeouts,
/// connection errors) are automatically retried up to `MAX_RETRY_ROUNDS` times with
/// progressive cooldowns and reduced concurrency.
async fn fetch_emails_with_retries(
    fetcher: &GmailFetcher,
    ids_to_fetch: Vec<String>,
    initial_concurrency: usize,
    rate_limit: Option<Arc<Semaphore>>,
    pb: &ProgressBar,
    stats: &mut SyncStats,
) -> Vec<FetchedEmail> {
    let mut all_emails = Vec::new();
    let mut pending_ids = ids_to_fetch;
    let mut concurrency = initial_concurrency;

    for round in 0..=MAX_RETRY_ROUNDS {
        if pending_ids.is_empty() {
            break;
        }

        if round > 0 {
            let cooldown_secs = RETRY_COOLDOWN_BASE_SECS * round as u64;
            concurrency = (concurrency / 2).max(MIN_RETRY_CONCURRENCY);
            tracing::info!(
                "Retry round {}/{}: {} emails, cooldown {}s, concurrency {}",
                round, MAX_RETRY_ROUNDS, pending_ids.len(), cooldown_secs, concurrency
            );
            pb.set_message(format!("Waiting {}s before retry round {}...", cooldown_secs, round));
            tokio::time::sleep(Duration::from_secs(cooldown_secs)).await;
            pb.set_message(format!("Retrying {} emails (round {})...", pending_ids.len(), round));
            // Extend the progress bar to show retry progress
            pb.set_length(pb.length().unwrap_or(0) + pending_ids.len() as u64);
        }

        let fetch_results: Vec<Result<Option<FetchedEmail>, (String, anyhow::Error)>> =
            stream::iter(pending_ids.iter().cloned())
                .map(|id| {
                    let rate_limit = rate_limit.clone();
                    async move {
                        // Acquire shared rate limit permit (if multi-account sync)
                        let _permit = match &rate_limit {
                            Some(sem) => Some(sem.acquire().await.expect("rate limit semaphore closed")),
                            None => None,
                        };
                        match fetcher.fetch_email_full_with_retry(&id).await {
                            Ok(email) => Ok(email),
                            Err(e) => Err((id, e)),
                        }
                    }
                })
                .buffer_unordered(concurrency)
                .inspect(|_| pb.inc(1))
                .collect()
                .await;

        let mut retryable_ids = Vec::new();

        for result in fetch_results {
            match result {
                Ok(Some(email)) => {
                    if round > 0 {
                        stats.retried += 1;
                    }
                    all_emails.push(email);
                }
                Ok(None) => {
                    stats.skipped += 1;
                }
                Err((id, err)) => {
                    if round < MAX_RETRY_ROUNDS && gmail::is_retryable_email_error(&err) {
                        retryable_ids.push(id);
                    } else {
                        stats.failed += 1;
                        tracing::warn!("Failed to fetch email {}: {:#}", id, err);
                    }
                }
            }
        }

        if round == 0 && !retryable_ids.is_empty() {
            tracing::info!(
                "{} emails failed with retryable errors, will retry up to {} rounds",
                retryable_ids.len(),
                MAX_RETRY_ROUNDS
            );
        }

        pending_ids = retryable_ids;
    }

    all_emails
}

/// Process a list of messages with parallel fetching and batch inserts
async fn process_messages(
    fetcher: &GmailFetcher,
    db: &Database,
    messages: Vec<MessageRef>,
    stats: &mut SyncStats,
) -> Result<()> {
    let pb = ProgressBar::new(messages.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
            .expect("Invalid progress bar template")
            .progress_chars("#>-"),
    );
    pb.enable_steady_tick(Duration::from_millis(100));

    // Step 1: Bulk check which emails already exist (single query)
    pb.set_message("Checking existing emails...");
    let all_ids: Vec<&str> = messages.iter().map(|m| m.id.as_str()).collect();
    let existing_ids = db.get_existing_gmail_ids(&all_ids).await?;

    // Filter to only new messages
    let to_fetch: Vec<&MessageRef> = messages
        .iter()
        .filter(|m| !existing_ids.contains(&m.id))
        .collect();

    stats.skipped = existing_ids.len();
    let new_count = to_fetch.len();
    stats.total_fetched = new_count;

    if new_count == 0 {
        pb.finish_with_message("All emails already synced!");
        tracing::info!("{}", stats.summary());
        return Ok(());
    }

    tracing::info!(
        "Found {} new emails to fetch ({} already synced)",
        new_count,
        stats.skipped
    );

    // Update progress bar for actual work
    pb.set_length(new_count as u64);
    pb.set_position(0);

    // Step 2: Fetch with automatic retry rounds for rate-limited failures
    pb.set_message("Fetching emails...");
    let concurrency = max_concurrent_fetches();
    tracing::info!("Fetching emails with concurrency {}", concurrency);

    let ids_to_fetch: Vec<String> = to_fetch.iter().map(|m| m.id.clone()).collect();
    let fetched_emails = fetch_emails_with_retries(
        fetcher, ids_to_fetch, concurrency, None, &pb, stats,
    ).await;

    // Step 3: Batch insert fetched emails
    pb.set_message("Storing emails...");
    let mut batch: Vec<EmailData> = Vec::with_capacity(BATCH_INSERT_SIZE);

    for email in fetched_emails {
        let event_type = gmail::infer_event_type(
            email.subject.as_deref(),
            email.snippet.as_deref(),
        );

        batch.push(EmailData {
            gmail_id: email.gmail_id,
            thread_id: email.thread_id,
            subject: email.subject,
            snippet: email.snippet,
            sender: email.sender,
            recipient: email.recipient,
            raw_body: email.raw_body,
            event_type: event_type.to_string(),
            gmail_date: email.internal_date,
            account_id: None, // Legacy mode - no account tracking
        });

        if batch.len() >= BATCH_INSERT_SIZE {
            match db.insert_raw_emails_batch(&batch).await {
                Ok(inserted) => {
                    stats.synced += inserted;
                    stats.new_emails += inserted;
                    tracing::debug!("Batch inserted {} emails", inserted);
                }
                Err(e) => {
                    stats.failed += batch.len();
                    tracing::warn!("Batch insert failed: {}", e);
                }
            }
            batch.clear();
        }
    }

    // Insert remaining batch
    if !batch.is_empty() {
        match db.insert_raw_emails_batch(&batch).await {
            Ok(inserted) => {
                stats.synced += inserted;
                stats.new_emails += inserted;
                tracing::debug!("Final batch inserted {} emails", inserted);
            }
            Err(e) => {
                stats.failed += batch.len();
                tracing::warn!("Final batch insert failed: {}", e);
            }
        }
    }

    pb.finish_with_message("Done!");
    tracing::info!("{}", stats.summary());

    Ok(())
}

/// Get sync status (counts of emails in database)
pub async fn get_sync_status(db: &Database) -> Result<String> {
    let counts = db.get_email_counts().await?;
    Ok(format!(
        "Database status:\n  Total emails: {}\n  Pending: {}\n  Processed: {}",
        counts.total, counts.pending, counts.processed
    ))
}

// ==================== Multi-Account Sync Functions ====================

/// Sync Walmart emails from Gmail to the database with account tracking
pub async fn sync_emails_with_days_and_account(
    db: &Database,
    gmail_client: GmailClient,
    days: u32,
    account_id: i64,
    rate_limit: Option<Arc<Semaphore>>,
) -> Result<SyncStats> {
    let fetcher = GmailFetcher::new(gmail_client);
    let mut stats = SyncStats::default();

    tracing::info!("Fetching Walmart emails from the last {} days for account {}...", days, account_id);
    let messages = fetcher.list_walmart_emails(Some(days)).await?;
    stats.total_found = messages.len();

    if messages.is_empty() {
        tracing::info!("No Walmart emails found in the last {} days", days);
        return Ok(stats);
    }

    tracing::info!("Found {} Walmart-related emails", messages.len());

    process_messages_with_account(&fetcher, db, messages, &mut stats, account_id, rate_limit).await?;

    Ok(stats)
}

/// Sync with a custom query and account tracking
pub async fn sync_emails_with_query_and_account(
    db: &Database,
    gmail_client: GmailClient,
    query: &str,
    account_id: i64,
    rate_limit: Option<Arc<Semaphore>>,
) -> Result<SyncStats> {
    let fetcher = GmailFetcher::new(gmail_client);
    let mut stats = SyncStats::default();

    tracing::info!("Fetching emails with query: {} for account {}", query, account_id);
    let messages = fetcher.list_emails_with_query(query).await?;
    stats.total_found = messages.len();

    if messages.is_empty() {
        tracing::info!("No matching emails found");
        return Ok(stats);
    }

    process_messages_with_account(&fetcher, db, messages, &mut stats, account_id, rate_limit).await?;

    Ok(stats)
}

/// Process messages with account_id tracking
async fn process_messages_with_account(
    fetcher: &GmailFetcher,
    db: &Database,
    messages: Vec<MessageRef>,
    stats: &mut SyncStats,
    account_id: i64,
    rate_limit: Option<Arc<Semaphore>>,
) -> Result<()> {
    let pb = ProgressBar::new(messages.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
            .expect("Invalid progress bar template")
            .progress_chars("#>-"),
    );
    pb.enable_steady_tick(Duration::from_millis(100));

    // Step 1: Bulk check which emails already exist for this account
    pb.set_message("Checking existing emails...");
    let all_ids: Vec<&str> = messages.iter().map(|m| m.id.as_str()).collect();
    let existing_ids = db.get_existing_gmail_ids_for_account(account_id, &all_ids).await?;

    // Filter to only new messages
    let to_fetch: Vec<&MessageRef> = messages
        .iter()
        .filter(|m| !existing_ids.contains(&m.id))
        .collect();

    stats.skipped = existing_ids.len();
    let new_count = to_fetch.len();
    stats.total_fetched = new_count;

    if new_count == 0 {
        pb.finish_with_message("All emails already synced!");
        tracing::info!("{}", stats.summary());
        return Ok(());
    }

    tracing::info!(
        "Found {} new emails to fetch ({} already synced)",
        new_count,
        stats.skipped
    );

    // Update progress bar for actual work
    pb.set_length(new_count as u64);
    pb.set_position(0);

    // Step 2: Fetch with automatic retry rounds for rate-limited failures
    pb.set_message("Fetching emails...");
    let concurrency = max_concurrent_fetches();
    tracing::info!("Fetching emails with concurrency {}", concurrency);

    let ids_to_fetch: Vec<String> = to_fetch.iter().map(|m| m.id.clone()).collect();
    let fetched_emails = fetch_emails_with_retries(
        fetcher, ids_to_fetch, concurrency, rate_limit, &pb, stats,
    ).await;

    // Step 3: Batch insert fetched emails
    pb.set_message("Storing emails...");
    let mut batch: Vec<EmailData> = Vec::with_capacity(BATCH_INSERT_SIZE);

    for email in fetched_emails {
        let event_type = gmail::infer_event_type(
            email.subject.as_deref(),
            email.snippet.as_deref(),
        );

        batch.push(EmailData {
            gmail_id: email.gmail_id,
            thread_id: email.thread_id,
            subject: email.subject,
            snippet: email.snippet,
            sender: email.sender,
            recipient: email.recipient,
            raw_body: email.raw_body,
            event_type: event_type.to_string(),
            gmail_date: email.internal_date,
            account_id: Some(account_id),
        });

        if batch.len() >= BATCH_INSERT_SIZE {
            match db.insert_raw_emails_batch_with_account(account_id, &batch).await {
                Ok(inserted) => {
                    stats.synced += inserted;
                    stats.new_emails += inserted;
                    tracing::debug!("Batch inserted {} emails", inserted);
                }
                Err(e) => {
                    stats.failed += batch.len();
                    tracing::warn!("Batch insert failed: {}", e);
                }
            }
            batch.clear();
        }
    }

    // Insert remaining batch
    if !batch.is_empty() {
        match db.insert_raw_emails_batch_with_account(account_id, &batch).await {
            Ok(inserted) => {
                stats.synced += inserted;
                stats.new_emails += inserted;
                tracing::debug!("Final batch inserted {} emails", inserted);
            }
            Err(e) => {
                stats.failed += batch.len();
                tracing::warn!("Final batch insert failed: {}", e);
            }
        }
    }

    pb.finish_with_message("Done!");
    tracing::info!("{}", stats.summary());

    Ok(())
}

// ============================================================================
// Lightweight new-email check (no downloads)
// ============================================================================

/// Result of a lightweight new-email check
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewEmailCheck {
    /// New emails on Gmail not yet in local DB
    pub total_new: usize,
    /// Emails in local DB not yet processed into orders
    pub total_pending: i64,
}

/// Check for new emails across all active accounts without downloading content.
///
/// Lists Gmail message IDs and deduplicates against the local DB.
/// When `fetch_since` is provided (e.g. "2025-01-02"), uses the same date range as sync.
/// Otherwise falls back to the last 60 days.
/// Returns counts of new (unsynced) and pending (unprocessed) emails.
/// Fails silently per-account — auth/network errors are logged and skipped.
pub async fn check_new_emails(
    db: &Database,
    client_secret_path: &Path,
    base_dir: &Path,
    fetch_since: Option<&str>,
) -> NewEmailCheck {
    let mut result = NewEmailCheck {
        total_new: 0,
        total_pending: 0,
    };

    let accounts = match db.list_accounts().await {
        Ok(accs) => accs,
        Err(err) => {
            tracing::warn!("Failed to list accounts for new email check: {}", err);
            return result;
        }
    };

    for acc in &accounts {
        if !acc.is_active {
            continue;
        }

        // Count locally pending emails for this account
        if let Ok(pending) = fetch_pending_email_count_filtered(db, Some(acc.id)).await {
            result.total_pending += pending;
        }

        // Resolve token cache path
        let token_path = PathBuf::from(&acc.token_cache_path);
        let resolved_path = if token_path.is_absolute() {
            token_path
        } else {
            base_dir.join(token_path)
        };

        let account_auth = AccountAuth::with_path(&acc.email, resolved_path);

        // Try to build Gmail client (may fail if token expired / offline)
        let gmail_client = match get_gmail_client_for_account(client_secret_path, &account_auth).await {
            Ok(client) => client,
            Err(err) => {
                tracing::warn!(email = %acc.email, error = %err, "Auth failed during new email check");
                continue;
            }
        };

        let fetcher = GmailFetcher::new(gmail_client);

        // List Gmail IDs (cheap — IDs only, no content)
        // Use fetch_since date if provided, otherwise default to last 60 days
        let list_result = match fetch_since {
            Some(since_date) => {
                let query = gmail::build_walmart_query_since(since_date);
                fetcher.list_emails_with_query(&query).await
            }
            None => fetcher.list_walmart_emails(Some(60)).await,
        };
        let messages = match list_result {
            Ok(msgs) => msgs,
            Err(err) => {
                tracing::warn!(email = %acc.email, error = %err, "Failed to list Gmail messages");
                continue;
            }
        };

        if messages.is_empty() {
            continue;
        }

        // Dedup against local DB
        let all_ids: Vec<&str> = messages.iter().map(|msg| msg.id.as_str()).collect();
        match db.get_existing_gmail_ids_for_account(acc.id, &all_ids).await {
            Ok(existing) => {
                let new_count = messages.len().saturating_sub(existing.len());
                result.total_new += new_count;
                tracing::info!(
                    email = %acc.email,
                    total = messages.len(),
                    existing = existing.len(),
                    new = new_count,
                    "New email check complete for account"
                );
            }
            Err(err) => {
                tracing::warn!(email = %acc.email, error = %err, "Failed to check existing gmail IDs");
            }
        }
    }

    tracing::info!(
        total_new = result.total_new,
        total_pending = result.total_pending,
        "New email check complete"
    );

    result
}
