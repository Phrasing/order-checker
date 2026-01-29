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
use std::sync::Arc;

/// Tracking info to create after transaction commits
struct PendingTrackingEntry {
    order_id: String,
    tracking_number: String,
    carrier: String,
}

/// Maximum concurrent email parsing tasks (CPU-bound, can be parallel)
const MAX_CONCURRENT_PARSING: usize = 50;

/// Number of emails to process in a single batch (limits memory usage and lock duration)
const BATCH_SIZE: i64 = 100;

/// Maximum concurrent database write tasks (unused - now using single transaction)
#[allow(dead_code)]
const MAX_CONCURRENT_DB_WRITES: usize = 1;

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
}

impl ProcessStats {
    pub fn summary(&self) -> String {
        format!(
            "Processing complete: {} pending, {} processed, {} skipped, {} failed",
            self.total_pending, self.processed, self.skipped, self.failed
        )
    }

    fn add(&mut self, other: &ProcessStats) {
        self.total_pending += other.total_pending;
        self.processed += other.processed;
        self.failed += other.failed;
        self.skipped += other.skipped;
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

/// Process all pending emails and reconcile into orders
pub async fn process_pending_events(db: &Database) -> Result<ProcessStats> {
    let mut total_stats = ProcessStats::default();
    let parser = Arc::new(WalmartEmailParser::new());

    loop {
        let mut batch_stats = ProcessStats::default();
        
        // Fetch a batch of pending raw emails
        let pending_emails = fetch_pending_emails(db, BATCH_SIZE).await?;
        batch_stats.total_pending = pending_emails.len();

        if pending_emails.is_empty() {
            if total_stats.total_pending == 0 {
                tracing::info!("No pending emails to process");
            }
            break;
        }

        tracing::info!("Processing batch of {} pending emails", pending_emails.len());

        // PHASE 1: Parse all emails in parallel (CPU-bound, use blocking thread pool)
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
            .buffer_unordered(MAX_CONCURRENT_PARSING)
            .collect()
            .await;

        // Separate successes from failures
        let mut to_process: Vec<ParsedEmail> = Vec::new();
        let mut parse_failures: Vec<(i64, String)> = Vec::new();
        let mut skipped_ids: Vec<i64> = Vec::new();

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

        // PHASE 2: Write to database in a SINGLE TRANSACTION (eliminates lock contention)
        let mut processed_ids: Vec<i64> = Vec::new();
        let mut db_failed_entries: Vec<(i64, String)> = Vec::new();
        let mut pending_tracking: Vec<PendingTrackingEntry> = Vec::new();

        // Begin transaction for all DB writes
        let mut tx = db.pool().begin().await.context("Failed to begin transaction")?;

        for parsed in &to_process {
            let result = apply_parsed_email_to_db_tx(&mut tx, parsed, &mut pending_tracking).await;
            match result {
                Ok(ProcessResult::Processed) => {
                    batch_stats.processed += 1;
                    processed_ids.push(parsed.id);
                    tracing::debug!("Processed email {}", parsed.gmail_id);
                }
                            Ok(ProcessResult::Skipped(reason)) => {
                                skipped_ids.push(parsed.id);
                                tracing::warn!(
                                    "Skipped email {}: {}. Subject: {:?}. Body len: {}",
                                    parsed.gmail_id,
                                    reason,
                                    parsed.raw_email.subject,
                                    parsed.raw_email.raw_body.len()
                                );
                            }                Err(e) => {
                    let error_msg = format!("{:#}", e);
                    batch_stats.failed += 1;
                    db_failed_entries.push((parsed.id, error_msg.clone()));
                    tracing::warn!("Failed to process email {}: {}", parsed.gmail_id, error_msg);
                }
            }
        }

        // Commit the transaction
        tx.commit().await.context("Failed to commit transaction")?;
        tracing::info!("Committed {} order updates in batch", batch_stats.processed);

        // Create tracking cache entries AFTER transaction commits (non-critical)
        for entry in pending_tracking {
            if let Err(e) = create_tracking_cache_entry(db, &entry.order_id, &entry.tracking_number, &entry.carrier).await {
                tracing::warn!("Failed to create tracking cache entry for {}: {}", entry.tracking_number, e);
            }
        }

        // Combine parse failures with DB failures
        let mut all_failed: Vec<(i64, String)> = parse_failures;
        all_failed.extend(db_failed_entries);

        // Batch update email statuses
        if !processed_ids.is_empty() {
            batch_mark_emails_processed(db, &processed_ids).await?;
        }
        if !skipped_ids.is_empty() {
            batch_mark_emails_skipped(db, &skipped_ids).await?;
        }
        if !all_failed.is_empty() {
            batch_mark_emails_failed(db, &all_failed).await?;
        }
        
        total_stats.add(&batch_stats);
    }

    tracing::info!("{}", total_stats.summary());

    // Diagnostic: verify orders are actually persisted in the database
    match sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM orders")
        .fetch_one(db.pool())
        .await
    {
        Ok((order_count,)) => tracing::info!("Orders in database after processing: {}", order_count),
        Err(err) => tracing::error!("Failed to count orders after processing: {}", err),
    }

    if let Err(e) = process_missing_product_images(db).await {
        tracing::warn!("Failed to process product images: {}", e);
    }

    Ok(total_stats)
}

enum ProcessResult {
    Processed,
    Skipped(String),
}

/// Fetch all pending raw emails from the database
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
    email: RawEmail,
) -> Result<ParsedEmail, (i64, String, String)> {
    // Decode quoted-printable encoding if present
    let needs_decode = email.raw_body.contains("=20") || email.raw_body.contains("=3D");
    let html = if needs_decode {
        decode_quoted_printable(&email.raw_body)
    } else {
        email.raw_body.clone()
    };

    // Detect email type from the decoded HTML
    let email_type = parser.detect_email_type(&html);

    // Convert gmail_date (millis string) to DateTime for date fallback
    let fallback_date = email.gmail_date.as_ref()
        .and_then(|d| d.parse::<i64>().ok())
        .and_then(chrono::DateTime::from_timestamp_millis);

    // Parse order if it's a known type
    let order = if email_type != EmailType::Unknown {
        match parser.parse_order(&html, fallback_date) {
            Ok(mut order) => {
                order.account_id = email.account_id;
                order.recipient = email.recipient.clone();
                Some(order)
            }
            Err(e) => {
                return Err((email.id, email.gmail_id.clone(), format!("{:#}", e)));
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
            apply_shipping_tx(tx, order, &parsed.raw_email, pending_tracking).await?;
        }
        EmailType::Delivery => {
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
    update_order_from_confirmation_tx(tx, order).await?;
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
    update_order_status_tx(tx, &order.id, OrderStatus::Shipped).await?;

    // Fill in total_cost if this email has one and the order doesn't yet
    if order.total_cost.is_some() {
        sqlx::query("UPDATE orders SET total_cost = ? WHERE id = ? AND (total_cost IS NULL OR total_cost = 0)")
            .bind(order.total_cost)
            .bind(&order.id)
            .execute(&mut **tx)
            .await?;
    }

    // Record the shipped date from the email's gmail_date
    if let Some(ref gmail_date) = email.gmail_date {
        update_shipped_date_tx(tx, &order.id, gmail_date).await?;
    }

    if let (Some(tracking), Some(carrier)) = (&order.tracking_number, &order.carrier) {
        update_order_tracking_tx(tx, &order.id, tracking, carrier).await?;

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
    update_order_status_tx(tx, &order.id, OrderStatus::Delivered).await?;

    // Fill in total_cost if this email has one and the order doesn't yet
    if order.total_cost.is_some() {
        sqlx::query("UPDATE orders SET total_cost = ? WHERE id = ? AND (total_cost IS NULL OR total_cost = 0)")
            .bind(order.total_cost)
            .bind(&order.id)
            .execute(&mut **tx)
            .await?;
    }

    upsert_items_for_event_tx(tx, &order.id, &order.items, ItemStatus::Delivered).await?;

    record_email_event_tx(tx, &order.id, "delivery", email).await?;
    tracing::info!("Order {} marked as delivered", order.id);
    Ok(())
}

// ============================================================================
// Database helper functions
// ============================================================================


/// Batch update item statuses for multiple items in one query
async fn batch_update_item_status(
    db: &Database,
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
    q.execute(db.pool()).await?;
    Ok(())
}

/// Batch cancel items by name in one query
async fn batch_cancel_items_by_name(
    db: &Database,
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
    q.execute(db.pool()).await?;
    Ok(())
}

async fn check_all_items_canceled(db: &Database, order_id: &str) -> Result<bool> {
    let non_canceled: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM line_items WHERE order_id = ? AND status != 'canceled'"
    )
    .bind(order_id)
    .fetch_one(db.pool())
    .await?;

    Ok(non_canceled.0 == 0)
}

async fn record_email_event(
    db: &Database,
    order_id: &str,
    event_type: &str,
    email: &RawEmail,
) -> Result<()> {
    // Note: raw_html is NULL because it's already stored in raw_emails table
    // Storing it again would be redundant and slow (50-150KB per email)
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
    .execute(db.pool())
    .await?;
    Ok(())
}

/// Process missing product images via rembg server and cache the results.
async fn process_missing_product_images(db: &Database) -> Result<()> {
    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT image_url FROM line_items WHERE image_url IS NOT NULL AND image_url != ''"
    )
    .fetch_all(db.pool())
    .await?;

    if rows.is_empty() {
        return Ok(());
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
        let processor = ImageProcessor::new_rembg_http_default(db.pool().clone()).await?;
        let created = processor.process_missing_thumbnails().await?;
        if created > 0 {
            tracing::info!("Backfilled {} image thumbnails", created);
        }
        return Ok(());
    }

    tracing::info!(
        "Processing {} product images via rembg server",
        missing.len()
    );

    let processor = ImageProcessor::new_rembg_http_default(db.pool().clone()).await?;
    let _ = processor.process_batch(missing).await?;
    let created = processor.process_missing_thumbnails().await?;
    if created > 0 {
        tracing::info!("Backfilled {} image thumbnails", created);
    }
    Ok(())
}

// ============================================================================
// Transaction-aware database helper functions
// ============================================================================

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

    for item in &order.items {
        sqlx::query(
            r#"
            INSERT INTO line_items (order_id, name, quantity, price, image_url, status)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_id, name) DO UPDATE SET
                image_url = COALESCE(line_items.image_url, excluded.image_url),
                price = COALESCE(line_items.price, excluded.price)
            "#
        )
        .bind(&order.id)
        .bind(&item.name)
        .bind(item.quantity as i32)
        .bind(item.price)
        .bind(&item.image_url)
        .bind(item.status.as_str())
        .execute(&mut **tx)
        .await
        .context("Failed to insert line item")?;
    }

    Ok(())
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

async fn insert_line_items_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order_id: &str,
    items: &[crate::models::LineItem],
) -> Result<()> {
    for item in items {
        sqlx::query(
            r#"
            INSERT INTO line_items (order_id, name, quantity, price, image_url, status)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_id, name) DO UPDATE SET
                image_url = COALESCE(line_items.image_url, excluded.image_url),
                price = COALESCE(line_items.price, excluded.price)
            "#
        )
        .bind(order_id)
        .bind(&item.name)
        .bind(item.quantity as i32)
        .bind(item.price)
        .bind(&item.image_url)
        .bind(item.status.as_str())
        .execute(&mut **tx)
        .await
        .context("Failed to insert line item")?;
    }

    Ok(())
}

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

async fn update_order_from_confirmation_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order: &WalmartOrder,
) -> Result<()> {
    if order.total_cost.is_some() {
        sqlx::query("UPDATE orders SET total_cost = ? WHERE id = ?")
            .bind(order.total_cost)
            .bind(&order.id)
            .execute(&mut **tx)
            .await?;
    }

    for item in &order.items {
        sqlx::query(
            r#"
            INSERT INTO line_items (order_id, name, quantity, price, image_url, status)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_id, name) DO UPDATE SET
                image_url = COALESCE(line_items.image_url, excluded.image_url),
                price = COALESCE(line_items.price, excluded.price)
            "#
        )
        .bind(&order.id)
        .bind(&item.name)
        .bind(item.quantity as i32)
        .bind(item.price)
        .bind(&item.image_url)
        .bind(item.status.as_str())
        .execute(&mut **tx)
        .await?;
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

async fn update_order_tracking_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order_id: &str,
    tracking_number: &str,
    carrier: &str,
) -> Result<()> {
    sqlx::query("UPDATE orders SET tracking_number = ?, carrier = ? WHERE id = ?")
        .bind(tracking_number)
        .bind(carrier)
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

async fn update_shipped_date_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order_id: &str,
    shipped_date: &str,
) -> Result<()> {
    let formatted = millis_to_rfc3339(shipped_date);
    sqlx::query("UPDATE orders SET shipped_date = ? WHERE id = ? AND shipped_date IS NULL")
        .bind(&formatted)
        .bind(order_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
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

async fn mark_email_processed(db: &Database, email_id: i64, _error: Option<&str>) -> Result<()> {
    sqlx::query(
        "UPDATE raw_emails SET processing_status = 'processed', processed_at = datetime('now') WHERE id = ?"
    )
    .bind(email_id)
    .execute(db.pool())
    .await?;
    Ok(())
}

async fn mark_email_failed(db: &Database, email_id: i64, error: &str) -> Result<()> {
    sqlx::query(
        "UPDATE raw_emails SET processing_status = 'failed', error_message = ?, processed_at = datetime('now') WHERE id = ?"
    )
    .bind(error)
    .bind(email_id)
    .execute(db.pool())
    .await?;
    Ok(())
}

async fn mark_email_skipped(db: &Database, email_id: i64) -> Result<()> {
    sqlx::query(
        "UPDATE raw_emails SET processing_status = 'skipped', processed_at = datetime('now') WHERE id = ?"
    )
    .bind(email_id)
    .execute(db.pool())
    .await?;
    Ok(())
}

// ============================================================================
// Batch status update functions for performance
// ============================================================================

/// Batch mark multiple emails as processed
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

/// Batch mark multiple emails as failed with their error messages
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::OrderStatus;

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
}
