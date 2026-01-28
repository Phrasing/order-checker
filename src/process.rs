//! Event processing and reconciliation module
//!
//! This module handles processing pending raw emails and reconciling them
//! into the orders database using event-sourcing principles.

use crate::db::Database;
use crate::models::{ItemStatus, OrderStatus, WalmartOrder};
use crate::parsing::{EmailType, WalmartEmailParser};
use crate::tracking::create_tracking_cache_entry;
use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use sqlx::Sqlite;
use std::sync::Arc;

/// Tracking info to create after transaction commits
struct PendingTrackingEntry {
    order_id: String,
    tracking_number: String,
    carrier: String,
}

/// Maximum concurrent email parsing tasks (CPU-bound, can be parallel)
const MAX_CONCURRENT_PARSING: usize = 50;

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
}

/// Raw email row from database
#[derive(Debug)]
struct RawEmail {
    id: i64,
    gmail_id: String,
    subject: Option<String>,
    raw_body: String,
    event_type: String,
    gmail_date: Option<String>,
    account_id: Option<i64>,
}

/// Parsed email result ready for database writes
struct ParsedEmail {
    id: i64,
    gmail_id: String,
    email_type: EmailType,
    order: Option<WalmartOrder>,
    html: String,
    raw_email: RawEmail,
}

/// Process all pending emails and reconcile into orders
pub async fn process_pending_events(db: &Database) -> Result<ProcessStats> {
    let mut stats = ProcessStats::default();
    let parser = Arc::new(WalmartEmailParser::new());

    // Fetch all pending raw emails
    let pending_emails = fetch_pending_emails(db).await?;
    stats.total_pending = pending_emails.len();

    if pending_emails.is_empty() {
        tracing::info!("No pending emails to process");
        return Ok(stats);
    }

    tracing::info!("Processing {} pending emails (parsing: {}, db writes: {})",
                   pending_emails.len(), MAX_CONCURRENT_PARSING, MAX_CONCURRENT_DB_WRITES);

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

    for result in parsed_emails {
        match result {
            Ok(parsed) => {
                if parsed.email_type == EmailType::Unknown {
                    skipped_ids.push(parsed.id);
                    stats.skipped += 1;
                } else {
                    to_process.push(parsed);
                }
            }
            Err((id, _gmail_id, error)) => {
                parse_failures.push((id, error));
                stats.failed += 1;
            }
        }
    }

    tracing::info!("Parsed {} emails: {} to process, {} skipped, {} failed",
                   stats.total_pending, to_process.len(), stats.skipped, stats.failed);

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
                stats.processed += 1;
                processed_ids.push(parsed.id);
                tracing::debug!("Processed email {}", parsed.gmail_id);
            }
            Ok(ProcessResult::Skipped(reason)) => {
                skipped_ids.push(parsed.id);
                tracing::debug!("Skipped email {}: {}", parsed.gmail_id, reason);
            }
            Err(e) => {
                let error_msg = format!("{:#}", e);
                stats.failed += 1;
                db_failed_entries.push((parsed.id, error_msg.clone()));
                tracing::warn!("Failed to process email {}: {}", parsed.gmail_id, error_msg);
            }
        }
    }

    // Commit the transaction
    tx.commit().await.context("Failed to commit transaction")?;
    tracing::info!("Committed {} order updates in single transaction", stats.processed);

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

    tracing::info!("{}", stats.summary());
    Ok(stats)
}

enum ProcessResult {
    Processed,
    Skipped(String),
}

/// Fetch all pending raw emails from the database
async fn fetch_pending_emails(db: &Database) -> Result<Vec<RawEmail>> {
    // Order by date, but prioritize confirmations first within the same date
    // This ensures orders are created from confirmations before other events update them
    let rows: Vec<(i64, String, Option<String>, String, String, Option<String>, Option<i64>)> = sqlx::query_as(
        r#"
        SELECT id, gmail_id, subject, raw_body, event_type, gmail_date, account_id
        FROM raw_emails
        WHERE processing_status = 'pending'
        ORDER BY gmail_date ASC,
                 CASE event_type
                     WHEN 'confirmation' THEN 0
                     ELSE 1
                 END ASC
        "#
    )
    .fetch_all(db.pool())
    .await
    .context("Failed to fetch pending emails")?;

    Ok(rows.into_iter().map(|(id, gmail_id, subject, raw_body, event_type, gmail_date, account_id)| {
        RawEmail {
            id,
            gmail_id,
            subject,
            raw_body,
            event_type,
            gmail_date,
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

    // Parse order if it's a known type
    let order = if email_type != EmailType::Unknown {
        match parser.parse_order(&html) {
            Ok(mut order) => {
                order.account_id = email.account_id;
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
        html,
        raw_email: email,
    })
}

/// Apply a parsed email to the database (IO-bound, should be serialized)
async fn apply_parsed_email_to_db(
    db: &Database,
    parsed: &ParsedEmail,
) -> Result<ProcessResult> {
    let order = parsed.order.as_ref()
        .ok_or_else(|| anyhow::anyhow!("No order data for non-unknown email type"))?;

    match parsed.email_type {
        EmailType::Unknown => {
            return Ok(ProcessResult::Skipped("Unknown email type".to_string()));
        }
        EmailType::Confirmation => {
            apply_confirmation(db, order, &parsed.raw_email).await?;
        }
        EmailType::Cancellation => {
            apply_cancellation(db, order, &parsed.raw_email).await?;
        }
        EmailType::Shipping => {
            apply_shipping(db, order, &parsed.raw_email).await?;
        }
        EmailType::Delivery => {
            apply_delivery(db, order, &parsed.raw_email).await?;
        }
    }

    Ok(ProcessResult::Processed)
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

/// Process a single email and apply reconciliation logic (legacy, for compatibility)
#[allow(dead_code)]
async fn process_single_email(
    db: &Database,
    parser: &WalmartEmailParser,
    email: &RawEmail,
) -> Result<ProcessResult> {
    // Decode quoted-printable encoding if present
    let needs_decode = email.raw_body.contains("=20") || email.raw_body.contains("=3D");
    let html = if needs_decode {
        decode_quoted_printable(&email.raw_body)
    } else {
        email.raw_body.clone()
    };

    tracing::debug!(
        gmail_id = %email.gmail_id,
        qp_decode = needs_decode,
        body_len = html.len(),
        "Processing email"
    );

    // Detect email type from the decoded HTML
    let email_type = parser.detect_email_type(&html);
    tracing::debug!("Detected email type: {:?}", email_type);

    match email_type {
        EmailType::Unknown => {
            return Ok(ProcessResult::Skipped("Unknown email type".to_string()));
        }
        EmailType::Confirmation => {
            process_confirmation(db, parser, &html, email).await?;
        }
        EmailType::Cancellation => {
            process_cancellation(db, parser, &html, email).await?;
        }
        EmailType::Shipping => {
            process_shipping(db, parser, &html, email).await?;
        }
        EmailType::Delivery => {
            process_delivery(db, parser, &html, email).await?;
        }
    }

    Ok(ProcessResult::Processed)
}

/// Apply a confirmation to the database (pre-parsed order)
async fn apply_confirmation(
    db: &Database,
    order: &WalmartOrder,
    email: &RawEmail,
) -> Result<()> {
    // INSERT OR IGNORE handles duplicates - no need to check order_exists first
    insert_order(db, order).await?;

    // Update with any new information (items, total) - harmless if order was just created
    update_order_from_confirmation(db, order).await?;

    // Record the email event
    record_email_event(db, &order.id, "confirmation", email).await?;

    Ok(())
}

/// Process a confirmation email - insert new order (legacy)
#[allow(dead_code)]
async fn process_confirmation(
    db: &Database,
    parser: &WalmartEmailParser,
    html: &str,
    email: &RawEmail,
) -> Result<()> {
    let mut order = parser.parse_order(html)
        .context("Failed to parse confirmation email")?;

    // Set account_id from the email source
    order.account_id = email.account_id;

    apply_confirmation(db, &order, email).await
}

/// Apply a cancellation to the database (pre-parsed order)
async fn apply_cancellation(
    db: &Database,
    order: &WalmartOrder,
    email: &RawEmail,
) -> Result<()> {
    // Ensure order exists (INSERT OR IGNORE handles duplicates)
    insert_order(db, order).await?;

    // Mark items as canceled if they match
    if order.items.is_empty() {
        // Full order cancellation
        update_order_status(db, &order.id, OrderStatus::Canceled).await?;
        cancel_all_items(db, &order.id).await?;
        tracing::info!("Order {} fully canceled", order.id);
    } else {
        // Partial cancellation - batch cancel specific items
        let item_names: Vec<&str> = order.items.iter().map(|i| i.name.as_str()).collect();
        batch_cancel_items_by_name(db, &order.id, &item_names).await?;

        // Check if all items are now canceled
        let all_canceled = check_all_items_canceled(db, &order.id).await?;
        if all_canceled {
            update_order_status(db, &order.id, OrderStatus::Canceled).await?;
        } else {
            update_order_status(db, &order.id, OrderStatus::PartiallyCanceled).await?;
        }
        tracing::info!("Order {} partially canceled ({} items)", order.id, item_names.len());
    }

    // Record the email event
    record_email_event(db, &order.id, "cancellation", email).await?;

    Ok(())
}

/// Process a cancellation email - update order/items status (legacy)
#[allow(dead_code)]
async fn process_cancellation(
    db: &Database,
    parser: &WalmartEmailParser,
    html: &str,
    email: &RawEmail,
) -> Result<()> {
    let mut order = parser.parse_order(html)
        .context("Failed to parse cancellation email")?;

    // Set account_id from the email source
    order.account_id = email.account_id;

    apply_cancellation(db, &order, email).await
}

/// Apply a shipping notification to the database (pre-parsed order)
async fn apply_shipping(
    db: &Database,
    order: &WalmartOrder,
    email: &RawEmail,
) -> Result<()> {
    // Ensure order exists (INSERT OR IGNORE handles duplicates)
    insert_order(db, order).await?;

    // Update order status to shipped
    update_order_status(db, &order.id, OrderStatus::Shipped).await?;

    // Record the shipped date from the email's gmail_date
    if let Some(ref gmail_date) = email.gmail_date {
        update_shipped_date(db, &order.id, gmail_date).await?;
    }

    // Update tracking info if available
    if let (Some(tracking), Some(carrier)) = (&order.tracking_number, &order.carrier) {
        update_order_tracking(db, &order.id, tracking, carrier).await?;

        // Create tracking cache entry for 17track lookup
        if let Err(e) = create_tracking_cache_entry(db, &order.id, tracking, carrier).await {
            tracing::warn!("Failed to create tracking cache entry for {}: {}", tracking, e);
        }

        tracing::info!("Order {} shipped via {} - tracking: {}", order.id, carrier, tracking);
    } else {
        tracing::info!("Order {} marked as shipped (no tracking number)", order.id);
    }

    // Batch mark items as shipped
    if !order.items.is_empty() {
        let item_names: Vec<&str> = order.items.iter().map(|i| i.name.as_str()).collect();
        batch_update_item_status(db, &order.id, &item_names, ItemStatus::Shipped).await?;
    }

    // Record the email event
    record_email_event(db, &order.id, "shipping", email).await?;

    Ok(())
}

/// Process a shipping email - update order/items status (legacy)
#[allow(dead_code)]
async fn process_shipping(
    db: &Database,
    parser: &WalmartEmailParser,
    html: &str,
    email: &RawEmail,
) -> Result<()> {
    let mut order = parser.parse_order(html)
        .context("Failed to parse shipping email")?;

    // Set account_id from the email source
    order.account_id = email.account_id;

    apply_shipping(db, &order, email).await
}

/// Apply a delivery notification to the database (pre-parsed order)
async fn apply_delivery(
    db: &Database,
    order: &WalmartOrder,
    email: &RawEmail,
) -> Result<()> {
    // Ensure order exists (INSERT OR IGNORE handles duplicates)
    insert_order(db, order).await?;

    // Update order status to delivered
    update_order_status(db, &order.id, OrderStatus::Delivered).await?;

    // Batch mark items as delivered
    if !order.items.is_empty() {
        let item_names: Vec<&str> = order.items.iter().map(|i| i.name.as_str()).collect();
        batch_update_item_status(db, &order.id, &item_names, ItemStatus::Delivered).await?;
    }

    // Record the email event
    record_email_event(db, &order.id, "delivery", email).await?;

    tracing::info!("Order {} marked as delivered", order.id);
    Ok(())
}

/// Process a delivery email - update order status to delivered (legacy)
#[allow(dead_code)]
async fn process_delivery(
    db: &Database,
    parser: &WalmartEmailParser,
    html: &str,
    email: &RawEmail,
) -> Result<()> {
    let mut order = parser.parse_order(html)
        .context("Failed to parse delivery email")?;

    // Set account_id from the email source
    order.account_id = email.account_id;

    apply_delivery(db, &order, email).await
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
    insert_order_tx(tx, order).await?;
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

    if !order.items.is_empty() {
        let item_names: Vec<&str> = order.items.iter().map(|i| i.name.as_str()).collect();
        batch_update_item_status_tx(tx, &order.id, &item_names, ItemStatus::Shipped).await?;
    }

    record_email_event_tx(tx, &order.id, "shipping", email).await?;
    Ok(())
}

/// Apply a delivery notification to the database within a transaction
async fn apply_delivery_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order: &WalmartOrder,
    email: &RawEmail,
) -> Result<()> {
    insert_order_tx(tx, order).await?;
    update_order_status_tx(tx, &order.id, OrderStatus::Delivered).await?;

    // Fill in total_cost if this email has one and the order doesn't yet
    if order.total_cost.is_some() {
        sqlx::query("UPDATE orders SET total_cost = ? WHERE id = ? AND (total_cost IS NULL OR total_cost = 0)")
            .bind(order.total_cost)
            .bind(&order.id)
            .execute(&mut **tx)
            .await?;
    }

    if !order.items.is_empty() {
        let item_names: Vec<&str> = order.items.iter().map(|i| i.name.as_str()).collect();
        batch_update_item_status_tx(tx, &order.id, &item_names, ItemStatus::Delivered).await?;
    }

    record_email_event_tx(tx, &order.id, "delivery", email).await?;
    tracing::info!("Order {} marked as delivered", order.id);
    Ok(())
}

// ============================================================================
// Database helper functions
// ============================================================================

async fn order_exists(db: &Database, order_id: &str) -> Result<bool> {
    let result: Option<(i32,)> = sqlx::query_as(
        "SELECT 1 FROM orders WHERE id = ? LIMIT 1"
    )
    .bind(order_id)
    .fetch_optional(db.pool())
    .await?;

    Ok(result.is_some())
}

async fn insert_order(db: &Database, order: &WalmartOrder) -> Result<()> {
    // Insert the order (including tracking info and account_id if available)
    // Use INSERT OR IGNORE to handle concurrent inserts for the same order
    sqlx::query(
        r#"
        INSERT OR IGNORE INTO orders (id, order_date, total_cost, status, tracking_number, carrier, account_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        "#
    )
    .bind(&order.id)
    .bind(order.order_date.to_rfc3339())
    .bind(order.total_cost)
    .bind(order.status.as_str())
    .bind(&order.tracking_number)
    .bind(&order.carrier)
    .bind(order.account_id)
    .execute(db.pool())
    .await
    .context("Failed to insert order")?;

    // Insert line items (use INSERT OR IGNORE for concurrent processing)
    for item in &order.items {
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO line_items (order_id, name, quantity, price, image_url, status)
            VALUES (?, ?, ?, ?, ?, ?)
            "#
        )
        .bind(&order.id)
        .bind(&item.name)
        .bind(item.quantity as i32)
        .bind(item.price)
        .bind(&item.image_url)
        .bind(item.status.as_str())
        .execute(db.pool())
        .await
        .context("Failed to insert line item")?;
    }

    Ok(())
}

async fn update_order_from_confirmation(db: &Database, order: &WalmartOrder) -> Result<()> {
    // Update total if we have it
    if order.total_cost.is_some() {
        sqlx::query("UPDATE orders SET total_cost = ? WHERE id = ?")
            .bind(order.total_cost)
            .bind(&order.id)
            .execute(db.pool())
            .await?;
    }

    // Add any new items using INSERT OR IGNORE (eliminates N+1 SELECT queries)
    for item in &order.items {
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO line_items (order_id, name, quantity, price, image_url, status)
            VALUES (?, ?, ?, ?, ?, ?)
            "#
        )
        .bind(&order.id)
        .bind(&item.name)
        .bind(item.quantity as i32)
        .bind(item.price)
        .bind(&item.image_url)
        .bind(item.status.as_str())
        .execute(db.pool())
        .await?;
    }

    Ok(())
}

async fn update_order_status(db: &Database, order_id: &str, status: OrderStatus) -> Result<()> {
    sqlx::query("UPDATE orders SET status = ? WHERE id = ?")
        .bind(status.as_str())
        .bind(order_id)
        .execute(db.pool())
        .await?;
    Ok(())
}

async fn update_order_tracking(
    db: &Database,
    order_id: &str,
    tracking_number: &str,
    carrier: &str,
) -> Result<()> {
    sqlx::query("UPDATE orders SET tracking_number = ?, carrier = ? WHERE id = ?")
        .bind(tracking_number)
        .bind(carrier)
        .bind(order_id)
        .execute(db.pool())
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

async fn update_shipped_date(db: &Database, order_id: &str, shipped_date: &str) -> Result<()> {
    let formatted = millis_to_rfc3339(shipped_date);
    sqlx::query("UPDATE orders SET shipped_date = ? WHERE id = ? AND shipped_date IS NULL")
        .bind(&formatted)
        .bind(order_id)
        .execute(db.pool())
        .await?;
    Ok(())
}

async fn cancel_all_items(db: &Database, order_id: &str) -> Result<()> {
    sqlx::query("UPDATE line_items SET status = 'canceled' WHERE order_id = ?")
        .bind(order_id)
        .execute(db.pool())
        .await?;
    Ok(())
}

async fn cancel_item_by_name(db: &Database, order_id: &str, item_name: &str) -> Result<()> {
    sqlx::query(
        "UPDATE line_items SET status = 'canceled' WHERE order_id = ? AND name = ?"
    )
    .bind(order_id)
    .bind(item_name)
    .execute(db.pool())
    .await?;
    Ok(())
}

async fn update_item_status_by_name(
    db: &Database,
    order_id: &str,
    item_name: &str,
    status: ItemStatus,
) -> Result<()> {
    sqlx::query(
        "UPDATE line_items SET status = ? WHERE order_id = ? AND name = ?"
    )
    .bind(status.as_str())
    .bind(order_id)
    .bind(item_name)
    .execute(db.pool())
    .await?;
    Ok(())
}

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

// ============================================================================
// Transaction-aware database helper functions
// ============================================================================

async fn insert_order_tx<'a>(
    tx: &mut sqlx::Transaction<'a, Sqlite>,
    order: &WalmartOrder,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT OR IGNORE INTO orders (id, order_date, total_cost, status, tracking_number, carrier, account_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        "#
    )
    .bind(&order.id)
    .bind(order.order_date.to_rfc3339())
    .bind(order.total_cost)
    .bind(order.status.as_str())
    .bind(&order.tracking_number)
    .bind(&order.carrier)
    .bind(order.account_id)
    .execute(&mut **tx)
    .await
    .context("Failed to insert order")?;

    for item in &order.items {
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO line_items (order_id, name, quantity, price, image_url, status)
            VALUES (?, ?, ?, ?, ?, ?)
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
            INSERT OR IGNORE INTO line_items (order_id, name, quantity, price, image_url, status)
            VALUES (?, ?, ?, ?, ?, ?)
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

    #[tokio::test]
    async fn test_process_empty_queue() {
        let db = Database::in_memory().await.expect("Should create DB");
        db.run_migrations().await.expect("Should run migrations");

        let stats = process_pending_events(&db).await.expect("Should process");
        assert_eq!(stats.total_pending, 0);
        assert_eq!(stats.processed, 0);
    }
}
