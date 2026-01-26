//! Email ingestion module
//!
//! Handles syncing emails from Gmail to the local database.
//! This is the first step in the pipeline - we fetch and store raw emails
//! before parsing them into orders.

pub mod gmail;

use crate::auth::GmailClient;
use crate::db::{Database, EmailData};
use anyhow::Result;
use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;

pub use gmail::{FetchedEmail, GmailFetcher, MessageRef};

/// Configuration for sync performance
const MAX_CONCURRENT_FETCHES: usize = 10;
const BATCH_INSERT_SIZE: usize = 50;

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
}

impl SyncStats {
    pub fn summary(&self) -> String {
        format!(
            "Sync complete: {} found, {} synced, {} skipped, {} failed",
            self.total_found, self.synced, self.skipped, self.failed
        )
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

/// Sync all Walmart emails (no date filter)
pub async fn sync_emails(db: &Database, gmail_client: GmailClient) -> Result<SyncStats> {
    let fetcher = GmailFetcher::new(gmail_client);
    let mut stats = SyncStats::default();

    tracing::info!("Fetching all Walmart emails from Gmail...");
    let messages = fetcher.list_walmart_emails(None).await?;
    stats.total_found = messages.len();

    if messages.is_empty() {
        tracing::info!("No Walmart emails found");
        return Ok(stats);
    }

    tracing::info!("Found {} Walmart-related emails", messages.len());

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

    // Step 2: Parallel fetch with bounded concurrency
    pb.set_message("Fetching emails...");

    let fetch_results: Vec<Result<FetchedEmail, (String, anyhow::Error)>> = stream::iter(to_fetch)
        .map(|msg_ref| {
            let id = msg_ref.id.clone();
            async move {
                match fetcher.fetch_email_full(&id).await {
                    Ok(email) => Ok(email),
                    Err(e) => Err((id, e)),
                }
            }
        })
        .buffer_unordered(MAX_CONCURRENT_FETCHES)
        .inspect(|_| pb.inc(1))
        .collect()
        .await;

    // Step 3: Process results and batch insert
    pb.set_message("Storing emails...");
    let mut batch: Vec<EmailData> = Vec::with_capacity(BATCH_INSERT_SIZE);

    for result in fetch_results {
        match result {
            Ok(email) => {
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
                    raw_body: email.raw_body,
                    event_type: event_type.to_string(),
                    gmail_date: email.internal_date,
                    account_id: None, // Legacy mode - no account tracking
                });

                // Insert batch when full
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
            Err((id, e)) => {
                stats.failed += 1;
                tracing::warn!("Failed to fetch email {}: {}", id, e);
            }
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

    process_messages_with_account(&fetcher, db, messages, &mut stats, account_id).await?;

    Ok(stats)
}

/// Sync with a custom query and account tracking
pub async fn sync_emails_with_query_and_account(
    db: &Database,
    gmail_client: GmailClient,
    query: &str,
    account_id: i64,
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

    process_messages_with_account(&fetcher, db, messages, &mut stats, account_id).await?;

    Ok(stats)
}

/// Process messages with account_id tracking
async fn process_messages_with_account(
    fetcher: &GmailFetcher,
    db: &Database,
    messages: Vec<MessageRef>,
    stats: &mut SyncStats,
    account_id: i64,
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

    // Step 2: Parallel fetch with bounded concurrency
    pb.set_message("Fetching emails...");

    let fetch_results: Vec<Result<FetchedEmail, (String, anyhow::Error)>> = stream::iter(to_fetch)
        .map(|msg_ref| {
            let id = msg_ref.id.clone();
            async move {
                match fetcher.fetch_email_full(&id).await {
                    Ok(email) => Ok(email),
                    Err(e) => Err((id, e)),
                }
            }
        })
        .buffer_unordered(MAX_CONCURRENT_FETCHES)
        .inspect(|_| pb.inc(1))
        .collect()
        .await;

    // Step 3: Process results and batch insert
    pb.set_message("Storing emails...");
    let mut batch: Vec<EmailData> = Vec::with_capacity(BATCH_INSERT_SIZE);

    for result in fetch_results {
        match result {
            Ok(email) => {
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
                    raw_body: email.raw_body,
                    event_type: event_type.to_string(),
                    gmail_date: email.internal_date,
                    account_id: Some(account_id),
                });

                // Insert batch when full
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
            Err((id, e)) => {
                stats.failed += 1;
                tracing::warn!("Failed to fetch email {}: {}", id, e);
            }
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
