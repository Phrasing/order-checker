use anyhow::Result;
use clap::{Parser, Subcommand};
use sqlx::Row;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};
use tracing_appender::rolling;

use walmart_dashboard::{
    auth::{self, AccountAuth},
    db::Database,
    images, ingestion, process, tracking,
};

/// Walmart Order Dashboard - Track and reconcile orders from email
#[derive(Parser)]
#[command(name = "walmart-dashboard")]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Path to the SQLite database file
    #[arg(short, long, default_value = "orders.db")]
    database: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Sync emails from Gmail to the local database
    Sync {
        /// Path to Google OAuth client_secret.json
        #[arg(long, default_value = "client_secret.json")]
        client_secret: PathBuf,

        /// Path to cache OAuth tokens (legacy, ignored if accounts are configured)
        #[arg(long, default_value = "token_cache.json")]
        token_cache: PathBuf,

        /// Custom search query (optional, defaults to Walmart emails)
        #[arg(long)]
        query: Option<String>,

        /// Only fetch emails from the last N days (default: 5)
        #[arg(long, short = 'n', default_value = "5")]
        days: u32,

        /// Sync only this specific account (email address)
        #[arg(long)]
        account: Option<String>,
    },

    /// Process pending emails and reconcile into orders
    Process {
        /// Reset skipped/failed emails to pending before processing
        #[arg(long)]
        reset: bool,
    },

    /// Sync emails and process them concurrently (pipelined)
    SyncAndProcess {
        /// Path to Google OAuth client_secret.json
        #[arg(long, default_value = "client_secret.json")]
        client_secret: PathBuf,

        /// Path to cache OAuth tokens (legacy, ignored if accounts are configured)
        #[arg(long, default_value = "token_cache.json")]
        token_cache: PathBuf,

        /// Custom search query (optional, defaults to Walmart emails)
        #[arg(long)]
        query: Option<String>,

        /// Only fetch emails from the last N days (default: 5)
        #[arg(long, short = 'n', default_value = "5")]
        days: u32,

        /// Sync only this specific account (email address)
        #[arg(long)]
        account: Option<String>,
    },

    /// Show sync status and database statistics
    Status,

    /// Clear cached OAuth token (for re-authentication)
    ClearAuth,

    /// Initialize the database (run migrations)
    Init,

    /// Debug: dump a sample email from the database
    DebugEmail {
        /// Email ID to dump (default: first pending)
        #[arg(short, long)]
        id: Option<i64>,
        /// Filter by event type (order_confirmation, shipping, delivery, cancellation)
        #[arg(short = 't', long)]
        event_type: Option<String>,
    },

    /// Clear all raw emails from the database (for resync)
    ClearEmails,

    /// Clear all orders and line items from the database (for reprocessing)
    ClearOrders,

    /// Backfill missing image thumbnails from cached images
    BackfillThumbnails {
        /// Rebuild all thumbnails from cached images
        #[arg(long)]
        force: bool,
    },

    /// Backfill missing product images via rembg (and rebuild thumbnails)
    BackfillImages {
        /// Rebuild all cached images and thumbnails
        #[arg(long)]
        force: bool,
    },

    /// Search emails by subject pattern
    SearchEmails {
        /// Pattern to search for in subject (case-insensitive)
        #[arg(short, long)]
        pattern: String,
    },

    /// Debug: show events for a specific order
    DebugOrder {
        /// Order ID to query
        #[arg(short, long)]
        id: String,
    },

    /// Fetch tracking status from 17track.net for shipped orders
    FetchTracking {
        /// Specific order ID to fetch tracking for
        #[arg(long)]
        order_id: Option<String>,

        /// Force refresh even if cache is fresh
        #[arg(long)]
        force: bool,
    },

    /// Refresh stale tracking entries in the cache
    RefreshTracking {
        /// Dry run - don't update database, just show what would be refreshed
        #[arg(long)]
        dry_run: bool,
    },

    /// Show tracking status for a specific order
    TrackingStatus {
        /// Order ID to show tracking for
        order_id: String,
    },

    // ==================== Account Management ====================

    /// Add a new Gmail account (triggers OAuth flow)
    AddAccount {
        /// Path to Google OAuth client_secret.json
        #[arg(long, default_value = "client_secret.json")]
        client_secret: PathBuf,
    },

    /// List all configured Gmail accounts
    ListAccounts,

    /// Remove a Gmail account
    RemoveAccount {
        /// Email address of the account to remove
        email: String,

        /// Also delete all synced emails and orders for this account
        #[arg(long)]
        delete_data: bool,
    },

    /// Clear OAuth token for a specific account (for re-authentication)
    ClearAccountAuth {
        /// Email address of the account
        email: String,
    },

    // ==================== Data Maintenance ====================

    /// Fix order dates by re-extracting from raw emails
    FixDates {
        /// Dry run - don't update database, just show what would be updated
        #[arg(long)]
        dry_run: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file if present
    let _ = dotenvy::dotenv();

    // Initialize logging with file output
    let log_dir = PathBuf::from("logs");
    std::fs::create_dir_all(&log_dir).ok();

    let file_appender = rolling::daily(&log_dir, "walmart-cli.log");
    let (file_writer, _guard) = tracing_appender::non_blocking(file_appender);

    // Console layer: INFO level by default, compact format
    let console_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .compact()
        .with_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        );

    // File layer: DEBUG for our crate, WARN for noisy dependencies
    let file_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .with_writer(file_writer)
        .with_ansi(false)
        .with_filter(tracing_subscriber::EnvFilter::new(
            "info,walmart_dashboard=debug,html5ever=warn,markup5ever=warn,selectors=warn"
        ));

    tracing_subscriber::registry()
        .with(console_layer)
        .with(file_layer)
        .init();

    let cli = Cli::parse();

    // Create database connection
    let db_path = cli.database;
    tracing::info!("Using database: {}", db_path.display());

    match cli.command {
        Commands::Init => {
            tracing::info!("Initializing database...");
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;
            println!("Database initialized successfully at: {}", db_path.display());
        }

        Commands::Status => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;
            let status = ingestion::get_sync_status(&db).await?;
            println!("{}", status);

            // Also show order counts
            let order_status = get_order_status(&db).await?;
            println!("\n{}", order_status);
        }

        Commands::ClearAuth => {
            auth::clear_cached_token()?;
            println!("OAuth token cache cleared. You will need to re-authenticate on next sync.");
        }

        Commands::Sync {
            client_secret,
            token_cache,
            query,
            days,
            account,
        } => {
            // Initialize database
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            // Get accounts to sync
            let accounts_to_sync = if let Some(email) = account {
                // Sync specific account
                match db.get_account_by_email(&email).await? {
                    Some(acc) if acc.is_active => vec![acc],
                    Some(_) => {
                        println!("Account {} is deactivated.", email);
                        return Ok(());
                    }
                    None => {
                        println!("Account {} not found. Run 'add-account' first.", email);
                        return Ok(());
                    }
                }
            } else {
                // Get all active accounts
                db.list_accounts().await?
            };

            // If no accounts configured, use legacy single-account mode
            if accounts_to_sync.is_empty() {
                println!("No accounts configured. Using legacy single-account mode.");
                println!("Authenticating with Gmail...");
                let gmail_client = auth::get_gmail_client(&client_secret, &token_cache).await?;

                println!("Starting email sync (last {} days)...", days);
                let stats = if let Some(custom_query) = query.clone() {
                    ingestion::sync_emails_with_query(&db, gmail_client, &custom_query).await?
                } else {
                    ingestion::sync_emails_with_days(&db, gmail_client, days).await?
                };

                println!("\n{}", stats.summary());
            } else {
                // Sync each configured account
                let mut total_fetched = 0;
                let mut total_inserted = 0;

                for acc in &accounts_to_sync {
                    println!("\n=== Syncing account: {} ===", acc.email);

                    let account_auth = AccountAuth::with_path(
                        &acc.email,
                        PathBuf::from(&acc.token_cache_path),
                    );

                    match auth::get_gmail_client_for_account(&client_secret, &account_auth).await {
                        Ok(gmail_client) => {
                            println!("Starting email sync (last {} days)...", days);
                            let stats = if let Some(ref custom_query) = query {
                                ingestion::sync_emails_with_query_and_account(
                                    &db,
                                    gmail_client,
                                    custom_query,
                                    acc.id,
                                    None,
                                ).await?
                            } else {
                                ingestion::sync_emails_with_days_and_account(
                                    &db,
                                    gmail_client,
                                    days,
                                    acc.id,
                                    None,
                                ).await?
                            };

                            println!("{}", stats.summary());
                            total_fetched += stats.total_fetched;
                            total_inserted += stats.new_emails;

                            // Update last sync time
                            db.update_account_last_sync(acc.id).await?;
                        }
                        Err(e) => {
                            eprintln!("Failed to authenticate {}: {}", acc.email, e);
                            eprintln!("Run 'clear-account-auth {}' and try again.", acc.email);
                        }
                    }
                }

                println!("\n=== Sync Complete ===");
                println!("  Accounts synced: {}", accounts_to_sync.len());
                println!("  Total fetched: {}", total_fetched);
                println!("  Total new: {}", total_inserted);
            }

            // Show updated status
            let status = ingestion::get_sync_status(&db).await?;
            println!("\n{}", status);
        }

        Commands::Process { reset } => {
            // Initialize database
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            // Optionally reset skipped/failed emails
            if reset {
                let count = db.reset_email_status().await?;
                println!("Reset {} emails to pending status", count);
            }

            println!("Processing pending emails...");
            let stats = process::process_pending_events(&db).await?;
            println!("\n{}", stats.summary());

            // Show order status
            let order_status = get_order_status(&db).await?;
            println!("\n{}", order_status);
        }

        Commands::SyncAndProcess {
            client_secret,
            token_cache: _,
            query,
            days,
            account,
        } => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            // Get accounts to sync
            let accounts_to_sync = if let Some(email) = account {
                match db.get_account_by_email(&email).await? {
                    Some(acc) if acc.is_active => vec![acc],
                    Some(_) => {
                        println!("Account {} is deactivated.", email);
                        return Ok(());
                    }
                    None => {
                        println!("Account {} not found. Run 'add-account' first.", email);
                        return Ok(());
                    }
                }
            } else {
                db.list_accounts().await?
            };

            if accounts_to_sync.is_empty() {
                println!("No accounts configured. Use 'add-account' first, or use separate 'sync' + 'process' commands for legacy mode.");
                return Ok(());
            }

            let num_accounts = accounts_to_sync.len();
            let db = Arc::new(db);
            let ingestion_done = Arc::new(AtomicBool::new(false));

            // Spawn processing task — starts immediately, processes emails as they arrive
            let db_process = db.clone();
            let ingestion_done_clone = ingestion_done.clone();
            let process_handle = tokio::spawn(async move {
                process::process_pending_events_concurrent(&db_process, Some(ingestion_done_clone)).await
            });

            // Sync all accounts in parallel
            println!("Syncing {} account(s) in parallel and processing concurrently...", num_accounts);

            let client_secret = Arc::new(client_secret);
            let query = Arc::new(query);
            let mut sync_handles = tokio::task::JoinSet::new();

            for acc in accounts_to_sync {
                let db = db.clone();
                let client_secret = client_secret.clone();
                let query = query.clone();

                sync_handles.spawn(async move {
                    println!("\n=== Syncing account: {} ===", acc.email);

                    let account_auth = AccountAuth::with_path(
                        &acc.email,
                        PathBuf::from(&acc.token_cache_path),
                    );

                    match auth::get_gmail_client_for_account(&client_secret, &account_auth).await {
                        Ok(gmail_client) => {
                            println!("Starting email sync (last {} days) for {}...", days, acc.email);
                            let stats = if let Some(ref custom_query) = *query {
                                ingestion::sync_emails_with_query_and_account(
                                    &db, gmail_client, custom_query, acc.id, None,
                                ).await
                            } else {
                                ingestion::sync_emails_with_days_and_account(
                                    &db, gmail_client, days, acc.id, None,
                                ).await
                            };

                            match stats {
                                Ok(stats) => {
                                    println!("{}", stats.summary());
                                    db.update_account_last_sync(acc.id).await.ok();
                                    Ok((stats.total_fetched, stats.new_emails))
                                }
                                Err(e) => Err(format!("Sync failed for {}: {}", acc.email, e))
                            }
                        }
                        Err(e) => {
                            Err(format!("Failed to authenticate {}: {}. Run 'clear-account-auth {}' and try again.", acc.email, e, acc.email))
                        }
                    }
                });
            }

            // Collect results from all sync tasks
            let mut total_fetched = 0usize;
            let mut total_inserted = 0usize;
            let mut errors: Vec<String> = Vec::new();

            while let Some(result) = sync_handles.join_next().await {
                match result {
                    Ok(Ok((fetched, inserted))) => {
                        total_fetched += fetched;
                        total_inserted += inserted;
                    }
                    Ok(Err(e)) => {
                        eprintln!("{}", e);
                        errors.push(e);
                    }
                    Err(e) => {
                        let msg = format!("Sync task panicked: {}", e);
                        eprintln!("{}", msg);
                        errors.push(msg);
                    }
                }
            }

            // Signal ingestion complete
            ingestion_done.store(true, Ordering::Release);

            println!("\n=== Sync Complete ===");
            println!("  Accounts synced: {}", num_accounts);
            println!("  Total fetched: {}", total_fetched);
            println!("  Total new: {}", total_inserted);
            if !errors.is_empty() {
                println!("  Errors: {}", errors.len());
            }

            // Wait for processing to finish draining
            println!("\nWaiting for processing to finish...");
            let process_stats = process_handle.await??;
            println!("\n{}", process_stats.summary());

            let order_status = get_order_status(&db).await?;
            println!("\n{}", order_status);
        }

        Commands::DebugEmail { id, event_type: filter_type } => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            let query = match (id, filter_type) {
                (Some(id), _) => format!("SELECT id, gmail_id, subject, raw_body, event_type FROM raw_emails WHERE id = {}", id),
                (None, Some(etype)) => format!("SELECT id, gmail_id, subject, raw_body, event_type FROM raw_emails WHERE event_type = '{}' LIMIT 1", etype),
                (None, None) => "SELECT id, gmail_id, subject, raw_body, event_type FROM raw_emails LIMIT 1".to_string(),
            };

            let row: Option<(i64, String, Option<String>, String, String)> =
                sqlx::query_as(&query)
                    .fetch_optional(db.pool())
                    .await?;

            if let Some((id, gmail_id, subject, raw_body, event_type)) = row {
                println!("=== Email ID: {} ===", id);
                println!("Gmail ID: {}", gmail_id);
                println!("Subject: {:?}", subject);
                println!("Event Type: {}", event_type);
                println!("Body length: {} chars", raw_body.len());

                if raw_body.is_empty() {
                    println!("\nRaw body was cleared after processing.");
                    println!("Use Gmail API to re-fetch by gmail_id if needed.");
                } else {
                    // Show if body is still quoted-printable encoded
                    if raw_body.contains("=3D") || raw_body.contains("=20") {
                        println!("Body appears to still be quoted-printable encoded");
                    }

                    // Parse email using actual parser
                    let parser = walmart_dashboard::parsing::WalmartEmailParser::new();
                    let order_id = parser.extract_order_id(&raw_body);
                    let total = parser.extract_total_price(&raw_body);
                    let order_date = parser.extract_order_date(&raw_body);

                    println!("\n--- Parser results ---");
                    println!("  Order ID: {:?}", order_id);
                    println!("  Total: {:?}", total);
                    println!("  Order Date: {:?}", order_date);

                    // If order exists, show current DB value
                    if let Ok(oid) = &order_id {
                        let db_order: Option<(Option<f64>,)> = sqlx::query_as(
                            "SELECT total_cost FROM orders WHERE id = ?"
                        )
                        .bind(oid)
                        .fetch_optional(db.pool())
                        .await?;

                        if let Some((db_total,)) = db_order {
                            println!("  DB Total: {:?}", db_total);
                        } else {
                            println!("  (Order not in DB)");
                        }
                    }

                    println!("\n--- First 500 chars of body ---");
                    println!("{}", &raw_body[..std::cmp::min(500, raw_body.len())]);
                }
            } else {
                println!("No email found");
            }
        }

        Commands::ClearEmails => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            let result = sqlx::query("DELETE FROM raw_emails")
                .execute(db.pool())
                .await?;

            println!("Cleared {} emails from database", result.rows_affected());
            println!("Run 'sync' to re-fetch emails from Gmail");
        }

        Commands::ClearOrders => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            // Clear line_items first (due to foreign key)
            let items_result = sqlx::query("DELETE FROM line_items")
                .execute(db.pool())
                .await?;

            // Clear orders
            let orders_result = sqlx::query("DELETE FROM orders")
                .execute(db.pool())
                .await?;

            // Also clear raw_emails — bodies are wiped after processing so
            // resetting to pending is useless; they must be re-fetched from Gmail.
            let emails_result = sqlx::query("DELETE FROM raw_emails")
                .execute(db.pool())
                .await?;

            println!("Cleared {} orders, {} line items, {} emails", orders_result.rows_affected(), items_result.rows_affected(), emails_result.rows_affected());
            println!("Run 'sync' to re-fetch and reprocess emails from Gmail");
        }

        Commands::BackfillThumbnails { force } => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            if force {
                let result = sqlx::query("DELETE FROM image_thumbnails")
                    .execute(db.pool())
                    .await?;
                println!("Cleared {} thumbnails", result.rows_affected());
            }

            let processor = images::ImageProcessor::new(
                db.pool().clone(),
                Arc::new(images::NoopRemover),
            )
            .await?;

            let created = processor.process_missing_thumbnails().await?;
            println!("Backfilled {} thumbnails", created);
        }

        Commands::BackfillImages { force } => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            if force {
                let thumbs = sqlx::query("DELETE FROM image_thumbnails")
                    .execute(db.pool())
                    .await?;
                let images = sqlx::query("DELETE FROM images")
                    .execute(db.pool())
                    .await?;
                println!(
                    "Cleared {} images and {} thumbnails",
                    images.rows_affected(),
                    thumbs.rows_affected()
                );
            }

            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT DISTINCT image_url FROM line_items WHERE image_url IS NOT NULL AND image_url != ''",
            )
            .fetch_all(db.pool())
            .await?;

            if rows.is_empty() {
                println!("No product image URLs found in line_items");
                return Ok(());
            }

            let mut url_ids: Vec<(String, String)> = Vec::with_capacity(rows.len());
            for (url,) in rows {
                let id = images::image_id_for_url(&url);
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

            let processor = images::ImageProcessor::new_rembg_http_default(db.pool().clone()).await?;

            if missing.is_empty() {
                println!("No missing images. Checking thumbnails...");
            } else {
                println!("Processing {} product images via rembg server", missing.len());
                let _ = processor.process_batch(missing).await?;
            }

            let created = processor.process_missing_thumbnails().await?;
            println!("Backfilled {} thumbnails", created);
        }

        Commands::SearchEmails { pattern } => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            // First show processing status breakdown
            let statuses: Vec<(String, i64)> = sqlx::query_as(
                "SELECT processing_status, COUNT(*) FROM raw_emails GROUP BY processing_status"
            )
            .fetch_all(db.pool())
            .await?;

            println!("Email processing status breakdown:");
            for (status, count) in &statuses {
                println!("  {}: {}", status, count);
            }

            // Search for emails matching the pattern
            let search_pattern = format!("%{}%", pattern);
            let rows: Vec<(i64, Option<String>, String, String)> = sqlx::query_as(
                "SELECT id, subject, event_type, processing_status FROM raw_emails WHERE subject LIKE ? COLLATE NOCASE ORDER BY id DESC LIMIT 20"
            )
            .bind(&search_pattern)
            .fetch_all(db.pool())
            .await?;

            println!("\nEmails matching '{}':", pattern);
            if rows.is_empty() {
                println!("  No emails found matching that pattern");
            } else {
                for (id, subject, event_type, status) in rows {
                    println!("  [{}] {} | type: {} | status: {}",
                        id,
                        subject.unwrap_or_else(|| "(no subject)".to_string()),
                        event_type,
                        status
                    );
                }
            }
        }

        Commands::DebugOrder { id } => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            // Get order info (including tracking)
            let order: Option<(String, String, Option<f64>, String, Option<String>, Option<String>)> = sqlx::query_as(
                "SELECT id, order_date, total_cost, status, tracking_number, carrier FROM orders WHERE id = ?"
            )
            .bind(&id)
            .fetch_optional(db.pool())
            .await?;

            if let Some((order_id, order_date, total, status, tracking, carrier)) = order {
                println!("=== Order {} ===", order_id);
                println!("  Date: {}", order_date);
                println!("  Total: {:?}", total);
                println!("  Status: {}", status);
                if let (Some(t), Some(c)) = (&tracking, &carrier) {
                    println!("  Tracking: {} ({})", t, c);
                }

                // Get line items
                let items: Vec<(String, i32, String)> = sqlx::query_as(
                    "SELECT name, quantity, status FROM line_items WHERE order_id = ?"
                )
                .bind(&id)
                .fetch_all(db.pool())
                .await?;

                println!("\nLine items ({}):", items.len());
                for (name, qty, item_status) in items {
                    println!("  - {} (qty: {}) [{}]", name, qty, item_status);
                }

                // Get email events
                let events: Vec<(i64, String, Option<String>)> = sqlx::query_as(
                    "SELECT id, event_type, email_subject FROM email_events WHERE order_id = ?"
                )
                .bind(&id)
                .fetch_all(db.pool())
                .await?;

                println!("\nEmail events ({}):", events.len());
                for (event_id, event_type, subject) in events {
                    println!("  [{}] {} - {:?}", event_id, event_type, subject);
                }
            } else {
                println!("Order {} not found", id);
            }
        }

        Commands::FetchTracking { order_id, force } => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            let service = tracking::TrackingService::new();

            if let Some(order_id) = order_id {
                // Fetch tracking for a specific order
                let order: Option<(Option<String>, Option<String>)> = sqlx::query_as(
                    "SELECT tracking_number, carrier FROM orders WHERE id = ?"
                )
                .bind(&order_id)
                .fetch_optional(db.pool())
                .await?;

                match order {
                    Some((Some(tracking_number), Some(carrier))) => {
                        println!("Fetching tracking for order {} ({} - {})...", order_id, carrier, tracking_number);
                        match service.get_tracking_status(&db, &tracking_number, &carrier, force).await {
                            Ok(status) => {
                                print_tracking_status(&status);
                            }
                            Err(e) => {
                                eprintln!("Error fetching tracking: {}", e);
                            }
                        }
                    }
                    Some(_) => {
                        println!("Order {} has no tracking information", order_id);
                    }
                    None => {
                        println!("Order {} not found", order_id);
                    }
                }
            } else {
                // Fetch tracking for all shipped orders without cached status
                let orders: Vec<(String, String, String)> = sqlx::query_as(
                    r#"
                    SELECT o.id, o.tracking_number, o.carrier
                    FROM orders o
                    LEFT JOIN tracking_cache tc ON o.tracking_number = tc.tracking_number
                    WHERE o.tracking_number IS NOT NULL
                      AND o.carrier IS NOT NULL
                      AND o.status IN ('shipped', 'delivered')
                      AND (tc.id IS NULL OR ? = 1)
                    ORDER BY o.order_date DESC
                    LIMIT 20
                    "#
                )
                .bind(force as i32)
                .fetch_all(db.pool())
                .await?;

                if orders.is_empty() {
                    println!("No orders with tracking numbers need fetching");
                    return Ok(());
                }

                println!("Fetching tracking for {} orders...\n", orders.len());

                for (order_id, tracking_number, carrier) in orders {
                    print!("Order {} ({} - {})... ", order_id, carrier, tracking_number);
                    match service.get_tracking_status(&db, &tracking_number, &carrier, force).await {
                        Ok(status) => {
                            println!("{}", status.state.display_name());
                        }
                        Err(e) => {
                            println!("Error: {}", e);
                        }
                    }
                }
            }
        }

        Commands::RefreshTracking { dry_run } => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            let service = tracking::TrackingService::new();

            if dry_run {
                println!("Dry run - checking for stale tracking entries...\n");
            } else {
                println!("Refreshing stale tracking entries...\n");
            }

            match service.refresh_stale_tracking(&db, dry_run).await {
                Ok(result) => {
                    println!("\nRefresh complete:");
                    println!("  Updated: {}", result.updated);
                    println!("  Errors:  {}", result.errors);
                    println!("  Skipped: {} (too many consecutive errors)", result.skipped);
                }
                Err(e) => {
                    eprintln!("Error refreshing tracking: {}", e);
                }
            }
        }

        Commands::TrackingStatus { order_id } => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            // Get tracking for the order
            let tracking_list = tracking::get_tracking_for_order(&db, &order_id).await?;

            if tracking_list.is_empty() {
                // Check if order exists
                let order: Option<(Option<String>, Option<String>)> = sqlx::query_as(
                    "SELECT tracking_number, carrier FROM orders WHERE id = ?"
                )
                .bind(&order_id)
                .fetch_optional(db.pool())
                .await?;

                match order {
                    Some((Some(tracking), Some(carrier))) => {
                        println!("Order {} has tracking {} ({}) but no cached status yet.", order_id, tracking, carrier);
                        println!("Run 'fetch-tracking --order-id {}' to fetch status.", order_id);
                    }
                    Some(_) => {
                        println!("Order {} has no tracking information.", order_id);
                    }
                    None => {
                        println!("Order {} not found.", order_id);
                    }
                }
            } else {
                println!("=== Tracking for Order {} ===\n", order_id);
                for status in tracking_list {
                    print_tracking_status(&status);
                    println!();
                }
            }
        }

        // ==================== Account Management ====================

        Commands::AddAccount { client_secret } => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            println!("Adding a new Gmail account...");
            println!("A browser window will open for authentication.\n");

            // Store token files alongside the database
            let token_dir = db_path.parent().unwrap_or_else(|| std::path::Path::new("."));

            // Trigger OAuth flow and get email
            let (email, token_path) = auth::authenticate_new_account(&client_secret, None, token_dir).await?;

            // Check if account already exists
            if let Some(existing) = db.get_account_by_email(&email).await? {
                if existing.is_active {
                    println!("Account {} is already configured.", email);
                    return Ok(());
                } else {
                    // Reactivate the account
                    sqlx::query("UPDATE accounts SET is_active = 1, token_cache_path = ? WHERE email = ?")
                        .bind(token_path.to_string_lossy().to_string())
                        .bind(&email)
                        .execute(db.pool())
                        .await?;
                    println!("Reactivated account: {}", email);
                    return Ok(());
                }
            }

            // Add new account to database
            let token_path_str = token_path.to_string_lossy().to_string();
            db.add_account(&email, &token_path_str).await?;

            println!("\nSuccessfully added account: {}", email);
            println!("Token cached at: {}", token_path_str);
            println!("\nRun 'sync' to fetch emails from this account.");
        }

        Commands::ListAccounts => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            let accounts = db.list_accounts().await?;

            if accounts.is_empty() {
                println!("No Gmail accounts configured.");
                println!("\nRun 'add-account' to add a Gmail account.");
            } else {
                println!("Configured Gmail accounts:\n");
                for account in accounts {
                    let sync_info = account.last_sync_at
                        .map(|t| format!("Last sync: {}", t))
                        .unwrap_or_else(|| "Never synced".to_string());

                    // Get email/order counts for this account
                    let email_count: (i64,) = sqlx::query_as(
                        "SELECT COUNT(*) FROM raw_emails WHERE account_id = ?"
                    )
                    .bind(account.id)
                    .fetch_one(db.pool())
                    .await?;

                    let order_count: (i64,) = sqlx::query_as(
                        "SELECT COUNT(*) FROM orders WHERE account_id = ?"
                    )
                    .bind(account.id)
                    .fetch_one(db.pool())
                    .await?;

                    println!("  {} (ID: {})", account.email, account.id);
                    println!("    {} | {} emails | {} orders",
                        sync_info, email_count.0, order_count.0);
                    println!("    Token: {}", account.token_cache_path);
                    println!();
                }
            }
        }

        Commands::RemoveAccount { email, delete_data } => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            let account = db.get_account_by_email(&email).await?;

            match account {
                Some(account) => {
                    if delete_data {
                        // Delete all data and the account
                        let (orders, emails) = db.delete_account_data(account.id).await?;

                        // Also delete the token cache file
                        let token_path = std::path::Path::new(&account.token_cache_path);
                        if token_path.exists() {
                            std::fs::remove_file(token_path)?;
                        }

                        println!("Removed account: {}", email);
                        println!("  Deleted {} orders and {} emails", orders, emails);
                        println!("  Removed token cache");
                    } else {
                        // Just deactivate
                        db.deactivate_account(&email).await?;
                        println!("Deactivated account: {}", email);
                        println!("  Data preserved. Use --delete-data to remove all data.");
                    }
                }
                None => {
                    println!("Account {} not found.", email);
                }
            }
        }

        Commands::ClearAccountAuth { email } => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            let account = db.get_account_by_email(&email).await?;

            match account {
                Some(account) => {
                    let account_auth = AccountAuth::with_path(
                        &account.email,
                        PathBuf::from(&account.token_cache_path),
                    );
                    account_auth.clear_token()?;
                    println!("Cleared OAuth token for: {}", email);
                    println!("You will need to re-authenticate on next sync.");
                }
                None => {
                    println!("Account {} not found.", email);
                }
            }
        }

        Commands::FixDates { dry_run } => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            let parser = walmart_dashboard::parsing::WalmartEmailParser::new();

            // Get all raw emails and extract order_id and date from each
            // Prefer confirmation emails as they have the most reliable date
            let emails: Vec<(i64, String, String)> = sqlx::query_as(
                r#"
                SELECT id, raw_body, event_type
                FROM raw_emails
                WHERE processing_status = 'processed'
                ORDER BY
                    CASE event_type
                        WHEN 'confirmation' THEN 1
                        WHEN 'shipping' THEN 2
                        WHEN 'delivery' THEN 3
                        ELSE 4
                    END,
                    id
                "#
            )
            .fetch_all(db.pool())
            .await?;

            let mut updated = 0;
            let mut failed = 0;
            let mut cleared_bodies = 0;
            let mut order_dates: std::collections::HashMap<String, String> = std::collections::HashMap::new();

            // First pass: collect dates from all emails
            for (_id, raw_body, _event_type) in &emails {
                if raw_body.is_empty() {
                    cleared_bodies += 1;
                    continue;
                }
                if let Ok(order_id) = parser.extract_order_id(raw_body) {
                    // Only use first date found (confirmation emails are first due to ORDER BY)
                    if !order_dates.contains_key(&order_id) {
                        if let Ok(date) = parser.extract_order_date(raw_body) {
                            let date_str = date.format("%Y-%m-%dT%H:%M:%SZ").to_string();
                            order_dates.insert(order_id, date_str);
                        }
                    }
                }
            }

            if cleared_bodies > 0 {
                println!("Skipped {} emails with cleared raw bodies", cleared_bodies);
            }
            println!("Found dates for {} orders", order_dates.len());

            // Second pass: update orders
            for (order_id, date_str) in &order_dates {
                // Check current date
                let current: Option<(Option<String>,)> = sqlx::query_as(
                    "SELECT order_date FROM orders WHERE id = ?"
                )
                .bind(order_id)
                .fetch_optional(db.pool())
                .await?;

                if let Some((current_date,)) = current {
                    if dry_run {
                        println!("[DRY RUN] {} : {:?} -> {}", order_id, current_date, date_str);
                    } else {
                        sqlx::query("UPDATE orders SET order_date = ? WHERE id = ?")
                            .bind(date_str)
                            .bind(order_id)
                            .execute(db.pool())
                            .await?;
                    }
                    updated += 1;
                } else {
                    failed += 1; // Order not found in DB
                }
            }

            if dry_run {
                println!("\n[DRY RUN] Would update {} orders", updated);
            } else {
                println!("Updated {} order dates", updated);
            }
            if failed > 0 {
                println!("Could not find {} orders in database", failed);
            }
        }
    }

    Ok(())
}

/// Get order statistics for status display
async fn get_order_status(db: &Database) -> Result<String> {
    let total: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM orders")
        .fetch_one(db.pool())
        .await?;

    let by_status: Vec<(String, i64)> = sqlx::query_as(
        "SELECT status, COUNT(*) FROM orders GROUP BY status ORDER BY status"
    )
    .fetch_all(db.pool())
    .await?;

    let with_total: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM orders WHERE total_cost IS NOT NULL"
    )
    .fetch_one(db.pool())
    .await?;

    let mut result = format!("Order status:\n  Total orders: {}", total.0);
    for (status, count) in by_status {
        result.push_str(&format!("\n  {}: {}", status, count));
    }

    // Show how many orders have totals
    let without_total = total.0 - with_total.0;
    result.push_str(&format!("\n\n  With total: {}", with_total.0));
    if without_total > 0 {
        result.push_str(&format!("\n  Missing total: {} (no confirmation email)", without_total));
    }

    Ok(result)
}

/// Print tracking status in a formatted way
fn print_tracking_status(status: &tracking::CachedTracking) {
    println!("Tracking: {} ({})", status.tracking_number, status.carrier);
    println!("  Status: {}", status.state.display_name());
    if let Some(desc) = &status.state_description {
        println!("  Latest: {}", desc);
    }
    if status.is_delivered {
        if let Some(date) = &status.delivery_date {
            println!("  Delivered: {}", date);
        } else {
            println!("  Delivered: Yes");
        }
    }
    println!("  Last fetched: {}", status.last_fetched_at);
    println!("  Fetch count: {}", status.fetch_count);

    if let Some(error) = &status.last_error {
        println!("  Last error: {}", error);
        println!("  Consecutive errors: {}", status.consecutive_errors);
    }

    if !status.events.is_empty() {
        println!("\n  Recent events:");
        for (i, event) in status.events.iter().take(5).enumerate() {
            let time = event.event_time_iso.as_ref()
                .or(event.event_time.as_ref())
                .map(|s| s.as_str())
                .unwrap_or("Unknown time");
            let location = event.location.as_ref()
                .map(|s| format!(" @ {}", s))
                .unwrap_or_default();
            println!("    {}. [{}] {}{}", i + 1, time, event.description, location);
        }
        if status.events.len() > 5 {
            println!("    ... and {} more events", status.events.len() - 5);
        }
    }
}