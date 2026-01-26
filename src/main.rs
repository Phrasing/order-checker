use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod auth;
mod db;
mod ingestion;
mod models;
mod parsing;
mod process;
mod tracking;
mod web;

use db::Database;

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

        /// Path to cache OAuth tokens (ignored if --account is specified)
        #[arg(long, default_value = "token_cache.json")]
        token_cache: PathBuf,

        /// Email address of account to sync (from add-account)
        #[arg(long, short = 'a')]
        account: Option<String>,

        /// Custom search query (optional, defaults to Walmart emails)
        #[arg(long)]
        query: Option<String>,

        /// Only fetch emails from the last N days (default: 5)
        #[arg(long, short = 'n', default_value = "5")]
        days: u32,
    },

    /// Process pending emails and reconcile into orders
    Process {
        /// Reset skipped/failed emails to pending before processing
        #[arg(long)]
        reset: bool,
    },

    /// Start the web dashboard server
    Serve {
        /// Port to run the web server on
        #[arg(short, long, default_value = "3000")]
        port: u16,
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
    },

    /// Clear all raw emails from the database (for resync)
    ClearEmails,

    /// Clear all orders and line items from the database (for reprocessing)
    ClearOrders,

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

    /// Check delivery status for shipped FedEx orders via carrier API
    CheckDelivery {
        /// Dry run - don't update database, just show what would be updated
        #[arg(long)]
        dry_run: bool,
    },

    /// Add a new Gmail account for syncing
    AddAccount {
        /// Path to Google OAuth client_secret.json
        #[arg(long, default_value = "client_secret.json")]
        client_secret: PathBuf,
    },

    /// List all configured Gmail accounts
    ListAccounts,

    /// Debug: show account_id distribution in orders and emails
    DebugAccounts,

    /// Associate orphan emails (NULL account_id) with an account
    AssociateEmails {
        /// Account ID to associate orphan emails with
        #[arg(long)]
        account_id: i64,
    },

    /// Register an account from an existing token cache file
    RegisterToken {
        /// Path to the existing token cache file
        #[arg(long, default_value = "token_cache.json")]
        token_cache: PathBuf,

        /// Path to Google OAuth client_secret.json
        #[arg(long, default_value = "client_secret.json")]
        client_secret: PathBuf,
    },

    /// Reassign emails and orders from one account to another
    ReassignAccount {
        /// Source account ID to move data from
        #[arg(long)]
        from_id: i64,

        /// Target account ID to move data to
        #[arg(long)]
        to_id: i64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file if present
    let _ = dotenvy::dotenv();

    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .compact(),
        )
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
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
            account,
            query,
            days,
        } => {
            // Initialize database
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            // Determine which account to sync
            let (gmail_client, account_id) = if let Some(email) = account {
                // Look up account in database
                let acc = db.get_account_by_email(&email).await?
                    .ok_or_else(|| anyhow::anyhow!(
                        "Account '{}' not found. Run 'list-accounts' to see configured accounts.",
                        email
                    ))?;

                println!("Syncing account: {}", acc.email);
                let token_path = std::path::Path::new(&acc.token_cache_path);
                let client = auth::get_gmail_client(&client_secret, token_path).await?;
                (client, Some(acc.id))
            } else {
                // Legacy mode - use token_cache directly
                println!("Authenticating with Gmail...");
                let client = auth::get_gmail_client(&client_secret, &token_cache).await?;
                (client, None)
            };

            // Run sync
            println!("Starting email sync (last {} days)...", days);
            let stats = if let Some(account_id) = account_id {
                // Account-aware sync
                if let Some(custom_query) = query {
                    ingestion::sync_emails_with_query_and_account(&db, gmail_client, &custom_query, account_id).await?
                } else {
                    ingestion::sync_emails_with_days_and_account(&db, gmail_client, days, account_id).await?
                }
            } else {
                // Legacy sync (no account tracking)
                if let Some(custom_query) = query {
                    ingestion::sync_emails_with_query(&db, gmail_client, &custom_query).await?
                } else {
                    ingestion::sync_emails_with_days(&db, gmail_client, days).await?
                }
            };

            // Update last_sync_at if using account
            if let Some(account_id) = account_id {
                db.update_account_last_sync(account_id).await?;
            }

            println!("\n{}", stats.summary());

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

        Commands::Serve { port } => {
            // Initialize database
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            println!("Starting web dashboard at http://localhost:{}", port);
            web::serve(db, port).await?;
        }

        Commands::DebugEmail { id } => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            let query = match id {
                Some(id) => format!("SELECT id, gmail_id, subject, raw_body, event_type FROM raw_emails WHERE id = {}", id),
                None => "SELECT id, gmail_id, subject, raw_body, event_type FROM raw_emails LIMIT 1".to_string(),
            };

            let row: Option<(i64, String, Option<String>, String, String)> =
                sqlx::query_as(&query)
                    .fetch_optional(db.pool())
                    .await?;

            if let Some((id, gmail_id, subject, raw_body, event_type)) = row {
                println!("=== XYZMARKER Email ID: {} ===", id);
                println!("Gmail ID: {}", gmail_id);
                println!("Subject: {:?}", subject);
                println!("Event Type: {}", event_type);
                println!("Body length: {} chars", raw_body.len());
                println!("\n--- First 500 chars of body ---");
                println!("{}", &raw_body[..std::cmp::min(500, raw_body.len())]);
                println!("\n--- Contains patterns ---");
                println!("  =20: {}", raw_body.contains("=20"));
                println!("  =3D: {}", raw_body.contains("=3D"));
                println!("  shipped: {}", raw_body.to_lowercase().contains("shipped"));
                println!("  confirmed: {}", raw_body.to_lowercase().contains("confirmed"));
                println!("  cancel: {}", raw_body.to_lowercase().contains("cancel"));
                println!("  delivered: {}", raw_body.to_lowercase().contains("delivered"));

                // Search for order number patterns (limit to 5)
                let order_pattern = regex::Regex::new(r"(?i)order\s*(?:number|#|num\.?)?[:\s]*([0-9-]{8,})").unwrap();
                println!("\n--- Order ID patterns found (first 5) ---");
                for (i, cap) in order_pattern.captures_iter(&raw_body).take(5).enumerate() {
                    if let Some(m) = cap.get(1) {
                        println!("  {}: {}", i + 1, m.as_str());
                    }
                }

                // Check for price-related patterns FIRST
                println!("\n--- Price/Total patterns ---");
                println!("  order-total: {}", raw_body.contains("order-total"));
                println!("  automation-id: {}", raw_body.contains("automation-id"));
                println!("  fees: {}", raw_body.to_lowercase().contains("fees"));
                println!("  $106: {}", raw_body.contains("$106"));
                println!("  106.84: {}", raw_body.contains("106.84"));

                // Search for price patterns
                let price_pattern = regex::Regex::new(r"\$[\d,]+\.\d{2}").unwrap();
                println!("\n--- All prices found (first 20) ---");
                for (i, cap) in price_pattern.find_iter(&raw_body).take(20).enumerate() {
                    println!("  {}: {}", i + 1, cap.as_str());
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

            // Reset all emails to pending
            let emails_reset = db.reset_email_status().await?;

            // Also reset "processed" emails to "pending"
            let processed_reset = sqlx::query(
                "UPDATE raw_emails SET processing_status = 'pending', error_message = NULL, processed_at = NULL WHERE processing_status = 'processed'"
            )
            .execute(db.pool())
            .await?;

            // Re-infer event_type based on subject patterns (fixes classification for "Thanks for your order" emails)
            sqlx::query(
                r#"
                UPDATE raw_emails SET event_type =
                    CASE
                        WHEN LOWER(subject) LIKE '%confirmed%' OR LOWER(subject) LIKE '%confirmation%' OR LOWER(subject) LIKE '%thanks for your%' THEN 'confirmation'
                        WHEN LOWER(subject) LIKE '%cancel%' THEN 'cancellation'
                        WHEN LOWER(subject) LIKE '%shipped%' OR LOWER(subject) LIKE '%on its way%' THEN 'shipping'
                        WHEN LOWER(subject) LIKE '%delivered%' OR LOWER(subject) LIKE '%arrived%' THEN 'delivery'
                        ELSE event_type
                    END
                "#
            )
            .execute(db.pool())
            .await?;

            println!("Cleared {} orders and {} line items", orders_result.rows_affected(), items_result.rows_affected());
            println!("Reset {} processed emails to pending", processed_reset.rows_affected());
            println!("Run 'process' to reprocess emails with updated parsing logic");
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

        Commands::CheckDelivery { dry_run: _ } => {
            // FedEx tracking via API is no longer available due to Akamai Bot Manager protection.
            // The Akamai sensor_data generation requires JavaScript execution which cannot be
            // done programmatically without browser automation.
            println!("FedEx delivery status checking is currently unavailable.");
            println!();
            println!("FedEx uses Akamai Bot Manager which requires browser-generated cookies.");
            println!("To check delivery status, use one of these alternatives:");
            println!("  1. Visit https://www.fedex.com/fedextrack/ in a browser");
            println!("  2. Use browser automation (Playwright/Puppeteer)");
            println!("  3. Use FedEx's official tracking API with an API key");
            println!();
            println!("The experimental Akamai code has been moved to src/tracking/akamai/");
            println!("for reference, but it cannot generate valid sensor_data without JS execution.");
        }

        Commands::AddAccount { client_secret } => {
            // Initialize database
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            println!("Starting OAuth flow for new account...");
            println!("A browser window will open. Please sign in with your Gmail account.");

            // Trigger OAuth and discover email
            let (email, token_path) = auth::authenticate_new_account(&client_secret).await?;

            // Check if account already exists
            if let Some(existing) = db.get_account_by_email(&email).await? {
                println!("\nAccount {} is already registered (id: {})", email, existing.id);
                return Ok(());
            }

            // Store in database
            let account_id = db.add_account(&email, &token_path.display().to_string()).await?;

            println!("\nAccount added successfully!");
            println!("  Email: {}", email);
            println!("  Token cache: {}", token_path.display());
            println!("  Account ID: {}", account_id);
            println!("\nYou can now sync emails for this account.");
        }

        Commands::ListAccounts => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            let accounts = db.list_accounts().await?;

            if accounts.is_empty() {
                println!("No accounts configured.");
                println!("\nTo add an account, run: cargo run -- add-account");
            } else {
                println!("Configured Gmail accounts:\n");
                for account in accounts {
                    println!("  [{}] {}", account.id, account.email);
                    if let Some(last_sync) = account.last_sync_at {
                        println!("      Last sync: {}", last_sync);
                    } else {
                        println!("      Last sync: never");
                    }
                    println!("      Token cache: {}", account.token_cache_path);
                    println!();
                }
            }
        }

        Commands::DebugAccounts => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            println!("=== Account ID Distribution Debug ===\n");

            // Show accounts table
            let accounts: Vec<(i64, String)> = sqlx::query_as(
                "SELECT id, email FROM accounts ORDER BY id"
            )
            .fetch_all(db.pool())
            .await?;

            println!("Registered accounts:");
            if accounts.is_empty() {
                println!("  (none)");
            } else {
                for (id, email) in &accounts {
                    println!("  ID {} = {}", id, email);
                }
            }

            // Show orders by account_id
            println!("\nOrders by account_id:");
            let order_counts: Vec<(Option<i64>, i64)> = sqlx::query_as(
                "SELECT account_id, COUNT(*) FROM orders GROUP BY account_id ORDER BY account_id"
            )
            .fetch_all(db.pool())
            .await?;

            if order_counts.is_empty() {
                println!("  (no orders)");
            } else {
                for (acc_id, count) in &order_counts {
                    match acc_id {
                        Some(id) => println!("  account_id={}: {} orders", id, count),
                        None => println!("  account_id=NULL: {} orders", count),
                    }
                }
            }

            // Show raw_emails by account_id
            println!("\nRaw emails by account_id:");
            let email_counts: Vec<(Option<i64>, i64)> = sqlx::query_as(
                "SELECT account_id, COUNT(*) FROM raw_emails GROUP BY account_id ORDER BY account_id"
            )
            .fetch_all(db.pool())
            .await?;

            if email_counts.is_empty() {
                println!("  (no emails)");
            } else {
                for (acc_id, count) in &email_counts {
                    match acc_id {
                        Some(id) => println!("  account_id={}: {} emails", id, count),
                        None => println!("  account_id=NULL: {} emails", count),
                    }
                }
            }

            // Show sample orders with their account_id
            println!("\nSample orders (first 5):");
            let sample_orders: Vec<(String, String, Option<i64>)> = sqlx::query_as(
                "SELECT id, status, account_id FROM orders ORDER BY order_date DESC LIMIT 5"
            )
            .fetch_all(db.pool())
            .await?;

            for (id, status, acc_id) in sample_orders {
                println!("  {} [{}] account_id={:?}", id, status, acc_id);
            }
        }

        Commands::AssociateEmails { account_id } => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            // Verify account exists
            let account: Option<(String,)> = sqlx::query_as(
                "SELECT email FROM accounts WHERE id = ?"
            )
            .bind(account_id)
            .fetch_optional(db.pool())
            .await?;

            let email = match account {
                Some((e,)) => e,
                None => {
                    println!("Error: Account ID {} not found", account_id);
                    println!("\nUse 'list-accounts' to see available accounts.");
                    return Ok(());
                }
            };

            println!("Associating orphan emails with account: {} (id={})", email, account_id);

            // Count orphan emails
            let (orphan_count,): (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM raw_emails WHERE account_id IS NULL"
            )
            .fetch_one(db.pool())
            .await?;

            if orphan_count == 0 {
                println!("No orphan emails found (all emails already have account_id set).");
                return Ok(());
            }

            // Update orphan emails
            let result = sqlx::query(
                "UPDATE raw_emails SET account_id = ? WHERE account_id IS NULL"
            )
            .bind(account_id)
            .execute(db.pool())
            .await?;

            println!("Updated {} emails to account_id={}", result.rows_affected(), account_id);

            // Also update orphan orders
            let (orphan_orders,): (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM orders WHERE account_id IS NULL"
            )
            .fetch_one(db.pool())
            .await?;

            if orphan_orders > 0 {
                let orders_result = sqlx::query(
                    "UPDATE orders SET account_id = ? WHERE account_id IS NULL"
                )
                .bind(account_id)
                .execute(db.pool())
                .await?;

                println!("Updated {} orders to account_id={}", orders_result.rows_affected(), account_id);
            }

            println!("\nDone! Run 'debug-accounts' to verify the changes.");
        }

        Commands::ReassignAccount { from_id, to_id } => {
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            // Verify both accounts exist
            let from_email: Option<(String,)> = sqlx::query_as(
                "SELECT email FROM accounts WHERE id = ?"
            )
            .bind(from_id)
            .fetch_optional(db.pool())
            .await?;

            let to_email: Option<(String,)> = sqlx::query_as(
                "SELECT email FROM accounts WHERE id = ?"
            )
            .bind(to_id)
            .fetch_optional(db.pool())
            .await?;

            let from = match from_email {
                Some((e,)) => e,
                None => {
                    println!("Error: Source account ID {} not found", from_id);
                    return Ok(());
                }
            };

            let to = match to_email {
                Some((e,)) => e,
                None => {
                    println!("Error: Target account ID {} not found", to_id);
                    return Ok(());
                }
            };

            println!("Reassigning data from {} (id={}) to {} (id={})", from, from_id, to, to_id);

            // Update emails
            let emails_result = sqlx::query(
                "UPDATE raw_emails SET account_id = ? WHERE account_id = ?"
            )
            .bind(to_id)
            .bind(from_id)
            .execute(db.pool())
            .await?;

            println!("Updated {} emails", emails_result.rows_affected());

            // Update orders
            let orders_result = sqlx::query(
                "UPDATE orders SET account_id = ? WHERE account_id = ?"
            )
            .bind(to_id)
            .bind(from_id)
            .execute(db.pool())
            .await?;

            println!("Updated {} orders", orders_result.rows_affected());

            println!("\nDone! Run 'debug-accounts' to verify.");
        }

        Commands::RegisterToken { token_cache, client_secret } => {
            // Initialize database
            let db = Database::from_file(&db_path).await?;
            db.run_migrations().await?;

            // Check if token cache file exists
            if !token_cache.exists() {
                println!("Error: Token cache file not found: {}", token_cache.display());
                return Ok(());
            }

            println!("Discovering email from token cache: {}", token_cache.display());

            // Authenticate using the existing token cache
            let gmail_client = auth::get_gmail_client(&client_secret, &token_cache).await?;

            // Get the email address
            let email = auth::get_authenticated_email(&gmail_client).await?;
            println!("Discovered email: {}", email);

            // Check if account already exists
            if let Some(existing) = db.get_account_by_email(&email).await? {
                println!("\nAccount {} is already registered (id: {})", email, existing.id);
                return Ok(());
            }

            // Store in database with the existing token cache path
            let account_id = db.add_account(&email, &token_cache.display().to_string()).await?;

            println!("\nAccount registered successfully!");
            println!("  Email: {}", email);
            println!("  Token cache: {}", token_cache.display());
            println!("  Account ID: {}", account_id);
            println!("\nYou can now sync emails for this account with:");
            println!("  cargo run -- sync --account {}", email);
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

    let mut result = format!("Order status:\n  Total orders: {}", total.0);
    for (status, count) in by_status {
        result.push_str(&format!("\n  {}: {}", status, count));
    }

    Ok(result)
}
