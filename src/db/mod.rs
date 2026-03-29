//! Database operations for the Walmart Order Reconciler
//!
//! This module handles SQLite database connections and CRUD operations
//! for orders, line items, email events, and accounts.

use anyhow::Result;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::collections::HashSet;
use std::path::Path;
use std::str::FromStr;

/// Database connection pool wrapper
pub struct Database {
    pool: SqlitePool,
}

impl Database {
    /// Create a new database connection pool with WAL mode and busy timeout
    pub async fn new(database_url: &str) -> Result<Self> {
        let options = SqliteConnectOptions::from_str(database_url)?
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(std::time::Duration::from_secs(30))
            .pragma("foreign_keys", "ON")
            .pragma("synchronous", "NORMAL")
            .pragma("cache_size", "-16000");

        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await?;

        Ok(Self { pool })
    }

    /// Create an in-memory database for testing
    pub async fn in_memory() -> Result<Self> {
        Self::new("sqlite::memory:").await
    }

    /// Create a file-based database
    pub async fn from_file(path: &Path) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(std::time::Duration::from_secs(30))
            .pragma("foreign_keys", "ON")
            .pragma("synchronous", "NORMAL")
            .pragma("cache_size", "-16000");

        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await?;

        Ok(Self { pool })
    }

    /// Get a reference to the connection pool
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Execute a SQL string that may contain multiple statements.
    ///
    /// `sqlx::raw_sql(sql).execute(pool)` only runs the **first** statement.
    /// This method uses `.execute_many()` and drains the returned stream so
    /// that every statement in the SQL text is executed.
    async fn execute_sql(&self, sql: &str) -> Result<()> {
        let mut stream = sqlx::raw_sql(sql).execute_many(&self.pool);
        while let Some(_result) = stream.try_next().await? {}
        Ok(())
    }

    /// Run migrations to set up the database schema
    pub async fn run_migrations(&self) -> Result<()> {
        // Phase 1: Core schema migrations (tables and structure)
        let schema_migrations = [
            include_str!("../../migrations/001_initial_schema.sql"),
            include_str!("../../migrations/002_raw_emails.sql"),
            include_str!("../../migrations/003_tracking_info.sql"),
            include_str!("../../migrations/004_tracking_cache.sql"),
            include_str!("../../migrations/005_accounts.sql"),
            include_str!("../../migrations/006_fix_line_items_duplicates.sql"),
            include_str!("../../migrations/012_images.sql"),
            include_str!("../../migrations/013_image_thumbnails.sql"),
        ];

        for (i, migration_sql) in schema_migrations.iter().enumerate() {
            self.execute_sql(migration_sql).await?;
            tracing::debug!("Migration {} completed", i + 1);
        }

        // Phase 2: Add columns that may already exist
        // Must run before data migrations (007+) that reference these columns
        // SQLite doesn't support IF NOT EXISTS for ALTER TABLE ADD COLUMN
        let optional_columns = [
            "ALTER TABLE orders ADD COLUMN tracking_number TEXT",
            "ALTER TABLE orders ADD COLUMN carrier TEXT",
            "ALTER TABLE orders ADD COLUMN account_id INTEGER REFERENCES accounts(id)",
            "ALTER TABLE raw_emails ADD COLUMN account_id INTEGER REFERENCES accounts(id)",
            "ALTER TABLE orders ADD COLUMN shipped_date TEXT",
            "ALTER TABLE accounts ADD COLUMN profile_picture_url TEXT",
            "ALTER TABLE raw_emails ADD COLUMN recipient TEXT",
            "ALTER TABLE orders ADD COLUMN recipient TEXT",
            "ALTER TABLE orders ADD COLUMN cancel_reason TEXT",
            "ALTER TABLE tracking_cache ADD COLUMN estimated_delivery TEXT",
        ];
        for sql in optional_columns {
            match sqlx::query(sql).execute(&self.pool).await {
                Ok(_) => tracing::debug!("Added column successfully"),
                Err(e) => {
                    if !e.to_string().contains("duplicate column name") {
                        return Err(e.into());
                    }
                    tracing::debug!("Column already exists, skipping");
                }
            }
        }

        // Phase 3: Data migrations that depend on optional columns
        let data_migrations = [
            ("007", include_str!("../../migrations/007_shipped_date.sql")),
            ("008", include_str!("../../migrations/008_fix_shipped_date.sql")),
            ("009", include_str!("../../migrations/009_profile_picture.sql")),
            ("011", include_str!("../../migrations/011_clear_processed_bodies.sql")),
        ];  

        for (name, migration_sql) in data_migrations {
            self.execute_sql(migration_sql).await?;
            tracing::debug!("Migration {} completed", name);
        }

        // Phase 4: Indexes that depend on optional columns
        self.execute_sql(include_str!("../../migrations/010_indexes.sql")).await?;
        tracing::debug!("Migration 010 (indexes) completed");

        // Phase 5: One-time data reset to allow clean re-sync after fixing the
        // fetch_email_full base64 double-decode bug (empty bodies).
        let sentinel_exists: bool = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='_reset_017')"
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or(false);

        if !sentinel_exists {
            sqlx::query("DELETE FROM email_events").execute(&self.pool).await?;
            sqlx::query("DELETE FROM line_items").execute(&self.pool).await?;
            sqlx::query("DELETE FROM orders").execute(&self.pool).await?;
            sqlx::query("DELETE FROM tracking_cache").execute(&self.pool).await?;
            sqlx::query("DELETE FROM tracking_events").execute(&self.pool).await?;
            sqlx::query("DELETE FROM raw_emails").execute(&self.pool).await?;
            sqlx::raw_sql("DROP TABLE IF EXISTS _reset_014").execute(&self.pool).await?;
            sqlx::raw_sql("DROP TABLE IF EXISTS _reset_015").execute(&self.pool).await?;
            sqlx::raw_sql("DROP TABLE IF EXISTS _reset_016").execute(&self.pool).await?;
            sqlx::raw_sql("DROP TABLE IF EXISTS line_items_new").execute(&self.pool).await?;
            sqlx::raw_sql("CREATE TABLE IF NOT EXISTS _reset_017 (done INTEGER PRIMARY KEY DEFAULT 1)")
                .execute(&self.pool).await?;
            tracing::info!("One-time data reset completed (017) — re-sync required");
        }

        // Phase 6: Fix order dates from email timestamps (idempotent)
        self.execute_sql(include_str!("../../migrations/014_fix_order_dates.sql")).await?;
        tracing::debug!("Migration 014 (fix order dates) completed");

        // Phase 7: Normalize any non-ISO order_date values
        self.normalize_order_dates().await?;

        // Phase 8: Reconcile order statuses where cancellation events were overridden
        self.reconcile_order_statuses().await?;

        tracing::info!("All database migrations completed successfully");
        Ok(())
    }

    /// Normalize order_date values that aren't in ISO 8601 format.
    ///
    /// Some legacy data may have dates stored as "Jul 18, 2025" instead of
    /// "2025-07-18T00:00:00Z". This breaks SQL `ORDER BY order_date DESC`
    /// because "J" > "2" lexicographically.
    async fn normalize_order_dates(&self) -> Result<()> {
        // Find all order_date values that don't start with a year (non-ISO)
        let bad_rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT id, order_date FROM orders WHERE order_date NOT LIKE '20%'"
        )
        .fetch_all(&self.pool)
        .await?;

        if bad_rows.is_empty() {
            return Ok(());
        }

        tracing::info!(
            count = bad_rows.len(),
            "Found non-ISO order_date values, normalizing"
        );

        let date_formats = [
            "%b %d, %Y",   // "Jul 18, 2025"
            "%B %d, %Y",   // "July 18, 2025"
            "%m/%d/%Y",    // "07/18/2025"
            "%d-%b-%Y",    // "18-Jul-2025"
        ];

        for (order_id, raw_date) in &bad_rows {
            let mut normalized = None;
            for fmt in &date_formats {
                if let Ok(parsed) = chrono::NaiveDate::parse_from_str(raw_date.trim(), fmt) {
                    normalized = Some(format!("{}T00:00:00Z", parsed));
                    break;
                }
            }

            if let Some(iso_date) = normalized {
                tracing::debug!(
                    order_id = %order_id,
                    from = %raw_date,
                    to = %iso_date,
                    "Normalizing order_date"
                );
                sqlx::query("UPDATE orders SET order_date = ? WHERE id = ?")
                    .bind(&iso_date)
                    .bind(order_id)
                    .execute(&self.pool)
                    .await?;
            } else {
                tracing::warn!(
                    order_id = %order_id,
                    date = %raw_date,
                    "Could not parse non-ISO order_date, leaving as-is"
                );
            }
        }

        Ok(())
    }

    /// Fix orders that show "shipped" but have a cancellation event.
    ///
    /// This can happen when a shipping email is processed after a cancellation
    /// email, overriding the status. The status precedence guard in process.rs
    /// prevents this going forward, but existing data needs reconciliation.
    async fn reconcile_order_statuses(&self) -> Result<()> {
        let affected: Vec<(String,)> = sqlx::query_as(
            r#"
            SELECT DISTINCT o.id
            FROM orders o
            WHERE o.status = 'shipped'
            AND EXISTS (
                SELECT 1 FROM email_events ee
                WHERE ee.order_id = o.id AND ee.event_type = 'cancellation'
            )
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        if !affected.is_empty() {
            tracing::info!(
                count = affected.len(),
                "Found shipped orders with cancellation events, reconciling"
            );

            for (order_id,) in &affected {
                let (non_canceled,): (i64,) = sqlx::query_as(
                    "SELECT COUNT(*) FROM line_items WHERE order_id = ? AND status != 'canceled'",
                )
                .bind(order_id)
                .fetch_one(&self.pool)
                .await?;

                let new_status = if non_canceled == 0 {
                    "canceled"
                } else {
                    "partially_canceled"
                };

                sqlx::query("UPDATE orders SET status = ? WHERE id = ?")
                    .bind(new_status)
                    .bind(order_id)
                    .execute(&self.pool)
                    .await?;

                tracing::info!(
                    order_id = %order_id,
                    from = "shipped",
                    to = %new_status,
                    "Reconciled order status"
                );
            }
        }

        // Also fix orders misclassified as "shipping" when the email subject
        // indicates delivery (e.g., "Delivered: ..."). This happens when the
        // HTML body lacks explicit delivery keywords but has tracking URLs.
        let misclassified: Vec<(String,)> = sqlx::query_as(
            r#"
            SELECT DISTINCT ee.order_id
            FROM email_events ee
            WHERE ee.event_type = 'shipping'
            AND ee.email_subject LIKE 'Delivered:%'
            AND EXISTS (
                SELECT 1 FROM orders o
                WHERE o.id = ee.order_id AND o.status = 'shipped'
            )
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        for (order_id,) in &misclassified {
            sqlx::query("UPDATE orders SET status = 'delivered' WHERE id = ?")
                .bind(order_id)
                .execute(&self.pool)
                .await?;

            sqlx::query("UPDATE line_items SET status = 'delivered' WHERE order_id = ?")
                .bind(order_id)
                .execute(&self.pool)
                .await?;

            tracing::info!(
                order_id = %order_id,
                from = "shipped",
                to = "delivered",
                "Reconciled misclassified delivery"
            );
        }

        // Phase 3: Deduplicate email_events (clean up duplicates from prior re-fetch cycles)
        let dedup_result = sqlx::query(
            r#"
            DELETE FROM email_events
            WHERE id NOT IN (
                SELECT MIN(id) FROM email_events
                GROUP BY order_id, event_type, email_subject
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        if dedup_result.rows_affected() > 0 {
            tracing::info!(
                count = dedup_result.rows_affected(),
                "Removed duplicate email_events"
            );
        }

        Ok(())
    }

    /// Check if a gmail_id already exists in raw_emails
    pub async fn email_exists(&self, gmail_id: &str) -> Result<bool> {
        let result: Option<(i32,)> = sqlx::query_as(
            "SELECT 1 FROM raw_emails WHERE gmail_id = ? LIMIT 1"
        )
        .bind(gmail_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(result.is_some())
    }

    /// Bulk check which gmail_ids already exist in the database
    /// Returns a HashSet of IDs that are already stored
    pub async fn get_existing_gmail_ids(&self, ids: &[&str]) -> Result<HashSet<String>> {
        if ids.is_empty() {
            return Ok(HashSet::new());
        }

        // SQLite has a limit on the number of variables (SQLITE_MAX_VARIABLE_NUMBER, default 999)
        // Process in chunks to avoid hitting this limit
        const CHUNK_SIZE: usize = 500;
        let mut existing = HashSet::new();

        for chunk in ids.chunks(CHUNK_SIZE) {
            let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
            let query = format!(
                "SELECT gmail_id FROM raw_emails WHERE gmail_id IN ({}) AND processing_status != 'pending'",
                placeholders.join(", ")
            );

            let mut query_builder = sqlx::query(&query);
            for id in chunk {
                query_builder = query_builder.bind(*id);
            }

            let rows = query_builder.fetch_all(&self.pool).await?;
            for row in rows {
                let id: String = row.get("gmail_id");
                existing.insert(id);
            }
        }

        Ok(existing)
    }

    /// Insert a raw email into the staging table
    pub async fn insert_raw_email(
        &self,
        gmail_id: &str,
        thread_id: Option<&str>,
        subject: Option<&str>,
        snippet: Option<&str>,
        sender: Option<&str>,
        recipient: Option<&str>,
        raw_body: &str,
        event_type: &str,
        gmail_date: Option<&str>,
    ) -> Result<i64> {
        let result = sqlx::query(
            r#"
            INSERT INTO raw_emails (gmail_id, thread_id, subject, snippet, sender, recipient, raw_body, event_type, gmail_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#
        )
        .bind(gmail_id)
        .bind(thread_id)
        .bind(subject)
        .bind(snippet)
        .bind(sender)
        .bind(recipient)
        .bind(raw_body)
        .bind(event_type)
        .bind(gmail_date)
        .execute(&self.pool)
        .await?;

        Ok(result.last_insert_rowid())
    }

    /// Batch insert raw emails using a transaction with multi-row INSERT
    pub async fn insert_raw_emails_batch(&self, emails: &[EmailData]) -> Result<usize> {
        if emails.is_empty() {
            return Ok(0);
        }

        let mut tx = self.pool.begin().await?;
        let mut inserted = 0usize;

        // 9 columns per row, SQLite limit 999 → max 111 rows per statement
        const MAX_ROWS: usize = 100;

        for chunk in emails.chunks(MAX_ROWS) {
            let row_placeholder = "(?,?,?,?,?,?,?,?,?)";
            let placeholders: Vec<&str> = chunk.iter().map(|_| row_placeholder).collect();
            let sql = format!(
                "INSERT OR IGNORE INTO raw_emails \
                 (gmail_id, thread_id, subject, snippet, sender, recipient, raw_body, event_type, gmail_date) \
                 VALUES {}",
                placeholders.join(", ")
            );
            let mut query = sqlx::query(&sql);
            for email in chunk {
                query = query
                    .bind(&email.gmail_id)
                    .bind(&email.thread_id)
                    .bind(&email.subject)
                    .bind(&email.snippet)
                    .bind(&email.sender)
                    .bind(&email.recipient)
                    .bind(&email.raw_body)
                    .bind(&email.event_type)
                    .bind(&email.gmail_date);
            }
            let result = query.execute(&mut *tx).await?;
            inserted += result.rows_affected() as usize;
        }

        tx.commit().await?;
        Ok(inserted)
    }

    /// Get count of raw emails by processing status
    pub async fn get_email_counts(&self) -> Result<EmailCounts> {
        let total: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM raw_emails")
            .fetch_one(&self.pool)
            .await?;

        let pending: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM raw_emails WHERE processing_status = 'pending'"
        )
        .fetch_one(&self.pool)
        .await?;

        let processed: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM raw_emails WHERE processing_status = 'processed'"
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(EmailCounts {
            total: total.0,
            pending: pending.0,
            processed: processed.0,
        })
    }

    /// Reset skipped/failed emails back to pending status
    pub async fn reset_email_status(&self) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE raw_emails SET processing_status = 'pending', error_message = NULL, processed_at = NULL WHERE processing_status IN ('skipped', 'failed')"
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    // ==================== Account Operations ====================

    /// Add a new Gmail account
    pub async fn add_account(&self, email: &str, token_cache_path: &str) -> Result<i64> {
        let result = sqlx::query(
            r#"
            INSERT INTO accounts (email, token_cache_path)
            VALUES (?, ?)
            "#,
        )
        .bind(email)
        .bind(token_cache_path)
        .execute(&self.pool)
        .await?;

        Ok(result.last_insert_rowid())
    }

    /// Get account by email address
    pub async fn get_account_by_email(&self, email: &str) -> Result<Option<Account>> {
        let row: Option<(i64, String, Option<String>, Option<String>, String, i32, Option<String>, String, String)> =
            sqlx::query_as(
                r#"
                SELECT id, email, display_name, profile_picture_url, token_cache_path, is_active, last_sync_at, created_at, updated_at
                FROM accounts
                WHERE email = ?
                "#,
            )
            .bind(email)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(
            |(id, email, display_name, profile_picture_url, token_cache_path, is_active, last_sync_at, created_at, updated_at)| {
                Account {
                    id,
                    email,
                    display_name,
                    profile_picture_url,
                    token_cache_path,
                    is_active: is_active != 0,
                    last_sync_at,
                    created_at,
                    updated_at,
                }
            },
        ))
    }

    /// Get account by ID
    pub async fn get_account_by_id(&self, id: i64) -> Result<Option<Account>> {
        let row: Option<(i64, String, Option<String>, Option<String>, String, i32, Option<String>, String, String)> =
            sqlx::query_as(
                r#"
                SELECT id, email, display_name, profile_picture_url, token_cache_path, is_active, last_sync_at, created_at, updated_at
                FROM accounts
                WHERE id = ?
                "#,
            )
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(
            |(id, email, display_name, profile_picture_url, token_cache_path, is_active, last_sync_at, created_at, updated_at)| {
                Account {
                    id,
                    email,
                    display_name,
                    profile_picture_url,
                    token_cache_path,
                    is_active: is_active != 0,
                    last_sync_at,
                    created_at,
                    updated_at,
                }
            },
        ))
    }

    /// List all active accounts
    pub async fn list_accounts(&self) -> Result<Vec<Account>> {
        let rows: Vec<(i64, String, Option<String>, Option<String>, String, i32, Option<String>, String, String)> =
            sqlx::query_as(
                r#"
                SELECT id, email, display_name, profile_picture_url, token_cache_path, is_active, last_sync_at, created_at, updated_at
                FROM accounts
                WHERE is_active = 1
                ORDER BY email
                "#,
            )
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .into_iter()
            .map(
                |(id, email, display_name, profile_picture_url, token_cache_path, is_active, last_sync_at, created_at, updated_at)| {
                    Account {
                        id,
                        email,
                        display_name,
                        profile_picture_url,
                        token_cache_path,
                        is_active: is_active != 0,
                        last_sync_at,
                        created_at,
                        updated_at,
                    }
                },
            )
            .collect())
    }

    /// Deactivate an account (soft delete)
    pub async fn deactivate_account(&self, email: &str) -> Result<bool> {
        let result = sqlx::query("UPDATE accounts SET is_active = 0 WHERE email = ?")
            .bind(email)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Update the last_sync_at timestamp for an account
    pub async fn update_account_last_sync(&self, account_id: i64) -> Result<()> {
        sqlx::query("UPDATE accounts SET last_sync_at = datetime('now') WHERE id = ?")
            .bind(account_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Update account profile picture URL
    pub async fn update_account_profile_picture(&self, account_id: i64, url: Option<&str>) -> Result<()> {
        sqlx::query("UPDATE accounts SET profile_picture_url = ? WHERE id = ?")
            .bind(url)
            .bind(account_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Delete account data (emails and orders) for a specific account
    pub async fn delete_account_data(&self, account_id: i64) -> Result<(u64, u64)> {
        // Delete orders first (due to foreign key constraints)
        let orders_deleted = sqlx::query("DELETE FROM orders WHERE account_id = ?")
            .bind(account_id)
            .execute(&self.pool)
            .await?
            .rows_affected();

        // Delete raw emails
        let emails_deleted = sqlx::query("DELETE FROM raw_emails WHERE account_id = ?")
            .bind(account_id)
            .execute(&self.pool)
            .await?
            .rows_affected();

        // Finally delete the account
        sqlx::query("DELETE FROM accounts WHERE id = ?")
            .bind(account_id)
            .execute(&self.pool)
            .await?;

        Ok((orders_deleted, emails_deleted))
    }

    /// Check existing gmail_ids for a specific account
    pub async fn get_existing_gmail_ids_for_account(
        &self,
        account_id: i64,
        ids: &[&str],
    ) -> Result<HashSet<String>> {
        if ids.is_empty() {
            return Ok(HashSet::new());
        }

        const CHUNK_SIZE: usize = 500;
        let mut existing = HashSet::new();

        for chunk in ids.chunks(CHUNK_SIZE) {
            let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
            let query = format!(
                "SELECT gmail_id FROM raw_emails WHERE (account_id = ? OR account_id IS NULL) AND gmail_id IN ({}) AND processing_status != 'pending'",
                placeholders.join(", ")
            );

            let mut query_builder = sqlx::query(&query);
            query_builder = query_builder.bind(account_id);
            for id in chunk {
                query_builder = query_builder.bind(*id);
            }

            let rows = query_builder.fetch_all(&self.pool).await?;
            for row in rows {
                let id: String = row.get("gmail_id");
                existing.insert(id);
            }
        }

        Ok(existing)
    }

    /// Batch insert raw emails with account_id using multi-row INSERT
    pub async fn insert_raw_emails_batch_with_account(
        &self,
        account_id: i64,
        emails: &[EmailData],
    ) -> Result<usize> {
        if emails.is_empty() {
            return Ok(0);
        }

        let mut tx = self.pool.begin().await?;
        let mut inserted = 0usize;

        // 10 columns per row, SQLite limit 999 → max 99 rows per statement
        const MAX_ROWS: usize = 90;

        for chunk in emails.chunks(MAX_ROWS) {
            let row_placeholder = "(?,?,?,?,?,?,?,?,?,?)";
            let placeholders: Vec<&str> = chunk.iter().map(|_| row_placeholder).collect();
            let sql = format!(
                "INSERT OR IGNORE INTO raw_emails \
                 (gmail_id, thread_id, subject, snippet, sender, recipient, raw_body, event_type, gmail_date, account_id) \
                 VALUES {}",
                placeholders.join(", ")
            );
            let mut query = sqlx::query(&sql);
            for email in chunk {
                query = query
                    .bind(&email.gmail_id)
                    .bind(&email.thread_id)
                    .bind(&email.subject)
                    .bind(&email.snippet)
                    .bind(&email.sender)
                    .bind(&email.recipient)
                    .bind(&email.raw_body)
                    .bind(&email.event_type)
                    .bind(&email.gmail_date)
                    .bind(account_id);
            }
            let result = query.execute(&mut *tx).await?;
            inserted += result.rows_affected() as usize;
        }

        tx.commit().await?;
        Ok(inserted)
    }
}

/// Email count statistics
#[derive(Debug)]
pub struct EmailCounts {
    pub total: i64,
    pub pending: i64,
    pub processed: i64,
}

/// Data for batch email insertion
#[derive(Debug, Clone)]
pub struct EmailData {
    pub gmail_id: String,
    pub thread_id: Option<String>,
    pub subject: Option<String>,
    pub snippet: Option<String>,
    pub sender: Option<String>,
    pub recipient: Option<String>,
    pub raw_body: String,
    pub event_type: String,
    pub gmail_date: Option<String>,
    pub account_id: Option<i64>,
}

/// Represents a Gmail account in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub id: i64,
    pub email: String,
    pub display_name: Option<String>,
    pub profile_picture_url: Option<String>,
    pub token_cache_path: String,
    pub is_active: bool,
    pub last_sync_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_in_memory_db() {
        let db = Database::in_memory().await.expect("Should create in-memory DB");
        db.run_migrations().await.expect("Should run migrations");
    }
}
