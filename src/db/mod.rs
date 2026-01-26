//! Database operations for the Walmart Order Reconciler
//!
//! This module handles SQLite database connections and CRUD operations
//! for orders, line items, email events, and accounts.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqlitePoolOptions, Row, SqlitePool};
use std::collections::HashSet;
use std::path::Path;

/// Database connection pool wrapper
pub struct Database {
    pool: SqlitePool,
}

impl Database {
    /// Create a new database connection pool
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;

        Ok(Self { pool })
    }

    /// Create an in-memory database for testing
    pub async fn in_memory() -> Result<Self> {
        Self::new("sqlite::memory:").await
    }

    /// Create a file-based database
    pub async fn from_file(path: &Path) -> Result<Self> {
        let url = format!("sqlite:{}?mode=rwc", path.display());
        Self::new(&url).await
    }

    /// Get a reference to the connection pool
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Run migrations to set up the database schema
    pub async fn run_migrations(&self) -> Result<()> {
        // Run all migrations in order
        let migrations = [
            include_str!("../../migrations/001_initial_schema.sql"),
            include_str!("../../migrations/002_raw_emails.sql"),
            include_str!("../../migrations/003_tracking_info.sql"),
            include_str!("../../migrations/004_tracking_cache.sql"),
            include_str!("../../migrations/005_accounts.sql"),
        ];

        for (i, migration_sql) in migrations.iter().enumerate() {
            sqlx::raw_sql(migration_sql)
                .execute(&self.pool)
                .await?;
            tracing::debug!("Migration {} completed", i + 1);
        }

        // Handle columns that may already exist - SQLite doesn't support IF NOT EXISTS for ALTER TABLE
        let optional_columns = [
            "ALTER TABLE orders ADD COLUMN tracking_number TEXT",
            "ALTER TABLE orders ADD COLUMN carrier TEXT",
            "ALTER TABLE orders ADD COLUMN account_id INTEGER REFERENCES accounts(id)",
            "ALTER TABLE raw_emails ADD COLUMN account_id INTEGER REFERENCES accounts(id)",
        ];
        for sql in optional_columns {
            match sqlx::query(sql).execute(&self.pool).await {
                Ok(_) => tracing::debug!("Added column successfully"),
                Err(e) => {
                    // Ignore "duplicate column name" errors (SQLite error code 1)
                    if !e.to_string().contains("duplicate column name") {
                        return Err(e.into());
                    }
                    tracing::debug!("Column already exists, skipping");
                }
            }
        }

        tracing::info!("All database migrations completed successfully");
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
                "SELECT gmail_id FROM raw_emails WHERE gmail_id IN ({})",
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
        raw_body: &str,
        event_type: &str,
        gmail_date: Option<&str>,
    ) -> Result<i64> {
        let result = sqlx::query(
            r#"
            INSERT INTO raw_emails (gmail_id, thread_id, subject, snippet, sender, raw_body, event_type, gmail_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            "#
        )
        .bind(gmail_id)
        .bind(thread_id)
        .bind(subject)
        .bind(snippet)
        .bind(sender)
        .bind(raw_body)
        .bind(event_type)
        .bind(gmail_date)
        .execute(&self.pool)
        .await?;

        Ok(result.last_insert_rowid())
    }

    /// Batch insert raw emails using a transaction
    /// Uses INSERT OR IGNORE to handle duplicates gracefully
    pub async fn insert_raw_emails_batch(&self, emails: &[EmailData]) -> Result<usize> {
        if emails.is_empty() {
            return Ok(0);
        }

        let mut tx = self.pool.begin().await?;
        let mut inserted = 0;

        for email in emails {
            let result = sqlx::query(
                r#"
                INSERT OR IGNORE INTO raw_emails (gmail_id, thread_id, subject, snippet, sender, raw_body, event_type, gmail_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                "#
            )
            .bind(&email.gmail_id)
            .bind(&email.thread_id)
            .bind(&email.subject)
            .bind(&email.snippet)
            .bind(&email.sender)
            .bind(&email.raw_body)
            .bind(&email.event_type)
            .bind(&email.gmail_date)
            .execute(&mut *tx)
            .await?;

            if result.rows_affected() > 0 {
                inserted += 1;
            }
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
        let row: Option<(i64, String, Option<String>, String, i32, Option<String>, String, String)> =
            sqlx::query_as(
                r#"
                SELECT id, email, display_name, token_cache_path, is_active, last_sync_at, created_at, updated_at
                FROM accounts
                WHERE email = ?
                "#,
            )
            .bind(email)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(
            |(id, email, display_name, token_cache_path, is_active, last_sync_at, created_at, updated_at)| {
                Account {
                    id,
                    email,
                    display_name,
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
        let row: Option<(i64, String, Option<String>, String, i32, Option<String>, String, String)> =
            sqlx::query_as(
                r#"
                SELECT id, email, display_name, token_cache_path, is_active, last_sync_at, created_at, updated_at
                FROM accounts
                WHERE id = ?
                "#,
            )
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(
            |(id, email, display_name, token_cache_path, is_active, last_sync_at, created_at, updated_at)| {
                Account {
                    id,
                    email,
                    display_name,
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
        let rows: Vec<(i64, String, Option<String>, String, i32, Option<String>, String, String)> =
            sqlx::query_as(
                r#"
                SELECT id, email, display_name, token_cache_path, is_active, last_sync_at, created_at, updated_at
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
                |(id, email, display_name, token_cache_path, is_active, last_sync_at, created_at, updated_at)| {
                    Account {
                        id,
                        email,
                        display_name,
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
                "SELECT gmail_id FROM raw_emails WHERE account_id = ? AND gmail_id IN ({})",
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

    /// Batch insert raw emails with account_id
    pub async fn insert_raw_emails_batch_with_account(
        &self,
        account_id: i64,
        emails: &[EmailData],
    ) -> Result<usize> {
        if emails.is_empty() {
            return Ok(0);
        }

        let mut tx = self.pool.begin().await?;
        let mut inserted = 0;

        for email in emails {
            let result = sqlx::query(
                r#"
                INSERT OR IGNORE INTO raw_emails (gmail_id, thread_id, subject, snippet, sender, raw_body, event_type, gmail_date, account_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(&email.gmail_id)
            .bind(&email.thread_id)
            .bind(&email.subject)
            .bind(&email.snippet)
            .bind(&email.sender)
            .bind(&email.raw_body)
            .bind(&email.event_type)
            .bind(&email.gmail_date)
            .bind(account_id)
            .execute(&mut *tx)
            .await?;

            if result.rows_affected() > 0 {
                inserted += 1;
            }
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
