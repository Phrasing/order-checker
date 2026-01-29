//! Walmart Order Reconciler
//!
//! A high-performance dashboard for tracking Walmart orders using event-sourcing.
//! Parses confirmation, cancellation, shipping, and delivery emails to build
//! a complete view of order state.
//!
//! # Architecture
//!
//! - **Event Sourcing**: Order state is derived from multiple email events
//! - **ID Normalization**: Handles different ID formats across email types
//! - **Fuzzy Matching**: Parses dynamic CSS classes in Walmart emails
//! - **Gmail Integration**: Fetches emails directly from Gmail API
//!
//! # Modules
//!
//! - `models`: Core data structures (WalmartOrder, LineItem, etc.)
//! - `parsing`: Email HTML parsing with fuzzy class matching
//! - `db`: SQLite database operations
//! - `auth`: OAuth2 authentication for Gmail API
//! - `ingestion`: Email fetching and sync from Gmail
//! - `process`: Event processing and order reconciliation
//! - `web`: Web dashboard and view models (used by both Axum and Tauri)

pub mod auth;
pub mod db;
pub mod ingestion;
pub mod images;
pub mod models;
pub mod parsing;
pub mod process;
pub mod tracking;
pub mod web;

// Re-export core types
pub use models::{ItemStatus, LineItem, OrderStatus, WalmartOrder};
pub use parsing::WalmartEmailParser;
pub use ingestion::{sync_emails, sync_emails_with_days, GmailFetcher, SyncStats};
pub use process::{process_pending_events, ProcessStats};

// Re-export web module types for Tauri
pub use web::{OrderViewModel, ItemViewModel, StatusCounts, DashboardData, get_dashboard_data};
