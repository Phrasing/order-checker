//! Web dashboard module
//!
//! Provides view models and data-fetching functions for the dashboard.
//! Used by both the Axum web server and the Tauri desktop application.

use crate::db::Database;
use anyhow::Result;
use askama::Template;
use axum::{
    extract::State,
    response::Html,
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::TcpListener;

/// Application state shared across handlers
pub struct AppState {
    pub db: Database,
}

/// View model for an order in the dashboard
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderViewModel {
    pub id: String,
    pub order_date: String,
    pub status: String,
    pub total_cost: Option<String>,
    pub items: Vec<ItemViewModel>,
    pub tracking_number: Option<String>,
    pub carrier: Option<String>,
}

/// View model for a line item
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemViewModel {
    pub name: String,
    pub quantity: u32,
    pub status: String,
}

/// Status counts for the summary cards
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StatusCounts {
    pub confirmed: i64,
    pub shipped: i64,
    pub delivered: i64,
    pub canceled: i64,
    pub partially_canceled: i64,
}

/// View model for an account in the dashboard
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountViewModel {
    pub id: i64,
    pub email: String,
    pub display_name: Option<String>,
    pub order_count: i64,
    pub last_sync_at: Option<String>,
}

/// Dashboard data structure for Tauri IPC
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardData {
    pub orders: Vec<OrderViewModel>,
    pub total_orders: i64,
    pub pending_emails: i64,
    pub status_counts: StatusCounts,
    pub last_updated: String,
    /// List of configured accounts
    pub accounts: Vec<AccountViewModel>,
    /// Currently selected account ID (None = all accounts)
    pub selected_account_id: Option<i64>,
}

/// Dashboard template
#[derive(Template)]
#[template(path = "dashboard.html")]
pub struct DashboardTemplate {
    pub orders: Vec<OrderViewModel>,
    pub total_orders: i64,
    pub pending_emails: i64,
    pub status_counts: StatusCounts,
    pub last_updated: String,
}

/// Start the web server
pub async fn serve(db: Database, port: u16) -> Result<()> {
    let state = Arc::new(AppState { db });

    let app = Router::new()
        .route("/", get(dashboard_handler))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("Starting web server at http://localhost:{}", port);

    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

/// Dashboard handler - renders the main order view
async fn dashboard_handler(
    State(state): State<Arc<AppState>>,
) -> Html<String> {
    match build_dashboard(&state.db).await {
        Ok(template) => Html(template.render().unwrap_or_else(|e| {
            format!("<html><body><h1>Template error: {}</h1></body></html>", e)
        })),
        Err(e) => Html(format!(
            "<html><body><h1>Error loading dashboard</h1><p>{}</p></body></html>",
            e
        )),
    }
}

/// Build the dashboard data from the database (for Askama template)
async fn build_dashboard(db: &Database) -> Result<DashboardTemplate> {
    let data = get_dashboard_data(db).await?;

    Ok(DashboardTemplate {
        orders: data.orders,
        total_orders: data.total_orders,
        pending_emails: data.pending_emails,
        status_counts: data.status_counts,
        last_updated: data.last_updated,
    })
}

/// Get dashboard data - public API for Tauri and other consumers
/// When account_id is None, returns data for all accounts
pub async fn get_dashboard_data(db: &Database) -> Result<DashboardData> {
    get_dashboard_data_filtered(db, None).await
}

/// Get dashboard data filtered by account
pub async fn get_dashboard_data_filtered(
    db: &Database,
    account_id: Option<i64>,
) -> Result<DashboardData> {
    // Fetch accounts list
    let accounts = fetch_account_view_models(db).await?;

    // Fetch orders with their items (optionally filtered by account)
    let orders = fetch_orders_with_items_filtered(db, account_id).await?;

    // Get counts (optionally filtered by account)
    let total_orders = orders.len() as i64;
    let pending_emails = fetch_pending_email_count_filtered(db, account_id).await?;
    let status_counts = fetch_status_counts_filtered(db, account_id).await?;

    let last_updated = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();

    Ok(DashboardData {
        orders,
        total_orders,
        pending_emails,
        status_counts,
        last_updated,
        accounts,
        selected_account_id: account_id,
    })
}

/// Fetch all orders with their line items
pub async fn fetch_orders_with_items(db: &Database) -> Result<Vec<OrderViewModel>> {
    // Fetch orders
    let order_rows: Vec<(String, String, Option<f64>, String, Option<String>, Option<String>)> = sqlx::query_as(
        r#"
        SELECT id, order_date, total_cost, status, tracking_number, carrier
        FROM orders
        ORDER BY order_date DESC
        "#
    )
    .fetch_all(db.pool())
    .await?;

    let mut orders = Vec::new();

    for (id, order_date, total_cost, status, tracking_number, carrier) in order_rows {
        // Fetch items for this order
        let item_rows: Vec<(String, i32, String)> = sqlx::query_as(
            r#"
            SELECT name, quantity, status
            FROM line_items
            WHERE order_id = ?
            "#
        )
        .bind(&id)
        .fetch_all(db.pool())
        .await?;

        let items: Vec<ItemViewModel> = item_rows
            .into_iter()
            .map(|(name, quantity, status)| ItemViewModel {
                name,
                quantity: quantity as u32,
                status,
            })
            .collect();

        // Format the date for display
        let formatted_date = format_date(&order_date);

        // Format total cost
        let formatted_total = total_cost.map(|t| format!("{:.2}", t));

        orders.push(OrderViewModel {
            id,
            order_date: formatted_date,
            status,
            total_cost: formatted_total,
            items,
            tracking_number,
            carrier,
        });
    }

    Ok(orders)
}

/// Format ISO date to a readable format
fn format_date(iso_date: &str) -> String {
    chrono::DateTime::parse_from_rfc3339(iso_date)
        .map(|dt| dt.format("%b %d, %Y").to_string())
        .unwrap_or_else(|_| iso_date.to_string())
}

/// Fetch count of pending emails
pub async fn fetch_pending_email_count(db: &Database) -> Result<i64> {
    let (count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM raw_emails WHERE processing_status = 'pending'"
    )
    .fetch_one(db.pool())
    .await?;
    Ok(count)
}

/// Fetch counts by order status
pub async fn fetch_status_counts(db: &Database) -> Result<StatusCounts> {
    fetch_status_counts_filtered(db, None).await
}

// ==================== Account-Filtered Functions ====================

/// Fetch account view models with order counts
pub async fn fetch_account_view_models(db: &Database) -> Result<Vec<AccountViewModel>> {
    let rows: Vec<(i64, String, Option<String>, Option<String>)> = sqlx::query_as(
        r#"
        SELECT id, email, display_name, last_sync_at
        FROM accounts
        WHERE is_active = 1
        ORDER BY email
        "#
    )
    .fetch_all(db.pool())
    .await?;

    let mut accounts = Vec::new();

    for (id, email, display_name, last_sync_at) in rows {
        // Get order count for this account
        let (order_count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM orders WHERE account_id = ?"
        )
        .bind(id)
        .fetch_one(db.pool())
        .await?;

        accounts.push(AccountViewModel {
            id,
            email,
            display_name,
            order_count,
            last_sync_at,
        });
    }

    Ok(accounts)
}

/// Fetch orders filtered by account
pub async fn fetch_orders_with_items_filtered(
    db: &Database,
    account_id: Option<i64>,
) -> Result<Vec<OrderViewModel>> {
    // Build query based on whether we're filtering by account
    let order_rows: Vec<(String, String, Option<f64>, String, Option<String>, Option<String>)> = match account_id {
        Some(acc_id) => {
            sqlx::query_as(
                r#"
                SELECT id, order_date, total_cost, status, tracking_number, carrier
                FROM orders
                WHERE account_id = ?
                ORDER BY order_date DESC
                "#
            )
            .bind(acc_id)
            .fetch_all(db.pool())
            .await?
        }
        None => {
            sqlx::query_as(
                r#"
                SELECT id, order_date, total_cost, status, tracking_number, carrier
                FROM orders
                ORDER BY order_date DESC
                "#
            )
            .fetch_all(db.pool())
            .await?
        }
    };

    let mut orders = Vec::new();

    for (id, order_date, total_cost, status, tracking_number, carrier) in order_rows {
        // Fetch items for this order
        let item_rows: Vec<(String, i32, String)> = sqlx::query_as(
            r#"
            SELECT name, quantity, status
            FROM line_items
            WHERE order_id = ?
            "#
        )
        .bind(&id)
        .fetch_all(db.pool())
        .await?;

        let items: Vec<ItemViewModel> = item_rows
            .into_iter()
            .map(|(name, quantity, status)| ItemViewModel {
                name,
                quantity: quantity as u32,
                status,
            })
            .collect();

        // Format the date for display
        let formatted_date = format_date(&order_date);

        // Format total cost
        let formatted_total = total_cost.map(|t| format!("{:.2}", t));

        orders.push(OrderViewModel {
            id,
            order_date: formatted_date,
            status,
            total_cost: formatted_total,
            items,
            tracking_number,
            carrier,
        });
    }

    Ok(orders)
}

/// Fetch pending email count filtered by account
pub async fn fetch_pending_email_count_filtered(
    db: &Database,
    account_id: Option<i64>,
) -> Result<i64> {
    let (count,): (i64,) = match account_id {
        Some(acc_id) => {
            sqlx::query_as(
                "SELECT COUNT(*) FROM raw_emails WHERE processing_status = 'pending' AND account_id = ?"
            )
            .bind(acc_id)
            .fetch_one(db.pool())
            .await?
        }
        None => {
            sqlx::query_as(
                "SELECT COUNT(*) FROM raw_emails WHERE processing_status = 'pending'"
            )
            .fetch_one(db.pool())
            .await?
        }
    };
    Ok(count)
}

/// Fetch status counts filtered by account
pub async fn fetch_status_counts_filtered(
    db: &Database,
    account_id: Option<i64>,
) -> Result<StatusCounts> {
    let mut counts = StatusCounts::default();

    let rows: Vec<(String, i64)> = match account_id {
        Some(acc_id) => {
            sqlx::query_as(
                "SELECT status, COUNT(*) FROM orders WHERE account_id = ? GROUP BY status"
            )
            .bind(acc_id)
            .fetch_all(db.pool())
            .await?
        }
        None => {
            sqlx::query_as(
                "SELECT status, COUNT(*) FROM orders GROUP BY status"
            )
            .fetch_all(db.pool())
            .await?
        }
    };

    for (status, count) in rows {
        match status.as_str() {
            "confirmed" => counts.confirmed = count,
            "shipped" => counts.shipped = count,
            "delivered" => counts.delivered = count,
            "canceled" => counts.canceled = count,
            "partially_canceled" => counts.partially_canceled = count,
            _ => {}
        }
    }

    Ok(counts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_date() {
        let iso = "2024-01-15T00:00:00+00:00";
        let formatted = format_date(iso);
        assert_eq!(formatted, "Jan 15, 2024");
    }

    #[test]
    fn test_format_date_invalid() {
        let invalid = "not-a-date";
        let formatted = format_date(invalid);
        assert_eq!(formatted, "not-a-date");
    }
}
