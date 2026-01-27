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
use std::collections::HashMap;
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
    pub shipped_date: Option<String>,
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
    pub profile_picture_url: Option<String>,
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
    get_dashboard_data_with_dates(db, account_id, None, None).await
}

/// Get dashboard data filtered by account and date range
/// start_date and end_date should be in ISO format "YYYY-MM-DD"
pub async fn get_dashboard_data_with_dates(
    db: &Database,
    account_id: Option<i64>,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<DashboardData> {
    // Fetch accounts list
    let accounts = fetch_account_view_models(db).await?;

    // Fetch orders with their items (optionally filtered by account and date)
    let orders = fetch_orders_with_items_and_dates(db, account_id, start_date, end_date).await?;

    // Get counts (optionally filtered by account and date)
    let total_orders = orders.len() as i64;
    let pending_emails = fetch_pending_email_count_filtered(db, account_id).await?;
    let status_counts = fetch_status_counts_with_dates(db, account_id, start_date, end_date).await?;

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

/// Batch-fetch all line items for a set of order IDs, grouped by order_id.
/// Uses a single query instead of N individual queries.
async fn fetch_items_for_orders(db: &Database, order_ids: &[&str]) -> Result<HashMap<String, Vec<ItemViewModel>>> {
    if order_ids.is_empty() {
        return Ok(HashMap::new());
    }

    // Build a single query with IN clause
    let placeholders: String = order_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let query = format!(
        "SELECT order_id, name, quantity, status FROM line_items WHERE order_id IN ({})",
        placeholders
    );

    let mut q = sqlx::query_as::<_, (String, String, i32, String)>(&query);
    for id in order_ids {
        q = q.bind(*id);
    }

    let item_rows = q.fetch_all(db.pool()).await?;

    let mut items_by_order: HashMap<String, Vec<ItemViewModel>> = HashMap::with_capacity(order_ids.len());
    for (order_id, name, quantity, status) in item_rows {
        items_by_order.entry(order_id).or_default().push(ItemViewModel {
            name,
            quantity: quantity as u32,
            status,
        });
    }

    Ok(items_by_order)
}

/// Build OrderViewModels from raw order rows + pre-fetched items map
fn build_order_view_models(
    order_rows: Vec<(String, String, Option<String>, Option<f64>, String, Option<String>, Option<String>)>,
    mut items_by_order: HashMap<String, Vec<ItemViewModel>>,
) -> Vec<OrderViewModel> {
    let mut orders = Vec::with_capacity(order_rows.len());

    for (id, order_date, shipped_date, total_cost, status, tracking_number, carrier) in order_rows {
        let items = items_by_order.remove(&id).unwrap_or_default();
        let formatted_date = format_date(&order_date);
        let formatted_total = total_cost.map(|t| format!("{:.2}", t));

        orders.push(OrderViewModel {
            id,
            order_date: formatted_date,
            shipped_date: shipped_date.as_deref().map(format_date),
            status,
            total_cost: formatted_total,
            items,
            tracking_number,
            carrier,
        });
    }

    orders
}

/// Fetch all orders with their line items (2 queries instead of N+1)
pub async fn fetch_orders_with_items(db: &Database) -> Result<Vec<OrderViewModel>> {
    let order_rows: Vec<(String, String, Option<String>, Option<f64>, String, Option<String>, Option<String>)> = sqlx::query_as(
        r#"
        SELECT id, order_date, shipped_date, total_cost, status, tracking_number, carrier
        FROM orders
        ORDER BY order_date DESC
        "#
    )
    .fetch_all(db.pool())
    .await?;

    let order_ids: Vec<&str> = order_rows.iter().map(|(id, ..)| id.as_str()).collect();
    let items_by_order = fetch_items_for_orders(db, &order_ids).await?;

    Ok(build_order_view_models(order_rows, items_by_order))
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

/// Fetch account view models with order counts (single query with LEFT JOIN)
pub async fn fetch_account_view_models(db: &Database) -> Result<Vec<AccountViewModel>> {
    let rows: Vec<(i64, String, Option<String>, Option<String>, Option<String>, i64)> = sqlx::query_as(
        r#"
        SELECT a.id, a.email, a.display_name, a.profile_picture_url, a.last_sync_at,
               COUNT(o.id) as order_count
        FROM accounts a
        LEFT JOIN orders o ON a.id = o.account_id
        WHERE a.is_active = 1
        GROUP BY a.id
        ORDER BY a.email
        "#
    )
    .fetch_all(db.pool())
    .await?;

    Ok(rows
        .into_iter()
        .map(|(id, email, display_name, profile_picture_url, last_sync_at, order_count)| {
            AccountViewModel {
                id,
                email,
                display_name,
                profile_picture_url,
                order_count,
                last_sync_at,
            }
        })
        .collect())
}

/// Fetch orders filtered by account
pub async fn fetch_orders_with_items_filtered(
    db: &Database,
    account_id: Option<i64>,
) -> Result<Vec<OrderViewModel>> {
    fetch_orders_with_items_and_dates(db, account_id, None, None).await
}

/// Fetch orders filtered by account and date range
/// start_date and end_date should be in ISO format "YYYY-MM-DD"
pub async fn fetch_orders_with_items_and_dates(
    db: &Database,
    account_id: Option<i64>,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<Vec<OrderViewModel>> {
    tracing::debug!(
        "fetch_orders_with_items_and_dates: account_id={:?}, start_date={:?}, end_date={:?}",
        account_id, start_date, end_date
    );
    // Build query based on filters
    let order_rows: Vec<(String, String, Option<String>, Option<f64>, String, Option<String>, Option<String>)> =
        match (account_id, start_date, end_date) {
            (Some(acc_id), Some(start), Some(end)) => {
                sqlx::query_as(
                    r#"
                    SELECT id, order_date, shipped_date, total_cost, status, tracking_number, carrier
                    FROM orders
                    WHERE account_id = ? AND substr(order_date, 1, 10) >= ? AND substr(order_date, 1, 10) <= ?
                    ORDER BY order_date DESC
                    "#
                )
                .bind(acc_id)
                .bind(start)
                .bind(end)
                .fetch_all(db.pool())
                .await?
            }
            (Some(acc_id), Some(start), None) => {
                sqlx::query_as(
                    r#"
                    SELECT id, order_date, shipped_date, total_cost, status, tracking_number, carrier
                    FROM orders
                    WHERE account_id = ? AND substr(order_date, 1, 10) >= ?
                    ORDER BY order_date DESC
                    "#
                )
                .bind(acc_id)
                .bind(start)
                .fetch_all(db.pool())
                .await?
            }
            (Some(acc_id), None, Some(end)) => {
                sqlx::query_as(
                    r#"
                    SELECT id, order_date, shipped_date, total_cost, status, tracking_number, carrier
                    FROM orders
                    WHERE account_id = ? AND substr(order_date, 1, 10) <= ?
                    ORDER BY order_date DESC
                    "#
                )
                .bind(acc_id)
                .bind(end)
                .fetch_all(db.pool())
                .await?
            }
            (Some(acc_id), None, None) => {
                sqlx::query_as(
                    r#"
                    SELECT id, order_date, shipped_date, total_cost, status, tracking_number, carrier
                    FROM orders
                    WHERE account_id = ?
                    ORDER BY order_date DESC
                    "#
                )
                .bind(acc_id)
                .fetch_all(db.pool())
                .await?
            }
            (None, Some(start), Some(end)) => {
                tracing::debug!("Using date-filtered query: {} to {}", start, end);
                sqlx::query_as(
                    r#"
                    SELECT id, order_date, shipped_date, total_cost, status, tracking_number, carrier
                    FROM orders
                    WHERE substr(order_date, 1, 10) >= ? AND substr(order_date, 1, 10) <= ?
                    ORDER BY order_date DESC
                    "#
                )
                .bind(start)
                .bind(end)
                .fetch_all(db.pool())
                .await?
            }
            (None, Some(start), None) => {
                sqlx::query_as(
                    r#"
                    SELECT id, order_date, shipped_date, total_cost, status, tracking_number, carrier
                    FROM orders
                    WHERE substr(order_date, 1, 10) >= ?
                    ORDER BY order_date DESC
                    "#
                )
                .bind(start)
                .fetch_all(db.pool())
                .await?
            }
            (None, None, Some(end)) => {
                sqlx::query_as(
                    r#"
                    SELECT id, order_date, shipped_date, total_cost, status, tracking_number, carrier
                    FROM orders
                    WHERE substr(order_date, 1, 10) <= ?
                    ORDER BY order_date DESC
                    "#
                )
                .bind(end)
                .fetch_all(db.pool())
                .await?
            }
            (None, None, None) => {
                tracing::debug!("Using unfiltered query (no date range)");
                sqlx::query_as(
                    r#"
                    SELECT id, order_date, shipped_date, total_cost, status, tracking_number, carrier
                    FROM orders
                    ORDER BY order_date DESC
                    "#
                )
                .fetch_all(db.pool())
                .await?
            }
        };

    tracing::debug!("Query returned {} orders", order_rows.len());

    let order_ids: Vec<&str> = order_rows.iter().map(|(id, ..)| id.as_str()).collect();
    let items_by_order = fetch_items_for_orders(db, &order_ids).await?;

    Ok(build_order_view_models(order_rows, items_by_order))
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
    fetch_status_counts_with_dates(db, account_id, None, None).await
}

/// Fetch status counts filtered by account and date range
pub async fn fetch_status_counts_with_dates(
    db: &Database,
    account_id: Option<i64>,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<StatusCounts> {
    let mut counts = StatusCounts::default();

    // Build dynamic query based on filters
    let rows: Vec<(String, i64)> = match (account_id, start_date, end_date) {
        (Some(acc_id), Some(start), Some(end)) => {
            sqlx::query_as(
                "SELECT status, COUNT(*) FROM orders WHERE account_id = ? AND substr(order_date, 1, 10) >= ? AND substr(order_date, 1, 10) <= ? GROUP BY status"
            )
            .bind(acc_id)
            .bind(start)
            .bind(end)
            .fetch_all(db.pool())
            .await?
        }
        (Some(acc_id), Some(start), None) => {
            sqlx::query_as(
                "SELECT status, COUNT(*) FROM orders WHERE account_id = ? AND substr(order_date, 1, 10) >= ? GROUP BY status"
            )
            .bind(acc_id)
            .bind(start)
            .fetch_all(db.pool())
            .await?
        }
        (Some(acc_id), None, Some(end)) => {
            sqlx::query_as(
                "SELECT status, COUNT(*) FROM orders WHERE account_id = ? AND substr(order_date, 1, 10) <= ? GROUP BY status"
            )
            .bind(acc_id)
            .bind(end)
            .fetch_all(db.pool())
            .await?
        }
        (Some(acc_id), None, None) => {
            sqlx::query_as(
                "SELECT status, COUNT(*) FROM orders WHERE account_id = ? GROUP BY status"
            )
            .bind(acc_id)
            .fetch_all(db.pool())
            .await?
        }
        (None, Some(start), Some(end)) => {
            sqlx::query_as(
                "SELECT status, COUNT(*) FROM orders WHERE substr(order_date, 1, 10) >= ? AND substr(order_date, 1, 10) <= ? GROUP BY status"
            )
            .bind(start)
            .bind(end)
            .fetch_all(db.pool())
            .await?
        }
        (None, Some(start), None) => {
            sqlx::query_as(
                "SELECT status, COUNT(*) FROM orders WHERE substr(order_date, 1, 10) >= ? GROUP BY status"
            )
            .bind(start)
            .fetch_all(db.pool())
            .await?
        }
        (None, None, Some(end)) => {
            sqlx::query_as(
                "SELECT status, COUNT(*) FROM orders WHERE substr(order_date, 1, 10) <= ? GROUP BY status"
            )
            .bind(end)
            .fetch_all(db.pool())
            .await?
        }
        (None, None, None) => {
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
