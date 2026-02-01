//! Web dashboard module
//!
//! Provides view models and data-fetching functions for the dashboard.
//! Used by the Tauri desktop application.

use crate::db::Database;
use crate::images::image_id_for_url;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// View model for an order in the dashboard
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderViewModel {
    pub id: String,
    pub order_date: String,
    /// Raw ISO 8601 date for reliable sorting (order_date is formatted for display)
    pub order_date_raw: String,
    pub shipped_date: Option<String>,
    pub status: String,
    pub total_cost: Option<String>,
    pub items: Vec<ItemViewModel>,
    pub tracking_number: Option<String>,
    pub carrier: Option<String>,
    pub recipient: Option<String>,
    pub thumbnail_id: Option<String>,
    pub thumbnail_url: Option<String>,
    pub cancel_reason: Option<String>,
}

/// View model for a line item
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemViewModel {
    pub name: String,
    pub quantity: u32,
    pub status: String,
    pub image_id: Option<String>,
    pub image_url: Option<String>,
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

/// Paginated response wrapper for orders
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginatedOrders {
    pub orders: Vec<OrderViewModel>,
    pub has_more: bool,
    pub next_cursor: Option<String>,
    pub total_count: i64,
}

/// Enhanced dashboard data with pagination support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardDataV2 {
    pub paginated_orders: PaginatedOrders,
    pub status_counts: StatusCounts,
    pub accounts: Vec<AccountViewModel>,
    pub selected_account_id: Option<i64>,
    pub pending_emails: i64,
    pub last_updated: String,
}

/// Aggregate statistics calculated efficiently without fetching full order data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateStats {
    pub total_orders: i64,
    pub total_spent: f64,
    pub avg_order: f64,
    pub total_quantity: i64,
    pub orders_this_week: i64,
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
        "SELECT order_id, name, quantity, status, image_url FROM line_items WHERE order_id IN ({})",
        placeholders
    );

    let mut q = sqlx::query_as::<_, (String, String, i32, String, Option<String>)>(&query);
    for id in order_ids {
        q = q.bind(*id);
    }

    let item_rows = q.fetch_all(db.pool()).await?;

    let mut items_by_order: HashMap<String, Vec<ItemViewModel>> = HashMap::with_capacity(order_ids.len());
    for (order_id, name, quantity, status, image_url) in item_rows {
        let image_id = image_url.as_ref().map(|url| image_id_for_url(url));

        items_by_order.entry(order_id).or_default().push(ItemViewModel {
            name,
            quantity: quantity as u32,
            status,
            image_id,
            image_url,
        });
    }

    Ok(items_by_order)
}

/// Build OrderViewModels from raw order rows + pre-fetched items map
fn build_order_view_models(
    order_rows: Vec<(String, String, Option<String>, Option<f64>, String, Option<String>, Option<String>, Option<String>, Option<String>)>,
    mut items_by_order: HashMap<String, Vec<ItemViewModel>>,
) -> Vec<OrderViewModel> {
    let mut orders = Vec::with_capacity(order_rows.len());

    for (id, order_date, shipped_date, total_cost, status, tracking_number, carrier, recipient, cancel_reason) in order_rows {
        let items = items_by_order.remove(&id).unwrap_or_default();
        let (thumbnail_id, thumbnail_url) = items
            .iter()
            .find_map(|item| {
                if item.image_id.is_some() || item.image_url.is_some() {
                    Some((item.image_id.clone(), item.image_url.clone()))
                } else {
                    None
                }
            })
            .unwrap_or((None, None));

        // Use the "effective date" — same logic as displayDate() in JS and the SQL queries:
        // shipped_date for shipped/delivered orders, order_date otherwise.
        let effective_date = if status == "shipped" || status == "delivered" {
            shipped_date.as_deref().unwrap_or(&order_date)
        } else {
            &order_date
        };

        let formatted_date = format_date(effective_date);
        let sortable_date = normalize_date_for_sorting(effective_date);
        let formatted_total = total_cost.map(|t| format!("{:.2}", t));

        orders.push(OrderViewModel {
            id,
            order_date: formatted_date,
            order_date_raw: sortable_date,
            shipped_date: shipped_date.as_deref().map(format_date),
            status,
            total_cost: formatted_total,
            items,
            tracking_number,
            carrier,
            recipient,
            thumbnail_id,
            thumbnail_url,
            cancel_reason,
        });
    }

    orders
}

/// Fetch all orders with their line items (2 queries instead of N+1)
pub async fn fetch_orders_with_items(db: &Database) -> Result<Vec<OrderViewModel>> {
    let order_rows: Vec<(String, String, Option<String>, Option<f64>, String, Option<String>, Option<String>, Option<String>, Option<String>)> = sqlx::query_as(
        r#"
        SELECT id, order_date, shipped_date, total_cost, status, tracking_number, carrier, recipient, cancel_reason
        FROM orders
        ORDER BY COALESCE(CASE WHEN status IN ('shipped','delivered') THEN shipped_date END, order_date) DESC
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

/// Ensure a date string is in ISO 8601 format for reliable sorting.
/// If already ISO, returns as-is. If in display format ("Jul 18, 2025"),
/// converts to "2025-07-18T00:00:00Z".
fn normalize_date_for_sorting(date_str: &str) -> String {
    // Already ISO — pass through
    if date_str.starts_with("20") {
        return date_str.to_string();
    }
    // Try display formats
    let formats = ["%b %d, %Y", "%B %d, %Y", "%m/%d/%Y"];
    for fmt in &formats {
        if let Ok(parsed) = chrono::NaiveDate::parse_from_str(date_str.trim(), fmt) {
            return format!("{}T00:00:00Z", parsed);
        }
    }
    // Unrecognized — return as-is
    date_str.to_string()
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

    // Use the "effective date" — the same date the frontend displays:
    //   shipped_date for shipped/delivered orders, order_date otherwise.
    // This prevents orders with a wrong order_date (e.g. Utc::now() fallback)
    // from leaking through date filters when shipped_date is correct.
    const EFF_DATE: &str =
        "COALESCE(CASE WHEN status IN ('shipped','delivered') THEN shipped_date END, order_date)";

    // Build WHERE clause and bind values dynamically
    let mut conditions: Vec<String> = Vec::new();

    if account_id.is_some() {
        conditions.push("account_id = ?".to_string());
    }
    if start_date.is_some() {
        conditions.push(format!("substr({}, 1, 10) >= ?", EFF_DATE));
    }
    if end_date.is_some() {
        conditions.push(format!("substr({}, 1, 10) <= ?", EFF_DATE));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let sql = format!(
        "SELECT id, order_date, shipped_date, total_cost, status, tracking_number, carrier, recipient, cancel_reason \
         FROM orders {} ORDER BY {} DESC",
        where_clause, EFF_DATE
    );

    let mut query = sqlx::query_as::<_, (String, String, Option<String>, Option<f64>, String, Option<String>, Option<String>, Option<String>, Option<String>)>(&sql);

    if let Some(acc_id) = account_id {
        query = query.bind(acc_id);
    }
    if let Some(start) = start_date {
        query = query.bind(start);
    }
    if let Some(end) = end_date {
        query = query.bind(end);
    }

    let order_rows = query.fetch_all(db.pool()).await?;

    tracing::info!("Dashboard query returned {} orders", order_rows.len());

    let order_ids: Vec<&str> = order_rows.iter().map(|(id, ..)| id.as_str()).collect();
    let items_by_order = fetch_items_for_orders(db, &order_ids).await?;

    Ok(build_order_view_models(order_rows, items_by_order))
}

/// Fetch orders with cursor-based pagination
/// More efficient than OFFSET for large datasets - uses index seeks instead of table scans
pub async fn fetch_orders_paginated(
    db: &Database,
    account_id: Option<i64>,
    start_date: Option<&str>,
    end_date: Option<&str>,
    status_filter: Option<&str>,
    cursor: Option<&str>,
    limit: usize,
) -> Result<PaginatedOrders> {
    tracing::debug!(
        "fetch_orders_paginated: account_id={:?}, status={:?}, cursor={:?}, limit={}",
        account_id, status_filter, cursor, limit
    );

    const EFF_DATE: &str =
        "COALESCE(CASE WHEN status IN ('shipped','delivered') THEN shipped_date END, order_date)";

    let mut conditions: Vec<String> = Vec::new();

    if account_id.is_some() {
        conditions.push("account_id = ?".to_string());
    }
    if start_date.is_some() {
        conditions.push(format!("substr({}, 1, 10) >= ?", EFF_DATE));
    }
    if end_date.is_some() {
        conditions.push(format!("substr({}, 1, 10) <= ?", EFF_DATE));
    }
    if let Some(status) = status_filter {
        if status != "all" {
            conditions.push("status = ?".to_string());
        }
    }

    // Cursor-based pagination: fetch orders AFTER cursor (effective_date < cursor_date OR (same date AND id > cursor_id))
    if cursor.is_some() {
        conditions.push(format!(
            "({} < (SELECT {} FROM orders WHERE id = ?) OR \
             ({} = (SELECT {} FROM orders WHERE id = ?) AND id > ?))",
            EFF_DATE, EFF_DATE, EFF_DATE, EFF_DATE
        ));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    // Fetch limit + 1 to detect if there are more results
    let sql = format!(
        "SELECT id, order_date, shipped_date, total_cost, status, \
         tracking_number, carrier, recipient, cancel_reason \
         FROM orders {} ORDER BY {} DESC, id ASC LIMIT ?",
        where_clause, EFF_DATE
    );

    let mut query = sqlx::query_as::<_, (String, String, Option<String>, Option<f64>, String, Option<String>, Option<String>, Option<String>, Option<String>)>(&sql);

    // Bind parameters in order
    if let Some(acc_id) = account_id {
        query = query.bind(acc_id);
    }
    if let Some(start) = start_date {
        query = query.bind(start);
    }
    if let Some(end) = end_date {
        query = query.bind(end);
    }
    if let Some(status) = status_filter {
        if status != "all" {
            query = query.bind(status);
        }
    }
    if let Some(cursor_id) = cursor {
        query = query.bind(cursor_id).bind(cursor_id).bind(cursor_id);
    }
    query = query.bind((limit + 1) as i64);

    let mut order_rows = query.fetch_all(db.pool()).await?;

    // Check if there are more results
    let has_more = order_rows.len() > limit;
    if has_more {
        order_rows.truncate(limit);
    }

    let next_cursor = order_rows.last().map(|(id, ..)| id.clone());

    tracing::debug!(
        "Fetched {} orders, has_more={}, next_cursor={:?}",
        order_rows.len(),
        has_more,
        next_cursor
    );

    // Fetch total count with same filters (excluding cursor for total count)
    let mut count_conditions: Vec<String> = Vec::new();
    if account_id.is_some() {
        count_conditions.push("account_id = ?".to_string());
    }
    if start_date.is_some() {
        count_conditions.push(format!("substr({}, 1, 10) >= ?", EFF_DATE));
    }
    if end_date.is_some() {
        count_conditions.push(format!("substr({}, 1, 10) <= ?", EFF_DATE));
    }
    if let Some(status) = status_filter {
        if status != "all" {
            count_conditions.push("status = ?".to_string());
        }
    }

    let count_where = if count_conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", count_conditions.join(" AND "))
    };

    let count_sql = format!("SELECT COUNT(*) FROM orders {}", count_where);
    let mut count_query = sqlx::query_scalar::<_, i64>(&count_sql);

    // Bind parameters for count query
    if let Some(acc_id) = account_id {
        count_query = count_query.bind(acc_id);
    }
    if let Some(start) = start_date {
        count_query = count_query.bind(start);
    }
    if let Some(end) = end_date {
        count_query = count_query.bind(end);
    }
    if let Some(status) = status_filter {
        if status != "all" {
            count_query = count_query.bind(status);
        }
    }

    let total_count = count_query.fetch_one(db.pool()).await?;

    // Fetch items for these orders
    let order_ids: Vec<&str> = order_rows.iter().map(|(id, ..)| id.as_str()).collect();
    let items_by_order = fetch_items_for_orders(db, &order_ids).await?;

    Ok(PaginatedOrders {
        orders: build_order_view_models(order_rows, items_by_order),
        has_more,
        next_cursor,
        total_count,
    })
}

/// Search orders by query string (searches order ID, recipient, and item names)
/// Returns paginated results with the same filters as fetch_orders_paginated
pub async fn search_orders(
    db: &Database,
    query: &str,
    account_id: Option<i64>,
    start_date: Option<&str>,
    end_date: Option<&str>,
    status_filter: Option<&str>,
    limit: usize,
) -> Result<Vec<OrderViewModel>> {
    tracing::debug!(
        "search_orders: query='{}', account_id={:?}, status={:?}, limit={}",
        query, account_id, status_filter, limit
    );

    if query.trim().is_empty() {
        return Ok(Vec::new());
    }

    let search_pattern = format!("%{}%", query.trim());

    const EFF_DATE: &str =
        "COALESCE(CASE WHEN status IN ('shipped','delivered') THEN shipped_date END, order_date)";

    let mut conditions: Vec<String> = Vec::new();

    // Search condition: order ID, recipient, total cost, or item name
    conditions.push(
        "(orders.id LIKE ? OR orders.recipient LIKE ? OR \
         CAST(orders.total_cost AS TEXT) LIKE ? OR \
         EXISTS (SELECT 1 FROM line_items WHERE line_items.order_id = orders.id AND line_items.name LIKE ?))".to_string()
    );

    if account_id.is_some() {
        conditions.push("account_id = ?".to_string());
    }
    if start_date.is_some() {
        conditions.push(format!("substr({}, 1, 10) >= ?", EFF_DATE));
    }
    if end_date.is_some() {
        conditions.push(format!("substr({}, 1, 10) <= ?", EFF_DATE));
    }
    if let Some(status) = status_filter {
        if status != "all" {
            conditions.push("status = ?".to_string());
        }
    }

    let where_clause = format!("WHERE {}", conditions.join(" AND "));

    let sql = format!(
        "SELECT DISTINCT id, order_date, shipped_date, total_cost, status, \
         tracking_number, carrier, recipient, cancel_reason \
         FROM orders {} ORDER BY {} DESC, id ASC LIMIT ?",
        where_clause, EFF_DATE
    );

    let mut query_builder = sqlx::query_as::<_, (String, String, Option<String>, Option<f64>, String, Option<String>, Option<String>, Option<String>, Option<String>)>(&sql);

    // Bind search pattern four times (for id, recipient, total_cost, and item name)
    query_builder = query_builder.bind(&search_pattern).bind(&search_pattern).bind(&search_pattern).bind(&search_pattern);

    // Bind other parameters in order
    if let Some(acc_id) = account_id {
        query_builder = query_builder.bind(acc_id);
    }
    if let Some(start) = start_date {
        query_builder = query_builder.bind(start);
    }
    if let Some(end) = end_date {
        query_builder = query_builder.bind(end);
    }
    if let Some(status) = status_filter {
        if status != "all" {
            query_builder = query_builder.bind(status);
        }
    }
    query_builder = query_builder.bind(limit as i64);

    let order_rows = query_builder.fetch_all(db.pool()).await?;

    tracing::debug!("Search found {} matching orders", order_rows.len());

    // Fetch items for these orders
    let order_ids: Vec<&str> = order_rows.iter().map(|(id, ..)| id.as_str()).collect();
    let items_by_order = fetch_items_for_orders(db, &order_ids).await?;

    Ok(build_order_view_models(order_rows, items_by_order))
}

/// Fetch aggregate statistics efficiently without loading full order data
/// Calculates totals, averages, and counts using database aggregation
pub async fn fetch_aggregate_stats(
    db: &Database,
    account_id: Option<i64>,
    start_date: Option<&str>,
    end_date: Option<&str>,
) -> Result<AggregateStats> {
    tracing::debug!(
        "fetch_aggregate_stats: account_id={:?}, start_date={:?}, end_date={:?}",
        account_id, start_date, end_date
    );

    const EFF_DATE: &str =
        "COALESCE(CASE WHEN status IN ('shipped','delivered') THEN shipped_date END, order_date)";

    let mut conditions: Vec<String> = vec!["status != 'canceled'".to_string()];

    if account_id.is_some() {
        conditions.push("account_id = ?".to_string());
    }
    if start_date.is_some() {
        conditions.push(format!("substr({}, 1, 10) >= ?", EFF_DATE));
    }
    if end_date.is_some() {
        conditions.push(format!("substr({}, 1, 10) <= ?", EFF_DATE));
    }

    let where_clause = format!("WHERE {}", conditions.join(" AND "));

    // Lightweight aggregation query - no full data transfer
    let sql = format!(
        "SELECT
            COUNT(*) as total_orders,
            COALESCE(SUM(total_cost), 0) as total_spent,
            COALESCE(AVG(total_cost), 0) as avg_order,
            (SELECT COALESCE(SUM(quantity), 0) FROM line_items
             WHERE order_id IN (SELECT id FROM orders {})) as total_quantity,
            COUNT(CASE WHEN date({}) >= date('now', '-7 days') THEN 1 END) as orders_this_week
         FROM orders {}",
        where_clause, EFF_DATE, where_clause
    );

    let mut query = sqlx::query_as::<_, (i64, f64, f64, i64, i64)>(&sql);

    // Bind parameters
    if let Some(acc_id) = account_id {
        query = query.bind(acc_id);
    }
    if let Some(start) = start_date {
        query = query.bind(start);
    }
    if let Some(end) = end_date {
        query = query.bind(end);
    }

    // Bind again for the subquery
    if let Some(acc_id) = account_id {
        query = query.bind(acc_id);
    }
    if let Some(start) = start_date {
        query = query.bind(start);
    }
    if let Some(end) = end_date {
        query = query.bind(end);
    }

    let (total_orders, total_spent, avg_order, total_quantity, orders_this_week) =
        query.fetch_one(db.pool()).await?;

    tracing::debug!(
        "Aggregate stats: total_orders={}, total_spent={:.2}, avg_order={:.2}",
        total_orders, total_spent, avg_order
    );

    Ok(AggregateStats {
        total_orders,
        total_spent,
        avg_order,
        total_quantity,
        orders_this_week,
    })
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

    // Same effective date expression as fetch_orders_with_items_and_dates
    const EFF_DATE: &str =
        "COALESCE(CASE WHEN status IN ('shipped','delivered') THEN shipped_date END, order_date)";

    let mut conditions: Vec<String> = Vec::new();

    if account_id.is_some() {
        conditions.push("account_id = ?".to_string());
    }
    if start_date.is_some() {
        conditions.push(format!("substr({}, 1, 10) >= ?", EFF_DATE));
    }
    if end_date.is_some() {
        conditions.push(format!("substr({}, 1, 10) <= ?", EFF_DATE));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let sql = format!(
        "SELECT status, COUNT(*) FROM orders {} GROUP BY status",
        where_clause
    );

    let mut query = sqlx::query_as::<_, (String, i64)>(&sql);

    if let Some(acc_id) = account_id {
        query = query.bind(acc_id);
    }
    if let Some(start) = start_date {
        query = query.bind(start);
    }
    if let Some(end) = end_date {
        query = query.bind(end);
    }

    let rows = query.fetch_all(db.pool()).await?;

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
