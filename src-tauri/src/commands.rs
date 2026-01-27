//! Tauri IPC command handlers
//!
//! These commands are called from the frontend JavaScript via tauri.invoke()

use serde::Serialize;
use std::path::PathBuf;
use std::sync::Arc;
use tauri::State;
use tokio::sync::Mutex;
use walmart_dashboard::db::Database;
use walmart_dashboard::process;
use walmart_dashboard::tracking::{self, TrackingService};
use walmart_dashboard::web::{
    AccountViewModel, DashboardData, fetch_account_view_models, get_dashboard_data_with_dates,
};

/// Application state managed by Tauri
pub struct AppState {
    pub db: Arc<Mutex<Database>>,
    pub db_path: PathBuf,
    pub tracking_service: TrackingService,
}

/// Get dashboard data for the frontend
/// If account_id is provided, filters data to that account only
/// If start_date/end_date are provided (in "YYYY-MM-DD" format), filters by date range
#[tauri::command]
pub async fn get_dashboard(
    state: State<'_, AppState>,
    account_id: Option<i64>,
    start_date: Option<String>,
    end_date: Option<String>,
) -> Result<DashboardData, String> {
    let db = state.db.lock().await;

    get_dashboard_data_with_dates(
        &db,
        account_id,
        start_date.as_deref(),
        end_date.as_deref(),
    )
    .await
    .map_err(|e| e.to_string())
}

/// Refresh dashboard data (same as get_dashboard, provided for semantic clarity)
#[tauri::command]
pub async fn refresh_dashboard(
    state: State<'_, AppState>,
    account_id: Option<i64>,
    start_date: Option<String>,
    end_date: Option<String>,
) -> Result<DashboardData, String> {
    get_dashboard(state, account_id, start_date, end_date).await
}

/// List all configured accounts
#[tauri::command]
pub async fn list_accounts(
    state: State<'_, AppState>,
) -> Result<Vec<AccountViewModel>, String> {
    let db = state.db.lock().await;
    fetch_account_view_models(&db)
        .await
        .map_err(|e| e.to_string())
}

/// Get the database path
#[tauri::command]
pub async fn get_db_path(
    state: State<'_, AppState>,
) -> Result<String, String> {
    Ok(state.db_path.display().to_string())
}

/// Tracking status response for frontend
#[derive(Serialize)]
pub struct TrackingStatusResponse {
    pub tracking_number: String,
    pub carrier: String,
    pub state: String,
    pub state_display: String,
    pub state_description: Option<String>,
    pub is_delivered: bool,
    pub delivery_date: Option<String>,
    pub last_fetched_at: String,
    pub events: Vec<TrackingEventResponse>,
}

#[derive(Serialize)]
pub struct TrackingEventResponse {
    pub time: Option<String>,
    pub description: String,
    pub location: Option<String>,
}

/// Get cached tracking status for an order
#[tauri::command]
pub async fn get_tracking_status(
    state: State<'_, AppState>,
    order_id: String,
) -> Result<Option<TrackingStatusResponse>, String> {
    let db = state.db.lock().await;

    let tracking_list = tracking::get_tracking_for_order(&db, &order_id)
        .await
        .map_err(|e| e.to_string())?;

    if let Some(t) = tracking_list.first() {
        Ok(Some(TrackingStatusResponse {
            tracking_number: t.tracking_number.clone(),
            carrier: t.carrier.clone(),
            state: t.state.as_str().to_string(),
            state_display: t.state.display_name().to_string(),
            state_description: t.state_description.clone(),
            is_delivered: t.is_delivered,
            delivery_date: t.delivery_date.clone(),
            last_fetched_at: t.last_fetched_at.clone(),
            events: t.events.iter().take(5).map(|e| TrackingEventResponse {
                time: e.event_time_iso.clone().or_else(|| e.event_time.clone()),
                description: e.description.clone(),
                location: e.location.clone(),
            }).collect(),
        }))
    } else {
        Ok(None)
    }
}

/// Fetch fresh tracking status from 17track.net with automatic session recovery
#[tauri::command]
pub async fn fetch_tracking(
    state: State<'_, AppState>,
    order_id: String,
) -> Result<Option<TrackingStatusResponse>, String> {
    tracing::info!(
        order_id = %order_id,
        "Fetch tracking command invoked"
    );

    let db = state.db.lock().await;

    // Get order tracking info
    let order: Option<(Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT tracking_number, carrier FROM orders WHERE id = ?"
    )
    .bind(&order_id)
    .fetch_optional(db.pool())
    .await
    .map_err(|e| e.to_string())?;

    match order {
        Some((Some(tracking_number), Some(carrier))) => {
            tracing::debug!(
                order_id = %order_id,
                tracking_number = %tracking_number,
                carrier = %carrier,
                "Found order tracking info, fetching status with recovery"
            );

            // Use recovery method for resilience against session issues
            let result = state
                .tracking_service
                .get_tracking_status_with_recovery(&db, &tracking_number, &carrier, true)
                .await
                .map_err(|e| {
                    tracing::error!(
                        order_id = %order_id,
                        error = %e,
                        "Failed to fetch tracking after recovery attempts"
                    );
                    e.to_string()
                })?;

            tracing::info!(
                order_id = %order_id,
                state = result.state.as_str(),
                is_delivered = result.is_delivered,
                "Tracking fetch completed"
            );

            Ok(Some(TrackingStatusResponse {
                tracking_number: result.tracking_number,
                carrier: result.carrier,
                state: result.state.as_str().to_string(),
                state_display: result.state.display_name().to_string(),
                state_description: result.state_description,
                is_delivered: result.is_delivered,
                delivery_date: result.delivery_date,
                last_fetched_at: result.last_fetched_at,
                events: result.events.iter().take(5).map(|e| TrackingEventResponse {
                    time: e.event_time_iso.clone().or_else(|| e.event_time.clone()),
                    description: e.description.clone(),
                    location: e.location.clone(),
                }).collect(),
            }))
        }
        _ => {
            tracing::debug!(
                order_id = %order_id,
                "Order has no tracking information"
            );
            Ok(None)
        }
    }
}

/// Manually restart the Chrome tracking session.
/// Use this if tracking fetches are consistently failing due to session issues.
#[tauri::command]
pub async fn restart_tracking_session(
    state: State<'_, AppState>,
) -> Result<String, String> {
    tracing::info!("Manual session restart requested");

    state
        .tracking_service
        .restart_session()
        .await
        .map_err(|e| e.to_string())?;

    Ok("Tracking session restarted successfully".to_string())
}

// ==================== Process Commands ====================

/// Result from processing pending emails
#[derive(Serialize)]
pub struct ProcessResult {
    pub success: bool,
    pub pending: usize,
    pub processed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub message: String,
}

/// Process pending emails into orders
/// Use this after syncing emails via CLI to update orders
#[tauri::command]
pub async fn process_emails(
    state: State<'_, AppState>,
) -> Result<ProcessResult, String> {
    let db = state.db.lock().await;

    let stats = process::process_pending_events(&db)
        .await
        .map_err(|e| e.to_string())?;

    Ok(ProcessResult {
        success: true,
        pending: stats.total_pending,
        processed: stats.processed,
        failed: stats.failed,
        skipped: stats.skipped,
        message: format!(
            "Processed {} of {} pending emails ({} failed, {} skipped)",
            stats.processed, stats.total_pending, stats.failed, stats.skipped
        ),
    })
}
