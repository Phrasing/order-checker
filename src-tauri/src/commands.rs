//! Tauri IPC command handlers
//!
//! These commands are called from the frontend JavaScript via tauri.invoke()

use serde::Serialize;
use std::path::PathBuf;
use std::sync::Arc;
use tauri::State;
use walmart_dashboard::auth::{AccountAuth, get_gmail_client_for_account, fetch_profile_picture_url};
use walmart_dashboard::db::Database;
use walmart_dashboard::ingestion;
use walmart_dashboard::process;
use walmart_dashboard::tracking::{self, TrackingService};
use walmart_dashboard::web::{
    AccountViewModel, DashboardData, fetch_account_view_models, get_dashboard_data_with_dates,
};

/// Application state managed by Tauri
pub struct AppState {
    pub db: Arc<Database>,
    pub db_path: PathBuf,
    pub client_secret_path: PathBuf,
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
    get_dashboard_data_with_dates(
        &state.db,
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
    fetch_account_view_models(&state.db)
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
    let tracking_list = tracking::get_tracking_for_order(&state.db, &order_id)
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

    // Get order tracking info
    let order: Option<(Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT tracking_number, carrier FROM orders WHERE id = ?"
    )
    .bind(&order_id)
    .fetch_optional(state.db.pool())
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
                .get_tracking_status_with_recovery(&state.db, &tracking_number, &carrier, true)
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
    let stats = process::process_pending_events(&state.db)
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

// ==================== Sync Commands ====================

/// Result from syncing and processing orders
#[derive(Serialize)]
pub struct SyncResult {
    pub success: bool,
    pub emails_synced: usize,
    pub orders_processed: usize,
    pub errors: Vec<String>,
    pub message: String,
}

/// Internal sync+process logic (separated from tauri command to avoid State lifetime issues)
async fn perform_sync_and_process(
    db: Arc<Database>,
    db_path: PathBuf,
    client_secret_path: PathBuf,
    tracking_service: TrackingService,
) -> Result<SyncResult, String> {
    let mut errors = Vec::new();
    let mut total_synced = 0usize;

    // Resolve the base directory for token cache files (same directory as the database)
    let base_dir = db_path.parent().unwrap_or_else(|| std::path::Path::new("."));

    // Get all active accounts
    let accounts = db.list_accounts().await.map_err(|e| e.to_string())?;

    if accounts.is_empty() {
        return Ok(SyncResult {
            success: false,
            emails_synced: 0,
            orders_processed: 0,
            errors: vec!["No accounts configured. Use CLI to add an account.".to_string()],
            message: "No accounts configured".to_string(),
        });
    }

    // Sync each account
    for acc in &accounts {
        if !acc.is_active {
            continue;
        }

        tracing::info!(email = %acc.email, "Syncing emails for account");

        // Resolve token cache path relative to the database directory
        // (tokens are created by CLI in the project root alongside orders.db)
        let token_path = PathBuf::from(&acc.token_cache_path);
        let resolved_token_path = if token_path.is_absolute() {
            token_path
        } else {
            base_dir.join(token_path)
        };

        tracing::debug!(
            email = %acc.email,
            token_path = %resolved_token_path.display(),
            "Resolved token cache path"
        );

        // Create AccountAuth from resolved token path
        let account_auth = AccountAuth::with_path(
            &acc.email,
            resolved_token_path,
        );

        // Get Gmail client for this account
        match get_gmail_client_for_account(&client_secret_path, &account_auth).await {
            Ok(gmail_client) => {
                // Sync emails from last 5 days
                match ingestion::sync_emails_with_days_and_account(
                    &db,
                    gmail_client,
                    5,
                    acc.id,
                ).await {
                    Ok(stats) => {
                        total_synced += stats.synced;
                        tracing::info!(
                            email = %acc.email,
                            synced = stats.synced,
                            skipped = stats.skipped,
                            "Sync completed for account"
                        );

                        // Update last_sync_at timestamp
                        if let Err(e) = db.update_account_last_sync(acc.id).await {
                            tracing::warn!(email = %acc.email, error = %e, "Failed to update last_sync_at");
                        }

                        // Refresh profile picture URL
                        match fetch_profile_picture_url(&client_secret_path, &account_auth).await {
                            Ok(pic_url) => {
                                if let Err(e) = db.update_account_profile_picture(acc.id, pic_url.as_deref()).await {
                                    tracing::warn!(email = %acc.email, error = %e, "Failed to save profile picture URL");
                                }
                            }
                            Err(e) => {
                                tracing::warn!(email = %acc.email, error = %e, "Failed to fetch profile picture");
                            }
                        }
                    }
                    Err(e) => {
                        let err_msg = format!("Sync failed for {}: {}", acc.email, e);
                        tracing::error!("{}", err_msg);
                        errors.push(err_msg);
                    }
                }
            }
            Err(e) => {
                let err_msg = format!("Auth failed for {}: {}", acc.email, e);
                tracing::error!("{}", err_msg);
                errors.push(err_msg);
            }
        }
    }

    // Process all pending emails into orders
    let process_stats = process::process_pending_events(&db)
        .await
        .map_err(|e| e.to_string())?;

    // Fetch tracking info for any orders that are missing it
    if let Err(e) = tracking::fetch_missing_tracking_batch(&db, &tracking_service).await {
        let err_msg = format!("Tracking fetch failed: {}", e);
        tracing::error!("{}", err_msg);
        errors.push(err_msg);
    }

    // Sync order statuses from tracking data (e.g. shipped -> delivered)
    if let Err(e) = tracking::sync_delivered_from_tracking(&db).await {
        tracing::warn!("Failed to sync delivered orders from tracking: {}", e);
    }

    let success = errors.is_empty();
    let message = if success {
        format!(
            "Synced {} emails, processed {} orders",
            total_synced, process_stats.processed
        )
    } else {
        format!(
            "Synced {} emails, processed {} orders ({} errors)",
            total_synced, process_stats.processed, errors.len()
        )
    };

    Ok(SyncResult {
        success,
        emails_synced: total_synced,
        orders_processed: process_stats.processed,
        errors,
        message,
    })
}

/// Sync emails from Gmail and process them into orders
/// This combines the sync + process workflow into a single command
#[tauri::command]
pub async fn sync_and_process_orders(
    state: State<'_, AppState>,
) -> Result<SyncResult, String> {
    let db = Arc::clone(&state.db);
    let db_path = state.db_path.clone();
    let client_secret_path = state.client_secret_path.clone();
    let tracking_service = state.tracking_service.clone();

    // Run sync in a dedicated thread to avoid HRTB Send issues with the
    // tauri command macro (library futures contain non-Send-for-all-lifetimes types)
    let (tx, rx) = tokio::sync::oneshot::channel();
    let handle = tokio::runtime::Handle::current();

    std::thread::spawn(move || {
        let result = handle.block_on(perform_sync_and_process(db, db_path, client_secret_path, tracking_service));
        let _ = tx.send(result);
    });

    rx.await.map_err(|_| "Sync task failed".to_string())?
}
