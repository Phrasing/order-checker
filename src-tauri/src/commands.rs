//! Tauri IPC command handlers
//!
//! These commands are called from the frontend JavaScript via tauri.invoke()

use futures::future;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tauri::{AppHandle, Emitter, State};
use base64::Engine;
use sqlx::Row;
use sqlx::types::chrono::Utc;
use walmart_dashboard::auth::{self, AccountAuth, CallbackFlowDelegate, get_gmail_client_for_account, fetch_profile_picture_url};
use walmart_dashboard::db::Database;
use tokio::sync::Semaphore;
use walmart_dashboard::ingestion;
use walmart_dashboard::process;
use walmart_dashboard::tracking::{self, TrackingService};
use walmart_dashboard::web::{
    AccountViewModel, AggregateStats, DashboardData, DashboardDataV2, PaginatedOrders,
    fetch_account_view_models, get_dashboard_data_with_dates,
};

/// Application state managed by Tauri
pub struct AppState {
    pub db: Arc<Database>,
    pub db_path: PathBuf,
    pub client_secret_path: PathBuf,
    pub tracking_service: TrackingService,
    /// Directory for storing OAuth token cache files (app data dir).
    /// Kept outside the project tree to avoid triggering the Tauri dev file watcher.
    pub token_dir: PathBuf,
    /// Cancellation sender for an in-progress OAuth flow (if any).
    pub auth_cancel: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
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

/// Get dashboard data with pagination support (V2)
/// Returns paginated orders for improved performance with large datasets
#[tauri::command]
pub async fn get_dashboard_v2(
    state: State<'_, AppState>,
    account_id: Option<i64>,
    start_date: Option<String>,
    end_date: Option<String>,
    status_filter: Option<String>,
    cursor: Option<String>,
    limit: Option<usize>,
) -> Result<DashboardDataV2, String> {
    let db = &state.db;
    let page_size = limit.unwrap_or(100).min(500); // Default 100, cap at 500

    let paginated_orders = walmart_dashboard::web::fetch_orders_paginated(
        db,
        account_id,
        start_date.as_deref(),
        end_date.as_deref(),
        status_filter.as_deref(),
        cursor.as_deref(),
        page_size,
    )
    .await
    .map_err(|e| e.to_string())?;

    let status_counts = walmart_dashboard::web::fetch_status_counts_with_dates(
        db,
        account_id,
        start_date.as_deref(),
        end_date.as_deref(),
    )
    .await
    .map_err(|e| e.to_string())?;

    let accounts = walmart_dashboard::web::fetch_account_view_models(db)
        .await
        .map_err(|e| e.to_string())?;

    let pending_emails = walmart_dashboard::web::fetch_pending_email_count_filtered(db, account_id)
        .await
        .map_err(|e| e.to_string())?;

    // Generate timestamp using the same format as existing dashboard
    let last_updated = format!("{}", Utc::now().format("%Y-%m-%d %H:%M:%S UTC"));

    Ok(DashboardDataV2 {
        paginated_orders,
        status_counts,
        accounts,
        selected_account_id: account_id,
        pending_emails,
        last_updated,
    })
}

/// Fetch more orders for pagination
#[tauri::command]
pub async fn fetch_more_orders(
    state: State<'_, AppState>,
    account_id: Option<i64>,
    start_date: Option<String>,
    end_date: Option<String>,
    status_filter: Option<String>,
    cursor: String,
    limit: Option<usize>,
) -> Result<PaginatedOrders, String> {
    let db = &state.db;
    let page_size = limit.unwrap_or(100).min(500);

    walmart_dashboard::web::fetch_orders_paginated(
        db,
        account_id,
        start_date.as_deref(),
        end_date.as_deref(),
        status_filter.as_deref(),
        Some(&cursor),
        page_size,
    )
    .await
    .map_err(|e| e.to_string())
}

/// Search orders by query string
/// Searches across order ID, recipient email, and item names
#[tauri::command]
pub async fn search_orders(
    state: State<'_, AppState>,
    query: String,
    account_id: Option<i64>,
    start_date: Option<String>,
    end_date: Option<String>,
    status_filter: Option<String>,
    limit: Option<usize>,
) -> Result<Vec<walmart_dashboard::web::OrderViewModel>, String> {
    let db = &state.db;
    let search_limit = limit.unwrap_or(100).min(500); // Default 100, cap at 500

    walmart_dashboard::web::search_orders(
        db,
        &query,
        account_id,
        start_date.as_deref(),
        end_date.as_deref(),
        status_filter.as_deref(),
        search_limit,
    )
    .await
    .map_err(|e| e.to_string())
}

/// Get aggregate statistics without loading full order data
/// Much faster than loading all orders for large datasets
#[tauri::command]
pub async fn get_aggregate_stats(
    state: State<'_, AppState>,
    account_id: Option<i64>,
    start_date: Option<String>,
    end_date: Option<String>,
) -> Result<AggregateStats, String> {
    walmart_dashboard::web::fetch_aggregate_stats(
        &state.db,
        account_id,
        start_date.as_deref(),
        end_date.as_deref(),
    )
    .await
    .map_err(|e| e.to_string())
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

/// Cached image response for frontend lazy-loading
#[derive(Serialize)]
pub struct CachedImageResponse {
    pub data_url: String,
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
                .get_tracking_status(&state.db, &tracking_number, &carrier, true)
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

/// Manually clear the tracking client and global credential cache.
///
/// With the new V8 architecture, credentials are cached globally for 1 hour
/// across all Track17Client instances. This clears both the local client and
/// the shared credential cache, forcing fresh credential extraction (~400-500ms)
/// on the next tracking request.
///
/// Use when tracking fetches consistently fail due to credential issues.
#[tauri::command]
pub async fn restart_tracking_session(
    state: State<'_, AppState>,
) -> Result<String, String> {
    tracing::info!("Manual credential cache clear requested");

    state
        .tracking_service
        .restart_session()
        .await
        .map_err(|e| e.to_string())?;

    Ok("Tracking client and credentials cleared successfully".to_string())
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

/// Progress event payload emitted during sync stages
#[derive(Clone, Serialize)]
struct SyncProgress {
    stage: u8,
    total_stages: u8,
    label: String,
    detail: String,
}

fn emit_progress(app: &AppHandle, stage: u8, label: &str, detail: &str) {
    let _ = app.emit("sync-progress", SyncProgress {
        stage,
        total_stages: 5,
        label: label.to_string(),
        detail: detail.to_string(),
    });
}

/// Internal sync+process logic (separated from tauri command to avoid State lifetime issues)
async fn perform_sync_and_process(
    db: Arc<Database>,
    client_secret_path: PathBuf,
    tracking_service: TrackingService,
    fetch_since: Option<String>,
    token_dir: PathBuf,
    app: AppHandle,
) -> Result<SyncResult, String> {
    let mut errors = Vec::new();
    let mut total_synced = 0usize;

    // Use token_dir for resolving token cache paths
    let base_dir = &token_dir;

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

    // Pre-warm tracking client concurrently with email sync.
    // First call (or after 1-hour cache expiry) takes ~400-500ms to extract
    // credentials via brief Chrome launch. Overlapping this with email fetch
    // hides the latency and ensures warm cache for batch tracking operations.
    let tracking_warmup = {
        let ts = tracking_service.clone();
        tokio::spawn(async move {
            if let Err(e) = ts.ensure_initialized().await {
                tracing::warn!("Tracking pre-warm failed (will retry lazily): {}", e);
            }
        })
    };

    // Stage 1+2: Concurrent sync + process
    let active_accounts: Vec<_> = accounts.iter().filter(|a| a.is_active).collect();
    emit_progress(&app, 1, "Syncing & processing", &format!(
        "Syncing {} account{} and processing concurrently...",
        active_accounts.len(),
        if active_accounts.len() == 1 { "" } else { "s" }
    ));
    let max_fetches = ingestion::max_concurrent_fetches();

    let ingestion_done = Arc::new(AtomicBool::new(false));

    // Build sync futures (unchanged logic per account)
    let sync_futures: Vec<_> = active_accounts
        .iter()
        .map(|acc| {
            let db = db.clone();
            let base_dir = base_dir.to_path_buf();
            let client_secret_path = client_secret_path.clone();
            let fetch_since = fetch_since.clone();
            let email = acc.email.clone();
            let acc_id = acc.id;
            let token_cache_path = acc.token_cache_path.clone();
            // Per-account semaphore: Gmail quotas are per-user, so each account
            // gets its own concurrency limit instead of sharing a single pool.
            let rate_limit = Some(Arc::new(Semaphore::new(max_fetches)));

            async move {
                tracing::info!(email = %email, "Syncing emails for account");

                let token_path = PathBuf::from(&token_cache_path);
                let resolved_token_path = if token_path.is_absolute() {
                    token_path
                } else {
                    base_dir.join(token_path)
                };

                tracing::debug!(
                    email = %email,
                    token_path = %resolved_token_path.display(),
                    "Resolved token cache path"
                );

                let account_auth = AccountAuth::with_path(&email, resolved_token_path);

                match get_gmail_client_for_account(&client_secret_path, &account_auth).await {
                    Ok(gmail_client) => {
                        let sync_result = match &fetch_since {
                            Some(since_date) => {
                                let query = ingestion::gmail::build_walmart_query_since(since_date);
                                ingestion::sync_emails_with_query_and_account(
                                    &db, gmail_client, &query, acc_id, rate_limit.clone(),
                                ).await
                            }
                            None => {
                                ingestion::sync_emails_with_days_and_account(
                                    &db, gmail_client, 5, acc_id, rate_limit.clone(),
                                ).await
                            }
                        };
                        match sync_result {
                            Ok(stats) => {
                                tracing::info!(
                                    email = %email,
                                    synced = stats.synced,
                                    skipped = stats.skipped,
                                    "Sync completed for account"
                                );
                                if let Err(e) = db.update_account_last_sync(acc_id).await {
                                    tracing::warn!(email = %email, error = %e, "Failed to update last_sync_at");
                                }
                                match fetch_profile_picture_url(&client_secret_path, &account_auth).await {
                                    Ok(pic_url) => {
                                        if let Err(e) = db.update_account_profile_picture(acc_id, pic_url.as_deref()).await {
                                            tracing::warn!(email = %email, error = %e, "Failed to save profile picture URL");
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!(email = %email, error = %e, "Failed to fetch profile picture");
                                    }
                                }
                                Ok(stats.synced)
                            }
                            Err(e) => {
                                let err_msg = format!("Sync failed for {}: {}", email, e);
                                tracing::error!("{}", err_msg);
                                Err(err_msg)
                            }
                        }
                    }
                    Err(e) => {
                        let err_msg = format!("Auth failed for {}: {}", email, e);
                        tracing::error!("{}", err_msg);
                        Err(err_msg)
                    }
                }
            }
        })
        .collect();

    // Spawn processing task — starts immediately, processes emails as they arrive from sync
    let db_process = db.clone();
    let ingestion_done_clone = ingestion_done.clone();
    let process_handle = tokio::spawn(async move {
        process::process_pending_events_concurrent(&db_process, Some(ingestion_done_clone)).await
    });

    // Run all sync tasks to completion
    let sync_results = future::join_all(sync_futures).await;

    // Signal that all ingestion is complete
    ingestion_done.store(true, Ordering::Release);

    for result in sync_results {
        match result {
            Ok(synced) => total_synced += synced,
            Err(err_msg) => errors.push(err_msg),
        }
    }

    // Wait for processing to drain remaining queue
    emit_progress(&app, 2, "Finishing processing", &format!(
        "Draining remaining emails after syncing {}...",
        total_synced
    ));
    let process_stats = match process_handle.await {
        Ok(Ok(stats)) => stats,
        Ok(Err(e)) => {
            errors.push(format!("Processing failed: {}", e));
            process::ProcessStats::default()
        }
        Err(e) => {
            errors.push(format!("Processing task panicked: {}", e));
            process::ProcessStats::default()
        }
    };

    // Ensure tracking client pre-warm has completed before tracking stage
    // (credential extraction should be done by now)
    let _ = tracking_warmup.await;

    // Stage 3: Images + tracking (concurrent)
    emit_progress(&app, 3, "Fetching images & tracking", "Updating images and tracking data...");
    let db_img = db.clone();
    let db_track = db.clone();
    let db_delivered = db.clone();
    let ts = tracking_service.clone();

    let (img_result, tracking_result) = tokio::join!(
        async move {
            process::process_missing_product_images(&db_img).await
        },
        async move {
            tracking::fetch_missing_tracking_batch(&db_track, &ts).await
        },
    );

    // Stage 4: Sync delivered statuses (must be sequential after tracking fetch)
    emit_progress(&app, 4, "Syncing statuses", "Updating delivery statuses...");
    let delivered_result = tracking::sync_delivered_from_tracking(&db_delivered).await;

    if let Err(e) = img_result {
        tracing::warn!("Failed to process product images: {}", e);
    }
    if let Err(e) = tracking_result {
        let err_msg = format!("Tracking fetch failed: {}", e);
        tracing::error!("{}", err_msg);
        errors.push(err_msg);
    }
    if let Err(e) = delivered_result {
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

    // Stage 5: Complete
    emit_progress(&app, 5, "Sync complete", &message);

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
    app: AppHandle,
    state: State<'_, AppState>,
    fetch_since: Option<String>,
) -> Result<SyncResult, String> {
    tracing::info!("Sync command invoked");
    let db = Arc::clone(&state.db);
    let client_secret_path = state.client_secret_path.clone();
    let tracking_service = state.tracking_service.clone();
    let token_dir = state.token_dir.clone();

    // Run sync in a dedicated thread to avoid HRTB Send issues with the
    // tauri command macro (library futures contain non-Send-for-all-lifetimes types)
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let result = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt.block_on(perform_sync_and_process(
                db,
                client_secret_path,
                tracking_service,
                fetch_since,
                token_dir,
                app,
            )),
            Err(e) => Err(format!("Failed to create sync runtime: {}", e)),
        };
        let _ = tx.send(result);
    });

    rx.await.map_err(|_| "Sync task failed".to_string())?
}

/// Fetch cached product image as a data URL (base64) for lazy-loading in the UI
#[tauri::command]
pub async fn get_cached_image(
    state: State<'_, AppState>,
    image_id: String,
) -> Result<Option<CachedImageResponse>, String> {
    let row = sqlx::query(
        "SELECT image_bytes, content_type FROM images WHERE id = ?",
    )
    .bind(&image_id)
    .fetch_optional(state.db.pool())
    .await
    .map_err(|e| e.to_string())?;

    let row = match row {
        Some(row) => row,
        None => return Ok(None),
    };

    let bytes: Vec<u8> = row.try_get("image_bytes").map_err(|e| e.to_string())?;
    let content_type: Option<String> = row.try_get("content_type").map_err(|e| e.to_string())?;
    let content_type = content_type.unwrap_or_else(|| "image/png".to_string());
    let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
    let data_url = format!("data:{};base64,{}", content_type, encoded);

    Ok(Some(CachedImageResponse { data_url }))
}

/// Fetch cached thumbnails as data URLs for a batch of image IDs.
#[tauri::command]
pub async fn get_cached_thumbnails(
    state: State<'_, AppState>,
    image_ids: Vec<String>,
) -> Result<HashMap<String, String>, String> {
    if image_ids.is_empty() {
        return Ok(HashMap::new());
    }

    const CHUNK_SIZE: usize = 400;
    let mut results: HashMap<String, String> = HashMap::new();

    for chunk in image_ids.chunks(CHUNK_SIZE) {
        let placeholders: Vec<&str> = chunk.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT image_id, thumb_bytes, content_type FROM image_thumbnails WHERE image_id IN ({})",
            placeholders.join(", ")
        );
        let mut query = sqlx::query(&sql);
        for id in chunk {
            query = query.bind(id);
        }
        let rows = query
            .fetch_all(state.db.pool())
            .await
            .map_err(|e| e.to_string())?;

        for row in rows {
            let image_id: String = row.try_get("image_id").map_err(|e| e.to_string())?;
            let bytes: Vec<u8> = row.try_get("thumb_bytes").map_err(|e| e.to_string())?;
            let content_type: Option<String> =
                row.try_get("content_type").map_err(|e| e.to_string())?;
            let content_type = content_type.unwrap_or_else(|| "image/png".to_string());
            let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
            let data_url = format!("data:{};base64,{}", content_type, encoded);
            results.insert(image_id, data_url);
        }
    }

    Ok(results)
}

/// Lightweight check for new emails without downloading content.
/// When `fetch_since` is provided (e.g. "2025-01-02"), checks the same date range as sync.
#[tauri::command]
pub async fn check_new_emails(
    state: State<'_, AppState>,
    fetch_since: Option<String>,
) -> Result<ingestion::NewEmailCheck, String> {
    let db = Arc::clone(&state.db);
    let client_secret_path = state.client_secret_path.clone();
    let token_dir = state.token_dir.clone();

    let (tx, rx) = tokio::sync::oneshot::channel();

    std::thread::spawn(move || {
        let result = match tokio::runtime::Runtime::new() {
            Ok(rt) => Ok(rt.block_on(async {
                ingestion::check_new_emails(
                    &db,
                    &client_secret_path,
                    &token_dir,
                    fetch_since.as_deref(),
                ).await
            })),
            Err(e) => Err(format!("Failed to create email check runtime: {}", e)),
        };
        let _ = tx.send(result);
    });

    rx.await.map_err(|_| "Email check task failed".to_string())?
}

/// Add a new Gmail account via OAuth flow.
/// Opens a browser for the user to authorize, then stores the account in the DB.
#[tauri::command]
pub async fn add_account(
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<String, String> {
    let db = Arc::clone(&state.db);
    let client_secret_path = state.client_secret_path.clone();
    let token_dir = state.token_dir.clone();
    let app_handle = app.clone();

    // Set up cancellation channel so the frontend can abort the flow
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
    {
        let mut guard = state.auth_cancel.lock().map_err(|e| e.to_string())?;
        *guard = Some(cancel_tx);
    }

    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let result = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt.block_on(async {
                // Create a delegate that opens the auth URL via Tauri's opener plugin
                // and emits the URL to the frontend for copy/re-open functionality
                let app_for_delegate = app_handle.clone();
                let delegate = CallbackFlowDelegate::new(move |url: &str| {
                    let _ = app_for_delegate.emit("auth-url", url.to_string());
                    use tauri_plugin_opener::OpenerExt;
                    if let Err(e) = app_for_delegate.opener().open_url(url, None::<&str>) {
                        tracing::warn!("Failed to open auth URL via opener plugin: {}", e);
                    }
                });

                // Race auth flow against cancellation
                tokio::select! {
                    result = auth::authenticate_new_account(
                        &client_secret_path,
                        Some(Box::new(delegate)),
                        &token_dir,
                    ) => {
                        let (email, token_path) = result.map_err(|e| e.to_string())?;

                        // Check if account already exists
                        if let Some(existing) = db.get_account_by_email(&email).await.map_err(|e| e.to_string())? {
                            if existing.is_active {
                                return Ok(email);
                            }
                            // Reactivate deactivated account
                            sqlx::query("UPDATE accounts SET is_active = 1, token_cache_path = ? WHERE email = ?")
                                .bind(token_path.to_string_lossy().to_string())
                                .bind(&email)
                                .execute(db.pool())
                                .await
                                .map_err(|e| e.to_string())?;
                            return Ok(email);
                        }

                        let token_path_str = token_path.to_string_lossy().to_string();
                        db.add_account(&email, &token_path_str).await.map_err(|e| e.to_string())?;

                        // Fetch and store profile picture
                        let account_auth = AccountAuth::with_path(&email, token_path);
                        if let Ok(Some(pic_url)) = fetch_profile_picture_url(&client_secret_path, &account_auth).await {
                            if let Ok(Some(acc)) = db.get_account_by_email(&email).await {
                                let _ = db.update_account_profile_picture(acc.id, Some(&pic_url)).await;
                            }
                        }

                        Ok(email)
                    }
                    _ = cancel_rx => {
                        Err("Authentication cancelled".to_string())
                    }
                }
            }),
            Err(e) => Err(format!("Failed to create runtime: {}", e)),
        };
        let _ = tx.send(result);
    });

    let result = rx.await.map_err(|_| "Add account task failed".to_string())?;

    // Clear cancel sender (may already be None if cancelled)
    if let Ok(mut guard) = state.auth_cancel.lock() {
        *guard = None;
    }

    result
}

/// Cancel an in-progress OAuth add-account flow.
#[tauri::command]
pub async fn cancel_add_account(
    state: State<'_, AppState>,
) -> Result<(), String> {
    let cancel_tx = {
        let mut guard = state.auth_cancel.lock().map_err(|e| e.to_string())?;
        guard.take()
    };

    if let Some(tx) = cancel_tx {
        let _ = tx.send(());
        Ok(())
    } else {
        Ok(()) // No active auth flow — silently succeed
    }
}

/// Remove a Gmail account and delete all its data (orders, emails, token cache).
#[tauri::command]
pub async fn remove_account(
    state: State<'_, AppState>,
    account_id: i64,
) -> Result<String, String> {
    let db = &state.db;

    let account = db.get_account_by_id(account_id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Account not found".to_string())?;

    let (orders, emails) = db.delete_account_data(account_id)
        .await
        .map_err(|e| e.to_string())?;

    // Delete token cache file (resolve relative paths against token_dir)
    let token_path = PathBuf::from(&account.token_cache_path);
    let resolved_token_path = if token_path.is_absolute() {
        token_path
    } else {
        state.token_dir.join(token_path)
    };
    if resolved_token_path.exists() {
        let _ = std::fs::remove_file(&resolved_token_path);
    }

    Ok(format!("Removed {} ({} orders, {} emails deleted)", account.email, orders, emails))
}
