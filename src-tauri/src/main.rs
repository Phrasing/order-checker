// Prevents additional console window on Windows in release
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod commands;

use commands::AppState;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tauri::{Emitter, Manager};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};
use tracing_appender::rolling;
use walmart_dashboard::db::Database;
use walmart_dashboard::tracking::TrackingService;

/// Initialize logging with both console and file output.
/// Returns a guard that must be kept alive for the duration of the app.
fn init_logging(log_dir: PathBuf) -> tracing_appender::non_blocking::WorkerGuard {
    // Ensure log directory exists
    std::fs::create_dir_all(&log_dir).ok();

    // Create daily rolling file appender
    let file_appender = rolling::daily(&log_dir, "walmart-dashboard.log");
    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);

    // Console layer: INFO level by default, compact format
    let console_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .compact()
        .with_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        );

    // File layer: DEBUG level, full format with timestamps
    let file_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .with_writer(file_writer)
        .with_ansi(false)
        .with_filter(tracing_subscriber::EnvFilter::new(
            "info,walmart_dashboard=debug,walmart_dashboard_tauri=debug",
        ));

    tracing_subscriber::registry()
        .with(console_layer)
        .with(file_layer)
        .init();

    tracing::info!("Logging initialized. Log files: {}", log_dir.display());

    guard
}

fn main() {
    // Determine log directory - use app data dir or fallback to ./logs
    let log_dir = dirs::data_dir()
        .map(|d| d.join("walmart-dashboard").join("logs"))
        .unwrap_or_else(|| PathBuf::from("logs"));

    // Initialize logging and keep guard alive for the entire app lifetime
    let _log_guard = init_logging(log_dir);

    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            // Determine database path
            // Check multiple locations for orders.db:
            // 1. Current directory
            // 2. Parent directory (when running from src-tauri via cargo tauri dev)
            // 3. Fall back to app data dir
            let possible_paths = [
                PathBuf::from("orders.db"),
                PathBuf::from("../orders.db"),
            ];

            let db_path = possible_paths
                .iter()
                .find(|p| p.exists())
                .cloned()
                .unwrap_or_else(|| {
                    // Fall back to app data directory
                    app.path()
                        .app_data_dir()
                        .unwrap_or_else(|_| PathBuf::from("."))
                        .join("orders.db")
                });

            // Canonicalize to absolute path so the sqlx pool connection string
            // is never ambiguous across different tokio runtimes / working directories.
            let db_path = std::fs::canonicalize(&db_path).unwrap_or(db_path);

            tracing::info!("Using database: {}", db_path.display());

            // Find client_secret.json for OAuth
            let client_secret_paths = [
                PathBuf::from("client_secret.json"),
                PathBuf::from("../client_secret.json"),
            ];

            let client_secret_path = client_secret_paths
                .iter()
                .find(|p| p.exists())
                .cloned()
                .unwrap_or_else(|| PathBuf::from("client_secret.json"));

            tracing::info!("Using client_secret: {}", client_secret_path.display());

            // Ensure parent directory exists (synchronous, before async DB init)
            if let Some(parent) = db_path.parent() {
                let parent: &Path = parent;
                if !parent.exists() {
                    std::fs::create_dir_all(parent).ok();
                }
            }

            // Initialize DB on the Tauri async runtime — avoids creating a
            // throwaway tokio Runtime whose death could orphan pool connections.
            let db = tauri::async_runtime::block_on(async {
                let db = Database::from_file(&db_path).await?;
                db.run_migrations().await?;
                Ok::<_, anyhow::Error>(db)
            })
            .expect("Failed to initialize database");

            // Wrap db in Arc (SqlitePool is already Send+Sync, no Mutex needed)
            let db = Arc::new(db);
            let tracking_service = TrackingService::new();

            // Clone for background tasks before moving into AppState
            let db_for_task = Arc::clone(&db);
            let tracking_service_for_task = tracking_service.clone();
            let app_handle = app.handle().clone();

            let db_for_email_check = Arc::clone(&db);
            let client_secret_for_check = client_secret_path.clone();
            let db_path_for_check = db_path.clone();
            let app_handle_for_check = app.handle().clone();

            app.manage(AppState {
                db,
                db_path,
                client_secret_path,
                tracking_service,
            });

            // Spawn background tracking fetch task
            tauri::async_runtime::spawn(async move {
                // Let app fully initialize first
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

                tracing::info!("Starting background tracking fetch...");

                let db = &*db_for_task;

                // Batch fetch orders missing cached tracking data
                if let Err(e) =
                    walmart_dashboard::tracking::fetch_missing_tracking_batch(db, &tracking_service_for_task)
                        .await
                {
                    tracing::error!("Failed to fetch missing tracking: {}", e);
                }

                // Batch refresh stale entries (>4 hours old, not delivered)
                if let Err(e) =
                    walmart_dashboard::tracking::refresh_stale_tracking_batch(db, &tracking_service_for_task, 4)
                        .await
                {
                    tracing::error!("Failed to refresh stale tracking: {}", e);
                }

                // Sync order status from tracking data (shipped -> delivered)
                if let Err(e) = walmart_dashboard::tracking::sync_delivered_from_tracking(db).await
                {
                    tracing::error!("Failed to sync delivered orders: {}", e);
                }

                tracing::info!("Background tracking fetch complete");

                // Emit event to notify frontend that sync is complete
                if let Err(e) = app_handle.emit("tracking-sync-complete", ()) {
                    tracing::error!("Failed to emit tracking-sync-complete event: {}", e);
                }
            });

            // Spawn background new-email check task
            tauri::async_runtime::spawn(async move {
                // Wait for app to initialize (slightly after tracking task)
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

                tracing::info!("Starting background new-email check...");

                let base_dir = db_path_for_check
                    .parent()
                    .unwrap_or_else(|| std::path::Path::new("."))
                    .to_path_buf();

                let check = walmart_dashboard::ingestion::check_new_emails(
                    &db_for_email_check,
                    &client_secret_for_check,
                    &base_dir,
                    None, // No fetchSince at startup; frontend will re-check with user's date
                )
                .await;

                if let Err(err) = app_handle_for_check.emit("new-emails-available", &check) {
                    tracing::error!("Failed to emit new-emails-available event: {}", err);
                }
            });

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::get_dashboard,
            commands::refresh_dashboard,
            commands::get_db_path,
            commands::get_tracking_status,
            commands::fetch_tracking,
            commands::get_cached_image,
            commands::get_cached_thumbnails,
            commands::list_accounts,
            commands::restart_tracking_session,
            commands::process_emails,
            commands::sync_and_process_orders,
            commands::check_new_emails,
        ])
        .build(tauri::generate_context!())
        .expect("Error while building Tauri application")
        .run(|app_handle, event| {
            if let tauri::RunEvent::Exit = event {
                // Clean up tracking service on exit (closes Chrome browser)
                if let Some(state) = app_handle.try_state::<AppState>() {
                    tracing::info!("Application exiting, cleaning up...");
                    tauri::async_runtime::block_on(async {
                        state.tracking_service.shutdown().await;
                    });
                }
            }
        });
}
