// Prevents additional console window on Windows in release
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod commands;

use commands::AppState;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tauri::{Emitter, Manager};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use walmart_dashboard::db::Database;
use walmart_dashboard::tracking::TrackingService;

/// Initialize logging with both console and file output.
/// Returns a guard that must be kept alive for the duration of the app.
fn init_logging(log_dir: PathBuf) -> tracing_appender::non_blocking::WorkerGuard {
    // Ensure log directory exists
    std::fs::create_dir_all(&log_dir).ok();

    // Create daily rolling file appender with format: walmart-dashboard.YYYY-MM-DD.log
    let file_appender = RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix("walmart-dashboard")
        .filename_suffix("log")
        .build(&log_dir)
        .expect("Failed to create log file appender");
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
    // Uses com.order-checker.app to match Tauri's app identifier
    let log_dir = dirs::data_dir()
        .map(|d| d.join("com.order-checker.app").join("logs"))
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
            // Priority: 1) Dev paths, 2) Bundled resource, 3) App data dir
            let mut client_secret_paths = vec![
                PathBuf::from("client_secret.json"),
                PathBuf::from("../client_secret.json"),
            ];

            // Add bundled resource path (for production builds)
            if let Ok(resource_dir) = app.path().resource_dir() {
                client_secret_paths.push(resource_dir.join("client_secret.json"));
            }

            // Add app data dir as fallback (user can manually place file there)
            if let Ok(app_data_dir) = app.path().app_data_dir() {
                client_secret_paths.push(app_data_dir.join("client_secret.json"));
            }

            let client_secret_path = client_secret_paths
                .iter()
                .find(|p| p.exists())
                .cloned()
                .unwrap_or_else(|| PathBuf::from("client_secret.json"));

            tracing::info!("Using client_secret: {}", client_secret_path.display());
            tracing::debug!("Searched paths: {:?}", client_secret_paths);

            // Determine token cache directory — use app data dir so token files
            // don't land in the project tree (which would trigger the Tauri dev file watcher).
            let token_dir = app.path()
                .app_data_dir()
                .unwrap_or_else(|_| PathBuf::from("."));
            std::fs::create_dir_all(&token_dir).ok();
            tracing::info!("Using token dir: {}", token_dir.display());

            // Determine models directory for ONNX models (background removal)
            let models_dir = app.path()
                .app_data_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join("models");
            std::fs::create_dir_all(&models_dir).ok();
            tracing::info!("Using models dir: {}", models_dir.display());

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

            // Migrate existing file-based tokens to secure credential storage
            // This is a one-time migration for existing users
            tauri::async_runtime::block_on(async {
                migrate_existing_tokens(&db, &token_dir).await;
            });

            let db_for_email_check = Arc::clone(&db);
            let client_secret_for_check = client_secret_path.clone();
            let token_dir_for_check = token_dir.clone();
            let app_handle_for_check = app.handle().clone();

            // Clone for image reprocessing task
            let db_for_image_check = Arc::clone(&db);
            let models_dir_for_check = models_dir.clone();
            let app_handle_for_image_check = app.handle().clone();

            app.manage(AppState {
                db,
                db_path,
                client_secret_path,
                tracking_service,
                token_dir,
                models_dir,
                auth_cancel: std::sync::Mutex::new(None),
            });

            // Spawn background new-email check task
            tauri::async_runtime::spawn(async move {
                // Wait for app to initialize (slightly after tracking task)
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

                tracing::info!("Starting background new-email check...");

                let check = walmart_dashboard::ingestion::check_new_emails(
                    &db_for_email_check,
                    &client_secret_for_check,
                    &token_dir_for_check,
                    None, // No fetchSince at startup; frontend will re-check with user's date
                )
                .await;

                if let Err(err) = app_handle_for_check.emit("new-emails-available", &check) {
                    tracing::error!("Failed to emit new-emails-available event: {}", err);
                }
            });

            // Spawn background task to check ONNX availability on startup
            // and reprocess non-transparent images if ONNX is now available
            tauri::async_runtime::spawn(async move {
                // Wait a bit for app to initialize
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

                tracing::info!("Checking ONNX/background removal availability...");

                // Try to create an ImageProcessor with ONNX
                match walmart_dashboard::images::ImageProcessor::new(
                    db_for_image_check.pool().clone(),
                    &models_dir_for_check,
                ).await {
                    Ok((processor, onnx_status)) => {
                        // Check if ONNX is actually working (not NoopRemover)
                        if let Some(walmart_dashboard::images::OnnxStatus::NeedsVcRedist(_)) = &onnx_status {
                            tracing::warn!("ONNX unavailable - prompting user to install VC++ Redistributable");
                            // Emit event immediately so user is prompted on startup
                            let _ = app_handle_for_image_check.emit("onnx-unavailable", serde_json::json!({
                                "message": "Background removal unavailable. Install Visual C++ Redistributable for transparent product images.",
                                "download_url": "https://aka.ms/vs/17/release/vc_redist.x64.exe"
                            }));
                            return;
                        }

                        tracing::info!("ONNX background removal is available");

                        // Check if there are any non-transparent images to reprocess
                        let non_transparent_count: i64 = sqlx::query_scalar(
                            "SELECT COUNT(*) FROM images WHERE is_transparent = 0"
                        )
                        .fetch_one(db_for_image_check.pool())
                        .await
                        .unwrap_or(0);

                        if non_transparent_count == 0 {
                            tracing::debug!("No non-transparent images to reprocess");
                            return;
                        }

                        // ONNX is working and there are non-transparent images - reprocess them
                        tracing::info!(
                            "Found {} non-transparent images, reprocessing with background removal...",
                            non_transparent_count
                        );

                        match processor.reprocess_non_transparent_images().await {
                            Ok(count) => {
                                if count > 0 {
                                    tracing::info!("Successfully reprocessed {} images with transparency", count);
                                    // Notify frontend to refresh
                                    let _ = app_handle_for_image_check.emit("images-reprocessed", serde_json::json!({
                                        "count": count,
                                        "message": format!("Reprocessed {} images with transparent backgrounds", count)
                                    }));
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to reprocess images: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!("Could not initialize ImageProcessor: {}", e);
                    }
                }
            });

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::get_dashboard,
            commands::get_dashboard_v2,
            commands::fetch_more_orders,
            commands::search_orders,
            commands::get_aggregate_stats,
            commands::get_upcoming_deliveries,
            commands::get_db_path,
            commands::get_tracking_status,
            commands::fetch_tracking,
            commands::get_cached_image,
            commands::get_cached_thumbnails,
            commands::list_accounts,
            commands::refresh_shipped_tracking,
            commands::restart_tracking_session,
            commands::process_emails,
            commands::sync_and_process_orders,
            commands::check_new_emails,
            commands::add_account,
            commands::cancel_add_account,
            commands::complete_auth_with_code,
            commands::remove_account,
            commands::clear_all_data,
            commands::check_onnx_status,
        ])
        .build(tauri::generate_context!())
        .expect("Error while building Tauri application")
        .run(|app_handle, event| {
            if let tauri::RunEvent::Exit = event {
                // Clean up tracking service on exit.
                // With V8 architecture, .close() is a no-op (no persistent browser).
                // This mainly drops the HTTP client and local state.
                if let Some(state) = app_handle.try_state::<AppState>() {
                    tracing::info!("Application exiting, cleaning up...");
                    tauri::async_runtime::block_on(async {
                        state.tracking_service.shutdown().await;
                    });
                }
            }
        });
}

/// Migrate existing file-based tokens to secure credential storage (Windows Credential Manager).
/// This is a one-time migration for users who have existing token files.
async fn migrate_existing_tokens(
    db: &std::sync::Arc<walmart_dashboard::db::Database>,
    token_dir: &std::path::Path,
) {
    use walmart_dashboard::auth;

    // Get all active accounts from database
    let accounts = match db.list_accounts().await {
        Ok(accounts) => accounts,
        Err(err) => {
            tracing::warn!("Failed to list accounts for token migration: {}", err);
            return;
        }
    };

    if accounts.is_empty() {
        return;
    }

    tracing::info!("Checking {} accounts for token migration to secure storage...", accounts.len());

    let mut migrated = 0;
    for account in accounts {
        // Resolve the token path
        let token_path = std::path::PathBuf::from(&account.token_cache_path);
        let resolved_path = if token_path.is_absolute() {
            token_path
        } else {
            token_dir.join(token_path)
        };

        // Check if file exists and credential manager doesn't have the token
        if resolved_path.exists() && !auth::has_secure_token(&account.email) {
            match auth::migrate_token_to_secure(&account.email, &resolved_path) {
                Ok(true) => {
                    tracing::info!(email = %account.email, "Migrated token to secure storage");
                    migrated += 1;
                }
                Ok(false) => {}
                Err(err) => {
                    tracing::warn!(
                        email = %account.email,
                        error = %err,
                        "Failed to migrate token to secure storage"
                    );
                }
            }
        }
    }

    if migrated > 0 {
        tracing::info!("Migrated {} token(s) to secure credential storage", migrated);
    }
}
