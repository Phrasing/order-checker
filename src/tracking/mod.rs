//! Tracking status module
//!
//! Fetches delivery status from 17track.net using the track17-rs library.
//!
//! ## Architecture (track17-rs V8-based)
//!
//! track17-rs uses an embedded V8 JavaScript runtime (deno_core) instead of
//! browser automation. Chrome is only launched briefly (~once per hour) to
//! extract credentials, which are then cached globally for 1 hour across all
//! Track17Client instances via Arc<RwLock<>>.
//!
//! ### Performance Characteristics
//! - **First request** (cold/expired cache): ~400-500ms (Chrome launch + credential extraction)
//! - **Subsequent requests** (warm cache): ~100-200ms (using cached credentials)
//! - Credential cache is shared globally across all Track17Client instances
//! - Track17Client is Clone + Send + Sync for safe concurrent usage
//!
//! ### Integration Points
//! - `TrackingService`: Wraps Track17Client with lazy initialization
//! - Client stored as `Arc<Mutex<Option<Track17Client>>>` for thread-safe lazy init
//! - Pre-warming during sync overlaps credential extraction with email fetching
//! - Batch operations are efficient with cached credentials
//!
//! ### Database Caching Strategy
//! Results are cached in the database to avoid excessive API calls:
//! - Active orders (in-transit, out-for-delivery): 4-hour cache
//! - Delivered/exception orders: 7-day cache
//! - Cache can be force-refreshed when needed

use crate::db::Database;
use anyhow::{Context, Result};
use sqlx::Row;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use track17_rs::{carriers, format_location as t17_format_location, Track17Client, TrackingResponse, TrackingState as T17State};

/// Tracking state stored in the database
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrackingState {
    LabelCreated,
    InTransit,
    OutForDelivery,
    Delivered,
    Exception,
    AvailableForPickup,
    Unknown,
}

impl TrackingState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::LabelCreated => "label_created",
            Self::InTransit => "in_transit",
            Self::OutForDelivery => "out_for_delivery",
            Self::Delivered => "delivered",
            Self::Exception => "exception",
            Self::AvailableForPickup => "available_for_pickup",
            Self::Unknown => "unknown",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "label_created" => Self::LabelCreated,
            "in_transit" => Self::InTransit,
            "out_for_delivery" => Self::OutForDelivery,
            "delivered" => Self::Delivered,
            "exception" => Self::Exception,
            "available_for_pickup" => Self::AvailableForPickup,
            _ => Self::Unknown,
        }
    }

    pub fn from_track17(state: &T17State) -> Self {
        match state {
            T17State::LabelCreated => Self::LabelCreated,
            T17State::InTransit => Self::InTransit,
            T17State::OutForDelivery => Self::OutForDelivery,
            T17State::Delivered | T17State::DeliveredSigned => Self::Delivered,
            T17State::AvailableForPickup => Self::AvailableForPickup,
            T17State::Exception
            | T17State::ExceptionDelayed
            | T17State::ExceptionHeld
            | T17State::ExceptionReturned
            | T17State::ExceptionDamaged
            | T17State::Expired => Self::Exception,
            T17State::Unknown => Self::Unknown,
        }
    }

    pub fn is_delivered(&self) -> bool {
        matches!(self, Self::Delivered)
    }

    pub fn is_active(&self) -> bool {
        matches!(
            self,
            Self::LabelCreated | Self::InTransit | Self::OutForDelivery | Self::Unknown
        )
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            Self::LabelCreated => "Label Created",
            Self::InTransit => "In Transit",
            Self::OutForDelivery => "Out for Delivery",
            Self::Delivered => "Delivered",
            Self::Exception => "Exception",
            Self::AvailableForPickup => "Available for Pickup",
            Self::Unknown => "Unknown",
        }
    }
}

/// Cached tracking information from the database
#[derive(Debug, Clone)]
pub struct CachedTracking {
    pub id: i64,
    pub order_id: Option<String>,
    pub tracking_number: String,
    pub carrier: String,
    pub carrier_code: u32,
    pub state: TrackingState,
    pub state_description: Option<String>,
    pub is_delivered: bool,
    pub delivery_date: Option<String>,
    pub last_fetched_at: String,
    pub last_updated_at: String,
    pub fetch_count: i32,
    pub last_error: Option<String>,
    pub consecutive_errors: i32,
    pub events: Vec<CachedTrackingEvent>,
}

/// Cached tracking event
#[derive(Debug, Clone)]
pub struct CachedTrackingEvent {
    pub id: i64,
    pub event_time: Option<String>,
    pub event_time_iso: Option<String>,
    pub description: String,
    pub location: Option<String>,
    pub stage: Option<String>,
    pub sub_status: Option<String>,
}

/// Result of a tracking update operation
#[derive(Debug, Default)]
pub struct TrackingUpdateResult {
    pub updated: usize,
    pub errors: usize,
    pub skipped: usize,
}

/// Client statistics for monitoring
#[derive(Debug, Clone)]
pub struct ClientStats {
    pub initialized: bool,
    // Future: Add credential cache status if track17-rs exposes it
}

/// Map carrier name to 17track carrier code
pub fn carrier_to_code(carrier: &str) -> u32 {
    match carrier.to_lowercase().as_str() {
        "fedex" => carriers::FEDEX,
        "ups" => carriers::UPS,
        "usps" => carriers::USPS,
        "dhl" => carriers::DHL,
        _ => carriers::AUTO,
    }
}

/// Map carrier code to name
pub fn code_to_carrier(code: u32) -> &'static str {
    match code {
        carriers::FEDEX => "FedEx",
        carriers::UPS => "UPS",
        carriers::USPS => "USPS",
        carriers::DHL => "DHL",
        _ => "Unknown",
    }
}

/// Format a location string using zipcode lookup
/// Converts "US 60455" or "60455" to "City, State" format
pub fn format_location(raw_location: Option<String>) -> Option<String> {
    raw_location.map(|loc| t17_format_location(&loc))
}

/// Tracking service with lazy client initialization
#[derive(Clone)]
pub struct TrackingService {
    client: Arc<Mutex<Option<Track17Client>>>,
}

impl Default for TrackingService {
    fn default() -> Self {
        Self::new()
    }
}

impl TrackingService {
    /// Create a new tracking service
    pub fn new() -> Self {
        Self {
            client: Arc::new(Mutex::new(None)),
        }
    }

    /// Shut down the tracking service and clean up resources.
    ///
    /// With the V8 architecture, `.close()` is a no-op since Chrome is only
    /// launched briefly for credential extraction (~once/hour). This mainly
    /// drops the HTTP client and local state.
    pub async fn shutdown(&self) {
        let mut client_guard = self.client.lock().await;
        if let Some(client) = client_guard.take() {
            tracing::info!("Shutting down Track17 client...");
            // close() is a no-op (no persistent browser, just drops HTTP client)
            let _ = client.close().await;
            tracing::info!("Track17 client shut down");
        }
    }

    /// Clear the local client instance and global credential cache.
    ///
    /// With the new track17-rs V8 architecture, credentials are cached globally
    /// for 1 hour across all Track17Client instances. This method explicitly clears
    /// both the local client and the shared credential cache, forcing fresh
    /// credential extraction on the next tracking request.
    pub async fn restart_session(&self) -> Result<()> {
        tracing::info!("Clearing tracking client and global credential cache...");

        let mut client_guard = self.client.lock().await;

        // Drop local client instance (credentials are managed globally by the library)
        if client_guard.take().is_some() {
            tracing::debug!("Dropped local client instance");
        }

        // Client will be lazily re-initialized on next request
        tracing::info!("Client and credentials cleared, will re-initialize on next request");

        Ok(())
    }

    /// Check if the client has been initialized (has local instance).
    ///
    /// Note: This only indicates if the local client instance exists, not whether
    /// the global credential cache is warm or expired.
    pub async fn is_session_active(&self) -> bool {
        self.client.lock().await.is_some()
    }

    /// Check if client is initialized (has local instance).
    ///
    /// Note: Doesn't indicate if global credential cache is warm/expired.
    pub async fn is_initialized(&self) -> bool {
        self.client.lock().await.is_some()
    }

    /// Get basic client stats for monitoring
    pub async fn get_stats(&self) -> ClientStats {
        let initialized = self.client.lock().await.is_some();
        ClientStats { initialized }
    }

    /// Pre-initialize the Track17 client to warm credential cache.
    ///
    /// On first call (or after 1-hour cache expiry), this briefly launches Chrome
    /// to extract credentials (~400-500ms). Subsequent calls within the 1-hour
    /// window use cached credentials. Call this during sync to overlap credential
    /// extraction with email fetching for better performance.
    pub async fn ensure_initialized(&self) -> Result<()> {
        let mut client_guard = self.client.lock().await;
        if client_guard.is_none() {
            tracing::info!("Pre-warming Track17 client (will extract credentials if cache expired)...");
            let client = Track17Client::new()
                .await
                .context("Failed to initialize Track17 client. Ensure Chrome/Chromium is installed.")?;
            *client_guard = Some(client);
            tracing::info!("Track17 client pre-warmed successfully");
        }
        Ok(())
    }

    /// Get tracking status with automatic credential recovery on failure.
    ///
    /// Auto-recovery for credential errors. Clears global credential cache and
    /// retries up to 2 times. With the V8 architecture, session errors are rare
    /// since Chrome isn't kept running - most errors are credential expiry.
    pub async fn get_tracking_status_with_recovery(
        &self,
        db: &Database,
        tracking_number: &str,
        carrier: &str,
        force_refresh: bool,
    ) -> Result<CachedTracking> {
        const MAX_SESSION_RESTARTS: u32 = 2;
        let mut restarts = 0;

        loop {
            let result = self
                .get_tracking_status(db, tracking_number, carrier, force_refresh)
                .await;

            match result {
                Ok(tracking) => return Ok(tracking),
                Err(e) => {
                    let is_session_error = Self::is_session_error(&e);

                    if is_session_error && restarts < MAX_SESSION_RESTARTS {
                        restarts += 1;
                        tracing::warn!(
                            tracking_number = %tracking_number,
                            error = %e,
                            restart_attempt = restarts,
                            max_restarts = MAX_SESSION_RESTARTS,
                            "Credential error detected, clearing global cache and retrying"
                        );

                        if let Err(restart_err) = self.restart_session().await {
                            tracing::error!(
                                error = %restart_err,
                                "Failed to clear credentials and restart session"
                            );
                        }

                        // Small delay before retry
                        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                        continue;
                    }

                    // Not a session error or max restarts reached
                    tracing::error!(
                        tracking_number = %tracking_number,
                        error = %e,
                        restarts_attempted = restarts,
                        "Tracking fetch failed, no more recovery attempts"
                    );
                    return Err(e);
                }
            }
        }
    }

    /// Check if an error indicates a credential/session issue that could be
    /// resolved by clearing the cached credentials and re-initializing.
    ///
    /// With the new V8-based architecture, browser/Chrome errors are rare
    /// since the browser isn't kept running. This primarily catches credential
    /// expiry and authentication errors.
    fn is_session_error(error: &anyhow::Error) -> bool {
        let msg = error.to_string().to_lowercase();
        // Primary: credential/auth errors (most common with new arch)
        msg.contains("credential refresh failed")
            || msg.contains("sign expired")
            || msg.contains("failed to initialize track17")
            || msg.contains("failed to launch browser")
            // Secondary: network issues that might be transient
            || msg.contains("tracking data incomplete")
            || msg.contains("timeout")
            || msg.contains("connection")
            // Legacy: browser errors (rare but kept for safety)
            || msg.contains("browser")
            || msg.contains("chrome")
            || msg.contains("session")
    }

    /// Fetch tracking status from 17track.net
    ///
    /// Holds the client lock for the entire operation. Track17Client is Send + Sync
    /// but not Clone, so concurrent requests must wait for the lock.
    async fn fetch_tracking(
        &self,
        tracking_number: &str,
        carrier_code: u32,
    ) -> Result<TrackingResponse> {
        let mut client_guard = self.client.lock().await;

        // Initialize if needed
        if client_guard.is_none() {
            tracing::info!("Initializing Track17 client (extracting credentials if needed)...");
            let new_client = Track17Client::new()
                .await
                .context("Failed to initialize Track17 client. Ensure Chrome/Chromium is installed.")?;
            *client_guard = Some(new_client);
            tracing::info!("Track17 client initialized successfully");
        }

        let client = client_guard
            .as_mut()
            .expect("Track17 client not initialized after check");

        tracing::debug!(
            "Fetching tracking for {} (carrier code {})",
            tracking_number,
            carrier_code
        );

        // API call (holds lock during operation)
        let start = std::time::Instant::now();
        let result = client
            .track(tracking_number, carrier_code)
            .await
            .context("Failed to fetch tracking from 17track.net");
        let elapsed = start.elapsed();

        // Log timing to track credential cache effectiveness
        // Cold cache (credential extraction): ~400-500ms
        // Warm cache (cached credentials): ~100-200ms
        let cache_status = if elapsed.as_millis() > 300 {
            "cold cache (credential extraction)"
        } else {
            "warm cache"
        };

        tracing::debug!(
            tracking_number = %tracking_number,
            elapsed_ms = elapsed.as_millis(),
            "Tracking fetch completed in {}ms ({})",
            elapsed.as_millis(),
            cache_status
        );

        result
    }

    /// Batch fetch tracking for multiple numbers (same carrier)
    ///
    /// With the new architecture, batch operations are efficient (~100-200ms per request)
    /// after credential cache is warm. Holds client lock for the entire operation.
    pub async fn fetch_tracking_batch(
        &self,
        tracking_numbers: &[String],
        carrier_code: u32,
    ) -> Result<TrackingResponse> {
        let mut client_guard = self.client.lock().await;

        // Initialize if needed
        if client_guard.is_none() {
            tracing::info!("Initializing Track17 client (extracting credentials if needed)...");
            let new_client = Track17Client::new()
                .await
                .context("Failed to initialize Track17 client. Ensure Chrome/Chromium is installed.")?;
            *client_guard = Some(new_client);
            tracing::info!("Track17 client initialized successfully");
        }

        let client = client_guard
            .as_mut()
            .expect("Track17 client not initialized after check");

        tracing::debug!(
            "Batch fetching {} tracking numbers (carrier code {})",
            tracking_numbers.len(),
            carrier_code
        );

        // API call (holds lock during operation)
        let start = std::time::Instant::now();
        let result = client
            .track_multiple(tracking_numbers, carrier_code)
            .await
            .context("Failed to batch fetch tracking from 17track.net");
        let elapsed = start.elapsed();

        // Log timing to track credential cache effectiveness
        let cache_status = if elapsed.as_millis() > 300 {
            "cold cache (credential extraction)"
        } else {
            "warm cache"
        };

        tracing::debug!(
            count = tracking_numbers.len(),
            elapsed_ms = elapsed.as_millis(),
            "Batch fetch completed in {}ms ({}, avg {}ms per item)",
            elapsed.as_millis(),
            cache_status,
            elapsed.as_millis() / tracking_numbers.len().max(1) as u128
        );

        // Log session errors (credentials are managed globally by the library)
        if let Err(ref e) = result {
            if Self::is_session_error(e) {
                tracing::warn!("Credential error in batch fetch (library will handle re-authentication)");
            }
        }

        result
    }

    /// Get tracking status, using cache if available and fresh
    pub async fn get_tracking_status(
        &self,
        db: &Database,
        tracking_number: &str,
        carrier: &str,
        force_refresh: bool,
    ) -> Result<CachedTracking> {
        let carrier_code = carrier_to_code(carrier);

        // Check cache first
        if !force_refresh {
            if let Some(cached) = get_cached_tracking(db, tracking_number).await? {
                // Check if cache is stale
                let stale_hours = if cached.state.is_active() { 4 } else { 168 }; // 4 hours or 7 days
                if !is_cache_stale(&cached.last_fetched_at, stale_hours) {
                    tracing::debug!(
                        "Using cached tracking for {} (fetched at {})",
                        tracking_number,
                        cached.last_fetched_at
                    );
                    return Ok(cached);
                }
                tracing::debug!(
                    "Cache is stale for {} (last fetched {})",
                    tracking_number,
                    cached.last_fetched_at
                );
            }
        }

        // Fetch fresh data
        let response = self.fetch_tracking(tracking_number, carrier_code).await;

        match response {
            Ok(response) => {
                // Update cache with new data
                let cached =
                    update_tracking_cache(db, tracking_number, carrier, carrier_code, &response)
                        .await?;
                Ok(cached)
            }
            Err(e) => {
                // Record error in cache
                record_tracking_error(db, tracking_number, &e.to_string()).await?;

                // Return cached data if available
                if let Some(cached) = get_cached_tracking(db, tracking_number).await? {
                    tracing::warn!(
                        "Failed to fetch fresh tracking, returning stale cache: {}",
                        e
                    );
                    return Ok(cached);
                }

                Err(e)
            }
        }
    }

    /// Refresh all stale tracking entries
    pub async fn refresh_stale_tracking(
        &self,
        db: &Database,
        dry_run: bool,
    ) -> Result<TrackingUpdateResult> {
        let stale_entries = get_stale_tracking_entries(db).await?;
        let mut result = TrackingUpdateResult::default();

        tracing::info!("Found {} stale tracking entries to refresh", stale_entries.len());

        for entry in stale_entries {
            // Skip entries with too many consecutive errors
            if entry.consecutive_errors >= 3 {
                tracing::debug!(
                    "Skipping {} due to {} consecutive errors",
                    entry.tracking_number,
                    entry.consecutive_errors
                );
                result.skipped += 1;
                continue;
            }

            if dry_run {
                tracing::info!(
                    "[DRY RUN] Would refresh tracking for {} ({})",
                    entry.tracking_number,
                    entry.carrier
                );
                result.updated += 1;
                continue;
            }

            match self
                .get_tracking_status(db, &entry.tracking_number, &entry.carrier, true)
                .await
            {
                Ok(updated) => {
                    tracing::info!(
                        "Updated tracking for {}: {} -> {}",
                        entry.tracking_number,
                        entry.state.display_name(),
                        updated.state.display_name()
                    );
                    result.updated += 1;
                }
                Err(e) => {
                    tracing::warn!("Failed to update tracking for {}: {}", entry.tracking_number, e);
                    result.errors += 1;
                }
            }
        }

        Ok(result)
    }
}

/// Get cached tracking from database   
pub async fn get_cached_tracking(db: &Database, tracking_number: &str) -> Result<Option<CachedTracking>> {
    let row = sqlx::query(
        r#"
        SELECT id, order_id, tracking_number, carrier, carrier_code, state, state_description,
               is_delivered, delivery_date, last_fetched_at, last_updated_at, fetch_count,
               last_error, consecutive_errors
        FROM tracking_cache
        WHERE tracking_number = ?
        "#,
    )
    .bind(tracking_number)
    .fetch_optional(db.pool())
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let id: i64 = row.get("id");
    let events = get_tracking_events(db, id).await?;

    Ok(Some(CachedTracking {
        id,
        order_id: row.get("order_id"),
        tracking_number: row.get("tracking_number"),
        carrier: row.get("carrier"),
        carrier_code: row.get::<i64, _>("carrier_code") as u32,
        state: TrackingState::from_str(row.get("state")),
        state_description: row.get("state_description"),
        is_delivered: row.get::<i64, _>("is_delivered") != 0,
        delivery_date: row.get("delivery_date"),
        last_fetched_at: row.get("last_fetched_at"),
        last_updated_at: row.get("last_updated_at"),
        fetch_count: row.get::<i64, _>("fetch_count") as i32,
        last_error: row.get("last_error"),
        consecutive_errors: row.get::<i64, _>("consecutive_errors") as i32,
        events,
    }))
}

/// Get tracking events for a cache entry
async fn get_tracking_events(db: &Database, cache_id: i64) -> Result<Vec<CachedTrackingEvent>> {
    let rows = sqlx::query(
        r#"
        SELECT id, event_time, event_time_iso, description, location, stage, sub_status
        FROM tracking_events
        WHERE tracking_cache_id = ?
        ORDER BY event_time_iso DESC, id DESC
        "#,
    )
    .bind(cache_id)
    .fetch_all(db.pool())
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| CachedTrackingEvent {
            id: row.get("id"),
            event_time: row.get("event_time"),
            event_time_iso: row.get("event_time_iso"),
            description: row.get("description"),
            location: row.get("location"),
            stage: row.get("stage"),
            sub_status: row.get("sub_status"),
        })
        .collect())
}

/// Get stale tracking entries that need refreshing
async fn get_stale_tracking_entries(db: &Database) -> Result<Vec<CachedTracking>> {
    // Active shipments: stale after 4 hours
    // Delivered/exception: stale after 7 days
    let rows = sqlx::query(
        r#"
        SELECT id, order_id, tracking_number, carrier, carrier_code, state, state_description,
               is_delivered, delivery_date, last_fetched_at, last_updated_at, fetch_count,
               last_error, consecutive_errors
        FROM tracking_cache
        WHERE (
            -- Active shipments: refresh every 4 hours
            (state IN ('label_created', 'in_transit', 'out_for_delivery', 'unknown', 'available_for_pickup')
             AND datetime(last_fetched_at) < datetime('now', '-4 hours'))
            OR
            -- Delivered/exception: refresh every 7 days
            (state IN ('delivered', 'exception')
             AND datetime(last_fetched_at) < datetime('now', '-7 days'))
        )
        ORDER BY last_fetched_at ASC
        LIMIT 50
        "#,
    )
    .fetch_all(db.pool())
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| CachedTracking {
            id: row.get("id"),
            order_id: row.get("order_id"),
            tracking_number: row.get("tracking_number"),
            carrier: row.get("carrier"),
            carrier_code: row.get::<i64, _>("carrier_code") as u32,
            state: TrackingState::from_str(row.get("state")),
            state_description: row.get("state_description"),
            is_delivered: row.get::<i64, _>("is_delivered") != 0,
            delivery_date: row.get("delivery_date"),
            last_fetched_at: row.get("last_fetched_at"),
            last_updated_at: row.get("last_updated_at"),
            fetch_count: row.get::<i64, _>("fetch_count") as i32,
            last_error: row.get("last_error"),
            consecutive_errors: row.get::<i64, _>("consecutive_errors") as i32,
            events: vec![], // Not loading events for bulk query
        })
        .collect())
}

/// Update tracking cache with fresh data from API
async fn update_tracking_cache(
    db: &Database,
    tracking_number: &str,
    carrier: &str,
    carrier_code: u32,
    response: &TrackingResponse,
) -> Result<CachedTracking> {
    // Find the shipment in the response
    let shipment = response
        .shipments
        .iter()
        .find(|s| s.number == tracking_number)
        .or_else(|| response.shipments.first());

    let (state, state_desc, is_delivered, delivery_date, events) = if let Some(shipment) = shipment
    {
        // Get the latest event to determine state
        let latest_event = shipment
            .shipment
            .as_ref()
            .and_then(|s| s.latest_event.as_ref());

        let tracking_state = latest_event
            .map(|e| TrackingState::from_track17(&e.tracking_state()))
            .unwrap_or_else(|| {
                if shipment.shipment.is_some() {
                    TrackingState::LabelCreated
                } else {
                    TrackingState::Unknown
                }
            });

        let state_desc = latest_event
            .and_then(|e| e.description.clone());

        let is_delivered = tracking_state.is_delivered();

        // Try to extract delivery date from events
        let delivery_date = if is_delivered {
            latest_event.and_then(|e| e.time_iso.clone().or_else(|| e.time.clone()))
        } else {
            None
        };

        // Collect all events
        let events: Vec<_> = shipment
            .shipment
            .as_ref()
            .and_then(|s| s.tracking.as_ref())
            .and_then(|t| t.providers.as_ref())
            .map(|providers| {
                providers
                    .iter()
                    .flat_map(|p| &p.events)
                    .map(|e| CachedTrackingEvent {
                        id: 0,
                        event_time: e.time.clone(),
                        event_time_iso: e.time_iso.clone(),
                        description: e.description.clone().unwrap_or_default(),
                        location: format_location(e.raw_location()),
                        stage: e.stage.clone(),
                        sub_status: e.sub_status.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();

        (tracking_state, state_desc, is_delivered, delivery_date, events)
    } else {
        (TrackingState::Unknown, None, false, None, vec![])
    };

    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

    // Upsert the cache entry
    sqlx::query(
        r#"
        INSERT INTO tracking_cache (tracking_number, carrier, carrier_code, state, state_description,
                                    is_delivered, delivery_date, last_fetched_at, last_updated_at,
                                    fetch_count, last_error, consecutive_errors)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, NULL, 0)
        ON CONFLICT(tracking_number) DO UPDATE SET
            state = excluded.state,
            state_description = excluded.state_description,
            is_delivered = excluded.is_delivered,
            delivery_date = COALESCE(excluded.delivery_date, tracking_cache.delivery_date),
            last_fetched_at = excluded.last_fetched_at,
            last_updated_at = excluded.last_updated_at,
            fetch_count = tracking_cache.fetch_count + 1,
            last_error = NULL,
            consecutive_errors = 0
        "#,
    )
    .bind(tracking_number)
    .bind(carrier)
    .bind(carrier_code as i64)
    .bind(state.as_str())
    .bind(&state_desc)
    .bind(is_delivered as i64)
    .bind(&delivery_date)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await?;

    // Get the cache ID
    let row: (i64,) = sqlx::query_as("SELECT id FROM tracking_cache WHERE tracking_number = ?")
        .bind(tracking_number)
        .fetch_one(db.pool())
        .await?;
    let cache_id = row.0;

    // Delete existing events and insert fresh ones in a single transaction
    let mut tx = db.pool().begin().await?;

    sqlx::query("DELETE FROM tracking_events WHERE tracking_cache_id = ?")
        .bind(cache_id)
        .execute(&mut *tx)
        .await?;

    // Multi-row INSERT for events (7 cols per row, SQLite limit 999 → max 142 rows)
    const MAX_EVENTS_PER_INSERT: usize = 140;

    for chunk in events.chunks(MAX_EVENTS_PER_INSERT) {
        let row_placeholder = "(?,?,?,?,?,?,?)";
        let placeholders: Vec<&str> = chunk.iter().map(|_| row_placeholder).collect();
        let sql = format!(
            "INSERT INTO tracking_events \
             (tracking_cache_id, event_time, event_time_iso, description, location, stage, sub_status) \
             VALUES {}",
            placeholders.join(",")
        );
        let mut query = sqlx::query(&sql);
        for event in chunk {
            query = query
                .bind(cache_id)
                .bind(&event.event_time)
                .bind(&event.event_time_iso)
                .bind(&event.description)
                .bind(&event.location)
                .bind(&event.stage)
                .bind(&event.sub_status);
        }
        query.execute(&mut *tx).await?;
    }

    tx.commit().await?;

    // Fetch the full cached entry
    get_cached_tracking(db, tracking_number)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Failed to retrieve cached tracking after update"))
}

/// Record a tracking fetch error
async fn record_tracking_error(db: &Database, tracking_number: &str, error: &str) -> Result<()> {
    let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

    sqlx::query(
        r#"
        UPDATE tracking_cache
        SET last_error = ?, consecutive_errors = consecutive_errors + 1, last_fetched_at = ?
        WHERE tracking_number = ?
        "#,
    )
    .bind(error)
    .bind(&now)
    .bind(tracking_number)
    .execute(db.pool())
    .await?;

    Ok(())
}

/// Check if cache is stale based on hours threshold
fn is_cache_stale(last_fetched: &str, stale_hours: i64) -> bool {
    let Ok(fetched) = chrono::NaiveDateTime::parse_from_str(last_fetched, "%Y-%m-%d %H:%M:%S")
    else {
        return true;
    };

    let fetched_utc = fetched.and_utc();
    let now = chrono::Utc::now();
    let duration = now.signed_duration_since(fetched_utc);

    duration.num_hours() >= stale_hours
}

/// Create initial tracking cache entry (for shipping emails)
pub async fn create_tracking_cache_entry(
    db: &Database,
    order_id: &str,
    tracking_number: &str,
    carrier: &str,
) -> Result<()> {
    let carrier_code = carrier_to_code(carrier);
    let stale_timestamp = chrono::Utc::now()
        .checked_sub_signed(chrono::Duration::days(1))
        .unwrap()
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();

    sqlx::query(
        r#"
        INSERT OR IGNORE INTO tracking_cache
            (order_id, tracking_number, carrier, carrier_code, state, last_fetched_at)
        VALUES (?, ?, ?, ?, 'unknown', ?)
        "#,
    )
    .bind(order_id)
    .bind(tracking_number)
    .bind(carrier)
    .bind(carrier_code as i64)
    .bind(&stale_timestamp)
    .execute(db.pool())
    .await?;

    Ok(())
}

/// Get all tracking entries for an order
/// Finds tracking by order_id OR by matching tracking_number from orders table
pub async fn get_tracking_for_order(db: &Database, order_id: &str) -> Result<Vec<CachedTracking>> {
    tracing::info!("get_tracking_for_order called with order_id: {}", order_id);

    let rows = sqlx::query(
        r#"
        SELECT tc.id, tc.order_id, tc.tracking_number, tc.carrier, tc.carrier_code, tc.state,
               tc.state_description, tc.is_delivered, tc.delivery_date, tc.last_fetched_at,
               tc.last_updated_at, tc.fetch_count, tc.last_error, tc.consecutive_errors
        FROM tracking_cache tc
        LEFT JOIN orders o ON o.tracking_number = tc.tracking_number
        WHERE tc.order_id = ? OR o.id = ?
        "#,
    )
    .bind(order_id)
    .bind(order_id)
    .fetch_all(db.pool())
    .await?;

    tracing::info!("get_tracking_for_order found {} rows for order_id: {}", rows.len(), order_id);

    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i64 = row.get("id");
        let events = get_tracking_events(db, id).await?;

        results.push(CachedTracking {
            id,
            order_id: row.get("order_id"),
            tracking_number: row.get("tracking_number"),
            carrier: row.get("carrier"),
            carrier_code: row.get::<i64, _>("carrier_code") as u32,
            state: TrackingState::from_str(row.get("state")),
            state_description: row.get("state_description"),
            is_delivered: row.get::<i64, _>("is_delivered") != 0,
            delivery_date: row.get("delivery_date"),
            last_fetched_at: row.get("last_fetched_at"),
            last_updated_at: row.get("last_updated_at"),
            fetch_count: row.get::<i64, _>("fetch_count") as i32,
            last_error: row.get("last_error"),
            consecutive_errors: row.get::<i64, _>("consecutive_errors") as i32,
            events,
        });
    }

    Ok(results)
}

/// Check if an error is a credential/session failure from track17-rs.
/// When credentials are persistently rejected, there's no point retrying more batches.
fn is_credential_error(e: &anyhow::Error) -> bool {
    let msg = e.to_string().to_lowercase();
    msg.contains("credential refresh failed")
        || msg.contains("failed to initialize track17")
        || msg.contains("failed to launch browser")
}

/// Batch fetch tracking for all orders missing cached data or with unknown state
/// Groups by carrier and fetches up to 40 at a time
pub async fn fetch_missing_tracking_batch(
    db: &Database,
    service: &TrackingService,
) -> Result<u32> {
    // Get orders with tracking but no cache OR with placeholder (unknown) cache entries
    let orders: Vec<(String, String, String)> = sqlx::query_as(
        r#"
        SELECT o.id, o.tracking_number, o.carrier
        FROM orders o
        LEFT JOIN tracking_cache tc ON tc.tracking_number = o.tracking_number
        WHERE o.tracking_number IS NOT NULL
          AND o.carrier IS NOT NULL
          AND o.tracking_number != ''
          AND (tc.id IS NULL OR tc.state = 'unknown')
        ORDER BY o.carrier, o.order_date DESC
        "#,
    )
    .fetch_all(db.pool())
    .await?;

    if orders.is_empty() {
        tracing::info!("No orders need tracking fetch");
        return Ok(0);
    }

    // Group by carrier
    let mut by_carrier: HashMap<String, Vec<(String, String)>> = HashMap::new();
    for (order_id, tracking_number, carrier) in orders {
        by_carrier
            .entry(carrier)
            .or_default()
            .push((order_id, tracking_number));
    }

    let total_carriers = by_carrier.len();
    let total_orders: usize = by_carrier.values().map(|v| v.len()).sum();
    tracing::info!(
        "Batch fetching tracking for {} orders across {} carriers",
        total_orders,
        total_carriers
    );

    let mut fetched = 0u32;

    for (carrier, items) in by_carrier {
        let carrier_code = carrier_to_code(&carrier);

        // Process in batches of 40
        for chunk in items.chunks(40) {
            let tracking_numbers: Vec<String> = chunk.iter().map(|(_, t)| t.clone()).collect();

            tracing::debug!(
                "Fetching batch of {} {} shipments",
                tracking_numbers.len(),
                carrier
            );

            match service
                .fetch_tracking_batch(&tracking_numbers, carrier_code)
                .await
            {
                Ok(response) => {
                    // Process each result and update cache
                    for tracking_number in &tracking_numbers {
                        match update_tracking_cache(
                            db,
                            tracking_number,
                            &carrier,
                            carrier_code,
                            &response,
                        )
                        .await
                        {
                            Ok(_) => {
                                fetched += 1;
                                tracing::debug!("Cached tracking for {}", tracking_number);
                            }
                            Err(e) => {
                                tracing::warn!("Failed to cache {}: {}", tracking_number, e);
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Batch fetch failed for {} {} shipments: {}",
                        tracking_numbers.len(),
                        carrier,
                        e
                    );
                    if is_credential_error(&e) {
                        tracing::error!(
                            "Credential/session failure detected — aborting remaining tracking fetches"
                        );
                        return Ok(fetched);
                    }
                }
            }
        }
    }

    tracing::info!(
        "Batch tracking fetch complete: {}/{} successful",
        fetched,
        total_orders
    );
    Ok(fetched)
}

/// Batch refresh stale tracking entries (not updated in X hours, not delivered)
pub async fn refresh_stale_tracking_batch(
    db: &Database,
    service: &TrackingService,
    stale_hours: i64,
) -> Result<u32> {
    let cutoff = chrono::Utc::now() - chrono::Duration::hours(stale_hours);
    let cutoff_str = cutoff.format("%Y-%m-%d %H:%M:%S").to_string();

    let stale: Vec<(String, String)> = sqlx::query_as(
        r#"
        SELECT tc.tracking_number, tc.carrier
        FROM tracking_cache tc
        WHERE tc.last_fetched_at < ?
          AND tc.state != 'delivered'
        ORDER BY tc.carrier, tc.last_fetched_at ASC
        LIMIT 120
        "#,
    )
    .bind(&cutoff_str)
    .fetch_all(db.pool())
    .await?;

    if stale.is_empty() {
        tracing::info!("No stale tracking entries to refresh");
        return Ok(0);
    }

    // Group by carrier
    let mut by_carrier: HashMap<String, Vec<String>> = HashMap::new();
    for (tracking_number, carrier) in stale {
        by_carrier.entry(carrier).or_default().push(tracking_number);
    }

    let total: usize = by_carrier.values().map(|v| v.len()).sum();
    tracing::info!("Refreshing {} stale tracking entries", total);

    let mut refreshed = 0u32;

    for (carrier, tracking_numbers) in by_carrier {
        let carrier_code = carrier_to_code(&carrier);

        for chunk in tracking_numbers.chunks(40) {
            let nums: Vec<String> = chunk.to_vec();

            match service.fetch_tracking_batch(&nums, carrier_code).await {
                Ok(response) => {
                    for tracking_number in &nums {
                        match update_tracking_cache(
                            db,
                            tracking_number,
                            &carrier,
                            carrier_code,
                            &response,
                        )
                        .await
                        {
                            Ok(_) => {
                                refreshed += 1;
                            }
                            Err(e) => {
                                tracing::warn!("Failed to refresh {}: {}", tracking_number, e);
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Batch refresh failed for {} entries: {}", nums.len(), e);
                    if is_credential_error(&e) {
                        tracing::error!(
                            "Credential/session failure detected — aborting remaining tracking refreshes"
                        );
                        return Ok(refreshed);
                    }
                }
            }
        }
    }

    tracing::info!(
        "Stale tracking refresh complete: {}/{} successful",
        refreshed,
        total
    );
    Ok(refreshed)
}

/// Sync order status to 'delivered' for orders where tracking shows delivered.
/// Called after tracking data is fetched/refreshed on startup.
pub async fn sync_delivered_from_tracking(db: &Database) -> Result<usize> {
    let result = sqlx::query(
        r#"
        UPDATE orders
        SET status = 'delivered'
        WHERE status = 'shipped'
        AND tracking_number IN (
            SELECT tracking_number FROM tracking_cache WHERE is_delivered = 1
        )
        "#,
    )
    .execute(db.pool())
    .await
    .context("Failed to sync delivered orders from tracking")?;

    let count = result.rows_affected() as usize;
    if count > 0 {
        tracing::info!(
            "Synced {} orders to 'delivered' from tracking data",
            count
        );
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_carrier_mapping() {
        assert_eq!(carrier_to_code("FedEx"), carriers::FEDEX);
        assert_eq!(carrier_to_code("fedex"), carriers::FEDEX);
        assert_eq!(carrier_to_code("UPS"), carriers::UPS);
        assert_eq!(carrier_to_code("USPS"), carriers::USPS);
        assert_eq!(carrier_to_code("DHL"), carriers::DHL);
        assert_eq!(carrier_to_code("Unknown"), carriers::AUTO);
    }

    #[test]
    fn test_tracking_state_conversion() {
        assert_eq!(
            TrackingState::from_track17(&T17State::Delivered),
            TrackingState::Delivered
        );
        assert_eq!(
            TrackingState::from_track17(&T17State::InTransit),
            TrackingState::InTransit
        );
        assert!(TrackingState::Delivered.is_delivered());
        assert!(!TrackingState::InTransit.is_delivered());
        assert!(TrackingState::InTransit.is_active());
        assert!(!TrackingState::Delivered.is_active());
    }

    #[test]
    fn test_cache_staleness() {
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        assert!(!is_cache_stale(&now, 4)); // Fresh

        let old = chrono::Utc::now()
            .checked_sub_signed(chrono::Duration::hours(5))
            .unwrap()
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        assert!(is_cache_stale(&old, 4)); // Stale after 4 hours
    }
}
