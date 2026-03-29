//! OAuth2 authentication module for Gmail API
//!
//! Handles authentication using client_secret.json and persists
//! tokens to a local cache file for reuse across sessions.
//!
//! Supports multiple Gmail accounts with separate token caches.

use anyhow::{anyhow, Context, Result};
use google_gmail1::hyper::client::HttpConnector;
use google_gmail1::hyper_rustls::HttpsConnector;
use google_gmail1::{hyper, hyper_rustls, oauth2, Gmail};
use sha2::{Digest, Sha256};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use url::Url;

/// Default path for token cache file
pub const DEFAULT_TOKEN_CACHE_PATH: &str = "token_cache.json";

/// Custom OAuth flow delegate that calls a callback with the auth URL
/// instead of printing to stdout or opening the browser directly.
/// Used by the Tauri app to open the URL via the system browser plugin
/// and show an in-app auth overlay.
pub struct CallbackFlowDelegate<F: Fn(&str) + Send + Sync> {
    on_url: F,
}

impl<F: Fn(&str) + Send + Sync> CallbackFlowDelegate<F> {
    pub fn new(on_url: F) -> Self {
        Self { on_url }
    }
}

impl<F: Fn(&str) + Send + Sync> oauth2::authenticator_delegate::InstalledFlowDelegate
    for CallbackFlowDelegate<F>
{
    fn present_user_url<'a>(
        &'a self,
        url: &'a str,
        _need_code: bool,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>> {
        (self.on_url)(url);
        Box::pin(async { Ok(String::new()) })
    }
}

/// Non-interactive delegate that fails immediately if browser auth is needed.
/// Used by `get_gmail_client_for_account` to prevent sync from hanging when a
/// token is expired/revoked. Silent token refresh still works — only interactive
/// OAuth (browser redirect) is blocked.
pub struct NonInteractiveFlowDelegate;

impl oauth2::authenticator_delegate::InstalledFlowDelegate for NonInteractiveFlowDelegate {
    fn present_user_url<'a>(
        &'a self,
        _url: &'a str,
        _need_code: bool,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>> {
        Box::pin(async {
            Err("Token expired or revoked. Please re-connect this account.".to_string())
        })
    }
}

/// Gmail API scopes we need
const GMAIL_SCOPES: &[&str] = &[
    "https://www.googleapis.com/auth/gmail.readonly",
    "profile",
];

/// Type alias for the authenticated Gmail client
pub type GmailClient = Gmail<HttpsConnector<HttpConnector>>;

/// Authenticates with Google and returns a Gmail API client.
///
/// This function:
/// 1. Reads OAuth2 credentials from `client_secret.json`
/// 2. Checks for cached tokens in `token_cache.json`
/// 3. If no valid token exists, opens a browser for user authorization
/// 4. Caches the new token for future use
///
/// # Arguments
/// * `client_secret_path` - Path to the client_secret.json file from Google Cloud Console
/// * `token_cache_path` - Path where access tokens will be cached
///
/// # Returns
/// An authenticated Gmail API client ready for making requests
pub async fn get_gmail_client(
    client_secret_path: &Path,
    token_cache_path: &Path,
) -> Result<GmailClient> {
    tracing::info!("Initializing Gmail authentication...");

    // Read the client secret file
    let secret = oauth2::read_application_secret(client_secret_path)
        .await
        .context("Failed to read client_secret.json. Make sure the file exists and is valid.")?;

    // Build the authenticator with token persistence
    let auth = oauth2::InstalledFlowAuthenticator::builder(
        secret,
        oauth2::InstalledFlowReturnMethod::HTTPRedirect,
    )
    .persist_tokens_to_disk(token_cache_path)
    .build()
    .await
    .context("Failed to build authenticator")?;

    // Pre-authorize to ensure we have valid tokens
    // This will prompt for browser auth if needed
    auth.token(GMAIL_SCOPES)
        .await
        .context("Failed to get access token. You may need to re-authenticate.")?;

    tracing::info!("Gmail authentication successful");

    // Build the HTTP client with HTTPS support using google_gmail1's re-exported hyper_rustls
    let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .context("Failed to load native TLS roots")?
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();

    let client = hyper::Client::builder().build(https_connector);

    // Create the Gmail API client
    let gmail = Gmail::new(client, auth);

    Ok(gmail)
}

/// Remove cached token (for re-authentication)
pub fn clear_cached_token() -> Result<()> {
    let path = Path::new(DEFAULT_TOKEN_CACHE_PATH);
    if path.exists() {
        std::fs::remove_file(path).context("Failed to remove cached token")?;
        tracing::info!("Cached token cleared");
    }
    Ok(())
}

// ==================== Multi-Account Support ====================

/// Authentication context for a specific Gmail account.
/// Uses a unique token cache file per account based on email hash.
#[derive(Debug, Clone)]
pub struct AccountAuth {
    /// The Gmail email address for this account
    pub email: String,
    /// Path to the token cache file for this account
    pub token_cache_path: PathBuf,
}

impl AccountAuth {
    /// Create an AccountAuth from an email address.
    /// Generates a unique token cache path based on SHA256 hash of the email.
    pub fn from_email(email: &str) -> Self {
        let token_cache_path = Self::generate_token_path(email);
        Self {
            email: email.to_string(),
            token_cache_path,
        }
    }

    /// Create an AccountAuth with a specific token cache path.
    /// Used when loading accounts from the database.
    pub fn with_path(email: &str, token_cache_path: PathBuf) -> Self {
        Self {
            email: email.to_string(),
            token_cache_path,
        }
    }

    /// Generate a unique token cache path for an email address.
    /// Uses first 8 characters of SHA256 hash to avoid collisions.
    pub fn generate_token_path(email: &str) -> PathBuf {
        let mut hasher = Sha256::new();
        hasher.update(email.as_bytes());
        let hash = hasher.finalize();
        let hash_prefix = format!("{:x}", hash).chars().take(8).collect::<String>();
        PathBuf::from(format!("token_cache_{}.json", hash_prefix))
    }

    /// Check if this account has a cached token
    pub fn has_cached_token(&self) -> bool {
        self.token_cache_path.exists()
    }

    /// Remove the cached token for this account
    pub fn clear_token(&self) -> Result<()> {
        if self.token_cache_path.exists() {
            std::fs::remove_file(&self.token_cache_path)
                .context("Failed to remove cached token")?;
            tracing::info!(email = %self.email, "Cached token cleared");
        }
        Ok(())
    }
}

/// Authenticate a specific Gmail account and return a client.
/// This triggers the OAuth flow if no valid token is cached.
///
/// Uses secure credential storage (Windows Credential Manager) with a hybrid approach:
/// 1. Restore token from credential manager to temp file for yup-oauth2 compatibility
/// 2. Let yup-oauth2 handle token refresh if needed
/// 3. Migrate updated token back to credential manager
pub async fn get_gmail_client_for_account(
    client_secret_path: &Path,
    account_auth: &AccountAuth,
) -> Result<GmailClient> {
    tracing::info!(email = %account_auth.email, "Initializing Gmail authentication for account...");

    // Step 1: Try to restore token from credential manager to file for yup-oauth2
    let restored_from_secure = restore_token_to_file(
        &account_auth.email,
        &account_auth.token_cache_path,
    ).unwrap_or(false);

    if restored_from_secure {
        tracing::debug!(email = %account_auth.email, "Restored token from secure storage");
    }

    // Read the client secret file
    let secret = oauth2::read_application_secret(client_secret_path)
        .await
        .context("Failed to read client_secret.json. Make sure the file exists and is valid.")?;

    // Build the authenticator with token persistence for this specific account.
    // Uses NonInteractiveFlowDelegate so that expired/revoked tokens fail fast
    // instead of blocking forever waiting for browser auth.
    let auth = oauth2::InstalledFlowAuthenticator::builder(
        secret,
        oauth2::InstalledFlowReturnMethod::HTTPRedirect,
    )
    .persist_tokens_to_disk(&account_auth.token_cache_path)
    .flow_delegate(Box::new(NonInteractiveFlowDelegate))
    .build()
    .await
    .context("Failed to build authenticator")?;

    // Pre-authorize to ensure we have valid tokens (may trigger refresh)
    auth.token(GMAIL_SCOPES)
        .await
        .context("Failed to get access token. You may need to re-authenticate.")?;

    tracing::info!(email = %account_auth.email, "Gmail authentication successful");

    // Step 2: Migrate token back to secure storage (may have been refreshed)
    if let Err(err) = migrate_token_to_secure(&account_auth.email, &account_auth.token_cache_path) {
        tracing::warn!(
            email = %account_auth.email,
            error = %err,
            "Failed to migrate token to secure storage"
        );
    }

    // Build the HTTP client
    let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .context("Failed to load native TLS roots")?
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();

    let client = hyper::Client::builder().build(https_connector);

    // Create the Gmail API client
    let gmail = Gmail::new(client, auth);

    Ok(gmail)
}

/// Get the authenticated email address from a Gmail client.
/// Makes a profile request to determine which account is authenticated.
pub async fn get_authenticated_email(client: &GmailClient) -> Result<String> {
    let profile = client
        .users()
        .get_profile("me")
        .doit()
        .await
        .context("Failed to get Gmail profile")?;

    profile
        .1
        .email_address
        .ok_or_else(|| anyhow!("No email address in Gmail profile"))
}

/// Authenticate with a new account (triggers OAuth flow) and return the email.
/// This is used during the add-account flow to discover the email address.
///
/// When `flow_delegate` is `Some`, the delegate controls how the auth URL is
/// presented (e.g. via Tauri's opener plugin). When `None`, the default
/// yup-oauth2 delegate opens the system browser / prints to stdout.
pub async fn authenticate_new_account(
    client_secret_path: &Path,
    flow_delegate: Option<Box<dyn oauth2::authenticator_delegate::InstalledFlowDelegate>>,
    token_dir: &Path,
) -> Result<(String, PathBuf)> {
    tracing::info!("Starting OAuth flow for new account...");

    // Ensure token directory exists
    std::fs::create_dir_all(token_dir)
        .context("Failed to create token directory")?;

    // Use a temporary token path inside token_dir
    let temp_path = token_dir.join("token_cache_temp.json");

    // Read the client secret file
    let secret = oauth2::read_application_secret(client_secret_path)
        .await
        .context("Failed to read client_secret.json")?;

    // Build the authenticator
    let mut builder = oauth2::InstalledFlowAuthenticator::builder(
        secret.clone(),
        oauth2::InstalledFlowReturnMethod::HTTPRedirect,
    )
    .persist_tokens_to_disk(&temp_path);

    if let Some(delegate) = flow_delegate {
        builder = builder.flow_delegate(delegate);
    }

    let auth = builder
    .build()
    .await
    .context("Failed to build authenticator")?;

    // Trigger OAuth flow
    auth.token(GMAIL_SCOPES)
        .await
        .context("Failed to authenticate. Please complete the OAuth flow in your browser.")?;

    // Build client to get profile
    let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .context("Failed to load native TLS roots")?
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();

    let http_client = hyper::Client::builder().build(https_connector);
    let gmail = Gmail::new(http_client, auth);

    // Get the email address
    let email = get_authenticated_email(&gmail).await?;
    tracing::info!(email = %email, "Authenticated as");

    // Generate the permanent token path inside token_dir
    let permanent_path = token_dir.join(AccountAuth::generate_token_path(&email));

    // Rename temp token to permanent path
    if temp_path.exists() {
        std::fs::rename(&temp_path, &permanent_path)
            .context("Failed to move token to permanent location")?;
    }

    Ok((email, permanent_path))
}

/// Response from Google's OAuth2 userinfo endpoint
#[derive(serde::Deserialize)]
struct UserInfoResponse {
    picture: Option<String>,
}

/// Response from Google's token exchange endpoint
#[derive(Debug, serde::Deserialize)]
struct TokenExchangeResponse {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: u64,
    #[allow(dead_code)]
    token_type: String,
    #[allow(dead_code)]
    scope: Option<String>,
    id_token: Option<String>,
}

/// Response from Gmail profile endpoint
#[derive(Debug, serde::Deserialize)]
struct GmailProfileResponse {
    #[serde(rename = "emailAddress")]
    email_address: String,
}

/// Token storage format compatible with yup-oauth2's disk persistence.
/// This must match the format that `InstalledFlowAuthenticator::persist_tokens_to_disk` uses.
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct StoredTokenEntry {
    pub scopes: Vec<String>,
    pub token: StoredToken,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct StoredToken {
    pub access_token: Option<String>,
    pub refresh_token: Option<String>,
    /// RFC3339 formatted expiry time
    pub expires_at: Option<String>,
    pub id_token: Option<String>,
}

// ==================== Secure Credential Storage ====================

/// Service name for Windows Credential Manager entries.
const CREDENTIAL_SERVICE: &str = "com.order-checker.app";

/// Store OAuth token securely in Windows Credential Manager.
/// The token data is JSON-encoded and stored encrypted via DPAPI.
pub fn store_token_secure(email: &str, token_entries: &[StoredTokenEntry]) -> Result<()> {
    let entry = keyring::Entry::new(CREDENTIAL_SERVICE, email)
        .context("Failed to create keyring entry")?;

    let token_json = serde_json::to_string(token_entries)
        .context("Failed to serialize token")?;

    entry.set_password(&token_json)
        .context("Failed to store token in credential manager")?;

    tracing::info!(email = %email, "Token stored securely in credential manager");
    Ok(())
}

/// Retrieve OAuth token from Windows Credential Manager.
pub fn retrieve_token_secure(email: &str) -> Result<Option<Vec<StoredTokenEntry>>> {
    let entry = keyring::Entry::new(CREDENTIAL_SERVICE, email)
        .context("Failed to create keyring entry")?;

    match entry.get_password() {
        Ok(json) => {
            let tokens: Vec<StoredTokenEntry> = serde_json::from_str(&json)
                .context("Failed to parse stored token")?;
            Ok(Some(tokens))
        }
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(err) => Err(anyhow!("Failed to retrieve token: {}", err)),
    }
}

/// Delete OAuth token from Windows Credential Manager.
pub fn delete_token_secure(email: &str) -> Result<()> {
    let entry = keyring::Entry::new(CREDENTIAL_SERVICE, email)
        .context("Failed to create keyring entry")?;

    match entry.delete_password() {
        Ok(()) => {
            tracing::info!(email = %email, "Token deleted from credential manager");
            Ok(())
        }
        Err(keyring::Error::NoEntry) => Ok(()), // Already deleted
        Err(err) => Err(anyhow!("Failed to delete token: {}", err)),
    }
}

/// Check if a secure token exists for the given email.
pub fn has_secure_token(email: &str) -> bool {
    keyring::Entry::new(CREDENTIAL_SERVICE, email)
        .and_then(|entry| entry.get_password())
        .is_ok()
}

/// Migrate a file-based token to secure credential manager and delete the file.
/// Returns Ok(true) if migration occurred, Ok(false) if no file existed.
///
/// Stores the raw JSON from yup-oauth2 without parsing into typed structs,
/// as yup-oauth2's serialization format may differ from our StoredTokenEntry format.
pub fn migrate_token_to_secure(email: &str, file_path: &Path) -> Result<bool> {
    if !file_path.exists() {
        return Ok(false);
    }

    let json = std::fs::read_to_string(file_path)
        .context("Failed to read token file")?;

    // Validate it's valid JSON without strict struct matching
    // yup-oauth2 uses a different format for expires_at (object vs string)
    let _: serde_json::Value = serde_json::from_str(&json)
        .context("Failed to parse token file as JSON")?;

    // Store the raw JSON in credential manager
    let entry = keyring::Entry::new(CREDENTIAL_SERVICE, email)
        .context("Failed to create keyring entry")?;
    entry.set_password(&json)
        .context("Failed to store token in credential manager")?;

    // Delete plaintext file
    std::fs::remove_file(file_path)
        .context("Failed to delete plaintext token file")?;

    tracing::info!(email = %email, path = %file_path.display(), "Migrated token to credential manager");
    Ok(true)
}

/// Restore token from credential manager to a temporary file for yup-oauth2 compatibility.
/// Writes the raw JSON back exactly as it was stored (preserving yup-oauth2's format).
/// Returns Ok(true) if restored, Ok(false) if no token exists.
pub fn restore_token_to_file(email: &str, file_path: &Path) -> Result<bool> {
    let entry = keyring::Entry::new(CREDENTIAL_SERVICE, email)
        .context("Failed to create keyring entry")?;

    match entry.get_password() {
        Ok(json) => {
            // Ensure parent directory exists
            if let Some(parent) = file_path.parent() {
                std::fs::create_dir_all(parent).ok();
            }

            // Write the raw JSON back exactly as stored
            std::fs::write(file_path, json)
                .context("Failed to write temporary token file")?;

            tracing::debug!(email = %email, "Restored token from credential manager to temp file");
            Ok(true)
        }
        Err(keyring::Error::NoEntry) => Ok(false),
        Err(err) => Err(anyhow!("Failed to retrieve token: {}", err)),
    }
}

/// Fetch the Google profile picture URL for an account.
///
/// Uses the OAuth2 userinfo endpoint which returns the user's public profile picture.
/// Requires the `profile` scope to be granted.
pub async fn fetch_profile_picture_url(
    client_secret_path: &Path,
    account_auth: &AccountAuth,
) -> Result<Option<String>> {
    // Build authenticator to get a valid access token
    let secret = oauth2::read_application_secret(client_secret_path)
        .await
        .context("Failed to read client_secret.json")?;

    let auth = oauth2::InstalledFlowAuthenticator::builder(
        secret,
        oauth2::InstalledFlowReturnMethod::HTTPRedirect,
    )
    .persist_tokens_to_disk(&account_auth.token_cache_path)
    .build()
    .await
    .context("Failed to build authenticator")?;

    let token = auth
        .token(GMAIL_SCOPES)
        .await
        .context("Failed to get access token for profile picture")?;

    let token_str = token
        .token()
        .ok_or_else(|| anyhow!("No access token available"))?;

    // Make HTTP request to Google's userinfo endpoint
    let https_connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()
        .context("Failed to load native TLS roots")?
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();

    let client = hyper::Client::builder().build(https_connector);

    let request = hyper::Request::builder()
        .uri("https://www.googleapis.com/oauth2/v3/userinfo")
        .header("Authorization", format!("Bearer {}", token_str))
        .body(hyper::Body::empty())
        .context("Failed to build userinfo request")?;

    let response = client
        .request(request)
        .await
        .context("Failed to fetch userinfo")?;

    let body_bytes = hyper::body::to_bytes(response.into_body())
        .await
        .context("Failed to read userinfo response body")?;

    let user_info: UserInfoResponse =
        serde_json::from_slice(&body_bytes).context("Failed to parse userinfo response")?;

    Ok(user_info.picture)
}

/// Manually exchange an authorization code for tokens.
/// Used when automatic localhost redirect capture fails.
///
/// This function:
/// 1. Parses the authorization code from the redirect URL
/// 2. Exchanges the code for tokens via Google's token endpoint
/// 3. Fetches the user's email from Gmail API
/// 4. Writes the token cache in yup-oauth2 compatible format
///
/// # Arguments
/// * `client_secret_path` - Path to client_secret.json
/// * `redirect_url` - The full redirect URL including the code parameter
/// * `token_dir` - Directory where token cache files are stored
///
/// # Returns
/// Tuple of (email, token_cache_path)
pub async fn exchange_auth_code_manually(
    client_secret_path: &Path,
    redirect_url: &str,
    token_dir: &Path,
) -> Result<(String, PathBuf)> {
    tracing::info!("Manually exchanging auth code from redirect URL...");

    // Parse the redirect URL to extract the code
    let parsed_url = Url::parse(redirect_url)
        .context("Invalid redirect URL format")?;

    let code = parsed_url
        .query_pairs()
        .find(|(key, _)| key == "code")
        .map(|(_, value)| value.to_string())
        .ok_or_else(|| anyhow!("No authorization code found in URL"))?;

    // Extract redirect_uri from the URL (scheme://host:port/)
    let redirect_uri = match parsed_url.port() {
        Some(port) => format!(
            "{}://{}:{}/",
            parsed_url.scheme(),
            parsed_url.host_str().unwrap_or("localhost"),
            port
        ),
        None => format!(
            "{}://{}/",
            parsed_url.scheme(),
            parsed_url.host_str().unwrap_or("localhost")
        ),
    };

    tracing::debug!("Extracted code, using redirect_uri: {}", redirect_uri);

    // Read client secret
    let secret = oauth2::read_application_secret(client_secret_path)
        .await
        .context("Failed to read client_secret.json")?;

    // Exchange code for tokens via HTTP POST
    let http_client = reqwest::Client::new();
    let token_response = http_client    
        .post("https://oauth2.googleapis.com/token")
        .form(&[
            ("code", code.as_str()),
            ("client_id", secret.client_id.as_str()),
            ("client_secret", secret.client_secret.as_str()),
            ("redirect_uri", redirect_uri.as_str()),
            ("grant_type", "authorization_code"),
        ])
        .send()
        .await
        .context("Failed to send token exchange request")?;

    if !token_response.status().is_success() {
        let error_text = token_response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "Token exchange failed: {}. The authorization code may have expired.",
            error_text
        ));
    }

    let tokens: TokenExchangeResponse = token_response
        .json()
        .await
        .context("Failed to parse token response")?;

    tracing::debug!("Token exchange successful, fetching user profile...");

    // Get user email from Gmail API using the access token
    let profile_response = http_client
        .get("https://www.googleapis.com/gmail/v1/users/me/profile")
        .bearer_auth(&tokens.access_token)
        .send()
        .await
        .context("Failed to fetch Gmail profile")?;

    if !profile_response.status().is_success() {
        let error_text = profile_response.text().await.unwrap_or_default();
        return Err(anyhow!("Failed to get Gmail profile: {}", error_text));
    }

    let profile: GmailProfileResponse = profile_response
        .json()
        .await
        .context("Failed to parse Gmail profile response")?;

    let email = profile.email_address;
    tracing::info!(email = %email, "Authenticated as");

    // Calculate expiry time
    let expires_at = chrono::Utc::now() + chrono::Duration::seconds(tokens.expires_in as i64);
    let expires_at_str = expires_at.to_rfc3339();

    // Build token entry for storage
    let token_entry = vec![StoredTokenEntry {
        scopes: GMAIL_SCOPES.iter().map(|s| s.to_string()).collect(),
        token: StoredToken {
            access_token: Some(tokens.access_token),
            refresh_token: tokens.refresh_token,
            expires_at: Some(expires_at_str),
            id_token: tokens.id_token,
        },
    }];

    // Store token in secure credential manager (Windows Credential Manager / DPAPI)
    store_token_secure(&email, &token_entry)?;

    // Ensure token directory exists (still needed for yup-oauth2 compatibility path)
    std::fs::create_dir_all(token_dir)
        .context("Failed to create token directory")?;

    // Generate token cache path (used as identifier in database, but file may not exist)
    let token_cache_path = token_dir.join(AccountAuth::generate_token_path(&email));

    tracing::info!(
        email = %email,
        "Token stored securely in credential manager"
    );

    Ok((email, token_cache_path))
}
