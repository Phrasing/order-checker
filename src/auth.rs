//! OAuth2 authentication module for Gmail API
//!
//! Handles authentication using client_secret.json and persists
//! tokens to a local cache file for reuse across sessions.
//!
//! Supports multiple Gmail accounts with separate token caches.

use anyhow::{anyhow, Context, Result};
use google_gmail1::api::Scope;
use google_gmail1::hyper::client::HttpConnector;
use google_gmail1::hyper_rustls::HttpsConnector;
use google_gmail1::{hyper, hyper_rustls, oauth2, Gmail};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

/// Default paths for credential files
pub const DEFAULT_CLIENT_SECRET_PATH: &str = "client_secret.json";
pub const DEFAULT_TOKEN_CACHE_PATH: &str = "token_cache.json";

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

/// Convenience function using default paths
pub async fn authenticate() -> Result<GmailClient> {
    get_gmail_client(
        Path::new(DEFAULT_CLIENT_SECRET_PATH),
        Path::new(DEFAULT_TOKEN_CACHE_PATH),
    )
    .await
}

/// Check if credentials are already cached
pub fn has_cached_token() -> bool {
    Path::new(DEFAULT_TOKEN_CACHE_PATH).exists()
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
pub async fn get_gmail_client_for_account(
    client_secret_path: &Path,
    account_auth: &AccountAuth,
) -> Result<GmailClient> {
    tracing::info!(email = %account_auth.email, "Initializing Gmail authentication for account...");

    // Read the client secret file
    let secret = oauth2::read_application_secret(client_secret_path)
        .await
        .context("Failed to read client_secret.json. Make sure the file exists and is valid.")?;

    // Build the authenticator with token persistence for this specific account
    let auth = oauth2::InstalledFlowAuthenticator::builder(
        secret,
        oauth2::InstalledFlowReturnMethod::HTTPRedirect,
    )
    .persist_tokens_to_disk(&account_auth.token_cache_path)
    .build()
    .await
    .context("Failed to build authenticator")?;

    // Pre-authorize to ensure we have valid tokens
    auth.token(GMAIL_SCOPES)
        .await
        .context("Failed to get access token. You may need to re-authenticate.")?;

    tracing::info!(email = %account_auth.email, "Gmail authentication successful");

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
pub async fn authenticate_new_account(
    client_secret_path: &Path,
) -> Result<(String, PathBuf)> {
    tracing::info!("Starting OAuth flow for new account...");

    // Use a temporary token path first
    let temp_path = PathBuf::from("token_cache_temp.json");

    // Read the client secret file
    let secret = oauth2::read_application_secret(client_secret_path)
        .await
        .context("Failed to read client_secret.json")?;

    // Build the authenticator
    let auth = oauth2::InstalledFlowAuthenticator::builder(
        secret.clone(),
        oauth2::InstalledFlowReturnMethod::HTTPRedirect,
    )
    .persist_tokens_to_disk(&temp_path)
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

    // Generate the permanent token path
    let permanent_path = AccountAuth::generate_token_path(&email);

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
