//! Gmail API client for fetching Walmart order emails
//!
//! This module handles:
//! - Searching for Walmart order-related emails
//! - Paginating through message lists
//! - Fetching raw email content

use crate::auth::GmailClient;
use anyhow::{Context, Result};
use base64::{engine::general_purpose::URL_SAFE, Engine};
use google_gmail1::client::Error as GmailApiError;
use google_gmail1::api::Scope;
use google_gmail1::hyper::StatusCode;
use mailparse::MailHeaderMap;
use std::time::Duration;
use tokio::time::sleep;

/// Base search query for Walmart order emails
/// Matches: order confirmations, cancellations, shipping updates, delivery/arrival
/// Uses "from:walmart" to match both walmart.com and help@walmart.com
const WALMART_BASE_QUERY: &str =
    "from:walmart subject:(order OR preorder OR canceled OR cancelled OR delivery OR delivered OR shipped OR delayed OR confirmed OR confirmation OR arrived)";

const MAX_FETCH_RETRIES: usize = 4;
const FETCH_BASE_DELAY_MS: u64 = 500;
const FETCH_MAX_DELAY_MS: u64 = 8000;

/// Build the Walmart search query with an optional date filter
pub fn build_walmart_query(days: Option<u32>) -> String {
    match days {
        Some(d) if d > 0 => format!("{} newer_than:{}d", WALMART_BASE_QUERY, d),
        _ => WALMART_BASE_QUERY.to_string(),
    }
}

/// Build the Walmart search query with an absolute date filter.
/// Uses Gmail's `after:` operator for a stable cutoff regardless of when sync runs.
/// `since_date` format: "YYYY-MM-DD"
pub fn build_walmart_query_since(since_date: &str) -> String {
    let gmail_date = since_date.replace('-', "/");
    format!("{} after:{}", WALMART_BASE_QUERY, gmail_date)
}

/// Represents a fetched email message
#[derive(Debug, Clone)]
pub struct FetchedEmail {
    /// Gmail message ID
    pub gmail_id: String,
    /// Gmail thread ID
    pub thread_id: Option<String>,
    /// Email subject
    pub subject: Option<String>,
    /// Short snippet/preview
    pub snippet: Option<String>,
    /// Sender email
    pub sender: Option<String>,
    /// Recipient email
    pub recipient: Option<String>,
    /// Raw email body (decoded from base64)
    pub raw_body: String,
    /// Gmail internal date (milliseconds since epoch)
    pub internal_date: Option<String>,
}

/// Gmail message reference (ID only, for listing)
#[derive(Debug, Clone)]
pub struct MessageRef {
    pub id: String,
    pub thread_id: Option<String>,
}

/// Gmail fetcher for retrieving Walmart order emails
pub struct GmailFetcher {
    client: GmailClient,
    user_id: String,
}

impl GmailFetcher {
    /// Create a new Gmail fetcher for the authenticated user
    pub fn new(client: GmailClient) -> Self {
        Self {
            client,
            user_id: "me".to_string(), // "me" refers to the authenticated user
        }
    }

    /// Create with a specific user ID (for service accounts)
    pub fn with_user_id(client: GmailClient, user_id: String) -> Self {
        Self { client, user_id }
    }

    /// List all message IDs matching the Walmart search query
    ///
    /// This handles pagination automatically to get all matching messages.
    /// Use `days` to limit to emails from the last N days (None = all time).
    pub async fn list_walmart_emails(&self, days: Option<u32>) -> Result<Vec<MessageRef>> {
        let query = build_walmart_query(days);
        self.list_emails_with_query(&query).await
    }

    /// List all message IDs matching a custom search query
    pub async fn list_emails_with_query(&self, query: &str) -> Result<Vec<MessageRef>> {
        let mut all_messages = Vec::new();
        let mut page_token: Option<String> = None;
        let mut had_retries = false;

        tracing::info!("Searching Gmail with query: {}", query);

        loop {
            let mut attempt = 0usize;
            // Build the list request with explicit readonly scope
            let response = loop {
                let mut request = self.client
                    .users()
                    .messages_list(&self.user_id)
                    .q(query)
                    .max_results(500)
                    .add_scope(Scope::Readonly); // Explicit scope

                if let Some(token) = &page_token {
                    request = request.page_token(token);
                }

                // Execute the request with retry for transient errors
                match request.doit().await.context("Failed to list Gmail messages") {
                    Ok((_, response)) => break response,
                    Err(err) => {
                        if is_retryable_email_error(&err) && attempt < MAX_FETCH_RETRIES {
                            let jitter_key = page_token.as_deref().unwrap_or(query);
                            let delay = retry_delay(attempt, jitter_key);
                            tracing::warn!(
                                attempt = attempt + 1,
                                delay_ms = delay.as_millis() as u64,
                                error = %err,
                                "Transient Gmail list error, retrying"
                            );
                            had_retries = true;
                            sleep(delay).await;
                            attempt += 1;
                            continue;
                        }

                        return Err(err);
                    }
                }
            };

            // Extract message references
            if let Some(messages) = response.messages {
                for msg in messages {
                    if let Some(id) = msg.id {
                        all_messages.push(MessageRef {
                            id,
                            thread_id: msg.thread_id,
                        });
                    }
                }
            }

            tracing::debug!("Fetched {} message IDs so far", all_messages.len());

            // Check for more pages
            match response.next_page_token {
                Some(token) => page_token = Some(token),
                None => break,
            }
        }

        if had_retries {
            tracing::warn!(
                total = all_messages.len(),
                "Gmail list required retries; sync may be incomplete if rate limits persist"
            );
        }
        tracing::info!("Found {} Walmart-related emails", all_messages.len());
        Ok(all_messages)
    }

    /// Fetch the full content of a single email in RAW format
    pub async fn fetch_email(&self, message_id: &str) -> Result<FetchedEmail> {
        // Fetch the message in RAW format with explicit readonly scope
        let (_, message) = self.client
            .users()
            .messages_get(&self.user_id, message_id)
            .format("raw")
            .add_scope(Scope::Readonly) // Explicit scope
            .doit()
            .await
            .context(format!("Failed to fetch email {}", message_id))?;

        // The raw field is already the base64-encoded MIME content as Vec<u8>
        // We need to decode it to get the actual email content
        let raw_body = if let Some(ref raw) = message.raw {
            // First convert Vec<u8> to String (it's base64 encoded)
            let raw_str = String::from_utf8_lossy(raw);
            // Then decode the base64
            match URL_SAFE.decode(raw_str.as_bytes()) {
                Ok(bytes) => String::from_utf8_lossy(&bytes).to_string(),
                Err(_) => {
                    // If base64 decode fails, the raw bytes might already be decoded
                    String::from_utf8_lossy(raw).to_string()
                }
            }
        } else {
            String::new()
        };

        // Extract headers for subject, sender, and recipient
        let (subject, sender, recipient) = self.extract_headers(&message);

        Ok(FetchedEmail {
            gmail_id: message.id.unwrap_or_default(),
            thread_id: message.thread_id,
            subject,
            snippet: message.snippet,
            sender,
            recipient,
            raw_body,
            internal_date: message.internal_date.map(|d| d.to_string()),
        })
    }

    /// Fetch the full content of a single email using RAW format (OPTIMIZED - single API call)
    /// This gets the complete MIME message which we then parse for headers and HTML body
    pub async fn fetch_email_full(&self, message_id: &str) -> Result<FetchedEmail> {
        // Single API call with RAW format - contains everything we need
        let (_, message) = self.client
            .users()
            .messages_get(&self.user_id, message_id)
            .format("raw")
            .add_scope(Scope::Readonly)
            .doit()
            .await
            .context(format!("Failed to fetch email {}", message_id))?;

        // The raw field contains the MIME message. The google-gmail1 crate may
        // already base64-decode it, so try decoding first and fall back to using
        // the raw bytes directly if that fails.
        let (raw_body, subject, sender, recipient) = if let Some(ref raw) = message.raw {
            // Try base64 decoding (URL-safe then standard), fall back to raw bytes
            let mime_bytes = {
                let raw_str = String::from_utf8_lossy(raw);
                URL_SAFE.decode(raw_str.as_bytes())
                    .or_else(|_| base64::engine::general_purpose::STANDARD.decode(raw_str.as_bytes()))
                    .unwrap_or_else(|_| raw.clone())
            };

            // Use mailparse to parse the MIME structure
            match mailparse::parse_mail(&mime_bytes) {
                Ok(parsed) => {
                    let subject = parsed.headers.get_first_header("Subject").map(|h| h.get_value());
                    let sender = parsed.headers.get_first_header("From").map(|h| h.get_value());
                    let recipient = parsed.headers.get_first_header("To").map(|h| h.get_value());
                    let html = find_html_part(&parsed).unwrap_or_default();
                    if html.is_empty() {
                        tracing::warn!("No HTML part found for email {}", message_id);
                    }
                    (html, subject, sender, recipient)
                }
                Err(e) => {
                    tracing::warn!("Failed to parse MIME for email {}: {}", message_id, e);
                    (String::new(), None, None, None)
                }
            }
        } else {
            (String::new(), None, None, None)
        };

        Ok(FetchedEmail {
            gmail_id: message.id.unwrap_or_default(),
            thread_id: message.thread_id,
            subject,
            snippet: message.snippet,
            sender,
            recipient,
            raw_body,
            internal_date: message.internal_date.map(|d| d.to_string()),
        })
    }

    /// Fetch the full content of a single email with retries for transient errors.
    /// Returns Ok(None) when the message no longer exists (404).
    pub async fn fetch_email_full_with_retry(&self, message_id: &str) -> Result<Option<FetchedEmail>> {
        let mut attempt = 0usize;

        loop {
            match self.fetch_email_full(message_id).await {
                Ok(email) => return Ok(Some(email)),
                Err(err) => {
                    if is_not_found_email_error(&err) {
                        tracing::warn!(
                            message_id = %message_id,
                            error = %err,
                            "Email no longer exists in Gmail, skipping"
                        );
                        return Ok(None);
                    }

                    if is_retryable_email_error(&err) && attempt < MAX_FETCH_RETRIES {
                        let delay = retry_delay(attempt, message_id);
                        tracing::warn!(
                            message_id = %message_id,
                            attempt = attempt + 1,
                            delay_ms = delay.as_millis() as u64,
                            error = %err,
                            "Transient Gmail fetch error, retrying"
                        );
                        sleep(delay).await;
                        attempt += 1;
                        continue;
                    }

                    return Err(err);
                }
            }
        }
    }

    /// Extract subject, sender, and recipient from message headers
    fn extract_headers(&self, message: &google_gmail1::api::Message) -> (Option<String>, Option<String>, Option<String>) {
        let mut subject = None;
        let mut sender = None;
        let mut recipient = None;

        if let Some(payload) = &message.payload {
            if let Some(headers) = &payload.headers {
                for header in headers {
                    if let (Some(name), Some(value)) = (&header.name, &header.value) {
                        match name.to_lowercase().as_str() {
                            "subject" => subject = Some(value.clone()),
                            "from" => sender = Some(value.clone()),
                            "to" => recipient = Some(value.clone()),
                            _ => {}
                        }
                    }
                }
            }
        }

        (subject, sender, recipient)
    }

    /// Get count of emails matching the Walmart query
    pub async fn count_walmart_emails(&self, days: Option<u32>) -> Result<u32> {
        let query = build_walmart_query(days);
        let (_, response) = self.client
            .users()
            .messages_list(&self.user_id)
            .q(&query)
            .max_results(1)
            .add_scope(Scope::Readonly) // Explicit scope
            .doit()
            .await
            .context("Failed to count Gmail messages")?;

        Ok(response.result_size_estimate.unwrap_or(0))
    }
}

fn retry_delay(attempt: usize, message_id: &str) -> Duration {
    let exp = 2u64.saturating_pow(attempt.min(6) as u32);
    let base = FETCH_BASE_DELAY_MS.saturating_mul(exp);
    let jitter = message_id
        .bytes()
        .take(6)
        .fold(0u64, |acc, b| acc.wrapping_add(b as u64))
        % 250;
    let delay_ms = (base + jitter).min(FETCH_MAX_DELAY_MS);
    Duration::from_millis(delay_ms)
}

fn is_not_found_email_error(error: &anyhow::Error) -> bool {
    status_from_error(error) == Some(StatusCode::NOT_FOUND)
}

pub fn is_retryable_email_error(error: &anyhow::Error) -> bool {
    if let Some(status) = status_from_error(error) {
        if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::REQUEST_TIMEOUT {
            return true;
        }
        if status.is_server_error() {
            return true;
        }
        return false;
    }

    for cause in error.chain() {
        if let Some(api_err) = cause.downcast_ref::<GmailApiError>() {
            match api_err {
                GmailApiError::HttpError(_) | GmailApiError::Io(_) | GmailApiError::JsonDecodeError(_, _) => {
                    return true;
                }
                _ => {}
            }
        }
        if let Some(io_err) = cause.downcast_ref::<std::io::Error>() {
            use std::io::ErrorKind;
            if matches!(
                io_err.kind(),
                ErrorKind::TimedOut
                    | ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::ConnectionRefused
                    | ErrorKind::Interrupted
                    | ErrorKind::UnexpectedEof
            ) {
                return true;
            }
        }
    }

    let msg = error.to_string().to_lowercase();
    msg.contains("timeout")
        || msg.contains("temporarily")
        || msg.contains("rate")
        || msg.contains("unavailable")
}

fn status_from_error(error: &anyhow::Error) -> Option<StatusCode> {
    for cause in error.chain() {
        if let Some(api_err) = cause.downcast_ref::<GmailApiError>() {
            if let GmailApiError::Failure(response) = api_err {
                return Some(response.status());
            }
        }
    }
    None
}

/// Infer email event type from subject or snippet
pub fn infer_event_type(subject: Option<&str>, snippet: Option<&str>) -> &'static str {
    let combined = format!(
        "{} {}",
        subject.unwrap_or(""),
        snippet.unwrap_or("")
    ).to_lowercase();

    if combined.contains("confirmed") || combined.contains("confirmation") || combined.contains("order placed") || combined.contains("thanks for your") {
        "confirmation"
    } else if combined.contains("cancel") {
        "cancellation"
    } else if combined.contains("shipped") || combined.contains("on its way") || combined.contains("tracking") {
        "shipping"
    } else if combined.contains("delivered") || combined.contains("has arrived") {
        "delivery"
    } else if combined.contains("delay") {
        "delay"
    } else {
        "unknown"
    }
}

/// Recursively find the first text/html part in the MIME structure
fn find_html_part(parsed: &mailparse::ParsedMail) -> Option<String> {
    if parsed.ctype.mimetype == "text/html" {
        return parsed.get_body().ok();
    }

    for subpart in &parsed.subparts {
        if let Some(html) = find_html_part(subpart) {
            return Some(html);
        }
    }

    None
}