//! Gmail API client for fetching Walmart order emails
//!
//! This module handles:
//! - Searching for Walmart order-related emails
//! - Paginating through message lists
//! - Fetching raw email content

use crate::auth::GmailClient;
use anyhow::{Context, Result};
use base64::{engine::general_purpose::URL_SAFE, Engine};
use google_gmail1::api::Scope;

/// Base search query for Walmart order emails
/// Matches: order confirmations, cancellations, shipping updates, delivery/arrival
/// Uses "from:walmart" to match both walmart.com and help@walmart.com
const WALMART_BASE_QUERY: &str =
    "from:walmart subject:(order OR preorder OR canceled OR cancelled OR delivery OR delivered OR shipped OR delayed OR confirmed OR confirmation OR arrived)";

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

        tracing::info!("Searching Gmail with query: {}", query);

        loop {
            // Build the list request with explicit readonly scope
            let mut request = self.client
                .users()
                .messages_list(&self.user_id)
                .q(query)
                .max_results(500)
                .add_scope(Scope::Readonly); // Explicit scope

            if let Some(token) = &page_token {
                request = request.page_token(token);
            }

            // Execute the request
            let (_, response) = request
                .doit()
                .await
                .context("Failed to list Gmail messages")?;

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

        // Extract headers for subject and sender
        let (subject, sender) = self.extract_headers(&message);

        Ok(FetchedEmail {
            gmail_id: message.id.unwrap_or_default(),
            thread_id: message.thread_id,
            subject,
            snippet: message.snippet,
            sender,
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

        // The raw field contains the base64url-encoded MIME message
        let (raw_body, subject, sender) = if let Some(ref raw) = message.raw {
            let raw_str = String::from_utf8_lossy(raw);

            // Try URL-safe base64 first, then standard base64
            let decoded = URL_SAFE.decode(raw_str.as_bytes())
                .or_else(|_| base64::engine::general_purpose::STANDARD.decode(raw_str.as_bytes()));

            match decoded {
                Ok(bytes) => {
                    let mime_content = String::from_utf8_lossy(&bytes).to_string();
                    // Extract headers from MIME content
                    let (subj, sndr) = extract_headers_from_mime(&mime_content);
                    // Extract HTML from MIME content
                    let html = extract_html_from_mime(&mime_content);
                    (html, subj, sndr)
                }
                Err(_) => {
                    let mime_content = raw_str.to_string();
                    let (subj, sndr) = extract_headers_from_mime(&mime_content);
                    let html = extract_html_from_mime(&mime_content);
                    (html, subj, sndr)
                }
            }
        } else {
            (String::new(), None, None)
        };

        Ok(FetchedEmail {
            gmail_id: message.id.unwrap_or_default(),
            thread_id: message.thread_id,
            subject,
            snippet: message.snippet,
            sender,
            raw_body,
            internal_date: message.internal_date.map(|d| d.to_string()),
        })
    }

    /// Extract subject and sender from message headers
    fn extract_headers(&self, message: &google_gmail1::api::Message) -> (Option<String>, Option<String>) {
        let mut subject = None;
        let mut sender = None;

        if let Some(payload) = &message.payload {
            if let Some(headers) = &payload.headers {
                for header in headers {
                    if let (Some(name), Some(value)) = (&header.name, &header.value) {
                        match name.to_lowercase().as_str() {
                            "subject" => subject = Some(value.clone()),
                            "from" => sender = Some(value.clone()),
                            _ => {}
                        }
                    }
                }
            }
        }

        (subject, sender)
    }

    /// Extract the HTML body from message payload
    fn extract_body_from_payload(&self, message: &google_gmail1::api::Message) -> String {
        if let Some(payload) = &message.payload {
            // Debug: log the payload structure
            tracing::debug!(
                "Payload: mime_type={:?}, has_body={}, has_parts={}, parts_count={}",
                payload.mime_type,
                payload.body.is_some(),
                payload.parts.is_some(),
                payload.parts.as_ref().map(|p| p.len()).unwrap_or(0)
            );

            // Try to find HTML part first, then plain text
            if let Some(body) = self.find_body_part(payload, "text/html") {
                return body;
            }
            if let Some(body) = self.find_body_part(payload, "text/plain") {
                return body;
            }

            // Fallback: if payload itself has body data, decode it
            if let Some(body) = &payload.body {
                if let Some(data) = &body.data {
                    let data_str = String::from_utf8_lossy(data);
                    tracing::debug!("Using payload body directly, data length: {}", data.len());
                    if let Ok(bytes) = URL_SAFE.decode(data_str.as_bytes()) {
                        return String::from_utf8_lossy(&bytes).to_string();
                    }
                }
            }
        }
        String::new()
    }

    /// Recursively search for a body part with the specified MIME type
    fn find_body_part(&self, part: &google_gmail1::api::MessagePart, mime_type: &str) -> Option<String> {
        // Log what we're looking at
        let part_mime = part.mime_type.as_deref().unwrap_or("none");
        let has_data = part.body.as_ref().and_then(|b| b.data.as_ref()).is_some();
        let data_len = part.body.as_ref()
            .and_then(|b| b.data.as_ref())
            .map(|d| d.len())
            .unwrap_or(0);

        tracing::trace!(
            "Examining part: mime={}, has_data={}, data_len={}, num_parts={}",
            part_mime,
            has_data,
            data_len,
            part.parts.as_ref().map(|p| p.len()).unwrap_or(0)
        );

        // First, search in child parts (for multipart messages)
        if let Some(parts) = &part.parts {
            for (i, child) in parts.iter().enumerate() {
                tracing::trace!("Searching child part {}", i);
                if let Some(body) = self.find_body_part(child, mime_type) {
                    return Some(body);
                }
            }
        }

        // Check if this part matches the requested MIME type
        if part_mime == mime_type {
            if let Some(body) = &part.body {
                if let Some(data) = &body.data {
                    // The data field is base64url-encoded bytes
                    // First convert Vec<u8> to String, then decode base64
                    let data_str = String::from_utf8_lossy(data);
                    tracing::debug!("Found {} body, data length: {}", mime_type, data.len());
                    return URL_SAFE.decode(data_str.as_bytes())
                        .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
                        .ok();
                } else {
                    tracing::trace!("Part matches {} but has no data", mime_type);
                }
            }
        }

        None
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

/// Extract Subject and From headers from raw MIME content
fn extract_headers_from_mime(mime_content: &str) -> (Option<String>, Option<String>) {
    let mut subject = None;
    let mut sender = None;

    // Headers end at the first blank line
    let header_end = mime_content.find("\r\n\r\n")
        .or_else(|| mime_content.find("\n\n"))
        .unwrap_or(mime_content.len().min(4096)); // Limit search to first 4KB

    let headers = &mime_content[..header_end];

    for line in headers.lines() {
        let lower = line.to_lowercase();
        if lower.starts_with("subject:") {
            subject = Some(line[8..].trim().to_string());
        } else if lower.starts_with("from:") {
            sender = Some(line[5..].trim().to_string());
        }

        // Stop once we have both
        if subject.is_some() && sender.is_some() {
            break;
        }
    }

    (subject, sender)
}

/// Extract the MIME boundary from a Content-Type header
fn extract_mime_boundary(content: &str) -> Option<String> {
    let lower = content.to_lowercase();
    if let Some(boundary_pos) = lower.find("boundary=") {
        let after_boundary = &content[boundary_pos + 9..];
        // Handle quoted boundary: boundary="value" or unquoted: boundary=value
        if after_boundary.starts_with('"') {
            // Quoted boundary
            if let Some(end_quote) = after_boundary[1..].find('"') {
                return Some(after_boundary[1..end_quote + 1].to_string());
            }
        } else {
            // Unquoted boundary - ends at whitespace, semicolon, or newline
            let end = after_boundary.find(|c: char| c.is_whitespace() || c == ';')
                .unwrap_or(after_boundary.len());
            return Some(after_boundary[..end].to_string());
        }
    }
    None
}

/// Extract HTML content from a raw MIME email message
fn extract_html_from_mime(mime_content: &str) -> String {
    // First, try to extract the MIME boundary if this is a multipart message
    let boundary = extract_mime_boundary(mime_content);

    // Look for Content-Type: text/html section
    let lower = mime_content.to_lowercase();

    // Find the start of the HTML section
    if let Some(html_type_pos) = lower.find("content-type: text/html") {
        // Find the blank line that separates headers from content
        let search_start = html_type_pos;
        // Try both \r\n\r\n and \n\n for the blank line separator
        let blank_line_offset = mime_content[search_start..].find("\r\n\r\n")
            .map(|p| (p, 4))
            .or_else(|| mime_content[search_start..].find("\n\n").map(|p| (p, 2)));

        if let Some((offset, sep_len)) = blank_line_offset {
            let content_start = search_start + offset + sep_len;

            // Check if content is base64 or quoted-printable.
            // Search backward from Content-Type to find the start of this header block
            // (previous blank line or start of content). This ensures we find
            // Content-Transfer-Encoding even when it appears before Content-Type.
            let header_block_start = lower[..html_type_pos]
                .rfind("\n\n")
                .map(|pos| pos + 2)
                .or_else(|| lower[..html_type_pos].rfind("\r\n\r\n").map(|pos| pos + 4))
                .unwrap_or(0);
            let header_section = &lower[header_block_start..content_start];

            // Find the end of this MIME part using the specific boundary if available
            let content = &mime_content[content_start..];
            let content_end = if let Some(ref b) = boundary {
                // Look for the specific MIME boundary: \r\n--boundary or \n--boundary
                let boundary_marker = format!("--{}", b);
                content.find(&format!("\r\n{}", boundary_marker))
                    .or_else(|| content.find(&format!("\n{}", boundary_marker)))
                    .unwrap_or(content.len())
            } else {
                content.len()
            };

            let part_content = &content[..content_end];

            if header_section.contains("base64") {
                // Decode base64
                let cleaned = part_content.replace(['\r', '\n', ' '], "");
                if let Ok(bytes) = URL_SAFE.decode(&cleaned).or_else(|_| {
                    base64::engine::general_purpose::STANDARD.decode(&cleaned)
                }) {
                    return String::from_utf8_lossy(&bytes).to_string();
                }
            } else if header_section.contains("quoted-printable") {
                // Decode quoted-printable
                if let Ok(bytes) = quoted_printable::decode(
                    part_content.as_bytes(),
                    quoted_printable::ParseMode::Robust
                ) {
                    return String::from_utf8_lossy(&bytes).to_string();
                }
            } else {
                // Plain content (7bit or 8bit)
                return part_content.to_string();
            }
        }
    }

    // Fallback: The entire content might be quoted-printable encoded HTML
    // Try to decode the whole thing and look for HTML
    if let Ok(decoded_bytes) = quoted_printable::decode(
        mime_content.as_bytes(),
        quoted_printable::ParseMode::Robust
    ) {
        let decoded = String::from_utf8_lossy(&decoded_bytes);
        if let Some(html_start) = decoded.find("<!DOCTYPE html").or_else(|| decoded.find("<!doctype html")) {
            if let Some(html_end) = decoded[html_start..].find("</html>") {
                return decoded[html_start..html_start + html_end + 7].to_string();
            }
        }
        if let Some(html_start) = decoded.find("<html") {
            if let Some(html_end) = decoded[html_start..].find("</html>") {
                return decoded[html_start..html_start + html_end + 7].to_string();
            }
        }
    }

    // Last fallback: look for raw HTML tags (might be 7bit encoded)
    if let Some(html_start) = mime_content.find("<!DOCTYPE html").or_else(|| mime_content.find("<!doctype html")) {
        if let Some(html_end) = mime_content[html_start..].find("</html>") {
            return mime_content[html_start..html_start + html_end + 7].to_string();
        }
    }

    if let Some(html_start) = mime_content.find("<html") {
        if let Some(html_end) = mime_content[html_start..].find("</html>") {
            return mime_content[html_start..html_start + html_end + 7].to_string();
        }
    }

    String::new()
}
