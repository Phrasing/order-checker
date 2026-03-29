//! Email parsing module for Walmart Order Reconciler
//!
//! This module handles parsing of Walmart confirmation and cancellation emails.
//! The key challenges addressed:
//!
//! 1. **Dynamic CSS Classes**: Walmart emails use classes like `productName-0-36726-75`
//!    where the numbers change randomly. We use regex-based fuzzy matching.
//!
//! 2. **Order ID Normalization**: Confirmation emails use hyphenated IDs ("2000141-70653310")
//!    while cancellation emails use plain IDs ("200014170653310"). We normalize to plain format.
//!
//! 3. **Robust Price Extraction**: Prices are found via `$` symbols in elements
//!    with class patterns matching `priceStyling`.

use crate::models::{ItemStatus, LineItem, OrderStatus, WalmartOrder};
use chrono::{DateTime, TimeZone, Utc};
use regex::Regex;
use scraper::{ElementRef, Html, Selector};
use std::collections::HashMap;
use std::sync::OnceLock;
use thiserror::Error;

/// Maximum depth to walk up DOM tree when searching for parent row elements.
/// Store delivery emails have deeply nested tables (span → td → tr → table → td → tr)
/// requiring depth 5-6. Using 8 for safety margin.
const MAX_ANCESTRY_DEPTH: usize = 8;

/// Cached fallback regex for order ID extraction (compiled once)
fn fallback_order_id_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| Regex::new(r"#(\d{7,}-\d{5,})").expect("Invalid fallback regex"))
}

/// Cached nested span regex for order ID extraction (compiled once)
fn nested_span_order_id_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r"(?i)order\s*(?:number|#|num\.?)?[:\s]*<a[^>]*>\s*<span[^>]*>([0-9\-]{10,})</span>"
        ).expect("Invalid nested span regex")
    })
}

fn order_id_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r"(?i)order\s*(?:number|#|num\.?)?[:\s]*(?:<[^>]*>)*\s*([0-9-]{10,})")
            .expect("Invalid order ID regex")
    })
}

fn price_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| Regex::new(r"\$\s*([0-9,]+\.?\d*)").expect("Invalid price regex"))
}

fn product_class_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r"(?i)productName[a-zA-Z0-9_-]*").expect("Invalid product class regex")
    })
}

fn price_class_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r"(?i)priceStyling[a-zA-Z0-9_-]*").expect("Invalid price class regex")
    })
}

fn date_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r"(?i)(?:ordered\s+on|order\s+date|placed\s+on)[:\s]*(?:\w+,\s+)?(\w+\s+\d{1,2},?\s+\d{4})"
        )
        .expect("Invalid date regex")
    })
}

fn alt_item_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r#"alt\s*=\s*["']quantity\s+(\d+)\s+item\s+([^"']+)["']"#)
            .expect("Invalid alt item regex")
    })
}

/// Matches an `<img ...>` or `<img ... />` tag (non-greedy).
fn img_tag_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r"(?is)<img\s[^>]*?>").expect("Invalid img tag regex")
    })
}

/// Extracts the value of a `src` attribute from an HTML tag fragment.
fn src_attr_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r#"src\s*=\s*["']([^"']+)["']"#).expect("Invalid src attr regex")
    })
}

fn alt_item_text_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r#"(?i)^\s*quantity\s+(\d+)\s+item\s+(.+?)\s*$"#)
            .expect("Invalid alt item text regex")
    })
}

fn item_name_class_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r"(?i)itemName[a-zA-Z0-9_-]*").expect("Invalid item name class regex")
    })
}

fn tracking_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r"(?i)(fedex|ups|usps|ontrac)\s+tracking\s+number\s*(?:<a[^>]*>)?([A-Z0-9]{10,30})(?:</a>)?"
        )
        .expect("Invalid tracking regex")
    })
}

fn html_tag_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| Regex::new(r"<[^>]+>").expect("Invalid html tag regex"))
}

fn whitespace_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| Regex::new(r"\s+").expect("Invalid whitespace regex"))
}

fn order_total_selector() -> Option<&'static Selector> {
    static SELECTOR: OnceLock<Option<Selector>> = OnceLock::new();
    SELECTOR.get_or_init(|| Selector::parse(r#"[automation-id="order-total"]"#).ok()).as_ref()
}

fn row_selector() -> Option<&'static Selector> {
    static SELECTOR: OnceLock<Option<Selector>> = OnceLock::new();
    SELECTOR.get_or_init(|| Selector::parse("tr").ok()).as_ref()
}

fn td_selector() -> Option<&'static Selector> {
    static SELECTOR: OnceLock<Option<Selector>> = OnceLock::new();
    SELECTOR.get_or_init(|| Selector::parse("td").ok()).as_ref()
}

fn img_selector() -> Option<&'static Selector> {
    static SELECTOR: OnceLock<Option<Selector>> = OnceLock::new();
    SELECTOR.get_or_init(|| Selector::parse("img").ok()).as_ref()
}

fn class_scan_selector() -> Option<&'static Selector> {
    static SELECTOR: OnceLock<Option<Selector>> = OnceLock::new();
    SELECTOR.get_or_init(|| Selector::parse("[class]").ok()).as_ref()
}

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Order ID not found in email")]
    OrderIdNotFound,

    #[error("Order date not found in email")]
    OrderDateNotFound,

    #[error("Failed to parse date: {0}")]
    DateParseError(String),
}

/// Result type for parsing operations
pub type ParseResult<T> = Result<T, ParseError>;

/// Email type detected during parsing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmailType {
    Confirmation,
    Cancellation,
    Shipping,
    Delivery,
    Unknown,
}

/// Case-insensitive byte search for ASCII needles. O(n*m) but fast for short needles.
/// Both haystack and needle are compared byte-by-byte with ASCII lowercasing.
fn find_case_insensitive(haystack: &str, needle: &str) -> Option<usize> {
    let needle_bytes = needle.as_bytes();
    let needle_len = needle_bytes.len();
    if needle_len > haystack.len() {
        return None;
    }
    let haystack_bytes = haystack.as_bytes();
    'outer: for pos in 0..=(haystack_bytes.len() - needle_len) {
        for offset in 0..needle_len {
            if haystack_bytes[pos + offset].to_ascii_lowercase()
                != needle_bytes[offset].to_ascii_lowercase()
            {
                continue 'outer;
            }
        }
        return Some(pos);
    }
    None
}

/// Parser for Walmart order emails
pub struct WalmartEmailParser;

impl Default for WalmartEmailParser {
    fn default() -> Self {
        Self::new()
    }
}

struct ParseContext<'a> {
    html: &'a str,
    lower: String,
    document: Html,
}

impl WalmartEmailParser {
    pub fn new() -> Self {
        Self
    }

    /// Detect the type of email based on content
    pub fn detect_email_type(&self, html: &str) -> EmailType {
        self.detect_email_type_raw(html)
    }

    /// Detect email type from subject line alone (high-confidence, short text).
    /// Returns `None` if the subject is inconclusive.
    /// Returns `Some(Unknown)` for known non-order emails to prevent HTML fallback
    /// from misclassifying promotional or membership content.
    pub fn detect_email_type_from_subject(&self, subject: &str) -> Option<EmailType> {
        let lower = subject.to_lowercase();

        // Non-order emails — return Unknown to prevent HTML body fallback misclassification.
        // Membership notices (e.g. "Your Walmart+ membership was canceled") contain "cancel"
        // but aren't order cancellations. Promotional emails (e.g. "Get free delivery with
        // code EXPRESS") contain delivery-adjacent keywords but aren't delivery notifications.
        if lower.contains("membership") || lower.contains("walmart+") {
            return Some(EmailType::Unknown);
        }
        if lower.contains("free delivery") || lower.contains("promo code") {
            return Some(EmailType::Unknown);
        }

        if lower.contains("cancel") {
            return Some(EmailType::Cancellation);
        }
        if lower.contains("delivered") || lower.contains("arrived") {
            return Some(EmailType::Delivery);
        }
        if lower.contains("shipped") || lower.contains("on its way") || lower.contains("on the way") {
            return Some(EmailType::Shipping);
        }
        if lower.contains("confirmed") || lower.contains("confirmation")
            || lower.contains("thanks for your delivery order")
            || lower.contains("thanks for your order")
        {
            return Some(EmailType::Confirmation);
        }
        None
    }

    /// Detect email type from raw HTML without any allocation.
    ///
    /// Uses case-insensitive byte search (`find_case_insensitive`) so the full
    /// 100-200KB email can be scanned without a `to_lowercase()` copy.
    /// Same priority order as `detect_email_type_lower`.
    pub(crate) fn detect_email_type_raw(&self, html: &str) -> EmailType {
        // 0. Promotional/marketing emails — Walmart campaign emails contain
        //    `<!-- Campaign: BAT-... -->` HTML comments. These often include delivery
        //    keywords ("delivered to your door") that would otherwise misclassify them.
        if find_case_insensitive(html, "<!-- campaign:").is_some() {
            return EmailType::Unknown;
        }

        // 1. Cancellation
        if find_case_insensitive(html, "order cancel").is_some()
            || find_case_insensitive(html, "item cancel").is_some()
            || find_case_insensitive(html, "been canceled").is_some()
            || find_case_insensitive(html, "was canceled").is_some()
            || find_case_insensitive(html, "is canceled").is_some()
            || find_case_insensitive(html, "delivery canceled").is_some()
            || find_case_insensitive(html, "has been cancelled").is_some()
            || find_case_insensitive(html, "was cancelled").is_some()
        {
            EmailType::Cancellation
        // 2. Delivery — require delivery-specific phrases, not bare "delivered" which
        //    matches promotional copy like "Get items delivered" or "free delivery".
        } else if find_case_insensitive(html, "has been delivered").is_some()
            || find_case_insensitive(html, "was delivered").is_some()
            || find_case_insensitive(html, "delivered to").is_some()
            || find_case_insensitive(html, "order delivered").is_some()
            || find_case_insensitive(html, "package delivered").is_some()
            || find_case_insensitive(html, "has arrived").is_some()
            || find_case_insensitive(html, "package arrived").is_some()
            || find_case_insensitive(html, "item arrived").is_some()
        {
            EmailType::Delivery
        } else if find_case_insensitive(html, "shipped").is_some()
            || find_case_insensitive(html, "on its way").is_some()
        {
            EmailType::Shipping
        } else if find_case_insensitive(html, "order confirmed").is_some()
            || find_case_insensitive(html, "order confirmation").is_some()
            || find_case_insensitive(html, "thanks for your order").is_some()
        {
            EmailType::Confirmation
        } else {
            EmailType::Unknown
        }
    }

    /// Detect email type from full HTML body (lower-confidence fallback).
    ///
    /// Priority order: Cancellation → Delivery → Shipping → Confirmation
    /// Specific types are checked first because generic confirmation-like text
    /// ("order confirmation", "thanks for your order") often appears in footer
    /// links and boilerplate across ALL email types.
    pub(crate) fn detect_email_type_lower(&self, lower: &str) -> EmailType {
        // 0. Promotional/marketing emails — Walmart campaign emails contain
        //    `<!-- campaign: bat-...` markers (already lowered). Skip before
        //    keyword matching to prevent misclassification.
        if lower.contains("<!-- campaign:") {
            return EmailType::Unknown;
        }

        // 1. Cancellation — most specific keywords
        if lower.contains("order cancel")
            || lower.contains("item cancel")
            || lower.contains("been canceled")
            || lower.contains("was canceled")
            || lower.contains("is canceled")
            || lower.contains("delivery canceled")
            || lower.contains("has been cancelled")
            || lower.contains("was cancelled")
        {
            EmailType::Cancellation
        // 2. Delivery — require delivery-specific phrases, not bare "delivered"
        //    which matches promotional copy (delivery emails also contain
        //    "Sold and shipped by Walmart", so shipping check must follow)
        } else if lower.contains("has been delivered")
            || lower.contains("was delivered")
            || lower.contains("delivered to")
            || lower.contains("order delivered")
            || lower.contains("package delivered")
            || lower.contains("has arrived")
            || lower.contains("package arrived")
            || lower.contains("item arrived")
        {
            EmailType::Delivery
        // 3. Shipping — specific verb phrases only
        //    NOTE: "tracking" removed — it appears in URLs across ALL Walmart email types
        } else if lower.contains("shipped") || lower.contains("on its way") {
            EmailType::Shipping
        // 4. Confirmation — general catch-all, checked last
        //    Many email types have "order confirmation" in footer links
        } else if lower.contains("order confirmed")
            || lower.contains("order confirmation")
            || lower.contains("thanks for your order")
        {
            EmailType::Confirmation
        } else {
            EmailType::Unknown
        }
    }

    /// Extract and normalize the order ID from email HTML
    ///
    /// CRITICAL: This always returns a normalized (hyphen-free) ID
    pub fn extract_order_id(&self, html: &str) -> ParseResult<String> {
        // Try the standard "Order number" pattern first
        if let Some(captures) = order_id_pattern().captures(html) {
            if let Some(id_match) = captures.get(1) {
                let raw_id = id_match.as_str();
                // NORMALIZE: Remove all hyphens to create consistent ID format
                let normalized = WalmartOrder::normalize_id(raw_id);
                tracing::info!("✓ Order ID extracted (primary pattern): raw='{}', normalized='{}'", raw_id, normalized);
                return Ok(normalized);
            }
        }

        // Fallback: Try to find #XXXXXXX-XXXXXXXX format (used in delivery emails)
        if let Some(captures) = fallback_order_id_pattern().captures(html) {
            if let Some(id_match) = captures.get(1) {
                let raw_id = id_match.as_str();
                let normalized = WalmartOrder::normalize_id(raw_id);
                tracing::warn!("✓ Order ID extracted (fallback pattern): raw='{}', normalized='{}'", raw_id, normalized);
                return Ok(normalized);
            }
        }

        // Fallback: Order ID nested in <span> inside <a> tag (delivery cancellation emails)
        if let Some(captures) = nested_span_order_id_pattern().captures(html) {
            if let Some(id_match) = captures.get(1) {
                let raw_id = id_match.as_str();
                let normalized = WalmartOrder::normalize_id(raw_id);
                tracing::warn!("✓ Order ID extracted (nested span pattern): raw='{}', normalized='{}'", raw_id, normalized);
                return Ok(normalized);
            }
        }

        tracing::error!("✗ Failed to extract order ID from email. HTML preview: {}...",
            &html.chars().take(500).collect::<String>());
        Err(ParseError::OrderIdNotFound)
    }

    /// Extract tracking number and carrier from shipping email HTML
    ///
    /// Returns (carrier, tracking_number) if found
    /// Supports: FedEx, UPS, USPS, OnTrac
    pub fn extract_tracking_info(&self, html: &str) -> Option<(String, String)> {
        if let Some(captures) = tracking_pattern().captures(html) {
            if let (Some(carrier_match), Some(tracking_match)) = (captures.get(1), captures.get(2)) {
                let carrier = carrier_match.as_str().to_string();
                let tracking_number = tracking_match.as_str().to_string();
                return Some((carrier, tracking_number));
            }
        }
        None
    }

    /// Extract the order date from email HTML
    pub fn extract_order_date(&self, html: &str) -> ParseResult<DateTime<Utc>> {
        if let Some(captures) = date_pattern().captures(html) {
            if let Some(date_match) = captures.get(1) {
                let date_str = date_match.as_str();
                // Try to parse common date formats
                return self.parse_date(date_str);
            }
        }
        Err(ParseError::OrderDateNotFound)
    }

    /// Parse a date string into DateTime<Utc>
    fn parse_date(&self, date_str: &str) -> ParseResult<DateTime<Utc>> {
        // Try common formats
        let formats = [
            "%B %d, %Y",     // January 15, 2024
            "%B %d %Y",      // January 15 2024
            "%b %d, %Y",     // Jan 15, 2024
            "%b %d %Y",      // Jan 15 2024
            "%m/%d/%Y",      // 01/15/2024
            "%Y-%m-%d",      // 2024-01-15
        ];

        let cleaned = date_str.trim().replace(",", " ").replace("  ", " ");

        for fmt in formats {
            if let Ok(naive) = chrono::NaiveDate::parse_from_str(&cleaned, fmt) {
                return Ok(Utc.from_utc_datetime(
                    &naive.and_hms_opt(0, 0, 0).unwrap()
                ));
            }
        }

        Err(ParseError::DateParseError(date_str.to_string()))
    }

    /// Find the byte offset where `<body` begins (or 0 if not found).
    /// Skipping `<head>` content avoids tokenizing ~24% of the HTML (CSS/styles)
    /// that is never queried during extraction.
    fn body_start_offset(html: &str) -> usize {
        let bytes = html.as_bytes();
        let len = bytes.len();
        for pos in 0..len.saturating_sub(5) {
            if bytes[pos] == b'<'
                && bytes[pos + 1].to_ascii_lowercase() == b'b'
                && bytes[pos + 2].to_ascii_lowercase() == b'o'
                && bytes[pos + 3].to_ascii_lowercase() == b'd'
                && bytes[pos + 4].to_ascii_lowercase() == b'y'
            {
                return pos;
            }
        }
        0 // no <body> tag found — use full HTML
    }

    fn build_context<'a>(&self, html: &'a str) -> ParseContext<'a> {
        let _span = tracing::debug_span!("build_context", html_len = html.len()).entered();

        // Strip <head> section — CSS/styles generate DOM nodes that are
        // tokenized and tree-built but never queried. All extraction operates
        // on <body> content.
        let body_html = &html[Self::body_start_offset(html)..];
        let lower = body_html.to_lowercase();

        let document = {
            let _dom_span = tracing::debug_span!("html_parse_document").entered();
            Html::parse_document(body_html)
        };

        ParseContext {
            html: body_html,
            lower,
            document,
        }
    }

    /// Find the byte offset of the earliest p13n/recommendation marker in the HTML.
    ///
    /// Everything after this offset is promotional content (recommended products,
    /// "continue your shopping", etc.) that we actively filter out during extraction.
    /// Truncating the HTML here before DOM parsing eliminates 60-80% of the document
    /// and removes the need for expensive ancestor walks.
    ///
    /// Uses case-insensitive byte search on the original HTML to avoid a full
    /// `to_lowercase()` allocation just for cutoff detection.
    fn recommendations_cutoff(html: &str) -> usize {
        let p13n_markers = [
            "automation-id=\"p13n-module\"",
            "automation-id='p13n-module'",
            "p13n-products",
            "you might also like",
            "recommended for you",
            "more from walmart",
            "based on your order",
            "continueyourshopping",
        ];

        let mut earliest_pos = html.len();
        for marker in p13n_markers {
            if let Some(pos) = find_case_insensitive(html, marker) {
                if pos < earliest_pos {
                    earliest_pos = pos;
                }
            }
        }

        // Walk backward to the last `>` that closes a complete tag, so we
        // never leave a partial/unclosed tag that html5ever might misinterpret.
        if earliest_pos < html.len() {
            let bytes = html.as_bytes();
            while earliest_pos > 0 && bytes[earliest_pos - 1] != b'>' {
                earliest_pos -= 1;
            }
        }

        earliest_pos
    }

    /// Extract total price from email
    pub fn extract_total_price(&self, html: &str) -> Option<f64> {
        let cutoff = Self::recommendations_cutoff(html);
        let context = self.build_context(&html[..cutoff]);
        self.extract_total_price_with_context(&context)
    }

    fn extract_total_price_with_context(&self, context: &ParseContext<'_>) -> Option<f64> {
        let document = &context.document;

        // Strategy 1: Look for automation-id="order-total" (Walmart's standard marker)
        if let Some(selector) = order_total_selector() {
            for element in document.select(selector) {
                if let Some(price) = self.extract_total_from_order_total_element(&element) {
                    return Some(price);
                }
            }
        }

        // Strategy 2: Look for "Includes all fees, taxes and discounts" marker
        if let Some(pos) = context.lower.find("includes all fees") {
            // Look for price within 300 chars after this marker
            let search_region = &context.html[pos..std::cmp::min(pos + 300, context.html.len())];
            if let Some(price) = self.extract_first_price(search_region) {
                return Some(price);
            }
        }

        // Strategy 3: Look for totalChargedClass pattern (Walmart's charged amount)
        if let Some(pos) = context.lower.find("totalchargedclass") {
            let search_region = &context.html[pos..std::cmp::min(pos + 200, context.html.len())];
            if let Some(price) = self.extract_first_price(search_region) {
                return Some(price);
            }
        }

        // Strategy 4: Look for elements containing "total" near a price
        let total_patterns = ["order total", "total:", "grand total", "total amount"];

        for pattern in total_patterns {
            if let Some(pos) = context.lower.find(pattern) {
                // Look for price within 200 chars after "total"
                let search_region = &context.html[pos..std::cmp::min(pos + 200, context.html.len())];
                if let Some(price) = self.extract_first_price(search_region) {
                    return Some(price);
                }
            }
        }

        // Fallback: look for price styling elements
        self.extract_price_from_styled_elements(&document)
    }

    /// Extract total from the Walmart order-total section, ignoring "you saved" rows.
    fn extract_total_from_order_total_element(&self, element: &ElementRef) -> Option<f64> {
        let mut fallback_prices: Vec<f64> = Vec::new();

        if let Some(selector) = row_selector() {
            for row in element.select(selector) {
                let row_text: String = row.text().collect();
                let row_text_trimmed = row_text.trim();
                if row_text_trimmed.is_empty() {
                    continue;
                }

                let row_lower = row_text_trimmed.to_lowercase();

                if row_lower.contains("you saved")
                    || row_lower.contains("saved a total")
                    || row_lower.contains("total savings")
                {
                    continue;
                }

                if row_lower.contains("includes all fees") {
                    if let Some(price) = self.extract_first_price(row_text_trimmed) {
                        return Some(price);
                    }
                }

                if row_lower.contains("order total")
                    || row_lower.contains("grand total")
                    || row_lower.contains("total amount")
                    || (row_lower.contains("total")
                        && !row_lower.contains("subtotal")
                        && !row_lower.contains("shipping")
                        && !row_lower.contains("tax")
                        && !row_lower.contains("discount")
                        && !row_lower.contains("savings"))
                {
                    if let Some(price) = self.extract_first_price(row_text_trimmed) {
                        return Some(price);
                    }
                }

                if let Some(price) = self.extract_first_price(row_text_trimmed) {
                    fallback_prices.push(price);
                }
            }
        }

        if let Some(price) = fallback_prices.last().copied() {
            return Some(price);
        }

        // Fallback: scan <td> elements directly (handles deeply nested table structures
        // where <tr> iteration might miss price-bearing cells)
        if let Some(td_sel) = td_selector() {
            let mut last_td_price: Option<f64> = None;
            for td in element.select(td_sel) {
                let td_text: String = td.text().collect();
                let td_trimmed = td_text.trim();
                if td_trimmed.is_empty() {
                    continue;
                }
                let td_lower = td_trimmed.to_lowercase();
                if td_lower.contains("you saved") || td_lower.contains("savings") {
                    continue;
                }
                if td_lower.contains("includes all fees") || td_lower.contains("order total") {
                    if let Some(price) = self.extract_first_price(td_trimmed) {
                        return Some(price);
                    }
                }
                if let Some(price) = self.extract_first_price(td_trimmed) {
                    last_td_price = Some(price);
                }
            }
            if let Some(price) = last_td_price {
                return Some(price);
            }
        }

        let text: String = element.text().collect();
        self.extract_last_price_excluding_savings(&text)
    }

    fn extract_last_price_excluding_savings(&self, text: &str) -> Option<f64> {
        let lower = text.to_lowercase();
        let mut cutoff: Option<usize> = None;

        for marker in ["you saved", "saved a total", "total savings"] {
            if let Some(pos) = lower.find(marker) {
                cutoff = Some(match cutoff {
                    Some(existing) => existing.min(pos),
                    None => pos,
                });
            }
        }

        if let Some(pos) = cutoff {
            return self.extract_last_price(&text[..pos]);
        }

        self.extract_last_price(text)
    }

    /// Extract the first price found in a string
    fn extract_first_price(&self, text: &str) -> Option<f64> {
        if let Some(captures) = price_pattern().captures(text) {
            if let Some(price_match) = captures.get(1) {
                let price_str = price_match.as_str().replace(',', "");
                return price_str.parse().ok();
            }
        }
        None
    }

    /// Extract the last price found in a string (useful for order totals)
    fn extract_last_price(&self, text: &str) -> Option<f64> {
        let mut last_price = None;
        for captures in price_pattern().captures_iter(text) {
            if let Some(price_match) = captures.get(1) {
                let price_str = price_match.as_str().replace(',', "");
                if let Ok(price) = price_str.parse::<f64>() {
                    last_price = Some(price);
                }
            }
        }
        last_price
    }

    /// Extract price from elements with priceStyling-like classes
    fn extract_price_from_styled_elements(&self, document: &Html) -> Option<f64> {
        if let Some(selector) = class_scan_selector() {
            for element in document.select(selector) {
                if let Some(class) = element.value().attr("class") {
                    if price_class_pattern().is_match(class) {
                        let text: String = element.text().collect();
                        if let Some(price) = self.extract_first_price(&text) {
                            return Some(price);
                        }
                    }
                }
            }
        }
        None
    }

    /// Helper to get text content from an ElementRef
    fn get_element_text(element: &ElementRef) -> String {
        element.text().collect::<String>().trim().to_string()
    }

    fn is_product_image_url(src: &str) -> bool {
        if src.is_empty() {
            return false;
        }

        let lower = src.to_ascii_lowercase();
        if !lower.contains("walmartimages.com") {
            return false;
        }

        if lower.contains("/dfw/")
            || lower.contains("w-mt.co")
            || lower.contains("rptrcks")
            || lower.contains("pixel")
        {
            return false;
        }

        true
    }

    fn find_best_product_image(root: &ElementRef) -> Option<String> {
        let img_selector = img_selector()?;
        for img in root.select(img_selector) {
            if let Some(src) = img.value().attr("src") {
                if Self::is_product_image_url(src) {
                    return Some(src.to_string());
                }
            }
        }

        None
    }

    /// Extract line items from email HTML using fuzzy class matching
    pub fn extract_items(&self, html: &str) -> Vec<LineItem> {
        let cutoff = Self::recommendations_cutoff(html);
        let context = self.build_context(&html[..cutoff]);
        self.extract_items_with_context(&context)
    }

    fn extract_items_with_context(&self, context: &ParseContext<'_>) -> Vec<LineItem> {
        // Strategy 1: Extract items from img alt attributes (shipping email format)
        // Format: alt="quantity N item ProductName"
        // This is the most reliable method for shipping/delivery emails
        let items = self.extract_items_from_alt_attributes(context);
        if !items.is_empty() {
            return items;
        }

        // Strategy 1.5: Extract items from itemName-* class (store delivery emails)
        // Store deliveries use "itemName-0-861324-47" instead of "productName-*"
        let items = self.extract_items_from_item_name_class(context);
        if !items.is_empty() {
            return items;
        }

        let mut items = Vec::new();

        // Strategy 2: Look for productName-* class pattern, but ONLY before the p13n section
        // The p13n (personalization) section contains recommended products, not order items
        let document = &context.document;

        let all_selector = match class_scan_selector() {
            Some(selector) => selector,
            None => return items,
        };

        // Single-pass collection: collect products, prices, and images in one iteration (O(n) instead of O(n²))
        // Then match them by common parent element using a unique row identifier

        // Helper to create a unique key for a row element
        fn get_row_key(element: &ElementRef) -> Option<String> {
            // Walk up to find parent row (tr, div, or similar container)
            let mut current = element.parent();
            for _ in 0..5 {
                if let Some(node) = current {
                    if let Some(elem) = ElementRef::wrap(node) {
                        let tag = elem.value().name();
                        if tag == "tr" || tag == "div" {
                            // Use a combination of tag name and text content as unique key
                            let text: String = elem.text().take(100).collect();
                            return Some(format!("{}:{}", tag, text.chars().take(50).collect::<String>()));
                        }
                        current = node.parent();
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            // Fallback: use element's own text if no parent row found
            let text: String = element.text().take(50).collect();
            Some(text)
        }

        struct ProductInfo<'a> {
            name: String,
            row_key: String,
            parent_elem: ElementRef<'a>,
        }

        let mut products: Vec<ProductInfo> = Vec::with_capacity(50);
        let mut prices: HashMap<String, f64> = HashMap::with_capacity(50);
        let mut images: HashMap<String, String> = HashMap::with_capacity(50);

        for element in document.select(all_selector) {
            if let Some(class) = element.value().attr("class") {
                // Collect products
                if product_class_pattern().is_match(class) {
                    let name = Self::get_element_text(&element);
                    if !name.is_empty() && name.len() > 2 {
                        if let Some(row_key) = get_row_key(&element) {
                            if let Some(parent) = element.parent() {
                                if let Some(parent_elem) = ElementRef::wrap(parent) {
                                    products.push(ProductInfo { name, row_key, parent_elem });
                                }
                            }
                        }
                    }
                }

                // Collect prices (indexed by row key)
                if price_class_pattern().is_match(class) {
                    let text = Self::get_element_text(&element);
                    if let Some(price) = self.extract_first_price(&text) {
                        if let Some(row_key) = get_row_key(&element) {
                            prices.insert(row_key, price);
                        }
                    }
                }
            }
        }

        // Match products to prices by common row key and extract images
        for product_info in products {
            let mut item = LineItem::new(product_info.name, 1);

            // Add price if found for this row
            if let Some(&price) = prices.get(&product_info.row_key) {
                item = item.with_price(price);
            }

            // Extract image from parent if not already cached
            if !images.contains_key(&product_info.row_key) {
                if let Some(src) = Self::find_best_product_image(&product_info.parent_elem) {
                    images.insert(product_info.row_key.clone(), src.clone());
                    item = item.with_image(src);
                }
            } else if let Some(src) = images.get(&product_info.row_key) {
                item = item.with_image(src.clone());
            }

            items.push(item);
        }

        // Strategy 3: Fallback - look for table rows with product info
        if items.is_empty() {
            items = self.extract_items_from_tables(document);
        }

        items
    }

    /// Extract items from img alt attributes (used in shipping emails)
    /// Format: alt="quantity N item ProductName"
    fn extract_items_from_alt_attributes(&self, context: &ParseContext<'_>) -> Vec<LineItem> {
        let mut items = Vec::new();

        let document = &context.document;
        let img_selector = match img_selector() {
            Some(sel) => sel,
            None => return items,
        };

        let mut index_by_name: HashMap<String, usize> = HashMap::new();
        for img in document.select(img_selector) {
            let alt = match img.value().attr("alt") {
                Some(value) => value.trim(),
                None => continue,
            };
            if alt.is_empty() {
                continue;
            }

            let src = match img.value().attr("src") {
                Some(value) => value.trim(),
                None => continue,
            };

            if !Self::is_product_image_url(src) {
                continue;
            }

            if let Some(captures) = alt_item_text_pattern().captures(alt) {
                if let (Some(qty_match), Some(name_match)) = (captures.get(1), captures.get(2)) {
                    let quantity: u32 = qty_match.as_str().parse().unwrap_or(1);
                    let name = name_match.as_str().trim().to_string();
                    if name.is_empty() || name.len() <= 3 {
                        continue;
                    }

                    let key = name.to_ascii_lowercase();
                    if let Some(existing_idx) = index_by_name.get(&key).copied() {
                        let existing = &mut items[existing_idx];
                        if existing.image_url.is_none() {
                            existing.image_url = Some(src.to_string());
                        }
                        if quantity > existing.quantity {
                            existing.quantity = quantity;
                        }
                    } else {
                        let mut item = LineItem::new(name, quantity);
                        item = item.with_image(src.to_string());
                        index_by_name.insert(key, items.len());
                        items.push(item);
                    }
                }
            }
        }

        if !items.is_empty() {
            return items;
        }

        for captures in alt_item_pattern().captures_iter(context.html) {
            if let (Some(qty_match), Some(name_match)) = (captures.get(1), captures.get(2)) {
                let quantity: u32 = qty_match.as_str().parse().unwrap_or(1);
                let name = name_match.as_str().trim().to_string();

                if !name.is_empty() && name.len() > 3 {
                    let item = LineItem::new(name, quantity);
                    items.push(item);
                }
            }
        }

        items
    }

    /// Extract items from itemName-* class elements (store delivery emails)
    /// These use a different class naming scheme than productName-* (confirmation emails)
    /// Structure: <span class="itemName-0-861324-47">Product Name</span>
    ///   with sibling cells containing "$499.00/EA", "Qty: 1", and image
    fn extract_items_from_item_name_class(&self, context: &ParseContext<'_>) -> Vec<LineItem> {
        let document = &context.document;
        let mut items = Vec::new();

        let all_selector = match class_scan_selector() {
            Some(sel) => sel,
            None => return items,
        };

        for element in document.select(all_selector) {
            if let Some(class) = element.value().attr("class") {
                if !item_name_class_pattern().is_match(class) {
                    continue;
                }

                let name = Self::get_element_text(&element);
                if name.is_empty() || name.len() <= 2 {
                    continue;
                }

                let mut item = LineItem::new(name, 1);

                // Walk up to a <tr> ancestor that contains price info.
                // Store delivery emails have nested tables, so the immediate <tr>
                // may only contain the item name. We need the outer <tr> that spans
                // all three columns (image, details, price).
                // For canceled/pickup emails with no price, fall back to nearest <tr>.
                let mut ancestor = element.parent();
                let mut row_ref: Option<ElementRef> = None;
                let mut nearest_tr: Option<ElementRef> = None;
                for _depth in 0..MAX_ANCESTRY_DEPTH {
                    match ancestor {
                        Some(node) => {
                            if let Some(elem) = ElementRef::wrap(node) {
                                if elem.value().name() == "tr" {
                                    if nearest_tr.is_none() {
                                        nearest_tr = Some(elem);
                                    }
                                    let text: String = elem.text().collect();
                                    if text.contains('$') {
                                        row_ref = Some(elem);
                                        break;
                                    }
                                }
                            }
                            ancestor = node.parent();
                        }
                        None => break,
                    }
                }

                // Fall back to nearest <tr> when no price-containing row found
                if row_ref.is_none() {
                    row_ref = nearest_tr;
                }

                if let Some(row) = row_ref {
                    let row_text: String = row.text().collect();

                    // Extract price from row text (look for $XXX.XX)
                    if let Some(price) = self.extract_first_price(&row_text) {
                        item = item.with_price(price);
                    }

                    // Extract quantity from "Qty: N" pattern
                    let lower_text = row_text.to_lowercase();
                    if let Some(qty_pos) = lower_text.find("qty:") {
                        let after_qty = &row_text[qty_pos + 4..];
                        let qty_str: String = after_qty.trim().chars()
                            .take_while(|chr| chr.is_ascii_digit())
                            .collect();
                        if let Ok(qty) = qty_str.parse::<u32>() {
                            if qty > 0 {
                                item.quantity = qty;
                            }
                        }
                    }

                    if let Some(src) = Self::find_best_product_image(&row) {
                        item = item.with_image(src);
                    }
                }

                items.push(item);
            }
        }

        items
    }

    /// Fallback: Extract items from table structures (common in older email formats)
    fn extract_items_from_tables(&self, document: &Html) -> Vec<LineItem> {
        let mut items = Vec::new();

        // Try to find table rows that look like product listings
        if let Some(tr_selector) = row_selector() {
            for row in document.select(tr_selector) {
                let row_text: String = row.text().collect();

                // Skip header rows and non-product rows
                let lower_row = row_text.to_lowercase();
                if lower_row.contains("item") && lower_row.contains("price") {
                    continue;
                }

                // Look for a price indicator in the row
                if let Some(price) = self.extract_first_price(&row_text) {
                    // Get the first non-price text as product name
                    let mut cells: Vec<String> = Vec::new();
                    if let Some(td_selector) = td_selector() {
                        for cell in row.select(td_selector) {
                            let text = Self::get_element_text(&cell);
                            if !text.is_empty() {
                                cells.push(text);
                            }
                        }
                    }

                    // First cell is usually the product name
                    if let Some(name) = cells.first() {
                        if !name.contains('$') && name.len() > 2 {
                            let item = LineItem::new(name.clone(), 1).with_price(price);
                            items.push(item);
                        }
                    }
                }
            }
        }

        items
    }

    /// Extract total price using string scanning only (no DOM parsing).
    ///
    /// Implements strategies 2-4 from `extract_total_price_with_context`:
    /// - "includes all fees" marker
    /// - "totalchargedclass" marker
    /// - "order total" / "total:" / "grand total" patterns
    ///
    /// Skips strategy 1 (DOM-based `automation-id="order-total"` section) since
    /// the whole point is to avoid DOM parsing.
    fn extract_total_price_regex(&self, html: &str) -> Option<f64> {
        // Strategy 2: "Includes all fees, taxes and discounts"
        if let Some(pos) = find_case_insensitive(html, "includes all fees") {
            let end = std::cmp::min(pos + 300, html.len());
            if let Some(price) = self.extract_first_price(&html[pos..end]) {
                return Some(price);
            }
        }

        // Strategy 3: totalChargedClass
        if let Some(pos) = find_case_insensitive(html, "totalchargedclass") {
            let end = std::cmp::min(pos + 200, html.len());
            if let Some(price) = self.extract_first_price(&html[pos..end]) {
                return Some(price);
            }
        }

        // Strategy 4: "order total", "total:", "grand total"
        let total_patterns = ["order total", "total:", "grand total", "total amount"];
        for pattern in total_patterns {
            if let Some(pos) = find_case_insensitive(html, pattern) {
                let end = std::cmp::min(pos + 200, html.len());
                if let Some(price) = self.extract_first_price(&html[pos..end]) {
                    return Some(price);
                }
            }
        }

        None
    }

    /// Extract items from `<img alt="quantity N item ...">` patterns directly
    /// on raw HTML using regex only — no DOM parsing required.
    ///
    /// This works because shipping/delivery emails embed item info in img alt
    /// attributes. After p13n truncation, all remaining alt-text items are
    /// legitimate order items (no recommendation filtering needed).
    fn extract_items_from_alt_regex(&self, html: &str) -> Vec<LineItem> {
        let mut items: Vec<LineItem> = Vec::new();
        let mut seen_names: HashMap<String, usize> = HashMap::new();

        // Iterate over <img> tags so we can extract both alt (name/qty) and src (image URL)
        for img_match in img_tag_pattern().find_iter(html) {
            let tag = img_match.as_str();

            let captures = match alt_item_pattern().captures(tag) {
                Some(cap) => cap,
                None => continue,
            };

            let (Some(qty_match), Some(name_match)) = (captures.get(1), captures.get(2)) else {
                continue;
            };

            let quantity: u32 = qty_match.as_str().parse().unwrap_or(1);
            let raw_name = name_match.as_str().trim();

            if raw_name.is_empty() || raw_name.len() <= 3 {
                continue;
            }

            // Decode HTML entities in the name (e.g., &amp; → &)
            let name = raw_name
                .replace("&amp;", "&")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", "\"")
                .replace("&#39;", "'");

            // Extract image URL from the src attribute of the same <img> tag
            let image_url = src_attr_pattern()
                .captures(tag)
                .and_then(|cap| cap.get(1))
                .map(|m| m.as_str().to_string())
                .filter(|src| Self::is_product_image_url(src));

            let key = name.to_ascii_lowercase();
            if let Some(&existing_idx) = seen_names.get(&key) {
                // Deduplicate: keep the higher quantity
                if quantity > items[existing_idx].quantity {
                    items[existing_idx].quantity = quantity;
                }
                // Fill in image if missing from a previous match
                if items[existing_idx].image_url.is_none() {
                    items[existing_idx].image_url = image_url;
                }
            } else {
                seen_names.insert(key, items.len());
                let mut item = LineItem::new(name, quantity);
                item.image_url = image_url;
                items.push(item);
            }
        }

        items
    }

    /// Fast path for shipping emails: pure regex/string extraction, no DOM parsing.
    ///
    /// Returns `Some(WalmartOrder)` on success, `None` if extraction fails
    /// (caller should fall back to full `parse_order()` DOM path).
    ///
    /// All fields needed by `apply_shipping_tx` are extractable without DOM:
    /// - order_id → regex
    /// - tracking + carrier → regex
    /// - total_cost → string scanning (strategies 2-4)
    /// - items → alt-text regex on truncated HTML
    /// - order_date → regex
    pub fn parse_shipping_fast(
        &self,
        html: &str,
        fallback_date: Option<DateTime<Utc>>,
    ) -> Option<WalmartOrder> {
        let _span = tracing::debug_span!("parse_shipping_fast").entered();

        // Truncate at p13n boundary (same as full path)
        let cutoff = Self::recommendations_cutoff(html);
        let truncated = &html[..cutoff];

        // Order ID is mandatory
        let order_id = self.extract_order_id(html).ok()?;

        let order_date = self.extract_order_date(html)
            .unwrap_or_else(|_| fallback_date.unwrap_or_else(Utc::now));

        let total_cost = self.extract_total_price_regex(truncated);
        let tracking_info = self.extract_tracking_info(html);

        // Extract items from alt-text patterns on truncated HTML
        let items: Vec<LineItem> = self
            .extract_items_from_alt_regex(truncated)
            .into_iter()
            .map(|item| item.with_status(ItemStatus::Shipped))
            .collect();

        let mut order = WalmartOrder::new(&order_id, order_date, OrderStatus::Shipped)
            .with_items(items);

        if let Some(total) = total_cost {
            order = order.with_total(total);
        }

        if let Some((carrier, tracking_number)) = tracking_info {
            order = order.with_tracking(tracking_number, carrier);
        }

        Some(order)
    }

    /// Fast path for delivery emails: pure regex/string extraction, no DOM parsing.
    ///
    /// Returns `Some(WalmartOrder)` when alt-text items are found (standard delivery).
    /// Returns `None` for store deliveries (which use `itemName-*` classes requiring DOM),
    /// causing the caller to fall back to `parse_order()`.
    pub fn parse_delivery_fast(
        &self,
        html: &str,
        fallback_date: Option<DateTime<Utc>>,
    ) -> Option<WalmartOrder> {
        let _span = tracing::debug_span!("parse_delivery_fast").entered();

        let cutoff = Self::recommendations_cutoff(html);
        let truncated = &html[..cutoff];

        let order_id = self.extract_order_id(html).ok()?;

        // Extract items via alt-text regex. If none found, this is likely a
        // store delivery using itemName-* classes — bail out to full DOM path.
        let items: Vec<LineItem> = self
            .extract_items_from_alt_regex(truncated)
            .into_iter()
            .map(|item| item.with_status(ItemStatus::Delivered))
            .collect();

        if items.is_empty() {
            return None;
        }

        let order_date = self.extract_order_date(html)
            .unwrap_or_else(|_| fallback_date.unwrap_or_else(Utc::now));

        let total_cost = self.extract_total_price_regex(truncated);

        let mut order = WalmartOrder::new(&order_id, order_date, OrderStatus::Delivered)
            .with_items(items);

        if let Some(total) = total_cost {
            order = order.with_total(total);
        }

        Some(order)
    }

    /// Fast path for cancellation emails: regex/string extraction, no DOM parsing.
    ///
    /// Returns `Some(WalmartOrder)` when the email can be fully parsed without DOM.
    /// Returns `None` for store pickup cancellations that use `itemName-*` classes
    /// (which require DOM), causing the caller to fall back to `parse_order()`.
    pub fn parse_cancellation_fast(
        &self,
        html: &str,
        fallback_date: Option<DateTime<Utc>>,
    ) -> Option<WalmartOrder> {
        let _span = tracing::debug_span!("parse_cancellation_fast").entered();
        let cutoff = Self::recommendations_cutoff(html);
        let truncated = &html[..cutoff];

        let order_id = self.extract_order_id(html).ok()?;
        let cancel_reason = self.extract_cancel_reason(html);

        // Try alt-text items for partial cancellations
        let items: Vec<LineItem> = self.extract_items_from_alt_regex(truncated)
            .into_iter()
            .map(|item| item.with_status(ItemStatus::Canceled))
            .collect();

        // If no alt-text items but HTML contains itemName-* classes,
        // this is a store pickup cancellation that needs DOM — bail out
        if items.is_empty() && find_case_insensitive(truncated, "itemname-").is_some() {
            return None;
        }

        let order_date = self.extract_order_date(html)
            .unwrap_or_else(|_| fallback_date.unwrap_or_else(Utc::now));

        let status = if items.is_empty() {
            OrderStatus::Canceled
        } else {
            OrderStatus::PartiallyCanceled
        };

        let mut order = WalmartOrder::new(&order_id, order_date, status)
            .with_items(items);
        if let Some(reason) = cancel_reason {
            order = order.with_cancel_reason(reason);
        }
        Some(order)
    }

    /// Parse a complete order from email HTML.
    /// `fallback_date` is used when HTML date extraction fails (e.g. gmail_date).
    pub fn parse_order(&self, html: &str, fallback_date: Option<DateTime<Utc>>) -> ParseResult<WalmartOrder> {
        // Truncate HTML at the p13n (recommendations) boundary before DOM parsing.
        // This eliminates 60-80% of the document that we'd otherwise parse into a
        // full DOM tree only to filter it out during element iteration.
        let cutoff = Self::recommendations_cutoff(html);
        let truncated_html = &html[..cutoff];

        let context = self.build_context(truncated_html);

        // Detect email type from the truncated content — type indicators
        // (shipped, delivered, canceled, confirmed) always appear in the
        // email header well before any p13n/recommendation content.
        let email_type = self.detect_email_type_lower(&context.lower);
        let order_id = self.extract_order_id(html)?;

        // For dates, prefer HTML extraction, then fallback_date, then now
        let order_date = self.extract_order_date(html)
            .unwrap_or_else(|_| fallback_date.unwrap_or_else(Utc::now));

        let total_cost = self.extract_total_price_with_context(&context);
        let items = self.extract_items_with_context(&context);

        // Determine status based on email type
        let status = match email_type {
            EmailType::Confirmation => OrderStatus::Confirmed,
            EmailType::Cancellation => {
                if items.is_empty() {
                    OrderStatus::Canceled
                } else {
                    OrderStatus::PartiallyCanceled
                }
            }
            EmailType::Shipping => OrderStatus::Shipped,
            EmailType::Delivery => OrderStatus::Delivered,
            EmailType::Unknown => OrderStatus::Confirmed,
        };

        // Set item status based on email type
        let items_with_status: Vec<LineItem> = items
            .into_iter()
            .map(|item| {
                let item_status = match email_type {
                    EmailType::Cancellation => ItemStatus::Canceled,
                    EmailType::Shipping => ItemStatus::Shipped,
                    EmailType::Delivery => ItemStatus::Delivered,
                    _ => ItemStatus::Ordered,
                };
                item.with_status(item_status)
            })
            .collect();

        let mut order = WalmartOrder::new(&order_id, order_date, status)
            .with_items(items_with_status);

        if let Some(total) = total_cost {
            order = order.with_total(total);
        }

        // Extract tracking info for shipping emails
        if email_type == EmailType::Shipping {
            if let Some((carrier, tracking_number)) = self.extract_tracking_info(html) {
                order = order.with_tracking(tracking_number, carrier);
            }
        }

        // Extract cancel reason for cancellation emails
        if email_type == EmailType::Cancellation {
            if let Some(reason) = self.extract_cancel_reason(html) {
                order = order.with_cancel_reason(reason);
            }
        }

        Ok(order)
    }

    /// Extract the cancellation reason from a cancel email's HTML.
    ///
    /// Walmart cancel emails contain a sentence starting with "We're sorry"
    /// that explains why the order was canceled. This method extracts that
    /// sentence and maps known patterns to short labels.
    fn extract_cancel_reason(&self, html: &str) -> Option<String> {
        // Strip HTML tags to get plain text for pattern matching
        let text = html
            .replace("&rsquo;", "\u{2019}")
            .replace("&#8217;", "\u{2019}")
            .replace("&nbsp;", " ");
        let text = html_tag_pattern().replace_all(&text, " ");
        let text = whitespace_pattern().replace_all(&text, " ");
        let lower = text.to_lowercase();

        // Look for the "we're sorry" sentence that contains the cancel reason
        // Handle both straight apostrophe and right single quotation mark (')
        let sorry_idx = lower.find("we\u{2019}re sorry")
            .or_else(|| lower.find("we're sorry"))?;

        // Extract from "we're sorry" to the next period
        let rest = &text[sorry_idx..];
        let end = rest.find('.').unwrap_or(rest.len());
        let sentence = rest[..end].trim();

        let sentence_lower = sentence.to_lowercase();

        // Map known phrases to short labels
        if sentence_lower.contains("policy review") {
            Some("Suspected Fraud".to_string())
        } else if sentence_lower.contains("quantity limit") {
            Some("Quantity limits".to_string())
        } else {
            // Return the full sentence as fallback for unknown reasons
            Some(sentence.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create sample Walmart confirmation email HTML with dynamic classes
    fn sample_confirmation_html() -> &'static str {
        r#"
        <!DOCTYPE html>
        <html>
        <head><title>Order Confirmation</title></head>
        <body>
            <div class="header-xyz-123">
                <h1>Order Confirmed!</h1>
            </div>
            <div class="orderInfo-abc-456">
                <p>Order number: 2000141-70653310</p>
                <p>Ordered on January 15, 2024</p>
            </div>
            <div class="productContainer-def-789">
                <div class="productName-0-36726-75">Samsung Galaxy S24 Ultra 256GB</div>
                <div class="priceStyling-1-48392-12">$1,299.99</div>
                <img src="https://i5.walmartimages.com/product123.jpg" />
            </div>
            <div class="productContainer-def-790">
                <div class="productName-0-36726-76">USB-C Charger 65W</div>
                <div class="priceStyling-1-48392-13">$24.99</div>
            </div>
            <div class="totalSection">
                <p>Order Total: $1,324.98</p>
            </div>
        </body>
        </html>
        "#
    }

    fn sample_shipping_alt_images_html() -> &'static str {
        r#"
        <html>
        <body>
            <div>Thanks for your delivery order</div>
            <table>
                <tr>
                    <td>
                        <img src="https://i5.walmartimages.com/seo/POKEMON-ME2-5-ASCENDED-HEROES.jpeg"
                             alt="quantity 5 item Pokemon Trading Card Game Mega Evolution 2 5 Ascended Heroes Tech Sticker Collection Randomly Selected" />
                    </td>
                </tr>
                <tr>
                    <td>
                        <img src="https://i5.walmartimages.com/seo/POKEMON-POSTER-COLLECTION.jpeg"
                             alt="quantity 2 item Pokemon Scarlet &amp; Violet Poster Collection" />
                    </td>
                </tr>
            </table>
        </body>
        </html>
        "#
    }

    /// Create sample Walmart cancellation email HTML (uses plain ID format)
    fn sample_cancellation_html() -> &'static str {
        r#"
        <!DOCTYPE html>
        <html>
        <head><title>Order Cancellation</title></head>
        <body>
            <div class="header-xyz-123">
                <h1>Item Canceled</h1>
            </div>
            <div class="orderInfo-abc-456">
                <p>Order number: 200014170653310</p>
            </div>
            <div class="canceledItem">
                <div class="productName-0-99999-88">Samsung Galaxy S24 Ultra 256GB</div>
                <p>This item has been canceled from your order.</p>
            </div>
        </body>
        </html>
        "# 
    }

    #[test]
    fn test_shipping_alt_images_extracts_images() {
        let parser = WalmartEmailParser::new();
        let items = parser.extract_items(sample_shipping_alt_images_html());

        assert_eq!(items.len(), 2, "Should extract two items from alt images");
        assert!(items.iter().all(|item| item.image_url.is_some()), "Each item should have an image");
        assert_eq!(items[0].quantity, 5);
        assert_eq!(items[1].quantity, 2);
    }

    #[test]
    fn test_fast_path_regex_extracts_images() {
        let parser = WalmartEmailParser::new();
        // The fast-path regex extractor should capture src URLs alongside alt text
        let items = parser.extract_items_from_alt_regex(sample_shipping_alt_images_html());

        assert_eq!(items.len(), 2, "Fast-path should extract two items");
        assert_eq!(items[0].quantity, 5);
        assert_eq!(items[1].quantity, 2);
        assert!(
            items.iter().all(|item| item.image_url.is_some()),
            "Fast-path regex should capture image URLs from src attributes"
        );
        assert!(
            items[0].image_url.as_ref().unwrap().contains("walmartimages.com"),
            "Image URL should be a Walmart CDN URL"
        );
    }

    #[test]
    fn test_extract_order_id_hyphenated() {
        let parser = WalmartEmailParser::new();
        let html = sample_confirmation_html();

        let order_id = parser.extract_order_id(html).expect("Should extract ID");

        // CRITICAL: ID should be normalized (no hyphens)
        assert_eq!(order_id, "200014170653310");
        assert!(!order_id.contains('-'), "ID should not contain hyphens");
    }

    #[test]
    fn test_extract_order_id_plain() {
        let parser = WalmartEmailParser::new();
        let html = sample_cancellation_html();

        let order_id = parser.extract_order_id(html).expect("Should extract ID");

        // Plain ID should remain the same
        assert_eq!(order_id, "200014170653310");
    }

    #[test]
    fn test_hyphenated_and_plain_ids_match() {
        let parser = WalmartEmailParser::new();

        let confirmation_id = parser
            .extract_order_id(sample_confirmation_html())
            .expect("Should extract confirmation ID");

        let cancellation_id = parser
            .extract_order_id(sample_cancellation_html())
            .expect("Should extract cancellation ID");

        // CRITICAL TEST: Both IDs must match after normalization
        assert_eq!(
            confirmation_id, cancellation_id,
            "Confirmation and cancellation IDs must match after normalization"
        );
    }

    #[test]
    fn test_detect_email_type() {
        let parser = WalmartEmailParser::new();

        assert_eq!(
            parser.detect_email_type(sample_confirmation_html()),
            EmailType::Confirmation
        );

        assert_eq!(
            parser.detect_email_type(sample_cancellation_html()),
            EmailType::Cancellation
        );

        assert_eq!(
            parser.detect_email_type("<html>Your order has shipped!</html>"),
            EmailType::Shipping
        );

        assert_eq!(
            parser.detect_email_type("<html>Your order has been delivered</html>"),
            EmailType::Delivery
        );
    }

    #[test]
    fn test_extract_items_with_dynamic_classes() {
        let parser = WalmartEmailParser::new();
        let html = sample_confirmation_html();

        let items = parser.extract_items(html);

        assert_eq!(items.len(), 2, "Should find 2 items");
        assert_eq!(items[0].name, "Samsung Galaxy S24 Ultra 256GB");
        assert_eq!(items[1].name, "USB-C Charger 65W");
    }

    #[test]
    fn test_extract_prices_from_styled_elements() {
        let parser = WalmartEmailParser::new();
        let html = sample_confirmation_html();

        let total = parser.extract_total_price(html);
        assert!(total.is_some(), "Should extract total price");
        assert!((total.unwrap() - 1324.98).abs() < 0.01);
    }

    #[test]
    fn test_extract_order_date() {
        let parser = WalmartEmailParser::new();
        let html = sample_confirmation_html();

        let date = parser.extract_order_date(html).expect("Should extract date");
        assert_eq!(date.format("%Y-%m-%d").to_string(), "2024-01-15");
    }

    #[test]
    fn test_parse_complete_confirmation_order() {
        let parser = WalmartEmailParser::new();
        let html = sample_confirmation_html();

        let order = parser.parse_order(html, None).expect("Should parse order");

        assert_eq!(order.id, "200014170653310");
        assert_eq!(order.status, OrderStatus::Confirmed);
        assert_eq!(order.items.len(), 2);
        assert!(order.total_cost.is_some());
    }

    #[test]
    fn test_parse_cancellation_order() {
        let parser = WalmartEmailParser::new();
        let html = sample_cancellation_html();

        let order = parser.parse_order(html, None).expect("Should parse order");

        assert_eq!(order.id, "200014170653310");
        // Should be canceled or partially canceled based on items found
        assert!(
            order.status == OrderStatus::Canceled
                || order.status == OrderStatus::PartiallyCanceled
        );

        // Items from cancellation email should have Canceled status
        for item in &order.items {
            assert_eq!(item.status, ItemStatus::Canceled);
        }
    }

    #[test]
    fn test_fuzzy_class_matching() {
        let parser = WalmartEmailParser::new();

        // Test various dynamic class formats
        let test_cases = vec![
            r#"<div class="productName-0-12345-67">Test Product</div>"#,
            r#"<div class="productName_abc_123">Test Product</div>"#,
            r#"<div class="productNameStyled-v2">Test Product</div>"#,
        ];

        for html in test_cases {
            let full_html = format!(
                "<html><body>{}<div class='priceStyling-1-1-1'>$10.00</div></body></html>",
                html
            );
            let items = parser.extract_items(&full_html);
            assert!(
                !items.is_empty(),
                "Should match fuzzy class pattern in: {}",
                html
            );
        }
    }

    #[test]
    fn test_price_parsing_formats() {
        let parser = WalmartEmailParser::new();

        let test_cases = vec![
            ("$10.00", 10.0),
            ("$1,234.56", 1234.56),
            ("$ 99.99", 99.99),
            ("$1234", 1234.0),
        ];

        for (input, expected) in test_cases {
            let price = parser.extract_first_price(input);
            assert!(price.is_some(), "Should parse: {}", input);
            assert!(
                (price.unwrap() - expected).abs() < 0.01,
                "Price mismatch for {}: got {:?}, expected {}",
                input,
                price,
                expected
            );
        }
    }

    #[test]
    fn test_extract_total_from_walmart_order_total_section() {
        let parser = WalmartEmailParser::new();

        // Simulates decoded Walmart email with automation-id="order-total" section
        let html = r#"
        <html>
        <body>
            <table automation-id="order-total">
                <tr>
                    <td>Subtotal</td>
                    <td>$99.99</td>
                </tr>
                <tr>
                    <td>Shipping</td>
                    <td>$0.00</td>
                </tr>
                <tr>
                    <td><strong>Includes all fees, taxes and discounts</strong></td>
                    <td><strong>$106.84</strong></td>
                </tr>
            </table>
        </body>
        </html>
        "#;

        let total = parser.extract_total_price(html);
        assert!(total.is_some(), "Should extract total from automation-id section");
        assert!((total.unwrap() - 106.84).abs() < 0.01, "Total should be $106.84");
    }

    #[test]
    fn test_extract_total_ignores_savings_row() {
        let parser = WalmartEmailParser::new();

        let html = r#"
        <html>
        <body>
            <table automation-id="order-total">
                <tr>
                    <td><strong>Includes all fees, taxes, discounts and driver tip</strong></td>
                    <td><strong>$641.23</strong></td>
                </tr>
                <tr>
                    <td>You saved a total of</td>
                    <td><strong>$960.24</strong></td>
                </tr>
            </table>
        </body>
        </html>
        "#;

        let total = parser.extract_total_price(html);
        assert!(total.is_some(), "Should extract total from order-total section");
        assert!((total.unwrap() - 641.23).abs() < 0.01, "Total should be $641.23");
    }

    #[test]
    fn test_extract_total_from_includes_all_fees_pattern() {
        let parser = WalmartEmailParser::new();

        // Alternative pattern without automation-id
        let html = r#"
        <html>
        <body>
            <div>
                <strong>Includes all fees, taxes and discounts</strong>
                <strong>$106.84</strong>
            </div>
        </body>
        </html>
        "#;

        let total = parser.extract_total_price(html);
        assert!(total.is_some(), "Should extract total from 'includes all fees' pattern");
        assert!((total.unwrap() - 106.84).abs() < 0.01, "Total should be $106.84");
    }

    /// Sample store delivery email HTML — uses itemName-* classes and #ORDER-ID format
    fn sample_store_delivery_html() -> &'static str {
        r#"
        <html>
        <body>
            <div>Your package arrived, Mark!</div>
            <div>completed your delivery from store at 6:49am on Thu, Jun 5</div>

            <div>
                <p>Order date: Thu, Apr 24, 2025</p>
                <p>Order&nbsp;<a href="https://example.com/track">
                    <span style="color:#6d6e71 !important;">#2000132-35127884</span>
                </a></p>
            </div>

            <table>
            <tr>
                <td valign="top" width="76px">
                    <img class="item-image" aria-hidden="true"
                         src="https://i5.walmartimages.com/seo/Nintendo-Switch-2.jpeg"
                         alt="item image" height="60" />
                </td>
                <td valign="top">
                    <table>
                        <tr><td><span class="itemName-0-861324-47">Nintendo Switch 2 + Mario Kart World Bundle</span></td></tr>
                        <tr><td>Preorder</td></tr>
                        <tr><td>$499.00/EA</td></tr>
                        <tr><td>Qty: 1</td></tr>
                    </table>
                </td>
                <td align="right" valign="top">
                    <table>
                        <tr><td class="price-0-861324-48" align="right"><span style="font-weight:bold;">$499.00</span></td></tr>
                    </table>
                </td>
            </tr>
            </table>

            <!-- Recommendation section (should be ignored) -->
            <div>
                <img data-tracking="p13n" href="https://example.com?athznid=ContinueYourShopping" />
                <a class="productName-0-861324-86" href="https://example.com">Recommended Product</a>
            </div>
        </body>
        </html>
        "#
    }

    #[test]
    fn test_store_delivery_email_type() {
        let parser = WalmartEmailParser::new();
        assert_eq!(
            parser.detect_email_type(sample_store_delivery_html()),
            EmailType::Delivery
        );
    }

    #[test]
    fn test_store_delivery_order_id() {
        let parser = WalmartEmailParser::new();
        let order_id = parser
            .extract_order_id(sample_store_delivery_html())
            .expect("Should extract order ID from store delivery email");
        assert_eq!(order_id, "200013235127884");
    }

    #[test]
    fn test_store_delivery_item_extraction() {
        let parser = WalmartEmailParser::new();
        let items = parser.extract_items(sample_store_delivery_html());

        assert_eq!(items.len(), 1, "Should extract exactly 1 item");
        assert!(
            items[0].name.contains("Nintendo Switch"),
            "Item name should contain 'Nintendo Switch', got: {}",
            items[0].name
        );
        assert_eq!(items[0].quantity, 1);
        assert!(items[0].price.is_some(), "Should extract price");
        assert!(
            (items[0].price.unwrap() - 499.0).abs() < 0.01,
            "Price should be $499.00"
        );
    }

    #[test]
    fn test_store_delivery_ignores_recommendations() {
        let parser = WalmartEmailParser::new();
        let items = parser.extract_items(sample_store_delivery_html());

        // Should NOT contain recommendation products
        for item in &items {
            assert!(
                !item.name.contains("Recommended"),
                "Should not extract recommended products, found: {}",
                item.name
            );
        }
    }

    #[test]
    fn test_store_delivery_full_parse() {
        let parser = WalmartEmailParser::new();
        let order = parser
            .parse_order(sample_store_delivery_html(), None)
            .expect("Should parse store delivery order");

        assert_eq!(order.id, "200013235127884");
        assert_eq!(order.status, OrderStatus::Delivered);
        assert!(!order.items.is_empty(), "Should have items");
        for item in &order.items {
            assert_eq!(item.status, ItemStatus::Delivered);
        }
    }

    /// Sample canceled pickup email HTML — uses itemName-* classes, no price shown
    fn sample_canceled_pickup_html() -> &'static str {
        r#"
        <html>
        <body>
            <h1>dawn's pickup was canceled</h1>
            <div>
                <span style="font-size:14px;">Order number: 2000133-31515436</span>
            </div>
            <div>1 item canceled</div>

            <table width="100%">
            <tr><td>
                <table role="presentation" width="100%">
                <tr><td colspan="3" height="16"></td></tr>
                <tr>
                    <td class="imageContainer-0-303428-15" valign="top" width="76px">
                        <div>
                            <img class="item-image" aria-hidden="true"
                                 src="https://i5.walmartimages.com/seo/EverStart-Battery.jpeg"
                                 alt="item image" height="60" />
                        </div>
                    </td>
                    <td valign="top">
                        <table role="presentation">
                            <tr><td class="pb6-0-303428-16">
                                <span class="itemName-0-303428-19">EverStart Plus Lead Acid Automotive Battery, Group Size 96R 12 Volt, 590 CCA</span>
                            </td></tr>
                            <tr><td class="textGrey-0-303428-17">Qty: 1</td></tr>
                        </table>
                    </td>
                </tr>
                </table>
            </td></tr>
            </table>
        </body>
        </html>
        "#
    }

    #[test]
    fn test_canceled_pickup_email_type() {
        let parser = WalmartEmailParser::new();
        assert_eq!(
            parser.detect_email_type(sample_canceled_pickup_html()),
            EmailType::Cancellation
        );
    }

    #[test]
    fn test_canceled_pickup_order_id() {
        let parser = WalmartEmailParser::new();
        let order_id = parser
            .extract_order_id(sample_canceled_pickup_html())
            .expect("Should extract order ID from canceled pickup email");
        assert_eq!(order_id, "200013331515436");
    }

    #[test]
    fn test_canceled_pickup_item_extraction() {
        let parser = WalmartEmailParser::new();
        let items = parser.extract_items(sample_canceled_pickup_html());

        assert_eq!(items.len(), 1, "Should extract exactly 1 item");
        assert!(
            items[0].name.contains("EverStart"),
            "Item name should contain 'EverStart', got: {}",
            items[0].name
        );
        assert_eq!(items[0].quantity, 1);
        // No price in canceled pickup emails
        assert!(items[0].price.is_none(), "Canceled pickup should have no price");
    }

    #[test]
    fn test_canceled_pickup_full_parse() {
        let parser = WalmartEmailParser::new();
        let order = parser
            .parse_order(sample_canceled_pickup_html(), None)
            .expect("Should parse canceled pickup order");

        assert_eq!(order.id, "200013331515436");
        // parse_order returns PartiallyCanceled when items are present —
        // apply_cancellation_tx later checks DB state to promote to Canceled
        assert_eq!(order.status, OrderStatus::PartiallyCanceled);
        assert!(!order.items.is_empty(), "Should have items");
        for item in &order.items {
            assert_eq!(item.status, ItemStatus::Canceled);
        }
    }

    /// Alternate delivery-from-store email — says "item arrived" (singular),
    /// NOT "package arrived" or "delivered". Has tracking URLs that could
    /// false-match as Shipping if delivery detection misses it.
    fn sample_alternate_delivery_html() -> &'static str {
        r#"
        <html>
        <body>
            <h2>1 item arrived</h2>

            <div>
                <p>Order date: Sat, Sep 27, 2025</p>
                <p>Order&nbsp;<a href="https://example.com/track">
                    <span style="color:#6d6e71 !important;">#2000138-35586017</span>
                </a></p>
            </div>

            <a href="https://w-mt.co/g/rptrcks/comm-smart-app/services/tracking/click">Track</a>

            <table>
            <tr>
                <td valign="top" width="76px">
                    <img class="item-image" aria-hidden="true"
                         src="https://i5.walmartimages.com/seo/Hyper-Tough-Trolley-Jack.jpeg"
                         alt="item image" height="60" />
                </td>
                <td valign="top">
                    <table role="presentation">
                        <tr><td><span class="itemName-0-355797-47">Hyper Tough T82011W Trolley Jack, 2 Ton Black and Red</span></td></tr>
                        <tr><td>$41.97/EA</td></tr>
                        <tr><td>Qty: 1</td></tr>
                    </table>
                </td>
                <td align="right" valign="top">
                    <table>
                        <tr><td class="price-0-355797-48" align="right"><span style="font-weight:bold;">$41.97</span></td></tr>
                    </table>
                </td>
            </tr>
            </table>

            <!-- Recommendation section -->
            <table align="center" automation-id="p13n-module" border="0">
                <tr><td>
                    <a class="productName-0-355797-88" href="https://example.com">Recommended Product</a>
                </td></tr>
            </table>
        </body>
        </html>
        "#
    }

    #[test]
    fn test_alternate_delivery_email_type() {
        let parser = WalmartEmailParser::new();
        // Must detect as Delivery, NOT Shipping (despite "tracking" in URLs)
        assert_eq!(
            parser.detect_email_type(sample_alternate_delivery_html()),
            EmailType::Delivery
        );
    }

    #[test]
    fn test_alternate_delivery_order_id() {
        let parser = WalmartEmailParser::new();
        let order_id = parser
            .extract_order_id(sample_alternate_delivery_html())
            .expect("Should extract order ID");
        assert_eq!(order_id, "200013835586017");
    }

    #[test]
    fn test_alternate_delivery_item_extraction() {
        let parser = WalmartEmailParser::new();
        let items = parser.extract_items(sample_alternate_delivery_html());

        assert_eq!(items.len(), 1, "Should extract exactly 1 item");
        assert!(
            items[0].name.contains("Hyper Tough"),
            "Item name should contain 'Hyper Tough', got: {}",
            items[0].name
        );
        assert_eq!(items[0].quantity, 1);
        assert!(items[0].price.is_some(), "Should extract price");
        assert!(
            (items[0].price.unwrap() - 41.97).abs() < 0.01,
            "Price should be $41.97"
        );
    }

    #[test]
    fn test_alternate_delivery_ignores_recommendations() {
        let parser = WalmartEmailParser::new();
        let items = parser.extract_items(sample_alternate_delivery_html());

        for item in &items {
            assert!(
                !item.name.contains("Recommended"),
                "Should not extract recommended products, found: {}",
                item.name
            );
        }
    }

    // ===== Preorder confirmation email tests =====

    fn sample_preorder_confirmation_html() -> &'static str {
        r#"
        <html>
        <body>
            <div>Thanks for your order, Golden!</div>
            <div>Order number: 2000136-80110060</div>
            <div>Ordered on October 9, 2025</div>
            <a href="https://w-mt.co/g/rptrcks/comm-smart-app/services/tracking/clickTracker?redirectTo=abc123">View order</a>
            <table automation-id="order-total">
                <tr>
                    <td>Subtotal</td>
                    <td>$359.88</td>
                </tr>
                <tr>
                    <td>Shipping</td>
                    <td>$0.00</td>
                </tr>
                <tr>
                    <td>Tax</td>
                    <td>$23.65</td>
                </tr>
                <tr>
                    <td><strong>Includes all fees, taxes, discounts and driver tip</strong></td>
                    <td><strong>$383.53</strong></td>
                </tr>
            </table>
        </body>
        </html>
        "#
    }

    #[test]
    fn test_preorder_confirmation_email_type() {
        let parser = WalmartEmailParser::new();
        // Body has "thanks for your order" AND "tracking" in URLs.
        // Must be classified as Confirmation, not Shipping.
        let email_type = parser.detect_email_type(sample_preorder_confirmation_html());
        assert_eq!(
            email_type,
            EmailType::Confirmation,
            "Preorder confirmation should be detected as Confirmation, not Shipping"
        );
    }

    #[test]
    fn test_preorder_confirmation_total() {
        let parser = WalmartEmailParser::new();
        let total = parser.extract_total_price(sample_preorder_confirmation_html());
        assert!(total.is_some(), "Should extract total from preorder email");
        assert!(
            (total.unwrap() - 383.53).abs() < 0.01,
            "Total should be $383.53, got: {:?}",
            total
        );
    }

    #[test]
    fn test_preorder_confirmation_order_id() {
        let parser = WalmartEmailParser::new();
        let order_id = parser.extract_order_id(sample_preorder_confirmation_html());
        assert!(order_id.is_ok(), "Should extract order ID");
        assert_eq!(order_id.unwrap(), "200013680110060");
    }

    /// Test parsing a real Walmart confirmation email end-to-end.
    /// This reads the actual .eml file, decodes MIME, and verifies the parser
    /// extracts the correct order total, ID, items, and email type.
    #[test]
    fn test_real_confirmation_email_total() {
        let eml_path = std::path::Path::new("emails/200013348923251-order-confirmation.eml");
        if !eml_path.exists() {
            eprintln!("Skipping test: .eml fixture not found at {:?}", eml_path);
            return;
        }

        let eml_bytes = std::fs::read(eml_path).expect("Should read .eml file");
        let parsed_mail = mailparse::parse_mail(&eml_bytes).expect("Should parse MIME");
        let html = crate::ingestion::gmail::find_html_part(&parsed_mail)
            .expect("Should find HTML part in MIME");

        assert!(!html.is_empty(), "Decoded HTML body should not be empty");
        // Verify QP decoding worked (no raw =3D remaining)
        assert!(
            !html.contains("automation-id=3D"),
            "HTML should be QP-decoded (no =3D in attributes)"
        );

        let parser = WalmartEmailParser::new();

        // Email type detection
        let email_type = parser.detect_email_type(&html);
        assert_eq!(email_type, EmailType::Confirmation, "Should detect as Confirmation");

        // Total extraction
        let total = parser.extract_total_price(&html);
        assert!(total.is_some(), "Should extract total price from real email");
        assert!(
            (total.unwrap() - 818.42).abs() < 0.01,
            "Total should be $818.42, got: {:?}",
            total
        );

        // Full order parsing
        let order = parser.parse_order(&html, None).expect("Should parse order");
        assert_eq!(order.id, "200013348923251");
        assert!(order.total_cost.is_some(), "Order should have total_cost");
        assert!(
            (order.total_cost.unwrap() - 818.42).abs() < 0.01,
            "Order total should be $818.42, got: {:?}",
            order.total_cost
        );
        assert_eq!(order.status, crate::models::OrderStatus::Confirmed);
        assert_eq!(order.items.len(), 2, "Order should have 2 items");
    }
}
