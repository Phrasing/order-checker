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
    SELECTOR.get_or_init(|| Selector::parse("div, span, td").ok()).as_ref()
}

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Order ID not found in email")]
    OrderIdNotFound,

    #[error("Order date not found in email")]
    OrderDateNotFound,

    #[error("Failed to parse date: {0}")]
    DateParseError(String),

    #[error("No items found in email")]
    NoItemsFound,

    #[error("Invalid HTML structure")]
    InvalidHtml,
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
    document_before_p13n: Html,
}

impl WalmartEmailParser {
    pub fn new() -> Self {
        Self
    }

    /// Detect the type of email based on content
    pub fn detect_email_type(&self, html: &str) -> EmailType {
        let lower = html.to_lowercase();
        self.detect_email_type_lower(&lower)
    }

    fn detect_email_type_lower(&self, lower: &str) -> EmailType {
        if lower.contains("order confirmed")
            || lower.contains("order confirmation")
            || lower.contains("thanks for your order")
        {
            EmailType::Confirmation
        } else if lower.contains("order cancel")
            || lower.contains("item cancel")
            || lower.contains("been canceled")
            || lower.contains("was canceled")
            || lower.contains("is canceled")
            || lower.contains("delivery canceled")
            || lower.contains("has been cancelled")
            || lower.contains("was cancelled")
        {
            EmailType::Cancellation
        } else if lower.contains("delivered") || lower.contains("has arrived") || lower.contains("package arrived") || lower.contains("item arrived") {
            // IMPORTANT: Check delivery BEFORE shipping, because delivery emails often contain
            // "Sold and shipped by Walmart" which would match the shipping check
            EmailType::Delivery
        } else if lower.contains("shipped") || lower.contains("on its way") || lower.contains("tracking") {
            EmailType::Shipping
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
                return Ok(normalized);
            }
        }

        // Fallback: Try to find #XXXXXXX-XXXXXXXX format (used in delivery emails)
        if let Some(captures) = fallback_order_id_pattern().captures(html) {
            if let Some(id_match) = captures.get(1) {
                let raw_id = id_match.as_str();
                let normalized = WalmartOrder::normalize_id(raw_id);
                return Ok(normalized);
            }
        }

        // Fallback: Order ID nested in <span> inside <a> tag (delivery cancellation emails)
        if let Some(captures) = nested_span_order_id_pattern().captures(html) {
            if let Some(id_match) = captures.get(1) {
                let raw_id = id_match.as_str();
                let normalized = WalmartOrder::normalize_id(raw_id);
                return Ok(normalized);
            }
        }

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

    fn build_context<'a>(&self, html: &'a str) -> ParseContext<'a> {
        let lower = html.to_lowercase();
        let cutoff = Self::recommendations_cutoff(&lower, html.len());
        let html_before_p13n = &html[..cutoff];

        let document = Html::parse_document(html);
        let document_before_p13n = Html::parse_document(html_before_p13n);

        ParseContext {
            html,
            lower,
            document,
            document_before_p13n,
        }
    }

    fn recommendations_cutoff(lower_html: &str, html_len: usize) -> usize {
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

        let mut earliest_pos = html_len;
        for marker in p13n_markers {
            if let Some(pos) = lower_html.find(marker) {
                if pos < earliest_pos {
                    earliest_pos = pos;
                }
            }
        }

        earliest_pos
    }

    /// Extract total price from email
    pub fn extract_total_price(&self, html: &str) -> Option<f64> {
        let context = self.build_context(html);
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
        let context = self.build_context(html);
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
        let document = &context.document_before_p13n;

        let all_selector = match class_scan_selector() {
            Some(selector) => selector,
            None => return items,
        };

        for element in document.select(all_selector) {
            if let Some(class) = element.value().attr("class") {
                if product_class_pattern().is_match(class) {
                    let name = Self::get_element_text(&element);
                    if !name.is_empty() && name.len() > 2 {
                        let mut item = LineItem::new(name, 1);

                        // Try to find associated price by looking at siblings/nearby elements
                        // Walk up to parent and search descendants
                        if let Some(parent) = element.parent() {
                            if let Some(parent_elem) = ElementRef::wrap(parent) {
                                // Look for price in parent's descendants
                                for child in parent_elem.select(all_selector) {
                                    if let Some(child_class) = child.value().attr("class") {
                                        if price_class_pattern().is_match(child_class) {
                                            let text = Self::get_element_text(&child);
                                            if let Some(price) = self.extract_first_price(&text) {
                                                item = item.with_price(price);
                                                break;
                                            }
                                        }
                                    }
                                }

                                if let Some(src) = Self::find_best_product_image(&parent_elem) {
                                    item = item.with_image(src);
                                }
                            }
                        }

                        items.push(item);
                    }
                }
            }
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

        let document = &context.document_before_p13n;
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
        let document = &context.document_before_p13n;
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
                for _depth in 0..10 {
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

    /// Parse a complete order from email HTML.
    /// `fallback_date` is used when HTML date extraction fails (e.g. gmail_date).
    pub fn parse_order(&self, html: &str, fallback_date: Option<DateTime<Utc>>) -> ParseResult<WalmartOrder> {
        let context = self.build_context(html);
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

        Ok(order)
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
}
