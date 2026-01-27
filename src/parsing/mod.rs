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
use thiserror::Error;

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
pub struct WalmartEmailParser {
    // Regex patterns compiled once for efficiency
    order_id_pattern: Regex,
    price_pattern: Regex,
    product_class_pattern: Regex,
    price_class_pattern: Regex,
    date_pattern: Regex,
    // Pattern to extract items from img alt attributes (shipping emails)
    alt_item_pattern: Regex,
    // Pattern to extract tracking number and carrier from shipping emails
    // Matches: "Fedex tracking number 123456" or "UPS tracking number <a...>123456</a>"
    tracking_pattern: Regex,
}

impl Default for WalmartEmailParser {
    fn default() -> Self {
        Self::new()
    }
}

impl WalmartEmailParser {
    pub fn new() -> Self {
        Self {
            // Match "Order number" or "Order #" followed by the ID
            // The ID might be directly after, or inside an <a> tag
            // Pattern handles: "Order number: 123" or "Order number:...<a...>123</a>"
            order_id_pattern: Regex::new(
                r"(?i)order\s*(?:number|#|num\.?)?[:\s]*(?:<[^>]*>)*\s*([0-9-]{10,})"
            ).expect("Invalid order ID regex"),

            // Match price formats: $123.45, $1,234.56
            price_pattern: Regex::new(
                r"\$\s*([0-9,]+\.?\d*)"
            ).expect("Invalid price regex"),

            // Fuzzy match for productName-* class pattern
            product_class_pattern: Regex::new(
                r"(?i)productName[a-zA-Z0-9_-]*"
            ).expect("Invalid product class regex"),

            // Fuzzy match for priceStyling-* class pattern
            price_class_pattern: Regex::new(
                r"(?i)priceStyling[a-zA-Z0-9_-]*"
            ).expect("Invalid price class regex"),

            // Common date formats in Walmart emails
            // Format found: "Order date: Thu, Jan 22, 2026" - need to skip optional day-of-week prefix
            date_pattern: Regex::new(
                r"(?i)(?:ordered\s+on|order\s+date|placed\s+on)[:\s]*(?:\w+,\s+)?(\w+\s+\d{1,2},?\s+\d{4})"
            ).expect("Invalid date regex"),

            // Pattern to extract items from img alt attributes in shipping emails
            // Format: "quantity N item ProductName" (e.g., "quantity 5 item 2025 Panini Donruss...")
            alt_item_pattern: Regex::new(
                r#"alt\s*=\s*["']quantity\s+(\d+)\s+item\s+([^"']+)["']"#
            ).expect("Invalid alt item regex"),

            // Pattern to extract tracking number and carrier from shipping emails
            // Matches: "Fedex tracking number 123456" or "UPS tracking number <a...>123456</a>"
            // Carriers: FedEx, UPS, USPS, OnTrac
            tracking_pattern: Regex::new(
                r"(?i)(fedex|ups|usps|ontrac)\s+tracking\s+number\s*(?:<a[^>]*>)?([A-Z0-9]{10,30})(?:</a>)?"
            ).expect("Invalid tracking regex"),
        }
    }

    /// Detect the type of email based on content
    pub fn detect_email_type(&self, html: &str) -> EmailType {
        let lower = html.to_lowercase();

        if lower.contains("order confirmed") || lower.contains("order confirmation") || lower.contains("thanks for your order") {
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
        } else if lower.contains("delivered") || lower.contains("has arrived") || lower.contains("package arrived") || lower.contains("items arrived") {
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
        if let Some(captures) = self.order_id_pattern.captures(html) {
            if let Some(id_match) = captures.get(1) {
                let raw_id = id_match.as_str();
                // NORMALIZE: Remove all hyphens to create consistent ID format
                let normalized = WalmartOrder::normalize_id(raw_id);
                return Ok(normalized);
            }
        }

        // Fallback: Try to find #XXXXXXX-XXXXXXXX format (used in delivery emails)
        // This pattern matches order IDs prefixed with # inside HTML tags
        let fallback_pattern = Regex::new(r"#(\d{7,}-\d{5,})").expect("Invalid fallback regex");
        if let Some(captures) = fallback_pattern.captures(html) {
            if let Some(id_match) = captures.get(1) {
                let raw_id = id_match.as_str();
                let normalized = WalmartOrder::normalize_id(raw_id);
                return Ok(normalized);
            }
        }

        // Fallback: Order ID nested in <span> inside <a> tag (delivery cancellation emails)
        // Pattern: Order number: <a href="..."><span style="...">2000143-29028812</span></a>
        let nested_span_pattern = Regex::new(
            r"(?i)order\s*(?:number|#|num\.?)?[:\s]*<a[^>]*>\s*<span[^>]*>([0-9\-]{10,})</span>"
        ).expect("Invalid nested span regex");
        if let Some(captures) = nested_span_pattern.captures(html) {
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
        if let Some(captures) = self.tracking_pattern.captures(html) {
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
        if let Some(captures) = self.date_pattern.captures(html) {
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

    /// Extract total price from email
    pub fn extract_total_price(&self, html: &str) -> Option<f64> {
        let document = Html::parse_document(html);

        // Strategy 1: Look for automation-id="order-total" (Walmart's standard marker)
        if let Ok(selector) = Selector::parse(r#"[automation-id="order-total"]"#) {
            for element in document.select(&selector) {
                let text: String = element.text().collect();
                // Look for the final price in this section (usually after "Includes all fees...")
                if let Some(price) = self.extract_last_price(&text) {
                    return Some(price);
                }
            }
        }

        // Strategy 2: Look for "Includes all fees, taxes and discounts" marker
        let lower_html = html.to_lowercase();
        if let Some(pos) = lower_html.find("includes all fees") {
            // Look for price within 300 chars after this marker
            let search_region = &html[pos..std::cmp::min(pos + 300, html.len())];
            if let Some(price) = self.extract_first_price(search_region) {
                return Some(price);
            }
        }

        // Strategy 3: Look for totalChargedClass pattern (Walmart's charged amount)
        if let Some(pos) = lower_html.find("totalchargedclass") {
            let search_region = &html[pos..std::cmp::min(pos + 200, html.len())];
            if let Some(price) = self.extract_first_price(search_region) {
                return Some(price);
            }
        }

        // Strategy 4: Look for elements containing "total" near a price
        let total_patterns = ["order total", "total:", "grand total", "total amount"];

        for pattern in total_patterns {
            if let Some(pos) = lower_html.find(pattern) {
                // Look for price within 200 chars after "total"
                let search_region = &html[pos..std::cmp::min(pos + 200, html.len())];
                if let Some(price) = self.extract_first_price(search_region) {
                    return Some(price);
                }
            }
        }

        // Fallback: look for price styling elements
        self.extract_price_from_styled_elements(&document)
    }

    /// Extract the first price found in a string
    fn extract_first_price(&self, text: &str) -> Option<f64> {
        if let Some(captures) = self.price_pattern.captures(text) {
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
        for captures in self.price_pattern.captures_iter(text) {
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
        // Use a universal selector to find all elements
        if let Ok(all_selector) = Selector::parse("*") {
            for element in document.select(&all_selector) {
                if let Some(class) = element.value().attr("class") {
                    // Use fuzzy matching for priceStyling-* classes
                    if self.price_class_pattern.is_match(class) {
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

    /// Extract line items from email HTML using fuzzy class matching
    pub fn extract_items(&self, html: &str) -> Vec<LineItem> {
        // Strategy 1: Extract items from img alt attributes (shipping email format)
        // Format: alt="quantity N item ProductName"
        // This is the most reliable method for shipping/delivery emails
        let items = self.extract_items_from_alt_attributes(html);
        if !items.is_empty() {
            return items;
        }

        let mut items = Vec::new();

        // Strategy 2: Look for productName-* class pattern, but ONLY before the p13n section
        // The p13n (personalization) section contains recommended products, not order items
        let html_before_p13n = self.get_html_before_recommendations(html);
        let document = Html::parse_document(&html_before_p13n);

        // Use universal selector to get all elements
        let all_selector = match Selector::parse("*") {
            Ok(s) => s,
            Err(_) => return items,
        };

        for element in document.select(&all_selector) {
            if let Some(class) = element.value().attr("class") {
                if self.product_class_pattern.is_match(class) {
                    let name = Self::get_element_text(&element);
                    if !name.is_empty() && name.len() > 2 {
                        let mut item = LineItem::new(name, 1);

                        // Try to find associated price by looking at siblings/nearby elements
                        // Walk up to parent and search descendants
                        if let Some(parent) = element.parent() {
                            if let Some(parent_elem) = ElementRef::wrap(parent) {
                                // Look for price in parent's descendants
                                for child in parent_elem.select(&all_selector) {
                                    if let Some(child_class) = child.value().attr("class") {
                                        if self.price_class_pattern.is_match(child_class) {
                                            let text = Self::get_element_text(&child);
                                            if let Some(price) = self.extract_first_price(&text) {
                                                item = item.with_price(price);
                                                break;
                                            }
                                        }
                                    }
                                }

                                // Look for image URL in parent's descendants
                                if let Ok(img_selector) = Selector::parse("img") {
                                    for img in parent_elem.select(&img_selector) {
                                        if let Some(src) = img.value().attr("src") {
                                            item = item.with_image(src.to_string());
                                            break;
                                        }
                                    }
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
            items = self.extract_items_from_tables(&document);
        }

        items
    }

    /// Extract items from img alt attributes (used in shipping emails)
    /// Format: alt="quantity N item ProductName"
    fn extract_items_from_alt_attributes(&self, html: &str) -> Vec<LineItem> {
        let mut items = Vec::new();

        for captures in self.alt_item_pattern.captures_iter(html) {
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

    /// Get HTML content before the recommendations/personalization section
    /// The p13n-module section contains recommended products, not order items
    fn get_html_before_recommendations(&self, html: &str) -> String {
        // Look for markers that indicate the start of the recommendations section
        let p13n_markers = [
            "automation-id=\"p13n-module\"",
            "automation-id='p13n-module'",
            "p13n-products",
            "You might also like",
            "Recommended for you",
            "More from Walmart",
            "Based on your order",
        ];

        let lower_html = html.to_lowercase();
        let mut earliest_pos = html.len();

        for marker in p13n_markers {
            if let Some(pos) = lower_html.find(&marker.to_lowercase()) {
                if pos < earliest_pos {
                    earliest_pos = pos;
                }
            }
        }

        // Return only the part of HTML before recommendations
        html[..earliest_pos].to_string()
    }

    /// Fallback: Extract items from table structures (common in older email formats)
    fn extract_items_from_tables(&self, document: &Html) -> Vec<LineItem> {
        let mut items = Vec::new();

        // Try to find table rows that look like product listings
        if let Ok(tr_selector) = Selector::parse("tr") {
            for row in document.select(&tr_selector) {
                let row_text: String = row.text().collect();

                // Skip header rows and non-product rows
                if row_text.to_lowercase().contains("item")
                    && row_text.to_lowercase().contains("price")
                {
                    continue;
                }

                // Look for a price indicator in the row
                if let Some(price) = self.extract_first_price(&row_text) {
                    // Get the first non-price text as product name
                    let mut cells: Vec<String> = Vec::new();
                    if let Ok(td_selector) = Selector::parse("td") {
                        for cell in row.select(&td_selector) {
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

    /// Parse a complete order from email HTML
    pub fn parse_order(&self, html: &str) -> ParseResult<WalmartOrder> {
        let email_type = self.detect_email_type(html);
        let order_id = self.extract_order_id(html)?;

        // For dates, use a fallback to now if not found
        let order_date = self.extract_order_date(html).unwrap_or_else(|_| Utc::now());

        let total_cost = self.extract_total_price(html);
        let items = self.extract_items(html);

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

        let order = parser.parse_order(html).expect("Should parse order");

        assert_eq!(order.id, "200014170653310");
        assert_eq!(order.status, OrderStatus::Confirmed);
        assert_eq!(order.items.len(), 2);
        assert!(order.total_cost.is_some());
    }

    #[test]
    fn test_parse_cancellation_order() {
        let parser = WalmartEmailParser::new();
        let html = sample_cancellation_html();

        let order = parser.parse_order(html).expect("Should parse order");

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
}
