//! Data models for the Walmart Order Reconciler
//!
//! This module contains the core data structures used to represent orders
//! and their associated items. These models support the event-sourcing
//! approach where an order's state is derived from multiple email events.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Represents the current status of an order.
/// An order's status is derived from processing multiple email events.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OrderStatus {
    /// Initial state after confirmation email
    Confirmed,
    /// At least one item has shipped
    Shipped,
    /// All items in the order were canceled
    Canceled,
    /// Some items were canceled, others remain
    PartiallyCanceled,
    /// All items have been delivered
    Delivered,
}

impl OrderStatus {
    /// Convert status to a string representation for database storage
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderStatus::Confirmed => "confirmed",
            OrderStatus::Shipped => "shipped",
            OrderStatus::Canceled => "canceled",
            OrderStatus::PartiallyCanceled => "partially_canceled",
            OrderStatus::Delivered => "delivered",
        }
    }

    /// Parse status from database string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "confirmed" => Some(OrderStatus::Confirmed),
            "shipped" => Some(OrderStatus::Shipped),
            "canceled" => Some(OrderStatus::Canceled),
            "partially_canceled" => Some(OrderStatus::PartiallyCanceled),
            "delivered" => Some(OrderStatus::Delivered),
            _ => None,
        }
    }
}

/// Represents the status of an individual line item within an order
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ItemStatus {
    /// Item is part of the order but hasn't shipped yet
    Ordered,
    /// Item has been shipped
    Shipped,    /// Item has been delivered
    Delivered,
    /// Item was canceled
    Canceled,
}

impl ItemStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ItemStatus::Ordered => "ordered",
            ItemStatus::Shipped => "shipped",
            ItemStatus::Delivered => "delivered",
            ItemStatus::Canceled => "canceled",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "ordered" => Some(ItemStatus::Ordered),
            "shipped" => Some(ItemStatus::Shipped),
            "delivered" => Some(ItemStatus::Delivered),
            "canceled" => Some(ItemStatus::Canceled),
            _ => None,
        }
    }
}

/// A line item within an order
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineItem {
    /// Product name
    pub name: String,
    /// Quantity ordered
    pub quantity: u32,
    /// Price per item (may be missing in cancellation emails)
    pub price: Option<f64>,
    /// URL to the product image (if available)
    pub image_url: Option<String>,
    /// Current status of this line item
    pub status: ItemStatus,
}

impl LineItem {
    pub fn new(name: String, quantity: u32) -> Self {
        Self {
            name,
            quantity,
            price: None,
            image_url: None,
            status: ItemStatus::Ordered,
        }
    }

    pub fn with_price(mut self, price: f64) -> Self {
        self.price = Some(price);
        self
    }

    pub fn with_image(mut self, url: String) -> Self {
        self.image_url = Some(url);
        self
    }

    pub fn with_status(mut self, status: ItemStatus) -> Self {
        self.status = status;
        self
    }
}

/// Represents a Walmart order parsed from email
///
/// # ID Normalization
/// CRITICAL: Walmart IDs vary in format between email types:
/// - Confirmation emails use hyphenated format: "2000141-70653310"
/// - Cancellation emails use plain format: "200014170653310"
///
/// All IDs MUST be normalized to the plain format (hyphens stripped)
/// to ensure proper matching in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalmartOrder {
    /// Normalized order ID (hyphens stripped)
    pub id: String,
    /// Date and time the order was placed
    pub order_date: DateTime<Utc>,
    /// Total cost of the order (may be missing in cancellation emails)
    pub total_cost: Option<f64>,
    /// Current status of the order
    pub status: OrderStatus,
    /// Line items in this order
    pub items: Vec<LineItem>,
    /// Tracking number from shipping email (if available)
    pub tracking_number: Option<String>,
    /// Carrier name (FedEx, UPS, USPS, etc.)
    pub carrier: Option<String>,
    /// Recipient email address
    pub recipient: Option<String>,
    /// Associated Gmail account ID (for multi-account support)
    pub account_id: Option<i64>,
}

impl WalmartOrder {
    /// Normalize a Walmart order ID by removing hyphens.
    /// This ensures IDs from different email types can be matched.
    ///
    /// # Examples
    /// ```
    /// use walmart_dashboard::models::WalmartOrder;
    ///
    /// assert_eq!(WalmartOrder::normalize_id("2000141-70653310"), "200014170653310");
    /// assert_eq!(WalmartOrder::normalize_id("200014170653310"), "200014170653310");
    /// ```
    pub fn normalize_id(id: &str) -> String {
        id.replace('-', "")
    }

    /// Create a new order with a normalized ID
    pub fn new(raw_id: &str, order_date: DateTime<Utc>, status: OrderStatus) -> Self {
        Self {
            id: Self::normalize_id(raw_id),
            order_date,
            total_cost: None,
            status,
            items: Vec::new(),
            tracking_number: None,
            carrier: None,
            recipient: None,
            account_id: None,
        }
    }

    pub fn with_account(mut self, account_id: Option<i64>) -> Self {
        self.account_id = account_id;
        self
    }

    pub fn with_recipient(mut self, recipient: Option<String>) -> Self {
        self.recipient = recipient;
        self
    }

    pub fn with_total(mut self, total: f64) -> Self {
        self.total_cost = Some(total);
        self
    }

    pub fn with_items(mut self, items: Vec<LineItem>) -> Self {
        self.items = items;
        self
    }

    pub fn add_item(&mut self, item: LineItem) {
        self.items.push(item);
    }

    pub fn with_tracking(mut self, tracking_number: String, carrier: String) -> Self {
        self.tracking_number = Some(tracking_number);
        self.carrier = Some(carrier);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_id_with_hyphen() {
        assert_eq!(WalmartOrder::normalize_id("2000141-70653310"), "200014170653310");
    }

    #[test]
    fn test_normalize_id_already_plain() {
        assert_eq!(WalmartOrder::normalize_id("200014170653310"), "200014170653310");
    }

    #[test]
    fn test_normalize_id_multiple_hyphens() {
        assert_eq!(WalmartOrder::normalize_id("2000-141-706-53310"), "200014170653310");
    }

    #[test]
    fn test_order_status_roundtrip() {
        let statuses = vec![
            OrderStatus::Confirmed,
            OrderStatus::Shipped,
            OrderStatus::Canceled,
            OrderStatus::PartiallyCanceled,
            OrderStatus::Delivered,
        ];

        for status in statuses {
            let s = status.as_str();
            let parsed = OrderStatus::from_str(s).expect("Should parse");
            assert_eq!(parsed, status);
        }
    }

    #[test]
    fn test_item_status_roundtrip() {
        let statuses = vec![
            ItemStatus::Ordered,
            ItemStatus::Shipped,
            ItemStatus::Canceled,
        ];

        for status in statuses {
            let s = status.as_str();
            let parsed = ItemStatus::from_str(s).expect("Should parse");
            assert_eq!(parsed, status);
        }
    }
}
