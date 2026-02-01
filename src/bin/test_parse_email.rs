use std::fs;
use walmart_dashboard::parsing::WalmartEmailParser;

fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("=== Parsing email file ===\n");

    let email_path = "emails/200014048308980-arrived.eml";
    println!("Reading: {}", email_path);

    let raw_html = fs::read_to_string(email_path)?;
    println!("File size: {} bytes\n", raw_html.len());

    let parser = WalmartEmailParser::new();

    println!("Attempting to parse...\n");

    match parser.parse_order(&raw_html, None) {
        Ok(order) => {
            println!("✅ SUCCESS! Parsed order:");
            println!("   Order ID: {}", order.id);
            println!("   Status: {:?}", order.status);
            println!("   Total Cost: {:?}", order.total_cost);
            println!("   Tracking: {:?}", order.tracking_number);
            println!("   Items: {}", order.items.len());
            for (i, item) in order.items.iter().enumerate() {
                println!("     {}. {} (qty: {}, status: {:?})",
                    i + 1, item.name, item.quantity, item.status);
            }
        }
        Err(e) => {
            println!("❌ FAILED to parse: {}", e);
        }
    }

    Ok(())
}
