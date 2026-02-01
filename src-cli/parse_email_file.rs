use anyhow::Result;
use std::fs;
use std::path::PathBuf;
use walmart_dashboard::parsing::WalmartEmailParser;

pub fn parse_email_file(path: &PathBuf) -> Result<()> {
    println!("Reading email file: {:?}", path);

    let raw_html = fs::read_to_string(path)?;
    println!("Email size: {} bytes", raw_html.len());

    let parser = WalmartEmailParser::new();

    println!("\n=== Attempting to parse email ===\n");

    match parser.parse_order(&raw_html) {
        Ok(order) => {
            println!("✅ Successfully parsed order!");
            println!("   Order ID: {}", order.id);
            println!("   Status: {:?}", order.status);
            println!("   Total Cost: {:?}", order.total_cost);
            println!("   Items: {}", order.items.len());
            for (i, item) in order.items.iter().enumerate() {
                println!("     {}. {} (qty: {})", i + 1, item.name, item.quantity);
            }
        }
        Err(e) => {
            println!("❌ Failed to parse email: {}", e);
        }
    }

    Ok(())
}
