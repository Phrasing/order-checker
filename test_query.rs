use sqlx::sqlite::SqliteConnectOptions;
use std::str::FromStr;
use sqlx::Row;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = SqliteConnectOptions::from_str("sqlite:orders.db")?
        .create_if_missing(true);
    
    let pool = sqlx::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;

    // Check for Arrived Pokemon emails for order 200014048308980
    println!("=== Raw Emails with 'Arrived' in subject ===");
    let rows: Vec<(i64, String, Option<String>, String, String, Option<String>, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT id, gmail_id, subject, event_type, processing_status, error_message, gmail_date, fetched_at FROM raw_emails WHERE subject LIKE '%Arrived%' LIMIT 10"
    )
    .fetch_all(&pool)
    .await?;

    for (id, gmail_id, subject, event_type, status, error, gmail_date, fetched_at) in rows {
        println!("\nID: {}", id);
        println!("  Gmail ID: {}", gmail_id);
        println!("  Subject: {:?}", subject);
        println!("  Event Type: {}", event_type);
        println!("  Status: {}", status);
        println!("  Error: {:?}", error);
        println!("  Gmail Date: {:?}", gmail_date);
        println!("  Fetched At: {:?}", fetched_at);
    }

    // Check pending emails
    println!("\n=== Pending Emails ===");
    let pending_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM raw_emails WHERE processing_status = 'pending'")
        .fetch_one(&pool)
        .await?;
    println!("Total pending: {}", pending_count.0);

    Ok(())
}
