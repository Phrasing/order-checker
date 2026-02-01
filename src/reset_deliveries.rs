use crate::db::Database;
use anyhow::Result;

/// Reset skipped delivery emails to pending status so they can be re-processed
pub async fn reset_skipped_deliveries(db: &Database) -> Result<usize> {
    let result = sqlx::query(
        r#"UPDATE raw_emails
        SET status = 'pending', processed_at = NULL
        WHERE status = 'skipped'
        AND event_type = 'delivery'
        AND subject LIKE '%Arrived%Pokemon%'
        LIMIT 25"#
    )
    .execute(db.pool())
    .await?;

    let rows_updated = result.rows_affected() as usize;

    tracing::info!("Reset {} skipped delivery emails to pending status", rows_updated);

    Ok(rows_updated)
}
