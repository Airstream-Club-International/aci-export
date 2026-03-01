//! BRN (Big Red Number) queries from Drupal database
//!
//! Uses `ssp_complete_brn` joined with `node_field_data` for BRN number and acquire date.

use crate::Result;
use sqlx::MySqlPool;

/// A single BRN record from Drupal (one row per BRN)
#[derive(Debug, serde::Serialize, Clone)]
pub struct Brn {
    /// Drupal user UID
    pub user_uid: u64,
    /// BRN number string (e.g., "07569")
    pub number: String,
    /// Unix timestamp when BRN was acquired by current owner
    pub acquire_date: Option<i64>,
}

/// Raw row from the `ssp_complete_brn` join
#[derive(Debug, sqlx::FromRow)]
struct BrnRow {
    user_id: i64,
    brn_number: String,
    acquire_date: Option<i64>,
}

/// Fetch all assigned BRNs from Drupal with acquire dates
pub async fn all(pool: &MySqlPool) -> Result<Vec<Brn>> {
    let rows: Vec<BrnRow> = sqlx::query_as(
        r#"
        SELECT
            b.user_id,
            n.title AS brn_number,
            b.acquire_date
        FROM ssp_complete_brn b
        JOIN node_field_data n ON n.nid = b.brn_id
        WHERE b.user_id IS NOT NULL
        "#,
    )
    .fetch_all(pool)
    .await?;

    let brns = rows
        .into_iter()
        .filter(|row| row.user_id > 0)
        .map(|row| Brn {
            user_uid: row.user_id as u64,
            number: row.brn_number.trim().to_string(),
            acquire_date: row.acquire_date,
        })
        .collect();

    Ok(brns)
}
