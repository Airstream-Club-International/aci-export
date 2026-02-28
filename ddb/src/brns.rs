//! BRN (Big Red Number) queries from Drupal database
//!
//! Uses the `v_brns` view which provides user_id and comma-separated BRN numbers.

use crate::Result;
use sqlx::MySqlPool;

/// A single BRN record from Drupal (one row per user/number pair)
#[derive(Debug, serde::Serialize, Clone)]
pub struct Brn {
    /// Drupal user UID
    pub user_uid: u64,
    /// BRN number string (e.g., "07569")
    pub number: String,
}

/// Raw row from the `v_brns` view (comma-separated numbers)
#[derive(Debug, sqlx::FromRow)]
struct BrnRow {
    user_id: i32,
    brns_values: String,
}

/// Fetch all BRNs from Drupal, expanding comma-separated values into individual records
pub async fn all(pool: &MySqlPool) -> Result<Vec<Brn>> {
    let rows: Vec<BrnRow> = sqlx::query_as("SELECT user_id, brns_values FROM v_brns")
        .fetch_all(pool)
        .await?;

    let brns = rows
        .into_iter()
        .filter(|row| row.user_id > 0)
        .flat_map(|row| {
            let user_uid = row.user_id as u64;
            row.brns_values
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .map(move |number| Brn { user_uid, number })
                .collect::<Vec<_>>()
        })
        .collect();

    Ok(brns)
}
