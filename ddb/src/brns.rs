//! BRN (Big Red Number) queries from Drupal database
//!
//! Current ownership comes from `ssp_complete_brn`, joined with `node_field_data`
//! for the BRN number and acquire date. Past ownership comes from the
//! `ssp_big_red_number_ownership` paragraphs hanging off each BRN node, which
//! `history` reads.

use crate::{Error, Result};
use chrono::NaiveDate;
use futures::TryFutureExt;
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

/// One continuous tenure of a BRN by a single user.
///
/// `end_date` is `None` for the number's current owner. Every BRN has at most
/// one such open span, and no other user's span starts after it.
#[derive(Debug, serde::Serialize, Clone, PartialEq, Eq)]
pub struct Ownership {
    /// BRN number string (e.g., "07569")
    pub number: String,
    /// Drupal user UID of the owner during this span
    pub user_uid: u64,
    pub start_date: NaiveDate,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_date: Option<NaiveDate>,
}

/// Raw ownership interval, one row per `ssp_big_red_number_ownership` paragraph.
#[derive(Debug, sqlx::FromRow)]
struct OwnershipRow {
    number: String,
    user_uid: u64,
    start_date: NaiveDate,
    end_date: Option<NaiveDate>,
}

/// Drupal writes one ownership paragraph per renewal, so an uninterrupted tenure
/// arrives as a run of adjacent intervals. `coalesce` orders the rows itself.
const FETCH_OWNERSHIP_HISTORY_QUERY: &str = r#"
    SELECT
        TRIM(n.title) AS number,
        CAST(owner.field_member_target_id AS UNSIGNED) AS user_uid,
        DATE(sd.field_start_date_value) AS start_date,
        DATE(ed.field_end_date_value) AS end_date
    FROM node__field_number_ownership o
    JOIN node_field_data n
        ON n.nid = o.entity_id
    JOIN paragraphs_item_field_data p
        ON p.id = o.field_number_ownership_target_id
    JOIN paragraph__field_member owner
        ON owner.entity_id = p.id AND owner.deleted = '0'
    JOIN paragraph__field_start_date sd
        ON sd.entity_id = p.id AND sd.deleted = '0'
    LEFT JOIN paragraph__field_end_date ed
        ON ed.entity_id = p.id AND ed.deleted = '0'
    WHERE o.deleted = '0'
      AND owner.field_member_target_id > 0
"#;

/// Fetch every recorded BRN tenure, one span per continuous ownership.
pub async fn history(pool: &MySqlPool) -> Result<Vec<Ownership>> {
    sqlx::query_as::<_, OwnershipRow>(FETCH_OWNERSHIP_HISTORY_QUERY)
        .fetch_all(pool)
        .map_ok(coalesce)
        .map_err(Error::from)
        .await
}

/// Merge each run of adjacent same-owner intervals into a single span.
///
/// Intervals a day or less apart continue a tenure; a longer gap starts a new
/// span, so a number reacquired years later reads as two tenures rather than one
/// that swallows the owner in between.
fn coalesce(mut rows: Vec<OwnershipRow>) -> Vec<Ownership> {
    rows.sort_by(|left, right| {
        left.number
            .cmp(&right.number)
            .then(left.start_date.cmp(&right.start_date))
            // Drupal holds a few numbers whose intervals start on the same date
            // under two owners. Neither can be the tenure that day, so order
            // them by owner to make which span comes first repeatable. Intervals
            // alike but for their end need no order: merging takes the later end
            // either way.
            .then(left.user_uid.cmp(&right.user_uid))
    });

    rows.into_iter().fold(Vec::new(), |mut spans, row| {
        match spans.last_mut() {
            Some(span) if continues(span, &row) => {
                span.end_date = later_end(span.end_date, row.end_date);
            }
            _ => spans.push(Ownership {
                number: row.number,
                user_uid: row.user_uid,
                start_date: row.start_date,
                end_date: row.end_date,
            }),
        }
        spans
    })
}

fn continues(span: &Ownership, row: &OwnershipRow) -> bool {
    span.number == row.number
        && span.user_uid == row.user_uid
        && match span.end_date {
            // An open span already runs to the present, so anything the owner
            // holds afterwards is the same tenure.
            None => true,
            Some(end) => row.start_date.signed_duration_since(end).num_days() <= 1,
        }
}

/// `None` outlives any date, being ownership that has not ended.
fn later_end(left: Option<NaiveDate>, right: Option<NaiveDate>) -> Option<NaiveDate> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        _ => None,
    }
}

pub mod db {
    use super::*;
    use ::db as app_db;

    impl Brn {
        /// Attach this BRN to an already-synced user.
        pub fn to_db_brn(&self, user_id: &str) -> app_db::brn::Brn {
            app_db::brn::Brn {
                user_id: user_id.to_string(),
                number: self.number.clone(),
            }
        }
    }

    impl Ownership {
        /// Attach this tenure to an already-synced user.
        pub fn to_db_ownership(&self, user_id: &str) -> app_db::brn::ownership::Ownership {
            app_db::brn::ownership::Ownership {
                number: self.number.clone(),
                user_id: user_id.to_string(),
                start_date: self.start_date,
                end_date: self.end_date,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(text: &str) -> NaiveDate {
        text.parse().expect("parse date")
    }

    fn row(number: &str, user_uid: u64, start: &str, end: Option<&str>) -> OwnershipRow {
        OwnershipRow {
            number: number.to_string(),
            user_uid,
            start_date: date(start),
            end_date: end.map(date),
        }
    }

    fn span(number: &str, user_uid: u64, start: &str, end: Option<&str>) -> Ownership {
        Ownership {
            number: number.to_string(),
            user_uid,
            start_date: date(start),
            end_date: end.map(date),
        }
    }

    #[test]
    fn renewals_collapse_into_one_tenure() {
        // The shape Drupal actually stores: one interval per renewal, each
        // starting the day after the last ended, the final one still open.
        let coalesced = coalesce(vec![
            row("00004", 2233, "2008-09-15", Some("2013-12-31")),
            row("00004", 2233, "2014-01-01", Some("2019-12-31")),
            row("00004", 2233, "2020-01-01", None),
        ]);

        assert_eq!(coalesced, vec![span("00004", 2233, "2008-09-15", None)]);
    }

    #[test]
    fn reassignment_keeps_the_previous_owner_span() {
        let coalesced = coalesce(vec![
            row("00010", 8798, "2016-07-15", Some("2019-12-31")),
            row("00010", 4242, "2020-01-01", None),
        ]);

        assert_eq!(
            coalesced,
            vec![
                span("00010", 8798, "2016-07-15", Some("2019-12-31")),
                span("00010", 4242, "2020-01-01", None),
            ]
        );
    }

    #[test]
    fn reacquisition_after_another_owner_stays_two_tenures() {
        // Merging on owner alone would swallow the middle owner's span, and
        // report the first owner as holding the number throughout.
        let coalesced = coalesce(vec![
            row("00042", 100, "2000-01-01", Some("2004-12-31")),
            row("00042", 200, "2005-01-01", Some("2009-12-31")),
            row("00042", 100, "2010-01-01", None),
        ]);

        assert_eq!(
            coalesced,
            vec![
                span("00042", 100, "2000-01-01", Some("2004-12-31")),
                span("00042", 200, "2005-01-01", Some("2009-12-31")),
                span("00042", 100, "2010-01-01", None),
            ]
        );
    }

    #[test]
    fn a_closed_tenure_ends_on_its_last_renewal() {
        let coalesced = coalesce(vec![
            row("00007", 900, "2010-01-01", Some("2014-12-31")),
            row("00007", 900, "2015-01-01", Some("2018-06-30")),
        ]);

        assert_eq!(
            coalesced,
            vec![span("00007", 900, "2010-01-01", Some("2018-06-30"))]
        );
    }

    #[test]
    fn an_open_interval_absorbs_what_follows_it() {
        // Drupal stores both an open interval and a later closed one for the
        // same owner; the tenure is still running, so the span stays open.
        let coalesced = coalesce(vec![
            row("00008", 950, "2019-01-01", None),
            row("00008", 950, "2020-01-01", Some("2021-12-31")),
        ]);

        assert_eq!(coalesced, vec![span("00008", 950, "2019-01-01", None)]);
    }

    #[test]
    fn a_lapse_longer_than_a_day_starts_a_new_tenure() {
        let coalesced = coalesce(vec![
            row("00055", 300, "2010-01-01", Some("2012-06-30")),
            row("00055", 300, "2013-01-01", None),
        ]);

        assert_eq!(
            coalesced,
            vec![
                span("00055", 300, "2010-01-01", Some("2012-06-30")),
                span("00055", 300, "2013-01-01", None),
            ]
        );
    }

    #[test]
    fn rows_are_ordered_before_merging() {
        // Drupal returns ownership paragraphs in no particular order, and a
        // number's intervals have to be walked oldest first to merge correctly.
        let coalesced = coalesce(vec![
            row("00009", 700, "2020-01-01", None),
            row("00009", 600, "2005-01-01", Some("2009-12-31")),
            row("00009", 700, "2010-01-01", Some("2019-12-31")),
        ]);

        assert_eq!(
            coalesced,
            vec![
                span("00009", 600, "2005-01-01", Some("2009-12-31")),
                span("00009", 700, "2010-01-01", None),
            ]
        );
    }

    #[test]
    fn one_owners_intervals_starting_together_merge_to_one_span() {
        // Whichever of the two the sort puts first, an unended interval leaves
        // the tenure open.
        let coalesced = coalesce(vec![
            row("00011", 800, "2015-01-01", None),
            row("00011", 800, "2015-01-01", Some("2016-12-31")),
        ]);

        assert_eq!(coalesced, vec![span("00011", 800, "2015-01-01", None)]);
    }

    #[test]
    fn two_owners_starting_the_same_day_order_by_owner() {
        // Contradictory source data: nobody can start a tenure the same day it
        // starts for someone else. Both spans are kept, in a repeatable order.
        let coalesced = coalesce(vec![
            row("00012", 999, "2018-01-01", Some("2019-12-31")),
            row("00012", 111, "2018-01-01", Some("2018-06-30")),
        ]);

        assert_eq!(
            coalesced,
            vec![
                span("00012", 111, "2018-01-01", Some("2018-06-30")),
                span("00012", 999, "2018-01-01", Some("2019-12-31")),
            ]
        );
    }

    #[test]
    fn same_owner_on_different_numbers_stays_separate() {
        let coalesced = coalesce(vec![
            row("00001", 500, "2010-01-01", None),
            row("00002", 500, "2010-01-02", None),
        ]);

        assert_eq!(
            coalesced,
            vec![
                span("00001", 500, "2010-01-01", None),
                span("00002", 500, "2010-01-02", None),
            ]
        );
    }
}
