use crate::{Error, Result};
use chrono::NaiveDate;
use futures::TryFutureExt;
use sqlx::MySqlPool;

/// International rally from Drupal
#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub struct InternationalRally {
    pub uid: u64,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_date: Option<NaiveDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub early_registration_date: Option<NaiveDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registration_end_date: Option<NaiveDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adult_price_cents: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub youth_price_cents: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_price_cents: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub campsite_price_cents: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifetime_member_discount_cents: Option<i32>,
    pub status: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub year: Option<i16>,
}

/// Rally registration from Drupal
#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub struct RallyRegistration {
    pub uid: u64,
    pub rally_uid: u64,
    pub user_uid: u64,
    pub partner_attending: bool,
    pub first_time_attendee: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount_paid_cents: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount_due_cents: Option<i32>,
    pub created: i64,
}

const FETCH_RALLIES_QUERY: &str = r#"
    SELECT
        nd.nid AS uid,
        nd.title,
        loc.field_location_value AS location,
        CAST(sd.field_start_date_value AS DATE) AS start_date,
        CAST(erd.field_early_registration_date_value AS DATE) AS early_registration_date,
        CAST(red.field_registration_end_date_value AS DATE) AS registration_end_date,
        CAST(ap.field_adult_price_value * 100 AS SIGNED) AS adult_price_cents,
        CAST(yp.field_youth_price_value * 100 AS SIGNED) AS youth_price_cents,
        CAST(cp.field_child_price_value * 100 AS SIGNED) AS child_price_cents,
        CAST(csp.field_campsite_price_value * 100 AS SIGNED) AS campsite_price_cents,
        CAST(lmd.field_lifetime_member_discount_value * 100 AS SIGNED) AS lifetime_member_discount_cents,
        nd.status,
        CAST(y.field_year_value AS SIGNED) AS year
    FROM node_field_data nd
    LEFT JOIN node__field_location loc ON loc.entity_id = nd.nid AND loc.deleted = 0
    LEFT JOIN node__field_start_date sd ON sd.entity_id = nd.nid AND sd.deleted = 0
    LEFT JOIN node__field_early_registration_date erd ON erd.entity_id = nd.nid AND erd.deleted = 0
    LEFT JOIN node__field_registration_end_date red ON red.entity_id = nd.nid AND red.deleted = 0
    LEFT JOIN node__field_adult_price ap ON ap.entity_id = nd.nid AND ap.deleted = 0
    LEFT JOIN node__field_youth_price yp ON yp.entity_id = nd.nid AND yp.deleted = 0
    LEFT JOIN node__field_child_price cp ON cp.entity_id = nd.nid AND cp.deleted = 0
    LEFT JOIN node__field_campsite_price csp ON csp.entity_id = nd.nid AND csp.deleted = 0
    LEFT JOIN node__field_lifetime_member_discount lmd ON lmd.entity_id = nd.nid AND lmd.deleted = 0
    LEFT JOIN node__field_year y ON y.entity_id = nd.nid AND y.deleted = 0
    WHERE nd.type = 'international_rally'
"#;

/// Fetch all international rallies from Drupal
pub async fn all_rallies(pool: &MySqlPool) -> Result<Vec<InternationalRally>> {
    sqlx::query_as::<_, InternationalRally>(FETCH_RALLIES_QUERY)
        .fetch_all(pool)
        .map_err(Error::from)
        .await
}

const FETCH_REGISTRATIONS_QUERY: &str = r#"
    SELECT
        nd.nid AS uid,
        fr.field_rally_target_id AS rally_uid,
        fur.field_user_registered_target_id AS user_uid,
        CASE WHEN a2fn.field_attendee_2_first_name_value IS NOT NULL THEN TRUE ELSE FALSE END AS partner_attending,
        COALESCE(fta.field_first_time_attendee_value, 0) AS first_time_attendee,
        CAST(fap.field_amount_paid_value * 100 AS SIGNED) AS amount_paid_cents,
        CAST(fad.field_amount_due_value * 100 AS SIGNED) AS amount_due_cents,
        nd.created
    FROM node_field_data nd
    JOIN node__field_rally fr ON fr.entity_id = nd.nid AND fr.deleted = 0
    JOIN node__field_user_registered fur ON fur.entity_id = nd.nid AND fur.deleted = 0
    LEFT JOIN node__field_attendee_2_first_name a2fn ON a2fn.entity_id = nd.nid AND a2fn.deleted = 0
    LEFT JOIN node__field_first_time_attendee fta ON fta.entity_id = nd.nid AND fta.deleted = 0
    LEFT JOIN node__field_amount_paid fap ON fap.entity_id = nd.nid AND fap.deleted = 0
    LEFT JOIN node__field_amount_due fad ON fad.entity_id = nd.nid AND fad.deleted = 0
    WHERE nd.type = 'rally_registration'
"#;

/// Fetch all rally registrations from Drupal
pub async fn all_registrations(pool: &MySqlPool) -> Result<Vec<RallyRegistration>> {
    sqlx::query_as::<_, RallyRegistration>(FETCH_REGISTRATIONS_QUERY)
        .fetch_all(pool)
        .map_err(Error::from)
        .await
}
