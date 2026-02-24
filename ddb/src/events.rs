//! Event queries from Drupal database
//!
//! Returns all published events with owner resolution via the
//! microsite join (event → field_club → main_site_club → ssp_club/ssp_region).

use crate::{Error, Result};
use chrono::{NaiveDate, NaiveDateTime};
use futures::TryFutureExt;
use sqlx::MySqlPool;

/// Event from Drupal
#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub struct Event {
    pub uid: u64,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_date: Option<NaiveDateTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_date: Option<NaiveDateTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub website_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registration_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registration_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registration_deadline: Option<NaiveDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contact_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contact_email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contact_phone: Option<String>,
    /// nid of the owning ssp_club or ssp_region node
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_uid: Option<u64>,
    /// "ssp_club" | "ssp_region" | NULL (international)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_node_type: Option<String>,
    pub status: bool,
    pub created: i64,
    pub changed: i64,
}

const FETCH_EVENTS_QUERY: &str = r#"
    SELECT
        e.nid AS uid,
        e.title,
        CAST(d.field_date_value AS DATETIME) AS start_date,
        CAST(d.field_date_end_value AS DATETIME) AS end_date,
        desc_f.field_event_description_value AS description,
        loc.field_event_location_name_value AS location_name,
        addr.field_event_address_value AS address,
        ph.field_event_phone_value AS phone,
        web.field_event_website_uri AS website_url,
        body.body_value AS body,
        rl.field_registration_link_uri AS registration_url,
        rl.field_registration_link_title AS registration_label,
        CAST(rdd.field_registration_deadline_value AS DATE) AS registration_deadline,
        cn.field_contact_name_value AS contact_name,
        ce.field_contact_email_value AS contact_email,
        cp.field_contact_phone_value AS contact_phone,
        msc.entity_id AS owner_uid,
        owner_nd.type AS owner_node_type,
        e.status,
        e.created,
        e.changed
    FROM node_field_data e
    LEFT JOIN node__field_date d ON e.nid = d.entity_id AND d.deleted = 0
    LEFT JOIN node__field_event_description desc_f ON e.nid = desc_f.entity_id AND desc_f.deleted = 0
    LEFT JOIN node__field_event_location_name loc ON e.nid = loc.entity_id AND loc.deleted = 0
    LEFT JOIN node__field_event_address addr ON e.nid = addr.entity_id AND addr.deleted = 0
    LEFT JOIN node__field_event_phone ph ON e.nid = ph.entity_id AND ph.deleted = 0
    LEFT JOIN node__field_event_website web ON e.nid = web.entity_id AND web.deleted = 0
    LEFT JOIN node__body body ON e.nid = body.entity_id AND body.deleted = 0
    LEFT JOIN node__field_registration_link rl ON e.nid = rl.entity_id AND rl.deleted = 0
    LEFT JOIN node__field_registration_deadline rdd ON e.nid = rdd.entity_id AND rdd.deleted = 0
    LEFT JOIN node__field_contact_name cn ON e.nid = cn.entity_id AND cn.deleted = 0
    LEFT JOIN node__field_contact_email ce ON e.nid = ce.entity_id AND ce.deleted = 0
    LEFT JOIN node__field_contact_phone cp ON e.nid = cp.entity_id AND cp.deleted = 0
    LEFT JOIN node__field_club fc ON e.nid = fc.entity_id AND fc.deleted = 0
    LEFT JOIN node__field_main_site_club msc
        ON fc.field_club_target_id = msc.field_main_site_club_target_id AND msc.deleted = 0
    LEFT JOIN node_field_data owner_nd ON msc.entity_id = owner_nd.nid
    WHERE e.type = 'event'
      AND e.status = 1
    GROUP BY e.nid
"#;

/// Fetch all published events from Drupal
pub async fn all(pool: &MySqlPool) -> Result<Vec<Event>> {
    sqlx::query_as::<_, Event>(FETCH_EVENTS_QUERY)
        .fetch_all(pool)
        .map_err(Error::from)
        .await
}
