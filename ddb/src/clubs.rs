use crate::{Error, Result};
use futures::TryFutureExt;
use sqlx::{MySql, MySqlPool};

pub async fn all(pool: &MySqlPool) -> Result<Vec<Club>> {
    sqlx::query_as::<_, Club>(FETCH_CLUBS_QUERY)
        .fetch_all(pool)
        .map_err(Error::from)
        .await
}

pub async fn by_uid(pool: &MySqlPool, uid: u64) -> Result<Option<Club>> {
    let club = fetch_clubs_query()
        .push(" AND nd.nid = ")
        .push_bind(uid)
        .build_query_as::<Club>()
        .fetch_optional(pool)
        .await?;

    Ok(club)
}

pub async fn by_number(pool: &MySqlPool, number: i32) -> Result<Option<Club>> {
    let club = fetch_clubs_query()
        .push(" AND cn.field_club_number_value = ")
        .push_bind(number)
        .build_query_as::<Club>()
        .fetch_optional(pool)
        .await?;

    Ok(club)
}

pub async fn search_by_name(pool: &MySqlPool, pattern: &str) -> Result<Vec<Club>> {
    let like = format!("%{pattern}%");
    fetch_clubs_query()
        .push(" AND LOWER(nd.title) LIKE LOWER(")
        .push_bind(like)
        .push(")")
        .build_query_as::<Club>()
        .fetch_all(pool)
        .map_err(Error::from)
        .await
}

const FETCH_CLUBS_QUERY: &str = r#"
        SELECT
            nd.nid as uid,
            cn.field_club_number_value as number,
            nd.title as name,
            nr.field_region_target_id as region,
            nd.status as active
        FROM node_field_data nd
        LEFT JOIN node__field_club_number cn ON cn.entity_id = nd.nid
        LEFT JOIN node__field_region nr ON nr.entity_id = nd.nid
        WHERE nd.type = 'ssp_club'
    "#;

fn fetch_clubs_query<'builder>() -> sqlx::QueryBuilder<'builder, MySql> {
    sqlx::QueryBuilder::new(FETCH_CLUBS_QUERY)
}

#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub struct Club {
    pub uid: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub number: Option<i64>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<u64>,
    pub active: bool,
}

pub mod db {
    use super::*;
    use ::db as app_db;

    impl From<Club> for app_db::club::Club {
        fn from(value: Club) -> Self {
            Self {
                uid: value.uid as i64,
                number: value.number,
                name: value.name,
                region: value.region.map(|r| r as i64),
            }
        }
    }

    impl From<crate::leadership::Leadership> for app_db::club::Leadership {
        fn from(value: crate::leadership::Leadership) -> Self {
            Self {
                id: None,
                club: app_db::club::Club {
                    uid: value.entity_uid as i64,
                    number: None,
                    name: String::new(),
                    region: None,
                },
                user: value.user.into(),
                role: value.role.into(),
                start_date: value.start_date,
                end_date: value.end_date,
            }
        }
    }
}
