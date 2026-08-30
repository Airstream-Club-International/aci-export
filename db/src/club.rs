use crate::{DB_INSERT_CHUNK_SIZE, Error, Result, leadership, retain_with_keys, user};
use futures::{StreamExt, TryFutureExt, TryStreamExt, stream};
use itertools::Itertools;
use sqlx::{PgPool, Postgres, QueryBuilder};

pub async fn all(pool: &PgPool) -> Result<Vec<Club>> {
    sqlx::query_as::<_, Club>(FETCH_CLUBS_QUERY)
        .fetch_all(pool)
        .map_err(Error::from)
        .await
}

pub async fn by_uid(pool: &PgPool, uid: i64) -> Result<Option<Club>> {
    let club = fetch_clubs_query()
        .push("where uid = ")
        .push_bind(uid)
        .build_query_as::<Club>()
        .fetch_optional(pool)
        .await?;

    Ok(club)
}

pub async fn by_number(pool: &PgPool, number: i32) -> Result<Option<Club>> {
    let club = fetch_clubs_query()
        .push("where number = ")
        .push_bind(number)
        .build_query_as::<Club>()
        .fetch_optional(pool)
        .await?;

    Ok(club)
}

const FETCH_CLUBS_QUERY: &str = r#"
    SELECT
        uid,
        number,
        name,
        region
    FROM
        clubs
"#;

fn fetch_clubs_query<'builder>() -> sqlx::QueryBuilder<'builder, Postgres> {
    sqlx::QueryBuilder::new(FETCH_CLUBS_QUERY)
}

#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub struct Club {
    pub uid: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub number: Option<i64>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<i64>,
}

pub async fn upsert_many(pool: &PgPool, clubs: &[Club]) -> Result<u64> {
    if clubs.is_empty() {
        return Ok(0);
    }
    let result = sqlx::QueryBuilder::new("INSERT INTO clubs(uid, number, name, region) ")
        .push_values(clubs, |mut b, club| {
            b.push_bind(club.uid)
                .push_bind(club.number)
                .push_bind(&club.name)
                .push_bind(club.region);
        })
        .push(
            r#"ON CONFLICT(uid) DO UPDATE SET
                name = excluded.name,
                number = excluded.number,
                region = excluded.region
            "#,
        )
        .build()
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

pub async fn retain(pool: &PgPool, clubs: &[Club]) -> Result<u64> {
    retain_with_keys(pool, "clubs", "uid", clubs, |club| club.uid).await
}

// ========== Club Leadership ==========

#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub struct Leadership {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    #[sqlx(flatten, try_from = "ClubRef")]
    pub club: Club,
    #[sqlx(flatten)]
    pub user: user::User,
    #[sqlx(flatten, try_from = "RoleRef")]
    pub role: leadership::Role,
    pub start_date: chrono::NaiveDate,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_date: Option<chrono::NaiveDate>,
}

// Intermediary structs for sqlx extraction
#[derive(Debug, sqlx::FromRow)]
struct ClubRef {
    club_uid: i64,
    club_number: Option<i64>,
    club_name: String,
    club_region: Option<i64>,
}

impl From<ClubRef> for Club {
    fn from(value: ClubRef) -> Self {
        Self {
            uid: value.club_uid,
            number: value.club_number,
            name: value.club_name,
            region: value.club_region,
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct RoleRef {
    role_uid: i64,
    role_title: String,
}

impl From<RoleRef> for leadership::Role {
    fn from(value: RoleRef) -> Self {
        Self {
            uid: value.role_uid,
            title: value.role_title,
        }
    }
}

const FETCH_LEADERSHIP_QUERY: &str = r#"
    SELECT
        lc.id,
        lc.start_date,
        lc.end_date,

        c.uid as club_uid,
        c.number as club_number,
        c.name as club_name,
        c.region as club_region,

        u.id,
        u.uid,
        u.email,
        u.first_name,
        u.last_name,

        r.uid as role_uid,
        r.title as role_title
    FROM
        leadership_club lc
        JOIN clubs c ON c.uid = lc.club
        JOIN users u ON u.id = lc.user_id
        JOIN leadership_role r ON r.uid = lc.role
"#;

fn fetch_leadership_query<'builder>() -> QueryBuilder<'builder, Postgres> {
    QueryBuilder::new(FETCH_LEADERSHIP_QUERY)
}

pub async fn all_leadership(
    pool: &PgPool,
    filter: leadership::DateFilter,
) -> Result<Vec<Leadership>> {
    let mut query = fetch_leadership_query();
    leadership::apply_date_filter(&mut query, &filter, false);
    query
        .build_query_as::<Leadership>()
        .fetch_all(pool)
        .await
        .map_err(Error::from)
}

pub async fn leadership_by_uid(
    pool: &PgPool,
    club_uid: i64,
    filter: leadership::DateFilter,
) -> Result<Vec<Leadership>> {
    let mut query = fetch_leadership_query();
    query.push("WHERE lc.club = ").push_bind(club_uid);
    leadership::apply_date_filter(&mut query, &filter, true);
    query
        .build_query_as::<Leadership>()
        .fetch_all(pool)
        .await
        .map_err(Error::from)
}

/// `leadership_club.club` holds club uids, so a club number matches on the
/// joined clubs row rather than on the leadership column.
fn leadership_by_number_query<'builder>(
    club_number: i32,
    filter: &leadership::DateFilter,
) -> QueryBuilder<'builder, Postgres> {
    let mut query = fetch_leadership_query();
    query
        .push("WHERE c.number = ")
        .push_bind(club_number as i64);
    leadership::apply_date_filter(&mut query, filter, true);
    query
}

pub async fn leadership_by_number(
    pool: &PgPool,
    club_number: i32,
    filter: leadership::DateFilter,
) -> Result<Vec<Leadership>> {
    leadership_by_number_query(club_number, &filter)
        .build_query_as::<Leadership>()
        .fetch_all(pool)
        .await
        .map_err(Error::from)
}

pub async fn upsert_leadership(pool: &PgPool, leadership: &[Leadership]) -> Result<u64> {
    if leadership.is_empty() {
        return Ok(0);
    }

    let affected: Vec<u64> = stream::iter(
        leadership
            .iter()
            .unique_by(|l| (l.club.uid, &l.user.id, l.role.uid, l.start_date)),
    )
    .chunks(DB_INSERT_CHUNK_SIZE)
    .map(Ok::<_, Error>)
    .and_then(|chunk| async move {
        let result = QueryBuilder::new(
            r#"INSERT INTO leadership_club(
                    club,
                    user_id,
                    role,
                    start_date,
                    end_date
                ) "#,
        )
        .push_values(&chunk, |mut b, lead| {
            b.push_bind(lead.club.uid)
                .push_bind(&lead.user.id)
                .push_bind(lead.role.uid)
                .push_bind(lead.start_date)
                .push_bind(lead.end_date);
        })
        .push(
            r#"ON CONFLICT(club, user_id, role, start_date) DO UPDATE SET
                    end_date = excluded.end_date
                "#,
        )
        .build()
        .execute(pool)
        .await?;
        Ok::<u64, Error>(result.rows_affected())
    })
    .try_collect()
    .await?;
    Ok(affected.iter().sum())
}

pub async fn retain_leadership(pool: &PgPool, leadership: &[Leadership]) -> Result<u64> {
    if leadership.is_empty() {
        return Ok(0);
    }

    let mut tx = pool.begin().await?;

    // Create temp table to hold keys to keep
    sqlx::query(
        r#"CREATE TEMP TABLE _keep_leadership_club (
            club BIGINT,
            user_id TEXT,
            role BIGINT,
            start_date DATE
        ) ON COMMIT DROP"#,
    )
    .execute(&mut *tx)
    .await?;

    // Insert keys in chunks
    for chunk in leadership.chunks(DB_INSERT_CHUNK_SIZE) {
        QueryBuilder::new("INSERT INTO _keep_leadership_club(club, user_id, role, start_date) ")
            .push_values(chunk, |mut b, lead| {
                b.push_bind(lead.club.uid)
                    .push_bind(&lead.user.id)
                    .push_bind(lead.role.uid)
                    .push_bind(lead.start_date);
            })
            .build()
            .execute(&mut *tx)
            .await?;
    }

    // Delete rows not in temp table
    let result = sqlx::query(
        r#"DELETE FROM leadership_club lc
           WHERE NOT EXISTS (
               SELECT 1 FROM _keep_leadership_club k
               WHERE k.club = lc.club
                 AND k.user_id = lc.user_id
                 AND k.role = lc.role
                 AND k.start_date = lc.start_date
           )"#,
    )
    .execute(&mut *tx)
    .await?;

    let total_affected = result.rows_affected();
    tx.commit().await?;
    Ok(total_affected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use leadership::DateFilter;

    #[test]
    fn leadership_by_number_matches_the_club_number() {
        // leadership_club.club holds the club uid, not its number.
        let query = leadership_by_number_query(13, &DateFilter::Current);

        let sql = query.sql();
        assert!(sql.contains("WHERE c.number = $1"), "{sql}");
        // The date filter composes onto that predicate; a second WHERE is invalid SQL.
        assert_eq!(sql.matches("WHERE").count(), 1, "{sql}");
    }
}
