use crate::{DB_INSERT_CHUNK_SIZE, Error, Result, leadership, retain_with_keys, user};
use futures::{StreamExt, TryFutureExt, TryStreamExt, stream};
use itertools::Itertools;
use sqlx::{PgPool, Postgres, QueryBuilder};

pub async fn all(pool: &PgPool) -> Result<Vec<Region>> {
    sqlx::query_as::<_, Region>(FETCH_REGIONS_QUERY)
        .fetch_all(pool)
        .map_err(Error::from)
        .await
}

pub async fn by_uid(pool: &PgPool, uid: i64) -> Result<Option<Region>> {
    let region = fetch_regions_query()
        .push("where uid = ")
        .push_bind(uid)
        .build_query_as::<Region>()
        .fetch_optional(pool)
        .await?;

    Ok(region)
}

pub async fn by_number(pool: &PgPool, number: i32) -> Result<Option<Region>> {
    let region = fetch_regions_query()
        .push("where number = ")
        .push_bind(number)
        .build_query_as::<Region>()
        .fetch_optional(pool)
        .await?;

    Ok(region)
}

pub async fn upsert_many(pool: &PgPool, regions: &[Region]) -> Result<u64> {
    if regions.is_empty() {
        return Ok(0);
    }
    let result = sqlx::QueryBuilder::new("INSERT INTO regions(uid, number, name) ")
        .push_values(regions, |mut b, region| {
            b.push_bind(region.uid)
                .push_bind(region.number)
                .push_bind(&region.name);
        })
        .push(
            r#"ON CONFLICT(number) DO UPDATE SET
                name = excluded.name,
                uid = excluded.uid
            "#,
        )
        .build()
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

pub async fn retain(pool: &PgPool, regions: &[Region]) -> Result<u64> {
    retain_with_keys(pool, "regions", "uid", regions, |region| region.uid).await
}

const FETCH_REGIONS_QUERY: &str = r#"
        select
            uid,
            number,
            name
        from regions
    "#;

fn fetch_regions_query<'builder>() -> sqlx::QueryBuilder<'builder, Postgres> {
    sqlx::QueryBuilder::new(FETCH_REGIONS_QUERY)
}

#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub struct Region {
    pub uid: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub number: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

// ========== Region Leadership ==========

#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub struct Leadership {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i64>,
    #[sqlx(flatten, try_from = "RegionRef")]
    pub region: Region,
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
struct RegionRef {
    region_uid: i64,
    region_number: Option<i32>,
    region_name: Option<String>,
}

impl From<RegionRef> for Region {
    fn from(value: RegionRef) -> Self {
        Self {
            uid: value.region_uid,
            number: value.region_number,
            name: value.region_name,
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
        lr.id,
        lr.start_date,
        lr.end_date,

        reg.uid as region_uid,
        reg.number as region_number,
        reg.name as region_name,

        u.id,
        u.uid,
        u.email,
        u.first_name,
        u.last_name,

        r.uid as role_uid,
        r.title as role_title
    FROM
        leadership_region lr
        JOIN regions reg ON reg.uid = lr.region
        JOIN users u ON u.id = lr.user_id
        JOIN leadership_role r ON r.uid = lr.role
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

/// `leadership_region.region` holds region uids, so a region number matches on
/// the joined regions row rather than on the leadership column.
fn leadership_by_number_query<'builder>(
    region_number: i32,
    filter: &leadership::DateFilter,
) -> QueryBuilder<'builder, Postgres> {
    let mut query = fetch_leadership_query();
    query
        .push("WHERE reg.number = ")
        .push_bind(region_number as i64);
    leadership::apply_date_filter(&mut query, filter, true);
    query
}

pub async fn leadership_by_number(
    pool: &PgPool,
    region_number: i32,
    filter: leadership::DateFilter,
) -> Result<Vec<Leadership>> {
    leadership_by_number_query(region_number, &filter)
        .build_query_as::<Leadership>()
        .fetch_all(pool)
        .await
        .map_err(Error::from)
}

pub async fn leadership_by_uid(
    pool: &PgPool,
    region_uid: i64,
    filter: leadership::DateFilter,
) -> Result<Vec<Leadership>> {
    let mut query = fetch_leadership_query();
    query.push("WHERE reg.uid = ").push_bind(region_uid);
    leadership::apply_date_filter(&mut query, &filter, true);
    query
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
            .unique_by(|l| (l.region.uid, &l.user.id, l.role.uid, l.start_date)),
    )
    .chunks(DB_INSERT_CHUNK_SIZE)
    .map(Ok::<_, Error>)
    .and_then(|chunk| async move {
        let result = QueryBuilder::new(
            r#"INSERT INTO leadership_region(
                    region,
                    user_id,
                    role,
                    start_date,
                    end_date
                ) "#,
        )
        .push_values(&chunk, |mut b, lead| {
            b.push_bind(lead.region.uid)
                .push_bind(&lead.user.id)
                .push_bind(lead.role.uid)
                .push_bind(lead.start_date)
                .push_bind(lead.end_date);
        })
        .push(
            r#"ON CONFLICT(region, user_id, role, start_date) DO UPDATE SET
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
        r#"CREATE TEMP TABLE _keep_leadership_region (
            region BIGINT,
            user_id TEXT,
            role BIGINT,
            start_date DATE
        ) ON COMMIT DROP"#,
    )
    .execute(&mut *tx)
    .await?;

    // Insert keys in chunks
    for chunk in leadership.chunks(DB_INSERT_CHUNK_SIZE) {
        QueryBuilder::new(
            "INSERT INTO _keep_leadership_region(region, user_id, role, start_date) ",
        )
        .push_values(chunk, |mut b, lead| {
            b.push_bind(lead.region.uid)
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
        r#"DELETE FROM leadership_region lr
           WHERE NOT EXISTS (
               SELECT 1 FROM _keep_leadership_region k
               WHERE k.region = lr.region
                 AND k.user_id = lr.user_id
                 AND k.role = lr.role
                 AND k.start_date = lr.start_date
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
    fn leadership_by_number_matches_the_region_number() {
        // A region's number and its uid are different namespaces, and
        // leadership_region.region holds the uid. Matching the number against
        // that column returns nothing for a region that has leadership.
        let query = leadership_by_number_query(5, &DateFilter::Current);

        let sql = query.sql();
        assert!(sql.contains("WHERE reg.number = $1"), "{sql}");
        // The date filter composes onto that predicate; a second WHERE is invalid SQL.
        assert_eq!(sql.matches("WHERE").count(), 1, "{sql}");
    }
}
