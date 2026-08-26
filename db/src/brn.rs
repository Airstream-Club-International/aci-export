use crate::{DB_INSERT_CHUNK_SIZE, Error, Result, retain_with_keys, user};
use futures::{StreamExt, TryStreamExt, stream};
use sqlx::{PgPool, Postgres};

#[derive(Debug, sqlx::FromRow, serde::Serialize, Clone)]
pub struct Brn {
    pub user_id: String,
    pub number: String,
}

pub const FETCH_BRN_QUERY: &str = r#"
    SELECT
        user_id,
        number
    FROM
        brns
"#;

fn fetch_brn_query<'builder>() -> sqlx::QueryBuilder<'builder, Postgres> {
    sqlx::QueryBuilder::new(FETCH_BRN_QUERY)
}

pub async fn by_number(pool: &PgPool, number: &str) -> Result<Option<Brn>> {
    let user = fetch_brn_query()
        .push("WHERE number = ")
        .push_bind(number)
        .build_query_as::<Brn>()
        .fetch_optional(pool)
        .await?;

    Ok(user)
}

pub async fn by_email(pool: &PgPool, email: &str) -> Result<Vec<Brn>> {
    let brns = fetch_brn_query()
        .push("WHERE user_id = ")
        .push_bind(user::id_for_email(email))
        .build_query_as::<Brn>()
        .fetch_all(pool)
        .await?;

    Ok(brns)
}

pub async fn upsert_many(pool: &PgPool, brns: &[Brn]) -> Result<u64> {
    if brns.is_empty() {
        return Ok(0);
    }
    let affected: Vec<u64> = stream::iter(brns)
        .chunks(DB_INSERT_CHUNK_SIZE)
        .map(Ok)
        .and_then(|chunk| async move {
            let result = sqlx::QueryBuilder::new(
                r#"INSERT INTO brns (
                    user_id,
                    number
                ) "#,
            )
            .push_values(chunk, |mut b, brn| {
                b.push_bind(&brn.user_id).push_bind(&brn.number);
            })
            .push(
                r#"ON CONFLICT(number) DO UPDATE SET
                user_id = excluded.user_id
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

pub async fn retain(pool: &PgPool, users: &[Brn]) -> Result<u64> {
    retain_with_keys(pool, "brns", "number", users, |brn| brn.number.as_str()).await
}

/// Past and present holders of each BRN.
///
/// `brns` answers who holds a number today; this answers who held it when. A
/// number is reassigned when a member leaves or dies, so the current holder is
/// not the right answer for a leadership term that ended years ago.
pub mod ownership {
    use super::*;
    use chrono::NaiveDate;
    use itertools::Itertools;
    use sqlx::QueryBuilder;

    /// One continuous tenure of a BRN by a user. `end_date` is `None` for the
    /// current holder.
    #[derive(Debug, sqlx::FromRow, serde::Serialize, Clone)]
    pub struct Ownership {
        pub number: String,
        pub user_id: String,
        pub start_date: NaiveDate,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub end_date: Option<NaiveDate>,
    }

    const FETCH_OWNERSHIP_QUERY: &str = r#"
        SELECT
            number,
            user_id,
            start_date,
            end_date
        FROM
            brn_ownership
    "#;

    fn fetch_ownership_query<'builder>() -> QueryBuilder<'builder, Postgres> {
        QueryBuilder::new(FETCH_OWNERSHIP_QUERY)
    }

    /// Every recorded tenure of one number, oldest first.
    pub async fn by_number(pool: &PgPool, number: &str) -> Result<Vec<Ownership>> {
        let ownership = fetch_ownership_query()
            .push("WHERE number = ")
            .push_bind(number)
            .push(" ORDER BY start_date")
            .build_query_as::<Ownership>()
            .fetch_all(pool)
            .await?;

        Ok(ownership)
    }

    /// Every number one user has held, oldest first.
    pub async fn by_email(pool: &PgPool, email: &str) -> Result<Vec<Ownership>> {
        let ownership = fetch_ownership_query()
            .push("WHERE user_id = ")
            .push_bind(user::id_for_email(email))
            .push(" ORDER BY start_date, number")
            .build_query_as::<Ownership>()
            .fetch_all(pool)
            .await?;

        Ok(ownership)
    }

    pub async fn upsert_many(pool: &PgPool, ownership: &[Ownership]) -> Result<u64> {
        if ownership.is_empty() {
            return Ok(0);
        }
        let affected: Vec<u64> = stream::iter(
            ownership
                .iter()
                .unique_by(|own| (&own.number, &own.user_id, own.start_date)),
        )
        .chunks(DB_INSERT_CHUNK_SIZE)
        .map(Ok::<_, Error>)
        .and_then(|chunk| async move {
            let result = QueryBuilder::new(
                r#"INSERT INTO brn_ownership (
                    number,
                    user_id,
                    start_date,
                    end_date
                ) "#,
            )
            .push_values(&chunk, |mut b, own| {
                b.push_bind(&own.number)
                    .push_bind(&own.user_id)
                    .push_bind(own.start_date)
                    .push_bind(own.end_date);
            })
            .push(
                r#"ON CONFLICT(number, user_id, start_date) DO UPDATE SET
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

    pub async fn retain(pool: &PgPool, ownership: &[Ownership]) -> Result<u64> {
        if ownership.is_empty() {
            return Ok(0);
        }

        let mut tx = pool.begin().await?;

        sqlx::query(
            r#"CREATE TEMP TABLE _keep_brn_ownership (
                number TEXT,
                user_id TEXT,
                start_date DATE
            ) ON COMMIT DROP"#,
        )
        .execute(&mut *tx)
        .await?;

        for chunk in ownership.chunks(DB_INSERT_CHUNK_SIZE) {
            QueryBuilder::new("INSERT INTO _keep_brn_ownership(number, user_id, start_date) ")
                .push_values(chunk, |mut b, own| {
                    b.push_bind(&own.number)
                        .push_bind(&own.user_id)
                        .push_bind(own.start_date);
                })
                .build()
                .execute(&mut *tx)
                .await?;
        }

        let result = sqlx::query(
            r#"DELETE FROM brn_ownership o
               WHERE NOT EXISTS (
                   SELECT 1 FROM _keep_brn_ownership k
                   WHERE k.number = o.number
                     AND k.user_id = o.user_id
                     AND k.start_date = o.start_date
               )"#,
        )
        .execute(&mut *tx)
        .await?;

        let total_affected = result.rows_affected();
        tx.commit().await?;
        Ok(total_affected)
    }
}
