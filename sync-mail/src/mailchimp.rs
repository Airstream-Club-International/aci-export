use crate::{Error, Result, settings::AciDatabaseSettings};
use chrono::{DateTime, Utc};
use futures::TryFutureExt;
use mailchimp::{
    RetryPolicy,
    members::{MembersQuery, member_id},
};
use sqlx::{Database, Encode, MySqlPool, PgPool, Type, query::QueryAs};
use std::{collections::HashSet, time::Instant};

#[derive(Debug, serde::Serialize)]
pub struct JobSyncResult {
    pub name: String,
    pub deleted: usize,
    pub upserted: usize,
}

#[derive(Debug, serde::Serialize)]
pub struct DryRunResult {
    pub name: String,
    pub upserted: usize,
    pub would_delete: Vec<DryRunEntry>,
}

#[derive(Debug, serde::Serialize)]
pub struct DryRunEntry {
    pub id: String,
    pub email_address: String,
    pub status: Option<mailchimp::members::MemberStatus>,
}

#[derive(Debug, sqlx::FromRow, Clone, serde::Serialize, Default)]
pub struct Job {
    pub id: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub api_key: String,
    pub name: String,
    pub list: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub club: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<i32>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Default, Clone)]
pub struct JobUpdate {
    pub id: i64,
    pub name: Option<String>,
    pub api_key: Option<String>,
    pub list: Option<String>,
    pub club: Option<i64>,
    pub region: Option<i32>,
}

trait MaybeBind<'q, DB>
where
    DB: Database,
{
    fn maybe_bind<T>(self, v: &'q Option<T>) -> Self
    where
        T: 'q + Encode<'q, DB> + Type<DB>;
}

impl<'q, DB, O> MaybeBind<'q, DB> for QueryAs<'q, DB, O, <DB as Database>::Arguments<'q>>
where
    DB: Database,
{
    fn maybe_bind<T>(self, v: &'q Option<T>) -> Self
    where
        T: 'q + Encode<'q, DB> + Type<DB>,
    {
        if let Some(value) = v {
            self.bind(value)
        } else {
            self
        }
    }
}

impl JobUpdate {
    pub fn setters(&self) -> Vec<String> {
        fn maybe_setter<T>(v: &Option<T>, name: &str, index: &mut u8, results: &mut Vec<String>) {
            if v.is_some() {
                results.push(format!("{name} = ${index}"));
                *index += 1;
            }
        }
        let mut index: u8 = 2;
        let mut results = vec![];
        maybe_setter(&self.name, "name", &mut index, &mut results);
        maybe_setter(&self.api_key, "api_key", &mut index, &mut results);
        maybe_setter(&self.list, "list", &mut index, &mut results);
        maybe_setter(&self.club, "club", &mut index, &mut results);
        maybe_setter(&self.region, "region", &mut index, &mut results);
        results
    }

    pub fn binds<'q, DB, O>(
        &'q self,
        q: QueryAs<'q, DB, O, <DB as Database>::Arguments<'q>>,
    ) -> QueryAs<'q, DB, O, <DB as Database>::Arguments<'q>>
    where
        DB: Database,
        i32: Encode<'q, DB> + Type<DB>,
        i64: Encode<'q, DB> + Type<DB>,
        String: Encode<'q, DB> + Type<DB>,
    {
        q.bind(self.id)
            .maybe_bind(&self.name)
            .maybe_bind(&self.api_key)
            .maybe_bind(&self.list)
            .maybe_bind(&self.club)
            .maybe_bind(&self.region)
    }
}

impl Job {
    pub async fn all(db: &PgPool) -> Result<Vec<Self>> {
        sqlx::query_as("select id, name, api_key, list, club, region, created_at from mailchimp")
            .fetch_all(db)
            .map_err(Error::from)
            .await
    }

    pub async fn get(db: &PgPool, job_id: i64) -> Result<Option<Self>> {
        sqlx::query_as(
            r#"select id, name, api_key, list, club, region, created_at from mailchimp where id = $1;"#,
        )
        .bind(job_id)
        .fetch_optional(db)
        .map_err(Error::from)
        .await
    }

    pub async fn create(db: &PgPool, job: &Self) -> Result<Self> {
        sqlx::query_as(
            r#"
            insert into mailchimp (name, api_key, list, club, region)
            values ($1, $2, $3, $4, $5)
            returning *;
            "#,
        )
        .bind(&job.name)
        .bind(&job.api_key)
        .bind(&job.list)
        .bind(job.club)
        .bind(job.region)
        .fetch_one(db)
        .map_err(Error::from)
        .await
    }

    pub async fn update(db: &PgPool, update: &JobUpdate) -> Result<Self> {
        let setters = update.setters().join(",");
        if setters.is_empty() {
            return Self::get(db, update.id)
                .await?
                .ok_or(Error::from(sqlx::Error::RowNotFound));
        }
        let query_str = format!(
            r#"
            update mailchimp set
                {setters}
            where id = $1
            returning *;
            "#,
        );
        let query = sqlx::query_as(&query_str);
        update.binds(query).fetch_one(db).map_err(Error::from).await
    }

    pub async fn delete(db: &PgPool, id: i64) -> Result<()> {
        sqlx::query(r#"delete from mailchimp where id = $1"#)
            .bind(id)
            .execute(db)
            .await?;
        Ok(())
    }

    fn client(&self) -> Result<mailchimp::Client> {
        Ok(mailchimp::client::from_api_key(&self.api_key)?)
    }

    async fn db_members(&self, db: &MySqlPool) -> Result<Vec<ddb::members::Member>> {
        let db_members = if let Some(club) = self.club {
            ddb::members::by_club(db, club as u64).await?
        } else if let Some(region) = self.region {
            ddb::members::by_region(db, region as u64).await?
        } else {
            ddb::members::all(db).await?
        };
        Ok(db_members)
    }

    fn merge_fields(&self) -> Result<mailchimp::merge_fields::MergeFields> {
        if self.club.is_some() {
            mailchimp::merge_fields::MergeFields::club()
        } else {
            // region or all
            mailchimp::merge_fields::MergeFields::all()
        }
        .map_err(Error::from)
    }

    #[tracing::instrument(skip_all, name = "merge_fields", fields(name = self.name, id = self.id))]
    pub async fn sync_merge_fields(
        &self,
        process_deletes: bool,
    ) -> Result<(Vec<String>, Vec<String>, Vec<String>)> {
        let client = self.client()?;
        mailchimp::merge_fields::sync(&client, &self.list, self.merge_fields()?, process_deletes)
            .map_err(Error::from)
            .await
    }

    /// Run sync for multiple jobs in parallel, returning results keyed by job ID
    /// Jobs that fail are logged but don't stop other jobs from syncing
    pub async fn sync_many(
        jobs: Vec<Self>,
        ddb_settings: AciDatabaseSettings,
    ) -> std::collections::HashMap<i64, JobSyncResult> {
        use futures::StreamExt;

        futures::stream::iter(jobs)
            .map(|job| {
                let ddb_settings = ddb_settings.clone();
                async move {
                    let name = job.name.clone();
                    let id = job.id;
                    match job.sync(ddb_settings).await {
                        Ok((deleted, upserted)) => Some((
                            id,
                            JobSyncResult {
                                name,
                                deleted,
                                upserted,
                            },
                        )),
                        Err(e) => {
                            tracing::error!(job_id = id, job_name = name, "sync failed: {e}");
                            None
                        }
                    }
                }
            })
            .buffered(20)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flatten()
            .collect()
    }

    /// Run dry_run for multiple jobs in parallel, returning results keyed by job ID
    /// Jobs that fail are logged but don't stop other jobs from running
    pub async fn dry_run_many(
        jobs: Vec<Self>,
        ddb_settings: AciDatabaseSettings,
    ) -> std::collections::HashMap<i64, DryRunResult> {
        use futures::StreamExt;

        futures::stream::iter(jobs)
            .map(|job| {
                let ddb_settings = ddb_settings.clone();
                async move {
                    let name = job.name.clone();
                    let id = job.id;
                    match job.dry_run(ddb_settings).await {
                        Ok(result) => Some((id, result)),
                        Err(e) => {
                            tracing::error!(job_id = id, job_name = name, "dry-run failed: {e}");
                            None
                        }
                    }
                }
            })
            .buffered(20)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flatten()
            .collect()
    }

    /// Load Drupal members for this job and convert them into the MailChimp
    /// member shape (with mailing addresses injected). Returned tuple is
    /// `(db_members, mc_members)` because callers typically need both:
    /// `mc_members` for upsert/hash computation, `db_members` for tag updates.
    async fn prepare_mc_members(
        &self,
        db: &MySqlPool,
    ) -> Result<(Vec<ddb::members::Member>, Vec<mailchimp::members::Member>)> {
        let db_members = self.db_members(db).await?;
        let merge_fields = self.merge_fields()?;
        let db_addresses =
            ddb::members::mailing_address::for_members(db, db_members.iter()).await?;
        let mc_members = ddb::members::mailchimp::to_members_with_address(
            &db_members,
            &db_addresses,
            &merge_fields,
        )
        .await?;
        Ok((db_members, mc_members))
    }

    #[tracing::instrument(skip_all, name = "sync", fields(name = self.name, id = self.id))]
    pub async fn sync(&self, ddb_url: AciDatabaseSettings) -> Result<(usize, usize)> {
        let db = ddb_url.connect().await?;
        tracing::info!("starting sync");
        let start = Instant::now();
        tracing::debug!("querying ddb");
        let (db_members, mc_members) = self.prepare_mc_members(&db).await?;

        tracing::debug!("upserting members");
        let client = self.client()?;
        let upserted = mailchimp::members::upsert_many(
            &client,
            &self.list,
            futures::stream::iter(mc_members),
            RetryPolicy::Retries(3),
        )
        .await?;

        tracing::debug!("deleting removed members");
        let deleted = mailchimp::members::retain(&client, &self.list, &upserted).await?;

        tracing::debug!("updating tags");
        let tag_updates = ddb::members::mailchimp::to_tag_updates(&db_members);
        mailchimp::members::tags::update_many(
            &client,
            &self.list,
            &tag_updates,
            RetryPolicy::with_retries(3),
        )
        .await?;

        let duration = start.elapsed().as_secs();
        tracing::info!(
            deleted,
            upserted = upserted.len(),
            duration,
            "sync completed"
        );

        Ok((deleted, upserted.len()))
    }

    /// Compute what `sync()` would delete from MailChimp, without actually deleting
    /// (and without upserting). Useful for verifying the retain() set before running
    /// destructive changes.
    #[tracing::instrument(skip_all, name = "dry_run", fields(name = self.name, id = self.id))]
    pub async fn dry_run(&self, ddb_url: AciDatabaseSettings) -> Result<DryRunResult> {
        let db = ddb_url.connect().await?;
        let client = self.client()?;
        let audience_query = MembersQuery {
            fields: "members.id,members.email_address,members.status".to_string(),
            ..Default::default()
        };

        // Drupal prep and the MailChimp audience fetch are independent — run
        // them in parallel so the audience round-trips overlap with the db work.
        let (prep, audience) = tokio::try_join!(self.prepare_mc_members(&db), async {
            mailchimp::members::all_collect(&client, &self.list, audience_query)
                .await
                .map_err(anyhow::Error::from)
        },)?;
        let (_db_members, mc_members) = prep;

        // Mirror what upsert_many would produce: the hash of each emitted email.
        // (to_members already filters via is_valid_email at the source.)
        let upserted: HashSet<String> = mc_members
            .iter()
            .map(|m| member_id(&m.email_address))
            .collect();

        let would_delete: Vec<DryRunEntry> = audience
            .into_iter()
            .filter(|m| m.status != Some(mailchimp::members::MemberStatus::Cleaned))
            .filter(|m| !upserted.contains(&m.id))
            .map(|m| DryRunEntry {
                id: m.id,
                email_address: m.email_address,
                status: m.status,
            })
            .collect();

        Ok(DryRunResult {
            name: self.name.clone(),
            upserted: upserted.len(),
            would_delete,
        })
    }
}
