use crate::{Error, Result, settings::AciDatabaseSettings};
use chrono::{DateTime, Utc};
use futures::TryFutureExt;
use mailchimp::{
    RetryPolicy,
    interests::{Interests, Resolved},
    members::{self, EmailRename, Member, MemberStatus, MembersQuery, member_id},
    merge_fields::MergeFields,
};
use sqlx::{Database, Encode, MySqlPool, PgPool, Type, query::QueryAs};
use std::{collections::HashSet, time::Instant};

#[derive(Debug, serde::Serialize)]
pub struct JobSyncResult {
    pub name: String,
    pub renamed: usize,
    /// Renames MailChimp refused. The contact is left as it was, under its
    /// old address, and its source member is skipped this run.
    pub rename_failed: usize,
    pub archived: usize,
    pub resubscribed: usize,
    pub upserted: usize,
    /// Members new to the audience that received the default email
    /// preferences. Zero when the job has no preference group configured
    /// or the group does not exist on the audience yet.
    pub defaulted: usize,
}

#[derive(Debug, serde::Serialize)]
pub struct DryRunResult {
    pub name: String,
    pub upserted: usize,
    pub would_rename: Vec<EmailRename>,
    pub would_resubscribe: Vec<DryRunEntry>,
    pub would_archive: Vec<DryRunEntry>,
}

#[derive(Debug, serde::Serialize)]
pub struct DryRunEntry {
    pub id: String,
    pub email_address: String,
    pub status: Option<MemberStatus>,
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

    fn kind(&self) -> AudienceKind {
        if self.club.is_some() {
            AudienceKind::Club
        } else if self.region.is_some() {
            AudienceKind::Region
        } else {
            AudienceKind::All
        }
    }

    fn merge_fields(&self) -> Result<MergeFields> {
        match self.kind() {
            AudienceKind::Club => MergeFields::club(),
            AudienceKind::Region | AudienceKind::All => MergeFields::all(),
        }
        .map_err(Error::from)
    }

    /// The email preference group this job maintains. Only the all-members
    /// audience carries one; club and region audiences have none.
    fn interests(&self) -> Result<Option<Interests>> {
        match self.kind() {
            AudienceKind::All => Interests::all().map(Some).map_err(Error::from),
            AudienceKind::Club | AudienceKind::Region => Ok(None),
        }
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
                        Ok(result) => Some((id, result)),
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

    /// Audience query used by `sync` and `dry_run`. We need `status` to skip
    /// already-archived contacts in `retain`, `tags` so we can tell
    /// sync-archived (safe to resubscribe) from admin-archived (must remain
    /// archived) when a previously-archived member reappears in the source,
    /// and the `UID` merge field to pair a contact with its source member
    /// when the email address differs.
    fn audience_query() -> MembersQuery {
        MembersQuery {
            fields: "members.id,members.email_address,members.status,members.tags,members.merge_fields.UID".to_string(),
            ..Default::default()
        }
    }

    /// Ids of audience members archived by a previous sync run (status =
    /// archived AND tagged with [`SYNC_ARCHIVED_TAG`]). Admin-archived
    /// contacts lack the tag and are deliberately excluded so we never
    /// resubscribe a contact the audience owner archived by hand.
    fn sync_archived_ids(audience: &[Member]) -> HashSet<String> {
        audience
            .iter()
            .filter(|m| m.status == Some(MemberStatus::Archived))
            .filter(|m| m.tags.iter().any(|t| t.name == members::SYNC_ARCHIVED_TAG))
            .map(|m| m.id.clone())
            .collect()
    }

    /// Resolve the job's preference group on the audience, or `None` when
    /// the job has none or the group is not on the audience yet.
    async fn resolved_interests(&self, client: &mailchimp::Client) -> Result<Option<Resolved>> {
        let Some(interests) = self.interests()? else {
            return Ok(None);
        };
        let resolved = interests.resolve(client, &self.list).await?;
        if resolved.is_none() {
            tracing::warn!(
                category = interests.category.title,
                "preference group not on audience; new members get no defaults"
            );
        }
        Ok(resolved)
    }

    #[tracing::instrument(skip_all, name = "sync", fields(name = self.name, id = self.id))]
    pub async fn sync(&self, ddb_url: AciDatabaseSettings) -> Result<JobSyncResult> {
        let db = ddb_url.connect().await?;
        let client = self.client()?;
        tracing::info!("starting sync");
        let start = Instant::now();
        tracing::debug!("querying ddb and audience");

        // Run the Drupal prep and the MailChimp audience fetch in parallel; we
        // need both before we can upsert (the audience tells us which members
        // are sync-archived and need their status explicitly restored).
        // Every read completes before the first write.
        let (prep, mut audience, resolved) = tokio::try_join!(
            self.prepare_mc_members(&db),
            async {
                members::all_collect(&client, &self.list, Self::audience_query())
                    .await
                    .map_err(anyhow::Error::from)
            },
            self.resolved_interests(&client),
        )?;
        let (db_members, mut mc_members) = prep;

        // Contacts whose address changed on either side keep their record
        // (and their preferences) by being renamed before the upsert, rather
        // than recreated under the new address and archived under the old.
        let renames = members::email_renames(&audience, &mc_members);
        tracing::debug!(count = renames.len(), "renaming members");
        let renamed =
            members::rename_many(&client, &self.list, &renames, RetryPolicy::with_retries(3))
                .await?;
        members::apply_renames(&mut audience, &renamed.landed);

        // A refused rename leaves the contact under its old address. Its
        // source member is withheld from the upsert so the new address is
        // not created beside it, and the old contact is kept out of retain
        // so it is not archived. Both stay as they are until a later run
        // succeeds.
        let withheld: HashSet<String> = renamed.failed.iter().map(|r| member_id(&r.to)).collect();
        mc_members.retain(|m| !withheld.contains(&m.id));
        let mut keep: HashSet<String> = renamed.failed.iter().map(|r| r.id.clone()).collect();

        // For any member returning from a sync-driven archive, set status =
        // Subscribed on the PUT so MailChimp lifts the archive. Other members
        // keep `status_if_new` alone, so user-initiated unsubscribes via a
        // campaign link stay intact.
        let sync_archived = Self::sync_archived_ids(&audience);
        let mut resubscribed = 0;
        for member in &mut mc_members {
            if sync_archived.contains(&member.id) {
                member.status = Some(MemberStatus::Subscribed);
                resubscribed += 1;
            }
        }

        let audience_ids: HashSet<String> = audience.iter().map(|m| m.id.clone()).collect();
        let defaulted = match &resolved {
            Some(resolved) => {
                default_interests(&mut mc_members, &audience_ids, &sync_archived, resolved)
            }
            None => 0,
        };

        tracing::debug!(resubscribed, defaulted, "upserting members");
        let upserted = members::upsert_many(
            &client,
            &self.list,
            futures::stream::iter(mc_members),
            RetryPolicy::Retries(3),
        )
        .await?;

        tracing::debug!("archiving removed members");
        keep.extend(upserted.iter().cloned());
        let archived = members::retain(&client, &self.list, &audience, &keep).await?;

        tracing::debug!("updating tags");
        let tag_updates = ddb::members::mailchimp::to_tag_updates(&db_members);
        members::tags::update_many(
            &client,
            &self.list,
            &tag_updates,
            RetryPolicy::with_retries(3),
        )
        .await?;

        let duration = start.elapsed().as_secs();
        let result = JobSyncResult {
            name: self.name.clone(),
            renamed: renamed.landed.len(),
            rename_failed: renamed.failed.len(),
            archived,
            resubscribed,
            upserted: upserted.len(),
            defaulted,
        };
        tracing::info!(
            renamed = result.renamed,
            rename_failed = result.rename_failed,
            archived,
            resubscribed,
            upserted = result.upserted,
            defaulted,
            duration,
            "sync completed"
        );
        Ok(result)
    }

    /// Compute what `sync()` would archive and resubscribe in MailChimp,
    /// without actually performing those writes (and without upserting).
    /// Useful for verifying the retain set and the resubscribe set before
    /// running destructive changes.
    #[tracing::instrument(skip_all, name = "dry_run", fields(name = self.name, id = self.id))]
    pub async fn dry_run(&self, ddb_url: AciDatabaseSettings) -> Result<DryRunResult> {
        let db = ddb_url.connect().await?;
        let client = self.client()?;

        // Drupal prep and the MailChimp audience fetch are independent — run
        // them in parallel so the audience round-trips overlap with the db work.
        let (prep, mut audience) = tokio::try_join!(self.prepare_mc_members(&db), async {
            members::all_collect(&client, &self.list, Self::audience_query())
                .await
                .map_err(anyhow::Error::from)
        })?;
        let (_db_members, mc_members) = prep;

        // Renames happen before the upsert and change the audience's view of
        // the renamed contacts; mirror that so the archive set matches sync().
        let would_rename = members::email_renames(&audience, &mc_members);
        members::apply_renames(&mut audience, &would_rename);

        // Mirror what upsert_many would produce: the hash of each emitted email.
        // (to_members already filters via is_valid_email at the source.)
        let upserted: HashSet<String> = mc_members
            .iter()
            .map(|m| member_id(&m.email_address))
            .collect();

        let sync_archived = Self::sync_archived_ids(&audience);
        let would_resubscribe: Vec<DryRunEntry> = audience
            .iter()
            .filter(|m| sync_archived.contains(&m.id) && upserted.contains(&m.id))
            .map(|m| DryRunEntry {
                id: m.id.clone(),
                email_address: m.email_address.clone(),
                status: m.status.clone(),
            })
            .collect();

        let would_archive: Vec<DryRunEntry> = audience
            .into_iter()
            .filter(|m| m.is_live())
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
            would_rename,
            would_resubscribe,
            would_archive,
        })
    }

    /// Create the job's email preference group and interests on the
    /// audience if they are missing. Returns the resolved ids and the names
    /// of the interests created this call.
    #[tracing::instrument(skip_all, name = "interests", fields(name = self.name, id = self.id))]
    pub async fn sync_interests(
        &self,
    ) -> Result<Option<(mailchimp::interests::Resolved, Vec<String>)>> {
        let Some(interests) = self.interests()? else {
            return Ok(None);
        };
        let client = self.client()?;
        interests
            .sync(&client, &self.list)
            .map_ok(Some)
            .map_err(Error::from)
            .await
    }

    /// Switch every configured interest on for every contact in the
    /// audience that is not archived or cleaned. A one-time step when the
    /// group is first introduced; after that only new members are
    /// defaulted, by `sync`.
    ///
    /// Returns the number of contacts updated, or `None` when the job has no
    /// preference group or the group does not exist on the audience.
    #[tracing::instrument(skip_all, name = "seed_interests", fields(name = self.name, id = self.id))]
    pub async fn seed_interests(&self) -> Result<Option<usize>> {
        let Some(interests) = self.interests()? else {
            return Ok(None);
        };
        let client = self.client()?;
        let Some(resolved) = interests.resolve(&client, &self.list).await? else {
            return Ok(None);
        };
        let audience = members::all_collect(&client, &self.list, Self::audience_query()).await?;
        let member_ids: Vec<String> = audience
            .into_iter()
            .filter(|m| m.is_live())
            .map(|m| m.id)
            .collect();
        mailchimp::interests::update_many(
            &client,
            &self.list,
            &member_ids,
            &resolved.all_on(),
            RetryPolicy::with_retries(3),
        )
        .await?;
        Ok(Some(member_ids.len()))
    }
}

/// Which slice of the membership a job mirrors. Decides the merge field set
/// and whether the audience carries the email preference group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AudienceKind {
    All,
    Club,
    Region,
}

/// Switch every preference on for members who are new to the audience or
/// returning from a sync-driven archive, and count them. Every other
/// contact is left untouched: their preferences are theirs to set on the
/// hosted preferences page.
///
/// Returning members get the defaults because the contact may have been
/// archived before the group existed or before it was seeded, in which
/// case it would come back live with every preference off.
fn default_interests(
    mc_members: &mut [Member],
    audience_ids: &HashSet<String>,
    sync_archived: &HashSet<String>,
    resolved: &Resolved,
) -> usize {
    let mut defaulted = 0;
    for member in mc_members.iter_mut() {
        if !audience_ids.contains(&member.id) || sync_archived.contains(&member.id) {
            member.interests = Some(resolved.all_on());
            defaulted += 1;
        }
    }
    defaulted
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(email: &str) -> Member {
        Member {
            id: member_id(email),
            email_address: email.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn defaults_go_to_new_and_returning_members_only() {
        let resolved = Resolved {
            category_id: "cat".into(),
            ids: vec!["i1".into()],
        };
        let mut members = vec![
            member("new@x.org"),
            member("back@x.org"),
            member("same@x.org"),
        ];
        let audience_ids: HashSet<String> =
            [member_id("back@x.org"), member_id("same@x.org")].into();
        let sync_archived: HashSet<String> = [member_id("back@x.org")].into();

        let defaulted = default_interests(&mut members, &audience_ids, &sync_archived, &resolved);

        assert_eq!(defaulted, 2);
        assert_eq!(members[0].interests, Some(resolved.all_on()));
        assert_eq!(members[1].interests, Some(resolved.all_on()));
        assert_eq!(members[2].interests, None);
    }

    #[test]
    fn audience_kind_follows_club_then_region() {
        let job = Job::default();
        assert_eq!(job.kind(), AudienceKind::All);
        let club = Job {
            club: Some(1),
            ..Default::default()
        };
        assert_eq!(club.kind(), AudienceKind::Club);
        let region = Job {
            region: Some(1),
            ..Default::default()
        };
        assert_eq!(region.kind(), AudienceKind::Region);
        assert!(region.interests().expect("config").is_none());
        assert!(club.interests().expect("config").is_none());
        assert!(job.interests().expect("config").is_some());
    }
}
