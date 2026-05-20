use crate::{
    Client, Error, NO_QUERY, Result, RetryPolicy, Stream, batches, deserialize_null_string,
    paged_query_impl, paged_response_impl, query_default_impl,
};
use futures::{
    TryFutureExt,
    stream::{self, Stream as StdStream, StreamExt, TryStreamExt},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::RwLock;
use tokio_retry2::Retry;

fn log_batch_member_retry(err: &Error, sleep: std::time::Duration) {
    tracing::warn!(%err, sleep = sleep.as_secs(), "batch member update");
}

/// MailChimp returns a per-member error when a contact is locked out of
/// resubscribe by an abuse complaint, hard bounce, or unsubscribe — both the
/// title ("Member In Compliance State") and the detail string contain the
/// phrase "compliance state". The batch endpoint propagates the same phrase
/// into `error`. Matching the phrase keeps us robust to MailChimp not
/// committing to a stable `error_code` for this case across SDK versions.
fn is_compliance_error(err: &MemberBatchUpsertError) -> bool {
    err.error.contains("compliance state")
}

fn log_batch_tag_retry(err: &Error, sleep: std::time::Duration) {
    tracing::warn!(%err, sleep = sleep.as_secs(), "batch tag update");
}

pub fn all(client: &Client, list_id: &str, query: MembersQuery) -> Stream<Member> {
    client.fetch_stream::<MembersQuery, MembersResponse>(
        &format!("/3.0/lists/{list_id}/members"),
        query,
    )
}

pub async fn all_collect(
    client: &Client,
    list_id: &str,
    mut query: MembersQuery,
) -> Result<Vec<Member>> {
    // If the caller restricted fields, ensure total_items is included so we can
    // bound the parallel page dispatch.
    if !query.fields.is_empty() && !query.fields.split(',').any(|f| f.trim() == "total_items") {
        query.fields = format!("{},total_items", query.fields);
    }

    let path = format!("/3.0/lists/{list_id}/members");
    let mut first = query.clone();
    first.offset = 0;
    let first_page: MembersResponse = client.fetch(&path, &first).await?;
    let total = first_page.total_items as usize;
    let page_size = query.count.max(1);

    let mut all_members = first_page.members;
    if total <= page_size {
        return Ok(all_members);
    }

    let remaining: Vec<Vec<Member>> = stream::iter((page_size..total).step_by(page_size))
        .map(|offset| {
            let mut page = query.clone();
            page.offset = offset;
            let path = path.clone();
            let client = client.clone();
            async move {
                client
                    .fetch::<MembersResponse, _>(&path, &page)
                    .map_ok(|response| response.members)
                    .await
            }
        })
        .buffered(10)
        .try_collect()
        .await?;
    all_members.extend(remaining.into_iter().flatten());
    Ok(all_members)
}

pub async fn get(client: &Client, list_id: &str, member_id: &str) -> Result<Member> {
    client
        .fetch(
            &format!("/3.0/lists/{list_id}/members/{member_id}"),
            NO_QUERY,
        )
        .await
}

/// HTTP DELETE on a member archives them in MailChimp — the contact is
/// retained with `status: "archived"`, excluded from campaigns and counts.
/// Permanent removal goes through a different endpoint.
pub async fn delete(client: &Client, list_id: &str, member_id: &str) -> Result<()> {
    client
        .delete(&format!("/3.0/lists/{list_id}/members/{member_id}"))
        .await
}

/// Tag applied immediately before a sync-driven archive in [`retain`], so a
/// later sync run can tell sync-archived contacts (safe to re-subscribe when
/// they reappear in the source) apart from contacts the audience owner
/// archived by hand in the MailChimp UI (must stay archived).
pub const SYNC_ARCHIVED_TAG: &str = "archived-by-sync";

/// Archive every audience member not in `keep_keys`.
///
/// The audience is supplied by the caller (typically fetched once at the
/// start of a sync run and reused) rather than refetched here. Members whose
/// status is already `Archived` or `Cleaned` are skipped — there is nothing
/// to archive, and re-touching an already-archived contact would clobber the
/// admin-archived signal the next sync uses to decide whether to resubscribe.
///
/// Before each archive we set the [`SYNC_ARCHIVED_TAG`] tag so a future sync
/// run can recognize sync-driven archives and resubscribe them safely.
///
/// Returns the number of newly archived members.
pub async fn retain(
    client: &Client,
    list_id: &str,
    audience: &[Member],
    keep_keys: &HashSet<String>,
) -> Result<usize> {
    let to_archive: Vec<String> = audience
        .iter()
        .filter(|m| {
            !matches!(
                m.status,
                Some(MemberStatus::Cleaned) | Some(MemberStatus::Archived)
            )
        })
        .filter(|m| !keep_keys.contains(&m.id))
        .map(|m| m.id.clone())
        .collect();

    if to_archive.is_empty() {
        return Ok(0);
    }

    // Tag before archiving so the next sync can distinguish these from
    // admin-archived contacts. If the tag write fails we abort rather than
    // archiving without the marker — better to leave a stale audience entry
    // than to lose the ability to resubscribe a renewing member later.
    let tag_updates: Vec<(String, Vec<MemberTagUpdate>)> = to_archive
        .iter()
        .map(|id| {
            (
                id.clone(),
                vec![MemberTagUpdate {
                    name: SYNC_ARCHIVED_TAG.to_string(),
                    status: MemberTagStatus::Active,
                }],
            )
        })
        .collect();
    tags::update_many(client, list_id, &tag_updates, RetryPolicy::with_retries(3)).await?;

    futures::stream::iter(to_archive.iter())
        .map(|member_id| Ok::<_, crate::Error>((client.clone(), member_id)))
        .try_for_each_concurrent(10, |(client, member_id)| async move {
            delete(&client, list_id, member_id)
                .await
                .inspect_err(|err| tracing::error!(id = member_id, ?err, "failed to archive"))?;
            Ok(())
        })
        .await?;
    Ok(to_archive.len())
}

pub async fn for_email(client: &Client, list_id: &str, email: &str) -> Result<Member> {
    for_id(client, list_id, &member_id(email)).await
}

pub async fn for_id(client: &Client, list_id: &str, member_id: &str) -> Result<Member> {
    get(client, list_id, member_id).await
}

pub fn member_id(email: &str) -> String {
    format!("{:x}", md5::compute(email.to_lowercase()))
}

pub fn is_valid_email(email: &str) -> bool {
    let email = email.to_lowercase();
    !(email.is_empty() || email.ends_with("noemail.com") || email.ends_with("example.com"))
}

pub async fn upsert(
    client: &Client,
    list_id: &str,
    member_id: &str,
    member: &Member,
) -> Result<Member> {
    client
        .put(
            &format!("/3.0/lists/{list_id}/members/{member_id}",),
            member,
        )
        .await
}

/// Recommended max batch upsert size.
///
/// The Mailchimp docs state that batches up to 500 can be upserted
/// but in practice that size ends up timing out requests.   
pub const MEMBER_BATCH_UPSERT_MAX: usize = 300;

/// Upsert a given list of members into the given list
///
/// Retursn the list of ids of upserted members
pub async fn upsert_many(
    client: &Client,
    list_id: &str,
    members: impl StdStream<Item = Member>,
    retries: RetryPolicy,
) -> Result<HashSet<String>> {
    let upserted = Arc::new(RwLock::new(HashSet::new()));
    // chunk in max sizes and yse batch_upsert to upsert the members in the list
    members
        .chunks(MEMBER_BATCH_UPSERT_MAX)
        .map(Ok::<Vec<_>, Error>)
        .map_ok(|members| (client.clone(), members, upserted.clone(), retries))
        .try_for_each_concurrent(8, |(client, members, processed, retries)| async move {
            let response = Retry::spawn_notify(
                retries,
                || batch_upsert(&client, list_id, &members).map_err(Error::into_retry),
                log_batch_member_retry,
            )
            .await?;
            let mut set = processed.write().await;
            response
                .updated_members
                .into_iter()
                .chain(response.new_members)
                .for_each(|entry| {
                    set.insert(entry.id);
                });
            if response.error_count > 0 {
                response.errors.iter().for_each(|err| {
                    if is_compliance_error(err) {
                        // MailChimp won't let an abuse-complaint / hard-bounce
                        // / unsubscribe-locked contact be re-subscribed via the
                        // API. They have to opt back in themselves. Log
                        // distinctly so this is searchable in the sync log
                        // (separate from generic upsert failures).
                        tracing::warn!(
                            email = err.email_address,
                            error_code = err.error_code,
                            err = err.error,
                            kind = "compliance",
                            "member locked in compliance state (abuse, bounce, or unsubscribe) — cannot resubscribe via API"
                        );
                    } else {
                        tracing::warn!(
                            email = err.email_address,
                            error_code = err.error_code,
                            err = err.error,
                            "mailchimp upsert error"
                        );
                    }
                })
            }
            Ok(())
        })
        .await?;
    let inner = upserted.read_owned().await;
    Ok(inner.to_owned())
}

#[derive(Default, Debug, Deserialize)]
pub struct MemberBatchUpsertResponse {
    pub updated_members: Vec<Member>,
    pub new_members: Vec<Member>,
    pub total_created: u16,
    pub total_updated: u16,
    pub error_count: u16,
    pub errors: Vec<MemberBatchUpsertError>,
}

#[derive(Default, Debug, Deserialize)]
pub struct MemberBatchUpsertError {
    pub email_address: String,
    pub error: String,
    pub error_code: String,
    pub field: Option<String>,
    pub field_message: Option<String>,
}

pub async fn batch_upsert(
    client: &Client,
    list_id: &str,
    members: &[Member],
) -> Result<MemberBatchUpsertResponse> {
    #[derive(Serialize, Default)]
    struct MemberBatchUpsertRequest<'a> {
        members: &'a [Member],
        update_existing: bool,
    }
    let batch_request = MemberBatchUpsertRequest {
        members,
        update_existing: true,
    };
    client
        .post(&format!("/3.0/lists/{list_id}/",), &batch_request)
        .await
}

pub mod tags {
    use super::*;

    pub async fn for_id(client: &Client, list_id: &str, member_id: &str) -> Result<Vec<MemberTag>> {
        client
            .fetch(
                &format!("/3.0/lists/{list_id}/members/{member_id}/tags"),
                NO_QUERY,
            )
            .await
    }

    #[derive(Debug, Serialize)]
    struct TagsUpdateRequestBody<'a> {
        tags: &'a [MemberTagUpdate],
    }

    fn tags_update_path(prefix: &str, list_id: &str, member_id: &str) -> String {
        format!("{prefix}/lists/{list_id}/members/{member_id}/tags")
    }

    pub async fn update(
        client: &Client,
        list_id: &str,
        member_id: &str,
        updates: &[MemberTagUpdate],
    ) -> Result {
        let body = TagsUpdateRequestBody { tags: updates };
        client
            .post(&tags_update_path("/3.0", list_id, member_id), &body)
            .await
    }

    pub async fn update_many(
        client: &Client,
        list_id: &str,
        tag_updates: &[(String, Vec<MemberTagUpdate>)],
        retries: RetryPolicy,
    ) -> Result {
        futures::stream::iter(tag_updates)
            .chunks(1000)
            .map(Ok::<Vec<_>, Error>)
            .map_ok(|updates| (client.clone(), updates, retries))
            .try_for_each_concurrent(10, |(client, updates, retries)| async move {
                let mut batch = batches::Batch::default();
                for (member_id, updates) in updates {
                    let operation = batch::update(&mut batch, list_id, member_id, updates)?;
                    operation.operation_id = member_id.to_owned();
                }
                Retry::spawn_notify(
                    retries,
                    || batch.run(&client, true).map_err(Error::into_retry),
                    log_batch_tag_retry,
                )
                .await?;
                Ok(())
            })
            .await
    }

    pub mod batch {
        use super::*;

        pub fn update<'a>(
            batch: &'a mut batches::Batch,
            list_id: &str,
            member_id: &str,
            updates: &[MemberTagUpdate],
        ) -> Result<&'a mut batches::BatchOperation> {
            let body = TagsUpdateRequestBody { tags: updates };
            batch.post(&tags_update_path("", list_id, member_id), &body)
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default)]
#[serde(rename_all = "lowercase")]
pub enum MemberStatus {
    Subscribed,
    Unsubscribed,
    Cleaned,
    Pending,
    Transactional,
    Archived,
    #[default]
    Noop,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default)]
pub struct Member {
    #[serde(
        default,
        skip_serializing_if = "String::is_empty",
        deserialize_with = "deserialize_null_string::deserialize"
    )]
    pub id: String,
    #[serde(
        default,
        skip_serializing_if = "String::is_empty",
        deserialize_with = "deserialize_null_string::deserialize"
    )]
    pub email_address: String,
    #[serde(
        default,
        skip_serializing_if = "String::is_empty",
        deserialize_with = "deserialize_null_string::deserialize"
    )]
    pub full_name: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::deserialize_member_status::deserialize"
    )]
    pub status_if_new: Option<MemberStatus>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::deserialize_member_status::deserialize"
    )]
    pub status: Option<MemberStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_fields: Option<HashMap<String, serde_json::Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags_count: Option<u16>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<MemberTag>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct MemberTag {
    pub name: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct MemberTagUpdate {
    pub name: String,
    pub status: MemberTagStatus,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default)]
#[serde(rename_all = "lowercase")]
pub enum MemberTagStatus {
    #[default]
    Active,
    Inactive,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MembersQuery {
    pub fields: String,
    pub count: usize,
    pub offset: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MembersResponse {
    pub members: Vec<Member>,
    #[serde(default)]
    pub total_items: u64,
}

query_default_impl!(MembersQuery);
paged_query_impl!(
    MembersQuery,
    &["members.id", "members.email_address", "members.full_name",]
);
paged_response_impl!(MembersResponse, members, Member);
