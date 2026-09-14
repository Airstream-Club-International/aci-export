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

fn log_tag_retry(err: &Error, sleep: std::time::Duration) {
    tracing::warn!(%err, sleep = sleep.as_secs(), "tag update");
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
        .filter(|m| m.is_live())
        .filter(|m| !keep_keys.contains(&m.id))
        .map(|m| m.id.clone())
        .collect();

    tracing::debug!(
        count = to_archive.len(),
        audience = audience.len(),
        "archiving members missing from source"
    );
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

/// PATCH the given fields of an existing contact. Only the fields set on
/// `member` are sent, so this is how to change one attribute (such as the
/// email address) without restating the rest of the record.
async fn update(
    client: &Client,
    list_id: &str,
    member_id: &str,
    member: &Member,
) -> Result<Member> {
    client
        .patch(
            &format!("/3.0/lists/{list_id}/members/{member_id}",),
            member,
        )
        .await
}

/// A contact whose email address in MailChimp no longer matches the address
/// the membership database holds for the same user.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct EmailRename {
    /// Current MailChimp contact id (hash of the current address)
    pub id: String,
    pub from: String,
    pub to: String,
}

/// Pair audience contacts with source members by the `UID` merge field and
/// report every contact whose address differs from the source.
///
/// A contact is keyed by the hash of its email, so an address changed on
/// either side (a member editing it on a MailChimp form, or a change in the
/// membership database) makes the source member and the audience contact
/// look like two unrelated records: the sync would create the new address
/// fresh and archive the old one, and the contact's group preferences would
/// go to the archive with it. Renaming the existing contact instead keeps
/// its record, and with it those preferences.
///
/// Archived and cleaned contacts are left alone: they are handled by the
/// resubscribe and retain paths. A rename is also skipped when the target
/// address already exists in the audience (both records present), or when
/// more than one live contact or more than one target carries the same
/// `UID`, or two targets want the same address, since none of those has a
/// single contact to rename to a single address.
pub fn email_renames(audience: &[Member], targets: &[Member]) -> Vec<EmailRename> {
    let audience_ids: HashSet<&str> = audience.iter().map(|m| m.id.as_str()).collect();

    let mut by_uid: HashMap<u64, Vec<&Member>> = HashMap::new();
    for member in audience.iter().filter(|m| m.is_live()) {
        if let Some(uid) = member.uid() {
            by_uid.entry(uid).or_default().push(member);
        }
    }

    let mut targets_per_uid: HashMap<u64, usize> = HashMap::new();
    let mut targets_per_id: HashMap<&str, usize> = HashMap::new();
    for target in targets {
        if let Some(uid) = target.uid() {
            *targets_per_uid.entry(uid).or_default() += 1;
        }
        *targets_per_id.entry(target.id.as_str()).or_default() += 1;
    }

    targets
        .iter()
        .filter(|target| !audience_ids.contains(target.id.as_str()))
        .filter(|target| targets_per_id[target.id.as_str()] == 1)
        .filter_map(|target| {
            let uid = target.uid()?;
            if targets_per_uid[&uid] != 1 {
                return None;
            }
            match by_uid.get(&uid).map(Vec::as_slice) {
                Some([current]) => Some(EmailRename {
                    id: current.id.clone(),
                    from: current.email_address.clone(),
                    to: target.email_address.clone(),
                }),
                _ => None,
            }
        })
        .collect()
}

fn log_rename_retry(err: &Error, sleep: std::time::Duration) {
    tracing::warn!(%err, sleep = sleep.as_secs(), "member rename");
}

/// Outcome of [`rename_many`]: the renames that landed and the ones
/// MailChimp refused.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Renamed {
    pub landed: Vec<EmailRename>,
    pub failed: Vec<EmailRename>,
}

/// Apply [`email_renames`] results. A rename that MailChimp rejects is
/// reported in `failed` rather than failing the run; the caller decides
/// what to do with that contact.
///
/// A rename whose PATCH was applied but whose response was lost fails its
/// retry with a 404 on the old id. Before scoring such a rename as failed,
/// the new id is probed and, if the contact is there, the rename counts as
/// landed.
///
/// A landed rename means the contact is keyed by the hash of its new
/// address from this point on, so the caller's copy of the audience needs
/// [`apply_renames`].
pub async fn rename_many(
    client: &Client,
    list_id: &str,
    renames: &[EmailRename],
    retries: RetryPolicy,
) -> Result<Renamed> {
    let outcome = Arc::new(RwLock::new(Renamed::default()));
    stream::iter(renames)
        .map(Ok::<_, Error>)
        .try_for_each_concurrent(10, |rename| {
            let client = client.clone();
            let outcome = outcome.clone();
            async move {
                let patch = Member {
                    email_address: rename.to.clone(),
                    ..Default::default()
                };
                let result = Retry::spawn_notify(
                    retries,
                    || update(&client, list_id, &rename.id, &patch).map_err(Error::into_retry),
                    log_rename_retry,
                )
                .await;
                let landed = match result {
                    Ok(_) => true,
                    Err(err) => {
                        // A 404 on the old id is what a PATCH that landed
                        // without a response looks like on retry; any other
                        // error is a refusal.
                        let is_gone = matches!(&err, Error::Mailchimp(e) if e.status == 404);
                        let landed = is_gone
                            && for_id(&client, list_id, &member_id(&rename.to))
                                .await
                                .is_ok();
                        if !landed {
                            tracing::warn!(
                                id = rename.id,
                                from = rename.from,
                                to = rename.to,
                                %err,
                                "member rename failed"
                            );
                        }
                        landed
                    }
                };
                let mut outcome = outcome.write().await;
                if landed {
                    outcome.landed.push(rename.clone());
                } else {
                    outcome.failed.push(rename.clone());
                }
                Ok(())
            }
        })
        .await?;
    let outcome = outcome.read().await.clone();
    Ok(outcome)
}

/// Rewrite the audience entries for landed renames so later steps (retain,
/// new-member detection) see the contact under its new address.
pub fn apply_renames(audience: &mut [Member], renamed: &[EmailRename]) {
    let by_id: HashMap<&str, &EmailRename> = renamed.iter().map(|r| (r.id.as_str(), r)).collect();
    for member in audience.iter_mut() {
        if let Some(rename) = by_id.get(member.id.as_str()) {
            member.id = member_id(&rename.to);
            member.email_address = rename.to.clone();
        }
    }
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

    /// Sets at or below this size are sent as direct per-member requests.
    /// A batch is queued on MailChimp's side and waits there whatever its
    /// size, routinely for minutes, so it only pays off for large sets.
    const DIRECT_UPDATE_MAX: usize = 100;

    pub async fn update_many(
        client: &Client,
        list_id: &str,
        tag_updates: &[(String, Vec<MemberTagUpdate>)],
        retries: RetryPolicy,
    ) -> Result {
        if tag_updates.len() <= DIRECT_UPDATE_MAX {
            tracing::debug!(count = tag_updates.len(), "updating tags directly");
            return futures::stream::iter(tag_updates)
                .map(Ok::<_, Error>)
                .try_for_each_concurrent(10, |(member_id, updates)| {
                    let client = client.clone();
                    async move {
                        Retry::spawn_notify(
                            retries,
                            || {
                                update(&client, list_id, member_id, updates)
                                    .map_err(Error::into_retry)
                            },
                            log_tag_retry,
                        )
                        .await
                    }
                })
                .await;
        }
        tracing::debug!(count = tag_updates.len(), "updating tags in batches");
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
                let info = Retry::spawn_notify(
                    retries,
                    || batch.run(&client, true).map_err(Error::into_retry),
                    log_batch_tag_retry,
                )
                .await?;
                if info.errored_operations > 0 {
                    return Err(Error::BatchPartialFailure {
                        batch_id: info.id,
                        errored: info.errored_operations,
                        total: info.total_operations,
                    });
                }
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
    /// Group (interest) membership keyed by interest id. Only sent when set;
    /// MailChimp leaves any interest not named in the map untouched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interests: Option<HashMap<String, bool>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags_count: Option<u16>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<MemberTag>,
}

impl Member {
    /// A contact that campaigns can still reach or that a member can still
    /// act on. Archived and cleaned contacts are neither; they are handled
    /// only by the resubscribe path.
    pub fn is_live(&self) -> bool {
        !matches!(
            self.status,
            Some(MemberStatus::Archived) | Some(MemberStatus::Cleaned)
        )
    }

    /// The membership database user id carried in the `UID` merge field.
    /// MailChimp returns a number for a populated numeric field and an empty
    /// string for an unset one; a numeric string is accepted as well.
    pub fn uid(&self) -> Option<u64> {
        match self.merge_fields.as_ref()?.get("UID")? {
            serde_json::Value::Number(n) => n.as_u64(),
            serde_json::Value::String(s) => s.trim().parse().ok(),
            _ => None,
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn contact(email: &str, uid: Option<u64>, status: MemberStatus) -> Member {
        let mut merge_fields = HashMap::new();
        merge_fields.insert(
            "UID".to_string(),
            match uid {
                Some(uid) => serde_json::json!(uid),
                None => serde_json::json!(""),
            },
        );
        Member {
            id: member_id(email),
            email_address: email.to_string(),
            status: Some(status),
            merge_fields: Some(merge_fields),
            ..Default::default()
        }
    }

    fn source(email: &str, uid: u64) -> Member {
        contact(email, Some(uid), MemberStatus::Noop)
    }

    #[test]
    fn uid_reads_number_and_numeric_string_and_rejects_empty() {
        assert_eq!(
            contact("a@x.org", Some(7), MemberStatus::Subscribed).uid(),
            Some(7)
        );
        let mut m = contact("a@x.org", None, MemberStatus::Subscribed);
        assert_eq!(m.uid(), None);
        m.merge_fields
            .as_mut()
            .expect("merge fields")
            .insert("UID".into(), serde_json::json!("42"));
        assert_eq!(m.uid(), Some(42));
        m.merge_fields = None;
        assert_eq!(m.uid(), None);
    }

    #[test]
    fn address_changed_on_either_side_is_a_rename() {
        let audience = vec![contact("old@x.org", Some(1), MemberStatus::Subscribed)];
        let targets = vec![source("new@x.org", 1)];
        assert_eq!(
            email_renames(&audience, &targets),
            vec![EmailRename {
                id: member_id("old@x.org"),
                from: "old@x.org".into(),
                to: "new@x.org".into(),
            }]
        );
    }

    #[test]
    fn unsubscribed_contact_is_still_renamed() {
        // Preferences on an unsubscribed record are as worth keeping as any;
        // only archived and cleaned contacts are excluded.
        let audience = vec![contact("old@x.org", Some(1), MemberStatus::Unsubscribed)];
        let targets = vec![source("new@x.org", 1)];
        assert_eq!(email_renames(&audience, &targets).len(), 1);
    }

    #[test]
    fn matching_address_is_not_a_rename() {
        let audience = vec![contact("same@x.org", Some(1), MemberStatus::Subscribed)];
        let targets = vec![source("same@x.org", 1)];
        assert_eq!(email_renames(&audience, &targets), vec![]);
    }

    #[test]
    fn archived_and_cleaned_contacts_are_left_to_retain_and_resubscribe() {
        for status in [MemberStatus::Archived, MemberStatus::Cleaned] {
            let audience = vec![contact("old@x.org", Some(1), status)];
            let targets = vec![source("new@x.org", 1)];
            assert_eq!(email_renames(&audience, &targets), vec![]);
        }
    }

    #[test]
    fn target_already_present_is_not_a_rename() {
        // Both addresses exist as contacts: nothing to rename, the old one
        // goes through retain.
        let audience = vec![
            contact("old@x.org", Some(1), MemberStatus::Subscribed),
            contact("new@x.org", Some(1), MemberStatus::Subscribed),
        ];
        let targets = vec![source("new@x.org", 1)];
        assert_eq!(email_renames(&audience, &targets), vec![]);
    }

    #[test]
    fn duplicate_uid_in_audience_is_ambiguous() {
        let audience = vec![
            contact("one@x.org", Some(1), MemberStatus::Subscribed),
            contact("two@x.org", Some(1), MemberStatus::Subscribed),
        ];
        let targets = vec![source("three@x.org", 1)];
        assert_eq!(email_renames(&audience, &targets), vec![]);
    }

    #[test]
    fn two_targets_with_one_uid_are_ambiguous() {
        let audience = vec![contact("old@x.org", Some(1), MemberStatus::Subscribed)];
        let targets = vec![source("a@x.org", 1), source("b@x.org", 1)];
        assert_eq!(email_renames(&audience, &targets), vec![]);
    }

    #[test]
    fn two_targets_wanting_one_address_are_ambiguous() {
        // A household consolidating onto one inbox: two live contacts, one
        // address wanted by both. Neither is renamed.
        let audience = vec![
            contact("a@x.org", Some(1), MemberStatus::Subscribed),
            contact("b@x.org", Some(2), MemberStatus::Subscribed),
        ];
        let targets = vec![source("shared@x.org", 1), source("shared@x.org", 2)];
        assert_eq!(email_renames(&audience, &targets), vec![]);
    }

    #[test]
    fn is_live_excludes_archived_and_cleaned_only() {
        for (status, live) in [
            (MemberStatus::Subscribed, true),
            (MemberStatus::Unsubscribed, true),
            (MemberStatus::Pending, true),
            (MemberStatus::Transactional, true),
            (MemberStatus::Archived, false),
            (MemberStatus::Cleaned, false),
        ] {
            assert_eq!(
                contact("a@x.org", None, status.clone()).is_live(),
                live,
                "{status:?}"
            );
        }
    }

    #[test]
    fn missing_uid_on_either_side_is_ignored() {
        let audience = vec![contact("old@x.org", None, MemberStatus::Subscribed)];
        let targets = vec![source("new@x.org", 1)];
        assert_eq!(email_renames(&audience, &targets), vec![]);

        let audience = vec![contact("old@x.org", Some(1), MemberStatus::Subscribed)];
        let targets = vec![contact("new@x.org", None, MemberStatus::Noop)];
        assert_eq!(email_renames(&audience, &targets), vec![]);
    }

    #[test]
    fn rename_patch_carries_only_the_address() {
        let patch = Member {
            email_address: "new@x.org".into(),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(&patch).expect("serialize"),
            serde_json::json!({"email_address": "new@x.org"})
        );
    }
}

#[cfg(test)]
mod apply_renames_tests {
    use super::*;

    #[test]
    fn landed_rename_rekeys_the_audience_entry() {
        let mut audience = vec![
            Member {
                id: member_id("old@x.org"),
                email_address: "old@x.org".into(),
                ..Default::default()
            },
            Member {
                id: member_id("other@x.org"),
                email_address: "other@x.org".into(),
                ..Default::default()
            },
        ];
        apply_renames(
            &mut audience,
            &[EmailRename {
                id: member_id("old@x.org"),
                from: "old@x.org".into(),
                to: "new@x.org".into(),
            }],
        );
        assert_eq!(audience[0].id, member_id("new@x.org"));
        assert_eq!(audience[0].email_address, "new@x.org");
        assert_eq!(audience[1].email_address, "other@x.org");
    }
}
