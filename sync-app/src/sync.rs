use crate::{
    Context, Result,
    settings::{AciDatabaseSettings, AppSettings},
};
use db::{address, brn, club, leadership, member, region, standing_committee, user};
use itertools::Itertools;
use serde::Serialize;
use sqlx::PgPool;
use std::{collections::HashMap, time::Instant};

#[derive(Debug, Serialize)]
pub struct SyncStats {
    pub upserted: u64,
    pub deleted: u64,
    pub duration: u64,
}

impl SyncStats {
    fn new(upserted: u64, duration: u64) -> Self {
        Self {
            upserted,
            deleted: 0,
            duration,
        }
    }
}

pub type SyncStatsMap = std::collections::HashMap<String, SyncStats>;

pub async fn upsert_regions<I>(
    db: &PgPool,
    regions: I,
) -> Result<((String, SyncStats), Vec<region::Region>)>
where
    I: IntoIterator<Item = ddb::regions::Region>,
{
    let start = Instant::now();
    let db_regions = regions.into_iter().map(region::Region::from).collect_vec();
    let upserted = region::upsert_many(db, &db_regions).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(upserted, duration, "upserted regions");
    Ok((
        ("regions".to_string(), SyncStats::new(upserted, duration)),
        db_regions,
    ))
}

pub async fn retain_regions(
    db: &PgPool,
    stats: &mut (String, SyncStats),
    db_regions: &[region::Region],
) -> Result<()> {
    let start = Instant::now();
    let deleted = region::retain(db, db_regions).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(deleted, duration, "gc regions");
    stats.1.deleted = deleted;
    stats.1.duration += duration;
    Ok(())
}

pub async fn upsert_clubs<I>(
    db: &PgPool,
    clubs: I,
) -> Result<((String, SyncStats), Vec<club::Club>)>
where
    I: IntoIterator<Item = ddb::clubs::Club>,
{
    let start = Instant::now();
    let db_clubs = clubs.into_iter().map(club::Club::from).collect_vec();
    let upserted = club::upsert_many(db, &db_clubs).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(upserted, duration, "upserted clubs");
    Ok((
        ("clubs".into(), SyncStats::new(upserted, duration)),
        db_clubs,
    ))
}

pub async fn retain_clubs(
    db: &PgPool,
    stats: &mut (String, SyncStats),
    db_clubs: &[club::Club],
) -> Result<()> {
    let start = Instant::now();
    let deleted = club::retain(db, db_clubs).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(deleted, duration, "gc clubs");
    stats.1.deleted = deleted;
    stats.1.duration += duration;
    Ok(())
}

pub async fn upsert_users<I>(
    db: &PgPool,
    users: I,
) -> Result<((String, SyncStats), Vec<user::User>)>
where
    I: IntoIterator<Item = ddb::users::User>,
{
    let start = Instant::now();
    let db_users = users.into_iter().map(user::User::from).collect_vec();
    let upserted = user::upsert_many(db, &db_users).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(upserted, duration, "upserted users");
    Ok((
        ("users".into(), SyncStats::new(upserted, duration)),
        db_users,
    ))
}

pub async fn retain_users(
    db: &PgPool,
    stats: &mut (String, SyncStats),
    db_users: &[user::User],
) -> Result<()> {
    let start = Instant::now();
    let deleted = user::retain(db, db_users).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(deleted, duration, "gc users");
    stats.1.deleted = deleted;
    stats.1.duration += duration;
    Ok(())
}

pub async fn upsert_members<I>(
    db: &PgPool,
    members: I,
) -> Result<((String, SyncStats), Vec<member::Member>)>
where
    I: IntoIterator<Item = ddb::members::Member>,
{
    let start = Instant::now();
    let db_members = members.into_iter().map(member::Member::from).collect_vec();
    let upserted = member::upsert_many(db, &db_members).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(upserted, duration, "upserted members");
    Ok((
        ("members".into(), SyncStats::new(upserted, duration)),
        db_members,
    ))
}

pub async fn retain_members(
    db: &PgPool,
    stats: &mut (String, SyncStats),
    db_members: &[member::Member],
) -> Result<()> {
    let start = Instant::now();
    let deleted = member::retain(db, db_members).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(deleted, duration, "gc members");
    stats.1.deleted = deleted;
    stats.1.duration += duration;
    Ok(())
}

pub async fn upsert_addresses(
    db: &PgPool,
    ddb_members: &[ddb::members::Member],
    ddb_addresses: &mut HashMap<u64, ddb::members::Address>,
) -> Result<((String, SyncStats), Vec<address::Address>)> {
    let start = Instant::now();
    let db_addresses = ddb_members
        .iter()
        .filter_map(|ddb_member| {
            ddb_addresses
                .remove(&ddb_member.primary.uid)
                .map(|ddb_address| ddb_address.to_db_address_for_member(ddb_member))
        })
        .collect_vec();
    let upserted = address::upsert_many(db, &db_addresses).await?;
    let deleted = address::retain(db, &db_addresses).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(deleted, upserted, duration, "upserted addresses");
    Ok((
        ("addresses".to_string(), SyncStats::new(upserted, duration)),
        db_addresses,
    ))
}

pub async fn retain_addresses(
    db: &PgPool,
    stats: &mut (String, SyncStats),
    db_addresses: &[address::Address],
) -> Result<()> {
    let start = Instant::now();
    let deleted = address::retain(db, db_addresses).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(deleted, duration, "gc addresses");
    stats.1.deleted = deleted;
    stats.1.duration += duration;
    Ok(())
}

pub async fn upsert_brns(
    db: &PgPool,
    db_brns: &[brn::Brn],
) -> Result<((String, SyncStats), Vec<brn::Brn>)> {
    let start = Instant::now();
    let upserted = brn::upsert_many(db, db_brns).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(upserted, duration, "upserted brns");
    Ok((
        ("brns".to_string(), SyncStats::new(upserted, duration)),
        db_brns.to_vec(),
    ))
}

pub async fn retain_brns(
    db: &PgPool,
    stats: &mut (String, SyncStats),
    db_brns: &[brn::Brn],
) -> Result<()> {
    let start = Instant::now();
    let deleted = brn::retain(db, db_brns).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(deleted, duration, "gc brns");
    stats.1.deleted = deleted;
    stats.1.duration += duration;
    Ok(())
}

// ========== Leadership Role Sync ==========

pub async fn upsert_roles<I>(
    db: &PgPool,
    roles: I,
) -> Result<((String, SyncStats), Vec<leadership::Role>)>
where
    I: IntoIterator<Item = ddb::leadership::Role>,
{
    let start = Instant::now();
    let db_roles = roles.into_iter().map(leadership::Role::from).collect_vec();
    let upserted = leadership::upsert_roles(db, &db_roles).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(upserted, duration, "upserted leadership roles");
    Ok((
        (
            "leadership_roles".to_string(),
            SyncStats::new(upserted, duration),
        ),
        db_roles,
    ))
}

pub async fn retain_roles(
    db: &PgPool,
    stats: &mut (String, SyncStats),
    db_roles: &[leadership::Role],
) -> Result<()> {
    let start = Instant::now();
    let deleted = leadership::retain_roles(db, db_roles).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(deleted, duration, "gc leadership roles");
    stats.1.deleted = deleted;
    stats.1.duration += duration;
    Ok(())
}

// ========== Club Leadership Sync ==========

pub async fn upsert_club_leadership<I>(
    db: &PgPool,
    leadership: I,
) -> Result<((String, SyncStats), Vec<club::Leadership>)>
where
    I: IntoIterator<Item = ddb::leadership::Leadership>,
{
    let start = Instant::now();
    let db_leadership = leadership
        .into_iter()
        .map(club::Leadership::from)
        .collect_vec();
    let upserted = club::upsert_leadership(db, &db_leadership).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(upserted, duration, "upserted club leadership");
    Ok((
        (
            "leadership_club".to_string(),
            SyncStats::new(upserted, duration),
        ),
        db_leadership,
    ))
}

pub async fn retain_club_leadership(
    db: &PgPool,
    stats: &mut (String, SyncStats),
    db_leadership: &[club::Leadership],
) -> Result<()> {
    let start = Instant::now();
    let deleted = club::retain_leadership(db, db_leadership).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(deleted, duration, "gc club leadership");
    stats.1.deleted = deleted;
    stats.1.duration += duration;
    Ok(())
}

// ========== Region Leadership Sync ==========

pub async fn upsert_region_leadership<I>(
    db: &PgPool,
    leadership: I,
) -> Result<((String, SyncStats), Vec<region::Leadership>)>
where
    I: IntoIterator<Item = ddb::leadership::Leadership>,
{
    let start = Instant::now();
    let db_leadership = leadership
        .into_iter()
        .map(region::Leadership::from)
        .collect_vec();
    let upserted = region::upsert_leadership(db, &db_leadership).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(upserted, duration, "upserted region leadership");
    Ok((
        (
            "leadership_region".to_string(),
            SyncStats::new(upserted, duration),
        ),
        db_leadership,
    ))
}

pub async fn retain_region_leadership(
    db: &PgPool,
    stats: &mut (String, SyncStats),
    db_leadership: &[region::Leadership],
) -> Result<()> {
    let start = Instant::now();
    let deleted = region::retain_leadership(db, db_leadership).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(deleted, duration, "gc region leadership");
    stats.1.deleted = deleted;
    stats.1.duration += duration;
    Ok(())
}

// ========== International Leadership Sync ==========

pub async fn upsert_international_leadership<I>(
    db: &PgPool,
    leadership: I,
) -> Result<((String, SyncStats), Vec<leadership::Leadership>)>
where
    I: IntoIterator<Item = ddb::leadership::Leadership>,
{
    let start = Instant::now();
    let db_leadership = leadership
        .into_iter()
        .map(leadership::Leadership::from)
        .collect_vec();
    let upserted = leadership::upsert_leadership(db, &db_leadership).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(upserted, duration, "upserted international leadership");
    Ok((
        (
            "leadership_international".to_string(),
            SyncStats::new(upserted, duration),
        ),
        db_leadership,
    ))
}

pub async fn retain_international_leadership(
    db: &PgPool,
    stats: &mut (String, SyncStats),
    db_leadership: &[leadership::Leadership],
) -> Result<()> {
    let start = Instant::now();
    let deleted = leadership::retain_leadership(db, db_leadership).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(deleted, duration, "gc international leadership");
    stats.1.deleted = deleted;
    stats.1.duration += duration;
    Ok(())
}

// ========== Standing Committee Sync ==========

pub async fn upsert_standing_committees<I>(
    db: &PgPool,
    committees: I,
) -> Result<(
    (String, SyncStats),
    Vec<standing_committee::StandingCommittee>,
)>
where
    I: IntoIterator<Item = ddb::standing_committees::StandingCommittee>,
{
    let start = Instant::now();
    let db_committees = committees
        .into_iter()
        .map(standing_committee::StandingCommittee::from)
        .collect_vec();
    let upserted = standing_committee::upsert_many(db, &db_committees).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(upserted, duration, "upserted standing committees");
    Ok((
        (
            "standing_committees".to_string(),
            SyncStats::new(upserted, duration),
        ),
        db_committees,
    ))
}

pub async fn retain_standing_committees(
    db: &PgPool,
    stats: &mut (String, SyncStats),
    db_committees: &[standing_committee::StandingCommittee],
) -> Result<()> {
    let start = Instant::now();
    let deleted = standing_committee::retain(db, db_committees).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(deleted, duration, "gc standing committees");
    stats.1.deleted = deleted;
    stats.1.duration += duration;
    Ok(())
}

// ========== Standing Committee Leadership Sync ==========

pub async fn upsert_standing_committee_leadership<I>(
    db: &PgPool,
    leadership: I,
) -> Result<((String, SyncStats), Vec<standing_committee::Leadership>)>
where
    I: IntoIterator<Item = ddb::leadership::Leadership>,
{
    let start = Instant::now();
    let db_leadership = leadership
        .into_iter()
        .map(standing_committee::Leadership::from)
        .collect_vec();
    let upserted = standing_committee::upsert_leadership(db, &db_leadership).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(upserted, duration, "upserted standing committee leadership");
    Ok((
        (
            "leadership_standing_committee".to_string(),
            SyncStats::new(upserted, duration),
        ),
        db_leadership,
    ))
}

pub async fn retain_standing_committee_leadership(
    db: &PgPool,
    stats: &mut (String, SyncStats),
    db_leadership: &[standing_committee::Leadership],
) -> Result<()> {
    let start = Instant::now();
    let deleted = standing_committee::retain_leadership(db, db_leadership).await?;
    let duration = start.elapsed().as_secs();
    tracing::info!(deleted, duration, "gc standing committee leadership");
    stats.1.deleted = deleted;
    stats.1.duration += duration;
    Ok(())
}

#[tracing::instrument(skip_all, name = "sync")]
pub async fn run(
    app_settings: &AppSettings,
    ddb_settings: &AciDatabaseSettings,
) -> Result<SyncStatsMap> {
    let ddb = ddb_settings.connect().await?;
    let db = app_settings.db.connect().await?;

    sqlx::migrate!()
        .run(&db)
        .await
        .context("running migrations")?;

    tracing::info!("starting sync");
    let start = Instant::now();

    let ddb_regions = ddb::regions::all(&ddb).await?;
    let ddb_clubs = ddb::clubs::all(&ddb).await?;
    let ddb_standing_committees = ddb::standing_committees::all(&ddb).await?;
    let ddb_members = ddb::members::all(&ddb).await?;
    let db_brns = ddb_members
        .iter()
        .flat_map(Into::<Vec<brn::Brn>>::into)
        .collect_vec();
    let mut ddb_addresses = ddb::members::mailing_address::for_members(&ddb, &ddb_members).await?;

    // Fetch leadership data from DDB (all historical)
    let ddb_club_leadership =
        ddb::leadership::for_all_clubs(&ddb, ddb::leadership::DateFilter::All).await?;
    let ddb_region_leadership =
        ddb::leadership::for_all_regions(&ddb, ddb::leadership::DateFilter::All).await?;
    let ddb_international_leadership =
        ddb::leadership::for_international(&ddb, ddb::leadership::DateFilter::All).await?;
    let ddb_standing_committee_leadership =
        ddb::leadership::for_all_standing_committees(&ddb, ddb::leadership::DateFilter::All)
            .await?;

    // Collect all users from members AND leadership records
    let ddb_users = ddb_members
        .iter()
        .flat_map(|ddb_member| [Some(ddb_member.primary.clone()), ddb_member.partner.clone()])
        .flatten()
        .chain(ddb_club_leadership.iter().map(|lead| lead.user.clone()))
        .chain(ddb_region_leadership.iter().map(|lead| lead.user.clone()))
        .chain(
            ddb_international_leadership
                .iter()
                .map(|lead| lead.user.clone()),
        )
        .chain(
            ddb_standing_committee_leadership
                .iter()
                .map(|lead| lead.user.clone()),
        )
        .unique_by(|user| user.uid)
        .collect_vec();

    // Collect all unique roles from all leadership types
    let ddb_roles = ddb_club_leadership
        .iter()
        .chain(ddb_region_leadership.iter())
        .chain(ddb_international_leadership.iter())
        .chain(ddb_standing_committee_leadership.iter())
        .map(|lead| lead.role.clone())
        .unique_by(|role| role.uid)
        .collect_vec();

    // Upsert roles first (no dependencies)
    let (mut role_stats, db_roles) = upsert_roles(&db, ddb_roles).await?;

    let (mut region_stats, db_regions) = upsert_regions(&db, ddb_regions).await?;
    let (mut club_stats, db_clubs) = upsert_clubs(&db, ddb_clubs).await?;
    let (mut standing_committee_stats, db_standing_committees) =
        upsert_standing_committees(&db, ddb_standing_committees).await?;
    let (mut user_stats, db_users) = upsert_users(&db, ddb_users).await?;
    let (mut address_stats, db_addresses) =
        upsert_addresses(&db, &ddb_members, &mut ddb_addresses).await?;
    let (mut member_stats, db_members) = upsert_members(&db, ddb_members).await?;
    let (mut brn_stats, db_brns) = upsert_brns(&db, &db_brns).await?;

    // Upsert leadership (depends on roles, clubs, regions, standing committees, users)
    // Filter to only leadership records referencing existing entities
    let club_uids: std::collections::HashSet<i64> = db_clubs.iter().map(|c| c.uid).collect();
    let region_uids: std::collections::HashSet<i64> = db_regions.iter().map(|r| r.uid).collect();
    let standing_committee_uids: std::collections::HashSet<i64> =
        db_standing_committees.iter().map(|sc| sc.uid).collect();

    let (mut club_leadership_stats, db_club_leadership) = upsert_club_leadership(
        &db,
        ddb_club_leadership.into_iter().filter(|l| {
            let exists = club_uids.contains(&(l.entity_uid as i64));
            if !exists {
                tracing::warn!(
                    club_uid = l.entity_uid,
                    "leadership references non-existent club"
                );
            }
            exists
        }),
    )
    .await?;
    let (mut region_leadership_stats, db_region_leadership) = upsert_region_leadership(
        &db,
        ddb_region_leadership.into_iter().filter(|l| {
            let exists = region_uids.contains(&(l.entity_uid as i64));
            if !exists {
                tracing::warn!(
                    region_uid = l.entity_uid,
                    "leadership references non-existent region"
                );
            }
            exists
        }),
    )
    .await?;
    let (mut international_leadership_stats, db_international_leadership) =
        upsert_international_leadership(&db, ddb_international_leadership).await?;
    let (mut standing_committee_leadership_stats, db_standing_committee_leadership) =
        upsert_standing_committee_leadership(
            &db,
            ddb_standing_committee_leadership.into_iter().filter(|l| {
                let exists = standing_committee_uids.contains(&(l.entity_uid as i64));
                if !exists {
                    tracing::warn!(
                        standing_committee_uid = l.entity_uid,
                        "leadership references non-existent standing committee"
                    );
                }
                exists
            }),
        )
        .await?;

    retain_clubs(&db, &mut club_stats, &db_clubs).await?;
    retain_regions(&db, &mut region_stats, &db_regions).await?;
    retain_standing_committees(&db, &mut standing_committee_stats, &db_standing_committees).await?;
    retain_brns(&db, &mut brn_stats, &db_brns).await?;
    retain_members(&db, &mut member_stats, &db_members).await?;

    // Retain leadership before retaining users/roles
    retain_club_leadership(&db, &mut club_leadership_stats, &db_club_leadership).await?;
    retain_region_leadership(&db, &mut region_leadership_stats, &db_region_leadership).await?;
    retain_international_leadership(
        &db,
        &mut international_leadership_stats,
        &db_international_leadership,
    )
    .await?;
    retain_standing_committee_leadership(
        &db,
        &mut standing_committee_leadership_stats,
        &db_standing_committee_leadership,
    )
    .await?;

    retain_addresses(&db, &mut address_stats, &db_addresses).await?;
    retain_users(&db, &mut user_stats, &db_users).await?;
    retain_roles(&db, &mut role_stats, &db_roles).await?;

    let duration = start.elapsed().as_secs();
    tracing::info!(duration, "sync complete");

    let stats: SyncStatsMap = [
        brn_stats,
        address_stats,
        region_stats,
        club_stats,
        standing_committee_stats,
        user_stats,
        member_stats,
        role_stats,
        club_leadership_stats,
        region_leadership_stats,
        international_leadership_stats,
        standing_committee_leadership_stats,
    ]
    .into_iter()
    .collect();
    Ok(stats)
}
