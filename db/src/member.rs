use crate::{
    DB_INSERT_CHUNK_SIZE, Error, Result, club, retain_with_keys,
    user::{self, id_for_email},
};
use futures::{StreamExt, TryStreamExt, stream};
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::{collections::HashMap, fmt};

pub async fn all(pool: &PgPool) -> Result<Vec<Member>> {
    let all = fetch_members_query()
        .build_query_as::<Member>()
        .fetch_all(pool)
        .await?;
    Ok(dedupe_members(all))
}

pub async fn by_club(pool: &PgPool, uid: i64) -> Result<Vec<Member>> {
    let members = fetch_members_query()
        .push("WHERE local_club = ")
        .push_bind(uid)
        .build_query_as::<Member>()
        .fetch_all(pool)
        .await?;
    Ok(dedupe_members(members))
}

pub async fn by_region(pool: &PgPool, uid: i64) -> Result<Vec<Member>> {
    let all = fetch_members_query()
        .push("WHERE region.uid = ")
        .push_bind(uid)
        .build_query_as::<Member>()
        .fetch_all(pool)
        .await?;

    Ok(dedupe_members(all))
}

/// Remove affiliates in the given members list that are also regualr members
pub fn dedupe_members(members: Vec<Member>) -> Vec<Member> {
    let (regulars, mut affiliates): (Vec<Member>, Vec<Member>) = members
        .into_iter()
        .partition(|member| member.member_type != MemberType::Affiliate);
    let mut member_map: HashMap<String, Member> = regulars
        .into_iter()
        .map(|member| (member.primary.email.clone(), member))
        .collect();

    affiliates.retain(|affiliate| !member_map.contains_key(&affiliate.primary.email));
    affiliates.into_iter().for_each(|affiliate| {
        member_map.insert(affiliate.primary.email.clone(), affiliate);
    });
    member_map.into_values().collect()
}

pub async fn by_uid(pool: &PgPool, uid: i64) -> Result<Option<Member>> {
    let member = fetch_members_query()
        .push("WHERE primary_user = ")
        .push_bind(uid)
        .build_query_as::<Member>()
        .fetch_optional(pool)
        .await?;

    Ok(member)
}

pub async fn by_email(pool: &PgPool, email: &str) -> Result<Option<Member>> {
    let member = fetch_members_query()
        .push("WHERE email = ")
        .push_bind(email)
        .build_query_as::<Member>()
        .fetch_optional(pool)
        .await?;

    Ok(member)
}

const FETCH_MEMBERS_QUERY: &str = r#"
    SELECT
        uid,
        email,
        first_name,
        last_name,

        partner.uid as partner_uid,
        partner.email as partner_email,
        partner.first_name as partner_first_name,
        partner.last_name as partner_last_name,

        member.member_class,
        member.member_type,
        member.member_status,
        member.expiration_date,
        member.join_date,

        club.uid as club_uid,
        club.name as club_name,
        club.number as club_number,

        region.number as club_region

    FROM
        members member
        LEFT JOIN users "user" ON "user".id = member.primary_user
        LEFT JOIN users partner ON partner.id = member.partner_user
        LEFT JOIN clubs club ON club.number = member.local_club
        LEFT JOIN regions region ON region.uid = club.region
"#;

fn fetch_members_query<'builder>() -> sqlx::QueryBuilder<'builder, Postgres> {
    sqlx::QueryBuilder::new(FETCH_MEMBERS_QUERY)
}

pub async fn upsert_many(pool: &PgPool, members: &[Member]) -> Result<u64> {
    if members.is_empty() {
        return Ok(0);
    }

    let affected: Vec<u64> = stream::iter(members)
        .chunks(DB_INSERT_CHUNK_SIZE)
        .map(Ok)
        .and_then(|chunk| async move {
            let result = QueryBuilder::new(
                r#"INSERT INTO members(
                    primary_user,
                    partner_user,
                    member_class,
                    member_type,
                    member_status,
                    expiration_date,
                    join_date,
                    local_club
                ) "#,
            )
            .push_values(&chunk, |mut b, member| {
                b.push_bind(&member.primary.id)
                    .push_bind(member.partner.as_ref().map(|user| &user.id))
                    .push_bind(&member.member_class)
                    .push_bind(&member.member_type)
                    .push_bind(&member.member_status)
                    .push_bind(member.expiration_date)
                    .push_bind(member.join_date)
                    .push_bind(member.local_club.number);
            })
            .push(
                r#"ON CONFLICT(primary_user) DO UPDATE SET
                partner_user = excluded.partner_user,
                member_class = excluded.member_class,
                member_type = excluded.member_type,
                member_status = excluded.member_status,
                expiration_date = excluded.expiration_date,
                join_date = excluded.join_date,
                local_club = excluded.local_club
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

pub async fn retain(pool: &PgPool, members: &[Member]) -> Result<u64> {
    retain_with_keys(pool, "members", "primary_user", members, |member| {
        member.primary.id.as_str()
    })
    .await
}
pub mod mailing_address {
    use super::*;

    pub async fn by_uid(pool: &PgPool, uid: i64) -> Result<Option<Address>> {
        let member = fetch_mailing_address_query()
            .push("WHERE user = ")
            .push_bind(uid)
            .build_query_as::<Address>()
            .fetch_optional(pool)
            .await?;
        Ok(member)
    }

    pub async fn by_uids<I: IntoIterator<Item = i64>>(
        pool: &PgPool,
        uids: I,
    ) -> Result<HashMap<i64, Address>> {
        let mut builder = fetch_mailing_address_query();
        let mut seperated = builder.push("AND user IN (").separated(", ");
        for value in uids {
            seperated.push_bind(value);
        }
        seperated.push_unseparated(") ");
        let members: HashMap<i64, Address> = builder
            .build_query_as::<Address>()
            .fetch_all(pool)
            .await?
            .into_iter()
            .filter_map(|address| address.user_id.map(|user_id| (user_id, address)))
            .collect();
        Ok(members)
    }

    /// Get addresses for given members primary user ids
    pub async fn for_members(
        pool: &PgPool,
        members: impl IntoIterator<Item = &Member>,
    ) -> Result<HashMap<i64, Address>> {
        by_uids(pool, members.into_iter().map(|member| member.primary.uid)).await
    }

    pub async fn all(pool: &PgPool) -> Result<Vec<Address>> {
        let members = fetch_mailing_address_query()
            .build_query_as::<Address>()
            .fetch_all(pool)
            .await?;
        Ok(members)
    }

    fn fetch_mailing_address_query<'builder>() -> sqlx::QueryBuilder<'builder, Postgres> {
        sqlx::QueryBuilder::new(
            r#"
            SELECT
                user_id,
                state,
                country
            FROM addresses
            "#,
        )
    }
}

#[derive(Debug, serde::Serialize, Default, PartialEq, Eq, sqlx::Type)]
#[serde(rename_all = "lowercase")]
#[sqlx(type_name = "member_class", rename_all = "lowercase")]
pub enum MemberClass {
    #[default]
    Regular,
    Lifetime,
    Complimentary,
}

impl fmt::Display for MemberClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Regular => f.write_str("regular"),
            Self::Lifetime => f.write_str("lifetime"),
            Self::Complimentary => f.write_str("complimentary"),
        }
    }
}

impl TryFrom<String> for MemberClass {
    type Error = sqlx::Error;
    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "regular" => Ok(Self::Regular),
            "lifetime" => Ok(Self::Lifetime),
            "complimentary" => Ok(Self::Complimentary),
            other => Err(sqlx::Error::decode(format!(
                "unexpected member class {other}"
            ))),
        }
    }
}

#[derive(Debug, serde::Serialize, Default, PartialEq, Eq, sqlx::Type)]
#[serde(rename_all = "lowercase")]
#[sqlx(type_name = "member_status", rename_all = "lowercase")]
pub enum MemberStatus {
    #[default]
    Current,
    Lapsed,
}

impl fmt::Display for MemberStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Current => f.write_str("current"),
            Self::Lapsed => f.write_str("lapsed"),
        }
    }
}

impl TryFrom<String> for MemberStatus {
    type Error = sqlx::Error;
    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "current" => Ok(Self::Current),
            "lapsed" => Ok(Self::Lapsed),
            other => Err(sqlx::Error::decode(format!(
                "unexpected member status {other}"
            ))),
        }
    }
}

#[derive(Debug, serde::Serialize, Default, PartialEq, Eq, sqlx::Type)]
#[serde(rename_all = "lowercase")]
#[sqlx(type_name = "member_type", rename_all = "lowercase")]
pub enum MemberType {
    #[default]
    Regular,
    Affiliate,
}

impl fmt::Display for MemberType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Regular => f.write_str("regular"),
            Self::Affiliate => f.write_str("affiliate"),
        }
    }
}

impl TryFrom<String> for MemberType {
    type Error = sqlx::Error;
    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "field_home_club" | "regular" => Ok(Self::Regular),
            "field_memberships" | "affiliate" => Ok(Self::Affiliate),
            other => Err(sqlx::Error::decode(format!(
                "unexpected member type {other}",
            ))),
        }
    }
}

#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub struct Member {
    #[sqlx(default, try_from = "String")]
    pub member_class: MemberClass,
    #[sqlx(default, try_from = "String")]
    pub member_type: MemberType,
    #[sqlx(default, try_from = "String")]
    pub member_status: MemberStatus,
    #[sqlx(flatten)]
    pub primary: user::User,
    #[sqlx(flatten, try_from = "PartnerUser")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partner: Option<user::User>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration_date: Option<chrono::NaiveDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub join_date: Option<chrono::NaiveDate>,
    #[sqlx(flatten, try_from = "LocalClub")]
    pub local_club: club::Club,
}

#[derive(Debug, sqlx::FromRow, serde::Serialize, Clone)]
pub struct Address {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<i64>,
    pub state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
}

#[derive(Debug, sqlx::FromRow, serde::Serialize)]
struct PartnerUser {
    partner_uid: Option<i64>,
    partner_email: Option<String>,
    partner_first_name: Option<String>,
    partner_last_name: Option<String>,
}

impl From<PartnerUser> for Option<user::User> {
    fn from(value: PartnerUser) -> Option<user::User> {
        if let Some(uid) = value.partner_uid {
            let partner_email = value.partner_email.unwrap();
            Some(user::User {
                uid,
                id: id_for_email(&partner_email),
                email: partner_email,
                first_name: value.partner_first_name,
                last_name: value.partner_last_name,
            })
        } else {
            None
        }
    }
}

#[derive(Debug, sqlx::FromRow, serde::Serialize)]
struct LocalClub {
    club_name: Option<String>,
    club_uid: Option<i64>,
    club_number: Option<i64>,
    club_region: Option<i64>,
    club_region_uid: Option<i64>,
}

impl From<LocalClub> for club::Club {
    fn from(value: LocalClub) -> Self {
        Self {
            uid: value.club_uid.unwrap_or_default(),
            number: value.club_number,
            name: value.club_name.unwrap_or_default(),
            region: value.club_region,
        }
    }
}
