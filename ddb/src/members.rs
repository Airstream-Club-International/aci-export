use crate::{Result, clubs, clubs::Club, users::User};
use chrono::NaiveDate;
use itertools::Itertools;
use sqlx::{MySql, MySqlPool};
use std::{collections::HashMap, fmt};

pub async fn all(pool: &MySqlPool) -> Result<Vec<Member>> {
    let all = fetch_members_query()
        .push(" AND paragraphs_item_field_data.parent_field_name = 'field_home_club'")
        .build_query_as::<Member>()
        .fetch_all(pool)
        .await?;
    Ok(dedupe_members(all))
}

pub async fn by_club(pool: &MySqlPool, uid: u64) -> Result<Vec<Member>> {
    let all = fetch_club_members_query()
        .build_query_as::<Member>()
        .bind(Some(uid))
        .bind(Some(uid))
        .bind(None::<u64>)
        .fetch_all(pool)
        .await?;

    Ok(dedupe_members(all))
}

pub async fn by_region(pool: &MySqlPool, uid: u64) -> Result<Vec<Member>> {
    let all = fetch_club_members_query()
        .build_query_as::<Member>()
        .bind(None::<u64>)
        .bind(None::<u64>)
        .bind(Some(uid))
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

pub async fn by_uid(pool: &MySqlPool, uid: u64) -> Result<Option<Member>> {
    let member = fetch_members_query()
        .push("AND paragraphs_item_field_data.parent_field_name = 'field_home_club'")
        .push("AND users_field_data.uid = ")
        .push_bind(uid)
        .build_query_as::<Member>()
        .fetch_optional(pool)
        .await?;

    Ok(member)
}

pub async fn by_email(pool: &MySqlPool, email: &str) -> Result<Option<Member>> {
    let member = fetch_members_query()
        .push("AND users_field_data.mail = ")
        .push_bind(email)
        .build_query_as::<Member>()
        .fetch_optional(pool)
        .await?;

    Ok(member)
}

// ========== Membership History (for portal sync) ==========

/// Single membership period (one paragraph from DDB)
/// Used for syncing full membership history to the portal
#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub struct MembershipPeriod {
    /// Drupal paragraph ID (unique identifier for this membership record)
    pub paragraph_id: u64,
    /// User's Drupal UID
    pub user_uid: u64,
    /// Partner's Drupal UID (if any)
    pub partner_uid: Option<u64>,
    /// Club's Drupal NID
    pub club_uid: Option<u64>,
    #[sqlx(default, try_from = "String")]
    pub member_class: MemberClass,
    #[sqlx(default, try_from = "String")]
    pub member_type: MemberType,
    pub join_date: NaiveDate,
    pub leave_date: Option<NaiveDate>,
}

/// Fetch all membership periods (full history, no date filtering)
/// Unlike `all()` which returns current members only, this returns every
/// membership paragraph for portal history sync
pub async fn history_all(pool: &MySqlPool) -> Result<Vec<MembershipPeriod>> {
    sqlx::query_as::<_, MembershipPeriod>(FETCH_MEMBERSHIP_HISTORY_QUERY)
        .fetch_all(pool)
        .await
        .map_err(Into::into)
}

// ========== International Membership History (for portal sync) ==========

/// International membership period (ssp_international_membership paragraph)
/// Stored in portal with club_id = NULL and member_type = NULL
#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub struct InternationalMembershipPeriod {
    /// Drupal paragraph ID (unique identifier for this membership record)
    pub paragraph_id: u64,
    /// User's Drupal UID (from parent_id, can be NULL for orphaned paragraphs)
    pub user_uid: Option<u64>,
    /// Partner's Drupal UID (if any)
    pub partner_uid: Option<u64>,
    #[sqlx(default, try_from = "String")]
    pub member_class: MemberClass,
    /// Join date (can be NULL for malformed records)
    pub join_date: Option<NaiveDate>,
    pub leave_date: Option<NaiveDate>,
}

/// Fetch all international membership periods (full history, no date filtering)
pub async fn international_history_all(
    pool: &MySqlPool,
) -> Result<Vec<InternationalMembershipPeriod>> {
    sqlx::query_as::<_, InternationalMembershipPeriod>(FETCH_INTERNATIONAL_MEMBERSHIP_HISTORY_QUERY)
        .fetch_all(pool)
        .await
        .map_err(Into::into)
}

/// Query for international membership history
/// Paragraphs with type = 'ssp_international_membership'
/// Dedupes by (user_uid, join_date) keeping lowest paragraph_id
const FETCH_INTERNATIONAL_MEMBERSHIP_HISTORY_QUERY: &str = r#"
WITH all_international AS (
    SELECT
        p.id AS paragraph_id,
        CAST(p.parent_id AS UNSIGNED) AS user_uid,
        CAST(CASE WHEN pu.uid IS NOT NULL THEN md.partner_user_id END AS UNSIGNED) AS partner_uid,
        COALESCE(ttd.name, 'Regular') AS member_class,
        DATE(fjd.field_join_date_value) AS join_date,
        DATE(fld.field_leave_date_value) AS leave_date
    FROM paragraphs_item_field_data p
    INNER JOIN users_field_data u
        ON u.uid = p.parent_id
    INNER JOIN paragraph__field_join_date fjd
        ON fjd.entity_id = p.id
        AND fjd.deleted = '0'
    LEFT JOIN paragraph__field_leave_date fld
        ON fld.entity_id = p.id
        AND fld.deleted = '0'
    LEFT JOIN paragraph__field_membership_class mc
        ON mc.entity_id = p.id
        AND mc.deleted = '0'
    LEFT JOIN taxonomy_term_field_data ttd
        ON ttd.tid = mc.field_membership_class_target_id
    LEFT JOIN z_member_search_main md
        ON md.user_id = p.parent_id
    LEFT JOIN users_field_data pu
        ON pu.uid = md.partner_user_id
    WHERE p.status = '1'
        AND p.type = 'ssp_international_membership'
        AND fjd.field_join_date_value IS NOT NULL
),
dedupe_pick AS (
    SELECT
        user_uid, join_date,
        MIN(paragraph_id) AS paragraph_id
    FROM all_international
    GROUP BY user_uid, join_date
)
SELECT
    ai.paragraph_id,
    ai.user_uid,
    ai.partner_uid,
    ai.member_class,
    ai.join_date,
    ai.leave_date
FROM all_international ai
INNER JOIN dedupe_pick dp
    ON ai.paragraph_id = dp.paragraph_id
"#;

/// Query for full membership history - returns one row per membership paragraph
/// No date filtering (includes lapsed/expired memberships)
/// Dedupes by (user, club, join_date, member_type) keeping lowest paragraph_id
const FETCH_MEMBERSHIP_HISTORY_QUERY: &str = r#"
WITH all_memberships AS (
    SELECT
        p.id AS paragraph_id,
        CAST(p.parent_id AS UNSIGNED) AS user_uid,
        CAST(CASE WHEN pu.uid IS NOT NULL THEN md.partner_user_id END AS UNSIGNED) AS partner_uid,
        pc.field_club_target_id AS club_uid,
        COALESCE(ttd.name, 'Regular') AS member_class,
        CASE
            WHEN uhc.entity_id IS NOT NULL THEN 'regular'
            WHEN uic.entity_id IS NOT NULL THEN 'intraclub'
            WHEN uac.entity_id IS NOT NULL THEN 'affiliate'
            ELSE 'regular'
        END AS member_type,
        DATE(fjd.field_join_date_value) AS join_date,
        DATE(fld.field_leave_date_value) AS leave_date
    FROM paragraphs_item_field_data p
    INNER JOIN users_field_data u
        ON u.uid = p.parent_id
    INNER JOIN paragraph__field_club pc
        ON pc.entity_id = p.id
        AND pc.deleted = '0'
    INNER JOIN paragraph__field_join_date fjd
        ON fjd.entity_id = p.id
        AND fjd.deleted = '0'
    LEFT JOIN paragraph__field_leave_date fld
        ON fld.entity_id = p.id
        AND fld.deleted = '0'
    LEFT JOIN paragraph__field_membership_class mc
        ON mc.entity_id = p.id
        AND mc.deleted = '0'
    LEFT JOIN taxonomy_term_field_data ttd
        ON ttd.tid = mc.field_membership_class_target_id
    LEFT JOIN user__field_home_club uhc
        ON uhc.field_home_club_target_id = p.id
        AND uhc.deleted = '0'
    LEFT JOIN user__field_memberships uac
        ON uac.field_memberships_target_id = p.id
        AND uac.deleted = '0'
    LEFT JOIN user__field_intraclub_memberships uic
        ON uic.field_intraclub_memberships_target_id = p.id
        AND uic.deleted = '0'
    LEFT JOIN z_member_search_main md
        ON md.user_id = p.parent_id
    LEFT JOIN users_field_data pu
        ON pu.uid = md.partner_user_id
    WHERE p.status = '1'
        AND p.type = 'membership'
        AND fjd.field_join_date_value IS NOT NULL
        AND (uhc.entity_id IS NOT NULL OR uac.entity_id IS NOT NULL OR uic.entity_id IS NOT NULL)
),
dedupe_pick AS (
    SELECT
        user_uid, club_uid, join_date, member_type,
        MIN(paragraph_id) AS paragraph_id
    FROM all_memberships
    GROUP BY user_uid, club_uid, join_date, member_type
)
SELECT
    am.paragraph_id,
    am.user_uid,
    am.partner_uid,
    am.club_uid,
    am.member_class,
    am.member_type,
    am.join_date,
    am.leave_date
FROM all_memberships am
INNER JOIN dedupe_pick dp
    ON am.paragraph_id = dp.paragraph_id
"#;

const FETCH_ALL_MEMBERS_QUERY: &str = r#"
    SELECT
    	users_field_data.uid AS uid,
    	alldata.email AS email,
    	alldata.first_name AS first_name,
    	alldata.last_name AS last_name,
        CAST(alldata.birthdate AS DATE) AS birthday,
        DATE(FROM_UNIXTIME(users_field_data.login)) AS last_login,
        NULL AS pass,
        NULL AS gender,
        NULL AS race_tid,
        NULL AS communication_preference,
        NULL AS blue_beret_mail,
        NULL AS publish_info,
        NULL AS special_needs,
        NULL AS ada_parking,
        NULL AS member_notes,
        NULL AS military_status,
        NULL AS first_responder_status,
        CASE WHEN users_field_data.status = 1 THEN TRUE ELSE FALSE END AS active,

       	CAST(alldata.partner_user_id AS UNSIGNED) AS partner_uid,
    	alldata.partner_email AS partner_email,
    	alldata.partner_first_name AS partner_first_name,
    	alldata.partner_last_name AS partner_last_name,
        CAST(alldata.partner_birthdate AS DATE) AS partner_birthday,
        DATE(FROM_UNIXTIME(users_field_data.login)) AS partner_last_login,

    	IF(memclassterm.name IS NULL, "Regular", memclassterm.name) AS member_class,
    	paragraphs_item_field_data.parent_field_name AS member_type,
        alldata.personal_status_id as member_status, 

        CAST(node__field_club_number.field_club_number_value AS SIGNED) AS club_number, 
    	node_field_data_paragraph__field_club.nid AS club_uid,
    	node_field_data_paragraph__field_club.title AS club_name,
        node_field__data_paragraph_field_club__field_region_number.field_region_number_value as club_region,
        node_field_data_node__field_region.nid AS club_region_uid,
        brns.brns_values AS brns,

        CAST(alldata.membership_expire AS DATE) as expiration_date,
        CAST(alldata.membership_join_year AS DATE) as join_date

    FROM
    	paragraphs_item_field_data
    	LEFT JOIN paragraph__field_club ON paragraphs_item_field_data.id = paragraph__field_club.entity_id
    		AND paragraph__field_club.deleted = '0'
    		AND(paragraph__field_club.langcode = paragraphs_item_field_data.langcode
    			OR paragraph__field_club.bundle = 'membership')
    		INNER JOIN node_field_data node_field_data_paragraph__field_club ON paragraph__field_club.field_club_target_id = node_field_data_paragraph__field_club.nid
    		LEFT JOIN node__field_region node_field_data_paragraph__field_club__node__field_region ON node_field_data_paragraph__field_club.nid = node_field_data_paragraph__field_club__node__field_region.entity_id
    			AND node_field_data_paragraph__field_club__node__field_region.deleted = '0'
    	LEFT JOIN node__field_club_number ON 
            node__field_club_number.entity_id = node_field_data_paragraph__field_club.nid
    	LEFT JOIN node_field_data node_field_data_node__field_region ON 
            node_field_data_paragraph__field_club__node__field_region.field_region_target_id = node_field_data_node__field_region.nid
		LEFT JOIN node__field_region_number node_field__data_paragraph_field_club__field_region_number ON
			node_field_data_paragraph__field_club__node__field_region.field_region_target_id = node_field__data_paragraph_field_club__field_region_number.entity_id
    	LEFT JOIN paragraph__field_leave_date paragraph__field_leave_date ON paragraphs_item_field_data.id = paragraph__field_leave_date.entity_id
    		AND paragraph__field_leave_date.deleted = '0'
    		AND(paragraph__field_leave_date.langcode = paragraphs_item_field_data.langcode
    			OR paragraph__field_leave_date.bundle = 'membership')
    	LEFT JOIN paragraph__field_join_date paragraph__field_join_date ON paragraphs_item_field_data.id = paragraph__field_join_date.entity_id
    		AND paragraph__field_join_date.deleted = '0'
    		AND(paragraph__field_join_date.langcode = paragraphs_item_field_data.langcode
    			OR paragraph__field_join_date.bundle = 'membership')
    		INNER JOIN users_field_data users_field_data ON paragraphs_item_field_data.parent_id = users_field_data.uid
    		LEFT JOIN user__field_primary_member user_is_primary_member ON users_field_data.uid = user_is_primary_member.entity_id
    		INNER JOIN z_member_search_main alldata ON users_field_data.uid = alldata.user_id
    		INNER JOIN ssp_membership_international_membership rightmembership ON users_field_data.uid = rightmembership.user_id
    		LEFT JOIN paragraph__field_membership_class memclass ON rightmembership.paragraph_id = memclass.entity_id
    		LEFT JOIN taxonomy_term_field_data memclassterm ON memclass.field_membership_class_target_id = memclassterm.tid
    		LEFT JOIN v_brns brns ON users_field_data.uid = brns.user_id
    		LEFT JOIN user__field_home_club userhomeclub ON paragraphs_item_field_data.id = userhomeclub.field_home_club_target_id
    			AND userhomeclub.deleted = '0'
    	LEFT JOIN user__field_memberships useraffclub ON paragraphs_item_field_data.id = useraffclub.field_memberships_target_id
    		AND useraffclub.deleted = '0'
    	LEFT JOIN user__field_intraclub_memberships userintraclub ON paragraphs_item_field_data.id = userintraclub.field_intraclub_memberships_target_id
    		AND userintraclub.deleted = '0'
    WHERE (((paragraphs_item_field_data.status = '1')
    		AND(paragraphs_item_field_data.type IN('membership'))
    		AND(paragraph__field_leave_date.field_leave_date_value IS NOT NULL)
    		AND(paragraph__field_join_date.field_join_date_value IS NOT NULL))
    	AND((alldata.personal_status_id IN('947', '951', '1099'))
    	AND((CAST(paragraph__field_leave_date.field_leave_date_value AS DATE) >= DATE_SUB(NOW(), INTERVAL 1 YEAR)))
    AND((CAST(paragraph__field_join_date.field_join_date_value AS DATE) <= NOW()))
    AND(((useraffclub.entity_id IS NOT NULL
    	OR userhomeclub.entity_id IS NOT NULL
    	OR userintraclub.entity_id IS NOT NULL)))))
    AND(user_is_primary_member.field_primary_member_target_id IS NULL)
"#;

fn fetch_members_query<'builder>() -> sqlx::QueryBuilder<'builder, MySql> {
    sqlx::QueryBuilder::new(FETCH_ALL_MEMBERS_QUERY)
}

/* Parameters (MySQL/MariaDB positional):
   1) ?  -> club_nid (nullable)
   2) ?  -> echo of club_nid for the WHERE ? IS NOT NULL guard
   3) ?  -> region_nid (nullable)
*/

const FETCH_CLUB_MEMBERS_QUERY: &str = r#"
WITH club_scope AS (
  /* branch 1: single club */
  SELECT club_nid
  FROM (SELECT ? AS club_nid) AS one
  WHERE ? IS NOT NULL
  UNION ALL
  /* branch 2: all clubs in a region */
  SELECT nr.entity_id AS club_nid
  FROM node__field_region nr
  WHERE nr.deleted = '0'
    AND nr.field_region_target_id = ?
),

all_club_memberships AS (
  /* All memberships (including historical) for users in scoped clubs */
  SELECT
    p.parent_id AS uid,
    pc.field_club_target_id AS club_nid,
    fjd.field_join_date_value AS join_dt_raw
  FROM paragraphs_item_field_data p
  JOIN paragraph__field_club pc
    ON pc.entity_id = p.id
   AND pc.deleted = '0'
  LEFT JOIN paragraph__field_join_date fjd
    ON fjd.entity_id = p.id AND fjd.deleted = '0'
  WHERE p.status = '1'
    AND p.type = 'membership'
    AND pc.field_club_target_id IN (SELECT club_nid FROM club_scope)
    AND fjd.field_join_date_value IS NOT NULL
    AND DATE(fjd.field_join_date_value) <= CURRENT_DATE
),

earliest_join AS (
  /* Earliest join date across ALL memberships (including historical) per user */
  SELECT uid, MIN(DATE(join_dt_raw)) AS earliest_join_date
  FROM all_club_memberships
  GROUP BY uid
),

acp AS (
  /* Currently active memberships only */
  SELECT
    p.parent_id AS uid,
    p.id        AS paragraph_id,
    pc.field_club_target_id AS club_nid,
    fjd.field_join_date_value  AS join_dt_raw,
    fld.field_leave_date_value AS leave_dt_raw
  FROM paragraphs_item_field_data p
  JOIN paragraph__field_club pc
    ON pc.entity_id = p.id
   AND pc.deleted = '0'
  LEFT JOIN paragraph__field_join_date fjd
    ON fjd.entity_id = p.id AND fjd.deleted = '0'
  LEFT JOIN paragraph__field_leave_date fld
    ON fld.entity_id = p.id AND fld.deleted = '0'
  WHERE p.status = '1'
    AND p.type   = 'membership'
    AND pc.field_club_target_id IN (SELECT club_nid FROM club_scope)
    AND fjd.field_join_date_value IS NOT NULL
    AND DATE(fjd.field_join_date_value) <= CURRENT_DATE
    AND (fld.field_leave_date_value IS NULL OR DATE(fld.field_leave_date_value) >= CURRENT_DATE)
),

flags AS (
  SELECT
    a.uid,
    GREATEST(MAX(uhc.entity_id IS NOT NULL), MAX(uic.entity_id IS NOT NULL)) AS member_flag,
    MAX(uac.entity_id IS NOT NULL)                                          AS affiliate_flag,
    MAX(DATE(a.leave_dt_raw))                                               AS latest_expiration_date
  FROM acp a
  LEFT JOIN user__field_home_club uhc
    ON uhc.entity_id = a.uid
   AND uhc.field_home_club_target_id = a.paragraph_id
   AND uhc.deleted = '0'
  LEFT JOIN user__field_memberships uac
    ON uac.entity_id = a.uid
   AND uac.field_memberships_target_id = a.paragraph_id
   AND uac.deleted = '0'
  LEFT JOIN user__field_intraclub_memberships uic
    ON uic.entity_id = a.uid
   AND uic.field_intraclub_memberships_target_id = a.paragraph_id
   AND uic.deleted = '0'
  GROUP BY a.uid
),

active_pick AS (
  SELECT a1.uid, a1.paragraph_id, a1.club_nid
  FROM acp a1
  JOIN (
    SELECT uid, MAX(DATE(join_dt_raw)) AS max_join
    FROM acp
    GROUP BY uid
  ) pick
    ON pick.uid = a1.uid AND pick.max_join = DATE(a1.join_dt_raw)
)

SELECT
  /* ===================== PRIMARY FIELDS ===================== */
  u.uid                                        AS uid,
  DATE(FROM_UNIXTIME(u.login))                 AS last_login,
  md.first_name                                AS first_name,
  md.last_name                                 AS last_name,
  md.email                                     AS email,
  CAST(md.birthdate AS DATE)                   AS birthday,
  NULL                                         AS pass,
  NULL                                         AS gender,
  NULL                                         AS race_tid,
  NULL                                         AS communication_preference,
  NULL                                         AS blue_beret_mail,
  NULL                                         AS publish_info,
  NULL                                         AS special_needs,
  NULL                                         AS ada_parking,
  NULL                                         AS member_notes,
  NULL                                         AS military_status,
  NULL                                         AS first_responder_status,
  CASE WHEN u.status = 1 THEN TRUE ELSE FALSE END AS active,

  /* ===================== MEMBER INFORMATION FIELDS ===================== */
  CASE
    WHEN flags.member_flag = 1 THEN 'regular'
    WHEN flags.affiliate_flag = 1 THEN 'affiliate'
    ELSE NULL
  END                                          AS member_type,
  COALESCE(ttd.name, 'Regular')                AS member_class,
  md.personal_status_id                        AS member_status,
  earliest_join.earliest_join_date             AS join_date,
  flags.latest_expiration_date                 AS expiration_date,

  /* ===================== CLUB FIELDS ===================== */
  CAST(cnum.field_club_number_value AS SIGNED) AS club_number,
  nclub.nid                                    AS club_uid,
  nclub.title                                  AS club_name,
  rnum.field_region_number_value               AS club_region,
  region_node.nid                              AS club_region_uid,
  brns.brns_values                             AS brns,

  /* ===================== PARTNER FIELDS ===================== */
  CAST(md.partner_user_id AS UNSIGNED)         AS partner_uid,
  DATE(FROM_UNIXTIME(pu.login))                AS partner_last_login,
  md.partner_first_name                        AS partner_first_name,
  md.partner_last_name                         AS partner_last_name,
  md.partner_email                             AS partner_email,
  CAST(md.partner_birthdate AS DATE)           AS partner_birthday

FROM flags
JOIN earliest_join
  ON earliest_join.uid = flags.uid
JOIN users_field_data u
  ON u.uid = flags.uid
JOIN z_member_search_main md
  ON md.user_id = u.uid

LEFT JOIN users_field_data pu
  ON pu.uid = md.partner_user_id  /* get partner’s last_login */

LEFT JOIN user__field_primary_member pm_self
  ON pm_self.entity_id = u.uid
 AND pm_self.field_primary_member_target_id IS NOT NULL

LEFT JOIN active_pick
  ON active_pick.uid = u.uid

LEFT JOIN paragraph__field_membership_class mc
  ON mc.entity_id = active_pick.paragraph_id
 AND mc.deleted = '0'
LEFT JOIN taxonomy_term_field_data ttd
  ON ttd.tid = mc.field_membership_class_target_id

LEFT JOIN node_field_data nclub
  ON nclub.nid = active_pick.club_nid
LEFT JOIN node__field_club_number cnum
  ON cnum.entity_id = nclub.nid AND cnum.deleted = '0'
LEFT JOIN node__field_region cr
  ON cr.entity_id = nclub.nid AND cr.deleted = '0'
LEFT JOIN node_field_data region_node
  ON region_node.nid = cr.field_region_target_id
LEFT JOIN node__field_region_number rnum
  ON rnum.entity_id = region_node.nid AND rnum.deleted = '0'

LEFT JOIN v_brns brns
  ON brns.user_id = u.uid

WHERE
  md.personal_status_id IN ('947', '951', '1099')
  AND pm_self.entity_id IS NULL
  AND (flags.member_flag = 1 OR flags.affiliate_flag = 1)
"#;

fn fetch_club_members_query<'builder>() -> sqlx::QueryBuilder<'builder, MySql> {
    sqlx::QueryBuilder::new(FETCH_CLUB_MEMBERS_QUERY)
}
pub mod mailing_address {
    use super::*;

    pub async fn by_uid(pool: &MySqlPool, uid: u64) -> Result<Option<Address>> {
        let member = fetch_mailing_address_query()
            .push("AND user__field_address.entity_id = ")
            .push_bind(uid)
            .build_query_as::<Address>()
            .fetch_optional(pool)
            .await?;
        Ok(member)
    }

    pub async fn by_uids<I: IntoIterator<Item = u64>>(
        pool: &MySqlPool,
        uids: I,
    ) -> Result<HashMap<u64, Address>> {
        let mut builder = fetch_mailing_address_query();
        let mut seperated = builder
            .push("AND user__field_address.entity_id IN (")
            .separated(", ");
        for value in uids {
            seperated.push_bind(value);
        }
        seperated.push_unseparated(") ");
        let members: HashMap<u64, Address> = builder
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
        pool: &MySqlPool,
        members: impl IntoIterator<Item = &Member>,
    ) -> Result<HashMap<u64, Address>> {
        by_uids(pool, members.into_iter().map(|member| member.primary.uid)).await
    }

    pub async fn all(pool: &MySqlPool) -> Result<Vec<Address>> {
        let members = fetch_mailing_address_query()
            .build_query_as::<Address>()
            .fetch_all(pool)
            .await?;
        Ok(members)
    }

    fn fetch_mailing_address_query<'builder>() -> sqlx::QueryBuilder<'builder, MySql> {
        sqlx::QueryBuilder::new(
            r#"
            SELECT
                user__field_address.entity_id as user_id,
                paragraph__field_address.field_address_value AS street_address,
                paragraph__field_street_address_2.field_street_address_2_value AS street_address_2,
                paragraph__field_zip_code.field_zip_code_value AS zip_code,
                paragraph__field_city.field_city_value AS city,
                paragraph__field_state_name.field_state_name_value AS state,
                paragraph__field_country.field_country_value AS country
            FROM
                paragraph__field_use_as_mailing_address mail
                INNER JOIN user__field_address ON user__field_address.field_address_target_id = mail.entity_id
                INNER JOIN paragraph__field_address ON mail.entity_id = paragraph__field_address.entity_id
                LEFT JOIN paragraph__field_street_address_2 ON mail.entity_id = paragraph__field_street_address_2.entity_id
                LEFT JOIN paragraph__field_zip_code ON mail.entity_id = paragraph__field_zip_code.entity_id
                LEFT JOIN paragraph__field_city ON mail.entity_id = paragraph__field_city.entity_id
                LEFT JOIN paragraph__field_state_name ON mail.entity_id = paragraph__field_state_name.entity_id
                LEFT JOIN paragraph__field_country ON mail.entity_id = paragraph__field_country.entity_id
            WHERE
                mail.field_use_as_mailing_address_value = 1 
            "#,
        )
    }
}

#[derive(Debug, serde::Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
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

#[derive(Debug, serde::Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
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

impl TryFrom<i32> for MemberStatus {
    type Error = sqlx::Error;
    fn try_from(value: i32) -> std::result::Result<Self, Self::Error> {
        match value {
            947 | 1099 => Ok(Self::Current),
            951 => Ok(Self::Lapsed),
            other => Err(sqlx::Error::decode(format!(
                "unexpected member status {other}"
            ))),
        }
    }
}

#[derive(Debug, serde::Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MemberType {
    #[default]
    Regular,
    Intraclub,
    Affiliate,
}

impl fmt::Display for MemberType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Regular => f.write_str("regular"),
            Self::Intraclub => f.write_str("intraclub"),
            Self::Affiliate => f.write_str("affiliate"),
        }
    }
}

impl TryFrom<String> for MemberType {
    type Error = sqlx::Error;
    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        match value.to_lowercase().as_str() {
            "field_home_club" | "regular" => Ok(Self::Regular),
            "field_intraclub_memberships" | "intraclub" => Ok(Self::Intraclub),
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
    #[sqlx(default, try_from = "i32")]
    pub member_status: MemberStatus,
    #[sqlx(flatten)]
    pub primary: User,
    #[sqlx(flatten, try_from = "PartnerUser")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partner: Option<User>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration_date: Option<chrono::NaiveDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub join_date: Option<chrono::NaiveDate>,
    #[sqlx(flatten, try_from = "LocalClub")]
    pub local_club: Club,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[sqlx(flatten, try_from = "Brns")]
    pub brns: Vec<String>,
}

pub mod db {
    use super::*;
    use ::db as app_db;

    impl From<MemberClass> for app_db::member::MemberClass {
        fn from(value: MemberClass) -> Self {
            match value {
                MemberClass::Regular => Self::Regular,
                MemberClass::Lifetime => Self::Lifetime,
                MemberClass::Complimentary => Self::Complimentary,
            }
        }
    }

    impl From<MemberStatus> for app_db::member::MemberStatus {
        fn from(value: MemberStatus) -> Self {
            match value {
                MemberStatus::Current => Self::Current,
                MemberStatus::Lapsed => Self::Lapsed,
            }
        }
    }

    impl From<MemberType> for app_db::member::MemberType {
        fn from(value: MemberType) -> Self {
            match value {
                MemberType::Regular | MemberType::Intraclub => Self::Regular,
                MemberType::Affiliate => Self::Affiliate,
            }
        }
    }

    impl From<Member> for app_db::member::Member {
        fn from(value: Member) -> Self {
            Self {
                member_class: value.member_class.into(),
                member_type: value.member_type.into(),
                member_status: value.member_status.into(),
                primary: value.primary.into(),
                partner: value.partner.map(Into::into),
                expiration_date: value.expiration_date,
                join_date: value.join_date,
                local_club: value.local_club.into(),
            }
        }
    }

    impl From<&Member> for Vec<app_db::brn::Brn> {
        fn from(value: &Member) -> Vec<app_db::brn::Brn> {
            value
                .brns
                .iter()
                .map(|number| app_db::brn::Brn {
                    user_id: app_db::user::id_for_email(&value.primary.email),
                    number: number.to_owned(),
                })
                .collect()
        }
    }

    impl Address {
        pub fn to_db_address_for_member(self, member: &Member) -> app_db::address::Address {
            app_db::address::Address {
                user_id: app_db::user::id_for_email(&member.primary.email),
                state: self.state,
                country: self.country,
            }
        }
    }
}

pub mod mailchimp {
    use super::*;
    use ::mailchimp as mc;

    pub fn to_tag_updates(members: &[Member]) -> Vec<(String, Vec<mc::members::MemberTagUpdate>)> {
        members
            .iter()
            .flat_map(|member| {
                let tag_updates = to_member_tag_updates(member);
                let mut updates = Vec::with_capacity(2);
                if mc::members::is_valid_email(&member.primary.email) {
                    updates.push((
                        mc::members::member_id(&member.primary.email),
                        tag_updates.clone(),
                    ));
                }
                if let Some(partner) = &member.partner
                    && mc::members::is_valid_email(&partner.email)
                {
                    updates.push((mc::members::member_id(&partner.email), tag_updates));
                }
                updates
            })
            .collect_vec()
    }

    fn to_member_tag_updates(member: &Member) -> Vec<mc::members::MemberTagUpdate> {
        fn to_update<F: Fn(&Member) -> bool>(
            name: &str,
            member: &Member,
            f: F,
        ) -> mc::members::MemberTagUpdate {
            let status = if f(member) {
                mc::members::MemberTagStatus::Active
            } else {
                mc::members::MemberTagStatus::Inactive
            };
            mc::members::MemberTagUpdate {
                name: name.to_string(),
                status,
            }
        }
        vec![
            to_update("affiliate", member, |m| {
                m.member_type == MemberType::Affiliate
            }),
            to_update("member", member, |m| m.member_type == MemberType::Regular),
            to_update("lifetime", member, |m| {
                m.member_class == MemberClass::Lifetime
            }),
            to_update("lapsed", member, |m| {
                m.member_status == MemberStatus::Lapsed
            }),
        ]
    }
    pub async fn to_members_with_address(
        members: &[Member],
        addresses: &HashMap<u64, Address>,
        merge_fields: &mc::merge_fields::MergeFields,
    ) -> mc::Result<Vec<mc::members::Member>> {
        // Convert ddb members to mailchimp members while injecting address
        let result_vecs: Vec<Vec<mc::members::Member>> = members
            .iter()
            .map(|member| {
                let address = addresses.get(&member.primary.uid);
                to_members(member, &address.cloned(), merge_fields)
            })
            .try_collect()?;

        Ok(result_vecs.into_iter().flatten().collect())
    }

    pub fn to_members(
        member: &Member,
        address: &Option<Address>,
        merge_fields: &mc::merge_fields::MergeFields,
    ) -> mc::Result<Vec<mc::members::Member>> {
        let primary = to_member(member, address, &member.primary, merge_fields)?;

        let mut result = Vec::with_capacity(2);
        if let Some(partner_user) = &member.partner {
            let mut partner = to_member(member, address, partner_user, merge_fields)?;
            if let Some(ref mut merge_fields) = partner.merge_fields {
                merge_fields.insert("PRIMARY".into(), member.primary.email.clone().into());
            }
            if mc::members::is_valid_email(&partner.email_address) {
                result.push(partner);
            }
        }

        if mc::members::is_valid_email(&primary.email_address) {
            result.push(primary);
        }

        Ok(result)
    }

    fn to_member(
        member: &Member,
        address: &Option<Address>,
        user: &User,
        merge_fields: &mc::merge_fields::MergeFields,
    ) -> mc::Result<mc::members::Member> {
        let user_fields: Vec<mc::merge_fields::MergeFieldValue> = [
            merge_fields.to_value("FNAME", user.first_name.as_ref()),
            merge_fields.to_value("LNAME", user.last_name.as_ref()),
            merge_fields.to_value("UID", user.uid),
            merge_fields.to_value("BDAY", user.birthday),
            merge_fields.to_value("LLOGIN", user.last_login),
            merge_fields.to_value("JOIN", member.join_date),
            merge_fields.to_value("EXPIRE", member.expiration_date),
            merge_fields.to_value("BRN", member.brns.first()),
        ]
        .into_iter()
        .filter_map(|value| value.transpose())
        .chain(address_to_values(address, merge_fields).into_iter())
        .chain(club_to_values(&member.local_club, merge_fields).into_iter())
        .collect::<mc::Result<Vec<mc::merge_fields::MergeFieldValue>>>()?;
        Ok(mc::members::Member {
            id: mc::members::member_id(&user.email),
            email_address: user.email.clone(),
            merge_fields: Some(user_fields.into_iter().collect()),
            status_if_new: Some(mc::members::MemberStatus::Subscribed),
            ..Default::default()
        })
    }

    fn address_to_values(
        address: &Option<Address>,
        merge_fields: &mc::merge_fields::MergeFields,
    ) -> Vec<mc::Result<mc::merge_fields::MergeFieldValue>> {
        let Some(address) = address.as_ref() else {
            return vec![];
        };

        vec![
            merge_fields.to_value("ZIP", address.zip_code.as_ref()),
            merge_fields.to_value("STATE", address.state.as_ref()),
            merge_fields.to_value("COUNTRY", address.country.as_ref()),
        ]
        .into_iter()
        .filter_map(|value| value.transpose())
        .collect()
    }

    fn club_to_values(
        club: &clubs::Club,
        merge_fields: &mc::merge_fields::MergeFields,
    ) -> Vec<mc::Result<mc::merge_fields::MergeFieldValue>> {
        vec![
            merge_fields.to_value("CLUB", club.name.as_str()),
            merge_fields.to_value("CLUB_NR", club.number),
            merge_fields.to_value("REGION", club.region),
        ]
        .into_iter()
        .filter_map(|value| value.transpose())
        .collect()
    }
}

#[derive(Debug, sqlx::FromRow, serde::Serialize, Clone)]
pub struct Address {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub street_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub street_address_2: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zip_code: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
}

#[derive(Debug, sqlx::FromRow, serde::Serialize)]
struct PartnerUser {
    partner_uid: Option<u64>,
    partner_email: Option<String>,
    partner_first_name: Option<String>,
    partner_last_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    partner_birthday: Option<chrono::NaiveDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    partner_last_login: Option<chrono::NaiveDate>,
}

impl From<PartnerUser> for Option<User> {
    fn from(value: PartnerUser) -> Option<User> {
        if let Some(uid) = value.partner_uid {
            Some(User {
                uid,
                email: value.partner_email.unwrap(),
                first_name: value.partner_first_name,
                last_name: value.partner_last_name,
                birthday: value.partner_birthday,
                last_login: value.partner_last_login,
                pass: None, // Partners don't have their own password
                gender: None,
                race_tid: None,
                communication_preference: None,
                blue_beret_mail: None,
                publish_info: None,
                special_needs: None,
                ada_parking: None,
                member_notes: None,
                military_status: None,
                first_responder_status: None,
                active: true, // Partners inherit active status from primary
            })
        } else {
            None
        }
    }
}

#[derive(Debug, sqlx::FromRow, serde::Serialize)]
struct LocalClub {
    club_name: Option<String>,
    club_uid: Option<u64>,
    club_number: Option<i64>,
    club_region: Option<i64>,
    club_region_uid: Option<u64>,
}

impl From<LocalClub> for Club {
    fn from(value: LocalClub) -> Club {
        Club {
            uid: value.club_uid.unwrap_or_default(),
            number: value.club_number,
            name: value.club_name.unwrap_or_default(),
            region: value.club_region_uid,
            active: true, // LocalClub doesn't track status, default to active
        }
    }
}

#[derive(Debug, sqlx::FromRow, serde::Serialize)]
struct Brns {
    brns: Option<String>,
}

impl From<Brns> for Vec<String> {
    fn from(value: Brns) -> Self {
        value
            .brns
            .unwrap_or_default()
            .split(",")
            .map(|v| v.trim().to_string())
            .collect()
    }
}
