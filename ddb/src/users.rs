use crate::Result;
use sqlx::{MySqlPool, mysql::MySql};

/// Drupal user data.
///
/// **IMPORTANT**: This struct is flattened via `#[sqlx(flatten)]` in multiple queries.
/// When adding fields, you MUST also update these queries to include the new column:
/// - `users.rs`: `fetch_user_query()` - selects actual column values
/// - `leadership.rs`: `FETCH_LEADERSHIP_BASE` - uses NULL placeholders
/// - `members.rs`: `FETCH_ALL_MEMBERS_QUERY` - uses NULL placeholders
/// - `members.rs`: `FETCH_CLUB_MEMBERS_QUERY` - uses NULL placeholders
/// - `members.rs`: `impl From<PartnerUser> for Option<User>` - manual construction
#[derive(Debug, sqlx::FromRow, serde::Serialize, Clone)]
pub struct User {
    pub uid: u64,
    pub email: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub birthday: Option<chrono::NaiveDate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_login: Option<chrono::NaiveDate>,
    /// Drupal PHPass hash (e.g., "$S$E..."). Excluded from serialization.
    #[serde(skip_serializing)]
    pub pass: Option<String>,
    // Communication preferences
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gender: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub race_tid: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub communication_preference: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blue_beret_mail: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_info: Option<bool>,
    // Accessibility
    #[serde(skip_serializing_if = "Option::is_none")]
    pub special_needs: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ada_parking: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member_notes: Option<String>,
    // Background
    #[serde(skip_serializing_if = "Option::is_none")]
    pub military_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_responder_status: Option<String>,
    /// Account status: true = active (can log in), false = blocked
    pub active: bool,
}

fn fetch_user_query<'builder>() -> sqlx::QueryBuilder<'builder, MySql> {
    sqlx::QueryBuilder::new(
        r#"
            SELECT DISTINCT
                users_field_data.uid AS uid,
                users_field_data.mail as email,
                user__field_first_name.field_first_name_value AS first_name,
                user__field_last_name.field_last_name_value AS last_name,
                CAST(user__field_birth_date.field_birth_date_value AS DATE) AS birthday,
                DATE(FROM_UNIXTIME(users_field_data.login)) AS last_login,
                users_field_data.pass AS pass,
                ufg.field_gender_value AS gender,
                ufr.field_race_target_id AS race_tid,
                ufcp.field_communication_preferences_value AS communication_preference,
                ufbb.field_blue_beret_mail_value AS blue_beret_mail,
                ufpi.field_publish_info_value AS publish_info,
                CASE WHEN ufsm.field_special_member_value = 1 THEN TRUE ELSE FALSE END AS special_needs,
                CASE WHEN ufap.field_ada_parking_value = 1 THEN TRUE ELSE FALSE END AS ada_parking,
                ufspe.field_spe_value AS member_notes,
                ufmil.field_military_value AS military_status,
                uffr.field_first_responder_value AS first_responder_status,
                CASE WHEN users_field_data.status = 1 THEN TRUE ELSE FALSE END AS active
            FROM
                users_field_data
                LEFT JOIN user__field_first_name ON users_field_data.uid = user__field_first_name.entity_id
                LEFT JOIN user__field_last_name ON users_field_data.uid = user__field_last_name.entity_id
                LEFT JOIN user__field_birth_date ON users_field_data.uid = user__field_birth_date.entity_id
                LEFT JOIN user__field_gender ufg ON ufg.entity_id = users_field_data.uid AND ufg.deleted = '0'
                LEFT JOIN user__field_race ufr ON ufr.entity_id = users_field_data.uid AND ufr.deleted = '0'
                LEFT JOIN user__field_communication_preferences ufcp ON ufcp.entity_id = users_field_data.uid AND ufcp.deleted = '0'
                LEFT JOIN user__field_blue_beret_mail ufbb ON ufbb.entity_id = users_field_data.uid AND ufbb.deleted = '0'
                LEFT JOIN user__field_publish_info ufpi ON ufpi.entity_id = users_field_data.uid AND ufpi.deleted = '0'
                LEFT JOIN user__field_special_member ufsm ON ufsm.entity_id = users_field_data.uid AND ufsm.deleted = '0'
                LEFT JOIN user__field_ada_parking ufap ON ufap.entity_id = users_field_data.uid AND ufap.deleted = '0'
                LEFT JOIN user__field_spe ufspe ON ufspe.entity_id = users_field_data.uid AND ufspe.deleted = '0'
                LEFT JOIN user__field_military ufmil ON ufmil.entity_id = users_field_data.uid AND ufmil.deleted = '0'
                LEFT JOIN user__field_first_responder uffr ON uffr.entity_id = users_field_data.uid AND uffr.deleted = '0'
            WHERE
                users_field_data.mail IS NOT NULL
                AND
            "#,
    )
}

pub async fn by_uid(pool: &MySqlPool, uid: u64) -> Result<Option<User>> {
    let user = fetch_user_query()
        .push("users_field_data.uid = ")
        .push_bind(uid)
        .build_query_as::<User>()
        .fetch_optional(pool)
        .await?;

    Ok(user)
}

pub async fn by_email(pool: &MySqlPool, email: &str) -> Result<Option<User>> {
    let user = fetch_user_query()
        .push("users_field_data.mail = ")
        .push_bind(email)
        .build_query_as::<User>()
        .fetch_optional(pool)
        .await?;

    Ok(user)
}

/// Fetch all users with valid email addresses
pub async fn all(pool: &MySqlPool) -> Result<Vec<User>> {
    use futures::TryFutureExt;
    fetch_user_query()
        .push("users_field_data.mail != ''")
        .build_query_as::<User>()
        .fetch_all(pool)
        .map_err(Into::into)
        .await
}

/// User avatar from Drupal file_managed table.
#[derive(Debug, sqlx::FromRow)]
pub struct UserAvatar {
    /// Drupal user ID
    pub uid: u64,
    /// File URI (e.g., "public://pictures/2020-02/Marc164.png")
    pub uri: String,
}

/// Fetch all users with custom avatars (excluding default images).
pub async fn avatars(pool: &MySqlPool) -> Result<Vec<UserAvatar>> {
    let avatars = sqlx::query_as::<_, UserAvatar>(
        r#"
        SELECT u.uid, f.uri
        FROM users_field_data u
        JOIN user__user_picture p ON u.uid = p.entity_id
        JOIN file_managed f ON p.user_picture_target_id = f.fid
        WHERE f.uri NOT LIKE '%default%'
          AND f.uri LIKE 'public://%'
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(avatars)
}

/// Convert Drupal file URI to downloadable path.
/// e.g., "public://pictures/2020-02/Marc164.png" -> "/sites/default/files/pictures/2020-02/Marc164.png"
pub fn avatar_uri_to_path(uri: &str) -> Option<String> {
    uri.strip_prefix("public://")
        .map(|path| format!("/sites/default/files/{path}"))
}

pub mod db {
    use super::*;
    use ::db as app_db;

    impl From<User> for app_db::user::User {
        fn from(value: User) -> Self {
            Self {
                id: app_db::user::id_for_email(&value.email),
                uid: value.uid as i64,
                email: value.email,
                first_name: value.first_name,
                last_name: value.last_name,
            }
        }
    }
}
