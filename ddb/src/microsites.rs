//! Microsite content extraction from Drupal.
//!
//! Queries club microsites including:
//! - Homepage and content pages
//! - Menu structure
//! - Page body content (HTML)

use crate::{Error, Result};
use sqlx::MySqlPool;

/// A club with its microsite homepage.
#[derive(Debug, sqlx::FromRow)]
pub struct ClubMicrosite {
    /// Club node ID (Drupal)
    pub club_nid: u64,
    /// Club number (business identifier) - None for intraclubs
    pub club_number: Option<i64>,
    /// Club name from ssp_club node
    pub club_name: String,
    /// Microsite homepage node ID
    pub homepage_nid: u64,
    /// Whether this is an intraclub (no club number)
    pub is_intraclub: bool,
}

/// Fetch all clubs that have microsites.
///
/// Matches ssp_club nodes to microsite_homepage nodes by title.
/// Includes both regular clubs (with club_number) and intraclubs (without).
/// Also includes manual overrides for clubs where titles don't match.
pub async fn clubs_with_microsites(pool: &MySqlPool) -> Result<Vec<ClubMicrosite>> {
    sqlx::query_as::<_, ClubMicrosite>(
        r#"
        SELECT
            club.nid as club_nid,
            cn.field_club_number_value as club_number,
            club.title as club_name,
            hp.nid as homepage_nid,
            cn.field_club_number_value IS NULL as is_intraclub
        FROM node_field_data hp
        JOIN node_field_data club ON club.title = hp.title AND club.type = 'ssp_club'
        LEFT JOIN node__field_club_number cn ON cn.entity_id = club.nid
        WHERE hp.type = 'microsite_homepage'

        UNION

        -- Manual overrides for clubs where homepage title doesn't match club title
        -- Boondocking Streamers (club) -> Boondockers Airstream Club (homepage)
        -- Vintage Airstream Club (club) -> Vintage Airstream Club (VAC) (homepage)
        SELECT
            club.nid as club_nid,
            cn.field_club_number_value as club_number,
            club.title as club_name,
            hp.nid as homepage_nid,
            cn.field_club_number_value IS NULL as is_intraclub
        FROM node_field_data club
        JOIN node_field_data hp ON (club.nid, hp.nid) IN ((51008, 55629), (47596, 50698))
        LEFT JOIN node__field_club_number cn ON cn.entity_id = club.nid
        WHERE club.type = 'ssp_club' AND hp.type = 'microsite_homepage'

        ORDER BY is_intraclub, club_number, club_name
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(Error::from)
}

/// Club slug from Drupal path alias.
#[derive(Debug, sqlx::FromRow)]
pub struct ClubSlug {
    /// Club node ID (ssp_club.nid)
    pub club_nid: u64,
    /// URL slug from Drupal path_alias (without leading slash)
    pub slug: String,
}

/// Fetch URL slugs for all clubs with microsites.
///
/// Returns the Drupal path alias for each club's microsite homepage.
/// Maps by joining ssp_club → microsite_homepage (by title) → path_alias.
/// Also includes manual overrides for clubs where titles don't match.
pub async fn club_slugs(pool: &MySqlPool) -> Result<Vec<ClubSlug>> {
    sqlx::query_as::<_, ClubSlug>(
        r#"
        SELECT club.nid as club_nid, TRIM(LEADING '/' FROM pa.alias) as slug
        FROM node_field_data club
        JOIN node_field_data hp ON hp.title = club.title AND hp.type = 'microsite_homepage'
        JOIN path_alias pa ON pa.path = CONCAT('/node/', hp.nid)
        WHERE club.type = 'ssp_club'

        UNION

        -- Manual overrides for clubs where homepage title doesn't match club title
        SELECT club.nid as club_nid, TRIM(LEADING '/' FROM pa.alias) as slug
        FROM node_field_data club
        JOIN node_field_data hp ON (club.nid, hp.nid) IN ((51008, 55629), (47596, 50698))
        JOIN path_alias pa ON pa.path = CONCAT('/node/', hp.nid)
        WHERE club.type = 'ssp_club' AND hp.type = 'microsite_homepage'
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(Error::from)
}

/// A microsite page with its content and menu metadata.
#[derive(Debug)]
pub struct MicrositePage {
    /// Node ID
    pub nid: u64,
    /// Page title
    pub title: String,
    /// HTML body content
    pub body_html: String,
    /// Whether page is published
    pub status: bool,
    /// Menu item ID (if in menu)
    pub menu_id: Option<u64>,
    /// Menu title (may differ from node title)
    pub menu_title: Option<String>,
    /// Menu weight for ordering
    pub menu_weight: Option<i32>,
    /// Parent menu item UUID (for nesting)
    pub menu_parent: Option<String>,
    /// Hero banner image (public:// URI)
    pub hero_image: Option<String>,
    /// Navigation/thumbnail image (public:// URI)
    pub nav_image: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct PageRow {
    nid: u64,
    title: String,
    /// Override title from field_page_title (used by some lander pages)
    page_title: Option<String>,
    body_value: Option<String>,
    summary_value: Option<String>,
    /// Custom field_body (different from node body, used by ~58 pages)
    field_body_value: Option<String>,
    status: i8,
    menu_id: Option<u64>,
    menu_title: Option<String>,
    menu_weight: Option<i32>,
    menu_parent: Option<String>,
    /// Hero banner image file URI (public://...)
    hero_image_uri: Option<String>,
    /// Navigation image file URI (public://...)
    nav_image_uri: Option<String>,
}

/// A featured page paragraph with headline, content, optional button, and optional image.
#[derive(Debug, sqlx::FromRow)]
struct FeaturedPageRow {
    headline: Option<String>,
    summary_text_2: Option<String>,
    button_uri: Option<String>,
    button_title: Option<String>,
    image_uri: Option<String>,
}

impl From<PageRow> for MicrositePage {
    fn from(row: PageRow) -> Self {
        // Use page_title override if present, otherwise use node title
        let title = row.page_title.unwrap_or(row.title);

        // Combine all body sources - pages may use different combinations:
        // - node__body (standard body field)
        // - node__field_summary (summary field)
        // - node__field_body (custom body field, ~58 pages)
        let mut parts = Vec::new();
        if let Some(summary) = row.summary_value {
            parts.push(summary);
        }
        if let Some(body) = row.body_value {
            parts.push(body);
        }
        if let Some(field_body) = row.field_body_value {
            parts.push(field_body);
        }
        let body_html = parts.join("\n\n");

        Self {
            nid: row.nid,
            title,
            body_html,
            status: row.status == 1,
            menu_id: row.menu_id,
            menu_title: row.menu_title,
            menu_weight: row.menu_weight,
            menu_parent: row.menu_parent,
            hero_image: row.hero_image_uri,
            nav_image: row.nav_image_uri,
        }
    }
}

/// Fetch featured pages content for a node.
///
/// Some pages store content in `field_featured_pages` paragraphs instead of the body.
/// Each paragraph has a headline, summary_text_2 field, optional button link, and optional image.
async fn featured_pages_content(pool: &MySqlPool, nid: u64) -> Result<String> {
    let rows: Vec<FeaturedPageRow> = sqlx::query_as(
        r#"
        SELECT
            fh.field_headline_value as headline,
            fst.field_summary_text_2_value as summary_text_2,
            pb.field_button_uri as button_uri,
            pb.field_button_title as button_title,
            CAST(img_file.uri AS CHAR(255)) as image_uri
        FROM node__field_featured_pages fp
        LEFT JOIN paragraph__field_headline fh ON fh.entity_id = fp.field_featured_pages_target_id
        LEFT JOIN paragraph__field_summary_text_2 fst ON fst.entity_id = fp.field_featured_pages_target_id
        LEFT JOIN paragraph__field_button pb ON pb.entity_id = fp.field_featured_pages_target_id
        -- Image: paragraph -> field_image -> media -> field_media_image -> file
        LEFT JOIN paragraph__field_image pimg ON pimg.entity_id = fp.field_featured_pages_target_id
        LEFT JOIN media__field_media_image img_mfi ON img_mfi.entity_id = pimg.field_image_target_id
        LEFT JOIN file_managed img_file ON img_file.fid = img_mfi.field_media_image_target_id
        WHERE fp.entity_id = ?
        ORDER BY fp.delta
        "#,
    )
    .bind(nid)
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(String::new());
    }

    let mut html = String::new();
    for row in rows {
        // Render image if present (before headline)
        if let Some(uri) = row.image_uri {
            // Convert public:// URI to Drupal path
            let src = uri.replace("public://", "/sites/default/files/");
            html.push_str(&format!("<p><img src=\"{src}\" alt=\"\"></p>\n"));
        }
        if let Some(headline) = row.headline {
            html.push_str(&format!("<h3>{headline}</h3>\n"));
        }
        if let Some(content) = row.summary_text_2 {
            html.push_str(&content);
            html.push('\n');
        }
        // Render button as a link if present
        if let Some(uri) = row.button_uri {
            let title = row.button_title.unwrap_or_else(|| uri.clone());
            html.push_str(&format!("<p><a href=\"{uri}\">{title}</a></p>\n"));
        }
    }

    Ok(html)
}

/// Fetch all pages for a club's microsite.
///
/// Includes the homepage and all pages in its menu tree.
/// Uses menu structure for discovery (more reliable than field_club references).
pub async fn pages_for_club(pool: &MySqlPool, homepage_nid: u64) -> Result<Vec<MicrositePage>> {
    // First get the homepage's menu UUID for finding child pages
    // UUID is stored as VARBINARY in MySQL, so we cast it to CHAR
    let homepage_uuid: Option<String> = sqlx::query_scalar(
        r#"
        SELECT CAST(mlc.uuid AS CHAR(36))
        FROM menu_link_content mlc
        JOIN menu_link_content_data mld ON mld.id = mlc.id
        WHERE mld.link__uri = CONCAT('entity:node/', ?)
        AND mld.menu_name = 'microsites'
        LIMIT 1
        "#,
    )
    .bind(homepage_nid)
    .fetch_optional(pool)
    .await?;

    // Fetch homepage
    let homepage: Option<PageRow> = sqlx::query_as(
        r#"
        SELECT
            n.nid,
            n.title,
            pt.field_page_title_value as page_title,
            b.body_value,
            s.field_summary_value as summary_value,
            fb.field_body_value,
            n.status,
            mld.id as menu_id,
            mld.title as menu_title,
            mld.weight as menu_weight,
            mld.parent as menu_parent,
            CAST(hero_file.uri AS CHAR(255)) as hero_image_uri,
            CAST(nav_file.uri AS CHAR(255)) as nav_image_uri
        FROM node_field_data n
        LEFT JOIN node__field_page_title pt ON pt.entity_id = n.nid
        LEFT JOIN node__body b ON b.entity_id = n.nid
        LEFT JOIN node__field_summary s ON s.entity_id = n.nid
        LEFT JOIN node__field_body fb ON fb.entity_id = n.nid
        LEFT JOIN menu_link_content_data mld ON mld.link__uri = CONCAT('entity:node/', n.nid)
            AND mld.menu_name = 'microsites'
        -- Hero banner image: node -> field_hero_banner_image -> media -> field_media_image -> file
        LEFT JOIN node__field_hero_banner_image hbi ON hbi.entity_id = n.nid
        LEFT JOIN media__field_media_image hero_mfi ON hero_mfi.entity_id = hbi.field_hero_banner_image_target_id
        LEFT JOIN file_managed hero_file ON hero_file.fid = hero_mfi.field_media_image_target_id
        -- Navigation image: node -> field_navigatio_ -> media -> field_media_image -> file
        LEFT JOIN node__field_navigatio_ nav ON nav.entity_id = n.nid
        LEFT JOIN media__field_media_image nav_mfi ON nav_mfi.entity_id = nav.field_navigatio__target_id
        LEFT JOIN file_managed nav_file ON nav_file.fid = nav_mfi.field_media_image_target_id
        WHERE n.nid = ?
        "#,
    )
    .bind(homepage_nid)
    .fetch_optional(pool)
    .await?;

    let mut pages: Vec<MicrositePage> = Vec::new();

    if let Some(hp) = homepage {
        let mut page: MicrositePage = hp.into();
        // Append featured pages content if any
        let featured = featured_pages_content(pool, page.nid).await?;
        if !featured.is_empty() {
            if page.body_html.is_empty() {
                page.body_html = featured;
            } else {
                page.body_html.push_str("\n\n");
                page.body_html.push_str(&featured);
            }
        }
        pages.push(page);
    }

    // If homepage has a menu entry, find all child pages via menu structure
    if let Some(uuid) = homepage_uuid {
        let parent_ref = format!("menu_link_content:{uuid}");

        // Fetch all pages that are children of the homepage in the menu
        // This catches all node types (microsite_content, microsite_lander_new, etc.)
        let content_pages: Vec<PageRow> = sqlx::query_as(
            r#"
            SELECT
                n.nid,
                n.title,
                pt.field_page_title_value as page_title,
                b.body_value,
                s.field_summary_value as summary_value,
                fb.field_body_value,
                n.status,
                mld.id as menu_id,
                mld.title as menu_title,
                mld.weight as menu_weight,
                mld.parent as menu_parent,
                CAST(hero_file.uri AS CHAR(255)) as hero_image_uri,
                CAST(nav_file.uri AS CHAR(255)) as nav_image_uri
            FROM menu_link_content_data mld
            JOIN node_field_data n ON mld.link__uri = CONCAT('entity:node/', n.nid)
            LEFT JOIN node__field_page_title pt ON pt.entity_id = n.nid
            LEFT JOIN node__body b ON b.entity_id = n.nid
            LEFT JOIN node__field_summary s ON s.entity_id = n.nid
            LEFT JOIN node__field_body fb ON fb.entity_id = n.nid
            -- Hero banner image: node -> field_hero_banner_image -> media -> field_media_image -> file
            LEFT JOIN node__field_hero_banner_image hbi ON hbi.entity_id = n.nid
            LEFT JOIN media__field_media_image hero_mfi ON hero_mfi.entity_id = hbi.field_hero_banner_image_target_id
            LEFT JOIN file_managed hero_file ON hero_file.fid = hero_mfi.field_media_image_target_id
            -- Navigation image: node -> field_navigatio_ -> media -> field_media_image -> file
            LEFT JOIN node__field_navigatio_ nav ON nav.entity_id = n.nid
            LEFT JOIN media__field_media_image nav_mfi ON nav_mfi.entity_id = nav.field_navigatio__target_id
            LEFT JOIN file_managed nav_file ON nav_file.fid = nav_mfi.field_media_image_target_id
            WHERE mld.menu_name = 'microsites'
            AND mld.parent = ?
            AND mld.enabled = 1
            ORDER BY mld.weight, n.title
            "#,
        )
        .bind(&parent_ref)
        .fetch_all(pool)
        .await?;

        for row in content_pages {
            let mut page: MicrositePage = row.into();
            // Append featured pages content if any
            let featured = featured_pages_content(pool, page.nid).await?;
            if !featured.is_empty() {
                if page.body_html.is_empty() {
                    page.body_html = featured;
                } else {
                    page.body_html.push_str("\n\n");
                    page.body_html.push_str(&featured);
                }
            }
            pages.push(page);
        }
    }

    Ok(pages)
}

/// Extract media URLs from HTML content.
///
/// Finds all `/sites/default/files/` URLs that need to be downloaded.
pub fn extract_media_urls(html: &str) -> Vec<String> {
    use regex::Regex;
    use std::sync::LazyLock;

    static MEDIA_RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r#"(?:src|href)=["']([^"']*?/sites/default/files/[^"']+)["']"#)
            .expect("Invalid media regex")
    });

    MEDIA_RE
        .captures_iter(html)
        .filter_map(|cap| cap.get(1).map(|m| m.as_str().to_string()))
        .collect()
}

/// Homepage assets (banner image, logo, etc.)
#[derive(Debug, Default)]
pub struct HomepageAssets {
    /// Desktop banner image file path (public:// URI)
    pub banner_image: Option<String>,
    /// Club logo image file path (public:// URI)
    pub logo_image: Option<String>,
    /// Facebook page/group URL
    pub facebook_url: Option<String>,
}

/// Fetch homepage assets (banner image, logo, etc.) for a microsite.
pub async fn homepage_assets(pool: &MySqlPool, homepage_nid: u64) -> Result<HomepageAssets> {
    // Get banner image via: field_desktop_banner_image -> media -> field_media_image -> file
    // CAST is needed because utf8mb4_bin collation is interpreted as VARBINARY by sqlx
    let banner_image: Option<String> = sqlx::query_scalar(
        r#"
        SELECT CAST(f.uri AS CHAR(255))
        FROM node__field_desktop_banner_image dbi
        JOIN media__field_media_image mfi ON mfi.entity_id = dbi.field_desktop_banner_image_target_id
        JOIN file_managed f ON f.fid = mfi.field_media_image_target_id
        WHERE dbi.entity_id = ?
        LIMIT 1
        "#,
    )
    .bind(homepage_nid)
    .fetch_optional(pool)
    .await?;

    // Get logo image via: media with field_club pointing to homepage AND name contains 'logo'
    // Pick the first one by media ID (oldest upload)
    let logo_image: Option<String> = sqlx::query_scalar(
        r#"
        SELECT CAST(f.uri AS CHAR(255))
        FROM media__field_club mfc
        JOIN media_field_data m ON m.mid = mfc.entity_id
        JOIN media__field_media_image mfi ON mfi.entity_id = m.mid
        JOIN file_managed f ON f.fid = mfi.field_media_image_target_id
        WHERE mfc.field_club_target_id = ?
        AND m.name LIKE '%logo%'
        ORDER BY m.mid
        LIMIT 1
        "#,
    )
    .bind(homepage_nid)
    .fetch_optional(pool)
    .await?;

    // Get Facebook URL from social media paragraphs (field_social_media_new)
    // or from button field if it points to facebook.com
    let facebook_url: Option<String> = sqlx::query_scalar(
        r#"
        SELECT url FROM (
            -- From social_media_new paragraph reference
            SELECT sml.field_social_media_link_uri as url
            FROM node__field_social_media_new smn
            JOIN paragraph__field_social_media_link sml
                ON sml.entity_id = smn.field_social_media_new_target_id
            WHERE smn.entity_id = ?
            AND sml.field_social_media_link_uri LIKE '%facebook.com%'
            LIMIT 1
        ) social
        UNION
        SELECT url FROM (
            -- From button field (only if pointing to facebook)
            SELECT field_button_uri as url
            FROM node__field_button
            WHERE entity_id = ?
            AND field_button_uri LIKE '%facebook.com%'
            LIMIT 1
        ) button
        LIMIT 1
        "#,
    )
    .bind(homepage_nid)
    .bind(homepage_nid)
    .fetch_optional(pool)
    .await?;

    Ok(HomepageAssets {
        banner_image,
        logo_image,
        facebook_url,
    })
}

/// Convert a Drupal public:// URI to a /sites/default/files/ path.
pub fn drupal_uri_to_path(uri: &str) -> Option<String> {
    uri.strip_prefix("public://")
        .map(|path| format!("/sites/default/files/{path}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_media_urls() {
        let html = r#"
            <img src="/sites/default/files/images/photo.jpg">
            <a href="/sites/default/files/docs/manual.pdf">Download</a>
            <img src="https://example.com/external.jpg">
        "#;

        let urls = extract_media_urls(html);
        assert_eq!(urls.len(), 2);
        assert!(urls.contains(&"/sites/default/files/images/photo.jpg".to_string()));
        assert!(urls.contains(&"/sites/default/files/docs/manual.pdf".to_string()));
    }

    #[test]
    fn test_extract_media_urls_empty() {
        let html = "<p>No media here</p>";
        let urls = extract_media_urls(html);
        assert!(urls.is_empty());
    }

    #[test]
    fn test_drupal_uri_to_path() {
        assert_eq!(
            drupal_uri_to_path("public://2025-06/IMG_4377.jpeg"),
            Some("/sites/default/files/2025-06/IMG_4377.jpeg".to_string())
        );
        assert_eq!(drupal_uri_to_path("private://secret.pdf"), None);
        assert_eq!(drupal_uri_to_path("not-a-uri"), None);
    }
}
