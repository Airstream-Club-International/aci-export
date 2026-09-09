# aci-export

Export/Conduit from the ACI Drupal database to other tools.

## Drupal Schema Reference

For Drupal database schema documentation, use the `drupal-db` MCP server which provides:
- `schema` tool - documented field mappings, common queries, Drupal patterns
- `find` tool - efficient table/column search
- `query` tool - read-only SQL execution

## aci-ddb Crate Patterns

### Scope CTE for Club/Region Queries

Unified approach for querying by club, region, or all members:

```sql
WITH scope AS (
    -- If club_nid provided: 1 club
    -- If region_nid provided: all clubs in region
    -- If both NULL: empty (no filtering)
    SELECT nid AS club_nid FROM node_field_data
    WHERE type = 'ssp_club'
      AND (nid = ? OR field_region_target_id = ?)
)
SELECT ...
FROM ...
WHERE (
    NOT EXISTS (SELECT 1 FROM scope)
    OR club_nid IN (SELECT club_nid FROM scope)
)
```

### User Struct Compatibility

The `aci_ddb::users::User` struct includes a `pass` field. When reusing this struct in leadership queries via `#[sqlx(flatten)]`, include `NULL AS pass` even though leadership queries don't need password data:

```sql
SELECT
    usr.uid,
    COALESCE(md.email, usr.mail) AS email,
    ufn.field_first_name_value AS first_name,
    uln.field_last_name_value AS last_name,
    CAST(ubd.field_birth_date_value AS DATE) AS birthday,
    DATE(FROM_UNIXTIME(usr.login)) AS last_login,
    NULL AS pass  -- Required for User struct compatibility
FROM ...
```

Without this, sqlx will fail with "no column found for name: pass".

### Output Field Conventions

Standard field names for query results:

**Primary user:**
- `uid`, `last_login`, `first_name`, `last_name`, `email`, `birthday`

**Member info:**
- `member_type` ('regular' or 'affiliate')
- `member_class` (taxonomy term or 'Regular')
- `member_status` (personal_status_id)
- `join_date`, `expiration_date`

**Club (from active membership):**
- `club_uid`, `club_name`, `club_number`, `club_region`, `club_region_uid`

**Partner:**
- `partner_uid`, `partner_last_login`, `partner_first_name`, `partner_last_name`, `partner_email`, `partner_birthday`

## Email preferences on the all-members audience

The all-members MailChimp audience carries an "Email Preferences" group, the
checkboxes members see on the hosted preferences page linked from every
campaign footer. Its shape lives in `mailchimp/data/interests-all.toml`, and
`sync-mail interests` applies it. Every member is opted into every interest on
joining; the member sync defaults new and returning members and never touches
anyone else's choices.

Changing the list:

- **Add an interest.** Add it to the toml, run `sync-mail interests sync 1`
  (creates it on the audience), then `sync-mail interests seed 1 --interest
  "<name>"` so existing members are opted in without their other choices being
  reset. Campaigns for it need a saved segment.
- **Rename an interest.** Rename it in the MailChimp UI and in the toml in the
  same change. Matching is by name: with only one side renamed, the member
  sync logs "preference group is missing an interest" and skips defaulting,
  and `interests sync` would create the new name beside the old one.
- **Remove an interest.** Remove it from the toml and delete it in the
  MailChimp UI. `interests sync` never deletes; until the UI side is done it
  reports the interest as `extra`.

Never run `interests seed` without `--interest` once the page is live: that
form opts everyone into everything.
