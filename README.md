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

The ACI all-members MailChimp audience carries an "Email Preferences" group,
the checkboxes members see on the hosted preferences page linked from every
campaign footer. Its shape lives in `mailchimp/data/interests-aci.toml`. A
group belongs to one audience: a job maintains it only when the job's
`interests` setting names that config (`sync-mail update 1 --interests aci`),
and a job with no setting has no group. `sync-mail interests` applies it.
Every member is opted into every interest on joining; the member sync defaults
new and returning members and never touches anyone else's choices.

Changing the list:

- **Add an interest.** Add it to the toml, run `sync-mail interests sync 1`
  (creates it on the audience), then `sync-mail interests seed 1 --interest
  "<name>"` so existing members are opted in without their other choices being
  reset. Campaigns for it need a saved segment.
- **Rename an interest.** Change `name` in the toml and list the old name
  under `was`, then run `sync-mail interests sync 1`. The interest is renamed
  in place and members' settings for it are kept. Renaming only in the
  MailChimp UI leaves the member sync unable to match it: it logs "preference
  group is missing an interest" and skips defaulting until the toml catches up.
- **Remove an interest.** Remove it from the toml and run `sync-mail interests
  sync 1 --process-deletes`. Without the flag the interest is reported as
  `extra` and left alone. Deleting it drops every member's setting for it.

`sync-mail interests status 1` shows each interest with the number of members
holding it beside the audience's subscribed count.

`interests seed` refuses to touch an interest any member already holds,
because that would opt back in everyone who switched it off; once the page
is live the plain form refuses itself. `--force` is the override, for a
deliberate re-opt-in of everyone.

Edits made to the group in the MailChimp UI are the failure to watch for.
The member sync refuses to run, before writing anything, when a configured
interest is missing from the audience, so the scheduled run fails and the
error names the interest. `sync-mail interests check 1` compares the audience
to the config without changing anything and exits non-zero on any difference,
including interests added in the UI; schedule it alongside the sync.

## Keeping a contact the membership database does not list

The member sync archives every live audience contact that is not in the
membership database. A contact tagged `keep` in the MailChimp UI is the
exception: the sync leaves it on the audience, and never writes or touches
that tag itself. Use it for an address that must receive every campaign but
has no membership record, such as a club's archival inbox. The tag also holds
a lapsed member on the audience, since it means the same thing there.
`sync-mail run --dry-run` omits kept contacts from the would-archive list.
