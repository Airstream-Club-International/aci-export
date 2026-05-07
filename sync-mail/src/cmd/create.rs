use crate::{Result, cmd::print_json, mailchimp::Job, settings::Settings};

/// Create a new sync job
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// Name of the club
    #[arg(long)]
    name: String,
    /// Club or region to sync
    #[command(flatten)]
    target: Target,
    /// Mailchimp API key
    #[arg(long)]
    api_key: String,
    /// Mailchimp audience identifier
    #[arg(long)]
    list: String,
    /// Skip the merge-fields sync that runs before the job is inserted
    #[arg(long)]
    skip_field_sync: bool,
}

#[derive(Debug, clap::Args)]
#[group(required = true, multiple = false)]
struct Target {
    /// Club number to sync (resolved to uid via the Drupal database)
    #[arg(long)]
    club: Option<i32>,
    /// Partial club name (case-insensitive substring); errors if ambiguous
    #[arg(long)]
    club_name: Option<String>,
    /// Region uid to sync
    #[arg(long)]
    region: Option<i32>,
}

impl Cmd {
    pub async fn run(&self, settings: Settings) -> Result {
        let client = mailchimp::client::from_api_key(&self.api_key)?;
        mailchimp::lists::get(&client, &self.list).await?;

        let club_uid = self.resolve_club_uid(&settings).await?;

        let to_create = Job {
            name: self.name.clone(),
            club: club_uid,
            list: self.list.clone(),
            api_key: self.api_key.clone(),
            region: self.target.region,
            ..Default::default()
        };

        if !self.skip_field_sync {
            to_create.sync_merge_fields(true).await?;
        }

        let db = settings.mail.db.connect().await?;
        let job = Job::create(&db, &to_create).await?;
        print_json(&job)
    }

    async fn resolve_club_uid(&self, settings: &Settings) -> Result<Option<i64>> {
        match (&self.target.club, &self.target.club_name) {
            (Some(number), None) => {
                let ddb = settings.ddb.connect().await?;
                let club = ddb::clubs::by_number(&ddb, *number)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("Club number {number} not found"))?;
                Ok(Some(club.uid as i64))
            }
            (None, Some(pattern)) => {
                let ddb = settings.ddb.connect().await?;
                let mut matches = ddb::clubs::search_by_name(&ddb, pattern).await?;
                match matches.len() {
                    0 => anyhow::bail!("No club matched name {pattern:?}"),
                    1 => Ok(Some(matches.remove(0).uid as i64)),
                    n => {
                        let candidates = matches
                            .iter()
                            .map(|c| {
                                let number =
                                    c.number.map_or_else(|| "-".to_string(), |n| n.to_string());
                                let inactive = if c.active { "" } else { " [inactive]" };
                                format!("  {number:>5} (uid {}) {}{inactive}", c.uid, c.name)
                            })
                            .collect::<Vec<_>>()
                            .join("\n");
                        anyhow::bail!(
                            "Name {pattern:?} matched {n} clubs; re-run with --club <number>:\n{candidates}"
                        );
                    }
                }
            }
            _ => Ok(None),
        }
    }
}
