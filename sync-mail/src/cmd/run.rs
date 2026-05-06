use crate::{Result, cmd::print_json, mailchimp::Job, settings::Settings};

/// Sync the given club (or all) mailing list from the membership database
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// The id of the mailing list to sync
    id: Option<u64>,
    /// Compute what would be archived without making any changes in MailChimp
    #[arg(long)]
    dry_run: bool,
}

impl Cmd {
    pub async fn run(&self, settings: Settings) -> Result {
        let db = settings.mail.db.connect().await?;
        let jobs = if let Some(id) = self.id {
            let job = Job::get(&db, id as i64)
                .await?
                .ok_or_else(|| anyhow::anyhow!("sync job not found"))?;
            vec![job]
        } else {
            Job::all(&db).await?
        };

        if self.dry_run {
            let map = Job::dry_run_many(jobs, settings.ddb).await;
            return print_json(&map);
        }

        let map = Job::sync_many(jobs, settings.ddb).await;
        print_json(&map)
    }
}
