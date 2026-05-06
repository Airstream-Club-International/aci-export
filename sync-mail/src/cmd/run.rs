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
            let mut results = Vec::with_capacity(jobs.len());
            for job in jobs {
                let id = job.id;
                let name = job.name.clone();
                match job.dry_run(settings.ddb.clone()).await {
                    Ok(result) => results.push((id, result)),
                    Err(e) => {
                        tracing::error!(job_id = id, job_name = name, "dry-run failed: {e}");
                    }
                }
            }
            return print_json(&results);
        }

        let map = Job::sync_many(jobs, settings.ddb).await;
        print_json(&map)
    }
}
