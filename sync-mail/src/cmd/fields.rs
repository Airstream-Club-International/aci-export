use crate::{
    Result,
    cmd::print_json,
    mailchimp::{JOB_CONCURRENCY, Job},
    settings::Settings,
};
use futures::{StreamExt, TryFutureExt, TryStreamExt};

/// Sync the mailing list merge fields for a given club (or all) to mailchimp
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// The id of the mailing list to sync
    id: Option<u64>,
    /// Delete user added merge fields
    #[arg(long)]
    process_deletes: bool,
}

impl Cmd {
    pub async fn run(&self, settings: Settings) -> Result {
        #[derive(Debug, serde::Serialize)]
        struct JobResult {
            name: String,
            deleted: Vec<String>,
            added: Vec<String>,
            updated: Vec<String>,
        }
        let db = settings.mail.db.connect().await?;
        let jobs = if let Some(id) = self.id {
            let job = Job::get(&db, id as i64)
                .await?
                .ok_or_else(|| anyhow::anyhow!("sync job not found"))?;
            vec![job]
        } else {
            Job::all(&db).await?
        };
        let results = futures::stream::iter(jobs)
            .map(|job| async move {
                let name = job.name.clone();
                job.sync_merge_fields(self.process_deletes)
                    .map_ok(|(added, deleted, updated)| {
                        (
                            job.id,
                            JobResult {
                                name,
                                deleted: deleted.clone(),
                                added: added.clone(),
                                updated: updated.clone(),
                            },
                        )
                    })
                    .await
            })
            .buffered(JOB_CONCURRENCY)
            .try_collect::<Vec<(i64, JobResult)>>()
            .await?;
        let map: std::collections::HashMap<i64, JobResult> = results.into_iter().collect();
        print_json(&map)
    }
}
