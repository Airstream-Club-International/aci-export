use crate::{Result, cmd::print_json, mailchimp::Job, settings::Settings};

/// Manage the email preference group on the all-members audience
#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(subcommand)]
    cmd: InterestsCmd,
}

impl Cmd {
    pub async fn run(&self, settings: Settings) -> Result {
        self.cmd.run(settings).await
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum InterestsCmd {
    Sync(Sync),
    Seed(Seed),
}

impl InterestsCmd {
    async fn run(&self, settings: Settings) -> Result {
        match self {
            Self::Sync(cmd) => cmd.run(settings).await,
            Self::Seed(cmd) => cmd.run(settings).await,
        }
    }
}

async fn job(settings: &Settings, id: u64) -> Result<Job> {
    let db = settings.mail.db.connect().await?;
    Job::get(&db, id as i64)
        .await?
        .ok_or_else(|| anyhow::anyhow!("sync job not found"))
}

/// Create the preference group and its interests on the audience if missing.
///
/// MailChimp shows a group on the audience's hosted forms as soon as it
/// exists, so run this when the preferences page is ready to go live, and
/// follow it with `seed` so existing members start opted in.
#[derive(Debug, clap::Args)]
pub struct Sync {
    /// The id of the sync job
    id: u64,
}

impl Sync {
    pub async fn run(&self, settings: Settings) -> Result {
        #[derive(Debug, serde::Serialize)]
        struct SyncResult {
            resolved: mailchimp::interests::Resolved,
            created: Vec<String>,
        }
        let job = job(&settings, self.id).await?;
        match job.sync_interests().await? {
            Some((resolved, created)) => print_json(&SyncResult { resolved, created }),
            None => anyhow::bail!("job {} has no preference group configured", job.name),
        }
    }
}

/// Opt every current contact into all interests of the preference group.
///
/// One-time step after `sync` introduces the group. Overwrites any choice a
/// member has already made on the preferences page, so do not run it again
/// once the page is live.
#[derive(Debug, clap::Args)]
pub struct Seed {
    /// The id of the sync job
    id: u64,
}

impl Seed {
    pub async fn run(&self, settings: Settings) -> Result {
        #[derive(Debug, serde::Serialize)]
        struct SeedResult {
            seeded: usize,
        }
        let job = job(&settings, self.id).await?;
        match job.seed_interests().await? {
            Some(seeded) => print_json(&SeedResult { seeded }),
            None => anyhow::bail!(
                "job {} has no preference group configured, or it is not on the audience yet (run `interests sync` first)",
                job.name
            ),
        }
    }
}
