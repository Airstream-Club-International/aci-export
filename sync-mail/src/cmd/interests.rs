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

/// Bring the preference group on the audience in line with the config.
///
/// Creates the group and any missing interests, and renames interests whose
/// configured `was` names match. Interests the config does not name are
/// reported as `extra` and left alone unless `--process-deletes` is given.
///
/// MailChimp shows a group on the audience's hosted forms as soon as it
/// exists, so the first run is a launch step; follow it with `seed` so
/// existing members start opted in.
#[derive(Debug, clap::Args)]
pub struct Sync {
    /// The id of the sync job
    id: u64,
    /// Delete interests the config does not name, and every member's
    /// setting for them
    #[arg(long)]
    process_deletes: bool,
}

impl Sync {
    pub async fn run(&self, settings: Settings) -> Result {
        let job = job(&settings, self.id).await?;
        match job.sync_interests(self.process_deletes).await? {
            Some(synced) => print_json(&synced),
            None => anyhow::bail!("job {} has no preference group configured", job.name),
        }
    }
}

/// Opt every current contact into interests of the preference group.
///
/// With no `--interest`, every configured interest is switched on: the
/// one-time step after `sync` introduces the group. It overwrites any
/// choice a member has already made on the preferences page, so do not
/// run that form again once the page is live. With `--interest`, only the
/// named interests are switched on and the rest are left as they are: how
/// an interest added later reaches existing members.
#[derive(Debug, clap::Args)]
pub struct Seed {
    /// The id of the sync job
    id: u64,
    /// Switch on only this interest, by its configured name (repeatable)
    #[arg(long = "interest")]
    only: Vec<String>,
}

impl Seed {
    pub async fn run(&self, settings: Settings) -> Result {
        #[derive(Debug, serde::Serialize)]
        struct SeedResult {
            seeded: usize,
        }
        let job = job(&settings, self.id).await?;
        match job.seed_interests(&self.only).await? {
            Some(seeded) => print_json(&SeedResult { seeded }),
            None => anyhow::bail!(
                "job {} has no preference group configured, or it is not on the audience yet (run `interests sync` first)",
                job.name
            ),
        }
    }
}
