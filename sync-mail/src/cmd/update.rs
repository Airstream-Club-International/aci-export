use crate::{Result, cmd::print_json, mailchimp::Job, mailchimp::JobUpdate, settings::Settings};

/// Update a sync job
#[derive(Debug, clap::Args)]
pub struct Cmd {
    /// The id of the job to update
    id: u64,
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    club: Option<i64>,
    #[arg(long)]
    region: Option<i32>,
    #[arg(long)]
    api_key: Option<String>,
    #[arg(long)]
    list: Option<String>,
    /// Bundled interest config the audience carries (for example "aci")
    #[arg(long)]
    interests: Option<String>,
}

impl From<&Cmd> for JobUpdate {
    fn from(value: &Cmd) -> Self {
        Self {
            id: value.id as i64,
            name: value.name.clone(),
            club: value.club,
            region: value.region,
            api_key: value.api_key.clone(),
            list: value.list.clone(),
            interests: value.interests.clone(),
        }
    }
}

impl Cmd {
    pub async fn run(&self, settings: Settings) -> Result {
        let update = JobUpdate::from(self);
        let db = settings.mail.db.connect().await?;
        let job = Job::update(&db, &update).await?;
        print_json(&job)
    }
}
