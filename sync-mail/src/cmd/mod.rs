use crate::{Result, settings::Settings};

pub mod create;
pub mod delete;
pub mod fields;
pub mod interests;
pub mod list;
pub mod migrate;
pub mod run;
pub mod update;

pub fn print_json<T: ?Sized + serde::Serialize>(value: &T) -> Result {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[clap(subcommand)]
    cmd: SyncCmd,
}

impl Cmd {
    pub async fn run(&self, settings: Settings) -> Result {
        self.cmd.run(settings).await
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum SyncCmd {
    List(list::Cmd),
    Create(create::Cmd),
    Update(update::Cmd),
    Delete(delete::Cmd),
    Fields(fields::Cmd),
    Interests(interests::Cmd),
    Run(run::Cmd),
    Migrate(migrate::Cmd),
}

impl SyncCmd {
    async fn run(&self, settings: Settings) -> Result {
        match self {
            Self::List(cmd) => cmd.run(settings).await,
            Self::Create(cmd) => cmd.run(settings).await,
            Self::Update(cmd) => cmd.run(settings).await,
            Self::Delete(cmd) => cmd.run(settings).await,
            Self::Fields(cmd) => cmd.run(settings).await,
            Self::Interests(cmd) => cmd.run(settings).await,
            Self::Run(cmd) => cmd.run(settings).await,
            Self::Migrate(cmd) => cmd.run(settings).await,
        }
    }
}
