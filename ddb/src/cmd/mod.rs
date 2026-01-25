pub type Result<T = ()> = anyhow::Result<T>;

use anyhow::Context;
use sqlx::{Executor, MySqlPool};

pub async fn connect_from_env() -> Result<MySqlPool> {
    let url = std::env::var("ACI__DDB_URL").context("DDB_DB_URL environment variable not set")?;
    let pool = MySqlPool::connect(&url).await.context("opening database")?;
    let _ = pool
        .execute(
            r#"
            SET GLOBAL table_definition_cache = 4096;
            SET GLOBAL table_open_cache = 4096;
        "#,
        )
        .await
        .context("preparing database caches")?;
    Ok(pool)
}

pub mod clubs;
pub mod international;
pub mod members;
pub mod microsites;
pub mod regions;
pub mod standing_committees;
pub mod users;

pub fn print_json<T: ?Sized + serde::Serialize>(value: &T) -> Result {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[command(subcommand)]
    cmd: DdbCommand,
}

impl Cmd {
    pub async fn run(&self) -> Result {
        self.cmd.run().await
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum DdbCommand {
    Users(users::Cmd),
    Members(members::Cmd),
    Clubs(clubs::Cmd),
    Regions(regions::Cmd),
    StandingCommittees(standing_committees::Cmd),
    International(international::Cmd),
    Microsites(microsites::Cmd),
}

impl DdbCommand {
    pub async fn run(&self) -> Result {
        match self {
            Self::Users(cmd) => cmd.run().await,
            Self::Members(cmd) => cmd.run().await,
            Self::Clubs(cmd) => cmd.run().await,
            Self::Regions(cmd) => cmd.run().await,
            Self::StandingCommittees(cmd) => cmd.run().await,
            Self::International(cmd) => cmd.run().await,
            Self::Microsites(cmd) => cmd.run().await,
        }
    }
}
