use super::{Result, connect_from_env, print_json};
use db::brn::{self, ownership};

#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[command(subcommand)]
    cmd: BrnCmd,
}

impl Cmd {
    pub async fn run(&self) -> Result {
        self.cmd.run().await
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum BrnCmd {
    Email(Email),
    Number(Number),
    History(History),
}

impl BrnCmd {
    pub async fn run(&self) -> Result {
        match self {
            Self::Email(cmd) => cmd.run().await,
            Self::Number(cmd) => cmd.run().await,
            Self::History(cmd) => cmd.run().await,
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct Email {
    pub email: String,
}

impl Email {
    pub async fn run(&self) -> Result {
        let db = connect_from_env().await?;
        let brns = brn::by_email(&db, &self.email).await?;
        print_json(&brns)
    }
}

#[derive(Debug, clap::Args)]
pub struct Number {
    pub number: String,
}

impl Number {
    pub async fn run(&self) -> Result {
        let db = connect_from_env().await?;
        let brn = brn::by_number(&db, &self.number).await?;
        print_json(&brn)
    }
}

/// Past and present holders, which `email` and `number` do not show: a number is
/// reassigned when a member leaves or dies.
#[derive(Debug, clap::Args)]
pub struct History {
    #[command(subcommand)]
    subject: HistorySubject,
}

#[derive(Debug, clap::Subcommand)]
pub enum HistorySubject {
    /// Every recorded tenure of one number
    Number { number: String },
    /// Every number one person has held
    Email { email: String },
}

impl History {
    pub async fn run(&self) -> Result {
        let db = connect_from_env().await?;
        let spans = match &self.subject {
            HistorySubject::Number { number } => ownership::by_number(&db, number).await?,
            HistorySubject::Email { email } => ownership::by_email(&db, email).await?,
        };
        print_json(&spans)
    }
}
