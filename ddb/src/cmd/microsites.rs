//! Microsite sync commands.

use super::{connect_from_env, print_json, Result};
use aci_ddb::microsites::{self, ClubMicrosite, MicrositePage};

#[derive(Debug, clap::Args)]
pub struct Cmd {
    #[command(subcommand)]
    cmd: MicrositeCommand,
}

impl Cmd {
    pub async fn run(&self) -> Result {
        self.cmd.run().await
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum MicrositeCommand {
    /// List all clubs with microsites
    List(ListCmd),
    /// Show pages for a specific club
    Pages(PagesCmd),
}

impl MicrositeCommand {
    pub async fn run(&self) -> Result {
        match self {
            Self::List(cmd) => cmd.run().await,
            Self::Pages(cmd) => cmd.run().await,
        }
    }
}

/// List all clubs with microsites
#[derive(Debug, clap::Args)]
pub struct ListCmd;

impl ListCmd {
    pub async fn run(&self) -> Result {
        let pool = connect_from_env().await?;
        let clubs: Vec<ClubMicrosite> = microsites::clubs_with_microsites(&pool).await?;

        #[derive(serde::Serialize)]
        struct ClubInfo {
            club_nid: u64,
            club_number: Option<i64>,
            club_name: String,
            homepage_nid: u64,
            is_intraclub: bool,
        }

        let output: Vec<_> = clubs
            .into_iter()
            .map(|c| ClubInfo {
                club_nid: c.club_nid,
                club_number: c.club_number,
                club_name: c.club_name,
                homepage_nid: c.homepage_nid,
                is_intraclub: c.is_intraclub,
            })
            .collect();

        print_json(&output)
    }
}

/// Show pages for a specific club microsite
#[derive(Debug, clap::Args)]
pub struct PagesCmd {
    /// Club number to fetch pages for (regular clubs)
    #[arg(short, long, group = "selector")]
    club: Option<i64>,

    /// Club node ID to fetch pages for (intraclubs or by nid)
    #[arg(long, group = "selector")]
    nid: Option<u64>,
}

impl PagesCmd {
    pub async fn run(&self) -> Result {
        let pool = connect_from_env().await?;

        // Find the club's homepage
        let clubs: Vec<ClubMicrosite> = microsites::clubs_with_microsites(&pool).await?;
        let club = if let Some(club_num) = self.club {
            clubs
                .into_iter()
                .find(|c| c.club_number == Some(club_num))
                .ok_or_else(|| anyhow::anyhow!("Club {} not found or has no microsite", club_num))?
        } else if let Some(nid) = self.nid {
            clubs
                .into_iter()
                .find(|c| c.club_nid == nid)
                .ok_or_else(|| anyhow::anyhow!("Club nid {} not found or has no microsite", nid))?
        } else {
            anyhow::bail!("Either --club or --nid is required")
        };

        // Fetch pages
        let pages: Vec<MicrositePage> = microsites::pages_for_club(&pool, club.homepage_nid).await?;

        #[derive(serde::Serialize)]
        struct PageInfo {
            nid: u64,
            title: String,
            status: bool,
            menu_title: Option<String>,
            menu_weight: Option<i32>,
            body_length: usize,
            media_urls: Vec<String>,
        }

        let output: Vec<_> = pages
            .into_iter()
            .map(|p| {
                let media_urls = microsites::extract_media_urls(&p.body_html);
                PageInfo {
                    nid: p.nid,
                    title: p.title,
                    status: p.status,
                    menu_title: p.menu_title,
                    menu_weight: p.menu_weight,
                    body_length: p.body_html.len(),
                    media_urls,
                }
            })
            .collect();

        print_json(&output)
    }
}
