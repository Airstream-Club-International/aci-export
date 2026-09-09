//! Groups: an interest category on an audience and the interests inside it.
//! MailChimp shows a category on the audience's hosted forms as soon as it
//! exists, so creating one is a member-visible change.

use crate::{Client, Error, Result, RetryPolicy, batches, deserialize_null_string, read_config};
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio_retry2::Retry;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Default)]
pub struct InterestCategory {
    #[serde(
        default,
        skip_serializing_if = "String::is_empty",
        deserialize_with = "deserialize_null_string::deserialize"
    )]
    pub id: String,
    pub title: String,
    pub r#type: CategoryType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_order: Option<u32>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Default)]
#[serde(rename_all = "lowercase")]
pub enum CategoryType {
    #[default]
    Checkboxes,
    Dropdown,
    Radio,
    Hidden,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Default)]
pub struct Interest {
    #[serde(
        default,
        skip_serializing_if = "String::is_empty",
        deserialize_with = "deserialize_null_string::deserialize"
    )]
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_order: Option<u32>,
}

#[derive(Deserialize, Debug)]
struct CategoriesResponse {
    categories: Vec<InterestCategory>,
}

#[derive(Deserialize, Debug)]
struct InterestsResponse {
    interests: Vec<Interest>,
}

async fn categories(client: &Client, list_id: &str) -> Result<Vec<InterestCategory>> {
    let response: CategoriesResponse = client
        .fetch(
            &format!("/3.0/lists/{list_id}/interest-categories"),
            &[("count", "60")],
        )
        .await?;
    Ok(response.categories)
}

async fn create_category(
    client: &Client,
    list_id: &str,
    category: &InterestCategory,
) -> Result<InterestCategory> {
    client
        .post(
            &format!("/3.0/lists/{list_id}/interest-categories"),
            category,
        )
        .await
}

async fn interests(client: &Client, list_id: &str, category_id: &str) -> Result<Vec<Interest>> {
    let response: InterestsResponse = client
        .fetch(
            &format!("/3.0/lists/{list_id}/interest-categories/{category_id}/interests"),
            &[("count", "60")],
        )
        .await?;
    Ok(response.interests)
}

async fn create_interest(
    client: &Client,
    list_id: &str,
    category_id: &str,
    interest: &Interest,
) -> Result<Interest> {
    client
        .post(
            &format!("/3.0/lists/{list_id}/interest-categories/{category_id}/interests"),
            interest,
        )
        .await
}

/// The configured shape of one category and its interests.
#[derive(Deserialize, Debug, Clone)]
pub struct Interests {
    pub category: CategoryConfig,
    pub interests: Vec<InterestConfig>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct CategoryConfig {
    pub title: String,
    #[serde(default)]
    pub r#type: CategoryType,
}

#[derive(Deserialize, Debug, Clone)]
pub struct InterestConfig {
    pub name: String,
}

impl Interests {
    fn from_config<S>(source: S) -> Result<Self>
    where
        S: config::Source + Send + Sync + 'static,
    {
        read_config(source)
    }

    /// The email preference group for the all-members audience.
    pub fn all() -> Result<Self> {
        let str = include_str!("../data/interests-all.toml");
        Self::from_config(config::File::from_str(str, config::FileFormat::Toml))
    }

    /// Look up the configured category and interests on the audience without
    /// creating anything. `None` when the category does not exist yet; an
    /// error when the category exists but any configured interest is
    /// missing from it, since defaulting members into a partial group would
    /// silently leave them out of the rest.
    pub async fn resolve(&self, client: &Client, list_id: &str) -> Result<Option<Resolved>> {
        let Some(category) = categories(client, list_id)
            .await?
            .into_iter()
            .find(|c| c.title == self.category.title)
        else {
            return Ok(None);
        };
        let existing = interests(client, list_id, &category.id).await?;
        Ok(Some(self.resolved_in(category.id, &existing)?))
    }

    /// Match each configured interest, in configured order, to the
    /// interests that exist on the audience. Errors on the first configured
    /// interest that is not there.
    fn resolved_in(&self, category_id: String, existing: &[Interest]) -> Result<Resolved> {
        let interests = self
            .interests
            .iter()
            .map(|wanted| {
                existing
                    .iter()
                    .find(|i| i.name == wanted.name)
                    .map(|i| ResolvedInterest {
                        name: i.name.clone(),
                        id: i.id.clone(),
                    })
                    .ok_or_else(|| Error::MissingInterest(wanted.name.clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Resolved {
            category_id,
            interests,
        })
    }

    /// Interests on the audience that the config does not name. They are
    /// never created or removed here; reporting them is how a rename made
    /// in the MailChimp UI, or an interest dropped from config, gets seen.
    fn extra_in(&self, existing: &[Interest]) -> Vec<String> {
        existing
            .iter()
            .filter(|i| !self.interests.iter().any(|wanted| wanted.name == i.name))
            .map(|i| i.name.clone())
            .collect()
    }

    /// Create the category and any missing interests, in configured order.
    /// Existing ones are matched by title and name and left alone.
    pub async fn sync(&self, client: &Client, list_id: &str) -> Result<Synced> {
        let category = match categories(client, list_id)
            .await?
            .into_iter()
            .find(|c| c.title == self.category.title)
        {
            Some(category) => category,
            None => {
                create_category(
                    client,
                    list_id,
                    &InterestCategory {
                        title: self.category.title.clone(),
                        r#type: self.category.r#type.clone(),
                        ..Default::default()
                    },
                )
                .await?
            }
        };

        let mut existing = interests(client, list_id, &category.id).await?;
        let mut created = vec![];
        for (order, wanted) in self.interests.iter().enumerate() {
            if existing.iter().any(|i| i.name == wanted.name) {
                continue;
            }
            created.push(wanted.name.clone());
            let interest = create_interest(
                client,
                list_id,
                &category.id,
                &Interest {
                    id: String::new(),
                    name: wanted.name.clone(),
                    display_order: Some(order as u32 + 1),
                },
            )
            .await?;
            existing.push(interest);
        }
        Ok(Synced {
            resolved: self.resolved_in(category.id, &existing)?,
            created,
            extra: self.extra_in(&existing),
        })
    }
}

/// Outcome of [`Interests::sync`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Synced {
    pub resolved: Resolved,
    /// Names of the interests created by this call
    pub created: Vec<String>,
    /// Names of interests on the audience that the config does not name
    pub extra: Vec<String>,
}

/// A configured category as it exists on one audience.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Resolved {
    pub category_id: String,
    /// In configured order
    pub interests: Vec<ResolvedInterest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResolvedInterest {
    pub name: String,
    pub id: String,
}

impl Resolved {
    /// Every configured interest switched on: the default for a member new
    /// to the audience.
    pub fn all_on(&self) -> HashMap<String, bool> {
        self.interests
            .iter()
            .map(|i| (i.id.clone(), true))
            .collect()
    }

    /// Only the named interests switched on, leaving the rest as they are.
    /// This is how an interest added later is rolled out to existing
    /// members without touching the choices they have already made. Errors
    /// on a name that is not a configured interest.
    pub fn on(&self, names: &[String]) -> Result<HashMap<String, bool>> {
        names
            .iter()
            .map(|name| {
                self.interests
                    .iter()
                    .find(|i| &i.name == name)
                    .map(|i| (i.id.clone(), true))
                    .ok_or_else(|| Error::MissingInterest(name.clone()))
            })
            .collect()
    }
}

fn log_batch_interest_retry(err: &Error, sleep: std::time::Duration) {
    tracing::warn!(%err, sleep = sleep.as_secs(), "batch interest update");
}

/// Set the given interests on every listed contact, in batches. Interests
/// not named in `interests` are left as they are.
pub async fn update_many(
    client: &Client,
    list_id: &str,
    member_ids: &[String],
    interests: &HashMap<String, bool>,
    retries: RetryPolicy,
) -> Result<()> {
    #[derive(Serialize)]
    struct Body<'a> {
        interests: &'a HashMap<String, bool>,
    }
    let body = Body { interests };
    futures::stream::iter(member_ids)
        .chunks(1000)
        .map(Ok::<Vec<_>, Error>)
        .try_for_each_concurrent(10, |member_ids| {
            let client = client.clone();
            let body = &body;
            async move {
                let mut batch = batches::Batch::default();
                for member_id in member_ids {
                    let operation =
                        batch.patch(&format!("/lists/{list_id}/members/{member_id}"), body)?;
                    operation.operation_id = member_id.to_owned();
                }
                let info = Retry::spawn_notify(
                    retries,
                    || batch.run(&client, true).map_err(Error::into_retry),
                    log_batch_interest_retry,
                )
                .await?;
                if info.errored_operations > 0 {
                    return Err(Error::BatchPartialFailure {
                        batch_id: info.id,
                        errored: info.errored_operations,
                        total: info.total_operations,
                    });
                }
                Ok(())
            }
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_members_config_matches_the_board_spec_in_order() {
        let config = Interests::all().expect("parse bundled config");
        assert_eq!(config.category.title, "Email Preferences");
        assert_eq!(config.category.r#type, CategoryType::Checkboxes);
        let names: Vec<&str> = config.interests.iter().map(|i| i.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "General Club Information",
                "Member Benefits/Partners",
                "Caravans",
                "Short-Notice Caravan Go List",
                "International Rally",
                "National Event Rallies",
                "Club Business (Governance)",
            ]
        );
    }

    fn existing_for(config: &Interests) -> Vec<Interest> {
        config
            .interests
            .iter()
            .enumerate()
            .map(|(n, i)| Interest {
                id: format!("id{n}"),
                name: i.name.clone(),
                display_order: None,
            })
            .collect()
    }

    #[test]
    fn resolved_in_requires_every_configured_interest() {
        let config = Interests::all().expect("parse bundled config");
        let mut existing = existing_for(&config);
        let resolved = config
            .resolved_in("cat".into(), &existing)
            .expect("all present");
        assert_eq!(resolved.interests.len(), config.interests.len());
        assert_eq!(resolved.interests[2].id, "id2");
        assert_eq!(resolved.interests[2].name, "Caravans");

        existing.remove(2);
        match config.resolved_in("cat".into(), &existing) {
            Err(Error::MissingInterest(name)) => assert_eq!(name, "Caravans"),
            other => panic!("expected MissingInterest, got {other:?}"),
        }
    }

    #[test]
    fn extra_in_reports_interests_the_config_does_not_name() {
        let config = Interests::all().expect("parse bundled config");
        let mut existing = existing_for(&config);
        assert_eq!(config.extra_in(&existing), Vec::<String>::new());
        existing.push(Interest {
            id: "x".into(),
            name: "Renamed In The UI".into(),
            display_order: None,
        });
        assert_eq!(config.extra_in(&existing), ["Renamed In The UI"]);
    }

    fn resolved() -> Resolved {
        Resolved {
            category_id: "cat".into(),
            interests: vec![
                ResolvedInterest {
                    name: "A".into(),
                    id: "a".into(),
                },
                ResolvedInterest {
                    name: "B".into(),
                    id: "b".into(),
                },
            ],
        }
    }

    #[test]
    fn all_on_switches_every_resolved_interest_on() {
        let mut expected = HashMap::new();
        expected.insert("a".to_string(), true);
        expected.insert("b".to_string(), true);
        assert_eq!(resolved().all_on(), expected);
    }

    #[test]
    fn on_names_only_the_asked_interests_and_rejects_unknown_ones() {
        let mut expected = HashMap::new();
        expected.insert("b".to_string(), true);
        assert_eq!(resolved().on(&["B".to_string()]).expect("known"), expected);
        match resolved().on(&["C".to_string()]) {
            Err(Error::MissingInterest(name)) => assert_eq!(name, "C"),
            other => panic!("expected MissingInterest, got {other:?}"),
        }
    }

    #[test]
    fn category_wire_shape() {
        let fixture = r#"{"list_id":"x","id":"62b157dc7d","title":"Email Preferences","display_order":2,"type":"checkboxes"}"#;
        let category: InterestCategory = serde_json::from_str(fixture).expect("parse");
        assert_eq!(category.id, "62b157dc7d");
        assert_eq!(category.r#type, CategoryType::Checkboxes);
        let create = InterestCategory {
            title: "Email Preferences".into(),
            r#type: CategoryType::Checkboxes,
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(&create).expect("serialize"),
            serde_json::json!({"title":"Email Preferences","type":"checkboxes"})
        );
    }
}
