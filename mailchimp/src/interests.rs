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

pub async fn categories(client: &Client, list_id: &str) -> Result<Vec<InterestCategory>> {
    let response: CategoriesResponse = client
        .fetch(
            &format!("/3.0/lists/{list_id}/interest-categories"),
            &[("count", "60")],
        )
        .await?;
    Ok(response.categories)
}

pub async fn create_category(
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

pub async fn interests(client: &Client, list_id: &str, category_id: &str) -> Result<Vec<Interest>> {
    let response: InterestsResponse = client
        .fetch(
            &format!("/3.0/lists/{list_id}/interest-categories/{category_id}/interests"),
            &[("count", "60")],
        )
        .await?;
    Ok(response.interests)
}

pub async fn create_interest(
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
    pub fn from_config<S>(source: S) -> Result<Self>
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
    /// creating anything. `None` when the category does not exist yet.
    pub async fn resolve(&self, client: &Client, list_id: &str) -> Result<Option<Resolved>> {
        let Some(category) = categories(client, list_id)
            .await?
            .into_iter()
            .find(|c| c.title == self.category.title)
        else {
            return Ok(None);
        };
        let existing = interests(client, list_id, &category.id).await?;
        let ids = self
            .interests
            .iter()
            .filter_map(|wanted| existing.iter().find(|i| i.name == wanted.name))
            .map(|i| i.id.clone())
            .collect();
        Ok(Some(Resolved {
            category_id: category.id,
            ids,
        }))
    }

    /// Create the category and any missing interests, in configured order.
    /// Existing ones are matched by title and name and left alone.
    ///
    /// Returns the resolved ids and the names of the interests created.
    pub async fn sync(&self, client: &Client, list_id: &str) -> Result<(Resolved, Vec<String>)> {
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

        let existing = interests(client, list_id, &category.id).await?;
        let mut ids = Vec::with_capacity(self.interests.len());
        let mut created = vec![];
        for (order, wanted) in self.interests.iter().enumerate() {
            let interest = match existing.iter().find(|i| i.name == wanted.name) {
                Some(interest) => interest.clone(),
                None => {
                    created.push(wanted.name.clone());
                    create_interest(
                        client,
                        list_id,
                        &category.id,
                        &Interest {
                            name: wanted.name.clone(),
                            display_order: Some(order as u32 + 1),
                            ..Default::default()
                        },
                    )
                    .await?
                }
            };
            ids.push(interest.id);
        }
        Ok((
            Resolved {
                category_id: category.id,
                ids,
            },
            created,
        ))
    }
}

/// Interest ids for a configured category as they exist on one audience.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Resolved {
    pub category_id: String,
    pub ids: Vec<String>,
}

impl Resolved {
    /// Every configured interest switched on: the default for a member new
    /// to the audience.
    pub fn all_on(&self) -> HashMap<String, bool> {
        self.ids.iter().map(|id| (id.clone(), true)).collect()
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

    #[test]
    fn all_on_switches_every_resolved_interest_on() {
        let resolved = Resolved {
            category_id: "cat".into(),
            ids: vec!["a".into(), "b".into()],
        };
        let mut expected = HashMap::new();
        expected.insert("a".to_string(), true);
        expected.insert("b".to_string(), true);
        assert_eq!(resolved.all_on(), expected);
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
