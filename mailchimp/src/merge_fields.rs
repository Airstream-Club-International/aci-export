use crate::{
    Client, NO_QUERY, Result, Stream, deserialize_null_string, error::Error, paged_query_impl,
    paged_response_impl, query_default_impl, read_config,
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

pub fn all(client: &Client, list_id: &str, query: MergeFieldsQuery) -> Stream<MergeField> {
    client.fetch_stream::<MergeFieldsQuery, MergeFieldsResponse>(
        &format!("/3.0/lists/{list_id}/merge-fields"),
        query,
    )
}

pub async fn get(client: &Client, list_id: &str, merge_id: u32) -> Result<MergeField> {
    client
        .fetch(
            &format!("/3.0/lists/{list_id}/merge-fields/{merge_id}"),
            NO_QUERY,
        )
        .await
}

pub async fn create(client: &Client, list_id: &str, field: MergeField) -> Result<MergeField> {
    client
        .post(&format!("/3.0/lists/{list_id}/merge-fields"), &field)
        .await
}

pub async fn delete(client: &Client, list_id: &str, merge_id: &str) -> Result<()> {
    client
        .delete(&format!("/3.0/lists/{list_id}/merge-fields/{merge_id}"))
        .await
}

pub async fn update(
    client: &Client,
    list_id: &str,
    merge_id: &str,
    field: MergeField,
) -> Result<MergeField> {
    client
        .patch(
            &format!("/3.0/lists/{list_id}/merge-fields/{merge_id}",),
            &field,
        )
        .await
}

pub async fn sync(
    client: &Client,
    list_id: &str,
    target: MergeFields,
    process_deletes: bool,
) -> Result<(Vec<String>, Vec<String>, Vec<String>)> {
    type TaggedMergeField = (String, MergeField);

    let current: MergeFields = all(client, list_id, Default::default())
        .try_collect()
        .await?;

    fn collect_tags(fields: &[TaggedMergeField]) -> Vec<String> {
        fields
            .iter()
            .map(|(_, field)| field.tag.clone())
            .collect::<Vec<_>>()
    }

    let (to_delete, _): (Vec<TaggedMergeField>, Vec<TaggedMergeField>) = current
        .clone()
        .into_iter()
        .partition(|(key, _)| !target.contains_key(key));

    let mut to_add: Vec<TaggedMergeField> = vec![];
    let mut to_compare = vec![];
    for (key, field) in target {
        match current.get(&key) {
            Some(existing) => to_compare.push((existing, field)),
            None => to_add.push((key, field)),
        }
    }

    let deleted = collect_tags(&to_delete);
    if process_deletes {
        for (_, field) in to_delete {
            delete(client, list_id, &field.merge_id.to_string()).await?;
        }
    }

    let added = collect_tags(&to_add);
    for (_, field) in to_add {
        create(client, list_id, field).await?;
    }

    let mut updated = vec![];
    for (current, mut field) in to_compare {
        field.merge_id = current.merge_id;
        if field != *current {
            updated.push(field.tag.clone());
            update(client, list_id, &current.merge_id.to_string(), field).await?;
        }
    }
    Ok((added, deleted, updated))
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct MergeField {
    #[serde(
        default,
        skip_serializing_if = "crate::is_default",
        deserialize_with = "crate::deserialize_null_i32::deserialize"
    )]
    pub merge_id: i32,
    #[serde(
        default,
        skip_serializing_if = "String::is_empty",
        deserialize_with = "deserialize_null_string::deserialize"
    )]
    pub tag: String,
    #[serde(
        default,
        skip_serializing_if = "String::is_empty",
        deserialize_with = "deserialize_null_string::deserialize"
    )]
    pub name: String,

    #[serde(default)]
    pub r#type: MergeType,
    /// Whether the field is shown on the audience's hosted forms (signup and
    /// update-profile). Hidden by default: every synced field is owned by the
    /// membership database, and a value a member edits on a MailChimp form is
    /// overwritten on the next sync run.
    #[serde(default)]
    pub public: bool,
}

impl Default for MergeField {
    fn default() -> Self {
        Self {
            merge_id: 0,
            tag: "".to_string(),
            name: "".to_string(),
            r#type: MergeType::default(),
            public: false,
        }
    }
}

#[derive(PartialEq, Serialize, Deserialize, Eq, Debug, Clone, Default)]
#[serde(rename_all = "lowercase")]
pub enum MergeType {
    #[default]
    Text,
    Number,
    Address,
    Phone,
    Date,
    Url,
    ImageUrl,
    Radio,
    Dropdown,
    Birthday,
    Zip,
}

impl std::fmt::Display for MergeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = serde_json::to_string(self).map_err(|_| std::fmt::Error)?;
        f.write_str(&str)
    }
}

impl std::str::FromStr for MergeType {
    type Err = Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        serde_json::from_str(&format!("\"{s}\""))
            .map_err(|_| Error::InvalidMergeType(s.to_string()))
    }
}

#[derive(Debug, Default, Clone)]
pub struct MergeFields(HashMap<String, MergeField>);

impl MergeFields {
    pub fn from_config<S>(source: S) -> Result<Self>
    where
        S: config::Source + Send + Sync + 'static,
    {
        #[derive(Debug, serde::Deserialize, serde::Serialize)]
        struct MergeFieldsConfig {
            merge_fields: Vec<MergeField>,
        }
        impl TryFrom<MergeFieldsConfig> for MergeFields {
            type Error = Error;
            fn try_from(config: MergeFieldsConfig) -> Result<Self> {
                for field in config.merge_fields.iter() {
                    if field.tag.len() > 10 {
                        return Err(Error::merge_field(format!("tag too long: {}", field.tag)));
                    }
                }
                Ok(config.merge_fields.into_iter().collect())
            }
        }
        read_config::<MergeFieldsConfig, S>(source).and_then(TryInto::try_into)
    }

    /// Load merge fields configuration for club-specific syncs
    pub fn club() -> Result<Self> {
        let str = include_str!("../data/fields-club.toml");
        Self::from_config(config::File::from_str(str, config::FileFormat::Toml))
    }

    /// Load merge fields configuration for all members syncs
    pub fn all() -> Result<Self> {
        let str = include_str!("../data/fields-all.toml");
        Self::from_config(config::File::from_str(str, config::FileFormat::Toml))
    }
}

impl<'de> Deserialize<'de> for MergeFields {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        HashMap::<String, MergeField>::deserialize(deserializer).map(Self)
    }
}

impl Serialize for MergeFields {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl std::ops::Deref for MergeFields {
    type Target = HashMap<String, MergeField>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for MergeFields {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FromIterator<MergeField> for MergeFields {
    fn from_iter<T: IntoIterator<Item = MergeField>>(iter: T) -> Self {
        Self(
            iter.into_iter()
                .map(|field| (field.tag.clone(), field))
                .collect(),
        )
    }
}

impl Extend<MergeField> for MergeFields {
    fn extend<T: IntoIterator<Item = MergeField>>(&mut self, iter: T) {
        let iter = iter.into_iter().map(|field| (field.tag.clone(), field));
        self.0.extend(iter)
    }
}

impl IntoIterator for MergeFields {
    type Item = (String, MergeField);
    type IntoIter = std::collections::hash_map::IntoIter<String, MergeField>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl From<HashMap<String, MergeField>> for MergeFields {
    fn from(value: HashMap<String, MergeField>) -> Self {
        Self(value)
    }
}

pub type MergeFieldValue = (String, Value);

impl MergeFields {
    pub fn to_value<F>(&self, tag: &str, value: F) -> Result<Option<MergeFieldValue>>
    where
        F: ToMergeFieldValue + std::fmt::Debug,
    {
        match self.get(tag) {
            Some(field) => value.to_merge_field_value(field),
            None => Ok(None),
        }
    }
}

pub trait ToMergeFieldValue {
    fn to_merge_field_value(self, field: &MergeField) -> Result<Option<MergeFieldValue>>
    where
        Self: Sized;
}

impl ToMergeFieldValue for chrono::NaiveDate {
    fn to_merge_field_value(self, field: &MergeField) -> Result<Option<MergeFieldValue>> {
        match field.r#type {
            MergeType::Date => Ok(Some((
                field.tag.clone(),
                self.format("%Y-%m-%d").to_string().into(),
            ))),
            MergeType::Birthday => Ok(Some((
                field.tag.clone(),
                self.format("%m/%d").to_string().into(),
            ))),
            _ => Err(Error::InvalidMergeType(field.r#type.to_string())),
        }
    }
}

impl ToMergeFieldValue for u64 {
    fn to_merge_field_value(self, field: &MergeField) -> Result<Option<MergeFieldValue>> {
        match field.r#type {
            MergeType::Number => Ok(Some((field.tag.clone(), Value::from(self)))),
            MergeType::Text => Ok(Some((field.tag.clone(), Value::from(self.to_string())))),
            _ => Err(Error::InvalidMergeType(field.r#type.to_string())),
        }
    }
}

impl ToMergeFieldValue for i64 {
    fn to_merge_field_value(self, field: &MergeField) -> Result<Option<MergeFieldValue>> {
        match field.r#type {
            MergeType::Number => Ok(Some((field.tag.clone(), Value::from(self)))),
            MergeType::Text => Ok(Some((field.tag.clone(), Value::from(self.to_string())))),
            _ => Err(Error::InvalidMergeType(field.r#type.to_string())),
        }
    }
}

impl ToMergeFieldValue for &str {
    fn to_merge_field_value(self, field: &MergeField) -> Result<Option<MergeFieldValue>> {
        match field.r#type {
            MergeType::Text => Ok(Some((field.tag.clone(), self.to_string().into()))),
            _ => Err(Error::InvalidMergeType(field.r#type.to_string())),
        }
    }
}

impl ToMergeFieldValue for &String {
    fn to_merge_field_value(self, field: &MergeField) -> Result<Option<MergeFieldValue>> {
        self.as_str().to_merge_field_value(field)
    }
}

impl<T> ToMergeFieldValue for Option<T>
where
    T: ToMergeFieldValue,
{
    fn to_merge_field_value(self, field: &MergeField) -> Result<Option<MergeFieldValue>> {
        match self {
            Some(value) => value.to_merge_field_value(field),
            None => Ok(None),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MergeFieldsQuery {
    pub fields: String,
    pub count: usize,
    pub offset: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MergeFieldsResponse {
    pub merge_fields: Vec<MergeField>,
}

query_default_impl!(MergeFieldsQuery);
paged_query_impl!(
    MergeFieldsQuery,
    &[
        "merge_fields.merge_id",
        "merge_fields.tag",
        "merge_fields.name",
        "merge_fields.type",
        "merge_fields.public"
    ]
);
paged_response_impl!(MergeFieldsResponse, merge_fields, MergeField);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_fetches_the_public_flag() {
        // sync() compares the fetched field against config; a fetch that
        // omits `public` would read every field as hidden and never fix one.
        assert!(
            MergeFieldsQuery::default()
                .fields
                .split(',')
                .any(|f| f == "merge_fields.public")
        );
    }

    #[test]
    fn config_field_defaults_to_hidden() {
        assert!(!MergeField::default().public);
        let fields = MergeFields::from_config(config::File::from_str(
            r#"
            [[merge_fields]]
            tag = "FNAME"
            name = "First Name"
            type = "text"
            "#,
            config::FileFormat::Toml,
        ))
        .expect("parse config");
        assert!(!fields.get("FNAME").expect("FNAME present").public);
    }

    #[test]
    fn api_public_flag_is_compared() {
        // The shape MailChimp returns for a UI-created field; `public` must
        // survive deserialization so the sync sees it differ from config.
        let fixture = r#"{"merge_id":1,"tag":"FNAME","name":"First Name","type":"text","required":false,"public":true}"#;
        let current: MergeField = serde_json::from_str(fixture).expect("parse fixture");
        assert!(current.public);
        let target = MergeField {
            merge_id: 1,
            tag: "FNAME".into(),
            name: "First Name".into(),
            r#type: MergeType::Text,
            public: false,
        };
        assert_ne!(target, current);
    }
}
