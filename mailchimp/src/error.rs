use thiserror::Error;
pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("malformed api key")]
    MalformedAPIKey,
    #[error("malformed url")]
    MalformedUrl(#[from] url::ParseError),
    #[error("request error: {0}")]
    Request(#[from] reqwest::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("mailchimp error {}: {}", .0.status, .0.detail)]
    Mailchimp(MailchimError),
    #[error("unexpected value: {0}")]
    Value(serde_json::Value),
    #[error("unexpected or invalid number: {0}")]
    Number(String),
    #[error("invalid merge type: {0}")]
    InvalidMergeType(String),
    #[error("invalid merge field: {0}")]
    InvalidMergeField(String),
    #[error("config: {0}")]
    Config(#[from] config::ConfigError),
    #[error("batch {batch_id} had {errored} of {total} operations fail")]
    BatchPartialFailure {
        batch_id: String,
        errored: u16,
        total: u16,
    },
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct MailchimError {
    pub status: u16,
    pub r#type: Option<String>,
    pub title: String,
    pub detail: String,
    pub instance: String,
}

impl Error {
    pub fn value(value: serde_json::Value) -> Self {
        Self::Value(value)
    }

    pub fn number(value: &str) -> Self {
        Self::Number(value.to_string())
    }

    pub fn mailchimp(value: MailchimError) -> Self {
        Self::Mailchimp(value)
    }

    pub fn merge_field<S: ToString>(msg: S) -> Self {
        Self::InvalidMergeField(msg.to_string())
    }

    /// Returns true if this error is transient and the operation should be retried
    pub fn is_retryable(&self) -> bool {
        match self {
            // 401 Unauthorized - invalid API key, don't retry
            // 403 Forbidden - don't retry
            // 404 Not Found - don't retry
            Self::Mailchimp(err) if matches!(err.status, 401 | 403 | 404) => false,
            // Malformed API key - don't retry
            Self::MalformedAPIKey => false,
            // Other mailchimp errors (rate limits, server errors) - retry
            Self::Mailchimp(_) => true,
            // Request errors (network issues, timeouts) - retry
            Self::Request(_) => true,
            // All other errors - don't retry
            _ => false,
        }
    }

    /// Convert this error into a RetryError based on whether it's retryable
    pub fn into_retry(self) -> tokio_retry2::RetryError<Self> {
        if self.is_retryable() {
            tokio_retry2::RetryError::transient(self)
        } else {
            tokio_retry2::RetryError::permanent(self)
        }
    }
}
