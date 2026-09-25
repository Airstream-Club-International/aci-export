use std::time::Duration;
use thiserror::Error;
use tokio_retry2::RetryError;
pub type Result<T = ()> = std::result::Result<T, Error>;

/// How long a request refused with a 429 waits before its next attempt.
const RATE_LIMITED_BACKOFF: Duration = Duration::from_secs(10);

#[derive(Error, Debug)]
pub enum Error {
    #[error("malformed api key")]
    MalformedAPIKey,
    #[error("malformed url")]
    MalformedUrl(#[from] url::ParseError),
    #[error("request error: {0}")]
    Request(#[from] reqwest::Error),
    #[error("client connections closed")]
    Connections(#[from] tokio::sync::AcquireError),
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
    #[error("interest not on audience: {0}")]
    MissingInterest(String),
    #[error("no bundled interest config named {0:?}")]
    UnknownInterests(String),
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
            // 400 Bad Request - MailChimp refused the payload, resending it
            // changes nothing (invalid or already-taken email, bad field)
            // 401 Unauthorized - invalid API key, don't retry
            // 403 Forbidden - don't retry
            // 404 Not Found - don't retry
            Self::Mailchimp(err) if matches!(err.status, 400 | 401 | 403 | 404) => false,
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

    /// Convert this error into a RetryError based on whether it's retryable.
    /// A 429 waits `RATE_LIMITED_BACKOFF` in place of the policy's delay: the
    /// client stays under the connection limit on its own, so a 429 means
    /// something else on the account holds connections, and that outlasts a
    /// sub-second retry.
    pub fn into_retry(self) -> RetryError<Self> {
        match &self {
            Self::Mailchimp(err) if err.status == 429 => {
                RetryError::retry_after(self, RATE_LIMITED_BACKOFF)
            }
            _ if self.is_retryable() => RetryError::transient(self),
            _ => RetryError::permanent(self),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mailchimp(status: u16) -> Error {
        Error::Mailchimp(MailchimError {
            status,
            r#type: None,
            title: String::new(),
            detail: String::new(),
            instance: String::new(),
        })
    }

    #[test]
    fn client_rejections_are_permanent_and_the_rest_retry() {
        for status in [400, 401, 403, 404] {
            assert!(!mailchimp(status).is_retryable(), "{status}");
        }
        for status in [429, 500, 503] {
            assert!(mailchimp(status).is_retryable(), "{status}");
        }
    }

    #[test]
    fn a_429_waits_seconds_and_other_retries_keep_the_policy_delay() {
        match mailchimp(429).into_retry() {
            RetryError::Transient { retry_after, .. } => {
                assert_eq!(retry_after, Some(Duration::from_secs(10)));
            }
            other => panic!("429 should retry: {other:?}"),
        }
        match mailchimp(500).into_retry() {
            RetryError::Transient { retry_after, .. } => assert_eq!(retry_after, None),
            other => panic!("500 should retry: {other:?}"),
        }
        assert!(matches!(
            mailchimp(404).into_retry(),
            RetryError::Permanent(_)
        ));
    }
}
