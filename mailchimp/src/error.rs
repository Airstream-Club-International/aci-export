use thiserror::Error;
pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("malformed api key")]
    MalformedAPIKey,
    #[error("malformed url")]
    MalformedUrl(#[from] url::ParseError),
    #[error("request error")]
    Request(#[from] reqwest::Error),
    #[error("client connections closed")]
    Connections(#[from] tokio::sync::AcquireError),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Mailchimp(Box<MailchimError>),
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
    /// Per-field reasons MailChimp attaches to a 400 whose `detail` only
    /// points at them ("see the 'errors' array"). Absent on most errors.
    #[serde(default)]
    pub errors: Vec<FieldError>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct FieldError {
    #[serde(default)]
    pub field: String,
    pub message: String,
}

impl std::fmt::Display for MailchimError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { status, detail, .. } = self;
        write!(f, "mailchimp error {status}: {detail}")?;
        self.errors
            .iter()
            .try_for_each(|FieldError { field, message }| match field.as_str() {
                "" => write!(f, " [{message}]"),
                field => write!(f, " [{field}: {message}]"),
            })
    }
}

impl Error {
    pub fn value(value: serde_json::Value) -> Self {
        Self::Value(value)
    }

    pub fn number(value: &str) -> Self {
        Self::Number(value.to_string())
    }

    pub fn mailchimp(value: MailchimError) -> Self {
        Self::Mailchimp(Box::new(value))
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

    /// Convert this error into a RetryError based on whether it's retryable
    pub fn into_retry(self) -> tokio_retry2::RetryError<Self> {
        if self.is_retryable() {
            tokio_retry2::RetryError::transient(self)
        } else {
            tokio_retry2::RetryError::permanent(self)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mailchimp(status: u16) -> Error {
        Error::mailchimp(MailchimError {
            status,
            r#type: None,
            title: String::new(),
            detail: String::new(),
            instance: String::new(),
            errors: vec![],
        })
    }

    #[test]
    fn a_validation_error_shows_each_field_reason() {
        let body = r#"{
            "type": "https://mailchimp.com/developer/marketing/docs/errors/",
            "title": "Invalid Resource",
            "status": 400,
            "detail": "The resource submitted could not be validated. For field-specific details, see the 'errors' array.",
            "instance": "2b5c8a3e-0000-0000-0000-000000000000",
            "errors": [
                {"field": "email_address", "message": "This email address is already a list member."},
                {"field": "", "message": "Second reason."}
            ]
        }"#;
        let err = Error::mailchimp(serde_json::from_str(body).expect("parse 400 body"));
        assert_eq!(
            err.to_string(),
            "mailchimp error 400: The resource submitted could not be validated. \
             For field-specific details, see the 'errors' array. \
             [email_address: This email address is already a list member.] [Second reason.]"
        );
    }

    #[test]
    fn an_error_without_field_reasons_reads_as_before() {
        let body =
            r#"{"status":404,"title":"Resource Not Found","detail":"Not here.","instance":""}"#;
        let err = Error::mailchimp(serde_json::from_str(body).expect("parse 404 body"));
        assert_eq!(err.to_string(), "mailchimp error 404: Not here.");
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
}
