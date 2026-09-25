use futures::{
    Future as StdFuture, FutureExt, Stream as StdStream, StreamExt, TryFutureExt, future, stream,
};
use reqwest::{
    Method, RequestBuilder, StatusCode, Url,
    header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue},
};
use serde::{Serialize, de::DeserializeOwned};
use std::{fmt::Debug, pin::Pin, str::FromStr, sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tokio_retry2::strategy::jitter;

/// A type alias for `Future` that may return `crate::error::Error`
pub type Future<T> = Pin<Box<dyn StdFuture<Output = Result<T>> + Send>>;

/// A type alias for `Stream` that may result in `crate::error::Error`
pub type Stream<T> = Pin<Box<dyn StdStream<Item = Result<T>> + Send>>;

mod error;

pub mod batches;
pub mod health;
pub mod interests;
pub mod lists;
pub mod members;
pub mod merge_fields;

pub use error::{Error, Result};

/// The default timeout for API requests, in seconds. MailChimp gives up on a
/// call at 120 seconds; giving up sooner leaves it still working on a
/// request we have stopped counting, holding one of the account's
/// connections.
pub const DEFAULT_TIMEOUT: u64 = 120;
/// Requests a client has in flight at once. MailChimp refuses an account's
/// connections beyond 10 with a 429; the margin leaves room for anything
/// else using the account.
const MAX_CONNECTIONS: usize = 8;
/// How long a client waits after a 429 before reporting it, so a retry does
/// not arrive while whatever holds the account's connections still does.
const RATE_LIMITED_BACKOFF: Duration = Duration::from_secs(10);
/// A utility constant to pass an empty query slice to the various client fetch
/// functions
pub const NO_QUERY: &[&str; 0] = &[""; 0];
/// Default number of items to return in a query
pub const DEFAULT_QUERY_COUNT: usize = 1000;

#[derive(Debug, Clone)]
pub struct BasicAuth {
    auth_header: HeaderValue,
    endpoint: Url,
}

#[derive(Debug)]
struct DataCenter(String);

impl FromStr for DataCenter {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let mut parts = s.split('-');

        let _key = parts.next();
        let dc = parts.next();

        dc.map(|dc| Self(dc.to_string()))
            .ok_or(Error::MalformedAPIKey)
    }
}

#[derive(Debug, Clone)]
pub enum AuthMode {
    Basic(BasicAuth),
}

impl AuthMode {
    pub fn new_basic_auth(key: &str) -> Result<Self> {
        use base64::Engine;
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(format!("username:{key}").as_bytes());
        let auth_header = HeaderValue::from_str(&format!("Basic {encoded}"))
            .map_err(|_| Error::MalformedAPIKey)?;

        let dc: DataCenter = key.parse()?;
        let url = format!("https://{}.api.mailchimp.com", dc.0);
        let endpoint = Url::parse(&url)?;

        Ok(Self::Basic(BasicAuth {
            auth_header,
            endpoint,
        }))
    }

    pub fn has_token(&self) -> bool {
        match self {
            Self::Basic(_) => true,
        }
    }

    pub fn to_endpoint_url(&self) -> Url {
        match self {
            Self::Basic(auth) => auth.endpoint.clone(),
        }
    }

    pub fn to_request_url(&self, path: &str) -> Result<Url> {
        let mut uri = path.to_string();

        // Make sure we have the leading "/".
        if !uri.starts_with('/') {
            uri = format!("/{uri}");
        }

        self.to_endpoint_url().join(&uri).map_err(Error::from)
    }

    pub fn to_authorization_header(&self) -> HeaderValue {
        match self {
            Self::Basic(auth) => auth.auth_header.clone(),
        }
    }
}

pub fn read_config<'de, T: serde::Deserialize<'de>, S>(source: S) -> Result<T>
where
    S: config::Source + Send + Sync + 'static,
{
    let config = config::Config::builder()
        .add_source(source)
        .build()
        .and_then(|config| config.try_deserialize())?;
    Ok(config)
}

#[derive(Clone, Debug)]
pub struct Client {
    auth: AuthMode,
    client: reqwest::Client,
    /// Shared by every clone, so one job's requests draw on one budget.
    connections: Arc<Semaphore>,
    rate_limited_backoff: Duration,
}

pub mod client {
    use super::*;

    pub fn from_api_key(key: &str) -> Result<crate::Client> {
        let auth = crate::AuthMode::new_basic_auth(key)?;
        crate::Client::new(auth)
    }
}

impl Client {
    /// Create a new client using a given base URL and a default
    /// timeout. The library will use absoluate paths based on this
    /// base_url.
    pub fn new(auth: AuthMode) -> Result<Self> {
        Self::new_with_timeout(auth, DEFAULT_TIMEOUT)
    }

    /// Create a new client using a given base URL, and request
    /// timeout value.  The library will use absoluate paths based on
    /// the given base_url.
    pub fn new_with_timeout(auth: AuthMode, timeout: u64) -> Result<Self> {
        let client = reqwest::Client::builder()
            .gzip(true)
            .timeout(Duration::from_secs(timeout))
            .build()?;
        Ok(Self {
            auth,
            client,
            connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
            rate_limited_backoff: RATE_LIMITED_BACKOFF,
        })
    }

    /// Send a request once one of the client's connections is free, and hold
    /// the connection until `read` has consumed the response. reqwest starts
    /// its timeout at `send`, so waiting for a connection does not count
    /// against it. A 429 is returned only after `rate_limited_backoff`, with
    /// the connection released, so every caller's retry waits it out.
    fn send<R, F>(&self, request: RequestBuilder, read: F) -> Future<R>
    where
        R: 'static + Send,
        F: FnOnce(reqwest::Response) -> Future<R> + Send + 'static,
    {
        let connections = self.connections.clone();
        let backoff = self.rate_limited_backoff;
        async move {
            let (rate_limited, result) = {
                let _permit = connections.acquire().await?;
                let response = request.send().await?;
                let rate_limited = response.status() == StatusCode::TOO_MANY_REQUESTS;
                (rate_limited, read(response).await)
            };
            if rate_limited {
                tokio::time::sleep(backoff).await;
            }
            result
        }
        .boxed()
    }

    fn request(&self, method: Method, path: &str) -> Result<RequestBuilder> {
        let url = self.auth.to_request_url(path)?;

        // Set the default headers.
        let mut headers = HeaderMap::new();
        headers.append(AUTHORIZATION, self.auth.to_authorization_header());
        headers.append(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        Ok(self.client.request(method, url).headers(headers))
    }

    pub fn fetch<T, Q>(&self, path: &str, query: &Q) -> Future<T>
    where
        T: 'static + DeserializeOwned + Send,
        Q: Serialize + ?Sized,
    {
        match self.request(Method::GET, path) {
            Ok(builder) => self.send(builder.query(query), |response| {
                let status = response.status();
                if status.is_client_error() {
                    return response
                        .json::<error::MailchimError>()
                        .map_err(error::Error::from)
                        .and_then(|e| async move { Err(Error::mailchimp(e)) })
                        .boxed();
                }
                match response.error_for_status() {
                    Ok(result) => result
                        .bytes()
                        .map_err(Error::from)
                        .and_then(|bytes| async move {
                            // println!("{}", String::from_utf8_lossy(&bytes));
                            serde_json::from_slice(&bytes).map_err(error::Error::from)
                        })
                        .boxed(),
                    Err(e) => future::err(error::Error::from(e)).boxed(),
                }
            }),
            Err(e) => future::err(e).boxed(),
        }
    }

    // pub async fn fetch_all<Q, R>(&self, path: &str, mut query: Q) -> Result<Vec<R::Item>>
    // where
    //     R: PagedResponse + 'static,
    //     Q: PagedQuery + 'static + Serialize,
    // {
    //     let client = self.clone();
    //     let path = path.to_string();
    // }

    pub fn fetch_stream<Q, R>(&self, path: &str, mut query: Q) -> Stream<R::Item>
    where
        R: PagedResponse + 'static,
        Q: PagedQuery + 'static + Serialize,
    {
        let client = self.clone();
        let path = path.to_string();

        self.fetch::<R, _>(&path, &query)
            .map_ok(move |data| {
                // let mut query = query.clone();
                query.inc_offset(data.len());
                stream::try_unfold(
                    (data, client, path, query),
                    |(mut data, client, path, mut query)| async move {
                        match data.pop() {
                            Some(entry) => Ok(Some((entry, (data, client, path, query)))),
                            None => {
                                let mut data = client.fetch::<R, _>(&path, &query).await?;
                                let data_len = data.len();
                                tracing::info!(data_len, "data");
                                if data_len > 0 {
                                    query.inc_offset(data_len);
                                    let entry = data.pop().unwrap();
                                    Ok(Some((entry, (data, client, path, query))))
                                } else {
                                    Ok(None)
                                }
                            }
                        }
                    },
                )
            })
            .try_flatten_stream()
            .boxed()
    }

    pub fn submit<T, R>(&self, method: Method, path: &str, json: &T) -> Future<R>
    where
        T: Serialize + ?Sized,
        R: 'static + DeserializeOwned + std::marker::Send,
    {
        match self.request(method, path) {
            Ok(builder) => self.send(builder.json(json), |response| {
                let status = response.status();
                if status.is_client_error() {
                    return response
                        .json::<error::MailchimError>()
                        .map_err(error::Error::from)
                        .and_then(|e| async move { Err(Error::mailchimp(e)) })
                        .boxed();
                }
                match response.error_for_status() {
                    Ok(result) => result
                        .bytes()
                        .map_err(error::Error::from)
                        .and_then(|bytes| async move {
                            if bytes.is_empty() {
                                serde_json::from_str("null").map_err(error::Error::from)
                            } else {
                                // println!("{}", String::from_utf8_lossy(&bytes));
                                serde_json::from_slice(&bytes).map_err(error::Error::from)
                            }
                        })
                        .boxed(),
                    // Ok(result) => result.json().map_err(error::Error::from).boxed(),
                    Err(e) => future::err(error::Error::from(e)).boxed(),
                }
            }),
            Err(e) => future::err(e).boxed(),
        }
    }

    pub fn post<T, R>(&self, path: &str, json: &T) -> Future<R>
    where
        T: Serialize + ?Sized,
        R: 'static + DeserializeOwned + std::marker::Send,
    {
        self.submit(Method::POST, path, json)
    }

    pub fn patch<T, R>(&self, path: &str, json: &T) -> Future<R>
    where
        T: Serialize + ?Sized,
        R: 'static + DeserializeOwned + std::marker::Send,
    {
        self.submit(Method::PATCH, path, json)
    }

    pub fn put<T, R>(&self, path: &str, json: &T) -> Future<R>
    where
        T: Serialize + ?Sized,
        R: 'static + DeserializeOwned + std::marker::Send,
    {
        self.submit(Method::PUT, path, json)
    }

    pub fn delete(&self, path: &str) -> Future<()> {
        match self.request(Method::DELETE, path) {
            Ok(builder) => self.send(builder, |response| match response.error_for_status() {
                Ok(response) => response.bytes().map_ok(drop).map_err(Error::from).boxed(),
                Err(e) => future::err(error::Error::from(e)).boxed(),
            }),
            Err(e) => future::err(e).boxed(),
        }
    }
}

#[derive(Clone, Copy, Default)]
pub enum RetryPolicy {
    #[default]
    None,
    Retries(usize),
}

impl RetryPolicy {
    pub fn none() -> Self {
        Self::None
    }

    pub fn with_retries(retries: usize) -> Self {
        Self::Retries(retries)
    }
}

impl IntoIterator for RetryPolicy {
    type Item = Duration;
    type IntoIter = std::vec::IntoIter<Duration>;

    fn into_iter(self) -> Self::IntoIter {
        use tokio_retry2::strategy::ExponentialFactorBackoff;
        let retries = match self {
            Self::None => vec![],
            Self::Retries(retries) => ExponentialFactorBackoff::from_factor(2.)
                .max_delay_millis(5000)
                .map(jitter)
                .take(retries)
                .collect(),
        };
        retries.into_iter()
    }
}

pub trait PagedQuery: Clone + Send + Serialize + Sync {
    fn default_fields() -> &'static [&'static str];
    fn fields(&self) -> &str;
    fn set_fields(&mut self, fields: String);
    fn append_fields(&mut self, fields: &[&str]) {
        self.set_fields(format!("{},{}", self.fields(), fields.join(",")));
    }

    fn set_count(&mut self, count: usize);

    fn offset(&self) -> usize;
    fn set_offset(&mut self, offset: usize);

    fn inc_offset(&mut self, inc: usize) {
        self.set_offset(self.offset() + inc)
    }
}

pub trait PagedResponse: DeserializeOwned + Send + Sync + Debug {
    type Item: DeserializeOwned + Send + Sync + Debug;

    fn pop(&mut self) -> Option<Self::Item>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

macro_rules! paged_query_impl {
    ($query_type:ident, $default_fields:expr) => {
        impl crate::PagedQuery for $query_type {
            fn default_fields() -> &'static [&'static str] {
                $default_fields
            }

            fn fields(&self) -> &str {
                &self.fields
            }

            fn set_fields(&mut self, fields: String) {
                self.fields = fields;
            }

            fn set_count(&mut self, count: usize) {
                self.count = count;
            }

            fn offset(&self) -> usize {
                self.offset
            }

            fn set_offset(&mut self, offset: usize) {
                self.offset = offset;
            }
        }
    };
}

macro_rules! paged_response_impl {
    ($response_type:ident, $item_field:ident, $item_type:ident) => {
        impl crate::PagedResponse for $response_type {
            type Item = $item_type;

            fn pop(&mut self) -> Option<$item_type> {
                self.$item_field.pop()
            }
            fn len(&self) -> usize {
                self.$item_field.len()
            }
        }
    };
}

macro_rules! query_default_impl {
    ($query_type:ident) => {
        impl Default for $query_type {
            fn default() -> Self {
                use crate::PagedQuery;
                Self {
                    fields: Self::default_fields().join(","),
                    count: crate::DEFAULT_QUERY_COUNT,
                    offset: 0,
                }
            }
        }
    };
}

pub(crate) use {paged_query_impl, paged_response_impl, query_default_impl};

pub mod deserialize_null_string {
    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer).unwrap_or_default();

        Ok(s)
    }
}

/// Deserializer for Option<MemberStatus> that treats empty strings as None
pub mod deserialize_member_status {
    use crate::members::MemberStatus;
    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<MemberStatus>, D::Error>
    where
        D: Deserializer<'de>,
    {
        // First try to deserialize as Option<String> to handle empty strings
        let opt: Option<String> = Option::deserialize(deserializer)?;
        match opt {
            None => Ok(None),
            Some(s) if s.is_empty() => Ok(None),
            Some(s) => {
                // Parse the string into MemberStatus
                match s.as_str() {
                    "subscribed" => Ok(Some(MemberStatus::Subscribed)),
                    "unsubscribed" => Ok(Some(MemberStatus::Unsubscribed)),
                    "cleaned" => Ok(Some(MemberStatus::Cleaned)),
                    "pending" => Ok(Some(MemberStatus::Pending)),
                    "transactional" => Ok(Some(MemberStatus::Transactional)),
                    "archived" => Ok(Some(MemberStatus::Archived)),
                    "noop" => Ok(Some(MemberStatus::Noop)),
                    _ => Ok(None), // Unknown status treated as None
                }
            }
        }
    }
}

pub fn is_default<T>(value: &T) -> bool
where
    T: PartialEq + Default,
{
    *value == T::default()
}

pub mod deserialize_null_i32 {
    use super::I32Visitor;
    use serde::Deserializer;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<i32, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = deserializer.deserialize_i32(I32Visitor).unwrap_or_default();

        Ok(s)
    }
}

struct I32Visitor;

impl serde::de::Visitor<'_> for I32Visitor {
    type Value = i32;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an integer between -2^31 and 2^31")
    }

    fn visit_i8<E>(self, value: i8) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(value as i32)
    }

    fn visit_i16<E>(self, value: i16) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(value as i32)
    }

    fn visit_i32<E>(self, value: i32) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(value)
    }

    fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if value >= i64::from(i32::MIN) && value <= i64::from(i32::MAX) {
            Ok(value as i32)
        } else {
            Err(E::custom(format!("i32 out of range: {value}")))
        }
    }

    fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(value as i32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[derive(Default)]
    struct InFlight {
        now: AtomicUsize,
        peak: AtomicUsize,
    }

    const OK: (&str, &str) = ("200 OK", "{}");
    const TOO_MANY: (&str, &str) = (
        "429 Too Many Requests",
        r#"{"status":429,"title":"Too Many Requests","detail":"","instance":""}"#,
    );

    /// Answer every request with `reply`'s headers at once and its body after
    /// `delay`, recording the most requests held open at once.
    async fn serve(
        delay: Duration,
        reply: (&'static str, &'static str),
    ) -> (Client, Arc<InFlight>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        let endpoint = Url::parse(&format!("http://{addr}")).expect("parse endpoint");
        let in_flight = Arc::new(InFlight::default());
        let counts = in_flight.clone();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let counts = counts.clone();
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut buf = [0u8; 1024];
                    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                        let n = socket.read(&mut buf).await.expect("read request");
                        if n == 0 {
                            return;
                        }
                        head.extend_from_slice(&buf[..n]);
                    }
                    // Drain a request body so closing the socket cannot reset
                    // the connection before the response is read.
                    let text = String::from_utf8_lossy(&head).to_ascii_lowercase();
                    let length: usize = text
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
                        .map_or(0, |n| n.trim().parse().expect("parse content-length"));
                    let body_start = text.find("\r\n\r\n").expect("end of head") + 4;
                    let mut remaining = length.saturating_sub(head.len() - body_start);
                    while remaining > 0 {
                        let n = socket.read(&mut buf).await.expect("read body");
                        remaining = remaining.saturating_sub(n);
                    }
                    let now = counts.now.fetch_add(1, Ordering::SeqCst) + 1;
                    counts.peak.fetch_max(now, Ordering::SeqCst);
                    // The body follows the headers after `delay`, so the
                    // request stays open while its response is being read.
                    let (status, body) = reply;
                    let length = body.len();
                    let headers = format!(
                        "HTTP/1.1 {status}\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n"
                    );
                    socket
                        .write_all(headers.as_bytes())
                        .await
                        .expect("write headers");
                    tokio::time::sleep(delay).await;
                    counts.now.fetch_sub(1, Ordering::SeqCst);
                    socket.write_all(body.as_bytes()).await.expect("write body");
                });
            }
        });
        let auth = AuthMode::Basic(BasicAuth {
            auth_header: HeaderValue::from_static("Basic test"),
            endpoint,
        });
        (
            Client::new_with_timeout(auth, 1).expect("build client"),
            in_flight,
        )
    }

    /// Send `n` requests at once from clones of `client`, cycling through
    /// GET, POST and DELETE so every request path is under the cap.
    fn request_many(client: &Client, n: usize) -> impl StdFuture<Output = Vec<Result<()>>> {
        future::join_all((0..n).map(|i| {
            let client = client.clone();
            async move {
                let path = "/3.0/ping";
                match i % 3 {
                    0 => client.fetch::<Value, _>(path, NO_QUERY).await.map(drop),
                    1 => client.post::<_, Value>(path, &()).await.map(drop),
                    _ => client.delete(path).await,
                }
            }
        }))
    }

    #[tokio::test]
    async fn clones_of_a_client_share_a_cap_of_eight_requests() {
        let (client, in_flight) = serve(Duration::from_millis(100), OK).await;
        let results = request_many(&client, 20).await;
        assert!(results.iter().all(Result::is_ok), "{results:?}");
        assert_eq!(in_flight.peak.load(Ordering::SeqCst), 8);
    }

    #[tokio::test]
    async fn waiting_for_a_connection_does_not_count_against_the_timeout() {
        // Two waves of 700ms each: the second wave finishes 1.4s after it was
        // queued, past the 1s timeout, and only its own 700ms may count.
        let (client, _) = serve(Duration::from_millis(700), OK).await;
        let results = request_many(&client, 16).await;
        assert!(results.iter().all(Result::is_ok), "{results:?}");
    }

    #[tokio::test]
    async fn a_429_is_reported_after_the_backoff_with_its_connection_released() {
        let (client, _) = serve(Duration::ZERO, TOO_MANY).await;
        let client = Client {
            rate_limited_backoff: Duration::from_millis(500),
            ..client
        };
        // Nine requests against eight connections: each one waits the
        // backoff, and the ninth only starts late if the eight waiting hold
        // their connections through it.
        let start = tokio::time::Instant::now();
        let results = request_many(&client, 9).await;
        let elapsed = start.elapsed();
        assert!(results.iter().all(Result::is_err), "{results:?}");
        assert!(
            elapsed >= Duration::from_millis(500) && elapsed < Duration::from_millis(900),
            "{elapsed:?}"
        );
    }
}
