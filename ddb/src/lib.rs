mod error;
pub use error::{Error, Result};

pub mod addresses;
pub mod airstreams;
pub mod clubs;
pub mod leadership;
pub mod members;
pub mod microsites;
pub mod races;
pub mod rallies;
pub mod regions;
pub mod roles;
pub mod standing_committees;
pub mod users;

/// A type alias for `Future` that may return `crate::error::Error`
pub type Future<'a, T> = futures::future::BoxFuture<'a, Result<T>>;

/// A type alias for `Stream` that may result in `crate::error::Error`
pub type Stream<'a, T> = futures::stream::BoxStream<'a, Result<T>>;

pub async fn connect(url: &str) -> Result<sqlx::MySqlPool> {
    use sqlx::{ConnectOptions, Executor, MySqlPool, mysql::MySqlConnectOptions};
    use std::time::Duration;

    // Parse URL and set slow query threshold to 10s (default is 1s)
    // Bulk sync queries returning 100K+ rows legitimately take several seconds
    let options: MySqlConnectOptions = url
        .parse::<MySqlConnectOptions>()?
        .log_slow_statements(log::LevelFilter::Warn, Duration::from_secs(10));

    let pool = MySqlPool::connect_with(options).await?;
    let _ = pool
        .execute(
            r#"
            SET GLOBAL table_definition_cache = 4096;
            SET GLOBAL table_open_cache = 4096;
        "#,
        )
        .await?;
    Ok(pool)
}
