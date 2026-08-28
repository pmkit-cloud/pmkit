use std::{fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pmkit_market::{Asset, MarketDuration};
use tokio::sync::{Mutex, mpsc::Sender};

use crate::{DataSourceError, HistoricalDataSource, ReplayQuery, SourceSignal};

#[path = "cloud_cache.rs"]
mod cloud_cache;
#[path = "cloud_decode.rs"]
mod cloud_decode;
#[path = "cloud_http.rs"]
mod cloud_http;
#[path = "cloud_types.rs"]
mod cloud_types;

pub use cloud_types::{
    CloudCoverage, CloudCoverageInterval, CloudCoverageStatus, CloudMarketInstance,
    CloudOutcomeToken, CloudReplayError, CloudReplaySelector, RetrievalState,
};

const PRODUCTION_BASE_URL: &str = "https://pmkit.cloud/v1";

/// A `PMKit` Cloud API key that never exposes its value through formatting.
#[derive(Clone)]
pub struct CloudApiKey(Arc<str>);

impl CloudApiKey {
    /// Creates a non-empty, non-whitespace API key.
    ///
    /// # Errors
    ///
    /// Returns [`CloudReplayError::InvalidConfiguration`] when `value` is empty or whitespace.
    pub fn new(value: impl Into<String>) -> Result<Self, CloudReplayError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(CloudReplayError::InvalidConfiguration);
        }
        Ok(Self(Arc::from(value)))
    }

    fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for CloudApiKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CloudApiKey([REDACTED])")
    }
}

impl fmt::Display for CloudApiKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

/// A selector and bounded UTC window for Cloud replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudReplayQuery {
    /// Indexed replay selector.
    pub selector: CloudReplaySelector,
    /// Inclusive start of the replay window.
    pub from: DateTime<Utc>,
    /// Exclusive end of the replay window.
    pub to: DateTime<Utc>,
}

impl CloudReplayQuery {
    /// Selects a recurring typed asset and duration.
    #[must_use]
    pub const fn asset(
        asset: Asset,
        duration: MarketDuration,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Self {
        Self {
            selector: CloudReplaySelector::Asset { asset, duration },
            from,
            to,
        }
    }

    fn validate(&self) -> Result<(), CloudReplayError> {
        if self.from.timestamp_millis() >= self.to.timestamp_millis() {
            return Err(CloudReplayError::InvalidQuery);
        }
        if matches!(&self.selector, CloudReplaySelector::Series(series) if series.trim().is_empty())
        {
            return Err(CloudReplayError::InvalidQuery);
        }
        Ok(())
    }
}

/// Read-only historical source backed by the public `PMKit` Cloud API.
#[derive(Clone)]
pub struct PmKitCloudDataSource {
    api_key: CloudApiKey,
    base_url: Arc<str>,
    client: reqwest::Client,
    cache: Arc<Mutex<std::collections::HashMap<String, Arc<[u8]>>>>,
}

impl fmt::Debug for PmKitCloudDataSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PmKitCloudDataSource")
            .field("api_key", &self.api_key)
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl PmKitCloudDataSource {
    /// Creates the production source using `https://pmkit.cloud/v1`.
    ///
    /// # Errors
    ///
    /// Returns [`CloudReplayError::InvalidConfiguration`] when the client cannot be built.
    pub fn new(api_key: CloudApiKey) -> Result<Self, CloudReplayError> {
        Self::with_base_url(api_key, PRODUCTION_BASE_URL)
    }

    /// Creates a source with an explicit endpoint for tests or self-hosting.
    ///
    /// # Errors
    ///
    /// Returns [`CloudReplayError::InvalidConfiguration`] for an unsafe endpoint or
    /// when the HTTP client cannot be built.
    pub fn with_base_url(api_key: CloudApiKey, base_url: &str) -> Result<Self, CloudReplayError> {
        let url =
            reqwest::Url::parse(base_url).map_err(|_| CloudReplayError::InvalidConfiguration)?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || url.cannot_be_a_base()
            || url.query().is_some()
            || url.fragment().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(CloudReplayError::InvalidConfiguration);
        }
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|_| CloudReplayError::InvalidConfiguration)?;
        Ok(Self {
            api_key,
            base_url: Arc::from(base_url.trim_end_matches('/')),
            client,
            cache: Arc::new(Mutex::new(std::collections::HashMap::new())),
        })
    }

    /// Reads public coverage and concrete market discovery metadata.
    ///
    /// This performs only the public `/v1/coverage` read; it does not list or
    /// download segments and never starts archive retrieval.
    ///
    /// # Errors
    ///
    /// Returns a typed [`CloudReplayError`] when the query is invalid, the
    /// service cannot be reached, or the response is malformed.
    pub async fn coverage(
        &self,
        query: CloudReplayQuery,
    ) -> Result<CloudCoverage, CloudReplayError> {
        query.validate()?;
        cloud_http::coverage(self, &query).await
    }

    /// Replays verified Cloud segments into `PMKit` lifecycle signals.
    ///
    /// # Errors
    ///
    /// Returns a typed [`CloudReplayError`] when coverage, retrieval, transport,
    /// integrity, or decoding cannot safely serve the requested window.
    pub async fn replay_cloud(
        &self,
        query: CloudReplayQuery,
        sink: Sender<SourceSignal>,
    ) -> Result<(), CloudReplayError> {
        query.validate()?;
        cloud_http::replay(self, query, sink).await
    }
}

#[async_trait]
impl HistoricalDataSource for PmKitCloudDataSource {
    async fn replay(
        &self,
        query: ReplayQuery,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        if query.markets.is_empty() {
            return Err(DataSourceError::NotAvailable);
        }
        cloud_http::replay_markets(self, query.markets, query.from, query.to, sink)
            .await
            .map_err(|error| DataSourceError::ReplayGap {
                message: error.to_string(),
            })
    }
}

#[cfg(test)]
#[path = "cloud_test_support.rs"]
mod cloud_test_support;
#[cfg(test)]
#[path = "cloud_replay_tests.rs"]
mod replay_tests;
#[cfg(test)]
#[path = "cloud_tests.rs"]
mod tests;
