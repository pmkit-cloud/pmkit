use pmkit_core::MarketId;
use pmkit_market::{Asset, MarketDuration};
use serde::Deserialize;
use thiserror::Error;

/// Coverage status for a public Cloud replay interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudCoverageStatus {
    /// Every source interval is available.
    Available,
    /// The interval contains known missing evidence.
    KnownGap,
}

/// One observed or missing interval in a Cloud coverage response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CloudCoverageInterval {
    /// Inclusive interval start in Unix milliseconds.
    pub from_ts_ms: i64,
    /// Inclusive interval end in Unix milliseconds.
    pub to_ts_ms: i64,
    /// Whether this interval can be replayed.
    pub status: CloudCoverageStatus,
}

/// An outcome/token mapping for one concrete market instance.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CloudOutcomeToken {
    /// Human-readable outcome label.
    pub outcome: String,
    /// Concrete outcome token identifier.
    pub token_id: String,
}

/// A concrete market instance returned by Cloud coverage discovery.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CloudMarketInstance {
    /// Concrete market identifier.
    pub market_id: String,
    /// Condition identifier, when present in the catalog.
    #[serde(default)]
    pub condition_id: Option<String>,
    /// Ordered outcome/token mappings.
    #[serde(default)]
    pub outcome_tokens: Vec<CloudOutcomeToken>,
}

/// Public coverage metadata for a bounded Cloud replay query.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CloudCoverage {
    /// Observed and known-gap intervals.
    #[serde(default)]
    pub intervals: Vec<CloudCoverageInterval>,
    /// Latest timestamp the service guarantees is sealed.
    pub sealed_through_ms: Option<i64>,
    /// Concrete market instances discovered for this selector and range.
    #[serde(default)]
    pub instances: Vec<CloudMarketInstance>,
}

/// Indexed selector supported by `PMKit` Cloud replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloudReplaySelector {
    /// One concrete market instance.
    Market(MarketId),
    /// One recurring market family.
    Series(String),
    /// One typed asset and duration family.
    Asset {
        /// Underlying asset.
        asset: Asset,
        /// Market window duration.
        duration: MarketDuration,
    },
}

/// Public archive availability state.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetrievalState {
    /// Immediately downloadable.
    Hot,
    /// An explicit retrieval job is required.
    RestoreRequired,
    /// Retrieval is queued.
    Queued,
    /// Retrieval is in progress.
    Restoring,
    /// A restored copy is temporarily ready.
    ReadyUntil,
    /// The restored copy expired.
    Expired,
    /// Retrieval failed.
    Failed,
    /// Retrieval was cancelled.
    Cancelled,
}

/// Typed Cloud replay failure.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CloudReplayError {
    /// Source configuration is invalid.
    #[error("PMKit Cloud replay configuration is invalid")]
    InvalidConfiguration,
    /// The selector or time window is invalid.
    #[error("PMKit Cloud replay query is invalid")]
    InvalidQuery,
    /// Authentication failed.
    #[error("PMKit Cloud authentication failed")]
    Unauthorized,
    /// The plan does not permit this history.
    #[error("PMKit Cloud history is forbidden by the active plan")]
    Forbidden,
    /// An explicit retrieval operation is required or underway.
    #[error("PMKit Cloud retrieval is required: {state:?}")]
    RetrievalRequired {
        /// Current public retrieval state.
        state: RetrievalState,
    },
    /// Request or transfer quota is exhausted.
    #[error("PMKit Cloud replay quota is exhausted")]
    QuotaExceeded,
    /// The public service is temporarily unavailable.
    #[error("PMKit Cloud replay service is unavailable")]
    ServiceUnavailable,
    /// The requested window contains known missing evidence.
    #[error("PMKit Cloud replay contains a known gap")]
    KnownGap,
    /// The requested window is not sealed through its exclusive end.
    #[error("PMKit Cloud replay window is not fully sealed")]
    Unsealed,
    /// A public response violated its declared schema.
    #[error("PMKit Cloud replay response is malformed")]
    MalformedResponse,
    /// Downloaded bytes failed immutable integrity checks.
    #[error("PMKit Cloud replay integrity check failed")]
    IntegrityMismatch,
    /// The network request failed without exposing credentials.
    #[error("PMKit Cloud replay request failed")]
    Transport,
}
