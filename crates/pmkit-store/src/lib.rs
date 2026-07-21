//! Durable market, user, and strategy-decision streams for `PMKit`.

use std::num::NonZeroUsize;

use async_trait::async_trait;
use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
use pmkit_event::{CexReferenceEnvelope, PmAccountEnvelope, PmMarketEnvelope};
use pmkit_strategy::Actions;
use serde_json::Value;
use thiserror::Error;

mod turso_store;

pub use turso_store::TursoTapeStore;

/// Failure raised while recording or reading storage streams.
#[derive(Debug, Error)]
pub enum StoreError {
    /// The underlying storage operation failed.
    #[error("storage failed: {message}")]
    Storage {
        /// Storage-specific detail.
        message: String,
    },
    /// A stored JSON payload was invalid.
    #[error("stored JSON was invalid: {0}")]
    Json(#[from] serde_json::Error),
    /// A requested page size exceeded `SQLite`'s signed limit.
    #[error("requested page size exceeds SQLite's signed limit")]
    LimitTooLarge,
}

impl From<turso::Error> for StoreError {
    fn from(error: turso::Error) -> Self {
        Self::Storage {
            message: error.to_string(),
        }
    }
}

/// An event read back from a tape stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEvent {
    /// The event's source timestamp in milliseconds.
    pub timestamp_ms: i64,
    /// The portable event payload.
    pub payload: Value,
}

/// A strategy decision made for one market event.
#[derive(Debug, Clone)]
pub struct StrategyDecision {
    /// The portfolio that owns this decision stream.
    pub portfolio: PortfolioId,
    /// The owning run.
    pub run: RunId,
    /// The strategy making the decision.
    pub strategy: StrategyId,
    /// The exact market the decision addresses.
    pub market: MarketId,
    /// The decision timestamp in milliseconds.
    pub timestamp_ms: i64,
    /// The strategy's requested actions before runtime validation.
    pub actions: Actions,
}

impl StrategyDecision {
    /// Creates a decision record from a strategy's returned actions.
    #[must_use]
    pub const fn new(
        portfolio: PortfolioId,
        run: RunId,
        strategy: StrategyId,
        market: MarketId,
        timestamp_ms: i64,
        actions: Actions,
    ) -> Self {
        Self {
            portfolio,
            run,
            strategy,
            market,
            timestamp_ms,
            actions,
        }
    }
}

/// A durable recorder for the three `PMKit` streams.
///
/// One instance represents one operator's local storage. The trait does not
/// create a platform account or transmit SDK-user data to a paid API.
#[async_trait]
pub trait TapeStore: Send + Sync {
    /// Appends an event to the shared market tape.
    async fn append_market(&self, envelope: &PmMarketEnvelope) -> Result<(), StoreError>;

    /// Appends an authenticated-account frame to the operator's local tape.
    async fn append_account(&self, envelope: &PmAccountEnvelope) -> Result<(), StoreError>;

    /// Appends a CEX reference frame to the shared reference tape.
    async fn append_reference(&self, envelope: &CexReferenceEnvelope) -> Result<(), StoreError>;

    /// Appends one strategy decision for analytics.
    async fn append_decision(&self, decision: &StrategyDecision) -> Result<(), StoreError>;

    /// Returns at most `limit` market events in insertion order.
    async fn market_events(&self, limit: NonZeroUsize) -> Result<Vec<StoredEvent>, StoreError>;

    /// Returns at most `limit` user events in insertion order.
    async fn user_events(&self, limit: NonZeroUsize) -> Result<Vec<StoredEvent>, StoreError>;

    /// Returns at most `limit` CEX reference frames in insertion order.
    async fn reference_events(&self, limit: NonZeroUsize) -> Result<Vec<StoredEvent>, StoreError>;
}
