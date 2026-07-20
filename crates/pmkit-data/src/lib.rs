//! Market data-source traits for `PMKit`.
//!
//! A data source delivers [`MarketEvent`]s into a caller-provided channel. The
//! historical source replays a bounded window; the live source streams until
//! the subscription is dropped. Neither performs any venue signing.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pmkit_core::MarketId;
use pmkit_event::MarketEvent;
use pmkit_market::Outcome;
use pmkit_run::{EvidenceRequirement, RetrievalWait};
use thiserror::Error;
use tokio::sync::mpsc::Sender;

/// A failure while sourcing market data.
#[derive(Debug, Error)]
pub enum DataSourceError {
    /// The source could not be reached or initialised.
    #[error("data source unavailable: {message}")]
    Unavailable {
        /// Human-readable detail.
        message: String,
    },
    /// No data is available for the requested window.
    #[error("requested data is not available for the given window")]
    NotAvailable,
    /// The delivery channel was closed by the receiver.
    #[error("data delivery channel closed")]
    SinkClosed,
}

/// A bounded historical replay request.
#[derive(Debug, Clone)]
pub struct ReplayQuery {
    /// Exact markets to replay.
    pub markets: Vec<MarketId>,
    /// Inclusive start of the window.
    pub from: DateTime<Utc>,
    /// Exclusive end of the window.
    pub to: DateTime<Utc>,
    /// Required corroboration for the replayed data.
    pub evidence: EvidenceRequirement,
    /// Whether to wait for archive retrieval or return pending.
    pub retrieval_wait: RetrievalWait,
}

/// A source of historical market events for backtests.
#[async_trait]
pub trait HistoricalDataSource: Send + Sync {
    /// Replays the requested window, delivering events into `sink` in
    /// non-decreasing timestamp order.
    ///
    /// # Errors
    ///
    /// Returns [`DataSourceError`] if the window cannot be served or the sink
    /// is closed before the replay completes.
    async fn replay(
        &self,
        query: ReplayQuery,
        sink: Sender<MarketEvent>,
    ) -> Result<(), DataSourceError>;
}

/// A source of live market events for paper and live runs.
#[async_trait]
pub trait LiveDataSource: Send + Sync {
    /// Subscribes to a market outcome, delivering events into `sink` until the
    /// subscription is dropped.
    ///
    /// # Errors
    ///
    /// Returns [`DataSourceError`] if the subscription cannot be established or
    /// the sink is closed.
    async fn subscribe(
        &self,
        market: MarketId,
        outcome: Outcome,
        sink: Sender<MarketEvent>,
    ) -> Result<(), DataSourceError>;
}

#[cfg(test)]
mod tests {
    use super::{DataSourceError, HistoricalDataSource, ReplayQuery};
    use async_trait::async_trait;
    use pmkit_core::MarketId;
    use pmkit_event::MarketEvent;
    use pmkit_run::{EvidenceRequirement, RetrievalWait};
    use tokio::sync::mpsc::{self, Sender};

    struct StaticHistory {
        ticks: Vec<i64>,
    }

    #[async_trait]
    impl HistoricalDataSource for StaticHistory {
        async fn replay(
            &self,
            _query: ReplayQuery,
            sink: Sender<MarketEvent>,
        ) -> Result<(), DataSourceError> {
            for &timestamp_ms in &self.ticks {
                sink.send(MarketEvent::Tick { timestamp_ms })
                    .await
                    .map_err(|_| DataSourceError::SinkClosed)?;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn replay_delivers_every_event() -> Result<(), Box<dyn std::error::Error>> {
        let (tx, mut rx) = mpsc::channel(8);
        let source = StaticHistory {
            ticks: vec![1, 2, 3],
        };
        let query = ReplayQuery {
            markets: vec![MarketId::new("btc-5m")?],
            from: "2026-01-01T00:00:00Z".parse()?,
            to: "2026-02-01T00:00:00Z".parse()?,
            evidence: EvidenceRequirement::CorroboratedOnly,
            retrieval_wait: RetrievalWait::ReturnPending,
        };

        source.replay(query, tx).await?;

        let mut seen = Vec::new();
        while let Some(event) = rx.recv().await {
            seen.push(event.timestamp_ms());
        }
        assert_eq!(seen, vec![1, 2, 3]);
        Ok(())
    }
}
