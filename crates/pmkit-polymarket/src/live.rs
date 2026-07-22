use std::{fmt, sync::Arc};

use async_trait::async_trait;
use futures::StreamExt as _;
use pmkit_core::MarketId;
use pmkit_data::{
    DataSourceError, LiveDataSource, RawPmAccountFrame, RawPmMarketFrame, SourceSignal,
};
use pmkit_event::{MarketEvent, PmAccountEnvelope, PmMarketEnvelope};
use pmkit_market::Outcome;
use pmkit_store::{OwnerScope, PmEnvelope, StoreError, TapeStore};
use polymarket_client_sdk_v2::clob::ws::{BookUpdate, Client, LastTradePrice};
use polymarket_client_sdk_v2::error::Error as SdkError;
use thiserror::Error;
use tokio::sync::mpsc::Sender;

use crate::{MarketTokens, from_venue_side};

/// Adapts raw Polymarket frames into typed PM stream envelopes.
pub trait PolymarketFrameAdapter {
    /// Adapts one public-market frame.
    ///
    /// # Errors
    ///
    /// Returns [`DataSourceError`] when the frame cannot be adapted.
    fn adapt_market_frame(
        &self,
        frame: RawPmMarketFrame,
    ) -> Result<PmMarketEnvelope, DataSourceError>;

    /// Adapts one authenticated-account frame.
    ///
    /// # Errors
    ///
    /// Returns [`DataSourceError`] when the frame cannot be adapted.
    fn adapt_account_frame(
        &self,
        frame: RawPmAccountFrame,
    ) -> Result<PmAccountEnvelope, DataSourceError>;
}

/// Error raised while preserving a raw Polymarket frame before adapting it.
#[derive(Debug, Error)]
pub enum RawFrameAdapterError {
    /// Durable storage rejected the raw frame before adaptation.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The raw text could not be decoded into JSON for its durable projection.
    #[error("raw Polymarket frame was not JSON: {source}")]
    Json {
        /// The JSON decoding failure.
        #[source]
        source: serde_json::Error,
    },
    /// The raw frame's portfolio does not match the adapter's owner scope.
    #[error("account frame portfolio does not match adapter scope")]
    ScopeMismatch,
    /// The post-storage adapter could not produce a normalized fact.
    #[error(transparent)]
    Adapt(#[from] DataSourceError),
}

/// Persists exact Polymarket text frames before the caller deserializes them.
#[derive(Clone)]
pub struct RawPolymarketFrameAdapter {
    store: Arc<dyn TapeStore>,
    scope: OwnerScope,
    config_hash: String,
}

impl fmt::Debug for RawPolymarketFrameAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawPolymarketFrameAdapter")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl RawPolymarketFrameAdapter {
    /// Creates a PM-only raw-frame recorder for one portfolio/run scope.
    #[must_use]
    pub fn new(
        store: Arc<dyn TapeStore>,
        scope: OwnerScope,
        config_hash: impl Into<String>,
    ) -> Self {
        Self {
            store,
            scope,
            config_hash: config_hash.into(),
        }
    }

    /// Stores market text before invoking `adapt` to deserialize it.
    ///
    /// # Errors
    ///
    /// Returns [`RawFrameAdapterError`] when storage, JSON projection, or adaptation fails.
    pub async fn market<F>(
        &self,
        frame: RawPmMarketFrame,
        adapt: F,
    ) -> Result<PmMarketEnvelope, RawFrameAdapterError>
    where
        F: FnOnce(&[u8]) -> Result<MarketEvent, DataSourceError>,
    {
        let normalized = serde_json::from_slice(&frame.text)
            .map_err(|source| RawFrameAdapterError::Json { source })?;
        self.store
            .store_envelope(&PmEnvelope {
                schema_version: frame.metadata.schema_version,
                scope: self.scope.clone(),
                venue_id: "polymarket".into(),
                config_hash: self.config_hash.clone(),
                source_id: frame.metadata.source_id.clone(),
                connection_id: frame.metadata.connection_id.clone(),
                source_timestamp_ms: frame.metadata.source_time_ms,
                canonical_source_rank: frame.metadata.canonical_source_rank,
                connection_epoch: frame.metadata.connection_epoch,
                frame_sequence: frame.metadata.frame_sequence,
                receipt_timestamp_ms: frame.metadata.receipt_time_ms,
                ingest_sequence: i64::try_from(frame.metadata.ingest_sequence).map_err(|_| {
                    StoreError::Storage {
                        message: "PM ingest sequence exceeds storage range".into(),
                    }
                })?,
                raw_frame: frame.text.clone(),
                normalized,
            })
            .await?;
        let fact = adapt(&frame.text)?;
        Ok(PmMarketEnvelope {
            metadata: frame.metadata,
            raw_frame: frame.text,
            fact,
        })
    }

    /// Stores account text before invoking `adapt` to deserialize it.
    ///
    /// # Errors
    ///
    /// Returns [`RawFrameAdapterError`] when storage, JSON projection, or adaptation fails.
    pub async fn account<F>(
        &self,
        frame: RawPmAccountFrame,
        adapt: F,
    ) -> Result<PmAccountEnvelope, RawFrameAdapterError>
    where
        F: FnOnce(&[u8]) -> Result<pmkit_event::PmAccountEvent, DataSourceError>,
    {
        let normalized = serde_json::from_slice(&frame.text)
            .map_err(|source| RawFrameAdapterError::Json { source })?;
        if frame.portfolio != self.scope.portfolio_id {
            return Err(RawFrameAdapterError::ScopeMismatch);
        }
        self.store
            .store_envelope(&PmEnvelope {
                schema_version: frame.metadata.schema_version,
                scope: self.scope.clone(),
                venue_id: "polymarket".into(),
                config_hash: self.config_hash.clone(),
                source_id: frame.metadata.source_id.clone(),
                connection_id: frame.metadata.connection_id.clone(),
                source_timestamp_ms: frame.metadata.source_time_ms,
                canonical_source_rank: frame.metadata.canonical_source_rank,
                connection_epoch: frame.metadata.connection_epoch,
                frame_sequence: frame.metadata.frame_sequence,
                receipt_timestamp_ms: frame.metadata.receipt_time_ms,
                ingest_sequence: i64::try_from(frame.metadata.ingest_sequence).map_err(|_| {
                    StoreError::Storage {
                        message: "PM ingest sequence exceeds storage range".into(),
                    }
                })?,
                raw_frame: frame.text.clone(),
                normalized,
            })
            .await?;
        let fact = adapt(&frame.text)?;
        Ok(PmAccountEnvelope {
            portfolio: frame.portfolio,
            metadata: frame.metadata,
            raw_frame: frame.text,
            fact,
        })
    }
}

/// Polymarket order-book and trade WebSocket source.
#[derive(Clone)]
pub struct PolymarketLiveData {
    client: Client,
    tokens: MarketTokens,
}

impl PolymarketLiveData {
    /// Creates a live source from an SDK WebSocket client and market token map.
    #[must_use]
    pub const fn new(client: Client, tokens: MarketTokens) -> Self {
        Self { client, tokens }
    }
}

impl fmt::Debug for PolymarketLiveData {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PolymarketLiveData")
            .field("tokens", &self.tokens)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl LiveDataSource for PolymarketLiveData {
    async fn subscribe(
        &self,
        market: MarketId,
        outcome: Outcome,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        if &market != self.tokens.market() {
            return Err(DataSourceError::NotAvailable);
        }
        let token = self.tokens.token(outcome);
        let books = self
            .client
            .subscribe_orderbook(vec![token])
            .map_err(|error| data_error(&error))?;
        let trades = match self.client.subscribe_last_trade_price(vec![token]) {
            Ok(trades) => trades,
            Err(error) => {
                self.client
                    .unsubscribe_orderbook(&[token])
                    .map_err(|error| data_error(&error))?;
                return Err(data_error(&error));
            }
        };
        let mut books = Box::pin(books);
        let mut trades = Box::pin(trades);

        let result = loop {
            tokio::select! {
                update = books.next() => match update {
                    Some(Ok(update)) => {
                        if sink.send(SourceSignal::market_event(book_event(market.clone(), outcome, update))).await.is_err() {
                            break Err(DataSourceError::SinkClosed);
                        }
                    }
                    Some(Err(error)) => break Err(data_error(&error)),
                    None => break Ok(()),
                },
                update = trades.next() => match update {
                    Some(Ok(update)) => {
                        if let Some(event) = trade_event(market.clone(), outcome, &update)
                            && sink.send(SourceSignal::market_event(event)).await.is_err()
                        {
                            break Err(DataSourceError::SinkClosed);
                        }
                    }
                    Some(Err(error)) => break Err(data_error(&error)),
                    None => break Ok(()),
                },
            }
        };

        drop(books);
        drop(trades);
        let book_cleanup = self.client.unsubscribe_orderbook(&[token]);
        let trade_cleanup = self.client.unsubscribe_orderbook(&[token]);
        result?;
        book_cleanup.map_err(|error| data_error(&error))?;
        trade_cleanup.map_err(|error| data_error(&error))
    }
}

fn book_event(market: MarketId, outcome: Outcome, update: BookUpdate) -> MarketEvent {
    MarketEvent::BookUpdate {
        market,
        outcome,
        bids: update
            .bids
            .into_iter()
            .map(|level| (level.price, level.size))
            .collect(),
        asks: update
            .asks
            .into_iter()
            .map(|level| (level.price, level.size))
            .collect(),
        timestamp_ms: update.timestamp,
    }
}

fn trade_event(market: MarketId, outcome: Outcome, update: &LastTradePrice) -> Option<MarketEvent> {
    Some(MarketEvent::LastTrade {
        market,
        outcome,
        price: update.price,
        side: from_venue_side(update.side?)?,
        size: update.size?,
        timestamp_ms: update.timestamp,
    })
}

/// Parses a raw Polymarket market frame into a neutral market event.
///
/// # Errors
///
/// Returns [`DataSourceError::ReplayGap`] when the frame is not a recognized market shape.
pub fn parse_market_frame(
    raw: &[u8],
    tokens: &MarketTokens,
) -> Result<MarketEvent, DataSourceError> {
    let raw_text = std::str::from_utf8(raw).map_err(|_| DataSourceError::ReplayGap {
        message: "raw frame is not valid UTF-8".into(),
    })?;
    if let Ok(update) = serde_json::from_str::<BookUpdate>(raw_text) {
        let outcome =
            tokens
                .outcome(&update.asset_id)
                .ok_or_else(|| DataSourceError::ReplayGap {
                    message: "unknown market asset id".into(),
                })?;
        return Ok(book_event(tokens.market().clone(), outcome, update));
    }
    if let Ok(update) = serde_json::from_str::<LastTradePrice>(raw_text) {
        let outcome =
            tokens
                .outcome(&update.asset_id)
                .ok_or_else(|| DataSourceError::ReplayGap {
                    message: "unknown market asset id".into(),
                })?;
        return trade_event(tokens.market().clone(), outcome, &update).ok_or_else(|| {
            DataSourceError::ReplayGap {
                message: "incomplete last trade price".into(),
            }
        });
    }
    Err(DataSourceError::ReplayGap {
        message: "unrecognized Polymarket market frame".into(),
    })
}

fn data_error(error: &SdkError) -> DataSourceError {
    DataSourceError::Unavailable {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::significant_drop_tightening)]
    use std::{
        num::NonZeroUsize,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::{SystemTime, UNIX_EPOCH},
    };

    use async_trait::async_trait;
    use pmkit_book::Side;
    use pmkit_core::{MarketId, PortfolioId, RunId};
    use pmkit_data::{RawPmAccountFrame, RawPmMarketFrame};
    use pmkit_event::{MarketEvent, PmAccountEvent, StreamMetadata};

    use pmkit_market::Outcome;
    use pmkit_store::{
        CausalDecision, CausalIdentity, IntentOutcome, OwnerScope, PmEnvelope, ReplayCursor,
        ReplayPage, StoreError, TapeStore, TursoTapeStore,
    };
    use polymarket_client_sdk_v2::clob::ws::{BookUpdate, LastTradePrice};
    use rust_decimal::Decimal;

    use super::{RawFrameAdapterError, RawPolymarketFrameAdapter, book_event, trade_event};

    fn metadata(sequence: i64) -> StreamMetadata {
        StreamMetadata {
            schema_version: 1,
            source_id: "polymarket-market".into(),
            source_time_ms: 42,
            canonical_source_rank: 0,
            receipt_time_ms: 43,
            connection_id: "market-1".into(),
            connection_epoch: 2,
            frame_sequence: sequence,
            ingest_sequence: u64::try_from(sequence).unwrap_or_default(),
        }
    }

    fn scope() -> Result<OwnerScope, Box<dyn std::error::Error>> {
        Ok(OwnerScope::new(
            PortfolioId::new("alice")?,
            RunId::new("raw")?,
        ))
    }

    fn database_path() -> Result<PathBuf, std::time::SystemTimeError> {
        Ok(std::env::temp_dir().join(format!(
            "pmkit-polymarket-raw-{}.db",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        )))
    }

    #[test]
    fn venue_updates_map_to_neutral_events() -> Result<(), Box<dyn std::error::Error>> {
        let market = MarketId::new("btc-5m")?;
        let book: BookUpdate = serde_json::from_str(
            r#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[{"price":"0.49","size":"2"}],"asks":[{"price":"0.51","size":"3"}]}"#,
        )?;
        let trade: LastTradePrice = serde_json::from_str(
            r#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","price":"0.5","side":"BUY","size":"4","timestamp":"43"}"#,
        )?;

        let MarketEvent::BookUpdate {
            bids,
            asks,
            timestamp_ms,
            ..
        } = book_event(market.clone(), Outcome::Up, book)
        else {
            return Err("expected book update".into());
        };
        assert_eq!(bids, vec![(Decimal::new(49, 2), Decimal::from(2))]);
        assert_eq!(asks, vec![(Decimal::new(51, 2), Decimal::from(3))]);
        assert_eq!(timestamp_ms, 42);

        let Some(MarketEvent::LastTrade {
            price,
            side,
            size,
            timestamp_ms,
            ..
        }) = trade_event(market, Outcome::Up, &trade)
        else {
            return Err("expected last trade".into());
        };
        assert_eq!(price, Decimal::new(5, 1));
        assert_eq!(side, Side::Buy);
        assert_eq!(size, Decimal::from(4));
        assert_eq!(timestamp_ms, 43);
        Ok(())
    }
    #[test]
    fn parse_market_frame_accepts_book_update_json() -> Result<(), Box<dyn std::error::Error>> {
        use crate::MarketTokens;
        use polymarket_client_sdk_v2::types::U256;

        let tokens = MarketTokens::new(
            pmkit_core::MarketId::new("btc-5m")?,
            U256::from(1_u64),
            U256::from(2_u64),
        );
        let raw = br#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[{"price":"0.49","size":"2"}],"asks":[{"price":"0.51","size":"3"}]}"#;
        let event = super::parse_market_frame(raw, &tokens)?;
        let pmkit_event::MarketEvent::BookUpdate { timestamp_ms, .. } = event else {
            return Err("expected book update".into());
        };
        assert_eq!(timestamp_ms, 42);
        Ok(())
    }

    #[tokio::test]
    async fn raw_frames_are_stored_before_adaptation() -> Result<(), Box<dyn std::error::Error>> {
        let path = database_path()?;
        let store = Arc::new(TursoTapeStore::open_local(&path).await?);
        let adapter = RawPolymarketFrameAdapter::new(store.clone(), scope()?, "fixture");
        let text = br#"{ "event_type": "book" }"#.to_vec();
        let frame = adapter
            .market(
                RawPmMarketFrame {
                    metadata: metadata(7),
                    text: text.clone(),
                },
                |received| {
                    assert_eq!(received, text);
                    Ok(MarketEvent::Tick { timestamp_ms: 42 })
                },
            )
            .await?;
        let page = store
            .read_envelopes(&scope()?, None, NonZeroUsize::MIN)
            .await?;
        let Some(pmkit_store::ReplayItem::Envelope(stored)) = page.items.first() else {
            return Err("expected stored PM envelope".into());
        };
        assert_eq!(frame.raw_frame, text);
        assert_eq!(stored.raw_frame, text);
        drop(adapter);
        drop(page);
        Arc::try_unwrap(store)
            .map_err(|_| "store still referenced")?
            .delete_database()?;
        assert!(!path.exists());
        Ok(())
    }

    #[tokio::test]
    async fn account_frame_with_mismatched_owner_is_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = database_path()?;
        let store = Arc::new(TursoTapeStore::open_local(&path).await?);
        let adapter = RawPolymarketFrameAdapter::new(store.clone(), scope()?, "fixture");
        let frame = RawPmAccountFrame {
            portfolio: PortfolioId::new("bob")?,
            metadata: metadata(9),
            text: br#"{"event_type":"order_ack"}"#.to_vec(),
        };
        let result = adapter
            .account(frame, |_| {
                Ok(PmAccountEvent::OrderAck {
                    strategy: None,
                    order_id: "order-1".into(),
                    timestamp_ms: 42,
                })
            })
            .await;
        assert!(matches!(result, Err(RawFrameAdapterError::ScopeMismatch)));
        let page = store
            .read_envelopes(&scope()?, None, NonZeroUsize::MIN)
            .await?;
        assert!(page.items.is_empty());
        drop(adapter);
        Arc::try_unwrap(store)
            .map_err(|_| "store still referenced")?
            .delete_database()?;
        assert!(!path.exists());
        Ok(())
    }

    #[tokio::test]
    async fn store_failure_prevents_market_adaptation() -> Result<(), Box<dyn std::error::Error>> {
        let adapted = Arc::new(AtomicBool::new(false));
        let recorder = RawPolymarketFrameAdapter::new(Arc::new(FailingStore), scope()?, "fixture");
        let result = recorder
            .market(
                RawPmMarketFrame {
                    metadata: metadata(8),
                    text: br"{}".to_vec(),
                },
                {
                    let adapted = Arc::clone(&adapted);
                    move |_| {
                        adapted.store(true, Ordering::Relaxed);
                        Ok(MarketEvent::Tick { timestamp_ms: 42 })
                    }
                },
            )
            .await;
        assert!(matches!(result, Err(RawFrameAdapterError::Store(_))));
        assert!(!adapted.load(Ordering::Relaxed));
        Ok(())
    }

    struct FailingStore;

    #[async_trait]
    impl TapeStore for FailingStore {
        async fn store_envelope(&self, _envelope: &PmEnvelope) -> Result<(), StoreError> {
            Err(StoreError::Storage {
                message: "fixture failure".into(),
            })
        }

        async fn read_envelopes(
            &self,
            _scope: &OwnerScope,
            _after: Option<ReplayCursor>,
            _limit: NonZeroUsize,
        ) -> Result<ReplayPage, StoreError> {
            Err(StoreError::Storage {
                message: "fixture failure".into(),
            })
        }

        async fn store_decision(&self, _decision: &CausalDecision) -> Result<(), StoreError> {
            Err(StoreError::Storage {
                message: "fixture failure".into(),
            })
        }

        async fn store_intent_pending(
            &self,
            _identity: &CausalIdentity,
            _payload: &serde_json::Value,
        ) -> Result<(), StoreError> {
            Err(StoreError::Storage {
                message: "fixture failure".into(),
            })
        }

        async fn transition_intent(
            &self,
            _identity: &CausalIdentity,
            _outcome: IntentOutcome,
        ) -> Result<(), StoreError> {
            Err(StoreError::Storage {
                message: "fixture failure".into(),
            })
        }

        async fn read_pending_intents(
            &self,
            _scope: &OwnerScope,
        ) -> Result<Vec<pmkit_store::DurableIntent>, StoreError> {
            Err(StoreError::Storage {
                message: "fixture failure".into(),
            })
        }

        async fn read_unknown_intents(
            &self,
            _scope: &OwnerScope,
        ) -> Result<Vec<pmkit_store::DurableIntent>, StoreError> {
            Err(StoreError::Storage {
                message: "fixture failure".into(),
            })
        }

        async fn read_decisions(
            &self,
            _scope: &OwnerScope,
        ) -> Result<Vec<pmkit_store::CausalDecision>, StoreError> {
            Err(StoreError::Storage {
                message: "fixture failure".into(),
            })
        }
    }
}
