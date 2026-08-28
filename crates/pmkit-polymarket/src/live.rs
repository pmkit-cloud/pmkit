use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

use async_trait::async_trait;
use futures::StreamExt as _;
use pmkit_core::MarketId;
use pmkit_data::{
    DataSourceError, LiveDataSource, RawPmAccountFrame, RawPmMarketFrame, SourceSignal,
};
use pmkit_event::{
    MarketEvent, PmAccountEnvelope, PmMarketEnvelope, SourceEnvelope, StreamMetadata,
};
use pmkit_market::Outcome;
use pmkit_store::{OwnerScope, PM_ENVELOPE_VERSION, PmEnvelope, StoreError, TapeStore};
use polymarket_client_sdk_v2::clob::{
    types::Side as VenueSide,
    ws::{BookUpdate, Client, LastTradePrice, PriceChange},
};
use polymarket_client_sdk_v2::error::Error as SdkError;
use polymarket_client_sdk_v2::types::U256;
use thiserror::Error;
use tokio::sync::mpsc::Sender;

use crate::{MarketTokens, from_venue_side};

const MARKET_SOURCE_ID: &str = "polymarket:market-ws";

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
        let envelope = self.market_envelope(&frame)?;
        self.store.store_envelope_idempotent(&envelope).await?;
        let fact = adapt(&frame.text)?;
        Ok(PmMarketEnvelope {
            metadata: frame.metadata,
            raw_frame: frame.text,
            fact,
        })
    }

    pub(crate) fn market_envelope(
        &self,
        frame: &RawPmMarketFrame,
    ) -> Result<PmEnvelope, RawFrameAdapterError> {
        let payload: serde_json::Value = serde_json::from_slice(&frame.text)
            .map_err(|source| RawFrameAdapterError::Json { source })?;
        let normalized = serde_json::json!({
            "stream_id": format!(
                "market:{}:{}",
                frame.market,
                frame.outcome.to_string().to_lowercase()
            ),
            "canonical_market_id": frame.market.to_string(),
            "payload": payload,
        });
        Ok(PmEnvelope {
            schema_version: PM_ENVELOPE_VERSION,
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
        let payload: serde_json::Value = serde_json::from_slice(&frame.text)
            .map_err(|source| RawFrameAdapterError::Json { source })?;
        if frame.portfolio != self.scope.portfolio_id {
            return Err(RawFrameAdapterError::ScopeMismatch);
        }
        let normalized = serde_json::json!({
            "stream_id": format!("account:{}", frame.portfolio),
            "portfolio": frame.portfolio.to_string(),
            "payload": payload,
        });
        self.store
            .store_envelope_idempotent(&PmEnvelope {
                schema_version: PM_ENVELOPE_VERSION,
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

    /// Persists raw v2 tape evidence that intentionally has no replay projection.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when durable evidence storage fails.
    pub async fn store_public_tape_audit_frame(
        &self,
        frame: &pmkit_store::PublicTapeAuditFrame,
    ) -> Result<(), StoreError> {
        self.store.store_public_tape_audit_frame(frame).await
    }

    /// Persists a recorder interval before an export can materialize its evidence.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when durable gap storage fails.
    pub async fn store_replay_gap(
        &self,
        gap: &pmkit_store::ReplayGapInterval,
    ) -> Result<(), StoreError> {
        self.store.store_replay_gap(gap).await
    }

    pub(crate) async fn store_public_tape_import(
        &self,
        gaps: &[pmkit_store::ReplayGapInterval],
        audit_frames: &[pmkit_store::PublicTapeAuditFrame],
        envelopes: &[PmEnvelope],
    ) -> Result<(), StoreError> {
        self.store
            .store_public_tape_import(gaps, audit_frames, envelopes)
            .await
    }
}

/// Polymarket order-book and trade WebSocket source.
#[derive(Clone)]
pub struct PolymarketLiveData {
    client: Client,
    tokens: MarketTokens,
    connection_epoch: Arc<AtomicI64>,
}

impl PolymarketLiveData {
    /// Creates a live source from an SDK WebSocket client and market token map.
    #[must_use]
    pub fn new(client: Client, tokens: MarketTokens) -> Self {
        Self {
            client,
            tokens,
            connection_epoch: Arc::new(AtomicI64::new(0)),
        }
    }
}

struct MarketSubscriptions {
    client: Client,
    token: U256,
    count: u8,
}

impl MarketSubscriptions {
    const fn new(client: Client, token: U256) -> Self {
        Self {
            client,
            token,
            count: 0,
        }
    }

    const fn add(&mut self) {
        self.count = self.count.saturating_add(1);
    }

    fn close(mut self) -> Result<(), DataSourceError> {
        let mut first_error = None;
        while self.count > 0 {
            self.count -= 1;
            if let Err(error) = self.client.unsubscribe_orderbook(&[self.token])
                && first_error.is_none()
            {
                first_error = Some(data_error(&error));
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl Drop for MarketSubscriptions {
    fn drop(&mut self) {
        while self.count > 0 {
            self.count -= 1;
            let _ = self.client.unsubscribe_orderbook(&[self.token]);
        }
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
    #[expect(
        clippy::too_many_lines,
        reason = "market subscription keeps stream merge and cleanup in one lifecycle"
    )]
    async fn subscribe(
        &self,
        market: MarketId,
        outcome: Outcome,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        if &market != self.tokens.market() {
            return Err(DataSourceError::NotAvailable);
        }
        let connection_epoch = next_connection_epoch(&self.connection_epoch)?;
        let token = self.tokens.token(outcome);
        let mut subscriptions = MarketSubscriptions::new(self.client.clone(), token);
        let books = self
            .client
            .subscribe_orderbook(vec![token])
            .map_err(|error| data_error(&error))?;
        subscriptions.add();
        let prices = self
            .client
            .subscribe_prices(vec![token])
            .map_err(|error| data_error(&error))?;
        subscriptions.add();
        let trades = self
            .client
            .subscribe_last_trade_price(vec![token])
            .map_err(|error| data_error(&error))?;
        subscriptions.add();
        let mut books = Box::pin(books);
        let mut prices = Box::pin(prices);
        let mut trades = Box::pin(trades);
        let mut book = TokenBook::default();
        let mut pending_prices: VecDeque<PriceChange> = VecDeque::new();
        let mut pending_events = VecDeque::new();
        let mut sequence = 0;

        let result = loop {
            if let Some(event) = pending_events.pop_front() {
                let signal = match sequenced_market_signal(&mut sequence, connection_epoch, event) {
                    Ok(signal) => signal,
                    Err(error) => break Err(error),
                };
                if sink.send(signal).await.is_err() {
                    break Err(DataSourceError::SinkClosed);
                }
                continue;
            }
            tokio::select! {
                update = books.next() => match update {
                    Some(Ok(update)) => {
                        book.replace(&update, token)?;
                        let mut events = VecDeque::from([book.event(market.clone(), outcome)]);
                        let mut previous = book.timestamp_ms;
                        pending_prices.make_contiguous().sort_unstable_by_key(|change| change.timestamp);
                        for change in std::mem::take(&mut pending_prices) {
                            if change.timestamp <= previous {
                                continue;
                            }
                            if book.apply(&change, token)? {
                                previous = book.timestamp_ms;
                                events.push_back(book.event(market.clone(), outcome));
                            }
                        }
                        let event = events
                            .pop_front()
                            .ok_or_else(|| replay_gap("book snapshot produced no event"))?;
                        pending_events.extend(events);
                        let signal = sequenced_market_signal(
                            &mut sequence,
                            connection_epoch,
                            event,
                        )?;
                        if sink.send(signal).await.is_err() {
                            break Err(DataSourceError::SinkClosed);
                        }
                    }
                    Some(Err(error)) => break Err(data_error(&error)),
                    None => break Ok(()),
                },
                update = prices.next() => match update {
                    Some(Ok(update)) => {
                        if !book.initialized {
                            if pending_prices.len() >= MAX_PENDING_PRICE_CHANGES {
                                break Err(replay_gap("price changes exceeded pre-snapshot buffer"));
                            }
                            pending_prices.push_back(update);
                            continue;
                        }
                        if book.apply(&update, token)? {
                            let signal = sequenced_market_signal(
                                &mut sequence,
                                connection_epoch,
                                book.event(market.clone(), outcome),
                            )?;
                            if sink.send(signal).await.is_err() {
                                break Err(DataSourceError::SinkClosed);
                            }
                        }
                    }
                    Some(Err(error)) => break Err(data_error(&error)),
                    None => break Ok(()),
                },
                update = trades.next() => match update {
                    Some(Ok(update)) => {
                        if let Some(event) = trade_event(market.clone(), outcome, &update) {
                            let signal = match sequenced_market_signal(
                                &mut sequence,
                                connection_epoch,
                                event,
                            ) {
                                Ok(signal) => signal,
                                Err(error) => break Err(error),
                            };
                            if sink.send(signal).await.is_err() {
                                break Err(DataSourceError::SinkClosed);
                            }
                        }
                    }
                    Some(Err(error)) => break Err(data_error(&error)),
                    None => break Ok(()),
                },
            }
        };

        drop(books);
        drop(prices);
        drop(trades);
        let cleanup = subscriptions.close();
        result?;
        cleanup
    }
}

const MAX_PENDING_PRICE_CHANGES: usize = 1_024;

#[derive(Debug, Default)]
struct TokenBook {
    bids: BTreeMap<rust_decimal::Decimal, rust_decimal::Decimal>,
    asks: BTreeMap<rust_decimal::Decimal, rust_decimal::Decimal>,
    timestamp_ms: i64,
    initialized: bool,
}

impl TokenBook {
    fn replace(&mut self, update: &BookUpdate, token: U256) -> Result<(), DataSourceError> {
        if update.asset_id != token {
            return Err(replay_gap("book snapshot has the wrong asset id"));
        }
        if self.initialized && update.timestamp < self.timestamp_ms {
            return Err(replay_gap("stale book snapshot"));
        }
        self.bids = levels(&update.bids)?;
        self.asks = levels(&update.asks)?;
        self.timestamp_ms = update.timestamp;
        self.initialized = true;
        Ok(())
    }

    fn apply(&mut self, update: &PriceChange, token: U256) -> Result<bool, DataSourceError> {
        if !self.initialized {
            return Err(replay_gap("price change before initial book snapshot"));
        }
        if update.timestamp < self.timestamp_ms {
            return Err(replay_gap("stale price change"));
        }
        let mut matched = false;
        for change in update
            .price_changes
            .iter()
            .filter(|change| change.asset_id == token)
        {
            matched = true;
            let size = change
                .size
                .ok_or_else(|| replay_gap("price change lacks size"))?;
            if size.is_sign_negative() {
                return Err(replay_gap("price change has negative size"));
            }
            let target_levels = match change.side {
                VenueSide::Buy => &mut self.bids,
                VenueSide::Sell => &mut self.asks,
                _ => return Err(replay_gap("price change has unknown side")),
            };
            if size.is_zero() {
                target_levels.remove(&change.price);
            } else {
                target_levels.insert(change.price, size);
            }
        }
        if matched {
            self.timestamp_ms = update.timestamp;
        }
        Ok(matched)
    }

    fn event(&self, market: MarketId, outcome: Outcome) -> MarketEvent {
        MarketEvent::BookUpdate {
            market,
            outcome,
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(price, size)| (*price, *size))
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(price, size)| (*price, *size))
                .collect(),
            timestamp_ms: self.timestamp_ms,
        }
    }
}

fn levels(
    levels: &[polymarket_client_sdk_v2::clob::ws::types::response::OrderBookLevel],
) -> Result<BTreeMap<rust_decimal::Decimal, rust_decimal::Decimal>, DataSourceError> {
    let mut result = BTreeMap::new();
    for level in levels {
        if level.size.is_sign_negative() {
            return Err(replay_gap("book snapshot has negative size"));
        }
        if !level.size.is_zero() {
            result.insert(level.price, level.size);
        }
    }
    Ok(result)
}

fn replay_gap(message: &str) -> DataSourceError {
    DataSourceError::ReplayGap {
        message: message.to_owned(),
    }
}

fn next_connection_epoch(epoch: &AtomicI64) -> Result<i64, DataSourceError> {
    epoch
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map_err(|_| DataSourceError::ReplayGap {
            message: "Polymarket market connection epoch overflow".into(),
        })
}

fn sequenced_market_signal(
    sequence: &mut u64,
    connection_epoch: i64,
    fact: MarketEvent,
) -> Result<SourceSignal, DataSourceError> {
    *sequence = sequence
        .checked_add(1)
        .ok_or_else(|| DataSourceError::ReplayGap {
            message: "Polymarket market frame sequence overflow".into(),
        })?;
    let frame_sequence = i64::try_from(*sequence).map_err(|_| DataSourceError::ReplayGap {
        message: "Polymarket market frame sequence exceeds signed range".into(),
    })?;
    let timestamp_ms = fact.timestamp_ms();
    Ok(SourceSignal::Data(Box::new(SourceEnvelope::PmMarket(
        PmMarketEnvelope {
            metadata: StreamMetadata {
                schema_version: 1,
                source_id: MARKET_SOURCE_ID.into(),
                source_time_ms: timestamp_ms,
                canonical_source_rank: 0,
                receipt_time_ms: timestamp_ms,
                connection_id: MARKET_SOURCE_ID.into(),
                connection_epoch,
                frame_sequence,
                ingest_sequence: *sequence,
            },
            raw_frame: Vec::new(),
            fact,
        },
    ))))
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
            atomic::{AtomicBool, AtomicI64, Ordering},
        },
    };

    use async_trait::async_trait;
    use pmkit_book::Side;
    use pmkit_core::{MarketId, PortfolioId, RunId};
    use pmkit_data::{DataSourceError, RawPmAccountFrame, RawPmMarketFrame, SourceSignal};
    use pmkit_event::{MarketEvent, PmAccountEvent, SourceEnvelope, StreamMetadata};

    use pmkit_market::Outcome;
    use pmkit_store::{
        CausalDecision, CausalIdentity, IntentOutcome, OwnerScope, PmEnvelope, ReplayCursor,
        ReplayPage, StoreError, TapeStore, TursoTapeStore,
    };
    use polymarket_client_sdk_v2::{
        clob::ws::{BookUpdate, LastTradePrice, PriceChange},
        types::U256,
    };
    use rust_decimal::Decimal;

    use super::{
        RawFrameAdapterError, RawPolymarketFrameAdapter, TokenBook, book_event,
        next_connection_epoch, sequenced_market_signal, trade_event,
    };

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

    fn database_path() -> Result<(tempfile::TempDir, PathBuf), std::io::Error> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("pmkit-polymarket-raw.db");
        Ok((dir, path))
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
    fn price_change_batches_keep_complementary_books_isolated()
    -> Result<(), Box<dyn std::error::Error>> {
        let market = MarketId::new("btc-15m")?;
        let up_token = U256::from(1_u64);
        let down_token = U256::from(2_u64);
        let up_snapshot: BookUpdate = serde_json::from_str(
            r#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[{"price":"0.49","size":"2"}],"asks":[{"price":"0.51","size":"3"}]}"#,
        )?;
        let down_snapshot: BookUpdate = serde_json::from_str(
            r#"{"asset_id":"2","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[{"price":"0.98","size":"2"}],"asks":[{"price":"0.99","size":"3"}]}"#,
        )?;
        let batch: PriceChange = serde_json::from_str(
            r#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"43","price_changes":[{"asset_id":"1","price":"0.49","size":"0","side":"BUY"},{"asset_id":"1","price":"0.52","size":"4","side":"SELL"},{"asset_id":"2","price":"0.01","size":"5","side":"SELL"}]}"#,
        )?;
        let mut up = TokenBook::default();
        let mut down = TokenBook::default();
        up.replace(&up_snapshot, up_token)?;
        down.replace(&down_snapshot, down_token)?;

        assert!(up.apply(&batch, up_token)?);
        assert!(down.apply(&batch, down_token)?);
        let MarketEvent::BookUpdate {
            bids: up_bids,
            asks: up_asks,
            ..
        } = up.event(market.clone(), Outcome::Up)
        else {
            return Err("expected UP book".into());
        };
        let MarketEvent::BookUpdate {
            bids: down_bids,
            asks: down_asks,
            ..
        } = down.event(market, Outcome::Down)
        else {
            return Err("expected DOWN book".into());
        };
        assert!(up_bids.is_empty());
        assert_eq!(up_asks[0], (Decimal::new(51, 2), Decimal::from(3)));
        assert_eq!(up_asks[1], (Decimal::new(52, 2), Decimal::from(4)));
        assert_eq!(down_bids, vec![(Decimal::new(98, 2), Decimal::from(2))]);
        assert_eq!(down_asks[0], (Decimal::new(1, 2), Decimal::from(5)));
        assert!(
            !up_asks
                .iter()
                .any(|(price, _)| *price == Decimal::new(1, 2))
        );
        Ok(())
    }

    #[test]
    fn price_change_requires_a_snapshot_and_monotonic_time()
    -> Result<(), Box<dyn std::error::Error>> {
        let token = U256::from(1_u64);
        let snapshot: BookUpdate = serde_json::from_str(
            r#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[],"asks":[]}"#,
        )?;
        let stale: PriceChange = serde_json::from_str(
            r#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"41","price_changes":[{"asset_id":"1","price":"0.5","size":"1","side":"BUY"}]}"#,
        )?;
        let mut book = TokenBook::default();
        assert!(matches!(
            book.apply(&stale, token),
            Err(DataSourceError::ReplayGap { .. })
        ));
        book.replace(&snapshot, token)?;
        assert!(matches!(
            book.apply(&stale, token),
            Err(DataSourceError::ReplayGap { .. })
        ));
        Ok(())
    }

    #[test]
    fn production_sequence_increases_and_reconnect_epoch_advances()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given: equal-timestamp frames across one subscription and a reconnect.
        let market = MarketId::new("btc-5m")?;
        let mut sequence = 0;
        let mut reconnected_sequence = 0;
        let epochs = AtomicI64::new(0);
        let first_epoch = next_connection_epoch(&epochs)?;
        let second_epoch = next_connection_epoch(&epochs)?;
        let event = || MarketEvent::BookUpdate {
            market: market.clone(),
            outcome: Outcome::Up,
            bids: vec![(Decimal::new(49, 2), Decimal::ONE)],
            asks: vec![(Decimal::new(51, 2), Decimal::ONE)],
            timestamp_ms: 42,
        };

        // When: the production source assigns both frame identities.
        let first = sequenced_market_signal(&mut sequence, first_epoch, event())?;
        let second = sequenced_market_signal(&mut sequence, first_epoch, event())?;
        let reconnected =
            sequenced_market_signal(&mut reconnected_sequence, second_epoch, event())?;
        let (
            SourceSignal::Data(first),
            SourceSignal::Data(second),
            SourceSignal::Data(reconnected),
        ) = (first, second, reconnected)
        else {
            return Err("expected market data signals".into());
        };
        let (
            SourceEnvelope::PmMarket(first),
            SourceEnvelope::PmMarket(second),
            SourceEnvelope::PmMarket(reconnected),
        ) = (*first, *second, *reconnected)
        else {
            return Err("expected PM market envelopes".into());
        };

        // Then: sequence advances within an epoch and reconnect starts a distinct epoch.
        assert_eq!(
            (
                first.metadata.connection_epoch,
                first.metadata.frame_sequence
            ),
            (0, 1)
        );
        assert_eq!(
            (
                second.metadata.connection_epoch,
                second.metadata.frame_sequence
            ),
            (0, 2)
        );
        assert_eq!(
            (
                reconnected.metadata.connection_epoch,
                reconnected.metadata.frame_sequence,
            ),
            (1, 1)
        );
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
        let (_dir, path) = database_path()?;
        let store = Arc::new(TursoTapeStore::open_local(&path).await?);
        let adapter = RawPolymarketFrameAdapter::new(store.clone(), scope()?, "fixture");
        let text = br#"{ "event_type": "book" }"#.to_vec();
        let frame = adapter
            .market(
                RawPmMarketFrame {
                    market: MarketId::new("btc-5m")?,
                    outcome: Outcome::Up,
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
        assert_eq!(stored.canonical_market_id(), "btc-5m");
        assert_eq!(stored.canonical_stream_id(), "market:btc-5m:up");
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
        let (_dir, path) = database_path()?;
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
                    market: MarketId::new("btc-5m")?,
                    outcome: Outcome::Up,
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
