use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Instant,
};

use async_trait::async_trait;
use futures::StreamExt as _;
use pmkit_core::MarketId;
use pmkit_data::{
    DataSourceError, LIVE_HEARTBEAT_INTERVAL_MS, LiveDataSource, RawPmAccountFrame,
    RawPmMarketFrame, SourceSignal, live_watermark_now, now_ms,
};
use pmkit_event::{
    MarketEvent, PmAccountEnvelope, PmMarketEnvelope, SourceEnvelope, StreamMetadata,
};
use pmkit_market::Outcome;
use pmkit_store::{OwnerScope, PM_ENVELOPE_VERSION, PmEnvelope, StoreError, TapeStore};
use polymarket_client_sdk_v2::ws::connection::ConnectionState;
use polymarket_client_sdk_v2::{
    clob::{
        types::Side as VenueSide,
        ws::{BookUpdate, ChannelType, Client, LastTradePrice, PriceChange, WsMessage},
    },
    error::Error as SdkError,
    types::U256,
};
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
    /// Creates a credential-free source with the official SDK's default public endpoint.
    #[must_use]
    pub fn public(tokens: MarketTokens) -> Self {
        Self::new(Client::default(), tokens)
    }

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

/// One source-owned reference to one SDK unified market subscription.
struct MarketSubscriptionLease {
    client: Client,
    token: U256,
    subscribed: bool,
    released: bool,
}

impl MarketSubscriptionLease {
    const fn reserve(client: Client, token: U256) -> Self {
        Self {
            client,
            token,
            subscribed: false,
            released: false,
        }
    }

    const fn mark_subscribed(&mut self) {
        self.subscribed = true;
    }

    fn release_sync(&mut self) -> Result<(), DataSourceError> {
        if self.released {
            return Ok(());
        }
        self.released = true;
        if self.subscribed {
            self.client
                .unsubscribe_market_events(&[self.token])
                .map_err(|error| data_error(&error))
        } else {
            Ok(())
        }
    }

    async fn close(mut self) -> Result<(), DataSourceError> {
        let cleanup = self.release_sync();
        _ = self.client.shutdown_if_idle().await;
        cleanup
    }
}

impl Drop for MarketSubscriptionLease {
    fn drop(&mut self) {
        _ = self.release_sync();
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
        let mut connection_epoch = next_connection_epoch(&self.connection_epoch)?;
        let token = self.tokens.token(outcome);
        // ponytail: isolate outcome sockets until the SDK can atomically register every
        // consumer before one multi-token subscription sends its initial snapshots.
        let client = self.client.isolated().map_err(|error| data_error(&error))?;
        let mut subscription = MarketSubscriptionLease::reserve(client.clone(), token);
        let events = match client.subscribe_market_events(vec![token]) {
            Ok(events) => {
                subscription.mark_subscribed();
                events
            }
            Err(error) => {
                let setup_error = data_error(&error);
                return Err(subscription.close().await.err().unwrap_or(setup_error));
            }
        };
        let mut events = Box::pin(events);
        let mut book = TokenBook::default();
        let mut sequence = 0_u64;
        let mut connection_since = connected_since(&client);
        let mut heartbeat =
            tokio::time::interval(std::time::Duration::from_millis(LIVE_HEARTBEAT_INTERVAL_MS));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await;

        let result = loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    if book.initialized
                        && client.connection_state(ChannelType::Market).is_connected()
                        && sink.send(SourceSignal::Watermark(live_watermark_now())).await.is_err()
                    {
                        break Err(DataSourceError::SinkClosed);
                    }
                }
                update = events.next() => match update {
                    Some(Ok(update)) => {
                        if let Err(error) = observe_connection_state(
                            &client,
                            &mut connection_since,
                            &mut connection_epoch,
                            &mut sequence,
                            &mut book,
                            &self.connection_epoch,
                        ) {
                            break Err(error);
                        }
                        let event = match market_event(
                            &market,
                            outcome,
                            token,
                            &mut book,
                            update,
                        ) {
                            Ok(Some(event)) => event,
                            Ok(None) => continue,
                            Err(error) => break Err(error),
                        };
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
                    Some(Err(error)) => break Err(data_error(&error)),
                    None => break Err(unavailable("Polymarket market stream ended")),
                },
            }
        };

        drop(events);
        let cleanup = subscription.close().await;
        result?;
        cleanup
    }
}

#[derive(Debug, Default)]
struct TokenBook {
    bids: BTreeMap<rust_decimal::Decimal, rust_decimal::Decimal>,
    asks: BTreeMap<rust_decimal::Decimal, rust_decimal::Decimal>,
    timestamp_ms: i64,
    initialized: bool,
}

impl TokenBook {
    fn replace(&mut self, update: &BookUpdate, token: U256) -> Result<bool, DataSourceError> {
        if update.asset_id != token {
            return Ok(false);
        }
        let bids = levels(&update.bids)?;
        let asks = levels(&update.asks)?;
        validate_book(&bids, &asks)?;
        self.bids = bids;
        self.asks = asks;
        self.timestamp_ms = update.timestamp;
        self.initialized = true;
        Ok(true)
    }

    fn apply(&mut self, update: &PriceChange, token: U256) -> Result<bool, DataSourceError> {
        if !update
            .price_changes
            .iter()
            .any(|change| change.asset_id == token)
        {
            return Ok(false);
        }
        if !self.initialized {
            return Ok(false);
        }
        let mut bids = self.bids.clone();
        let mut asks = self.asks.clone();
        let matching = update
            .price_changes
            .iter()
            .filter(|change| change.asset_id == token)
            .collect::<Vec<_>>();
        for change in &matching {
            if change.price <= rust_decimal::Decimal::ZERO
                || change.price > rust_decimal::Decimal::ONE
            {
                return Err(replay_gap("price change price is outside (0, 1]"));
            }
            let size = change
                .size
                .ok_or_else(|| replay_gap("price change lacks size"))?;
            if size < rust_decimal::Decimal::ZERO {
                return Err(replay_gap("price change has negative size"));
            }
            let target_levels = match change.side {
                VenueSide::Buy => &mut bids,
                VenueSide::Sell => &mut asks,
                _ => return Err(replay_gap("price change has unknown side")),
            };
            if size.is_zero() {
                target_levels.remove(&change.price);
            } else {
                target_levels.insert(change.price, size);
            }
        }
        reconcile_reported_top(&mut bids, &mut asks, &matching)?;
        validate_book(&bids, &asks)?;
        self.bids = bids;
        self.asks = asks;
        self.timestamp_ms = update.timestamp;
        Ok(true)
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
        if level.price <= rust_decimal::Decimal::ZERO || level.price > rust_decimal::Decimal::ONE {
            return Err(replay_gap("book snapshot price is outside (0, 1]"));
        }
        if level.size < rust_decimal::Decimal::ZERO {
            return Err(replay_gap("book snapshot has negative size"));
        }
        if !level.size.is_zero() && result.insert(level.price, level.size).is_some() {
            return Err(replay_gap("book snapshot contains a duplicate price"));
        }
    }
    Ok(result)
}

fn reconcile_reported_top(
    bids: &mut BTreeMap<rust_decimal::Decimal, rust_decimal::Decimal>,
    asks: &mut BTreeMap<rust_decimal::Decimal, rust_decimal::Decimal>,
    changes: &[&polymarket_client_sdk_v2::clob::ws::types::response::PriceChangeBatchEntry],
) -> Result<(), DataSourceError> {
    let best_bid = changes.iter().rev().find_map(|change| change.best_bid);
    let best_ask = changes.iter().rev().find_map(|change| change.best_ask);
    if let Some(best_bid) = best_bid {
        if best_bid <= rust_decimal::Decimal::ZERO || best_bid > rust_decimal::Decimal::ONE {
            return Err(replay_gap("reported best bid is outside (0, 1]"));
        }
        bids.retain(|price, _| *price <= best_bid);
        if bids.last_key_value().map(|(price, _)| *price) != Some(best_bid) {
            return Err(replay_gap(
                "reported best bid is absent from reconstructed depth",
            ));
        }
    }
    if let Some(best_ask) = best_ask {
        if best_ask <= rust_decimal::Decimal::ZERO || best_ask > rust_decimal::Decimal::ONE {
            return Err(replay_gap("reported best ask is outside (0, 1]"));
        }
        asks.retain(|price, _| *price >= best_ask);
        if asks.first_key_value().map(|(price, _)| *price) != Some(best_ask) {
            return Err(replay_gap(
                "reported best ask is absent from reconstructed depth",
            ));
        }
    }
    Ok(())
}

fn validate_book(
    bids: &BTreeMap<rust_decimal::Decimal, rust_decimal::Decimal>,
    asks: &BTreeMap<rust_decimal::Decimal, rust_decimal::Decimal>,
) -> Result<(), DataSourceError> {
    if let Some(((bid, _), (ask, _))) = bids.last_key_value().zip(asks.first_key_value())
        && bid >= ask
    {
        return Err(replay_gap(format!(
            "book is crossed or locked: best_bid={bid} best_ask={ask}"
        )));
    }
    Ok(())
}

fn replay_gap(message: impl Into<String>) -> DataSourceError {
    DataSourceError::ReplayGap {
        message: message.into(),
    }
}

fn connected_since(client: &Client) -> Option<Instant> {
    match client.connection_state(ChannelType::Market) {
        ConnectionState::Connected { since } => Some(since),
        _ => None,
    }
}

fn observe_connection_state(
    client: &Client,
    observed_since: &mut Option<Instant>,
    connection_epoch: &mut i64,
    sequence: &mut u64,
    book: &mut TokenBook,
    epoch_counter: &AtomicI64,
) -> Result<(), DataSourceError> {
    let Some(since) = connected_since(client) else {
        return Ok(());
    };
    observe_connected_since(
        since,
        observed_since,
        connection_epoch,
        sequence,
        book,
        epoch_counter,
    )
}

fn observe_connected_since(
    since: Instant,
    observed_since: &mut Option<Instant>,
    connection_epoch: &mut i64,
    sequence: &mut u64,
    book: &mut TokenBook,
    epoch_counter: &AtomicI64,
) -> Result<(), DataSourceError> {
    if observed_since
        .as_ref()
        .is_some_and(|previous| *previous != since)
    {
        *connection_epoch = next_connection_epoch(epoch_counter)?;
        *sequence = 0;
        *book = TokenBook::default();
    }
    *observed_since = Some(since);
    Ok(())
}

fn market_event(
    market: &MarketId,
    outcome: Outcome,
    token: U256,
    book: &mut TokenBook,
    message: WsMessage,
) -> Result<Option<MarketEvent>, DataSourceError> {
    match message {
        WsMessage::Book(update) => book
            .replace(&update, token)
            .map(|matched| matched.then(|| book.event(market.clone(), outcome))),
        WsMessage::PriceChange(update) => book
            .apply(&update, token)
            .map(|matched| matched.then(|| book.event(market.clone(), outcome))),
        WsMessage::LastTradePrice(update) if update.asset_id == token => {
            if !book.initialized {
                return Ok(None);
            }
            trade_event(market.clone(), outcome, &update)
                .map(Some)
                .ok_or_else(|| replay_gap("invalid last trade price"))
        }
        _ => Ok(None),
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
                receipt_time_ms: now_ms(),
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

fn book_event(
    market: MarketId,
    outcome: Outcome,
    update: &BookUpdate,
) -> Result<MarketEvent, DataSourceError> {
    let token = update.asset_id;
    let mut book = TokenBook::default();
    if !book.replace(update, token)? {
        return Err(replay_gap("book snapshot token mismatch"));
    }
    Ok(book.event(market, outcome))
}

fn trade_event(market: MarketId, outcome: Outcome, update: &LastTradePrice) -> Option<MarketEvent> {
    let size = update.size?;
    if update.price <= rust_decimal::Decimal::ZERO
        || update.price > rust_decimal::Decimal::ONE
        || size <= rust_decimal::Decimal::ZERO
    {
        return None;
    }
    Some(MarketEvent::LastTrade {
        market,
        outcome,
        price: update.price,
        side: from_venue_side(update.side?)?,
        size,
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
        return book_event(tokens.market().clone(), outcome, &update);
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

fn unavailable(message: &str) -> DataSourceError {
    DataSourceError::Unavailable {
        message: message.to_owned(),
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
        time::{Duration, Instant},
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
        clob::ws::{BookUpdate, LastTradePrice, PriceChange, WsMessage},
        types::U256,
    };
    use rust_decimal::Decimal;

    use super::{
        RawFrameAdapterError, RawPolymarketFrameAdapter, TokenBook, book_event, market_event,
        next_connection_epoch, observe_connected_since, sequenced_market_signal, trade_event,
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
        } = book_event(market.clone(), Outcome::Up, &book)?
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
    fn token_book_applies_only_matching_batched_changes() -> Result<(), Box<dyn std::error::Error>>
    {
        let token = U256::from(1_u64);
        let snapshot: BookUpdate = serde_json::from_str(
            r#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[{"price":"0.49","size":"2"},{"price":"0.50","size":"1"}],"asks":[{"price":"0.51","size":"3"}]}"#,
        )?;
        let batch: PriceChange = serde_json::from_str(
            r#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"43","price_changes":[{"asset_id":"1","price":"0.49","size":"0","side":"BUY"},{"asset_id":"1","price":"0.48","size":"4","side":"BUY"},{"asset_id":"1","price":"0.51","size":"0","side":"SELL"},{"asset_id":"1","price":"0.52","size":"3","side":"SELL"},{"asset_id":"2","price":"0.01","size":"99","side":"SELL"}]}"#,
        )?;
        let mut book = TokenBook::default();
        assert!(book.replace(&snapshot, token)?);
        assert!(book.apply(&batch, token)?);

        assert_eq!(
            book.bids.iter().rev().collect::<Vec<_>>(),
            vec![
                (&Decimal::new(50, 2), &Decimal::ONE),
                (&Decimal::new(48, 2), &Decimal::from(4)),
            ]
        );
        assert_eq!(
            book.asks.iter().collect::<Vec<_>>(),
            vec![(&Decimal::new(52, 2), &Decimal::from(3))]
        );
        assert_eq!(book.timestamp_ms, 43);

        let MarketEvent::BookUpdate { bids, asks, .. } =
            book.event(MarketId::new("btc-5m")?, Outcome::Up)
        else {
            return Err("expected book update".into());
        };
        assert_eq!(
            bids,
            vec![
                (Decimal::new(50, 2), Decimal::ONE),
                (Decimal::new(48, 2), Decimal::from(4))
            ]
        );
        assert_eq!(asks, vec![(Decimal::new(52, 2), Decimal::from(3))]);

        let complement_only: PriceChange = serde_json::from_str(
            r#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"44","price_changes":[{"asset_id":"2","price":"0.02","size":"1","side":"BUY"}]}"#,
        )?;
        assert!(!book.apply(&complement_only, token)?);
        assert_eq!(book.timestamp_ms, 43);
        Ok(())
    }

    #[test]
    fn token_book_uses_reported_top_to_prune_stale_levels() -> Result<(), Box<dyn std::error::Error>>
    {
        let token = U256::from(1_u64);
        let snapshot: BookUpdate = serde_json::from_str(
            r#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[{"price":"0.41","size":"2"},{"price":"0.43","size":"1"}],"asks":[{"price":"0.44","size":"3"}]}"#,
        )?;
        let batch: PriceChange = serde_json::from_str(
            r#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"43","price_changes":[{"asset_id":"1","price":"0.42","size":"4","side":"SELL","best_bid":"0.41","best_ask":"0.42"}]}"#,
        )?;
        let mut book = TokenBook::default();
        book.replace(&snapshot, token)?;
        assert!(book.apply(&batch, token)?);
        assert_eq!(
            book.bids.last_key_value().map(|(price, _)| *price),
            Some(Decimal::new(41, 2))
        );
        assert_eq!(
            book.asks.first_key_value().map(|(price, _)| *price),
            Some(Decimal::new(42, 2))
        );
        Ok(())
    }

    #[test]
    fn token_book_holds_pre_snapshot_and_rejects_malformed_changes()
    -> Result<(), Box<dyn std::error::Error>> {
        let token = U256::from(1_u64);
        let delta: PriceChange = serde_json::from_str(
            r#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","price_changes":[{"asset_id":"1","price":"0.50","size":"1","side":"BUY"}]}"#,
        )?;
        let mut book = TokenBook::default();
        assert!(!book.apply(&delta, token)?);

        let snapshot: BookUpdate = serde_json::from_str(
            r#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[],"asks":[]}"#,
        )?;
        book.replace(&snapshot, token)?;
        for malformed in [
            r#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"43","price_changes":[{"asset_id":"1","price":"0.50","size":"1","side":"HOLD"}]}"#,
            r#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"43","price_changes":[{"asset_id":"1","price":"0.50","side":"BUY"}]}"#,
            r#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"43","price_changes":[{"asset_id":"1","price":"0","size":"1","side":"BUY"}]}"#,
            r#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"43","price_changes":[{"asset_id":"1","price":"1.01","size":"1","side":"BUY"}]}"#,
            r#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"43","price_changes":[{"asset_id":"1","price":"0.50","size":"-1","side":"BUY"}]}"#,
        ] {
            let malformed: PriceChange = serde_json::from_str(malformed)?;
            assert!(matches!(
                book.apply(&malformed, token),
                Err(DataSourceError::ReplayGap { .. })
            ));
            assert!(book.bids.is_empty());
            assert!(book.asks.is_empty());
        }
        Ok(())
    }

    #[test]
    fn token_book_accepts_ordered_messages_with_independent_times()
    -> Result<(), Box<dyn std::error::Error>> {
        let token = U256::from(1_u64);
        let market = MarketId::new("btc-5m")?;
        let snapshot: BookUpdate = serde_json::from_str(
            r#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[{"price":"0.49","size":"2"}],"asks":[{"price":"0.51","size":"3"}]}"#,
        )?;
        let trade: LastTradePrice = serde_json::from_str(
            r#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","price":"0.5","side":"BUY","size":"4","timestamp":"43"}"#,
        )?;
        let mut book = TokenBook::default();
        assert!(
            market_event(
                &market,
                Outcome::Up,
                token,
                &mut book,
                WsMessage::LastTradePrice(trade),
            )?
            .is_none()
        );
        book.replace(&snapshot, token)?;
        let regressed_trade: LastTradePrice = serde_json::from_str(
            r#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","price":"0.5","side":"BUY","size":"4","timestamp":"41"}"#,
        )?;
        assert!(
            market_event(
                &market,
                Outcome::Up,
                token,
                &mut book,
                WsMessage::LastTradePrice(regressed_trade),
            )?
            .is_some()
        );

        let crossed: PriceChange = serde_json::from_str(
            r#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"43","price_changes":[{"asset_id":"1","price":"0.52","size":"1","side":"BUY"}]}"#,
        )?;
        assert!(book.apply(&crossed, token).is_err());
        assert_eq!(
            book.bids.last_key_value().map(|(price, _)| *price),
            Some(Decimal::new(49, 2))
        );

        let regressed: PriceChange = serde_json::from_str(
            r#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"41","price_changes":[{"asset_id":"1","price":"0.48","size":"1","side":"BUY"}]}"#,
        )?;
        assert!(book.apply(&regressed, token)?);
        Ok(())
    }

    #[test]
    fn reconnect_resets_token_book_and_sequence() -> Result<(), Box<dyn std::error::Error>> {
        let token = U256::from(1_u64);
        let snapshot: BookUpdate = serde_json::from_str(
            r#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[{"price":"0.50","size":"1"}],"asks":[]}"#,
        )?;
        let mut book = TokenBook::default();
        book.replace(&snapshot, token)?;
        let epochs = AtomicI64::new(0);
        let mut epoch = next_connection_epoch(&epochs)?;
        let mut sequence = 7;
        let mut observed = None;
        let first_since = Instant::now();
        observe_connected_since(
            first_since,
            &mut observed,
            &mut epoch,
            &mut sequence,
            &mut book,
            &epochs,
        )?;
        observe_connected_since(
            first_since + Duration::from_secs(1),
            &mut observed,
            &mut epoch,
            &mut sequence,
            &mut book,
            &epochs,
        )?;
        assert_eq!(epoch, 1);
        assert_eq!(sequence, 0);
        assert!(!book.initialized);
        assert!(book.bids.is_empty());
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
        for invalid in [
            br#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[{"price":"1.01","size":"1"}],"asks":[]}"#.as_slice(),
            br#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[{"price":"0.49","size":"1"},{"price":"0.49","size":"2"}],"asks":[]}"#.as_slice(),
            br#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[{"price":"0.52","size":"1"}],"asks":[{"price":"0.51","size":"1"}]}"#.as_slice(),
        ] {
            assert!(super::parse_market_frame(invalid, &tokens).is_err());
        }
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
