use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use chrono::Utc;
use futures::{SinkExt as _, StreamExt as _};
use pmkit_core::MarketId;
use pmkit_data::{
    DataSourceError, LiveDataSource, RawPmAccountFrame, RawPmMarketFrame, SourceHeartbeat,
    SourceSignal,
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
use polymarket_client_sdk_v2::types::U256;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::public_protocol::{
    PublicInboundFrame, PublicMarketEvent, PublicOutboundFrame, decode_public_inbound,
    encode_public_outbound,
};
use crate::subscription::PublicSubscription;
use crate::{MarketTokens, from_venue_side};

const MARKET_SOURCE_ID: &str = "polymarket:market-ws";
const MARKET_WS_ENDPOINT: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const MARKET_PING_INTERVAL: Duration = Duration::from_secs(5);
const MARKET_PONG_TIMEOUT: Duration = Duration::from_secs(10);
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(0);

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
    tokens: MarketTokens,
    endpoint: Arc<str>,
    connection_epoch: Arc<AtomicI64>,
    ingest_sequence: Arc<AtomicU64>,
}

impl PolymarketLiveData {
    /// Creates a live source using Polymarket's public market WebSocket endpoint.
    ///
    /// `client` remains for API compatibility; raw market transport connects directly to the
    /// public endpoint and does not use the SDK client's WebSocket configuration.
    #[must_use]
    pub fn new(_client: Client, tokens: MarketTokens) -> Self {
        Self::with_endpoint(tokens, MARKET_WS_ENDPOINT)
    }

    fn with_endpoint(tokens: MarketTokens, endpoint: impl Into<Arc<str>>) -> Self {
        Self {
            tokens,
            endpoint: endpoint.into(),
            connection_epoch: Arc::new(AtomicI64::new(0)),
            ingest_sequence: Arc::new(AtomicU64::new(0)),
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one raw socket owns frame continuity and heartbeat lifecycle"
    )]
    async fn subscribe_transport(
        &self,
        outcome: Outcome,
        sink: Sender<SourceSignal>,
        ping_interval: Duration,
        pong_timeout: Duration,
    ) -> Result<(), DataSourceError> {
        let (mut socket, _) = connect_async(self.endpoint.as_ref())
            .await
            .map_err(|error| websocket_error("Polymarket market connection failed", error))?;
        let connection_epoch = next_connection_epoch(&self.connection_epoch)?;
        let connection_number = NEXT_CONNECTION_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| replay_gap("Polymarket connection id overflow"))?;
        let connection_id = format!("{}#{connection_number}", self.endpoint);
        let subscription = PublicSubscription::new(vec![self.tokens.token(outcome).to_string()]);
        socket
            .send(outbound_text(&PublicOutboundFrame::SubscriptionUpdate(
                subscription,
            ))?)
            .await
            .map_err(|error| websocket_error("Polymarket subscription send failed", error))?;

        let mut decoder = MarketFrameDecoder::new(self.tokens.clone(), outcome);
        let mut frame_sequence = 0;
        let mut ping_timer = tokio::time::interval(ping_interval);
        ping_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ping_timer.tick().await;
        let mut awaiting_pong = None;

        loop {
            tokio::select! {
                _ = ping_timer.tick() => {
                    if awaiting_pong.is_some_and(|sent: Instant| sent.elapsed() >= pong_timeout) {
                        return Err(DataSourceError::Unavailable {
                            message: "Polymarket market heartbeat timed out".into(),
                        });
                    }
                    if awaiting_pong.is_none() {
                        socket
                            .send(Message::Text("PING".into()))
                            .await
                            .map_err(|error| websocket_error("Polymarket heartbeat send failed", error))?;
                        awaiting_pong = Some(Instant::now());
                    }
                }
                incoming = socket.next() => {
                    let Some(message) = incoming else {
                        return Err(DataSourceError::Unavailable {
                            message: "Polymarket market stream ended".into(),
                        });
                    };
                    let message = message
                        .map_err(|error| websocket_error("Polymarket market stream failed", error))?;
                    let receipt_time_ms = Utc::now().timestamp_millis();
                    let (kind, raw_frame) = match message {
                        Message::Text(text) => (MarketSocketFrame::Data, text.as_bytes().to_vec()),
                        Message::Binary(bytes) => (MarketSocketFrame::Data, bytes.to_vec()),
                        Message::Ping(bytes) => (MarketSocketFrame::Ping, bytes.to_vec()),
                        Message::Pong(bytes) => (MarketSocketFrame::Pong, bytes.to_vec()),
                        Message::Close(frame) => {
                            let description = format!("{frame:?}");
                            let raw_frame = frame.map_or_else(Vec::new, |frame| {
                                let mut bytes = u16::from(frame.code).to_be_bytes().to_vec();
                                bytes.extend_from_slice(frame.reason.as_bytes());
                                bytes
                            });
                            let metadata = self.next_metadata(
                                &mut frame_sequence,
                                &connection_id,
                                connection_epoch,
                                receipt_time_ms,
                            )?;
                            send_heartbeat(&sink, metadata, raw_frame).await?;
                            return Err(DataSourceError::Unavailable {
                                message: format!("Polymarket market socket closed: {description}"),
                            });
                        }
                        Message::Frame(frame) => {
                            (MarketSocketFrame::Control, frame.payload().to_vec())
                        }
                    };
                    let mut metadata = self.next_metadata(
                        &mut frame_sequence,
                        &connection_id,
                        connection_epoch,
                        receipt_time_ms,
                    )?;
                    match kind {
                        MarketSocketFrame::Ping => {
                            socket
                                .flush()
                                .await
                                .map_err(|error| websocket_error("Polymarket pong failed", error))?;
                            awaiting_pong = None;
                            send_heartbeat(&sink, metadata, raw_frame).await?;
                        }
                        MarketSocketFrame::Pong => {
                            awaiting_pong = None;
                            send_heartbeat(&sink, metadata, raw_frame).await?;
                        }
                        MarketSocketFrame::Control => {
                            send_heartbeat(&sink, metadata, raw_frame).await?;
                        }
                        MarketSocketFrame::Data => {
                            if raw_frame == b"PONG" {
                                awaiting_pong = None;
                                send_heartbeat(&sink, metadata, raw_frame).await?;
                                continue;
                            }
                            if raw_frame == b"PING" {
                                socket
                                    .send(Message::Text("PONG".into()))
                                    .await
                                    .map_err(|error| websocket_error("Polymarket pong failed", error))?;
                                send_heartbeat(&sink, metadata, raw_frame).await?;
                                continue;
                            }
                            let inbound = match decode_public_inbound(&raw_frame) {
                                Ok(inbound) => inbound,
                                Err(error) => {
                                    send_heartbeat(&sink, metadata, raw_frame).await?;
                                    return Err(replay_gap(&format!(
                                        "malformed Polymarket public frame: {error}"
                                    )));
                                }
                            };
                            match inbound {
                                PublicInboundFrame::Market { event, .. } => {
                                    let fact = match decoder.project(event) {
                                        Ok(fact) => fact,
                                        Err(error) => {
                                            send_heartbeat(&sink, metadata, raw_frame).await?;
                                            return Err(error);
                                        }
                                    };
                                    if let Some(fact) = fact {
                                        metadata.source_time_ms = fact.timestamp_ms();
                                        sink.send(SourceSignal::Data(Box::new(
                                            SourceEnvelope::PmMarket(PmMarketEnvelope {
                                                metadata,
                                                raw_frame,
                                                fact,
                                            }),
                                        )))
                                        .await
                                        .map_err(|_| DataSourceError::SinkClosed)?;
                                    } else {
                                        send_heartbeat(&sink, metadata, raw_frame).await?;
                                    }
                                }
                                PublicInboundFrame::Ping { .. } => {
                                    socket
                                        .send(outbound_text(&PublicOutboundFrame::Pong)?)
                                        .await
                                        .map_err(|error| websocket_error("Polymarket pong failed", error))?;
                                    send_heartbeat(&sink, metadata, raw_frame).await?;
                                }
                                PublicInboundFrame::Pong { .. } => {
                                    awaiting_pong = None;
                                    send_heartbeat(&sink, metadata, raw_frame).await?;
                                }
                                PublicInboundFrame::SubscriptionUpdate { .. }
                                | PublicInboundFrame::UnknownControl { .. } => {
                                    send_heartbeat(&sink, metadata, raw_frame).await?;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn next_metadata(
        &self,
        frame_sequence: &mut i64,
        connection_id: &str,
        connection_epoch: i64,
        receipt_time_ms: i64,
    ) -> Result<StreamMetadata, DataSourceError> {
        *frame_sequence = frame_sequence
            .checked_add(1)
            .ok_or_else(|| replay_gap("Polymarket frame sequence overflow"))?;
        let ingest_sequence = self
            .ingest_sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| replay_gap("Polymarket ingest sequence overflow"))?;
        Ok(StreamMetadata {
            schema_version: 1,
            source_id: MARKET_SOURCE_ID.into(),
            source_time_ms: receipt_time_ms,
            canonical_source_rank: 0,
            receipt_time_ms,
            connection_id: connection_id.into(),
            connection_epoch,
            frame_sequence: *frame_sequence,
            ingest_sequence,
        })
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
        self.subscribe_transport(outcome, sink, MARKET_PING_INTERVAL, MARKET_PONG_TIMEOUT)
            .await
    }
}

#[derive(Clone, Copy)]
enum MarketSocketFrame {
    Data,
    Ping,
    Pong,
    Control,
}

fn outbound_text(frame: &PublicOutboundFrame) -> Result<Message, DataSourceError> {
    let bytes = encode_public_outbound(frame)
        .map_err(|error| replay_gap(&format!("cannot encode Polymarket control frame: {error}")))?;
    let text = String::from_utf8(bytes)
        .map_err(|error| replay_gap(&format!("Polymarket control frame was not UTF-8: {error}")))?;
    Ok(Message::Text(text.into()))
}

async fn send_heartbeat(
    sink: &Sender<SourceSignal>,
    metadata: StreamMetadata,
    raw_frame: Vec<u8>,
) -> Result<(), DataSourceError> {
    sink.send(SourceSignal::Heartbeat(SourceHeartbeat {
        metadata,
        raw_frame,
    }))
    .await
    .map_err(|_| DataSourceError::SinkClosed)
}

fn websocket_error(context: &str, error: impl fmt::Display) -> DataSourceError {
    DataSourceError::Unavailable {
        message: format!("{context}: {error}"),
    }
}

#[derive(Debug, Default)]
struct TokenBook {
    bids: BTreeMap<rust_decimal::Decimal, rust_decimal::Decimal>,
    asks: BTreeMap<rust_decimal::Decimal, rust_decimal::Decimal>,
    timestamp_ms: i64,
    snapshot_timestamp_ms: i64,
    initialized: bool,
}

impl TokenBook {
    fn replace(&mut self, update: &BookUpdate, token: U256) -> Result<(), DataSourceError> {
        if update.asset_id != token {
            return Err(replay_gap("book snapshot has the wrong asset id"));
        }
        if self.initialized && update.timestamp <= self.timestamp_ms {
            return Err(replay_gap("stale book snapshot"));
        }
        self.bids = levels(&update.bids)?;
        self.asks = levels(&update.asks)?;
        self.timestamp_ms = update.timestamp;
        self.snapshot_timestamp_ms = update.timestamp;
        self.initialized = true;
        Ok(())
    }

    fn apply(&mut self, update: &PriceChange, token: U256) -> Result<bool, DataSourceError> {
        let matching = update
            .price_changes
            .iter()
            .filter(|change| change.asset_id == token)
            .collect::<Vec<_>>();
        if matching.is_empty() {
            return Ok(false);
        }
        if !self.initialized {
            return Err(replay_gap("price change before initial book snapshot"));
        }
        if update.timestamp < self.timestamp_ms || update.timestamp == self.snapshot_timestamp_ms {
            return Err(replay_gap("stale price change"));
        }
        let mut seen_levels = BTreeSet::new();
        for change in &matching {
            let size = change
                .size
                .ok_or_else(|| replay_gap("price change lacks size"))?;
            if size.is_sign_negative() {
                return Err(replay_gap("price change has negative size"));
            }
            let side_code = match change.side {
                VenueSide::Buy => 0,
                VenueSide::Sell => 1,
                _ => return Err(replay_gap("price change has unknown side")),
            };
            if !seen_levels.insert((change.price, side_code)) {
                return Err(replay_gap("price change contains duplicate levels"));
            }
        }
        for change in &matching {
            let size = change
                .size
                .ok_or_else(|| replay_gap("price change lacks size"))?;
            let side_code = match change.side {
                VenueSide::Buy => 0,
                VenueSide::Sell => 1,
                _ => return Err(replay_gap("price change has unknown side")),
            };
            let target_levels = if side_code == 0 {
                &mut self.bids
            } else {
                &mut self.asks
            };
            if size.is_zero() {
                target_levels.remove(&change.price);
            } else {
                target_levels.insert(change.price, size);
            }
        }
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

/// Stateful market-frame projection for one known market outcome.
///
/// Supply the already-known market token mapping and outcome; this decoder does not perform token
/// discovery. Unprojected, unrelated-outcome, and control frames return `Ok(None)` while retaining
/// the book state for subsequent deltas.
#[derive(Debug)]
pub struct MarketFrameDecoder {
    tokens: MarketTokens,
    outcome: Outcome,
    book: TokenBook,
}

impl MarketFrameDecoder {
    /// Creates a decoder scoped to one known market and outcome token.
    #[must_use]
    pub fn new(tokens: MarketTokens, outcome: Outcome) -> Self {
        Self {
            tokens,
            outcome,
            book: TokenBook::default(),
        }
    }

    /// Projects one raw market frame while preserving state across snapshots and deltas.
    ///
    /// Returns `None` for controls, unsupported facts, or frames for the other outcome. A price
    /// change before its snapshot fails closed.
    ///
    /// # Errors
    ///
    /// Returns [`DataSourceError::ReplayGap`] for malformed frames or invalid book transitions.
    pub fn project_frame(&mut self, raw: &[u8]) -> Result<Option<MarketEvent>, DataSourceError> {
        self.decode(raw).map(|(_, fact)| fact)
    }

    pub(crate) fn decode(
        &mut self,
        raw: &[u8],
    ) -> Result<(MarketFrameKind, Option<MarketEvent>), DataSourceError> {
        let frame = decode_public_inbound(raw)
            .map_err(|error| replay_gap(&format!("malformed Polymarket public frame: {error}")))?;
        match frame {
            PublicInboundFrame::Market { event, .. } => {
                Ok((MarketFrameKind::Market, self.project(event)?))
            }
            PublicInboundFrame::Ping { .. } => Ok((MarketFrameKind::Ping, None)),
            PublicInboundFrame::Pong { .. } => Ok((MarketFrameKind::Pong, None)),
            PublicInboundFrame::SubscriptionUpdate { .. }
            | PublicInboundFrame::UnknownControl { .. } => Ok((MarketFrameKind::Other, None)),
        }
    }

    fn project(
        &mut self,
        event: PublicMarketEvent,
    ) -> Result<Option<MarketEvent>, DataSourceError> {
        let token = self.tokens.token(self.outcome);
        let market = self.tokens.market().clone();
        match event {
            PublicMarketEvent::OrderbookSnapshot(update) if update.asset_id == token => {
                self.book.replace(&update, token)?;
                Ok(Some(self.book.event(market, self.outcome)))
            }
            PublicMarketEvent::PriceChange(update) => self
                .book
                .apply(&update, token)?
                .then(|| self.book.event(market, self.outcome))
                .map_or(Ok(None), |event| Ok(Some(event))),
            PublicMarketEvent::LastTradePrice(update) if update.asset_id == token => {
                trade_event(market, self.outcome, &update)
                    .map(Some)
                    .ok_or_else(|| replay_gap("incomplete last trade price"))
            }
            PublicMarketEvent::BestBidAsk(update) if update.asset_id == token => {
                Ok(Some(MarketEvent::BestBidAsk {
                    market,
                    outcome: self.outcome,
                    bid: update.best_bid,
                    ask: update.best_ask,
                    timestamp_ms: update.timestamp,
                }))
            }
            PublicMarketEvent::OrderbookSnapshot(_)
            | PublicMarketEvent::LastTradePrice(_)
            | PublicMarketEvent::BestBidAsk(_)
            | PublicMarketEvent::TickSizeChange(_)
            | PublicMarketEvent::NewMarket(_)
            | PublicMarketEvent::MarketResolved(_) => Ok(None),
        }
    }
}

#[derive(Clone, Copy)]
pub enum MarketFrameKind {
    Market,
    Ping,
    Pong,
    Other,
}

/// Parses a raw Polymarket market frame into a neutral market event.
///
/// # Errors
///
/// Returns [`DataSourceError::ReplayGap`] when the frame is malformed, has no
/// supported strategy projection, or cannot be applied to a complete book.
pub fn parse_market_frame(
    raw: &[u8],
    tokens: &MarketTokens,
) -> Result<MarketEvent, DataSourceError> {
    let frame = decode_public_inbound(raw)
        .map_err(|error| replay_gap(&format!("malformed Polymarket public frame: {error}")))?;
    let PublicInboundFrame::Market { event, .. } = frame else {
        return Err(replay_gap("frame has no Polymarket market projection"));
    };
    let outcome = match &event {
        PublicMarketEvent::OrderbookSnapshot(update) => tokens.outcome(&update.asset_id),
        PublicMarketEvent::PriceChange(update) => update
            .price_changes
            .iter()
            .find_map(|change| tokens.outcome(&change.asset_id)),
        PublicMarketEvent::LastTradePrice(update) => tokens.outcome(&update.asset_id),
        PublicMarketEvent::TickSizeChange(update) => tokens.outcome(&update.asset_id),
        PublicMarketEvent::BestBidAsk(update) => tokens.outcome(&update.asset_id),
        PublicMarketEvent::NewMarket(update) => update
            .asset_ids
            .iter()
            .find_map(|asset_id| tokens.outcome(asset_id)),
        PublicMarketEvent::MarketResolved(update) => update
            .asset_ids
            .iter()
            .find_map(|asset_id| tokens.outcome(asset_id)),
    }
    .ok_or_else(|| replay_gap("unknown market asset id"))?;
    let mut decoder = MarketFrameDecoder::new(tokens.clone(), outcome);
    decoder
        .project(event)?
        .ok_or_else(|| replay_gap("unrecognized Polymarket market frame"))
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
        time::Duration,
    };

    use async_trait::async_trait;
    use futures::{SinkExt as _, StreamExt as _};
    use pmkit_core::{MarketId, PortfolioId, RunId};
    use pmkit_data::{DataSourceError, RawPmAccountFrame, RawPmMarketFrame, SourceSignal};
    use pmkit_event::{MarketEvent, PmAccountEvent, SourceEnvelope, StreamMetadata};

    use pmkit_market::Outcome;
    use pmkit_store::{
        CausalDecision, CausalIdentity, IntentOutcome, OwnerScope, PmEnvelope, ReplayCursor,
        ReplayPage, StoreError, TapeStore, TursoTapeStore,
    };
    use polymarket_client_sdk_v2::{
        clob::ws::{BookUpdate, PriceChange},
        types::U256,
    };
    use rust_decimal::Decimal;
    use tokio::{
        net::TcpListener,
        sync::{mpsc, oneshot},
        time::timeout,
    };
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    use super::{PolymarketLiveData, RawFrameAdapterError, RawPolymarketFrameAdapter, TokenBook};

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

    const SUBSCRIPTION_ACK: &str = r#"{"event_type":"subscription_update","assets_ids":["1"]}"#;
    const OTHER_OUTCOME: &str = r#"{"event_type":"best_bid_ask","market":"0x0000000000000000000000000000000000000000000000000000000000000001","asset_id":"2","best_bid":"0.48","best_ask":"0.52","spread":"0.04","timestamp":"41"}"#;
    const SNAPSHOT: &str = r#"{"event_type":"book","market":"0x0000000000000000000000000000000000000000000000000000000000000001","asset_id":"1","timestamp":"42","bids":[{"price":"0.49","size":"2"}],"asks":[{"price":"0.51","size":"3"}]}"#;
    const DELTA: &str = r#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"43","price_changes":[{"asset_id":"1","price":"0.50","size":"4","side":"BUY"}]}"#;
    const BEST_BID_ASK: &str = r#"{"event_type":"best_bid_ask","market":"0x0000000000000000000000000000000000000000000000000000000000000001","asset_id":"1","best_bid":"0.50","best_ask":"0.51","spread":"0.01","timestamp":"44"}"#;
    const TRADE: &str = r#"{"event_type":"last_trade_price","market":"0x0000000000000000000000000000000000000000000000000000000000000001","asset_id":"1","price":"0.505","side":"SELL","size":"1.25","timestamp":"45"}"#;

    fn test_tokens() -> Result<crate::MarketTokens, Box<dyn std::error::Error>> {
        Ok(crate::MarketTokens::new(
            MarketId::new("btc-5m")?,
            U256::from(1_u64),
            U256::from(2_u64),
        ))
    }

    fn assert_transport_metadata(
        metadata: &StreamMetadata,
        frame_sequence: i64,
        started_at_ms: i64,
        endpoint: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(metadata.source_id, "polymarket:market-ws");
        assert_eq!(metadata.frame_sequence, frame_sequence);
        assert_eq!(metadata.ingest_sequence, u64::try_from(frame_sequence - 1)?);
        assert!(metadata.receipt_time_ms >= started_at_ms);
        assert!(metadata.receipt_time_ms <= chrono::Utc::now().timestamp_millis());
        assert!(metadata.connection_id.starts_with(&format!("{endpoint}#")));
        assert_ne!(metadata.connection_id, metadata.source_id);
        Ok(())
    }

    fn spawn_market_socket_peer(
        listener: TcpListener,
        ping_seen_tx: oneshot::Sender<()>,
        resume_rx: oneshot::Receiver<()>,
    ) -> tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            let Some(Ok(Message::Text(subscription))) = socket.next().await else {
                return Err("expected market subscription".into());
            };
            let subscription: serde_json::Value = serde_json::from_str(subscription.as_ref())?;
            assert_eq!(subscription["type"], "market");
            assert_eq!(subscription["operation"], "subscribe");
            assert_eq!(subscription["assets_ids"], serde_json::json!(["1"]));
            assert!(
                subscription["initial_dump"]
                    .as_bool()
                    .ok_or("subscription initial_dump must be a boolean")?
            );

            socket.send(Message::Text(SUBSCRIPTION_ACK.into())).await?;
            socket.send(Message::Text(OTHER_OUTCOME.into())).await?;
            let Some(Ok(Message::Text(ping))) =
                timeout(Duration::from_secs(1), socket.next()).await?
            else {
                return Err("expected client PING".into());
            };
            let ping: &str = ping.as_ref();
            assert_eq!(ping, "PING");
            ping_seen_tx
                .send(())
                .map_err(|()| "client ping observer was dropped")?;
            resume_rx.await?;

            let native_ping = b"actual-native-ping";
            socket
                .send(Message::Ping(native_ping.to_vec().into()))
                .await?;
            loop {
                match socket.next().await {
                    Some(Ok(Message::Pong(payload))) => {
                        assert_eq!(payload.as_ref(), native_ping);
                        break;
                    }
                    Some(Ok(Message::Text(ping))) => {
                        let ping: &str = ping.as_ref();
                        assert_eq!(ping, "PING");
                    }
                    other => return Err(format!("expected native pong, received {other:?}").into()),
                }
            }
            socket.send(Message::Text("PONG".into())).await?;
            for frame in [SNAPSHOT, DELTA, BEST_BID_ASK, TRADE] {
                socket.send(Message::Text(frame.into())).await?;
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })
    }

    async fn assert_captured_market_rows(
        signals: &mut mpsc::Receiver<SourceSignal>,
        started_at_ms: i64,
        endpoint: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for (index, raw) in [SNAPSHOT, DELTA, BEST_BID_ASK, TRADE]
            .into_iter()
            .enumerate()
        {
            let Some(SourceSignal::Data(data)) =
                timeout(Duration::from_secs(1), signals.recv()).await?
            else {
                return Err("expected a projected market fact".into());
            };
            let SourceEnvelope::PmMarket(envelope) = *data else {
                return Err("expected a Polymarket market envelope".into());
            };
            let frame_sequence = i64::try_from(index + 5)?;
            assert_eq!(envelope.raw_frame, raw.as_bytes());
            assert_transport_metadata(&envelope.metadata, frame_sequence, started_at_ms, endpoint)?;
            assert_eq!(envelope.metadata.source_time_ms, i64::try_from(index + 42)?);
            assert_ne!(
                envelope.metadata.source_time_ms,
                envelope.metadata.receipt_time_ms
            );
            match (index, envelope.fact) {
                (
                    0,
                    MarketEvent::BookUpdate {
                        outcome: Outcome::Up,
                        bids,
                        asks,
                        timestamp_ms: 42,
                        ..
                    },
                ) => {
                    assert_eq!(bids, vec![(Decimal::new(49, 2), Decimal::from(2))]);
                    assert_eq!(asks, vec![(Decimal::new(51, 2), Decimal::from(3))]);
                }
                (
                    1,
                    MarketEvent::BookUpdate {
                        outcome: Outcome::Up,
                        bids,
                        timestamp_ms: 43,
                        ..
                    },
                ) => assert_eq!(
                    bids,
                    vec![
                        (Decimal::new(50, 2), Decimal::from(4)),
                        (Decimal::new(49, 2), Decimal::from(2)),
                    ]
                ),
                (
                    2,
                    MarketEvent::BestBidAsk {
                        outcome: Outcome::Up,
                        bid,
                        ask,
                        timestamp_ms: 44,
                        ..
                    },
                ) => {
                    assert_eq!(bid, Decimal::new(50, 2));
                    assert_eq!(ask, Decimal::new(51, 2));
                }
                (
                    3,
                    MarketEvent::LastTrade {
                        outcome: Outcome::Up,
                        price,
                        side,
                        size,
                        timestamp_ms: 45,
                        ..
                    },
                ) => {
                    assert_eq!(price, Decimal::new(505, 3));
                    assert_eq!(side, pmkit_book::Side::Sell);
                    assert_eq!(size, Decimal::new(125, 2));
                }
                (_, fact) => return Err(format!("unexpected projection {fact:?}").into()),
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn raw_socket_preserves_market_and_control_frames_with_received_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let source = PolymarketLiveData::with_endpoint(test_tokens()?, endpoint.clone());
        let started_at_ms = chrono::Utc::now().timestamp_millis();
        let (sink, mut signals) = mpsc::channel(16);
        let (ping_seen_tx, ping_seen_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        let server = spawn_market_socket_peer(listener, ping_seen_tx, resume_rx);

        let run_source = source.clone();
        let run = tokio::spawn(async move {
            run_source
                .subscribe_transport(
                    Outcome::Up,
                    sink,
                    Duration::from_millis(10),
                    Duration::from_millis(500),
                )
                .await
        });

        for (sequence, raw) in [
            (1, SUBSCRIPTION_ACK.as_bytes()),
            (2, OTHER_OUTCOME.as_bytes()),
        ] {
            let Some(SourceSignal::Heartbeat(heartbeat)) =
                timeout(Duration::from_secs(1), signals.recv()).await?
            else {
                return Err("expected a raw control heartbeat".into());
            };
            assert_eq!(heartbeat.raw_frame, raw);
            assert_transport_metadata(&heartbeat.metadata, sequence, started_at_ms, &endpoint)?;
            assert_eq!(
                heartbeat.metadata.source_time_ms,
                heartbeat.metadata.receipt_time_ms
            );
        }
        timeout(Duration::from_secs(1), ping_seen_rx).await??;
        assert!(
            timeout(Duration::from_millis(30), signals.recv())
                .await
                .is_err()
        );
        resume_tx
            .send(())
            .map_err(|()| "market socket server is not waiting to resume")?;

        for (sequence, raw) in [
            (3, b"actual-native-ping".as_slice()),
            (4, b"PONG".as_slice()),
        ] {
            let Some(SourceSignal::Heartbeat(heartbeat)) =
                timeout(Duration::from_secs(1), signals.recv()).await?
            else {
                return Err("expected an actual received heartbeat".into());
            };
            assert_eq!(heartbeat.raw_frame, raw);
            assert_transport_metadata(&heartbeat.metadata, sequence, started_at_ms, &endpoint)?;
            assert_eq!(
                heartbeat.metadata.source_time_ms,
                heartbeat.metadata.receipt_time_ms
            );
        }

        assert_captured_market_rows(&mut signals, started_at_ms, &endpoint).await?;

        server
            .await?
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        assert!(matches!(
            timeout(Duration::from_secs(1), run).await??,
            Err(DataSourceError::Unavailable { .. })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn missing_market_pong_times_out_without_synthetic_heartbeat()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            socket
                .next()
                .await
                .ok_or("market subscription frame missing")??;
            let Some(Ok(Message::Text(ping))) = socket.next().await else {
                return Err("expected client PING".into());
            };
            let ping: &str = ping.as_ref();
            assert_eq!(ping, "PING");
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });

        let source = PolymarketLiveData::with_endpoint(test_tokens()?, endpoint);
        let (sink, mut signals) = mpsc::channel(4);
        let error = timeout(
            Duration::from_secs(1),
            source.subscribe_transport(
                Outcome::Up,
                sink,
                Duration::from_millis(5),
                Duration::from_millis(25),
            ),
        )
        .await?
        .err()
        .ok_or("missing PONG must fail closed")?;
        assert!(error.to_string().contains("heartbeat timed out"));
        assert!(signals.try_recv().is_err());
        server
            .await?
            .map_err(|error| std::io::Error::other(error.to_string()))?;
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
    fn equal_timestamp_book_replacement_after_delta_fails_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let token = U256::from(1_u64);
        let snapshot: BookUpdate = serde_json::from_str(
            r#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[{"price":"0.49","size":"2"}],"asks":[{"price":"0.51","size":"3"}]}"#,
        )?;
        let applied_delta: PriceChange = serde_json::from_str(
            r#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"43","price_changes":[{"asset_id":"1","price":"0.52","size":"2","side":"SELL"}]}"#,
        )?;
        let equal_replacement: BookUpdate = serde_json::from_str(
            r#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"43","bids":[],"asks":[]}"#,
        )?;
        let later_replacement: BookUpdate = serde_json::from_str(
            r#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"44","bids":[],"asks":[]}"#,
        )?;
        let mut book = TokenBook::default();
        book.replace(&snapshot, token)?;
        assert!(book.apply(&applied_delta, token)?);
        assert!(matches!(
            book.replace(&equal_replacement, token),
            Err(DataSourceError::ReplayGap { .. })
        ));
        book.replace(&later_replacement, token)?;
        assert_eq!(book.timestamp_ms, 44);
        Ok(())
    }

    #[test]
    fn price_change_rejects_duplicate_levels_without_mutating_book()
    -> Result<(), Box<dyn std::error::Error>> {
        let token = U256::from(1_u64);
        let snapshot: BookUpdate = serde_json::from_str(
            r#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[{"price":"0.49","size":"2"}],"asks":[]}"#,
        )?;
        let duplicate: PriceChange = serde_json::from_str(
            r#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"43","price_changes":[{"asset_id":"1","price":"0.48","size":"1","side":"BUY"},{"asset_id":"1","price":"0.48","size":"2","side":"BUY"}]}"#,
        )?;
        let mut book = TokenBook::default();
        book.replace(&snapshot, token)?;
        assert!(matches!(
            book.apply(&duplicate, token),
            Err(DataSourceError::ReplayGap { message })
                if message == "price change contains duplicate levels"
        ));
        assert_eq!(book.timestamp_ms, 42);
        let MarketEvent::BookUpdate { bids, .. } =
            book.event(MarketId::new("btc-5m")?, Outcome::Up)
        else {
            return Err("expected book update".into());
        };
        assert_eq!(bids, vec![(Decimal::new(49, 2), Decimal::from(2))]);
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
        let raw = br#"{"event_type":"book","asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[{"price":"0.49","size":"2"}],"asks":[{"price":"0.51","size":"3"}]}"#;
        let event = super::parse_market_frame(raw, &tokens)?;
        let pmkit_event::MarketEvent::BookUpdate { timestamp_ms, .. } = event else {
            return Err("expected book update".into());
        };
        assert_eq!(timestamp_ms, 42);
        Ok(())
    }

    #[test]
    fn parse_market_frame_rejects_sdk_normalized_book_json()
    -> Result<(), Box<dyn std::error::Error>> {
        let tokens = test_tokens()?;
        let sdk_normalized = br#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[{"price":"0.49","size":"2"}],"asks":[{"price":"0.51","size":"3"}]}"#;

        assert!(matches!(
            super::parse_market_frame(sdk_normalized, &tokens),
            Err(DataSourceError::ReplayGap { .. })
        ));
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
