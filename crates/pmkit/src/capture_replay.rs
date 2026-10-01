//! Explicit replay of a raw run capture in its writer-assigned ingest order.
//!
//! This source deliberately does not implement [`pmkit_data::HistoricalDataSource`]:
//! ordinary backtests pass history through the event-time merge, which can reorder
//! observations from a capture. A recorded-order driver must consume
//! [`CaptureReplayReader`] directly (or use [`CaptureReplaySource::replay_recorded_order`])
//! and dispatch each `SourceSignal` before reading the next one. A future
//! `BacktestRun` recorded-order option should bypass `MergedFeed`; do not register
//! this source as ordinary `ReplaySpec` history.

use std::collections::HashMap;
use std::io::BufRead;

use pmkit_book::Side;
use pmkit_core::{MarketId, PortfolioId, StrategyId};
use pmkit_data::{
    BINANCE_VISION_SOURCE_ID, DataSourceError, GapRange as SourceGapRange,
    GapSequenceDomain as SourceGapSequenceDomain, SourceGap, SourceHeartbeat, SourceSignal,
    parse_binance_agg_trade_live,
};
use pmkit_event::{
    CexReferenceEnvelope, CexReferenceEvent, FillIdentity, Liquidity, MarketEvent,
    PmAccountEnvelope, PmAccountEvent, PmMarketEnvelope, SettlementIdentity, SourceEnvelope,
    StreamMetadata,
};
use pmkit_market::{Asset, Exchange, Outcome};
use pmkit_polymarket::{
    MarketFrameDecoder, MarketTokens, POLYBOLT_SOURCE_ID, PolyBoltDecoder, PolyBoltEvent,
    PolyBoltRawFrame,
};
use pmkit_tape::{
    GapRange, GapSequenceDomain, RawCaptureDraft, RawCaptureError, RawCapturePayload,
    RawCaptureReader, RawCaptureRecord,
};
use rust_decimal::Decimal;
use serde_json::Value;
use thiserror::Error;
use tokio::sync::mpsc;

/// A checked capture reader that emits source signals in writer-assigned order.
pub struct CaptureReplayReader<R: BufRead> {
    reader: RawCaptureReader<R>,
    gap: Option<RecordedGap>,
    stopped: bool,
    projection: ReplayProjectionState,
}

impl<R: BufRead> std::fmt::Debug for CaptureReplayReader<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CaptureReplayReader")
            .field("gap", &self.gap)
            .field("stopped", &self.stopped)
            .finish_non_exhaustive()
    }
}

impl<R: BufRead> CaptureReplayReader<R> {
    /// Creates a strict replay reader over one raw capture tape.
    #[must_use]
    pub fn new(reader: R) -> Self {
        Self::with_market_token_context(reader, None)
    }

    /// Creates a replay reader with caller-supplied, known Polymarket token mappings.
    ///
    /// Each map key must equal the market carried by its [`MarketTokens`]. The
    /// exact native Polymarket market-WebSocket source is decoded only against
    /// this context; token IDs and logical markets are never inferred from a
    /// capture witness or raw asset label.
    #[must_use]
    pub fn new_with_market_tokens(
        reader: R,
        market_tokens: HashMap<MarketId, MarketTokens>,
    ) -> Self {
        Self::with_market_token_context(reader, Some(market_tokens))
    }

    fn with_market_token_context(
        reader: R,
        polymarket_market_tokens: Option<HashMap<MarketId, MarketTokens>>,
    ) -> Self {
        Self {
            reader: RawCaptureReader::new_audit(reader),
            gap: None,
            stopped: false,
            projection: ReplayProjectionState {
                binance_frames: HashMap::new(),
                polymarket_market_tokens,
                polymarket_decoders: HashMap::new(),
                polymarket_frames: HashMap::new(),
                polybolt_decoders: HashMap::new(),
                polybolt_frames: HashMap::new(),
            },
        }
    }

    /// Returns the next captured signal, or `None` at clean EOF.
    ///
    /// Data envelopes retain exact source bytes and capture metadata. Heartbeats
    /// remain controls; gaps are emitted once and then become a terminal replay
    /// error on the next read. The normalized projection is required as the
    /// typed fact witness. Binance aggregate-trade, `PolyBolt` TWAP, and the exact
    /// native Polymarket market-WebSocket source are decoded and checked against
    /// that witness. Historical sources without original receipt-time provenance
    /// (including Binance Vision and `PMKit` Cloud) are rejected. Native PM replay
    /// requires caller-supplied [`MarketTokens`]; other source IDs remain
    /// witness-backed and are not claimed to use the native protocol.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt/incomplete tapes, invalid projection
    /// witnesses, unsupported control records, or a recorded gap after that gap
    /// has been delivered to the caller.
    pub fn next_signal(&mut self) -> Result<Option<SourceSignal>, CaptureReplayError> {
        if self.stopped {
            return Ok(None);
        }
        if let Some(gap) = self.gap.take() {
            self.stopped = true;
            return Err(CaptureReplayError::Gap {
                ingest_sequence: gap.ingest_sequence,
                source_id: gap.source_id,
                range: gap.range,
                unresolved_count: gap.unresolved_count,
                reason: gap.reason,
            });
        }
        let result = match self.reader.next_record() {
            Ok(Some(record))
                if matches!(
                    record.capture.source_id.as_str(),
                    BINANCE_VISION_SOURCE_ID | "pmkit-cloud"
                ) =>
            {
                Err(CaptureReplayError::UnsupportedBinanceVisionArchive {
                    ingest_sequence: record.ingest_sequence,
                })
            }
            Ok(Some(record))
                if matches!(&record.capture.payload, RawCapturePayload::Gap { .. }) =>
            {
                project_gap(record).map(|(signal, gap)| {
                    self.gap = Some(gap);
                    Some(signal)
                })
            }
            Ok(Some(record)) => project_record(record, &mut self.projection).map(Some),
            Ok(None) => validate_polybolt_eof(&self.projection.polybolt_frames)
                .and_then(|()| validate_polymarket_eof(&self.projection.polymarket_frames))
                .map(|()| None),
            Err(error) => Err(CaptureReplayError::Tape(error)),
        };
        if result.is_err() {
            self.stopped = true;
        }
        result
    }
}

/// A source wrapper for callers implementing an explicit recorded-order driver.
pub struct CaptureReplaySource<R: BufRead> {
    reader: CaptureReplayReader<R>,
}

impl<R: BufRead> std::fmt::Debug for CaptureReplaySource<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CaptureReplaySource")
            .field("reader", &self.reader)
            .finish()
    }
}

impl<R: BufRead> CaptureReplaySource<R> {
    /// Creates a recorded-order capture source.
    #[must_use]
    pub fn new(reader: R) -> Self {
        Self {
            reader: CaptureReplayReader::new(reader),
        }
    }

    /// Creates a recorded-order source with explicit known Polymarket token mappings.
    #[must_use]
    pub fn new_with_market_tokens(
        reader: R,
        market_tokens: HashMap<MarketId, MarketTokens>,
    ) -> Self {
        Self {
            reader: CaptureReplayReader::new_with_market_tokens(reader, market_tokens),
        }
    }

    /// Sends captured signals in append order, then EOF; a gap is sent before
    /// returning its fail-closed error. The receiver is not event-time merged.
    ///
    /// # Errors
    ///
    /// Returns on tape/projection failure, a recorded gap, or a closed sink.
    pub async fn replay_recorded_order(
        mut self,
        sink: mpsc::Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        loop {
            let signal = self
                .reader
                .next_signal()
                .map_err(|error| replay_source_error(&error))?;
            let Some(signal) = signal else {
                sink.send(SourceSignal::Eof)
                    .await
                    .map_err(|_| DataSourceError::SinkClosed)?;
                return Ok(());
            };
            sink.send(signal)
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
        }
    }
}

/// A failure while validating or replaying a raw capture.
#[derive(Debug, Error)]
pub enum CaptureReplayError {
    /// The generic capture tape failed strict integrity or sequence validation.
    #[error(transparent)]
    Tape(#[from] RawCaptureError),
    /// Native Polymarket market frames require explicit token mappings.
    #[error(
        "native Polymarket replay at ingest {ingest_sequence} for {source_id} requires explicit MarketTokens context"
    )]
    MissingMarketTokens {
        /// Ingest sequence of the native Polymarket record.
        ingest_sequence: u64,
        /// Exact native source identifier.
        source_id: String,
    },
    /// A historical source record does not retain its original receipt time.
    #[error(
        "historical source at ingest {ingest_sequence} has no original receipt time and cannot be replayed as a capture"
    )]
    UnsupportedBinanceVisionArchive {
        /// Ingest sequence of the historical record.
        ingest_sequence: u64,
    },
    /// A verified record declared a gap; the gap signal was already emitted.
    #[error(
        "recorded capture gap at ingest {ingest_sequence} for {source_id}: range {range:?}, unresolved count {unresolved_count:?}: {reason}"
    )]
    Gap {
        /// Ingest sequence of the captured gap.
        ingest_sequence: u64,
        /// Source that reported the missing frames.
        source_id: String,
        /// Missing range in its declared sequence domain.
        range: Option<GapRange>,
        /// Reported loss count when exact bounds are unavailable.
        unresolved_count: Option<u64>,
        /// Source-provided explanation.
        reason: String,
    },
    /// A tape gap could not be recovered from the validated record bytes.
    #[error("validated capture gap record was unavailable")]
    MalformedGapRecord,
    /// The fact witness did not match the capture metadata or expected schema.
    #[error("invalid normalized capture witness at ingest {ingest_sequence}: {message}")]
    Projection {
        /// Ingest sequence of the invalid record.
        ingest_sequence: u64,
        /// Validation failure detail.
        message: String,
    },
}

#[derive(Debug, Clone)]
struct RecordedGap {
    ingest_sequence: u64,
    source_id: String,
    range: Option<GapRange>,
    unresolved_count: Option<u64>,
    reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SourceConnection {
    source_id: String,
    connection_id: String,
    connection_epoch: u64,
}

#[derive(Debug, Clone)]
struct BinanceFrameProjection {
    frame_sequence: u64,
    next_projection_index: u64,
    fact: CexReferenceEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PolyBoltFrameKey {
    source_id: String,
    connection_epoch: u64,
    frame_sequence: u64,
}

struct PolyBoltDecoderState {
    connection_epoch: u64,
    decoder: PolyBoltDecoder,
}

struct PolyBoltFrameProjection {
    event: PolyBoltEvent,
    next_projection_index: u64,
    projection_count: u64,
    first_ingest_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PolymarketDecoderKey {
    source: SourceConnection,
    market: MarketId,
    outcome: Outcome,
}

struct PolymarketFrameProjection {
    frame_sequence: u64,
    raw_frame: Vec<u8>,
    unmatched_facts: Vec<MarketEvent>,
    first_ingest_sequence: u64,
}

struct ReplayProjectionState {
    binance_frames: HashMap<SourceConnection, BinanceFrameProjection>,
    polymarket_market_tokens: Option<HashMap<MarketId, MarketTokens>>,
    polymarket_decoders: HashMap<PolymarketDecoderKey, MarketFrameDecoder>,
    polymarket_frames: HashMap<SourceConnection, PolymarketFrameProjection>,
    polybolt_decoders: HashMap<String, PolyBoltDecoderState>,
    polybolt_frames: HashMap<PolyBoltFrameKey, PolyBoltFrameProjection>,
}

struct RecordProjectionContext {
    source_id: String,
    normalized_projection: Option<Value>,
    source_connection: SourceConnection,
    connection_epoch: u64,
    frame_sequence: u64,
    projection_index: u64,
    receipt_time_ms: i64,
    ingest_sequence: u64,
}

const POLYMARKET_MARKET_SOURCE_ID: &str = "polymarket:market-ws";

fn project_gap(
    record: RawCaptureRecord,
) -> Result<(SourceSignal, RecordedGap), CaptureReplayError> {
    let ingest_sequence = record.ingest_sequence;
    let capture = record.capture;
    let metadata = capture
        .transport
        .as_ref()
        .map(|_| metadata(&capture, ingest_sequence))
        .transpose()?;
    let source_id = capture.source_id.clone();
    let detected_at_ms = capture.receipt_time_ms;
    let RawCapturePayload::Gap {
        range,
        unresolved_count,
        reason,
        raw_frame,
    } = capture.payload
    else {
        return Err(CaptureReplayError::MalformedGapRecord);
    };
    let gap = RecordedGap {
        ingest_sequence,
        source_id: source_id.clone(),
        range,
        unresolved_count,
        reason: reason.clone(),
    };
    Ok((
        SourceSignal::Gap(SourceGap {
            source_id,
            detected_at_ms,
            metadata,
            raw_frame,
            range: range.map(|range| SourceGapRange {
                domain: match range.domain {
                    GapSequenceDomain::Transport => SourceGapSequenceDomain::Transport,
                    GapSequenceDomain::Channel => SourceGapSequenceDomain::Channel,
                },
                first_missing: range.first_missing,
                last_missing: range.last_missing,
            }),
            unresolved_count,
            reason,
        }),
        gap,
    ))
}

fn project_record(
    record: RawCaptureRecord,
    state: &mut ReplayProjectionState,
) -> Result<SourceSignal, CaptureReplayError> {
    let ingest_sequence = record.ingest_sequence;
    let projection_index = record.projection_index;
    let capture = record.capture;
    let transport = capture
        .transport
        .as_ref()
        .ok_or_else(|| projection_error(ingest_sequence, "missing transport metadata"))?;
    let source_connection = SourceConnection {
        source_id: capture.source_id.clone(),
        connection_id: transport.connection_id.clone(),
        connection_epoch: transport.connection_epoch,
    };
    let frame_sequence = transport.frame_sequence;
    let connection_epoch = transport.connection_epoch;
    let stream_metadata = metadata(&capture, ingest_sequence)?;
    if let Some(projection) = &capture.normalized_projection {
        validate_projection_metadata(projection, &capture, ingest_sequence)?;
    }
    let context = RecordProjectionContext {
        source_id: capture.source_id,
        normalized_projection: capture.normalized_projection,
        source_connection,
        connection_epoch,
        frame_sequence,
        projection_index,
        receipt_time_ms: capture.receipt_time_ms,
        ingest_sequence,
    };
    match capture.payload {
        RawCapturePayload::Frame { raw_bytes } | RawCapturePayload::Snapshot { raw_bytes } => {
            project_frame(state, &context, stream_metadata, raw_bytes)
        }
        RawCapturePayload::Control { name, raw_bytes } if name == "heartbeat" => {
            project_heartbeat(state, &context, stream_metadata, raw_bytes)
        }
        RawCapturePayload::Control { name, .. } => Err(projection_error(
            ingest_sequence,
            &format!("unsupported control record {name:?}"),
        )),
        RawCapturePayload::Gap { .. } => Err(CaptureReplayError::MalformedGapRecord),
    }
}

fn project_frame(
    state: &mut ReplayProjectionState,
    context: &RecordProjectionContext,
    metadata: StreamMetadata,
    raw_bytes: Vec<u8>,
) -> Result<SourceSignal, CaptureReplayError> {
    if raw_bytes.is_empty() {
        return Err(projection_error(
            context.ingest_sequence,
            "data frame is empty",
        ));
    }
    if context.source_id == POLYMARKET_MARKET_SOURCE_ID && state.polymarket_market_tokens.is_none()
    {
        return Err(CaptureReplayError::MissingMarketTokens {
            ingest_sequence: context.ingest_sequence,
            source_id: context.source_id.clone(),
        });
    }
    let source = project_frame_witness(context, metadata, raw_bytes)?;
    validate_source_projection(state, context, &source)?;
    Ok(SourceSignal::Data(Box::new(source)))
}

fn project_frame_witness(
    context: &RecordProjectionContext,
    metadata: StreamMetadata,
    raw_bytes: Vec<u8>,
) -> Result<SourceEnvelope, CaptureReplayError> {
    let projection = context
        .normalized_projection
        .as_ref()
        .ok_or_else(|| projection_error(context.ingest_sequence, "missing normalized witness"))?;
    let payload = projection
        .get("payload")
        .ok_or_else(|| projection_error(context.ingest_sequence, "witness has no payload"))?;
    let source = match string(payload, "kind", context.ingest_sequence)? {
        "reference_trade" | "reference_twap_60s_snapshot" | "reference_twap_60s_update" => {
            SourceEnvelope::CexReference(CexReferenceEnvelope {
                metadata,
                raw_frame: raw_bytes,
                fact: reference_fact(payload, context.ingest_sequence)?,
            })
        }
        _ if projection.get("portfolio").is_some() => {
            let portfolio = parse_id::<PortfolioId>(
                string(projection, "portfolio", context.ingest_sequence)?,
                context.ingest_sequence,
                "portfolio",
            )?;
            SourceEnvelope::PmAccount(PmAccountEnvelope {
                portfolio,
                metadata,
                raw_frame: raw_bytes,
                fact: account_fact(payload, context.ingest_sequence)?,
            })
        }
        _ => SourceEnvelope::PmMarket(PmMarketEnvelope {
            metadata,
            raw_frame: raw_bytes,
            fact: market_fact(payload, context.ingest_sequence)?,
        }),
    };
    let round_trip = match &source {
        SourceEnvelope::PmMarket(envelope) => pmkit_tape::market_envelope_json(envelope),
        SourceEnvelope::PmAccount(envelope) => pmkit_tape::account_envelope_json(envelope),
        SourceEnvelope::CexReference(envelope) => pmkit_tape::reference_envelope_json(envelope),
    };
    if &round_trip != projection {
        return Err(projection_error(
            context.ingest_sequence,
            "typed fact does not round-trip to its normalized witness",
        ));
    }
    Ok(source)
}

fn validate_source_projection(
    state: &mut ReplayProjectionState,
    context: &RecordProjectionContext,
    source: &SourceEnvelope,
) -> Result<(), CaptureReplayError> {
    if context.source_id == "binance:aggTrade"
        && let SourceEnvelope::CexReference(envelope) = source
    {
        validate_binance_projection(
            &mut state.binance_frames,
            context.source_connection.clone(),
            context.frame_sequence,
            context.projection_index,
            envelope,
            context.ingest_sequence,
        )?;
    }
    if context.source_id == POLYMARKET_MARKET_SOURCE_ID {
        let SourceEnvelope::PmMarket(envelope) = source else {
            return Err(projection_error(
                context.ingest_sequence,
                "native Polymarket market frame must project a PM market fact",
            ));
        };
        validate_polymarket_projection(state, context, envelope)?;
    }
    if context.source_id == POLYBOLT_SOURCE_ID {
        let SourceEnvelope::CexReference(envelope) = source else {
            return Err(projection_error(
                context.ingest_sequence,
                "PolyBolt raw frame must project a CEX reference fact",
            ));
        };
        validate_polybolt_projection(state, context, envelope)?;
    }
    Ok(())
}

fn project_heartbeat(
    state: &mut ReplayProjectionState,
    context: &RecordProjectionContext,
    metadata: StreamMetadata,
    raw_bytes: Vec<u8>,
) -> Result<SourceSignal, CaptureReplayError> {
    if context.normalized_projection.is_some() {
        return Err(projection_error(
            context.ingest_sequence,
            "heartbeat must not have a normalized witness",
        ));
    }
    advance_polybolt_heartbeat(
        state,
        &context.source_id,
        context.connection_epoch,
        context.receipt_time_ms,
        &raw_bytes,
        context.ingest_sequence,
    )?;
    Ok(SourceSignal::Heartbeat(SourceHeartbeat {
        metadata,
        raw_frame: raw_bytes,
    }))
}

fn validate_polymarket_projection(
    state: &mut ReplayProjectionState,
    context: &RecordProjectionContext,
    envelope: &PmMarketEnvelope,
) -> Result<(), CaptureReplayError> {
    let Some(market_tokens) = state.polymarket_market_tokens.as_ref() else {
        return Err(CaptureReplayError::MissingMarketTokens {
            ingest_sequence: context.ingest_sequence,
            source_id: context.source_id.clone(),
        });
    };
    let source = context.source_connection.clone();

    if let Some(frame) = state.polymarket_frames.get_mut(&source) {
        if frame.frame_sequence == context.frame_sequence {
            if frame.raw_frame != envelope.raw_frame {
                return Err(projection_error(
                    context.ingest_sequence,
                    "repeated Polymarket projection changed its raw frame",
                ));
            }
            return consume_polymarket_fact(frame, envelope, context.ingest_sequence);
        }
        if !frame.unmatched_facts.is_empty() {
            return Err(projection_error(
                frame.first_ingest_sequence,
                "native Polymarket frame is missing normalized projections",
            ));
        }
    }
    if context.projection_index != 0 {
        return Err(projection_error(
            context.ingest_sequence,
            "native Polymarket projection has no decoded frame",
        ));
    }

    let mut facts = Vec::new();
    for (market, tokens) in market_tokens {
        if market != tokens.market() {
            return Err(projection_error(
                context.ingest_sequence,
                &format!(
                    "MarketTokens context key {market} does not match resolver market {}",
                    tokens.market()
                ),
            ));
        }
        for outcome in [Outcome::Up, Outcome::Down] {
            let key = PolymarketDecoderKey {
                source: source.clone(),
                market: market.clone(),
                outcome,
            };
            let decoder = state
                .polymarket_decoders
                .entry(key)
                .or_insert_with(|| MarketFrameDecoder::new(tokens.clone(), outcome));
            if let Some(fact) = decoder
                .project_frame(&envelope.raw_frame)
                .map_err(|error| {
                    projection_error(
                        context.ingest_sequence,
                        &format!("invalid raw Polymarket market frame: {error}"),
                    )
                })?
            {
                facts.push(fact);
            }
        }
    }

    let mut frame = PolymarketFrameProjection {
        frame_sequence: context.frame_sequence,
        raw_frame: envelope.raw_frame.clone(),
        unmatched_facts: facts,
        first_ingest_sequence: context.ingest_sequence,
    };
    consume_polymarket_fact(&mut frame, envelope, context.ingest_sequence)?;
    state.polymarket_frames.insert(source, frame);
    Ok(())
}

fn consume_polymarket_fact(
    frame: &mut PolymarketFrameProjection,
    envelope: &PmMarketEnvelope,
    ingest_sequence: u64,
) -> Result<(), CaptureReplayError> {
    let Some(index) = frame
        .unmatched_facts
        .iter()
        .position(|fact| fact == &envelope.fact)
    else {
        return Err(projection_error(
            ingest_sequence,
            "raw Polymarket frame does not match its normalized witness",
        ));
    };
    if frame.unmatched_facts[index].timestamp_ms() != envelope.metadata.source_time_ms {
        return Err(projection_error(
            ingest_sequence,
            "raw Polymarket timestamp does not match capture source timestamp",
        ));
    }
    frame.unmatched_facts.remove(index);
    Ok(())
}

fn validate_polymarket_eof(
    frames: &HashMap<SourceConnection, PolymarketFrameProjection>,
) -> Result<(), CaptureReplayError> {
    if let Some(frame) = frames
        .values()
        .find(|frame| !frame.unmatched_facts.is_empty())
    {
        return Err(projection_error(
            frame.first_ingest_sequence,
            "native Polymarket frame is missing normalized projections",
        ));
    }
    Ok(())
}

fn validate_binance_projection(
    frames: &mut HashMap<SourceConnection, BinanceFrameProjection>,
    source: SourceConnection,
    frame_sequence: u64,
    projection_index: u64,
    envelope: &CexReferenceEnvelope,
    ingest_sequence: u64,
) -> Result<(), CaptureReplayError> {
    let decoded = if projection_index == 0 {
        if frames
            .get(&source)
            .is_some_and(|frame| frame.frame_sequence == frame_sequence)
        {
            return Err(projection_error(
                ingest_sequence,
                "duplicate Binance projection index",
            ));
        }
        let CexReferenceEvent::Trade { asset, .. } = &envelope.fact else {
            return Err(projection_error(
                ingest_sequence,
                "Binance raw frame must project one trade",
            ));
        };
        let text = std::str::from_utf8(&envelope.raw_frame).map_err(|error| {
            projection_error(ingest_sequence, &format!("invalid Binance UTF-8: {error}"))
        })?;
        let decoded = parse_binance_agg_trade_live(text, *asset).map_err(|error| {
            projection_error(
                ingest_sequence,
                &format!("invalid raw Binance frame: {error}"),
            )
        })?;
        frames.insert(
            source,
            BinanceFrameProjection {
                frame_sequence,
                next_projection_index: 1,
                fact: decoded.clone(),
            },
        );
        decoded
    } else {
        let frame = frames.get_mut(&source).ok_or_else(|| {
            projection_error(
                ingest_sequence,
                "repeated Binance projection has no decoded frame",
            )
        })?;
        if frame.frame_sequence != frame_sequence || frame.next_projection_index != projection_index
        {
            return Err(projection_error(
                ingest_sequence,
                "Binance projection index does not match its decoded frame",
            ));
        }
        frame.next_projection_index = frame
            .next_projection_index
            .checked_add(1)
            .ok_or_else(|| projection_error(ingest_sequence, "projection index exhausted"))?;
        frame.fact.clone()
    };
    if decoded != envelope.fact {
        return Err(projection_error(
            ingest_sequence,
            "raw Binance frame does not match its normalized witness",
        ));
    }
    Ok(())
}

fn validate_polybolt_projection(
    state: &mut ReplayProjectionState,
    context: &RecordProjectionContext,
    envelope: &CexReferenceEnvelope,
) -> Result<(), CaptureReplayError> {
    let key = PolyBoltFrameKey {
        source_id: context.source_id.clone(),
        connection_epoch: context.connection_epoch,
        frame_sequence: context.frame_sequence,
    };
    let next_projection_index = context
        .projection_index
        .checked_add(1)
        .ok_or_else(|| projection_error(context.ingest_sequence, "projection index exhausted"))?;

    if let Some(frame) = state.polybolt_frames.get_mut(&key) {
        if frame.next_projection_index != context.projection_index {
            return Err(projection_error(
                context.ingest_sequence,
                "PolyBolt projection index does not match its decoded frame",
            ));
        }
        validate_polybolt_fact(
            &frame.event,
            context.projection_index,
            &envelope.fact,
            context.ingest_sequence,
        )?;
        frame.next_projection_index = next_projection_index;
        let complete = frame.next_projection_index == frame.projection_count;
        if complete {
            state.polybolt_frames.remove(&key);
        }
        return Ok(());
    }

    if context.projection_index != 0 {
        return Err(projection_error(
            context.ingest_sequence,
            "repeated PolyBolt projection has no decoded frame",
        ));
    }
    ensure_no_pending_polybolt_frames(
        &state.polybolt_frames,
        &context.source_id,
        context.ingest_sequence,
    )?;
    let event = decode_polybolt_frame(
        &mut state.polybolt_decoders,
        &state.polybolt_frames,
        &context.source_id,
        context.connection_epoch,
        &envelope.raw_frame,
        envelope.metadata.receipt_time_ms,
        context.ingest_sequence,
    )?;
    let projection_count = polybolt_projection_count(&event, context.ingest_sequence)?;
    validate_polybolt_fact(
        &event,
        context.projection_index,
        &envelope.fact,
        context.ingest_sequence,
    )?;
    if next_projection_index < projection_count {
        state.polybolt_frames.insert(
            key,
            PolyBoltFrameProjection {
                event,
                next_projection_index,
                projection_count,
                first_ingest_sequence: context.ingest_sequence,
            },
        );
    }
    Ok(())
}

fn polybolt_projection_count(
    event: &PolyBoltEvent,
    ingest_sequence: u64,
) -> Result<u64, CaptureReplayError> {
    let count = match event {
        PolyBoltEvent::Snapshot { prices } => prices.len(),
        PolyBoltEvent::Update { .. } => 1,
        _ => {
            return Err(projection_error(
                ingest_sequence,
                "PolyBolt raw frame does not contain a TWAP fact",
            ));
        }
    };
    u64::try_from(count)
        .map_err(|_| projection_error(ingest_sequence, "PolyBolt snapshot has too many points"))
}

fn validate_polybolt_fact(
    event: &PolyBoltEvent,
    projection_index: u64,
    fact: &CexReferenceEvent,
    ingest_sequence: u64,
) -> Result<(), CaptureReplayError> {
    let (expected_price, expected_timestamp, snapshot) = match event {
        PolyBoltEvent::Snapshot { prices } => {
            let index = usize::try_from(projection_index)
                .map_err(|_| projection_error(ingest_sequence, "projection index exceeds usize"))?;
            let point = prices.get(index).ok_or_else(|| {
                projection_error(
                    ingest_sequence,
                    "PolyBolt snapshot projection index is out of range",
                )
            })?;
            (point.exact_value, point.timestamp_ms, true)
        }
        PolyBoltEvent::Update { price } if projection_index == 0 => {
            (price.exact_value, price.timestamp_ms, false)
        }
        PolyBoltEvent::Update { .. } => {
            return Err(projection_error(
                ingest_sequence,
                "PolyBolt update must have projection index zero",
            ));
        }
        _ => {
            return Err(projection_error(
                ingest_sequence,
                "PolyBolt raw frame does not contain a TWAP fact",
            ));
        }
    };
    let (asset, witness_price, witness_timestamp, witness_snapshot) = match fact {
        CexReferenceEvent::Twap60sSnapshot {
            asset,
            price,
            timestamp_ms,
        } => (*asset, *price, *timestamp_ms, true),
        CexReferenceEvent::Twap60sUpdate {
            asset,
            price,
            timestamp_ms,
        } => (*asset, *price, *timestamp_ms, false),
        CexReferenceEvent::Trade { .. } => {
            return Err(projection_error(
                ingest_sequence,
                "PolyBolt raw frame type does not match its normalized witness",
            ));
        }
    };
    if asset != Asset::Btc
        || witness_snapshot != snapshot
        || witness_price != expected_price
        || witness_timestamp != expected_timestamp
    {
        return Err(projection_error(
            ingest_sequence,
            "raw PolyBolt frame does not match its normalized TWAP witness",
        ));
    }
    Ok(())
}

fn decode_polybolt_frame(
    decoders: &mut HashMap<String, PolyBoltDecoderState>,
    frames: &HashMap<PolyBoltFrameKey, PolyBoltFrameProjection>,
    source_id: &str,
    connection_epoch: u64,
    raw_bytes: &[u8],
    receipt_time_ms: i64,
    ingest_sequence: u64,
) -> Result<PolyBoltEvent, CaptureReplayError> {
    let decoder = polybolt_decoder_for_epoch(
        decoders,
        frames,
        source_id,
        connection_epoch,
        ingest_sequence,
    )?;
    decoder
        .decode(
            PolyBoltRawFrame {
                receipt_time_ms,
                bytes: raw_bytes.to_vec(),
            },
            connection_epoch,
        )
        .map(|record| record.event)
        .map_err(|error| {
            projection_error(
                ingest_sequence,
                &format!("invalid raw PolyBolt frame: {error}"),
            )
        })
}

fn advance_polybolt_heartbeat(
    state: &mut ReplayProjectionState,
    source_id: &str,
    connection_epoch: u64,
    receipt_time_ms: i64,
    raw_bytes: &[u8],
    ingest_sequence: u64,
) -> Result<(), CaptureReplayError> {
    if source_id != POLYBOLT_SOURCE_ID {
        return Ok(());
    }
    ensure_no_pending_polybolt_frames(&state.polybolt_frames, source_id, ingest_sequence)?;
    let decoder = polybolt_decoder_for_epoch(
        &mut state.polybolt_decoders,
        &state.polybolt_frames,
        source_id,
        connection_epoch,
        ingest_sequence,
    )?;
    let Ok(wire) = serde_json::from_slice::<Value>(raw_bytes) else {
        return Ok(());
    };
    if ["v", "channel", "seq", "payload"]
        .iter()
        .any(|field| wire.get(field).is_none())
    {
        return Ok(());
    }
    decoder
        .decode(
            PolyBoltRawFrame {
                receipt_time_ms,
                bytes: raw_bytes.to_vec(),
            },
            connection_epoch,
        )
        .map(|_| ())
        .map_err(|error| {
            projection_error(
                ingest_sequence,
                &format!("invalid raw PolyBolt heartbeat frame: {error}"),
            )
        })
}

fn polybolt_decoder_for_epoch<'a>(
    decoders: &'a mut HashMap<String, PolyBoltDecoderState>,
    frames: &HashMap<PolyBoltFrameKey, PolyBoltFrameProjection>,
    source_id: &str,
    connection_epoch: u64,
    ingest_sequence: u64,
) -> Result<&'a mut PolyBoltDecoder, CaptureReplayError> {
    let state = decoders
        .entry(source_id.to_owned())
        .or_insert_with(|| PolyBoltDecoderState {
            connection_epoch,
            decoder: PolyBoltDecoder::default(),
        });
    if state.connection_epoch != connection_epoch {
        ensure_no_pending_polybolt_frames(frames, source_id, ingest_sequence)?;
        state.decoder.reset();
        state.connection_epoch = connection_epoch;
    }
    Ok(&mut state.decoder)
}

fn ensure_no_pending_polybolt_frames(
    frames: &HashMap<PolyBoltFrameKey, PolyBoltFrameProjection>,
    source_id: &str,
    ingest_sequence: u64,
) -> Result<(), CaptureReplayError> {
    if frames.keys().any(|key| key.source_id == source_id) {
        return Err(projection_error(
            ingest_sequence,
            "PolyBolt snapshot is missing normalized point projections",
        ));
    }
    Ok(())
}

fn validate_polybolt_eof(
    frames: &HashMap<PolyBoltFrameKey, PolyBoltFrameProjection>,
) -> Result<(), CaptureReplayError> {
    if let Some(frame) = frames.values().next() {
        return Err(projection_error(
            frame.first_ingest_sequence,
            "PolyBolt snapshot is missing normalized point projections",
        ));
    }
    Ok(())
}

fn metadata(
    capture: &RawCaptureDraft,
    ingest_sequence: u64,
) -> Result<StreamMetadata, CaptureReplayError> {
    let transport = capture
        .transport
        .as_ref()
        .ok_or_else(|| projection_error(ingest_sequence, "missing transport metadata"))?;
    let source_time_ms = transport
        .source_time_ms
        .ok_or_else(|| projection_error(ingest_sequence, "missing source timestamp"))?;
    Ok(StreamMetadata {
        schema_version: capture.schema_version,
        source_id: capture.source_id.clone(),
        source_time_ms,
        canonical_source_rank: capture.canonical_source_rank,
        receipt_time_ms: capture.receipt_time_ms,
        connection_id: transport.connection_id.clone(),
        connection_epoch: signed_sequence(transport.connection_epoch, ingest_sequence, "epoch")?,
        frame_sequence: signed_sequence(transport.frame_sequence, ingest_sequence, "frame")?,
        ingest_sequence: transport.ingest_sequence,
    })
}

fn signed_sequence(
    value: u64,
    ingest_sequence: u64,
    field: &str,
) -> Result<i64, CaptureReplayError> {
    i64::try_from(value)
        .map_err(|_| projection_error(ingest_sequence, &format!("{field} exceeds i64 range")))
}

fn validate_projection_metadata(
    projection: &Value,
    capture: &RawCaptureDraft,
    ingest_sequence: u64,
) -> Result<(), CaptureReplayError> {
    let transport = capture
        .transport
        .as_ref()
        .ok_or_else(|| projection_error(ingest_sequence, "missing transport metadata"))?;
    let source_time_ms = transport
        .source_time_ms
        .ok_or_else(|| projection_error(ingest_sequence, "missing source timestamp"))?;
    let checks = [
        ("schema_version", capture.schema_version.into()),
        ("source_time_ms", source_time_ms.into()),
        (
            "canonical_source_rank",
            capture.canonical_source_rank.into(),
        ),
        ("receipt_time_ms", capture.receipt_time_ms.into()),
        ("connection_epoch", transport.connection_epoch.into()),
        ("frame_sequence", transport.frame_sequence.into()),
        ("ingest_sequence", transport.ingest_sequence.into()),
    ];
    for (field, expected) in checks {
        if projection.get(field) != Some(&expected) {
            return Err(projection_error(
                ingest_sequence,
                &format!("witness {field} does not match capture metadata"),
            ));
        }
    }
    for (field, expected) in [
        ("source_id", capture.source_id.as_str()),
        ("connection_id", transport.connection_id.as_str()),
    ] {
        if projection.get(field).and_then(Value::as_str) != Some(expected) {
            return Err(projection_error(
                ingest_sequence,
                &format!("witness {field} does not match capture metadata"),
            ));
        }
    }
    Ok(())
}

fn market_fact(value: &Value, sequence: u64) -> Result<MarketEvent, CaptureReplayError> {
    let timestamp_ms = integer(value, "ts", sequence)?;
    Ok(match string(value, "kind", sequence)? {
        "book_update" => MarketEvent::BookUpdate {
            market: parse_id::<MarketId>(string(value, "market", sequence)?, sequence, "market")?,
            outcome: outcome(string(value, "outcome", sequence)?, sequence)?,
            bids: levels(value, "bids", sequence)?,
            asks: levels(value, "asks", sequence)?,
            timestamp_ms,
        },
        "best_bid_ask" => MarketEvent::BestBidAsk {
            market: parse_id::<MarketId>(string(value, "market", sequence)?, sequence, "market")?,
            outcome: outcome(string(value, "outcome", sequence)?, sequence)?,
            bid: decimal(value, "bid", sequence)?,
            ask: decimal(value, "ask", sequence)?,
            timestamp_ms,
        },
        "last_trade" => MarketEvent::LastTrade {
            market: parse_id::<MarketId>(string(value, "market", sequence)?, sequence, "market")?,
            outcome: outcome(string(value, "outcome", sequence)?, sequence)?,
            price: decimal(value, "price", sequence)?,
            side: side(string(value, "side", sequence)?, sequence)?,
            size: decimal(value, "size", sequence)?,
            timestamp_ms,
        },
        "fill" => MarketEvent::Fill {
            strategy: optional_id::<StrategyId>(value, "strategy", sequence)?,
            order_id: string(value, "order_id", sequence)?.to_owned(),
            market: parse_id::<MarketId>(string(value, "market", sequence)?, sequence, "market")?,
            outcome: outcome(string(value, "outcome", sequence)?, sequence)?,
            price: decimal(value, "price", sequence)?,
            size: decimal(value, "size", sequence)?,
            side: side(string(value, "side", sequence)?, sequence)?,
            fee: decimal(value, "fee", sequence)?,
            liquidity: liquidity(string(value, "liquidity", sequence)?, sequence)?,
            timestamp_ms,
        },
        "order_ack" => MarketEvent::OrderAck {
            strategy: optional_id::<StrategyId>(value, "strategy", sequence)?,
            order_id: string(value, "order_id", sequence)?.to_owned(),
            timestamp_ms,
        },
        "tick" => MarketEvent::Tick { timestamp_ms },
        kind => {
            return Err(projection_error(
                sequence,
                &format!("unsupported PM market kind {kind:?}"),
            ));
        }
    })
}

fn account_fact(value: &Value, sequence: u64) -> Result<PmAccountEvent, CaptureReplayError> {
    let timestamp_ms = integer(value, "ts", sequence)?;
    Ok(match string(value, "kind", sequence)? {
        "fill" => PmAccountEvent::Fill {
            identity: fill_identity(field(value, "identity", sequence)?, sequence)?,
            strategy: optional_id::<StrategyId>(value, "strategy", sequence)?,
            order_id: string(value, "order_id", sequence)?.to_owned(),
            market: parse_id::<MarketId>(string(value, "market", sequence)?, sequence, "market")?,
            outcome: outcome(string(value, "outcome", sequence)?, sequence)?,
            price: decimal(value, "price", sequence)?,
            size: decimal(value, "size", sequence)?,
            side: side(string(value, "side", sequence)?, sequence)?,
            fee: decimal(value, "fee", sequence)?,
            liquidity: liquidity(string(value, "liquidity", sequence)?, sequence)?,
            timestamp_ms,
        },
        "order_ack" => PmAccountEvent::OrderAck {
            strategy: optional_id::<StrategyId>(value, "strategy", sequence)?,
            order_id: string(value, "order_id", sequence)?.to_owned(),
            timestamp_ms,
        },
        "order_cancelled" => PmAccountEvent::OrderCancelled {
            strategy: optional_id::<StrategyId>(value, "strategy", sequence)?,
            order_id: string(value, "order_id", sequence)?.to_owned(),
            timestamp_ms,
        },
        "order_rejected" => PmAccountEvent::OrderRejected {
            strategy: optional_id::<StrategyId>(value, "strategy", sequence)?,
            order_id: string(value, "order_id", sequence)?.to_owned(),
            reason: string(value, "reason", sequence)?.to_owned(),
            timestamp_ms,
        },
        "order_status" => PmAccountEvent::OrderStatus {
            strategy: optional_id::<StrategyId>(value, "strategy", sequence)?,
            order_id: string(value, "order_id", sequence)?.to_owned(),
            status: string(value, "status", sequence)?.to_owned(),
            timestamp_ms,
        },
        "settlement" => PmAccountEvent::Settlement {
            identity: settlement_identity(field(value, "identity", sequence)?, sequence)?,
            market: parse_id::<MarketId>(string(value, "market", sequence)?, sequence, "market")?,
            outcome: outcome(string(value, "outcome", sequence)?, sequence)?,
            settled_size: decimal(value, "settled_size", sequence)?,
            proceeds: decimal(value, "proceeds", sequence)?,
            timestamp_ms,
        },
        kind => {
            return Err(projection_error(
                sequence,
                &format!("unsupported PM account kind {kind:?}"),
            ));
        }
    })
}

fn reference_fact(value: &Value, sequence: u64) -> Result<CexReferenceEvent, CaptureReplayError> {
    let timestamp_ms = integer(value, "ts", sequence)?;
    Ok(match string(value, "kind", sequence)? {
        "reference_trade" => CexReferenceEvent::Trade {
            asset: parse_asset(string(value, "asset", sequence)?, sequence)?,
            exchange: exchange(string(value, "exchange", sequence)?, sequence)?,
            aggregate_trade_id: unsigned(value, "aggregate_trade_id", sequence)?,
            price: decimal(value, "price", sequence)?,
            qty: decimal(value, "qty", sequence)?,
            is_buyer_maker: boolean(value, "is_buyer_maker", sequence)?,
            timestamp_ms,
        },
        "reference_twap_60s_snapshot" => CexReferenceEvent::Twap60sSnapshot {
            asset: parse_asset(string(value, "asset", sequence)?, sequence)?,
            price: decimal(value, "price", sequence)?,
            timestamp_ms,
        },
        "reference_twap_60s_update" => CexReferenceEvent::Twap60sUpdate {
            asset: parse_asset(string(value, "asset", sequence)?, sequence)?,
            price: decimal(value, "price", sequence)?,
            timestamp_ms,
        },
        kind => {
            return Err(projection_error(
                sequence,
                &format!("unsupported reference kind {kind:?}"),
            ));
        }
    })
}

fn levels(
    value: &Value,
    field_name: &str,
    sequence: u64,
) -> Result<Vec<(Decimal, Decimal)>, CaptureReplayError> {
    let values = field(value, field_name, sequence)?
        .as_array()
        .ok_or_else(|| projection_error(sequence, &format!("{field_name} must be an array")))?;
    values
        .iter()
        .map(|level| {
            let pair = level
                .as_array()
                .filter(|pair| pair.len() == 2)
                .ok_or_else(|| {
                    projection_error(
                        sequence,
                        &format!("{field_name} contains a malformed level"),
                    )
                })?;
            Ok((
                decimal_at(&pair[0], sequence)?,
                decimal_at(&pair[1], sequence)?,
            ))
        })
        .collect()
}

fn fill_identity(value: &Value, sequence: u64) -> Result<FillIdentity, CaptureReplayError> {
    match string(value, "source", sequence)? {
        "venue" => Ok(FillIdentity::Venue(
            string(value, "id", sequence)?.to_owned(),
        )),
        "transport" => Ok(FillIdentity::Transport {
            source_id: string(value, "source_id", sequence)?.to_owned(),
            connection_id: string(value, "connection_id", sequence)?.to_owned(),
            connection_epoch: integer(value, "connection_epoch", sequence)?,
            frame_sequence: integer(value, "frame_sequence", sequence)?,
        }),
        other => Err(projection_error(
            sequence,
            &format!("unknown fill identity {other:?}"),
        )),
    }
}

fn settlement_identity(
    value: &Value,
    sequence: u64,
) -> Result<SettlementIdentity, CaptureReplayError> {
    match string(value, "source", sequence)? {
        "venue" => Ok(SettlementIdentity::Venue(
            string(value, "id", sequence)?.to_owned(),
        )),
        "transport" => Ok(SettlementIdentity::Transport {
            source_id: string(value, "source_id", sequence)?.to_owned(),
            connection_id: string(value, "connection_id", sequence)?.to_owned(),
            connection_epoch: integer(value, "connection_epoch", sequence)?,
            frame_sequence: integer(value, "frame_sequence", sequence)?,
        }),
        other => Err(projection_error(
            sequence,
            &format!("unknown settlement identity {other:?}"),
        )),
    }
}

fn optional_id<T>(
    value: &Value,
    field_name: &str,
    sequence: u64,
) -> Result<Option<T>, CaptureReplayError>
where
    T: CaptureId,
{
    match field(value, field_name, sequence)? {
        Value::Null => Ok(None),
        Value::String(id) => T::parse(id.clone())
            .map(Some)
            .map_err(|error| projection_error(sequence, &format!("invalid {field_name}: {error}"))),
        _ => Err(projection_error(
            sequence,
            &format!("{field_name} must be a string or null"),
        )),
    }
}

trait CaptureId: Sized {
    fn parse(value: String) -> Result<Self, pmkit_core::EmptyIdError>;
}

impl CaptureId for MarketId {
    fn parse(value: String) -> Result<Self, pmkit_core::EmptyIdError> {
        Self::new(value)
    }
}
impl CaptureId for PortfolioId {
    fn parse(value: String) -> Result<Self, pmkit_core::EmptyIdError> {
        Self::new(value)
    }
}
impl CaptureId for StrategyId {
    fn parse(value: String) -> Result<Self, pmkit_core::EmptyIdError> {
        Self::new(value)
    }
}

fn parse_id<T: CaptureId>(
    value: &str,
    sequence: u64,
    field_name: &str,
) -> Result<T, CaptureReplayError> {
    T::parse(value.to_owned())
        .map_err(|error| projection_error(sequence, &format!("invalid {field_name}: {error}")))
}

fn outcome(value: &str, sequence: u64) -> Result<Outcome, CaptureReplayError> {
    match value.to_ascii_lowercase().as_str() {
        "up" => Ok(Outcome::Up),
        "down" => Ok(Outcome::Down),
        _ => Err(projection_error(
            sequence,
            &format!("unknown outcome {value:?}"),
        )),
    }
}

fn side(value: &str, sequence: u64) -> Result<Side, CaptureReplayError> {
    match value {
        "buy" => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        _ => Err(projection_error(
            sequence,
            &format!("unknown side {value:?}"),
        )),
    }
}

fn liquidity(value: &str, sequence: u64) -> Result<Liquidity, CaptureReplayError> {
    match value {
        "maker" => Ok(Liquidity::Maker),
        "taker" => Ok(Liquidity::Taker),
        _ => Err(projection_error(
            sequence,
            &format!("unknown liquidity {value:?}"),
        )),
    }
}

fn parse_asset(value: &str, sequence: u64) -> Result<Asset, CaptureReplayError> {
    value
        .parse()
        .map_err(|error| projection_error(sequence, &format!("invalid asset: {error}")))
}

fn exchange(value: &str, sequence: u64) -> Result<Exchange, CaptureReplayError> {
    match value.to_ascii_lowercase().as_str() {
        "binance" => Ok(Exchange::Binance),
        "chainlink" => Ok(Exchange::Chainlink),
        "vatic" => Ok(Exchange::Vatic),
        "bybit" => Ok(Exchange::Bybit),
        "coinbase" => Ok(Exchange::Coinbase),
        "okx" => Ok(Exchange::Okx),
        "kraken" => Ok(Exchange::Kraken),
        _ => Err(projection_error(
            sequence,
            &format!("unknown exchange {value:?}"),
        )),
    }
}

fn decimal(value: &Value, field_name: &str, sequence: u64) -> Result<Decimal, CaptureReplayError> {
    decimal_at(field(value, field_name, sequence)?, sequence)
}

fn decimal_at(value: &Value, sequence: u64) -> Result<Decimal, CaptureReplayError> {
    value
        .as_str()
        .ok_or_else(|| projection_error(sequence, "decimal witness must be a string"))?
        .parse()
        .map_err(|error| projection_error(sequence, &format!("invalid decimal witness: {error}")))
}

fn string<'a>(
    value: &'a Value,
    field_name: &str,
    sequence: u64,
) -> Result<&'a str, CaptureReplayError> {
    field(value, field_name, sequence)?
        .as_str()
        .ok_or_else(|| projection_error(sequence, &format!("{field_name} must be a string")))
}

fn integer(value: &Value, field_name: &str, sequence: u64) -> Result<i64, CaptureReplayError> {
    field(value, field_name, sequence)?
        .as_i64()
        .ok_or_else(|| projection_error(sequence, &format!("{field_name} must be an i64")))
}

fn unsigned(value: &Value, field_name: &str, sequence: u64) -> Result<u64, CaptureReplayError> {
    field(value, field_name, sequence)?
        .as_u64()
        .ok_or_else(|| projection_error(sequence, &format!("{field_name} must be a u64")))
}

fn boolean(value: &Value, field_name: &str, sequence: u64) -> Result<bool, CaptureReplayError> {
    field(value, field_name, sequence)?
        .as_bool()
        .ok_or_else(|| projection_error(sequence, &format!("{field_name} must be a boolean")))
}

fn field<'a>(
    value: &'a Value,
    field_name: &str,
    sequence: u64,
) -> Result<&'a Value, CaptureReplayError> {
    value
        .get(field_name)
        .ok_or_else(|| projection_error(sequence, &format!("missing {field_name}")))
}

fn projection_error(sequence: u64, message: &str) -> CaptureReplayError {
    CaptureReplayError::Projection {
        ingest_sequence: sequence,
        message: message.to_owned(),
    }
}

fn replay_source_error(error: &CaptureReplayError) -> DataSourceError {
    DataSourceError::ReplayGap {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io::Cursor};

    use pmkit_book::Side;
    use pmkit_core::MarketId;
    use pmkit_data::SourceSignal;
    use pmkit_event::{
        CexReferenceEnvelope, CexReferenceEvent, MarketEvent, PmMarketEnvelope, SourceEnvelope,
        StrategyFact, StreamMetadata,
    };
    use pmkit_market::{Asset, Exchange, Outcome};
    use pmkit_polymarket::{MarketTokens, POLYBOLT_SOURCE_ID};
    use pmkit_tape::{
        CaptureTransport, GapRange, GapSequenceDomain, RawCaptureDraft, RawCapturePayload,
        RawCaptureReader, RawCaptureWriter,
    };
    use rust_decimal::Decimal;
    use tokio::sync::mpsc;

    use super::{CaptureReplayReader, CaptureReplaySource};

    fn metadata(source: &str, frame: i64, ingest: u64) -> StreamMetadata {
        StreamMetadata {
            schema_version: 3,
            source_id: source.to_owned(),
            source_time_ms: 1_700_000_000_000 + frame,
            canonical_source_rank: 8,
            receipt_time_ms: 1_700_000_000_100 + frame,
            connection_id: format!("{source}-connection"),
            connection_epoch: 2,
            frame_sequence: frame,
            ingest_sequence: ingest,
        }
    }

    fn frame(
        source: &SourceEnvelope,
        raw: &[u8],
    ) -> Result<RawCaptureDraft, std::num::TryFromIntError> {
        let metadata = source.metadata();
        let projection = match source {
            SourceEnvelope::PmMarket(envelope) => pmkit_tape::market_envelope_json(envelope),
            SourceEnvelope::PmAccount(envelope) => pmkit_tape::account_envelope_json(envelope),
            SourceEnvelope::CexReference(envelope) => pmkit_tape::reference_envelope_json(envelope),
        };
        Ok(RawCaptureDraft {
            schema_version: metadata.schema_version,
            source_id: metadata.source_id.clone(),
            canonical_source_rank: metadata.canonical_source_rank,
            receipt_time_ms: metadata.receipt_time_ms,
            transport: Some(CaptureTransport {
                source_time_ms: Some(metadata.source_time_ms),
                connection_id: metadata.connection_id.clone(),
                connection_epoch: u64::try_from(metadata.connection_epoch)?,
                frame_sequence: u64::try_from(metadata.frame_sequence)?,
                ingest_sequence: metadata.ingest_sequence,
            }),
            normalized_projection: Some(projection),
            payload: RawCapturePayload::Frame {
                raw_bytes: raw.to_vec(),
            },
        })
    }

    fn native_pm_frame(
        fact: MarketEvent,
        raw: &[u8],
        frame_sequence: i64,
        ingest_sequence: u64,
    ) -> Result<RawCaptureDraft, std::num::TryFromIntError> {
        let mut metadata = metadata(
            super::POLYMARKET_MARKET_SOURCE_ID,
            frame_sequence,
            ingest_sequence,
        );
        metadata.source_time_ms = fact.timestamp_ms();
        frame(
            &SourceEnvelope::PmMarket(PmMarketEnvelope {
                metadata,
                raw_frame: Vec::new(),
                fact,
            }),
            raw,
        )
    }

    fn known_market_tokens(
        market: MarketId,
    ) -> Result<HashMap<MarketId, MarketTokens>, Box<dyn std::error::Error>> {
        let tokens = MarketTokens::new(market.clone(), "1".parse()?, "2".parse()?);
        Ok(HashMap::from([(market, tokens)]))
    }

    fn heartbeat(source_id: &str, frame_sequence: u64, ingest_sequence: u64) -> RawCaptureDraft {
        RawCaptureDraft {
            schema_version: 3,
            source_id: source_id.to_owned(),
            canonical_source_rank: 4,
            receipt_time_ms: 20,
            transport: Some(CaptureTransport {
                source_time_ms: Some(10),
                connection_id: format!("{source_id}-connection"),
                connection_epoch: 1,
                frame_sequence,
                ingest_sequence,
            }),
            normalized_projection: None,
            payload: RawCapturePayload::Control {
                name: "heartbeat".to_owned(),
                raw_bytes: Vec::new(),
            },
        }
    }

    #[test]
    fn replays_native_polymarket_snapshot_delta_bba_and_trade_once_per_frame()
    -> Result<(), Box<dyn std::error::Error>> {
        let market = MarketId::new("btc-5m")?;
        let raw_frames: [&[u8]; 6] = [
            br#"{"event_type":"book","market":"0x0000000000000000000000000000000000000000000000000000000000000001","asset_id":"1","timestamp":"42","bids":[{"price":"0.49","size":"2"}],"asks":[{"price":"0.51","size":"3"}]}"#,
            br#"{"event_type":"book","market":"0x0000000000000000000000000000000000000000000000000000000000000001","asset_id":"2","timestamp":"42","bids":[{"price":"0.44","size":"1"}],"asks":[{"price":"0.55","size":"2"}]}"#,
            br#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"43","price_changes":[{"asset_id":"1","price":"0.50","size":"4","side":"BUY"},{"asset_id":"2","price":"0.54","size":"5","side":"SELL"}]}"#,
            br#"{"event_type":"price_change","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"43","price_changes":[{"asset_id":"1","price":"0.50","size":"4","side":"BUY"},{"asset_id":"2","price":"0.54","size":"5","side":"SELL"}]}"#,
            br#"{"event_type":"best_bid_ask","market":"0x0000000000000000000000000000000000000000000000000000000000000001","asset_id":"1","best_bid":"0.50","best_ask":"0.51","spread":"0.01","timestamp":"44"}"#,
            br#"{"event_type":"last_trade_price","market":"0x0000000000000000000000000000000000000000000000000000000000000001","asset_id":"1","price":"0.505","side":"SELL","size":"1.25","timestamp":"45"}"#,
        ];
        let expected = [
            MarketEvent::BookUpdate {
                market: market.clone(),
                outcome: Outcome::Up,
                bids: vec![(Decimal::new(49, 2), Decimal::from(2))],
                asks: vec![(Decimal::new(51, 2), Decimal::from(3))],
                timestamp_ms: 42,
            },
            MarketEvent::BookUpdate {
                market: market.clone(),
                outcome: Outcome::Down,
                bids: vec![(Decimal::new(44, 2), Decimal::ONE)],
                asks: vec![(Decimal::new(55, 2), Decimal::from(2))],
                timestamp_ms: 42,
            },
            MarketEvent::BookUpdate {
                market: market.clone(),
                outcome: Outcome::Up,
                bids: vec![
                    (Decimal::new(50, 2), Decimal::from(4)),
                    (Decimal::new(49, 2), Decimal::from(2)),
                ],
                asks: vec![(Decimal::new(51, 2), Decimal::from(3))],
                timestamp_ms: 43,
            },
            MarketEvent::BookUpdate {
                market: market.clone(),
                outcome: Outcome::Down,
                bids: vec![(Decimal::new(44, 2), Decimal::ONE)],
                asks: vec![
                    (Decimal::new(54, 2), Decimal::from(5)),
                    (Decimal::new(55, 2), Decimal::from(2)),
                ],
                timestamp_ms: 43,
            },
            MarketEvent::BestBidAsk {
                market: market.clone(),
                outcome: Outcome::Up,
                bid: Decimal::new(50, 2),
                ask: Decimal::new(51, 2),
                timestamp_ms: 44,
            },
            MarketEvent::LastTrade {
                market,
                outcome: Outcome::Up,
                price: Decimal::new(505, 3),
                side: Side::Sell,
                size: Decimal::new(125, 2),
                timestamp_ms: 45,
            },
        ];
        let frame_sequences = [0, 1, 2, 2, 3, 4];
        let mut writer = RawCaptureWriter::new(Vec::new());
        for (index, (fact, raw)) in expected.iter().cloned().zip(raw_frames).enumerate() {
            writer.append(native_pm_frame(
                fact,
                raw,
                frame_sequences[index],
                u64::try_from(index)?,
            )?)?;
        }

        let tokens = known_market_tokens(MarketId::new("btc-5m")?);
        let mut reader =
            CaptureReplayReader::new_with_market_tokens(Cursor::new(writer.into_inner()), tokens?);
        for (index, (expected, raw)) in expected.into_iter().zip(raw_frames).enumerate() {
            let Some(SourceSignal::Data(envelope)) = reader.next_signal()? else {
                return Err("missing native Polymarket projection".into());
            };
            let SourceEnvelope::PmMarket(envelope) = envelope.as_ref() else {
                return Err("expected a native PM market envelope".into());
            };
            assert_eq!(envelope.fact, expected);
            assert_eq!(envelope.raw_frame, raw);
            assert_eq!(envelope.metadata.source_time_ms, expected.timestamp_ms());
            assert_eq!(
                envelope.metadata.source_id,
                super::POLYMARKET_MARKET_SOURCE_ID
            );
            assert_eq!(envelope.metadata.frame_sequence, frame_sequences[index]);
            assert_eq!(envelope.metadata.ingest_sequence, u64::try_from(index)?);
        }
        assert!(reader.next_signal()?.is_none());
        Ok(())
    }

    #[test]
    fn requires_native_token_context_and_rejects_tampered_price_or_outcome()
    -> Result<(), Box<dyn std::error::Error>> {
        let market = MarketId::new("btc-5m")?;
        let raw = br#"{"event_type":"last_trade_price","market":"0x0000000000000000000000000000000000000000000000000000000000000001","asset_id":"1","price":"0.505","side":"SELL","size":"1.25","timestamp":"45"}"#;
        let fact = MarketEvent::LastTrade {
            market: market.clone(),
            outcome: Outcome::Up,
            price: Decimal::new(505, 3),
            side: Side::Sell,
            size: Decimal::new(125, 2),
            timestamp_ms: 45,
        };

        let mut writer = RawCaptureWriter::new(Vec::new());
        writer.append(native_pm_frame(fact.clone(), raw, 0, 0)?)?;
        let tape = writer.into_inner();
        let mut reader = CaptureReplayReader::new(Cursor::new(tape));
        assert!(matches!(
            reader.next_signal(),
            Err(super::CaptureReplayError::MissingMarketTokens { .. })
        ));

        let mut tampered_price = fact.clone();
        if let MarketEvent::LastTrade { price, .. } = &mut tampered_price {
            *price = Decimal::new(506, 3);
        }
        let mut tampered_outcome = fact;
        if let MarketEvent::LastTrade { outcome, .. } = &mut tampered_outcome {
            *outcome = Outcome::Down;
        }

        for tampered in [tampered_price, tampered_outcome] {
            let mut writer = RawCaptureWriter::new(Vec::new());
            writer.append(native_pm_frame(tampered, raw, 0, 0)?)?;
            let mut reader = CaptureReplayReader::new_with_market_tokens(
                Cursor::new(writer.into_inner()),
                known_market_tokens(market.clone())?,
            );
            assert!(matches!(
                reader.next_signal(),
                Err(super::CaptureReplayError::Projection { message, .. })
                    if message == "raw Polymarket frame does not match its normalized witness"
            ));
        }
        Ok(())
    }

    fn recorded_order_fixture(
        raw_frames: [&[u8]; 4],
    ) -> Result<(Vec<u8>, [SourceEnvelope; 4]), Box<dyn std::error::Error>> {
        let market = MarketId::new("btc-5m")?;
        let mut pm_metadata = metadata("polymarket:market", 0, 42);
        pm_metadata.source_time_ms += 3;
        let pm = SourceEnvelope::PmMarket(PmMarketEnvelope {
            metadata: pm_metadata,
            raw_frame: Vec::new(),
            fact: MarketEvent::LastTrade {
                market,
                outcome: Outcome::Up,
                price: Decimal::new(123_456, 5),
                side: Side::Buy,
                size: Decimal::new(25_001, 3),
                timestamp_ms: 1_700_000_000_000,
            },
        });
        let mut trade_metadata = metadata("binance:aggTrade", 0, 7);
        trade_metadata.source_time_ms += 1;
        let trade = SourceEnvelope::CexReference(CexReferenceEnvelope {
            metadata: trade_metadata,
            raw_frame: Vec::new(),
            fact: CexReferenceEvent::Trade {
                asset: Asset::Btc,
                exchange: Exchange::Binance,
                aggregate_trade_id: 99,
                price: Decimal::new(123_456_789, 4),
                qty: Decimal::new(10_001, 5),
                is_buyer_maker: false,
                timestamp_ms: 1_700_000_000_001,
            },
        });
        let snapshot = SourceEnvelope::CexReference(CexReferenceEnvelope {
            metadata: metadata("polybolt:oracle", 0, 0),
            raw_frame: Vec::new(),
            fact: CexReferenceEvent::Twap60sSnapshot {
                asset: Asset::Btc,
                price: Decimal::new(987_654_321, 7),
                timestamp_ms: 1_700_000_000_002,
            },
        });
        let mut update_metadata = metadata("polybolt:oracle", 1, 1);
        update_metadata.source_time_ms -= 2;
        let update = SourceEnvelope::CexReference(CexReferenceEnvelope {
            metadata: update_metadata,
            raw_frame: Vec::new(),
            fact: CexReferenceEvent::Twap60sUpdate {
                asset: Asset::Btc,
                price: Decimal::new(987_654_322, 7),
                timestamp_ms: 1_700_000_000_003,
            },
        });
        let mut writer = RawCaptureWriter::new(Vec::new());
        writer.append(frame(&pm, raw_frames[0])?)?;
        writer.append(frame(&trade, raw_frames[1])?)?;
        writer.append(frame(&snapshot, raw_frames[2])?)?;
        writer.append(frame(&update, raw_frames[3])?)?;
        writer.append(RawCaptureDraft {
            schema_version: 3,
            source_id: "polybolt:oracle".into(),
            canonical_source_rank: 8,
            receipt_time_ms: 1_700_000_000_104,
            transport: Some(CaptureTransport {
                source_time_ms: Some(1_700_000_000_004),
                connection_id: "polybolt:oracle-connection".into(),
                connection_epoch: 2,
                frame_sequence: 2,
                ingest_sequence: 2,
            }),
            normalized_projection: None,
            payload: RawCapturePayload::Control {
                name: "heartbeat".into(),
                raw_bytes: Vec::new(),
            },
        })?;
        writer.append(RawCaptureDraft {
            schema_version: 3,
            source_id: "polybolt:oracle".into(),
            canonical_source_rank: 8,
            receipt_time_ms: 1_700_000_000_106,
            transport: Some(CaptureTransport {
                source_time_ms: Some(1_700_000_000_006),
                connection_id: "polybolt:oracle-connection".into(),
                connection_epoch: 2,
                frame_sequence: 4,
                ingest_sequence: 4,
            }),
            normalized_projection: None,
            payload: RawCapturePayload::Gap {
                range: Some(GapRange {
                    domain: GapSequenceDomain::Transport,
                    first_missing: 3,
                    last_missing: 3,
                }),
                unresolved_count: None,
                reason: "oracle frame missing".into(),
                raw_frame: b"oracle gap report".to_vec(),
            },
        })?;
        Ok((writer.into_inner(), [pm, trade, snapshot, update]))
    }

    #[tokio::test]
    async fn replays_pm_trade_twap_heartbeat_and_gap_in_capture_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let raw_pm = b"{ \"event\" : \"pm-trade\" }";
        let raw_trade = br#"{"e":"aggTrade","a":99,"p":"12345.6789","q":"0.10001","T":1700000000001,"m":false}"#;
        let raw_snapshot = b"oracle snapshot raw bytes";
        let raw_update = b"oracle update raw bytes";
        let (capture, expected) =
            recorded_order_fixture([raw_pm, raw_trade, raw_snapshot, raw_update])?;
        let (sink, mut signals) = mpsc::channel(16);
        let replay = CaptureReplaySource::new(Cursor::new(capture))
            .replay_recorded_order(sink)
            .await;
        let Err(error) = replay else {
            return Err("recorded gap unexpectedly replayed successfully".into());
        };
        assert!(matches!(
            error,
            pmkit_data::DataSourceError::ReplayGap { .. }
        ));
        let mut observed = Vec::new();
        while let Some(signal) = signals.recv().await {
            observed.push(signal);
        }
        assert_eq!(observed.len(), 6);
        let facts = observed[..4]
            .iter()
            .map(|signal| {
                let SourceSignal::Data(envelope) = signal else {
                    return Err(std::io::Error::other("expected source fact").into());
                };
                Ok(envelope.clone().into_strategy_fact())
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
        assert_eq!(facts, expected.map(SourceEnvelope::into_strategy_fact));
        let SourceSignal::Data(trade_replayed) = &observed[1] else {
            return Err("missing CEX trade".into());
        };
        assert!(
            matches!(trade_replayed.as_ref(), SourceEnvelope::CexReference(envelope)
            if envelope.raw_frame == raw_trade)
        );
        assert!(matches!(&observed[4], SourceSignal::Heartbeat(heartbeat)
            if heartbeat.raw_frame.is_empty()
                && heartbeat.metadata.receipt_time_ms == 1_700_000_000_104));
        assert!(matches!(&observed[5], SourceSignal::Gap(gap)
            if gap.raw_frame == b"oracle gap report"
                && gap.source_id == "polybolt:oracle"
                && gap.range == Some(pmkit_data::GapRange {
                    domain: pmkit_data::GapSequenceDomain::Transport,
                    first_missing: 3,
                    last_missing: 3,
                })
                && gap.metadata.as_ref().is_some_and(|metadata|
                    metadata.source_id == "polybolt:oracle"
                        && metadata.connection_epoch == 2
                        && metadata.frame_sequence == 4
                        && metadata.source_time_ms == 1_700_000_000_006)));
        let SourceSignal::Data(pm_replayed) = &observed[0] else {
            return Err("missing PM event".into());
        };
        assert!(
            matches!(pm_replayed.as_ref(), SourceEnvelope::PmMarket(envelope)
            if envelope.raw_frame == raw_pm
                && envelope.metadata.canonical_source_rank == 8
                && envelope.metadata.receipt_time_ms == 1_700_000_000_100)
        );
        let SourceSignal::Data(oracle_replayed) = &observed[2] else {
            return Err("missing oracle snapshot".into());
        };
        assert!(
            matches!(oracle_replayed.as_ref(), SourceEnvelope::CexReference(envelope)
            if envelope.raw_frame == raw_snapshot
                && envelope.metadata.canonical_source_rank == 8
                && envelope.metadata.ingest_sequence == 0
                && matches!(envelope.fact, CexReferenceEvent::Twap60sSnapshot { price, .. }
                    if price == Decimal::new(987_654_321, 7)))
        );
        let SourceSignal::Data(update_replayed) = &observed[3] else {
            return Err("missing oracle update".into());
        };
        assert!(
            matches!(update_replayed.as_ref(), SourceEnvelope::CexReference(envelope)
            if envelope.raw_frame == raw_update
                && matches!(envelope.fact, CexReferenceEvent::Twap60sUpdate { price, .. }
                    if price == Decimal::new(987_654_322, 7)))
        );
        assert!(matches!(
            facts[0],
            StrategyFact::Market(MarketEvent::LastTrade { .. })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn rejects_binance_vision_archive_before_witness_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let raw = b"99,1,1,99,99,1700000000000,false";
        let fact =
            pmkit_data::parse_binance_vision_agg_trade_row(std::str::from_utf8(raw)?, Asset::Btc)?;
        let mut source_metadata = metadata(super::BINANCE_VISION_SOURCE_ID, 0, 0);
        // The tape schema requires a receipt value, but this archive receipt is only a placeholder.
        source_metadata.receipt_time_ms = 1_700_000_000_100;
        let source = SourceEnvelope::CexReference(CexReferenceEnvelope {
            metadata: source_metadata,
            raw_frame: Vec::new(),
            fact,
        });
        let mut writer = RawCaptureWriter::new(Vec::new());
        writer.append(frame(&source, raw)?)?;

        let (sink, mut signals) = mpsc::channel(1);
        let error = CaptureReplaySource::new(Cursor::new(writer.into_inner()))
            .replay_recorded_order(sink)
            .await;
        let Err(pmkit_data::DataSourceError::ReplayGap { message }) = error else {
            return Err("Binance Vision archive capture did not return a replay gap".into());
        };
        assert_eq!(
            message,
            "historical source at ingest 0 has no original receipt time and cannot be replayed as a capture"
        );
        assert!(signals.recv().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn rejects_pmkit_cloud_before_generic_witness_replay()
    -> Result<(), Box<dyn std::error::Error>> {
        let raw = br#"{"event_time_ms":1700000000000,"event_ordinal":8,"payload":{"event_type":"book","asset_id":"token-up","bids":[{"price":"0.49","size":"2"}],"asks":[{"price":"0.51","size":"3"}]}}"#;
        let mut cloud_metadata = metadata("pmkit-cloud", 8, 0);
        cloud_metadata.source_time_ms = 1_700_000_000_000;
        cloud_metadata.receipt_time_ms = 1_700_000_000_000;
        let source = SourceEnvelope::PmMarket(PmMarketEnvelope {
            metadata: cloud_metadata,
            raw_frame: Vec::new(),
            fact: MarketEvent::BookUpdate {
                market: MarketId::new("btc-5m")?,
                outcome: Outcome::Up,
                bids: vec![(Decimal::new(49, 2), Decimal::from(2))],
                asks: vec![(Decimal::new(51, 2), Decimal::from(3))],
                timestamp_ms: 1_700_000_000_000,
            },
        });
        let mut writer = RawCaptureWriter::new(Vec::new());
        writer.append(frame(&source, raw)?)?;
        let tape = writer.into_inner();
        let mut reader = RawCaptureReader::new(Cursor::new(tape.clone()));
        let Some(record) = reader.next_record()? else {
            return Err("missing valid cloud capture record".into());
        };
        assert_eq!(record.capture.source_id, "pmkit-cloud");
        assert!(record.capture.normalized_projection.is_some());
        assert!(reader.next_record()?.is_none());

        let (sink, mut signals) = mpsc::channel(1);
        let error = CaptureReplaySource::new(Cursor::new(tape))
            .replay_recorded_order(sink)
            .await;
        let Err(pmkit_data::DataSourceError::ReplayGap { message }) = error else {
            return Err("PMKit Cloud historical record did not return a replay gap".into());
        };
        assert_eq!(
            message,
            "historical source at ingest 0 has no original receipt time and cannot be replayed as a capture"
        );
        assert!(signals.recv().await.is_none());
        Ok(())
    }

    #[test]
    fn rejects_binance_raw_frame_that_disagrees_with_its_witness()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = SourceEnvelope::CexReference(CexReferenceEnvelope {
            metadata: metadata("binance:aggTrade", 0, 0),
            raw_frame: Vec::new(),
            fact: CexReferenceEvent::Trade {
                asset: Asset::Btc,
                exchange: Exchange::Binance,
                aggregate_trade_id: 99,
                price: Decimal::from(2),
                qty: Decimal::ONE,
                is_buyer_maker: false,
                timestamp_ms: 1_700_000_000_000,
            },
        });
        let raw = br#"{"e":"aggTrade","a":99,"p":"1","q":"1","T":1700000000000,"m":false}"#;
        let mut writer = RawCaptureWriter::new(Vec::new());
        writer.append(frame(&source, raw)?)?;
        let mut reader = CaptureReplayReader::new(Cursor::new(writer.into_inner()));
        assert!(matches!(
            reader.next_signal(),
            Err(super::CaptureReplayError::Projection { .. })
        ));
        assert!(reader.next_signal()?.is_none());
        Ok(())
    }

    type PolyboltSnapshotFixture = ([i64; 4], [Vec<u8>; 4], [CexReferenceEvent; 4]);

    fn polybolt_snapshot_fixture() -> Result<PolyboltSnapshotFixture, Box<dyn std::error::Error>> {
        let timestamps = [
            1_700_000_000_010,
            1_700_000_000_020,
            1_700_000_000_030,
            1_700_000_000_040,
        ];
        let [
            first_timestamp,
            second_timestamp,
            update_timestamp,
            reset_timestamp,
        ] = timestamps;
        let snapshot_raw = serde_json::to_vec(&serde_json::json!({
            "v": 1,
            "channel": "price.crypto.twap",
            "seq": 10,
            "ts": first_timestamp,
            "snapshot": true,
            "payload": {
                "symbol": "btcusd",
                "source": "chainlink",
                "window_seconds": 60,
                "data": [
                    {"timestamp": first_timestamp, "full_accuracy_value": "100.125"},
                    {"timestamp": second_timestamp, "full_accuracy_value": "100.25"}
                ]
            }
        }))?;
        let heartbeat_raw = serde_json::to_vec(&serde_json::json!({
            "v": 1,
            "channel": "price.crypto.twap",
            "seq": 11,
            "ts": second_timestamp,
            "payload": {
                "symbol": "ethusd",
                "source": "chainlink",
                "window_seconds": 60,
                "timestamp": second_timestamp,
                "full_accuracy_value": "2000"
            }
        }))?;
        let update_raw = serde_json::to_vec(&serde_json::json!({
            "v": 1,
            "channel": "price.crypto.twap",
            "seq": 12,
            "ts": update_timestamp,
            "snapshot": false,
            "payload": {
                "symbol": "btcusd",
                "source": "chainlink",
                "window_seconds": 60,
                "timestamp": update_timestamp,
                "full_accuracy_value": "100.5"
            }
        }))?;
        let reset_raw = serde_json::to_vec(&serde_json::json!({
            "v": 1,
            "channel": "price.crypto.twap",
            "seq": 0,
            "ts": reset_timestamp,
            "snapshot": false,
            "payload": {
                "symbol": "btcusd",
                "source": "chainlink",
                "window_seconds": 60,
                "timestamp": reset_timestamp,
                "full_accuracy_value": "101"
            }
        }))?;
        let facts = [
            CexReferenceEvent::Twap60sSnapshot {
                asset: Asset::Btc,
                price: Decimal::new(100_125, 3),
                timestamp_ms: first_timestamp,
            },
            CexReferenceEvent::Twap60sSnapshot {
                asset: Asset::Btc,
                price: Decimal::new(10_025, 2),
                timestamp_ms: second_timestamp,
            },
            CexReferenceEvent::Twap60sUpdate {
                asset: Asset::Btc,
                price: Decimal::new(1_005, 1),
                timestamp_ms: update_timestamp,
            },
            CexReferenceEvent::Twap60sUpdate {
                asset: Asset::Btc,
                price: Decimal::from(101),
                timestamp_ms: reset_timestamp,
            },
        ];
        Ok((
            timestamps,
            [snapshot_raw, heartbeat_raw, update_raw, reset_raw],
            facts,
        ))
    }

    #[test]
    fn validates_polybolt_snapshot_points_update_heartbeat_and_epoch_reset()
    -> Result<(), Box<dyn std::error::Error>> {
        let (
            [
                first_timestamp,
                second_timestamp,
                update_timestamp,
                reset_timestamp,
            ],
            [snapshot_raw, heartbeat_raw, update_raw, reset_raw],
            [
                first_snapshot_fact,
                second_snapshot_fact,
                update_fact,
                reset_fact,
            ],
        ) = polybolt_snapshot_fixture()?;
        let mut first_metadata = metadata(POLYBOLT_SOURCE_ID, 0, 0);
        first_metadata.source_time_ms = first_timestamp;
        let mut second_metadata = metadata(POLYBOLT_SOURCE_ID, 0, 1);
        second_metadata.source_time_ms = second_timestamp;
        let first_point = SourceEnvelope::CexReference(CexReferenceEnvelope {
            metadata: first_metadata,
            raw_frame: Vec::new(),
            fact: first_snapshot_fact.clone(),
        });
        let second_point = SourceEnvelope::CexReference(CexReferenceEnvelope {
            metadata: second_metadata,
            raw_frame: Vec::new(),
            fact: second_snapshot_fact.clone(),
        });
        let mut update_metadata = metadata(POLYBOLT_SOURCE_ID, 2, 3);
        update_metadata.source_time_ms = update_timestamp;
        let update = SourceEnvelope::CexReference(CexReferenceEnvelope {
            metadata: update_metadata,
            raw_frame: Vec::new(),
            fact: update_fact.clone(),
        });
        let mut reset_metadata = metadata(POLYBOLT_SOURCE_ID, 0, 4);
        reset_metadata.connection_epoch = 3;
        reset_metadata.source_time_ms = reset_timestamp;
        let reset_update = SourceEnvelope::CexReference(CexReferenceEnvelope {
            metadata: reset_metadata,
            raw_frame: Vec::new(),
            fact: reset_fact.clone(),
        });

        let mut writer = RawCaptureWriter::new(Vec::new());
        writer.append(frame(&first_point, &snapshot_raw)?)?;
        writer.append(frame(&second_point, &snapshot_raw)?)?;
        writer.append(RawCaptureDraft {
            schema_version: 3,
            source_id: POLYBOLT_SOURCE_ID.into(),
            canonical_source_rank: 8,
            receipt_time_ms: 1_700_000_000_100,
            transport: Some(CaptureTransport {
                source_time_ms: Some(second_timestamp),
                connection_id: format!("{POLYBOLT_SOURCE_ID}-connection"),
                connection_epoch: 2,
                frame_sequence: 1,
                ingest_sequence: 2,
            }),
            normalized_projection: None,
            payload: RawCapturePayload::Control {
                name: "heartbeat".into(),
                raw_bytes: heartbeat_raw.clone(),
            },
        })?;
        writer.append(frame(&update, &update_raw)?)?;
        writer.append(frame(&reset_update, &reset_raw)?)?;

        let mut reader = CaptureReplayReader::new(Cursor::new(writer.into_inner()));
        for fact in [&first_snapshot_fact, &second_snapshot_fact] {
            let Some(SourceSignal::Data(envelope)) = reader.next_signal()? else {
                return Err("missing captured PolyBolt snapshot point".into());
            };
            assert!(
                matches!(envelope.as_ref(), SourceEnvelope::CexReference(envelope)
                if envelope.raw_frame == snapshot_raw && &envelope.fact == fact)
            );
        }
        assert!(
            matches!(reader.next_signal()?, Some(SourceSignal::Heartbeat(heartbeat))
            if heartbeat.raw_frame == heartbeat_raw)
        );
        for (raw, fact) in [
            (update_raw.as_slice(), &update_fact),
            (reset_raw.as_slice(), &reset_fact),
        ] {
            let Some(SourceSignal::Data(envelope)) = reader.next_signal()? else {
                return Err("missing captured PolyBolt update".into());
            };
            assert!(
                matches!(envelope.as_ref(), SourceEnvelope::CexReference(envelope)
                if envelope.raw_frame == raw && &envelope.fact == fact)
            );
        }
        assert!(reader.next_signal()?.is_none());
        Ok(())
    }

    #[test]
    fn rejects_polybolt_raw_price_that_disagrees_with_its_witness()
    -> Result<(), Box<dyn std::error::Error>> {
        let timestamp_ms = 1_700_000_000_010;
        let raw = serde_json::to_vec(&serde_json::json!({
            "v": 1,
            "channel": "price.crypto.twap",
            "seq": 10,
            "ts": timestamp_ms,
            "snapshot": false,
            "payload": {
                "symbol": "btcusd",
                "source": "chainlink",
                "window_seconds": 60,
                "timestamp": timestamp_ms,
                "full_accuracy_value": "100.125"
            }
        }))?;
        let mut source_metadata = metadata(POLYBOLT_SOURCE_ID, 0, 0);
        source_metadata.source_time_ms = timestamp_ms;
        let source = SourceEnvelope::CexReference(CexReferenceEnvelope {
            metadata: source_metadata,
            raw_frame: Vec::new(),
            fact: CexReferenceEvent::Twap60sUpdate {
                asset: Asset::Btc,
                price: Decimal::new(100_126, 3),
                timestamp_ms,
            },
        });
        let mut writer = RawCaptureWriter::new(Vec::new());
        writer.append(frame(&source, &raw)?)?;
        let mut reader = CaptureReplayReader::new(Cursor::new(writer.into_inner()));
        let Err(error) = reader.next_signal() else {
            return Err("false TWAP price was accepted".into());
        };
        assert!(
            error
                .to_string()
                .contains("raw PolyBolt frame does not match its normalized TWAP witness")
        );
        assert!(reader.next_signal()?.is_none());
        Ok(())
    }

    #[test]
    fn reader_emits_gap_then_stops_with_its_validated_details()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut writer = RawCaptureWriter::new(Vec::new());
        writer.append(RawCaptureDraft {
            schema_version: 1,
            source_id: "oracle".into(),
            canonical_source_rank: 4,
            receipt_time_ms: 20,
            transport: Some(CaptureTransport {
                source_time_ms: Some(10),
                connection_id: "connection".into(),
                connection_epoch: 0,
                frame_sequence: 1,
                ingest_sequence: 0,
            }),
            normalized_projection: None,
            payload: RawCapturePayload::Gap {
                range: Some(GapRange {
                    domain: GapSequenceDomain::Transport,
                    first_missing: 0,
                    last_missing: 0,
                }),
                unresolved_count: None,
                reason: "missing first frame".into(),
                raw_frame: b"gap".to_vec(),
            },
        })?;
        let mut reader = CaptureReplayReader::new(Cursor::new(writer.into_inner()));
        assert!(matches!(reader.next_signal()?, Some(SourceSignal::Gap(_))));
        assert!(matches!(
            reader.next_signal(),
            Err(super::CaptureReplayError::Gap {
                ingest_sequence: 0,
                range: Some(GapRange {
                    domain: GapSequenceDomain::Transport,
                    first_missing: 0,
                    last_missing: 0,
                }),
                unresolved_count: None,
                ..
            })
        ));
        assert!(reader.next_signal()?.is_none());
        Ok(())
    }

    #[test]
    fn local_gap_has_no_transport_metadata_or_effect_on_frame_continuity()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut writer = RawCaptureWriter::new(Vec::new());
        writer.append(heartbeat("quiet", 4, 17))?;
        writer.append(RawCaptureDraft {
            schema_version: 3,
            source_id: "quiet".into(),
            canonical_source_rank: 4,
            receipt_time_ms: 99,
            transport: None,
            normalized_projection: None,
            payload: RawCapturePayload::Gap {
                range: None,
                unresolved_count: Some(3),
                reason: "local silence".into(),
                raw_frame: Vec::new(),
            },
        })?;
        writer.append(heartbeat("quiet", 5, 18))?;
        let tape = writer.into_inner();
        let mut audit = RawCaptureReader::new_audit(Cursor::new(tape.clone()));
        assert!(audit.next_record()?.is_some());
        assert!(audit.next_record()?.is_some());
        assert!(audit.next_record()?.is_some());
        assert!(audit.next_record()?.is_none());

        let mut reader = CaptureReplayReader::new(Cursor::new(tape));
        assert!(matches!(
            reader.next_signal()?,
            Some(SourceSignal::Heartbeat(heartbeat)) if heartbeat.metadata.frame_sequence == 4
        ));
        let Some(SourceSignal::Gap(gap)) = reader.next_signal()? else {
            return Err("missing unobserved local gap".into());
        };
        assert_eq!(gap.source_id, "quiet");
        assert_eq!(gap.detected_at_ms, 99);
        assert!(gap.metadata.is_none());
        assert!(gap.raw_frame.is_empty());
        assert_eq!(gap.range, None);
        assert_eq!(gap.unresolved_count, Some(3));
        assert!(matches!(
            reader.next_signal(),
            Err(super::CaptureReplayError::Gap {
                range: None,
                unresolved_count: Some(3),
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn channel_gap_bounds_do_not_replace_physical_frame_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut writer = RawCaptureWriter::new(Vec::new());
        writer.append(RawCaptureDraft {
            schema_version: 3,
            source_id: "channel".into(),
            canonical_source_rank: 4,
            receipt_time_ms: 22,
            transport: Some(CaptureTransport {
                source_time_ms: Some(11),
                connection_id: "channel-connection".into(),
                connection_epoch: 1,
                frame_sequence: 4,
                ingest_sequence: 27,
            }),
            normalized_projection: None,
            payload: RawCapturePayload::Gap {
                range: Some(GapRange {
                    domain: GapSequenceDomain::Channel,
                    first_missing: 200,
                    last_missing: 201,
                }),
                unresolved_count: Some(2),
                reason: "channel sequence lost".into(),
                raw_frame: Vec::new(),
            },
        })?;
        writer.append(heartbeat("channel", 5, 28))?;
        let tape = writer.into_inner();
        let mut audit = RawCaptureReader::new_audit(Cursor::new(tape.clone()));
        assert!(audit.next_record()?.is_some());
        assert!(audit.next_record()?.is_some());
        assert!(audit.next_record()?.is_none());

        let mut reader = CaptureReplayReader::new(Cursor::new(tape));
        let Some(SourceSignal::Gap(gap)) = reader.next_signal()? else {
            return Err("missing reported channel gap".into());
        };
        assert_eq!(gap.detected_at_ms, 22);
        assert_eq!(gap.raw_frame, Vec::<u8>::new());
        assert_eq!(
            gap.range,
            Some(pmkit_data::GapRange {
                domain: pmkit_data::GapSequenceDomain::Channel,
                first_missing: 200,
                last_missing: 201,
            })
        );
        assert_eq!(gap.unresolved_count, Some(2));
        let Some(metadata) = gap.metadata else {
            return Err("reported channel gap lost its transport metadata".into());
        };
        assert_eq!(metadata.source_time_ms, 11);
        assert_eq!(metadata.receipt_time_ms, 22);
        assert_eq!(metadata.frame_sequence, 4);
        assert_eq!(metadata.ingest_sequence, 27);
        assert!(matches!(
            reader.next_signal(),
            Err(super::CaptureReplayError::Gap {
                range: Some(GapRange {
                    domain: GapSequenceDomain::Channel,
                    first_missing: 200,
                    last_missing: 201,
                }),
                unresolved_count: Some(2),
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn rejects_a_transport_gap_reported_two_frames_after_its_range()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut writer = RawCaptureWriter::new(Vec::new());
        writer.append(heartbeat("transport", 3, 10))?;
        let error = writer.append(RawCaptureDraft {
            schema_version: 3,
            source_id: "transport".into(),
            canonical_source_rank: 4,
            receipt_time_ms: 22,
            transport: Some(CaptureTransport {
                source_time_ms: Some(11),
                connection_id: "transport-connection".into(),
                connection_epoch: 1,
                frame_sequence: 7,
                ingest_sequence: 12,
            }),
            normalized_projection: None,
            payload: RawCapturePayload::Gap {
                range: Some(GapRange {
                    domain: GapSequenceDomain::Transport,
                    first_missing: 4,
                    last_missing: 5,
                }),
                unresolved_count: None,
                reason: "transport sequence lost".into(),
                raw_frame: b"gap".to_vec(),
            },
        });
        assert!(matches!(
            error,
            Err(pmkit_tape::RawCaptureError::MalformedRecord { message })
                if message == "transport gap reporting frame must immediately follow its range"
        ));
        Ok(())
    }

    #[test]
    fn missing_raw_source_time_cannot_be_filled_from_a_gap_witness()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut writer = RawCaptureWriter::new(Vec::new());
        writer.append(RawCaptureDraft {
            schema_version: 3,
            source_id: "oracle".into(),
            canonical_source_rank: 4,
            receipt_time_ms: 20,
            transport: Some(CaptureTransport {
                source_time_ms: None,
                connection_id: "oracle-connection".into(),
                connection_epoch: 1,
                frame_sequence: 1,
                ingest_sequence: 7,
            }),
            normalized_projection: Some(serde_json::json!({
                "source_id": "oracle",
                "source_time_ms": 10,
                "receipt_time_ms": 20,
                "connection_id": "oracle-connection",
                "connection_epoch": 1,
                "frame_sequence": 1,
                "ingest_sequence": 7,
            })),
            payload: RawCapturePayload::Gap {
                range: Some(GapRange {
                    domain: GapSequenceDomain::Transport,
                    first_missing: 0,
                    last_missing: 0,
                }),
                unresolved_count: None,
                reason: "source timestamp absent".into(),
                raw_frame: Vec::new(),
            },
        })?;
        let mut reader = CaptureReplayReader::new(Cursor::new(writer.into_inner()));
        assert!(matches!(
            reader.next_signal(),
            Err(super::CaptureReplayError::Projection { message, .. })
                if message == "missing source timestamp"
        ));
        assert!(reader.next_signal()?.is_none());
        Ok(())
    }

    #[test]
    fn data_requires_source_time_in_transport_not_just_its_witness()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = SourceEnvelope::CexReference(CexReferenceEnvelope {
            metadata: metadata("oracle:test", 0, 5),
            raw_frame: Vec::new(),
            fact: CexReferenceEvent::Twap60sUpdate {
                asset: Asset::Btc,
                price: Decimal::from(1),
                timestamp_ms: 1_700_000_000_000,
            },
        });
        let mut draft = frame(&source, b"actual raw frame")?;
        draft
            .transport
            .as_mut()
            .ok_or("missing test transport")?
            .source_time_ms = None;
        let mut writer = RawCaptureWriter::new(Vec::new());
        writer.append(draft)?;
        let mut reader = CaptureReplayReader::new(Cursor::new(writer.into_inner()));
        assert!(matches!(
            reader.next_signal(),
            Err(super::CaptureReplayError::Projection { message, .. })
                if message == "missing source timestamp"
        ));
        assert!(reader.next_signal()?.is_none());
        Ok(())
    }
}
