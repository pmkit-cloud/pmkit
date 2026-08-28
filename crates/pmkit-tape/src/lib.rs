//! Local user-tape sinks for `PMKit` authenticated-account frames.
//!
//! A [`UserTapeSink`] records [`PmAccountEnvelope`]s to durable local storage.
//! [`JsonLinesTape`] writes one JSON object per line over any [`Write`], which
//! `zstd`-compresses well and is trivial to replay.

use std::io::{self, Write};

use pmkit_book::Side;
use pmkit_event::{
    CexReferenceEnvelope, CexReferenceEvent, FillIdentity, Liquidity, MarketEvent,
    PmAccountEnvelope, PmAccountEvent, PmMarketEnvelope, PolymarketReferenceEnvelope,
    SettlementIdentity, StreamMetadata,
};
use serde_json::json;

mod raw;
mod spool;
mod spool_closed;
mod spool_fs;
mod spool_record;
mod spool_recovery;

#[cfg(test)]
mod spool_closed_tests;
#[cfg(test)]
mod spool_manual_tests;
#[cfg(test)]
mod spool_tests;

pub use raw::{
    RAW_TAPE_SCHEMA_VERSION, RawJsonLinesTape, RawTapeError, RawTapeRecord, RawTapeSink,
    decode_raw_record, recoverable_raw_tape_prefix,
};
pub use spool::{
    RAW_SPOOL_SCHEMA_VERSION, RecoveryUncertainty, SpoolCheckpoint, SpoolChunk, SpoolError,
    SpoolFrame, TailRecovery,
};
pub use spool_closed::{VerifiedSpoolChunk, enumerate_closed_chunks};
pub use spool_fs::RawSpoolWriter;
pub use spool_recovery::recover_open_chunk;

/// A sink that records market events to a durable local tape.
pub trait UserTapeSink {
    /// Appends one event to the tape.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying writer fails.
    fn append(&mut self, envelope: &PmAccountEnvelope) -> io::Result<()>;

    /// Flushes buffered data to the underlying writer.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying writer fails to flush.
    fn flush(&mut self) -> io::Result<()>;
}

/// A JSON-lines tape over any writer.
#[derive(Debug)]
pub struct JsonLinesTape<W: Write> {
    writer: W,
}

impl<W: Write> JsonLinesTape<W> {
    /// Creates a tape that writes to `writer`.
    pub const fn new(writer: W) -> Self {
        Self { writer }
    }

    /// Consumes the tape and returns the underlying writer.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W: Write> UserTapeSink for JsonLinesTape<W> {
    fn append(&mut self, envelope: &PmAccountEnvelope) -> io::Result<()> {
        let line =
            serde_json::to_string(&account_envelope_json(envelope)).map_err(io::Error::other)?;
        writeln!(self.writer, "{line}")
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

const fn side_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

const fn liquidity_str(liquidity: Liquidity) -> &'static str {
    match liquidity {
        Liquidity::Maker => "maker",
        Liquidity::Taker => "taker",
    }
}

fn fill_identity_json(identity: &FillIdentity) -> serde_json::Value {
    match identity {
        FillIdentity::Venue(id) => json!({
            "source": "venue",
            "id": id,
        }),
        FillIdentity::Transport {
            source_id,
            connection_id,
            connection_epoch,
            frame_sequence,
        } => json!({
            "source": "transport",
            "source_id": source_id,
            "connection_id": connection_id,
            "connection_epoch": connection_epoch,
            "frame_sequence": frame_sequence,
        }),
    }
}

fn settlement_identity_json(identity: &SettlementIdentity) -> serde_json::Value {
    match identity {
        SettlementIdentity::Venue(id) => json!({
            "source": "venue",
            "id": id,
        }),
        SettlementIdentity::Transport {
            source_id,
            connection_id,
            connection_epoch,
            frame_sequence,
        } => json!({
            "source": "transport",
            "source_id": source_id,
            "connection_id": connection_id,
            "connection_epoch": connection_epoch,
            "frame_sequence": frame_sequence,
        }),
    }
}

fn levels(levels: &[(rust_decimal::Decimal, rust_decimal::Decimal)]) -> Vec<serde_json::Value> {
    levels
        .iter()
        .map(|(price, size)| json!([price.to_string(), size.to_string()]))
        .collect()
}

// CLIPPY-ALLOW: preserves the portable JSON contract for every market fact variant.
#[allow(clippy::too_many_lines)]
/// Serializes one normalized PM market fact as a portable JSON payload.
#[must_use]
pub fn event_json(event: &MarketEvent) -> serde_json::Value {
    match event {
        MarketEvent::BookUpdate {
            market,
            outcome,
            bids,
            asks,
            timestamp_ms,
        } => json!({
            "kind": "book_update",
            "ts": timestamp_ms,
            "market": market.to_string(),
            "outcome": outcome.to_string(),
            "bids": levels(bids),
            "asks": levels(asks),
        }),
        MarketEvent::BestBidAsk {
            market,
            outcome,
            bid,
            ask,
            timestamp_ms,
        } => json!({
            "kind": "best_bid_ask",
            "ts": timestamp_ms,
            "market": market.to_string(),
            "outcome": outcome.to_string(),
            "bid": bid.to_string(),
            "ask": ask.to_string(),
        }),
        MarketEvent::LastTrade {
            market,
            outcome,
            price,
            side,
            size,
            timestamp_ms,
        } => json!({
            "kind": "last_trade",
            "ts": timestamp_ms,
            "market": market.to_string(),
            "outcome": outcome.to_string(),
            "price": price.to_string(),
            "side": side_str(*side),
            "size": size.to_string(),
        }),
        MarketEvent::Fill {
            strategy,
            order_id,
            market,
            outcome,
            price,
            size,
            side,
            fee,
            liquidity,
            timestamp_ms,
        } => json!({
            "kind": "fill",
            "ts": timestamp_ms,
            "strategy": strategy.as_ref().map(ToString::to_string),
            "order_id": order_id,
            "market": market.to_string(),
            "outcome": outcome.to_string(),
            "price": price.to_string(),
            "size": size.to_string(),
            "side": side_str(*side),
            "fee": fee.to_string(),
            "liquidity": liquidity_str(*liquidity),
        }),
        MarketEvent::OrderAck {
            strategy,
            order_id,
            timestamp_ms,
        } => json!({
            "kind": "order_ack",
            "ts": timestamp_ms,
            "strategy": strategy.as_ref().map(ToString::to_string),
            "order_id": order_id,
        }),
        MarketEvent::Tick { timestamp_ms } => json!({
            "kind": "tick",
            "ts": timestamp_ms,
        }),
    }
}

/// Serializes a PM market envelope while preserving its transport metadata.
#[must_use]
pub fn market_envelope_json(envelope: &PmMarketEnvelope) -> serde_json::Value {
    envelope_json(&envelope.metadata, &event_json(&envelope.fact))
}

/// Serializes a PM account envelope while preserving its transport metadata.
#[must_use]
pub fn account_envelope_json(envelope: &PmAccountEnvelope) -> serde_json::Value {
    let payload = match &envelope.fact {
        PmAccountEvent::Fill {
            identity,
            strategy,
            order_id,
            market,
            outcome,
            price,
            size,
            side,
            fee,
            liquidity,
            timestamp_ms,
        } => json!({
            "kind": "fill",
            "ts": timestamp_ms,
            "identity": fill_identity_json(identity),
            "strategy": strategy.as_ref().map(ToString::to_string),
            "order_id": order_id,
            "market": market.to_string(),
            "outcome": outcome.to_string(),
            "price": price.to_string(),
            "size": size.to_string(),
            "side": side_str(*side),
            "fee": fee.to_string(),
            "liquidity": liquidity_str(*liquidity),
        }),
        PmAccountEvent::OrderAck {
            strategy,
            order_id,
            timestamp_ms,
        } => json!({
            "kind": "order_ack",
            "ts": timestamp_ms,
            "strategy": strategy.as_ref().map(ToString::to_string),
            "order_id": order_id,
        }),
        PmAccountEvent::OrderCancelled {
            strategy,
            order_id,
            timestamp_ms,
        } => json!({
            "kind": "order_cancelled",
            "ts": timestamp_ms,
            "strategy": strategy.as_ref().map(ToString::to_string),
            "order_id": order_id,
        }),
        PmAccountEvent::OrderRejected {
            strategy,
            order_id,
            reason,
            timestamp_ms,
        } => json!({
            "kind": "order_rejected",
            "ts": timestamp_ms,
            "strategy": strategy.as_ref().map(ToString::to_string),
            "order_id": order_id,
            "reason": reason,
        }),
        PmAccountEvent::OrderStatus {
            strategy,
            order_id,
            status,
            timestamp_ms,
        } => json!({
            "kind": "order_status",
            "ts": timestamp_ms,
            "strategy": strategy.as_ref().map(ToString::to_string),
            "order_id": order_id,
            "status": status,
        }),
        PmAccountEvent::Settlement {
            identity,
            market,
            outcome,
            settled_size,
            proceeds,
            timestamp_ms,
        } => json!({
            "kind": "settlement",
            "ts": timestamp_ms,
            "identity": settlement_identity_json(identity),
            "market": market.to_string(),
            "outcome": outcome.to_string(),
            "settled_size": settled_size.to_string(),
            "proceeds": proceeds.to_string(),
        }),
    };
    let mut value = envelope_json(&envelope.metadata, &payload);
    value["portfolio"] = json!(envelope.portfolio.to_string());
    value
}

/// Serializes a CEX reference envelope while preserving its transport metadata.
#[must_use]
pub fn reference_envelope_json(envelope: &CexReferenceEnvelope) -> serde_json::Value {
    let payload = match &envelope.fact {
        CexReferenceEvent::Trade {
            asset,
            exchange,
            aggregate_trade_id,
            price,
            qty,
            is_buyer_maker,
            timestamp_ms,
        } => json!({
            "kind": "reference_trade",
            "ts": timestamp_ms,
            "asset": asset.to_string(),
            "exchange": exchange.to_string(),
            "aggregate_trade_id": aggregate_trade_id,
            "price": price.to_string(),
            "qty": qty.to_string(),
            "is_buyer_maker": is_buyer_maker,
        }),
    };
    envelope_json(&envelope.metadata, &payload)
}

/// Serializes a Polymarket RTDS envelope while preserving its transport metadata.
#[must_use]
pub fn polymarket_reference_envelope_json(
    envelope: &PolymarketReferenceEnvelope,
) -> serde_json::Value {
    let fact = &envelope.fact;
    let payload = json!({
        "kind": "polymarket_twap",
        "ts": fact.timestamp_ms,
        "provider_timestamp_ms": fact.provider_timestamp_ms,
        "asset": fact.asset.to_string(),
        "symbol": fact.symbol,
        "window_s": fact.window_s,
        "value": fact.value,
        "full_accuracy_value": fact.full_accuracy_value,
    });
    envelope_json(&envelope.metadata, &payload)
}

fn envelope_json(metadata: &StreamMetadata, payload: &serde_json::Value) -> serde_json::Value {
    json!({
        "schema_version": metadata.schema_version,
        "source_id": metadata.source_id,
        "source_time_ms": metadata.source_time_ms,
        "canonical_source_rank": metadata.canonical_source_rank,
        "receipt_time_ms": metadata.receipt_time_ms,
        "connection_id": metadata.connection_id,
        "connection_epoch": metadata.connection_epoch,
        "frame_sequence": metadata.frame_sequence,
        "ingest_sequence": metadata.ingest_sequence,
        "payload": payload,
    })
}

#[cfg(test)]
mod tests {
    use super::{JsonLinesTape, UserTapeSink, account_envelope_json};
    use pmkit_core::{MarketId, PortfolioId};
    use pmkit_event::{PmAccountEnvelope, PmAccountEvent, SettlementIdentity, StreamMetadata};
    use pmkit_market::Outcome;
    use rust_decimal::Decimal;

    #[test]
    fn writes_one_json_line_per_account_envelope() -> Result<(), Box<dyn std::error::Error>> {
        let mut tape = JsonLinesTape::new(Vec::new());
        tape.append(&PmAccountEnvelope {
            portfolio: PortfolioId::new("paper")?,
            metadata: StreamMetadata {
                schema_version: 1,
                source_id: "polymarket".into(),
                source_time_ms: 1,
                canonical_source_rank: 2,
                receipt_time_ms: 2,
                connection_id: "ws-1".into(),
                connection_epoch: 3,
                frame_sequence: 4,
                ingest_sequence: 3,
            },
            raw_frame: Vec::new(),
            fact: PmAccountEvent::OrderAck {
                strategy: None,
                order_id: "order-1".into(),
                timestamp_ms: 1,
            },
        })?;
        tape.flush()?;

        let text = String::from_utf8(tape.into_inner())?;
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("\"kind\":\"order_ack\""));
        assert!(lines[0].contains("\"portfolio\":\"paper\""));
        assert!(lines[0].contains("\"canonical_source_rank\":2"));
        assert!(lines[0].contains("\"connection_epoch\":3"));
        assert!(lines[0].contains("\"frame_sequence\":4"));
        assert!(lines[0].contains("\"ingest_sequence\":3"));
        Ok(())
    }

    #[test]
    fn settlement_transport_identity_survives_json_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given: a settlement identified by exact transport coordinates.
        let envelope = PmAccountEnvelope {
            portfolio: PortfolioId::new("paper")?,
            metadata: StreamMetadata {
                schema_version: 4,
                source_id: "polymarket:user-ws".into(),
                source_time_ms: 1,
                canonical_source_rank: 0,
                receipt_time_ms: 2,
                connection_id: "account-1".into(),
                connection_epoch: 3,
                frame_sequence: 4,
                ingest_sequence: 5,
            },
            raw_frame: Vec::new(),
            fact: PmAccountEvent::Settlement {
                identity: SettlementIdentity::Transport {
                    source_id: "polymarket:user-ws".into(),
                    connection_id: "account-1".into(),
                    connection_epoch: 3,
                    frame_sequence: 4,
                },
                market: MarketId::new("btc-5m")?,
                outcome: Outcome::Up,
                settled_size: Decimal::from(10),
                proceeds: Decimal::from(10),
                timestamp_ms: 1,
            },
        };

        // When: the account envelope crosses the durable JSON boundary.
        let value = account_envelope_json(&envelope);

        // Then: identity is transport provenance, not settlement economics.
        assert_eq!(value["payload"]["identity"]["source"], "transport");
        assert_eq!(
            value["payload"]["identity"]["source_id"],
            "polymarket:user-ws"
        );
        assert_eq!(value["payload"]["identity"]["connection_epoch"], 3);
        assert_eq!(value["payload"]["identity"]["frame_sequence"], 4);
        Ok(())
    }
}
