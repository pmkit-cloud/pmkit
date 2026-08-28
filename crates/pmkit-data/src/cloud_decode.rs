use pmkit_book::Side;
use pmkit_core::MarketId;
use pmkit_event::{MarketEvent, PmMarketEnvelope, SourceEnvelope, StreamMetadata};
use pmkit_market::Outcome;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

use super::{cloud_http::Segment, cloud_types::CloudReplayError};
use crate::SourceSignal;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    event_time_ms: i64,
    event_ordinal: i64,
    payload: Value,
}

pub(super) fn decode(
    segment: &Segment,
    bytes: &[u8],
) -> Result<Vec<SourceSignal>, CloudReplayError> {
    let market =
        MarketId::new(&segment.market_id).map_err(|_| CloudReplayError::MalformedResponse)?;
    let mut signals = Vec::new();
    let mut previous_ordinal = None;
    for (index, line) in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .enumerate()
    {
        let row =
            serde_json::from_slice::<Row>(line).map_err(|_| CloudReplayError::MalformedResponse)?;
        if row.event_time_ms < segment.from_ts_ms
            || row.event_time_ms > segment.to_ts_ms
            || row.event_ordinal < 0
            || previous_ordinal.is_some_and(|previous| row.event_ordinal <= previous)
        {
            return Err(CloudReplayError::MalformedResponse);
        }
        previous_ordinal = Some(row.event_ordinal);
        let fact = market_event(market.clone(), segment, &row)?;
        let ingest_sequence =
            u64::try_from(index).map_err(|_| CloudReplayError::MalformedResponse)?;
        signals.push(SourceSignal::Data(Box::new(SourceEnvelope::PmMarket(
            PmMarketEnvelope {
                metadata: StreamMetadata {
                    schema_version: 1,
                    source_id: "pmkit-cloud".into(),
                    source_time_ms: row.event_time_ms,
                    canonical_source_rank: row.event_ordinal,
                    receipt_time_ms: row.event_time_ms,
                    connection_id: segment.id.clone(),
                    connection_epoch: 0,
                    frame_sequence: row.event_ordinal,
                    ingest_sequence,
                },
                raw_frame: line.to_vec(),
                fact,
            },
        ))));
    }
    if signals.is_empty() {
        return Err(CloudReplayError::MalformedResponse);
    }
    Ok(signals)
}

fn market_event(
    market: MarketId,
    segment: &Segment,
    row: &Row,
) -> Result<MarketEvent, CloudReplayError> {
    let outcome = outcome(segment, &row.payload)?;
    let kind = row.payload.get("event_type").and_then(Value::as_str);
    match kind {
        Some("book") => Ok(MarketEvent::BookUpdate {
            market,
            outcome,
            bids: levels(&row.payload, "bids")?,
            asks: levels(&row.payload, "asks")?,
            timestamp_ms: row.event_time_ms,
        }),
        Some("best_bid_ask") => Ok(MarketEvent::BestBidAsk {
            market,
            outcome,
            bid: decimal(&row.payload, "best_bid")?,
            ask: decimal(&row.payload, "best_ask")?,
            timestamp_ms: row.event_time_ms,
        }),
        Some("last_trade_price") => Ok(MarketEvent::LastTrade {
            market,
            outcome,
            price: decimal(&row.payload, "price")?,
            side: side(&row.payload)?,
            size: decimal(&row.payload, "size")?,
            timestamp_ms: row.event_time_ms,
        }),
        _ => Err(CloudReplayError::MalformedResponse),
    }
}

fn outcome(segment: &Segment, payload: &Value) -> Result<Outcome, CloudReplayError> {
    let token = payload.get("asset_id").and_then(Value::as_str);
    let label = segment
        .outcome_tokens
        .iter()
        .find(|mapping| Some(mapping.token_id.as_str()) == token)
        .map(|mapping| mapping.outcome.to_ascii_lowercase());
    match label.as_deref() {
        Some("up" | "yes") => Ok(Outcome::Up),
        Some("down" | "no") => Ok(Outcome::Down),
        _ => Err(CloudReplayError::MalformedResponse),
    }
}

fn decimal(value: &Value, key: &str) -> Result<Decimal, CloudReplayError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or(CloudReplayError::MalformedResponse)?
        .parse()
        .map_err(|_| CloudReplayError::MalformedResponse)
}

fn levels(value: &Value, key: &str) -> Result<Vec<(Decimal, Decimal)>, CloudReplayError> {
    value
        .get(key)
        .and_then(Value::as_array)
        .ok_or(CloudReplayError::MalformedResponse)?
        .iter()
        .map(|level| Ok((decimal(level, "price")?, decimal(level, "size")?)))
        .collect()
}

fn side(value: &Value) -> Result<Side, CloudReplayError> {
    match value.get("side").and_then(Value::as_str) {
        Some("BUY") => Ok(Side::Buy),
        Some("SELL") => Ok(Side::Sell),
        _ => Err(CloudReplayError::MalformedResponse),
    }
}
