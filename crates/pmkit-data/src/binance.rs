use pmkit_event::CexReferenceEvent;
use pmkit_market::{Asset, Exchange};
use rust_decimal::Decimal;
use serde_json::{Map, Value};
use thiserror::Error;

use crate::DataSourceError;

/// A CEX history source that can reproduce live reference events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CexHistorySource {
    /// Binance Vision `aggTrades` archives.
    BinanceVisionAggTrades,
}

/// Returns the official history source matched to an exchange's live feed.
///
/// # Errors
///
/// Returns [`DataSourceError::HistoryUnavailable`] when the exchange has no
/// matched official archive and therefore cannot supply strategy input.
pub const fn binance_history_source(
    exchange: Exchange,
) -> Result<CexHistorySource, DataSourceError> {
    match exchange {
        Exchange::Binance => Ok(CexHistorySource::BinanceVisionAggTrades),
        Exchange::Chainlink
        | Exchange::Vatic
        | Exchange::Bybit
        | Exchange::Coinbase
        | Exchange::Okx
        | Exchange::Kraken => Err(DataSourceError::HistoryUnavailable { exchange }),
    }
}

/// A malformed Binance aggregate-trade payload or archive row.
#[derive(Debug, Error)]
pub enum BinanceAggTradeParseError {
    /// The live payload was not valid JSON.
    #[error("invalid Binance aggregate-trade JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    /// A required field was absent.
    #[error("missing Binance aggregate-trade field: {0}")]
    MissingField(&'static str),
    /// The payload was not an `aggTrade` event.
    #[error("expected Binance aggTrade event")]
    WrongEventType,
    /// The aggregate trade ID was invalid.
    #[error("invalid Binance aggregate trade ID")]
    InvalidAggregateTradeId,
    /// The exchange timestamp was invalid.
    #[error("invalid Binance aggregate-trade timestamp")]
    InvalidTimestamp,
    /// The trade price was invalid.
    #[error("invalid Binance aggregate-trade price")]
    InvalidPrice,
    /// The trade quantity was invalid.
    #[error("invalid Binance aggregate-trade quantity")]
    InvalidQuantity,
    /// The buyer-maker indicator was invalid.
    #[error("invalid Binance aggregate-trade buyer-maker flag")]
    InvalidBuyerMaker,
    /// The Vision row did not contain the official required columns.
    #[error("invalid Binance Vision aggTrades row")]
    InvalidVisionRow,
}

const VISION_MICROSECOND_THRESHOLD: i64 = 1_735_689_600_000_000;
pub const BINANCE_REFERENCE_SOURCE_ID: &str = "binance:aggTrade";

#[derive(Debug, Clone, Copy)]
struct BinanceAggTradeFields {
    asset: Asset,
    aggregate_trade_id: u64,
    price: Decimal,
    qty: Decimal,
    timestamp_ms: i64,
    is_buyer_maker: bool,
}

/// Parses a Binance `@aggTrade` payload into the replayable reference fact.
///
/// # Errors
///
/// Returns [`BinanceAggTradeParseError`] when an official field is absent or
/// malformed.
pub fn parse_binance_agg_trade_live(
    payload: &str,
    asset: Asset,
) -> Result<CexReferenceEvent, BinanceAggTradeParseError> {
    let value: Value = serde_json::from_str(payload)?;
    let object = value
        .as_object()
        .ok_or(BinanceAggTradeParseError::WrongEventType)?;
    if json_field(object, "e")?.as_str() != Some("aggTrade") {
        return Err(BinanceAggTradeParseError::WrongEventType);
    }

    binance_trade_from_fields(BinanceAggTradeFields {
        asset,
        aggregate_trade_id: json_field(object, "a")?
            .as_u64()
            .ok_or(BinanceAggTradeParseError::InvalidAggregateTradeId)?,
        price: json_field(object, "p")?
            .as_str()
            .ok_or(BinanceAggTradeParseError::InvalidPrice)?
            .parse()
            .map_err(|_| BinanceAggTradeParseError::InvalidPrice)?,
        qty: json_field(object, "q")?
            .as_str()
            .ok_or(BinanceAggTradeParseError::InvalidQuantity)?
            .parse()
            .map_err(|_| BinanceAggTradeParseError::InvalidQuantity)?,
        timestamp_ms: json_field(object, "T")?
            .as_i64()
            .ok_or(BinanceAggTradeParseError::InvalidTimestamp)?,
        is_buyer_maker: json_field(object, "m")?
            .as_bool()
            .ok_or(BinanceAggTradeParseError::InvalidBuyerMaker)?,
    })
}

/// Parses one official Binance Vision `aggTrades` CSV row into the reference fact.
///
/// # Errors
///
/// Returns [`BinanceAggTradeParseError`] when an official column is absent or
/// malformed.
pub fn parse_binance_vision_agg_trade_row(
    row: &str,
    asset: Asset,
) -> Result<CexReferenceEvent, BinanceAggTradeParseError> {
    let mut columns = row.split(',');
    let aggregate_trade_id = vision_column(&mut columns)?
        .parse()
        .map_err(|_| BinanceAggTradeParseError::InvalidAggregateTradeId)?;
    let price = vision_column(&mut columns)?
        .parse()
        .map_err(|_| BinanceAggTradeParseError::InvalidPrice)?;
    let qty = vision_column(&mut columns)?
        .parse()
        .map_err(|_| BinanceAggTradeParseError::InvalidQuantity)?;
    let _first_trade_id = vision_column(&mut columns)?;
    let _last_trade_id = vision_column(&mut columns)?;
    let timestamp_ms = vision_column(&mut columns)?
        .parse()
        .map_err(|_| BinanceAggTradeParseError::InvalidTimestamp)?;
    let is_buyer_maker = vision_column(&mut columns)?
        .parse()
        .map_err(|_| BinanceAggTradeParseError::InvalidBuyerMaker)?;

    binance_trade_from_fields(BinanceAggTradeFields {
        asset,
        aggregate_trade_id,
        price,
        qty,
        timestamp_ms,
        is_buyer_maker,
    })
}

const fn binance_trade_from_fields(
    fields: BinanceAggTradeFields,
) -> Result<CexReferenceEvent, BinanceAggTradeParseError> {
    let timestamp_ms = if fields.timestamp_ms >= VISION_MICROSECOND_THRESHOLD {
        fields.timestamp_ms / 1_000
    } else {
        fields.timestamp_ms
    };
    if timestamp_ms < 0 {
        return Err(BinanceAggTradeParseError::InvalidTimestamp);
    }

    Ok(CexReferenceEvent::Trade {
        asset: fields.asset,
        exchange: Exchange::Binance,
        aggregate_trade_id: fields.aggregate_trade_id,
        price: fields.price,
        qty: fields.qty,
        is_buyer_maker: fields.is_buyer_maker,
        timestamp_ms,
    })
}

pub fn reference_trade_identity(fact: &CexReferenceEvent) -> Result<(i64, u64), DataSourceError> {
    let CexReferenceEvent::Trade {
        aggregate_trade_id, ..
    } = fact;
    Ok((
        i64::try_from(*aggregate_trade_id).map_err(|_| DataSourceError::ReplayGap {
            message: "Binance aggregate trade ID exceeds frame range".into(),
        })?,
        *aggregate_trade_id,
    ))
}

fn json_field<'a>(
    object: &'a Map<String, Value>,
    field: &'static str,
) -> Result<&'a Value, BinanceAggTradeParseError> {
    object
        .get(field)
        .ok_or(BinanceAggTradeParseError::MissingField(field))
}

fn vision_column<'a>(
    columns: &mut std::str::Split<'a, char>,
) -> Result<&'a str, BinanceAggTradeParseError> {
    columns
        .next()
        .ok_or(BinanceAggTradeParseError::InvalidVisionRow)
}

#[cfg(test)]
mod tests {
    use super::{
        BINANCE_REFERENCE_SOURCE_ID, BinanceAggTradeParseError, binance_history_source,
        parse_binance_agg_trade_live, parse_binance_vision_agg_trade_row, reference_trade_identity,
    };
    use crate::DataSourceError;
    use pmkit_event::{CexReferenceEnvelope, CexReferenceEvent, StreamMetadata};
    use pmkit_market::{Asset, Exchange};

    #[test]
    fn binance_agg_trade_matches_vision_row() -> Result<(), Box<dyn std::error::Error>> {
        let live =
            parse_binance_agg_trade_live(include_str!("../fixtures/agg_trade.json"), Asset::Btc)?;
        let history = parse_binance_vision_agg_trade_row(
            include_str!("../fixtures/agg_trades.csv").trim(),
            Asset::Btc,
        )?;

        assert_eq!(live, history);
        let live_envelope = CexReferenceEnvelope {
            metadata: StreamMetadata {
                schema_version: 1,
                source_id: BINANCE_REFERENCE_SOURCE_ID.into(),
                source_time_ms: 1_710_000_000_123,
                canonical_source_rank: 0,
                receipt_time_ms: 1_710_000_000_200,
                connection_id: "live-1".into(),
                connection_epoch: 0,
                frame_sequence: 12345,
                ingest_sequence: 12345,
            },
            fact: live,
        };
        let history_envelope = CexReferenceEnvelope {
            metadata: StreamMetadata {
                schema_version: 1,
                source_id: BINANCE_REFERENCE_SOURCE_ID.into(),
                source_time_ms: 1_710_000_000_123,
                canonical_source_rank: 0,
                receipt_time_ms: 1_710_000_001_000,
                connection_id: "archive-1".into(),
                connection_epoch: 0,
                frame_sequence: 12345,
                ingest_sequence: 12345,
            },
            fact: history,
        };

        assert_eq!(live_envelope.fact, history_envelope.fact);
        assert_ne!(
            live_envelope.metadata.receipt_time_ms,
            history_envelope.metadata.receipt_time_ms
        );
        Ok(())
    }

    #[test]
    fn bybit_is_history_unavailable() {
        assert!(matches!(
            binance_history_source(Exchange::Bybit),
            Err(DataSourceError::HistoryUnavailable {
                exchange: Exchange::Bybit
            })
        ));
    }

    #[test]
    fn malformed_aggregate_id_or_timestamp_is_rejected() {
        assert!(matches!(
            parse_binance_agg_trade_live(
                r#"{"e":"aggTrade","a":"not-an-id","p":"1","q":"1","T":1,"m":false}"#,
                Asset::Btc
            ),
            Err(BinanceAggTradeParseError::InvalidAggregateTradeId)
        ));
        assert!(matches!(
            parse_binance_agg_trade_live(
                r#"{"e":"aggTrade","a":1,"p":"1","q":"1","T":"not-a-timestamp","m":false}"#,
                Asset::Btc
            ),
            Err(BinanceAggTradeParseError::InvalidTimestamp)
        ));
    }

    #[test]
    #[allow(clippy::panic)]
    fn post_2025_vision_microsecond_timestamp_matches_live_millis()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given: a live event with a 2025 millisecond timestamp and a Vision row
        // whose official archive encodes the same instant in microseconds.
        let live = parse_binance_agg_trade_live(
            r#"{"e":"aggTrade","a":7,"p":"0.42","q":"1","T":1735689600123,"m":false}"#,
            Asset::Btc,
        )?;
        let history =
            parse_binance_vision_agg_trade_row("7,0.42,1,5,6,1735689600123000,false", Asset::Btc)?;

        // Then: both sources produce the same normalized timestamp.
        assert_eq!(live, history);
        let timestamp_ms = match history {
            CexReferenceEvent::Trade { timestamp_ms, .. } => timestamp_ms,
        };
        assert_eq!(timestamp_ms, 1_735_689_600_123);
        Ok(())
    }

    #[test]
    fn aggregate_trade_identity_is_shared_by_live_and_history()
    -> Result<(), Box<dyn std::error::Error>> {
        let fact = parse_binance_agg_trade_live(
            r#"{"e":"aggTrade","a":7,"p":"0.42","q":"1","T":1735689600123,"m":false}"#,
            Asset::Btc,
        )?;

        assert!(matches!(reference_trade_identity(&fact), Ok((7, 7))));
        assert_eq!(BINANCE_REFERENCE_SOURCE_ID, "binance:aggTrade");
        Ok(())
    }
}
