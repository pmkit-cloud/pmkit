//! Polymarket RTDS Chainlink TWAP source.
//!
//! The official Polymarket SDK owns the RTDS connection, heartbeat,
//! reconnection, and subscription lifecycle. This module only validates the
//! `crypto_prices_twap_sixty` payload and adapts it to `PMKit`'s causal envelope.

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures::StreamExt as _;
use pmkit_data::{DataSourceError, LiveCexDataSource, SourceSignal};
use pmkit_event::{
    PolymarketReferenceEnvelope, PolymarketTwapEvent, SourceEnvelope, StreamMetadata,
};
use pmkit_market::Asset;
use polymarket_client_sdk_v2::rtds::{Client, RtdsMessage, Subscription};
use polymarket_client_sdk_v2::ws::connection::ConnectionState;
use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio::sync::mpsc::Sender;

/// The default credential-free Polymarket RTDS endpoint.
pub const POLYMARKET_RTDS_ENDPOINT: &str = "wss://ws-live-data.polymarket.com";
/// The RTDS topic carrying Chainlink 60-second TWAP updates.
pub const POLYMARKET_RTDS_TOPIC: &str = "crypto_prices_twap_sixty";
/// The stable `PMKit` source identity for the RTDS TWAP stream.
pub const POLYMARKET_RTDS_SOURCE_ID: &str = "polymarket:rtds:crypto_prices_twap_sixty";

/// A malformed Polymarket RTDS TWAP message.
#[derive(Debug, Error)]
pub enum PolymarketRtdsParseError {
    /// The message was not valid JSON.
    #[error("invalid Polymarket RTDS JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    /// A required field was absent.
    #[error("missing Polymarket RTDS field: {0}")]
    MissingField(&'static str),
    /// The top-level or payload value was not an object.
    #[error("invalid Polymarket RTDS message shape")]
    InvalidShape,
    /// The message was for another RTDS topic.
    #[error("unexpected Polymarket RTDS topic")]
    WrongTopic,
    /// The message was not an update.
    #[error("unexpected Polymarket RTDS message type")]
    WrongMessageType,
    /// The payload symbol did not match the subscribed asset.
    #[error("unexpected Polymarket RTDS symbol")]
    WrongSymbol,
    /// A timestamp was missing, non-integral, or negative.
    #[error("invalid Polymarket RTDS timestamp")]
    InvalidTimestamp,
    /// The payload window was not exactly 60 seconds.
    #[error("invalid Polymarket RTDS TWAP window")]
    InvalidWindow,
    /// The display value was not a finite positive JSON number.
    #[error("invalid Polymarket RTDS TWAP value")]
    InvalidValue,
    /// The full-accuracy value was not a positive signed E18 integer string.
    #[error("invalid Polymarket RTDS full-accuracy E18 value")]
    InvalidFullAccuracyValue,
}

/// Parses one Polymarket `crypto_prices_twap_sixty` update.
///
/// The payload timestamp is retained as the source observation time. The
/// outer timestamp is retained separately as the provider publication time;
/// callers must not substitute one for the other.
///
/// # Errors
///
/// Returns [`PolymarketRtdsParseError`] when the topic, symbol, window, value,
/// precision, or timestamps do not satisfy the RTDS contract.
pub fn parse_polymarket_rtds_twap(
    message: &str,
    asset: Asset,
) -> Result<PolymarketTwapEvent, PolymarketRtdsParseError> {
    let value: Value = serde_json::from_str(message)?;
    parse_polymarket_rtds_twap_value(&value, asset)
}

/// Parses a UTF-8 RTDS message without first allocating a string.
///
/// # Errors
///
/// Returns the corresponding validation error for an invalid RTDS update.
pub fn parse_polymarket_rtds_twap_bytes(
    message: &[u8],
    asset: Asset,
) -> Result<PolymarketTwapEvent, PolymarketRtdsParseError> {
    let message =
        std::str::from_utf8(message).map_err(|_| PolymarketRtdsParseError::InvalidShape)?;
    parse_polymarket_rtds_twap(message, asset)
}

fn parse_polymarket_rtds_twap_value(
    value: &Value,
    asset: Asset,
) -> Result<PolymarketTwapEvent, PolymarketRtdsParseError> {
    let object = value
        .as_object()
        .ok_or(PolymarketRtdsParseError::InvalidShape)?;
    if string_field(object, "topic")? != POLYMARKET_RTDS_TOPIC {
        return Err(PolymarketRtdsParseError::WrongTopic);
    }
    if string_field(object, "type")? != "update" {
        return Err(PolymarketRtdsParseError::WrongMessageType);
    }
    let provider_timestamp_ms = timestamp_field(object, "timestamp")?;
    let payload = object
        .get("payload")
        .and_then(Value::as_object)
        .ok_or(PolymarketRtdsParseError::InvalidShape)?;
    let symbol = string_field(payload, "symbol")?.to_owned();
    if symbol != format!("{asset}/usd") {
        return Err(PolymarketRtdsParseError::WrongSymbol);
    }
    let timestamp_ms = timestamp_field(payload, "timestamp")?;
    let window_s = payload
        .get("window_s")
        .and_then(Value::as_u64)
        .filter(|window| *window == 60)
        .ok_or(PolymarketRtdsParseError::InvalidWindow)?;
    let display_value = payload
        .get("value")
        .and_then(Value::as_number)
        .and_then(serde_json::Number::as_f64)
        .filter(|number| number.is_finite() && *number > 0.0)
        .ok_or(PolymarketRtdsParseError::InvalidValue)?;
    let full_accuracy_value = string_field(payload, "full_accuracy_value")?.to_owned();
    validate_full_accuracy_value(&full_accuracy_value)?;

    Ok(PolymarketTwapEvent {
        asset,
        symbol,
        timestamp_ms,
        provider_timestamp_ms,
        value: display_value,
        full_accuracy_value,
        window_s,
    })
}

fn string_field<'a>(
    object: &'a Map<String, Value>,
    field: &'static str,
) -> Result<&'a str, PolymarketRtdsParseError> {
    object
        .get(field)
        .ok_or(PolymarketRtdsParseError::MissingField(field))?
        .as_str()
        .ok_or(PolymarketRtdsParseError::InvalidShape)
}

fn timestamp_field(
    object: &Map<String, Value>,
    field: &'static str,
) -> Result<i64, PolymarketRtdsParseError> {
    object
        .get(field)
        .ok_or(PolymarketRtdsParseError::MissingField(field))?
        .as_i64()
        .filter(|timestamp| *timestamp >= 0)
        .ok_or(PolymarketRtdsParseError::InvalidTimestamp)
}

fn validate_full_accuracy_value(value: &str) -> Result<(), PolymarketRtdsParseError> {
    let digits = value.strip_prefix('-').unwrap_or(value);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(PolymarketRtdsParseError::InvalidFullAccuracyValue);
    }
    if !matches!(value.parse::<i128>(), Ok(scaled) if scaled > 0) {
        return Err(PolymarketRtdsParseError::InvalidFullAccuracyValue);
    }
    Ok(())
}

/// A live credential-free Polymarket RTDS 60-second TWAP source.
#[derive(Debug, Clone)]
pub struct PolymarketRtdsLive {
    asset: Asset,
    endpoint: Arc<str>,
}

impl PolymarketRtdsLive {
    /// Creates a source using the public Polymarket RTDS endpoint.
    #[must_use]
    pub fn new(asset: Asset) -> Self {
        Self::with_endpoint(asset, POLYMARKET_RTDS_ENDPOINT)
    }

    /// Creates a source with a custom endpoint for controlled proxies.
    #[must_use]
    pub fn with_endpoint(asset: Asset, endpoint: &str) -> Self {
        Self {
            asset,
            endpoint: Arc::from(endpoint.to_owned()),
        }
    }

    /// Returns the subscribed asset.
    #[must_use]
    pub const fn asset(&self) -> Asset {
        self.asset
    }

    /// Returns the configured RTDS endpoint.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Connects through the official SDK and emits typed RTDS reference
    /// envelopes until the stream ends.
    ///
    /// The SDK owns connection setup, heartbeat, reconnection, and
    /// subscription management. Its `subscribe_raw` stream contains parsed
    /// messages, so `PMKit` deliberately does not expose a lossless wire frame.
    ///
    /// # Errors
    ///
    /// Returns [`DataSourceError::Unavailable`] on SDK or stream failure,
    /// [`DataSourceError::ReplayGap`] on a malformed update, and
    /// [`DataSourceError::SinkClosed`] when the receiver is dropped.
    pub async fn subscribe_reference(
        &self,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        let client = Client::new(
            self.endpoint.as_ref(),
            polymarket_client_sdk_v2::ws::config::Config::default(),
        )
        .map_err(|error| unavailable(format!("Polymarket RTDS client failed: {error}")))?;
        let subscription = Subscription::builder()
            .topic(POLYMARKET_RTDS_TOPIC.to_owned())
            .msg_type("update".to_owned())
            .filters(format!(r#"{{"symbol":"{}/usd"}}"#, self.asset))
            .build();
        let stream = client.subscribe_raw(subscription).map_err(|error| {
            unavailable(format!("Polymarket RTDS subscription failed: {error}"))
        })?;
        let mut stream = Box::pin(stream);
        let mut connection_since = None;
        let mut connection_epoch = 0_i64;
        let mut frame_sequence = 0_i64;
        let mut ingest_sequence = 0_u64;

        loop {
            tokio::select! {
                () = sink.closed() => return Err(DataSourceError::SinkClosed),
                message = stream.next() => {
                    let Some(message) = message else {
                        return Err(unavailable("Polymarket RTDS stream ended"));
                    };
                    let message = message
                        .map_err(|error| unavailable(format!("Polymarket RTDS stream failed: {error}")))?;
                    update_connection_identity(
                        &client,
                        &mut connection_since,
                        &mut connection_epoch,
                        &mut frame_sequence,
                    )?;
                    frame_sequence = frame_sequence
                        .checked_add(1)
                        .ok_or_else(|| replay_gap("Polymarket RTDS frame sequence overflow"))?;
                    ingest_sequence = ingest_sequence
                        .checked_add(1)
                        .ok_or_else(|| replay_gap("Polymarket RTDS ingest sequence overflow"))?;
                    let fact = parse_sdk_message(&message, self.asset)
                        .map_err(|error| replay_gap(format!("invalid Polymarket RTDS update: {error}")))?;
                    sink.send(SourceSignal::Data(Box::new(SourceEnvelope::PolymarketReference(
                        PolymarketReferenceEnvelope {
                            metadata: StreamMetadata {
                                schema_version: 1,
                                source_id: POLYMARKET_RTDS_SOURCE_ID.to_owned(),
                                source_time_ms: fact.timestamp_ms,
                                canonical_source_rank: 1,
                                receipt_time_ms: now_ms(),
                                connection_id: self.endpoint.to_string(),
                                connection_epoch,
                                frame_sequence,
                                ingest_sequence,
                            },
                            fact,
                        },
                    ))))
                    .await
                    .map_err(|_| DataSourceError::SinkClosed)?;
                }
            }
        }
    }
}

#[async_trait]
impl LiveCexDataSource for PolymarketRtdsLive {
    async fn subscribe_reference(&self, sink: Sender<SourceSignal>) -> Result<(), DataSourceError> {
        Self::subscribe_reference(self, sink).await
    }
}

fn parse_sdk_message(
    message: &RtdsMessage,
    asset: Asset,
) -> Result<PolymarketTwapEvent, PolymarketRtdsParseError> {
    let value = json!({
        "topic": &message.topic,
        "type": &message.msg_type,
        "timestamp": message.timestamp,
        "payload": &message.payload,
    });
    parse_polymarket_rtds_twap_value(&value, asset)
}

fn update_connection_identity(
    client: &Client,
    connection_since: &mut Option<std::time::Instant>,
    connection_epoch: &mut i64,
    frame_sequence: &mut i64,
) -> Result<(), DataSourceError> {
    let ConnectionState::Connected { since } = client.connection_state() else {
        return Ok(());
    };
    match connection_since {
        None => *connection_since = Some(since),
        Some(previous) if *previous != since => {
            *connection_epoch = connection_epoch
                .checked_add(1)
                .ok_or_else(|| replay_gap("Polymarket RTDS connection epoch overflow"))?;
            *connection_since = Some(since);
            *frame_sequence = 0;
        }
        Some(_) => {}
    }
    Ok(())
}

fn unavailable(message: impl Into<String>) -> DataSourceError {
    DataSourceError::Unavailable {
        message: message.into(),
    }
}

fn replay_gap(message: impl Into<String>) -> DataSourceError {
    DataSourceError::ReplayGap {
        message: message.into(),
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{POLYMARKET_RTDS_TOPIC, PolymarketRtdsParseError, parse_polymarket_rtds_twap};
    use pmkit_market::Asset;

    const MESSAGE: &str = r#"{"topic":"crypto_prices_twap_sixty","payload":{"symbol":"btc/usd","timestamp":1785178800000,"value":65000.5,"full_accuracy_value":"65000500000000000000000","window_s":60},"timestamp":1785178800123,"type":"update"}"#;

    #[test]
    fn parser_preserves_observation_publication_and_exact_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let event = parse_polymarket_rtds_twap(MESSAGE, Asset::Btc)?;
        assert_eq!(event.asset, Asset::Btc);
        assert_eq!(event.symbol, "btc/usd");
        assert_eq!(event.timestamp_ms, 1_785_178_800_000);
        assert_eq!(event.provider_timestamp_ms, 1_785_178_800_123);
        assert!((event.value - 65_000.5).abs() < f64::EPSILON);
        assert_eq!(event.full_accuracy_value, "65000500000000000000000");
        assert_eq!(event.window_s, 60);
        Ok(())
    }

    #[test]
    fn parser_rejects_wrong_scope_and_invalid_values() {
        let wrong_topic = MESSAGE.replace(POLYMARKET_RTDS_TOPIC, "crypto_prices_twap_thirty");
        assert!(matches!(
            parse_polymarket_rtds_twap(&wrong_topic, Asset::Btc),
            Err(PolymarketRtdsParseError::WrongTopic)
        ));
        let wrong_symbol = MESSAGE.replace("btc/usd", "eth/usd");
        assert!(matches!(
            parse_polymarket_rtds_twap(&wrong_symbol, Asset::Btc),
            Err(PolymarketRtdsParseError::WrongSymbol)
        ));
        let wrong_window = MESSAGE.replace("\"window_s\":60", "\"window_s\":30");
        assert!(matches!(
            parse_polymarket_rtds_twap(&wrong_window, Asset::Btc),
            Err(PolymarketRtdsParseError::InvalidWindow)
        ));
        let invalid_value = MESSAGE.replace("\"value\":65000.5", "\"value\":0");
        assert!(matches!(
            parse_polymarket_rtds_twap(&invalid_value, Asset::Btc),
            Err(PolymarketRtdsParseError::InvalidValue)
        ));
        let invalid_precision = MESSAGE.replace(
            "\"full_accuracy_value\":\"65000500000000000000000\"",
            "\"full_accuracy_value\":\"65000.5\"",
        );
        assert!(matches!(
            parse_polymarket_rtds_twap(&invalid_precision, Asset::Btc),
            Err(PolymarketRtdsParseError::InvalidFullAccuracyValue)
        ));
    }
}
