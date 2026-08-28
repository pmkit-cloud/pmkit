//! Polymarket RTDS Chainlink TWAP source.
//!
//! This module owns the RTDS wire protocol and adapts the credential-free
//! `crypto_prices_twap_sixty` stream into `PMKit`'s typed reference envelope.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use pmkit_data::{DataSourceError, LiveCexDataSource, SourceSignal};
use pmkit_event::{
    PolymarketReferenceEnvelope, PolymarketTwapEvent, SourceEnvelope, StreamMetadata,
};
use pmkit_market::Asset;
use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::{connect_async, tungstenite::Message};

/// The default credential-free Polymarket RTDS endpoint.
pub const POLYMARKET_RTDS_ENDPOINT: &str = "wss://ws-live-data.polymarket.com";
/// The RTDS topic carrying Chainlink 60-second TWAP updates.
pub const POLYMARKET_RTDS_TOPIC: &str = "crypto_prices_twap_sixty";
/// The stable `PMKit` source identity for the RTDS TWAP stream.
pub const POLYMARKET_RTDS_SOURCE_ID: &str = "polymarket:rtds:crypto_prices_twap_sixty";
/// The application heartbeat required by Polymarket RTDS.
pub const POLYMARKET_RTDS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// A malformed Polymarket RTDS TWAP frame.
#[derive(Debug, Error)]
pub enum PolymarketRtdsParseError {
    /// The frame was not valid JSON.
    #[error("invalid Polymarket RTDS JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    /// A required field was absent.
    #[error("missing Polymarket RTDS field: {0}")]
    MissingField(&'static str),
    /// The top-level or payload value was not an object.
    #[error("invalid Polymarket RTDS frame shape")]
    InvalidShape,
    /// The frame was for another RTDS topic.
    #[error("unexpected Polymarket RTDS topic")]
    WrongTopic,
    /// The frame was not an update.
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
    raw: &str,
    asset: Asset,
) -> Result<PolymarketTwapEvent, PolymarketRtdsParseError> {
    let value: Value = serde_json::from_str(raw)?;
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

/// Parses a raw UTF-8 RTDS frame without first allocating a string.
///
/// # Errors
///
/// Returns [`PolymarketRtdsParseError::InvalidJson`] for invalid UTF-8 or JSON,
/// or the corresponding validation error for an invalid RTDS update.
pub fn parse_polymarket_rtds_twap_bytes(
    raw: &[u8],
    asset: Asset,
) -> Result<PolymarketTwapEvent, PolymarketRtdsParseError> {
    let raw = std::str::from_utf8(raw).map_err(|_| PolymarketRtdsParseError::InvalidShape)?;
    parse_polymarket_rtds_twap(raw, asset)
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

/// Builds the exact one-symbol subscription sent to RTDS.
#[must_use]
pub fn polymarket_rtds_subscription(asset: Asset) -> String {
    json!({
        "action": "subscribe",
        "subscriptions": [{
            "topic": POLYMARKET_RTDS_TOPIC,
            "type": "update",
            "filters": format!(r#"{{"symbol":"{asset}/usd"}}"#),
        }],
    })
    .to_string()
}

/// A live credential-free Polymarket RTDS 60-second TWAP source.
#[derive(Debug, Clone)]
pub struct PolymarketRtdsLive {
    asset: Asset,
    endpoint: Arc<str>,
    heartbeat_interval: Duration,
}

impl PolymarketRtdsLive {
    /// Creates a source using the public Polymarket RTDS endpoint.
    #[must_use]
    pub fn new(asset: Asset) -> Self {
        Self::with_endpoint(asset, POLYMARKET_RTDS_ENDPOINT)
    }

    /// Creates a source with a custom endpoint for deterministic tests or proxies.
    #[must_use]
    pub fn with_endpoint(asset: Asset, endpoint: &str) -> Self {
        Self {
            asset,
            endpoint: Arc::from(endpoint.to_owned()),
            heartbeat_interval: POLYMARKET_RTDS_HEARTBEAT_INTERVAL,
        }
    }

    /// Overrides the application heartbeat interval.
    #[must_use]
    pub const fn with_heartbeat_interval(mut self, heartbeat_interval: Duration) -> Self {
        self.heartbeat_interval = heartbeat_interval;
        self
    }

    /// Returns the subscribed asset.
    #[must_use]
    pub const fn asset(&self) -> Asset {
        self.asset
    }

    /// Returns the configured WebSocket endpoint.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Connects and emits typed RTDS reference envelopes until the stream ends.
    ///
    /// # Errors
    ///
    /// Returns [`DataSourceError::Unavailable`] on transport failure or stream
    /// termination, [`DataSourceError::ReplayGap`] on a malformed update, and
    /// [`DataSourceError::SinkClosed`] when the receiver is dropped.
    pub async fn subscribe_reference(
        &self,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        let (socket, _) = connect_async(self.endpoint.as_ref())
            .await
            .map_err(|error| unavailable(format!("Polymarket RTDS connection failed: {error}")))?;
        let (mut writer, mut reader) = socket.split();
        writer
            .send(Message::Text(
                polymarket_rtds_subscription(self.asset).into(),
            ))
            .await
            .map_err(|error| {
                unavailable(format!("Polymarket RTDS subscription failed: {error}"))
            })?;

        let heartbeat_interval = if self.heartbeat_interval.is_zero() {
            Duration::from_millis(1)
        } else {
            self.heartbeat_interval
        };
        let mut heartbeat = tokio::time::interval(heartbeat_interval);
        heartbeat.tick().await;
        let mut sequence = 0_u64;

        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    writer
                        .send(Message::Text("PING".into()))
                        .await
                        .map_err(|error| unavailable(format!("Polymarket RTDS heartbeat failed: {error}")))?;
                }
                message = reader.next() => {
                    let Some(message) = message else {
                        return Err(unavailable("Polymarket RTDS stream ended"));
                    };
                    let message = message
                        .map_err(|error| unavailable(format!("Polymarket RTDS stream failed: {error}")))?;
                    let Message::Text(text) = message else {
                        continue;
                    };
                    let text = text.to_string();
                    match text.as_str() {
                        "PONG" => continue,
                        "PING" => {
                            writer
                                .send(Message::Text("PONG".into()))
                                .await
                                .map_err(|error| unavailable(format!("Polymarket RTDS pong failed: {error}")))?;
                            continue;
                        }
                        _ => {}
                    }
                    let receipt_time_ms = now_ms();
                    sequence = sequence
                        .checked_add(1)
                        .ok_or_else(|| replay_gap("Polymarket RTDS frame sequence overflow"))?;
                    let frame_sequence = i64::try_from(sequence)
                        .map_err(|_| replay_gap("Polymarket RTDS frame sequence exceeds signed range"))?;
                    let fact = parse_polymarket_rtds_twap(&text, self.asset)
                        .map_err(|error| replay_gap(format!("invalid Polymarket RTDS update: {error}")))?;
                    sink.send(SourceSignal::Data(Box::new(SourceEnvelope::PolymarketReference(
                        PolymarketReferenceEnvelope {
                            metadata: StreamMetadata {
                                schema_version: 1,
                                source_id: POLYMARKET_RTDS_SOURCE_ID.to_owned(),
                                source_time_ms: fact.timestamp_ms,
                                canonical_source_rank: 1,
                                receipt_time_ms,
                                connection_id: self.endpoint.to_string(),
                                connection_epoch: 0,
                                frame_sequence,
                                ingest_sequence: sequence,
                            },
                            raw_frame: text.into_bytes(),
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
    use super::{
        POLYMARKET_RTDS_SOURCE_ID, POLYMARKET_RTDS_TOPIC, PolymarketRtdsLive,
        PolymarketRtdsParseError, parse_polymarket_rtds_twap, polymarket_rtds_subscription,
    };
    use futures::{SinkExt, StreamExt};
    use pmkit_data::SourceSignal;
    use pmkit_event::{PolymarketTwapEvent, SourceEnvelope, StrategyFact};
    use pmkit_market::Asset;
    use serde_json::Value;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    const FRAME: &str = r#"{"topic":"crypto_prices_twap_sixty","payload":{"symbol":"btc/usd","timestamp":1785178800000,"value":65000.5,"full_accuracy_value":"65000500000000000000000","window_s":60},"timestamp":1785178800123,"type":"update"}"#;

    #[test]
    fn parser_preserves_observation_publication_and_exact_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let event = parse_polymarket_rtds_twap(FRAME, Asset::Btc)?;
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
    fn subscription_uses_exact_rtds_wire_shape() -> Result<(), Box<dyn std::error::Error>> {
        let value: Value = serde_json::from_str(&polymarket_rtds_subscription(Asset::Btc))?;
        assert_eq!(value["action"], "subscribe");
        assert_eq!(value["subscriptions"][0]["topic"], POLYMARKET_RTDS_TOPIC);
        assert_eq!(value["subscriptions"][0]["type"], "update");
        assert_eq!(
            value["subscriptions"][0]["filters"],
            r#"{"symbol":"btc/usd"}"#
        );
        Ok(())
    }

    #[test]
    fn parser_rejects_wrong_scope_and_invalid_values() {
        let wrong_topic = FRAME.replace(POLYMARKET_RTDS_TOPIC, "crypto_prices_twap_thirty");
        assert!(matches!(
            parse_polymarket_rtds_twap(&wrong_topic, Asset::Btc),
            Err(PolymarketRtdsParseError::WrongTopic)
        ));
        let wrong_symbol = FRAME.replace("btc/usd", "eth/usd");
        assert!(matches!(
            parse_polymarket_rtds_twap(&wrong_symbol, Asset::Btc),
            Err(PolymarketRtdsParseError::WrongSymbol)
        ));
        let wrong_window = FRAME.replace("\"window_s\":60", "\"window_s\":30");
        assert!(matches!(
            parse_polymarket_rtds_twap(&wrong_window, Asset::Btc),
            Err(PolymarketRtdsParseError::InvalidWindow)
        ));
        let invalid_value = FRAME.replace("\"value\":65000.5", "\"value\":0");
        assert!(matches!(
            parse_polymarket_rtds_twap(&invalid_value, Asset::Btc),
            Err(PolymarketRtdsParseError::InvalidValue)
        ));
        let invalid_precision = FRAME.replace(
            "\"full_accuracy_value\":\"65000500000000000000000\"",
            "\"full_accuracy_value\":\"65000.5\"",
        );
        assert!(matches!(
            parse_polymarket_rtds_twap(&invalid_precision, Asset::Btc),
            Err(PolymarketRtdsParseError::InvalidFullAccuracyValue)
        ));
    }

    #[tokio::test]
    async fn source_subscribes_preserves_metadata_and_sends_text_heartbeat()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            let Some(Ok(Message::Text(subscription))) = socket.next().await else {
                return Err("expected RTDS subscription".into());
            };
            let subscription: Value = serde_json::from_str(&subscription)?;
            assert_eq!(
                subscription["subscriptions"][0]["topic"],
                POLYMARKET_RTDS_TOPIC
            );
            assert_eq!(
                subscription["subscriptions"][0]["filters"],
                r#"{"symbol":"btc/usd"}"#
            );
            socket.send(Message::Text(FRAME.into())).await?;
            loop {
                let Some(message) = socket.next().await else {
                    return Err("expected heartbeat".into());
                };
                match message? {
                    Message::Text(text) if text == "PING" => {
                        socket.send(Message::Text("PONG".into())).await?;
                        break;
                    }
                    _ => {}
                }
            }
            socket.close(None).await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });

        let source = PolymarketRtdsLive::with_endpoint(Asset::Btc, &format!("ws://{address}"))
            .with_heartbeat_interval(std::time::Duration::from_millis(5));
        let (sink, mut events) = mpsc::channel(1);
        let result = source.subscribe_reference(sink).await;
        assert!(result.is_err());
        let Some(SourceSignal::Data(envelope)) = events.recv().await else {
            return Err("expected one RTDS event".into());
        };
        let SourceEnvelope::PolymarketReference(envelope) = *envelope else {
            return Err("expected Polymarket reference envelope".into());
        };
        assert_eq!(envelope.metadata.source_id, POLYMARKET_RTDS_SOURCE_ID);
        assert_eq!(envelope.metadata.source_time_ms, 1_785_178_800_000);
        assert!(envelope.metadata.receipt_time_ms > 0);
        assert_eq!(envelope.metadata.frame_sequence, 1);
        assert_eq!(envelope.metadata.ingest_sequence, 1);
        assert_eq!(envelope.raw_frame, FRAME.as_bytes());
        assert!(matches!(
            envelope.clone().fact,
            PolymarketTwapEvent {
                asset: Asset::Btc,
                ..
            }
        ));
        assert!(matches!(
            SourceEnvelope::PolymarketReference(envelope).into_strategy_fact(),
            StrategyFact::PolymarketReference(_)
        ));
        server.await??;
        Ok(())
    }
}
