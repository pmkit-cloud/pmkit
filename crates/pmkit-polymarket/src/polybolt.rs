//! Authenticated `PolyBolt` transport and exact decoder for Polymarket's BTC/USD 60-second TWAP.
//!
//! This path is intentionally separate from the legacy RTDS client. `PolyBolt` frames remain
//! available alongside decoded facts so callers can persist the exact input bytes and receipt time.

use std::{
    collections::HashMap,
    fmt,
    hash::BuildHasher,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use futures::{SinkExt as _, StreamExt as _};
use polymarket_client_sdk_v2::auth::{Credentials, ExposeSecret as _};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, error::Error as WebSocketError},
};

/// The `PolyBolt` WebSocket endpoint.
pub const POLYBOLT_ENDPOINT: &str = "wss://ws-live-v2.polymarket.com/ws";
/// Stable source identity for the 60-second `PolyBolt` TWAP feed.
pub const POLYBOLT_SOURCE_ID: &str = "polymarket:polybolt:price.crypto.twap:60";
/// `PolyBolt`'s crypto TWAP channel.
pub const POLYBOLT_TWAP_CHANNEL: &str = "price.crypto.twap";

const AUTH_RID: &str = "a1";
const AUTH_TIMEOUT: Duration = Duration::from_secs(10);
const SUBSCRIBE_FRAME: &str = r#"{"op":"subscribe","subscriptions":[{"channel":"price.crypto.twap","filter":{"symbol":"btcusd","window_seconds":60}}]}"#;
const MAX_DRAIN_DELAY: Duration = Duration::from_secs(10);
const MAX_ABNORMAL_DELAY: Duration = Duration::from_secs(30);

/// `PolyBolt` data-source or protocol failure.
#[derive(Debug, Error)]
pub enum PolyBoltError {
    /// A received frame was not a supported `PolyBolt` v1 TWAP frame.
    #[error("invalid PolyBolt frame")]
    InvalidFrame,
    /// The server sent a frame version this client does not understand.
    #[error("unsupported PolyBolt protocol version")]
    UnsupportedVersion,
    /// The server sent a different channel than the subscribed TWAP channel.
    #[error("unexpected PolyBolt channel")]
    UnexpectedChannel,
    /// The exact full-accuracy price cannot be represented by `Decimal`.
    #[error("PolyBolt full-accuracy price is not representable as Decimal")]
    UnrepresentableValue,
    /// Authentication was rejected because credentials are invalid.
    #[error("PolyBolt authentication failed (auth_invalid)")]
    AuthenticationInvalid,
    /// The server requires authentication for the requested channel.
    #[error("PolyBolt authentication is required (auth_required)")]
    AuthenticationRequired,
    /// The authentication service is temporarily unavailable.
    #[error("PolyBolt authentication is unavailable (auth_unavailable)")]
    AuthenticationUnavailable,
    /// Authentication did not complete before the handshake timeout.
    #[error("PolyBolt authentication timed out")]
    AuthenticationTimeout,
    /// The authentication handshake returned an unexpected response.
    #[error("unexpected PolyBolt authentication response")]
    AuthenticationProtocol,
    /// The server closed the connection because authentication failed.
    #[error("PolyBolt closed the connection for authentication (4001)")]
    AuthenticationClose,
    /// The server closed the connection because this consumer was too slow.
    #[error("PolyBolt closed the connection for a slow consumer (4002)")]
    SlowConsumer,
    /// The server is draining and its one permitted reconnect was exhausted.
    #[error("PolyBolt draining reconnect was exhausted (4003)")]
    DrainingReconnectExhausted,
    /// The server reported a `PolyBolt` policy/configuration bug.
    #[error("PolyBolt server policy error (4008)")]
    PolicyBug,
    /// The server closed the connection with an unclassified close code.
    #[error("PolyBolt connection closed (code {code})")]
    Closed {
        /// WebSocket close code.
        code: u16,
    },
    /// The server disconnected without a close frame (WebSocket code 1006).
    #[error("PolyBolt connection ended without a close frame (1006)")]
    AbnormalClose,
    /// The HTTP WebSocket handshake was rejected and cannot be retried.
    #[error("PolyBolt WebSocket handshake rejected (HTTP {status})")]
    HandshakeRejected {
        /// HTTP response status.
        status: u16,
    },
    /// Repeated temporary handshake failures could not be recovered.
    #[error("PolyBolt temporary WebSocket handshake failure (HTTP {status})")]
    HandshakeUnavailable {
        /// HTTP response status.
        status: u16,
        /// Server-provided retry delay, if any.
        retry_after: Option<Duration>,
    },
    /// A local WebSocket I/O operation failed.
    #[error("PolyBolt WebSocket transport failed")]
    Transport,
    /// A connection epoch overflowed its public metadata range.
    #[error("PolyBolt connection epoch overflow")]
    ConnectionEpochOverflow,
    /// The per-connection transport frame counter overflowed.
    #[error("PolyBolt transport frame sequence overflow")]
    TransportFrameSequenceOverflow,
}

/// Exact TWAP price from a snapshot or live update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolyBoltTwapPrice {
    /// Lowercase USD pair (for this source, `btcusd`).
    pub symbol: String,
    /// Oracle source (for this source, `chainlink`).
    pub source: String,
    /// TWAP window in seconds (for this source, `60`).
    pub window_seconds: u64,
    /// Oracle point timestamp in Unix milliseconds.
    pub timestamp_ms: i64,
    /// Exact decimal parsed only from `full_accuracy_value`.
    pub exact_value: Decimal,
}

/// Exact bytes of a received `PolyBolt` data message or native control payload with local receipt time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolyBoltRawFrame {
    /// Local receipt time in Unix milliseconds, captured before decoding.
    pub receipt_time_ms: i64,
    /// Exact WebSocket text/binary message bytes or native control payload bytes.
    pub bytes: Vec<u8>,
}

/// A sequenced `PolyBolt` discontinuity, retained with the raw frame that exposed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolyBoltGap {
    /// Next expected sequence, or `None` if the prior sequence was `u64::MAX`.
    pub expected_sequence: Option<u64>,
    /// Sequence number on the frame reporting the gap.
    pub observed_sequence: u64,
    /// Frames reported dropped since the previous delivery.
    pub dropped: u64,
}

impl PolyBoltGap {
    /// Returns an exact missing sequence interval when sequence numbers identify one.
    #[must_use]
    pub const fn missing_sequence_range(&self) -> Option<(u64, u64)> {
        match self.expected_sequence {
            Some(expected) if self.observed_sequence > expected => {
                Some((expected, self.observed_sequence - 1))
            }
            _ => None,
        }
    }
}

/// The decoded meaning of one raw incoming data frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolyBoltEvent {
    /// A snapshot explicitly contained no historical points.
    EmptySnapshot {
        /// Snapshot symbol.
        symbol: String,
        /// Snapshot source.
        source: String,
        /// Snapshot TWAP window.
        window_seconds: u64,
    },
    /// A non-empty snapshot; each point inherits the payload metadata.
    Snapshot {
        /// Snapshot points in the server-provided order.
        prices: Vec<PolyBoltTwapPrice>,
    },
    /// One live TWAP update.
    Update {
        /// Live price point.
        price: PolyBoltTwapPrice,
    },
    /// A valid frame for another symbol on the shared channel sequence.
    OtherSymbol {
        /// Symbol carried by the frame.
        symbol: String,
    },
    /// A valid frame for a channel other than this source's subscription.
    OtherChannel {
        /// Channel carried by the frame.
        channel: String,
    },
    /// A sequence jump or server-reported dropped frame; do not project its price.
    Gap(PolyBoltGap),
}

/// A validated frame together with the exact raw input retained for replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolyBoltRecord {
    /// Raw incoming bytes and local receipt timestamp.
    pub raw: PolyBoltRawFrame,
    /// Connection epoch; sequence expectations reset when this value changes.
    pub connection_epoch: u64,
    /// Channel-level sequence number.
    pub sequence: u64,
    /// Top-level server timestamp in Unix milliseconds.
    pub server_timestamp_ms: i64,
    /// Decoded event, including explicit gap and empty-snapshot variants.
    pub event: PolyBoltEvent,
}

/// Kind of native WebSocket control frame retained as liveness evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolyBoltControlKind {
    /// A WebSocket ping, answered by the client before delivery.
    Ping,
    /// A WebSocket pong received from the server.
    Pong,
}

/// Exact native control payload with its receipt and local transport position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolyBoltControl {
    /// Exact control payload bytes and local receipt timestamp.
    pub raw: PolyBoltRawFrame,
    /// Connection that delivered the control frame.
    pub connection_epoch: u64,
    /// Per-connection transport frame number (not a `PolyBolt` channel sequence).
    pub frame_sequence: u64,
    /// Native WebSocket control type.
    pub kind: PolyBoltControlKind,
}

/// A `PolyBolt` data record or native WebSocket control frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolyBoltItem {
    /// Decoded data record with its local transport frame number.
    Record {
        /// Validated record; `record.sequence` remains the server channel sequence.
        record: PolyBoltRecord,
        /// Per-connection transport frame number, independent of channel sequence.
        frame_sequence: u64,
    },
    /// Native ping/pong retained as exact liveness evidence.
    Control(PolyBoltControl),
}

/// Stateless-schema decoder with per-channel, per-connection sequence tracking.
#[derive(Debug, Default)]
pub struct PolyBoltDecoder {
    next_sequence: HashMap<String, Option<u64>>,
}

impl PolyBoltDecoder {
    /// Starts a fresh connection sequence domain.
    pub fn reset(&mut self) {
        self.next_sequence.clear();
    }

    /// Validates and decodes one exact incoming data frame.
    ///
    /// Sequence continuity is checked before symbol/channel filtering. A discontinuity is returned
    /// as a record, preserving the raw frame, and its price is deliberately not projected.
    ///
    /// # Errors
    ///
    /// Returns [`PolyBoltError`] for malformed frames, unsupported versions, or inexact values.
    pub fn decode(
        &mut self,
        raw: PolyBoltRawFrame,
        connection_epoch: u64,
    ) -> Result<PolyBoltRecord, PolyBoltError> {
        let wire: WireFrame =
            serde_json::from_slice(&raw.bytes).map_err(|_| PolyBoltError::InvalidFrame)?;
        if wire.version != 1 {
            return Err(PolyBoltError::UnsupportedVersion);
        }

        let expected = self.next_sequence.get(&wire.channel).copied();
        self.next_sequence
            .insert(wire.channel.clone(), wire.sequence.checked_add(1));
        let gap = (expected.flatten().is_some_and(|next| next != wire.sequence)
            || wire.dropped.unwrap_or_default() > 0)
            .then_some(PolyBoltGap {
                expected_sequence: expected.flatten(),
                observed_sequence: wire.sequence,
                dropped: wire.dropped.unwrap_or_default(),
            });

        let event = if let Some(gap) = gap {
            PolyBoltEvent::Gap(gap)
        } else if wire.channel != POLYBOLT_TWAP_CHANNEL {
            PolyBoltEvent::OtherChannel {
                channel: wire.channel,
            }
        } else {
            decode_payload(wire.payload, wire.snapshot)?
        };

        Ok(PolyBoltRecord {
            raw,
            connection_epoch,
            sequence: wire.sequence,
            server_timestamp_ms: wire.timestamp_ms,
            event,
        })
    }
}

#[derive(Debug, Deserialize)]
struct WireFrame {
    #[serde(rename = "v")]
    version: u8,
    channel: String,
    #[serde(rename = "seq")]
    sequence: u64,
    #[serde(rename = "ts")]
    timestamp_ms: i64,
    #[serde(default)]
    snapshot: Option<bool>,
    #[serde(default)]
    dropped: Option<u64>,
    payload: Value,
}

#[derive(Debug, Deserialize)]
struct WirePayload {
    symbol: String,
    source: String,
    window_seconds: u64,
    #[serde(default)]
    data: Option<Vec<WirePrice>>,
    #[serde(default)]
    timestamp: Option<i64>,
    #[serde(default)]
    full_accuracy_value: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WirePrice {
    timestamp: i64,
    full_accuracy_value: String,
}

fn decode_payload(
    payload: Value,
    snapshot_flag: Option<bool>,
) -> Result<PolyBoltEvent, PolyBoltError> {
    let payload: WirePayload =
        serde_json::from_value(payload).map_err(|_| PolyBoltError::InvalidFrame)?;
    let is_snapshot = payload.data.is_some();
    if snapshot_flag == Some(true) && !is_snapshot || snapshot_flag == Some(false) && is_snapshot {
        return Err(PolyBoltError::InvalidFrame);
    }
    if payload.symbol != "btcusd" {
        return Ok(PolyBoltEvent::OtherSymbol {
            symbol: payload.symbol,
        });
    }
    if payload.source != "chainlink" || payload.window_seconds != 60 {
        return Err(PolyBoltError::InvalidFrame);
    }

    if let Some(points) = payload.data {
        if points.is_empty() {
            return Ok(PolyBoltEvent::EmptySnapshot {
                symbol: payload.symbol,
                source: payload.source,
                window_seconds: payload.window_seconds,
            });
        }
        let prices = points
            .iter()
            .map(|point| {
                exact_price(
                    &payload.symbol,
                    &payload.source,
                    payload.window_seconds,
                    point,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(PolyBoltEvent::Snapshot { prices });
    }

    let timestamp = payload.timestamp.ok_or(PolyBoltError::InvalidFrame)?;
    let exact = payload
        .full_accuracy_value
        .ok_or(PolyBoltError::InvalidFrame)?;
    Ok(PolyBoltEvent::Update {
        price: parse_price(
            payload.symbol,
            payload.source,
            payload.window_seconds,
            timestamp,
            &exact,
        )?,
    })
}

fn exact_price(
    symbol: &str,
    source: &str,
    window_seconds: u64,
    point: &WirePrice,
) -> Result<PolyBoltTwapPrice, PolyBoltError> {
    parse_price(
        symbol.to_owned(),
        source.to_owned(),
        window_seconds,
        point.timestamp,
        &point.full_accuracy_value,
    )
}

fn parse_price(
    symbol: String,
    source: String,
    window_seconds: u64,
    timestamp_ms: i64,
    full_accuracy_value: &str,
) -> Result<PolyBoltTwapPrice, PolyBoltError> {
    let exact_value = Decimal::from_str_exact(full_accuracy_value)
        .map_err(|_| PolyBoltError::UnrepresentableValue)?;
    Ok(PolyBoltTwapPrice {
        symbol,
        source,
        window_seconds,
        timestamp_ms,
        exact_value,
    })
}

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Authenticated `PolyBolt` transport for the BTC/USD 60-second TWAP.
pub struct PolyBoltClient {
    endpoint: String,
    credentials: Credentials,
    socket: Socket,
    decoder: PolyBoltDecoder,
    retry_state: RetryState,
    connection_epoch: u64,
    next_transport_frame_sequence: u64,
}

impl fmt::Debug for PolyBoltClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PolyBoltClient")
            .field("endpoint", &self.endpoint)
            .field("connection_epoch", &self.connection_epoch)
            .finish_non_exhaustive()
    }
}

impl PolyBoltClient {
    /// Connects to `PolyBolt`, authenticates with caller-supplied CLOB credentials, and subscribes.
    ///
    /// # Errors
    ///
    /// Returns [`PolyBoltError`] if the connection or authentication handshake fails.
    pub async fn connect(credentials: Credentials) -> Result<Self, PolyBoltError> {
        Self::connect_to(POLYBOLT_ENDPOINT, credentials).await
    }

    async fn connect_to(endpoint: &str, credentials: Credentials) -> Result<Self, PolyBoltError> {
        let socket = connect_authenticated(endpoint, &credentials).await?;
        Ok(Self {
            endpoint: endpoint.to_owned(),
            credentials,
            socket,
            decoder: PolyBoltDecoder::default(),
            retry_state: RetryState::default(),
            connection_epoch: 0,
            next_transport_frame_sequence: 0,
        })
    }

    /// Reads and validates the next data record, filtering native WebSocket controls.
    ///
    /// Authentication/control messages and ping/pong payloads are not returned by this method.
    /// Use [`Self::next_with_control`] to retain native ping/pong liveness evidence.
    ///
    /// # Errors
    ///
    /// Returns [`PolyBoltError`] on authentication errors, an unrecoverable close, or invalid data.
    pub async fn next(&mut self) -> Result<PolyBoltRecord, PolyBoltError> {
        loop {
            match self.next_with_control().await? {
                PolyBoltItem::Record { record, .. } => return Ok(record),
                PolyBoltItem::Control(_) => {}
            }
        }
    }

    /// Reads the next data record or exact native ping/pong control payload.
    ///
    /// Data records keep the server's channel sequence; `frame_sequence` is a separate
    /// per-connection transport counter shared with native control frames. Authentication and
    /// credential-error messages are consumed or rejected and are never returned as tape data.
    ///
    /// # Errors
    ///
    /// Returns [`PolyBoltError`] on authentication errors, an unrecoverable close, invalid data,
    /// or transport frame counter overflow.
    pub async fn next_with_control(&mut self) -> Result<PolyBoltItem, PolyBoltError> {
        loop {
            match read_frame(&mut self.socket).await? {
                SocketRead::Frame(raw) => {
                    let frame_sequence = self.take_transport_frame_sequence()?;
                    return Ok(PolyBoltItem::Record {
                        record: self.decoder.decode(raw, self.connection_epoch)?,
                        frame_sequence,
                    });
                }
                SocketRead::Control { raw, kind } => {
                    let frame_sequence = self.take_transport_frame_sequence()?;
                    return Ok(PolyBoltItem::Control(PolyBoltControl {
                        raw,
                        connection_epoch: self.connection_epoch,
                        frame_sequence,
                        kind,
                    }));
                }
                SocketRead::Ignored => {}
                SocketRead::Close(code) => {
                    let delay = self.retry_state.delay_for_close(code)?;
                    tokio::time::sleep(delay).await;
                    self.socket = connect_authenticated(&self.endpoint, &self.credentials).await?;
                    self.connection_epoch = self
                        .connection_epoch
                        .checked_add(1)
                        .ok_or(PolyBoltError::ConnectionEpochOverflow)?;
                    self.decoder.reset();
                    self.next_transport_frame_sequence = 0;
                }
            }
        }
    }

    fn take_transport_frame_sequence(&mut self) -> Result<u64, PolyBoltError> {
        let sequence = self.next_transport_frame_sequence;
        self.next_transport_frame_sequence = sequence
            .checked_add(1)
            .ok_or(PolyBoltError::TransportFrameSequenceOverflow)?;
        Ok(sequence)
    }
}

#[derive(Debug, Default)]
struct RetryState {
    draining_reconnected: bool,
    abnormal_attempt: u32,
}

impl RetryState {
    fn delay_for_close(&mut self, code: u16) -> Result<Duration, PolyBoltError> {
        match code {
            4003 if !self.draining_reconnected => {
                self.draining_reconnected = true;
                Ok(full_jitter(MAX_DRAIN_DELAY))
            }
            4003 => Err(PolyBoltError::DrainingReconnectExhausted),
            1006 => {
                let delay = full_jitter(abnormal_backoff_cap(self.abnormal_attempt));
                self.abnormal_attempt = self.abnormal_attempt.saturating_add(1);
                Ok(delay)
            }
            _ => Err(close_error(code)),
        }
    }
}

fn abnormal_backoff_cap(attempt: u32) -> Duration {
    Duration::from_secs(1_u64.checked_shl(attempt.min(5)).unwrap_or(32).min(30))
        .min(MAX_ABNORMAL_DELAY)
}

const fn close_error(code: u16) -> PolyBoltError {
    match code {
        4001 => PolyBoltError::AuthenticationClose,
        4002 => PolyBoltError::SlowConsumer,
        4008 => PolyBoltError::PolicyBug,
        _ => PolyBoltError::Closed { code },
    }
}

fn full_jitter(maximum: Duration) -> Duration {
    let hasher = std::collections::hash_map::RandomState::new();
    let range = maximum.as_nanos().saturating_add(1);
    let nanos = u128::from(hasher.hash_one(Instant::now())) % range;
    let seconds = u64::try_from(nanos / 1_000_000_000).unwrap_or(u64::MAX);
    let subsec_nanos = u32::try_from(nanos % 1_000_000_000).unwrap_or_default();
    Duration::new(seconds, subsec_nanos)
}

async fn connect_authenticated(
    endpoint: &str,
    credentials: &Credentials,
) -> Result<Socket, PolyBoltError> {
    let mut attempt = 0_u32;
    loop {
        match connect_authenticated_once(endpoint, credentials).await {
            Err(PolyBoltError::HandshakeUnavailable {
                status,
                retry_after,
            }) => {
                let delay =
                    retry_after.unwrap_or_else(|| full_jitter(abnormal_backoff_cap(attempt)));
                tokio::time::sleep(delay).await;
                attempt = attempt.saturating_add(1);
                let _ = status;
            }
            result => return result,
        }
    }
}

async fn connect_authenticated_once(
    endpoint: &str,
    credentials: &Credentials,
) -> Result<Socket, PolyBoltError> {
    let (mut socket, _) = connect_async(endpoint).await.map_err(connect_error)?;
    let auth = auth_frame(credentials)?;
    socket
        .send(Message::Text(auth.into()))
        .await
        .map_err(|_| PolyBoltError::Transport)?;
    match timeout(AUTH_TIMEOUT, wait_for_auth(&mut socket)).await {
        Ok(result) => result?,
        Err(_) => return Err(PolyBoltError::AuthenticationTimeout),
    }
    socket
        .send(Message::Text(SUBSCRIBE_FRAME.into()))
        .await
        .map_err(|_| PolyBoltError::Transport)?;
    Ok(socket)
}

fn auth_frame(credentials: &Credentials) -> Result<String, PolyBoltError> {
    let body = serde_json::json!({
        "op": "auth",
        "rid": AUTH_RID,
        "auth": {
            "apiKey": credentials.key().to_string(),
            "secret": credentials.secret().expose_secret(),
            "passphrase": credentials.passphrase().expose_secret(),
        }
    });
    serde_json::to_string(&body).map_err(|_| PolyBoltError::AuthenticationProtocol)
}

async fn wait_for_auth(socket: &mut Socket) -> Result<(), PolyBoltError> {
    loop {
        match socket.next().await {
            Some(Ok(Message::Text(text))) => {
                let value: Value = serde_json::from_str(&text)
                    .map_err(|_| PolyBoltError::AuthenticationProtocol)?;
                if let Some(error) = authentication_error(&value) {
                    return Err(error);
                }
                if value.get("op").and_then(Value::as_str) == Some("authed") {
                    if value
                        .get("rid")
                        .and_then(Value::as_str)
                        .is_some_and(|rid| rid != AUTH_RID)
                    {
                        return Err(PolyBoltError::AuthenticationProtocol);
                    }
                    return Ok(());
                }
                if value.get("op").is_some() {
                    return Err(PolyBoltError::AuthenticationProtocol);
                }
            }
            Some(Ok(Message::Ping(_))) => {
                socket.flush().await.map_err(|_| PolyBoltError::Transport)?;
            }
            Some(Ok(Message::Close(frame))) => {
                return Err(close_error(frame.map_or(1006, |frame| frame.code.into())));
            }
            Some(Ok(Message::Pong(_) | Message::Binary(_) | Message::Frame(_))) => {}
            Some(Err(WebSocketError::ConnectionClosed | WebSocketError::AlreadyClosed)) | None => {
                return Err(PolyBoltError::AbnormalClose);
            }
            Some(Err(_)) => return Err(PolyBoltError::Transport),
        }
    }
}

fn connect_error(error: WebSocketError) -> PolyBoltError {
    let WebSocketError::Http(response) = error else {
        return PolyBoltError::Transport;
    };
    let status = response.status().as_u16();
    if matches!(status, 429 | 503) {
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .and_then(parse_retry_after);
        PolyBoltError::HandshakeUnavailable {
            status,
            retry_after,
        }
    } else {
        PolyBoltError::HandshakeRejected { status }
    }
}

fn parse_retry_after(value: &str) -> Option<Duration> {
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let deadline = DateTime::parse_from_rfc2822(value)
        .ok()?
        .with_timezone(&Utc);
    let remaining = deadline.signed_duration_since(Utc::now());
    (remaining.num_milliseconds() > 0)
        .then(|| Duration::from_millis(remaining.num_milliseconds().try_into().unwrap_or_default()))
}

enum SocketRead {
    Frame(PolyBoltRawFrame),
    Ignored,
    Control {
        raw: PolyBoltRawFrame,
        kind: PolyBoltControlKind,
    },
    Close(u16),
}

async fn read_frame(socket: &mut Socket) -> Result<SocketRead, PolyBoltError> {
    loop {
        match socket.next().await {
            Some(Ok(Message::Text(text))) => {
                let receipt_time_ms = Utc::now().timestamp_millis();
                let bytes = text.as_bytes().to_vec();
                if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                    if let Some(error) = authentication_error(&value) {
                        return Err(error);
                    }
                    if value.get("op").is_some() {
                        return Ok(SocketRead::Ignored);
                    }
                }
                return Ok(SocketRead::Frame(PolyBoltRawFrame {
                    receipt_time_ms,
                    bytes,
                }));
            }
            Some(Ok(Message::Binary(bytes))) => {
                return Ok(SocketRead::Frame(PolyBoltRawFrame {
                    receipt_time_ms: Utc::now().timestamp_millis(),
                    bytes: bytes.to_vec(),
                }));
            }
            Some(Ok(Message::Ping(payload))) => {
                let raw = PolyBoltRawFrame {
                    receipt_time_ms: Utc::now().timestamp_millis(),
                    bytes: payload.to_vec(),
                };
                socket.flush().await.map_err(|_| PolyBoltError::Transport)?;
                return Ok(SocketRead::Control {
                    raw,
                    kind: PolyBoltControlKind::Ping,
                });
            }
            Some(Ok(Message::Pong(payload))) => {
                return Ok(SocketRead::Control {
                    raw: PolyBoltRawFrame {
                        receipt_time_ms: Utc::now().timestamp_millis(),
                        bytes: payload.to_vec(),
                    },
                    kind: PolyBoltControlKind::Pong,
                });
            }
            Some(Ok(Message::Close(frame))) => {
                return Ok(SocketRead::Close(
                    frame.map_or(1006, |frame| frame.code.into()),
                ));
            }
            Some(Ok(Message::Frame(_))) => {}
            Some(Err(WebSocketError::ConnectionClosed | WebSocketError::AlreadyClosed)) | None => {
                return Ok(SocketRead::Close(1006));
            }
            Some(Err(_)) => return Err(PolyBoltError::Transport),
        }
    }
}

fn authentication_error(value: &Value) -> Option<PolyBoltError> {
    let object = value.as_object()?;
    let candidates = ["op", "error", "code", "status"]
        .into_iter()
        .filter_map(|key| object.get(key).and_then(Value::as_str));
    for status in candidates {
        match status {
            "auth_invalid" => return Some(PolyBoltError::AuthenticationInvalid),
            "auth_required" => return Some(PolyBoltError::AuthenticationRequired),
            "auth_unavailable" => return Some(PolyBoltError::AuthenticationUnavailable),
            _ => {}
        }
    }
    object
        .get("error")
        .and_then(Value::as_object)
        .and_then(|error| error.get("code"))
        .and_then(Value::as_str)
        .and_then(|status| match status {
            "auth_invalid" => Some(PolyBoltError::AuthenticationInvalid),
            "auth_required" => Some(PolyBoltError::AuthenticationRequired),
            "auth_unavailable" => Some(PolyBoltError::AuthenticationUnavailable),
            _ => None,
        })
}

#[cfg(test)]
mod tests {
    use futures::{SinkExt as _, StreamExt as _};
    use polymarket_client_sdk_v2::auth::{Credentials, Uuid};
    use serde_json::{Value, json};
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    use super::{
        POLYBOLT_TWAP_CHANNEL, PolyBoltClient, PolyBoltDecoder, PolyBoltError, PolyBoltEvent,
        PolyBoltItem, PolyBoltRawFrame, RetryState, authentication_error, close_error,
    };

    fn raw_frame(
        sequence: u64,
        channel: &str,
        payload: Value,
        snapshot: Option<bool>,
        dropped: Option<u64>,
    ) -> PolyBoltRawFrame {
        let mut frame = json!({
            "v": 1,
            "channel": channel,
            "seq": sequence,
            "ts": 5678,
            "snapshot": snapshot,
            "dropped": dropped,
        });
        frame["payload"] = payload;
        PolyBoltRawFrame {
            receipt_time_ms: 1234,
            bytes: frame.to_string().into_bytes(),
        }
    }

    fn twap_payload(symbol: &str, timestamp: i64, value: &str) -> Value {
        json!({
            "symbol": symbol,
            "source": "chainlink",
            "window_seconds": 60,
            "timestamp": timestamp,
            "full_accuracy_value": value,
        })
    }

    #[test]
    fn decodes_nonempty_and_empty_snapshots() -> Result<(), PolyBoltError> {
        let snapshot = json!({
            "symbol": "btcusd",
            "source": "chainlink",
            "window_seconds": 60,
            "data": [{"timestamp": 100, "full_accuracy_value": "1234.0000000000000000001"}],
        });
        let mut decoder = PolyBoltDecoder::default();
        let record = decoder.decode(
            raw_frame(1, POLYBOLT_TWAP_CHANNEL, snapshot, Some(true), None),
            7,
        )?;
        assert!(matches!(
            record.event,
            PolyBoltEvent::Snapshot { ref prices }
                if prices.len() == 1
                    && prices[0].timestamp_ms == 100
                    && prices[0].exact_value.to_string() == "1234.0000000000000000001"
        ));

        let empty = json!({
            "symbol": "btcusd",
            "source": "chainlink",
            "window_seconds": 60,
            "data": [],
        });
        let record = decoder.decode(
            raw_frame(2, POLYBOLT_TWAP_CHANNEL, empty, Some(true), None),
            7,
        )?;
        assert!(matches!(record.event, PolyBoltEvent::EmptySnapshot { .. }));
        Ok(())
    }

    #[test]
    fn update_uses_only_exact_full_accuracy_decimal_values() -> Result<(), PolyBoltError> {
        let mut decoder = PolyBoltDecoder::default();
        let valid = decoder.decode(
            raw_frame(
                1,
                POLYBOLT_TWAP_CHANNEL,
                twap_payload("btcusd", 100, "1234.123456789012345678901"),
                None,
                None,
            ),
            0,
        )?;
        assert!(matches!(
            valid.event,
            PolyBoltEvent::Update { ref price }
                if price.exact_value.to_string() == "1234.123456789012345678901"
        ));

        for (sequence, value) in [
            (2, "1.00000000000000000000000000001"),
            (3, "79228162514264337593543950336"),
        ] {
            assert!(matches!(
                decoder.decode(
                    raw_frame(
                        sequence,
                        POLYBOLT_TWAP_CHANNEL,
                        twap_payload("btcusd", 101, value),
                        None,
                        None,
                    ),
                    0,
                ),
                Err(PolyBoltError::UnrepresentableValue)
            ));
        }

        let mut numeric = twap_payload("btcusd", 102, "1.25");
        numeric["full_accuracy_value"] = json!(1.250_000_000_000_000_2);
        assert!(matches!(
            decoder.decode(raw_frame(4, POLYBOLT_TWAP_CHANNEL, numeric, None, None), 0,),
            Err(PolyBoltError::InvalidFrame)
        ));

        Ok(())
    }

    #[test]
    fn channel_sequence_includes_other_symbols_and_retains_gap_frame() -> Result<(), PolyBoltError>
    {
        let mut decoder = PolyBoltDecoder::default();
        let first = decoder.decode(
            raw_frame(
                40,
                POLYBOLT_TWAP_CHANNEL,
                twap_payload("btcusd", 100, "1"),
                None,
                None,
            ),
            3,
        )?;
        assert!(matches!(first.event, PolyBoltEvent::Update { .. }));
        let other_symbol = decoder.decode(
            raw_frame(
                41,
                POLYBOLT_TWAP_CHANNEL,
                twap_payload("ethusd", 101, "2"),
                None,
                None,
            ),
            3,
        )?;
        assert!(matches!(
            other_symbol.event,
            PolyBoltEvent::OtherSymbol { ref symbol } if symbol == "ethusd"
        ));
        let next = decoder.decode(
            raw_frame(
                42,
                POLYBOLT_TWAP_CHANNEL,
                twap_payload("btcusd", 102, "3"),
                None,
                None,
            ),
            3,
        )?;
        assert!(matches!(next.event, PolyBoltEvent::Update { .. }));

        let raw = raw_frame(
            44,
            POLYBOLT_TWAP_CHANNEL,
            twap_payload("btcusd", 103, "4"),
            None,
            None,
        );
        let record = decoder.decode(raw.clone(), 3)?;
        assert_eq!(record.raw, raw);
        assert!(matches!(
            record.event,
            PolyBoltEvent::Gap(gap)
                if gap.expected_sequence == Some(43)
                    && gap.observed_sequence == 44
                    && gap.missing_sequence_range() == Some((43, 43))
        ));

        decoder.reset();
        let after_reconnect = decoder.decode(
            raw_frame(
                0,
                POLYBOLT_TWAP_CHANNEL,
                twap_payload("btcusd", 104, "5"),
                None,
                None,
            ),
            4,
        )?;
        assert_eq!(after_reconnect.connection_epoch, 4);
        assert!(matches!(
            after_reconnect.event,
            PolyBoltEvent::Update { .. }
        ));
        Ok(())
    }

    #[test]
    fn other_channel_frame_is_valid_and_retains_exact_bytes() -> Result<(), PolyBoltError> {
        let mut decoder = PolyBoltDecoder::default();
        let raw = raw_frame(
            12,
            "price.other",
            twap_payload("btcusd", 101, "2"),
            None,
            None,
        );
        let record = decoder.decode(raw.clone(), 0)?;

        assert_eq!(record.raw, raw);
        assert!(matches!(
            record.event,
            PolyBoltEvent::OtherChannel { ref channel } if channel == "price.other"
        ));
        Ok(())
    }

    #[test]
    fn dropped_count_marks_gap_even_when_sequence_is_dense() -> Result<(), PolyBoltError> {
        let mut decoder = PolyBoltDecoder::default();
        decoder.decode(
            raw_frame(
                9,
                POLYBOLT_TWAP_CHANNEL,
                twap_payload("btcusd", 100, "1"),
                None,
                None,
            ),
            0,
        )?;
        let record = decoder.decode(
            raw_frame(
                10,
                POLYBOLT_TWAP_CHANNEL,
                twap_payload("btcusd", 101, "2"),
                None,
                Some(2),
            ),
            0,
        )?;
        assert!(matches!(
            record.event,
            PolyBoltEvent::Gap(gap)
                if gap.expected_sequence == Some(10)
                    && gap.observed_sequence == 10
                    && gap.dropped == 2
                    && gap.missing_sequence_range().is_none()
        ));
        Ok(())
    }

    #[test]
    fn close_codes_and_draining_reconnect_policy_are_bounded() -> Result<(), PolyBoltError> {
        assert!(matches!(
            close_error(4001),
            PolyBoltError::AuthenticationClose
        ));
        assert!(matches!(close_error(4002), PolyBoltError::SlowConsumer));
        assert!(matches!(close_error(4008), PolyBoltError::PolicyBug));

        let mut retry = RetryState::default();
        let delay = retry.delay_for_close(4003)?;
        assert!(delay <= std::time::Duration::from_secs(10));
        assert!(matches!(
            retry.delay_for_close(4003),
            Err(PolyBoltError::DrainingReconnectExhausted)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn next_with_control_retains_ping_payload_and_separate_transport_sequence()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let expected_control_bytes = [
            b"polybolt-native-ping".to_vec(),
            b"polybolt-native-pong".to_vec(),
        ];
        let challenge_frame = expected_control_bytes[0].clone();
        let response_frame = expected_control_bytes[1].clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            let Some(Ok(Message::Text(_auth))) = socket.next().await else {
                return Err(std::io::Error::other("expected PolyBolt auth frame").into());
            };
            socket
                .send(Message::Text(
                    json!({"op": "authed", "rid": "a1"}).to_string().into(),
                ))
                .await?;
            let Some(Ok(Message::Text(_subscription))) = socket.next().await else {
                return Err(std::io::Error::other("expected PolyBolt subscription").into());
            };
            socket
                .send(Message::Text(
                    json!({"op": "subscribed"}).to_string().into(),
                ))
                .await?;
            socket
                .send(Message::Ping(challenge_frame.clone().into()))
                .await?;
            assert!(matches!(
                socket.next().await,
                Some(Ok(Message::Pong(payload))) if payload.as_ref() == challenge_frame
            ));
            socket.send(Message::Pong(response_frame.into())).await?;
            let frame = String::from_utf8(
                raw_frame(
                    77,
                    POLYBOLT_TWAP_CHANNEL,
                    twap_payload("btcusd", 100, "1.5"),
                    None,
                    None,
                )
                .bytes,
            )?;
            socket.send(Message::Text(frame.into())).await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });

        let credentials = Credentials::new(
            Uuid::nil(),
            "polybolt-secret".to_owned(),
            "polybolt-passphrase".to_owned(),
        );
        let mut client =
            PolyBoltClient::connect_to(&format!("ws://{address}"), credentials).await?;
        let PolyBoltItem::Control(control) = client.next_with_control().await? else {
            return Err(std::io::Error::other("expected native ping control proof").into());
        };
        assert_eq!(control.kind, super::PolyBoltControlKind::Ping);
        assert_eq!(control.raw.bytes, expected_control_bytes[0]);
        assert!(control.raw.receipt_time_ms > 0);
        assert_eq!(control.connection_epoch, 0);
        assert_eq!(control.frame_sequence, 0);

        let PolyBoltItem::Control(control) = client.next_with_control().await? else {
            return Err(std::io::Error::other("expected native pong control proof").into());
        };
        assert_eq!(control.kind, super::PolyBoltControlKind::Pong);
        assert_eq!(control.raw.bytes, expected_control_bytes[1]);
        assert!(control.raw.receipt_time_ms > 0);
        assert_eq!(control.connection_epoch, 0);
        assert_eq!(control.frame_sequence, 1);

        let PolyBoltItem::Record {
            record,
            frame_sequence,
        } = client.next_with_control().await?
        else {
            return Err(std::io::Error::other("expected following TWAP record").into());
        };
        assert_eq!(record.sequence, 77);
        assert_eq!(record.connection_epoch, 0);
        assert_eq!(frame_sequence, 2);
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn authentication_stays_off_debug_errors_and_recorded_data()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let api_key = Uuid::nil();
        let secret = "polybolt-secret-do-not-leak";
        let passphrase = "polybolt-passphrase-do-not-leak";
        let server_key = api_key;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            let Some(Ok(Message::Text(auth))) = socket.next().await else {
                return Err(std::io::Error::other("expected PolyBolt auth frame").into());
            };
            let auth: Value = serde_json::from_str(&auth)?;
            assert_eq!(auth["op"], "auth");
            assert_eq!(auth["rid"], "a1");
            assert_eq!(auth["auth"]["apiKey"], server_key.to_string());
            assert_eq!(auth["auth"]["secret"], secret);
            assert_eq!(auth["auth"]["passphrase"], passphrase);

            socket
                .send(Message::Text(
                    json!({
                        "op": "authed",
                        "rid": "a1",
                        "apiKey": server_key.to_string(),
                        "secret": secret,
                        "passphrase": passphrase,
                    })
                    .to_string()
                    .into(),
                ))
                .await?;
            let Some(Ok(Message::Text(subscription))) = socket.next().await else {
                return Err(std::io::Error::other("expected PolyBolt subscription").into());
            };
            assert_eq!(subscription.as_str(), super::SUBSCRIBE_FRAME);
            let frame = String::from_utf8(
                raw_frame(
                    1,
                    POLYBOLT_TWAP_CHANNEL,
                    twap_payload("btcusd", 100, "1.5"),
                    None,
                    None,
                )
                .bytes,
            )?;
            socket.send(Message::Text(frame.into())).await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });

        let credentials = Credentials::new(api_key, secret.to_owned(), passphrase.to_owned());
        let mut client =
            PolyBoltClient::connect_to(&format!("ws://{address}"), credentials).await?;
        let debug = format!("{client:?}");
        let auth_error = authentication_error(&json!({
            "op": "auth_invalid",
            "detail": secret,
            "apiKey": api_key.to_string(),
            "passphrase": passphrase,
        }))
        .ok_or_else(|| std::io::Error::other("expected auth error"))?;
        let error_text = format!("{auth_error}; {auth_error:?}");
        for sensitive in [
            api_key.to_string(),
            secret.to_owned(),
            passphrase.to_owned(),
        ] {
            assert!(!debug.contains(&sensitive));
            assert!(!error_text.contains(&sensitive));
        }

        let record = client.next().await?;
        let taped_frame = String::from_utf8(record.raw.bytes)?;
        for sensitive in [
            api_key.to_string(),
            secret.to_owned(),
            passphrase.to_owned(),
        ] {
            assert!(!taped_frame.contains(&sensitive));
        }
        server.await??;
        Ok(())
    }
}
