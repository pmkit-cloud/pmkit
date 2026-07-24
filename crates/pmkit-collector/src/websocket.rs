//! Concrete `tokio-tungstenite` transport for the collector.

use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::{CollectedFrame, Connection, Subscription, Transport, TransportError};

/// A `tokio-tungstenite` WebSocket transport.
///
/// On connect it opens `url` and sends each subscription `topic` as an exact
/// text frame. Heartbeats are WebSocket pings.
#[derive(Debug, Clone)]
pub struct WebSocketTransport {
    url: String,
}

impl WebSocketTransport {
    /// Creates a transport that connects to `url`.
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

/// One live WebSocket connection.
#[derive(Debug)]
pub struct WebSocketConnection {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

#[async_trait]
impl Transport for WebSocketTransport {
    async fn connect(&self, shard: &[Subscription]) -> Result<Box<dyn Connection>, TransportError> {
        let (mut socket, _) =
            connect_async(self.url.as_str())
                .await
                .map_err(|error| TransportError::Connect {
                    message: error.to_string(),
                })?;
        for subscription in shard {
            socket
                .send(Message::Text(subscription.topic.as_str().into()))
                .await
                .map_err(|error| TransportError::Connect {
                    message: error.to_string(),
                })?;
        }
        Ok(Box::new(WebSocketConnection { socket }))
    }
}

#[async_trait]
impl Connection for WebSocketConnection {
    async fn recv(&mut self) -> Result<Option<CollectedFrame>, TransportError> {
        while let Some(message) = self.socket.next().await {
            let message = message.map_err(|error| TransportError::Stream {
                message: error.to_string(),
            })?;
            match message {
                Message::Text(text) => {
                    return Ok(Some(CollectedFrame {
                        receipt_time_ms: now_ms(),
                        raw: text.to_string(),
                    }));
                }
                Message::Close(_) => return Ok(None),
                // Control and binary frames are not part of the raw text tape.
                Message::Ping(_) | Message::Pong(_) | Message::Binary(_) | Message::Frame(_) => {}
            }
        }
        Ok(None)
    }

    async fn heartbeat(&mut self) -> Result<(), TransportError> {
        self.socket
            .send(Message::Ping(Vec::new().into()))
            .await
            .map_err(|error| TransportError::Stream {
                message: error.to_string(),
            })
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
        .unwrap_or(i64::MAX)
}
