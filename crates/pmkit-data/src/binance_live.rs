use std::sync::{
    Arc,
    atomic::{AtomicI64, AtomicU64, Ordering},
};

use async_trait::async_trait;
use chrono::Utc;
use futures_util::StreamExt;
use pmkit_event::{CexReferenceEnvelope, CexReferenceEvent, SourceEnvelope, StreamMetadata};
use pmkit_market::Asset;
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    DataSourceError, LiveCexDataSource, SourceHeartbeat, SourceSignal,
    binance::BINANCE_REFERENCE_SOURCE_ID, parse_binance_agg_trade_live,
};

const BINANCE_WS_BASE: &str = "wss://stream.binance.com:9443/ws";
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(0);

/// A live Binance `@aggTrade` source paired with Vision archive replay.
#[derive(Debug, Clone)]
pub struct BinanceAggTradeLive {
    asset: Asset,
    endpoint: Arc<str>,
    connection_epoch: Arc<AtomicI64>,
    ingest_sequence: Arc<AtomicU64>,
}

impl BinanceAggTradeLive {
    /// Creates a source using Binance's public aggregate-trade endpoint.
    #[must_use]
    pub fn new(asset: Asset) -> Self {
        Self::with_endpoint(asset, BINANCE_WS_BASE)
    }

    /// Creates a source with a custom endpoint for tests or controlled proxies.
    #[must_use]
    pub fn with_endpoint(asset: Asset, base_url: &str) -> Self {
        Self {
            asset,
            endpoint: Arc::from(format!("{base_url}/{}@aggTrade", asset.binance_symbol())),
            connection_epoch: Arc::new(AtomicI64::new(0)),
            ingest_sequence: Arc::new(AtomicU64::new(0)),
        }
    }
}

fn decode_live_message(
    text: &str,
    asset: Asset,
    connection_id: &str,
    connection_epoch: i64,
    frame_sequence: i64,
    ingest_sequence: u64,
    receipt_time_ms: i64,
) -> Result<CexReferenceEnvelope, DataSourceError> {
    let raw_frame = text.as_bytes().to_vec();
    let fact =
        parse_binance_agg_trade_live(text, asset).map_err(|error| DataSourceError::ReplayGap {
            message: format!("invalid Binance live aggregate trade: {error}"),
        })?;
    let source_time_ms = match &fact {
        CexReferenceEvent::Trade { timestamp_ms, .. }
        | CexReferenceEvent::Twap60sSnapshot { timestamp_ms, .. }
        | CexReferenceEvent::Twap60sUpdate { timestamp_ms, .. } => *timestamp_ms,
    };
    Ok(CexReferenceEnvelope {
        metadata: StreamMetadata {
            schema_version: 1,
            source_id: BINANCE_REFERENCE_SOURCE_ID.to_owned(),
            source_time_ms,
            canonical_source_rank: 1,
            receipt_time_ms,
            connection_id: connection_id.to_owned(),
            connection_epoch,
            frame_sequence,
            ingest_sequence,
        },
        raw_frame,
        fact,
    })
}

#[async_trait]
impl LiveCexDataSource for BinanceAggTradeLive {
    async fn subscribe_reference(&self, sink: Sender<SourceSignal>) -> Result<(), DataSourceError> {
        let (mut socket, _) = connect_async(self.endpoint.as_ref())
            .await
            .map_err(|error| DataSourceError::Unavailable {
                message: format!("Binance aggTrade connection failed: {error}"),
            })?;
        let connection_id = format!(
            "{}#{}",
            self.endpoint,
            NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed)
        );
        let connection_epoch = self.connection_epoch.fetch_add(1, Ordering::Relaxed);
        let mut frame_sequence = 0_i64;
        loop {
            let message = tokio::select! {
                message = socket.next() => message,
                () = sink.closed() => return Err(DataSourceError::SinkClosed),
            };
            let Some(message) = message else {
                break;
            };
            let message = message.map_err(|error| DataSourceError::Unavailable {
                message: format!("Binance aggTrade stream failed: {error}"),
            })?;
            let receipt_time_ms = Utc::now().timestamp_millis();
            let text = match message {
                Message::Ping(payload) | Message::Pong(payload) => {
                    let current_frame_sequence = frame_sequence;
                    frame_sequence = frame_sequence.checked_add(1).ok_or_else(|| {
                        DataSourceError::ReplayGap {
                            message: "Binance frame sequence exceeds i64 range".into(),
                        }
                    })?;
                    let ingest_sequence = self.ingest_sequence.fetch_add(1, Ordering::Relaxed);
                    sink.send(SourceSignal::Heartbeat(SourceHeartbeat {
                        metadata: StreamMetadata {
                            schema_version: 1,
                            source_id: BINANCE_REFERENCE_SOURCE_ID.to_owned(),
                            source_time_ms: receipt_time_ms,
                            canonical_source_rank: 1,
                            receipt_time_ms,
                            connection_id: connection_id.clone(),
                            connection_epoch,
                            frame_sequence: current_frame_sequence,
                            ingest_sequence,
                        },
                        raw_frame: payload.to_vec(),
                    }))
                    .await
                    .map_err(|_| DataSourceError::SinkClosed)?;
                    continue;
                }
                Message::Text(text) => text,
                Message::Close(_) => {
                    return Err(DataSourceError::Unavailable {
                        message: "Binance aggTrade stream closed".into(),
                    });
                }
                _ => continue,
            };
            let current_frame_sequence = frame_sequence;
            frame_sequence =
                frame_sequence
                    .checked_add(1)
                    .ok_or_else(|| DataSourceError::ReplayGap {
                        message: "Binance frame sequence exceeds i64 range".into(),
                    })?;
            let ingest_sequence = self.ingest_sequence.fetch_add(1, Ordering::Relaxed);
            let envelope = decode_live_message(
                &text,
                self.asset,
                &connection_id,
                connection_epoch,
                current_frame_sequence,
                ingest_sequence,
                receipt_time_ms,
            )?;
            sink.send(SourceSignal::Data(Box::new(SourceEnvelope::CexReference(
                envelope,
            ))))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        }
        Err(DataSourceError::Unavailable {
            message: "Binance aggTrade stream ended".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{BinanceAggTradeLive, decode_live_message};
    use crate::{DataSourceError, LiveCexDataSource, SourceSignal};
    use chrono::Utc;
    use futures_util::{SinkExt, StreamExt};
    use pmkit_event::{CexReferenceEvent, SourceEnvelope};
    use pmkit_market::{Asset, Exchange};
    use tokio::net::TcpListener;
    use tokio::sync::{mpsc, oneshot};
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    const FIRST_FRAME: &str =
        r#"{"e":"aggTrade","a":7,"p":"0.42","q":"1","T":1735689600123,"m":false}"#;
    const SECOND_FRAME: &str =
        r#"{"e":"aggTrade","a":8,"p":"0.43","q":"2","T":1735689601123,"m":false}"#;
    const TEST_TIMEOUT: Duration = Duration::from_secs(3);

    #[test]
    fn live_decode_records_injected_receipt_latency() -> Result<(), Box<dyn std::error::Error>> {
        let source_time_ms = 1_735_689_600_123;
        let receipt_time_ms = source_time_ms + 250;
        let text = r#"{"e":"aggTrade","a":7,"p":"0.42","q":"1","T":1735689600123,"m":false}"#;
        let envelope =
            decode_live_message(text, Asset::Btc, "connection-1", 2, 11, 19, receipt_time_ms)?;

        assert_eq!(envelope.metadata.source_time_ms, source_time_ms);
        assert_eq!(
            envelope.metadata.receipt_time_ms - envelope.metadata.source_time_ms,
            250
        );
        assert_eq!(envelope.metadata.connection_id, "connection-1");
        assert_eq!(envelope.metadata.connection_epoch, 2);
        assert_eq!(envelope.metadata.frame_sequence, 11);
        assert_eq!(envelope.metadata.ingest_sequence, 19);
        assert_eq!(envelope.raw_frame, text.as_bytes());
        assert!(matches!(
            envelope.fact,
            CexReferenceEvent::Trade {
                aggregate_trade_id: 7,
                timestamp_ms,
                ..
            } if timestamp_ms == source_time_ms
        ));
        Ok(())
    }

    #[tokio::test]
    async fn live_trade_receipt_time_uses_local_clock_window()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        const SOURCE_TIME_MS: i64 = 1_735_689_600_123;

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (ready_tx, ready_rx) = oneshot::channel();
        let (write_tx, write_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            if ready_tx.send(()).is_err() {
                return Err("loopback test stopped before trade write".into());
            }
            write_rx.await?;
            socket.send(Message::Text(FIRST_FRAME.into())).await?;
            socket.close(None).await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });

        let source = BinanceAggTradeLive::with_endpoint(Asset::Btc, &format!("ws://{address}"));
        let (sink, mut events) = mpsc::channel(1);
        let subscription = tokio::spawn(async move { source.subscribe_reference(sink).await });
        tokio::time::timeout(TEST_TIMEOUT, ready_rx).await??;
        let before_server_write_ms = Utc::now().timestamp_millis();
        if write_tx.send(()).is_err() {
            return Err("loopback server stopped before sending the trade".into());
        }

        let Some(SourceSignal::Data(data)) =
            tokio::time::timeout(TEST_TIMEOUT, events.recv()).await?
        else {
            return Err("expected native trade from loopback socket".into());
        };
        let after_sink_receive_ms = Utc::now().timestamp_millis();
        let SourceEnvelope::CexReference(envelope) = *data else {
            return Err("expected CEX reference envelope".into());
        };

        assert_eq!(envelope.metadata.schema_version, 1);
        assert_eq!(envelope.metadata.source_id, "binance:aggTrade");
        assert_eq!(envelope.metadata.source_time_ms, SOURCE_TIME_MS);
        assert_ne!(
            envelope.metadata.receipt_time_ms,
            envelope.metadata.source_time_ms
        );
        assert!(
            (before_server_write_ms..=after_sink_receive_ms)
                .contains(&envelope.metadata.receipt_time_ms),
            "receipt time {} outside local clock window [{before_server_write_ms}, {after_sink_receive_ms}]",
            envelope.metadata.receipt_time_ms
        );
        assert_eq!(envelope.metadata.canonical_source_rank, 1);
        assert!(
            envelope
                .metadata
                .connection_id
                .contains(&address.to_string())
        );
        assert_eq!(envelope.metadata.connection_epoch, 0);
        assert_eq!(envelope.metadata.frame_sequence, 0);
        assert_eq!(envelope.metadata.ingest_sequence, 0);
        assert_eq!(envelope.raw_frame, FIRST_FRAME.as_bytes());
        assert_eq!(
            envelope.fact,
            CexReferenceEvent::Trade {
                asset: Asset::Btc,
                exchange: Exchange::Binance,
                aggregate_trade_id: 7,
                price: "0.42".parse()?,
                qty: "1".parse()?,
                is_buyer_maker: false,
                timestamp_ms: SOURCE_TIME_MS,
            }
        );

        tokio::time::timeout(TEST_TIMEOUT, server).await???;
        let result = tokio::time::timeout(TEST_TIMEOUT, subscription).await??;
        assert!(
            result.is_err(),
            "closed loopback socket should end the subscription"
        );
        Ok(())
    }

    #[tokio::test]
    async fn live_ping_emits_native_heartbeat_while_quiet()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        const PING_PAYLOAD: &[u8] = b"binance-native-ping";

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            socket.send(Message::Ping(PING_PAYLOAD.into())).await?;
            let Some(Ok(Message::Pong(payload))) = futures_util::StreamExt::next(&mut socket).await
            else {
                return Err("expected native pong response".into());
            };
            assert_eq!(payload.as_ref(), PING_PAYLOAD);
            socket.close(None).await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });

        let source = BinanceAggTradeLive::with_endpoint(Asset::Btc, &format!("ws://{address}"));
        let (sink, mut signals) = mpsc::channel(1);
        assert!(source.subscribe_reference(sink).await.is_err());

        let Some(SourceSignal::Heartbeat(heartbeat)) = signals.recv().await else {
            return Err("expected transport heartbeat for native ping".into());
        };
        assert_eq!(heartbeat.raw_frame, PING_PAYLOAD);
        assert_eq!(heartbeat.metadata.source_id, "binance:aggTrade");
        assert_eq!(
            heartbeat.metadata.source_time_ms,
            heartbeat.metadata.receipt_time_ms
        );
        assert!(heartbeat.metadata.receipt_time_ms > 0);
        assert!(
            heartbeat
                .metadata
                .connection_id
                .contains(&address.to_string())
        );
        assert_eq!(heartbeat.metadata.connection_epoch, 0);
        assert_eq!(heartbeat.metadata.frame_sequence, 0);
        assert_eq!(heartbeat.metadata.ingest_sequence, 0);
        assert!(matches!(
            signals.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));

        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn live_message_matches_normalized_trade_contract()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            socket.send(Message::Text(FIRST_FRAME.into())).await?;
            socket.send(Message::Text(SECOND_FRAME.into())).await?;
            socket.close(None).await?;

            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            socket.send(Message::Text(FIRST_FRAME.into())).await?;
            socket.close(None).await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        let source = BinanceAggTradeLive::with_endpoint(Asset::Btc, &format!("ws://{address}"));
        let (sink, mut events) = mpsc::channel(1);
        let first_subscription = {
            let source = source.clone();
            let sink = sink.clone();
            tokio::spawn(async move { source.subscribe_reference(sink).await })
        };

        let Some(SourceSignal::Data(first)) =
            tokio::time::timeout(TEST_TIMEOUT, events.recv()).await?
        else {
            return Err("expected first normalized CEX event".into());
        };
        let SourceEnvelope::CexReference(first) = *first else {
            return Err("expected CEX envelope".into());
        };
        let CexReferenceEvent::Trade {
            aggregate_trade_id,
            price,
            ..
        } = first.fact
        else {
            return Err("expected trade fact".into());
        };
        assert_eq!(aggregate_trade_id, 7);
        assert_eq!(price.to_string(), "0.42");
        assert_eq!(first.metadata.source_id, "binance:aggTrade");
        assert_eq!(first.metadata.connection_epoch, 0);
        assert_eq!(first.metadata.frame_sequence, 0);
        assert_eq!(first.metadata.ingest_sequence, 0);
        assert_eq!(first.raw_frame, FIRST_FRAME.as_bytes());

        let Some(SourceSignal::Data(second)) =
            tokio::time::timeout(TEST_TIMEOUT, events.recv()).await?
        else {
            return Err("expected second normalized CEX event".into());
        };
        let SourceEnvelope::CexReference(second) = *second else {
            return Err("expected CEX envelope".into());
        };
        assert_eq!(second.metadata.connection_id, first.metadata.connection_id);
        assert_eq!(second.metadata.connection_epoch, 0);
        assert_eq!(second.metadata.frame_sequence, 1);
        assert_eq!(second.metadata.ingest_sequence, 1);
        assert_eq!(second.raw_frame, SECOND_FRAME.as_bytes());
        assert!(matches!(
            second.fact,
            CexReferenceEvent::Trade {
                aggregate_trade_id: 8,
                ..
            }
        ));
        let result = tokio::time::timeout(TEST_TIMEOUT, first_subscription).await??;
        assert!(result.is_err());

        let result = tokio::time::timeout(TEST_TIMEOUT, source.subscribe_reference(sink)).await?;
        assert!(result.is_err());
        let Some(SourceSignal::Data(reconnected)) =
            tokio::time::timeout(TEST_TIMEOUT, events.recv()).await?
        else {
            return Err("expected event after reconnect".into());
        };
        let SourceEnvelope::CexReference(reconnected) = *reconnected else {
            return Err("expected CEX envelope".into());
        };
        assert_ne!(
            reconnected.metadata.connection_id,
            first.metadata.connection_id
        );
        assert_eq!(reconnected.metadata.connection_epoch, 1);
        assert_eq!(reconnected.metadata.frame_sequence, 0);
        assert_eq!(reconnected.metadata.ingest_sequence, 2);
        assert_eq!(reconnected.raw_frame, FIRST_FRAME.as_bytes());
        assert!(matches!(
            reconnected.fact,
            CexReferenceEvent::Trade {
                aggregate_trade_id: 7,
                ..
            }
        ));
        tokio::time::timeout(TEST_TIMEOUT, server).await???;
        Ok(())
    }

    #[tokio::test]
    async fn live_source_stops_when_sink_closes_while_socket_is_quiet()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (accepted, connected) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            let _ = accepted.send(());
            let _ = socket.next().await;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        let source = BinanceAggTradeLive::with_endpoint(Asset::Btc, &format!("ws://{address}"));
        let (sink, receiver) = mpsc::channel(1);
        let subscription = tokio::spawn(async move { source.subscribe_reference(sink).await });

        tokio::time::timeout(TEST_TIMEOUT, connected).await??;
        drop(receiver);
        let result = tokio::time::timeout(TEST_TIMEOUT, subscription).await??;
        assert!(matches!(result, Err(DataSourceError::SinkClosed)));
        tokio::time::timeout(TEST_TIMEOUT, server).await???;
        Ok(())
    }
}
