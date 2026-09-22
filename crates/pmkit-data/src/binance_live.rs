use std::sync::Arc;

use async_trait::async_trait;
use futures_util::StreamExt;
use pmkit_event::{CexReferenceEnvelope, CexReferenceEvent, SourceEnvelope, StreamMetadata};
use pmkit_market::Asset;
use tokio::sync::mpsc::Sender;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{
    DataSourceError, LIVE_HEARTBEAT_INTERVAL_MS, LiveCexDataSource, SourceSignal,
    binance::{BINANCE_REFERENCE_SOURCE_ID, reference_trade_identity},
    live_watermark_now, now_ms, parse_binance_agg_trade_live,
};

const BINANCE_WS_BASE: &str = "wss://stream.binance.com:9443/ws";
const BINANCE_CONNECT_TIMEOUT_MS: u64 = 1_000;
const BINANCE_MAX_RECONNECT_ATTEMPTS: usize = 3;
const BINANCE_RECONNECT_DELAY_MS: u64 = 100;

/// A live Binance `@aggTrade` source paired with Vision archive replay.
#[derive(Debug, Clone)]
pub struct BinanceAggTradeLive {
    asset: Asset,
    endpoint: Arc<str>,
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
        }
    }
}

#[async_trait]
impl LiveCexDataSource for BinanceAggTradeLive {
    #[expect(
        clippy::too_many_lines,
        reason = "one source-owned loop keeps reconnect, heartbeat, and sequencing fail-closed"
    )]
    async fn subscribe_reference(&self, sink: Sender<SourceSignal>) -> Result<(), DataSourceError> {
        let mut connection_epoch = 0_i64;
        let mut had_connection = false;
        let mut reconnect_attempts = 0_usize;
        let mut last_aggregate_trade_id: Option<u64> = None;

        loop {
            let connection = tokio::time::timeout(
                std::time::Duration::from_millis(BINANCE_CONNECT_TIMEOUT_MS),
                connect_async(self.endpoint.as_ref()),
            )
            .await
            .map_err(|_| "Binance aggTrade connection timed out".to_owned())
            .and_then(|connection| {
                connection.map_err(|error| format!("Binance aggTrade connection failed: {error}"))
            });
            let (mut socket, _) = match connection {
                Ok(connection) => {
                    if had_connection {
                        connection_epoch = connection_epoch.checked_add(1).ok_or_else(|| {
                            DataSourceError::ReplayGap {
                                message: "Binance connection epoch overflow".into(),
                            }
                        })?;
                    }
                    had_connection = true;
                    connection
                }
                Err(message) => {
                    let error = DataSourceError::Unavailable { message };
                    if reconnect_attempts >= BINANCE_MAX_RECONNECT_ATTEMPTS {
                        return Err(error);
                    }
                    reconnect_attempts += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(
                        BINANCE_RECONNECT_DELAY_MS,
                    ))
                    .await;
                    continue;
                }
            };
            let mut continuity_validated = false;
            let mut heartbeat =
                tokio::time::interval(std::time::Duration::from_millis(LIVE_HEARTBEAT_INTERVAL_MS));
            heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            heartbeat.tick().await;
            let disconnect = loop {
                let message = tokio::select! {
                    _ = heartbeat.tick() => {
                        if continuity_validated {
                            sink.send(SourceSignal::Watermark(live_watermark_now()))
                                .await
                                .map_err(|_| DataSourceError::SinkClosed)?;
                        }
                        continue;
                    }
                    message = socket.next() => message,
                };
                let Some(message) = message else {
                    break DataSourceError::Unavailable {
                        message: "Binance aggTrade stream ended".into(),
                    };
                };
                let message = match message {
                    Ok(message) => message,
                    Err(error) => {
                        break DataSourceError::Unavailable {
                            message: format!("Binance aggTrade stream failed: {error}"),
                        };
                    }
                };
                let Message::Text(text) = message else {
                    continue;
                };
                let fact = parse_binance_agg_trade_live(&text, self.asset).map_err(|error| {
                    DataSourceError::ReplayGap {
                        message: format!("invalid Binance live aggregate trade: {error}"),
                    }
                })?;
                let (aggregate_trade_id, timestamp_ms) = match &fact {
                    CexReferenceEvent::Trade {
                        aggregate_trade_id,
                        timestamp_ms,
                        ..
                    } => (*aggregate_trade_id, *timestamp_ms),
                };
                if let Some(previous) = last_aggregate_trade_id {
                    let expected =
                        previous
                            .checked_add(1)
                            .ok_or_else(|| DataSourceError::ReplayGap {
                                message: "Binance aggregate trade ID overflow".into(),
                            })?;
                    if aggregate_trade_id != expected {
                        return Err(DataSourceError::ReplayGap {
                            message: format!(
                                "Binance aggregate trade gap: expected {expected}, got {aggregate_trade_id}"
                            ),
                        });
                    }
                }
                last_aggregate_trade_id = Some(aggregate_trade_id);
                continuity_validated = true;
                let (frame_sequence, ingest_sequence) = reference_trade_identity(&fact)?;
                sink.send(SourceSignal::Data(Box::new(SourceEnvelope::CexReference(
                    CexReferenceEnvelope {
                        metadata: StreamMetadata {
                            schema_version: 1,
                            source_id: BINANCE_REFERENCE_SOURCE_ID.to_owned(),
                            source_time_ms: timestamp_ms,
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
                reconnect_attempts = 0;
            };
            if reconnect_attempts >= BINANCE_MAX_RECONNECT_ATTEMPTS {
                return Err(disconnect);
            }
            reconnect_attempts += 1;
            tokio::time::sleep(std::time::Duration::from_millis(BINANCE_RECONNECT_DELAY_MS)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BINANCE_MAX_RECONNECT_ATTEMPTS, BinanceAggTradeLive};
    use crate::{DataSourceError, LIVE_HEARTBEAT_INTERVAL_MS, LiveCexDataSource, SourceSignal};
    use futures_util::SinkExt;
    use pmkit_event::{CexReferenceEvent, SourceEnvelope};
    use pmkit_market::Asset;
    use tokio::net::TcpListener;
    use tokio::sync::{mpsc, oneshot};
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    #[tokio::test]
    async fn reconnect_preserves_receipt_order_and_connection_epoch()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            for payload in [
                r#"{"e":"aggTrade","a":7,"p":"0.42","q":"1","T":1735689600123,"m":false}"#,
                r#"{"e":"aggTrade","a":8,"p":"0.43","q":"1","T":1735689601123,"m":false}"#,
            ] {
                let (stream, _) = listener.accept().await?;
                let mut socket = accept_async(stream).await?;
                socket.send(Message::Text(payload.into())).await?;
                socket.close(None).await?;
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        let source = BinanceAggTradeLive::with_endpoint(Asset::Btc, &format!("ws://{address}"));
        let (sink, mut events) = mpsc::channel(8);

        let result = source.subscribe_reference(sink).await;
        assert!(matches!(result, Err(DataSourceError::Unavailable { .. })));
        let mut envelopes = Vec::new();
        while let Some(signal) = events.recv().await {
            let SourceSignal::Data(envelope) = signal else {
                return Err("reconnect must not synthesize a lifecycle signal".into());
            };
            let SourceEnvelope::CexReference(envelope) = *envelope else {
                return Err("expected CEX envelope".into());
            };
            envelopes.push(envelope);
        }

        assert_eq!(envelopes.len(), 2);
        let expected_connection = format!("ws://{address}/btcusdt@aggTrade");
        assert_eq!(
            envelopes
                .iter()
                .map(|envelope| envelope.metadata.source_id.as_str())
                .collect::<Vec<_>>(),
            vec!["binance:aggTrade", "binance:aggTrade"]
        );
        assert_eq!(
            envelopes
                .iter()
                .map(|envelope| envelope.metadata.connection_id.as_str())
                .collect::<Vec<_>>(),
            vec![expected_connection.as_str(), expected_connection.as_str()]
        );
        assert_eq!(
            envelopes
                .iter()
                .map(|envelope| envelope.metadata.connection_epoch)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(
            envelopes
                .iter()
                .map(|envelope| envelope.metadata.frame_sequence)
                .collect::<Vec<_>>(),
            vec![7, 8]
        );
        assert_eq!(
            envelopes
                .iter()
                .map(|envelope| envelope.metadata.ingest_sequence)
                .collect::<Vec<_>>(),
            vec![7, 8]
        );
        assert!(
            envelopes[0].metadata.receipt_time_ms <= envelopes[1].metadata.receipt_time_ms,
            "receipt ordering must survive reconnect"
        );
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn reconnect_withholds_watermark_until_first_validated_trade()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (reconnected, reconnected_rx) = oneshot::channel();
        let (send_trade, send_trade_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            socket
                .send(Message::Text(
                    r#"{"e":"aggTrade","a":7,"p":"0.42","q":"1","T":1735689600123,"m":false}"#
                        .into(),
                ))
                .await?;
            drop(socket);

            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            let _ = reconnected.send(());
            let _ = send_trade_rx.await;
            socket
                .send(Message::Text(
                    r#"{"e":"aggTrade","a":8,"p":"0.43","q":"1","T":1735689601123,"m":false}"#
                        .into(),
                ))
                .await?;
            drop(socket);
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        let source = BinanceAggTradeLive::with_endpoint(Asset::Btc, &format!("ws://{address}"));
        let (sink, mut events) = mpsc::channel(8);
        let source_task = tokio::spawn(async move { source.subscribe_reference(sink).await });

        let Some(SourceSignal::Data(first)) =
            tokio::time::timeout(std::time::Duration::from_secs(2), events.recv()).await?
        else {
            return Err("expected first trade before reconnect".into());
        };
        let SourceEnvelope::CexReference(first) = *first else {
            return Err("expected CEX envelope".into());
        };
        assert!(matches!(
            first.fact,
            CexReferenceEvent::Trade {
                aggregate_trade_id: 7,
                ..
            }
        ));
        tokio::time::timeout(std::time::Duration::from_secs(2), reconnected_rx)
            .await
            .map_err(|_| std::io::Error::other("timed out waiting for reconnect"))??;

        let no_signal = tokio::time::timeout(
            std::time::Duration::from_millis(LIVE_HEARTBEAT_INTERVAL_MS + 100),
            events.recv(),
        )
        .await;
        assert!(
            no_signal.is_err(),
            "reconnect emitted a signal before continuity was validated"
        );

        let _ = send_trade.send(());
        let Some(SourceSignal::Data(second)) =
            tokio::time::timeout(std::time::Duration::from_secs(2), events.recv()).await?
        else {
            return Err("expected validated reconnect trade as the next signal".into());
        };
        let SourceEnvelope::CexReference(second) = *second else {
            return Err("expected CEX envelope".into());
        };
        assert!(matches!(
            second.fact,
            CexReferenceEvent::Trade {
                aggregate_trade_id: 8,
                ..
            }
        ));

        source_task.abort();
        tokio::time::timeout(std::time::Duration::from_secs(2), server).await???;
        Ok(())
    }

    #[tokio::test]
    async fn initial_warmup_withholds_watermark_until_first_trade()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (connected, connected_rx) = oneshot::channel();
        let (send_trade, send_trade_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            let _ = connected.send(());
            let _ = send_trade_rx.await;
            socket
                .send(Message::Text(
                    r#"{"e":"aggTrade","a":7,"p":"0.42","q":"1","T":1735689600123,"m":false}"#
                        .into(),
                ))
                .await?;
            socket.close(None).await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        let source = BinanceAggTradeLive::with_endpoint(Asset::Btc, &format!("ws://{address}"));
        let (sink, mut events) = mpsc::channel(8);
        let source_task = tokio::spawn(async move { source.subscribe_reference(sink).await });

        tokio::time::timeout(std::time::Duration::from_secs(2), connected_rx).await??;
        let no_signal = tokio::time::timeout(
            std::time::Duration::from_millis(LIVE_HEARTBEAT_INTERVAL_MS + 100),
            events.recv(),
        )
        .await;
        assert!(
            no_signal.is_err(),
            "initial connection emitted a watermark before its first trade"
        );

        let _ = send_trade.send(());
        let Some(SourceSignal::Data(envelope)) =
            tokio::time::timeout(std::time::Duration::from_secs(2), events.recv()).await?
        else {
            return Err("expected first trade after warm-up".into());
        };
        let SourceEnvelope::CexReference(envelope) = *envelope else {
            return Err("expected CEX envelope".into());
        };
        assert!(matches!(
            envelope.fact,
            CexReferenceEvent::Trade {
                aggregate_trade_id: 7,
                ..
            }
        ));

        source_task.abort();
        tokio::time::timeout(std::time::Duration::from_secs(2), server).await???;
        Ok(())
    }

    #[tokio::test]
    async fn reconnect_attempts_reset_after_data_delivery()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let mut accepted = 0;
            for _ in 0..(BINANCE_MAX_RECONNECT_ATTEMPTS - 1) {
                let (stream, _) = listener.accept().await?;
                let socket = accept_async(stream).await?;
                drop(socket);
                accepted += 1;
            }
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            socket
                .send(Message::Text(
                    r#"{"e":"aggTrade","a":7,"p":"0.42","q":"1","T":1735689600123,"m":false}"#
                        .into(),
                ))
                .await?;
            drop(socket);
            accepted += 1;
            for _ in 0..BINANCE_MAX_RECONNECT_ATTEMPTS {
                let (stream, _) = listener.accept().await?;
                let socket = accept_async(stream).await?;
                drop(socket);
                accepted += 1;
            }
            Ok::<usize, Box<dyn std::error::Error + Send + Sync>>(accepted)
        });
        let source = BinanceAggTradeLive::with_endpoint(Asset::Btc, &format!("ws://{address}"));
        let (sink, mut events) = mpsc::channel(8);

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            source.subscribe_reference(sink),
        )
        .await?;
        assert!(matches!(result, Err(DataSourceError::Unavailable { .. })));
        let Some(SourceSignal::Data(envelope)) = events.recv().await else {
            return Err("expected one normalized CEX event".into());
        };
        let SourceEnvelope::CexReference(envelope) = *envelope else {
            return Err("expected CEX envelope".into());
        };
        let CexReferenceEvent::Trade {
            aggregate_trade_id, ..
        } = envelope.fact;
        assert_eq!(aggregate_trade_id, 7);
        assert!(events.recv().await.is_none());
        let accepted = tokio::time::timeout(std::time::Duration::from_secs(1), server).await??;
        assert_eq!(accepted?, BINANCE_MAX_RECONNECT_ATTEMPTS * 2);
        Ok(())
    }

    #[tokio::test]
    async fn connect_handshake_timeout_is_bounded_and_unavailable()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (release, wait) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let mut streams = Vec::new();
            for _ in 0..=BINANCE_MAX_RECONNECT_ATTEMPTS {
                let (stream, _) = listener.accept().await?;
                streams.push(stream);
            }
            let accepted = streams.len();
            let _ = wait.await;
            Ok::<usize, Box<dyn std::error::Error + Send + Sync>>(accepted)
        });
        let source = BinanceAggTradeLive::with_endpoint(Asset::Btc, &format!("ws://{address}"));
        let (sink, mut events) = mpsc::channel(8);

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(6),
            source.subscribe_reference(sink),
        )
        .await;
        drop(release);
        let accepted = tokio::time::timeout(std::time::Duration::from_secs(1), server).await??;
        let result = result?;
        assert!(matches!(
            result,
            Err(DataSourceError::Unavailable { message })
                if message == "Binance aggTrade connection timed out"
        ));
        assert!(events.recv().await.is_none());
        assert_eq!(accepted?, BINANCE_MAX_RECONNECT_ATTEMPTS + 1);
        Ok(())
    }

    #[tokio::test]
    async fn reconnect_rejects_non_consecutive_aggregate_trade_before_emitting_it()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            for payload in [
                r#"{"e":"aggTrade","a":7,"p":"0.42","q":"1","T":1735689600123,"m":false}"#,
                r#"{"e":"aggTrade","a":9,"p":"0.43","q":"1","T":1735689601123,"m":false}"#,
            ] {
                let (stream, _) = listener.accept().await?;
                let mut socket = accept_async(stream).await?;
                socket.send(Message::Text(payload.into())).await?;
                socket.close(None).await?;
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        let source = BinanceAggTradeLive::with_endpoint(Asset::Btc, &format!("ws://{address}"));
        let (sink, mut events) = mpsc::channel(8);

        let result = source.subscribe_reference(sink).await;
        assert!(matches!(
            result,
            Err(DataSourceError::ReplayGap { message })
                if message == "Binance aggregate trade gap: expected 8, got 9"
        ));
        let mut envelopes = Vec::new();
        while let Some(signal) = events.recv().await {
            let SourceSignal::Data(envelope) = signal else {
                return Err("reconnect must not synthesize a lifecycle signal".into());
            };
            let SourceEnvelope::CexReference(envelope) = *envelope else {
                return Err("expected CEX envelope".into());
            };
            envelopes.push(envelope);
        }

        assert_eq!(envelopes.len(), 1);
        let CexReferenceEvent::Trade {
            aggregate_trade_id, ..
        } = &envelopes[0].fact;
        assert_eq!(*aggregate_trade_id, 7);
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn terminal_disconnect_is_bounded_and_fails_closed()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let mut accepted = 0;
            for _ in 0..=BINANCE_MAX_RECONNECT_ATTEMPTS {
                let (stream, _) = listener.accept().await?;
                let mut socket = accept_async(stream).await?;
                socket.close(None).await?;
                accepted += 1;
            }
            Ok::<usize, Box<dyn std::error::Error + Send + Sync>>(accepted)
        });
        let source = BinanceAggTradeLive::with_endpoint(Asset::Btc, &format!("ws://{address}"));
        let (sink, mut events) = mpsc::channel(8);

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            source.subscribe_reference(sink),
        )
        .await?;
        assert!(matches!(result, Err(DataSourceError::Unavailable { .. })));
        assert!(events.recv().await.is_none());
        let accepted = tokio::time::timeout(std::time::Duration::from_secs(1), server).await??;
        assert_eq!(accepted?, BINANCE_MAX_RECONNECT_ATTEMPTS + 1);
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
            socket
                .send(Message::Text(
                    r#"{"e":"aggTrade","a":7,"p":"0.42","q":"1","T":1735689600123,"m":false}"#
                        .into(),
                ))
                .await?;
            socket.close(None).await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });
        let source = BinanceAggTradeLive::with_endpoint(Asset::Btc, &format!("ws://{address}"));
        let (sink, mut events) = mpsc::channel(2);

        let result = source.subscribe_reference(sink).await;
        assert!(result.is_err());
        let Some(SourceSignal::Data(envelope)) = events.recv().await else {
            return Err("expected one normalized CEX event".into());
        };
        let SourceEnvelope::CexReference(envelope) = *envelope else {
            return Err("expected CEX envelope".into());
        };
        let CexReferenceEvent::Trade {
            aggregate_trade_id,
            price,
            ..
        } = envelope.fact;
        assert_eq!(aggregate_trade_id, 7);
        assert_eq!(price.to_string(), "0.42");
        assert_eq!(envelope.metadata.source_id, "binance:aggTrade");
        assert_eq!(envelope.metadata.frame_sequence, 7);
        server.await??;
        Ok(())
    }
}
