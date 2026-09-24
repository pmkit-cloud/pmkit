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
    async fn subscribe_reference(&self, sink: Sender<SourceSignal>) -> Result<(), DataSourceError> {
        let (mut socket, _) = connect_async(self.endpoint.as_ref())
            .await
            .map_err(|error| DataSourceError::Unavailable {
                message: format!("Binance aggTrade connection failed: {error}"),
            })?;
        let mut heartbeat =
            tokio::time::interval(std::time::Duration::from_millis(LIVE_HEARTBEAT_INTERVAL_MS));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await;
        loop {
            let message = tokio::select! {
                _ = heartbeat.tick() => {
                    sink.send(SourceSignal::Watermark(live_watermark_now()))
                        .await
                        .map_err(|_| DataSourceError::SinkClosed)?;
                    continue;
                }
                message = socket.next() => message,
            };
            let Some(message) = message else {
                break;
            };
            let message = message.map_err(|error| DataSourceError::Unavailable {
                message: format!("Binance aggTrade stream failed: {error}"),
            })?;
            let Message::Text(text) = message else {
                continue;
            };
            let fact = parse_binance_agg_trade_live(&text, self.asset).map_err(|error| {
                DataSourceError::ReplayGap {
                    message: format!("invalid Binance live aggregate trade: {error}"),
                }
            })?;
            let timestamp_ms = match &fact {
                CexReferenceEvent::Trade { timestamp_ms, .. } => *timestamp_ms,
            };
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
                        connection_epoch: 0,
                        frame_sequence,
                        ingest_sequence,
                    },
                    fact,
                },
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
    use super::BinanceAggTradeLive;
    use crate::{LiveCexDataSource, SourceSignal};
    use futures_util::SinkExt;
    use pmkit_event::{CexReferenceEvent, SourceEnvelope};
    use pmkit_market::Asset;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

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
