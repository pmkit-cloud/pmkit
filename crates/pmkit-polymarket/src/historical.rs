use std::{num::NonZeroUsize, sync::Arc};

use async_trait::async_trait;
use pmkit_data::{DataSourceError, HistoricalDataSource, ReplayQuery, SourceSignal};
use pmkit_event::{PmMarketEnvelope, SourceEnvelope, StreamMetadata};
use pmkit_market::Outcome;
use pmkit_store::{OwnerScope, ReplayItem, TapeStore};
use tokio::sync::mpsc::Sender;

use crate::{MarketTokens, live::MarketFrameDecoder};

/// A Polymarket historical source that replays durable PM market envelopes from storage.
#[derive(Clone)]
pub struct PolymarketHistoricalData {
    store: Arc<dyn TapeStore>,
    scope: OwnerScope,
    tokens: MarketTokens,
}

impl std::fmt::Debug for PolymarketHistoricalData {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PolymarketHistoricalData")
            .field("scope", &self.scope)
            .field("tokens", &self.tokens)
            .finish_non_exhaustive()
    }
}

impl PolymarketHistoricalData {
    /// Creates a historical source for one market scope backed by durable storage.
    #[must_use]
    pub fn new(store: Arc<dyn TapeStore>, scope: OwnerScope, tokens: MarketTokens) -> Self {
        Self {
            store,
            scope,
            tokens,
        }
    }
}

#[async_trait]
impl HistoricalDataSource for PolymarketHistoricalData {
    async fn replay(
        &self,
        query: ReplayQuery,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        let market = self.tokens.market();
        if !query.markets.iter().any(|candidate| candidate == market) {
            sink.send(SourceSignal::Eof)
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            return Ok(());
        }

        let from_ms = query.from.timestamp_millis();
        let to_ms = query.to.timestamp_millis();
        let stream_prefix = format!("market:{market}:");
        let mut up_decoder = MarketFrameDecoder::new(self.tokens.clone(), Outcome::Up);
        let mut down_decoder = MarketFrameDecoder::new(self.tokens.clone(), Outcome::Down);
        let mut cursor = None;
        let limit = NonZeroUsize::new(256).ok_or_else(|| DataSourceError::ReplayGap {
            message: "invalid page limit".into(),
        })?;

        loop {
            let page = self
                .store
                .read_envelopes(&self.scope, cursor, limit)
                .await
                .map_err(|error| DataSourceError::ReplayGap {
                    message: error.to_string(),
                })?;
            if page.items.is_empty() {
                break;
            }

            for item in page.items {
                match item {
                    ReplayItem::Envelope(envelope) => {
                        if envelope.source_timestamp_ms < from_ms {
                            continue;
                        }
                        if envelope.source_timestamp_ms >= to_ms {
                            sink.send(SourceSignal::Eof)
                                .await
                                .map_err(|_| DataSourceError::SinkClosed)?;
                            return Ok(());
                        }
                        let stream_id = envelope.canonical_stream_id();
                        let event = match stream_id.strip_prefix(&stream_prefix) {
                            Some("up") => up_decoder.decode(&envelope.raw_frame)?.1,
                            Some("down") => down_decoder.decode(&envelope.raw_frame)?.1,
                            _ => match up_decoder.decode(&envelope.raw_frame)?.1 {
                                Some(event) => Some(event),
                                None => down_decoder.decode(&envelope.raw_frame)?.1,
                            },
                        };
                        let Some(event) = event else {
                            continue;
                        };
                        let metadata = StreamMetadata {
                            schema_version: envelope.schema_version,
                            source_id: envelope.source_id.clone(),
                            source_time_ms: envelope.source_timestamp_ms,
                            canonical_source_rank: envelope.canonical_source_rank,
                            receipt_time_ms: envelope.receipt_timestamp_ms,
                            connection_id: envelope.connection_id.clone(),
                            connection_epoch: envelope.connection_epoch,
                            frame_sequence: envelope.frame_sequence,
                            ingest_sequence: u64::try_from(envelope.ingest_sequence).map_err(
                                |_| DataSourceError::ReplayGap {
                                    message: "ingest sequence exceeds replay range".into(),
                                },
                            )?,
                        };
                        sink.send(SourceSignal::Data(Box::new(SourceEnvelope::PmMarket(
                            PmMarketEnvelope {
                                metadata,
                                raw_frame: envelope.raw_frame,
                                fact: event,
                            },
                        ))))
                        .await
                        .map_err(|_| DataSourceError::SinkClosed)?;
                    }
                    ReplayItem::Gap(gap) => {
                        return Err(DataSourceError::ReplayGap {
                            message: format!(
                                "replay gap at source_time={} ingest_sequence={}",
                                gap.source_timestamp_ms, gap.ingest_sequence
                            ),
                        });
                    }
                }
            }

            cursor = page.next_cursor;
        }

        sink.send(SourceSignal::Eof)
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::significant_drop_tightening)]
    use std::{path::PathBuf, sync::Arc};

    use pmkit_book::Side;
    use pmkit_core::{MarketId, PortfolioId, RunId};
    use pmkit_data::{HistoricalDataSource, ReplayQuery, SourceSignal};
    use pmkit_event::{MarketEvent, PmMarketEnvelope, SourceEnvelope, StreamMetadata};
    use pmkit_market::Outcome;
    use pmkit_store::{OwnerScope, PM_ENVELOPE_VERSION, PmEnvelope, TapeStore, TursoTapeStore};
    use polymarket_client_sdk_v2::types::U256;
    use rust_decimal::Decimal;
    use serde_json::json;

    use super::PolymarketHistoricalData;
    use crate::{MarketTokens, live::MarketFrameDecoder};

    fn database_path() -> Result<(tempfile::TempDir, PathBuf), std::io::Error> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("pmkit-polymarket-historical.db");
        Ok((dir, path))
    }

    fn scope() -> Result<OwnerScope, Box<dyn std::error::Error>> {
        Ok(OwnerScope::new(
            PortfolioId::new("alice")?,
            RunId::new("historical")?,
        ))
    }

    fn tokens() -> Result<MarketTokens, Box<dyn std::error::Error>> {
        Ok(MarketTokens::new(
            MarketId::new("btc-5m")?,
            U256::from(1_u64),
            U256::from(2_u64),
        ))
    }

    fn envelope(
        scope: OwnerScope,
        source_timestamp_ms: i64,
        ingest_sequence: i64,
        raw_frame: &[u8],
    ) -> PmEnvelope {
        PmEnvelope {
            schema_version: PM_ENVELOPE_VERSION,
            scope,
            venue_id: "polymarket".into(),
            config_hash: "fixture".into(),
            source_id: "polymarket-market".into(),
            connection_id: "connection-1".into(),
            source_timestamp_ms,
            canonical_source_rank: 0,
            connection_epoch: 0,
            frame_sequence: ingest_sequence,
            receipt_timestamp_ms: source_timestamp_ms + 1,
            ingest_sequence,
            raw_frame: raw_frame.to_vec(),
            normalized: json!({"kind": "book_update"}),
        }
    }

    #[tokio::test]
    async fn historical_replay_emits_market_signals_and_eof()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given: one stored market envelope in the same scope.
        let (_dir, path) = database_path()?;
        let store = std::sync::Arc::new(TursoTapeStore::open_local(&path).await?);
        let scope = scope()?;
        let market = envelope(
            scope.clone(),
            1_735_689_600_000,
            1,
            br#"{"event_type":"book","asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[{"price":"0.49","size":"2"}],"asks":[{"price":"0.51","size":"3"}]}"#,
        );
        store.store_envelope(&market).await?;
        drop(store);

        // When: the historical source replays the market window.
        let reopened = std::sync::Arc::new(TursoTapeStore::open_local(&path).await?);
        let source = PolymarketHistoricalData::new(reopened.clone(), scope, tokens()?);
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        source
            .replay(
                ReplayQuery {
                    markets: vec![MarketId::new("btc-5m")?],
                    from: "2025-01-01T00:00:00Z".parse()?,
                    to: "2025-12-31T23:59:59Z".parse()?,
                    evidence: pmkit_run::EvidenceRequirement::CorroboratedOnly,
                    retrieval_wait: pmkit_run::RetrievalWait::ReturnPending,
                },
                tx,
            )
            .await?;

        // Then: exactly one market signal is emitted, followed by EOF.
        let mut seen_market = false;
        while let Some(signal) = rx.recv().await {
            match signal {
                SourceSignal::Data(envelope) => {
                    let SourceEnvelope::PmMarket(market_envelope) = *envelope else {
                        return Err("expected market envelope".into());
                    };
                    assert_eq!(market_envelope.fact.timestamp_ms(), 42);
                    assert_eq!(market_envelope.metadata.source_time_ms, 1_735_689_600_000);
                    seen_market = true;
                }
                SourceSignal::Eof => break,
                SourceSignal::Gap(_) => return Err("unexpected replay gap signal".into()),
                SourceSignal::Heartbeat(_) | SourceSignal::Watermark(_) => {}
            }
        }
        assert!(seen_market);
        drop(source);
        Arc::try_unwrap(reopened)
            .map_err(|_| "store still referenced")?
            .delete_database()?;
        assert!(!path.exists());
        Ok(())
    }

    fn capture_envelope(
        scope: OwnerScope,
        source_timestamp_ms: i64,
        ingest_sequence: i64,
        raw_frame: &[u8],
    ) -> PmEnvelope {
        let payload = serde_json::from_slice(raw_frame).unwrap_or(serde_json::Value::Null);
        PmEnvelope {
            schema_version: PM_ENVELOPE_VERSION,
            scope,
            venue_id: "polymarket".into(),
            config_hash: "historical-capture".into(),
            source_id: "capture:polymarket-market-ws".into(),
            connection_id: "capture-connection".into(),
            source_timestamp_ms,
            canonical_source_rank: 7,
            connection_epoch: 3,
            frame_sequence: ingest_sequence + 100,
            receipt_timestamp_ms: source_timestamp_ms + 25,
            ingest_sequence,
            raw_frame: raw_frame.to_vec(),
            normalized: json!({
                "stream_id": "market:btc-5m:up",
                "canonical_market_id": "btc-5m",
                "payload": payload,
            }),
        }
    }

    fn expected_capture_facts(market: MarketId) -> Vec<MarketEvent> {
        vec![
            MarketEvent::BookUpdate {
                market: market.clone(),
                outcome: Outcome::Up,
                bids: vec![(Decimal::new(49, 2), Decimal::from(2))],
                asks: vec![
                    (Decimal::new(51, 2), Decimal::from(3)),
                    (Decimal::new(52, 2), Decimal::from(4)),
                ],
                timestamp_ms: 1_000,
            },
            MarketEvent::BookUpdate {
                market: market.clone(),
                outcome: Outcome::Up,
                bids: vec![
                    (Decimal::new(50, 2), Decimal::from(4)),
                    (Decimal::new(49, 2), Decimal::from(2)),
                ],
                asks: vec![
                    (Decimal::new(515, 3), Decimal::new(15, 1)),
                    (Decimal::new(52, 2), Decimal::from(4)),
                ],
                timestamp_ms: 1_001,
            },
            MarketEvent::BestBidAsk {
                market: market.clone(),
                outcome: Outcome::Up,
                bid: Decimal::new(50, 2),
                ask: Decimal::new(515, 3),
                timestamp_ms: 1_002,
            },
            MarketEvent::LastTrade {
                market,
                outcome: Outcome::Up,
                price: Decimal::new(505, 3),
                side: Side::Sell,
                size: Decimal::new(125, 2),
                timestamp_ms: 1_003,
            },
        ]
    }

    fn assert_capture_replay(
        replayed: &[PmMarketEnvelope],
        expected_facts: &[MarketEvent],
        records: &[PmEnvelope],
    ) -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(replayed.len(), expected_facts.len());
        for (index, ((actual, expected_fact), record)) in
            replayed.iter().zip(expected_facts).zip(records).enumerate()
        {
            assert_eq!(&actual.fact, expected_fact);
            assert_eq!(actual.raw_frame, record.raw_frame);
            assert_eq!(
                actual.metadata,
                StreamMetadata {
                    schema_version: record.schema_version,
                    source_id: record.source_id.clone(),
                    source_time_ms: record.source_timestamp_ms,
                    canonical_source_rank: record.canonical_source_rank,
                    receipt_time_ms: record.receipt_timestamp_ms,
                    connection_id: record.connection_id.clone(),
                    connection_epoch: record.connection_epoch,
                    frame_sequence: record.frame_sequence,
                    ingest_sequence: u64::try_from(record.ingest_sequence)?,
                }
            );
            assert_eq!(actual.metadata.ingest_sequence, u64::try_from(index + 1)?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn historical_replay_matches_live_capture_projection_and_reports_gap()
    -> Result<(), Box<dyn std::error::Error>> {
        let raw_frames: Vec<&[u8]> = include_str!("../fixtures/historical_public_capture.jsonl")
            .lines()
            .map(str::as_bytes)
            .collect();
        assert_eq!(raw_frames.len(), 5);

        let tokens = tokens()?;
        let mut live_decoder = MarketFrameDecoder::new(tokens.clone(), Outcome::Up);
        let live_facts = raw_frames[..4]
            .iter()
            .map(|raw| {
                live_decoder.project_frame(raw)?.ok_or_else(|| {
                    pmkit_data::DataSourceError::ReplayGap {
                        message: "capture frame had no fact".into(),
                    }
                })
            })
            .collect::<Result<Vec<_>, pmkit_data::DataSourceError>>()?;
        assert!(matches!(
            live_decoder.decode(raw_frames[4]),
            Err(pmkit_data::DataSourceError::ReplayGap { .. })
        ));
        assert_eq!(live_facts, expected_capture_facts(MarketId::new("btc-5m")?));

        let (_dir, path) = database_path()?;
        let store = Arc::new(TursoTapeStore::open_local(&path).await?);
        let scope = scope()?;
        let records: Vec<_> = raw_frames
            .iter()
            .enumerate()
            .map(|(index, raw)| {
                Ok(capture_envelope(
                    scope.clone(),
                    1_735_689_600_000 + i64::try_from(index)?,
                    i64::try_from(index + 1)?,
                    raw,
                ))
            })
            .collect::<Result<_, Box<dyn std::error::Error>>>()?;
        for record in &records {
            store.store_envelope(record).await?;
        }

        let source = PolymarketHistoricalData::new(store.clone(), scope, tokens);
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let result = source
            .replay(
                ReplayQuery {
                    markets: vec![MarketId::new("btc-5m")?],
                    from: "2025-01-01T00:00:00Z".parse()?,
                    to: "2025-01-02T00:00:00Z".parse()?,
                    evidence: pmkit_run::EvidenceRequirement::CorroboratedOnly,
                    retrieval_wait: pmkit_run::RetrievalWait::ReturnPending,
                },
                tx,
            )
            .await;
        assert!(matches!(
            result,
            Err(pmkit_data::DataSourceError::ReplayGap { .. })
        ));

        let mut replayed = Vec::<PmMarketEnvelope>::new();
        let mut saw_eof = false;
        while let Some(signal) = rx.recv().await {
            match signal {
                SourceSignal::Data(envelope) => {
                    let SourceEnvelope::PmMarket(envelope) = *envelope else {
                        return Err("expected market envelope".into());
                    };
                    replayed.push(envelope);
                }
                SourceSignal::Eof => saw_eof = true,
                SourceSignal::Gap(_) => return Err("unexpected replay gap signal".into()),
                SourceSignal::Heartbeat(_) | SourceSignal::Watermark(_) => {}
            }
        }
        assert!(!saw_eof);
        assert_capture_replay(&replayed, &live_facts, &records)?;

        drop(source);
        Arc::try_unwrap(store)
            .map_err(|_| "store still referenced")?
            .delete_database()?;
        assert!(!path.exists());
        Ok(())
    }

    #[tokio::test]
    async fn historical_replay_fails_on_corrupt_record() -> Result<(), Box<dyn std::error::Error>> {
        // Given: a stored market envelope whose integrity digest is corrupted.
        let (_dir, path) = database_path()?;
        let store = std::sync::Arc::new(TursoTapeStore::open_local(&path).await?);
        let scope = scope()?;
        let market = envelope(
            scope.clone(),
            1_735_689_600_000,
            1,
            br#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[{"price":"0.49","size":"2"}],"asks":[{"price":"0.51","size":"3"}]}"#,
        );
        store.store_envelope(&market).await?;
        drop(store);

        // Corrupt the stored raw digest through the low-level database API.
        let database = turso::Builder::new_local(&path.to_string_lossy())
            .build()
            .await?;
        let connection = database.connect()?;
        connection
            .execute(
                "UPDATE pm_envelopes SET raw_sha256 = ?1 WHERE ingest_sequence = ?2",
                ("corrupt", market.ingest_sequence),
            )
            .await?;
        drop(connection);
        drop(database);

        // When: the historical source replays the corrupt window.
        let store = std::sync::Arc::new(TursoTapeStore::open_local(&path).await?);
        let source = PolymarketHistoricalData::new(store.clone(), scope, tokens()?);
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let result = source
            .replay(
                ReplayQuery {
                    markets: vec![MarketId::new("btc-5m")?],
                    from: "2025-01-01T00:00:00Z".parse()?,
                    to: "2025-12-31T23:59:59Z".parse()?,
                    evidence: pmkit_run::EvidenceRequirement::CorroboratedOnly,
                    retrieval_wait: pmkit_run::RetrievalWait::ReturnPending,
                },
                tx,
            )
            .await;

        // Then: replay returns a typed gap and the sink is closed.
        assert!(matches!(
            result,
            Err(pmkit_data::DataSourceError::ReplayGap { .. })
        ));
        assert!(rx.recv().await.is_none());
        drop(source);
        Arc::try_unwrap(store)
            .map_err(|_| "store still referenced")?
            .delete_database()?;
        assert!(!path.exists());
        Ok(())
    }
}
