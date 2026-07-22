use std::{num::NonZeroUsize, sync::Arc};

use async_trait::async_trait;
use pmkit_data::{DataSourceError, HistoricalDataSource, ReplayQuery, SourceSignal};
use pmkit_event::{PmMarketEnvelope, SourceEnvelope, StreamMetadata};
use pmkit_store::{OwnerScope, ReplayItem, TapeStore};
use tokio::sync::mpsc::Sender;

use crate::{MarketTokens, parse_market_frame};

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
                        let event = parse_market_frame(&envelope.raw_frame, &self.tokens)?;
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

    use pmkit_core::{MarketId, PortfolioId, RunId};
    use pmkit_data::{HistoricalDataSource, ReplayQuery, SourceSignal};
    use pmkit_event::SourceEnvelope;
    use pmkit_store::{OwnerScope, PmEnvelope, TapeStore, TursoTapeStore};
    use polymarket_client_sdk_v2::types::U256;
    use serde_json::json;

    use super::PolymarketHistoricalData;
    use crate::MarketTokens;

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
            schema_version: 1,
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
            br#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[{"price":"0.49","size":"2"}],"asks":[{"price":"0.51","size":"3"}]}"#,
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
                SourceSignal::Watermark(_) => {}
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
