//! Historical replay of normalized Polymarket RTDS reference envelopes.

use std::{num::NonZeroUsize, sync::Arc};

use async_trait::async_trait;
use pmkit_data::{DataSourceError, HistoricalDataSource, ReplayQuery, SourceSignal};
use pmkit_event::{PolymarketReferenceEnvelope, SourceEnvelope};
use pmkit_market::Asset;
use pmkit_store::{OwnerScope, PM_ENVELOPE_VERSION, PmEnvelope, ReplayItem, TapeStore};
use tokio::sync::mpsc::Sender;

use crate::POLYMARKET_RTDS_SOURCE_ID;

/// Historical Polymarket RTDS TWAP data backed by the durable tape.
#[derive(Clone)]
pub struct PolymarketRtdsHistorical {
    store: Arc<dyn TapeStore>,
    scope: OwnerScope,
    asset: Asset,
}

impl std::fmt::Debug for PolymarketRtdsHistorical {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PolymarketRtdsHistorical")
            .field("scope", &self.scope)
            .field("asset", &self.asset)
            .finish_non_exhaustive()
    }
}

impl PolymarketRtdsHistorical {
    /// Creates an RTDS historical source for one asset and owner scope.
    #[must_use]
    pub fn new(store: Arc<dyn TapeStore>, scope: OwnerScope, asset: Asset) -> Self {
        Self {
            store,
            scope,
            asset,
        }
    }

    async fn read_page(
        &self,
        cursor: Option<pmkit_store::ReplayCursor>,
        limit: NonZeroUsize,
    ) -> Result<pmkit_store::ReplayPage, DataSourceError> {
        self.store
            .read_envelopes(&self.scope, cursor, limit)
            .await
            .map_err(|error| gap(error.to_string()))
    }
}

#[async_trait]
impl HistoricalDataSource for PolymarketRtdsHistorical {
    async fn replay(
        &self,
        query: ReplayQuery,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        let from_ms = query.from.timestamp_millis();
        let to_ms = query.to.timestamp_millis();
        if from_ms >= to_ms {
            return Err(gap("replay window must have from < to"));
        }
        let limit = NonZeroUsize::new(256).ok_or_else(|| gap("invalid page limit"))?;
        let mut cursor = None;
        let mut seen_cursors = Vec::new();
        let mut previous_timestamp = None;
        let mut previous_coverage_timestamp = None;
        let mut before_anchor = None;
        let mut after_anchor = None;
        let mut references = Vec::new();

        // ponytail: buffer the requested interval to fail before evaluation; replace with a
        // store snapshot API only if multi-month replay memory becomes material.
        loop {
            let page = self.read_page(cursor.clone(), limit).await?;
            if page.items.is_empty() {
                break;
            }
            for item in page.items {
                let ReplayItem::Envelope(envelope) = item else {
                    return Err(gap("replay contains a gap"));
                };
                let Some(reference) = self.match_envelope(&envelope)? else {
                    continue;
                };
                let timestamp = reference.fact.timestamp_ms;
                if previous_timestamp.is_some_and(|previous| timestamp <= previous) {
                    return Err(gap("reference timestamps are not strictly increasing"));
                }
                previous_timestamp = Some(timestamp);
                if timestamp <= from_ms {
                    before_anchor = Some(timestamp);
                }
                if timestamp >= to_ms {
                    after_anchor.get_or_insert(timestamp);
                }
                if timestamp >= from_ms.saturating_sub(1_000)
                    && timestamp <= to_ms.saturating_add(1_000)
                {
                    if previous_coverage_timestamp
                        .is_some_and(|previous| timestamp - previous > 1_000)
                    {
                        return Err(gap("reference timestamp gap exceeds 1000 ms"));
                    }
                    previous_coverage_timestamp = Some(timestamp);
                }
                if timestamp >= from_ms && timestamp < to_ms {
                    references.push(reference);
                }
            }
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            if seen_cursors.iter().any(|seen| seen == &next_cursor)
                || cursor
                    .as_ref()
                    .is_some_and(|current| current == &next_cursor)
            {
                return Err(gap("replay cursor repeated or stalled"));
            }
            seen_cursors.push(next_cursor.clone());
            cursor = Some(next_cursor);
        }

        let before = before_anchor.ok_or_else(|| gap("missing start coverage anchor"))?;
        if from_ms - before > 1_000 {
            return Err(gap("start coverage anchor is too old"));
        }
        let after = after_anchor.ok_or_else(|| gap("missing end coverage anchor"))?;
        if after - to_ms > 1_000 {
            return Err(gap("end coverage anchor is too late"));
        }

        for reference in references {
            let timestamp = reference.fact.timestamp_ms;
            sink.send(SourceSignal::Data(Box::new(
                SourceEnvelope::PolymarketReference(reference),
            )))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
            sink.send(SourceSignal::Watermark(timestamp))
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
        }
        sink.send(SourceSignal::Watermark(to_ms))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        sink.send(SourceSignal::Eof)
            .await
            .map_err(|_| DataSourceError::SinkClosed)
    }
}

impl PolymarketRtdsHistorical {
    fn match_envelope(
        &self,
        envelope: &PmEnvelope,
    ) -> Result<Option<PolymarketReferenceEnvelope>, DataSourceError> {
        if envelope.source_id != POLYMARKET_RTDS_SOURCE_ID {
            return Ok(None);
        }
        if envelope.schema_version != PM_ENVELOPE_VERSION {
            return Err(gap("invalid durable envelope schema version"));
        }
        if envelope.venue_id != "polymarket" {
            return Err(gap("invalid durable envelope venue"));
        }
        if envelope.scope != self.scope {
            return Err(gap(
                "durable envelope scope does not match configured owner",
            ));
        }
        if envelope.config_hash != "runtime" {
            return Err(gap("invalid durable envelope config hash"));
        }
        if !envelope.raw_frame.is_empty() {
            return Err(gap("Polymarket RTDS raw frame must be empty"));
        }
        let reference = pmkit_tape::polymarket_reference_envelope_from_json(&envelope.normalized)
            .map_err(gap)?;
        if reference.fact.asset != self.asset {
            return Ok(None);
        }
        if reference.metadata.source_id != POLYMARKET_RTDS_SOURCE_ID
            || reference.metadata.source_time_ms != envelope.source_timestamp_ms
            || reference.metadata.canonical_source_rank != envelope.canonical_source_rank
            || reference.metadata.receipt_time_ms != envelope.receipt_timestamp_ms
            || reference.metadata.connection_id != envelope.connection_id
            || reference.metadata.connection_epoch != envelope.connection_epoch
            || reference.metadata.frame_sequence != envelope.frame_sequence
            || i64::try_from(reference.metadata.ingest_sequence).ok()
                != Some(envelope.ingest_sequence)
        {
            return Err(gap("durable metadata does not match normalized envelope"));
        }
        Ok(Some(reference))
    }
}

fn gap(message: impl Into<String>) -> DataSourceError {
    DataSourceError::ReplayGap {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::significant_drop_tightening)]

    use std::sync::Arc;

    use chrono::{TimeZone, Utc};
    use pmkit_core::{PortfolioId, RunId};
    use pmkit_data::{HistoricalDataSource, ReplayQuery, SourceSignal};
    use pmkit_event::{
        PolymarketReferenceEnvelope, PolymarketTwapEvent, SourceEnvelope, StreamMetadata,
    };
    use pmkit_market::Asset;
    use pmkit_run::{EvidenceRequirement, RetrievalWait};
    use pmkit_store::{OwnerScope, PM_ENVELOPE_VERSION, PmEnvelope, TapeStore, TursoTapeStore};
    use serde_json::json;

    use super::PolymarketRtdsHistorical;

    fn scope() -> Result<OwnerScope, Box<dyn std::error::Error>> {
        Ok(OwnerScope::new(
            PortfolioId::new("alice")?,
            RunId::new("rtds")?,
        ))
    }

    fn envelope(
        scope: OwnerScope,
        timestamp_ms: i64,
        ingest_sequence: i64,
    ) -> Result<PmEnvelope, Box<dyn std::error::Error>> {
        envelope_for_asset(scope, timestamp_ms, ingest_sequence, Asset::Btc)
    }

    fn envelope_for_asset(
        scope: OwnerScope,
        timestamp_ms: i64,
        ingest_sequence: i64,
        asset: Asset,
    ) -> Result<PmEnvelope, Box<dyn std::error::Error>> {
        let reference = PolymarketReferenceEnvelope {
            metadata: StreamMetadata {
                schema_version: 1,
                source_id: super::POLYMARKET_RTDS_SOURCE_ID.into(),
                source_time_ms: timestamp_ms,
                canonical_source_rank: 1,
                receipt_time_ms: timestamp_ms + 1,
                connection_id: "rtds".into(),
                connection_epoch: 0,
                frame_sequence: ingest_sequence,
                ingest_sequence: u64::try_from(ingest_sequence)?,
            },
            fact: PolymarketTwapEvent {
                asset,
                symbol: format!("{asset}/usd"),
                timestamp_ms,
                provider_timestamp_ms: timestamp_ms,
                value: 1.0,
                full_accuracy_value: timestamp_ms.to_string(),
                window_s: 60,
            },
        };
        Ok(PmEnvelope {
            schema_version: PM_ENVELOPE_VERSION,
            scope,
            venue_id: "polymarket".into(),
            config_hash: "runtime".into(),
            source_id: super::POLYMARKET_RTDS_SOURCE_ID.into(),
            connection_id: "rtds".into(),
            source_timestamp_ms: timestamp_ms,
            canonical_source_rank: 1,
            connection_epoch: 0,
            frame_sequence: ingest_sequence,
            receipt_timestamp_ms: timestamp_ms + 1,
            ingest_sequence,
            raw_frame: Vec::new(),
            normalized: pmkit_tape::polymarket_reference_envelope_json(&reference),
        })
    }

    fn query(from_ms: i64, to_ms: i64) -> Result<ReplayQuery, Box<dyn std::error::Error>> {
        Ok(ReplayQuery {
            markets: Vec::new(),
            from: Utc
                .timestamp_millis_opt(from_ms)
                .single()
                .ok_or("invalid from")?,
            to: Utc
                .timestamp_millis_opt(to_ms)
                .single()
                .ok_or("invalid to")?,
            evidence: EvidenceRequirement::CorroboratedOnly,
            retrieval_wait: RetrievalWait::ReturnPending,
        })
    }

    #[tokio::test]
    async fn replay_validates_then_emits_ordered_data_and_watermarks()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("rtds.db");
        let store = Arc::new(TursoTapeStore::open_local(&path).await?);
        let owner = scope()?;
        for (index, timestamp_ms) in (999_000..=1_000_300).enumerate() {
            store
                .store_envelope(&envelope(
                    owner.clone(),
                    timestamp_ms,
                    i64::try_from(index + 1)?,
                )?)
                .await?;
        }
        let source = PolymarketRtdsHistorical::new(store, owner, Asset::Btc);
        let (tx, mut rx) = tokio::sync::mpsc::channel(2_000);
        source.replay(query(1_000_000, 1_000_250)?, tx).await?;
        let mut timestamps = Vec::new();
        let mut watermarks = Vec::new();
        while let Some(signal) = rx.recv().await {
            match signal {
                SourceSignal::Data(envelope) => {
                    let SourceEnvelope::PolymarketReference(reference) = *envelope else {
                        return Err("wrong source".into());
                    };
                    timestamps.push(reference.fact.timestamp_ms);
                }
                SourceSignal::Watermark(timestamp) => watermarks.push(timestamp),
                SourceSignal::Eof => break,
            }
        }
        assert_eq!(timestamps.first(), Some(&1_000_000));
        assert_eq!(timestamps.last(), Some(&1_000_249));
        assert_eq!(watermarks.last(), Some(&1_000_250));
        assert!(watermarks.windows(2).all(|window| window[0] < window[1]));
        drop(source);
        Ok(())
    }

    #[tokio::test]
    async fn replay_skips_valid_eth_records_sharing_the_rtds_source_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("rtds.db");
        let store = Arc::new(TursoTapeStore::open_local(&path).await?);
        let owner = scope()?;
        for (index, (asset, timestamp_ms)) in [
            (Asset::Btc, 999_500),
            (Asset::Eth, 999_750),
            (Asset::Btc, 1_000_000),
            (Asset::Eth, 1_000_250),
            (Asset::Btc, 1_000_500),
        ]
        .into_iter()
        .enumerate()
        {
            store
                .store_envelope(&envelope_for_asset(
                    owner.clone(),
                    timestamp_ms,
                    i64::try_from(index + 1)?,
                    asset,
                )?)
                .await?;
        }
        let source = PolymarketRtdsHistorical::new(store, owner, Asset::Btc);
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        source.replay(query(1_000_000, 1_000_250)?, tx).await?;
        let mut timestamps = Vec::new();
        while let Some(signal) = rx.recv().await {
            match signal {
                SourceSignal::Data(envelope) => {
                    let SourceEnvelope::PolymarketReference(reference) = *envelope else {
                        return Err("wrong source".into());
                    };
                    assert_eq!(reference.fact.asset, Asset::Btc);
                    timestamps.push(reference.fact.timestamp_ms);
                }
                SourceSignal::Eof => break,
                SourceSignal::Watermark(_) => {}
            }
        }
        assert_eq!(timestamps, vec![1_000_000]);
        Ok(())
    }

    #[tokio::test]
    async fn replay_rejects_corruption_and_coverage_failures_before_data()
    -> Result<(), Box<dyn std::error::Error>> {
        for case in ["raw", "metadata", "config", "gap", "end"] {
            let directory = tempfile::tempdir()?;
            let store =
                Arc::new(TursoTapeStore::open_local(directory.path().join("rtds.db")).await?);
            let owner = scope()?;
            let rows = if case == "gap" {
                vec![999_500, 1_000_000, 1_002_001]
            } else if case == "end" {
                vec![999_500, 1_000_000]
            } else {
                vec![999_500, 1_000_000, 1_000_500]
            };
            for (index, timestamp_ms) in rows.into_iter().enumerate() {
                let mut record = envelope(owner.clone(), timestamp_ms, i64::try_from(index + 1)?)?;
                if timestamp_ms == 1_000_000 {
                    match case {
                        "raw" => record.raw_frame = vec![1],
                        "metadata" => record.normalized["connection_id"] = json!("other"),
                        "config" => record.config_hash = "corrupt".into(),
                        _ => {}
                    }
                }
                store.store_envelope(&record).await?;
            }
            let source = PolymarketRtdsHistorical::new(store, owner, Asset::Btc);
            let (tx, mut rx) = tokio::sync::mpsc::channel(16);
            assert!(
                source
                    .replay(query(1_000_000, 1_000_250)?, tx)
                    .await
                    .is_err(),
                "{case}"
            );
            while let Ok(signal) = rx.try_recv() {
                assert!(!matches!(signal, SourceSignal::Data(_)), "{case}");
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn replay_rejects_corrupt_normalized_representation_before_data()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("rtds.db");
        let store = Arc::new(TursoTapeStore::open_local(&path).await?);
        let owner = scope()?;
        for (index, timestamp_ms) in [999_500, 1_000_000, 1_000_500].into_iter().enumerate() {
            let mut record = envelope(owner.clone(), timestamp_ms, i64::try_from(index + 1)?)?;
            if timestamp_ms == 1_000_000 {
                record.normalized["representation"] = json!("wrong");
            }
            store.store_envelope(&record).await?;
        }
        let source = PolymarketRtdsHistorical::new(store, owner, Asset::Btc);
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        assert!(matches!(
            source.replay(query(1_000_000, 1_000_250)?, tx).await,
            Err(pmkit_data::DataSourceError::ReplayGap { .. })
        ));
        assert!(rx.try_recv().is_err());
        Ok(())
    }
    #[tokio::test]
    async fn matching_envelope_rejects_wrong_owner_scope() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(TursoTapeStore::open_local(directory.path().join("rtds.db")).await?);
        let owner = scope()?;
        let source = PolymarketRtdsHistorical::new(store, owner.clone(), Asset::Btc);
        let mut record = envelope(owner, 1_000_000, 1)?;
        record.scope = OwnerScope::new(PortfolioId::new("bob")?, RunId::new("rtds")?);
        assert!(source.match_envelope(&record).is_err());
        Ok(())
    }
}
