use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use pmkit_event::{CexReferenceEnvelope, CexReferenceEvent, SourceEnvelope, StreamMetadata};
use pmkit_market::Asset;
use tokio::sync::mpsc::Sender;

use crate::{
    DataSourceError, HistoricalDataSource, ReplayQuery, SourceSignal, VerifiedBinanceArchiveCache,
    binance::{BINANCE_VISION_SOURCE_ID, reference_trade_identity},
};

/// A Binance Vision historical source for one reference asset.
#[derive(Debug, Clone)]
pub struct BinanceVisionHistory {
    cache: Arc<VerifiedBinanceArchiveCache>,
    asset: Asset,
}

impl BinanceVisionHistory {
    /// Creates a source backed by verified Binance Vision archives.
    #[must_use]
    pub const fn new(cache: Arc<VerifiedBinanceArchiveCache>, asset: Asset) -> Self {
        Self { cache, asset }
    }
}

#[async_trait]
impl HistoricalDataSource for BinanceVisionHistory {
    async fn replay(
        &self,
        query: ReplayQuery,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        if query.from >= query.to {
            return Err(DataSourceError::ReplayGap {
                message: "Binance history query window must be non-empty and ordered".into(),
            });
        }
        for date in archive_dates(query.from, query.to) {
            for (fact, raw_frame) in self.cache.replay_rows(self.asset, date).await? {
                let source_time_ms = reference_timestamp(&fact);
                if source_time_ms < query.from.timestamp_millis()
                    || source_time_ms >= query.to.timestamp_millis()
                {
                    continue;
                }
                let (frame_sequence, ingest_sequence) = reference_trade_identity(&fact)?;
                sink.send(SourceSignal::Data(Box::new(SourceEnvelope::CexReference(
                    CexReferenceEnvelope {
                        metadata: StreamMetadata {
                            schema_version: 1,
                            source_id: BINANCE_VISION_SOURCE_ID.to_owned(),
                            source_time_ms,
                            canonical_source_rank: 1,
                            // Historical replay clock, not a known local/network receipt time.
                            receipt_time_ms: source_time_ms,
                            connection_id: date.to_string(),
                            connection_epoch: 0,
                            frame_sequence,
                            ingest_sequence,
                        },
                        raw_frame,
                        fact,
                    },
                ))))
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            }
        }
        sink.send(SourceSignal::Watermark(query.to.timestamp_millis()))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        sink.send(SourceSignal::Eof)
            .await
            .map_err(|_| DataSourceError::SinkClosed)
    }
}

fn archive_dates(from: DateTime<Utc>, to: DateTime<Utc>) -> Vec<NaiveDate> {
    let mut date = from.date_naive();
    let end = to.date_naive();
    let mut dates = Vec::new();
    while date < end {
        dates.push(date);
        date += Duration::days(1);
    }
    if date == end && from < to && to.time() != chrono::NaiveTime::default() {
        dates.push(date);
    }
    dates
}

const fn reference_timestamp(fact: &CexReferenceEvent) -> i64 {
    match fact {
        CexReferenceEvent::Trade { timestamp_ms, .. }
        | CexReferenceEvent::Twap60sSnapshot { timestamp_ms, .. }
        | CexReferenceEvent::Twap60sUpdate { timestamp_ms, .. } => *timestamp_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::{BinanceVisionHistory, archive_dates};
    use crate::{
        DataSourceError, HistoricalDataSource, ReplayQuery, SourceSignal,
        binance::BINANCE_VISION_SOURCE_ID,
        binance_cache_test_support::{TempRoot, archive, cache, date, write_cached},
    };
    use chrono::{DateTime, Utc};
    use pmkit_event::SourceEnvelope;
    use pmkit_market::Asset;
    use pmkit_run::{EvidenceRequirement, RetrievalWait};
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn timestamp(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
        value.parse()
    }

    fn trade_row(aggregate_trade_id: u64, timestamp_ms: i64) -> String {
        format!(
            "{aggregate_trade_id},1,1,{aggregate_trade_id},{aggregate_trade_id},{timestamp_ms},true,true\n"
        )
    }

    #[test]
    fn archive_dates_covers_partial_same_day() -> Result<(), chrono::ParseError> {
        assert_eq!(
            archive_dates(
                timestamp("2026-01-01T12:00:00Z")?,
                timestamp("2026-01-01T18:00:00Z")?
            ),
            vec![timestamp("2026-01-01T00:00:00Z")?.date_naive()]
        );
        Ok(())
    }

    #[test]
    fn archive_dates_uses_exclusive_end_day() -> Result<(), chrono::ParseError> {
        assert_eq!(
            archive_dates(
                timestamp("2026-01-01T00:00:00Z")?,
                timestamp("2026-01-02T00:00:00Z")?
            ),
            vec![timestamp("2026-01-01T00:00:00Z")?.date_naive()]
        );
        Ok(())
    }

    #[test]
    fn archive_dates_handles_multi_day_and_empty_windows() -> Result<(), chrono::ParseError> {
        assert_eq!(
            archive_dates(
                timestamp("2026-01-01T00:00:00Z")?,
                timestamp("2026-01-03T00:00:00Z")?
            )
            .len(),
            2
        );
        assert!(
            archive_dates(
                timestamp("2026-01-02T00:00:00Z")?,
                timestamp("2026-01-01T00:00:00Z")?
            )
            .is_empty()
        );
        assert!(
            archive_dates(
                timestamp("2026-01-01T00:00:00Z")?,
                timestamp("2026-01-01T00:00:00Z")?
            )
            .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    async fn replay_emits_only_facts_in_the_half_open_query_window()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TempRoot::new()?;
        let from = timestamp("2026-01-01T12:00:00Z")?;
        let to = timestamp("2026-01-02T12:00:00Z")?;
        let from_ms = from.timestamp_millis();
        let to_ms = to.timestamp_millis();
        let before_from = timestamp("2026-01-01T11:59:59.999Z")?.timestamp_millis();
        let inside_first_day = timestamp("2026-01-01T23:59:59.999Z")?.timestamp_millis();
        let inside_second_day = timestamp("2026-01-02T11:59:59.999Z")?.timestamp_millis();
        let after_to = timestamp("2026-01-02T12:00:00.001Z")?.timestamp_millis();
        let before_row = trade_row(1, before_from);
        let microsecond_row = format!("2,1,1,2,2,{},true,true\r\n", from_ms * 1_000);
        let inside_first_row = trade_row(3, inside_first_day);
        let inside_second_row = trade_row(4, inside_second_day);
        let at_to_row = trade_row(5, to_ms);
        let after_to_row = trade_row(6, after_to);
        let first_day_rows = format!("{before_row}{microsecond_row}{inside_first_row}");
        let second_day_rows = format!("{inside_second_row}{at_to_row}{after_to_row}");
        write_cached(root.path(), date(1)?, &archive(&first_day_rows)?)?;
        write_cached(root.path(), date(2)?, &archive(&second_day_rows)?)?;
        let cache = Arc::new(cache(&root, 1_000_000, "http://127.0.0.1:1"));
        let source = BinanceVisionHistory::new(cache, Asset::Btc);
        let (tx, mut rx) = mpsc::channel(8);

        source
            .replay(
                ReplayQuery {
                    markets: Vec::new(),
                    from,
                    to,
                    evidence: EvidenceRequirement::CorroboratedOnly,
                    retrieval_wait: RetrievalWait::ReturnPending,
                },
                tx,
            )
            .await?;

        let mut timestamps = Vec::new();
        let mut raw_frames = Vec::new();
        let mut watermark = None;
        let mut saw_eof = false;
        while let Some(signal) = rx.recv().await {
            match signal {
                SourceSignal::Data(envelope) => {
                    assert!(watermark.is_none(), "data followed the watermark");
                    let SourceEnvelope::CexReference(envelope) = *envelope else {
                        return Err("expected a CEX reference envelope".into());
                    };
                    assert_eq!(envelope.metadata.source_id, BINANCE_VISION_SOURCE_ID);
                    assert_eq!(
                        envelope.metadata.receipt_time_ms,
                        envelope.metadata.source_time_ms
                    );
                    assert_eq!(
                        super::reference_timestamp(&envelope.fact),
                        envelope.metadata.source_time_ms
                    );
                    timestamps.push(envelope.metadata.source_time_ms);
                    raw_frames.push(envelope.raw_frame);
                }
                SourceSignal::Watermark(timestamp_ms) => {
                    assert!(watermark.replace(timestamp_ms).is_none());
                }
                SourceSignal::Eof => {
                    assert_eq!(watermark, Some(to_ms));
                    saw_eof = true;
                    break;
                }
                SourceSignal::Gap(_) | SourceSignal::Heartbeat(_) => {
                    return Err("unexpected control signal during history replay".into());
                }
            }
        }

        assert_eq!(
            timestamps,
            vec![from_ms, inside_first_day, inside_second_day]
        );
        assert_eq!(
            raw_frames,
            vec![
                microsecond_row.as_bytes().to_vec(),
                inside_first_row.as_bytes().to_vec(),
                inside_second_row.as_bytes().to_vec(),
            ]
        );
        assert!(saw_eof);
        assert!(rx.recv().await.is_none());
        let root_path = root.path().to_path_buf();
        drop(root);
        assert!(!root_path.exists());
        Ok(())
    }

    #[tokio::test]
    async fn replay_rejects_empty_query_bounds() -> Result<(), Box<dyn std::error::Error>> {
        let root = TempRoot::new()?;
        let source = BinanceVisionHistory::new(
            Arc::new(cache(&root, 1_000_000, "http://127.0.0.1:1")),
            Asset::Btc,
        );
        let at = timestamp("2026-01-01T12:00:00Z")?;
        let (tx, mut rx) = mpsc::channel(2);

        let result = source
            .replay(
                ReplayQuery {
                    markets: Vec::new(),
                    from: at,
                    to: at,
                    evidence: EvidenceRequirement::CorroboratedOnly,
                    retrieval_wait: RetrievalWait::ReturnPending,
                },
                tx,
            )
            .await;

        assert!(matches!(result, Err(DataSourceError::ReplayGap { .. })));
        assert!(rx.recv().await.is_none());
        let root_path = root.path().to_path_buf();
        drop(root);
        assert!(!root_path.exists());
        Ok(())
    }
}
