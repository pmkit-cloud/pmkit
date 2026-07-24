use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use pmkit_event::{CexReferenceEnvelope, CexReferenceEvent, SourceEnvelope, StreamMetadata};
use pmkit_market::Asset;
use tokio::sync::mpsc::Sender;

use crate::{
    DataSourceError, HistoricalDataSource, ReplayQuery, SourceSignal, VerifiedBinanceArchiveCache,
    binance::{BINANCE_REFERENCE_SOURCE_ID, reference_trade_identity},
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
        for date in archive_dates(query.from, query.to) {
            for fact in self.cache.replay(self.asset, date).await? {
                let (frame_sequence, ingest_sequence) = reference_trade_identity(&fact)?;
                sink.send(SourceSignal::Data(Box::new(SourceEnvelope::CexReference(
                    CexReferenceEnvelope {
                        metadata: StreamMetadata {
                            schema_version: 1,
                            source_id: BINANCE_REFERENCE_SOURCE_ID.to_owned(),
                            source_time_ms: reference_timestamp(&fact),
                            canonical_source_rank: 1,
                            receipt_time_ms: reference_timestamp(&fact),
                            connection_id: date.to_string(),
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
        CexReferenceEvent::Trade { timestamp_ms, .. } => *timestamp_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::archive_dates;
    use chrono::{DateTime, Utc};

    fn timestamp(value: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
        value.parse()
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
}
