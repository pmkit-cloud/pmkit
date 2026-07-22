use std::sync::Arc;

use async_trait::async_trait;
use chrono::Duration;
use pmkit_event::{CexReferenceEnvelope, CexReferenceEvent, SourceEnvelope, StreamMetadata};
use pmkit_market::Asset;
use tokio::sync::mpsc::Sender;

use crate::{
    DataSourceError, HistoricalDataSource, ReplayQuery, SourceSignal, VerifiedBinanceArchiveCache,
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
        let mut date = query.from.date_naive();
        let end = query.to.date_naive();
        let mut sequence = 0_i64;
        while date < end {
            for fact in self.cache.replay(self.asset, date).await? {
                sequence = sequence.saturating_add(1);
                sink.send(SourceSignal::Data(Box::new(SourceEnvelope::CexReference(
                    CexReferenceEnvelope {
                        metadata: StreamMetadata {
                            schema_version: 1,
                            source_id: "binance-vision:aggTrades".to_owned(),
                            source_time_ms: reference_timestamp(&fact),
                            canonical_source_rank: 1,
                            receipt_time_ms: reference_timestamp(&fact),
                            connection_id: date.to_string(),
                            connection_epoch: 0,
                            frame_sequence: sequence,
                            ingest_sequence: u64::try_from(sequence).unwrap_or_default(),
                        },
                        fact,
                    },
                ))))
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            }
            date += Duration::days(1);
        }
        sink.send(SourceSignal::Watermark(query.to.timestamp_millis()))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        sink.send(SourceSignal::Eof)
            .await
            .map_err(|_| DataSourceError::SinkClosed)
    }
}

const fn reference_timestamp(fact: &CexReferenceEvent) -> i64 {
    match fact {
        CexReferenceEvent::Trade { timestamp_ms, .. }
        | CexReferenceEvent::BestBidOffer { timestamp_ms, .. } => *timestamp_ms,
    }
}
