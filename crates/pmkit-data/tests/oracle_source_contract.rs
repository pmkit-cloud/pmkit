//! Contract tests for live oracle source signals.

use async_trait::async_trait;
use pmkit_data::{
    DataSourceError, GapRange, GapSequenceDomain, LiveOracleDataSource, SourceGap, SourceHeartbeat,
    SourceSignal,
};
use pmkit_event::StreamMetadata;
use pmkit_market::Asset;
use tokio::sync::mpsc::{self, Sender};

struct GapOracle;

#[async_trait]
impl LiveOracleDataSource for GapOracle {
    async fn subscribe_oracle(
        &self,
        asset: Asset,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        assert_eq!(asset, Asset::Btc);
        sink.send(SourceSignal::Gap(SourceGap {
            source_id: "polybolt:oracle".into(),
            detected_at_ms: 1_735_689_600_130,
            metadata: Some(StreamMetadata {
                schema_version: 1,
                source_id: "polybolt:oracle".into(),
                source_time_ms: 1_735_689_600_123,
                canonical_source_rank: 2,
                receipt_time_ms: 1_735_689_600_130,
                connection_id: "oracle-connection-3".into(),
                connection_epoch: 3,
                frame_sequence: 42,
                ingest_sequence: 42,
            }),
            raw_frame: br#"{"kind":"sequence_gap","sequence":42}"#.to_vec(),
            range: Some(GapRange {
                domain: GapSequenceDomain::Channel,
                first_missing: 17,
                last_missing: 19,
            }),
            unresolved_count: None,
            reason: "oracle sequence discontinuity".into(),
        }))
        .await
        .map_err(|_| DataSourceError::SinkClosed)
    }
}

#[test]
fn source_heartbeat_clones_exact_transport_bytes() -> Result<(), Box<dyn std::error::Error>> {
    let raw_frame = br#"{"type":"pong"}"#.to_vec();
    let signal = SourceSignal::Heartbeat(SourceHeartbeat {
        metadata: StreamMetadata {
            schema_version: 1,
            source_id: "polybolt:oracle".into(),
            source_time_ms: 1_735_689_600_123,
            canonical_source_rank: 2,
            receipt_time_ms: 1_735_689_600_130,
            connection_id: "oracle-connection-3".into(),
            connection_epoch: 3,
            frame_sequence: 42,
            ingest_sequence: 42,
        },
        raw_frame: raw_frame.clone(),
    });

    assert_eq!(signal.clone(), signal);
    let SourceSignal::Heartbeat(heartbeat) = signal else {
        return Err("expected a heartbeat signal".into());
    };
    assert_eq!(heartbeat.raw_frame, raw_frame);
    Ok(())
}

#[test]
fn unobserved_oracle_gap_has_no_fabricated_reporting_frame_or_bounds() {
    let gap = SourceGap {
        source_id: "polybolt:oracle".into(),
        detected_at_ms: 1_735_689_600_130,
        metadata: None,
        raw_frame: Vec::new(),
        range: None,
        unresolved_count: None,
        reason: "local oracle silence detected".into(),
    };

    assert!(gap.metadata.is_none());
    assert!(gap.raw_frame.is_empty());
    assert!(gap.range.is_none());
    assert!(gap.unresolved_count.is_none());
}

#[tokio::test]
async fn oracle_source_emits_replayable_sequence_gap() -> Result<(), Box<dyn std::error::Error>> {
    let (sink, mut signals) = mpsc::channel(1);
    GapOracle.subscribe_oracle(Asset::Btc, sink).await?;

    let Some(SourceSignal::Gap(gap)) = signals.recv().await else {
        return Err("expected an explicit source gap signal".into());
    };
    assert_eq!(gap.source_id, "polybolt:oracle");
    let Some(metadata) = gap.metadata.as_ref() else {
        return Err("expected a real reporting frame".into());
    };
    assert_eq!(metadata.source_id, gap.source_id);
    assert_eq!(gap.detected_at_ms, metadata.receipt_time_ms);
    assert_eq!(metadata.source_time_ms, 1_735_689_600_123);
    assert_eq!(metadata.receipt_time_ms, 1_735_689_600_130);
    assert_eq!(metadata.connection_id, "oracle-connection-3");
    assert_eq!(metadata.connection_epoch, 3);
    assert_eq!(metadata.frame_sequence, 42);
    assert_eq!(metadata.ingest_sequence, 42);
    assert_eq!(gap.raw_frame, br#"{"kind":"sequence_gap","sequence":42}"#);
    assert_eq!(
        gap.range,
        Some(GapRange {
            domain: GapSequenceDomain::Channel,
            first_missing: 17,
            last_missing: 19,
        })
    );
    assert_eq!(gap.unresolved_count, None);
    assert_eq!(gap.reason, "oracle sequence discontinuity");
    Ok(())
}
