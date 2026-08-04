use pmkit_core::{PortfolioId, RunId};
use serde_json::{Value, json};

use crate::{
    OwnerScope, PM_ENVELOPE_VERSION, PmEnvelope, ReplayGapInterval, StoreError, TapeStore,
    TursoTapeStore, decode_sealed_closed_day_manifest,
};

const SNAPSHOT_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SNAPSHOT_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn snapshot_envelope(
    scope: OwnerScope,
    snapshot: &str,
    canonical_source_rank: i64,
) -> Result<PmEnvelope, serde_json::Error> {
    Ok(PmEnvelope {
        schema_version: PM_ENVELOPE_VERSION,
        scope,
        venue_id: "polymarket".into(),
        config_hash: "fixture".into(),
        source_id: "pmkit-reconciler-v1".into(),
        connection_id: format!("reconciliation:{snapshot}"),
        source_timestamp_ms: 1_000,
        canonical_source_rank,
        connection_epoch: 0,
        frame_sequence: 0,
        receipt_timestamp_ms: 1_000,
        ingest_sequence: 1,
        raw_frame: serde_json::to_vec(&json!({"discovery_snapshot_sha256": snapshot}))?,
        normalized: json!({
            "canonical_market_id": "token-1",
            "portable_market": {
                "series_id": "btc-usd-5m",
                "asset": "BTC",
                "duration_seconds": 300,
                "market_id": "token-1",
                "condition_id": "condition-1",
                "outcome_tokens": [
                    {"outcome": "up", "token_id": "token-up"},
                    {"outcome": "down", "token_id": "token-down"}
                ],
                "open_time_ms": 0,
                "close_time_ms": 300_000
            },
            "payload": {"price": "0.42"}
        }),
    })
}

async fn export_with_gap(
    segment_snapshot: &str,
    gap_snapshot: Option<&str>,
) -> Result<Result<Value, StoreError>, Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let scope = OwnerScope::new(PortfolioId::new("snapshot-gap")?, RunId::new("fixture")?);
    let result = {
        let store = TursoTapeStore::open_local(directory.path().join("export.db")).await?;
        store
            .store_envelope(&snapshot_envelope(scope.clone(), segment_snapshot, 0)?)
            .await?;
        store
            .store_replay_gap(&ReplayGapInterval {
                scope: scope.clone(),
                partition: "token-1:0".into(),
                discovery_snapshot_sha256: gap_snapshot.map(str::to_owned),
                start_time_ms: 0,
                end_time_ms: Some(59_999),
                reason: "fixture_gap".into(),
            })
            .await?;
        let result = super::export_market_segments(
            &store,
            &scope,
            &decode_sealed_closed_day_manifest(json!({
                "version": 2,
                "day": "1970-01-01",
                "day_seal": "sealed"
            }))?,
        )
        .await;
        store.delete_database()?;
        result
    };
    Ok(result)
}

#[test]
fn portable_market_export_rolls_before_the_byte_limit() -> Result<(), Box<dyn std::error::Error>> {
    // Given: two deterministic rows that cannot share a bounded logical segment.
    let rows = vec![
        json!({"event_time_ms": 1_000, "row_ordinal": 0, "payload": {"price": "0.42"}}),
        json!({"event_time_ms": 1_001, "row_ordinal": 1, "payload": {"price": "0.43"}}),
    ];

    // When: the production row roller receives the bounded equivalent of 32 MiB.
    let first = super::roll_rows(&rows, 80)?;
    let second = super::roll_rows(&rows, 80)?;

    // Then: it rolls before the limit and addresses stable ordinal subparts.
    assert_eq!(first.len(), 2);
    assert_eq!(first, second);
    assert_eq!(
        super::segment_id(super::SegmentIdInput {
            source_manifest_sha256: "a",
            series_id: "btc-usd-5m",
            market_id: "market-01",
            discovery_snapshot_sha256: None,
            minute_start: 0,
            subpart_ordinal: 0
        }),
        super::segment_id(super::SegmentIdInput {
            source_manifest_sha256: "a",
            series_id: "btc-usd-5m",
            market_id: "market-01",
            discovery_snapshot_sha256: None,
            minute_start: 0,
            subpart_ordinal: 0
        })
    );
    assert_ne!(
        super::segment_id(super::SegmentIdInput {
            source_manifest_sha256: "a",
            series_id: "btc-usd-5m",
            market_id: "market-01",
            discovery_snapshot_sha256: None,
            minute_start: 0,
            subpart_ordinal: 0
        }),
        super::segment_id(super::SegmentIdInput {
            source_manifest_sha256: "a",
            series_id: "btc-usd-5m",
            market_id: "market-01",
            discovery_snapshot_sha256: None,
            minute_start: 0,
            subpart_ordinal: 1
        })
    );
    Ok(())
}

#[tokio::test]
async fn snapshot_gap_blocks_matching_segment() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a gap and segment from the same discovery snapshot and market minute.

    // When: the snapshot's segment is exported.
    let result = export_with_gap(SNAPSHOT_A, Some(SNAPSHOT_A)).await?;

    // Then: observed coverage remains blocked.
    assert!(matches!(result, Err(StoreError::Storage { .. })));
    Ok(())
}

#[tokio::test]
async fn snapshot_gap_does_not_block_another_snapshot_segment()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: snapshot B has the same market/minute shape as an A-scoped gap.

    // When: snapshot B's segment is exported.
    let export = export_with_gap(SNAPSHOT_B, Some(SNAPSHOT_A)).await??;

    // Then: only B's observed segment is authorized and declares its own snapshot.
    assert_eq!(
        export["segments"][0]["discovery_snapshot_sha256"],
        SNAPSHOT_B
    );
    Ok(())
}

#[tokio::test]
async fn legacy_gap_blocks_snapshot_segment() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a legacy gap has no snapshot identity for the same market minute.

    // When: a snapshot-scoped segment is exported.
    let result = export_with_gap(SNAPSHOT_B, None).await?;

    // Then: unknown legacy coverage remains conservatively blocked.
    assert!(matches!(result, Err(StoreError::Storage { .. })));
    Ok(())
}

#[tokio::test]
async fn snapshot_segments_do_not_merge() -> Result<(), Box<dyn std::error::Error>> {
    // Given: two otherwise-identical observed segments for separate snapshots.
    let directory = tempfile::tempdir()?;
    let scope = OwnerScope::new(
        PortfolioId::new("snapshot-grouping")?,
        RunId::new("fixture")?,
    );
    {
        let store = TursoTapeStore::open_local(directory.path().join("export.db")).await?;
        store
            .store_envelope(&snapshot_envelope(scope.clone(), SNAPSHOT_A, 0)?)
            .await?;
        store
            .store_envelope(&snapshot_envelope(scope.clone(), SNAPSHOT_B, 1)?)
            .await?;

        // When: the observed day is exported.
        let export = super::export_market_segments(
            &store,
            &scope,
            &decode_sealed_closed_day_manifest(json!({
                "version": 2,
                "day": "1970-01-01",
                "day_seal": "sealed"
            }))?,
        )
        .await?;

        // Then: each immutable snapshot retains a distinct segment identity.
        let segments = export["segments"].as_array().ok_or("segments")?;
        assert_eq!(segments.len(), 2);
        assert_ne!(segments[0]["segment_id"], segments[1]["segment_id"]);
        store.delete_database()?;
    }
    Ok(())
}
