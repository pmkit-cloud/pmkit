//! Public portable market export contract tests.

use pmkit_core::{PortfolioId, RunId};
use pmkit_store::{
    OwnerScope, PM_ENVELOPE_VERSION, PmEnvelope, TapeStore, TursoTapeStore,
    decode_sealed_closed_day_manifest, export_market_segments,
};
use serde_json::json;

#[tokio::test]
#[allow(clippy::significant_drop_tightening)]
async fn portable_market_export_baseline_preserves_observed_segment()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: one observed normalized market record in a sealed UTC day.
    let directory = tempfile::tempdir()?;
    let scope = OwnerScope::new(PortfolioId::new("research")?, RunId::new("baseline")?);
    let store = TursoTapeStore::open_local(directory.path().join("export.db")).await?;
    store
        .store_envelope(&PmEnvelope {
            schema_version: PM_ENVELOPE_VERSION,
            scope: scope.clone(),
            venue_id: "venue".into(),
            config_hash: "config".into(),
            source_id: "observer".into(),
            connection_id: "connection".into(),
            source_timestamp_ms: 1_000,
            canonical_source_rank: 0,
            connection_epoch: 0,
            frame_sequence: 0,
            receipt_timestamp_ms: 1_001,
            ingest_sequence: 1,
            raw_frame: Vec::new(),
            normalized: json!({
                "canonical_market_id": "market-01",
                "portable_market": {
                    "series_id": "btc-usd-5m",
                    "asset": "BTC",
                    "duration_seconds": 300,
                    "market_id": "market-01",
                    "condition_id": "condition-01",
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
        .await?;

    // When: the existing public export is materialized.
    let export = export_market_segments(
        &store,
        &scope,
        &decode_sealed_closed_day_manifest(json!({
            "version": 2,
            "day": "1970-01-01",
            "day_seal": "sealed"
        }))?,
    )
    .await?;

    // Then: it is observed and declares one bounded, digest-addressed segment.
    assert_eq!(export["schema_version"], 1);
    assert_eq!(export["coverage"], "observed");
    assert_eq!(export["segments"][0]["market_id"], "market-01");
    assert_eq!(export["segments"][0]["from_ts_ms"], 1_000);
    assert_eq!(export["segments"][0]["to_ts_ms"], 1_000);
    assert_eq!(export["segments"][0]["rows"], 1);
    assert!(export["segments"][0]["sha256"].is_string());
    store.delete_database()?;
    Ok(())
}

#[tokio::test]
#[allow(clippy::significant_drop_tightening)]
async fn portable_market_export_emits_v1_market_identity() -> Result<(), Box<dyn std::error::Error>>
{
    // Given: an observed market row with portable recurring and concrete metadata.
    let directory = tempfile::tempdir()?;
    let scope = OwnerScope::new(PortfolioId::new("research")?, RunId::new("v1")?);
    let store = TursoTapeStore::open_local(directory.path().join("export.db")).await?;
    store
        .store_envelope(&PmEnvelope {
            schema_version: PM_ENVELOPE_VERSION,
            scope: scope.clone(),
            venue_id: "venue".into(),
            config_hash: "config".into(),
            source_id: "observer".into(),
            connection_id: "connection".into(),
            source_timestamp_ms: 1_000,
            canonical_source_rank: 0,
            connection_epoch: 0,
            frame_sequence: 0,
            receipt_timestamp_ms: 1_001,
            ingest_sequence: 1,
            raw_frame: Vec::new(),
            normalized: json!({
                "canonical_market_id": "market-01",
                "portable_market": {
                    "series_id": "btc-usd-5m",
                    "asset": "BTC",
                    "duration_seconds": 300,
                    "market_id": "market-01",
                    "condition_id": "condition-01",
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
        .await?;

    // When: the record is materialized into the public export.
    let export = export_market_segments(
        &store,
        &scope,
        &decode_sealed_closed_day_manifest(json!({
            "version": 2,
            "day": "1970-01-01",
            "day_seal": "sealed"
        }))?,
    )
    .await?;

    // Then: recurring, concrete, ordered, and UTC-minute metadata is declared.
    let segment = &export["segments"][0];
    assert_eq!(export["schema_version"], 1);
    assert_eq!(export["coverage"], "observed");
    assert!(export["source_manifest_sha256"].is_string());
    assert_eq!(segment["series_id"], "btc-usd-5m");
    assert_eq!(segment["asset"], "BTC");
    assert_eq!(segment["duration_seconds"], 300);
    assert_eq!(segment["market_id"], "market-01");
    assert_eq!(segment["condition_id"], "condition-01");
    assert_eq!(
        segment["outcome_tokens"],
        json!([
            {"outcome": "up", "token_id": "token-up"},
            {"outcome": "down", "token_id": "token-down"}
        ])
    );
    assert_eq!(segment["market_open_time_ms"], 0);
    assert_eq!(segment["market_close_time_ms"], 300_000);
    assert_eq!(segment["partition_start_time_ms"], 0);
    assert_eq!(segment["partition_end_time_ms"], 59_999);
    store.delete_database()?;
    Ok(())
}
