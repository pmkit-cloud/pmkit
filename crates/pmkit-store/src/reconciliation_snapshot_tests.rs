use std::collections::BTreeSet;

use pmkit_core::{PortfolioId, RunId};
use pmkit_tape::{SpoolChunk, SpoolFrame};
use serde_json::json;

use super::{
    OwnerScope, RawMarketLaneRecord, ReconciliationRequest, TapeStore, TursoTapeStore,
    reconcile_and_store_redundant_market_evidence, reconcile_redundant_market_evidence,
};

fn record(
    lane_id: &str,
    snapshot: &str,
) -> Result<RawMarketLaneRecord, Box<dyn std::error::Error>> {
    Ok(RawMarketLaneRecord {
        chunk: SpoolChunk::new(lane_id, format!("{lane_id}-shard"), 0)?,
        frame: SpoolFrame::new(
            0,
            0,
            1_000,
            snapshot,
            serde_json::to_vec(&json!({
                "market_id": "btc-5m",
                "event_time_ms": 1_000,
                "venue_id": "polymarket",
                "config_hash": "fixture",
                "normalized": {
                    "stream_id": "market:btc-5m:up",
                    "canonical_market_id": "btc-5m",
                    "payload": {"price": "0.49", "size": "2"}
                }
            }))?,
        ),
        market_id: "btc-5m".to_owned(),
    })
}

#[test]
fn never_matches_identical_events_across_discovery_snapshots()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: identical content captured by two lanes assigned different discovery snapshots.
    let result = reconcile_redundant_market_evidence(ReconciliationRequest {
        scope: OwnerScope::new(PortfolioId::new("reconciliation")?, RunId::new("snapshot")?),
        expected_lanes: BTreeSet::from(["lane-a".to_owned(), "lane-b".to_owned()]),
        records: vec![
            record("lane-a", &"a".repeat(64))?,
            record("lane-b", &"b".repeat(64))?,
        ],
        failures: Vec::new(),
    })?;

    // When: reconciliation groups canonical hashes by snapshot, market, and minute.

    // Then: no cross-snapshot occurrence becomes observed coverage.
    assert!(result.occurrences.is_empty());
    assert_eq!(result.gaps.len(), 2);
    assert!(result.gaps.iter().all(|gap| gap.partition == "btc-5m:0"));
    Ok(())
}

#[test]
fn rejects_non_hex_discovery_snapshot_digests() -> Result<(), Box<dyn std::error::Error>> {
    // Given: both lanes use a 64-byte digest containing a non-hex lowercase character.
    let invalid_snapshot = "g".repeat(64);
    let request = ReconciliationRequest {
        scope: OwnerScope::new(PortfolioId::new("reconciliation")?, RunId::new("digest")?),
        expected_lanes: BTreeSet::from(["lane-a".to_owned(), "lane-b".to_owned()]),
        records: vec![
            record("lane-a", &invalid_snapshot)?,
            record("lane-b", &invalid_snapshot)?,
        ],
        failures: Vec::new(),
    };

    // When: reconciliation parses the redundant evidence.
    let result = reconcile_redundant_market_evidence(request)?;

    // Then: it records uncertainty instead of creating observed coverage.
    assert!(result.occurrences.is_empty());
    assert_eq!(result.gaps.len(), 2);
    assert!(
        result
            .gaps
            .iter()
            .all(|gap| gap.reason == "malformed_record")
    );
    Ok(())
}

#[tokio::test]
async fn persists_same_partition_gaps_for_distinct_discovery_snapshots()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: each snapshot has only one of the two required redundant lanes.
    let scope = OwnerScope::new(
        PortfolioId::new("reconciliation")?,
        RunId::new("gap-identity")?,
    );
    let request = ReconciliationRequest {
        scope: scope.clone(),
        expected_lanes: BTreeSet::from(["lane-a".to_owned(), "lane-b".to_owned()]),
        records: vec![
            record("lane-a", &"a".repeat(64))?,
            record("lane-a", &"b".repeat(64))?,
        ],
        failures: Vec::new(),
    };
    let directory = tempfile::tempdir()?;
    {
        let store = TursoTapeStore::open_local(directory.path().join("gap-identity.db")).await?;

        // When: both snapshot-specific reconciliation gaps are persisted atomically.
        reconcile_and_store_redundant_market_evidence(&store, request).await?;
        let gaps = store.read_replay_gaps(&scope).await?;

        // Then: neither snapshot's gap overwrites the other identical market-minute gap.
        assert_eq!(gaps.len(), 2);
        assert!(gaps.iter().all(|gap| gap.partition == "btc-5m:0"));
        assert_eq!(
            gaps.iter()
                .map(|gap| gap.discovery_snapshot_sha256.clone())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([Some("a".repeat(64)), Some("b".repeat(64))])
        );
        store.delete_database()?;
    }
    Ok(())
}
