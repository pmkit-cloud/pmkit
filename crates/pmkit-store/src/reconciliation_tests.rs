use std::collections::BTreeSet;

use pmkit_core::{PortfolioId, RunId};
use pmkit_tape::{SpoolChunk, SpoolFrame};
use serde_json::json;

use super::{
    OwnerScope, RawMarketLaneRecord, ReconciliationFailure, ReconciliationFailureReason,
    ReconciliationRequest, TapeStore, TursoTapeStore, decode_sealed_closed_day_manifest,
    export_market_segments, reconcile_and_store_redundant_market_evidence,
    reconcile_redundant_market_evidence,
};

const SNAPSHOT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn scope() -> Result<OwnerScope, Box<dyn std::error::Error>> {
    Ok(OwnerScope::new(
        PortfolioId::new("reconciliation")?,
        RunId::new("fixture")?,
    ))
}

fn record(
    lane_id: &str,
    frame_sequence: u64,
    receipt_time_ms: i64,
) -> Result<RawMarketLaneRecord, Box<dyn std::error::Error>> {
    Ok(RawMarketLaneRecord {
        chunk: SpoolChunk::new(lane_id, format!("{lane_id}-shard"), 0)?,
        frame: SpoolFrame::new(
            u64::from(lane_id == "lane-b"),
            frame_sequence,
            receipt_time_ms,
            SNAPSHOT,
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

fn relabel_market(
    record: &mut RawMarketLaneRecord,
    market_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let raw = String::from_utf8(record.frame.raw_bytes.clone())?;
    record.market_id = market_id.to_owned();
    record.frame.raw_bytes = raw.replace("btc-5m", market_id).into_bytes();
    Ok(())
}

#[test]
fn reconciles_reordered_duplicate_occurrences_without_collector_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: two lanes receive two legitimate identical events in different arrival orders.
    let mut reconnected = record("lane-a", 0, 2_002)?;
    reconnected.frame.connection_epoch = 1;
    let request = ReconciliationRequest {
        scope: scope()?,
        expected_lanes: BTreeSet::from(["lane-a".to_owned(), "lane-b".to_owned()]),
        records: vec![
            record("lane-b", 99, 2_010)?,
            record("lane-a", 1, 2_001)?,
            record("lane-b", 3, 2_003)?,
            reconnected,
        ],
        failures: Vec::new(),
    };

    // When: redundant raw evidence is reconciled twice from the same fixtures.
    let first = reconcile_redundant_market_evidence(request.clone())?;
    let second = reconcile_redundant_market_evidence(request)?;

    // Then: both real occurrences survive once with byte-identical canonical output.
    assert_eq!(first, second);
    assert!(first.gaps.is_empty());
    assert_eq!(first.occurrences.len(), 2);
    assert_eq!(
        first
            .occurrences
            .iter()
            .map(|occurrence| occurrence.occurrence_ordinal)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(
        first.occurrences[0].content_sha256,
        first.occurrences[1].content_sha256
    );
    Ok(())
}

#[tokio::test]
async fn persists_a_partition_gap_without_unioning_a_divergent_occurrence()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: lane A has one extra identical event for a market minute.
    let request = ReconciliationRequest {
        scope: scope()?,
        expected_lanes: BTreeSet::from(["lane-a".to_owned(), "lane-b".to_owned()]),
        records: vec![
            record("lane-a", 1, 2_001)?,
            record("lane-a", 2, 2_002)?,
            record("lane-b", 1, 2_101)?,
        ],
        failures: Vec::new(),
    };
    let directory = tempfile::tempdir()?;
    {
        let store = TursoTapeStore::open_local(directory.path().join("reconciliation.db")).await?;

        // When: the same divergent fixture is reconciled and persisted twice.
        let first = reconcile_and_store_redundant_market_evidence(&store, request.clone()).await?;
        let second = reconcile_and_store_redundant_market_evidence(&store, request).await?;

        // Then: only the verified intersection persists and its scoped gap blocks export.
        assert_eq!(first, second);
        assert_eq!(first.occurrences.len(), 1);
        assert_eq!(first.gaps.len(), 1);
        assert_eq!(first.gaps[0].partition, "btc-5m:0");
        assert_eq!(
            store
                .read_envelopes(
                    &scope()?,
                    None,
                    std::num::NonZeroUsize::new(8).ok_or("page size")?,
                )
                .await?
                .items
                .len(),
            1
        );
        assert_eq!(store.read_replay_gaps(&scope()?).await?.len(), 1);
        assert!(
            export_market_segments(
                &store,
                &scope()?,
                &decode_sealed_closed_day_manifest(json!({
                    "version": 2,
                    "day": "1970-01-01",
                    "day_seal": "sealed",
                }))?,
            )
            .await
            .is_err()
        );
        store.delete_database()?;
    }
    Ok(())
}

#[test]
fn scopes_malformed_outage_and_checkpoint_gaps_to_their_own_partitions()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: malformed BTC evidence, valid ETH evidence, and two known lane failures.
    let mut malformed = record("lane-a", 1, 2_001)?;
    malformed.frame.raw_bytes = b"not-json".to_vec();
    let mut eth_a = record("lane-a", 2, 2_002)?;
    let mut eth_b = record("lane-b", 2, 2_102)?;
    relabel_market(&mut eth_a, "eth-5m")?;
    relabel_market(&mut eth_b, "eth-5m")?;
    let result = reconcile_redundant_market_evidence(ReconciliationRequest {
        scope: scope()?,
        expected_lanes: BTreeSet::from(["lane-a".to_owned(), "lane-b".to_owned()]),
        records: vec![malformed, record("lane-b", 1, 2_101)?, eth_a, eth_b],
        failures: vec![
            ReconciliationFailure {
                lane_id: "lane-b".to_owned(),
                discovery_snapshot_sha256: SNAPSHOT.to_owned(),
                market_id: "ada-5m".to_owned(),
                minute_start_ms: 0,
                reason: ReconciliationFailureReason::LaneOutage,
            },
            ReconciliationFailure {
                lane_id: "lane-a".to_owned(),
                discovery_snapshot_sha256: SNAPSHOT.to_owned(),
                market_id: "sol-5m".to_owned(),
                minute_start_ms: 0,
                reason: ReconciliationFailureReason::CheckpointFailure,
            },
        ],
    })?;

    // When: the processor matches only complete, parseable redundant evidence.

    // Then: valid ETH survives while all uncertainty remains outside its partition.
    assert_eq!(result.occurrences.len(), 1);
    assert_eq!(
        result.occurrences[0].envelope.canonical_market_id(),
        "eth-5m"
    );
    assert!(result.gaps.iter().all(|gap| gap.partition != "eth-5m:0"));
    assert!(
        result
            .gaps
            .iter()
            .any(|gap| gap.partition == "btc-5m:0" && gap.reason == "malformed_record")
    );
    assert!(
        result
            .gaps
            .iter()
            .any(|gap| gap.partition == "ada-5m:0" && gap.reason == "lane_outage")
    );
    assert!(
        result
            .gaps
            .iter()
            .any(|gap| gap.partition == "sol-5m:0" && gap.reason == "checkpoint_failure")
    );
    Ok(())
}
