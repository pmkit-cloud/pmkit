//! Prints deterministic canonical output and a scoped gap for a deleted lane occurrence.

use std::collections::BTreeSet;

use pmkit_core::{PortfolioId, RunId};
use pmkit_store::{
    OwnerScope, RawMarketLaneRecord, ReconciliationRequest, reconcile_redundant_market_evidence,
};
use pmkit_tape::{SpoolChunk, SpoolFrame};
use serde_json::json;

const SNAPSHOT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn record(
    lane_id: &str,
    sequence: u64,
    receipt_time_ms: i64,
) -> Result<RawMarketLaneRecord, Box<dyn std::error::Error>> {
    Ok(RawMarketLaneRecord {
        chunk: SpoolChunk::new(lane_id, format!("{lane_id}-shard"), 0)?,
        frame: SpoolFrame::new(
            u64::from(lane_id == "lane-b"),
            sequence,
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = OwnerScope::new(
        PortfolioId::new("reconciliation")?,
        RunId::new("manual-fixture")?,
    );
    let result = reconcile_redundant_market_evidence(ReconciliationRequest {
        scope,
        expected_lanes: BTreeSet::from(["lane-a".to_owned(), "lane-b".to_owned()]),
        records: vec![
            record("lane-b", 9, 2_010)?,
            record("lane-a", 1, 2_001)?,
            record("lane-a", 2, 2_002)?,
        ],
        failures: Vec::new(),
    })?;
    for occurrence in result.occurrences {
        println!(
            "hash={} ordinal={} canonical_bytes={}",
            occurrence.content_sha256,
            occurrence.occurrence_ordinal,
            String::from_utf8(occurrence.canonical_bytes)?,
        );
    }
    for gap in result.gaps {
        println!(
            "gap={} {}..={:?} {}",
            gap.partition, gap.start_time_ms, gap.end_time_ms, gap.reason,
        );
    }
    Ok(())
}
