use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

use crate::{OwnerScope, PM_ENVELOPE_VERSION, PmEnvelope, ReplayGapInterval, TapeStore};

use super::types::{
    CanonicalOccurrence, ParsedRecord, Partition, RawMarketLaneRecord, ReconciliationError,
    ReconciliationFailure, ReconciliationRequest, ReconciliationResult, UTC_MINUTE_MS,
    WireMarketRecord,
};

const RECONCILER_SOURCE_ID: &str = "pmkit-reconciler-v1";

/// Reconciles raw evidence without arrival-order or collector-metadata dependence.
///
/// # Errors
///
/// Returns [`ReconciliationError`] when the configured lanes are invalid or an
/// occurrence ordinal cannot be represented by the durable envelope format.
pub fn reconcile_redundant_market_evidence(
    request: ReconciliationRequest,
) -> Result<ReconciliationResult, ReconciliationError> {
    if request.expected_lanes.len() != 2 {
        return Err(ReconciliationError::ExpectedTwoLanes);
    }
    let mut gaps = request
        .failures
        .iter()
        .map(|failure| gap_from_failure(&request.scope, &request.expected_lanes, failure))
        .collect::<Result<Vec<_>, _>>()?;
    let mut groups =
        BTreeMap::<Partition, BTreeMap<String, BTreeMap<String, Vec<ParsedRecord>>>>::new();
    for raw in request.records {
        if !request.expected_lanes.contains(raw.chunk.replica_id()) {
            return Err(ReconciliationError::UnexpectedLane {
                lane_id: raw.chunk.replica_id().to_owned(),
            });
        }
        match parse_record(&raw) {
            Some(record) => groups
                .entry(record.partition.clone())
                .or_default()
                .entry(record.content_sha256.clone())
                .or_default()
                .entry(record.lane_id.clone())
                .or_default()
                .push(record),
            None => gaps.push(malformed_gap(&request.scope, &raw)),
        }
    }
    let mut occurrences = Vec::new();
    let mut stream_ranks = BTreeMap::<(i64, String, String), i64>::new();
    for (partition, hashes) in groups {
        for (content_sha256, mut lanes) in hashes {
            for lane_id in &request.expected_lanes {
                lanes.entry(lane_id.clone()).or_default();
            }
            let matching = lanes.values().map(Vec::len).min().unwrap_or_default();
            let total = lanes.values().map(Vec::len).max().unwrap_or_default();
            let template = lanes
                .values()
                .find_map(|records| records.first())
                .ok_or(ReconciliationError::OrdinalOutOfRange)?;
            for ordinal in 0..matching {
                let ordinal =
                    u64::try_from(ordinal).map_err(|_| ReconciliationError::OrdinalOutOfRange)?;
                let rank = next_rank(&mut stream_ranks, template)?;
                occurrences.push(CanonicalOccurrence {
                    content_sha256: content_sha256.clone(),
                    occurrence_ordinal: ordinal,
                    canonical_bytes: template.canonical_bytes.clone(),
                    envelope: canonical_envelope(&request.scope, template, rank),
                });
            }
            if matching != total {
                gaps.push(disagreement_gap(&request.scope, &partition, &lanes));
            }
        }
    }
    Ok(ReconciliationResult { occurrences, gaps })
}

/// Persists a complete reconciliation atomically through the existing store seam.
///
/// # Errors
///
/// Returns [`ReconciliationError`] when reconciliation is invalid or the tape
/// store cannot atomically persist the canonical envelopes and replay gaps.
pub async fn reconcile_and_store_redundant_market_evidence(
    store: &dyn TapeStore,
    request: ReconciliationRequest,
) -> Result<ReconciliationResult, ReconciliationError> {
    let result = reconcile_redundant_market_evidence(request)?;
    let envelopes = result
        .occurrences
        .iter()
        .map(|occurrence| occurrence.envelope.clone())
        .collect::<Vec<_>>();
    store
        .store_public_tape_import(&result.gaps, &[], &envelopes)
        .await?;
    Ok(result)
}

fn parse_record(raw: &RawMarketLaneRecord) -> Option<ParsedRecord> {
    let wire: WireMarketRecord = serde_json::from_slice(&raw.frame.raw_bytes).ok()?;
    let minute_start_ms = wire.event_time_ms.div_euclid(UTC_MINUTE_MS) * UTC_MINUTE_MS;
    if wire.market_id != raw.market_id
        || minute_start_ms != raw.chunk.minute_start_ms()
        || wire.market_id.is_empty()
        || wire.venue_id.is_empty()
        || wire.config_hash.is_empty()
        || !valid_digest(&raw.frame.discovery_snapshot_sha256)
        || wire
            .normalized
            .get("canonical_market_id")
            .and_then(Value::as_str)
            != Some(&wire.market_id)
        || wire
            .normalized
            .get("stream_id")
            .and_then(Value::as_str)
            .is_none()
    {
        return None;
    }
    let partition = Partition {
        snapshot: raw.frame.discovery_snapshot_sha256.clone(),
        market_id: wire.market_id.clone(),
        minute_start_ms,
    };
    let canonical_bytes = serde_json::to_vec(&json!({
        "discovery_snapshot_sha256": partition.snapshot,
        "market_id": partition.market_id,
        "minute_start_ms": partition.minute_start_ms,
        "event_time_ms": wire.event_time_ms,
        "venue_id": wire.venue_id,
        "config_hash": wire.config_hash,
        "normalized": wire.normalized,
    }))
    .ok()?;
    Some(ParsedRecord {
        lane_id: raw.chunk.replica_id().to_owned(),
        partition,
        event_time_ms: wire.event_time_ms,
        venue_id: wire.venue_id,
        config_hash: wire.config_hash,
        normalized: wire.normalized,
        content_sha256: crate::integrity::sha256_hex(&canonical_bytes),
        canonical_bytes,
    })
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
}

fn next_rank(
    ranks: &mut BTreeMap<(i64, String, String), i64>,
    record: &ParsedRecord,
) -> Result<i64, ReconciliationError> {
    let stream = record.normalized["stream_id"]
        .as_str()
        .ok_or(ReconciliationError::OrdinalOutOfRange)?
        .to_owned();
    let rank = ranks
        .entry((
            record.event_time_ms,
            record.partition.market_id.clone(),
            stream,
        ))
        .or_default();
    let current = *rank;
    *rank = current
        .checked_add(1)
        .ok_or(ReconciliationError::OrdinalOutOfRange)?;
    Ok(current)
}

fn canonical_envelope(scope: &OwnerScope, record: &ParsedRecord, rank: i64) -> PmEnvelope {
    PmEnvelope {
        schema_version: PM_ENVELOPE_VERSION,
        scope: scope.clone(),
        venue_id: record.venue_id.clone(),
        config_hash: record.config_hash.clone(),
        source_id: RECONCILER_SOURCE_ID.to_owned(),
        connection_id: format!("reconciliation:{}", record.partition.snapshot),
        source_timestamp_ms: record.event_time_ms,
        canonical_source_rank: rank,
        connection_epoch: 0,
        frame_sequence: rank,
        receipt_timestamp_ms: record.event_time_ms,
        ingest_sequence: rank,
        raw_frame: record.canonical_bytes.clone(),
        normalized: record.normalized.clone(),
    }
}

fn gap_from_failure(
    scope: &OwnerScope,
    expected_lanes: &BTreeSet<String>,
    failure: &ReconciliationFailure,
) -> Result<ReplayGapInterval, ReconciliationError> {
    if !expected_lanes.contains(&failure.lane_id) {
        return Err(ReconciliationError::UnexpectedLane {
            lane_id: failure.lane_id.clone(),
        });
    }
    let partition = Partition {
        snapshot: failure.discovery_snapshot_sha256.clone(),
        market_id: failure.market_id.clone(),
        minute_start_ms: failure.minute_start_ms,
    };
    Ok(partition_gap(scope, &partition, failure.reason.label()))
}

fn malformed_gap(scope: &OwnerScope, raw: &RawMarketLaneRecord) -> ReplayGapInterval {
    let partition = Partition {
        snapshot: raw.frame.discovery_snapshot_sha256.clone(),
        market_id: raw.market_id.clone(),
        minute_start_ms: raw.chunk.minute_start_ms(),
    };
    partition_gap(scope, &partition, "malformed_record")
}

fn disagreement_gap(
    scope: &OwnerScope,
    partition: &Partition,
    lanes: &BTreeMap<String, Vec<ParsedRecord>>,
) -> ReplayGapInterval {
    let counts = lanes
        .iter()
        .map(|(lane_id, records)| format!("{lane_id}={}", records.len()))
        .collect::<Vec<_>>()
        .join(",");
    partition_gap(scope, partition, &format!("lane_disagreement:{counts}"))
}

fn partition_gap(scope: &OwnerScope, partition: &Partition, reason: &str) -> ReplayGapInterval {
    ReplayGapInterval {
        scope: scope.clone(),
        partition: format!("{}:{}", partition.market_id, partition.minute_start_ms),
        start_time_ms: partition.minute_start_ms,
        end_time_ms: partition.minute_start_ms.checked_add(UTC_MINUTE_MS - 1),
        reason: reason.to_owned(),
    }
}
