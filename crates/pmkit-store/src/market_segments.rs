use std::{collections::BTreeMap, num::NonZeroUsize};

use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    OwnerScope, ReplayGapInterval, ReplayItem, SealedClosedDayManifest, StoreError, TapeStore,
};

const EXPORT_PAGE_SIZE: usize = 512;
const MAX_LOGICAL_SEGMENT_BYTES: usize = 32 * 1024 * 1024;
const UTC_MINUTE_MS: i64 = 60_000;
const UTC_DAY_MS: i64 = 86_400_000;
const PORTABLE_MARKET_EXPORT_VERSION: u16 = 1;

#[path = "market_segment_parts.rs"]
mod market_segment_parts;

use market_segment_parts::{
    SegmentIdInput, encode_rows, market_metadata, metadata_is_valid, roll_rows, segment_id,
};

/// One immutable portable segment declaration and its exact NDJSON bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedMarketSegment {
    /// The portable manifest declaration for `bytes`.
    pub declaration: Value,
    /// Exact newline-delimited portable rows addressed by the declaration digest.
    pub bytes: Vec<u8>,
}

/// A portable market export manifest with its independently addressable bodies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedMarketSegments {
    /// The canonical portable export manifest.
    pub manifest: Value,
    /// Segment bodies declared by `manifest`.
    pub segments: Vec<MaterializedMarketSegment>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketMetadata {
    series_id: String,
    asset: Option<String>,
    duration_seconds: Option<u64>,
    market_id: String,
    condition_id: String,
    outcome_tokens: Vec<OutcomeToken>,
    open_time_ms: i64,
    close_time_ms: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutcomeToken {
    outcome: String,
    token_id: String,
}

#[derive(Debug, Clone)]
struct SegmentRows {
    metadata: MarketMetadata,
    legacy_coverage: bool,
    rows: Vec<Value>,
}

/// Exports one sealed day as a portable market export manifest.
///
/// # Errors
///
/// Returns [`StoreError`] when observed evidence, metadata, or bounds are invalid.
pub async fn export_market_segments(
    store: &dyn TapeStore,
    scope: &OwnerScope,
    manifest: &SealedClosedDayManifest,
) -> Result<Value, StoreError> {
    Ok(
        export_market_segments_with_artifacts(store, scope, manifest)
            .await?
            .manifest,
    )
}

/// Exports one sealed day as a portable manifest plus exact segment bodies.
///
/// # Errors
///
/// Returns [`StoreError`] when observed evidence, metadata, or bounds are invalid.
pub async fn export_market_segments_with_artifacts(
    store: &dyn TapeStore,
    scope: &OwnerScope,
    manifest: &SealedClosedDayManifest,
) -> Result<MaterializedMarketSegments, StoreError> {
    let mut segments = BTreeMap::new();
    let mut cursor = None;
    let page_size = NonZeroUsize::new(EXPORT_PAGE_SIZE)
        .ok_or_else(|| storage_error("portable export page size is invalid"))?;
    loop {
        let page = store.read_envelopes(scope, cursor, page_size).await?;
        for item in page.items {
            match item {
                ReplayItem::Envelope(envelope) => {
                    materialize_envelope(manifest, &mut segments, &envelope)?;
                }
                ReplayItem::Gap(gap) if manifest.contains(gap.source_timestamp_ms) => {
                    return Err(storage_error(
                        "portable market export contains an observed replay gap",
                    ));
                }
                ReplayItem::Gap(_) => {}
            }
        }
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    ensure_gap_free_segments(store, scope, &segments).await?;
    let source_manifest_sha256 = digest_json(manifest.document())?;
    let mut declared = Vec::new();
    let mut materialized = Vec::new();
    for ((market_id, minute_start), segment) in segments {
        for (subpart, rows) in roll_rows(&segment.rows, MAX_LOGICAL_SEGMENT_BYTES)?
            .into_iter()
            .enumerate()
        {
            let subpart_ordinal = u64::try_from(subpart)
                .map_err(|_| storage_error("portable segment subpart ordinal is invalid"))?;
            let (declaration, bytes) = materialize_segment(
                &market_id,
                minute_start,
                &source_manifest_sha256,
                &segment,
                &rows,
                subpart_ordinal,
            )?;
            declared.push(declaration.clone());
            materialized.push(MaterializedMarketSegment { declaration, bytes });
        }
    }
    let manifest = json!({
        "schema_version": PORTABLE_MARKET_EXPORT_VERSION,
        "coverage": "observed",
        "source_manifest_sha256": source_manifest_sha256,
        "segments": declared,
    });
    Ok(MaterializedMarketSegments {
        manifest: with_artifact_sha256(manifest)?,
        segments: materialized,
    })
}

fn materialize_envelope(
    manifest: &SealedClosedDayManifest,
    segments: &mut BTreeMap<(String, i64), SegmentRows>,
    envelope: &crate::PmEnvelope,
) -> Result<(), StoreError> {
    if !manifest.contains(envelope.source_timestamp_ms) || envelope.source_timestamp_ms < 0 {
        return Err(storage_error(
            "portable market export envelope is outside the sealed day",
        ));
    }
    let market_id = envelope.canonical_market_id().to_owned();
    if market_id.is_empty() {
        return Err(storage_error(
            "portable market export envelope has no market identity",
        ));
    }
    let (metadata, legacy_coverage) = market_metadata(&envelope.normalized, &market_id)?;
    if !legacy_coverage && !metadata_is_valid(&metadata) {
        return Err(storage_error("portable market metadata is malformed"));
    }
    let minute_start = envelope.source_timestamp_ms / UTC_MINUTE_MS * UTC_MINUTE_MS;
    let row = json!({
        "event_time_ms": envelope.source_timestamp_ms,
        "event_ordinal": envelope.canonical_source_rank,
        "payload": envelope.normalized.get("payload").cloned().unwrap_or(Value::Null),
    });
    segments
        .entry((metadata.market_id.clone(), minute_start))
        .or_insert_with(|| SegmentRows {
            metadata,
            legacy_coverage,
            rows: Vec::new(),
        })
        .rows
        .push(row);
    Ok(())
}

async fn ensure_gap_free_segments(
    store: &dyn TapeStore,
    scope: &OwnerScope,
    segments: &BTreeMap<(String, i64), SegmentRows>,
) -> Result<(), StoreError> {
    let gaps = store.read_replay_gaps(scope).await?;
    for ((market_id, minute_start), segment) in segments {
        let end = if segment.legacy_coverage {
            minute_start / UTC_DAY_MS * UTC_DAY_MS + UTC_DAY_MS - 1
        } else {
            minute_start + UTC_MINUTE_MS - 1
        };
        let partition = format!("{market_id}:{minute_start}");
        if gaps.iter().any(|gap| {
            (gap.partition == "all_subscribed" || gap.partition == partition)
                && intervals_intersect(*minute_start, end, gap)
        }) {
            return Err(storage_error(
                "portable market export intersects a replay gap",
            ));
        }
    }
    Ok(())
}

fn intervals_intersect(start: i64, end: i64, gap: &ReplayGapInterval) -> bool {
    gap.start_time_ms <= end && gap.end_time_ms.unwrap_or(i64::MAX) >= start
}

fn materialize_segment(
    market_id: &str,
    minute_start: i64,
    source_manifest_sha256: &str,
    segment: &SegmentRows,
    rows: &[Value],
    subpart_ordinal: u64,
) -> Result<(Value, Vec<u8>), StoreError> {
    if segment.metadata.open_time_ms > segment.metadata.close_time_ms {
        return Err(storage_error("portable market bounds are malformed"));
    }
    let content = encode_rows(rows)?;
    let from_time_ms = rows
        .first()
        .and_then(|row| row["event_time_ms"].as_i64())
        .ok_or_else(|| storage_error("portable segment has no lower bound"))?;
    let to_time_ms = rows
        .last()
        .and_then(|row| row["event_time_ms"].as_i64())
        .ok_or_else(|| storage_error("portable segment has no upper bound"))?;
    let segment_id = segment_id(SegmentIdInput {
        source_manifest_sha256,
        series_id: &segment.metadata.series_id,
        market_id,
        minute_start,
        subpart_ordinal,
    });
    let declaration = json!({
        "segment_id": segment_id,
        "series_id": segment.metadata.series_id,
        "asset": segment.metadata.asset,
        "duration_seconds": segment.metadata.duration_seconds,
        "market_id": market_id,
        "condition_id": segment.metadata.condition_id,
        "outcome_tokens": segment.metadata.outcome_tokens.iter().map(|mapping| json!({"outcome": mapping.outcome, "token_id": mapping.token_id})).collect::<Vec<_>>(),
        "market_open_time_ms": segment.metadata.open_time_ms,
        "market_close_time_ms": segment.metadata.close_time_ms,
        "partition_start_time_ms": minute_start,
        "partition_end_time_ms": minute_start + UTC_MINUTE_MS - 1,
        "subpart_ordinal": subpart_ordinal,
        "from_time_ms": from_time_ms,
        "to_time_ms": to_time_ms,
        "from_ts_ms": from_time_ms,
        "to_ts_ms": to_time_ms,
        "rows": rows.len(),
        "bytes": content.len(),
        "sha256": crate::integrity::sha256_hex(&content),
        "source_manifest_sha256": source_manifest_sha256,
        "artifact_key": format!("portable-market-export-v1/{segment_id}.jsonl"),
    });
    Ok((declaration, content))
}

fn digest_json(value: &Value) -> Result<String, StoreError> {
    serde_json::to_vec(value)
        .map(|bytes| crate::integrity::sha256_hex(&bytes))
        .map_err(|_| storage_error("source manifest is not encodable"))
}

fn with_artifact_sha256(mut artifact: Value) -> Result<Value, StoreError> {
    let digest = digest_json(&artifact)?;
    artifact
        .as_object_mut()
        .ok_or_else(|| storage_error("portable export manifest is not an object"))?
        .insert("artifact_sha256".into(), Value::String(digest));
    Ok(artifact)
}

fn storage_error(message: &str) -> StoreError {
    StoreError::Storage {
        message: message.into(),
    }
}

#[cfg(test)]
#[path = "market_segments_tests.rs"]
mod market_segments_tests;
