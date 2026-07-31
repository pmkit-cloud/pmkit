use std::{collections::BTreeMap, num::NonZeroUsize};

use pmkit_core::RunId;
use serde_json::{Value, json};

use crate::{
    OwnerScope, REPLAY_BUNDLE_VERSION, ReplayItem, SealedClosedDayManifest, StoreError, TapeStore,
};

const EXPORT_PAGE_SIZE: usize = 512;
const UTC_DAY_MS: i64 = 86_400_000;

/// One uploadable, treated market/time segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedMarketSegment {
    /// Cloud catalog segment declaration.
    pub declaration: Value,
    /// Exact newline-delimited JSON bytes addressed by `declaration.sha256`.
    pub bytes: Vec<u8>,
}

/// A Cloud-compatible manifest plus the segment bodies it declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedMarketSegments {
    /// Bundle manifest for the Cloud publication endpoint.
    pub manifest: Value,
    /// Bodies to upload using each segment's `artifact_key`.
    pub segments: Vec<MaterializedMarketSegment>,
}

/// Materializes verified replay envelopes into deterministic, market-scoped
/// JSONL segments and a Cloud-compatible segment manifest.
///
/// # Errors
///
/// Returns a storage error when the source contains a replay gap, an envelope
/// outside the sealed day or without a market identity, or an invalid
/// timestamp/serialization result.
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

/// Materializes verified replay envelopes into UTC-day, market-scoped JSONL
/// segments and returns both their Cloud manifest and exact upload bodies.
///
/// # Errors
///
/// Returns a storage error when the source contains a replay gap, an envelope
/// outside the sealed day or without a market identity, or an invalid
/// timestamp/serialization result.
pub async fn export_market_segments_with_artifacts(
    store: &dyn TapeStore,
    scope: &OwnerScope,
    manifest: &SealedClosedDayManifest,
) -> Result<MaterializedMarketSegments, StoreError> {
    let page_size = NonZeroUsize::new(EXPORT_PAGE_SIZE).unwrap_or(NonZeroUsize::MIN);
    let mut segments: BTreeMap<(String, i64), Vec<Value>> = BTreeMap::new();
    let mut cursor = None;
    loop {
        let page = store.read_envelopes(scope, cursor, page_size).await?;
        for item in page.items {
            match item {
                ReplayItem::Envelope(envelope) => {
                    materialize_envelope(manifest, &mut segments, envelope)?;
                }
                ReplayItem::Gap(gap) => {
                    if !manifest.contains(gap.source_timestamp_ms) {
                        continue;
                    }
                    return Err(StoreError::Storage {
                        message: format!(
                            "market segment export evidence gap at source_timestamp_ms {} ingest_sequence {}: {:?}",
                            gap.source_timestamp_ms, gap.ingest_sequence, gap.reason
                        ),
                    });
                }
            }
        }
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    ensure_gap_free_segments(store, scope, &segments).await?;
    let source_manifest_sha256 =
        crate::integrity::sha256_hex(manifest.document().to_string().as_bytes());
    let mut declared = Vec::new();
    let mut materialized = Vec::new();
    for ((market_id, day_start), rows) in segments {
        let (declaration, content) = materialize_segment(
            &scope.run_id,
            &market_id,
            day_start,
            &source_manifest_sha256,
            &rows,
        )?;
        declared.push(declaration.clone());
        materialized.push(MaterializedMarketSegment {
            declaration,
            bytes: content.into_bytes(),
        });
    }

    Ok(MaterializedMarketSegments {
        manifest: with_artifact_sha256(json!({
        "schema_version": REPLAY_BUNDLE_VERSION,
        "coverage": "observed",
        "scope": {
            "portfolio": scope.portfolio_id.to_string(),
            "run": scope.run_id.to_string(),
        },
        "manifest": {
            "mode": "public_market_tape",
            "portfolio": scope.portfolio_id.to_string(),
            "run": scope.run_id.to_string(),
            "closed_day": manifest.closed_day(),
        },
        "segments": declared,
        })),
        segments: materialized,
    })
}

fn materialize_envelope(
    manifest: &SealedClosedDayManifest,
    segments: &mut BTreeMap<(String, i64), Vec<Value>>,
    envelope: crate::PmEnvelope,
) -> Result<(), StoreError> {
    if !manifest.contains(envelope.source_timestamp_ms) {
        return Err(StoreError::Storage {
            message: format!(
                "market segment export cannot use envelope outside sealed day {} at source_timestamp_ms {}",
                manifest.closed_day(),
                envelope.source_timestamp_ms
            ),
        });
    }
    let market_id = envelope.canonical_market_id().to_owned();
    if market_id.is_empty() {
        return Err(StoreError::Storage {
            message: format!(
                "market segment export cannot classify envelope at source_timestamp_ms {}",
                envelope.source_timestamp_ms
            ),
        });
    }
    let crate::PmEnvelope {
        source_timestamp_ms,
        ingest_sequence,
        source_id,
        normalized,
        ..
    } = envelope;
    if source_timestamp_ms < 0 {
        return Err(StoreError::Storage {
            message: format!(
                "market segment export cannot use negative source_timestamp_ms {source_timestamp_ms}"
            ),
        });
    }
    let day_start = source_timestamp_ms / UTC_DAY_MS * UTC_DAY_MS;
    segments
        .entry((market_id, day_start))
        .or_default()
        .push(json!({
            "source_timestamp_ms": source_timestamp_ms,
            "ingest_sequence": ingest_sequence,
            "source_id": source_id,
            "normalized": normalized,
        }));
    Ok(())
}

fn intervals_intersect(from_time_ms: i64, to_time_ms: i64, gap: &crate::ReplayGapInterval) -> bool {
    gap.start_time_ms <= to_time_ms && gap.end_time_ms.unwrap_or(i64::MAX) >= from_time_ms
}

fn segment_partition(market_id: &str, day_start: i64) -> String {
    format!("{market_id}:{day_start}")
}

async fn ensure_gap_free_segments(
    store: &dyn TapeStore,
    scope: &OwnerScope,
    segments: &BTreeMap<(String, i64), Vec<Value>>,
) -> Result<(), StoreError> {
    let gaps = store.read_replay_gaps(scope).await?;
    for (market_id, day_start) in segments.keys() {
        let day_end = day_start
            .checked_add(UTC_DAY_MS - 1)
            .ok_or_else(|| StoreError::Storage {
                message: "segment day upper bound is invalid".into(),
            })?;
        let partition = segment_partition(market_id, *day_start);
        if let Some(gap) = gaps.iter().find(|gap| {
            (gap.partition == "all_subscribed" || gap.partition == partition)
                && intervals_intersect(*day_start, day_end, gap)
        }) {
            return Err(StoreError::Storage {
                message: format!(
                    "market segment export evidence gap in partition {} for market {} day {}",
                    gap.partition, market_id, day_start
                ),
            });
        }
    }
    Ok(())
}

fn materialize_segment(
    run_id: &RunId,
    market_id: &str,
    day_start: i64,
    source_manifest_sha256: &str,
    rows: &[Value],
) -> Result<(Value, String), StoreError> {
    let content = rows
        .iter()
        .map(|row| {
            serde_json::to_string(row)
                .map(|line| format!("{line}\n"))
                .map_err(|error| StoreError::Storage {
                    message: error.to_string(),
                })
        })
        .collect::<Result<String, _>>()?;
    let from_ts_ms = rows
        .first()
        .and_then(|row| row["source_timestamp_ms"].as_i64())
        .ok_or_else(|| StoreError::Storage {
            message: "segment has no lower bound".into(),
        })?;
    let to_ts_ms = rows
        .last()
        .and_then(|row| row["source_timestamp_ms"].as_i64())
        .ok_or_else(|| StoreError::Storage {
            message: "segment has no upper bound".into(),
        })?;
    let segment_id = format!("{run_id}-{market_id}-{day_start}");
    Ok((
        json!({
            "segment_id": segment_id,
            "market_id": market_id,
            "partition_id": segment_partition(market_id, day_start),
            "artifact_key": format!("segments/{market_id}/{segment_id}.jsonl"),
            "from_ts_ms": from_ts_ms,
            "to_ts_ms": to_ts_ms,
            "rows": rows.len(),
            "bytes": content.len(),
            "sha256": crate::integrity::sha256_hex(content.as_bytes()),
            "source_manifest_sha256": source_manifest_sha256,
        }),
        content,
    ))
}

fn with_artifact_sha256(mut artifact: Value) -> Value {
    let digest = crate::integrity::sha256_hex(artifact.to_string().as_bytes());
    if let Some(fields) = artifact.as_object_mut() {
        fields.insert("artifact_sha256".to_owned(), Value::String(digest));
    }
    artifact
}
