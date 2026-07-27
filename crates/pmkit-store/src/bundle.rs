//! Replay bundle export: a self-contained reproducibility artifact.
//!
//! [`export_replay_bundle`] gathers a run's durable evidence into one JSON
//! document: the reproducibility manifest, every PM envelope (raw frame plus
//! normalized fact) in canonical order, every causal decision, and the CEX
//! archive checksums the caller verified. It fails closed on any replay gap or
//! non-UTF-8 raw frame so an exported bundle never claims incomplete or corrupt
//! evidence.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use pmkit_core::RunId;
use serde_json::{Value, json};

use crate::{OwnerScope, ReplayItem, StoreError, TapeStore};

/// Replay bundle schema version. Bumping it requires a migration entry per the
/// storage compatibility policy.
pub const REPLAY_BUNDLE_VERSION: u16 = 1;

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
/// without a market identity, or an invalid timestamp/serialization result.
pub async fn export_market_segments(
    store: &dyn TapeStore,
    scope: &OwnerScope,
    manifest: &Value,
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
/// without a market identity, or an invalid timestamp/serialization result.
pub async fn export_market_segments_with_artifacts(
    store: &dyn TapeStore,
    scope: &OwnerScope,
    manifest: &Value,
) -> Result<MaterializedMarketSegments, StoreError> {
    let page_size = NonZeroUsize::new(EXPORT_PAGE_SIZE).unwrap_or(NonZeroUsize::MIN);
    let mut segments: BTreeMap<(String, i64), Vec<Value>> = BTreeMap::new();
    let mut cursor = None;

    loop {
        let page = store.read_envelopes(scope, cursor, page_size).await?;
        for item in page.items {
            match item {
                ReplayItem::Envelope(envelope) => {
                    let token_id = envelope.canonical_market_id().to_owned();
                    if token_id.is_empty() {
                        return Err(StoreError::Storage {
                            message: format!(
                                "market segment export cannot classify envelope at source_timestamp_ms {}",
                                envelope.source_timestamp_ms
                            ),
                        });
                    }
                    if envelope.source_timestamp_ms < 0 {
                        return Err(StoreError::Storage {
                            message: format!(
                                "market segment export cannot use negative source_timestamp_ms {}",
                                envelope.source_timestamp_ms
                            ),
                        });
                    }
                    let day_start = envelope.source_timestamp_ms / UTC_DAY_MS * UTC_DAY_MS;
                    segments
                        .entry((token_id, day_start))
                        .or_default()
                        .push(json!({
                            "source_timestamp_ms": envelope.source_timestamp_ms,
                            "ingest_sequence": envelope.ingest_sequence,
                            "source_id": envelope.source_id,
                            "normalized": envelope.normalized,
                        }));
                }
                ReplayItem::Gap(gap) => {
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

    let mut declared = Vec::new();
    let mut materialized = Vec::new();
    for ((token_id, day_start), rows) in segments {
        let (declaration, content) =
            materialize_segment(&scope.run_id, &token_id, day_start, &rows)?;
        declared.push(declaration.clone());
        materialized.push(MaterializedMarketSegment {
            declaration,
            bytes: content.into_bytes(),
        });
    }

    Ok(MaterializedMarketSegments {
        manifest: with_artifact_sha256(json!({
        "schema_version": REPLAY_BUNDLE_VERSION,
        "scope": {
            "portfolio": scope.portfolio_id.to_string(),
            "run": scope.run_id.to_string(),
        },
        "manifest": manifest.clone(),
        "segments": declared,
        })),
        segments: materialized,
    })
}

fn materialize_segment(
    run_id: &RunId,
    token_id: &str,
    day_start: i64,
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
    let segment_id = format!("{run_id}-{token_id}-{day_start}");
    Ok((
        json!({
            "segment_id": segment_id,
            "token_id": token_id,
            "artifact_key": format!("segments/{token_id}/{segment_id}.jsonl"),
            "from_ts_ms": from_ts_ms,
            "to_ts_ms": to_ts_ms,
            "rows": rows.len(),
            "bytes": content.len(),
            "sha256": crate::integrity::sha256_hex(content.as_bytes()),
        }),
        content,
    ))
}

/// A verified CEX archive checksum recorded alongside a replay bundle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheChecksum {
    /// Archive key or path the checksum belongs to.
    pub key: String,
    /// Lowercase-hex SHA-256 of the verified archive.
    pub sha256_hex: String,
}

/// Exports a replay bundle for one owner scope.
///
/// # Errors
///
/// Returns [`StoreError`] when a store read fails or the durable evidence is
/// incomplete (a replay gap) or corrupt (a non-UTF-8 raw frame).
pub async fn export_replay_bundle(
    store: &dyn TapeStore,
    scope: &OwnerScope,
    manifest: &Value,
    cache_checksums: &[CacheChecksum],
) -> Result<Value, StoreError> {
    let page_size = NonZeroUsize::new(EXPORT_PAGE_SIZE).unwrap_or(NonZeroUsize::MIN);
    let mut evidence = Vec::new();
    let mut cursor = None;
    loop {
        let page = store.read_envelopes(scope, cursor, page_size).await?;
        for item in page.items {
            match item {
                ReplayItem::Envelope(envelope) => {
                    let raw_frame =
                        String::from_utf8(envelope.raw_frame).map_err(|_| StoreError::Storage {
                            message: "replay bundle raw frame is not valid UTF-8".into(),
                        })?;
                    evidence.push(json!({
                        "source_id": envelope.source_id,
                        "connection_id": envelope.connection_id,
                        "source_timestamp_ms": envelope.source_timestamp_ms,
                        "canonical_source_rank": envelope.canonical_source_rank,
                        "connection_epoch": envelope.connection_epoch,
                        "frame_sequence": envelope.frame_sequence,
                        "ingest_sequence": envelope.ingest_sequence,
                        "raw_frame": raw_frame,
                        "normalized": envelope.normalized,
                    }));
                }
                ReplayItem::Gap(gap) => {
                    return Err(StoreError::Storage {
                        message: format!(
                            "replay bundle evidence gap at source_timestamp_ms {} ingest_sequence {}: {:?}",
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

    let decisions: Vec<Value> = store
        .read_decisions(scope)
        .await?
        .into_iter()
        .map(|decision| {
            json!({
                "correlation_id": decision.identity.correlation_id,
                "source_timestamp_ms": decision.identity.source_timestamp_ms,
                "ingest_sequence": decision.identity.ingest_sequence,
                "payload": decision.payload,
            })
        })
        .collect();

    let checksums: Vec<Value> = cache_checksums
        .iter()
        .map(|checksum| {
            json!({
                "key": checksum.key,
                "sha256_hex": checksum.sha256_hex,
            })
        })
        .collect();

    Ok(with_artifact_sha256(json!({
        "schema_version": REPLAY_BUNDLE_VERSION,
        "scope": {
            "portfolio": scope.portfolio_id.to_string(),
            "run": scope.run_id.to_string(),
        },
        "manifest": manifest.clone(),
        "pm_evidence": evidence,
        "decisions": decisions,
        "cache_checksums": checksums,
    })))
}

fn with_artifact_sha256(mut artifact: Value) -> Value {
    let digest = crate::integrity::sha256_hex(artifact.to_string().as_bytes());
    if let Some(fields) = artifact.as_object_mut() {
        fields.insert("artifact_sha256".to_owned(), Value::String(digest));
    }
    artifact
}

#[cfg(test)]
mod artifact_digest_tests {
    use super::REPLAY_BUNDLE_VERSION;
    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};

    fn bundle_content() -> Value {
        json!({
            "schema_version": REPLAY_BUNDLE_VERSION,
            "scope": { "portfolio": "paper", "run": "run" },
            "manifest": { "mode": "backtest", "run": "run" },
            "pm_evidence": [{ "raw_frame": "frame" }],
            "decisions": [{ "payload": { "kind": "quote" } }],
            "cache_checksums": [{ "key": "archive.zip", "sha256_hex": "abc123" }],
        })
    }

    fn content_sha256(artifact: &Value) -> Result<String, &'static str> {
        let mut content = artifact.clone();
        content
            .as_object_mut()
            .ok_or("bundle must be an object")?
            .remove("artifact_sha256")
            .ok_or("bundle must contain artifact_sha256")?;
        Ok(format!(
            "{:x}",
            Sha256::digest(content.to_string().as_bytes())
        ))
    }

    #[test]
    fn bundle_self_digest_verifies() -> Result<(), Box<dyn std::error::Error>> {
        // Given / When
        let first = super::with_artifact_sha256(bundle_content());
        let second = super::with_artifact_sha256(bundle_content());
        let stored = first["artifact_sha256"]
            .as_str()
            .ok_or("artifact_sha256 must be a string")?;

        // Then
        assert_eq!(stored, content_sha256(&first)?);
        assert_eq!(first["artifact_sha256"], second["artifact_sha256"]);
        Ok(())
    }

    #[test]
    fn bundle_tamper_changes_digest() -> Result<(), Box<dyn std::error::Error>> {
        // Given
        let bundle = super::with_artifact_sha256(bundle_content());
        let stored = bundle["artifact_sha256"]
            .as_str()
            .ok_or("artifact_sha256 must be a string")?
            .to_owned();
        let mut tampered = bundle;
        tampered
            .get_mut("scope")
            .and_then(Value::as_object_mut)
            .ok_or("scope must be an object")?
            .insert("run".to_owned(), json!("rum"));

        // When
        let tampered_digest = content_sha256(&tampered)?;

        // Then
        assert_ne!(stored, tampered_digest);
        Ok(())
    }
}
