//! Replay bundle export: a self-contained reproducibility artifact.
//!
//! [`export_replay_bundle`] gathers a run's durable evidence into one JSON
//! document: the reproducibility manifest, every PM envelope (raw frame plus
//! normalized fact) in canonical order, every causal decision, and the CEX
//! archive checksums the caller verified. It fails closed on any replay gap or
//! non-UTF-8 raw frame so an exported bundle never claims incomplete or corrupt
//! evidence.

use std::num::NonZeroUsize;

use serde_json::{Value, json};

use crate::{OwnerScope, ReplayItem, StoreError, TapeStore};

/// Replay bundle schema version. Bumping it requires a migration entry per the
/// storage compatibility policy.
pub const REPLAY_BUNDLE_VERSION: u16 = 1;

const EXPORT_PAGE_SIZE: usize = 512;

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
        "coverage": "observed",
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
