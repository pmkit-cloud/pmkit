use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    OwnerScope, PmEnvelope, ReplayGap, ReplayGapReason, ReplayItem, schema::PM_ENVELOPE_VERSION,
};

pub fn decode_row(
    row: &turso::Row,
    scope: &OwnerScope,
    source_timestamp_ms: i64,
    canonical_source_rank: i64,
    connection_epoch: i64,
    frame_sequence: i64,
    ingest_sequence: i64,
) -> ReplayItem {
    let gap = |reason| {
        ReplayItem::Gap(ReplayGap {
            scope: scope.clone(),
            source_timestamp_ms,
            ingest_sequence,
            reason,
        })
    };
    let Ok(schema_version) = row.get::<i64>(6) else {
        return gap(ReplayGapReason::UnsupportedSchemaVersion);
    };
    if schema_version != i64::from(PM_ENVELOPE_VERSION) {
        return gap(ReplayGapReason::UnsupportedSchemaVersion);
    }
    let (Ok(receipt_timestamp_ms), Ok(venue_id), Ok(config_hash), Ok(source_id), Ok(connection_id)) =
        (row.get(7), row.get(8), row.get(9), row.get(10), row.get(11))
    else {
        return gap(ReplayGapReason::NormalizedIntegrityMismatch);
    };
    let (Ok(raw_frame), Ok(raw_sha256), Ok(normalized_json), Ok(normalized_sha256)) = (
        row.get::<Vec<u8>>(12),
        row.get::<String>(13),
        row.get::<String>(14),
        row.get::<String>(15),
    ) else {
        return gap(ReplayGapReason::RawIntegrityMismatch);
    };
    if sha256_hex(&raw_frame) != raw_sha256 {
        return gap(ReplayGapReason::RawIntegrityMismatch);
    }
    if sha256_hex(normalized_json.as_bytes()) != normalized_sha256 {
        return gap(ReplayGapReason::NormalizedIntegrityMismatch);
    }
    let Ok(normalized) = serde_json::from_str::<Value>(&normalized_json) else {
        return gap(ReplayGapReason::NormalizedIntegrityMismatch);
    };
    if !normalized.is_object() {
        return gap(ReplayGapReason::NormalizedIntegrityMismatch);
    }
    let Ok(schema_version) = u16::try_from(schema_version) else {
        return gap(ReplayGapReason::UnsupportedSchemaVersion);
    };
    ReplayItem::Envelope(PmEnvelope {
        schema_version,
        scope: scope.clone(),
        venue_id,
        config_hash,
        source_id,
        connection_id,
        source_timestamp_ms,
        canonical_source_rank,
        connection_epoch,
        frame_sequence,
        receipt_timestamp_ms,
        ingest_sequence,
        raw_frame,
        normalized,
    })
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
