use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{RAW_SPOOL_SCHEMA_VERSION, SpoolChunk, SpoolError, SpoolFrame};

#[derive(Deserialize)]
struct WireSpoolFrame {
    schema_version: u16,
    replica_id: String,
    shard_id: String,
    minute_start_ms: i64,
    connection_epoch: u64,
    frame_sequence: u64,
    receipt_time_ms: i64,
    discovery_snapshot_sha256: String,
    raw_bytes: Vec<u8>,
}

pub fn encode_record(chunk: &SpoolChunk, frame: &SpoolFrame) -> Result<Vec<u8>, SpoolError> {
    validate_frame(chunk, frame)?;
    serde_json::to_vec(&serde_json::json!({
        "schema_version": RAW_SPOOL_SCHEMA_VERSION,
        "replica_id": chunk.replica_id(),
        "shard_id": chunk.shard_id(),
        "minute_start_ms": chunk.minute_start_ms(),
        "connection_epoch": frame.connection_epoch,
        "frame_sequence": frame.frame_sequence,
        "receipt_time_ms": frame.receipt_time_ms,
        "discovery_snapshot_sha256": frame.discovery_snapshot_sha256,
        "raw_bytes": frame.raw_bytes,
    }))
    .map_err(|error| SpoolError::MalformedRecord {
        message: error.to_string(),
    })
}

pub fn decode_record(line: &[u8]) -> Result<(SpoolChunk, SpoolFrame), SpoolError> {
    if !line.ends_with(b"\n") {
        return Err(SpoolError::MalformedRecord {
            message: "record has no trailing newline".to_owned(),
        });
    }
    let wire: WireSpoolFrame =
        serde_json::from_slice(line).map_err(|error| SpoolError::MalformedRecord {
            message: error.to_string(),
        })?;
    if wire.schema_version != RAW_SPOOL_SCHEMA_VERSION {
        return Err(SpoolError::UnsupportedSchemaVersion {
            found: wire.schema_version,
        });
    }
    let chunk = SpoolChunk::new(wire.replica_id, wire.shard_id, wire.minute_start_ms)?;
    let frame = SpoolFrame::new(
        wire.connection_epoch,
        wire.frame_sequence,
        wire.receipt_time_ms,
        wire.discovery_snapshot_sha256,
        wire.raw_bytes,
    );
    validate_frame(&chunk, &frame)?;
    Ok((chunk, frame))
}

pub fn checksum_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn validate_frame(chunk: &SpoolChunk, frame: &SpoolFrame) -> Result<(), SpoolError> {
    if frame.receipt_time_ms.div_euclid(60_000) != chunk.minute_start_ms().div_euclid(60_000) {
        return Err(SpoolError::RecordOutsideChunk {
            receipt_time_ms: frame.receipt_time_ms,
            minute_start_ms: chunk.minute_start_ms(),
        });
    }
    if !is_sha256_hex(&frame.discovery_snapshot_sha256) {
        return Err(SpoolError::InvalidDiscoveryDigest);
    }
    Ok(())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
