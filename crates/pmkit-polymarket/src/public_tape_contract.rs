use std::{
    collections::BTreeMap,
    fs,
    path::{Component, Path},
};

use pmkit_event::{MarketEvent, StreamMetadata};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::public_tape::PublicTapeImportError;

const VERSION: u8 = 2;
const PUBLIC_MARKET_DIRECTORY: &str = "pm-market";
const PUBLIC_MARKET_SOURCE_IDS: [&str; 2] = ["polymarket-market", "polymarket:market-ws"];

/// Certifies that a tape artifact is an accepted v2 public-market input.
///
/// Legacy TSV and files outside `pm-market/` are archival or private input and
/// must not enter audit or replay storage.
///
/// # Errors
///
/// Returns [`PublicTapeImportError::Invalid`] when the path or filename is not
/// a v2 public-market artifact.
pub fn certify_v2_public_market_input(
    tape_root: &Path,
    tape_file: &Path,
) -> Result<(), PublicTapeImportError> {
    let public_market_root = tape_root.join(PUBLIC_MARKET_DIRECTORY);
    let relative = tape_file
        .strip_prefix(&public_market_root)
        .map_err(|_| invalid("tape file is outside the v2 public-market path"))?;
    if relative.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(invalid("tape file escapes the v2 public-market path"));
    }
    let name = tape_file
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| invalid("v2 public-market tape file has no UTF-8 name"))?;
    if !name.ends_with(".v2.ndjson") && !name.ends_with(".v2.ndjson.zst") {
        return Err(invalid(
            "legacy TSV and non-v2 tape files are not importable",
        ));
    }
    Ok(())
}

/// Certifies the source identity before a v2 frame is retained or projected.
///
/// # Errors
///
/// Returns [`PublicTapeImportError::Invalid`] when the source is not public-market data.
pub fn certify_v2_public_market_source(source_id: &str) -> Result<(), PublicTapeImportError> {
    if PUBLIC_MARKET_SOURCE_IDS.contains(&source_id) {
        Ok(())
    } else {
        Err(invalid(
            "tape record is not from a certified public-market source",
        ))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrameRecord {
    pub(super) version: u8,
    pub(super) record_type: String,
    pub(super) received_at_ms: i64,
    pub(super) source_time_ms: Option<i64>,
    pub(super) source_id: String,
    pub(super) connection_id: u64,
    pub(super) epoch: u64,
    pub(super) frame_sequence: u64,
    pub(super) ingest_sequence: u64,
    pub(super) mapping_snapshot_sha256: String,
    pub(super) raw: String,
    pub(super) subframes: Vec<Subframe>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subframe {
    pub(super) index: usize,
    pub(super) projection: Projection,
    pub(super) duplicate_of: Option<usize>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Projection {
    Book,
    LastTradePrice,
    IntentionallyUnprojected,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MappingSnapshot {
    pub(super) version: u8,
    pub(super) mappings: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecorderGap {
    pub(super) version: u8,
    pub(super) record_type: String,
    pub(super) reason: String,
    pub(super) scope: Value,
    pub(super) start_time_ms: i64,
    pub(super) end_time_ms: Option<i64>,
}

pub fn verified_snapshot(
    root: &Path,
    hash: &str,
) -> Result<MappingSnapshot, PublicTapeImportError> {
    let path = root.join("pm-market/mappings").join(format!("{hash}.json"));
    let bytes = fs::read(&path).map_err(|source| PublicTapeImportError::Read {
        path: path.display().to_string(),
        source,
    })?;
    if hex_sha256(&bytes) != hash {
        return Err(invalid(
            "mapping snapshot digest does not match its identity",
        ));
    }
    let snapshot: MappingSnapshot = serde_json::from_slice(&bytes)
        .map_err(|error| invalid(format!("malformed mapping snapshot: {error}")))?;
    if snapshot.version != VERSION
        || serde_json::to_vec(&snapshot).map_err(|error| invalid(error.to_string()))? != bytes
    {
        return Err(invalid(
            "mapping snapshot is not immutable canonical v2 JSON",
        ));
    }
    Ok(snapshot)
}

pub fn validate_subframes(
    values: &[Value],
    subframes: &[Subframe],
) -> Result<(), PublicTapeImportError> {
    if values.len() != subframes.len() {
        return Err(invalid("subframe count does not match raw frame"));
    }
    for (index, (value, subframe)) in values.iter().zip(subframes).enumerate() {
        if subframe.index != index
            || subframe.projection != projection(value)
            || subframe.duplicate_of != values[..index].iter().position(|prior| prior == value)
        {
            return Err(invalid("subframe order or projection metadata is invalid"));
        }
    }
    Ok(())
}

pub fn metadata(record: &FrameRecord) -> Result<StreamMetadata, PublicTapeImportError> {
    Ok(StreamMetadata {
        schema_version: 1,
        source_id: record.source_id.clone(),
        source_time_ms: record
            .source_time_ms
            .ok_or_else(|| invalid("frame has no source timestamp"))?,
        canonical_source_rank: 0,
        receipt_time_ms: record.received_at_ms,
        connection_id: record.connection_id.to_string(),
        connection_epoch: record
            .epoch
            .try_into()
            .map_err(|_| invalid("connection epoch exceeds PMKit range"))?,
        frame_sequence: subframe_rank(record.frame_sequence, 0)?,
        ingest_sequence: record.ingest_sequence,
    })
}

pub fn event_outcome(event: &MarketEvent) -> Result<pmkit_market::Outcome, PublicTapeImportError> {
    match event {
        MarketEvent::BookUpdate { outcome, .. } | MarketEvent::LastTrade { outcome, .. } => {
            Ok(*outcome)
        }
        _ => Err(invalid("non-market projection")),
    }
}

pub fn subframe_rank(frame_sequence: u64, index: usize) -> Result<i64, PublicTapeImportError> {
    let frame =
        i64::try_from(frame_sequence).map_err(|_| invalid("frame sequence exceeds PMKit range"))?;
    let index = i64::try_from(index).map_err(|_| invalid("subframe index exceeds PMKit range"))?;
    frame
        .checked_shl(32)
        .and_then(|rank| rank.checked_add(index))
        .ok_or_else(|| invalid("subframe rank overflow"))
}

fn projection(value: &Value) -> Projection {
    match value.get("event_type").and_then(Value::as_str) {
        Some("book") => Projection::Book,
        Some("last_trade_price") => Projection::LastTradePrice,
        _ => Projection::IntentionallyUnprojected,
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn invalid(message: impl Into<String>) -> PublicTapeImportError {
    PublicTapeImportError::Invalid {
        message: message.into(),
    }
}
