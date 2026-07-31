use std::{collections::BTreeMap, fs, io::Cursor, path::Path};

use pmkit_data::RawPmMarketFrame;
use pmkit_event::StreamMetadata;
use pmkit_store::{OwnerScope, PmEnvelope, PublicTapeAuditFrame, ReplayGapInterval};
use serde_json::Value;
use thiserror::Error;

use crate::public_tape_contract::{
    FrameRecord, MappingSnapshot, Projection, RecorderGap, certify_v2_public_market_input,
    certify_v2_public_market_source, event_outcome, invalid, metadata, subframe_rank,
    validate_subframes, verified_snapshot,
};
use crate::{MarketTokens, RawFrameAdapterError, RawPolymarketFrameAdapter, parse_market_frame};

const VERSION: u8 = 2;

/// Error returned when a public tape v2 artifact cannot be imported safely.
#[derive(Debug, Error)]
pub enum PublicTapeImportError {
    /// Reading a tape artifact failed.
    #[error("read public tape artifact {path}: {source}")]
    Read {
        /// Artifact path.
        path: String,
        /// I/O failure.
        #[source]
        source: std::io::Error,
    },
    /// The producer artifact violates the completed v2 contract.
    #[error("invalid public tape v2 artifact: {message}")]
    Invalid {
        /// Contract violation detail.
        message: String,
    },
    /// Persisting or projecting one imported frame failed.
    #[error(transparent)]
    Adapter(#[from] RawFrameAdapterError),
}

/// The durable outcome of importing one v2 tape artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicTapeImportReport {
    /// Exact source frames retained as audit evidence.
    pub audit_frames: usize,
    /// Book and complete last-trade projections retained for replay.
    pub projected_frames: usize,
    /// Recorder intervals persisted before export.
    pub replay_gaps: usize,
}

/// Imports the completed `pm-money` public tape v2 contract into `PMKit` storage.
#[derive(Debug, Clone)]
pub struct PublicTapeImporter {
    adapter: RawPolymarketFrameAdapter,
    scope: OwnerScope,
    markets: BTreeMap<String, MarketTokens>,
}

impl PublicTapeImporter {
    /// Creates an importer keyed by immutable producer market identifiers.
    #[must_use]
    pub const fn new(
        adapter: RawPolymarketFrameAdapter,
        scope: OwnerScope,
        markets: BTreeMap<String, MarketTokens>,
    ) -> Self {
        Self {
            adapter,
            scope,
            markets,
        }
    }

    /// Imports one certified public-market v2 hour and its recorder-gap sidecars.
    ///
    /// # Errors
    ///
    /// Returns an error without projecting malformed, stale, or unordered evidence.
    pub async fn import_file(
        &self,
        tape_root: &Path,
        tape_file: &Path,
    ) -> Result<PublicTapeImportReport, PublicTapeImportError> {
        certify_v2_public_market_input(tape_root, tape_file)?;
        let bytes = fs::read(tape_file).map_err(|source| PublicTapeImportError::Read {
            path: tape_file.display().to_string(),
            source,
        })?;
        let bytes = if tape_file
            .extension()
            .is_some_and(|extension| extension == "zst")
        {
            zstd::stream::decode_all(Cursor::new(bytes)).map_err(|source| {
                PublicTapeImportError::Read {
                    path: tape_file.display().to_string(),
                    source,
                }
            })?
        } else {
            bytes
        };
        if bytes.last() != Some(&b'\n') {
            return Err(invalid("tape hour is not newline terminated"));
        }
        let records = bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| {
                let record: FrameRecord = serde_json::from_slice(line)
                    .map_err(|error| invalid(format!("malformed v2 frame: {error}")))?;
                certify_v2_public_market_source(&record.source_id)?;
                Ok(record)
            })
            .collect::<Result<Vec<_>, PublicTapeImportError>>()?;
        for record in &records {
            let snapshot = verified_snapshot(tape_root, &record.mapping_snapshot_sha256)?;
            self.validate_record(record, &snapshot)?;
        }
        let replay_gaps = self.read_gap_sidecars(tape_root)?;
        let mut report = PublicTapeImportReport {
            audit_frames: 0,
            projected_frames: 0,
            replay_gaps: replay_gaps.len(),
        };
        let mut audit_frames = Vec::with_capacity(records.len());
        let mut envelopes = Vec::new();
        for record in records {
            let snapshot = verified_snapshot(tape_root, &record.mapping_snapshot_sha256)?;
            let (audit_frame, projected_envelopes) = self.prepare_record(&record, &snapshot)?;
            report.audit_frames += 1;
            report.projected_frames += projected_envelopes.len();
            audit_frames.push(audit_frame);
            envelopes.extend(projected_envelopes);
        }
        self.adapter
            .store_public_tape_import(&replay_gaps, &audit_frames, &envelopes)
            .await
            .map_err(adapter_error)?;
        Ok(report)
    }

    fn prepare_record(
        &self,
        record: &FrameRecord,
        snapshot: &MappingSnapshot,
    ) -> Result<(PublicTapeAuditFrame, Vec<PmEnvelope>), PublicTapeImportError> {
        if record.version != VERSION || record.record_type != "frame" {
            return Err(invalid("unsupported v2 record identity"));
        }
        let partition = format!("pm-money-v2:{}", record.mapping_snapshot_sha256);
        let metadata = metadata(record)?;
        let audit_frame = PublicTapeAuditFrame {
            scope: self.scope.clone(),
            partition,
            source_id: record.source_id.clone(),
            connection_id: record.connection_id.to_string(),
            connection_epoch: metadata.connection_epoch,
            frame_sequence: metadata.frame_sequence,
            ingest_sequence: metadata
                .ingest_sequence
                .try_into()
                .map_err(|_| invalid("ingest sequence exceeds PMKit range"))?,
            receipt_timestamp_ms: metadata.receipt_time_ms,
            source_timestamp_ms: record.source_time_ms,
            raw_frame: record.raw.as_bytes().to_vec(),
        };
        let payload: Value = serde_json::from_str(&record.raw)
            .map_err(|error| invalid(format!("raw frame is not JSON: {error}")))?;
        let values = match payload {
            Value::Array(values) => values,
            value => vec![value],
        };
        validate_subframes(&values, &record.subframes)?;
        let mut envelopes = Vec::new();
        for (index, (value, subframe)) in values.iter().zip(&record.subframes).enumerate() {
            match subframe.projection {
                Projection::Book | Projection::LastTradePrice => {
                    let tokens = self.resolve_tokens(value, snapshot)?;
                    let raw = serde_json::to_vec(value)
                        .map_err(|error| invalid(format!("encode subframe: {error}")))?;
                    let fact = parse_market_frame(&raw, tokens)
                        .map_err(|error| invalid(error.to_string()))?;
                    let outcome = event_outcome(&fact)?;
                    let frame = RawPmMarketFrame {
                        market: tokens.market().clone(),
                        outcome,
                        metadata: StreamMetadata {
                            frame_sequence: subframe_rank(record.frame_sequence, index)?,
                            ..metadata.clone()
                        },
                        text: raw,
                    };
                    envelopes.push(self.adapter.market_envelope(&frame)?);
                }
                Projection::IntentionallyUnprojected => {}
            }
        }
        Ok((audit_frame, envelopes))
    }

    fn validate_record(
        &self,
        record: &FrameRecord,
        snapshot: &MappingSnapshot,
    ) -> Result<(), PublicTapeImportError> {
        if record.version != VERSION || record.record_type != "frame" {
            return Err(invalid("unsupported v2 record identity"));
        }
        metadata(record)?;
        i64::try_from(record.ingest_sequence)
            .map_err(|_| invalid("ingest sequence exceeds PMKit range"))?;
        let payload: Value = serde_json::from_str(&record.raw)
            .map_err(|error| invalid(format!("raw frame is not JSON: {error}")))?;
        let values = match payload {
            Value::Array(values) => values,
            value => vec![value],
        };
        validate_subframes(&values, &record.subframes)?;
        for (index, (value, subframe)) in values.iter().zip(&record.subframes).enumerate() {
            match subframe.projection {
                Projection::Book | Projection::LastTradePrice => {
                    let tokens = self.resolve_tokens(value, snapshot)?;
                    let raw = serde_json::to_vec(value)
                        .map_err(|error| invalid(format!("encode subframe: {error}")))?;
                    let fact = parse_market_frame(&raw, tokens)
                        .map_err(|error| invalid(error.to_string()))?;
                    event_outcome(&fact)?;
                    subframe_rank(record.frame_sequence, index)?;
                }
                Projection::IntentionallyUnprojected => {}
            }
        }
        Ok(())
    }

    fn read_gap_sidecars(
        &self,
        tape_root: &Path,
    ) -> Result<Vec<ReplayGapInterval>, PublicTapeImportError> {
        let directory = tape_root.join("pm-market/gaps");
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => {
                return Err(PublicTapeImportError::Read {
                    path: directory.display().to_string(),
                    source,
                });
            }
        };
        let mut paths = entries
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| PublicTapeImportError::Read {
                path: directory.display().to_string(),
                source,
            })?;
        paths.sort();
        let mut gaps = Vec::new();
        for path in paths.into_iter().filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "jsonl")
        }) {
            let bytes = fs::read(&path).map_err(|source| PublicTapeImportError::Read {
                path: path.display().to_string(),
                source,
            })?;
            if bytes.last() != Some(&b'\n') {
                return Err(invalid("recorder gap sidecar is not newline terminated"));
            }
            for line in bytes
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
            {
                let gap: RecorderGap = serde_json::from_slice(line)
                    .map_err(|error| invalid(format!("malformed recorder gap: {error}")))?;
                if gap.version != VERSION || gap.record_type != "gap" {
                    return Err(invalid("unsupported recorder gap identity"));
                }
                gaps.push(ReplayGapInterval {
                    scope: self.scope.clone(),
                    partition: gap
                        .scope
                        .as_str()
                        .ok_or_else(|| invalid("recorder gap scope is not a partition"))?
                        .to_owned(),
                    start_time_ms: gap.start_time_ms,
                    end_time_ms: gap.end_time_ms,
                    reason: gap.reason,
                });
            }
        }
        Ok(gaps)
    }

    fn resolve_tokens<'a>(
        &'a self,
        value: &Value,
        snapshot: &'a MappingSnapshot,
    ) -> Result<&'a MarketTokens, PublicTapeImportError> {
        let asset = value
            .get("asset_id")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("projected subframe has no asset_id"))?;
        let market = snapshot
            .mappings
            .get(asset)
            .ok_or_else(|| invalid("projected asset is absent from mapping snapshot"))?;
        if value.get("market").and_then(Value::as_str) != Some(market) {
            return Err(invalid(
                "projected subframe market disagrees with mapping snapshot",
            ));
        }
        self.markets
            .get(market)
            .ok_or_else(|| invalid("mapping snapshot market is not configured"))
    }
}

const fn adapter_error(error: pmkit_store::StoreError) -> PublicTapeImportError {
    PublicTapeImportError::Adapter(RawFrameAdapterError::Store(error))
}
