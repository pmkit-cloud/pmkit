//! Run-scoped raw source capture, separate from PM-only causal storage.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use pmkit_core::RunId;
use pmkit_data::{GapSequenceDomain as SourceGapSequenceDomain, SourceGap, SourceSignal};
use pmkit_event::{SourceEnvelope, StreamMetadata};
use pmkit_tape::{
    CaptureTransport, GapRange, GapSequenceDomain as CaptureGapSequenceDomain, RawCaptureDraft,
    RawCaptureError, RawCapturePayload, RawCaptureWriter,
};
use thiserror::Error;

/// Enables one append-only capture file per run under a directory.
#[derive(Debug, Clone)]
pub struct CaptureConfig {
    directory: PathBuf,
}

impl CaptureConfig {
    /// Stores run captures under `directory`, without replacing existing files.
    #[must_use]
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }

    pub(crate) fn open(&self, run: &RunId) -> Result<CaptureRecorder, CaptureError> {
        fs::create_dir_all(&self.directory)?;
        let path = self.directory.join(format!("run-{run}.jsonl"));
        let file = OpenOptions::new().write(true).create_new(true).open(path)?;
        Ok(CaptureRecorder::new(RawCaptureWriter::new(file)))
    }

    /// Returns the path used for `run`.
    #[must_use]
    pub fn path_for(&self, run: &RunId) -> PathBuf {
        self.directory.join(format!("run-{run}.jsonl"))
    }
}

/// A thread-safe per-run writer assigning one ingest sequence across all sources.
#[derive(Clone)]
pub(crate) struct CaptureRecorder {
    writer: Arc<Mutex<RawCaptureWriter<File>>>,
}

impl std::fmt::Debug for CaptureRecorder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CaptureRecorder")
            .finish_non_exhaustive()
    }
}

impl CaptureRecorder {
    fn new(writer: RawCaptureWriter<File>) -> Self {
        Self {
            writer: Arc::new(Mutex::new(writer)),
        }
    }

    /// Persists a source data, gap, or heartbeat signal in observed order.
    /// Watermarks and EOF are merge lifecycle signals, not captured events.
    ///
    /// # Errors
    ///
    /// Returns an error when metadata is invalid or the tape cannot be written.
    pub(crate) fn record(&self, signal: &SourceSignal) -> Result<(), CaptureError> {
        let Some(draft) = capture_draft(signal)? else {
            return Ok(());
        };
        let mut writer = self.writer.lock().map_err(|_| CaptureError::LockPoisoned)?;
        writer.append(draft)?;
        writer.flush()?;
        drop(writer);
        Ok(())
    }
}

/// Failure while creating or writing a run capture.
#[derive(Debug, Error)]
pub enum CaptureError {
    /// A source supplied a negative epoch or frame ordinal.
    #[error("invalid {field} in capture source metadata: {value}")]
    InvalidSequence {
        /// Name of the invalid sequence field.
        field: &'static str,
        /// Invalid sequence value.
        value: i64,
    },
    /// A normalized event has no original raw frame to preserve.
    #[error(
        "source {source_id} frame sequence {frame_sequence} (source ingest sequence {ingest_sequence}) did not provide original frame bytes"
    )]
    MissingRawFrame {
        /// ID of the source that omitted the original frame.
        source_id: String,
        /// Sequence number of the frame without original bytes.
        frame_sequence: i64,
        /// Source ingest sequence of the frame without original bytes.
        ingest_sequence: u64,
    },
    /// A historical frame has no original receipt-time provenance.
    #[error(
        "historical source {source_id} frame sequence {frame_sequence} has no original receipt time and cannot be captured"
    )]
    UnknownReceiptTime {
        /// ID of the source whose receipt time is unknown.
        source_id: String,
        /// Sequence number of the frame whose receipt time is unknown.
        frame_sequence: i64,
    },
    /// A gap's logical source identity disagrees with its reporting frame.
    #[error("gap source {source_id} does not match reporting source {metadata_source_id}")]
    GapSourceMismatch {
        /// Logical source identity declared by the gap.
        source_id: String,
        /// Source identity on the reporting frame metadata.
        metadata_source_id: String,
    },
    /// An unobserved gap contains reporting-frame provenance.
    #[error("unobserved gap for source {source_id} contains reporting-frame provenance")]
    UnobservedGapHasProvenance {
        /// Logical source identity declared by the gap.
        source_id: String,
    },
    /// Capture is unavailable after its writer lock was poisoned.
    #[error("capture writer lock poisoned")]
    LockPoisoned,
    /// The capture file could not be created or written.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// The raw capture tape rejected a record.
    #[error(transparent)]
    Tape(#[from] RawCaptureError),
}

pub(crate) fn capture_merged_source_signal(
    recorder: Option<&CaptureRecorder>,
    signal: &SourceSignal,
) -> Result<(), CaptureError> {
    if let Some(recorder) = recorder {
        recorder.record(signal)?;
    }
    Ok(())
}

fn capture_draft(signal: &SourceSignal) -> Result<Option<RawCaptureDraft>, CaptureError> {
    let draft = match signal {
        SourceSignal::Data(source) => frame_draft(source),
        SourceSignal::Gap(gap) => gap_draft(gap),
        SourceSignal::Heartbeat(heartbeat) => control_draft(
            &heartbeat.metadata,
            "heartbeat",
            heartbeat.raw_frame.clone(),
        ),
        SourceSignal::Watermark(_) | SourceSignal::Eof => return Ok(None),
    }?;
    Ok(Some(draft))
}

fn frame_draft(source: &SourceEnvelope) -> Result<RawCaptureDraft, CaptureError> {
    let source_metadata = source.metadata();
    reject_unknown_receipt_time(source_metadata)?;

    let (metadata, raw_bytes, projection) = match source {
        SourceEnvelope::PmMarket(envelope) => (
            &envelope.metadata,
            &envelope.raw_frame,
            pmkit_tape::market_envelope_json(envelope),
        ),
        SourceEnvelope::PmAccount(envelope) => (
            &envelope.metadata,
            &envelope.raw_frame,
            pmkit_tape::account_envelope_json(envelope),
        ),
        SourceEnvelope::CexReference(envelope) => (
            &envelope.metadata,
            &envelope.raw_frame,
            pmkit_tape::reference_envelope_json(envelope),
        ),
    };
    if raw_bytes.is_empty() {
        return Err(CaptureError::MissingRawFrame {
            source_id: metadata.source_id.clone(),
            frame_sequence: metadata.frame_sequence,
            ingest_sequence: metadata.ingest_sequence,
        });
    }
    draft(
        metadata,
        RawCapturePayload::Frame {
            raw_bytes: raw_bytes.clone(),
        },
        Some(projection),
        Some(metadata.source_time_ms),
    )
}

fn reject_unknown_receipt_time(metadata: &StreamMetadata) -> Result<(), CaptureError> {
    if matches!(
        metadata.source_id.as_str(),
        pmkit_data::BINANCE_VISION_SOURCE_ID | "pmkit-cloud"
    ) {
        return Err(CaptureError::UnknownReceiptTime {
            source_id: metadata.source_id.clone(),
            frame_sequence: metadata.frame_sequence,
        });
    }
    Ok(())
}

fn gap_draft(gap: &SourceGap) -> Result<RawCaptureDraft, CaptureError> {
    if let Some(metadata) = &gap.metadata {
        reject_unknown_receipt_time(metadata)?;
    }

    let payload = RawCapturePayload::Gap {
        range: gap.range.map(|range| GapRange {
            domain: match range.domain {
                SourceGapSequenceDomain::Transport => CaptureGapSequenceDomain::Transport,
                SourceGapSequenceDomain::Channel => CaptureGapSequenceDomain::Channel,
            },
            first_missing: range.first_missing,
            last_missing: range.last_missing,
        }),
        unresolved_count: gap.unresolved_count,
        reason: gap.reason.clone(),
        raw_frame: gap.raw_frame.clone(),
    };

    if let Some(metadata) = &gap.metadata {
        if gap.source_id != metadata.source_id {
            return Err(CaptureError::GapSourceMismatch {
                source_id: gap.source_id.clone(),
                metadata_source_id: metadata.source_id.clone(),
            });
        }
        draft(metadata, payload, None, None)
    } else {
        if gap.range.is_some() || !gap.raw_frame.is_empty() {
            return Err(CaptureError::UnobservedGapHasProvenance {
                source_id: gap.source_id.clone(),
            });
        }
        Ok(RawCaptureDraft {
            schema_version: 1,
            source_id: gap.source_id.clone(),
            canonical_source_rank: 0,
            receipt_time_ms: gap.detected_at_ms,
            transport: None,
            normalized_projection: None,
            payload,
        })
    }
}

fn control_draft(
    metadata: &StreamMetadata,
    name: &str,
    raw_bytes: Vec<u8>,
) -> Result<RawCaptureDraft, CaptureError> {
    reject_unknown_receipt_time(metadata)?;
    draft(
        metadata,
        RawCapturePayload::Control {
            name: name.to_owned(),
            raw_bytes,
        },
        None,
        None,
    )
}

fn draft(
    metadata: &StreamMetadata,
    payload: RawCapturePayload,
    normalized_projection: Option<serde_json::Value>,
    source_time_ms: Option<i64>,
) -> Result<RawCaptureDraft, CaptureError> {
    Ok(RawCaptureDraft {
        schema_version: metadata.schema_version,
        source_id: metadata.source_id.clone(),
        canonical_source_rank: metadata.canonical_source_rank,
        receipt_time_ms: metadata.receipt_time_ms,
        transport: Some(CaptureTransport {
            source_time_ms,
            connection_id: metadata.connection_id.clone(),
            connection_epoch: sequence("connection_epoch", metadata.connection_epoch)?,
            frame_sequence: sequence("frame_sequence", metadata.frame_sequence)?,
            ingest_sequence: metadata.ingest_sequence,
        }),
        normalized_projection,
        payload,
    })
}

fn sequence(field: &'static str, value: i64) -> Result<u64, CaptureError> {
    u64::try_from(value).map_err(|_| CaptureError::InvalidSequence { field, value })
}

#[cfg(test)]
mod tests {
    use super::{CaptureConfig, CaptureError, capture_draft};
    use pmkit_core::{MarketId, RunId};
    use pmkit_data::{
        GapRange as SourceGapRange, GapSequenceDomain, SourceGap, SourceHeartbeat, SourceSignal,
    };
    use pmkit_event::{
        CexReferenceEnvelope, CexReferenceEvent, MarketEvent, PmMarketEnvelope, SourceEnvelope,
        StreamMetadata,
    };
    use pmkit_market::{Asset, Outcome};
    use pmkit_tape::{
        GapSequenceDomain as CaptureGapSequenceDomain, RawCapturePayload, RawCaptureReader,
        RawCaptureWriter,
    };
    use rust_decimal::Decimal;
    use std::io::Cursor;

    fn metadata(sequence: i64) -> StreamMetadata {
        StreamMetadata {
            schema_version: 1,
            source_id: "oracle:test".into(),
            source_time_ms: 123,
            canonical_source_rank: 7,
            receipt_time_ms: 456,
            connection_id: "ws-1".into(),
            connection_epoch: 2,
            frame_sequence: sequence,
            ingest_sequence: 8,
        }
    }

    fn assert_unknown_receipt_refusal(
        signal: &SourceSignal,
        source_id: &str,
        frame_sequence: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let run = RunId::new("receipt-time-refusal")?;
        let config = CaptureConfig::new(directory.path());
        let path = config.path_for(&run);
        let recorder = config.open(&run)?;
        let Err(error) = recorder.record(signal) else {
            return Err(
                std::io::Error::other("reserved source receipt time must be rejected").into(),
            );
        };

        assert!(matches!(
            &error,
            CaptureError::UnknownReceiptTime {
                source_id: actual_source_id,
                frame_sequence: actual_frame_sequence,
            } if actual_source_id == source_id && *actual_frame_sequence == frame_sequence
        ));
        assert_eq!(
            error.to_string(),
            format!(
                "historical source {source_id} frame sequence {frame_sequence} has no original receipt time and cannot be captured"
            )
        );
        assert!(std::fs::read(path)?.is_empty());
        Ok(())
    }

    #[test]
    fn capture_roundtrip_keeps_original_bytes_and_receipt_time()
    -> Result<(), Box<dyn std::error::Error>> {
        let raw = br#"{ "price" : "1.25" }"#.to_vec();
        let signal = SourceSignal::Data(Box::new(SourceEnvelope::CexReference(
            CexReferenceEnvelope {
                metadata: metadata(4),
                raw_frame: raw.clone(),
                fact: CexReferenceEvent::Twap60sUpdate {
                    asset: Asset::Btc,
                    price: Decimal::new(125, 2),
                    timestamp_ms: 123,
                },
            },
        )));
        let draft = capture_draft(&signal)?.ok_or("missing frame draft")?;
        let mut writer = RawCaptureWriter::new(Vec::new());
        assert_eq!(writer.append(draft)?, 0);
        let bytes = writer.into_inner();
        let mut reader = RawCaptureReader::new(Cursor::new(bytes));
        let record = reader.next_record()?.ok_or("missing capture record")?;

        assert_eq!(record.ingest_sequence, 0);
        assert_eq!(record.capture.source_id, "oracle:test");
        assert_eq!(record.capture.schema_version, 1);
        assert_eq!(
            record
                .capture
                .transport
                .as_ref()
                .and_then(|transport| transport.source_time_ms),
            Some(123)
        );
        assert_eq!(record.capture.canonical_source_rank, 7);
        assert_eq!(record.capture.receipt_time_ms, 456);
        let transport = record
            .capture
            .transport
            .as_ref()
            .ok_or("missing transport")?;
        assert_eq!(transport.connection_id, "ws-1");
        assert_eq!(transport.connection_epoch, 2);
        assert_eq!(transport.frame_sequence, 4);
        assert_eq!(transport.ingest_sequence, 8);
        assert_eq!(
            record
                .capture
                .normalized_projection
                .as_ref()
                .and_then(|projection| projection["payload"]["kind"].as_str()),
            Some("reference_twap_60s_update")
        );
        assert_eq!(
            record
                .capture
                .normalized_projection
                .as_ref()
                .and_then(|projection| projection["payload"]["price"].as_str()),
            Some("1.25")
        );
        assert_eq!(
            record.capture.payload,
            RawCapturePayload::Frame { raw_bytes: raw }
        );
        assert!(reader.next_record()?.is_none());
        Ok(())
    }

    #[test]
    fn locally_detected_gap_is_written_with_no_reporting_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let gap = SourceSignal::Gap(SourceGap {
            source_id: "oracle:logical".into(),
            detected_at_ms: 789,
            metadata: None,
            raw_frame: Vec::new(),
            range: None,
            unresolved_count: Some(2),
            reason: "local timeout; no wire reporting frame".into(),
        });
        let mut writer = RawCaptureWriter::new(Vec::new());
        assert_eq!(
            writer.append(capture_draft(&gap)?.ok_or("missing gap")?)?,
            0
        );

        let bytes = writer.into_inner();
        let record: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(
            record["body"]["capture"]["payload"],
            serde_json::json!({
                "kind": "gap",
                "range": null,
                "unresolved_count": 2,
                "reason": "local timeout; no wire reporting frame",
                "raw_frame": []
            })
        );
        assert_eq!(record["body"]["capture"]["source_id"], "oracle:logical");
        assert_eq!(record["body"]["capture"]["receipt_time_ms"], 789);
        assert_eq!(
            record["body"]["capture"]["transport"],
            serde_json::Value::Null
        );
        Ok(())
    }

    #[test]
    fn empty_reporting_gap_keeps_its_witness_and_reported_count()
    -> Result<(), Box<dyn std::error::Error>> {
        let signal = SourceSignal::Gap(SourceGap {
            source_id: "oracle:test".into(),
            detected_at_ms: 789,
            metadata: Some(metadata(4)),
            raw_frame: Vec::new(),
            range: Some(SourceGapRange {
                domain: GapSequenceDomain::Channel,
                first_missing: 2,
                last_missing: 3,
            }),
            unresolved_count: Some(2),
            reason: "native empty gap report".into(),
        });
        let draft = capture_draft(&signal)?.ok_or("missing gap")?;
        let mut writer = RawCaptureWriter::new(Vec::new());
        writer.append(draft)?;
        let record: serde_json::Value = serde_json::from_slice(&writer.into_inner())?;

        assert_eq!(record["body"]["capture"]["source_id"], "oracle:test");
        assert_eq!(record["body"]["capture"]["receipt_time_ms"], 456);
        assert_eq!(
            record["body"]["capture"]["transport"]["source_time_ms"],
            serde_json::Value::Null
        );
        assert_eq!(
            record["body"]["capture"]["transport"]["connection_id"],
            "ws-1"
        );
        assert_eq!(
            record["body"]["capture"]["transport"]["connection_epoch"],
            2
        );
        assert_eq!(record["body"]["capture"]["transport"]["frame_sequence"], 4);
        assert_eq!(record["body"]["capture"]["payload"]["unresolved_count"], 2);
        assert_eq!(
            record["body"]["capture"]["payload"]["raw_frame"],
            serde_json::json!([])
        );
        Ok(())
    }

    #[test]
    fn unobserved_gap_cannot_claim_a_range_or_reporting_frame() {
        let signal = SourceSignal::Gap(SourceGap {
            source_id: "oracle:logical".into(),
            detected_at_ms: 789,
            metadata: None,
            raw_frame: Vec::new(),
            range: Some(SourceGapRange {
                domain: GapSequenceDomain::Transport,
                first_missing: 2,
                last_missing: 3,
            }),
            unresolved_count: Some(2),
            reason: "local uncertainty".into(),
        });

        assert!(matches!(
            capture_draft(&signal),
            Err(CaptureError::UnobservedGapHasProvenance { .. })
        ));
    }

    #[test]
    fn reporting_gap_source_must_match_its_frame() {
        let signal = SourceSignal::Gap(SourceGap {
            source_id: "oracle:logical".into(),
            detected_at_ms: 789,
            metadata: Some(metadata(4)),
            raw_frame: Vec::new(),
            range: None,
            unresolved_count: None,
            reason: "upstream gap".into(),
        });

        assert!(matches!(
            capture_draft(&signal),
            Err(CaptureError::GapSourceMismatch { .. })
        ));
    }

    #[test]
    fn binance_vision_capture_refuses_unknown_receipt_without_writing()
    -> Result<(), Box<dyn std::error::Error>> {
        let source_id = pmkit_data::BINANCE_VISION_SOURCE_ID;
        let mut vision_metadata = metadata(4);
        vision_metadata.source_id = source_id.into();
        let signal = SourceSignal::Data(Box::new(SourceEnvelope::CexReference(
            CexReferenceEnvelope {
                metadata: vision_metadata,
                raw_frame: b"2,1,1,2,2,1735689600000000,true,true".to_vec(),
                fact: CexReferenceEvent::Trade {
                    asset: Asset::Btc,
                    exchange: pmkit_market::Exchange::Binance,
                    aggregate_trade_id: 1,
                    price: Decimal::new(2, 0),
                    qty: Decimal::new(2, 0),
                    is_buyer_maker: true,
                    timestamp_ms: 1_735_689_600_000,
                },
            },
        )));

        let Err(error) = capture_draft(&signal) else {
            return Err(std::io::Error::other("vision receipt time must be rejected").into());
        };
        assert!(matches!(
            &error,
            CaptureError::UnknownReceiptTime {
                source_id: actual_source_id,
                frame_sequence: 4,
            } if actual_source_id == source_id
        ));
        assert_eq!(
            error.to_string(),
            "historical source binance:vision:aggTrades frame sequence 4 has no original receipt time and cannot be captured"
        );

        let directory = tempfile::tempdir()?;
        let run = RunId::new("vision-capture")?;
        let config = CaptureConfig::new(directory.path());
        let path = config.path_for(&run);
        let recorder = config.open(&run)?;
        assert!(matches!(
            recorder.record(&signal),
            Err(CaptureError::UnknownReceiptTime { .. })
        ));
        assert!(std::fs::read(&path)?.is_empty());
        Ok(())
    }

    #[test]
    fn cloud_historical_row_refuses_original_receipt_time_without_writing()
    -> Result<(), Box<dyn std::error::Error>> {
        let raw = br#"{"event_time_ms":123,"event_ordinal":7,"payload":{"event_type":"book","asset_id":"token-up","bids":[{"price":"0.49","size":"2"}],"asks":[{"price":"0.51","size":"3"}]}}"#.to_vec();
        let mut cloud_metadata = metadata(7);
        cloud_metadata.source_id = "pmkit-cloud".into();
        cloud_metadata.source_time_ms = 123;
        cloud_metadata.receipt_time_ms = 123;
        let signal = SourceSignal::Data(Box::new(SourceEnvelope::PmMarket(PmMarketEnvelope {
            metadata: cloud_metadata,
            raw_frame: raw,
            fact: MarketEvent::BookUpdate {
                market: MarketId::new("btc-5m")?,
                outcome: Outcome::Up,
                bids: vec![(Decimal::new(49, 2), Decimal::from(2))],
                asks: vec![(Decimal::new(51, 2), Decimal::from(3))],
                timestamp_ms: 123,
            },
        })));

        let Err(error) = capture_draft(&signal) else {
            return Err(std::io::Error::other("cloud receipt time must be rejected").into());
        };
        assert!(matches!(
            &error,
            CaptureError::UnknownReceiptTime {
                source_id,
                frame_sequence: 7,
            } if source_id == "pmkit-cloud"
        ));
        assert_eq!(
            error.to_string(),
            "historical source pmkit-cloud frame sequence 7 has no original receipt time and cannot be captured"
        );

        let directory = tempfile::tempdir()?;
        let run = RunId::new("cloud-capture")?;
        let config = CaptureConfig::new(directory.path());
        let path = config.path_for(&run);
        let recorder = config.open(&run)?;
        assert!(matches!(
            recorder.record(&signal),
            Err(CaptureError::UnknownReceiptTime {
                source_id,
                frame_sequence: 7,
            }) if source_id == "pmkit-cloud"
        ));
        assert!(std::fs::read(&path)?.is_empty());
        Ok(())
    }

    #[test]
    fn reserved_receipt_sources_refuse_heartbeats_before_writing()
    -> Result<(), Box<dyn std::error::Error>> {
        for (source_id, frame_sequence) in [
            (pmkit_data::BINANCE_VISION_SOURCE_ID, 31),
            ("pmkit-cloud", 32),
        ] {
            let mut heartbeat_metadata = metadata(frame_sequence);
            heartbeat_metadata.source_id = source_id.into();
            let signal = SourceSignal::Heartbeat(SourceHeartbeat {
                metadata: heartbeat_metadata,
                raw_frame: b"heartbeat wire frame".to_vec(),
            });

            assert_unknown_receipt_refusal(&signal, source_id, frame_sequence)?;
        }
        Ok(())
    }

    #[test]
    fn reserved_reporting_gap_sources_refuse_before_writing()
    -> Result<(), Box<dyn std::error::Error>> {
        for (source_id, frame_sequence) in [
            (pmkit_data::BINANCE_VISION_SOURCE_ID, 41),
            ("pmkit-cloud", 42),
        ] {
            let mut reporting_metadata = metadata(frame_sequence);
            reporting_metadata.source_id = source_id.into();
            let signal = SourceSignal::Gap(SourceGap {
                source_id: "oracle:registered".into(),
                detected_at_ms: 789,
                metadata: Some(reporting_metadata),
                raw_frame: b"reported gap frame".to_vec(),
                range: Some(SourceGapRange {
                    domain: GapSequenceDomain::Channel,
                    first_missing: 2,
                    last_missing: 3,
                }),
                unresolved_count: Some(2),
                reason: "upstream gap report".into(),
            });

            assert_unknown_receipt_refusal(&signal, source_id, frame_sequence)?;
        }
        Ok(())
    }

    #[test]
    fn empty_data_frame_is_rejected_with_source_sequences() -> Result<(), String> {
        let signal = SourceSignal::Data(Box::new(SourceEnvelope::CexReference(
            CexReferenceEnvelope {
                metadata: metadata(4),
                raw_frame: Vec::new(),
                fact: CexReferenceEvent::Twap60sUpdate {
                    asset: Asset::Btc,
                    price: Decimal::new(125, 2),
                    timestamp_ms: 123,
                },
            },
        )));

        let Err(error) = capture_draft(&signal) else {
            return Err("empty data frames must be rejected".to_owned());
        };
        assert!(matches!(&error, CaptureError::MissingRawFrame { .. }));
        assert_eq!(
            error.to_string(),
            "source oracle:test frame sequence 4 (source ingest sequence 8) did not provide original frame bytes"
        );
        Ok(())
    }

    #[test]
    fn gaps_and_heartbeats_are_captured_but_watermarks_are_not()
    -> Result<(), Box<dyn std::error::Error>> {
        let gap = SourceSignal::Gap(SourceGap {
            source_id: "oracle:test".into(),
            detected_at_ms: 789,
            metadata: Some(metadata(4)),
            raw_frame: b"reported frame".to_vec(),
            range: Some(SourceGapRange {
                domain: GapSequenceDomain::Channel,
                first_missing: 2,
                last_missing: 3,
            }),
            unresolved_count: Some(2),
            reason: "upstream gap".into(),
        });
        let gap = capture_draft(&gap)?.ok_or("missing gap")?;
        assert_eq!(gap.normalized_projection, None);
        assert_eq!(gap.source_id, "oracle:test");
        assert_eq!(gap.receipt_time_ms, 456);
        let transport = gap
            .transport
            .as_ref()
            .ok_or("missing reporting transport")?;
        assert_eq!(transport.source_time_ms, None);
        assert_eq!(transport.connection_id, "ws-1");
        assert_eq!(transport.connection_epoch, 2);
        assert_eq!(transport.frame_sequence, 4);
        assert_eq!(transport.ingest_sequence, 8);
        assert!(matches!(
            gap.payload,
            RawCapturePayload::Gap {
                range: Some(pmkit_tape::GapRange {
                    domain: CaptureGapSequenceDomain::Channel,
                    first_missing: 2,
                    last_missing: 3,
                }),
                unresolved_count: Some(2),
                ref raw_frame,
                ..
            } if raw_frame == b"reported frame"
        ));

        let heartbeat = SourceSignal::Heartbeat(pmkit_data::SourceHeartbeat {
            metadata: metadata(5),
            raw_frame: Vec::new(),
        });
        let heartbeat = capture_draft(&heartbeat)?.ok_or("missing heartbeat")?;
        assert_eq!(heartbeat.normalized_projection, None);
        assert_eq!(
            heartbeat
                .transport
                .as_ref()
                .map(|transport| transport.ingest_sequence),
            Some(8)
        );
        assert!(matches!(
            heartbeat.payload,
            RawCapturePayload::Control { ref name, ref raw_bytes }
                if name == "heartbeat" && raw_bytes.is_empty()
        ));
        assert!(capture_draft(&SourceSignal::Watermark(10))?.is_none());
        assert!(capture_draft(&SourceSignal::Eof)?.is_none());
        Ok(())
    }

    #[test]
    fn capture_paths_are_run_scoped() -> Result<(), Box<dyn std::error::Error>> {
        let config = CaptureConfig::new("captures");
        assert_eq!(
            config.path_for(&RunId::new("one")?),
            std::path::Path::new("captures/run-one.jsonl")
        );
        Ok(())
    }
}
