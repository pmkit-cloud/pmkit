use std::collections::HashMap;
use std::io::{self, BufRead, Write};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Schema version for generic run-wide raw capture records.
pub const RAW_CAPTURE_SCHEMA_VERSION: u16 = 4;

/// Which sequence domain an exact gap range uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GapSequenceDomain {
    /// The physical frame ordinal on the reporting connection.
    Transport,
    /// A source-level channel sequence, independent of transport frames.
    Channel,
}

/// An inclusive range of missing sequences in one domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GapRange {
    /// Numbering domain for this range.
    pub domain: GapSequenceDomain,
    /// First missing sequence, inclusive.
    pub first_missing: u64,
    /// Last missing sequence, inclusive.
    pub last_missing: u64,
}

/// Transport identity and counters for an actually observed source frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureTransport {
    /// Source timestamp in milliseconds, when supplied by the source.
    pub source_time_ms: Option<i64>,
    /// Physical connection that delivered or reported the observation.
    pub connection_id: String,
    /// Connection lifetime number, incremented on reconnect.
    pub connection_epoch: u64,
    /// Frame ordinal within the connection lifetime.
    pub frame_sequence: u64,
    /// Source-local sequence, distinct from the tape's run-wide ingest sequence.
    pub ingest_sequence: u64,
}

/// One source observation before it is assigned a run-wide ingest sequence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawCaptureDraft {
    /// Source envelope schema version.
    pub schema_version: u16,
    /// Canonical source identity, or logical subscription identity if not yet observed.
    pub source_id: String,
    /// Deterministic rank used for canonical replay ordering.
    pub canonical_source_rank: i64,
    /// Real local receipt/detection timestamp in milliseconds.
    pub receipt_time_ms: i64,
    /// Real transport metadata for an observed frame; absent for a local, unwitnessed gap.
    pub transport: Option<CaptureTransport>,
    /// Optional normalized fact witness; exact source bytes remain in `payload`.
    pub normalized_projection: Option<serde_json::Value>,
    /// Exact raw frame bytes or an explicit capture control record.
    pub payload: RawCapturePayload,
}

/// Opaque raw data and control records accepted by the generic capture tape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RawCapturePayload {
    /// Original bytes of a source frame.
    Frame {
        /// Exact bytes before adaptation or decoding.
        raw_bytes: Vec<u8>,
    },
    /// Original bytes of a source snapshot, distinguished from incremental frames.
    Snapshot {
        /// Exact bytes before adaptation or decoding.
        raw_bytes: Vec<u8>,
    },
    /// A source-reported or locally detected discontinuity.
    Gap {
        /// Exact missing range and its independent sequence domain, when known.
        range: Option<GapRange>,
        /// Reported number of missing items when no exact range is available.
        unresolved_count: Option<u64>,
        /// Reason the discontinuity was detected.
        reason: String,
        /// Exact bytes of the reporting frame; may be empty for a real observed frame.
        raw_frame: Vec<u8>,
    },
    /// A non-data transport or run control observation.
    Control {
        /// Stable descriptive control name.
        name: String,
        /// Exact original control bytes, if any.
        raw_bytes: Vec<u8>,
    },
}

/// One checksummed record in a run-wide capture tape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawCaptureRecord {
    /// Monotonic sequence assigned in writer append order, starting at zero.
    pub ingest_sequence: u64,
    /// Zero-based index of this fact projection within its source frame.
    pub projection_index: u64,
    /// Source observation and original payload.
    pub capture: RawCaptureDraft,
}

/// Failure while writing, replaying, or recovering a raw capture tape.
#[derive(Debug, Error)]
pub enum RawCaptureError {
    /// A record has no terminating newline.
    #[error("incomplete raw capture record")]
    IncompleteTail,
    /// A record uses an unsupported schema version.
    #[error("unsupported raw capture schema version {found}")]
    UnsupportedSchemaVersion {
        /// Version declared by the record.
        found: u16,
    },
    /// A complete record is malformed or violates the capture contract.
    #[error("malformed raw capture record: {message}")]
    MalformedRecord {
        /// Decode or validation detail.
        message: String,
    },
    /// A complete record's canonical body does not match its SHA-256 digest.
    #[error("raw capture checksum mismatch at ingest sequence {ingest_sequence}")]
    ChecksumMismatch {
        /// Sequence declared by the corrupt record.
        ingest_sequence: u64,
    },
    /// A run-wide ingest sequence is absent, duplicated, or reordered.
    #[error("raw capture ingest sequence mismatch: expected {expected}, found {found}")]
    IngestSequenceMismatch {
        /// Next expected run-wide sequence.
        expected: u64,
        /// Sequence found in the record.
        found: u64,
    },
    /// A source frame sequence jumped without an explicit gap record.
    #[error(
        "raw capture source sequence gap for {source_id}/{connection_id} epoch {connection_epoch}: expected {expected}, found {found}"
    )]
    SourceSequenceGap {
        /// Stable source identity.
        source_id: String,
        /// Physical connection identity.
        connection_id: String,
        /// Connection epoch.
        connection_epoch: u64,
        /// Next expected source frame sequence.
        expected: u64,
        /// Sequence observed in the record.
        found: u64,
    },
    /// A recorded gap is surfaced as a fail-closed replay error.
    #[error(
        "raw capture gap at ingest sequence {ingest_sequence} for {source_id}: range {range:?}, unresolved count {unresolved_count:?}: {reason}"
    )]
    Gap {
        /// Run-wide record sequence containing the gap.
        ingest_sequence: u64,
        /// Stable source identity.
        source_id: String,
        /// Exact missing range, when reported.
        range: Option<GapRange>,
        /// Reported loss count when no exact range is available.
        unresolved_count: Option<u64>,
        /// Reason the discontinuity was detected.
        reason: String,
    },
    /// A writer cannot continue after an earlier partial I/O failure.
    #[error("raw capture writer is poisoned after an I/O failure")]
    WriterPoisoned,
    /// A reader cannot continue after an earlier validation failure.
    #[error("raw capture reader is poisoned after a validation failure")]
    ReaderPoisoned,
    /// A recovered partial tail must be followed by an explicit gap record.
    #[error("recovered raw capture requires an explicit gap record before more data")]
    RecoveryGapRequired,
    /// Sequence arithmetic exceeded its supported range.
    #[error("raw capture sequence exhausted")]
    SequenceExhausted,
    /// An underlying reader or writer failed.
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Append-only writer assigning one sequence across all sources in a run.
#[derive(Debug)]
pub struct RawCaptureWriter<W: Write> {
    writer: W,
    validator: CaptureValidator,
    recovery_gap_required: bool,
    failed: bool,
}

impl<W: Write> RawCaptureWriter<W> {
    /// Creates a new tape whose run-wide sequence starts at zero.
    #[must_use]
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            validator: CaptureValidator::default(),
            recovery_gap_required: false,
            failed: false,
        }
    }

    /// Resumes after `recovery`; the caller must truncate the writer to `valid_bytes()` first.
    ///
    /// If recovery discarded a partial tail, the next append must explicitly record the
    /// resulting uncertainty as a gap.
    #[must_use]
    pub fn resume(writer: W, recovery: RawCaptureRecovery) -> Self {
        let recovery_gap_required = recovery.discarded_tail_bytes > 0;
        Self {
            writer,
            validator: recovery.validator,
            recovery_gap_required,
            failed: false,
        }
    }

    /// Appends one observation in call order and returns its assigned run sequence.
    ///
    /// # Errors
    ///
    /// Returns [`RawCaptureError`] for invalid source sequencing or an underlying write failure.
    pub fn append(&mut self, capture: RawCaptureDraft) -> Result<u64, RawCaptureError> {
        if self.failed {
            return Err(RawCaptureError::WriterPoisoned);
        }
        if self.recovery_gap_required && !matches!(capture.payload, RawCapturePayload::Gap { .. }) {
            return Err(RawCaptureError::RecoveryGapRequired);
        }
        let record = RawCaptureRecord {
            ingest_sequence: self.validator.next_ingest_sequence,
            projection_index: self.validator.expected_projection_index(&capture)?,
            capture,
        };
        self.validator.check_record(&record, false)?;
        let encoded = encode_record(&record)?;
        if let Err(error) = self
            .writer
            .write_all(&encoded)
            .and_then(|()| self.writer.write_all(b"\n"))
        {
            self.failed = true;
            return Err(RawCaptureError::Io(error));
        }
        self.validator.commit_record(&record)?;
        self.recovery_gap_required = false;
        Ok(record.ingest_sequence)
    }

    /// Flushes buffered bytes to the underlying writer.
    ///
    /// # Errors
    ///
    /// Returns [`RawCaptureError`] when a prior write failed or flushing fails.
    pub fn flush(&mut self) -> Result<(), RawCaptureError> {
        if self.failed {
            return Err(RawCaptureError::WriterPoisoned);
        }
        if let Err(error) = self.writer.flush() {
            self.failed = true;
            return Err(RawCaptureError::Io(error));
        }
        Ok(())
    }

    /// Consumes the tape and returns its underlying writer.
    #[must_use]
    pub fn into_inner(self) -> W {
        self.writer
    }
}

/// Checked reader that refuses incomplete, corrupt, discontinuous, or gapped replay.
#[derive(Debug)]
pub struct RawCaptureReader<R: BufRead> {
    reader: R,
    validator: CaptureValidator,
    reject_gaps: bool,
    failed: bool,
    eof: bool,
}

impl<R: BufRead> RawCaptureReader<R> {
    /// Creates a strict reader over one capture tape.
    #[must_use]
    pub fn new(reader: R) -> Self {
        Self::with_gap_policy(reader, true)
    }

    /// Creates an audit reader that returns explicit gap records and continues validation.
    #[must_use]
    pub fn new_audit(reader: R) -> Self {
        Self::with_gap_policy(reader, false)
    }

    fn with_gap_policy(reader: R, reject_gaps: bool) -> Self {
        Self {
            reader,
            validator: CaptureValidator::default(),
            reject_gaps,
            failed: false,
            eof: false,
        }
    }

    /// Reads the next verified record, or `None` after clean EOF.
    ///
    /// # Errors
    ///
    /// Returns [`RawCaptureError`] on truncation, checksum/sequence failure, or an explicit gap.
    pub fn next_record(&mut self) -> Result<Option<RawCaptureRecord>, RawCaptureError> {
        if self.failed {
            return Err(RawCaptureError::ReaderPoisoned);
        }
        if self.eof {
            return Ok(None);
        }
        let mut line = Vec::new();
        let read = match self.reader.read_until(b'\n', &mut line) {
            Ok(read) => read,
            Err(error) => {
                self.failed = true;
                return Err(RawCaptureError::Io(error));
            }
        };
        if read == 0 {
            self.eof = true;
            return Ok(None);
        }
        if !line.ends_with(b"\n") {
            self.failed = true;
            return Err(RawCaptureError::IncompleteTail);
        }
        let record = match decode_record(&line) {
            Ok(record) => record,
            Err(error) => {
                self.failed = true;
                return Err(error);
            }
        };
        if let Err(error) = self.validator.check_record(&record, self.reject_gaps) {
            self.failed = true;
            return Err(error);
        }
        if let Err(error) = self.validator.commit_record(&record) {
            self.failed = true;
            return Err(error);
        }
        Ok(Some(record))
    }
}

/// Valid prefix and uncertainty found while recovering an interrupted open tape.
#[derive(Debug)]
pub struct RawCaptureRecovery {
    valid_bytes: usize,
    discarded_tail_bytes: usize,
    validator: CaptureValidator,
}

impl RawCaptureRecovery {
    /// Byte offset to retain before reopening the tape for append.
    #[must_use]
    pub const fn valid_bytes(&self) -> usize {
        self.valid_bytes
    }

    /// Number of bytes discarded from an unterminated final record.
    #[must_use]
    pub const fn discarded_tail_bytes(&self) -> usize {
        self.discarded_tail_bytes
    }

    /// Next run-wide ingest sequence after the validated prefix.
    #[must_use]
    pub const fn next_ingest_sequence(&self) -> u64 {
        self.validator.next_ingest_sequence
    }
}

/// Validates complete records, including explicit gaps, and identifies an unterminated final tail.
///
/// A checksum, sequence, or malformed-record failure is never recoverable.
///
/// # Errors
///
/// Returns [`RawCaptureError`] when any complete record is invalid.
pub fn recover_raw_capture_prefix(bytes: &[u8]) -> Result<RawCaptureRecovery, RawCaptureError> {
    let mut validator = CaptureValidator::default();
    let mut valid_bytes = 0;
    let mut discarded_tail_bytes = 0;
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        if !line.ends_with(b"\n") {
            discarded_tail_bytes = line.len();
            break;
        }
        let record = decode_record(line)?;
        validator.check_record(&record, false)?;
        validator.commit_record(&record)?;
        valid_bytes += line.len();
    }
    Ok(RawCaptureRecovery {
        valid_bytes,
        discarded_tail_bytes,
        validator,
    })
}

#[derive(Debug, Default)]
struct CaptureValidator {
    next_ingest_sequence: u64,
    next_source_sequences: HashMap<SourceConnection, u64>,
    last_projections: HashMap<SourceConnection, ProjectionFrame>,
}

impl CaptureValidator {
    fn check_record(
        &self,
        record: &RawCaptureRecord,
        reject_gap: bool,
    ) -> Result<(), RawCaptureError> {
        let capture = &record.capture;
        if record.ingest_sequence != self.next_ingest_sequence {
            return Err(RawCaptureError::IngestSequenceMismatch {
                expected: self.next_ingest_sequence,
                found: record.ingest_sequence,
            });
        }
        self.check_record_envelope(record)?;
        match &capture.payload {
            RawCapturePayload::Gap {
                range,
                unresolved_count,
                reason,
                raw_frame,
            } => {
                if reason.is_empty()
                    || range.is_some_and(|range| range.first_missing > range.last_missing)
                {
                    return Err(RawCaptureError::MalformedRecord {
                        message: "invalid gap range or reason".to_owned(),
                    });
                }
                match (capture.transport.as_ref(), range) {
                    (None, None) if raw_frame.is_empty() => {}
                    (None, _) => {
                        return Err(RawCaptureError::MalformedRecord {
                            message: "unobserved gaps require an empty raw frame and no range"
                                .to_owned(),
                        });
                    }
                    (Some(transport), range) => {
                        if transport.connection_id.is_empty() {
                            return Err(RawCaptureError::MalformedRecord {
                                message: "connection_id must be non-empty".to_owned(),
                            });
                        }
                        let source = source_connection(capture).ok_or_else(|| {
                            RawCaptureError::MalformedRecord {
                                message: "gap transport metadata is incomplete".to_owned(),
                            }
                        })?;
                        if let Some(GapRange {
                            domain: GapSequenceDomain::Transport,
                            first_missing,
                            last_missing,
                        }) = range
                        {
                            if let Some(expected) = self.next_source_sequences.get(&source)
                                && *first_missing != *expected
                            {
                                return Err(source_gap_error(&source, *expected, *first_missing));
                            }
                            let reporting_sequence = last_missing
                                .checked_add(1)
                                .ok_or(RawCaptureError::SequenceExhausted)?;
                            if transport.frame_sequence != reporting_sequence {
                                return Err(RawCaptureError::MalformedRecord {
                                    message: "transport gap reporting frame must immediately follow its range"
                                        .to_owned(),
                                });
                            }
                            transport
                                .frame_sequence
                                .checked_add(1)
                                .ok_or(RawCaptureError::SequenceExhausted)?;
                        } else {
                            self.check_frame_sequence(capture, &source)?;
                        }
                    }
                }
                if reject_gap {
                    return Err(RawCaptureError::Gap {
                        ingest_sequence: record.ingest_sequence,
                        source_id: capture.source_id.clone(),
                        range: *range,
                        unresolved_count: *unresolved_count,
                        reason: reason.clone(),
                    });
                }
            }
            RawCapturePayload::Control { name, .. } => {
                if name.is_empty() {
                    return Err(RawCaptureError::MalformedRecord {
                        message: "control name must be non-empty".to_owned(),
                    });
                }
                let source =
                    source_connection(capture).ok_or_else(|| RawCaptureError::MalformedRecord {
                        message: "control records require transport metadata".to_owned(),
                    })?;
                self.check_frame_sequence(capture, &source)?;
            }
            RawCapturePayload::Frame { .. } | RawCapturePayload::Snapshot { .. } => {}
        }
        self.next_ingest_sequence
            .checked_add(1)
            .ok_or(RawCaptureError::SequenceExhausted)?;
        Ok(())
    }

    fn check_record_envelope(&self, record: &RawCaptureRecord) -> Result<(), RawCaptureError> {
        let capture = &record.capture;
        if capture.source_id.is_empty() {
            return Err(RawCaptureError::MalformedRecord {
                message: "source_id must be non-empty".to_owned(),
            });
        }
        if !matches!(capture.payload, RawCapturePayload::Gap { .. }) {
            let Some(transport) = capture.transport.as_ref() else {
                return Err(RawCaptureError::MalformedRecord {
                    message: "frame, snapshot, and control records require transport metadata"
                        .to_owned(),
                });
            };
            if transport.connection_id.is_empty() {
                return Err(RawCaptureError::MalformedRecord {
                    message: "connection_id must be non-empty".to_owned(),
                });
            }
        }
        let expected_projection_index = self.expected_projection_index(capture)?;
        if record.projection_index != expected_projection_index {
            return Err(RawCaptureError::MalformedRecord {
                message: format!(
                    "projection index mismatch: expected {expected_projection_index}, found {}",
                    record.projection_index
                ),
            });
        }
        Ok(())
    }

    fn expected_projection_index(&self, capture: &RawCaptureDraft) -> Result<u64, RawCaptureError> {
        let Some((kind, raw_bytes)) = frame_payload(capture) else {
            return Ok(0);
        };
        let source =
            source_connection(capture).ok_or_else(|| RawCaptureError::MalformedRecord {
                message: "frame and snapshot records require transport metadata".to_owned(),
            })?;
        let transport =
            capture
                .transport
                .as_ref()
                .ok_or_else(|| RawCaptureError::MalformedRecord {
                    message: "frame and snapshot records require transport metadata".to_owned(),
                })?;
        if let Some(previous) = self.last_projections.get(&source)
            && previous.frame_sequence == transport.frame_sequence
        {
            let raw_hash = <[u8; 32]>::from(Sha256::digest(raw_bytes));
            if previous.receipt_time_ms != capture.receipt_time_ms
                || previous.kind != kind
                || previous.raw_hash != raw_hash
            {
                return Err(RawCaptureError::MalformedRecord {
                    message: "duplicate frame sequence has mismatched receipt time or raw frame"
                        .to_owned(),
                });
            }
            let Some(projection) = &capture.normalized_projection else {
                return Err(RawCaptureError::MalformedRecord {
                    message: "repeated frame requires a distinct normalized projection".to_owned(),
                });
            };
            if previous.witnesses.iter().any(Option::is_none)
                || previous
                    .witnesses
                    .iter()
                    .any(|witness| witness.as_ref() == Some(projection))
            {
                return Err(RawCaptureError::MalformedRecord {
                    message: "frame projection is missing or duplicated".to_owned(),
                });
            }
            return u64::try_from(previous.witnesses.len())
                .map_err(|_| RawCaptureError::SequenceExhausted);
        }
        self.check_frame_sequence(capture, &source)?;
        Ok(0)
    }

    fn check_frame_sequence(
        &self,
        capture: &RawCaptureDraft,
        source: &SourceConnection,
    ) -> Result<(), RawCaptureError> {
        let transport =
            capture
                .transport
                .as_ref()
                .ok_or_else(|| RawCaptureError::MalformedRecord {
                    message: "observed records require transport metadata".to_owned(),
                })?;
        if let Some(expected) = self.next_source_sequences.get(source)
            && transport.frame_sequence != *expected
        {
            return Err(source_gap_error(
                source,
                *expected,
                transport.frame_sequence,
            ));
        }
        transport
            .frame_sequence
            .checked_add(1)
            .ok_or(RawCaptureError::SequenceExhausted)?;
        Ok(())
    }

    fn commit_record(&mut self, record: &RawCaptureRecord) -> Result<(), RawCaptureError> {
        self.next_ingest_sequence = record
            .ingest_sequence
            .checked_add(1)
            .ok_or(RawCaptureError::SequenceExhausted)?;
        let capture = &record.capture;
        let source = source_connection(capture);
        let next_source_sequence = match &capture.payload {
            RawCapturePayload::Gap { .. } => {
                if let Some(source) = source.as_ref() {
                    self.last_projections.remove(source);
                    Some(
                        capture
                            .transport
                            .as_ref()
                            .ok_or_else(|| RawCaptureError::MalformedRecord {
                                message: "gap transport metadata is incomplete".to_owned(),
                            })?
                            .frame_sequence
                            .checked_add(1)
                            .ok_or(RawCaptureError::SequenceExhausted)?,
                    )
                } else {
                    None
                }
            }
            RawCapturePayload::Control { .. } => {
                let source = source
                    .as_ref()
                    .ok_or_else(|| RawCaptureError::MalformedRecord {
                        message: "control records require transport metadata".to_owned(),
                    })?;
                self.last_projections.remove(source);
                Some(
                    capture
                        .transport
                        .as_ref()
                        .ok_or_else(|| RawCaptureError::MalformedRecord {
                            message: "control records require transport metadata".to_owned(),
                        })?
                        .frame_sequence
                        .checked_add(1)
                        .ok_or(RawCaptureError::SequenceExhausted)?,
                )
            }
            RawCapturePayload::Frame { .. } | RawCapturePayload::Snapshot { .. } => {
                let source = source
                    .as_ref()
                    .ok_or_else(|| RawCaptureError::MalformedRecord {
                        message: "data records require transport metadata".to_owned(),
                    })?;
                let Some((kind, raw_bytes)) = frame_payload(capture) else {
                    return Err(RawCaptureError::MalformedRecord {
                        message: "data record has no frame payload".to_owned(),
                    });
                };
                let frame_sequence = capture
                    .transport
                    .as_ref()
                    .ok_or_else(|| RawCaptureError::MalformedRecord {
                        message: "data records require transport metadata".to_owned(),
                    })?
                    .frame_sequence;
                if let Some(previous) = self.last_projections.get_mut(source)
                    && previous.frame_sequence == frame_sequence
                {
                    previous
                        .witnesses
                        .push(capture.normalized_projection.clone());
                } else {
                    self.last_projections.insert(
                        source.clone(),
                        ProjectionFrame {
                            frame_sequence,
                            receipt_time_ms: capture.receipt_time_ms,
                            kind,
                            raw_hash: <[u8; 32]>::from(Sha256::digest(raw_bytes)),
                            witnesses: vec![capture.normalized_projection.clone()],
                        },
                    );
                }
                Some(
                    frame_sequence
                        .checked_add(1)
                        .ok_or(RawCaptureError::SequenceExhausted)?,
                )
            }
        };
        if let (Some(source), Some(next_source_sequence)) = (source, next_source_sequence) {
            self.next_source_sequences
                .insert(source, next_source_sequence);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ProjectionFrame {
    frame_sequence: u64,
    receipt_time_ms: i64,
    kind: FrameKind,
    raw_hash: [u8; 32],
    witnesses: Vec<Option<serde_json::Value>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FrameKind {
    Frame,
    Snapshot,
}

fn source_connection(capture: &RawCaptureDraft) -> Option<SourceConnection> {
    let transport = capture.transport.as_ref()?;
    Some(SourceConnection {
        source_id: capture.source_id.clone(),
        connection_id: transport.connection_id.clone(),
        connection_epoch: transport.connection_epoch,
    })
}

fn frame_payload(capture: &RawCaptureDraft) -> Option<(FrameKind, &[u8])> {
    match &capture.payload {
        RawCapturePayload::Frame { raw_bytes } => Some((FrameKind::Frame, raw_bytes)),
        RawCapturePayload::Snapshot { raw_bytes } => Some((FrameKind::Snapshot, raw_bytes)),
        RawCapturePayload::Gap { .. } | RawCapturePayload::Control { .. } => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SourceConnection {
    source_id: String,
    connection_id: String,
    connection_epoch: u64,
}

fn source_gap_error(source: &SourceConnection, expected: u64, found: u64) -> RawCaptureError {
    RawCaptureError::SourceSequenceGap {
        source_id: source.source_id.clone(),
        connection_id: source.connection_id.clone(),
        connection_epoch: source.connection_epoch,
        expected,
        found,
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRecordBody {
    schema_version: u16,
    ingest_sequence: u64,
    projection_index: u64,
    capture: RawCaptureDraft,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRecord {
    body: WireRecordBody,
    checksum_sha256: String,
}

fn encode_record(record: &RawCaptureRecord) -> Result<Vec<u8>, RawCaptureError> {
    let body = WireRecordBody {
        schema_version: RAW_CAPTURE_SCHEMA_VERSION,
        ingest_sequence: record.ingest_sequence,
        projection_index: record.projection_index,
        capture: record.capture.clone(),
    };
    let body_bytes =
        serde_json::to_vec(&body).map_err(|error| RawCaptureError::MalformedRecord {
            message: error.to_string(),
        })?;
    serde_json::to_vec(&WireRecord {
        body,
        checksum_sha256: checksum_hex(&body_bytes),
    })
    .map_err(|error| RawCaptureError::MalformedRecord {
        message: error.to_string(),
    })
}

fn decode_record(line: &[u8]) -> Result<RawCaptureRecord, RawCaptureError> {
    if !line.ends_with(b"\n") {
        return Err(RawCaptureError::IncompleteTail);
    }
    let wire_value: serde_json::Value =
        serde_json::from_slice(line).map_err(|error| RawCaptureError::MalformedRecord {
            message: error.to_string(),
        })?;
    let schema_version = wire_value
        .get("body")
        .and_then(|body| body.get("schema_version"))
        .and_then(serde_json::Value::as_u64)
        .and_then(|version| u16::try_from(version).ok())
        .ok_or_else(|| RawCaptureError::MalformedRecord {
            message: "missing or invalid tape schema version".to_owned(),
        })?;
    if schema_version != RAW_CAPTURE_SCHEMA_VERSION {
        return Err(RawCaptureError::UnsupportedSchemaVersion {
            found: schema_version,
        });
    }
    let wire: WireRecord =
        serde_json::from_value(wire_value).map_err(|error| RawCaptureError::MalformedRecord {
            message: error.to_string(),
        })?;
    let ingest_sequence = wire.body.ingest_sequence;
    let body_bytes =
        serde_json::to_vec(&wire.body).map_err(|error| RawCaptureError::MalformedRecord {
            message: error.to_string(),
        })?;
    if checksum_hex(&body_bytes) != wire.checksum_sha256 {
        return Err(RawCaptureError::ChecksumMismatch { ingest_sequence });
    }
    Ok(RawCaptureRecord {
        ingest_sequence,
        projection_index: wire.body.projection_index,
        capture: wire.body.capture,
    })
}

fn checksum_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
