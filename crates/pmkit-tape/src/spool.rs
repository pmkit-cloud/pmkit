use std::path::{Path, PathBuf};

use serde::Serialize;
use thiserror::Error;

/// Schema version for redundant raw-spool records and checksum sidecars.
pub const RAW_SPOOL_SCHEMA_VERSION: u16 = 1;
const MINUTE_MS: i64 = 60_000;

/// The portable identity of one replica/shard UTC-minute spool chunk.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct SpoolChunk {
    replica_id: String,
    shard_id: String,
    minute_start_ms: i64,
}

impl SpoolChunk {
    /// Creates a chunk identity from caller-configured lane identifiers.
    ///
    /// # Errors
    ///
    /// Returns [`SpoolError`] when an identifier is not portable or the minute is unaligned.
    pub fn new(
        replica_id: impl Into<String>,
        shard_id: impl Into<String>,
        minute_start_ms: i64,
    ) -> Result<Self, SpoolError> {
        let replica_id = replica_id.into();
        let shard_id = shard_id.into();
        validate_identifier("replica_id", &replica_id)?;
        validate_identifier("shard_id", &shard_id)?;
        if minute_start_ms.rem_euclid(MINUTE_MS) != 0 {
            return Err(SpoolError::MinuteNotAligned { minute_start_ms });
        }
        Ok(Self {
            replica_id,
            shard_id,
            minute_start_ms,
        })
    }

    /// Static replica identity for this chunk.
    #[must_use]
    pub fn replica_id(&self) -> &str {
        &self.replica_id
    }

    /// Physical connection shard identity for this chunk.
    #[must_use]
    pub fn shard_id(&self) -> &str {
        &self.shard_id
    }

    /// UTC minute start as Unix milliseconds.
    #[must_use]
    pub const fn minute_start_ms(&self) -> i64 {
        self.minute_start_ms
    }

    /// Path of the mutable, recoverable chunk file beneath `root`.
    #[must_use]
    pub fn open_path(&self, root: &Path) -> PathBuf {
        self.directory(root).join(format!("{}.open", self.stem()))
    }

    /// Path of the immutable closed chunk file beneath `root`.
    #[must_use]
    pub fn closed_path(&self, root: &Path) -> PathBuf {
        self.directory(root).join(format!("{}.jsonl", self.stem()))
    }

    /// Path of the checksum sidecar for the closed chunk beneath `root`.
    #[must_use]
    pub fn checksum_path(&self, root: &Path) -> PathBuf {
        self.directory(root)
            .join(format!("{}.jsonl.sha256", self.stem()))
    }

    pub(super) fn from_filename(
        replica_id: &str,
        shard_id: &str,
        name: &str,
    ) -> Result<Self, SpoolError> {
        let stem = name
            .strip_suffix(".jsonl")
            .ok_or_else(|| SpoolError::MalformedChunkName {
                name: name.to_owned(),
            })?;
        let minute_start_ms = stem
            .strip_prefix("minute-")
            .ok_or_else(|| SpoolError::MalformedChunkName {
                name: name.to_owned(),
            })?
            .parse()
            .map_err(|_| SpoolError::MalformedChunkName {
                name: name.to_owned(),
            })?;
        Self::new(replica_id, shard_id, minute_start_ms)
    }

    fn directory(&self, root: &Path) -> PathBuf {
        root.join(&self.replica_id).join(&self.shard_id)
    }

    fn stem(&self) -> String {
        format!("minute-{}", self.minute_start_ms)
    }
}

/// One original binary frame plus its independent-connection provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpoolFrame {
    /// Physical connection lifetime number, incremented on reconnect.
    pub connection_epoch: u64,
    /// Frame ordinal within the connection lifetime.
    pub frame_sequence: u64,
    /// Local Unix receipt time in milliseconds.
    pub receipt_time_ms: i64,
    /// SHA-256 digest of the complete discovery snapshot used by the lane.
    pub discovery_snapshot_sha256: String,
    /// Exact source bytes before adaptation or decoding.
    pub raw_bytes: Vec<u8>,
}

impl SpoolFrame {
    /// Creates a frame record; the writer validates its snapshot digest and minute.
    #[must_use]
    pub fn new(
        connection_epoch: u64,
        frame_sequence: u64,
        receipt_time_ms: i64,
        discovery_snapshot_sha256: impl Into<String>,
        raw_bytes: Vec<u8>,
    ) -> Self {
        Self {
            connection_epoch,
            frame_sequence,
            receipt_time_ms,
            discovery_snapshot_sha256: discovery_snapshot_sha256.into(),
            raw_bytes,
        }
    }
}

/// Typed uncertainty produced when a process dies while appending a line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryUncertainty {
    /// The unterminated tail was discarded because it could be partially written.
    InterruptedFinalLine {
        /// Number of discarded tail bytes.
        discarded_bytes: u64,
    },
}

/// Result of validating and repairing one mutable chunk file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TailRecovery {
    /// Exact prefix length retained after recovery.
    pub valid_bytes: u64,
    /// Uncertainty about a discarded partial tail, if one was present.
    pub uncertainty: Option<RecoveryUncertainty>,
}

/// A checkpoint identity that can only be created from a verified closed chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpoolCheckpoint {
    pub(crate) chunk: SpoolChunk,
    pub(crate) path: PathBuf,
    pub(crate) byte_length: u64,
    pub(crate) sha256_hex: String,
}

impl SpoolCheckpoint {
    /// Closed chunk identity covered by this checkpoint.
    #[must_use]
    pub const fn chunk(&self) -> &SpoolChunk {
        &self.chunk
    }

    /// Closed chunk path that was verified.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Exact verified byte length.
    #[must_use]
    pub const fn byte_length(&self) -> u64 {
        self.byte_length
    }

    /// SHA-256 binding the chunk identity and exact bytes.
    #[must_use]
    pub fn sha256_hex(&self) -> &str {
        &self.sha256_hex
    }
}

/// Failure while writing, recovering, or verifying a raw spool.
#[derive(Debug, Error)]
pub enum SpoolError {
    /// A caller-supplied lane identifier cannot safely become a path component.
    #[error("invalid {field}: {value}")]
    InvalidIdentifier {
        /// Identifier field name.
        field: &'static str,
        /// Rejected identifier value.
        value: String,
    },
    /// A requested chunk boundary is not an exact UTC-minute boundary.
    #[error("minute is not aligned: {minute_start_ms}")]
    MinuteNotAligned {
        /// Requested minute start in Unix milliseconds.
        minute_start_ms: i64,
    },
    /// A record receipt does not belong to the writer's chunk minute.
    #[error("record receipt {receipt_time_ms} is outside chunk {minute_start_ms}")]
    RecordOutsideChunk {
        /// Frame receipt time in Unix milliseconds.
        receipt_time_ms: i64,
        /// Expected chunk minute start in Unix milliseconds.
        minute_start_ms: i64,
    },
    /// A discovery snapshot digest is not a lowercase SHA-256 hex string.
    #[error("invalid discovery snapshot digest")]
    InvalidDiscoveryDigest,
    /// A previously closed chunk may never be reopened for mutation.
    #[error("closed chunk already exists: {path}")]
    ClosedChunkExists {
        /// Existing immutable chunk path.
        path: PathBuf,
    },
    /// A filesystem operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A complete record cannot be decoded or validated.
    #[error("malformed spool record: {message}")]
    MalformedRecord {
        /// Decode or validation detail.
        message: String,
    },
    /// A raw spool record declares an unsupported schema version.
    #[error("unsupported raw spool schema version {found}")]
    UnsupportedSchemaVersion {
        /// Schema version found in the record.
        found: u16,
    },
    /// A closed chunk has an incomplete final line.
    #[error("closed chunk has an incomplete final line: {path}")]
    IncompleteClosedChunk {
        /// Closed chunk with a non-newline-terminated tail.
        path: PathBuf,
    },
    /// A closed chunk does not have its required checksum sidecar.
    #[error("checksum sidecar is missing: {path}")]
    MissingChecksum {
        /// Closed chunk that has no sidecar.
        path: PathBuf,
    },
    /// A checksum sidecar does not bind the expected chunk metadata or bytes.
    #[error("checksum mismatch: {path}")]
    ChecksumMismatch {
        /// Chunk or sidecar path that failed validation.
        path: PathBuf,
    },
    /// A closed chunk filename is not a portable raw-spool filename.
    #[error("malformed closed chunk name: {name}")]
    MalformedChunkName {
        /// Filename that did not encode a portable chunk minute.
        name: String,
    },
}

fn validate_identifier(field: &'static str, value: &str) -> Result<(), SpoolError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(SpoolError::InvalidIdentifier {
            field,
            value: value.to_owned(),
        });
    }
    Ok(())
}
