use std::collections::BTreeSet;

use pmkit_tape::{SpoolChunk, SpoolFrame};
use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;

use crate::{OwnerScope, PmEnvelope, ReplayGapInterval, StoreError};

pub(super) const UTC_MINUTE_MS: i64 = 60_000;

/// One collector record with metadata excluded from canonical content hashes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawMarketLaneRecord {
    /// Existing immutable lane/shard/minute spool identity.
    pub chunk: SpoolChunk,
    /// Existing raw frame plus collector-only provenance.
    pub frame: SpoolFrame,
    /// Canonical market identity parsed from this record.
    pub market_id: String,
}

/// One known partition uncertainty from a failed lane or checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationFailure {
    /// Lane that could not provide complete evidence.
    pub lane_id: String,
    /// Discovery snapshot that assigned the affected market.
    pub discovery_snapshot_sha256: String,
    /// Canonical market identity.
    pub market_id: String,
    /// Affected UTC minute start.
    pub minute_start_ms: i64,
    /// Typed cause of the uncertainty.
    pub reason: ReconciliationFailureReason,
}

/// Causes that block a known market/minute partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationFailureReason {
    /// A redundant lane was unavailable for the partition.
    LaneOutage,
    /// A checkpoint could not verify the durable input chunk.
    CheckpointFailure,
}

impl ReconciliationFailureReason {
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::LaneOutage => "lane_outage",
            Self::CheckpointFailure => "checkpoint_failure",
        }
    }
}

/// Complete input for reconciling exactly two redundant lanes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationRequest {
    /// Durable owner of the emitted canonical evidence and gaps.
    pub scope: OwnerScope,
    /// The two complete lanes required for corroboration.
    pub expected_lanes: BTreeSet<String>,
    /// Raw records from both lanes in arbitrary arrival order.
    pub records: Vec<RawMarketLaneRecord>,
    /// Known partition-scoped failures that must block export.
    pub failures: Vec<ReconciliationFailure>,
}

/// One corroborated content occurrence with its stable ordinal among duplicates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalOccurrence {
    /// SHA-256 of canonical content plus discovery/market/minute identity.
    pub content_sha256: String,
    /// Zero-based occurrence ordinal among equal content in the partition.
    pub occurrence_ordinal: u64,
    /// Exact canonical bytes addressed by `content_sha256`.
    pub canonical_bytes: Vec<u8>,
    /// The storage-ready canonical envelope.
    pub envelope: PmEnvelope,
}

/// Canonical occurrences and partition-scoped intervals from reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationResult {
    /// Only the lane-intersection occurrences.
    pub occurrences: Vec<CanonicalOccurrence>,
    /// Every known loss, disagreement, malformed input, or checkpoint failure.
    pub gaps: Vec<ReplayGapInterval>,
}

/// Failure while reconciling redundant evidence.
#[derive(Debug, Error)]
pub enum ReconciliationError {
    /// The processor requires exactly two distinct lane identities.
    #[error("redundant reconciliation requires exactly two lanes")]
    ExpectedTwoLanes,
    /// A record or failure names a lane outside the configured redundant pair.
    #[error("unexpected reconciliation lane: {lane_id}")]
    UnexpectedLane {
        /// Rejected lane identity.
        lane_id: String,
    },
    /// A deterministic ordinal cannot be represented by durable PM fields.
    #[error("reconciliation ordinal exceeds durable range")]
    OrdinalOutOfRange,
    /// Persisting canonical evidence or typed gaps failed.
    #[error(transparent)]
    Store(#[from] StoreError),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireMarketRecord {
    pub(super) market_id: String,
    pub(super) event_time_ms: i64,
    pub(super) venue_id: String,
    pub(super) config_hash: String,
    pub(super) normalized: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Partition {
    pub(super) snapshot: String,
    pub(super) market_id: String,
    pub(super) minute_start_ms: i64,
}

#[derive(Debug, Clone)]
pub(super) struct ParsedRecord {
    pub(super) lane_id: String,
    pub(super) partition: Partition,
    pub(super) event_time_ms: i64,
    pub(super) venue_id: String,
    pub(super) config_hash: String,
    pub(super) normalized: Value,
    pub(super) canonical_bytes: Vec<u8>,
    pub(super) content_sha256: String,
}
