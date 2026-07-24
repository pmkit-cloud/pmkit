//! Durable PM envelopes and causal decisions for `PMKit`.
//!
//! [`TursoTapeStore`] implements [`TapeStore`] over a local Turso (libSQL)
//! database. It stores:
//!
//! - **Versioned PM envelopes** — typed normalized projections, optional raw
//!   frames, SHA-256 integrity digests, and deterministic replay cursors.
//! - **Causal decisions** — portable strategy snapshots with risk verdicts.
//! - **Idempotent order intents** — pending → accepted/rejected/unknown
//!   transitions linked to decision correlation IDs.
//!
//! All queries are owner-scoped (`PortfolioId` + `RunId`) and use static
//! parameterized SQL. `delete_database()` removes the entire local database
//! and sidecar files. Storage is opt-in; omitting it leaves the default
//! no-storage path unchanged.

use std::num::NonZeroUsize;

use async_trait::async_trait;
use pmkit_core::{PortfolioId, RunId};
use serde_json::Value;
use thiserror::Error;

mod bundle;
mod chain;
mod chain_store;
mod decoder;
mod integrity;
mod local_files;
mod log;
mod migrations;
mod raw;
mod rpc;
mod schema;
mod source;
mod turso_store;
mod wallet;
mod wallet_reducer;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod migration_tests;

#[cfg(test)]
mod record_version_tests;

#[cfg(test)]
mod chain_tests;

pub use bundle::{CacheChecksum, REPLAY_BUNDLE_VERSION, export_replay_bundle};
pub use chain::{Address, AddressError, ChainId, ContractRegistry, LegacyV1Contracts};
pub use chain_store::{CanonicalLogStore, ingest_finalized_batch};
pub use decoder::{DecodeError, decode_raw_log};
pub use log::{
    CanonicalChainLog, CanonicalLogIdentity, CanonicalLogSegment, ChainCheckpoint, ChainEvent,
    OutcomeTokenAmount, TradeSide,
};
pub use rpc::{JsonRpcFinalizedProvider, RpcProviderConfig};
pub use source::{
    BlockHead, CanonicalLogSource, ChainSourceError, FinalizedBlockCoverage, FinalizedBlockRange,
    FinalizedProviderHead, FinalizedRawLogBatch, FinalizedRawLogProvider,
    FixtureCanonicalLogSource, ProviderIdentity, RawLogIdentity, RawRpcLog,
    agree_on_finalized_heads,
};
pub use turso_store::TursoTapeStore;
pub use wallet::{
    WalletActivity, WalletActivityKind, WalletPosition, WalletQuery, WalletSnapshot, WalletTrade,
};

/// Failure raised while recording or reading storage streams.
#[derive(Debug, Error)]
pub enum StoreError {
    /// The underlying storage operation failed.
    #[error("storage failed: {message}")]
    Storage {
        /// Storage-specific detail.
        message: String,
    },
    /// The database was written by a newer binary and cannot be downgraded safely.
    #[error(
        "database schema version {database_version} exceeds supported version {max_supported_version}"
    )]
    DatabaseSchemaTooNew {
        /// The newest migration recorded on disk.
        database_version: i64,
        /// The newest migration known to this binary.
        max_supported_version: i64,
    },
    /// A causal decision or durable intent uses a record version this binary cannot decode.
    #[error(
        "{record_type} schema version {schema_version} is unsupported; maximum supported version is {max_supported_version}"
    )]
    UnsupportedRecordSchemaVersion {
        /// The durable table containing the unsupported record.
        record_type: &'static str,
        /// The record schema version stored on disk.
        schema_version: i64,
        /// The newest record schema version this binary can decode.
        max_supported_version: i64,
    },
    /// A source identity already exists in its owner scope.
    #[error("source identity already exists")]
    DuplicateSourceIdentity,
    /// A causal decision or intent identity already exists.
    #[error("causal identity already exists")]
    DuplicateCausalIdentity,
    /// A cursor belongs to a different owner scope.
    #[error("replay cursor belongs to another owner scope")]
    ScopeMismatch,
    /// No pending intent can transition for the supplied identity.
    #[error("pending intent was not found")]
    PendingIntentNotFound,
    /// A requested page size exceeded `SQLite`'s signed limit.
    #[error("requested page size exceeds SQLite's signed limit")]
    LimitTooLarge,
    /// A log belongs to an unsupported chain or contract registry entry.
    #[error("canonical log is outside the configured Polygon contract registry")]
    UnsupportedCanonicalLog,
    /// A canonical log could not be decoded from durable storage.
    #[error("canonical log could not be decoded: {message}")]
    CanonicalLogDecode {
        /// Serialization detail from the durable record.
        message: String,
    },
    /// A canonical segment contains an invalid checkpoint relationship.
    #[error("canonical segment does not begin after its common ancestor")]
    InvalidCanonicalSegment,
}

impl From<turso::Error> for StoreError {
    fn from(error: turso::Error) -> Self {
        Self::Storage {
            message: error.to_string(),
        }
    }
}

/// The portfolio/run boundary that owns durable PM records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerScope {
    /// The portfolio that owns the records.
    pub portfolio_id: PortfolioId,
    /// The run that owns the records.
    pub run_id: RunId,
}

impl OwnerScope {
    /// Creates an owner scope from its portfolio and run identifiers.
    #[must_use]
    pub const fn new(portfolio_id: PortfolioId, run_id: RunId) -> Self {
        Self {
            portfolio_id,
            run_id,
        }
    }
}

/// A cursor in deterministic PM source order within one owner scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayCursor {
    /// The owner scope whose rows this cursor may continue.
    pub scope: OwnerScope,
    /// The source timestamp of the last replayed record.
    pub source_timestamp_ms: i64,
    /// The canonical source rank of the last replayed record.
    pub canonical_source_rank: i64,
    /// The connection epoch of the last replayed record.
    pub connection_epoch: i64,
    /// The frame sequence of the last replayed record.
    pub frame_sequence: i64,
}

impl ReplayCursor {
    /// Creates a cursor from the final record on a replay page.
    #[must_use]
    pub fn from_envelope(envelope: &PmEnvelope) -> Self {
        Self {
            scope: envelope.scope.clone(),
            source_timestamp_ms: envelope.source_timestamp_ms,
            canonical_source_rank: envelope.canonical_source_rank,
            connection_epoch: envelope.connection_epoch,
            frame_sequence: envelope.frame_sequence,
        }
    }
}

/// A versioned PM transport frame and its normalized projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PmEnvelope {
    /// The envelope schema version.
    pub schema_version: u16,
    /// The portfolio/run scope that owns this frame.
    pub scope: OwnerScope,
    /// The PM venue identifier.
    pub venue_id: String,
    /// The configuration digest active while the frame was captured.
    pub config_hash: String,
    /// The upstream source identity.
    pub source_id: String,
    /// The connection identity that delivered the frame.
    pub connection_id: String,
    /// The upstream source timestamp in milliseconds.
    pub source_timestamp_ms: i64,
    /// The source's deterministic rank in canonical PM replay order.
    pub canonical_source_rank: i64,
    /// The monotonically increasing epoch for the source connection.
    pub connection_epoch: i64,
    /// The monotonically increasing frame number within the connection epoch.
    pub frame_sequence: i64,
    /// The local receipt timestamp in milliseconds.
    pub receipt_timestamp_ms: i64,
    /// The monotonically assigned ingest sequence.
    pub ingest_sequence: i64,
    /// The PM transport frame when the source provides raw bytes.
    pub raw_frame: Vec<u8>,
    /// The normalized PM projection derived from the frame.
    pub normalized: Value,
}

/// The identity shared by causal decisions and durable intents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CausalIdentity {
    /// The portfolio/run scope that owns the causal record.
    pub scope: OwnerScope,
    /// The caller-provided correlation identifier.
    pub correlation_id: String,
    /// The source timestamp that caused the record.
    pub source_timestamp_ms: i64,
    /// The ingest sequence that caused the record.
    pub ingest_sequence: i64,
}

/// A normalized strategy decision tied to one causal identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CausalDecision {
    /// The identity of the decision.
    pub identity: CausalIdentity,
    /// The normalized decision payload.
    pub payload: Value,
}

/// A durable pending or terminal intent read back for recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableIntent {
    /// The identity shared by the decision and the intent record.
    pub identity: CausalIdentity,
    /// The normalized intent payload.
    pub payload: Value,
}

/// A terminal outcome for a previously durable pending intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntentOutcome {
    /// The external venue accepted the intent.
    Accepted,
    /// The external venue rejected the intent.
    Rejected,
    /// Submission may have happened but has not been reconciled.
    Unknown,
}

/// A typed reason why one replay row cannot produce a PM envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayGapReason {
    /// The stored record uses an unsupported envelope schema version.
    UnsupportedSchemaVersion,
    /// The raw frame no longer matches its stored digest.
    RawIntegrityMismatch,
    /// The normalized projection is invalid or no longer matches its digest.
    NormalizedIntegrityMismatch,
}

/// A replayable record whose contents are missing or corrupt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayGap {
    /// The scope that owns the missing or corrupt record.
    pub scope: OwnerScope,
    /// The source timestamp of the affected record.
    pub source_timestamp_ms: i64,
    /// The ingest sequence of the affected record.
    pub ingest_sequence: i64,
    /// The typed reason replay cannot emit a normalized fact.
    pub reason: ReplayGapReason,
}

/// One item in a scoped PM replay page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayItem {
    /// A lossless envelope that passed versioned integrity checks.
    Envelope(PmEnvelope),
    /// A typed gap for a missing or corrupt stored envelope.
    Gap(ReplayGap),
}

/// A deterministic page of scoped PM replay records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayPage {
    /// The ordered replay records.
    pub items: Vec<ReplayItem>,
    /// The cursor to continue after the final replay record.
    pub next_cursor: Option<ReplayCursor>,
}

/// A durable PM envelope and causal-decision store.
#[async_trait]
pub trait TapeStore: Send + Sync {
    /// Stores one lossless PM envelope or rejects a duplicate source identity.
    async fn store_envelope(&self, envelope: &PmEnvelope) -> Result<(), StoreError>;

    /// Reads one deterministic PM replay page in the supplied owner scope.
    async fn read_envelopes(
        &self,
        scope: &OwnerScope,
        after: Option<ReplayCursor>,
        limit: NonZeroUsize,
    ) -> Result<ReplayPage, StoreError>;

    /// Stores one normalized causal decision exactly once.
    async fn store_decision(&self, decision: &CausalDecision) -> Result<(), StoreError>;

    /// Stores an idempotent intent in its durable pending state.
    async fn store_intent_pending(
        &self,
        identity: &CausalIdentity,
        payload: &Value,
    ) -> Result<(), StoreError>;

    /// Transitions one existing pending intent to a terminal outcome exactly once.
    async fn transition_intent(
        &self,
        identity: &CausalIdentity,
        outcome: IntentOutcome,
    ) -> Result<(), StoreError>;

    /// Transitions an intent and optionally persists its venue order id.
    async fn transition_intent_with_order(
        &self,
        identity: &CausalIdentity,
        outcome: IntentOutcome,
        venue_order_id: Option<&str>,
    ) -> Result<(), StoreError> {
        let _ = venue_order_id;
        self.transition_intent(identity, outcome).await
    }

    /// Lists durable intents still in the pending state for one owner scope.
    async fn read_pending_intents(
        &self,
        scope: &OwnerScope,
    ) -> Result<Vec<DurableIntent>, StoreError>;

    /// Lists durable intents whose terminal outcome is unknown for one owner scope.
    async fn read_unknown_intents(
        &self,
        scope: &OwnerScope,
    ) -> Result<Vec<DurableIntent>, StoreError>;

    /// Lists causal decisions recorded for one owner scope in canonical order.
    async fn read_decisions(&self, scope: &OwnerScope) -> Result<Vec<CausalDecision>, StoreError>;

    /// Persists the portfolio-wide live kill state.
    async fn set_kill_state(
        &self,
        _portfolio: &PortfolioId,
        _killed: bool,
    ) -> Result<(), StoreError> {
        Err(StoreError::Storage {
            message: "kill-state persistence is not configured".into(),
        })
    }

    /// Reads the portfolio-wide live kill state.
    async fn kill_state(&self, _portfolio: &PortfolioId) -> Result<bool, StoreError> {
        Ok(false)
    }
}
