use std::{
    fmt,
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use serde_json::Value;

use crate::{
    CausalDecision, CausalIdentity, IntentOutcome, OwnerScope, PmEnvelope, ReplayCursor,
    ReplayPage, StoreError, TapeStore,
    integrity::{decode_row, sha256_hex},
    local_files::{remove_database, restrict_permissions},
    schema::{
        INSERT_DECISION, INSERT_ENVELOPE, INSERT_PENDING_INTENT, READ_ENVELOPES, SCHEMA,
        TRANSITION_PENDING_INTENT,
    },
};

/// A local Turso-backed implementation of [`TapeStore`].
pub struct TursoTapeStore {
    _database: turso::Database,
    connection: turso::Connection,
    path: PathBuf,
}

impl fmt::Debug for TursoTapeStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TursoTapeStore")
            .finish_non_exhaustive()
    }
}

impl TursoTapeStore {
    /// Opens a local Turso database and creates `PMKit`'s owner-scoped tables.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the database cannot be opened, initialized, or secured.
    pub async fn open_local(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref().to_path_buf();
        let database = turso::Builder::new_local(&path.to_string_lossy())
            .build()
            .await?;
        let connection = database.connect()?;
        connection.execute_batch(SCHEMA).await?;
        restrict_permissions(&path)?;
        Ok(Self {
            _database: database,
            connection,
            path,
        })
    }

    /// Closes and removes the local database plus `SQLite` sidecar files.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when a database file cannot be removed.
    pub fn delete_database(&self) -> Result<(), StoreError> {
        remove_database(&self.path)
    }
}

#[async_trait]
impl TapeStore for TursoTapeStore {
    async fn store_envelope(&self, envelope: &PmEnvelope) -> Result<(), StoreError> {
        let normalized_json =
            serde_json::to_string(&envelope.normalized).map_err(|error| json_error(&error))?;
        let written = self
            .connection
            .execute(
                INSERT_ENVELOPE,
                (
                    envelope.scope.portfolio_id.to_string(),
                    envelope.scope.run_id.to_string(),
                    envelope.source_id.as_str(),
                    envelope.connection_id.as_str(),
                    envelope.source_timestamp_ms,
                    envelope.ingest_sequence,
                    i64::from(envelope.schema_version),
                    envelope.receipt_timestamp_ms,
                    envelope.venue_id.as_str(),
                    envelope.config_hash.as_str(),
                    envelope.raw_frame.as_slice(),
                    sha256_hex(&envelope.raw_frame),
                    normalized_json.as_str(),
                    sha256_hex(normalized_json.as_bytes()),
                ),
            )
            .await?;
        if written == 0 {
            return Err(StoreError::DuplicateSourceIdentity);
        }
        Ok(())
    }

    async fn read_envelopes(
        &self,
        scope: &OwnerScope,
        after: Option<ReplayCursor>,
        limit: NonZeroUsize,
    ) -> Result<ReplayPage, StoreError> {
        let cursor = match after {
            Some(cursor) if cursor.scope != *scope => return Err(StoreError::ScopeMismatch),
            Some(cursor) => Some(cursor),
            None => None,
        };
        let limit = i64::try_from(limit.get()).map_err(|_| StoreError::LimitTooLarge)?;
        let cursor_timestamp = cursor.as_ref().map(|value| value.source_timestamp_ms);
        let cursor_sequence = cursor.as_ref().map_or(0, |value| value.ingest_sequence);
        let mut rows = self
            .connection
            .query(
                READ_ENVELOPES,
                (
                    scope.portfolio_id.to_string(),
                    scope.run_id.to_string(),
                    cursor_timestamp,
                    cursor_sequence,
                    limit,
                ),
            )
            .await?;
        let mut items = Vec::new();
        let mut next_cursor = None;
        while let Some(row) = rows.next().await? {
            let source_timestamp_ms = row.get(0)?;
            let ingest_sequence = row.get(1)?;
            next_cursor = Some(ReplayCursor::new(
                scope.clone(),
                source_timestamp_ms,
                ingest_sequence,
            ));
            items.push(decode_row(
                &row,
                scope,
                source_timestamp_ms,
                ingest_sequence,
            ));
        }
        Ok(ReplayPage { items, next_cursor })
    }

    async fn store_decision(&self, decision: &CausalDecision) -> Result<(), StoreError> {
        let written = self
            .connection
            .execute(
                INSERT_DECISION,
                causal_params(
                    &decision.identity,
                    serde_json::to_string(&decision.payload).map_err(|error| json_error(&error))?,
                ),
            )
            .await?;
        if written == 0 {
            return Err(StoreError::DuplicateCausalIdentity);
        }
        Ok(())
    }

    async fn store_intent_pending(
        &self,
        identity: &CausalIdentity,
        payload: &Value,
    ) -> Result<(), StoreError> {
        let written = self
            .connection
            .execute(
                INSERT_PENDING_INTENT,
                causal_params(
                    identity,
                    serde_json::to_string(payload).map_err(|error| json_error(&error))?,
                ),
            )
            .await?;
        if written == 0 {
            return Err(StoreError::DuplicateCausalIdentity);
        }
        Ok(())
    }

    async fn transition_intent(
        &self,
        identity: &CausalIdentity,
        outcome: IntentOutcome,
    ) -> Result<(), StoreError> {
        let written = self
            .connection
            .execute(
                TRANSITION_PENDING_INTENT,
                (
                    outcome.as_sql(),
                    identity.scope.portfolio_id.to_string(),
                    identity.scope.run_id.to_string(),
                    identity.correlation_id.as_str(),
                    identity.source_timestamp_ms,
                    identity.ingest_sequence,
                ),
            )
            .await?;
        if written == 0 {
            return Err(StoreError::PendingIntentNotFound);
        }
        Ok(())
    }
}

impl IntentOutcome {
    const fn as_sql(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
            Self::Unknown => "unknown",
        }
    }
}

fn causal_params(
    identity: &CausalIdentity,
    payload_json: String,
) -> (String, String, &str, i64, i64, String) {
    (
        identity.scope.portfolio_id.to_string(),
        identity.scope.run_id.to_string(),
        identity.correlation_id.as_str(),
        identity.source_timestamp_ms,
        identity.ingest_sequence,
        payload_json,
    )
}

fn json_error(error: &serde_json::Error) -> StoreError {
    StoreError::Storage {
        message: error.to_string(),
    }
}
