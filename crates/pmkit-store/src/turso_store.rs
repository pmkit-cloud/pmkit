use std::{
    fmt,
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use serde_json::Value;

use crate::{
    CausalDecision, CausalIdentity, DurableIntent, IntentOutcome, OwnerScope, PmEnvelope,
    PortfolioId, ReplayCursor, ReplayPage, RunId, StoreError, TapeStore,
    integrity::{decode_row, sha256_hex},
    local_files::{remove_database, restrict_permissions},
    migrations::{MIGRATIONS, apply},
    schema::{
        CAUSAL_DECISION_SCHEMA_VERSION, DURABLE_INTENT_SCHEMA_VERSION, INSERT_DECISION,
        INSERT_ENVELOPE, INSERT_PENDING_INTENT, READ_DECISIONS, READ_ENVELOPES, READ_KILL_STATE,
        READ_PENDING_INTENTS, READ_UNKNOWN_INTENTS, TRANSITION_PENDING_INTENT, UPSERT_KILL_STATE,
    },
};

/// A local Turso-backed implementation of [`TapeStore`].
pub struct TursoTapeStore {
    database: turso::Database,
    pub(crate) connection: turso::Connection,
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
        apply(&connection, MIGRATIONS).await?;
        restrict_permissions(&path)?;
        Ok(Self {
            database,
            connection,
            path,
        })
    }

    /// Closes and removes the local database plus `SQLite` sidecar files.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when a database file cannot be removed.
    pub fn delete_database(self) -> Result<(), StoreError> {
        let Self {
            database,
            connection,
            path,
        } = self;
        drop(connection);
        drop(database);
        remove_database(&path)
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
                turso::params![
                    envelope.scope.portfolio_id.to_string(),
                    envelope.scope.run_id.to_string(),
                    envelope.source_id.as_str(),
                    envelope.connection_id.as_str(),
                    envelope.source_timestamp_ms,
                    envelope.canonical_source_rank,
                    envelope.canonical_market_id(),
                    envelope.connection_epoch,
                    envelope.frame_sequence,
                    envelope.ingest_sequence,
                    i64::from(envelope.schema_version),
                    envelope.receipt_timestamp_ms,
                    envelope.venue_id.as_str(),
                    envelope.config_hash.as_str(),
                    envelope.raw_frame.as_slice(),
                    sha256_hex(&envelope.raw_frame),
                    normalized_json.as_str(),
                    sha256_hex(normalized_json.as_bytes()),
                ],
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
        let cursor_source_rank = cursor
            .as_ref()
            .map_or(0, |value| value.canonical_source_rank);
        let cursor_market_id = cursor
            .as_ref()
            .map_or("", |value| value.canonical_market_id.as_str());
        let cursor_connection_epoch = cursor.as_ref().map_or(0, |value| value.connection_epoch);
        let cursor_frame_sequence = cursor.as_ref().map_or(0, |value| value.frame_sequence);
        let mut rows = self
            .connection
            .query(
                READ_ENVELOPES,
                (
                    scope.portfolio_id.to_string(),
                    scope.run_id.to_string(),
                    cursor_timestamp,
                    cursor_source_rank,
                    cursor_market_id,
                    cursor_connection_epoch,
                    cursor_frame_sequence,
                    limit,
                ),
            )
            .await?;
        let mut items = Vec::new();
        let mut next_cursor = None;
        while let Some(row) = rows.next().await? {
            let source_timestamp_ms = row.get(0)?;
            let canonical_source_rank = row.get(1)?;
            let canonical_market_id = row.get(2)?;
            let connection_epoch = row.get(3)?;
            let frame_sequence = row.get(4)?;
            let ingest_sequence = row.get(5)?;
            let item = decode_row(
                &row,
                scope,
                source_timestamp_ms,
                canonical_source_rank,
                connection_epoch,
                frame_sequence,
                ingest_sequence,
            );
            next_cursor = Some(ReplayCursor {
                scope: scope.clone(),
                source_timestamp_ms,
                canonical_source_rank,
                canonical_market_id,
                connection_epoch,
                frame_sequence,
            });
            items.push(item);
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
                    CAUSAL_DECISION_SCHEMA_VERSION,
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
                    DURABLE_INTENT_SCHEMA_VERSION,
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
                    None::<&str>,
                ),
            )
            .await?;
        if written == 0 {
            return Err(StoreError::PendingIntentNotFound);
        }
        Ok(())
    }

    async fn transition_intent_with_order(
        &self,
        identity: &CausalIdentity,
        outcome: IntentOutcome,
        venue_order_id: Option<&str>,
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
                    venue_order_id,
                ),
            )
            .await?;
        if written == 0 {
            return Err(StoreError::PendingIntentNotFound);
        }
        Ok(())
    }

    async fn read_pending_intents(
        &self,
        scope: &OwnerScope,
    ) -> Result<Vec<DurableIntent>, StoreError> {
        read_intents_by_state(self, scope, READ_PENDING_INTENTS).await
    }

    async fn read_unknown_intents(
        &self,
        scope: &OwnerScope,
    ) -> Result<Vec<DurableIntent>, StoreError> {
        read_intents_by_state(self, scope, READ_UNKNOWN_INTENTS).await
    }

    async fn read_decisions(&self, scope: &OwnerScope) -> Result<Vec<CausalDecision>, StoreError> {
        let mut rows = self
            .connection
            .query(
                READ_DECISIONS,
                (scope.portfolio_id.to_string(), scope.run_id.to_string()),
            )
            .await?;
        let mut decisions = Vec::new();
        while let Some(row) = rows.next().await? {
            let portfolio_id: String = row.get(0)?;
            let run_id: String = row.get(1)?;
            let portfolio_id =
                PortfolioId::new(portfolio_id).map_err(|error| StoreError::Storage {
                    message: format!("invalid portfolio id in decision: {error}"),
                })?;
            let run_id = RunId::new(run_id).map_err(|error| StoreError::Storage {
                message: format!("invalid run id in decision: {error}"),
            })?;
            let correlation_id: String = row.get(2)?;
            let source_timestamp_ms: i64 = row.get(3)?;
            let ingest_sequence: i64 = row.get(4)?;
            ensure_supported_record_schema_version(
                "causal_decisions",
                row.get(5)?,
                CAUSAL_DECISION_SCHEMA_VERSION,
            )?;
            let payload_json: String = row.get(6)?;
            decisions.push(CausalDecision {
                identity: CausalIdentity {
                    scope: OwnerScope::new(portfolio_id, run_id),
                    correlation_id,
                    source_timestamp_ms,
                    ingest_sequence,
                },
                payload: serde_json::from_str(&payload_json).map_err(|error| json_error(&error))?,
            });
        }
        Ok(decisions)
    }

    async fn set_kill_state(
        &self,
        portfolio: &PortfolioId,
        killed: bool,
    ) -> Result<(), StoreError> {
        self.connection
            .execute(
                UPSERT_KILL_STATE,
                (portfolio.to_string(), i64::from(killed)),
            )
            .await?;
        Ok(())
    }

    async fn kill_state(&self, portfolio: &PortfolioId) -> Result<bool, StoreError> {
        let mut rows = self
            .connection
            .query(READ_KILL_STATE, [portfolio.to_string()])
            .await?;
        let Some(row) = rows.next().await? else {
            return Ok(false);
        };
        Ok(row.get::<i64>(0)? != 0)
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

async fn read_intents_by_state(
    store: &TursoTapeStore,
    scope: &OwnerScope,
    sql: &str,
) -> Result<Vec<DurableIntent>, StoreError> {
    let mut rows = store
        .connection
        .query(
            sql,
            (scope.portfolio_id.to_string(), scope.run_id.to_string()),
        )
        .await?;
    let mut intents = Vec::new();
    while let Some(row) = rows.next().await? {
        let portfolio_id: String = row.get(0)?;
        let run_id: String = row.get(1)?;
        let portfolio_id = PortfolioId::new(portfolio_id).map_err(|error| StoreError::Storage {
            message: format!("invalid portfolio id in durable intent: {error}"),
        })?;
        let run_id = RunId::new(run_id).map_err(|error| StoreError::Storage {
            message: format!("invalid run id in durable intent: {error}"),
        })?;
        let correlation_id: String = row.get(2)?;
        let source_timestamp_ms: i64 = row.get(3)?;
        let ingest_sequence: i64 = row.get(4)?;
        ensure_supported_record_schema_version(
            "durable_intents",
            row.get(5)?,
            DURABLE_INTENT_SCHEMA_VERSION,
        )?;
        let payload_json: String = row.get(6)?;
        intents.push(DurableIntent {
            identity: CausalIdentity {
                scope: OwnerScope::new(portfolio_id, run_id),
                correlation_id,
                source_timestamp_ms,
                ingest_sequence,
            },
            payload: serde_json::from_str(&payload_json).map_err(|error| json_error(&error))?,
        });
    }
    Ok(intents)
}

fn causal_params(
    identity: &CausalIdentity,
    schema_version: i64,
    payload_json: String,
) -> (String, String, &str, i64, i64, i64, String) {
    (
        identity.scope.portfolio_id.to_string(),
        identity.scope.run_id.to_string(),
        identity.correlation_id.as_str(),
        identity.source_timestamp_ms,
        identity.ingest_sequence,
        schema_version,
        payload_json,
    )
}

const fn ensure_supported_record_schema_version(
    record_type: &'static str,
    schema_version: i64,
    max_supported_version: i64,
) -> Result<(), StoreError> {
    if schema_version != max_supported_version {
        return Err(StoreError::UnsupportedRecordSchemaVersion {
            record_type,
            schema_version,
            max_supported_version,
        });
    }
    Ok(())
}

fn json_error(error: &serde_json::Error) -> StoreError {
    StoreError::Storage {
        message: error.to_string(),
    }
}
