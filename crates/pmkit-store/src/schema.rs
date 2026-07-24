pub const PM_ENVELOPE_VERSION: i64 = 1;
pub const CAUSAL_DECISION_SCHEMA_VERSION: i64 = 1;
pub const DURABLE_INTENT_SCHEMA_VERSION: i64 = 1;
pub const CURRENT_SCHEMA_VERSION: i64 = 2;

pub const CREATE_SCHEMA_MIGRATIONS: &str = "
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY CHECK (version > 0),
    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
)";

pub const RECORD_SCHEMA_MIGRATION: &str = "
INSERT INTO schema_migrations (version) VALUES (?1)";

pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS pm_envelopes (
    portfolio_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    source_timestamp_ms INTEGER NOT NULL,
    canonical_source_rank INTEGER NOT NULL,
    connection_epoch INTEGER NOT NULL,
    frame_sequence INTEGER NOT NULL,
    ingest_sequence INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    receipt_timestamp_ms INTEGER NOT NULL,
    venue_id TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    raw_frame BLOB NOT NULL,
    raw_sha256 TEXT NOT NULL,
    normalized_json TEXT NOT NULL,
    normalized_sha256 TEXT NOT NULL,
    PRIMARY KEY (portfolio_id, run_id, source_id, connection_id, source_timestamp_ms, connection_epoch, frame_sequence),
    UNIQUE (portfolio_id, run_id, source_timestamp_ms, canonical_source_rank, connection_epoch, frame_sequence)
);
CREATE INDEX IF NOT EXISTS pm_envelopes_owner_cursor
    ON pm_envelopes (
        portfolio_id, run_id, source_timestamp_ms, canonical_source_rank, connection_epoch, frame_sequence
    );
CREATE TABLE IF NOT EXISTS causal_decisions (
    portfolio_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    source_timestamp_ms INTEGER NOT NULL,
    ingest_sequence INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (portfolio_id, run_id, correlation_id, source_timestamp_ms, ingest_sequence)
);
CREATE TABLE IF NOT EXISTS durable_intents (
    portfolio_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    source_timestamp_ms INTEGER NOT NULL,
    ingest_sequence INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'accepted', 'rejected', 'unknown')),
    payload_json TEXT NOT NULL,
    PRIMARY KEY (portfolio_id, run_id, correlation_id, source_timestamp_ms, ingest_sequence)
);
CREATE INDEX IF NOT EXISTS durable_intents_owner_pending
    ON durable_intents (portfolio_id, run_id, state, source_timestamp_ms, ingest_sequence);
CREATE TABLE IF NOT EXISTS portfolio_kill_state (
    portfolio_id TEXT PRIMARY KEY,
    killed INTEGER NOT NULL CHECK (killed IN (0, 1))
);
CREATE TABLE IF NOT EXISTS canonical_chain_logs (
    chain_id INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    block_hash TEXT NOT NULL,
    transaction_hash TEXT NOT NULL,
    transaction_index INTEGER NOT NULL,
    log_index INTEGER NOT NULL,
    contract_address TEXT NOT NULL,
    event_json TEXT NOT NULL,
    PRIMARY KEY (chain_id, block_number, block_hash, transaction_hash, transaction_index, log_index)
);
CREATE INDEX IF NOT EXISTS canonical_chain_logs_order
    ON canonical_chain_logs (chain_id, block_number, transaction_index, log_index);
CREATE TABLE IF NOT EXISTS canonical_chain_checkpoints (
    chain_id INTEGER PRIMARY KEY,
    block_number INTEGER NOT NULL,
    block_hash TEXT NOT NULL
);
";

pub const INSERT_ENVELOPE: &str = "
INSERT INTO pm_envelopes (
    portfolio_id, run_id, source_id, connection_id, source_timestamp_ms, canonical_source_rank,
    connection_epoch, frame_sequence, ingest_sequence, schema_version, receipt_timestamp_ms,
    venue_id, config_hash, raw_frame, raw_sha256, normalized_json, normalized_sha256
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
ON CONFLICT DO NOTHING";

pub const READ_ENVELOPES: &str = "
SELECT source_timestamp_ms, canonical_source_rank, connection_epoch, frame_sequence,
       ingest_sequence, schema_version, receipt_timestamp_ms, venue_id, config_hash, source_id,
       connection_id, raw_frame, raw_sha256, normalized_json, normalized_sha256
FROM pm_envelopes
WHERE portfolio_id = ?1 AND run_id = ?2
  AND (?3 IS NULL OR (source_timestamp_ms, canonical_source_rank, connection_epoch, frame_sequence) > (?3, ?4, ?5, ?6))
ORDER BY source_timestamp_ms, canonical_source_rank, connection_epoch, frame_sequence
LIMIT ?7";

pub const INSERT_DECISION: &str = "
INSERT INTO causal_decisions (
    portfolio_id, run_id, correlation_id, source_timestamp_ms, ingest_sequence, schema_version, payload_json
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
ON CONFLICT DO NOTHING";

pub const INSERT_PENDING_INTENT: &str = "
INSERT INTO durable_intents (
    portfolio_id, run_id, correlation_id, source_timestamp_ms, ingest_sequence, schema_version, state, payload_json
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7)
ON CONFLICT DO NOTHING";

pub const TRANSITION_PENDING_INTENT: &str = "
UPDATE durable_intents SET state = ?1, payload_json =
    CASE WHEN ?7 IS NULL THEN payload_json
         ELSE json_set(payload_json, '$.venue_order_id', ?7)
    END
WHERE portfolio_id = ?2 AND run_id = ?3 AND correlation_id = ?4
  AND source_timestamp_ms = ?5 AND ingest_sequence = ?6
  AND state IN ('pending', 'unknown')";

pub const DELETE_CANONICAL_LOGS_AFTER: &str = "
DELETE FROM canonical_chain_logs WHERE chain_id = ?1 AND block_number > ?2";

pub const INSERT_CANONICAL_LOG: &str = "
INSERT INTO canonical_chain_logs (
    chain_id, block_number, block_hash, transaction_hash, transaction_index, log_index,
    contract_address, event_json
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
ON CONFLICT DO NOTHING";

pub const UPSERT_CANONICAL_CHECKPOINT: &str = "
INSERT INTO canonical_chain_checkpoints (chain_id, block_number, block_hash)
VALUES (?1, ?2, ?3)
ON CONFLICT(chain_id) DO UPDATE SET block_number = excluded.block_number, block_hash = excluded.block_hash";

pub const READ_CANONICAL_LOGS: &str = "
SELECT block_number, block_hash, transaction_hash, transaction_index, log_index, contract_address, event_json
FROM canonical_chain_logs
WHERE chain_id = ?1 AND (?2 IS NULL OR block_number >= ?2) AND (?3 IS NULL OR block_number <= ?3)
ORDER BY block_number, transaction_index, log_index";

pub const READ_CANONICAL_CHECKPOINT: &str = "
SELECT block_number, block_hash FROM canonical_chain_checkpoints WHERE chain_id = ?1";

pub const READ_CANONICAL_BLOCK_HASH: &str = "
SELECT block_hash FROM canonical_chain_logs
WHERE chain_id = ?1 AND block_number = ?2
GROUP BY block_hash";

pub const READ_CANONICAL_TIP: &str = "
SELECT block_number, block_hash FROM canonical_chain_logs
WHERE chain_id = ?1
ORDER BY block_number DESC, transaction_index DESC, log_index DESC LIMIT 1";

pub const READ_PENDING_INTENTS: &str = "
SELECT portfolio_id, run_id, correlation_id, source_timestamp_ms, ingest_sequence, schema_version, payload_json
FROM durable_intents
WHERE portfolio_id = ?1 AND run_id = ?2 AND state = 'pending'
ORDER BY source_timestamp_ms, ingest_sequence";

pub const READ_UNKNOWN_INTENTS: &str = "
SELECT portfolio_id, run_id, correlation_id, source_timestamp_ms, ingest_sequence, schema_version, payload_json
FROM durable_intents
WHERE portfolio_id = ?1 AND run_id = ?2 AND state = 'unknown'
ORDER BY source_timestamp_ms, ingest_sequence";

pub const READ_DECISIONS: &str = "
SELECT portfolio_id, run_id, correlation_id, source_timestamp_ms, ingest_sequence, schema_version, payload_json
FROM causal_decisions
WHERE portfolio_id = ?1 AND run_id = ?2
ORDER BY source_timestamp_ms, ingest_sequence";

pub const UPSERT_KILL_STATE: &str = "
INSERT INTO portfolio_kill_state (portfolio_id, killed) VALUES (?1, ?2)
ON CONFLICT(portfolio_id) DO UPDATE SET killed = excluded.killed";

pub const READ_KILL_STATE: &str = "
SELECT killed FROM portfolio_kill_state WHERE portfolio_id = ?1";
