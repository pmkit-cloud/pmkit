/// Current durable PM envelope schema version.
pub const PM_ENVELOPE_VERSION: u16 = 6;
pub const CAUSAL_DECISION_SCHEMA_VERSION: i64 = 2;
pub const DURABLE_INTENT_SCHEMA_VERSION: i64 = 1;
pub const CURRENT_SCHEMA_VERSION: i64 = 11;

pub const CREATE_CLOUD_MATERIALIZATIONS: &str = "
CREATE TABLE IF NOT EXISTS pmkit_cloud_materializations (
    bundle_id TEXT PRIMARY KEY,
    manifest_sha256 TEXT NOT NULL,
    partition_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    artifact_sha256 TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'finalized', 'terminal')),
    release_id TEXT,
    terminal_reason TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
";

pub const CREATE_PUBLIC_TAPE_EVIDENCE: &str = "
CREATE TABLE IF NOT EXISTS pm_replay_gaps (
    portfolio_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    partition_id TEXT NOT NULL,
    start_time_ms INTEGER NOT NULL,
    end_time_ms INTEGER NOT NULL,
    unresolved INTEGER NOT NULL CHECK (unresolved IN (0, 1)),
    reason TEXT NOT NULL,
    PRIMARY KEY (portfolio_id, run_id, partition_id, start_time_ms, end_time_ms, unresolved, reason)
);
CREATE TABLE IF NOT EXISTS pm_public_tape_audit_frames (
    portfolio_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    partition_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    connection_epoch INTEGER NOT NULL,
    frame_sequence INTEGER NOT NULL,
    ingest_sequence INTEGER NOT NULL,
    receipt_timestamp_ms INTEGER NOT NULL,
    source_timestamp_ms INTEGER,
    raw_frame BLOB NOT NULL,
    raw_sha256 TEXT NOT NULL,
    PRIMARY KEY (portfolio_id, run_id, source_id, connection_id, connection_epoch, frame_sequence, ingest_sequence)
);
CREATE INDEX IF NOT EXISTS pm_replay_gaps_owner_interval
    ON pm_replay_gaps (portfolio_id, run_id, start_time_ms, end_time_ms);
CREATE INDEX IF NOT EXISTS pm_public_tape_audit_frames_owner_order
    ON pm_public_tape_audit_frames (portfolio_id, run_id, ingest_sequence);
";

pub const CREATE_SCHEMA_MIGRATIONS: &str = "
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY CHECK (version > 0),
    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
)";

pub const RECORD_SCHEMA_MIGRATION: &str = "
INSERT INTO schema_migrations (version) VALUES (?1)";

pub const CREATE_FINALIZED_CHAIN_CHECKPOINTS: &str = "
CREATE TABLE finalized_chain_checkpoints (
    chain_id INTEGER PRIMARY KEY,
    block_number INTEGER NOT NULL CHECK (block_number >= 0),
    block_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
)";

pub const MIGRATE_PM_ENVELOPES_V1_TO_V2: &str = "
UPDATE pm_envelopes SET schema_version = 2 WHERE schema_version = 1";

pub const MIGRATE_PM_ENVELOPES_V4_TO_V5: &str = "
UPDATE pm_envelopes SET schema_version = 5 WHERE schema_version = 4";

pub const MIGRATE_PM_ENVELOPES_V5_TO_V6: &str = "
UPDATE pm_envelopes SET schema_version = 6 WHERE schema_version = 5";

pub const MIGRATE_CAUSAL_DECISIONS_V1_TO_V2: &str = "
UPDATE causal_decisions SET schema_version = 2 WHERE schema_version = 1";

pub const MIGRATE_PM_ENVELOPES_V2_TO_V3: &[&str] = &[
    "ALTER TABLE pm_envelopes RENAME TO pm_envelopes_v2",
    "
CREATE TABLE pm_envelopes (
    portfolio_id TEXT NOT NULL, run_id TEXT NOT NULL, source_id TEXT NOT NULL,
    connection_id TEXT NOT NULL, source_timestamp_ms INTEGER NOT NULL,
    canonical_source_rank INTEGER NOT NULL, canonical_market_id TEXT NOT NULL,
    connection_epoch INTEGER NOT NULL, frame_sequence INTEGER NOT NULL,
    ingest_sequence INTEGER NOT NULL, schema_version INTEGER NOT NULL,
    receipt_timestamp_ms INTEGER NOT NULL, venue_id TEXT NOT NULL, config_hash TEXT NOT NULL,
    raw_frame BLOB NOT NULL, raw_sha256 TEXT NOT NULL, normalized_json TEXT NOT NULL,
    normalized_sha256 TEXT NOT NULL,
    PRIMARY KEY (portfolio_id, run_id, source_id, connection_id, source_timestamp_ms, canonical_market_id, connection_epoch, frame_sequence),
    UNIQUE (portfolio_id, run_id, source_timestamp_ms, canonical_source_rank, canonical_market_id, connection_epoch, frame_sequence)
)",
    "
INSERT INTO pm_envelopes SELECT portfolio_id, run_id, source_id, connection_id,
    source_timestamp_ms, canonical_source_rank,
    COALESCE(json_extract(normalized_json, '$.payload.market'), ''), connection_epoch,
    frame_sequence, ingest_sequence, CASE WHEN schema_version = 2 THEN 3 ELSE schema_version END,
    receipt_timestamp_ms, venue_id, config_hash, raw_frame, raw_sha256, normalized_json,
    normalized_sha256 FROM pm_envelopes_v2",
    "DROP TABLE pm_envelopes_v2",
    "CREATE INDEX pm_envelopes_owner_cursor ON pm_envelopes (portfolio_id, run_id, source_timestamp_ms, canonical_source_rank, canonical_market_id, connection_epoch, frame_sequence)",
];

pub const MIGRATE_PM_ENVELOPES_V3_TO_V4: &[&str] = &[
    "ALTER TABLE pm_envelopes RENAME TO pm_envelopes_v3",
    "
CREATE TABLE pm_envelopes (
    portfolio_id TEXT NOT NULL, run_id TEXT NOT NULL, source_id TEXT NOT NULL,
    connection_id TEXT NOT NULL, source_timestamp_ms INTEGER NOT NULL,
    canonical_source_rank INTEGER NOT NULL, canonical_market_id TEXT NOT NULL,
    stream_id TEXT NOT NULL, connection_epoch INTEGER NOT NULL,
    frame_sequence INTEGER NOT NULL, ingest_sequence INTEGER NOT NULL,
    schema_version INTEGER NOT NULL, receipt_timestamp_ms INTEGER NOT NULL,
    venue_id TEXT NOT NULL, config_hash TEXT NOT NULL, raw_frame BLOB NOT NULL,
    raw_sha256 TEXT NOT NULL, normalized_json TEXT NOT NULL,
    normalized_sha256 TEXT NOT NULL,
    PRIMARY KEY (portfolio_id, run_id, source_id, connection_id, source_timestamp_ms, canonical_market_id, stream_id, connection_epoch, frame_sequence),
    UNIQUE (portfolio_id, run_id, source_timestamp_ms, canonical_source_rank, canonical_market_id, stream_id, connection_epoch, frame_sequence)
)",
    "
INSERT INTO pm_envelopes (
    portfolio_id, run_id, source_id, connection_id, source_timestamp_ms,
    canonical_source_rank, canonical_market_id, stream_id, connection_epoch,
    frame_sequence, ingest_sequence, schema_version, receipt_timestamp_ms,
    venue_id, config_hash, raw_frame, raw_sha256, normalized_json, normalized_sha256
)
SELECT portfolio_id, run_id, source_id, connection_id, source_timestamp_ms,
    canonical_source_rank, canonical_market_id,
    CASE
        WHEN json_type(normalized_json, '$.stream_id') = 'text'
            THEN json_extract(normalized_json, '$.stream_id')
        WHEN json_type(normalized_json, '$.portfolio') = 'text'
            THEN 'account:' || json_extract(normalized_json, '$.portfolio')
        WHEN canonical_market_id <> ''
         AND json_type(normalized_json, '$.payload.outcome') = 'text'
            THEN 'market:' || canonical_market_id || ':'
                || lower(json_extract(normalized_json, '$.payload.outcome'))
        WHEN canonical_market_id <> '' THEN 'market:' || canonical_market_id || ':unknown'
        ELSE 'market-source:' || source_id
    END,
    connection_epoch, frame_sequence, ingest_sequence,
    CASE WHEN schema_version = 3 THEN 4 ELSE schema_version END,
    receipt_timestamp_ms, venue_id, config_hash, raw_frame, raw_sha256,
    normalized_json, normalized_sha256
FROM pm_envelopes_v3",
    "DROP TABLE pm_envelopes_v3",
    "CREATE INDEX pm_envelopes_owner_cursor ON pm_envelopes (portfolio_id, run_id, source_timestamp_ms, canonical_source_rank, canonical_market_id, stream_id, connection_epoch, frame_sequence)",
];

pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS pm_envelopes (
    portfolio_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    source_timestamp_ms INTEGER NOT NULL,
    canonical_source_rank INTEGER NOT NULL,
    canonical_market_id TEXT NOT NULL,
    stream_id TEXT NOT NULL,
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
    PRIMARY KEY (portfolio_id, run_id, source_id, connection_id, source_timestamp_ms, canonical_market_id, stream_id, connection_epoch, frame_sequence),
    UNIQUE (portfolio_id, run_id, source_timestamp_ms, canonical_source_rank, canonical_market_id, stream_id, connection_epoch, frame_sequence)
);
CREATE INDEX IF NOT EXISTS pm_envelopes_owner_cursor
    ON pm_envelopes (
        portfolio_id, run_id, source_timestamp_ms, canonical_source_rank, canonical_market_id, stream_id, connection_epoch, frame_sequence
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
    canonical_market_id, stream_id, connection_epoch, frame_sequence, ingest_sequence, schema_version,
    receipt_timestamp_ms, venue_id, config_hash, raw_frame, raw_sha256, normalized_json,
    normalized_sha256
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)
ON CONFLICT DO NOTHING";

pub const READ_ENVELOPES: &str = "
SELECT source_timestamp_ms, canonical_source_rank, canonical_market_id, stream_id,
       connection_epoch, frame_sequence, ingest_sequence, schema_version,
       receipt_timestamp_ms, venue_id, config_hash, source_id, connection_id,
       raw_frame, raw_sha256, normalized_json, normalized_sha256
FROM pm_envelopes
WHERE portfolio_id = ?1 AND run_id = ?2
  AND (?3 IS NULL OR (source_timestamp_ms, canonical_source_rank, canonical_market_id, stream_id, connection_epoch, frame_sequence) > (?3, ?4, ?5, ?6, ?7, ?8))
ORDER BY source_timestamp_ms, canonical_source_rank, canonical_market_id, stream_id, connection_epoch, frame_sequence
LIMIT ?9";

pub const READ_ENVELOPE_INTEGRITY: &str = "
SELECT raw_sha256, normalized_sha256
FROM pm_envelopes
WHERE portfolio_id = ?1 AND run_id = ?2 AND source_id = ?3 AND connection_id = ?4
  AND source_timestamp_ms = ?5 AND canonical_market_id = ?6 AND stream_id = ?7
  AND connection_epoch = ?8 AND frame_sequence = ?9";

pub const INSERT_REPLAY_GAP: &str = "
INSERT INTO pm_replay_gaps (
    portfolio_id, run_id, partition_id, start_time_ms, end_time_ms, unresolved, reason
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
ON CONFLICT DO NOTHING";

pub const READ_REPLAY_GAPS: &str = "
SELECT partition_id, start_time_ms, end_time_ms, unresolved, reason
FROM pm_replay_gaps
WHERE portfolio_id = ?1 AND run_id = ?2
ORDER BY start_time_ms, end_time_ms, partition_id";

pub const INSERT_PUBLIC_TAPE_AUDIT_FRAME: &str = "
INSERT INTO pm_public_tape_audit_frames (
    portfolio_id, run_id, partition_id, source_id, connection_id, connection_epoch,
    frame_sequence, ingest_sequence, receipt_timestamp_ms, source_timestamp_ms, raw_frame, raw_sha256
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
ON CONFLICT DO NOTHING";

pub const READ_PUBLIC_TAPE_AUDIT_FRAMES: &str = "
SELECT partition_id, source_id, connection_id, connection_epoch, frame_sequence,
       ingest_sequence, receipt_timestamp_ms, source_timestamp_ms, raw_frame, raw_sha256
FROM pm_public_tape_audit_frames
WHERE portfolio_id = ?1 AND run_id = ?2
ORDER BY ingest_sequence, connection_epoch, frame_sequence";

pub const INSERT_CLOUD_MATERIALIZATION: &str = "
INSERT INTO pmkit_cloud_materializations (
    bundle_id, manifest_sha256, partition_id, schema_version, artifact_sha256, state
) VALUES (?1, ?2, ?3, ?4, ?5, 'pending')
ON CONFLICT DO NOTHING";

pub const READ_CLOUD_MATERIALIZATION: &str = "
SELECT bundle_id, manifest_sha256, partition_id, schema_version, artifact_sha256,
       state, release_id, terminal_reason
FROM pmkit_cloud_materializations WHERE bundle_id = ?1";

pub const TRANSITION_CLOUD_MATERIALIZATION: &str = "
UPDATE pmkit_cloud_materializations
SET state = ?1, release_id = ?2, terminal_reason = ?3, updated_at = CURRENT_TIMESTAMP
WHERE bundle_id = ?4 AND state = 'pending'";

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

pub const READ_FINALIZED_CHAIN_CHECKPOINT: &str = "
SELECT block_number, block_hash FROM finalized_chain_checkpoints WHERE chain_id = ?1";

pub const UPSERT_FINALIZED_CHAIN_CHECKPOINT: &str = "
INSERT INTO finalized_chain_checkpoints (chain_id, block_number, block_hash)
VALUES (?1, ?2, ?3)
ON CONFLICT(chain_id) DO UPDATE SET
    block_number = excluded.block_number,
    block_hash = excluded.block_hash,
    updated_at = CURRENT_TIMESTAMP
WHERE excluded.block_number > finalized_chain_checkpoints.block_number
   OR (excluded.block_number = finalized_chain_checkpoints.block_number
       AND excluded.block_hash = finalized_chain_checkpoints.block_hash)";

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

pub const READ_ACCEPTED_INTENTS: &str = "
SELECT portfolio_id, run_id, correlation_id, source_timestamp_ms, ingest_sequence, schema_version, payload_json
FROM durable_intents
WHERE portfolio_id = ?1 AND run_id = ?2 AND state = 'accepted'
ORDER BY source_timestamp_ms, ingest_sequence";

pub const READ_REJECTED_INTENTS: &str = "
SELECT portfolio_id, run_id, correlation_id, source_timestamp_ms, ingest_sequence, schema_version, payload_json
FROM durable_intents
WHERE portfolio_id = ?1 AND run_id = ?2 AND state = 'rejected'
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
