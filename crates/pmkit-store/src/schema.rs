pub const PM_ENVELOPE_VERSION: i64 = 1;

pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS pm_envelopes (
    portfolio_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    source_timestamp_ms INTEGER NOT NULL,
    ingest_sequence INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    receipt_timestamp_ms INTEGER NOT NULL,
    venue_id TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    raw_frame BLOB NOT NULL,
    raw_sha256 TEXT NOT NULL,
    normalized_json TEXT NOT NULL,
    normalized_sha256 TEXT NOT NULL,
    PRIMARY KEY (portfolio_id, run_id, source_id, connection_id, source_timestamp_ms, ingest_sequence)
);
CREATE INDEX IF NOT EXISTS pm_envelopes_owner_cursor
    ON pm_envelopes (portfolio_id, run_id, source_timestamp_ms, ingest_sequence);
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
";

pub const INSERT_ENVELOPE: &str = "
INSERT INTO pm_envelopes (
    portfolio_id, run_id, source_id, connection_id, source_timestamp_ms, ingest_sequence,
    schema_version, receipt_timestamp_ms, venue_id, config_hash, raw_frame, raw_sha256,
    normalized_json, normalized_sha256
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
ON CONFLICT DO NOTHING";

pub const READ_ENVELOPES: &str = "
SELECT source_timestamp_ms, ingest_sequence, schema_version, receipt_timestamp_ms, venue_id,
       config_hash, source_id, connection_id, raw_frame, raw_sha256, normalized_json,
       normalized_sha256
FROM pm_envelopes
WHERE portfolio_id = ?1 AND run_id = ?2
  AND (?3 IS NULL OR source_timestamp_ms > ?3 OR (source_timestamp_ms = ?3 AND ingest_sequence > ?4))
ORDER BY source_timestamp_ms, ingest_sequence
LIMIT ?5";

pub const INSERT_DECISION: &str = "
INSERT INTO causal_decisions (
    portfolio_id, run_id, correlation_id, source_timestamp_ms, ingest_sequence, payload_json
) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
ON CONFLICT DO NOTHING";

pub const INSERT_PENDING_INTENT: &str = "
INSERT INTO durable_intents (
    portfolio_id, run_id, correlation_id, source_timestamp_ms, ingest_sequence, state, payload_json
) VALUES (?1, ?2, ?3, ?4, ?5, 'pending', ?6)
ON CONFLICT DO NOTHING";

pub const TRANSITION_PENDING_INTENT: &str = "
UPDATE durable_intents SET state = ?1
WHERE portfolio_id = ?2 AND run_id = ?3 AND correlation_id = ?4
  AND source_timestamp_ms = ?5 AND ingest_sequence = ?6 AND state = 'pending'";
