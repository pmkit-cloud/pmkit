use std::path::{Path, PathBuf};

use pmkit_core::{PortfolioId, RunId};
use serde_json::json;

use crate::{
    OwnerScope, PM_ENVELOPE_VERSION, PmEnvelope, ReplayItem, StoreError, TapeStore, TursoTapeStore,
    integrity::sha256_hex,
    migrations::{MIGRATIONS, Migration, apply},
};

const PM_ACCOUNT_ENVELOPE_V1: &str = include_str!("../tests/fixtures/pm-account-envelope-v1.json");
const PM_ACCOUNT_ENVELOPE_V2: &str = include_str!("../tests/fixtures/pm-account-envelope-v2.json");
const PM_ACCOUNT_ENVELOPE_V3: &str = include_str!("../tests/fixtures/pm-account-envelope-v3.json");
const PM_ACCOUNT_ENVELOPE_V4: &str = include_str!("../tests/fixtures/pm-account-envelope-v4.json");
const PM_MARKET_ENVELOPE_V3: &str = include_str!("../tests/fixtures/pm-market-envelope-v3.json");
const PM_MARKET_ENVELOPE_V4: &str = include_str!("../tests/fixtures/pm-market-envelope-v4.json");
const OLD_CAUSAL_DECISION: &str = include_str!("../tests/fixtures/causal-decision-v1.json");
const NEW_CAUSAL_DECISION: &str = include_str!("../tests/fixtures/causal-decision-v2.json");

fn database_path(name: &str) -> Result<(tempfile::TempDir, PathBuf), std::io::Error> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join(format!("pmkit-store-{name}.db"));
    Ok((dir, path))
}

async fn open_connection(
    path: &Path,
) -> Result<(turso::Database, turso::Connection), turso::Error> {
    let database = turso::Builder::new_local(&path.to_string_lossy())
        .build()
        .await?;
    let connection = database.connect()?;
    Ok((database, connection))
}

async fn migration_rows(
    connection: &turso::Connection,
) -> Result<Vec<(i64, String)>, turso::Error> {
    let mut rows = connection
        .query(
            "SELECT version, applied_at FROM schema_migrations ORDER BY version",
            (),
        )
        .await?;
    let mut migrations = Vec::new();
    while let Some(row) = rows.next().await? {
        migrations.push((row.get(0)?, row.get(1)?));
    }
    Ok(migrations)
}

#[tokio::test]
async fn migration_applies_and_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a fresh local database.
    let (_dir, path) = database_path("migration-idempotent")?;

    // When: the store opens twice.
    let first_rows = {
        let store = TursoTapeStore::open_local(&path).await?;
        migration_rows(&store.connection).await?
    };
    let reopened_rows = {
        let store = TursoTapeStore::open_local(&path).await?;
        let rows = migration_rows(&store.connection).await?;
        store.delete_database()?;
        rows
    };

    // Then: all current migrations are recorded once with timestamps.
    assert!(matches!(
        first_rows.as_slice(),
        [
            (1, first_applied_at),
            (2, second_applied_at),
            (3, third_applied_at),
            (4, fourth_applied_at),
            (5, fifth_applied_at),
            (6, sixth_applied_at),
            (7, seventh_applied_at),
            (8, eighth_applied_at),
            (9, ninth_applied_at)
        ]
            if !first_applied_at.is_empty()
                && !second_applied_at.is_empty()
                && !third_applied_at.is_empty()
                && !fourth_applied_at.is_empty()
                && !fifth_applied_at.is_empty()
                && !sixth_applied_at.is_empty()
                && !seventh_applied_at.is_empty()
                && !eighth_applied_at.is_empty()
                && !ninth_applied_at.is_empty()
    ));
    assert_eq!(reopened_rows, first_rows);
    Ok(())
}

#[tokio::test]
async fn migration_rejects_newer_version() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a database stamped one version newer than this binary supports.
    let (_dir, path) = database_path("migration-newer")?;
    let store = TursoTapeStore::open_local(&path).await?;
    drop(store);
    let (database, connection) = open_connection(&path).await?;
    connection
        .execute(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (10, CURRENT_TIMESTAMP)",
            (),
        )
        .await?;
    drop(connection);
    drop(database);

    // When: the older binary reopens it.
    // Then: opening fails closed instead of attempting a downgrade.
    assert!(matches!(
        TursoTapeStore::open_local(&path).await,
        Err(StoreError::DatabaseSchemaTooNew {
            database_version: 10,
            max_supported_version: 9,
        })
    ));
    Ok(())
}

async fn seed_pm_market_v3(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let (database, connection) = open_connection(path).await?;
    apply(&connection, &MIGRATIONS[..6]).await?;
    let fixture: serde_json::Value = serde_json::from_str(PM_MARKET_ENVELOPE_V3)?;
    let normalized = serde_json::to_string(&fixture["normalized"])?;
    connection
        .execute(
            "INSERT INTO pm_envelopes (
                portfolio_id, run_id, source_id, connection_id, source_timestamp_ms,
                canonical_source_rank, canonical_market_id, connection_epoch, frame_sequence,
                ingest_sequence, schema_version, receipt_timestamp_ms, venue_id, config_hash,
                raw_frame, raw_sha256, normalized_json, normalized_sha256
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
            turso::params![
                "paper",
                "run",
                "polymarket:market-ws",
                "market-1",
                1_i64,
                0_i64,
                "btc-5m",
                0_i64,
                1_i64,
                1_i64,
                3_i64,
                2_i64,
                "polymarket",
                "fixture",
                Vec::<u8>::new(),
                sha256_hex(&[]),
                normalized.clone(),
                sha256_hex(normalized.as_bytes()),
            ],
        )
        .await?;
    drop(connection);
    drop(database);
    Ok(())
}

fn current_pm_streams(
    scope: &OwnerScope,
) -> Result<(PmEnvelope, PmEnvelope), Box<dyn std::error::Error>> {
    let current_fixture: serde_json::Value = serde_json::from_str(PM_MARKET_ENVELOPE_V4)?;
    let down = PmEnvelope {
        schema_version: PM_ENVELOPE_VERSION,
        scope: scope.clone(),
        venue_id: "polymarket".into(),
        config_hash: "fixture".into(),
        source_id: "polymarket:market-ws".into(),
        connection_id: "market-1".into(),
        source_timestamp_ms: 1,
        canonical_source_rank: 0,
        connection_epoch: 0,
        frame_sequence: 1,
        receipt_timestamp_ms: 2,
        ingest_sequence: 2,
        raw_frame: Vec::new(),
        normalized: current_fixture["normalized"].clone(),
    };
    let account = PmEnvelope {
        schema_version: PM_ENVELOPE_VERSION,
        scope: scope.clone(),
        venue_id: "polymarket".into(),
        config_hash: "fixture".into(),
        source_id: "polymarket:user-ws".into(),
        connection_id: "account-1".into(),
        source_timestamp_ms: 1,
        canonical_source_rank: 0,
        connection_epoch: 0,
        frame_sequence: 1,
        receipt_timestamp_ms: 2,
        ingest_sequence: 3,
        raw_frame: Vec::new(),
        normalized: json!({
            "portfolio": "paper",
            "payload": {
                "kind": "fill",
                "market": "btc-5m",
                "outcome": "up"
            }
        }),
    };
    Ok((down, account))
}

#[tokio::test]
async fn migration_adds_stream_identity_and_limit_one_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: a v3 database containing one checked-in market/outcome envelope.
    let (_dir, path) = database_path("pm-stream-v4")?;
    seed_pm_market_v3(&path).await?;
    let scope = OwnerScope::new(PortfolioId::new("paper")?, RunId::new("run")?);
    let (down, account) = current_pm_streams(&scope)?;
    assert_eq!(down.schema_version, PM_ENVELOPE_VERSION);

    // When: the current store migrates it, appends the opposite outcome, and pages by one.
    let (first_items, second_items, third_items) = {
        let store = TursoTapeStore::open_local(&path).await?;
        let mut rows = store
            .connection
            .query(
                "SELECT canonical_market_id, stream_id, schema_version FROM pm_envelopes",
                (),
            )
            .await?;
        let row = rows.next().await?.ok_or("migrated PM row")?;
        assert_eq!(row.get::<String>(0)?, "btc-5m");
        assert_eq!(row.get::<String>(1)?, "market:btc-5m:up");
        assert_eq!(row.get::<i64>(2)?, 6);
        drop(rows);
        store.store_envelope(&down).await?;
        store.store_envelope(&account).await?;
        let first = store
            .read_envelopes(&scope, None, std::num::NonZeroUsize::MIN)
            .await?;
        let second = store
            .read_envelopes(
                &scope,
                first.next_cursor.clone(),
                std::num::NonZeroUsize::MIN,
            )
            .await?;
        let third = store
            .read_envelopes(
                &scope,
                second.next_cursor.clone(),
                std::num::NonZeroUsize::MIN,
            )
            .await?;
        let items = (first.items, second.items, third.items);
        store.delete_database()?;
        items
    };

    // Then: the cursor reaches account, down, and migrated-up streams without losing the market key.
    assert!(matches!(
        first_items.as_slice(),
        [ReplayItem::Envelope(envelope)] if envelope.normalized["portfolio"] == "paper"
    ));
    assert!(matches!(
        second_items.as_slice(),
        [ReplayItem::Envelope(envelope)] if envelope.normalized["payload"]["outcome"] == "down"
    ));
    assert!(matches!(
        third_items.as_slice(),
        [ReplayItem::Envelope(envelope)] if envelope.normalized["payload"]["outcome"] == "up"
    ));
    Ok(())
}

#[tokio::test]
async fn migration_rolls_back_on_failure() -> Result<(), Box<dyn std::error::Error>> {
    const FAILING_MIGRATION: Migration = Migration::new(
        10,
        &[
            "CREATE TABLE migration_partial_change (value INTEGER NOT NULL)",
            "CREATE TABLE migration_partial_change (",
        ],
    );

    // Given: a current database and a migration whose second statement is invalid.
    let (_dir, path) = database_path("migration-rollback")?;

    // When: the failing migration is applied.
    let (result, migrations, partial_table_count) = {
        let store = TursoTapeStore::open_local(&path).await?;
        let result = apply(&store.connection, &[FAILING_MIGRATION]).await;
        let migrations = migration_rows(&store.connection).await?;
        let partial_table_count = {
            let mut rows = store
                .connection
                .query(
                    "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'migration_partial_change'",
                    (),
                )
                .await?;
            let row = rows.next().await?.ok_or("table-count row")?;
            row.get::<i64>(0)?
        };
        drop(store);
        (result, migrations, partial_table_count)
    };

    // Then: its version and first schema change are both rolled back.
    assert!(matches!(result, Err(StoreError::Storage { .. })));
    assert!(matches!(
        migrations.as_slice(),
        [
            (1, _),
            (2, _),
            (3, _),
            (4, _),
            (5, _),
            (6, _),
            (7, _),
            (8, _),
            (9, _)
        ]
    ));
    assert_eq!(partial_table_count, 0);
    Ok(())
}

fn pm_account_fixture(
    fixture: &str,
    scope: OwnerScope,
    frame_sequence: i64,
) -> Result<PmEnvelope, Box<dyn std::error::Error>> {
    let fixture: serde_json::Value = serde_json::from_str(fixture)?;
    let schema_version = fixture["schema_version"]
        .as_u64()
        .ok_or("fixture schema_version")?;
    let normalized = fixture
        .get("normalized")
        .cloned()
        .ok_or("fixture normalized")?;
    Ok(PmEnvelope {
        schema_version: u16::try_from(schema_version)?,
        scope,
        venue_id: "polymarket".into(),
        config_hash: "fixture".into(),
        source_id: "polymarket:user-ws".into(),
        connection_id: "account-1".into(),
        source_timestamp_ms: 1_000 + frame_sequence,
        canonical_source_rank: 0,
        connection_epoch: 1,
        frame_sequence,
        receipt_timestamp_ms: 1_001 + frame_sequence,
        ingest_sequence: frame_sequence,
        raw_frame: format!("fixture-{frame_sequence}").into_bytes(),
        normalized,
    })
}

#[tokio::test]
async fn pm_account_envelope_version_migrates_old_and_reads_new_fixtures()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: a database at the pre-bump version containing v1 through v3 account envelopes.
    let (_dir, path) = database_path("pm-account-envelope-version")?;
    let scope = OwnerScope::new(PortfolioId::new("paper")?, RunId::new("run")?);
    let old_v1 = pm_account_fixture(PM_ACCOUNT_ENVELOPE_V1, scope.clone(), 1)?;
    let old_v2 = pm_account_fixture(PM_ACCOUNT_ENVELOPE_V2, scope.clone(), 2)?;
    let old_v3 = pm_account_fixture(PM_ACCOUNT_ENVELOPE_V3, scope.clone(), 3)?;
    let store = TursoTapeStore::open_local(&path).await?;
    store.store_envelope(&old_v1).await?;
    store.store_envelope(&old_v2).await?;
    store.store_envelope(&old_v3).await?;
    store
        .connection
        .execute(
            "DELETE FROM schema_migrations WHERE version IN (4, 5, 6, 7, 8, 9)",
            (),
        )
        .await?;
    drop(store);

    // When: the current store migrates the database and appends a current settlement envelope.
    let (items, migrations, new) = {
        let store = TursoTapeStore::open_local(&path).await?;
        let new = pm_account_fixture(PM_ACCOUNT_ENVELOPE_V4, scope.clone(), 4)?;
        store.store_envelope(&new).await?;
        let page = store
            .read_envelopes(
                &scope,
                None,
                std::num::NonZeroUsize::new(4).ok_or("fixture page size")?,
            )
            .await?;
        let migrations = migration_rows(&store.connection).await?;
        let items = page.items;
        drop(store);
        (items, migrations, new)
    };

    // Then: both fixtures replay under the current envelope version without losing JSON evidence.
    let mut migrated_v1 = old_v1;
    migrated_v1.schema_version = 6;
    let mut migrated_v2 = old_v2;
    migrated_v2.schema_version = 6;
    let mut migrated_v3 = old_v3;
    migrated_v3.schema_version = 6;
    assert_eq!(
        items,
        vec![
            ReplayItem::Envelope(migrated_v1),
            ReplayItem::Envelope(migrated_v2),
            ReplayItem::Envelope(migrated_v3),
            ReplayItem::Envelope(new)
        ]
    );
    assert!(matches!(
        migrations.as_slice(),
        [
            (1, _),
            (2, _),
            (3, _),
            (4, _),
            (5, _),
            (6, _),
            (7, _),
            (8, _),
            (9, _)
        ]
    ));
    Ok(())
}

#[tokio::test]
async fn decision_version_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a checked-in fixture created by the old bootstrap with no migration table.
    let (_dir, path) = database_path("migration-legacy")?;
    std::fs::write(
        &path,
        include_bytes!("../tests/fixtures/legacy-v0.db").as_slice(),
    )?;
    let (database, connection) = open_connection(&path).await?;
    connection
        .execute(
            "INSERT INTO causal_decisions (
                portfolio_id, run_id, correlation_id, source_timestamp_ms, ingest_sequence, payload_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            ("legacy", "run", "legacy-decision", 1_i64, 1_i64, r#"{"kind":"legacy-decision"}"#),
        )
        .await?;
    connection
        .execute(
            "INSERT INTO durable_intents (
                portfolio_id, run_id, correlation_id, source_timestamp_ms, ingest_sequence, state, payload_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, 'pending', ?6)",
            ("legacy", "run", "legacy-intent", 2_i64, 2_i64, r#"{"kind":"legacy-intent"}"#),
        )
        .await?;
    drop(connection);
    drop(database);

    // When: the current store opens the legacy fixture.
    let (migrations, killed, decision_version, intent_version, decisions, intents) = {
        let store = TursoTapeStore::open_local(&path).await?;
        let migrations = migration_rows(&store.connection).await?;
        let killed = store.kill_state(&PortfolioId::new("legacy")?).await?;
        let decision_version = {
            let mut rows = store.connection.query(
                "SELECT schema_version FROM causal_decisions WHERE correlation_id = 'legacy-decision'",
                (),
            ).await?;
            rows.next()
                .await?
                .ok_or("legacy decision row")?
                .get::<i64>(0)?
        };
        let intent_version = {
            let mut rows = store.connection.query(
                "SELECT schema_version FROM durable_intents WHERE correlation_id = 'legacy-intent'",
                (),
            ).await?;
            rows.next()
                .await?
                .ok_or("legacy intent row")?
                .get::<i64>(0)?
        };
        let scope = OwnerScope::new(PortfolioId::new("legacy")?, RunId::new("run")?);
        let decisions = store.read_decisions(&scope).await?;
        let intents = store.read_pending_intents(&scope).await?;
        drop(store);
        (
            migrations,
            killed,
            decision_version,
            intent_version,
            decisions,
            intents,
        )
    };

    // Then: legacy decisions advance to v2 while intents remain v1 without payload changes.
    assert!(matches!(
        migrations.as_slice(),
        [
            (1, _),
            (2, _),
            (3, _),
            (4, _),
            (5, _),
            (6, _),
            (7, _),
            (8, _),
            (9, _)
        ]
    ));
    assert!(killed);
    assert_eq!(decision_version, 2);
    assert_eq!(intent_version, 1);
    assert_eq!(decisions[0].payload, json!({"kind": "legacy-decision"}));
    assert_eq!(intents[0].payload, json!({"kind": "legacy-intent"}));
    Ok(())
}

#[tokio::test]
async fn decision_schema_v1_migrates_and_v2_reads() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a pre-bump decision row and database migration state.
    let (_dir, path) = database_path("causal-decision-v2")?;
    let old_payload: serde_json::Value = serde_json::from_str(OLD_CAUSAL_DECISION)?;
    let new_payload: serde_json::Value = serde_json::from_str(NEW_CAUSAL_DECISION)?;
    let store = TursoTapeStore::open_local(&path).await?;
    store
        .connection
        .execute(
            "INSERT INTO causal_decisions (
                portfolio_id, run_id, correlation_id, source_timestamp_ms,
                ingest_sequence, schema_version, payload_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6)",
            (
                "legacy",
                "run",
                "decision-v1",
                1_i64,
                1_i64,
                OLD_CAUSAL_DECISION,
            ),
        )
        .await?;
    store
        .connection
        .execute(
            "DELETE FROM schema_migrations WHERE version IN (5, 6, 7, 8, 9)",
            (),
        )
        .await?;
    drop(store);

    // When: the current store migrates the old row and writes a new decision.
    let store = TursoTapeStore::open_local(&path).await?;
    let scope = OwnerScope::new(PortfolioId::new("legacy")?, RunId::new("run")?);
    store
        .store_decision(&crate::CausalDecision {
            identity: crate::CausalIdentity {
                scope: scope.clone(),
                correlation_id: "decision-v2".into(),
                source_timestamp_ms: 2,
                ingest_sequence: 2,
            },
            payload: new_payload.clone(),
        })
        .await?;
    let decisions = store.read_decisions(&scope).await?;
    let migrations = migration_rows(&store.connection).await?;
    let mut versions = store
        .connection
        .query(
            "SELECT schema_version FROM causal_decisions ORDER BY ingest_sequence",
            (),
        )
        .await?;
    let first_version = versions
        .next()
        .await?
        .ok_or("old decision row")?
        .get::<i64>(0)?;
    let second_version = versions
        .next()
        .await?
        .ok_or("new decision row")?
        .get::<i64>(0)?;

    // Then: both payload generations read under decision schema version 2.
    assert_eq!(first_version, 2);
    assert_eq!(second_version, 2);
    assert_eq!(decisions[0].payload, old_payload);
    assert_eq!(decisions[1].payload, new_payload);
    assert!(matches!(
        migrations.as_slice(),
        [
            (1, _),
            (2, _),
            (3, _),
            (4, _),
            (5, _),
            (6, _),
            (7, _),
            (8, _),
            (9, _)
        ]
    ));
    drop(store);
    Ok(())
}
