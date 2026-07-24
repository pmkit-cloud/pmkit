use std::path::{Path, PathBuf};

use crate::{
    StoreError, TapeStore, TursoTapeStore,
    migrations::{Migration, apply},
};

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

    // Then: the one current migration is recorded once with a timestamp.
    assert!(matches!(
        first_rows.as_slice(),
        [(1, applied_at)] if !applied_at.is_empty()
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
            "INSERT INTO schema_migrations (version, applied_at) VALUES (2, CURRENT_TIMESTAMP)",
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
            database_version: 2,
            max_supported_version: 1,
        })
    ));
    Ok(())
}

#[tokio::test]
async fn migration_rolls_back_on_failure() -> Result<(), Box<dyn std::error::Error>> {
    const FAILING_MIGRATION: Migration = Migration::new(
        2,
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
    assert!(matches!(migrations.as_slice(), [(1, _)]));
    assert_eq!(partial_table_count, 0);
    Ok(())
}

#[tokio::test]
async fn legacy_fixture_migration_preserves_records() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a checked-in fixture created by the old bootstrap with no migration table.
    let (_dir, path) = database_path("migration-legacy")?;
    std::fs::write(
        &path,
        include_bytes!("../tests/fixtures/legacy-v0.db").as_slice(),
    )?;

    // When: the current store opens the legacy fixture.
    let (migrations, killed) = {
        let store = TursoTapeStore::open_local(&path).await?;
        let migrations = migration_rows(&store.connection).await?;
        let killed = store
            .kill_state(&pmkit_core::PortfolioId::new("legacy")?)
            .await?;
        drop(store);
        (migrations, killed)
    };

    // Then: it reaches version 1 without rewriting its committed record.
    assert!(matches!(migrations.as_slice(), [(1, _)]));
    assert!(killed);
    Ok(())
}
