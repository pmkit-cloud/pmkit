# PMKit storage compatibility

## Durable version boundary

`pm_envelopes.schema_version` is the version of the normalized envelope and its
serialized metadata. The current version is `1`, defined by
`pmkit-store::schema::PM_ENVELOPE_VERSION`.

Readers must reject unsupported versions with a typed replay/integrity error;
they must not silently deserialize an unknown shape or substitute defaults.
Raw frames and normalized hashes remain immutable evidence for every stored
envelope.

`causal_decisions.schema_version` and `durable_intents.schema_version` version
their stored records independently of `payload_json`. Both current record
versions are `1`; writers persist that version explicitly, and readers return a
typed `StoreError::UnsupportedRecordSchemaVersion` instead of skipping a row
whose version they cannot decode. The JSON payload shape is unchanged.

## Database schema migrations

`TursoTapeStore::open_local` reads `schema_migrations`, whose rows contain a
monotonically increasing integer `version` and an `applied_at` timestamp. It
applies each pending forward migration in order using one Turso transaction per
migration; the schema changes and version row commit together or roll back
together. A fresh database and a legacy database created by the former
`CREATE TABLE IF NOT EXISTS` bootstrap both begin at version `0` and follow the
same migration path to the current version.

Database migration version `2` adds non-null `schema_version` columns to
`causal_decisions` and `durable_intents`, each with default `1`. SQLite applies
that default to pre-column legacy rows, so their existing payloads and causal
identities remain unchanged while becoming explicit version-1 records.

Database migration version `3` creates `finalized_chain_checkpoints`, keyed by
`chain_id`, with the durable finalized block number, block hash, and update
timestamp. Finalized ingestion rejects a lower block number with
`StoreError::FinalizedHeadRegression`; equal-height updates require the same
hash. An advancing batch must carry complete linked header coverage through the
reported finalized `BlockHead`. Incomplete evidence is held without canonical
log writes, and a verified checkpoint advance commits in the same transaction
as its canonical logs.

Opening fails with a typed `StoreError` when the newest on-disk version exceeds
the binary's maximum supported version. PMKit never auto-downgrades a database.

Rollback is operational: stop the process and restore the prior database file
from backup, then run the prior binary. In-process migrations are forward-only;
there is no reverse-migration or automatic downgrade path.

## Change policy

Any change to a durable envelope, causal decision, intent, or chain-record shape
must include all of the following in one change:

1. Increment the relevant schema version.
2. Add a migration or an explicit incompatibility error for existing local
   stores.
3. Add a fixture covering the old and new representations.
4. Preserve raw evidence and stable owner/correlation identity.
5. Document rollback and downgrade behavior.

Adding an optional field is not automatically backward-compatible: readers must
define its default and writers must remain readable by the previous version, or
the change requires a version bump and migration.

Future durable schema changes must append a migration and increment the current
database schema version; they must not edit an already-released migration.

## Scope

This policy covers local Turso/libSQL storage and JSON-lines tape records. It
does not define the separate raw collector/object-store retention plane.
