# PMKit storage compatibility

## Durable version boundary

`pm_envelopes.schema_version` is the version of the normalized envelope and its
serialized metadata. The current version is `1`, defined by
`pmkit-store::schema::PM_ENVELOPE_VERSION`.

Readers must reject unsupported versions with a typed replay/integrity error;
they must not silently deserialize an unknown shape or substitute defaults.
Raw frames and normalized hashes remain immutable evidence for every stored
envelope.

## Database schema migrations

`TursoTapeStore::open_local` reads `schema_migrations`, whose rows contain a
monotonically increasing integer `version` and an `applied_at` timestamp. It
applies each pending forward migration in order using one Turso transaction per
migration; the schema changes and version row commit together or roll back
together. A fresh database and a legacy database created by the former
`CREATE TABLE IF NOT EXISTS` bootstrap both begin at version `0` and follow the
same migration path to the current version.

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
