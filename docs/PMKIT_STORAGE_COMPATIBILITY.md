# PMKit storage compatibility

## Durable version boundary

`pm_envelopes.schema_version` is the version of the normalized envelope and its
serialized metadata. The current version is `1`, defined by
`pmkit-store::schema::PM_ENVELOPE_VERSION`.

Readers must reject unsupported versions with a typed replay/integrity error;
they must not silently deserialize an unknown shape or substitute defaults.
Raw frames and normalized hashes remain immutable evidence for every stored
envelope.

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

The current store uses `CREATE TABLE IF NOT EXISTS` for bootstrap only. It is
not a migration mechanism. Until migration tooling exists, a durable schema
change must fail clearly on an existing incompatible database rather than
mutating it implicitly.

## Scope

This policy covers local Turso/libSQL storage and JSON-lines tape records. It
does not define the separate raw collector/object-store retention plane.
