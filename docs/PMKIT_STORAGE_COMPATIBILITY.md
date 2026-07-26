# PMKit storage compatibility

## Durable version boundary

`pm_envelopes.schema_version` is the version of the normalized envelope and its
serialized metadata. The current version is `6`, defined by
`pmkit-store::schema::PM_ENVELOPE_VERSION`.

Version `4` adds a stable stream identity that includes the envelope kind and,
for public-market envelopes, the exact market and outcome. Market streams use
`market:<market-id>:<up|down>` and account streams use
`account:<portfolio-id>`.

Version `5` adds a typed identity to normalized account fills. Polymarket fills
use the venue-provided `TradeMessage.id`. Boundaries without a venue fill id use
the persisted transport coordinates (`source_id`, `connection_id`,
`connection_epoch`, and `frame_sequence`) instead of inferring identity from
fill economics or timestamps. New normalized account payloads are version `3`
and require the tagged identity. Migrated version-1 and version-2 payloads keep
their immutable normalized JSON and derive the same transport identity during
replay from the durable envelope columns.

Version `6` adds the same typed identity boundary to normalized account
settlements. Settlement identity is either venue-assigned or the persisted
transport coordinates (`source_id`, `connection_id`, `connection_epoch`, and
`frame_sequence`); settlement economics and timestamps are never identity.
New normalized account payloads are version `4` and require the tagged
settlement identity. Migrated version-1 through version-3 payloads retain their
immutable normalized JSON and derive transport identity during replay.

Readers must reject unsupported versions with a typed replay/integrity error;
they must not silently deserialize an unknown shape or substitute defaults.
Raw frames and normalized hashes remain immutable evidence for every stored
envelope.

`causal_decisions.schema_version` and `durable_intents.schema_version` version
their stored records independently of `payload_json`. Current causal decisions
are version `2`, recording the resolved maker/taker simulation fee model;
durable intents remain version `1`. Writers persist each version explicitly,
and readers return a typed `StoreError::UnsupportedRecordSchemaVersion` instead
of skipping a row whose version they cannot decode.

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

Database migration version `4` advances durable PM envelopes from version `1`
to version `2` for the owner-scoped settlement account-event shape. It updates
only rows whose `schema_version` is exactly `1`; unsupported versions remain
untouched and replay as `ReplayGapReason::UnsupportedSchemaVersion`. Existing
`normalized_json`, raw frames, and integrity hashes remain byte-for-byte
unchanged because all pre-v2 account variants retain their original shape.

Database migration version `5` advances only `causal_decisions` rows whose
record schema is exactly version `1` to version `2`. Existing decision payloads
and causal identities remain byte-for-byte unchanged and readable; new version-2
decision snapshots include the resolved simulation fee model. Unsupported
record versions remain untouched and fail closed in the reader.

Database migration version `6` rebuilds `pm_envelopes` with a persisted
`canonical_market_id` extracted from each normalized payload. The market key is
part of both durable uniqueness and replay cursor ordering, so two markets with
identical source timestamp/rank/connection/frame metadata remain distinct and
replay in a total deterministic order. Version-2 envelope rows advance to
version 3; records without a market retain the empty key, and raw/normalized
evidence remains byte-for-byte unchanged.

Database migration version `7` preserves migration 6's
`canonical_market_id` and adds `stream_id` as the next replay-cursor and
uniqueness discriminator. Existing v3 market rows derive market/outcome streams,
account rows derive owner streams, and legacy rows without enough typed evidence
fall back to their stable source identity. Version-3 rows advance to version 4;
raw frames, normalized JSON, and both integrity hashes remain byte-for-byte
unchanged. Checked-in v3/v4 fixtures and a limit-1 cursor test lock the migration.

Database migration version `8` advances version-4 PM envelopes to version `5`.
It preserves raw frames, normalized JSON, and both integrity hashes byte-for-byte.
Checked-in account fixtures cover version-1 order acknowledgements, version-2
settlements, and version-3 fills carrying venue identity. During replay, only
legacy normalized account versions may derive transport identity when the field
is absent; missing identity in a version-3 fill fails closed.

Database migration version `9` advances version-5 PM envelopes to version `6`.
Raw frames, normalized JSON, and both integrity hashes remain byte-for-byte
unchanged. Checked-in account fixtures cover legacy identity-free settlements
and the version-4 payload carrying stable settlement identity. Missing identity
in a version-4 settlement fails closed; older payloads use only durable
transport coordinates as their migration fallback.

Opening fails with a typed `StoreError` when the newest on-disk version exceeds
the binary's maximum supported version. PMKit never auto-downgrades a database.

Rollback is operational: stop the process and restore the prior database file
from backup, then run the prior binary. In-process migrations are forward-only;
there is no reverse-migration or automatic downgrade path. A pre-v9 binary
rejects a v9 database as too new, so rollback requires the pre-v9 backup.

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
