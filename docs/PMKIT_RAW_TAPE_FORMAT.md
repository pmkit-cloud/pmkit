# PMKit raw tape format

## Version 1 record

Raw WebSocket text frames are UTF-8 JSON Lines. Each line is one JSON object:

```json
{"schema_version":1,"receipt_time_ms":42,"connection_id":"connection-1","raw":"{\"event_type\":\"book\"}"}
```

- `schema_version` is the integer `1`. Readers reject other versions.
- `receipt_time_ms` is the local Unix receipt time in milliseconds.
- `connection_id` identifies one connection lifetime; reconnects use a new ID.
- `raw` is the exact UTF-8 text frame received before adaptation.

Records end with one `\n`. Embedded newlines in `raw` are JSON-escaped, so a
physical line remains one record.

## Flush and crash recovery

`append_raw` completes only after the full JSON object and its trailing newline
are accepted by the writer. `flush` pushes buffered bytes to the underlying
writer and must be called during graceful shutdown. It does not promise disk
`fsync` or remote object durability.

After a crash, readers scan newline-terminated records in order. A final byte
tail without a newline is incomplete and must be discarded before appending.
A malformed newline-terminated record or unsupported schema version is a
fail-closed corruption error, not a skippable frame.

## Redundant raw spool v1

The existing v1 raw-line format remains compatible with `RawTapeSink`. The
additive raw-spool format stores independently captured binary frames before
adaptation. Callers choose a portable filesystem root; PMKit does not prescribe
an absolute path or a deployment-specific directory.

Each frame line includes `schema_version: 1`, `replica_id`, `shard_id`,
`minute_start_ms`, `connection_epoch`, `frame_sequence`, `receipt_time_ms`, a
lowercase `discovery_snapshot_sha256`, and `raw_bytes`. `raw_bytes` is a JSON
byte array so arbitrary original bytes round-trip without UTF-8 conversion.
`minute_start_ms` is an exact Unix UTC-minute boundary.

The chunk layout is:

```text
<root>/<replica_id>/<shard_id>/minute-<utc-minute-ms>.open
<root>/<replica_id>/<shard_id>/minute-<utc-minute-ms>.jsonl
<root>/<replica_id>/<shard_id>/minute-<utc-minute-ms>.jsonl.sha256
```

Replica and shard IDs are restricted to portable alphanumeric, `_`, and `-`
components. A writer accepts only frames whose receipt time falls within its
minute. A reconnect increments `connection_epoch`; `frame_sequence` may restart
for the new epoch. Repeated byte-identical frames are retained as distinct
records.

### Checked-in raw-spool compatibility fixture

`crates/pmkit-tape/tests/fixtures/raw-spool-v1.jsonl` is the portable v1
fixture. It contains a byte-preserving frame with lane, shard, UTC-minute,
connection epoch, sequence, receipt time, and discovery-snapshot provenance.
The contract test decodes the fixture, re-encodes it, and rejects version `2`
with `SpoolError::UnsupportedSchemaVersion`; consumers must retain that
fail-closed behavior.

Raw-spool v1 has no implicit migration. A record with an absent, older, or
future schema version is not interpreted as v1. Upgrade by writing new complete
v1 chunks while preserving prior chunks as immutable evidence; rollback means
using a reader that supports the retained chunk version rather than rewriting
those chunks in place.

On opening an existing `.open` chunk, recovery validates each complete line and
truncates only an unterminated final line, returning typed uncertainty with its
discarded byte count. A malformed newline-terminated line is fail-closed. The
validated prefix is never rewritten.

Close flushes and fsyncs the `.open` file, then atomically publishes it as a
new `.jsonl` name without replacing a pre-existing closed chunk, writes a
checksum sidecar, and fsyncs the shard directory. The sidecar SHA-256 is the
digest of the exact `.jsonl` bytes; its separate identity fields bind that
digest to the chunk. Consumers enumerate only `.jsonl` chunks with a matching
sidecar and fully valid lines. A checkpoint identity is constructed only from
such a verified closed chunk. Attempting to open or close over an already
closed chunk fails rather than rewriting its evidence.

## Compatibility and migration

The pre-v1 development format used `received_ms` and had no `schema_version`.
The v1 decoder reports it as unsupported version `0`; it never guesses that
`received_ms` means `receipt_time_ms`.

Migration is an explicit line-by-line rewrite that adds `schema_version: 1`,
renames `received_ms` to `receipt_time_ms`, and preserves `connection_id` and
`raw` byte-for-byte after JSON decoding. Keep the original tape until the v1
rewrite passes complete-record validation. Rollback means restoring that
original tape; v1 files are not rewritten in place and have no implicit
downgrade path.

Object-store durability, checksums, and process-loss upload recovery belong to
P2-3 and do not change this record format.

## Object-store durability plane (P2-3)

`pmkit-archive` durably retains raw records without changing the record format
above. Records are buffered into fixed-size **segments** of newline-delimited
v1 records and uploaded through an S3-shaped multipart `ObjectStore`: initiate,
upload parts, then complete. Each part and each full segment carries a SHA-256
checksum.

A segment becomes durable only once the atomic `manifest.json` references it.
The manifest is the sole source of truth: it lists every committed segment with
its checksum and record count and is written last, as a single atomic object
(`put`). A segment whose multipart upload completes but whose manifest commit
never lands is ignored on recovery and never counted as durable.

## Manifest schema and recovery

The manifest is `{"schema_version":1,"segments":[{"key","sha256_hex",
"records"}]}`. Readers reject any other `schema_version` with a fail-closed
corruption error; they never guess an unknown shape.

On restart, recovery reads the manifest, re-reads every committed segment and
verifies its checksum (a mismatch or missing segment fails closed), then aborts
any dangling multipart uploads left by the dead process. Recovery therefore
never overstates durability and never resurrects partial uploads.

## Compatibility and rollback

A change to the manifest or segment shape follows
`docs/PMKIT_STORAGE_COMPATIBILITY.md`: bump `schema_version`, add a migration or
explicit incompatibility error, add old/new fixtures, and preserve committed
segment checksums as immutable evidence. Rollback means retaining the previous
manifest and segments; committed segment objects are immutable and are never
rewritten in place. The concrete S3 client that implements `ObjectStore` is a
separate product-infrastructure project and does not change this plane's
contract.

## Columnar derivation (P2-4 evaluation)

Parquet/columnar files are **not** part of the OSS raw plane. The newline-
delimited v1 segments are the authoritative, immutable evidence. Any future
columnar derivation is a pure secondary artifact of the separate retention
project: it must be re-derivable from committed segments, verifiable against the
manifest's segment checksums, and never a replacement for the raw segments. No
`arrow`/`parquet` dependency lives in OSS, and adding one is gated on that
secondary-only contract.

## Retention and compaction (P2-5)

Raw evidence is immutable in OSS. `pmkit-archive` writes each segment once to a
monotonically indexed key through the multipart + atomic-manifest path and never
rewrites it in place; reopening an archive only appends new segments. OSS never
compacts, merges, or deletes raw segments.

Retention windows, archival tiering, and deriving-and-compacting *secondary*
analytical files are retention-project concerns. Those secondary files are
deferred (see the columnar note above), so their compaction lives with them in
the separate project. Any such compaction operates only on derived secondaries
and must leave the raw segments and their checksums untouched.
