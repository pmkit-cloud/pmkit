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
