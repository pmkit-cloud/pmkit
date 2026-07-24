# decisions

## Todo 3: manifest path redaction

Manifest v1 serializes `runtime.manifest_dir` as the fixed string `"<redacted>"`.
Todo 5 should preserve that representation so provenance never reintroduces an
absolute path or a user-identifying basename.

## 2026-07-24 - Turso schema migration baseline

- Database schema versions are monotonically increasing `i64` integers recorded
  in `schema_migrations(version, applied_at)`; the current baseline is version 1.
- `open_local` uses Turso 0.7's `unchecked_transaction()` once per ordered
  migration. Its SQL statements and version insert commit together, and any
  statement failure triggers an explicit rollback before the error escapes.
- A missing migration table means version 0. Fresh and pre-migration legacy
  databases therefore follow the same version-0-to-version-1 path. A version
  above the binary maximum returns `StoreError::DatabaseSchemaTooNew`; no
  in-process downgrade exists.

## Todo 5: manifest v2 provenance

- `parse_manifest` returns `VersionedManifest::{V1, V2}`. `ManifestV1` remains
  readable unchanged; `ManifestV2` adds one top-level `provenance` object with
  nested git commit/dirty state, the workspace `Cargo.lock` SHA-256, and the
  rustc toolchain identity. Migration from v1 to v2 is additive only.
- `build_manifest` selects `Provenance::current()`, whose values are generated
  by `build.rs` at compile time. `build_manifest_with_provenance` accepts a
  typed value for deterministic tests and future manifest transformations.
- A build without git records `git.commit = "unknown"` and
  `git.dirty = false`; runtime code never executes git or rustc.
