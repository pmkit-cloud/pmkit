# learnings

## 2026-07-24 - Versioned causal records

- Keep every migration's numeric version fixed after release: migration 1
  remains the legacy bootstrap, and migration 2 only appends the two causal
  record columns.
- A constant `DEFAULT 1` on `ALTER TABLE ... ADD COLUMN ... NOT NULL` migrates
  pre-column Turso/SQLite rows in place without rewriting `payload_json`.
- Writers bind record versions explicitly; readers validate before decoding and
  return `StoreError::UnsupportedRecordSchemaVersion` so no row is skipped.
- Keep decision and intent version constants separate even while both are 1;
  Todo 20/21 can bump only the durable shape they actually change and follow the
  same migration + old/new fixture pattern.

## 2026-07-24 - Verified finalized block linkage

- `BlockHead` is `{ chain_id: ChainId, block_number: u64, block_hash: String,
  parent_hash: String }`; `BlockHead::new(chain_id, block_number, block_hash,
  parent_hash)` requires both hashes, so adapters cannot fabricate a default.
- `FinalizedBlockCoverage` remains `{ range: FinalizedBlockRange, blocks:
  Vec<BlockHead> }`. `FinalizedBlockCoverage::new(range, blocks)` now enforces
  complete ordered range coverage plus linkage, and `verify(&self) ->
  Result<(), ChainSourceError>` re-verifies deserialized evidence.
- Linkage is publicly exposed as `verify_block_header_linkage(blocks:
  &[BlockHead]) -> Result<(), ChainSourceError>`. It compares every header after
  the first with its preceding header; the first header retains its external
  `parent_hash` anchor for Todo 24 progression but cannot prove that anchor from
  the current batch alone.
- `FinalizedRawLogBatch` is `{ provider, range, head, finalized, coverage, logs
  }`; `FinalizedRawLogBatch::verify(&self)` preserves all prior finalized-range,
  chain, identity, and duplicate checks and adds coverage/range equality plus
  linkage. A mismatch returns `ChainSourceError::BrokenBlockLinkage {
  block_number, expected_parent_hash, actual_parent_hash }`.

## 2026-07-24 - Gamma resolution facts

- `MarketResolutionEvent` carries `market: MarketId`, categorical
  `outcome: Outcome`, exact `resolution_price: Decimal`, and
  `timestamp_ms: i64`; Todo 8/11 can consume this shape without introducing
  floating-point settlement values.
