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

## 2026-07-24 - Persisted finalized progression

- Migration 3 creates `finalized_chain_checkpoints(chain_id PRIMARY KEY,
  block_number, block_hash, updated_at)`. `TursoTapeStore::finalized_checkpoint`
  restores the per-chain checkpoint after restart for Todo 25/27 coordination.
- `ingest_finalized_batch` rejects a lower provider finalized height with
  `StoreError::FinalizedHeadRegression`. Equal heights require the persisted
  hash; advancing heights require coverage through the full finalized
  `BlockHead` and Todo-23 parent linkage back to the persisted hash.
- Incomplete coverage is held with no canonical writes. A verified advance
  filters logs at or below the effective finalized head and commits canonical
  replacement plus the monotonic checkpoint upsert in one transaction.

## 2026-07-24 - Enriched Polymarket order status

- `OrderStatus` is `Open(OrderStatusDetails) | Accepted(OrderStatusDetails) |
  Rejected(OrderStatusDetails) | Cancelled(OrderStatusDetails) |
  Unknown(OrderStatusDetails)`, where `OrderStatusDetails` is `{ filled_qty:
  Option<Decimal>, price: Option<Decimal>, fee: Option<Decimal>,
  settlement_reference: Option<String> }`.
- The Executor seam remains `async fn query_status(&self, order_id: &OrderId)
  -> Result<OrderStatus, ExecError>`.
- Polymarket `size_matched` and `price` populate the corresponding fields. Its
  order response has no fee or settlement transaction reference, so both stay
  `None`; associated trade IDs are not relabeled as settlement references.
- Unknown venue status strings fail closed as `ExecError::Transport`, while an
  absent order (HTTP 404) maps to `ExecError::NotFound`.

## 2026-07-24 - Fail-closed intent recovery

- Todo 10 aborts recovery instead of adding another durable intent state.
  Missing venue IDs, status-query timeouts or failures, and
  `OrderStatus::Unknown` return `StartError::ExecutionState`.
- The existing `pending` or `unknown` row stays unresolved on abort. Only
  unambiguous `Open`/`Accepted` or `Rejected`/`Cancelled` statuses transition
  it to the existing accepted or rejected outcome.
