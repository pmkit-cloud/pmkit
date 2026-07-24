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

## 2026-07-24 - Owner-scoped settlement events

- `PmAccountEvent::Settlement` is `{ market: MarketId, outcome: Outcome,
  settled_size: Decimal, proceeds: Decimal, timestamp_ms: i64 }`. Ownership
  remains on `PmAccountEnvelope::portfolio`; settlement does not belong to
  `MarketEvent` and carries no duplicate cancel/reject state.
- Account-envelope JSON serializes settlement as `kind = "settlement"` with
  exact decimal strings for `settled_size` and `proceeds`. It routes through
  `SourceEnvelope::PmAccount` to `StrategyFact::Account` unchanged.
- `PM_ENVELOPE_VERSION` is `2`; database migration `4` updates only
  `pm_envelopes.schema_version = 1` rows to `2`. Existing normalized JSON and
  hashes are preserved, while every other unsupported version remains a typed
  `ReplayGapReason::UnsupportedSchemaVersion`.
- Market lifecycle remains source-gated: current Polymarket market streams
  expose book/trade frames and account streams expose order/trade frames, but
  neither exposes authoritative open, paused, or closed transitions.

## 2026-07-24 - Fail-closed intent recovery

- Todo 10 aborts recovery instead of adding another durable intent state.
  Missing venue IDs, status-query timeouts or failures, and
  `OrderStatus::Unknown` return `StartError::ExecutionState`.
- The existing `pending` or `unknown` row stays unresolved on abort. Only
  unambiguous `Open`/`Accepted` or `Rejected`/`Cancelled` statuses transition
  it to the existing accepted or rejected outcome.

## 2026-07-24 - Multi-provider finality quorum

- `agree_on_finalized_heads(configured_provider_count: usize, provider_heads:
  &[FinalizedProviderHead]) -> Result<BlockHead, ChainSourceError>` uses a
  strict majority of configured providers with a minimum of two corroborating
  identities. Two configured providers retain exact legacy head/finality
  equality; larger configurations quorum on the complete finalized block.
- `agree_on_finalized_log_batches(configured_provider_count: usize, batches:
  &[FinalizedRawLogBatch]) -> Result<FinalizedRawLogBatch, ChainSourceError>`
  requires the same quorum on verified range, coverage, and provider-neutral
  logs ending at the agreed finalized height.

## 2026-07-24 - Authoritative live fill and settlement ledger

- A live run with an authenticated account source treats `PmAccountEvent::Fill`
  and `PmAccountEvent::Settlement` as its only position/PnL authority;
  `MarketEvent::Fill` remains a compatibility fallback only when no account
  source is configured, so one physical fill never has two writers.
- `LiveRiskState` deduplicates normalized fill identity `(order, market,
  outcome, side, price, size, timestamp)` and settlement identity `(market,
  outcome, size, proceeds, timestamp)`. It owns the fill count, realized PnL,
  fees, positions, and marked daily PnL; duplicate durable replay is a no-op.
- Todo 15 can rebuild these identities and balances by replaying the durable
  account records into a fresh `LiveRiskState`; no durable derived snapshot was
  added as a second authority.

## 2026-07-24 - Durable paper-account ledger

- Paper records reuse owner-scoped `CausalDecision` rows; no table or migration
  was added. Each payload is tagged `record_type = "paper_ledger"`, version 1,
  and carries `event_id = "paper-ledger-{sequence}"`, contiguous sequence,
  logical timestamp, and one event: cash movement, order placement,
  ack/rejection, cancel, fill, or settlement.
- Submission is represented as `OrderPlaced` followed by `OrderAck` or
  `OrderRejected`. The ack stores the generated order id, immediate/resting/
  delayed state, and exact activation time. This reconstructs rejected-attempt
  ID consumption, partial resting quantities, delayed orders, and the next id.
- `PaperExecutor::reconstruct` deduplicates byte-equivalent stable event
  identities, rejects conflicting duplicates, sequence gaps, dangling/unknown
  order transitions, mismatched or oversize fills, and inconsistent
  settlements, then rebuilds cash, fees, realized PnL, per-market positions,
  open orders, and `SimEngine` from records only. No derived snapshot exists.
- `paper.rs` restores before feed consumption and persists executor entries
  through `store_decision` after each mutation. Strategy positions now come
  from `PaperExecutor::positions_for_market`, so the durable reducer is the
  paper account authority that Todos 20 and 21 should extend rather than a
  parallel position vector.

## 2026-07-24 - Tightening-only scoped risk limits

- `PartialRiskLimits` mirrors all eight numeric `RiskLimits` fields as
  `Option`s. `RiskLimitOverrides` is `{ per_market: HashMap<MarketId,
  PartialRiskLimits>, per_strategy: HashMap<StrategyId, PartialRiskLimits> }`
  and is attached to `StrategyRegistration` through `.risk_overrides(...)`;
  existing `RiskLimits` struct literals and registrations remain unchanged.
- `RiskLimitOverrides::effective_limits(&RiskLimits, &MarketId, &StrategyId)
  -> RiskLimits` clones the globals, then tightens each field with the matching
  market and strategy values via `min`; larger override values cannot loosen a
  global limit, and empty maps return an exact clone of the globals.
- `live.rs` precomputes `effective_limits_by_strategy` before the event loop and
  passes the selected plain `RiskLimits` to `passes_aggregated_risk`. Todo 14
  can add its rate state alongside that map; Todo 15 should keep replaying into
  the existing `LiveRiskState` and leave override configuration non-durable.

## 2026-07-24 - Parallel deterministic backtests

- `PmkitBuilder::start` uses a Tokio `JoinSet` capped by
  `RuntimeConfig.backtest_concurrency.get()`. Only independent backtest drivers
  enter the set; paper/live runs drain it first and remain sequential barriers,
  while each backtest keeps its existing single ordered event loop.
- Duplicate run IDs are rejected before any task is spawned. `AppHandle` keeps
  keyed reports in its `HashMap` and exposes submission order through
  `reports_ordered(&self) -> Vec<(&RunId, &RunReport)>`, so completion timing and
  `HashMap` iteration cannot affect callers.

## 2026-07-24 - Restart-safe logical-time order rate limits

- `OrderRateLimits` is the live-risk companion spec: 100 accepted submissions
  per strategy and 1,000 per portfolio in a 60,000-ms logical window by
  default. `OrderRateState` anchors each fixed window at its first accepted
  event timestamp and includes the exact end (`start + duration`); only a
  later timestamp opens a new window.
- The limiter runs only in the `Action::Place` branch after open-order capacity
  and Todo-13 effective limits pass. A denied submission gets the durable
  `order submission rate limit` risk verdict and increments `rejected`; market
  data and intent reconciliation never call the limiter.
- Live decision identities are strategy-scoped. Startup reconstructs accepted
  logical timestamps from accepted durable decision verdicts plus pending and
  unknown intents, deduplicated by causal action identity, then replays them in
  timestamp order so only the latest fixed window remains in memory. Todo 15
  should preserve this decision/intent replay before reconciliation; Todo 22
  can read the portfolio window without making it a second durable authority.

## 2026-07-24 - Reconstructed live risk state

- Live startup pages through owner-scoped `TapeStore::read_envelopes` and replays
  durable account fills and settlements into the same `LiveRiskState` that
  receives new account events. Todo-11 fill/settlement identities therefore
  deduplicate startup replay against later live delivery without a snapshot.
- Durable account payloads are parsed fail-closed. Replay gaps, malformed or
  owner-mismatched payloads, settlements without a matching position, and
  oversettlements abort startup through `StartError::Storage`; no partial state
  reaches the event loop.
- Decisions and pending/unknown intents remain the durable inputs for Todo-14
  order-rate reconstruction and Todo-10 intent recovery because they contain no
  fill facts. Open-order risk counts remain sourced from the existing venue
  reconciliation, not a persisted derived snapshot. Todo 22 can read restored
  `portfolio_notional`/`market_notional` directly from `LiveRiskState`.
