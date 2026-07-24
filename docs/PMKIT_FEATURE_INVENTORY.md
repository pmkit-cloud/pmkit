# PMKit feature inventory

This is the review inventory for PMKit OSS. Statuses describe the current
repository, not the long-term target.

## Status vocabulary

- **Implemented**: usable public behavior exists and is tested.
- **Partial**: an important seam exists, but a production path is missing.
- **Gap**: not implemented.
- **Deliberately absent**: excluded by an explicit product or safety boundary.

## 1. Core domain

### Implemented

- Typed ownership IDs: `PortfolioId`, `MarketId`, `StrategyId`, `RunId`.
- `PortfolioKey`, `StrategyKey`, and explicit Backtest/Paper/Live modes.
- Empty and whitespace-only IDs fail with typed errors.
- Portfolio and strategy ownership isolation.

**Crate:** `pmkit-core`.

## 2. Market primitives

### Implemented

- Assets: BTC, ETH, SOL, XRP.
- Binary outcomes: Up and Down.
- Market durations: 5m and 15m.
- Exchange identifiers: Binance, Bybit, Coinbase, OKX, Kraken, Chainlink, Vatic.
- Exchange-specific symbols for supported assets.

**Crate:** `pmkit-market`.

## 3. Money, books, and math

### Implemented

- Decimal USDC `Money` type.
- Order books, positions, best bid/ask, midpoint, spread, imbalance.
- Book walking and fill application.
- Kelly sizing, post-only clamping, drawdown penalty, budget caps, VPIN, and OFI.
- Fee rates, maker rebates, taker fees, effective cost.
- GBM/LMSR fair value, probability gaps, logits, sigmoid, normal-CDF approximation,
  momentum, and probability blending.

**Crates:** `pmkit-money`, `pmkit-book`, `pmkit-math`, `pmkit-accounting`.

### Implemented

- Chain- and venue-independent `pmkit-accounting::PortfolioLedger` for
  normalized fills and settlements, including cash, fees, realized PnL,
  marked unrealized PnL, equity, and token positions.

### Gaps

- Portfolio-wide exposure aggregation.
- Model calibration and model registry.

## 4. Events and data contracts

### Implemented

- Normalized market events: book, BBA, trade, fill, order acknowledgement, tick.
- Normalized account events: fill and order acknowledgement.
- CEX reference events: matched reference trades only.
- Typed lossless envelopes for PM market, PM account, and CEX reference streams.
- Envelope metadata: schema version, source/connection identity, source/receipt time,
  connection epoch, frame sequence, canonical rank, ingest sequence, raw frame,
  normalized projection.
- Strategy-facing `StrategyFact` excludes transport metadata.

**Crate:** `pmkit-event`.

### Gaps

- Normalized cancel, reject, settlement, resolution, and market-lifecycle events.
- Envelope migration/version tooling.

## 5. Market data

### Implemented

- `HistoricalDataSource` and `LiveDataSource` traits.
- Raw PM market/account frame source contracts.
- Data, watermark, and EOF source lifecycle signals.
- Typed source failures and replay gaps.
- Binance `@aggTrade` parser.
- Binance Vision `aggTrades` parser.
- Live/history parity tests preserving aggregate ID and exchange timestamp.
- Verified bounded Binance archive cache:
  - transfer, ZIP, and CSV byte limits;
  - SHA-256 verification;
  - per-key locking;
  - atomic installation;
  - bounded quota and eviction;
  - typed replay gaps.
- `BinanceVisionHistory` source adapter.
- Unsupported exchanges fail with `HistoryUnavailable`.

**Crate:** `pmkit-data`.

### Partial

- Binance history is concrete; Binance live is an injection contract, not a shipped
  Binance WebSocket client.

### Deliberately absent

- CEX BBO/depth strategy inputs.
- CEX firehose persistence.
- Bybit/OKX/Coinbase/Kraken history until matched official archives exist.
- Live-only strategy inputs.

## 6. Deterministic feed merge

### Implemented

- Multiple named source tasks.
- Canonical ordering by source time, source rank, connection/frame identity.
- Warm-up and watermark handling.
- Late-record and stale-source rejection.
- EOF validation.
- Source error propagation and sibling cancellation.
- Fail-closed `ReplayGap` behavior before strategy evaluation.

**Crate:** `pmkit` (`feed.rs`).

### Gaps

- Better source-specific diagnostics.
- Feed health/lag metrics.
- Multi-asset coordination.

## 7. Strategy SDK

### Implemented

- `Strategy` and `StrategyFactory` traits.
- Immutable `StrategyContext` containing fact, market, book, positions, and time.
- Actions: place, cancel, replace quotes, cancel all.
- Strategy and factory error types.
- Executable example strategies: `threshold_taker`, `market_maker`,
  `momentum`, and `inventory_aware`, each self-checking against a deterministic
  scenario.

**Crates:** `pmkit-strategy`, `pmkit-strategy-testkit`.

### Deliberately absent

- No production strategy bundle.
- No convergence sniper or dual-side maker extraction.
- No dynamic plugins.
- No credential, socket, or mutable-wallet access from strategies.

### Candidate additions

- Strategy metrics helpers (deterministic book/fact/fill builders, a
  single-market harness, and action assertions now ship in
  `pmkit-strategy-testkit`).

## 8. Execution and simulation

### Implemented

- Unified `Executor` boundary.
- `PlaceOrder`, `OrderId`, execution snapshots, submit/cancel/cancel-all.
- Typed rejected/transport/not-found errors.
- Conservative simulation:
  - takers walk the book;
  - makers rest;
  - later crossing updates fill makers;
  - simulated IDs, cancels, and fills.
- Paper executor using the same executor boundary without venue calls.

**Crates:** `pmkit-exec`, `pmkit-sim`, `pmkit-paper`.

### Gaps

- Queue position, latency, slippage, market impact, and partial-fill models.
- Configurable simulation fee/latency models.
- Persistent paper-account state.

## 9. Polymarket adapter

### Implemented

- Neutral/venue side mapping.
- `MarketTokens`, outcome/token conversion, and venue order conversion.
- Authenticated CLOB execution and cancellation.
- Public market WebSocket source.
- Book/trade parsing.
- Raw market/account frame interception.
- Store-before-adapt behavior.
- Historical cursor source backed by stored PM envelopes.
- Gamma SDK client in `pmkit-api` for market metadata and token/outcome helpers.

**Crates:** `pmkit-polymarket`, `pmkit-api`.

### Partial

- Authenticated account subscription and typed lifecycle mapping now exist;
  complete venue reconciliation and settlement mapping remain.

### Gaps

- Runtime market discovery from Gamma.
- Full venue reconciliation workflow.

## 10. Run specifications and runtime safety

### Implemented

- `BacktestRun`, `PaperRun`, `LiveRun`, `ReplaySpec`, and `RunSpec`.
- Evidence requirements: corroborated-only or single-source.
- Retrieval policy: wait or return pending.
- Tape policy: required or best effort.
- Exact environment-based live consent.
- Explicit risk limits:
  - order notional;
  - position notional;
  - open orders;
  - max loss.
- Startup, reconciliation, and tape-flush timeouts.
- Shutdown policy: leave, cancel owned, cancel all explicitly.
- Strategy name/version/config revision metadata.

**Crates:** `pmkit-run`, `pmkit-runtime`, `pmkit-spec`.

### Gaps

- Runtime kill switch.
- Per-market/per-strategy limits.
- Daily loss limits.
- Rate limits.
- Persistent risk state.

## 11. Orchestration

### Implemented

- `Pmkit::builder`.
- Multiple run registration and duplicate-ID validation.
- Backtest, paper, and live drivers.
- Run reports and `AppHandle` wait/report access.
- Reproducibility manifests.
- Optional storage through `.storage(...)`.
- Causal decision recording in all three modes.
- Durable pending intent before live submission.
- Accepted/rejected/unknown transitions and restart reconciliation by correlation ID.
- Cooperative run cancellation via a shared `Cancellation` token and
  `RunId`-scoped `RunLifecycleEvent` subscriptions, exposed on the builder
  without leaking executor or storage internals.

**Crate:** `pmkit`.

### Gaps

- Configured backtest concurrency is currently sequential.
- Structured runtime metrics.
- CLI runner.

## 12. Tape and durable storage

### Implemented

- JSON-lines normalized event serialization.
- PM market/account/CEX envelope serialization.
- Raw UTF-8 frame recorder suitable for OSS collectors.
- `TapeStore` trait.
- Turso/libSQL local store.
- Versioned PM envelope persistence with SDK-derived normalized projections and
  optional raw-frame capture for injected sources.
- Owner-scoped cursor pagination.
- Causal decision storage.
- Durable pending/accepted/rejected/unknown intent storage.
- Corruption detection and typed replay gaps.
- Whole-database deletion.
- Canonical chain log/checkpoint storage.
- Reorg replacement segments.
- Storage opt-in; CEX events are not stored.
- Reliable raw-frame collector (`pmkit-collector`): transport-agnostic
  reconnect with fresh per-epoch connection identity, subscription sharding,
  bounded-channel backpressure, heartbeats, and graceful drain/flush over the
  v1 `RawTapeSink`, with a concrete `tokio-tungstenite` transport.

**Crates:** `pmkit-tape`, `pmkit-store`, `pmkit-collector`, `pmkit-archive`.

### Gaps

- Schema migrations.
- Remote Turso deployment guidance.
- Secondary-file derive/compact retention (deferred to the separate retention
  project; raw evidence is immutable and is never compacted or deleted in OSS).
- Encryption.
- Multi-process writer guarantees.
- Venue order-ID persistence tied to a real status-query consumer.

### Deliberately absent

- Concrete S3 client. The durable object-store *contract* (multipart segments,
  atomic manifest, checksums, process-loss recovery) is OSS in `pmkit-archive`
  with an `FsObjectStore` reference; the real S3 adapter that implements
  `ObjectStore` belongs to the separate product-infrastructure project.
- Parquet retention plane.
- Long-term cloud archive policy.
- WebSocket discovery daemon.

Those belong in a separate reliability-focused collector/storage project.

## 13. On-chain reconstruction and API

### Implemented

- Polygon chain identity and contract registry.
- Current pUSD, Conditional Tokens, CTF exchange, and negative-risk exchange
  contracts.
- Optional historical V1 exchange addresses.
- Canonical log identity with block/transaction/log ordering.
- Typed events for collateral transfers, ERC-1155 transfers, splits, merges,
  redemptions, fills, matches, and fees.
- Canonical checkpoints and reorg replacement segments.
- Wallet reconstruction of collateral, token positions, settlement, trades, and
  activity.
- Chain-truth API for positions, closed positions, trades, activity, balance, and
  pagination.
- Typed `NotReconstructibleFromChain` responses for off-chain-only order data.

**Crates:** `pmkit-store`, `pmkit-api`.

### Not implemented

- Real RPC ingestion.
- Etherscan ingestion/backfills.
- ABI decoding from live RPC logs.
- Finality tracking.
- Multi-provider failover.
- RPC consistency checks.

## 14. Reproducibility

### Implemented

- Redacted run manifests.
- Run/portfolio/strategy IDs.
- Runtime configuration.
- Replay window and evidence policy.
- Retrieval policy.
- No credentials in manifests.
- Replay bundle export (`pmkit-store::export_replay_bundle`): a versioned JSON
  artifact bundling the manifest, every PM envelope (raw frame plus normalized
  fact) in canonical order, every causal decision, and the verified CEX archive
  checksums, failing closed on any replay gap or corrupt frame.

**Crates:** `pmkit-manifest`, `pmkit-store`.

### Gaps

- Manifest schema versioning.
- Git commit and dependency-lock capture.
- Artifact hashes.

## 15. Explicit non-features

- No generic `pmkit run strategy-name` CLI.
- No YAML/TOML deployment language.
- No implicit wallet × strategy × market product model.
- No credentials in strategy context, storage, or manifests.
- No fabricated CLOB data for off-chain-only concepts.
- No CEX BBO/depth/firehose persistence.
- No unmatched exchange history.

## Review order

### Cross-cutting goal: one strategy, three reliable modes

The primary product goal is that a strategy is written once and can run under
backtest, paper, and live without changing its decision logic. The mode may
change execution and timing, but the strategy-facing facts, market identity,
source pairing, causal recording, and failure semantics must remain equivalent.

Required work:

- [ ] Define a golden strategy-input contract shared by all three drivers.
- [ ] Use the same normalized PM event parser in historical, paper, and live
  paths.
- [ ] Add live Binance `@aggTrade` source injection and pair it with the
  existing Vision historical source.
- [ ] Keep CEX source identity, exchange timestamp, aggregate ID, and ordering
  identical between live and replay.
- [ ] Add one fixture strategy run in all three modes and compare normalized
  facts, decision snapshots, and causal correlation IDs.
- [ ] Make source failure, premature EOF, stale data, and storage failure obey
  the same fail-closed contract in all modes.
- [ ] Add a mode-parity integration test before adding more exchanges.

The `pm-money` Binance/Bybit/Coinbase/Kraken/OKX feed code is a source of
adapter candidates, not an automatic extraction target. Each adapter must have
an official historical counterpart before it can reach a strategy.

### 1. Real on-chain ingestion and finality

**Current decision:** build a narrow provider-neutral ingestion boundary first,
not a wholesale copy of the private `pm-onchain` crate.

The first OSS slice should provide:

1. A finalized-block-aware source trait.
2. A raw RPC-log boundary that preserves block hash, transaction index, log index,
   and provider identity.
3. Separate ABI/event decoding from provider transport.
4. Canonical segment construction against the existing `CanonicalLogStore`.
5. Explicit common-ancestor discovery for reorg replacement.
6. Fixture tests for finality, provider disagreement, missing logs, and reorgs.

Do not initially import Etherscan backfills, identity resolution, analytics joins,
or the private operational database. Those can be adapters built on the OSS seam.

### 2. Venue reconciliation and order IDs

Only add persisted venue order IDs together with a status-query/recovery consumer.
The current correlation-based pending/unknown reconciliation remains the safe
contract until that consumer exists.

### 3. Authenticated account/order lifecycle

Complete raw authenticated subscriptions and normalize acknowledgement, rejection,
cancel, fill, and settlement events.

### 4. Reliable raw tape collector

Keep the raw recorder OSS. Design the collector and S3/Parquet retention plane
separately with explicit durability, reconnect, replay, and compaction guarantees.

### 5. Portfolio accounting and PnL

Add a chain/venue-independent accounting ledger only after event coverage is complete.

### 6. Simulation realism

Add latency, partial fills, queue position, slippage, and impact as explicit models.

### 7. Runtime and CLI ergonomics

Add cancellation, kill switches, metrics, and eventually a typed CLI.

### 8. Additional strategies and feeds

Add examples before production strategies. Add another exchange only after its
official history and live normalized schema are replay-equivalent.

## Fix/add backlog

Tasks are ordered by risk and dependency. Each task should ship with its
behavioral test and documentation update.

### P0: correctness and contract hardening

- [x] **P0-1: propagate tape serialization failures.** Replace
  `JsonLinesTape`'s silent `unwrap_or_default()` serialization fallback with a
  typed `io::Error`; prove malformed/unsupported serialization cannot become a
  successful empty tape line.
- [x] **P0-2: make ingest-sequence overflow explicit.** Remove
  `unwrap_or_default()` from `BinanceVisionHistory`; return a typed source error
  if the sequence cannot fit the envelope type.
- [x] **P0-3: test Binance date-window boundaries.** Lock behavior for same-day,
  midnight-exclusive, multi-day, empty, and reversed replay windows.
- [x] **P0-4: document storage compatibility.** Add an explicit schema-version
  and migration policy before changing durable envelope or intent shapes. See
  `docs/PMKIT_STORAGE_COMPATIBILITY.md`.
- [x] **P0-5: run full API documentation.** Make `cargo doc --workspace
  --no-deps` part of the normal acceptance gate and fix every broken public
  example/link.

### P1: real on-chain truth

- [x] **P1-1: define finalized raw-log provider contract.** Add block head,
  finalized height, raw log identity, provider identity, and typed provider
  errors. Keep it independent from Turso and Alloy. Implemented in
  `pmkit-store::FinalizedRawLogProvider`; no RPC client or decoder is included.
- [x] **P1-2: add raw-log-to-event decoder boundary.** Decode only registered
  Polygon contracts/events into existing `ChainEvent` values; reject unknown
  addresses/topics instead of guessing. The first verified slice covers the
  standard ERC-20 `Transfer`; ERC-1155, exchange, and conditional-token topics
  remain rejected until ABI signatures are verified.
- [x] **P1-3: build canonical segment ingestion.** Fetch only finalized ranges,
  construct `CanonicalLogSegment`, validate against the stored checkpoint, and
  call the existing transactional replacement operation via
  `pmkit_store::ingest_finalized_batch`.
- [x] **P1-4: add finality/reorg fixtures.** Cover provider disagreement, missing
  blocks, duplicate logs, reorg replacement, stale ancestor, and restart. The
  provider disagreement and block-coverage contracts are validated before raw
  decoding; canonical reorg/restart cases remain covered by `chain_tests`.
- [x] **P1-5: add one provider adapter.** Prefer Alloy JSON-RPC first; keep
  Etherscan/backfill as a separate adapter, not a core dependency. The first
  adapter is the narrow `pmkit-store::JsonRpcFinalizedProvider` over existing
  `reqwest`; it has no Etherscan/backfill path.
- [x] **P1-6: add operational sync controls.** `JsonRpcFinalizedProvider` now
  enforces bounded inclusive ranges, retry limits, and a per-provider request
  semaphore. `ingest_finalized_batch` commits each decoded canonical segment as
  one transaction, so the durable checkpoint advances only with the segment;
  requests do not spawn detached tasks and remain cancellation-safe. Focused
  tests cover oversized ranges and transient HTTP recovery.
- [x] **P1-6a: evaluate matched CEX BBO as a future extension.** No event type
  is added: the repository has no matched official historical BBO source or
  parity fixture, and the explicit non-feature boundary remains in force.
- [x] **P1-6b: add the live Binance trade source.** `BinanceAggTradeLive`
  subscribes to official `@aggTrade` and uses the same parser, source ID, and
  aggregate-trade sequence identity as `BinanceVisionHistory`. The live source
  is accepted by the paper/live drivers, while the Vision source feeds
  backtests; parser parity tests prove live/archive facts match, and feed
  fixtures prove the normalized reference contract is stable across all three
  modes.

### P1: venue lifecycle and recovery

- [x] **P1-7: complete authenticated PM account source.** `PolymarketUserData`
  consumes the SDK's typed user stream, maps order placement/update,
  cancellation, rejection, fill, failed, retrying, and unknown statuses into
  PMKit account events, and is wired into Paper/Live feed construction. Tapes
  persist the versioned PMKit-owned typed envelope; injected raw sources can
  still preserve original frames.
- [x] **P1-8: persist venue order IDs with accepted intents.** Accepted IDs are
  stored in the versioned durable intent payload, avoiding a write-only schema
  column while the status-query consumer is introduced.
- [x] **P1-9: implement venue status recovery.** Live startup reads pending and
  unknown intents, queries each persisted venue ID through the typed executor
  status seam, and transitions known outcomes idempotently.
- [x] **P1-10: map terminal lifecycle events.** Typed PM account events and
  causal intent outcomes cover accepted, rejected, cancelled, filled, retrying,
  unknown, and status-query recovery paths.

### P1: accounting and risk

- [x] **P1-11: add a portfolio accounting ledger.** `pmkit-accounting` tracks
  cash, token balances, fees, realized PnL, marked unrealized PnL, equity, and
  settlement from normalized fill/settlement inputs without venue or chain
  dependencies.
- [x] **P1-12: add portfolio risk aggregation.** Live pre-submission risk now
  aggregates portfolio, market, strategy, open-order, and daily-loss exposure,
  including reserved accepted orders, before any venue call.
- [x] **P1-13: add runtime kill switch.** Owner-scoped kill state is durable in
  the tape store, checked fail-closed at live startup and before submission, and
  latched when the configured loss limit is breached.

### P1: simulation parity

- [x] **P1-14: add configurable latency model.** Conservative simulation delays
  order activation by an explicit duration; causal decisions record observation
  and decision times, durable intents record submission time, and fill events
  retain the actual fill time.
- [x] **P1-15: add partial fills and queue position.** The zero-cost default
  preserves the original conservative behavior; explicit queue-ahead basis
  points reduce available crossed liquidity and leave residual maker quantity
  resting.
- [x] **P1-16: add slippage and impact models.** Explicit adverse slippage and
  impact basis points adjust taker fills without violating limit prices, and all
  simulation inputs are recorded in manifests and causal decision snapshots.

### P2: reliable OSS tape and cloud storage

- [x] **P2-1: define raw tape record format.** Version 1 is newline-delimited
  UTF-8 JSON containing schema version, connection identity, local receipt time,
  and the exact raw text frame. Graceful flush and fail-closed crash-tail
  recovery are specified in `docs/PMKIT_RAW_TAPE_FORMAT.md` and locked by the
  raw-tape contract tests.
- [x] **P2-2: build the OSS collector.** `pmkit-collector` drives an injected
  transport, preserving every text frame into the v1 `RawTapeSink` without
  dropping evidence. It reconnects with a fresh `connection_id` per epoch under
  a bounded reconnect budget, shards subscriptions one connection each, applies
  backpressure through one bounded channel, heartbeats each connection, and on
  shutdown drains buffered frames and flushes. `WebSocketTransport` is the
  concrete `tokio-tungstenite` implementation; object-store durability stays in
  P2-3.
- [x] **P2-3: define durable object-store sink.** `pmkit-archive` defines the
  durability contract: an S3-shaped multipart `ObjectStore`, an atomic
  schema-versioned `Manifest` that is the sole source of durability truth,
  SHA-256 checksums per part and segment, bounded retry on transient failures,
  and process-loss recovery that verifies committed segments, aborts dangling
  uploads, and fails closed on corruption or version mismatch. `FsObjectStore`
  is the filesystem reference; a concrete S3 client stays in the separate
  product-infrastructure project.
- [x] **P2-4: evaluate Parquet separately.** Decision: defer columnar/Parquet
  derivation to the separate retention project; add no `arrow`/`parquet`
  dependency to OSS now. Rationale: the raw newline-delimited v1 segments in
  `pmkit-archive` are the immutable evidence and their durability + recovery are
  now proven, but a columnar plane is an analytics/retention concern that §12
  places outside OSS. Preconditions for ever adding it: it must be a pure
  secondary derived from committed segments (never a replacement), fully
  re-derivable, and verifiable against the manifest's segment checksums so raw
  evidence stays authoritative. No event or storage shape changes; the
  `Parquet retention plane` boundary remains in force. See the columnar note in
  `docs/PMKIT_RAW_TAPE_FORMAT.md`.
- [x] **P2-5: add retention/compaction.** In-scope half implemented: raw
  evidence is immutable by construction in `pmkit-archive` (each segment is
  written once to a monotonically indexed key via multipart + atomic manifest
  and never rewritten in place), locked by a reopen-and-append test proving
  previously committed segments stay byte- and checksum-identical while new
  records land in new segments. Out-of-scope half deferred: deriving and
  compacting secondary analytical files is a retention-project concern (those
  secondary files were deferred in P2-4), and OSS never mutates, compacts, or
  deletes raw evidence.

### P2: SDK ergonomics

- [x] **P2-6: add standard strategy examples.** `market_maker` (two-sided
  requote around mid), `momentum` (CEX-reference-driven taker), and
  `inventory_aware` (position-skewed buy/reduce) live in
  `crates/pmkit-strategy/examples/`, each a self-checking executable that
  asserts its produced `Actions` against a deterministic scenario.
- [x] **P2-7: add strategy test utilities.** `pmkit-strategy-testkit` ships
  deterministic builders (`book`, `tick`, `last_trade`, `reference_trade`,
  `account_fill`, `position`), a single-market `Harness` that drives a strategy
  and returns its `Actions`, and action assertions (`assert_no_actions`,
  `assert_placed`, `assert_cancels_all`, `placed_orders`).
- [x] **P2-8: add run cancellation and event subscriptions.** A shared
  `Cancellation` token stops each run at its next event boundary (live runs
  still route through their shutdown policy so owned orders are handled), and
  builder `.subscribe(...)` publishes `RunId`-scoped `RunLifecycleEvent`s
  (`Started`/`Completed`/`Cancelled`) with no executor or storage internals.
- [x] **P2-9: add typed CLI only after API stabilizes.** Decision: defer. The
  task's precondition is unmet — the public API is pre-`0.1.0` and still
  changing (see README status), so no CLI is added now. When the API
  stabilizes the CLI must be typed-only: no generic `pmkit run strategy-name`
  entry point, no dynamic plugin protocol, and no untyped YAML/TOML config, per
  the §15 non-features. No code or dependency added.
- [x] **P2-10: add replay bundle export.** `pmkit-store::export_replay_bundle`
  assembles a versioned JSON bundle from the run manifest, every durable PM
  envelope (raw frame plus normalized fact) in canonical order, every causal
  decision, and the caller's verified CEX archive checksums. It paginates the
  store and fails closed on any replay gap or non-UTF-8 raw frame.

### Review gate for every task

- [ ] Does this belong in OSS, or is it cloud/product infrastructure?
- [ ] Does it preserve PM-owned evidence versus public CEX context?
- [ ] Does it add a real consumer, or only a write-only field/abstraction?
- [ ] Is the behavior replayable and deterministic?
- [ ] Is failure fail-closed where money or truth is involved?
- [ ] Is there a focused test and an executable surface check?

## Review evidence

- `cargo test --workspace`: 174 tests passed.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `cargo doc --workspace --no-deps`: passed.
- `pmkit-store` P1-1 focused tests: 13 passed.
- `pmkit-polymarket` P1-7 typed account-source tests: 15 passed.
- `pmkit-accounting` P1-11 ledger tests: 3 passed.
- P1-12/P1-13 risk and persistent kill-state tests: 49 focused tests passed.
- P1-14 through P1-16 simulation tests cover delayed activation timestamps,
  queue-adjusted partial fills, slippage/impact, and durable model inputs.
- P2-1 raw-tape tests cover schema version, connection identity, receipt time,
  exact raw text, newline framing, legacy-version rejection, crash-tail
  recovery, malformed complete records, and flush forwarding.
- P2-2 collector tests cover subscription sharding, reconnect with a fresh
  connection identity after clean close and error, bounded-channel backpressure
  preserving every frame in order, periodic heartbeats, graceful drain/flush on
  shutdown, fail-closed reconnect-budget exhaustion, and a real
  `tokio-tungstenite` transport collecting a frame from a local server. The
  `cargo run -p pmkit-collector --example collect` surface prints a reconnecting
  run's tape.
- P2-3 archive tests cover multipart segment round-trip and recovery, checksum
  tamper fail-closed, process-loss abort of dangling uploads, manifest-is-sole-
  truth for an orphan completed segment, unsupported-manifest-version rejection,
  and transient-put retry. The `cargo run -p pmkit-archive --example durable`
  surface writes, closes, reopens, and recovers durable evidence.
- P2-4 is a documented evaluation only: no code or dependency; columnar
  derivation is deferred to the separate retention project as a checksum-
  verifiable secondary of the raw segments.
- P2-5 locks raw-evidence immutability with a reopen-and-append archive test
  (7 archive tests) proving committed segments stay byte- and checksum-identical
  while new records append; secondary-file compaction is deferred.
- P2-6 adds three self-checking example strategies (`market_maker`, `momentum`,
  `inventory_aware`), each run via `cargo run -p pmkit-strategy --example <name>`
  and asserting its `Actions` against a deterministic scenario.
- P2-7 adds `pmkit-strategy-testkit` (6 tests) driving a sample strategy through
  the harness with the builders and asserting via the action helpers.
- P2-8 adds run cancellation and lifecycle subscriptions (pmkit tests cover a
  pre-cancelled run stopping with zero events and a normal run publishing
  `Started` then `Completed`).
- P2-9 is a documented deferral: no code or dependency; the typed CLI is gated
  on API stabilization and must stay typed-only when added.
- P2-10 adds `export_replay_bundle` (pmkit-store tests cover a bundle carrying
  manifest, PM evidence, decisions, and cache checksums, plus a fail-closed
  corrupt-evidence case; `cargo run -p pmkit-store --example replay_bundle`
  prints a real bundle).
- P0-1 through P0-5, P1-1 through P1-16, and P2-1 through P2-10 were resolved
  after review; the entire P0/P1/P2 backlog is complete.
