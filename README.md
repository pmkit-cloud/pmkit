# PMKit

PMKit is an open-source **Rust SDK first** engine for building, backtesting, and
running trading strategies on Polymarket prediction markets.

You write a small Rust binary that links your strategies and configures one or
more backtest, paper, and live runs. PMKit owns orchestration, shared market
feeds, deterministic ordering, risk, order lifecycle, reconciliation, tape
writing, and shutdown.

> **Not affiliated with Polymarket.** PMKit is an independent, community project.
> Nothing here is financial advice. Live trading places real orders with real
> money — you are solely responsible for your use of this software.

## Status

Early. The public API is being shaped from a private engine. Expect breaking
changes until `0.1.0` stabilizes.

## Workspace

| Crate | Purpose |
|---|---|
| [`pmkit-core`](crates/pmkit-core) | Pure ownership value types: `PortfolioId`, `MarketId`, `StrategyId`, `RunId`, `Mode`, `PortfolioKey`, `StrategyKey`. |
| [`pmkit-money`](crates/pmkit-money) | `Money` — a USDC monetary amount. |
| [`pmkit-market`](crates/pmkit-market) | Market-data domain primitives: `Asset`, `Outcome`, `MarketDuration`, `Exchange`. |
| [`pmkit-book`](crates/pmkit-book) | `Side`, `OrderBookL2`, `Position`, plus book and sizing math. |
| [`pmkit-math`](crates/pmkit-math) | Pure pricing and fee math: `fees`, `signals`, `fill`, `fair_value`. |
| [`pmkit-event`](crates/pmkit-event) | Neutral `MarketEvent` model, typed stream envelopes (`PmMarketEnvelope`, `PmAccountEnvelope`, `CexReferenceEnvelope`), and `StrategyFact`. |
| [`pmkit-exec`](crates/pmkit-exec) | The `Executor` trait (live/paper/backtest boundary) and `PlaceOrder`/`OrderId`. |
| [`pmkit-strategy`](crates/pmkit-strategy) | The `Strategy`/`StrategyFactory` traits, `StrategyContext`, and `Actions`. |
| [`pmkit-runtime`](crates/pmkit-runtime) | `RiskLimits`, `RuntimeConfig`, `LiveOrderPolicy`, `StrategyRegistration`. |
| [`pmkit-run`](crates/pmkit-run) | Run-spec primitives: `EvidenceRequirement`, `RetrievalWait`, `TapePolicy`, `LiveConsent`. |
| [`pmkit-data`](crates/pmkit-data) | `HistoricalDataSource`/`LiveDataSource` traits, raw PM frame sources, Binance `@aggTrade`/Vision `aggTrades` parity, and verified bounded archive cache. |
| [`pmkit-sim`](crates/pmkit-sim) | Conservative fill-simulation engine. |
| [`pmkit-paper`](crates/pmkit-paper) | `PaperExecutor` implementing the executor trait over the sim engine. |
| [`pmkit-spec`](crates/pmkit-spec) | Run specs: `BacktestRun`/`PaperRun`/`LiveRun`/`RunSpec`/`ReplaySpec`. |
| [`pmkit-tape`](crates/pmkit-tape) | Local JSON-lines user-tape sink for market events and envelope serialization. |
| [`pmkit-store`](crates/pmkit-store) | Durable Turso-backed `TapeStore` for lossless PM envelopes, causal decisions, and idempotent order intents. |
| [`pmkit`](crates/pmkit) | Orchestration engine: `Pmkit` builder, `AppHandle`, deterministic feed merge, causal recording, and the backtest/paper/live drivers. |
| [`pmkit-polymarket`](crates/pmkit-polymarket) | Polymarket venue adapter: live WebSocket data, authenticated CLOB execution, raw frame interception, and neutral/venue mapping. |
| [`pmkit-collector`](crates/pmkit-collector) | Reliable OSS raw-frame collector: reconnect, subscription sharding, bounded-channel backpressure, heartbeat, and graceful shutdown over a `RawTapeSink`, with a `tokio-tungstenite` transport. |

The public SDK stays exchange-neutral; venue specifics (Polymarket signing,
slugs, condition IDs) live behind the adapter, never in the core.

### Roadmap

The component library and the orchestration engine are in place. All three run
modes drive end to end: `Pmkit::builder(config).run(spec).start()` runs backtests
(replay), paper (live data + simulated fills), and live (consent-gated, risk-gated
Polymarket order routing), each returning a report. Remaining: on-chain
replay/decode; live reconciliation is bounded by
`RuntimeConfig.shutdown.reconciliation_timeout`, and shutdown applies the
configured `LiveOrderPolicy`.

## Mental model

You declare **runs**. A run is one portfolio in one mode with one or more
strategy registrations. PMKit derives two ownership keys:

```rust
use pmkit_core::{Mode, PortfolioKey, StrategyKey};

// One PortfolioKey owns balances, positions, orders, risk, and kill state.
let research = PortfolioKey::backtest("research")?;

// Mode is part of the key: the same portfolio id in a different mode is a
// different owner.
assert_ne!(
    PortfolioKey::paper("alice")?,
    PortfolioKey::live("alice")?,
);
# Ok::<(), pmkit_core::EmptyIdError>(())
```

Rules:

1. One `PortfolioKey` owns balances, positions, orders, risk, kill state,
   reconciliation, and executor state.
2. One `StrategyKey` owns one mutable strategy instance.
3. Reusing the same factory across wallets, markets, or modes creates separate
   strategy instances.
4. A strategy returns intents. It never receives credentials or calls an
   exchange directly.

## Data truth and ownership

PMKit enforces a strict boundary between **owned Polymarket evidence** and
**public CEX reference context**.

### Typed lossless envelopes

Every PM market and authenticated-account frame is captured as a versioned
envelope that preserves the byte-identical raw transport frame alongside a
normalized projection. Each envelope carries:

- `schema_version` — envelope format version.
- `source_id` / `connection_id` — upstream source and delivery connection.
- `source_timestamp_ms` / `receipt_timestamp_ms` — exchange time and local receipt.
- `canonical_source_rank` — deterministic rank for replay ordering.
- `connection_epoch` / `frame_sequence` — monotonic ordering within a connection.
- `ingest_sequence` — monotonic sequence assigned at ingest.
- `raw_frame` — the unmodified provider text.
- `normalized` — the derived strategy-visible fact.

Strategies receive only `StrategyFact` (normalized `MarketEvent`,
`PmAccountEvent`, or `CexReferenceEvent`). They cannot access raw frames,
receipt times, connection identity, or ingest sequence.

### Verified bounded Binance replay cache

CEX reference data uses a matched live/history source pair: Binance `@aggTrade`
for live and Binance Vision `aggTrades` for history. The cache:

- Downloads official archives to a per-key locked temporary path.
- Enforces transfer, ZIP, and CSV byte limits before eviction.
- SHA-256 verifies the archive before use.
- Atomically renames into the cache directory.
- Serializes LRU/quota state under `CachePolicy::Bounded { max_bytes }`.
- Returns a typed `ReplayGap` when an archive is missing, corrupt, or exceeds bounds.

Bybit and other exchanges return `HistoryUnavailable` until a matched official
archive exists. PMKit does not use BBO, depth, or `@trade` as strategy input.

### Whole-database deletion

`TursoTapeStore::delete_database()` removes the local SQLite database and all
sidecar files. Storage is opt-in via `PmkitBuilder::storage()`; when omitted,
no durable records are written and the default JSONL tape path is unchanged.

### Source eligibility and replay gaps

A source used by a live strategy must have an equivalent historical adapter
producing the same normalized record. Missing or corrupt history fails closed
with a typed `ReplayGap`. The deterministic feed merge aborts before strategy
evaluation on source error, premature EOF, stale reference, or late record.

### Opt-in storage

```rust
use pmkit_store::TursoTapeStore;
use pmkit::Pmkit;

let store = TursoTapeStore::open_local("./pmkit.db").await?;
let handle = Pmkit::builder(config)
    .run(backtest_spec)
    .storage(Arc::new(store))
    .start()
    .await?;
```

Omitting `.storage()` leaves every run in the no-storage path. CEX reference
events are never persisted to the tape store.

## Out of scope

- No generic `pmkit run strategy-name` CLI or dynamic plugin protocol.
- No YAML/TOML deployment language — Rust is the typed source of truth.
- No implicit wallet × strategy × market products.
- No strategy access to credentials, sockets, or a mutable wallet.
- No CEX BBO/depth/firehose persistence, live-only strategy inputs, or
  credentials in storage.
- No Bybit or other CEX parity until a matched official archive exists.

### Onchain wallet reconstruction

`pmkit-store` ingests parsed canonical Polygon logs through a trait-first
source boundary. A replacement segment names its common-ancestor checkpoint;
storage transactionally deletes later logs before persisting replacement logs
and rebuilding wallet balances, outcome-token positions, settlement, fills,
and activity. The registry accepts only Polygon (137), the current pUSD, CTF,
and V2 exchange contracts shown in its explicit configuration; historical V1
exchange addresses require opt-in configuration for a backfill.

`pmkit-api` exposes versioned, chain-truth positions, closed positions, trades,
and activity with their documented offset limits. It does not expose live
valuation, display metadata, user profiles, or signed-order lifecycle. CLOB
`/data/orders` and `/data/order/{id}` return typed
`NotReconstructibleFromChain` results rather than fabricated data.

## Building

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you explicitly state
otherwise, any contribution intentionally submitted for inclusion in this
project by you, as defined in the Apache-2.0 license, shall be dual licensed as
above, without any additional terms or conditions.
