# PMKit

PMKit is an open-source **Rust SDK first** engine for building, backtesting, and
running trading strategies on prediction markets (Polymarket in v1).

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
| [`pmkit-core`](crates/pmkit-core) | Pure ownership value types: `PortfolioId`, `MarketId`, `StrategyId`, `Mode`, `PortfolioKey`, `StrategyKey`. |
| [`pmkit-market`](crates/pmkit-market) | Market-data domain primitives for crypto up/down prediction markets: `Asset`, `Outcome`, `MarketDuration`, `Exchange`. |

More crates are migrated from the private engine as they are decoupled from
venue-specific and credential-bearing code. The public SDK stays
exchange-neutral; venue specifics (Polymarket signing, slugs, condition IDs)
live behind adapters, never in the core.

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

## Not in v1

- No generic `pmkit run strategy-name` CLI or dynamic plugin protocol.
- No YAML/TOML deployment language — Rust is the typed source of truth.
- No implicit wallet × strategy × market products.
- No strategy access to credentials, sockets, or a mutable wallet.

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
