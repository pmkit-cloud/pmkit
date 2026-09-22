# Evidence source candidates

Notes on third-party APIs evaluated as **independent corroboration sources**,
not as replay/primary data sources. PMKit's fail-closed rule stands: no
external API is ever trusted to fill a gap, decode a payload we didn't
capture ourselves, or substitute for the primary exchange/chain source.
A corroboration source may only be used to check agreement with our own
independently-derived result, after the fact.

## Orbscan (https://docs.orbscan.com/)

REST API, cursor-paginated, `Authorization: Bearer <key>`. An API key is
available (see task/owner note — not committed anywhere).

Relevant endpoints for PMKit, under `market/`:

- `GET /v1/orderbook/coverage` — `{ availableFrom, availableTo }` timestamps
  for the whole stored order-book table. Same shape as PMKit Cloud's
  `coverage`/`KnownGap` contract (`crates/pmkit-data/src/cloud_http.rs`).
- `GET /v1/orderbook/events` — cursor-paginated `book` snapshots (bids/asks)
  and `price_change` deltas per `tokenId`, each with `timestamp`,
  `indexedTimestamp`, and a `sourceHash`.
- `GET /v1/orderbook/markets` — token/market catalog (`conditionId`,
  `tokenId`, `slug`, outcomes) for identity resolution.

Also present but not evaluated for PMKit use (wallet/trader endpoints:
`/v1/trader/{address}/activity`, `/positions/*`, `/deposits-withdrawals`,
`/v1/tx/{txHash}` decode) — these are candidates for `pm-money`'s
`pm-onchain` reconciliation tooling instead, see below.

### Where it could plug in

1. **PR #19 `EvidenceRequirement::CorroboratedOnly`**
   (`crates/pmkit-data/src/cloud_http.rs`, `cloud_types.rs`). Currently
   permanently rejected as `EvidenceUnsupported` because there is no second
   independent source to corroborate against. Orbscan's order-book events
   are a genuine second, independently-sourced feed and would make
   `CorroboratedOnly` implementable: compare our replayed book state against
   Orbscan's for the same window; disagreement fails closed, agreement is
   the corroboration proof. This does not change single-source
   `AllowSingleSource` replay semantics.
2. **Post-capture verification** (`src/final_verifier.rs` in
   `btc-lockin-live`). After a capture window is sealed, independently
   cross-check the reconstructed Polymarket book state against Orbscan's
   `orderbook/events` for the same window as an out-of-band sanity check.
   This never touches or trusts the raw capture payload itself — it is a
   check performed on the already-derived, already-sealed result.
3. **`pm-money`'s `pm-onchain`** (task #52 scope) — the trader/activity
   endpoints (`/v1/trader/{address}/activity`, `/deposits-withdrawals`,
   `/v1/tx/{txHash}`) return decoded wallet trading activity directly,
   which could simplify or corroborate the hand-rolled Etherscan log
   fetch + ABI decode in `crates/pm-onchain/src/etherscan.rs`,
   `decode.rs`, `ctf.rs`. Same corroboration-only framing applies: it is
   a candidate *authoritative source* input to task #52's blocked owner
   decisions, not something to wire in unilaterally.

### What remains blocked

Using Orbscan for any of the above still requires the same trust-boundary
decision flagged in task #50 (stage 2, signed-manifest/trust boundary):
who is trusted, what counts as disagreement, what the fail-closed response
to a mismatch or an unavailable/unreachable corroboration source is, and
whether corroboration failure blocks a run or only downgrades a warning.
No implementation should proceed until that decision is made explicitly by
the owner. This document records the finding only; it does not authorize
integration.
