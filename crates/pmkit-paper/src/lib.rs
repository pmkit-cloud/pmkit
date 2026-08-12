//! Paper executor for `PMKit`.
//!
//! [`PaperExecutor`] implements the [`Executor`] trait against an in-memory
//! [`SimEngine`]. Orders are simulated, never sent to a venue; resulting fills
//! are delivered as [`MarketEvent`]s on a channel the runtime owns. Feed book
//! updates through [`PaperExecutor::update_book`] so resting makers can fill.

use std::sync::{Mutex, PoisonError};

use async_trait::async_trait;
use pmkit_book::{OrderBookL2, Position, Side};
use pmkit_core::{MarketId, StrategyId};
use pmkit_event::MarketEvent;
use pmkit_exec::{ExecError, ExecutionSnapshot, Executor, OrderId, PlaceOrder};
use pmkit_market::Outcome;
use pmkit_money::Money;
use pmkit_sim::{FeeModel, MarketCategory, SimEngine, SimulationConfig};
use rust_decimal::Decimal;
use tokio::sync::mpsc::Sender;

mod ledger;
mod wire;

pub use ledger::{PaperAccountState, PaperLedgerEntry, PaperLedgerError, PaperOpenOrder};

use ledger::{OrderState, PaperLedger};

// allow: SIZE_OK — executor mutations and their ledger writes share one lock boundary.

/// An executor that simulates fills instead of routing to a venue.
#[derive(Debug)]
pub struct PaperExecutor {
    state: Mutex<ExecutorState>,
    fills: Sender<MarketEvent>,
}

#[derive(Debug)]
struct ExecutorState {
    engine: SimEngine,
    ledger: PaperLedger,
    config: SimulationConfig,
    last_timestamp_ms: i64,
}

impl PaperExecutor {
    /// Creates a paper executor that delivers fills to `fills`, minting order
    /// ids as `"{id_prefix}-{n}"` and charging taker fees for `category`.
    #[must_use]
    pub fn new(
        fills: Sender<MarketEvent>,
        id_prefix: impl Into<String>,
        category: MarketCategory,
    ) -> Self {
        Self::with_account_config(
            fills,
            id_prefix,
            category,
            SimulationConfig::default(),
            Money::ZERO,
        )
    }

    /// Creates a paper executor with explicit simulation inputs.
    #[must_use]
    pub fn with_config(
        fills: Sender<MarketEvent>,
        id_prefix: impl Into<String>,
        category: MarketCategory,
        config: SimulationConfig,
    ) -> Self {
        Self::with_account_config(fills, id_prefix, category, config, Money::ZERO)
    }

    /// Creates a paper executor whose account starts from one durable cash movement.
    #[must_use]
    pub fn with_account_config(
        fills: Sender<MarketEvent>,
        id_prefix: impl Into<String>,
        category: MarketCategory,
        config: SimulationConfig,
        initial_cash: Money,
    ) -> Self {
        let fee_model = config
            .fee_model
            .unwrap_or_else(|| FeeModel::for_category(category));
        Self::with_account_fee_config(
            fills,
            id_prefix,
            SimulationConfig {
                fee_model: Some(fee_model),
                ..config
            },
            initial_cash,
        )
    }

    /// Creates a paper executor from a fee-resolved simulation configuration.
    #[must_use]
    pub fn with_account_fee_config(
        fills: Sender<MarketEvent>,
        id_prefix: impl Into<String>,
        config: SimulationConfig,
        initial_cash: Money,
    ) -> Self {
        let id_prefix = id_prefix.into();
        Self {
            state: Mutex::new(ExecutorState {
                engine: SimEngine::with_fee_config(id_prefix.clone(), 0, config),
                ledger: PaperLedger::new(initial_cash, id_prefix),
                config,
                last_timestamp_ms: 0,
            }),
            fills,
        }
    }

    /// Reconstructs a paper executor by replaying durable ledger entries.
    ///
    /// # Errors
    ///
    /// Returns [`PaperLedgerError`] when records are corrupt or inconsistent.
    pub fn reconstruct(
        fills: Sender<MarketEvent>,
        id_prefix: impl Into<String>,
        category: MarketCategory,
        config: SimulationConfig,
        entries: &[PaperLedgerEntry],
    ) -> Result<Self, PaperLedgerError> {
        let fee_model = config
            .fee_model
            .unwrap_or_else(|| FeeModel::for_category(category));
        Self::reconstruct_with_fee_config(
            fills,
            id_prefix,
            SimulationConfig {
                fee_model: Some(fee_model),
                ..config
            },
            entries,
        )
    }

    /// Reconstructs a paper executor from records and a fee-resolved configuration.
    ///
    /// # Errors
    ///
    /// Returns [`PaperLedgerError`] when records are corrupt or inconsistent.
    pub fn reconstruct_with_fee_config(
        fills: Sender<MarketEvent>,
        id_prefix: impl Into<String>,
        config: SimulationConfig,
        entries: &[PaperLedgerEntry],
    ) -> Result<Self, PaperLedgerError> {
        let id_prefix = id_prefix.into();
        let ledger = PaperLedger::reconstruct(entries, id_prefix)?;
        let engine = ledger.rebuild_engine(config)?;
        let last_timestamp_ms = ledger.last_timestamp_ms();
        Ok(Self {
            state: Mutex::new(ExecutorState {
                engine,
                ledger,
                config,
                last_timestamp_ms,
            }),
            fills,
        })
    }

    /// Applies a book update, delivering any fills it triggers on resting
    /// maker orders.
    ///
    /// # Errors
    ///
    /// Returns [`ExecError::Transport`] if the fill receiver has closed.
    pub async fn update_book(
        &self,
        market: &MarketId,
        outcome: Outcome,
        book: OrderBookL2,
    ) -> Result<(), ExecError> {
        let timestamp_ms = book.timestamp_ms;
        let drained = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let ExecutorState {
                engine,
                ledger,
                last_timestamp_ms,
                ..
            } = &mut *state;
            let maturing = ledger.maturing_delayed(market, outcome, timestamp_ms);
            engine.update_book(market, outcome, book);
            let drained = engine.drain_fills();
            for fill in &drained {
                ledger
                    .record_fill(fill)
                    .map_err(|error| execution_error(&error))?;
            }
            for order_id in maturing {
                if ledger.contains_order(&order_id) {
                    ledger
                        .cancel(&order_id, timestamp_ms)
                        .map_err(|error| execution_error(&error))?;
                }
            }
            *last_timestamp_ms = timestamp_ms;
            drop(state);
            drained
        };
        self.deliver(drained).await
    }

    /// Applies one owner-scoped paper settlement.
    ///
    /// # Errors
    ///
    /// Returns [`PaperLedgerError`] if it does not exactly consume the held position.
    pub fn settle(
        &self,
        market: MarketId,
        outcome: Outcome,
        settled_size: Decimal,
        proceeds: Decimal,
        timestamp_ms: i64,
    ) -> Result<(), PaperLedgerError> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state
            .ledger
            .settle(market, outcome, settled_size, proceeds, timestamp_ms)?;
        state.last_timestamp_ms = timestamp_ms;
        drop(state);
        Ok(())
    }

    /// Returns the account projection derived from the ledger.
    #[must_use]
    pub fn account_state(&self) -> PaperAccountState {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .ledger
            .state()
    }

    /// Returns positions isolated to one exact market.
    #[must_use]
    pub fn positions_for_market(&self, market: &MarketId) -> Vec<Position> {
        self.account_state()
            .positions
            .into_iter()
            .filter(|position| position.market == *market)
            .map(|position| Position {
                outcome: position.outcome,
                qty: position.quantity,
                avg_entry: position.average_entry,
                unrealized_pnl: Decimal::ZERO,
            })
            .collect()
    }

    /// Returns the number of unique fills in the reconstructed ledger.
    #[must_use]
    pub fn fill_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .ledger
            .fill_count()
    }

    /// Drains ledger entries not yet handed to durable storage.
    pub fn drain_ledger(&self) -> Vec<PaperLedgerEntry> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .ledger
            .drain_pending()
    }

    /// Returns the oldest ledger entry that durable storage has not acknowledged.
    #[must_use]
    pub fn pending_ledger_entry(&self) -> Option<PaperLedgerEntry> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .ledger
            .pending_entry()
    }

    /// Acknowledges the current oldest pending ledger entry after its durable commit.
    pub fn acknowledge_ledger_entry(&self, event_id: &str) -> bool {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .ledger
            .acknowledge_pending(event_id)
    }

    /// Submits an order while retaining its strategy ownership in the ledger.
    ///
    /// # Errors
    ///
    /// Returns [`ExecError`] when the simulated order or ledger transition fails.
    pub async fn submit_for_strategy(
        &self,
        order: &PlaceOrder,
        strategy: StrategyId,
        now_ms: i64,
    ) -> Result<OrderId, ExecError> {
        let (id, drained) = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let ExecutorState {
                engine,
                ledger,
                config,
                last_timestamp_ms,
            } = &mut *state;
            ensure_market_limits(order, config)?;
            ensure_sufficient_cash(order, ledger)?;
            let (placement_id, expected_order_id) = ledger
                .begin_order(order, Some(strategy.clone()), now_ms)
                .map_err(|error| execution_error(&error))?;
            let id = engine.submit_for_strategy(order, strategy, now_ms);
            if let Some(actual) = &id {
                let order_state = if order.post_only {
                    OrderState::Resting
                } else if config.activation_latency_ms > 0 {
                    OrderState::Delayed
                } else {
                    OrderState::Immediate
                };
                ledger
                    .acknowledge(
                        placement_id,
                        actual.0.clone(),
                        order_state,
                        now_ms.saturating_add(config.activation_latency_ms),
                        now_ms,
                    )
                    .map_err(|error| execution_error(&error))?;
            } else {
                ledger
                    .reject(placement_id, expected_order_id, now_ms)
                    .map_err(|error| execution_error(&error))?;
            }
            let drained = engine.drain_fills();
            for fill in &drained {
                ledger
                    .record_fill(fill)
                    .map_err(|error| execution_error(&error))?;
            }
            *last_timestamp_ms = now_ms;
            drop(state);
            (id, drained)
        };
        self.deliver(drained).await?;
        id.ok_or_else(|| ExecError::Rejected {
            reason: "no fill available".to_owned(),
        })
    }

    async fn deliver(&self, fills: Vec<MarketEvent>) -> Result<(), ExecError> {
        for fill in fills {
            self.fills
                .send(fill)
                .await
                .map_err(|_| ExecError::Transport {
                    message: "paper fill receiver closed".to_owned(),
                })?;
        }
        Ok(())
    }
}

/// Rejects an order that violates the venue market limits before it reaches
/// the ledger or the book, mirroring the venue rejecting it server-side. A
/// paper run that fills such orders reports an edge the live venue would
/// refuse; the rule itself lives in [`pmkit_exec::MarketLimits::check`], so
/// paper cannot drift from what live enforces.
fn ensure_market_limits(order: &PlaceOrder, config: &SimulationConfig) -> Result<(), ExecError> {
    config.market_limits.map_or(Ok(()), |limits| {
        limits
            .check(order)
            .map_err(|violation| ExecError::Rejected {
                reason: violation.to_string(),
            })
    })
}

/// Rejects a buy the account cannot pay for, before it reaches the ledger or
/// the book.
///
/// The venue holds collateral against a resting bid and refuses an order that
/// exceeds the free balance. Without this, a paper run buys with money it
/// never had and reports a return on capital that never existed — the same
/// optimistic bias as filling below the venue minimum, on the account side.
/// Open buys hold their notional, so the same dollar cannot back two orders
/// and a resting order that later fills is already paid for.
///
/// A run created without cash is an unfunded simulation — fills only, no
/// balance to respect — and stays unconstrained.
fn ensure_sufficient_cash(order: &PlaceOrder, ledger: &PaperLedger) -> Result<(), ExecError> {
    if order.side != Side::Buy {
        return Ok(());
    }
    let Some(available) = ledger.available_cash() else {
        return Ok(());
    };
    let cost = Money::from_decimal(order.price * order.qty);
    if cost > available {
        return Err(ExecError::Rejected {
            reason: format!("buy of {cost} exceeds the available paper balance of {available}"),
        });
    }
    Ok(())
}

#[async_trait]
impl Executor for PaperExecutor {
    async fn preflight(&self) -> Result<ExecutionSnapshot, ExecError> {
        Ok(self.snapshot())
    }

    async fn reconcile(&self) -> Result<ExecutionSnapshot, ExecError> {
        Ok(self.snapshot())
    }

    async fn submit(&self, order: &PlaceOrder, now_ms: i64) -> Result<OrderId, ExecError> {
        let (id, drained) = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let ExecutorState {
                engine,
                ledger,
                config,
                last_timestamp_ms,
            } = &mut *state;
            ensure_market_limits(order, config)?;
            ensure_sufficient_cash(order, ledger)?;
            let (placement_id, expected_order_id) = ledger
                .begin_order(order, None, now_ms)
                .map_err(|error| execution_error(&error))?;
            let id = engine.submit(order, now_ms);
            if let Some(actual) = &id {
                if actual.0 != expected_order_id {
                    return Err(ExecError::Transport {
                        message: "paper simulator order id diverged from ledger".into(),
                    });
                }
                let order_state = if order.post_only {
                    OrderState::Resting
                } else if config.activation_latency_ms > 0 {
                    OrderState::Delayed
                } else {
                    OrderState::Immediate
                };
                ledger
                    .acknowledge(
                        placement_id,
                        actual.0.clone(),
                        order_state,
                        now_ms.saturating_add(config.activation_latency_ms),
                        now_ms,
                    )
                    .map_err(|error| execution_error(&error))?;
            } else {
                ledger
                    .reject(placement_id, expected_order_id, now_ms)
                    .map_err(|error| execution_error(&error))?;
            }
            let drained = engine.drain_fills();
            for fill in &drained {
                ledger
                    .record_fill(fill)
                    .map_err(|error| execution_error(&error))?;
            }
            *last_timestamp_ms = now_ms;
            drop(state);
            (id, drained)
        };
        self.deliver(drained).await?;
        id.ok_or_else(|| ExecError::Rejected {
            reason: "no fill available".to_owned(),
        })
    }

    async fn cancel(&self, order_id: &OrderId) -> Result<(), ExecError> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let timestamp_ms = state.last_timestamp_ms;
        if state.engine.cancel(order_id).is_some() {
            state
                .ledger
                .cancel(order_id, timestamp_ms)
                .map_err(|error| execution_error(&error))?;
        }
        drop(state);
        Ok(())
    }

    async fn cancel_all(&self) -> Result<(), ExecError> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let timestamp_ms = state.last_timestamp_ms;
        let open = state
            .engine
            .open_orders()
            .into_iter()
            .map(|order| order.order_id)
            .collect::<Vec<_>>();
        state.engine.cancel_all();
        for order_id in open {
            state
                .ledger
                .cancel(&order_id, timestamp_ms)
                .map_err(|error| execution_error(&error))?;
        }
        drop(state);
        Ok(())
    }
}

impl PaperExecutor {
    fn snapshot(&self) -> ExecutionSnapshot {
        let account = self.account_state();
        let mut open_orders = account
            .resting_orders
            .into_iter()
            .chain(account.delayed_orders)
            .map(|order| order.order_id)
            .collect::<Vec<_>>();
        open_orders.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        ExecutionSnapshot { open_orders }
    }
}

fn execution_error(error: &PaperLedgerError) -> ExecError {
    ExecError::Transport {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::PaperExecutor;
    use pmkit_book::{OrderBookL2, Side};
    use pmkit_core::MarketId;
    use pmkit_event::{Liquidity, MarketEvent};
    use pmkit_exec::{ExecError, Executor, MarketLimits, PlaceOrder, TimeInForce};
    use pmkit_market::Outcome;
    use pmkit_money::Money;
    use pmkit_sim::{MarketCategory, SimulationConfig};
    use rust_decimal::Decimal;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn taker_order_delivers_a_fill() -> Result<(), Box<dyn std::error::Error>> {
        let (tx, mut rx) = mpsc::channel(8);
        let paper = PaperExecutor::new(tx, "paper", MarketCategory::Crypto);

        let market = MarketId::new("btc-5m")?;
        paper
            .update_book(
                &market,
                Outcome::Up,
                OrderBookL2 {
                    bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
                    asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
                    timestamp_ms: 0,
                    last_trade_price: None,
                },
            )
            .await?;

        let order = PlaceOrder {
            market: market.clone(),
            outcome: Outcome::Up,
            side: Side::Buy,
            price: Decimal::new(50, 2),
            qty: Decimal::from(10),
            post_only: false,
            tif: TimeInForce::Gtc,
        };
        let id = paper.submit(&order, 100).await?;
        assert!(id.0.starts_with("paper-"));

        let fill = rx.recv().await.ok_or("expected a fill event")?;
        let MarketEvent::Fill {
            liquidity, size, ..
        } = fill
        else {
            return Err("expected a fill".into());
        };
        assert_eq!(liquidity, Liquidity::Taker);
        assert_eq!(size, Decimal::from(10));
        Ok(())
    }

    #[tokio::test]
    async fn preflight_reports_resting_orders() -> Result<(), Box<dyn std::error::Error>> {
        let (tx, _rx) = mpsc::channel(8);
        let paper = PaperExecutor::new(tx, "paper", MarketCategory::Crypto);
        let market = MarketId::new("btc-5m")?;
        paper
            .update_book(
                &market,
                Outcome::Up,
                OrderBookL2 {
                    bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
                    asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
                    timestamp_ms: 0,
                    last_trade_price: None,
                },
            )
            .await?;
        let order = PlaceOrder {
            market,
            outcome: Outcome::Up,
            side: Side::Buy,
            price: Decimal::new(45, 2),
            qty: Decimal::from(10),
            post_only: true,
            tif: TimeInForce::Gtc,
        };
        let id = paper.submit(&order, 100).await?;

        let snapshot = paper.preflight().await?;

        assert_eq!(snapshot.open_orders, vec![id]);
        Ok(())
    }

    #[tokio::test]
    async fn sub_minimum_order_is_rejected_before_the_ledger()
    -> Result<(), Box<dyn std::error::Error>> {
        let (tx, _rx) = mpsc::channel(8);
        let paper = PaperExecutor::with_config(
            tx,
            "paper",
            MarketCategory::Crypto,
            SimulationConfig {
                market_limits: Some(MarketLimits {
                    min_order_size: Decimal::from(5),
                    tick_size: Decimal::new(1, 2),
                }),
                ..SimulationConfig::default()
            },
        );
        let market = MarketId::new("btc-5m")?;
        paper
            .update_book(
                &market,
                Outcome::Up,
                OrderBookL2 {
                    bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
                    asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
                    timestamp_ms: 0,
                    last_trade_price: None,
                },
            )
            .await?;
        paper.drain_ledger();

        let mut order = PlaceOrder {
            market,
            outcome: Outcome::Up,
            side: Side::Buy,
            price: Decimal::new(50, 2),
            qty: Decimal::from(3),
            post_only: false,
            tif: TimeInForce::Gtc,
        };
        let Err(ExecError::Rejected { reason }) = paper.submit(&order, 100).await else {
            return Err("expected a typed rejection".into());
        };
        assert!(reason.contains("below the venue minimum"));
        // The rejection happens before the ledger: a sub-minimum order the
        // venue would refuse leaves no durable trace to replay.
        assert!(paper.pending_ledger_entry().is_none());

        // At the minimum the order trades normally.
        order.qty = Decimal::from(5);
        paper.submit(&order, 100).await?;
        Ok(())
    }

    #[tokio::test]
    async fn off_tick_price_is_rejected_before_the_ledger() -> Result<(), Box<dyn std::error::Error>>
    {
        let (tx, _rx) = mpsc::channel(8);
        let paper = PaperExecutor::with_config(
            tx,
            "paper",
            MarketCategory::Crypto,
            SimulationConfig {
                market_limits: Some(MarketLimits {
                    min_order_size: Decimal::ONE,
                    tick_size: Decimal::new(1, 2),
                }),
                ..SimulationConfig::default()
            },
        );
        let market = MarketId::new("btc-5m")?;
        paper
            .update_book(
                &market,
                Outcome::Up,
                OrderBookL2 {
                    bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
                    asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
                    timestamp_ms: 0,
                    last_trade_price: None,
                },
            )
            .await?;
        paper.drain_ledger();

        let mut order = PlaceOrder {
            market,
            outcome: Outcome::Up,
            side: Side::Buy,
            price: Decimal::new(505, 3),
            qty: Decimal::from(10),
            post_only: false,
            tif: TimeInForce::Gtc,
        };
        let Err(ExecError::Rejected { reason }) = paper.submit(&order, 100).await else {
            return Err("expected a typed rejection".into());
        };
        assert!(reason.contains("off the venue tick grid"));
        // The rejection happens before the ledger: an off-grid price the
        // venue would refuse leaves no durable trace to replay.
        assert!(paper.pending_ledger_entry().is_none());

        // On the grid the order trades normally.
        order.price = Decimal::new(50, 2);
        paper.submit(&order, 100).await?;
        Ok(())
    }

    /// A funded executor with a book quoting 0.44 / 0.46.
    async fn funded_paper(
        cash: Money,
    ) -> Result<(PaperExecutor, mpsc::Receiver<MarketEvent>, MarketId), Box<dyn std::error::Error>>
    {
        let (tx, rx) = mpsc::channel(8);
        let paper = PaperExecutor::with_account_config(
            tx,
            "paper",
            MarketCategory::Crypto,
            SimulationConfig::default(),
            cash,
        );
        let market = MarketId::new("btc-5m")?;
        paper
            .update_book(
                &market,
                Outcome::Up,
                OrderBookL2 {
                    bids: vec![(Decimal::new(44, 2), Decimal::from(500))],
                    asks: vec![(Decimal::new(46, 2), Decimal::from(500))],
                    timestamp_ms: 0,
                    last_trade_price: None,
                },
            )
            .await?;
        paper.drain_ledger();
        Ok((paper, rx, market))
    }

    fn buy(market: &MarketId, price: Decimal, qty: Decimal, post_only: bool) -> PlaceOrder {
        PlaceOrder {
            market: market.clone(),
            outcome: Outcome::Up,
            side: Side::Buy,
            price,
            qty,
            post_only,
            tif: TimeInForce::Gtc,
        }
    }

    #[tokio::test]
    async fn unaffordable_buy_is_rejected_before_the_ledger()
    -> Result<(), Box<dyn std::error::Error>> {
        let (paper, _fills, market) = funded_paper(Money::usdc(10)).await?;

        // 30 shares at 0.50 costs 15, above the 10 on the account.
        let order = buy(&market, Decimal::new(50, 2), Decimal::from(30), false);
        let Err(ExecError::Rejected { reason }) = paper.submit(&order, 100).await else {
            return Err("expected a typed rejection".into());
        };
        assert!(reason.contains("exceeds the available paper balance"));
        // The rejection happens before the ledger: an order the venue would
        // refuse for lack of collateral leaves no durable trace to replay.
        assert!(paper.pending_ledger_entry().is_none());

        // Within the balance the same order trades normally.
        let affordable = buy(&market, Decimal::new(50, 2), Decimal::from(10), false);
        paper.submit(&affordable, 100).await?;
        Ok(())
    }

    #[tokio::test]
    async fn open_buys_hold_their_notional_against_later_orders()
    -> Result<(), Box<dyn std::error::Error>> {
        let (paper, _fills, market) = funded_paper(Money::usdc(10)).await?;

        // A resting bid for 8 of the 10 available.
        let resting = buy(&market, Decimal::new(40, 2), Decimal::from(20), true);
        paper.submit(&resting, 100).await?;

        // A second order for 5 more fits the raw balance but not the free one:
        // the same dollar cannot back two orders.
        let second = buy(&market, Decimal::new(40, 2), Decimal::from(12), true);
        let Err(ExecError::Rejected { reason }) = paper.submit(&second, 100).await else {
            return Err("expected a typed rejection".into());
        };
        assert!(reason.contains("exceeds the available paper balance"));

        // What the reservation leaves free still trades.
        let within_free = buy(&market, Decimal::new(40, 2), Decimal::from(5), true);
        paper.submit(&within_free, 100).await?;
        Ok(())
    }

    #[tokio::test]
    async fn sells_and_unfunded_runs_stay_unconstrained() -> Result<(), Box<dyn std::error::Error>>
    {
        // An unfunded run is a fills-only simulation: no balance to respect.
        let (tx, _rx) = mpsc::channel(8);
        let paper = PaperExecutor::new(tx, "paper", MarketCategory::Crypto);
        let market = MarketId::new("btc-5m")?;
        paper
            .update_book(
                &market,
                Outcome::Up,
                OrderBookL2 {
                    bids: vec![(Decimal::new(44, 2), Decimal::from(500))],
                    asks: vec![(Decimal::new(46, 2), Decimal::from(500))],
                    timestamp_ms: 0,
                    last_trade_price: None,
                },
            )
            .await?;
        paper
            .submit(
                &buy(&market, Decimal::new(50, 2), Decimal::from(100), false),
                100,
            )
            .await?;

        // A sell raises cash rather than spending it, so it rests whatever the
        // balance is; holdings are the accounting ledger's business, not this
        // guard's. Priced above the book so it rests instead of crossing.
        let (funded, _funded_fills, funded_market) = funded_paper(Money::usdc(10)).await?;
        let sell = PlaceOrder {
            side: Side::Sell,
            ..buy(
                &funded_market,
                Decimal::new(60, 2),
                Decimal::from(100),
                true,
            )
        };
        funded.submit(&sell, 100).await?;
        Ok(())
    }
}
