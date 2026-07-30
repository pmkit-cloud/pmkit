//! Conservative fill-simulation engine for `PMKit` paper and backtest runs.
//!
//! The engine holds per-outcome order books and resting maker orders. Taker
//! orders fill immediately by walking the book; maker orders rest and fill when
//! a later book update crosses them. Fills are emitted as
//! [`MarketEvent::Fill`], never touching a real venue.

// allow: SIZE_OK — the simulator's order lifecycle is one state machine.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use pmkit_book::{OrderBookL2, Side};
use pmkit_core::{MarketId, StrategyId};
use pmkit_event::{Liquidity, MarketEvent};
use pmkit_exec::{OrderId, PlaceOrder, TimeInForce};
use pmkit_market::Outcome;
use pmkit_math::fill::walk_book;
use rust_decimal::Decimal;

mod fee;

pub use fee::{FeeModel, FeeModelError};
pub use pmkit_math::fees::MarketCategory;

type BookKey = (MarketId, Outcome);

#[derive(Debug, Clone)]
struct RestingOrder {
    order_id: String,
    order: PlaceOrder,
    submitted_ms: i64,
    active_at_ms: i64,
    strategy: Option<StrategyId>,
}

#[derive(Debug, Clone)]
struct DelayedOrder {
    order_id: String,
    order: PlaceOrder,
    submitted_ms: i64,
    active_at_ms: i64,
    strategy: Option<StrategyId>,
}

/// Current simulator-owned order state used by read-only report projections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimOpenOrder {
    /// Stable simulated order identity.
    pub order_id: OrderId,
    /// Exact owning market.
    pub market: MarketId,
    /// Strategy that submitted the order when known.
    pub strategy: Option<StrategyId>,
    /// Limit price.
    pub price: Decimal,
    /// Quantity not yet filled.
    pub remaining_qty: Decimal,
}

/// Explicit inputs for conservative simulation behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SimulationConfig {
    /// Delay from submission until an order can fill.
    pub activation_latency_ms: i64,
    /// Share of crossed maker liquidity assumed ahead in queue.
    pub maker_queue_ahead_bps: u16,
    /// Adverse taker slippage in basis points.
    pub slippage_bps: u16,
    /// Adverse taker impact in basis points.
    pub market_impact_bps: u16,
    /// Optional fee override; unset preserves the selected category's legacy fees.
    pub fee_model: Option<FeeModel>,
    /// Venue minimum order size in **shares** (Polymarket `orderMinSize`);
    /// executors reject smaller orders the way the venue would. Unset skips
    /// the check.
    pub min_order_size: Option<Decimal>,
    /// Venue price increment: valid prices are multiples of it inside
    /// `[tick, 1 - tick]`. Off-grid orders are refused the way the venue
    /// would refuse them instead of filling. Unset skips the check.
    pub tick_size: Option<Decimal>,
}

/// A conservative fill-simulation engine shared by paper and backtest modes.
#[derive(Debug)]
pub struct SimEngine {
    books: HashMap<BookKey, OrderBookL2>,
    resting: HashMap<String, RestingOrder>,
    delayed: Vec<DelayedOrder>,
    pending_fills: Vec<MarketEvent>,
    next_id: AtomicU64,
    id_prefix: String,
    fee_model: FeeModel,
    config: SimulationConfig,
}

impl SimEngine {
    /// Creates an engine that mints order ids as `"{id_prefix}-{n}"` from
    /// `start_id`, charging taker fees for `category`.
    #[must_use]
    pub fn new(id_prefix: impl Into<String>, start_id: u64, category: MarketCategory) -> Self {
        Self::with_config(id_prefix, start_id, category, SimulationConfig::default())
    }

    /// Creates an engine with explicit latency, queue, slippage, and impact inputs.
    #[must_use]
    pub fn with_config(
        id_prefix: impl Into<String>,
        start_id: u64,
        category: MarketCategory,
        config: SimulationConfig,
    ) -> Self {
        let fee_model = config
            .fee_model
            .unwrap_or_else(|| FeeModel::for_category(category));
        Self::with_fee_config(
            id_prefix,
            start_id,
            SimulationConfig {
                fee_model: Some(fee_model),
                ..config
            },
        )
    }

    /// Creates an engine from a fee-resolved simulation configuration.
    #[must_use]
    pub fn with_fee_config(
        id_prefix: impl Into<String>,
        start_id: u64,
        config: SimulationConfig,
    ) -> Self {
        let fee_model = config.fee_model.unwrap_or_default();
        Self {
            books: HashMap::new(),
            resting: HashMap::new(),
            delayed: Vec::new(),
            pending_fills: Vec::new(),
            next_id: AtomicU64::new(start_id),
            id_prefix: id_prefix.into(),
            fee_model,
            config,
        }
    }

    /// Replaces the book for one market outcome, expires GTD orders the new
    /// book time has passed, and fills any crossed makers.
    pub fn update_book(&mut self, market: &MarketId, outcome: Outcome, book: OrderBookL2) {
        let now_ms = book.timestamp_ms;
        self.books.insert((market.clone(), outcome), book);
        self.expire_orders(market, outcome, now_ms);
        self.activate_delayed(market, outcome);
        self.check_resting_fills(market, outcome);
    }

    /// Submits an order. Post-only orders rest; others take liquidity. Returns
    /// the minted order id, or `None` when there is no book, no fill, or the
    /// order is already expired.
    pub fn submit(&mut self, order: &PlaceOrder, now_ms: i64) -> Option<OrderId> {
        self.submit_with_strategy(order, None, now_ms)
    }

    /// Submits an order while retaining its strategy ownership for reporting.
    pub fn submit_for_strategy(
        &mut self,
        order: &PlaceOrder,
        strategy: StrategyId,
        now_ms: i64,
    ) -> Option<OrderId> {
        self.submit_with_strategy(order, Some(strategy), now_ms)
    }

    fn submit_with_strategy(
        &mut self,
        order: &PlaceOrder,
        strategy: Option<StrategyId>,
        now_ms: i64,
    ) -> Option<OrderId> {
        if Self::expired(order, now_ms)
            || self.below_min_order_size(order)
            || self.off_tick_grid(order)
        {
            return None;
        }
        let order_id = self.next_order_id();
        if order.post_only {
            self.submit_maker(order_id, order, strategy, now_ms)
        } else if self.config.activation_latency_ms > 0 {
            self.delayed.push(DelayedOrder {
                order_id: order_id.clone(),
                order: order.clone(),
                submitted_ms: now_ms,
                active_at_ms: now_ms.saturating_add(self.config.activation_latency_ms),
                strategy,
            });
            Some(OrderId(order_id))
        } else {
            self.submit_taker(order_id, order, now_ms, now_ms)
        }
    }

    /// Cancels a resting order, returning the notional it freed.
    pub fn cancel(&mut self, order_id: &OrderId) -> Option<Decimal> {
        self.resting
            .remove(&order_id.0)
            .map(|r| r.order.qty * r.order.price)
            .or_else(|| {
                self.delayed
                    .iter()
                    .position(|order| order.order_id == order_id.0)
                    .map(|index| {
                        let order = self.delayed.remove(index);
                        order.order.qty * order.order.price
                    })
            })
    }

    /// Cancels every resting order, returning the total notional freed.
    pub fn cancel_all(&mut self) -> Decimal {
        let freed = self
            .open_orders()
            .iter()
            .map(|order| order.remaining_qty * order.price)
            .sum();
        self.resting.clear();
        self.delayed.clear();
        freed
    }

    /// Returns all delayed and resting orders in deterministic id order.
    #[must_use]
    pub fn open_orders(&self) -> Vec<SimOpenOrder> {
        let mut orders = self
            .resting
            .values()
            .map(|order| SimOpenOrder {
                order_id: OrderId(order.order_id.clone()),
                market: order.order.market.clone(),
                strategy: order.strategy.clone(),
                price: order.order.price,
                remaining_qty: order.order.qty,
            })
            .chain(self.delayed.iter().map(|order| SimOpenOrder {
                order_id: OrderId(order.order_id.clone()),
                market: order.order.market.clone(),
                strategy: order.strategy.clone(),
                price: order.order.price,
                remaining_qty: order.order.qty,
            }))
            .collect::<Vec<_>>();
        orders.sort_by(|left, right| left.order_id.0.cmp(&right.order_id.0));
        orders
    }

    /// Returns the total resting maker notional.
    #[must_use]
    pub fn resting_committed(&self) -> Decimal {
        self.resting
            .values()
            .map(|r| r.order.qty * r.order.price)
            .sum()
    }

    /// Returns the number of resting maker orders.
    #[must_use]
    pub fn resting_count(&self) -> usize {
        self.resting.len()
    }

    /// Returns resting maker order ids in deterministic order.
    #[must_use]
    pub fn resting_order_ids(&self) -> Vec<OrderId> {
        let mut ids: Vec<_> = self.resting.keys().cloned().map(OrderId).collect();
        ids.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        ids
    }

    /// Drains and returns the fills accumulated since the last call.
    pub fn drain_fills(&mut self) -> Vec<MarketEvent> {
        std::mem::take(&mut self.pending_fills)
    }

    fn submit_maker(
        &mut self,
        order_id: String,
        order: &PlaceOrder,
        strategy: Option<StrategyId>,
        now_ms: i64,
    ) -> Option<OrderId> {
        let would_cross = {
            let book = self.books.get(&(order.market.clone(), order.outcome))?;
            match order.side {
                Side::Buy => book.best_ask().is_some_and(|(ask, _)| order.price >= ask),
                Side::Sell => book.best_bid().is_some_and(|(bid, _)| order.price <= bid),
            }
        };
        if would_cross {
            return None;
        }
        self.resting.insert(
            order_id.clone(),
            RestingOrder {
                order_id: order_id.clone(),
                order: order.clone(),
                submitted_ms: now_ms,
                active_at_ms: now_ms.saturating_add(self.config.activation_latency_ms),
                strategy,
            },
        );
        Some(OrderId(order_id))
    }

    fn submit_taker(
        &mut self,
        order_id: String,
        order: &PlaceOrder,
        submitted_ms: i64,
        fill_ms: i64,
    ) -> Option<OrderId> {
        let (vwap, fill_qty) = {
            let book = self.books.get(&(order.market.clone(), order.outcome))?;
            let (levels, is_buy) = match order.side {
                Side::Buy => (&book.asks, true),
                Side::Sell => (&book.bids, false),
            };
            walk_book(levels, order.qty, order.price, is_buy)?
        };
        let adverse_bps = Decimal::from(
            u32::from(self.config.slippage_bps) + u32::from(self.config.market_impact_bps),
        ) / Decimal::from(10_000_u32);
        let fill_price = match order.side {
            Side::Buy => (vwap * (Decimal::ONE + adverse_bps)).min(order.price),
            Side::Sell => (vwap * (Decimal::ONE - adverse_bps)).max(order.price),
        };
        let fee = self
            .fee_model
            .fee_order(fill_qty, fill_price, Liquidity::Taker)?;
        self.pending_fills.push(MarketEvent::Fill {
            strategy: None,
            order_id: order_id.clone(),
            market: order.market.clone(),
            outcome: order.outcome,
            price: fill_price,
            size: fill_qty,
            side: order.side,
            fee,
            liquidity: Liquidity::Taker,
            timestamp_ms: fill_ms.max(submitted_ms),
        });
        Some(OrderId(order_id))
    }

    const fn expired(order: &PlaceOrder, now_ms: i64) -> bool {
        match order.tif {
            TimeInForce::Gtc => false,
            TimeInForce::Gtd { expire_at_ms } => expire_at_ms <= now_ms,
        }
    }

    /// Removes expired GTD orders for one market outcome before any fill
    /// check, mirroring the venue removing them server-side. Expiry at the
    /// book timestamp wins over a fill at the same timestamp: an order that
    /// expired at `t` must not fill at `t`.
    fn expire_orders(&mut self, market: &MarketId, outcome: Outcome, now_ms: i64) {
        self.resting.retain(|_, resting| {
            resting.order.market != *market
                || resting.order.outcome != outcome
                || !Self::expired(&resting.order, now_ms)
        });
        self.delayed.retain(|delayed| {
            delayed.order.market != *market
                || delayed.order.outcome != outcome
                || !Self::expired(&delayed.order, now_ms)
        });
    }

    fn activate_delayed(&mut self, market: &MarketId, outcome: Outcome) {
        let Some(book) = self.books.get(&(market.clone(), outcome)) else {
            return;
        };
        let now_ms = book.timestamp_ms;
        let mut remaining = Vec::new();
        for delayed in std::mem::take(&mut self.delayed) {
            if delayed.order.market == *market
                && delayed.order.outcome == outcome
                && delayed.active_at_ms <= now_ms
            {
                self.submit_taker(
                    delayed.order_id,
                    &delayed.order,
                    delayed.submitted_ms,
                    now_ms,
                );
            } else {
                remaining.push(delayed);
            }
        }
        self.delayed = remaining;
    }

    /// True when the config sets a venue minimum and the order is smaller.
    /// The minimum is in shares (Polymarket `orderMinSize`), not a notional:
    /// dividing it by the price inflates the threshold and starves the run.
    fn below_min_order_size(&self, order: &PlaceOrder) -> bool {
        self.config
            .min_order_size
            .is_some_and(|min_order_size| order.qty < min_order_size)
    }

    /// True when the config sets a venue tick and the price is off its grid.
    /// Valid venue prices are multiples of the tick inside `[tick, 1 - tick]`;
    /// a book-crossing fill at an off-grid price reports an edge the venue
    /// would refuse to quote.
    fn off_tick_grid(&self, order: &PlaceOrder) -> bool {
        self.config.tick_size.is_some_and(|tick_size| {
            order.price < tick_size
                || order.price > Decimal::ONE - tick_size
                || !(order.price % tick_size).is_zero()
        })
    }

    fn next_order_id(&self) -> String {
        format!(
            "{}-{}",
            self.id_prefix,
            self.next_id.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn check_resting_fills(&mut self, market: &MarketId, outcome: Outcome) {
        let to_fill: Vec<String> = {
            let Some(book) = self.books.get(&(market.clone(), outcome)) else {
                return;
            };
            self.resting
                .values()
                .filter(|r| {
                    r.order.market == *market
                        && r.order.outcome == outcome
                        && r.active_at_ms <= book.timestamp_ms
                })
                .filter(|r| match r.order.side {
                    Side::Buy => book.best_ask().is_some_and(|(ask, _)| ask <= r.order.price),
                    Side::Sell => book.best_bid().is_some_and(|(bid, _)| bid >= r.order.price),
                })
                .map(|r| r.order_id.clone())
                .collect()
        };

        for order_id in to_fill {
            if let Some(mut resting) = self.resting.remove(&order_id) {
                let Some(book) = self.books.get(&(market.clone(), outcome)) else {
                    continue;
                };
                let top_quantity = match resting.order.side {
                    Side::Buy => book
                        .best_ask()
                        .map_or(Decimal::ZERO, |(_, quantity)| quantity),
                    Side::Sell => book
                        .best_bid()
                        .map_or(Decimal::ZERO, |(_, quantity)| quantity),
                };
                let available = top_quantity
                    * (Decimal::ONE
                        - Decimal::from(self.config.maker_queue_ahead_bps)
                            / Decimal::from(10_000_u32));
                let fill_quantity = resting.order.qty.min(available.max(Decimal::ZERO));
                if fill_quantity.is_zero() {
                    self.resting.insert(order_id, resting);
                    continue;
                }
                let Some(fee) =
                    self.fee_model
                        .fee_order(fill_quantity, resting.order.price, Liquidity::Maker)
                else {
                    self.resting.insert(order_id, resting);
                    continue;
                };
                self.pending_fills.push(MarketEvent::Fill {
                    strategy: None,
                    order_id: resting.order_id.clone(),
                    market: resting.order.market.clone(),
                    outcome: resting.order.outcome,
                    price: resting.order.price,
                    size: fill_quantity,
                    side: resting.order.side,
                    fee,
                    liquidity: Liquidity::Maker,
                    timestamp_ms: book.timestamp_ms.max(resting.submitted_ms),
                });
                resting.order.qty -= fill_quantity;
                if !resting.order.qty.is_zero() {
                    self.resting.insert(order_id, resting);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
