use std::collections::HashMap;

use pmkit_accounting::{AccountingError, LedgerFill, LedgerPosition, PortfolioLedger, Settlement};
use pmkit_book::{OrderBookL2, Side};
use pmkit_core::{MarketId, StrategyId};
use pmkit_event::MarketEvent;
use pmkit_exec::{OrderId, PlaceOrder, TimeInForce};
use pmkit_market::Outcome;
use pmkit_money::Money;
use pmkit_sim::{SimEngine, SimulationConfig};
use rust_decimal::Decimal;
use thiserror::Error;

// allow: SIZE_OK — exhaustive ledger transitions stay together for fail-closed auditing.

/// A typed failure that prevents unsafe paper-ledger reconstruction.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PaperLedgerError {
    /// A tagged ledger payload could not be decoded.
    #[error("paper ledger record is invalid: {message}")]
    InvalidRecord {
        /// The malformed field or value.
        message: String,
    },
    /// The durable row uses a newer ledger schema.
    #[error("paper ledger schema version {version} is unsupported")]
    UnsupportedVersion {
        /// The unsupported durable version.
        version: u16,
    },
    /// Unique records are not contiguous in ledger order.
    #[error("paper ledger sequence gap: expected {expected}, found {actual}")]
    SequenceGap {
        /// The next required sequence.
        expected: u64,
        /// The sequence found on disk.
        actual: u64,
    },
    /// One event identity maps to different payloads.
    #[error("paper ledger event identity {event_id} has conflicting records")]
    ConflictingDuplicate {
        /// The reused stable event identity.
        event_id: String,
    },
    /// A record contradicts preceding ledger state.
    #[error("paper ledger event {event_id} is inconsistent: {message}")]
    Inconsistent {
        /// The event that violated replay invariants.
        event_id: String,
        /// The failed invariant.
        message: String,
    },
    /// Applying a validly shaped money event failed accounting rules.
    #[error(transparent)]
    Accounting(#[from] AccountingError),
}

/// One reconstructed open paper order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaperOpenOrder {
    /// Stable simulated order identity.
    pub order_id: OrderId,
    /// Exact owning market.
    pub market: MarketId,
    /// Strategy that submitted the order when known.
    pub strategy: Option<StrategyId>,
    /// Traded outcome token.
    pub outcome: Outcome,
    /// Buy or sell direction.
    pub side: Side,
    /// Limit price.
    pub price: Decimal,
    /// Quantity not yet filled.
    pub remaining_qty: Decimal,
    /// Original logical submission time.
    pub submitted_ms: i64,
    /// Earliest logical activation time.
    pub active_at_ms: i64,
}

/// Complete paper-account state derived solely by ledger replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaperAccountState {
    /// Cash after movements, fills, fees, and settlements.
    pub cash: Money,
    /// Cumulative charged fees.
    pub fees: Money,
    /// Cumulative realized profit and loss.
    pub realized_pnl: Money,
    /// Open positions isolated by market and outcome.
    pub positions: Vec<LedgerPosition>,
    /// Active maker orders.
    pub resting_orders: Vec<PaperOpenOrder>,
    /// Taker orders awaiting activation latency.
    pub delayed_orders: Vec<PaperOpenOrder>,
    /// Next numeric suffix the simulator will mint.
    pub next_order_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderState {
    Immediate,
    Resting,
    Delayed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedOrder {
    pub(crate) market: MarketId,
    pub(crate) strategy: Option<StrategyId>,
    pub(crate) outcome: Outcome,
    pub(crate) side: Side,
    pub(crate) price: Decimal,
    pub(crate) qty: Decimal,
    pub(crate) post_only: bool,
    pub(crate) tif: TimeInForce,
}

impl RecordedOrder {
    fn from_order(order: &PlaceOrder, strategy: Option<StrategyId>) -> Self {
        Self {
            market: order.market.clone(),
            strategy,
            outcome: order.outcome,
            side: order.side,
            price: order.price,
            qty: order.qty,
            post_only: order.post_only,
            tif: order.tif,
        }
    }

    fn to_order(&self) -> PlaceOrder {
        PlaceOrder {
            market: self.market.clone(),
            outcome: self.outcome,
            side: self.side,
            price: self.price,
            qty: self.qty,
            post_only: self.post_only,
            tif: self.tif,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaperLedgerEvent {
    CashMovement {
        amount: Decimal,
    },
    OrderPlaced {
        order: RecordedOrder,
    },
    OrderAck {
        placement_id: String,
        order_id: String,
        state: OrderState,
        active_at_ms: i64,
    },
    OrderRejected {
        placement_id: String,
        order_id: String,
    },
    OrderCancelled {
        order_id: String,
    },
    Fill {
        order_id: String,
        market: MarketId,
        outcome: Outcome,
        price: Decimal,
        size: Decimal,
        side: Side,
        fee: Decimal,
    },
    Settlement {
        market: MarketId,
        outcome: Outcome,
        settled_size: Decimal,
        proceeds: Decimal,
    },
}

/// One immutable, stable-identity paper-ledger record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaperLedgerEntry {
    pub(crate) event_id: String,
    pub(crate) sequence: u64,
    pub(crate) timestamp_ms: i64,
    pub(crate) event: PaperLedgerEvent,
}

#[derive(Debug, Clone)]
struct Placement {
    order: RecordedOrder,
    expected_order_id: String,
    terminal: Option<PlacementOutcome>,
}

#[derive(Debug, Clone)]
struct TrackedOrder {
    order: RecordedOrder,
    state: OrderState,
    submitted_ms: i64,
    active_at_ms: i64,
}

#[derive(Debug, Clone, Copy)]
enum PlacementOutcome {
    Ack(OrderState),
    Rejected,
}

#[derive(Debug)]
pub struct PaperLedger {
    account: PortfolioLedger,
    entries: Vec<PaperLedgerEntry>,
    pending: Vec<PaperLedgerEntry>,
    placements: HashMap<String, Placement>,
    orders: HashMap<String, TrackedOrder>,
    next_sequence: u64,
    next_order_id: u64,
    fill_count: usize,
    id_prefix: String,
    cash_funded: bool,
}

impl PaperLedger {
    pub(crate) fn new(initial_cash: Money, id_prefix: String) -> Self {
        let mut ledger = Self::empty(PortfolioLedger::new(initial_cash), id_prefix);
        ledger.cash_funded = initial_cash > Money::ZERO;
        let entry = PaperLedgerEntry {
            event_id: ledger.next_event_id(),
            sequence: 0,
            timestamp_ms: 0,
            event: PaperLedgerEvent::CashMovement {
                amount: initial_cash.as_decimal(),
            },
        };
        ledger.next_sequence = 1;
        ledger.entries.push(entry.clone());
        ledger.pending.push(entry);
        ledger
    }

    pub(crate) fn reconstruct(
        entries: &[PaperLedgerEntry],
        id_prefix: String,
    ) -> Result<Self, PaperLedgerError> {
        let unique = deduplicate(entries)?;
        let initial_cash = unique
            .iter()
            .filter_map(|entry| match entry.event {
                PaperLedgerEvent::CashMovement { amount } => Some(amount),
                PaperLedgerEvent::OrderPlaced { .. }
                | PaperLedgerEvent::OrderAck { .. }
                | PaperLedgerEvent::OrderRejected { .. }
                | PaperLedgerEvent::OrderCancelled { .. }
                | PaperLedgerEvent::Fill { .. }
                | PaperLedgerEvent::Settlement { .. } => None,
            })
            .sum();
        let mut ledger = Self::empty(
            PortfolioLedger::new(Money::from_decimal(initial_cash)),
            id_prefix,
        );
        ledger.cash_funded = initial_cash > Decimal::ZERO;
        for entry in &unique {
            if entry.sequence != ledger.next_sequence {
                return Err(PaperLedgerError::SequenceGap {
                    expected: ledger.next_sequence,
                    actual: entry.sequence,
                });
            }
            ledger.apply(entry)?;
            ledger.next_sequence += 1;
        }
        if let Some((event_id, _)) = ledger
            .placements
            .iter()
            .find(|(_, placement)| placement.terminal.is_none())
        {
            return Err(PaperLedgerError::Inconsistent {
                event_id: event_id.clone(),
                message: "order placement has no acknowledgement or rejection".into(),
            });
        }
        if let Some((order_id, _)) = ledger
            .orders
            .iter()
            .find(|(_, order)| order.state == OrderState::Immediate)
        {
            return Err(PaperLedgerError::Inconsistent {
                event_id: order_id.clone(),
                message: "immediate order has no terminal fill".into(),
            });
        }
        ledger.entries = unique;
        Ok(ledger)
    }

    fn empty(account: PortfolioLedger, id_prefix: String) -> Self {
        Self {
            account,
            entries: Vec::new(),
            pending: Vec::new(),
            placements: HashMap::new(),
            orders: HashMap::new(),
            next_sequence: 0,
            next_order_id: 0,
            fill_count: 0,
            id_prefix,
            cash_funded: false,
        }
    }

    /// Cash still free to commit, or `None` when the run declared no cash and
    /// is therefore not cash-constrained.
    ///
    /// Open buy orders hold their notional the way the venue holds collateral
    /// against a resting bid: a run cannot commit the same dollar twice, and a
    /// resting order that later fills is already paid for.
    pub(crate) fn available_cash(&self) -> Option<Money> {
        if !self.cash_funded {
            return None;
        }
        let committed: Decimal = self
            .orders
            .values()
            .filter(|tracked| tracked.order.side == Side::Buy)
            .map(|tracked| tracked.order.price * tracked.order.qty)
            .sum();
        Some(self.account.cash() - Money::from_decimal(committed))
    }

    pub(crate) fn begin_order(
        &mut self,
        order: &PlaceOrder,
        strategy: Option<StrategyId>,
        timestamp_ms: i64,
    ) -> Result<(String, String), PaperLedgerError> {
        let placement_id = self.next_event_id();
        let expected_order_id = format!("{}-{}", self.id_prefix, self.next_order_id);
        self.append(
            timestamp_ms,
            PaperLedgerEvent::OrderPlaced {
                order: RecordedOrder::from_order(order, strategy),
            },
        )?;
        Ok((placement_id, expected_order_id))
    }

    pub(crate) fn acknowledge(
        &mut self,
        placement_id: String,
        order_id: String,
        state: OrderState,
        active_at_ms: i64,
        timestamp_ms: i64,
    ) -> Result<(), PaperLedgerError> {
        self.append(
            timestamp_ms,
            PaperLedgerEvent::OrderAck {
                placement_id,
                order_id,
                state,
                active_at_ms,
            },
        )
    }

    pub(crate) fn reject(
        &mut self,
        placement_id: String,
        order_id: String,
        timestamp_ms: i64,
    ) -> Result<(), PaperLedgerError> {
        self.append(
            timestamp_ms,
            PaperLedgerEvent::OrderRejected {
                placement_id,
                order_id,
            },
        )
    }

    pub(crate) fn cancel(
        &mut self,
        order_id: &OrderId,
        timestamp_ms: i64,
    ) -> Result<(), PaperLedgerError> {
        if self.orders.contains_key(&order_id.0) {
            self.append(
                timestamp_ms,
                PaperLedgerEvent::OrderCancelled {
                    order_id: order_id.0.clone(),
                },
            )?;
        }
        Ok(())
    }

    pub(crate) fn record_fill(&mut self, fill: &MarketEvent) -> Result<(), PaperLedgerError> {
        let MarketEvent::Fill {
            order_id,
            market,
            outcome,
            price,
            size,
            side,
            fee,
            timestamp_ms,
            ..
        } = fill
        else {
            return Err(PaperLedgerError::InvalidRecord {
                message: "paper ledger received a non-fill event as a fill".into(),
            });
        };
        self.append(
            *timestamp_ms,
            PaperLedgerEvent::Fill {
                order_id: order_id.clone(),
                market: market.clone(),
                outcome: *outcome,
                price: *price,
                size: *size,
                side: *side,
                fee: *fee,
            },
        )
    }

    pub(crate) fn settle(
        &mut self,
        market: MarketId,
        outcome: Outcome,
        settled_size: Decimal,
        proceeds: Decimal,
        timestamp_ms: i64,
    ) -> Result<(), PaperLedgerError> {
        self.append(
            timestamp_ms,
            PaperLedgerEvent::Settlement {
                market,
                outcome,
                settled_size,
                proceeds,
            },
        )
    }

    pub(crate) fn maturing_delayed(
        &self,
        market: &MarketId,
        outcome: Outcome,
        timestamp_ms: i64,
    ) -> Vec<OrderId> {
        self.orders
            .iter()
            .filter(|(_, tracked)| {
                tracked.state == OrderState::Delayed
                    && tracked.order.market == *market
                    && tracked.order.outcome == outcome
                    && tracked.active_at_ms <= timestamp_ms
            })
            .map(|(order_id, _)| OrderId(order_id.clone()))
            .collect()
    }

    pub(crate) fn contains_order(&self, order_id: &OrderId) -> bool {
        self.orders.contains_key(&order_id.0)
    }

    pub(crate) fn drain_pending(&mut self) -> Vec<PaperLedgerEntry> {
        std::mem::take(&mut self.pending)
    }

    pub(crate) fn pending_entry(&self) -> Option<PaperLedgerEntry> {
        self.pending.first().cloned()
    }

    pub(crate) fn acknowledge_pending(&mut self, event_id: &str) -> bool {
        if self
            .pending
            .first()
            .is_some_and(|entry| entry.event_id == event_id)
        {
            self.pending.remove(0);
            return true;
        }
        false
    }

    pub(crate) const fn fill_count(&self) -> usize {
        self.fill_count
    }

    pub(crate) fn last_timestamp_ms(&self) -> i64 {
        self.entries.last().map_or(0, |entry| entry.timestamp_ms)
    }

    pub(crate) fn state(&self) -> PaperAccountState {
        let mut positions = self.account.positions().to_vec();
        positions.sort_by(|left, right| {
            left.market
                .to_string()
                .cmp(&right.market.to_string())
                .then_with(|| outcome_rank(left.outcome).cmp(&outcome_rank(right.outcome)))
        });
        let (mut resting_orders, mut delayed_orders) = (Vec::new(), Vec::new());
        for (order_id, tracked) in &self.orders {
            let open = PaperOpenOrder {
                order_id: OrderId(order_id.clone()),
                market: tracked.order.market.clone(),
                strategy: tracked.order.strategy.clone(),
                outcome: tracked.order.outcome,
                side: tracked.order.side,
                price: tracked.order.price,
                remaining_qty: tracked.order.qty,
                submitted_ms: tracked.submitted_ms,
                active_at_ms: tracked.active_at_ms,
            };
            match tracked.state {
                OrderState::Resting => resting_orders.push(open),
                OrderState::Delayed => delayed_orders.push(open),
                OrderState::Immediate => {}
            }
        }
        resting_orders.sort_by(|left, right| left.order_id.0.cmp(&right.order_id.0));
        delayed_orders.sort_by(|left, right| left.order_id.0.cmp(&right.order_id.0));
        PaperAccountState {
            cash: self.account.cash(),
            fees: self.account.fees(),
            realized_pnl: self.account.realized_pnl(),
            positions,
            resting_orders,
            delayed_orders,
            next_order_id: self.next_order_id,
        }
    }

    pub(crate) fn rebuild_engine(
        &self,
        config: SimulationConfig,
    ) -> Result<SimEngine, PaperLedgerError> {
        let mut engine = SimEngine::with_fee_config(self.id_prefix.clone(), 0, config);
        for entry in &self.entries {
            let PaperLedgerEvent::OrderPlaced { order } = &entry.event else {
                continue;
            };
            let placement = self.placements.get(&entry.event_id).ok_or_else(|| {
                PaperLedgerError::Inconsistent {
                    event_id: entry.event_id.clone(),
                    message: "order placement disappeared during replay".into(),
                }
            })?;
            let mut restored_order = order.clone();
            if let Some(open) = self.orders.get(&placement.expected_order_id) {
                restored_order.qty = open.order.qty;
            }
            let restored_order = restored_order.to_order();
            if restored_order.post_only {
                engine.update_book(
                    &restored_order.market,
                    restored_order.outcome,
                    empty_book(i64::MIN),
                );
            }
            let restored_id = engine.submit(&restored_order, entry.timestamp_ms);
            match placement.terminal {
                Some(PlacementOutcome::Ack(
                    OrderState::Resting | OrderState::Delayed | OrderState::Immediate,
                )) => {
                    if let Some(actual) = restored_id.as_ref()
                        && actual.0 != placement.expected_order_id
                    {
                        return Err(PaperLedgerError::Inconsistent {
                            event_id: entry.event_id.clone(),
                            message: "simulator order id diverged during reconstruction".into(),
                        });
                    }
                    if !self.orders.contains_key(&placement.expected_order_id) {
                        close_replayed_order(
                            &mut engine,
                            &restored_order,
                            restored_id.as_ref(),
                            entry.timestamp_ms,
                            config,
                        );
                    }
                }
                Some(PlacementOutcome::Rejected) | None => {
                    close_replayed_order(
                        &mut engine,
                        &restored_order,
                        restored_id.as_ref(),
                        entry.timestamp_ms,
                        config,
                    );
                }
            }
        }
        engine.drain_fills();
        Ok(engine)
    }

    fn next_event_id(&self) -> String {
        format!("paper-ledger-{}", self.next_sequence)
    }

    fn append(
        &mut self,
        timestamp_ms: i64,
        event: PaperLedgerEvent,
    ) -> Result<(), PaperLedgerError> {
        let entry = PaperLedgerEntry {
            event_id: self.next_event_id(),
            sequence: self.next_sequence,
            timestamp_ms,
            event,
        };
        self.apply(&entry)?;
        self.next_sequence += 1;
        self.entries.push(entry.clone());
        self.pending.push(entry);
        Ok(())
    }

    fn apply(&mut self, entry: &PaperLedgerEntry) -> Result<(), PaperLedgerError> {
        match &entry.event {
            PaperLedgerEvent::CashMovement { .. } => Ok(()),
            PaperLedgerEvent::OrderPlaced { order } => self.apply_placement(entry, order),
            PaperLedgerEvent::OrderAck {
                placement_id,
                order_id,
                state,
                active_at_ms,
            } => self.apply_ack(entry, placement_id, order_id, *state, *active_at_ms),
            PaperLedgerEvent::OrderRejected {
                placement_id,
                order_id,
            } => self.apply_rejection(entry, placement_id, order_id),
            PaperLedgerEvent::OrderCancelled { order_id } => {
                self.orders.remove(order_id).map_or_else(
                    || {
                        Err(PaperLedgerError::Inconsistent {
                            event_id: entry.event_id.clone(),
                            message: format!("cancel references unknown order {order_id}"),
                        })
                    },
                    |_| Ok(()),
                )
            }
            PaperLedgerEvent::Fill {
                order_id,
                market,
                outcome,
                price,
                size,
                side,
                fee,
            } => self.apply_fill(
                entry, order_id, market, *outcome, *price, *size, *side, *fee,
            ),
            PaperLedgerEvent::Settlement {
                market,
                outcome,
                settled_size,
                proceeds,
            } => self.apply_settlement(entry, market, *outcome, *settled_size, *proceeds),
        }
    }

    fn apply_placement(
        &mut self,
        entry: &PaperLedgerEntry,
        order: &RecordedOrder,
    ) -> Result<(), PaperLedgerError> {
        let expected_order_id = format!("{}-{}", self.id_prefix, self.next_order_id);
        if self
            .placements
            .insert(
                entry.event_id.clone(),
                Placement {
                    order: order.clone(),
                    expected_order_id,
                    terminal: None,
                },
            )
            .is_some()
        {
            return Err(PaperLedgerError::ConflictingDuplicate {
                event_id: entry.event_id.clone(),
            });
        }
        self.next_order_id += 1;
        Ok(())
    }

    fn apply_ack(
        &mut self,
        entry: &PaperLedgerEntry,
        placement_id: &str,
        order_id: &str,
        state: OrderState,
        active_at_ms: i64,
    ) -> Result<(), PaperLedgerError> {
        let placement = self.placements.get_mut(placement_id).ok_or_else(|| {
            PaperLedgerError::Inconsistent {
                event_id: entry.event_id.clone(),
                message: format!("ack references unknown placement {placement_id}"),
            }
        })?;
        if placement.terminal.is_some() || placement.expected_order_id != order_id {
            return Err(PaperLedgerError::Inconsistent {
                event_id: entry.event_id.clone(),
                message: format!("ack does not match pending placement {placement_id}"),
            });
        }
        self.orders.insert(
            order_id.to_owned(),
            TrackedOrder {
                order: placement.order.clone(),
                state,
                submitted_ms: entry.timestamp_ms,
                active_at_ms,
            },
        );
        placement.terminal = Some(PlacementOutcome::Ack(state));
        Ok(())
    }

    fn apply_rejection(
        &mut self,
        entry: &PaperLedgerEntry,
        placement_id: &str,
        order_id: &str,
    ) -> Result<(), PaperLedgerError> {
        let placement = self.placements.get_mut(placement_id).ok_or_else(|| {
            PaperLedgerError::Inconsistent {
                event_id: entry.event_id.clone(),
                message: format!("rejection references unknown placement {placement_id}"),
            }
        })?;
        if placement.terminal.is_some() || placement.expected_order_id != order_id {
            return Err(PaperLedgerError::Inconsistent {
                event_id: entry.event_id.clone(),
                message: format!("rejection does not match pending placement {placement_id}"),
            });
        }
        placement.terminal = Some(PlacementOutcome::Rejected);
        Ok(())
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "fill records carry the complete money fact"
    )]
    fn apply_fill(
        &mut self,
        entry: &PaperLedgerEntry,
        order_id: &str,
        market: &MarketId,
        outcome: Outcome,
        price: Decimal,
        fill_size: Decimal,
        direction: Side,
        fee: Decimal,
    ) -> Result<(), PaperLedgerError> {
        let tracked =
            self.orders
                .get(order_id)
                .cloned()
                .ok_or_else(|| PaperLedgerError::Inconsistent {
                    event_id: entry.event_id.clone(),
                    message: format!("fill references unknown order {order_id}"),
                })?;
        if tracked.order.market != *market
            || tracked.order.outcome != outcome
            || tracked.order.side != direction
            || fill_size > tracked.order.qty
        {
            return Err(PaperLedgerError::Inconsistent {
                event_id: entry.event_id.clone(),
                message: format!("fill does not match order {order_id}"),
            });
        }
        self.account.apply_fill(LedgerFill {
            market: market.clone(),
            outcome,
            side: direction,
            price,
            quantity: fill_size,
            fee,
        })?;
        if tracked.state == OrderState::Resting && fill_size < tracked.order.qty {
            if let Some(open) = self.orders.get_mut(order_id) {
                open.order.qty -= fill_size;
            }
        } else {
            self.orders.remove(order_id);
        }
        self.fill_count += 1;
        Ok(())
    }

    fn apply_settlement(
        &mut self,
        entry: &PaperLedgerEntry,
        market: &MarketId,
        outcome: Outcome,
        settled_size: Decimal,
        proceeds: Decimal,
    ) -> Result<(), PaperLedgerError> {
        let held = self
            .account
            .positions()
            .iter()
            .find(|position| position.market == *market && position.outcome == outcome)
            .map_or(Decimal::ZERO, |position| position.quantity);
        if settled_size <= Decimal::ZERO || proceeds.is_sign_negative() || held != settled_size {
            return Err(PaperLedgerError::Inconsistent {
                event_id: entry.event_id.clone(),
                message: "settlement does not exactly consume the held position".into(),
            });
        }
        self.account.settle(&Settlement {
            market: market.clone(),
            outcome,
            payout_per_share: proceeds / settled_size,
        })?;
        Ok(())
    }
}

fn deduplicate(entries: &[PaperLedgerEntry]) -> Result<Vec<PaperLedgerEntry>, PaperLedgerError> {
    let mut seen = HashMap::new();
    let mut unique = Vec::new();
    for entry in entries {
        if let Some(previous) = seen.get(&entry.event_id) {
            if previous != entry {
                return Err(PaperLedgerError::ConflictingDuplicate {
                    event_id: entry.event_id.clone(),
                });
            }
        } else {
            seen.insert(entry.event_id.clone(), entry.clone());
            unique.push(entry.clone());
        }
    }
    Ok(unique)
}

const fn empty_book(timestamp_ms: i64) -> OrderBookL2 {
    OrderBookL2 {
        bids: Vec::new(),
        asks: Vec::new(),
        timestamp_ms,
        last_trade_price: None,
    }
}

fn close_replayed_order(
    engine: &mut SimEngine,
    order: &PlaceOrder,
    order_id: Option<&OrderId>,
    submitted_ms: i64,
    config: SimulationConfig,
) {
    if let Some(order_id) = order_id {
        engine.cancel(order_id);
    }
    if !order.post_only && config.activation_latency_ms > 0 {
        engine.update_book(
            &order.market,
            order.outcome,
            empty_book(submitted_ms.saturating_add(config.activation_latency_ms)),
        );
    }
}

const fn outcome_rank(outcome: Outcome) -> u8 {
    match outcome {
        Outcome::Up => 0,
        Outcome::Down => 1,
    }
}
