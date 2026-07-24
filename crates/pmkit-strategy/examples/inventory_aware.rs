//! An inventory-aware quoter that skews with its current position.
//!
//! Below the inventory cap it buys the cheap ask to build a position; at or
//! above the cap it stops buying and instead sells into the bid to reduce.
//! Run with `cargo run -p pmkit-strategy --example inventory_aware`.

use pmkit_book::{OrderBookL2, Position, Side};
use pmkit_core::MarketId;
use pmkit_event::{MarketEvent, StrategyFact};
use pmkit_exec::PlaceOrder;
use pmkit_market::Outcome;
use pmkit_strategy::{Action, Actions, LogicalTimestamp, Strategy, StrategyContext, StrategyError};
use rust_decimal::Decimal;

#[derive(Debug)]
struct InventoryAware {
    max_inventory: Decimal,
    size: Decimal,
}

impl InventoryAware {
    fn net_up(positions: &[Position]) -> Decimal {
        positions
            .iter()
            .filter(|position| position.outcome == Outcome::Up)
            .map(|position| position.qty)
            .sum()
    }
}

impl Strategy for InventoryAware {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        let inventory = Self::net_up(context.positions);
        if inventory >= self.max_inventory {
            // At the cap: reduce by selling into the bid.
            let Some((bid, _)) = context.book.best_bid() else {
                return Ok(Actions::none());
            };
            return Ok(Actions::place(PlaceOrder {
                market: context.market.clone(),
                outcome: Outcome::Up,
                side: Side::Sell,
                price: bid,
                qty: self.size,
                post_only: false,
            }));
        }

        // Below the cap: build by taking the ask.
        let Some((ask, _)) = context.book.best_ask() else {
            return Ok(Actions::none());
        };
        Ok(Actions::place(PlaceOrder {
            market: context.market.clone(),
            outcome: Outcome::Up,
            side: Side::Buy,
            price: ask,
            qty: self.size,
            post_only: false,
        }))
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let market = MarketId::new("btc-5m")?;
    let book = OrderBookL2 {
        bids: vec![(Decimal::new(48, 2), Decimal::ONE)],
        asks: vec![(Decimal::new(52, 2), Decimal::ONE)],
        ..OrderBookL2::default()
    };
    let fact = StrategyFact::Market(MarketEvent::Tick { timestamp_ms: 1 });
    let mut strategy = InventoryAware {
        max_inventory: Decimal::from(5),
        size: Decimal::ONE,
    };

    // Flat: build a position by taking the ask at 0.52.
    let flat = strategy.on_event(StrategyContext {
        fact: &fact,
        market: &market,
        book: &book,
        positions: &[] as &[Position],
        now: LogicalTimestamp::from_millis(1),
    })?;
    match flat.as_slice() {
        [Action::Place(order)] => {
            assert_eq!(order.side, Side::Buy);
            assert_eq!(order.price, Decimal::new(52, 2));
        }
        other => return Err(format!("expected a buy while flat: {other:?}").into()),
    }

    // At the cap (10 >= 5): reduce by selling into the bid at 0.48.
    let long = [Position {
        outcome: Outcome::Up,
        qty: Decimal::from(10),
        avg_entry: Decimal::new(50, 2),
        unrealized_pnl: Decimal::ZERO,
    }];
    let capped = strategy.on_event(StrategyContext {
        fact: &fact,
        market: &market,
        book: &book,
        positions: &long,
        now: LogicalTimestamp::from_millis(2),
    })?;
    match capped.as_slice() {
        [Action::Place(order)] => {
            assert_eq!(order.side, Side::Sell);
            assert_eq!(order.price, Decimal::new(48, 2));
        }
        other => return Err(format!("expected a sell at the cap: {other:?}").into()),
    }

    println!("inventory_aware bought while flat and sold to reduce at the cap");
    Ok(())
}
