//! A two-sided market maker that requotes around the book mid on each update.
//!
//! On every book update it cancels its resting quotes and posts a fresh bid
//! below mid and ask above mid by a fixed half-spread, both post-only. This is
//! the canonical maker refresh pattern; run it with
//! `cargo run -p pmkit-strategy --example market_maker`.

use pmkit_book::{OrderBookL2, Position, Side};
use pmkit_core::MarketId;
use pmkit_event::{MarketEvent, StrategyFact};
use pmkit_exec::PlaceOrder;
use pmkit_market::Outcome;
use pmkit_strategy::{Action, Actions, LogicalTimestamp, Strategy, StrategyContext, StrategyError};
use rust_decimal::Decimal;

#[derive(Debug)]
struct MarketMaker {
    half_spread: Decimal,
    size: Decimal,
}

impl Strategy for MarketMaker {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        let Some(mid) = context.book.mid_price() else {
            return Ok(Actions::none());
        };
        let bid_price = mid - self.half_spread;
        let ask_price = mid + self.half_spread;
        if bid_price <= Decimal::ZERO || ask_price >= Decimal::ONE {
            return Ok(Actions::none());
        }

        let mut actions = Actions::none();
        // Requote: drop stale quotes, then repost both sides.
        actions.push(Action::CancelAll);
        actions.push(Action::Place(PlaceOrder {
            market: context.market.clone(),
            outcome: Outcome::Up,
            side: Side::Buy,
            price: bid_price,
            qty: self.size,
            post_only: true,
        }));
        actions.push(Action::Place(PlaceOrder {
            market: context.market.clone(),
            outcome: Outcome::Up,
            side: Side::Sell,
            price: ask_price,
            qty: self.size,
            post_only: true,
        }));
        Ok(actions)
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
    let mut strategy = MarketMaker {
        half_spread: Decimal::new(1, 2),
        size: Decimal::ONE,
    };

    let actions = strategy.on_event(StrategyContext {
        fact: &fact,
        market: &market,
        book: &book,
        positions: &[] as &[Position],
        now: LogicalTimestamp::from_millis(1),
    })?;

    // mid = 0.50, half-spread 0.01 -> bid 0.49, ask 0.51, both post-only.
    match actions.as_slice() {
        [Action::CancelAll, Action::Place(bid), Action::Place(ask)] => {
            assert_eq!(bid.side, Side::Buy);
            assert_eq!(bid.price, Decimal::new(49, 2));
            assert!(bid.post_only);
            assert_eq!(ask.side, Side::Sell);
            assert_eq!(ask.price, Decimal::new(51, 2));
            assert!(ask.post_only);
        }
        other => return Err(format!("unexpected actions: {other:?}").into()),
    }

    println!("market_maker requoted bid 0.49 / ask 0.51 around mid 0.50");
    Ok(())
}
