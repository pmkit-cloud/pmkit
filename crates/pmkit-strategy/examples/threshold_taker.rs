//! A minimal strategy showing the SDK's strategy boundary.

use pmkit_book::{OrderBookL2, Position};
use pmkit_core::MarketId;
use pmkit_event::{MarketEvent, StrategyFact};
use pmkit_exec::{PlaceOrder, TimeInForce};
use pmkit_market::Outcome;
use pmkit_strategy::{Action, Actions, LogicalTimestamp, Strategy, StrategyContext, StrategyError};
use rust_decimal::Decimal;

#[derive(Debug)]
struct ThresholdTaker {
    threshold: Decimal,
    submitted: bool,
}

impl Strategy for ThresholdTaker {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        let Some((ask, _)) = context.book.best_ask() else {
            return Ok(Actions::none());
        };
        if self.submitted || ask > self.threshold {
            return Ok(Actions::none());
        }
        self.submitted = true;
        Ok(Actions::place(PlaceOrder {
            market: context.market.clone(),
            outcome: Outcome::Up,
            side: pmkit_book::Side::Buy,
            price: ask,
            qty: Decimal::ONE,
            post_only: false,
            tif: TimeInForce::Gtc,
        }))
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let market = MarketId::new("example-market")?;
    let book = OrderBookL2 {
        asks: vec![(Decimal::new(45, 2), Decimal::ONE)],
        ..OrderBookL2::default()
    };
    let fact = StrategyFact::Market(MarketEvent::Tick { timestamp_ms: 1 });
    let mut strategy = ThresholdTaker {
        threshold: Decimal::new(50, 2),
        submitted: false,
    };
    let actions = strategy.on_event(StrategyContext {
        fact: &fact,
        market: &market,
        book: &book,
        positions: &[] as &[Position],
        now: LogicalTimestamp::from_millis(1),
    })?;
    assert!(matches!(actions.as_slice(), [Action::Place(_)]));
    Ok(())
}
