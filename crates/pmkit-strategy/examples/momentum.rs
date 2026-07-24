//! A momentum taker driven by CEX reference trades.
//!
//! It keeps a short window of reference trade prices and, when the latest price
//! rises above the window average (upward momentum), takes the PM book's best
//! ask once. Reference facts drive the signal; the PM book sets the execution
//! price. Run with `cargo run -p pmkit-strategy --example momentum`.

use pmkit_book::{OrderBookL2, Position, Side};
use pmkit_core::MarketId;
use pmkit_event::{CexReferenceEvent, StrategyFact};
use pmkit_exec::PlaceOrder;
use pmkit_market::{Asset, Exchange, Outcome};
use pmkit_strategy::{Action, Actions, LogicalTimestamp, Strategy, StrategyContext, StrategyError};
use rust_decimal::Decimal;

#[derive(Debug)]
struct Momentum {
    window: usize,
    prices: Vec<Decimal>,
    size: Decimal,
    positioned: bool,
}

impl Strategy for Momentum {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        let StrategyFact::Reference(CexReferenceEvent::Trade { price, .. }) = context.fact else {
            return Ok(Actions::none());
        };

        self.prices.push(*price);
        if self.prices.len() > self.window {
            self.prices.remove(0);
        }
        if self.positioned || self.prices.len() < self.window {
            return Ok(Actions::none());
        }

        let count = Decimal::from(u64::try_from(self.prices.len()).unwrap_or(u64::MAX));
        let average = self.prices.iter().copied().sum::<Decimal>() / count;
        if *price <= average {
            return Ok(Actions::none());
        }

        let Some((ask, _)) = context.book.best_ask() else {
            return Ok(Actions::none());
        };
        self.positioned = true;
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

fn reference_trade(price: i64, timestamp_ms: i64) -> StrategyFact {
    StrategyFact::Reference(CexReferenceEvent::Trade {
        asset: Asset::Btc,
        exchange: Exchange::Binance,
        aggregate_trade_id: u64::try_from(timestamp_ms).unwrap_or(0),
        price: Decimal::new(price, 0),
        qty: Decimal::ONE,
        is_buyer_maker: false,
        timestamp_ms,
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let market = MarketId::new("btc-5m")?;
    let book = OrderBookL2 {
        bids: vec![(Decimal::new(48, 2), Decimal::ONE)],
        asks: vec![(Decimal::new(50, 2), Decimal::ONE)],
        ..OrderBookL2::default()
    };
    let mut strategy = Momentum {
        window: 3,
        prices: Vec::new(),
        size: Decimal::ONE,
        positioned: false,
    };

    // Rising reference prices: only the third fills the window and triggers.
    let mut fired = Vec::new();
    for (index, price) in [100_i64, 101, 103].into_iter().enumerate() {
        let ms = i64::try_from(index).unwrap_or(0) + 1;
        let fact = reference_trade(price, ms);
        let actions = strategy.on_event(StrategyContext {
            fact: &fact,
            market: &market,
            book: &book,
            positions: &[] as &[Position],
            now: LogicalTimestamp::from_millis(ms),
        })?;
        fired.push(!actions.is_empty());
        if let [Action::Place(order)] = actions.as_slice() {
            assert_eq!(order.side, Side::Buy);
            assert_eq!(order.price, Decimal::new(50, 2));
        }
    }

    assert_eq!(fired, vec![false, false, true]);
    println!("momentum took the ask 0.50 after upward reference momentum");
    Ok(())
}
