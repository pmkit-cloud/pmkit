use pmkit_book::Side;
use pmkit_core::MarketId;
use pmkit_event::{FillIdentity, Liquidity};
use pmkit_exec::PlaceOrder;
use pmkit_market::{Asset, Exchange, Outcome};
use pmkit_strategy::{Actions, Strategy, StrategyContext, StrategyError};
use rust_decimal::Decimal;

use super::{
    Harness, account_fill, assert_cancels_all, assert_no_actions, assert_placed, book,
    fact_timestamp, last_trade, placed_orders, position, reference_trade, tick,
};

/// A sample strategy: take the best ask once the book has one.
struct AskTaker;

impl Strategy for AskTaker {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        let Some((ask, _)) = context.book.best_ask() else {
            return Ok(Actions::none());
        };
        Ok(Actions::place(PlaceOrder {
            market: context.market.clone(),
            outcome: Outcome::Up,
            side: Side::Buy,
            price: ask,
            qty: Decimal::ONE,
            post_only: false,
        }))
    }
}

#[test]
fn book_builder_exposes_top_of_book() {
    let built = book(
        &[(Decimal::new(48, 2), Decimal::ONE)],
        &[(Decimal::new(52, 2), Decimal::TWO)],
    );
    assert_eq!(built.best_bid(), Some((Decimal::new(48, 2), Decimal::ONE)));
    assert_eq!(built.best_ask(), Some((Decimal::new(52, 2), Decimal::TWO)));
    assert_eq!(built.mid_price(), Some(Decimal::new(50, 2)));
}

#[test]
fn fact_timestamp_reads_every_fact_kind() -> Result<(), Box<dyn std::error::Error>> {
    let market = MarketId::new("btc-5m")?;
    assert_eq!(fact_timestamp(&tick(7)), 7);
    assert_eq!(
        fact_timestamp(&last_trade(
            &market,
            Outcome::Up,
            Decimal::new(50, 2),
            Side::Buy,
            Decimal::ONE,
            11
        )),
        11
    );
    assert_eq!(
        fact_timestamp(&reference_trade(
            Asset::Btc,
            Exchange::Binance,
            1,
            Decimal::new(100, 0),
            Decimal::ONE,
            false,
            13
        )),
        13
    );
    assert_eq!(
        fact_timestamp(&account_fill(
            FillIdentity::Venue("fill-1".into()),
            "order-1",
            &market,
            Outcome::Up,
            Decimal::new(50, 2),
            Decimal::ONE,
            Side::Buy,
            Decimal::ZERO,
            Liquidity::Taker,
            17
        )),
        17
    );
    Ok(())
}

#[test]
fn harness_drives_strategy_with_book_and_reports_actions() -> Result<(), Box<dyn std::error::Error>>
{
    let harness = Harness::new(MarketId::new("btc-5m")?)
        .with_book(book(&[], &[(Decimal::new(52, 2), Decimal::ONE)]));
    let mut strategy = AskTaker;

    let actions = harness.feed(&mut strategy, &tick(1))?;
    assert_placed(&actions, Side::Buy, Outcome::Up, Decimal::new(52, 2));
    assert_eq!(placed_orders(&actions).len(), 1);
    Ok(())
}

#[test]
fn harness_with_empty_book_yields_no_actions() -> Result<(), Box<dyn std::error::Error>> {
    let harness = Harness::new(MarketId::new("btc-5m")?);
    let mut strategy = AskTaker;

    let actions = harness.feed(&mut strategy, &tick(1))?;
    assert_no_actions(&actions);
    Ok(())
}

/// Records how many positions the harness passed to the strategy.
struct PositionProbe<'a>(&'a mut i32);

impl Strategy for PositionProbe<'_> {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        *self.0 = i32::try_from(context.positions.len()).unwrap_or(-1);
        Ok(Actions::none())
    }
}

#[test]
fn positions_flow_through_the_harness() -> Result<(), Box<dyn std::error::Error>> {
    let harness = Harness::new(MarketId::new("btc-5m")?).with_positions(vec![position(
        Outcome::Up,
        Decimal::from(3),
        Decimal::new(50, 2),
    )]);
    let mut captured = 0;
    // Drive the probe through the harness.
    let mut probe = PositionProbe(&mut captured);
    harness.feed(&mut probe, &tick(1))?;
    assert_eq!(captured, 1);
    Ok(())
}

#[test]
fn assert_cancels_all_matches_cancel_all() {
    assert_cancels_all(&Actions::cancel_all());
}
