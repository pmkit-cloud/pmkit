use super::{PaperReport, StartError, absorb_fills, instantiate_strategies};
use pmkit_book::OrderBookL2;
use pmkit_event::MarketEvent;
use pmkit_exec::Executor;
use pmkit_market::Outcome;
use pmkit_paper::PaperExecutor;
use pmkit_sim::MarketCategory;
use pmkit_spec::PaperRun;
use pmkit_strategy::{Action, LogicalTimestamp, StrategyContext};
use std::collections::HashSet;

fn drain_fills(rx: &mut tokio::sync::mpsc::Receiver<MarketEvent>) -> Vec<MarketEvent> {
    let mut fills = Vec::new();
    while let Ok(event) = rx.try_recv() {
        fills.push(event);
    }
    fills
}

pub async fn drive(run: &PaperRun) -> Result<PaperReport, StartError> {
    let mut strategies = instantiate_strategies(run.strategies(), run.id())?;

    let (fill_tx, mut fill_rx) = tokio::sync::mpsc::channel(1024);
    let paper = PaperExecutor::new(fill_tx, "paper", MarketCategory::Crypto);

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1024);
    let mut subscribed = HashSet::new();
    for (market, _) in &strategies {
        if !subscribed.insert(market.clone()) {
            continue;
        }
        for outcome in [Outcome::Up, Outcome::Down] {
            let source = run.market_data().clone();
            let sink = event_tx.clone();
            let market = market.clone();
            tokio::spawn(async move { source.subscribe(market, outcome, sink).await });
        }
    }
    drop(event_tx);

    // ponytail: fee category fixed to Crypto; positions tracked; fill buffer bounded at 1024.
    let mut positions = Vec::new();
    let mut events_processed = 0_usize;
    let mut fills = 0_usize;

    while let Some(event) = event_rx.recv().await {
        events_processed += 1;
        if let MarketEvent::BookUpdate {
            market,
            outcome,
            bids,
            asks,
            timestamp_ms,
        } = &event
        {
            let book = OrderBookL2 {
                bids: bids.clone(),
                asks: asks.clone(),
                timestamp_ms: *timestamp_ms,
                last_trade_price: None,
            };
            let _ = paper.update_book(market, *outcome, book.clone()).await;
            fills += absorb_fills(&drain_fills(&mut fill_rx), &mut positions);
            for (registered_market, strategy) in &mut *strategies {
                if *registered_market != *market {
                    continue;
                }
                let context = StrategyContext {
                    market,
                    book: &book,
                    positions: &positions,
                    now: LogicalTimestamp::from_millis(*timestamp_ms),
                };
                if let Ok(actions) = strategy.on_event(context) {
                    for action in actions.as_slice() {
                        if let Action::Place(order) = action {
                            let _ = paper.submit(order, *timestamp_ms).await;
                        }
                    }
                }
                fills += absorb_fills(&drain_fills(&mut fill_rx), &mut positions);
            }
        }
    }
    fills += absorb_fills(&drain_fills(&mut fill_rx), &mut positions);

    Ok(PaperReport {
        run: run.id().clone(),
        events_processed,
        fills,
    })
}
