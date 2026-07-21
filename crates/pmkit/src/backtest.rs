use super::{BacktestReport, StartError, StrategyInstance, absorb_fills, instantiate_strategies};
use pmkit_book::OrderBookL2;
use pmkit_data::ReplayQuery;
use pmkit_event::MarketEvent;
use pmkit_sim::{MarketCategory, SimEngine};
use pmkit_spec::BacktestRun;
use pmkit_strategy::{Action, LogicalTimestamp, StrategyContext};

pub async fn drive(run: &BacktestRun) -> Result<BacktestReport, StartError> {
    let mut strategies = instantiate_strategies(run.strategies(), run.id())?;

    let markets = strategies
        .iter()
        .map(|(market, _)| market.clone())
        .collect();
    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
    let source = run.replay().source().clone();
    let query = ReplayQuery {
        markets,
        from: run.replay().from(),
        to: run.replay().to(),
        evidence: run.replay().evidence(),
        retrieval_wait: run.replay().retrieval_wait(),
    };
    let replay = tokio::spawn(async move { source.replay(query, tx).await });

    // ponytail: fee category fixed to Crypto; positions tracked from fills.
    let mut sim = SimEngine::new("bt", 0, MarketCategory::Crypto);
    let mut positions = Vec::new();
    let mut events_processed = 0_usize;
    let mut fills = 0_usize;

    while let Some(event) = rx.recv().await {
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
            sim.update_book(market, *outcome, book.clone());
            fills += absorb_fills(&sim.drain_fills(), &mut positions);
            fills += run_strategies(
                &mut strategies,
                market,
                &book,
                &mut positions,
                *timestamp_ms,
                &mut sim,
            );
        }
    }

    let _ = replay.await;
    Ok(BacktestReport {
        run: run.id().clone(),
        events_processed,
        fills,
    })
}

fn run_strategies(
    strategies: &mut [StrategyInstance],
    market: &pmkit_core::MarketId,
    book: &OrderBookL2,
    positions: &mut Vec<pmkit_book::Position>,
    timestamp_ms: i64,
    sim: &mut SimEngine,
) -> usize {
    let mut fills = 0;
    for (registered_market, strategy) in &mut *strategies {
        if *registered_market != *market {
            continue;
        }
        let context = StrategyContext {
            market,
            book,
            positions: positions.as_slice(),
            now: LogicalTimestamp::from_millis(timestamp_ms),
        };
        if let Ok(actions) = strategy.on_event(context) {
            for action in actions.as_slice() {
                if let Action::Place(order) = action {
                    sim.submit(order, timestamp_ms);
                }
            }
        }
        fills += absorb_fills(&sim.drain_fills(), positions);
    }
    fills
}
