use super::{
    BacktestReport, StartError, StrategyInstance, absorb_fills, instantiate_strategies,
    store_signal,
};
use crate::causal::{
    ActionRiskVerdict, CausalRecorder, CexTradeMetrics, DecisionKind, DecisionSnapshot,
};
use crate::feed::{FeedMode, MergedFeed, SourceTaskDefinition};
use pmkit_book::OrderBookL2;
use pmkit_data::ReplayQuery;
use pmkit_event::{MarketEvent, SourceEnvelope, StrategyFact};
use pmkit_sim::{MarketCategory, SimEngine};
use pmkit_spec::BacktestRun;
use pmkit_store::{CausalIdentity, OwnerScope, TapeStore};
use pmkit_strategy::{Action, LogicalTimestamp, StrategyContext};
use rust_decimal::Decimal;

#[expect(
    clippy::too_many_lines,
    reason = "the backtest owns one ordered replay, sim, strategy, and recording loop"
)]
pub async fn drive(
    run: &BacktestRun,
    store: Option<&dyn TapeStore>,
) -> Result<BacktestReport, StartError> {
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
    let feed = MergedFeed::from_tasks(
        FeedMode::Backtest,
        vec![SourceTaskDefinition::new("pm", move |sink| async move {
            source.replay(query, sink).await
        })],
        Some(run.replay().to().timestamp_millis()),
    );
    let replay = tokio::spawn(async move { feed.forward(tx).await });

    // ponytail: fee category fixed to Crypto; positions tracked from fills.
    let mut sim = SimEngine::new("bt", 0, MarketCategory::Crypto);
    let mut positions = Vec::new();
    let mut events_processed = 0_usize;
    let mut fills = 0_usize;
    let scope = OwnerScope::new(run.portfolio().clone(), run.id().clone());

    while let Some(merged) = rx.recv().await {
        store_signal(
            store,
            &scope,
            &pmkit_data::SourceSignal::Data(Box::new(merged.source.clone())),
        )
        .await
        .map_err(|source| StartError::Storage {
            run: run.id().clone(),
            source,
        })?;
        let SourceEnvelope::PmMarket(envelope) = merged.source else {
            continue;
        };
        let event = envelope.fact;
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
            let (added, actions_placed) = run_strategies(
                &mut strategies,
                market,
                *outcome,
                &book,
                &mut positions,
                *timestamp_ms,
                &mut sim,
            );
            fills += added;
            if let Some(store) = store {
                let identity = CausalIdentity {
                    scope: scope.clone(),
                    correlation_id: format!("{market:?}:{timestamp_ms}"),
                    source_timestamp_ms: envelope.metadata.source_time_ms,
                    ingest_sequence: i64::try_from(envelope.metadata.ingest_sequence)
                        .unwrap_or(i64::MAX),
                };
                record_decision(store, &identity, &book, actions_placed)
                    .await
                    .map_err(|source| StartError::Storage {
                        run: run.id().clone(),
                        source,
                    })?;
            }
        }
    }

    replay
        .await
        .map_err(|error| StartError::Source {
            run: run.id().clone(),
            source: pmkit_data::DataSourceError::ReplayGap {
                message: format!("merged feed task failed: {error}"),
            },
        })?
        .map_err(|source| StartError::Source {
            run: run.id().clone(),
            source,
        })?;
    Ok(BacktestReport {
        run: run.id().clone(),
        events_processed,
        fills,
    })
}

fn run_strategies(
    strategies: &mut [StrategyInstance],
    market: &pmkit_core::MarketId,
    outcome: pmkit_market::Outcome,
    book: &OrderBookL2,
    positions: &mut Vec<pmkit_book::Position>,
    timestamp_ms: i64,
    sim: &mut SimEngine,
) -> (usize, u32) {
    let mut fills = 0;
    let mut actions_placed = 0_u32;
    for (registered_market, strategy) in &mut *strategies {
        if *registered_market != *market {
            continue;
        }
        let context = StrategyContext {
            fact: &StrategyFact::Market(MarketEvent::BookUpdate {
                market: market.clone(),
                outcome,
                bids: book.bids.clone(),
                asks: book.asks.clone(),
                timestamp_ms,
            }),
            market,
            book,
            positions: positions.as_slice(),
            now: LogicalTimestamp::from_millis(timestamp_ms),
        };
        if let Ok(actions) = strategy.on_event(context) {
            for action in actions.as_slice() {
                if let Action::Place(order) = action {
                    sim.submit(order, timestamp_ms);
                    actions_placed = actions_placed.saturating_add(1);
                }
            }
        }
        fills += absorb_fills(&sim.drain_fills(), positions);
    }
    (fills, actions_placed)
}

async fn record_decision(
    store: &dyn TapeStore,
    identity: &CausalIdentity,
    book: &OrderBookL2,
    actions_placed: u32,
) -> Result<(), pmkit_store::StoreError> {
    let snapshot = DecisionSnapshot::from_book(
        book,
        CexTradeMetrics {
            last_price: None,
            momentum: Decimal::ZERO,
            volume: Decimal::ZERO,
            cvd: Decimal::ZERO,
            vwap: None,
        },
    );
    let decision = if actions_placed == 0 {
        DecisionKind::NoAction
    } else {
        DecisionKind::Actions(
            (0..actions_placed)
                .map(ActionRiskVerdict::accepted)
                .collect(),
        )
    };
    CausalRecorder::new(store)
        .record_evaluation(identity, &snapshot, decision)
        .await
}
