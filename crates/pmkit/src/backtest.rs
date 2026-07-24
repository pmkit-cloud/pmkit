use super::{
    BacktestReport, RunControl, RunLifecycleEvent, StartError, StrategyInstance, absorb_fills,
    instantiate_strategies, store_signal,
};
use crate::feed::{FeedMode, MergedFeed, SourceTaskDefinition};
use pmkit_book::OrderBookL2;
use pmkit_data::ReplayQuery;
use pmkit_event::{MarketEvent, SourceEnvelope, StrategyFact};
use pmkit_sim::{MarketCategory, SimEngine, SimulationConfig};
use pmkit_spec::BacktestRun;
use pmkit_store::{CausalIdentity, OwnerScope, TapeStore};
use pmkit_strategy::{Action, LogicalTimestamp, StrategyContext};

#[expect(
    clippy::too_many_lines,
    reason = "the backtest owns one ordered replay, sim, strategy, and recording loop"
)]
pub async fn drive_with_control(
    run: &BacktestRun,
    store: Option<&dyn TapeStore>,
    control: &RunControl,
) -> Result<BacktestReport, StartError> {
    let mut strategies = instantiate_strategies(run.strategies(), run.id())?;

    let markets = strategies
        .iter()
        .map(|instance| instance.market.clone())
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
    let mut sources = vec![SourceTaskDefinition::new("pm", move |sink| async move {
        source.replay(query, sink).await
    })];
    if let Some(reference) = run.replay().reference_source_ref() {
        let reference = reference.clone();
        let reference_query = ReplayQuery {
            markets: strategies
                .iter()
                .map(|instance| instance.market.clone())
                .collect(),
            from: run.replay().from(),
            to: run.replay().to(),
            evidence: run.replay().evidence(),
            retrieval_wait: run.replay().retrieval_wait(),
        };
        sources.push(SourceTaskDefinition::new("cex", move |sink| async move {
            reference.replay(reference_query, sink).await
        }));
    }
    let feed = MergedFeed::from_tasks(
        FeedMode::Backtest,
        sources,
        Some(run.replay().to().timestamp_millis()),
    );
    let replay = tokio::spawn(async move { feed.forward(tx).await });

    // ponytail: fee category fixed to Crypto; positions tracked from fills.
    let simulation = run.simulation();
    let simulation_config = SimulationConfig {
        activation_latency_ms: i64::try_from(simulation.activation_latency.as_millis())
            .unwrap_or(i64::MAX),
        maker_queue_ahead_bps: simulation.maker_queue_ahead_bps,
        slippage_bps: simulation.slippage_bps,
        market_impact_bps: simulation.market_impact_bps,
    };
    let mut sim = SimEngine::with_config("bt", 0, MarketCategory::Crypto, simulation_config);
    let mut positions = Vec::new();
    let mut events_processed = 0_usize;
    let mut fills = 0_usize;
    let mut cex_metrics = crate::causal::CexTradeMetricsState::default();
    let scope = OwnerScope::new(run.portfolio().clone(), run.id().clone());
    control.emit(RunLifecycleEvent::Started {
        run: run.id().clone(),
    });
    if control.is_cancelled() {
        control.emit(RunLifecycleEvent::Cancelled {
            run: run.id().clone(),
        });
        return Ok(BacktestReport {
            run: run.id().clone(),
            events_processed,
            fills,
        });
    }

    while let Some(merged) = rx.recv().await {
        if control.is_cancelled() {
            control.emit(RunLifecycleEvent::Cancelled {
                run: run.id().clone(),
            });
            return Ok(BacktestReport {
                run: run.id().clone(),
                events_processed,
                fills,
            });
        }
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
        if let SourceEnvelope::CexReference(envelope) = &merged.source {
            cex_metrics.observe(&envelope.fact);
            continue;
        }
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
                crate::causal::record_book_decision(
                    store,
                    &identity,
                    &book,
                    cex_metrics.snapshot(),
                    actions_placed,
                    Some(simulation_config),
                )
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
    control.emit(RunLifecycleEvent::Completed {
        run: run.id().clone(),
    });
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
    for instance in &mut *strategies {
        if instance.market != *market {
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
        if let Ok(actions) = instance.strategy.on_event(context) {
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
