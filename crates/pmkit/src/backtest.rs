use super::{
    BacktestReport, RunControl, RunLifecycleEvent, StartError, StrategyInstance,
    instantiate_strategies, store_signal,
};
use crate::feed::{FeedMode, MergedFeed, SourceTaskDefinition};
use pmkit_accounting::{PortfolioExposure, PositionExposure, aggregate_exposure};
use pmkit_book::OrderBookL2;
use pmkit_core::MarketId;
use pmkit_data::ReplayQuery;
use pmkit_event::{MarketEvent, SourceEnvelope, StrategyFact};
use pmkit_market::Outcome;
use pmkit_sim::{SimEngine, SimulationConfig};
use pmkit_spec::BacktestRun;
use pmkit_store::{CausalIdentity, OwnerScope, TapeStore};
use pmkit_strategy::{Action, LogicalTimestamp, StrategyContext};
use rust_decimal::Decimal;
use std::collections::HashMap;

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
        fee_model: Some(simulation.resolved_fee_model()),
    };
    let mut sim = SimEngine::with_fee_config("bt", 0, simulation_config);
    let mut positions_by_market: HashMap<MarketId, Vec<pmkit_book::Position>> = HashMap::new();
    let mut marks: HashMap<(MarketId, Outcome), Decimal> = HashMap::new();
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
            exposure: report_exposure(&positions_by_market, &marks),
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
                exposure: report_exposure(&positions_by_market, &marks),
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
            if let Some(mark) = book.mid_price() {
                marks.insert((market.clone(), *outcome), mark);
            }
            sim.update_book(market, *outcome, book.clone());
            let drained = sim.drain_fills();
            fills += absorb_market_fills(&drained, &mut positions_by_market);
            let (added, actions_placed) = run_strategies(&mut RunStrategiesInputs {
                strategies: &mut strategies,
                market,
                outcome: *outcome,
                book: &book,
                positions_by_market: &mut positions_by_market,
                timestamp_ms: *timestamp_ms,
                sim: &mut sim,
            });
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
        exposure: report_exposure(&positions_by_market, &marks),
    })
}

fn absorb_market_fills(
    fills: &[MarketEvent],
    positions_by_market: &mut HashMap<MarketId, Vec<pmkit_book::Position>>,
) -> usize {
    for event in fills {
        if let MarketEvent::Fill {
            market,
            outcome,
            side,
            price,
            size,
            ..
        } = event
        {
            let positions = positions_by_market.entry(market.clone()).or_default();
            pmkit_book::book::apply_fill(positions, *outcome, *side, *price, *size);
        }
    }
    fills.len()
}

fn report_exposure(
    positions_by_market: &HashMap<MarketId, Vec<pmkit_book::Position>>,
    marks: &HashMap<(MarketId, Outcome), Decimal>,
) -> PortfolioExposure {
    let mut position_notionals = Vec::new();
    for (market, positions) in positions_by_market {
        let notional = positions
            .iter()
            .map(|position| {
                marks
                    .get(&(market.clone(), position.outcome))
                    .map_or(Decimal::ZERO, |mark| position.qty.abs() * *mark)
            })
            .sum();
        position_notionals.push(PositionExposure {
            market: market.clone(),
            notional,
        });
    }
    aggregate_exposure(&position_notionals, &[])
}

struct RunStrategiesInputs<'a> {
    strategies: &'a mut [StrategyInstance],
    market: &'a pmkit_core::MarketId,
    outcome: pmkit_market::Outcome,
    book: &'a OrderBookL2,
    positions_by_market: &'a mut HashMap<MarketId, Vec<pmkit_book::Position>>,
    timestamp_ms: i64,
    sim: &'a mut SimEngine,
}

fn run_strategies(inputs: &mut RunStrategiesInputs<'_>) -> (usize, u32) {
    let mut fills = 0;
    let mut actions_placed = 0_u32;
    for instance in inputs.strategies.iter_mut() {
        if instance.market != *inputs.market {
            continue;
        }
        let positions = inputs
            .positions_by_market
            .get(inputs.market)
            .map_or(&[] as &[pmkit_book::Position], Vec::as_slice);
        let context = StrategyContext {
            fact: &StrategyFact::Market(MarketEvent::BookUpdate {
                market: inputs.market.clone(),
                outcome: inputs.outcome,
                bids: inputs.book.bids.clone(),
                asks: inputs.book.asks.clone(),
                timestamp_ms: inputs.timestamp_ms,
            }),
            market: inputs.market,
            book: inputs.book,
            positions,
            now: LogicalTimestamp::from_millis(inputs.timestamp_ms),
        };
        if let Ok(actions) = instance.strategy.on_event(context) {
            for action in actions.as_slice() {
                if let Action::Place(order) = action {
                    inputs.sim.submit(order, inputs.timestamp_ms);
                    actions_placed = actions_placed.saturating_add(1);
                }
            }
        }
        let drained = inputs.sim.drain_fills();
        fills += absorb_market_fills(&drained, inputs.positions_by_market);
    }
    (fills, actions_placed)
}

#[cfg(test)]
mod tests {
    use crate::{
        Pmkit, RunReport, StartError,
        test_support::{config, risk},
    };
    use async_trait::async_trait;
    use pmkit_core::{MarketId, PortfolioId, RunId};
    use pmkit_data::{DataSourceError, HistoricalDataSource, SourceSignal};
    use pmkit_event::MarketEvent;
    use pmkit_market::Outcome;
    use pmkit_money::Money;
    use pmkit_run::{EvidenceRequirement, RetrievalWait};
    use pmkit_spec::{BacktestRun, ConservativeV1Config, ReplaySpec};
    use rust_decimal::Decimal;
    use std::{num::NonZeroUsize, sync::Arc, time::Duration};
    use tokio::sync::{Barrier, mpsc::Sender};

    struct RendezvousHistory {
        barrier: Option<Arc<Barrier>>,
    }

    #[async_trait]
    impl HistoricalDataSource for RendezvousHistory {
        async fn replay(
            &self,
            _query: crate::ReplayQuery,
            sink: Sender<SourceSignal>,
        ) -> Result<(), DataSourceError> {
            if let Some(barrier) = &self.barrier {
                barrier.wait().await;
            }
            sink.send(SourceSignal::market_event(MarketEvent::BookUpdate {
                market: MarketId::new("btc-5m").map_err(|_| DataSourceError::NotAvailable)?,
                outcome: Outcome::Up,
                bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
                asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
                timestamp_ms: 1,
            }))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
            sink.send(SourceSignal::Watermark(i64::MAX))
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            sink.send(SourceSignal::Eof)
                .await
                .map_err(|_| DataSourceError::SinkClosed)
        }
    }

    fn run(
        id: &str,
        barrier: Option<Arc<Barrier>>,
    ) -> Result<BacktestRun, Box<dyn std::error::Error>> {
        let replay = ReplaySpec::new(
            Arc::new(RendezvousHistory { barrier }),
            "2026-01-01T00:00:00Z".parse()?,
            "2026-02-01T00:00:00Z".parse()?,
            EvidenceRequirement::CorroboratedOnly,
            RetrievalWait::ReturnPending,
        );
        Ok(BacktestRun::new(
            RunId::new(id)?,
            PortfolioId::new("research")?,
            replay,
            Money::usdc(1_000),
            risk()?,
            ConservativeV1Config {
                activation_latency: Duration::ZERO,
                maker_queue_ahead_bps: 0,
                slippage_bps: 0,
                market_impact_bps: 0,
                fee_model: None,
            },
        ))
    }

    #[tokio::test]
    async fn concurrent_equals_sequential() -> Result<(), Box<dyn std::error::Error>> {
        // Given: identical two-run topologies with sequential and parallel limits.
        let sequential = Pmkit::builder(config()?)
            .run(run("a", None)?)
            .run(run("b", None)?)
            .start()
            .await?;
        let mut parallel_config = config()?;
        parallel_config.backtest_concurrency = NonZeroUsize::new(2).ok_or("nonzero concurrency")?;
        let barrier = Arc::new(Barrier::new(2));

        // When: both parallel sources must rendezvous before either can finish.
        let parallel = tokio::time::timeout(
            Duration::from_secs(1),
            Pmkit::builder(parallel_config)
                .run(run("a", Some(Arc::clone(&barrier)))?)
                .run(run("b", Some(barrier))?)
                .start(),
        )
        .await??;

        // Then: every report is identical regardless of scheduling concurrency.
        for id in [RunId::new("a")?, RunId::new("b")?] {
            let (
                Some(RunReport::Backtest(sequential_report)),
                Some(RunReport::Backtest(parallel_report)),
            ) = (sequential.report(&id), parallel.report(&id))
            else {
                return Err("expected matching backtest reports".into());
            };
            assert_eq!(sequential_report.run, parallel_report.run);
            assert_eq!(
                sequential_report.events_processed,
                parallel_report.events_processed
            );
            assert_eq!(sequential_report.fills, parallel_report.fills);
        }
        Ok(())
    }

    #[tokio::test]
    async fn duplicate_run_id_rejected() -> Result<(), Box<dyn std::error::Error>> {
        // Given: two independent backtests sharing one run identity.
        let duplicate = RunId::new("duplicate")?;

        // When: the topology is validated before scheduling work.
        let result = Pmkit::builder(config()?)
            .run(run("duplicate", None)?)
            .run(run("duplicate", None)?)
            .start()
            .await;

        // Then: duplicate detection remains fail-fast.
        assert!(matches!(
            result,
            Err(StartError::DuplicateRunId(run)) if run == duplicate
        ));
        Ok(())
    }

    #[tokio::test]
    async fn reports_are_exposed_in_submission_order() -> Result<(), Box<dyn std::error::Error>> {
        // Given: run submission order that differs from lexical order.
        let app = Pmkit::builder(config()?)
            .run(run("zeta", None)?)
            .run(run("alpha", None)?)
            .start()
            .await?;

        // When: reports are listed through the ordered public surface.
        let reports = app.reports_ordered();

        // Then: task completion and hash iteration cannot change submission order.
        let run_ids: Vec<_> = reports
            .into_iter()
            .map(|(run, _report)| run.to_string())
            .collect();
        assert_eq!(run_ids, ["zeta", "alpha"]);
        Ok(())
    }
}
