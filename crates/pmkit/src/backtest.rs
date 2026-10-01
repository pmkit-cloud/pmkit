use super::{
    BacktestReport, RunControl, RunLifecycleEvent, StartError, StrategyInstance,
    instantiate_strategies, observe_reconnect, store_signal,
};
use crate::capture_replay::CaptureReplaySource;
use crate::feed::{FeedError, FeedMode, MergedFeed, SourceTaskDefinition};
use pmkit_accounting::{
    ExposureReservation, PortfolioExposure, PositionExposure, aggregate_exposure,
};
use pmkit_book::OrderBookL2;
use pmkit_core::{MarketId, RunId};
use pmkit_data::{ReplayQuery, SourceSignal};
use pmkit_event::{CexReferenceEvent, MarketEvent, SourceEnvelope, StrategyFact};
use pmkit_market::{Asset, Outcome};
use pmkit_sim::{SimEngine, SimulationConfig};
use pmkit_spec::BacktestRun;
use pmkit_store::{OwnerScope, TapeStore};
use pmkit_strategy::{Action, LogicalTimestamp, StrategyContext};
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::io::BufRead;

#[expect(
    clippy::too_many_lines,
    reason = "the backtest owns one ordered replay, sim, strategy, and recording loop"
)]
pub async fn drive_with_control(
    run: &BacktestRun,
    store: Option<&dyn TapeStore>,
    control: &RunControl,
) -> Result<BacktestReport, StartError> {
    let strategies = instantiate_strategies(run.strategies(), run.id())?;
    let metrics = control.metrics_for(run.id());

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
    let primary_query = query.clone();
    for requirement in run.settlement_oracle_requirements() {
        let binding = run.settlement_oracle_bindings().iter().any(|binding| {
            binding.name() == requirement.name()
                && binding.market() == requirement.market()
                && !binding.asset().trim().is_empty()
        });
        let historical_source = run
            .replay()
            .reference_source_refs()
            .iter()
            .any(|(name, _)| name == requirement.name());
        if !binding || !historical_source {
            return Err(StartError::Source {
                run: run.id().clone(),
                source: pmkit_data::DataSourceError::Unavailable {
                    message: format!(
                        "required settlement oracle `{}` for market `{}` needs a matching asset binding and named historical replay source",
                        requirement.name(),
                        requirement.market(),
                    ),
                },
            });
        }
    }
    let mut sources = vec![SourceTaskDefinition::new("pm", move |sink| async move {
        source.replay(primary_query, sink).await
    })];
    for (name, reference) in run.replay().reference_source_refs() {
        let name = name.clone();
        let reference = reference.clone();
        let reference_query = query.clone();
        sources.push(SourceTaskDefinition::new(name, move |sink| async move {
            reference.replay(reference_query, sink).await
        }));
    }
    let feed = MergedFeed::from_tasks(
        FeedMode::Backtest,
        sources,
        Some(run.replay().to().timestamp_millis()),
    )
    .with_metrics(metrics.clone())
    .with_capture(control.capture().cloned());
    let replay = tokio::spawn(async move { feed.forward_run(tx).await });

    let mut processor =
        BacktestProcessor::new(run, store, strategies, metrics.clone(), HashSet::new());
    control.emit(RunLifecycleEvent::Started {
        run: run.id().clone(),
    });
    if control.is_cancelled() {
        control.emit(RunLifecycleEvent::Cancelled {
            run: run.id().clone(),
        });
        return Ok(processor.report());
    }

    while let Some(merged) = rx.recv().await {
        if control.is_cancelled() {
            control.emit(RunLifecycleEvent::Cancelled {
                run: run.id().clone(),
            });
            return Ok(processor.report());
        }
        processor
            .process_signal(control, SourceSignal::Data(Box::new(merged.source)), false)
            .await?;
    }

    replay
        .await
        .map_err(|error| StartError::Source {
            run: run.id().clone(),
            source: pmkit_data::DataSourceError::ReplayGap {
                message: format!("merged feed task failed: {error}"),
            },
        })?
        .map_err(|error| match error {
            FeedError::Source(source) => StartError::Source {
                run: run.id().clone(),
                source,
            },
            FeedError::Capture(source) => StartError::Capture {
                run: run.id().clone(),
                source,
            },
        })?;
    control.emit(RunLifecycleEvent::Completed {
        run: run.id().clone(),
    });
    Ok(processor.report())
}

/// A backtest that consumes one raw capture directly in its writer-assigned order.
///
/// The run's configured primary historical source is replaced by the capture;
/// named reference histories are rejected rather than silently mixed in.
pub struct RecordedBacktest<'a, R: BufRead + Send + 'static> {
    run: &'a BacktestRun,
    source: CaptureReplaySource<R>,
}

impl<R: BufRead + Send + 'static> std::fmt::Debug for RecordedBacktest<'_, R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RecordedBacktest")
            .field("run_id", self.run.id())
            .finish_non_exhaustive()
    }
}

impl<'a, R: BufRead + Send + 'static> RecordedBacktest<'a, R> {
    /// Creates a recorded-order backtest from a template run and one capture source.
    #[must_use]
    pub const fn replay_capture(run: &'a BacktestRun, source: CaptureReplaySource<R>) -> Self {
        Self { run, source }
    }

    /// Replays the capture without requiring internal runtime controls.
    ///
    /// # Errors
    ///
    /// Returns an error if validation, strategy initialization, replay, capture, or storage fails.
    pub async fn drive(self, store: Option<&dyn TapeStore>) -> Result<BacktestReport, StartError> {
        self.drive_with_control(store, &RunControl::default()).await
    }

    pub(crate) async fn drive_with_control(
        self,
        store: Option<&dyn TapeStore>,
        control: &RunControl,
    ) -> Result<BacktestReport, StartError> {
        let required_oracles = validate_recorded_run(self.run)?;
        let strategies = instantiate_strategies(self.run.strategies(), self.run.id())?;
        let metrics = control.metrics_for(self.run.id());
        let mut processor =
            BacktestProcessor::new(self.run, store, strategies, metrics, required_oracles);
        let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
        let replay = tokio::spawn(self.source.replay_recorded_order(tx));
        let mut eof_seen = false;
        control.emit(RunLifecycleEvent::Started {
            run: self.run.id().clone(),
        });

        while let Some(signal) = rx.recv().await {
            if control.is_cancelled() {
                control.emit(RunLifecycleEvent::Cancelled {
                    run: self.run.id().clone(),
                });
                replay.abort();
                let _ = replay.await;
                return Ok(processor.report());
            }
            if eof_seen {
                replay.abort();
                let _ = replay.await;
                return Err(recorded_source_error(
                    self.run.id(),
                    "capture emitted a signal after EOF",
                ));
            }
            eof_seen = matches!(signal, SourceSignal::Eof);
            if let Err(source) =
                crate::capture::capture_merged_source_signal(control.capture(), &signal)
            {
                replay.abort();
                let _ = replay.await;
                return Err(StartError::Capture {
                    run: self.run.id().clone(),
                    source,
                });
            }
            if let Err(error) = processor.process_signal(control, signal, true).await {
                replay.abort();
                let _ = replay.await;
                return Err(error);
            }
        }

        replay
            .await
            .map_err(|error| {
                recorded_source_error(
                    self.run.id(),
                    &format!("capture replay task failed: {error}"),
                )
            })?
            .map_err(|source| StartError::Source {
                run: self.run.id().clone(),
                source,
            })?;
        if !eof_seen {
            return Err(recorded_source_error(
                self.run.id(),
                "capture ended without an orderly EOF signal",
            ));
        }
        processor.validate_observed_oracles()?;
        control.emit(RunLifecycleEvent::Completed {
            run: self.run.id().clone(),
        });
        Ok(processor.report())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RequiredOracle {
    name: String,
    market: MarketId,
    asset: Asset,
    source_id: String,
}

fn validate_recorded_run(run: &BacktestRun) -> Result<HashSet<RequiredOracle>, StartError> {
    if !run.replay().reference_source_refs().is_empty() {
        return Err(recorded_source_error(
            run.id(),
            "recorded backtests cannot combine a capture with independent reference histories",
        ));
    }
    let mut required = HashSet::new();
    for requirement in run.settlement_oracle_requirements() {
        let binding = run.settlement_oracle_bindings().iter().find(|binding| {
            binding.name() == requirement.name() && binding.market() == requirement.market()
        });
        let Some(binding) = binding else {
            return Err(recorded_source_error(
                run.id(),
                &format!(
                    "required settlement oracle `{}` for market `{}` has no matching capture binding",
                    requirement.name(),
                    requirement.market(),
                ),
            ));
        };
        if binding.name().trim().is_empty() || binding.asset().trim().is_empty() {
            return Err(recorded_source_error(
                run.id(),
                &format!(
                    "required settlement oracle `{}` for market `{}` needs a nonblank name and asset binding",
                    requirement.name(),
                    requirement.market(),
                ),
            ));
        }
        let asset = binding.asset().parse::<Asset>().map_err(|error| {
            recorded_source_error(
                run.id(),
                &format!(
                    "required settlement oracle `{}` has an invalid asset binding: {error}",
                    requirement.name(),
                ),
            )
        })?;
        required.insert(RequiredOracle {
            name: binding.name().to_owned(),
            market: binding.market().clone(),
            asset,
            source_id: binding.source_id().to_owned(),
        });
    }
    Ok(required)
}

fn recorded_source_error(run: &RunId, message: &str) -> StartError {
    StartError::Source {
        run: run.clone(),
        source: pmkit_data::DataSourceError::ReplayGap {
            message: message.to_owned(),
        },
    }
}

#[derive(Debug, Clone, Copy)]
struct EventClocks {
    strategy_ms: i64,
    activation_ms: i64,
}

const fn strategy_timestamp_ms(source: &SourceEnvelope) -> Option<i64> {
    match source {
        SourceEnvelope::PmMarket(envelope) => match &envelope.fact {
            MarketEvent::BookUpdate { timestamp_ms, .. } => Some(*timestamp_ms),
            _ => None,
        },
        SourceEnvelope::CexReference(envelope) => Some(match &envelope.fact {
            CexReferenceEvent::Trade { timestamp_ms, .. }
            | CexReferenceEvent::Twap60sSnapshot { timestamp_ms, .. }
            | CexReferenceEvent::Twap60sUpdate { timestamp_ms, .. } => *timestamp_ms,
        }),
        SourceEnvelope::PmAccount(_) => None,
    }
}

struct BacktestProcessor<'a> {
    run: &'a BacktestRun,
    store: Option<&'a dyn TapeStore>,
    scope: OwnerScope,
    metrics: crate::RunMetrics,
    simulation_config: SimulationConfig,
    strategies: Vec<StrategyInstance>,
    sim: SimEngine,
    positions_by_market: HashMap<MarketId, Vec<pmkit_book::Position>>,
    marks: HashMap<(MarketId, Outcome), Decimal>,
    strategy_books: Vec<OrderBookL2>,
    connection_epochs: HashMap<String, i64>,
    cex_metrics: crate::causal::CexTradeMetricsState,
    required_oracles: HashSet<RequiredOracle>,
    observed_oracles: HashSet<RequiredOracle>,
}

impl<'a> BacktestProcessor<'a> {
    fn new(
        run: &'a BacktestRun,
        store: Option<&'a dyn TapeStore>,
        strategies: Vec<StrategyInstance>,
        metrics: crate::RunMetrics,
        required_oracles: HashSet<RequiredOracle>,
    ) -> Self {
        // ponytail: fee category fixed to Crypto; positions tracked from fills.
        let simulation = run.simulation();
        let simulation_config = SimulationConfig {
            activation_latency_ms: i64::try_from(simulation.activation_latency.as_millis())
                .unwrap_or(i64::MAX),
            maker_queue_ahead_bps: simulation.maker_queue_ahead_bps,
            slippage_bps: simulation.slippage_bps,
            market_impact_bps: simulation.market_impact_bps,
            fee_model: Some(simulation.resolved_fee_model()),
            market_limits: simulation.market_limits,
        };
        Self {
            run,
            store,
            scope: OwnerScope::new(run.portfolio().clone(), run.id().clone()),
            metrics,
            simulation_config,
            strategy_books: vec![OrderBookL2::default(); strategies.len()],
            strategies,
            sim: SimEngine::with_fee_config("bt", 0, simulation_config),
            positions_by_market: HashMap::new(),
            marks: HashMap::new(),
            connection_epochs: HashMap::new(),
            cex_metrics: crate::causal::CexTradeMetricsState::default(),
            required_oracles,
            observed_oracles: HashSet::new(),
        }
    }

    async fn process_signal(
        &mut self,
        _control: &RunControl,
        signal: SourceSignal,
        recorded_order: bool,
    ) -> Result<(), StartError> {
        store_signal(self.store, &self.scope, &signal)
            .await
            .map_err(|source| StartError::Storage {
                run: self.run.id().clone(),
                source,
            })?;
        match signal {
            SourceSignal::Data(source) => {
                if observe_reconnect(&source, &mut self.connection_epochs) {
                    self.metrics.reconnect();
                }
                if recorded_order && matches!(source.as_ref(), SourceEnvelope::PmAccount(_)) {
                    return Err(recorded_source_error(
                        self.run.id(),
                        "recorded PM account facts are unsupported because backtest order acknowledgements cannot be reconstructed safely",
                    ));
                }
                if recorded_order {
                    self.observe_oracle(&source);
                }
                if matches!(source.as_ref(), SourceEnvelope::PmMarket(_)) {
                    self.metrics.event();
                }
                if let Some(strategy_ms) = strategy_timestamp_ms(&source) {
                    let activation_ms = if recorded_order {
                        source.metadata().receipt_time_ms
                    } else {
                        strategy_ms
                    };
                    self.process_fact(
                        *source,
                        EventClocks {
                            strategy_ms,
                            activation_ms,
                        },
                    )
                    .await?;
                }
            }
            SourceSignal::Gap(gap) => {
                let range = gap.range.map_or_else(
                    || "unknown sequence range".to_owned(),
                    |range| {
                        format!(
                            "{:?} sequences {}..={}",
                            range.domain, range.first_missing, range.last_missing
                        )
                    },
                );
                return Err(recorded_source_error(
                    self.run.id(),
                    &format!(
                        "recorded capture gap for {} ({range}): {}",
                        gap.source_id, gap.reason,
                    ),
                ));
            }
            SourceSignal::Heartbeat(_) | SourceSignal::Watermark(_) | SourceSignal::Eof => {}
        }
        Ok(())
    }

    fn observe_oracle(&mut self, source: &SourceEnvelope) {
        let SourceEnvelope::CexReference(envelope) = source else {
            return;
        };
        let asset = match &envelope.fact {
            CexReferenceEvent::Twap60sSnapshot { asset, .. }
            | CexReferenceEvent::Twap60sUpdate { asset, .. } => *asset,
            CexReferenceEvent::Trade { .. } => return,
        };
        self.observed_oracles.extend(
            self.required_oracles
                .iter()
                .filter(|required| {
                    required.source_id == envelope.metadata.source_id && required.asset == asset
                })
                .cloned(),
        );
    }

    fn validate_observed_oracles(&self) -> Result<(), StartError> {
        let missing = self
            .required_oracles
            .difference(&self.observed_oracles)
            .map(|required| {
                format!(
                    "{} for market {} ({})",
                    required.name, required.market, required.asset
                )
            })
            .collect::<Vec<_>>();
        if missing.is_empty() {
            return Ok(());
        }
        Err(recorded_source_error(
            self.run.id(),
            &format!(
                "capture ended without required settlement oracle facts: {}",
                missing.join(", "),
            ),
        ))
    }

    async fn process_fact(
        &mut self,
        source: SourceEnvelope,
        clocks: EventClocks,
    ) -> Result<(), StartError> {
        if let SourceEnvelope::CexReference(envelope) = &source {
            self.cex_metrics.observe(&envelope.fact);
            let fact = StrategyFact::Reference(envelope.fact.clone());
            let (added, rejected, decisions, evaluations) =
                run_strategies(&mut RunStrategiesInputs {
                    strategies: &mut self.strategies,
                    fact: &fact,
                    market: None,
                    book: None,
                    event_stream: "reference",
                    strategy_books: &mut self.strategy_books,
                    positions_by_market: &mut self.positions_by_market,
                    clocks,
                    sim: &mut self.sim,
                });
            self.metrics.add_fills(added);
            self.metrics.add_rejected(rejected);
            self.metrics.add_decisions(decisions);
            if let Some(store) = self.store {
                for evaluation in evaluations {
                    let identity = crate::causal::strategy_decision_identity(
                        &self.scope,
                        "backtest-reference",
                        &evaluation.strategy,
                        &evaluation.market,
                        &evaluation.event_stream,
                        clocks.strategy_ms,
                        &envelope.metadata,
                    );
                    crate::causal::record_book_decision(
                        store,
                        &identity,
                        &evaluation.book,
                        self.cex_metrics.snapshot(),
                        evaluation.verdicts,
                        Some(self.simulation_config),
                    )
                    .await
                    .map_err(|source| StartError::Storage {
                        run: self.run.id().clone(),
                        source,
                    })?;
                }
            }
            return Ok(());
        }
        self.process_market_fact(source, clocks).await
    }

    async fn process_market_fact(
        &mut self,
        source: SourceEnvelope,
        clocks: EventClocks,
    ) -> Result<(), StartError> {
        let SourceEnvelope::PmMarket(envelope) = source else {
            return Ok(());
        };
        let event = envelope.fact;
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
                self.marks.insert((market.clone(), *outcome), mark);
            } else {
                self.marks.remove(&(market.clone(), *outcome));
            }
            let mut simulation_book = book.clone();
            simulation_book.timestamp_ms = clocks.activation_ms;
            self.sim.update_book(market, *outcome, simulation_book);
            let drained = self.sim.drain_fills();
            self.metrics
                .add_fills(absorb_market_fills(&drained, &mut self.positions_by_market));
            let fact = StrategyFact::Market(event.clone());
            let event_stream = format!("market:{market}:{}", outcome.to_string().to_lowercase());
            let (added, rejected, decisions, evaluations) =
                run_strategies(&mut RunStrategiesInputs {
                    strategies: &mut self.strategies,
                    fact: &fact,
                    market: Some(market),
                    book: Some(&book),
                    event_stream: &event_stream,
                    strategy_books: &mut self.strategy_books,
                    positions_by_market: &mut self.positions_by_market,
                    clocks,
                    sim: &mut self.sim,
                });
            self.metrics.add_fills(added);
            self.metrics.add_rejected(rejected);
            self.metrics.add_decisions(decisions);
            if let Some(store) = self.store {
                for evaluation in evaluations {
                    let identity = crate::causal::strategy_decision_identity(
                        &self.scope,
                        "backtest-market",
                        &evaluation.strategy,
                        &evaluation.market,
                        &evaluation.event_stream,
                        clocks.strategy_ms,
                        &envelope.metadata,
                    );
                    crate::causal::record_book_decision(
                        store,
                        &identity,
                        &evaluation.book,
                        self.cex_metrics.snapshot(),
                        evaluation.verdicts,
                        Some(self.simulation_config),
                    )
                    .await
                    .map_err(|source| StartError::Storage {
                        run: self.run.id().clone(),
                        source,
                    })?;
                }
            }
        }
        Ok(())
    }

    fn report(&self) -> BacktestReport {
        let metrics = self.metrics.snapshot();
        BacktestReport {
            run: self.run.id().clone(),
            events_processed: metrics.events_processed,
            fills: metrics.fills,
            metrics,
            exposure: report_exposure(
                &self.positions_by_market,
                &self.marks,
                &self.sim.open_orders(),
            ),
        }
    }
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
    open_orders: &[pmkit_sim::SimOpenOrder],
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
    let reservations = open_orders
        .iter()
        .filter_map(|order| {
            order.strategy.clone().map(|strategy| ExposureReservation {
                market: order.market.clone(),
                strategy,
                notional: order.remaining_qty * order.price,
            })
        })
        .collect::<Vec<_>>();
    aggregate_exposure(&position_notionals, &reservations)
}

struct RunStrategiesInputs<'a> {
    strategies: &'a mut [StrategyInstance],
    fact: &'a StrategyFact,
    market: Option<&'a pmkit_core::MarketId>,
    book: Option<&'a OrderBookL2>,
    event_stream: &'a str,
    strategy_books: &'a mut [OrderBookL2],
    positions_by_market: &'a mut HashMap<MarketId, Vec<pmkit_book::Position>>,
    clocks: EventClocks,
    sim: &'a mut SimEngine,
}

struct StrategyEvaluation {
    strategy: pmkit_core::StrategyId,
    market: MarketId,
    event_stream: String,
    book: OrderBookL2,
    verdicts: Vec<crate::causal::ActionRiskVerdict>,
}

fn run_strategies(
    inputs: &mut RunStrategiesInputs<'_>,
) -> (usize, usize, usize, Vec<StrategyEvaluation>) {
    let mut fills = 0;
    let mut rejected = 0_usize;
    let mut decisions = 0;
    let mut evaluations = Vec::new();
    for (index, instance) in inputs.strategies.iter_mut().enumerate() {
        if inputs
            .market
            .is_some_and(|market| instance.market != *market)
        {
            continue;
        }
        if let Some(book) = inputs.book {
            inputs.strategy_books[index] = book.clone();
        }
        let book = &inputs.strategy_books[index];
        let positions = inputs
            .positions_by_market
            .get(&instance.market)
            .map_or(&[] as &[pmkit_book::Position], Vec::as_slice);
        let context = StrategyContext {
            fact: inputs.fact,
            market: &instance.market,
            book,
            positions,
            now: LogicalTimestamp::from_millis(inputs.clocks.strategy_ms),
        };
        decisions += 1;
        let mut verdicts = Vec::new();
        if let Ok(actions) = instance.strategy.on_event(context) {
            for action in actions.as_slice() {
                match action {
                    Action::Place(order) => submit_sim_order(
                        inputs.sim,
                        &instance.id,
                        order,
                        inputs.clocks.activation_ms,
                        &mut verdicts,
                        &mut rejected,
                    ),
                    Action::Cancel(order_id) => {
                        inputs.sim.cancel_for_strategy(&instance.id, order_id);
                    }
                    Action::ReplaceQuotes { cancel, place } => {
                        for order_id in cancel {
                            inputs.sim.cancel_for_strategy(&instance.id, order_id);
                        }
                        for order in place {
                            submit_sim_order(
                                inputs.sim,
                                &instance.id,
                                order,
                                inputs.clocks.activation_ms,
                                &mut verdicts,
                                &mut rejected,
                            );
                        }
                    }
                    Action::CancelAll => {
                        inputs.sim.cancel_all_for_strategy(&instance.id);
                    }
                }
            }
        }
        let drained = inputs.sim.drain_fills();
        fills += absorb_market_fills(&drained, inputs.positions_by_market);
        evaluations.push(StrategyEvaluation {
            strategy: instance.id.clone(),
            market: instance.market.clone(),
            event_stream: inputs.event_stream.to_owned(),
            book: book.clone(),
            verdicts,
        });
    }
    (fills, rejected, decisions, evaluations)
}

fn submit_sim_order(
    sim: &mut SimEngine,
    strategy: &pmkit_core::StrategyId,
    order: &pmkit_exec::PlaceOrder,
    timestamp_ms: i64,
    verdicts: &mut Vec<crate::causal::ActionRiskVerdict>,
    rejected: &mut usize,
) {
    let action_index = u32::try_from(verdicts.len()).unwrap_or(u32::MAX);
    if sim
        .submit_for_strategy(order, strategy.clone(), timestamp_ms)
        .is_some()
    {
        verdicts.push(crate::causal::ActionRiskVerdict::accepted(action_index));
    } else {
        verdicts.push(crate::causal::ActionRiskVerdict::rejected(
            action_index,
            "simulation/execution rejection",
        ));
        *rejected += 1;
    }
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
                market_limits: None,
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
