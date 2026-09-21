use super::{
    PaperReport, RunControl, RunLifecycleEvent, StartError, StrategyInstance,
    instantiate_strategies, observe_reconnect, store_signal, validate_account_owner,
};
use crate::feed::{FeedMode, MergedFeed, SourceTaskDefinition};
use pmkit_accounting::{
    ExposureReservation, PortfolioExposure, PositionExposure, aggregate_exposure,
};
use pmkit_book::{OrderBookL2, Position};
use pmkit_event::{CexReferenceEvent, MarketEvent, PmAccountEvent, SourceEnvelope, StrategyFact};
use pmkit_exec::{ExecError, Executor, OrderId};
use pmkit_market::Outcome;
use pmkit_paper::{PaperExecutor, PaperLedgerEntry, PaperLedgerError};
use pmkit_sim::SimulationConfig;
use pmkit_spec::PaperRun;
use pmkit_store::{CausalDecision, CausalIdentity, OwnerScope, StoreError, TapeStore};
use pmkit_strategy::{Action, LogicalTimestamp, StrategyContext};
use rust_decimal::Decimal;
use serde_json::json;
use std::collections::{HashMap, HashSet};

// allow: SIZE_OK — the scoped driver and task-specific recovery tests must remain in this file.

fn drain_fills(rx: &mut tokio::sync::mpsc::Receiver<MarketEvent>) -> Vec<MarketEvent> {
    let mut fills = Vec::new();
    while let Ok(event) = rx.try_recv() {
        fills.push(event);
    }
    fills
}

async fn persist_paper_ledger(
    store: &dyn TapeStore,
    scope: &OwnerScope,
    paper: &PaperExecutor,
) -> Result<(), StoreError> {
    while let Some(entry) = paper.pending_ledger_entry() {
        let ingest_sequence =
            i64::try_from(entry.sequence()).map_err(|_| StoreError::CorruptPaperLedger {
                message: "paper ledger sequence exceeds storage range".into(),
            })?;
        store
            .store_decision(&CausalDecision {
                identity: CausalIdentity {
                    scope: scope.clone(),
                    correlation_id: entry.event_id().to_owned(),
                    source_timestamp_ms: entry.timestamp_ms(),
                    ingest_sequence,
                },
                payload: entry
                    .to_value()
                    .map_err(|error| corrupt_paper_ledger(&error))?,
            })
            .await?;
        if !paper.acknowledge_ledger_entry(entry.event_id()) {
            return Err(StoreError::Storage {
                message: "paper ledger pending entry changed before acknowledgement".into(),
            });
        }
    }
    Ok(())
}

async fn restore_paper_executor(
    store: &dyn TapeStore,
    scope: &OwnerScope,
    fills: tokio::sync::mpsc::Sender<MarketEvent>,
    id_prefix: &str,
    config: SimulationConfig,
) -> Result<Option<PaperExecutor>, StoreError> {
    let decisions = store.read_decisions(scope).await?;
    restore_paper_executor_from_decisions(&decisions, fills, id_prefix, config)
}

fn restore_paper_executor_from_decisions(
    decisions: &[CausalDecision],
    fills: tokio::sync::mpsc::Sender<MarketEvent>,
    id_prefix: &str,
    config: SimulationConfig,
) -> Result<Option<PaperExecutor>, StoreError> {
    let mut entries = Vec::new();
    for decision in decisions {
        let Some(entry) = PaperLedgerEntry::from_value(&decision.payload)
            .map_err(|error| corrupt_paper_ledger(&error))?
        else {
            continue;
        };
        let ingest_sequence = u64::try_from(decision.identity.ingest_sequence).map_err(|_| {
            StoreError::CorruptPaperLedger {
                message: "paper ledger ingest sequence is negative".into(),
            }
        })?;
        if decision.identity.correlation_id != entry.event_id()
            || decision.identity.source_timestamp_ms != entry.timestamp_ms()
            || ingest_sequence != entry.sequence()
        {
            return Err(StoreError::CorruptPaperLedger {
                message: "paper ledger payload does not match its durable identity".into(),
            });
        }
        entries.push(entry);
    }
    if entries.is_empty() {
        return Ok(None);
    }
    PaperExecutor::reconstruct_with_fee_config(fills, id_prefix, config, &entries)
        .map(Some)
        .map_err(|error| corrupt_paper_ledger(&error))
}

fn corrupt_paper_ledger(error: &PaperLedgerError) -> StoreError {
    StoreError::CorruptPaperLedger {
        message: error.to_string(),
    }
}

fn owned_paper_orders(paper: &PaperExecutor, strategy: &pmkit_core::StrategyId) -> Vec<OrderId> {
    let account = paper.account_state();
    account
        .resting_orders
        .into_iter()
        .chain(account.delayed_orders)
        .filter(|order| order.strategy.as_ref() == Some(strategy))
        .map(|order| order.order_id)
        .collect()
}

async fn cancel_paper_order(
    run: &PaperRun,
    paper: &PaperExecutor,
    store: Option<&dyn TapeStore>,
    scope: &OwnerScope,
    strategy: &pmkit_core::StrategyId,
    order_id: &OrderId,
) -> Result<(), StartError> {
    if owned_paper_orders(paper, strategy).contains(order_id) {
        paper
            .cancel(order_id)
            .await
            .map_err(|source| StartError::ExecutionState {
                run: run.id().clone(),
                source,
            })?;
        persist_or_drain_paper(store, scope, paper, run.id()).await?;
    }
    Ok(())
}

struct PaperActionContext<'a> {
    run: &'a PaperRun,
    paper: &'a PaperExecutor,
    store: Option<&'a dyn TapeStore>,
    scope: &'a OwnerScope,
    strategy: &'a pmkit_core::StrategyId,
    timestamp_ms: i64,
    metrics: &'a crate::RunMetrics,
}

async fn submit_paper_order(
    context: &PaperActionContext<'_>,
    order: &pmkit_exec::PlaceOrder,
    action_index: u32,
    marks: &HashMap<(pmkit_core::MarketId, Outcome), Decimal>,
    loss_limit: Decimal,
    loss_breached: &mut bool,
    verdicts: &mut Vec<crate::causal::ActionRiskVerdict>,
) -> Result<(), StartError> {
    let submit_result = context
        .paper
        .submit_for_strategy(order, context.strategy.clone(), context.timestamp_ms)
        .await;
    context.metrics.set_fills(context.paper.fill_count());
    if let Err(ExecError::Rejected { reason }) = &submit_result {
        context.metrics.reject();
        verdicts.push(crate::causal::ActionRiskVerdict::rejected(
            action_index,
            reason.clone(),
        ));
    }
    update_loss_breach(
        context.paper,
        marks,
        loss_limit,
        loss_breached,
        context.store,
        context.scope,
        context.run.id(),
        context.timestamp_ms,
    )
    .await?;
    persist_or_drain_paper(
        context.store,
        context.scope,
        context.paper,
        context.run.id(),
    )
    .await?;
    match submit_result {
        Ok(_) => verdicts.push(crate::causal::ActionRiskVerdict::accepted(action_index)),
        Err(ExecError::Rejected { .. }) => {}
        Err(source) => {
            return Err(StartError::ExecutionState {
                run: context.run.id().clone(),
                source,
            });
        }
    }
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "paper risk admission explicitly carries the shared exposure inputs"
)]
fn paper_order_rejection_reason(
    paper: &PaperExecutor,
    order: &pmkit_exec::PlaceOrder,
    strategy_market: &pmkit_core::MarketId,
    strategy: &pmkit_core::StrategyId,
    marks: &HashMap<(pmkit_core::MarketId, Outcome), Decimal>,
    limits: &pmkit_runtime::RiskLimits,
    effective_limits_by_strategy: &HashMap<pmkit_core::StrategyId, pmkit_runtime::RiskLimits>,
    loss_breached: bool,
) -> Option<&'static str> {
    if order.market != *strategy_market {
        return Some("market mismatch");
    }
    let account = paper.account_state();
    let mut positions_by_market = paper_positions_by_market(&account);
    let Some(daily_pnl) = crate::live::live_risk::marked_daily_pnl(
        &mut positions_by_market,
        marks,
        account.realized_pnl.as_decimal(),
        account.fees.as_decimal(),
    ) else {
        return Some("risk data unavailable");
    };
    let effective_limits = effective_limits_by_strategy.get(strategy).unwrap_or(limits);
    let market_positions = positions_by_market
        .get(strategy_market)
        .map_or(&[][..], Vec::as_slice);
    let exposure = paper_risk_exposure(
        &account,
        &positions_by_market,
        marks,
        strategy_market,
        strategy,
        order.outcome,
        daily_pnl,
    );
    if loss_breached
        || !crate::live::live_risk::passes_aggregated_risk(
            order,
            effective_limits,
            market_positions,
            exposure,
        )
    {
        Some("risk gate")
    } else {
        None
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "paper admission keeps the shared risk gate adjacent to submission"
)]
async fn submit_risk_checked_paper_order(
    context: &PaperActionContext<'_>,
    strategy_market: &pmkit_core::MarketId,
    order: &pmkit_exec::PlaceOrder,
    marks: &HashMap<(pmkit_core::MarketId, Outcome), Decimal>,
    limits: &pmkit_runtime::RiskLimits,
    loss_limit: Decimal,
    effective_limits_by_strategy: &HashMap<pmkit_core::StrategyId, pmkit_runtime::RiskLimits>,
    loss_breached: &mut bool,
    verdicts: &mut Vec<crate::causal::ActionRiskVerdict>,
) -> Result<(), StartError> {
    let action_index = u32::try_from(verdicts.len()).unwrap_or(u32::MAX);
    update_loss_breach(
        context.paper,
        marks,
        loss_limit,
        loss_breached,
        context.store,
        context.scope,
        context.run.id(),
        context.timestamp_ms,
    )
    .await?;
    if let Some(reason) = paper_order_rejection_reason(
        context.paper,
        order,
        strategy_market,
        context.strategy,
        marks,
        limits,
        effective_limits_by_strategy,
        *loss_breached,
    ) {
        context.metrics.reject();
        verdicts.push(crate::causal::ActionRiskVerdict::rejected(
            action_index,
            reason,
        ));
        return Ok(());
    }
    submit_paper_order(
        context,
        order,
        action_index,
        marks,
        loss_limit,
        loss_breached,
        verdicts,
    )
    .await?;
    Ok(())
}

async fn persist_or_drain_paper(
    store: Option<&dyn TapeStore>,
    scope: &OwnerScope,
    paper: &PaperExecutor,
    run: &pmkit_core::RunId,
) -> Result<(), StartError> {
    if let Some(store) = store {
        persist_paper_ledger(store, scope, paper)
            .await
            .map_err(|source| StartError::Storage {
                run: run.clone(),
                source,
            })?;
    } else {
        paper.drain_ledger();
    }
    Ok(())
}

fn paper_positions_by_market(
    account: &pmkit_paper::PaperAccountState,
) -> HashMap<pmkit_core::MarketId, Vec<Position>> {
    let mut positions_by_market = HashMap::new();
    for position in &account.positions {
        positions_by_market
            .entry(position.market.clone())
            .or_insert_with(Vec::new)
            .push(Position {
                outcome: position.outcome,
                qty: position.quantity,
                avg_entry: position.average_entry,
                unrealized_pnl: Decimal::ZERO,
            });
    }
    positions_by_market
}

fn paper_risk_exposure(
    account: &pmkit_paper::PaperAccountState,
    positions_by_market: &HashMap<pmkit_core::MarketId, Vec<Position>>,
    marks: &HashMap<(pmkit_core::MarketId, Outcome), Decimal>,
    market: &pmkit_core::MarketId,
    strategy: &pmkit_core::StrategyId,
    outcome: Outcome,
    daily_pnl: Decimal,
) -> crate::live::live_risk::PortfolioRiskExposure {
    let mut reserved_portfolio = Decimal::ZERO;
    let mut reserved_market = Decimal::ZERO;
    let mut reserved_strategy = Decimal::ZERO;
    let mut pending_position_notional = Decimal::ZERO;
    for order in account
        .resting_orders
        .iter()
        .chain(account.delayed_orders.iter())
    {
        let notional = order.remaining_qty * order.price;
        reserved_portfolio += notional;
        if order.market == *market {
            reserved_market += notional;
            if order.outcome == outcome {
                pending_position_notional += notional;
            }
        }
        if order.strategy.as_ref() == Some(strategy) {
            reserved_strategy += notional;
        }
    }
    crate::live::live_risk::PortfolioRiskExposure {
        portfolio_notional: crate::live::live_risk::portfolio_marked_notional(
            positions_by_market,
            marks,
        ) + reserved_portfolio,
        market_notional: positions_by_market
            .get(market)
            .map_or(Decimal::ZERO, |positions| {
                crate::live::live_risk::marked_position_notional(market, positions, marks)
            })
            + reserved_market,
        strategy_notional: reserved_strategy,
        pending_position_notional,
        daily_pnl,
        open_orders: account.resting_orders.len() + account.delayed_orders.len(),
    }
}

const PAPER_MAX_LOSS_BREACH_CORRELATION: &str = "paper-risk:max-loss";

async fn paper_loss_breached(
    store: Option<&dyn TapeStore>,
    scope: &OwnerScope,
) -> Result<bool, StoreError> {
    let Some(store) = store else {
        return Ok(false);
    };
    Ok(store.read_decisions(scope).await?.iter().any(|decision| {
        decision.identity.correlation_id == PAPER_MAX_LOSS_BREACH_CORRELATION
            && decision.payload["kind"] == "paper-risk-breach"
            && decision.payload["reason"] == "max_loss"
    }))
}

fn effective_max_loss(
    limits: &pmkit_runtime::RiskLimits,
    effective_limits_by_strategy: &HashMap<pmkit_core::StrategyId, pmkit_runtime::RiskLimits>,
) -> Decimal {
    effective_limits_by_strategy
        .values()
        .map(|effective| effective.max_loss.as_decimal())
        .fold(limits.max_loss.as_decimal(), |current, candidate| {
            if candidate < current {
                candidate
            } else {
                current
            }
        })
}

#[expect(
    clippy::too_many_arguments,
    reason = "durable paper loss latching carries the storage identity context"
)]
async fn update_loss_breach(
    paper: &PaperExecutor,
    marks: &HashMap<(pmkit_core::MarketId, Outcome), Decimal>,
    loss_limit: Decimal,
    loss_breached: &mut bool,
    store: Option<&dyn TapeStore>,
    scope: &OwnerScope,
    run: &pmkit_core::RunId,
    timestamp_ms: i64,
) -> Result<(), StartError> {
    if *loss_breached {
        return Ok(());
    }
    let account = paper.account_state();
    let mut positions_by_market = paper_positions_by_market(&account);
    let Some(daily_pnl) = crate::live::live_risk::marked_daily_pnl(
        &mut positions_by_market,
        marks,
        account.realized_pnl.as_decimal(),
        account.fees.as_decimal(),
    ) else {
        return Ok(());
    };
    if daily_pnl > -loss_limit {
        return Ok(());
    }
    *loss_breached = true;
    let Some(store) = store else {
        return Ok(());
    };
    store
        .store_decision(&CausalDecision {
            identity: CausalIdentity {
                scope: scope.clone(),
                correlation_id: PAPER_MAX_LOSS_BREACH_CORRELATION.into(),
                source_timestamp_ms: timestamp_ms,
                ingest_sequence: 0,
            },
            payload: json!({
                "kind": "paper-risk-breach",
                "reason": "max_loss",
            }),
        })
        .await
        .map_err(|source| StartError::Storage {
            run: run.clone(),
            source,
        })
}

#[expect(
    clippy::too_many_arguments,
    reason = "the shared dispatcher carries ordered paper execution state"
)]
async fn dispatch_paper_strategy(
    run: &PaperRun,
    paper: &PaperExecutor,
    store: Option<&dyn TapeStore>,
    scope: &OwnerScope,
    instance: &mut StrategyInstance,
    fact: &StrategyFact,
    book: &OrderBookL2,
    positions: &[Position],
    timestamp_ms: i64,
    marks: &HashMap<(pmkit_core::MarketId, Outcome), Decimal>,
    limits: &pmkit_runtime::RiskLimits,
    loss_limit: Decimal,
    effective_limits_by_strategy: &HashMap<pmkit_core::StrategyId, pmkit_runtime::RiskLimits>,
    loss_breached: &mut bool,
    fill_rx: &mut tokio::sync::mpsc::Receiver<MarketEvent>,
    metrics: &crate::RunMetrics,
) -> Result<Vec<crate::causal::ActionRiskVerdict>, StartError> {
    let context = StrategyContext {
        fact,
        market: &instance.market,
        book,
        positions,
        now: LogicalTimestamp::from_millis(timestamp_ms),
    };
    metrics.decision();
    let mut verdicts = Vec::new();
    if let Ok(actions) = instance.strategy.on_event(context) {
        let action_context = PaperActionContext {
            run,
            paper,
            store,
            scope,
            strategy: &instance.id,
            timestamp_ms,
            metrics,
        };
        for action in actions.as_slice() {
            match action {
                Action::Place(order) => {
                    submit_risk_checked_paper_order(
                        &action_context,
                        &instance.market,
                        order,
                        marks,
                        limits,
                        loss_limit,
                        effective_limits_by_strategy,
                        loss_breached,
                        &mut verdicts,
                    )
                    .await?;
                }
                Action::Cancel(order_id) => {
                    cancel_paper_order(run, paper, store, scope, &instance.id, order_id).await?;
                }
                Action::ReplaceQuotes { cancel, place } => {
                    for order_id in cancel {
                        cancel_paper_order(run, paper, store, scope, &instance.id, order_id)
                            .await?;
                    }
                    for order in place {
                        submit_risk_checked_paper_order(
                            &action_context,
                            &instance.market,
                            order,
                            marks,
                            limits,
                            loss_limit,
                            effective_limits_by_strategy,
                            loss_breached,
                            &mut verdicts,
                        )
                        .await?;
                    }
                }
                Action::CancelAll => {
                    for order_id in owned_paper_orders(paper, &instance.id) {
                        cancel_paper_order(run, paper, store, scope, &instance.id, &order_id)
                            .await?;
                    }
                }
            }
        }
    }
    drain_fills(fill_rx);
    metrics.set_fills(paper.fill_count());
    Ok(verdicts)
}

#[expect(
    clippy::too_many_lines,
    reason = "the paper run owns one ordered feed, executor, strategy, and recording loop"
)]
pub async fn drive_with_control(
    run: &PaperRun,
    store: Option<&dyn TapeStore>,
    control: &RunControl,
) -> Result<PaperReport, StartError> {
    let mut strategies = instantiate_strategies(run.strategies(), run.id())?;
    let effective_limits_by_strategy: HashMap<_, _> = run
        .strategies()
        .iter()
        .map(|registration| {
            (
                registration.id().clone(),
                registration.risk_overrides_ref().effective_limits(
                    run.risk(),
                    registration.market(),
                    registration.id(),
                ),
            )
        })
        .collect();
    let limits = run.risk().clone();
    let loss_limit = effective_max_loss(&limits, &effective_limits_by_strategy);
    let metrics = control.metrics_for(run.id());

    let (fill_tx, mut fill_rx) = tokio::sync::mpsc::channel(1024);
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
    let scope = OwnerScope::new(run.portfolio().clone(), run.id().clone());
    let paper = if let Some(store) = store {
        restore_paper_executor(store, &scope, fill_tx.clone(), "paper", simulation_config)
            .await
            .map_err(|source| StartError::Storage {
                run: run.id().clone(),
                source,
            })?
            .unwrap_or_else(|| {
                PaperExecutor::with_account_fee_config(
                    fill_tx,
                    "paper",
                    simulation_config,
                    run.initial_cash(),
                )
            })
    } else {
        PaperExecutor::with_account_fee_config(
            fill_tx,
            "paper",
            simulation_config,
            run.initial_cash(),
        )
    };
    metrics.set_fills(paper.fill_count());
    if let Some(store) = store {
        persist_paper_ledger(store, &scope, &paper)
            .await
            .map_err(|source| StartError::Storage {
                run: run.id().clone(),
                source,
            })?;
    } else {
        paper.drain_ledger();
    }
    let mut loss_breached =
        paper_loss_breached(store, &scope)
            .await
            .map_err(|source| StartError::Storage {
                run: run.id().clone(),
                source,
            })?;

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1024);
    let mut subscribed = HashSet::new();
    let mut sources = Vec::new();
    for instance in &strategies {
        if !subscribed.insert(instance.market.clone()) {
            continue;
        }
        for outcome in [Outcome::Up, Outcome::Down] {
            let source = run.market_data().clone();
            let market = instance.market.clone();
            let name = format!("pm:{market:?}:{outcome:?}");
            sources.push(SourceTaskDefinition::new(name, move |sink| async move {
                source.subscribe(market, outcome, sink).await
            }));
        }
    }
    for (name, reference) in run.reference_data_refs() {
        let name = name.clone();
        let reference = reference.clone();
        sources.push(SourceTaskDefinition::new(name, move |sink| async move {
            reference.subscribe_reference(sink).await
        }));
    }
    if let Some(account) = run.account_data_ref() {
        let account = account.clone();
        let portfolio = run.portfolio().clone();
        sources.push(SourceTaskDefinition::new(
            "pm-account",
            move |sink| async move { account.subscribe_account(portfolio, sink).await },
        ));
    }
    let feed = MergedFeed::from_tasks(FeedMode::Paper, sources, None).with_metrics(metrics.clone());
    let merge = tokio::spawn(async move { feed.forward(event_tx).await });

    let mut fills = paper.fill_count();
    metrics.set_fills(fills);
    let mut marks = HashMap::new();
    let mut strategy_books = vec![OrderBookL2::default(); strategies.len()];
    let mut connection_epochs = HashMap::new();
    let mut cex_metrics = crate::causal::CexTradeMetricsState::default();
    control.emit(RunLifecycleEvent::Started {
        run: run.id().clone(),
    });
    if control.is_cancelled() {
        control.emit(RunLifecycleEvent::Cancelled {
            run: run.id().clone(),
        });
        let metrics = metrics.snapshot();
        return Ok(PaperReport {
            run: run.id().clone(),
            events_processed: metrics.events_processed,
            fills: metrics.fills,
            metrics,
            exposure: report_exposure(&paper.account_state(), &marks),
        });
    }

    while let Some(merged) = event_rx.recv().await {
        if control.is_cancelled() {
            control.emit(RunLifecycleEvent::Cancelled {
                run: run.id().clone(),
            });
            let metrics = metrics.snapshot();
            return Ok(PaperReport {
                run: run.id().clone(),
                events_processed: metrics.events_processed,
                fills: metrics.fills,
                metrics,
                exposure: report_exposure(&paper.account_state(), &marks),
            });
        }
        validate_account_owner(run.id(), run.portfolio(), &merged.source)?;
        if observe_reconnect(&merged.source, &mut connection_epochs) {
            metrics.reconnect();
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
            let timestamp_ms = match &envelope.fact {
                CexReferenceEvent::Trade { timestamp_ms, .. } => *timestamp_ms,
            };
            update_loss_breach(
                &paper,
                &marks,
                loss_limit,
                &mut loss_breached,
                store,
                &scope,
                run.id(),
                timestamp_ms,
            )
            .await?;
            for (index, instance) in strategies.iter_mut().enumerate() {
                let positions = paper.positions_for_market(&instance.market);
                let verdicts = dispatch_paper_strategy(
                    run,
                    &paper,
                    store,
                    &scope,
                    instance,
                    &merged.fact,
                    &strategy_books[index],
                    &positions,
                    timestamp_ms,
                    &marks,
                    &limits,
                    loss_limit,
                    &effective_limits_by_strategy,
                    &mut loss_breached,
                    &mut fill_rx,
                    &metrics,
                )
                .await?;
                if let Some(store) = store {
                    let identity = crate::causal::strategy_decision_identity(
                        &scope,
                        "paper-reference",
                        &instance.id,
                        &instance.market,
                        timestamp_ms,
                        &envelope.metadata,
                    );
                    crate::causal::record_book_decision(
                        store,
                        &identity,
                        &strategy_books[index],
                        cex_metrics.snapshot(),
                        verdicts,
                        Some(simulation_config),
                    )
                    .await
                    .map_err(|source| StartError::Storage {
                        run: run.id().clone(),
                        source,
                    })?;
                }
            }
            continue;
        }
        if let SourceEnvelope::PmAccount(envelope) = &merged.source {
            if let PmAccountEvent::Settlement {
                market,
                outcome,
                settled_size,
                proceeds,
                timestamp_ms,
                ..
            } = &envelope.fact
            {
                paper
                    .settle(
                        market.clone(),
                        *outcome,
                        *settled_size,
                        *proceeds,
                        *timestamp_ms,
                    )
                    .map_err(|error| StartError::Storage {
                        run: run.id().clone(),
                        source: corrupt_paper_ledger(&error),
                    })?;
                update_loss_breach(
                    &paper,
                    &marks,
                    loss_limit,
                    &mut loss_breached,
                    store,
                    &scope,
                    run.id(),
                    *timestamp_ms,
                )
                .await?;
                if let Some(store) = store {
                    persist_paper_ledger(store, &scope, &paper)
                        .await
                        .map_err(|source| StartError::Storage {
                            run: run.id().clone(),
                            source,
                        })?;
                } else {
                    paper.drain_ledger();
                }
            }
            continue;
        }
        let SourceEnvelope::PmMarket(envelope) = merged.source else {
            continue;
        };
        let event = envelope.fact;
        metrics.event();
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
            } else {
                marks.remove(&(market.clone(), *outcome));
            }
            let fact = StrategyFact::Market(event.clone());
            let update_result = paper.update_book(market, *outcome, book.clone()).await;
            metrics.set_fills(paper.fill_count());
            update_result.map_err(|source| StartError::ExecutionState {
                run: run.id().clone(),
                source,
            })?;
            update_loss_breach(
                &paper,
                &marks,
                loss_limit,
                &mut loss_breached,
                store,
                &scope,
                run.id(),
                *timestamp_ms,
            )
            .await?;
            if let Some(store) = store {
                persist_paper_ledger(store, &scope, &paper)
                    .await
                    .map_err(|source| StartError::Storage {
                        run: run.id().clone(),
                        source,
                    })?;
            } else {
                paper.drain_ledger();
            }
            drain_fills(&mut fill_rx);
            fills = paper.fill_count();
            metrics.set_fills(fills);
            for (index, instance) in strategies.iter_mut().enumerate() {
                if instance.market != *market {
                    continue;
                }
                strategy_books[index] = book.clone();
                let positions = paper.positions_for_market(market);
                let verdicts = dispatch_paper_strategy(
                    run,
                    &paper,
                    store,
                    &scope,
                    instance,
                    &fact,
                    &strategy_books[index],
                    &positions,
                    *timestamp_ms,
                    &marks,
                    &limits,
                    loss_limit,
                    &effective_limits_by_strategy,
                    &mut loss_breached,
                    &mut fill_rx,
                    &metrics,
                )
                .await?;
                if let Some(store) = store {
                    let identity = crate::causal::strategy_decision_identity(
                        &scope,
                        "paper-market",
                        &instance.id,
                        market,
                        *timestamp_ms,
                        &envelope.metadata,
                    );
                    crate::causal::record_book_decision(
                        store,
                        &identity,
                        &strategy_books[index],
                        cex_metrics.snapshot(),
                        verdicts,
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
    }
    merge
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
    drain_fills(&mut fill_rx);
    fills = paper.fill_count();
    metrics.set_fills(fills);
    control.emit(RunLifecycleEvent::Completed {
        run: run.id().clone(),
    });

    let metrics = metrics.snapshot();
    Ok(PaperReport {
        run: run.id().clone(),
        events_processed: metrics.events_processed,
        fills: metrics.fills,
        metrics,
        exposure: report_exposure(&paper.account_state(), &marks),
    })
}

fn report_exposure(
    account: &pmkit_paper::PaperAccountState,
    marks: &HashMap<(pmkit_core::MarketId, Outcome), Decimal>,
) -> PortfolioExposure {
    let mut notionals = std::collections::HashMap::new();
    for position in &account.positions {
        let entry = notionals
            .entry(position.market.clone())
            .or_insert(Decimal::ZERO);
        *entry += marks
            .get(&(position.market.clone(), position.outcome))
            .map_or(Decimal::ZERO, |mark| position.quantity.abs() * *mark);
    }
    aggregate_exposure(
        &notionals
            .into_iter()
            .map(|(market, notional)| PositionExposure { market, notional })
            .collect::<Vec<_>>(),
        &account
            .resting_orders
            .iter()
            .chain(&account.delayed_orders)
            .filter_map(|order| {
                order.strategy.clone().map(|strategy| ExposureReservation {
                    market: order.market.clone(),
                    strategy,
                    notional: order.remaining_qty * order.price,
                })
            })
            .collect::<Vec<_>>(),
    )
}

#[cfg(test)]
mod ledger_tests {
    use super::{
        persist_paper_ledger, restore_paper_executor, restore_paper_executor_from_decisions,
    };
    use async_trait::async_trait;
    use pmkit_book::{OrderBookL2, Side};
    use pmkit_core::{MarketId, PortfolioId, RunId};
    use pmkit_exec::{Executor, PlaceOrder, TimeInForce};
    use pmkit_market::Outcome;
    use pmkit_money::Money;
    use pmkit_paper::PaperExecutor;
    use pmkit_sim::SimulationConfig;
    use pmkit_store::{
        CausalDecision, CausalIdentity, OwnerScope, StoreError, TapeStore, TursoTapeStore,
    };
    use rust_decimal::Decimal;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;
    use tokio::sync::mpsc;

    struct FailNthDecisionStore {
        inner: TursoTapeStore,
        fail_at: usize,
        attempts: AtomicUsize,
    }

    impl FailNthDecisionStore {
        const fn new(inner: TursoTapeStore, fail_at: usize) -> Self {
            Self {
                inner,
                fail_at,
                attempts: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl TapeStore for FailNthDecisionStore {
        async fn store_envelope(
            &self,
            envelope: &pmkit_store::PmEnvelope,
        ) -> Result<(), StoreError> {
            self.inner.store_envelope(envelope).await
        }

        async fn read_envelopes(
            &self,
            scope: &OwnerScope,
            after: Option<pmkit_store::ReplayCursor>,
            limit: std::num::NonZeroUsize,
        ) -> Result<pmkit_store::ReplayPage, StoreError> {
            self.inner.read_envelopes(scope, after, limit).await
        }

        async fn store_decision(&self, decision: &CausalDecision) -> Result<(), StoreError> {
            if self.attempts.fetch_add(1, Ordering::Relaxed) + 1 == self.fail_at {
                return Err(StoreError::Storage {
                    message: "injected decision write failure".into(),
                });
            }
            self.inner.store_decision(decision).await
        }

        async fn store_intent_pending(
            &self,
            identity: &CausalIdentity,
            payload: &serde_json::Value,
        ) -> Result<(), StoreError> {
            self.inner.store_intent_pending(identity, payload).await
        }

        async fn transition_intent(
            &self,
            identity: &CausalIdentity,
            outcome: pmkit_store::IntentOutcome,
        ) -> Result<(), StoreError> {
            self.inner.transition_intent(identity, outcome).await
        }

        async fn read_pending_intents(
            &self,
            scope: &OwnerScope,
        ) -> Result<Vec<pmkit_store::DurableIntent>, StoreError> {
            self.inner.read_pending_intents(scope).await
        }

        async fn read_unknown_intents(
            &self,
            scope: &OwnerScope,
        ) -> Result<Vec<pmkit_store::DurableIntent>, StoreError> {
            self.inner.read_unknown_intents(scope).await
        }

        async fn read_decisions(
            &self,
            scope: &OwnerScope,
        ) -> Result<Vec<CausalDecision>, StoreError> {
            self.inner.read_decisions(scope).await
        }
    }

    fn book(timestamp_ms: i64) -> OrderBookL2 {
        OrderBookL2 {
            bids: vec![(Decimal::new(40, 2), Decimal::from(100))],
            asks: vec![(Decimal::new(50, 2), Decimal::from(100))],
            timestamp_ms,
            last_trade_price: None,
        }
    }

    fn order(market: MarketId, price: Decimal, qty: Decimal, post_only: bool) -> PlaceOrder {
        PlaceOrder {
            market,
            outcome: Outcome::Up,
            side: Side::Buy,
            price,
            qty,
            post_only,
            tif: TimeInForce::Gtc,
        }
    }

    async fn flush(
        store: &dyn TapeStore,
        scope: &OwnerScope,
        paper: &PaperExecutor,
    ) -> Result<(), StoreError> {
        persist_paper_ledger(store, scope, paper).await
    }

    #[tokio::test]
    async fn paper_full_state_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        // Given a durable paper account with fills, settlement, open orders, and multiple markets.
        let directory = tempdir()?;
        let store = TursoTapeStore::open_local(directory.path().join("paper.db")).await?;
        let scope = OwnerScope::new(PortfolioId::new("alice")?, RunId::new("paper-round-trip")?);
        let config = SimulationConfig {
            activation_latency_ms: 100,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        };
        let (fill_tx, _fill_rx) = mpsc::channel(32);
        let paper =
            PaperExecutor::with_account_fee_config(fill_tx, "paper", config, Money::usdc(100));
        flush(&store, &scope, &paper).await?;

        let settled_market = MarketId::new("btc-5m")?;
        paper
            .update_book(&settled_market, Outcome::Up, book(0))
            .await?;
        let settled_order = order(
            settled_market.clone(),
            Decimal::new(60, 2),
            Decimal::from(2),
            false,
        );
        paper.submit(&settled_order, 0).await?;
        flush(&store, &scope, &paper).await?;
        paper
            .update_book(&settled_market, Outcome::Up, book(100))
            .await?;
        flush(&store, &scope, &paper).await?;
        paper.settle(
            settled_market,
            Outcome::Up,
            Decimal::from(2),
            Decimal::from(2),
            110,
        )?;
        flush(&store, &scope, &paper).await?;

        let held_market = MarketId::new("eth-5m")?;
        paper
            .update_book(&held_market, Outcome::Up, book(200))
            .await?;
        let held_order = order(
            held_market.clone(),
            Decimal::new(60, 2),
            Decimal::from(3),
            false,
        );
        paper.submit(&held_order, 200).await?;
        flush(&store, &scope, &paper).await?;
        paper
            .update_book(&held_market, Outcome::Up, book(300))
            .await?;
        flush(&store, &scope, &paper).await?;

        let resting_market = MarketId::new("sol-5m")?;
        paper
            .update_book(&resting_market, Outcome::Up, book(300))
            .await?;
        paper
            .submit(
                &order(resting_market, Decimal::new(45, 2), Decimal::from(4), true),
                300,
            )
            .await?;
        flush(&store, &scope, &paper).await?;

        let delayed_market = MarketId::new("xrp-5m")?;
        paper
            .update_book(&delayed_market, Outcome::Up, book(300))
            .await?;
        paper
            .submit(
                &order(delayed_market, Decimal::new(60, 2), Decimal::from(5), false),
                300,
            )
            .await?;
        flush(&store, &scope, &paper).await?;
        let before = paper.account_state();

        // When a new executor reconstructs exclusively from the durable records.
        let (restored_tx, _restored_rx) = mpsc::channel(32);
        let restored = restore_paper_executor(&store, &scope, restored_tx, "paper", config)
            .await?
            .ok_or("durable paper ledger was not found")?;
        let after = restored.account_state();

        // Then every derived balance and simulator order state is identical.
        assert_eq!(after, before);
        assert!(after.fees > Money::ZERO);
        assert!(after.realized_pnl > Money::ZERO);
        assert_eq!(after.positions.len(), 1);
        assert_eq!(after.positions[0].market, held_market);
        assert_eq!(after.resting_orders.len(), 1);
        assert_eq!(after.delayed_orders.len(), 1);
        assert_eq!(after.next_order_id, 4);
        drop(store);
        Ok(())
    }

    #[tokio::test]
    async fn paper_ledger_corrupt_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        // Given an order acknowledgement with no preceding placement.
        let directory = tempdir()?;
        let store = TursoTapeStore::open_local(directory.path().join("paper.db")).await?;
        let scope = OwnerScope::new(PortfolioId::new("alice")?, RunId::new("paper-corrupt")?);
        store
            .store_decision(&CausalDecision {
                identity: CausalIdentity {
                    scope: scope.clone(),
                    correlation_id: "paper-ledger-0".into(),
                    source_timestamp_ms: 10,
                    ingest_sequence: 0,
                },
                payload: json!({
                    "record_type": "paper_ledger",
                    "schema_version": 1,
                    "event_id": "paper-ledger-0",
                    "sequence": 0,
                    "timestamp_ms": 10,
                    "event": {
                        "kind": "order_ack",
                        "placement_id": "missing-placement",
                        "order_id": "paper-0",
                        "state": "resting",
                        "active_at_ms": 10
                    }
                }),
            })
            .await?;

        // When reconstruction encounters the inconsistent record.
        let (fill_tx, _fill_rx) = mpsc::channel(8);
        let error = restore_paper_executor(
            &store,
            &scope,
            fill_tx,
            "paper",
            SimulationConfig::default(),
        )
        .await
        .err()
        .ok_or("corrupt ledger unexpectedly restored")?;

        // Then recovery fails closed with the typed store error.
        assert!(matches!(error, StoreError::CorruptPaperLedger { .. }));
        drop(store);
        Ok(())
    }

    #[tokio::test]
    async fn paper_ledger_replay_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
        // Given a valid durable ledger containing a filled order.
        let directory = tempdir()?;
        let store = TursoTapeStore::open_local(directory.path().join("paper.db")).await?;
        let scope = OwnerScope::new(PortfolioId::new("alice")?, RunId::new("paper-idempotent")?);
        let (fill_tx, _fill_rx) = mpsc::channel(8);
        let paper = PaperExecutor::with_account_fee_config(
            fill_tx,
            "paper",
            SimulationConfig::default(),
            Money::usdc(10),
        );
        let market = MarketId::new("btc-5m")?;
        paper.update_book(&market, Outcome::Up, book(0)).await?;
        paper
            .submit(&order(market, Decimal::new(60, 2), Decimal::ONE, false), 0)
            .await?;
        flush(&store, &scope, &paper).await?;
        let decisions = store.read_decisions(&scope).await?;
        let expected = paper.account_state();
        let mut duplicated = decisions.clone();
        duplicated.extend(decisions);

        // When every durable record is replayed twice.
        let (restored_tx, _restored_rx) = mpsc::channel(8);
        let restored = restore_paper_executor_from_decisions(
            &duplicated,
            restored_tx,
            "paper",
            SimulationConfig::default(),
        )?
        .ok_or("durable paper ledger was not found")?;

        // Then fills and cash effects are applied exactly once.
        assert_eq!(restored.account_state(), expected);
        drop(store);
        Ok(())
    }

    #[tokio::test]
    async fn paper_ledger_retry_preserves_unwritten_tail_after_storage_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given a store that fails after durably accepting the first entry of an order batch.
        let directory = tempdir()?;
        let store = FailNthDecisionStore::new(
            TursoTapeStore::open_local(directory.path().join("paper.db")).await?,
            3,
        );
        let scope = OwnerScope::new(
            PortfolioId::new("alice")?,
            RunId::new("paper-retry-after-failure")?,
        );
        let (fill_tx, _fill_rx) = mpsc::channel(8);
        let paper = PaperExecutor::with_account_fee_config(
            fill_tx,
            "paper",
            SimulationConfig::default(),
            Money::usdc(10),
        );
        flush(&store, &scope, &paper).await?;
        let market = MarketId::new("btc-5m")?;
        paper.update_book(&market, Outcome::Up, book(0)).await?;
        paper
            .submit(&order(market, Decimal::new(45, 2), Decimal::ONE, true), 1)
            .await?;
        let expected = paper.account_state();

        // When the acknowledgement write fails and persistence is retried.
        let failure = flush(&store, &scope, &paper).await;
        assert!(matches!(failure, Err(StoreError::Storage { .. })));
        flush(&store, &scope, &paper).await?;

        // Then the durable ledger has the exact ordered tail once and restart rebuilds all state.
        let decisions = store.read_decisions(&scope).await?;
        assert_eq!(
            decisions
                .iter()
                .map(|decision| decision.identity.correlation_id.as_str())
                .collect::<Vec<_>>(),
            vec!["paper-ledger-0", "paper-ledger-1", "paper-ledger-2"]
        );
        let (restored_tx, _restored_rx) = mpsc::channel(8);
        let restored = restore_paper_executor(
            &store,
            &scope,
            restored_tx,
            "paper",
            SimulationConfig::default(),
        )
        .await?
        .ok_or("durable paper ledger was not found")?;
        assert_eq!(restored.account_state(), expected);
        drop(store);
        Ok(())
    }
}
