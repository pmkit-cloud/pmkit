use super::{PaperReport, StartError, absorb_fills, instantiate_strategies, store_signal};
use crate::feed::{FeedMode, MergedFeed, SourceTaskDefinition};
use pmkit_book::OrderBookL2;
use pmkit_event::{MarketEvent, SourceEnvelope, StrategyFact};
use pmkit_exec::Executor;
use pmkit_market::Outcome;
use pmkit_paper::PaperExecutor;
use pmkit_sim::MarketCategory;
use pmkit_sim::SimulationConfig;
use pmkit_spec::PaperRun;
use pmkit_store::{CausalIdentity, OwnerScope, TapeStore};
use pmkit_strategy::{Action, LogicalTimestamp, StrategyContext};
use std::collections::HashSet;

fn drain_fills(rx: &mut tokio::sync::mpsc::Receiver<MarketEvent>) -> Vec<MarketEvent> {
    let mut fills = Vec::new();
    while let Ok(event) = rx.try_recv() {
        fills.push(event);
    }
    fills
}

#[expect(
    clippy::too_many_lines,
    reason = "the paper run owns one ordered feed, executor, strategy, and recording loop"
)]
pub async fn drive(
    run: &PaperRun,
    store: Option<&dyn TapeStore>,
) -> Result<PaperReport, StartError> {
    let mut strategies = instantiate_strategies(run.strategies(), run.id())?;

    let (fill_tx, mut fill_rx) = tokio::sync::mpsc::channel(1024);
    let simulation = run.simulation();
    let simulation_config = SimulationConfig {
        activation_latency_ms: i64::try_from(simulation.activation_latency.as_millis())
            .unwrap_or(i64::MAX),
        maker_queue_ahead_bps: simulation.maker_queue_ahead_bps,
        slippage_bps: simulation.slippage_bps,
        market_impact_bps: simulation.market_impact_bps,
    };
    let paper =
        PaperExecutor::with_config(fill_tx, "paper", MarketCategory::Crypto, simulation_config);

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
    if let Some(reference) = run.reference_data_ref() {
        let reference = reference.clone();
        sources.push(SourceTaskDefinition::new("cex", move |sink| async move {
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
    let feed = MergedFeed::from_tasks(FeedMode::Paper, sources, None);
    let merge = tokio::spawn(async move { feed.forward(event_tx).await });

    // ponytail: fee category fixed to Crypto; positions tracked; fill buffer bounded at 1024.
    let mut positions = Vec::new();
    let mut events_processed = 0_usize;
    let mut fills = 0_usize;
    let mut cex_metrics = crate::causal::CexTradeMetricsState::default();
    let scope = OwnerScope::new(run.portfolio().clone(), run.id().clone());

    while let Some(merged) = event_rx.recv().await {
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
            let fact = StrategyFact::Market(event.clone());
            let _ = paper.update_book(market, *outcome, book.clone()).await;
            fills += absorb_fills(&drain_fills(&mut fill_rx), &mut positions);
            let mut actions_placed = 0_u32;
            for instance in &mut *strategies {
                if instance.market != *market {
                    continue;
                }
                let context = StrategyContext {
                    fact: &fact,
                    market,
                    book: &book,
                    positions: &positions,
                    now: LogicalTimestamp::from_millis(*timestamp_ms),
                };
                if let Ok(actions) = instance.strategy.on_event(context) {
                    for action in actions.as_slice() {
                        if let Action::Place(order) = action {
                            let _ = paper.submit(order, *timestamp_ms).await;
                            actions_placed = actions_placed.saturating_add(1);
                        }
                    }
                }
                fills += absorb_fills(&drain_fills(&mut fill_rx), &mut positions);
            }
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
    fills += absorb_fills(&drain_fills(&mut fill_rx), &mut positions);

    Ok(PaperReport {
        run: run.id().clone(),
        events_processed,
        fills,
    })
}
