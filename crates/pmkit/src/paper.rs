use super::{
    PaperReport, RunControl, RunLifecycleEvent, StartError, instantiate_strategies, store_signal,
};
use crate::feed::{FeedMode, MergedFeed, SourceTaskDefinition};
use pmkit_book::OrderBookL2;
use pmkit_event::{MarketEvent, PmAccountEvent, SourceEnvelope, StrategyFact};
use pmkit_exec::{ExecError, Executor};
use pmkit_market::Outcome;
use pmkit_paper::{PaperExecutor, PaperLedgerEntry, PaperLedgerError};
use pmkit_sim::MarketCategory;
use pmkit_sim::SimulationConfig;
use pmkit_spec::PaperRun;
use pmkit_store::{CausalDecision, CausalIdentity, OwnerScope, StoreError, TapeStore};
use pmkit_strategy::{Action, LogicalTimestamp, StrategyContext};
use std::collections::HashSet;

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
    for entry in paper.drain_ledger() {
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
    }
    Ok(())
}

async fn restore_paper_executor(
    store: &dyn TapeStore,
    scope: &OwnerScope,
    fills: tokio::sync::mpsc::Sender<MarketEvent>,
    id_prefix: &str,
    category: MarketCategory,
    config: SimulationConfig,
) -> Result<Option<PaperExecutor>, StoreError> {
    let decisions = store.read_decisions(scope).await?;
    restore_paper_executor_from_decisions(&decisions, fills, id_prefix, category, config)
}

fn restore_paper_executor_from_decisions(
    decisions: &[CausalDecision],
    fills: tokio::sync::mpsc::Sender<MarketEvent>,
    id_prefix: &str,
    category: MarketCategory,
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
    PaperExecutor::reconstruct(fills, id_prefix, category, config, &entries)
        .map(Some)
        .map_err(|error| corrupt_paper_ledger(&error))
}

fn corrupt_paper_ledger(error: &PaperLedgerError) -> StoreError {
    StoreError::CorruptPaperLedger {
        message: error.to_string(),
    }
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

    let (fill_tx, mut fill_rx) = tokio::sync::mpsc::channel(1024);
    let simulation = run.simulation();
    let simulation_config = SimulationConfig {
        activation_latency_ms: i64::try_from(simulation.activation_latency.as_millis())
            .unwrap_or(i64::MAX),
        maker_queue_ahead_bps: simulation.maker_queue_ahead_bps,
        slippage_bps: simulation.slippage_bps,
        market_impact_bps: simulation.market_impact_bps,
    };
    let scope = OwnerScope::new(run.portfolio().clone(), run.id().clone());
    let paper = if let Some(store) = store {
        restore_paper_executor(
            store,
            &scope,
            fill_tx.clone(),
            "paper",
            MarketCategory::Crypto,
            simulation_config,
        )
        .await
        .map_err(|source| StartError::Storage {
            run: run.id().clone(),
            source,
        })?
        .unwrap_or_else(|| {
            PaperExecutor::with_account_config(
                fill_tx,
                "paper",
                MarketCategory::Crypto,
                simulation_config,
                run.initial_cash(),
            )
        })
    } else {
        PaperExecutor::with_account_config(
            fill_tx,
            "paper",
            MarketCategory::Crypto,
            simulation_config,
            run.initial_cash(),
        )
    };
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

    let mut events_processed = 0_usize;
    let mut fills = paper.fill_count();
    let mut cex_metrics = crate::causal::CexTradeMetricsState::default();
    control.emit(RunLifecycleEvent::Started {
        run: run.id().clone(),
    });
    if control.is_cancelled() {
        control.emit(RunLifecycleEvent::Cancelled {
            run: run.id().clone(),
        });
        return Ok(PaperReport {
            run: run.id().clone(),
            events_processed,
            fills,
        });
    }

    while let Some(merged) = event_rx.recv().await {
        if control.is_cancelled() {
            control.emit(RunLifecycleEvent::Cancelled {
                run: run.id().clone(),
            });
            return Ok(PaperReport {
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
        if let SourceEnvelope::PmAccount(envelope) = &merged.source {
            if let PmAccountEvent::Settlement {
                market,
                outcome,
                settled_size,
                proceeds,
                timestamp_ms,
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
            let update_result = paper.update_book(market, *outcome, book.clone()).await;
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
            update_result.map_err(|source| StartError::ExecutionState {
                run: run.id().clone(),
                source,
            })?;
            drain_fills(&mut fill_rx);
            fills = paper.fill_count();
            let mut actions_placed = 0_u32;
            for instance in &mut *strategies {
                if instance.market != *market {
                    continue;
                }
                let positions = paper.positions_for_market(market);
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
                            let submit_result = paper.submit(order, *timestamp_ms).await;
                            if let Some(store) = store {
                                persist_paper_ledger(store, &scope, &paper).await.map_err(
                                    |source| StartError::Storage {
                                        run: run.id().clone(),
                                        source,
                                    },
                                )?;
                            } else {
                                paper.drain_ledger();
                            }
                            match submit_result {
                                Ok(_) => {
                                    actions_placed = actions_placed.saturating_add(1);
                                }
                                Err(ExecError::Rejected { .. }) => {}
                                Err(source) => {
                                    return Err(StartError::ExecutionState {
                                        run: run.id().clone(),
                                        source,
                                    });
                                }
                            }
                        }
                    }
                }
                drain_fills(&mut fill_rx);
                fills = paper.fill_count();
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
    drain_fills(&mut fill_rx);
    fills = paper.fill_count();
    control.emit(RunLifecycleEvent::Completed {
        run: run.id().clone(),
    });

    Ok(PaperReport {
        run: run.id().clone(),
        events_processed,
        fills,
    })
}

#[cfg(test)]
mod ledger_tests {
    use super::{
        persist_paper_ledger, restore_paper_executor, restore_paper_executor_from_decisions,
    };
    use pmkit_book::{OrderBookL2, Side};
    use pmkit_core::{MarketId, PortfolioId, RunId};
    use pmkit_exec::{Executor, PlaceOrder};
    use pmkit_market::Outcome;
    use pmkit_money::Money;
    use pmkit_paper::PaperExecutor;
    use pmkit_sim::{MarketCategory, SimulationConfig};
    use pmkit_store::{
        CausalDecision, CausalIdentity, OwnerScope, StoreError, TapeStore, TursoTapeStore,
    };
    use rust_decimal::Decimal;
    use serde_json::json;
    use tempfile::tempdir;
    use tokio::sync::mpsc;

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
    #[expect(
        clippy::too_many_lines,
        reason = "one round trip asserts every reconstructed account and order-state dimension"
    )]
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
        };
        let (fill_tx, _fill_rx) = mpsc::channel(32);
        let paper = PaperExecutor::with_account_config(
            fill_tx,
            "paper",
            MarketCategory::Crypto,
            config,
            Money::usdc(100),
        );
        flush(&store, &scope, &paper).await?;

        let settled_market = MarketId::new("btc-5m")?;
        paper
            .update_book(&settled_market, Outcome::Up, book(0))
            .await?;
        paper
            .submit(
                &order(
                    settled_market.clone(),
                    Decimal::new(60, 2),
                    Decimal::from(2),
                    false,
                ),
                0,
            )
            .await?;
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
        paper
            .submit(
                &order(
                    held_market.clone(),
                    Decimal::new(60, 2),
                    Decimal::from(3),
                    false,
                ),
                200,
            )
            .await?;
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
        let restored = restore_paper_executor(
            &store,
            &scope,
            restored_tx,
            "paper",
            MarketCategory::Crypto,
            config,
        )
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
            MarketCategory::Crypto,
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
        let paper = PaperExecutor::with_account_config(
            fill_tx,
            "paper",
            MarketCategory::Crypto,
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
            MarketCategory::Crypto,
            SimulationConfig::default(),
        )?
        .ok_or("durable paper ledger was not found")?;

        // Then fills and cash effects are applied exactly once.
        assert_eq!(restored.account_state(), expected);
        drop(store);
        Ok(())
    }
}
