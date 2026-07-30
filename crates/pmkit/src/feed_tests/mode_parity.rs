use crate::{
    RunControl, backtest, live, paper,
    test_support::{config, risk},
};
use async_trait::async_trait;
use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
use pmkit_data::{
    DataSourceError, HistoricalDataSource, LiveDataSource, ReplayQuery, SourceSignal,
};
use pmkit_event::{MarketEvent, StrategyFact};
use pmkit_exec::{ExecError, ExecutionSnapshot, Executor, OrderId, PlaceOrder};
use pmkit_market::Outcome;
use pmkit_money::Money;
use pmkit_run::{EvidenceRequirement, RetrievalWait};
use pmkit_runtime::StrategyRegistration;
use pmkit_spec::{BacktestRun, ConservativeV1Config, LiveRun, PaperRun, ReplaySpec};
use pmkit_store::{CausalDecision, OwnerScope, TapeStore, TursoTapeStore};
use pmkit_strategy::{
    Actions, Strategy, StrategyContext, StrategyError, StrategyFactory, StrategyInitError,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::Sender;

struct ParitySource;

async fn emit_market(
    market: MarketId,
    outcome: Outcome,
    sink: Sender<SourceSignal>,
) -> Result<(), DataSourceError> {
    if outcome == Outcome::Up {
        sink.send(SourceSignal::market_event(MarketEvent::BookUpdate {
            market,
            outcome,
            bids: vec![(Decimal::new(49, 2), Decimal::from(20))],
            asks: vec![(Decimal::new(51, 2), Decimal::from(20))],
            timestamp_ms: 1_000,
        }))
        .await
        .map_err(|_| DataSourceError::SinkClosed)?;
    }
    sink.send(SourceSignal::Watermark(i64::MAX))
        .await
        .map_err(|_| DataSourceError::SinkClosed)?;
    sink.send(SourceSignal::Eof)
        .await
        .map_err(|_| DataSourceError::SinkClosed)
}

#[async_trait]
impl HistoricalDataSource for ParitySource {
    async fn replay(
        &self,
        query: ReplayQuery,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        let market = query
            .markets
            .first()
            .cloned()
            .ok_or(DataSourceError::NotAvailable)?;
        emit_market(market, Outcome::Up, sink).await
    }
}

#[async_trait]
impl LiveDataSource for ParitySource {
    async fn subscribe(
        &self,
        market: MarketId,
        outcome: Outcome,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        emit_market(market, outcome, sink).await
    }
}

struct FactProbe(Arc<Mutex<Vec<StrategyFact>>>);

impl Strategy for FactProbe {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        match self.0.lock() {
            Ok(mut facts) => facts.push(context.fact.clone()),
            Err(poisoned) => poisoned.into_inner().push(context.fact.clone()),
        }
        Ok(Actions::none())
    }
}

struct FactProbeFactory(Arc<Mutex<Vec<StrategyFact>>>);

impl StrategyFactory for FactProbeFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(FactProbe(Arc::clone(&self.0))))
    }
}

struct NoopExecutor;

#[async_trait]
impl Executor for NoopExecutor {
    async fn preflight(&self) -> Result<ExecutionSnapshot, ExecError> {
        Ok(ExecutionSnapshot::default())
    }

    async fn reconcile(&self) -> Result<ExecutionSnapshot, ExecError> {
        Ok(ExecutionSnapshot::default())
    }

    async fn submit(&self, _order: &PlaceOrder, _now_ms: i64) -> Result<OrderId, ExecError> {
        Ok(OrderId("unused".into()))
    }

    async fn cancel(&self, _order_id: &OrderId) -> Result<(), ExecError> {
        Ok(())
    }

    async fn cancel_all(&self) -> Result<(), ExecError> {
        Ok(())
    }
}

fn simulation() -> ConservativeV1Config {
    ConservativeV1Config {
        activation_latency: Duration::ZERO,
        maker_queue_ahead_bps: 0,
        slippage_bps: 0,
        market_impact_bps: 0,
        fee_model: None,
        min_order_size: None,
        tick_size: None,
    }
}

fn registration(
    facts: &Arc<Mutex<Vec<StrategyFact>>>,
) -> Result<StrategyRegistration, Box<dyn std::error::Error>> {
    Ok(StrategyRegistration::new(
        StrategyId::new("parity-probe")?,
        MarketId::new("btc-5m")?,
        Arc::new(FactProbeFactory(Arc::clone(facts))),
    ))
}

fn captured(facts: &Mutex<Vec<StrategyFact>>) -> Vec<StrategyFact> {
    match facts.lock() {
        Ok(facts) => facts.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    }
}

fn portable_decision(decision: &CausalDecision) -> Value {
    json!({
        "timing": decision.payload["snapshot"]["timing"].clone(),
        "pm_book": decision.payload["snapshot"]["pm_book"].clone(),
        "cex_trade": decision.payload["snapshot"]["cex_trade"].clone(),
        "decision": decision.payload["decision"].clone(),
    })
}

#[tokio::test]
async fn actual_modes_observe_identical_facts_and_portable_decisions()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: equivalent runs using the actual backtest, paper, and live drivers.
    let directory = tempfile::tempdir()?;
    {
        let store = TursoTapeStore::open_local(directory.path().join("mode-parity.db")).await?;
        let portfolio = PortfolioId::new("parity")?;
        let backtest_facts = Arc::new(Mutex::new(Vec::new()));
        let paper_facts = Arc::new(Mutex::new(Vec::new()));
        let live_facts = Arc::new(Mutex::new(Vec::new()));
        let replay = ReplaySpec::new(
            Arc::new(ParitySource),
            "2026-01-01T00:00:00Z".parse()?,
            "2026-02-01T00:00:00Z".parse()?,
            EvidenceRequirement::CorroboratedOnly,
            RetrievalWait::ReturnPending,
        );
        let backtest_run = BacktestRun::new(
            RunId::new("parity-backtest")?,
            portfolio.clone(),
            replay,
            Money::usdc(1_000),
            risk()?,
            simulation(),
        )
        .strategy(registration(&backtest_facts)?);
        let paper_run = PaperRun::new(
            RunId::new("parity-paper")?,
            portfolio.clone(),
            Money::usdc(1_000),
            risk()?,
            Arc::new(ParitySource),
            simulation(),
        )
        .strategy(registration(&paper_facts)?);
        let live_run = LiveRun::new(
            RunId::new("parity-live")?,
            portfolio.clone(),
            Arc::new(NoopExecutor),
            Arc::new(ParitySource),
            risk()?,
        )
        .strategy(registration(&live_facts)?);

        // When: each driver persists the fact and its causal decision.
        let control = RunControl::default();
        let backtest_report =
            backtest::drive_with_control(&backtest_run, Some(&store), &control).await?;
        let paper_report = paper::drive_with_control(&paper_run, Some(&store), &control).await?;
        let live_report =
            live::drive_with_control(&live_run, &config()?, Some(&store), &control).await?;
        let mut decisions = Vec::new();
        for run in ["parity-backtest", "parity-paper", "parity-live"] {
            let scope = OwnerScope::new(portfolio.clone(), RunId::new(run)?);
            let decision = store
                .read_decisions(&scope)
                .await?
                .into_iter()
                .find(|decision| decision.payload["snapshot"].is_object())
                .ok_or("missing parity decision")?;
            decisions.push(portable_decision(&decision));
        }

        // Then: every driver exposes the same fact and mode-independent decision.
        assert_eq!(
            [
                backtest_report.events_processed,
                paper_report.events_processed,
                live_report.events_processed,
            ],
            [1, 1, 1]
        );
        let expected_facts = captured(&backtest_facts);
        assert_eq!(captured(&paper_facts), expected_facts);
        assert_eq!(captured(&live_facts), expected_facts);
        assert!(matches!(
            expected_facts.as_slice(),
            [StrategyFact::Market(_)]
        ));
        assert_eq!(decisions[1], decisions[0]);
        assert_eq!(decisions[2], decisions[0]);
        store.delete_database()?;
    }
    Ok(())
}
