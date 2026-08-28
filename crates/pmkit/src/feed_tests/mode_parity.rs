use crate::{
    RunControl, backtest, live, paper,
    test_support::{config, risk},
};
use async_trait::async_trait;
use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
use pmkit_data::{
    DataSourceError, HistoricalDataSource, LiveCexDataSource, LiveDataSource, ReplayQuery,
    SourceSignal,
};
use pmkit_event::{
    CexReferenceEnvelope, CexReferenceEvent, MarketEvent, PolymarketReferenceEnvelope,
    PolymarketTwapEvent, SourceEnvelope, StrategyFact, StreamMetadata,
};
use pmkit_exec::{ExecError, ExecutionSnapshot, Executor, OrderId, PlaceOrder};
use pmkit_market::{Asset, Exchange, Outcome};
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

struct ParityReference {
    source_id: &'static str,
    timestamp_ms: i64,
    rank: i64,
}

struct ParityRtdsReference {
    timestamp_ms: i64,
    rank: i64,
}

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
        if query.markets.is_empty() {
            return Err(DataSourceError::NotAvailable);
        }
        for market in query.markets {
            sink.send(SourceSignal::market_event(MarketEvent::BookUpdate {
                market,
                outcome: Outcome::Up,
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

async fn emit_reference(
    source: &ParityReference,
    sink: Sender<SourceSignal>,
) -> Result<(), DataSourceError> {
    sink.send(SourceSignal::Data(Box::new(SourceEnvelope::CexReference(
        CexReferenceEnvelope {
            metadata: StreamMetadata {
                schema_version: 1,
                source_id: source.source_id.into(),
                source_time_ms: source.timestamp_ms,
                canonical_source_rank: source.rank,
                receipt_time_ms: 2_000,
                connection_id: source.source_id.into(),
                connection_epoch: 0,
                frame_sequence: source.rank,
                ingest_sequence: source.rank.cast_unsigned(),
            },
            fact: CexReferenceEvent::Trade {
                asset: Asset::Btc,
                exchange: Exchange::Binance,
                aggregate_trade_id: 9,
                price: Decimal::from(42),
                qty: Decimal::ONE,
                is_buyer_maker: false,
                timestamp_ms: source.timestamp_ms,
            },
        },
    ))))
    .await
    .map_err(|_| DataSourceError::SinkClosed)?;
    sink.send(SourceSignal::Watermark(i64::MAX))
        .await
        .map_err(|_| DataSourceError::SinkClosed)?;
    sink.send(SourceSignal::Eof)
        .await
        .map_err(|_| DataSourceError::SinkClosed)
}

#[async_trait]
impl HistoricalDataSource for ParityReference {
    async fn replay(
        &self,
        _query: ReplayQuery,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        emit_reference(self, sink).await
    }
}

#[async_trait]
impl LiveCexDataSource for ParityReference {
    async fn subscribe_reference(&self, sink: Sender<SourceSignal>) -> Result<(), DataSourceError> {
        emit_reference(self, sink).await
    }
}

async fn emit_rtds_reference(
    source: &ParityRtdsReference,
    sink: Sender<SourceSignal>,
) -> Result<(), DataSourceError> {
    sink.send(SourceSignal::Data(Box::new(
        SourceEnvelope::PolymarketReference(PolymarketReferenceEnvelope {
            metadata: StreamMetadata {
                schema_version: 1,
                source_id: "rtds-parity".into(),
                source_time_ms: source.timestamp_ms,
                canonical_source_rank: source.rank,
                receipt_time_ms: source.timestamp_ms + 1,
                connection_id: "rtds-parity".into(),
                connection_epoch: 0,
                frame_sequence: 1,
                ingest_sequence: 1,
            },
            fact: PolymarketTwapEvent {
                asset: Asset::Btc,
                symbol: "btc/usd".into(),
                timestamp_ms: source.timestamp_ms,
                provider_timestamp_ms: source.timestamp_ms,
                value: 42.0,
                full_accuracy_value: "42000000000000000000".into(),
                window_s: 60,
            },
        }),
    )))
    .await
    .map_err(|_| DataSourceError::SinkClosed)?;
    sink.send(SourceSignal::Watermark(i64::MAX))
        .await
        .map_err(|_| DataSourceError::SinkClosed)?;
    sink.send(SourceSignal::Eof)
        .await
        .map_err(|_| DataSourceError::SinkClosed)
}

#[async_trait]
impl HistoricalDataSource for ParityRtdsReference {
    async fn replay(
        &self,
        _query: ReplayQuery,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        emit_rtds_reference(self, sink).await
    }
}

#[async_trait]
impl LiveCexDataSource for ParityRtdsReference {
    async fn subscribe_reference(&self, sink: Sender<SourceSignal>) -> Result<(), DataSourceError> {
        emit_rtds_reference(self, sink).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Observation {
    fact: StrategyFact,
    market: MarketId,
    empty_book: bool,
    position_count: usize,
    timestamp_ms: i64,
}

struct FactProbe(Arc<Mutex<Vec<Observation>>>);

impl Strategy for FactProbe {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        let observation = Observation {
            fact: context.fact.clone(),
            market: context.market.clone(),
            empty_book: context.book.bids.is_empty()
                && context.book.asks.is_empty()
                && context.book.last_trade_price.is_none(),
            position_count: context.positions.len(),
            timestamp_ms: context.now.0,
        };
        match self.0.lock() {
            Ok(mut facts) => facts.push(observation),
            Err(poisoned) => poisoned.into_inner().push(observation),
        }
        Ok(Actions::none())
    }
}

struct FactProbeFactory(Arc<Mutex<Vec<Observation>>>);

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
        market_limits: None,
    }
}

fn registration(
    facts: &Arc<Mutex<Vec<Observation>>>,
    strategy: &str,
    market: &str,
) -> Result<StrategyRegistration, Box<dyn std::error::Error>> {
    Ok(StrategyRegistration::new(
        StrategyId::new(strategy)?,
        MarketId::new(market)?,
        Arc::new(FactProbeFactory(Arc::clone(facts))),
    ))
}

fn captured(facts: &Mutex<Vec<Observation>>) -> Vec<Observation> {
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

#[expect(clippy::too_many_lines)]
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
        )
        .reference_source(Arc::new(ParityReference {
            source_id: "binance-parity",
            timestamp_ms: 2_000,
            rank: 1,
        }))
        .reference_source_named(
            "rtds",
            Arc::new(ParityRtdsReference {
                timestamp_ms: 2_001,
                rank: 2,
            }),
        );
        let backtest_run = BacktestRun::new(
            RunId::new("parity-backtest")?,
            portfolio.clone(),
            replay,
            Money::usdc(1_000),
            risk()?,
            simulation(),
        )
        .strategy(registration(&backtest_facts, "btc-probe", "btc-5m")?)
        .strategy(registration(&backtest_facts, "eth-probe", "eth-5m")?);
        let paper_run = PaperRun::new(
            RunId::new("parity-paper")?,
            portfolio.clone(),
            Money::usdc(1_000),
            risk()?,
            Arc::new(ParitySource),
            simulation(),
        )
        .reference_data(Arc::new(ParityReference {
            source_id: "binance-parity",
            timestamp_ms: 2_000,
            rank: 1,
        }))
        .reference_data_named(
            "rtds",
            Arc::new(ParityRtdsReference {
                timestamp_ms: 2_001,
                rank: 2,
            }),
        )
        .strategy(registration(&paper_facts, "btc-probe", "btc-5m")?)
        .strategy(registration(&paper_facts, "eth-probe", "eth-5m")?);
        let live_run = LiveRun::new(
            RunId::new("parity-live")?,
            portfolio.clone(),
            Arc::new(NoopExecutor),
            Arc::new(ParitySource),
            risk()?,
        )
        .reference_data(Arc::new(ParityReference {
            source_id: "binance-parity",
            timestamp_ms: 2_000,
            rank: 1,
        }))
        .reference_data_named(
            "rtds",
            Arc::new(ParityRtdsReference {
                timestamp_ms: 2_001,
                rank: 2,
            }),
        )
        .strategy(registration(&live_facts, "btc-probe", "btc-5m")?)
        .strategy(registration(&live_facts, "eth-probe", "eth-5m")?);

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
            [2, 2, 2]
        );
        let expected_facts = captured(&backtest_facts);
        assert_eq!(captured(&paper_facts), expected_facts);
        assert_eq!(captured(&live_facts), expected_facts);
        let rtds = expected_facts
            .iter()
            .filter(|observation| matches!(observation.fact, StrategyFact::PolymarketReference(_)))
            .collect::<Vec<_>>();
        assert_eq!(rtds.len(), 2);
        assert_eq!(
            rtds.iter()
                .map(|observation| observation.market.to_string())
                .collect::<Vec<_>>(),
            ["btc-5m", "eth-5m"]
        );
        assert!(rtds.iter().all(|observation| observation.empty_book));
        assert!(
            rtds.iter()
                .all(|observation| observation.position_count == 0)
        );
        assert!(
            rtds.iter()
                .all(|observation| observation.timestamp_ms == 2_001)
        );
        assert_eq!(decisions[1], decisions[0]);
        assert_eq!(decisions[2], decisions[0]);
        store.delete_database()?;
    }
    Ok(())
}
