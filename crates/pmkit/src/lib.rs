//! `PMKit` orchestration engine.
//!
//! [`Pmkit::builder`] collects [`RunSpec`]s; [`PmkitBuilder::start`] validates
//! them and drives each backtest end to end (replay -> simulation -> strategy
//! -> fills), returning an [`AppHandle`] that exposes each run's
//! [`RunReport`]. Paper runs are driven similarly against a live data source;
//! live runs route strategy orders through a consented executor behind a
//! risk gate with bounded execution-state reconciliation and optional tapes.

mod backtest;
/// Portable causal decision snapshots and durable execution recording.
pub mod causal;
/// Deterministic envelope-aware source merging.
pub mod feed;
pub(crate) mod live;
mod paper;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use pmkit_core::{MarketId, RunId};
use pmkit_data::SourceSignal;
use pmkit_event::{MarketEvent, SourceEnvelope};
use pmkit_exec::ExecError;
use pmkit_manifest::build_manifest;
use pmkit_run::LiveConsent;
use pmkit_runtime::{RuntimeConfig, StrategyRegistration};
use pmkit_spec::RunSpec;
use pmkit_store::{OwnerScope, PmEnvelope, StoreError, TapeStore};
use pmkit_strategy::Strategy;
use thiserror::Error;

/// A registered strategy instance keyed by its exact market.
type StrategyInstance = (MarketId, Box<dyn Strategy>);

pub use pmkit_data::ReplayQuery;

/// The terminal report of a backtest run.
#[derive(Debug, Clone)]
pub struct BacktestReport {
    /// The run this report belongs to.
    pub run: RunId,
    /// Total events consumed from the replay.
    pub events_processed: usize,
    /// Total simulated fills produced.
    pub fills: usize,
}

/// The terminal report of a paper run.
#[derive(Debug, Clone)]
pub struct PaperReport {
    /// The run this report belongs to.
    pub run: RunId,
    /// Total events consumed from the live feed.
    pub events_processed: usize,
    /// Total simulated fills produced.
    pub fills: usize,
}

/// The terminal report of a live run.
#[derive(Debug, Clone)]
pub struct LiveReport {
    /// The run this report belongs to.
    pub run: RunId,
    /// Total events consumed from the live feed.
    pub events_processed: usize,
    /// Total venue fills observed.
    pub fills: usize,
    /// Orders rejected by the risk gate before reaching the venue.
    pub rejected: usize,
}

/// The terminal report of a run.
#[derive(Debug, Clone)]
pub enum RunReport {
    /// A completed backtest.
    Backtest(BacktestReport),
    /// A completed paper run.
    Paper(PaperReport),
    /// A completed live run.
    Live(LiveReport),
}

/// A failure raised while starting runs.
#[derive(Debug, Error)]
pub enum StartError {
    /// Two runs shared the same [`RunId`].
    #[error("duplicate run id: {0}")]
    DuplicateRunId(RunId),
    /// A strategy factory failed during instantiation.
    #[error("strategy init failed for run {run}: {source}")]
    StrategyInit {
        /// The run whose strategy failed.
        run: RunId,
        /// The underlying factory error.
        source: pmkit_strategy::StrategyInitError,
    },
    /// A live run was configured without calling `enable_live`.
    #[error("live run {0} requires enable_live consent")]
    LiveConsentMissing(RunId),
    /// Live execution state could not be established safely.
    #[error("execution state unavailable for run {run}: {source}")]
    ExecutionState {
        /// The run whose executor could not establish state.
        run: RunId,
        /// The underlying executor failure.
        source: ExecError,
    },
    /// Live tape capture failed while configured as required.
    #[error("live tape failed for run {run}: {source}")]
    Tape {
        /// The run whose tape failed.
        run: RunId,
        /// The underlying filesystem failure.
        source: std::io::Error,
    },
    /// Configured durable PM storage failed.
    #[error("storage failed for run {run}: {source}")]
    Storage {
        /// The run whose durable storage failed.
        run: RunId,
        /// The underlying store failure.
        source: StoreError,
    },
}

/// A failure raised while interacting with a started runtime.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RuntimeError {
    /// The requested run is not part of this application.
    #[error("unknown run id: {0}")]
    UnknownRun(RunId),
}

/// Entry point to the engine.
#[derive(Debug)]
pub struct Pmkit;

impl Pmkit {
    /// Starts a builder with the given runtime configuration.
    #[must_use]
    pub const fn builder(config: RuntimeConfig) -> PmkitBuilder {
        PmkitBuilder {
            config,
            runs: Vec::new(),
            consent: None,
            store: None,
        }
    }
}

/// Collects runs before starting the engine.
pub struct PmkitBuilder {
    config: RuntimeConfig,
    runs: Vec<RunSpec>,
    consent: Option<LiveConsent>,
    store: Option<Arc<dyn TapeStore>>,
}

impl std::fmt::Debug for PmkitBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PmkitBuilder")
            .field("config", &self.config)
            .field("runs", &self.runs)
            .field("consent", &self.consent)
            .field("store", &self.store.as_ref().map(|_| "configured"))
            .finish()
    }
}

impl PmkitBuilder {
    /// Adds a run to the topology.
    #[must_use]
    pub fn run(mut self, spec: impl Into<RunSpec>) -> Self {
        self.runs.push(spec.into());
        self
    }

    /// Records explicit consent to place real live orders.
    #[must_use]
    pub const fn enable_live(mut self, consent: LiveConsent) -> Self {
        self.consent = Some(consent);
        self
    }

    /// Opts every configured run into durable PM-envelope and causal-decision storage.
    #[must_use]
    pub fn storage(mut self, store: Arc<dyn TapeStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Validates the topology and drives every run to completion.
    ///
    /// # Errors
    ///
    /// Returns [`StartError`] on a duplicate run id or a strategy that fails to
    /// initialise.
    pub async fn start(self) -> Result<AppHandle, StartError> {
        // ponytail: backtest_concurrency is accepted but v0 runs sequentially.
        let Self {
            config,
            runs,
            consent,
            store,
        } = self;

        let mut seen = HashSet::new();
        for spec in &runs {
            let id = run_id_of(spec);
            if !seen.insert(id.clone()) {
                return Err(StartError::DuplicateRunId(id.clone()));
            }
        }

        let mut reports = HashMap::new();
        let mut manifests = HashMap::new();
        for spec in runs {
            let manifest = build_manifest(&spec, &config);
            let manifest_id = run_id_of(&spec).clone();
            match spec {
                RunSpec::Backtest(run) => {
                    let report = backtest::drive(&run, store.as_deref()).await?;
                    reports.insert(run.id().clone(), RunReport::Backtest(report));
                }
                RunSpec::Paper(run) => {
                    let report = paper::drive(&run, store.as_deref()).await?;
                    reports.insert(run.id().clone(), RunReport::Paper(report));
                }
                RunSpec::Live(run) => {
                    if consent.is_none() {
                        return Err(StartError::LiveConsentMissing(run.id().clone()));
                    }
                    let report = live::drive_with_store(&run, &config, store.as_deref()).await?;
                    reports.insert(run.id().clone(), RunReport::Live(report));
                }
            }
            manifests.insert(manifest_id, manifest);
        }
        Ok(AppHandle {
            reports,
            manifests,
            config,
        })
    }
}

/// Handle to a started engine, holding each run's terminal report.
#[derive(Debug)]
pub struct AppHandle {
    reports: HashMap<RunId, RunReport>,
    manifests: HashMap<RunId, serde_json::Value>,
    config: RuntimeConfig,
}

impl AppHandle {
    /// Returns the persisted terminal report for `run`.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::UnknownRun`] when the application does not own
    /// the requested run.
    pub async fn wait_for(&self, run: RunId) -> Result<RunReport, RuntimeError> {
        std::future::ready(
            self.reports
                .get(&run)
                .cloned()
                .ok_or(RuntimeError::UnknownRun(run)),
        )
        .await
    }

    /// Returns the report for `run`, if it exists.
    #[must_use]
    pub fn report(&self, run: &RunId) -> Option<&RunReport> {
        self.reports.get(run)
    }

    /// Returns the reproducibility manifest for `run`, if it exists.
    #[must_use]
    pub fn manifest(&self, run: &RunId) -> Option<&serde_json::Value> {
        self.manifests.get(run)
    }

    /// Returns the runtime configuration the engine started with.
    #[must_use]
    pub const fn config(&self) -> &RuntimeConfig {
        &self.config
    }
}

fn run_id_of(spec: &RunSpec) -> &RunId {
    match spec {
        RunSpec::Backtest(run) => run.id(),
        RunSpec::Paper(run) => run.id(),
        RunSpec::Live(run) => run.id(),
    }
}

async fn store_signal(
    store: Option<&dyn TapeStore>,
    scope: &OwnerScope,
    signal: &SourceSignal,
) -> Result<(), StoreError> {
    let Some(store) = store else {
        return Ok(());
    };
    let envelope = match signal {
        SourceSignal::Data(envelope) => match envelope.as_ref() {
            SourceEnvelope::PmMarket(envelope) => PmEnvelope {
                schema_version: envelope.metadata.schema_version,
                scope: scope.clone(),
                venue_id: "polymarket".into(),
                config_hash: "runtime".into(),
                source_id: envelope.metadata.source_id.clone(),
                connection_id: envelope.metadata.connection_id.clone(),
                source_timestamp_ms: envelope.metadata.source_time_ms,
                canonical_source_rank: envelope.metadata.canonical_source_rank,
                connection_epoch: envelope.metadata.connection_epoch,
                frame_sequence: envelope.metadata.frame_sequence,
                receipt_timestamp_ms: envelope.metadata.receipt_time_ms,
                ingest_sequence: i64::try_from(envelope.metadata.ingest_sequence).map_err(
                    |_| StoreError::Storage {
                        message: "PM ingest sequence exceeds storage range".into(),
                    },
                )?,
                raw_frame: envelope.raw_frame.clone(),
                normalized: pmkit_tape::market_envelope_json(envelope),
            },
            SourceEnvelope::PmAccount(envelope) => PmEnvelope {
                schema_version: envelope.metadata.schema_version,
                scope: scope.clone(),
                venue_id: "polymarket".into(),
                config_hash: "runtime".into(),
                source_id: envelope.metadata.source_id.clone(),
                connection_id: envelope.metadata.connection_id.clone(),
                source_timestamp_ms: envelope.metadata.source_time_ms,
                canonical_source_rank: envelope.metadata.canonical_source_rank,
                connection_epoch: envelope.metadata.connection_epoch,
                frame_sequence: envelope.metadata.frame_sequence,
                receipt_timestamp_ms: envelope.metadata.receipt_time_ms,
                ingest_sequence: i64::try_from(envelope.metadata.ingest_sequence).map_err(
                    |_| StoreError::Storage {
                        message: "PM ingest sequence exceeds storage range".into(),
                    },
                )?,
                raw_frame: envelope.raw_frame.clone(),
                normalized: pmkit_tape::account_envelope_json(envelope),
            },
            SourceEnvelope::CexReference(_) => return Ok(()),
        },
        SourceSignal::Watermark(_) | SourceSignal::Eof => return Ok(()),
    };
    store.store_envelope(&envelope).await
}

fn absorb_fills(fills: &[MarketEvent], positions: &mut Vec<pmkit_book::Position>) -> usize {
    for event in fills {
        if let MarketEvent::Fill {
            outcome,
            side,
            price,
            size,
            ..
        } = event
        {
            pmkit_book::book::apply_fill(positions, *outcome, *side, *price, *size);
        }
    }
    fills.len()
}

fn instantiate_strategies(
    registrations: &[StrategyRegistration],
    run: &RunId,
) -> Result<Vec<StrategyInstance>, StartError> {
    let mut strategies = Vec::new();
    for registration in registrations {
        let strategy =
            registration
                .factory()
                .create()
                .map_err(|source| StartError::StrategyInit {
                    run: run.clone(),
                    source,
                })?;
        strategies.push((registration.market().clone(), strategy));
    }
    Ok(strategies)
}

#[cfg(test)]
mod backtest_tests;
#[cfg(test)]
mod causal_tests;
#[cfg(test)]
mod feed_tests;
#[cfg(test)]
mod live_tests;
#[cfg(test)]
mod paper_tests;
#[cfg(test)]
mod test_support;
