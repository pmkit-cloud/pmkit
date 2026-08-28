//! `PMKit` orchestration engine.
//!
//! [`Pmkit::builder`] collects [`RunSpec`]s; [`PmkitBuilder::start`] validates
//! them and drives each backtest end to end (replay -> simulation -> strategy
//! -> fills), returning an [`AppHandle`] that exposes each run's
//! [`RunReport`]. Paper runs are driven similarly against a live data source;
//! live runs route strategy orders through a consented executor behind a
//! risk gate with bounded execution-state reconciliation, optional tapes, and
//! opt-in durable PM-envelope and causal-decision storage via
//! [`PmkitBuilder::storage`].

mod backtest;
/// Portable causal decision snapshots and durable execution recording.
pub mod causal;
/// Deterministic envelope-aware source merging.
pub mod feed;
pub(crate) mod live;
mod paper;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::{
    sync::{Notify, mpsc},
    task::JoinSet,
};

use pmkit_accounting::PortfolioExposure;
use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
use pmkit_data::SourceSignal;
use pmkit_event::{CanonicalSourceKey, SourceEnvelope};
use pmkit_exec::ExecError;
use pmkit_manifest::build_manifest;
use pmkit_run::LiveConsent;
use pmkit_runtime::{RuntimeConfig, StrategyRegistration};
use pmkit_spec::RunSpec;
use pmkit_store::{OwnerScope, PM_ENVELOPE_VERSION, PmEnvelope, StoreError, TapeStore};
use pmkit_strategy::Strategy;
use thiserror::Error;

/// A registered strategy instance keyed by its exact market.
struct StrategyInstance {
    market: MarketId,
    id: StrategyId,
    strategy: Box<dyn Strategy>,
}

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
    /// Typed terminal metrics for this run.
    pub metrics: RunMetricsSnapshot,
    /// Portfolio-wide exposure aggregation over positions and reservations.
    pub exposure: PortfolioExposure,
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
    /// Typed terminal metrics for this run.
    pub metrics: RunMetricsSnapshot,
    /// Portfolio-wide exposure aggregation over positions and reservations.
    pub exposure: PortfolioExposure,
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
    /// Orders rejected by a risk gate or execution boundary.
    pub rejected: usize,
    /// Typed terminal metrics for this run.
    pub metrics: RunMetricsSnapshot,
    /// Portfolio-wide exposure aggregation over positions and reservations.
    pub exposure: PortfolioExposure,
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

/// Deterministic logical-time health for one merged source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedHealthSnapshot {
    /// The merge-owned source identifier that emitted the lifecycle signals.
    pub source: String,
    /// Timestamp from the latest event's canonical source key.
    pub last_event_timestamp_ms: Option<i64>,
    /// Most recent source watermark supplied to the merge.
    pub watermark_ms: Option<i64>,
    /// Distance from the merge's safe watermark frontier in logical milliseconds.
    pub logical_lag_ms: Option<i64>,
    /// Fail-closed replay gaps observed for this source.
    pub gap_count: usize,
}

/// Typed counters for one run, available at completion and on driver failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunMetricsSnapshot {
    /// The run these counters belong to.
    pub run: RunId,
    /// PM market events consumed by the run driver.
    pub events_processed: usize,
    /// Fills applied by the mode's authoritative fill owner.
    pub fills: usize,
    /// Orders rejected by the mode's execution boundary.
    pub rejected: usize,
    /// Observed PM source reconnects, derived from connection-epoch advances.
    pub reconnects: usize,
    /// Strategy decision evaluations completed by the run driver.
    pub decisions: usize,
    /// Per-source feed health derived by the deterministic merge.
    pub feed_health: Vec<FeedHealthSnapshot>,
}

#[derive(Debug, Clone)]
pub(crate) struct RunMetrics {
    run: RunId,
    events_processed: Arc<AtomicUsize>,
    fills: Arc<AtomicUsize>,
    rejected: Arc<AtomicUsize>,
    reconnects: Arc<AtomicUsize>,
    decisions: Arc<AtomicUsize>,
    feed_health: Arc<Mutex<Vec<FeedHealthSnapshot>>>,
}

impl RunMetrics {
    fn new(run: &RunId) -> Self {
        Self {
            run: run.clone(),
            events_processed: Arc::new(AtomicUsize::new(0)),
            fills: Arc::new(AtomicUsize::new(0)),
            rejected: Arc::new(AtomicUsize::new(0)),
            reconnects: Arc::new(AtomicUsize::new(0)),
            decisions: Arc::new(AtomicUsize::new(0)),
            feed_health: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub(crate) fn event(&self) {
        self.events_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn add_fills(&self, fills: usize) {
        self.fills.fetch_add(fills, Ordering::Relaxed);
    }

    pub(crate) fn set_fills(&self, fills: usize) {
        self.fills.store(fills, Ordering::Relaxed);
    }

    pub(crate) fn reject(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn add_rejected(&self, rejected: usize) {
        self.rejected.fetch_add(rejected, Ordering::Relaxed);
    }

    pub(crate) fn reconnect(&self) {
        self.reconnects.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn decision(&self) {
        self.decisions.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn add_decisions(&self, decisions: usize) {
        self.decisions.fetch_add(decisions, Ordering::Relaxed);
    }

    pub(crate) fn set_feed_health(&self, feed_health: Vec<FeedHealthSnapshot>) {
        match self.feed_health.lock() {
            Ok(mut health) => *health = feed_health,
            Err(poisoned) => *poisoned.into_inner() = feed_health,
        }
    }

    pub(crate) fn snapshot(&self) -> RunMetricsSnapshot {
        RunMetricsSnapshot {
            run: self.run.clone(),
            events_processed: self.events_processed.load(Ordering::Relaxed),
            fills: self.fills.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            decisions: self.decisions.load(Ordering::Relaxed),
            feed_health: match self.feed_health.lock() {
                Ok(health) => health.clone(),
                Err(poisoned) => poisoned.into_inner().clone(),
            },
        }
    }
}

/// A failure raised while starting runs.
#[derive(Debug, Error)]
pub enum StartError {
    /// A run driver failed after exposing its partial typed metrics.
    #[error("run failed: {source}")]
    RunFailed {
        /// Counters observed before the failure.
        diagnostics: RunMetricsSnapshot,
        /// The original typed failure.
        #[source]
        source: Box<Self>,
    },
    /// A required market-data source could not complete a safe lifecycle.
    #[error("data source failed for run {run}: {source}")]
    Source {
        /// The affected run.
        run: RunId,
        /// The fail-closed source error.
        source: pmkit_data::DataSourceError,
    },
    /// Two runs shared the same [`RunId`].
    #[error("duplicate run id: {0}")]
    DuplicateRunId(RunId),
    /// A backtest task could not be joined safely.
    #[error("backtest task failed: {0}")]
    BacktestTask(#[source] tokio::task::JoinError),
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
    /// A persisted portfolio kill switch blocked live execution.
    #[error("portfolio kill switch is active for live run {0}")]
    KillSwitchActive(RunId),
}

impl StartError {
    /// Returns partial run metrics when a started driver fails.
    #[must_use]
    pub const fn diagnostics(&self) -> Option<&RunMetricsSnapshot> {
        match self {
            Self::RunFailed { diagnostics, .. } => Some(diagnostics),
            _ => None,
        }
    }
}

/// A failure raised while interacting with a started runtime.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RuntimeError {
    /// The requested run is not part of this application.
    #[error("unknown run id: {0}")]
    UnknownRun(RunId),
}

/// A cooperative cancellation token shared with a started engine.
///
/// Cloneable. Calling [`Cancellation::cancel`] stops each run at its next event
/// boundary; a cancelled run still returns its partial report and, for live
/// runs, still applies its configured shutdown policy.
#[derive(Debug, Clone)]
pub struct Cancellation {
    flag: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl Default for Cancellation {
    fn default() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }
}

impl Cancellation {
    /// Creates an uncancelled token.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests cancellation of every run sharing this token.
    pub fn cancel(&self) {
        if !self.flag.swap(true, Ordering::SeqCst) {
            self.notify.notify_waiters();
        }
    }

    /// Returns `true` once cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    pub(crate) async fn wait(&self) {
        loop {
            if self.is_cancelled() {
                return;
            }
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

/// A lifecycle transition observed for one run.
///
/// Carries run identity only, never executor or storage internals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunLifecycleEvent {
    /// The run began consuming its feed.
    Started {
        /// The run this event belongs to.
        run: RunId,
    },
    /// The run finished its feed normally.
    Completed {
        /// The run this event belongs to.
        run: RunId,
    },
    /// The run stopped early because cancellation was requested.
    Cancelled {
        /// The run this event belongs to.
        run: RunId,
    },
}

/// Per-start control shared with every driver: cancellation and lifecycle
/// subscription.
#[derive(Debug, Clone, Default)]
pub(crate) struct RunControl {
    cancel: Option<Cancellation>,
    subscriber: Option<mpsc::UnboundedSender<RunLifecycleEvent>>,
    metrics: Option<RunMetrics>,
}

impl RunControl {
    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancel.as_ref().is_some_and(Cancellation::is_cancelled)
    }

    pub(crate) fn cancellation(&self) -> Option<Cancellation> {
        self.cancel.clone()
    }

    pub(crate) fn emit(&self, event: RunLifecycleEvent) {
        if let Some(subscriber) = &self.subscriber {
            let _ = subscriber.send(event);
        }
    }

    pub(crate) fn for_run(&self, run: &RunId) -> Self {
        Self {
            cancel: self.cancel.clone(),
            subscriber: self.subscriber.clone(),
            metrics: Some(RunMetrics::new(run)),
        }
    }

    pub(crate) fn metrics_for(&self, run: &RunId) -> RunMetrics {
        self.metrics.clone().unwrap_or_else(|| RunMetrics::new(run))
    }
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
            cancel: None,
            subscriber: None,
        }
    }
}

/// Collects runs before starting the engine.
pub struct PmkitBuilder {
    config: RuntimeConfig,
    runs: Vec<RunSpec>,
    consent: Option<LiveConsent>,
    store: Option<Arc<dyn TapeStore>>,
    cancel: Option<Cancellation>,
    subscriber: Option<mpsc::UnboundedSender<RunLifecycleEvent>>,
}

impl std::fmt::Debug for PmkitBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PmkitBuilder")
            .field("config", &self.config)
            .field("runs", &self.runs)
            .field("consent", &self.consent)
            .field("store", &self.store.as_ref().map(|_| "configured"))
            .field("cancel", &self.cancel)
            .field(
                "subscriber",
                &self.subscriber.as_ref().map(|_| "subscribed"),
            )
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

    /// Shares a cancellation token with every run this engine starts.
    #[must_use]
    pub fn cancellation(mut self, cancellation: Cancellation) -> Self {
        self.cancel = Some(cancellation);
        self
    }

    /// Subscribes to run lifecycle events for every run this engine starts.
    #[must_use]
    pub fn subscribe(mut self, subscriber: mpsc::UnboundedSender<RunLifecycleEvent>) -> Self {
        self.subscriber = Some(subscriber);
        self
    }

    /// Validates the topology and drives every run to completion.
    ///
    /// # Errors
    ///
    /// Returns [`StartError`] on a duplicate run id or a strategy that fails to
    /// initialise.
    pub async fn start(self) -> Result<AppHandle, StartError> {
        let Self {
            config,
            runs,
            consent,
            store,
            cancel,
            subscriber,
        } = self;
        let control = RunControl {
            cancel,
            subscriber,
            metrics: None,
        };

        let mut seen = HashSet::new();
        let mut report_order = Vec::with_capacity(runs.len());
        for spec in &runs {
            let id = run_id_of(spec);
            if !seen.insert(id.clone()) {
                return Err(StartError::DuplicateRunId(id.clone()));
            }
            report_order.push(id.clone());
        }

        let mut reports = HashMap::new();
        let mut manifests = HashMap::new();
        let mut backtests = JoinSet::new();
        for spec in runs {
            let manifest = build_manifest(&spec, &config);
            let manifest_id = run_id_of(&spec).clone();
            match spec {
                RunSpec::Backtest(run) => {
                    if backtests.len() >= config.backtest_concurrency.get() {
                        collect_backtest_report(&mut backtests, &mut reports).await?;
                    }
                    let store = store.clone();
                    let control = control.for_run(run.id());
                    let metrics = control.metrics_for(run.id());
                    backtests.spawn(async move {
                        backtest::drive_with_control(&run, store.as_deref(), &control)
                            .await
                            .map_err(|source| StartError::RunFailed {
                                diagnostics: metrics.snapshot(),
                                source: Box::new(source),
                            })
                    });
                }
                RunSpec::Paper(run) => {
                    while !backtests.is_empty() {
                        collect_backtest_report(&mut backtests, &mut reports).await?;
                    }
                    let control = control.for_run(run.id());
                    let metrics = control.metrics_for(run.id());
                    let report = paper::drive_with_control(&run, store.as_deref(), &control)
                        .await
                        .map_err(|source| StartError::RunFailed {
                            diagnostics: metrics.snapshot(),
                            source: Box::new(source),
                        })?;
                    reports.insert(run.id().clone(), RunReport::Paper(report));
                }
                RunSpec::Live(run) => {
                    while !backtests.is_empty() {
                        collect_backtest_report(&mut backtests, &mut reports).await?;
                    }
                    if consent.is_none() {
                        return Err(StartError::LiveConsentMissing(run.id().clone()));
                    }
                    let control = control.for_run(run.id());
                    let metrics = control.metrics_for(run.id());
                    let report =
                        live::drive_with_control(&run, &config, store.as_deref(), &control)
                            .await
                            .map_err(|source| StartError::RunFailed {
                                diagnostics: metrics.snapshot(),
                                source: Box::new(source),
                            })?;
                    reports.insert(run.id().clone(), RunReport::Live(report));
                }
            }
            manifests.insert(manifest_id, manifest);
        }
        while !backtests.is_empty() {
            collect_backtest_report(&mut backtests, &mut reports).await?;
        }
        Ok(AppHandle {
            reports,
            report_order,
            manifests,
            config,
        })
    }
}

async fn collect_backtest_report(
    backtests: &mut JoinSet<Result<BacktestReport, StartError>>,
    reports: &mut HashMap<RunId, RunReport>,
) -> Result<(), StartError> {
    let Some(joined) = backtests.join_next().await else {
        return Ok(());
    };
    let report = joined.map_err(StartError::BacktestTask)??;
    reports.insert(report.run.clone(), RunReport::Backtest(report));
    Ok(())
}

/// Handle to a started engine, holding each run's terminal report.
#[derive(Debug)]
pub struct AppHandle {
    reports: HashMap<RunId, RunReport>,
    report_order: Vec<RunId>,
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

    /// Returns the terminal typed metrics snapshot for `run`, if it completed.
    #[must_use]
    pub fn metrics(&self, run: &RunId) -> Option<&RunMetricsSnapshot> {
        self.reports.get(run).map(|report| match report {
            RunReport::Backtest(report) => &report.metrics,
            RunReport::Paper(report) => &report.metrics,
            RunReport::Live(report) => &report.metrics,
        })
    }

    /// Returns all reports in run submission order.
    #[must_use]
    pub fn reports_ordered(&self) -> Vec<(&RunId, &RunReport)> {
        self.report_order
            .iter()
            .filter_map(|run| self.reports.get_key_value(run))
            .collect()
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

pub(crate) fn observe_reconnect(
    source: &SourceEnvelope,
    connection_epochs: &mut HashMap<String, i64>,
) -> bool {
    let key = source.canonical_key();
    let (stream_id, connection_epoch) = match key {
        CanonicalSourceKey::Pm {
            stream_id,
            connection_epoch,
            ..
        }
        | CanonicalSourceKey::Polymarket {
            stream_id,
            connection_epoch,
            ..
        } => (stream_id, connection_epoch),
        CanonicalSourceKey::Cex { .. } => return false,
    };
    match connection_epochs.entry(stream_id) {
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(connection_epoch);
            false
        }
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            if connection_epoch > *entry.get() {
                entry.insert(connection_epoch);
                true
            } else {
                false
            }
        }
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
                schema_version: PM_ENVELOPE_VERSION,
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
                schema_version: PM_ENVELOPE_VERSION,
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
            SourceEnvelope::PolymarketReference(envelope) => PmEnvelope {
                schema_version: PM_ENVELOPE_VERSION,
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
                raw_frame: Vec::new(),
                normalized: pmkit_tape::polymarket_reference_envelope_json(envelope),
            },
            SourceEnvelope::CexReference(_) => return Ok(()),
        },
        SourceSignal::Watermark(_) | SourceSignal::Eof => return Ok(()),
    };
    store.store_envelope(&envelope).await
}

fn validate_account_owner(
    run: &RunId,
    portfolio: &PortfolioId,
    source: &SourceEnvelope,
) -> Result<(), StartError> {
    let SourceEnvelope::PmAccount(envelope) = source else {
        return Ok(());
    };
    if envelope.portfolio == *portfolio {
        return Ok(());
    }
    Err(StartError::Source {
        run: run.clone(),
        source: pmkit_data::DataSourceError::ReplayGap {
            message: format!(
                "account event owner {} does not match run {portfolio}",
                envelope.portfolio
            ),
        },
    })
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
        strategies.push(StrategyInstance {
            market: registration.market().clone(),
            id: registration.id().clone(),
            strategy,
        });
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
