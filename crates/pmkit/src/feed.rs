use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pmkit_data::{DataSourceError, SourceGap, SourceSignal};
use pmkit_event::{CanonicalSourceKey, SourceEnvelope, StrategyFact, StreamMetadata};
use tokio::sync::mpsc;
use tokio::task::{Id, JoinSet};
use tokio::time::Instant;

use crate::{FeedHealthSnapshot, RunMetrics};

const DEFAULT_SOURCE_SILENCE_TIMEOUT: Duration = Duration::from_secs(30);
const SOURCE_SIGNAL_CHANNEL_CAPACITY: usize = 128;

/// The lifecycle policy applied to one deterministic source merge.
#[derive(Debug, Clone, Copy)]
pub enum FeedMode {
    /// Historical replay requires watermarks through the requested end.
    Backtest,
    /// Paper sources must explicitly close their finite stream.
    Paper,
    /// Live sources must explicitly close their finite stream.
    Live,
}

/// A finite source stream used by deterministic fixtures.
#[derive(Debug, Clone)]
pub struct SourceDefinition {
    name: String,
    signals: Vec<SourceSignal>,
}

impl SourceDefinition {
    /// Builds a finite source that must explicitly signal EOF before completion.
    #[must_use]
    pub fn finite(name: impl Into<String>, signals: Vec<SourceSignal>) -> Self {
        Self {
            name: name.into(),
            signals,
        }
    }
}

type SourceFuture = Pin<Box<dyn Future<Output = Result<(), DataSourceError>> + Send>>;
type SourceTask = Box<dyn FnOnce(mpsc::Sender<SourceSignal>) -> SourceFuture + Send>;

#[derive(Debug)]
pub(crate) enum FeedError {
    Source(DataSourceError),
    Capture(crate::capture::CaptureError),
}

impl From<DataSourceError> for FeedError {
    fn from(error: DataSourceError) -> Self {
        Self::Source(error)
    }
}

impl From<crate::capture::CaptureError> for FeedError {
    fn from(error: crate::capture::CaptureError) -> Self {
        Self::Capture(error)
    }
}

/// One named production source owned by a [`MergedFeed`].
pub struct SourceTaskDefinition {
    name: String,
    task: SourceTask,
}

impl std::fmt::Debug for SourceTaskDefinition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SourceTaskDefinition")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl SourceTaskDefinition {
    /// Names and owns a source task that emits lifecycle signals.
    #[must_use]
    pub fn new<F, Fut>(name: impl Into<String>, task: F) -> Self
    where
        F: FnOnce(mpsc::Sender<SourceSignal>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), DataSourceError>> + Send + 'static,
    {
        Self {
            name: name.into(),
            task: Box::new(move |sink| Box::pin(task(sink))),
        }
    }
}

/// A normalized fact plus the non-strategy metadata used for storage and causality.
#[derive(Debug, Clone)]
pub struct MergedFact {
    /// The only value that may be passed to a strategy.
    pub fact: StrategyFact,
    /// Transport envelope retained only by the runtime persistence boundary.
    pub source: SourceEnvelope,
    /// Identity retained by the runtime before the envelope is discarded.
    pub metadata: StreamMetadata,
    /// The owner of an account fact, if this is an account frame.
    pub account_portfolio: Option<pmkit_core::PortfolioId>,
}

#[derive(Debug)]
struct QueuedFact {
    source: String,
    key: CanonicalSourceKey,
    fact: MergedFact,
}

impl PartialEq for QueuedFact {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for QueuedFact {}
impl PartialOrd for QueuedFact {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for QueuedFact {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| compare_pm_envelopes(&self.fact.source, &other.fact.source))
            .then_with(|| self.source.cmp(&other.source))
    }
}

#[derive(Debug, Default)]
struct SourceState {
    last_event_timestamp_ms: Option<i64>,
    last_metadata: Option<StreamMetadata>,
    last_signal_at: Option<Instant>,
    watermark: Option<i64>,
    eof_seen: bool,
    gap_count: usize,
}

enum Sources {
    Fixtures(Vec<SourceDefinition>),
    Tasks(Vec<SourceTaskDefinition>),
}

/// An envelope-aware merge that owns all source tasks and releases safe strategy facts.
pub struct MergedFeed {
    mode: FeedMode,
    sources: Sources,
    replay_end_ms: Option<i64>,
    source_silence_timeout: Duration,
    metrics: Option<RunMetrics>,
    capture: Option<crate::capture::CaptureRecorder>,
}

impl std::fmt::Debug for MergedFeed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MergedFeed")
            .field("mode", &self.mode)
            .field("replay_end_ms", &self.replay_end_ms)
            .finish_non_exhaustive()
    }
}

impl MergedFeed {
    /// Builds a merge from finite source fixtures.
    #[must_use]
    pub const fn from_fixture(
        mode: FeedMode,
        definitions: Vec<SourceDefinition>,
        replay_end_ms: Option<i64>,
    ) -> Self {
        Self {
            mode,
            sources: Sources::Fixtures(definitions),
            replay_end_ms,
            source_silence_timeout: DEFAULT_SOURCE_SILENCE_TIMEOUT,
            metrics: None,
            capture: None,
        }
    }

    /// Builds a production merge that owns and joins all source tasks.
    #[must_use]
    pub const fn from_tasks(
        mode: FeedMode,
        definitions: Vec<SourceTaskDefinition>,
        replay_end_ms: Option<i64>,
    ) -> Self {
        Self {
            mode,
            sources: Sources::Tasks(definitions),
            replay_end_ms,
            source_silence_timeout: DEFAULT_SOURCE_SILENCE_TIMEOUT,
            metrics: None,
            capture: None,
        }
    }

    /// Overrides the 30-second live-source silence limit.
    #[must_use]
    pub const fn with_source_silence_timeout(mut self, timeout: Duration) -> Self {
        self.source_silence_timeout = timeout;
        self
    }

    /// Attaches the run-owned metrics sink used by production drivers.
    #[must_use]
    pub(crate) fn with_metrics(mut self, metrics: RunMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Attaches the run-owned raw capture recorder for merge control signals and gaps.
    #[must_use]
    pub(crate) fn with_capture(mut self, capture: Option<crate::capture::CaptureRecorder>) -> Self {
        self.capture = capture;
        self
    }

    /// Streams causally safe facts. Source errors, early completion, and stale coverage fail closed.
    ///
    /// # Errors
    ///
    /// Returns [`DataSourceError::ReplayGap`] for an incomplete lifecycle and
    /// propagates source and output-channel failures.
    pub async fn forward(self, output: mpsc::Sender<MergedFact>) -> Result<(), DataSourceError> {
        self.forward_run(output).await.map_err(|error| match error {
            FeedError::Source(source) => source,
            FeedError::Capture(error) => DataSourceError::Unavailable {
                message: format!("raw source capture failed: {error}"),
            },
        })
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the merge owns task lifecycle, safe release ordering, and fail-closed health"
    )]
    pub(crate) async fn forward_run(
        self,
        output: mpsc::Sender<MergedFact>,
    ) -> Result<(), FeedError> {
        let Self {
            mode,
            sources,
            replay_end_ms,
            source_silence_timeout,
            metrics,
            capture,
        } = self;
        let release_on_receipt = matches!(mode, FeedMode::Live | FeedMode::Paper)
            && matches!(&sources, Sources::Tasks(_));
        let (tx, mut rx) = mpsc::channel(SOURCE_SIGNAL_CHANNEL_CAPACITY);
        let mut tasks: JoinSet<(String, Result<(), DataSourceError>)> = JoinSet::new();
        let mut states = HashMap::new();
        let mut source_names = BTreeSet::new();
        match &sources {
            Sources::Fixtures(definitions) => {
                for definition in definitions {
                    validate_source_name(&definition.name, &mut source_names)?;
                }
            }
            Sources::Tasks(definitions) => {
                for definition in definitions {
                    validate_source_name(&definition.name, &mut source_names)?;
                }
            }
        }
        drop(source_names);
        let mut task_sources: HashMap<Id, String> = HashMap::new();
        match sources {
            Sources::Fixtures(definitions) => {
                for definition in definitions {
                    states.insert(definition.name.clone(), SourceState::default());
                    let sender = tx.clone();
                    let name = definition.name;
                    let signals = definition.signals;
                    let task_name = name.clone();
                    let task_id = tasks
                        .spawn(async move {
                            let result = async {
                                for signal in signals {
                                    sender
                                        .send((name.clone(), signal))
                                        .await
                                        .map_err(|_| DataSourceError::SinkClosed)?;
                                }
                                Ok(())
                            }
                            .await;
                            (name, result)
                        })
                        .id();
                    task_sources.insert(task_id, task_name);
                }
            }
            Sources::Tasks(definitions) => {
                for definition in definitions {
                    states.insert(definition.name.clone(), SourceState::default());
                    let sender = tx.clone();
                    let name = definition.name;
                    let task = definition.task;
                    let task_name = name.clone();
                    let task_id = tasks
                        .spawn(async move {
                            let (source_tx, mut source_rx) =
                                mpsc::channel(SOURCE_SIGNAL_CHANNEL_CAPACITY);
                            let source = task(source_tx);
                            tokio::pin!(source);
                            let result = loop {
                                tokio::select! {
                                    signal = source_rx.recv() => match signal {
                                        Some(signal) => {
                                            if sender.send((name.clone(), signal)).await.is_err() {
                                                break Err(DataSourceError::SinkClosed);
                                            }
                                        }
                                        None => break source.await,
                                    },
                                    result = &mut source => {
                                        let pending = async {
                                            while let Ok(signal) = source_rx.try_recv() {
                                                sender
                                                    .send((name.clone(), signal))
                                                    .await
                                                    .map_err(|_| DataSourceError::SinkClosed)?;
                                            }
                                            Ok(())
                                        }
                                        .await;
                                        break result.and(pending);
                                    }
                                }
                            };
                            (name, result)
                        })
                        .id();
                    task_sources.insert(task_id, task_name);
                }
            }
        }
        drop(tx);
        if release_on_receipt {
            let started_at = Instant::now();
            for state in states.values_mut() {
                state.last_signal_at = Some(started_at);
            }
        }
        report_health(metrics.as_ref(), &states);
        let source_count = states.len();
        let mut completed = 0_usize;
        let mut queued = BinaryHeap::new();
        while completed < source_count || !queued.is_empty() {
            let silence_deadline = release_on_receipt
                .then(|| {
                    states
                        .values()
                        .filter(|state| !state.eof_seen)
                        .filter_map(|state| state.last_signal_at)
                        .map(|last_signal_at| last_signal_at + source_silence_timeout)
                        .min()
                })
                .flatten();
            tokio::select! {
                biased;
                () = async {
                    if let Some(deadline) = silence_deadline {
                        tokio::time::sleep_until(deadline).await;
                    }
                }, if silence_deadline.is_some() => {
                    for _ in 0..SOURCE_SIGNAL_CHANNEL_CAPACITY {
                        let Ok((source, signal)) = rx.try_recv() else {
                            break;
                        };
                        if let Err(error) = handle_signal(
                            (source, signal),
                            release_on_receipt,
                            &mut queued,
                            &mut states,
                            metrics.as_ref(),
                            capture.as_ref(),
                            &output,
                        )
                        .await
                        {
                            return abort(&mut tasks, error).await;
                        }
                    }
                    let now = Instant::now();
                    let silent_sources = states
                        .iter()
                        .filter(|(_, state)| {
                            !state.eof_seen
                                && state.last_signal_at.is_some_and(|last| {
                                    now.duration_since(last) >= source_silence_timeout
                                })
                        })
                        .map(|(source, _)| source.clone())
                        .collect::<BTreeSet<_>>();
                    if !silent_sources.is_empty() {
                        let source_list = silent_sources.iter().cloned().collect::<Vec<_>>().join(", ");
                        if let Err(error) = record_local_gaps(
                            silent_sources,
                            &mut states,
                            metrics.as_ref(),
                            capture.as_ref(),
                            "locally detected source silence",
                        ) {
                            return abort(&mut tasks, error).await;
                        }
                        return abort(
                            &mut tasks,
                            replay_gap(&format!("source silent for {source_silence_timeout:?}: {source_list}")),
                        )
                        .await;
                    }
                }
                Some((source, signal)) = rx.recv(), if completed < source_count => {
                    if let Err(error) = handle_signal(
                        (source, signal),
                        release_on_receipt,
                        &mut queued,
                        &mut states,
                        metrics.as_ref(),
                        capture.as_ref(),
                        &output,
                    )
                    .await
                    {
                        return abort(&mut tasks, error).await;
                    }
                }
                joined = tasks.join_next_with_id(), if completed < source_count => {
                    let Some(joined) = joined else {
                        if let Err(error) = record_unattributable_gap(
                            &mut states,
                            metrics.as_ref(),
                            capture.as_ref(),
                            "locally detected source task disconnect",
                        ) {
                            return abort(&mut tasks, error).await;
                        }
                        return abort(&mut tasks, replay_gap("source task set ended early")).await;
                    };
                    let (task_id, source, result) = match joined {
                        Ok((task_id, (source, result))) => (task_id, source, result),
                        Err(error) => {
                            let source = task_sources.remove(&error.id());
                            if let Some(source) = source {
                                if let Err(error) = record_gap(
                                    &source,
                                    &mut states,
                                    metrics.as_ref(),
                                    capture.as_ref(),
                                    "locally detected source task failure",
                                ) {
                                    return abort(&mut tasks, error).await;
                                }
                                return abort(&mut tasks, replay_gap("source task failed")).await;
                            }
                            if let Err(error) = record_unattributable_gap(
                                &mut states,
                                metrics.as_ref(),
                                capture.as_ref(),
                                "locally detected source task failure",
                            ) {
                                return abort(&mut tasks, error).await;
                            }
                            return abort(&mut tasks, replay_gap("source task failed without identity")).await;
                        }
                    };
                    task_sources.remove(&task_id);
                    if let Err(error) = result {
                        if matches!(&error, DataSourceError::ReplayGap { .. })
                            && let Err(capture_error) = record_gap(
                                &source,
                                &mut states,
                                metrics.as_ref(),
                                capture.as_ref(),
                                "locally detected source replay gap",
                            )
                        {
                            return abort(&mut tasks, capture_error).await;
                        }
                        return abort(&mut tasks, error).await;
                    }
                    let state = states.get(&source).ok_or_else(|| replay_gap("completed unknown source"))?;
                    let error = if !state.eof_seen {
                        Some(replay_gap("premature EOF"))
                    } else if matches!(mode, FeedMode::Backtest)
                        && replay_end_ms.is_some_and(|end| state.watermark.unwrap_or(i64::MIN) < end)
                    {
                        Some(replay_gap("historical coverage ends before replay end"))
                    } else {
                        None
                    };
                    if let Some(error) = error {
                        if let Err(capture_error) = record_gap(
                            &source,
                            &mut states,
                            metrics.as_ref(),
                            capture.as_ref(),
                            "locally detected source completion gap",
                        ) {
                            return abort(&mut tasks, capture_error).await;
                        }
                        return abort(&mut tasks, error).await;
                    }
                    completed += 1;
                    if !release_on_receipt {
                        release_safe(&mut queued, &states, &output).await?;
                    }
                }
                else => {
                    if completed == source_count {
                        if let Err(error) = record_queued_gaps(
                            &queued,
                            &mut states,
                            metrics.as_ref(),
                            capture.as_ref(),
                            "locally detected queued event beyond merge frontier",
                        ) {
                            return abort(&mut tasks, error).await;
                        }
                        return abort(&mut tasks, replay_gap("queued event exceeds terminal merge frontier")).await;
                    }
                    if let Err(error) = record_unattributable_gap(
                        &mut states,
                        metrics.as_ref(),
                        capture.as_ref(),
                        "locally detected source channel disconnect",
                    ) {
                        return abort(&mut tasks, error).await;
                    }
                    return abort(&mut tasks, replay_gap("source channel closed before source completion")).await;
                }
            }
        }
        if !release_on_receipt && states.values().any(|state| state.watermark.is_none()) {
            record_missing_watermark_gaps(
                &mut states,
                metrics.as_ref(),
                capture.as_ref(),
                "locally detected missing source watermark",
            )?;
            return Err(replay_gap("source did not warm up").into());
        }
        Ok(())
    }

    /// Collects a finite merged stream for tests and bounded replay consumers.
    ///
    /// # Errors
    ///
    /// Returns the same fail-closed source error as [`Self::forward`].
    pub async fn collect(self) -> Result<Vec<StrategyFact>, DataSourceError> {
        let (tx, mut rx) = mpsc::channel(128);
        let merge = tokio::spawn(async move { self.forward(tx).await });
        let mut facts = Vec::new();
        while let Some(fact) = rx.recv().await {
            facts.push(fact.fact);
        }
        merge
            .await
            .map_err(|error| replay_gap(&format!("merge task failed: {error}")))??;
        Ok(facts)
    }
}

async fn handle_signal(
    source_signal: (String, SourceSignal),
    release_on_receipt: bool,
    queued: &mut BinaryHeap<Reverse<QueuedFact>>,
    states: &mut HashMap<String, SourceState>,
    metrics: Option<&RunMetrics>,
    capture: Option<&crate::capture::CaptureRecorder>,
    output: &mpsc::Sender<MergedFact>,
) -> Result<(), FeedError> {
    let (source, signal) = source_signal;
    if matches!(signal, SourceSignal::Gap(_) | SourceSignal::Heartbeat(_)) {
        capture_merge_signal(capture, &signal)?;
    }
    let Some(state) = states.get_mut(&source) else {
        record_gaps(BTreeSet::from([source.clone()]), states, metrics);
        return Err(replay_gap("unknown source").into());
    };
    if release_on_receipt {
        state.last_signal_at = Some(Instant::now());
    }
    let metadata = signal_metadata(&signal).cloned();
    let is_explicit_gap = matches!(signal, SourceSignal::Gap(_));
    let mut received_fact = None;
    let error = match signal {
        SourceSignal::Data(envelope) => {
            let envelope = *envelope;
            let key = envelope.canonical_key();
            if state
                .watermark
                .is_some_and(|watermark| key.timestamp_ms() <= watermark)
            {
                Some(replay_gap("late record"))
            } else {
                capture_merge_signal(capture, &SourceSignal::Data(Box::new(envelope.clone())))?;
                state.last_event_timestamp_ms = Some(state.last_event_timestamp_ms.map_or_else(
                    || key.timestamp_ms(),
                    |previous| previous.max(key.timestamp_ms()),
                ));
                let fact = merged_fact(envelope);
                if release_on_receipt {
                    received_fact = Some(fact);
                } else {
                    queued.push(Reverse(QueuedFact {
                        source: source.clone(),
                        key,
                        fact,
                    }));
                }
                None
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
            Some(replay_gap(&format!(
                "source gap for {} ({range}; unresolved count {:?}): {}",
                gap.source_id, gap.unresolved_count, gap.reason
            )))
        }
        SourceSignal::Heartbeat(_) => None,
        SourceSignal::Watermark(watermark) => {
            if state.watermark.is_some_and(|previous| watermark < previous) {
                Some(replay_gap("watermark regressed"))
            } else {
                state.watermark = Some(watermark);
                None
            }
        }
        SourceSignal::Eof => {
            state.eof_seen = true;
            None
        }
    };
    if let Some(error) = error {
        if is_explicit_gap {
            record_gaps(BTreeSet::from([source]), states, metrics);
        } else {
            record_gap(
                &source,
                states,
                metrics,
                capture,
                "locally detected invalid source signal",
            )?;
        }
        return Err(error.into());
    }
    if let Some(metadata) = metadata {
        state.last_metadata = Some(metadata);
    }
    report_health(metrics, states);
    if let Some(fact) = received_fact {
        output
            .send(fact)
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
    } else if !release_on_receipt {
        release_safe(queued, states, output).await?;
    }
    Ok(())
}

/// Normalizes a source envelope and retains its persistence metadata.
pub(crate) fn merged_fact(envelope: SourceEnvelope) -> MergedFact {
    let metadata = envelope.metadata().clone();
    let account_portfolio = match &envelope {
        SourceEnvelope::PmAccount(account) => Some(account.portfolio.clone()),
        SourceEnvelope::PmMarket(_) | SourceEnvelope::CexReference(_) => None,
    };
    let fact = envelope.clone().into_strategy_fact();
    MergedFact {
        fact,
        source: envelope,
        metadata,
        account_portfolio,
    }
}

async fn release_safe(
    queued: &mut BinaryHeap<Reverse<QueuedFact>>,
    states: &HashMap<String, SourceState>,
    output: &mpsc::Sender<MergedFact>,
) -> Result<(), FeedError> {
    let watermark = merge_frontier(states);
    while watermark.is_some_and(|watermark| {
        queued
            .peek()
            .is_some_and(|next| next.0.key.timestamp_ms() <= watermark)
    }) {
        if let Some(Reverse(next)) = queued.pop() {
            output
                .send(next.fact)
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
        }
    }
    Ok(())
}

async fn abort(
    tasks: &mut JoinSet<(String, Result<(), DataSourceError>)>,
    error: impl Into<FeedError>,
) -> Result<(), FeedError> {
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    Err(error.into())
}

fn merge_frontier(states: &HashMap<String, SourceState>) -> Option<i64> {
    states
        .values()
        .map(|state| state.watermark)
        .collect::<Option<Vec<_>>>()
        .and_then(|watermarks| watermarks.into_iter().min())
}

fn record_gap(
    source: &str,
    states: &mut HashMap<String, SourceState>,
    metrics: Option<&RunMetrics>,
    capture: Option<&crate::capture::CaptureRecorder>,
    reason: &str,
) -> Result<(), FeedError> {
    record_local_gaps(
        BTreeSet::from([source.to_owned()]),
        states,
        metrics,
        capture,
        reason,
    )
}

fn record_queued_gaps(
    queued: &BinaryHeap<Reverse<QueuedFact>>,
    states: &mut HashMap<String, SourceState>,
    metrics: Option<&RunMetrics>,
    capture: Option<&crate::capture::CaptureRecorder>,
    reason: &str,
) -> Result<(), FeedError> {
    record_local_gaps(
        queued
            .iter()
            .map(|fact| fact.0.source.clone())
            .collect::<BTreeSet<_>>(),
        states,
        metrics,
        capture,
        reason,
    )
}

fn record_unattributable_gap(
    states: &mut HashMap<String, SourceState>,
    metrics: Option<&RunMetrics>,
    capture: Option<&crate::capture::CaptureRecorder>,
    reason: &str,
) -> Result<(), FeedError> {
    record_local_gaps(
        states
            .iter()
            .filter(|(_, state)| !state.eof_seen)
            .map(|(source, _)| source.clone())
            .collect::<BTreeSet<_>>(),
        states,
        metrics,
        capture,
        reason,
    )
}

fn record_missing_watermark_gaps(
    states: &mut HashMap<String, SourceState>,
    metrics: Option<&RunMetrics>,
    capture: Option<&crate::capture::CaptureRecorder>,
    reason: &str,
) -> Result<(), FeedError> {
    record_local_gaps(
        states
            .iter()
            .filter(|(_, state)| state.watermark.is_none())
            .map(|(source, _)| source.clone())
            .collect::<BTreeSet<_>>(),
        states,
        metrics,
        capture,
        reason,
    )
}

fn record_local_gaps(
    sources: BTreeSet<String>,
    states: &mut HashMap<String, SourceState>,
    metrics: Option<&RunMetrics>,
    capture: Option<&crate::capture::CaptureRecorder>,
    reason: &str,
) -> Result<(), FeedError> {
    if sources.is_empty() {
        return Ok(());
    }
    let detected_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| {
            replay_gap("system clock is before the Unix epoch while recording a local gap")
        })?
        .as_millis();
    let detected_at_ms = i64::try_from(detected_at_ms).map_err(|_| {
        replay_gap("system clock exceeds the supported range while recording a local gap")
    })?;
    for source in &sources {
        let state = states.entry(source.clone()).or_default();
        let reason = state.last_metadata.as_ref().map_or_else(
            || reason.to_owned(),
            |metadata| {
                format!(
                    "{reason} (last observed frame {} at source time {} ms)",
                    metadata.frame_sequence, metadata.source_time_ms
                )
            },
        );
        capture_merge_signal(
            capture,
            &SourceSignal::Gap(SourceGap {
                source_id: source.clone(),
                detected_at_ms,
                metadata: None,
                raw_frame: Vec::new(),
                range: None,
                unresolved_count: None,
                reason,
            }),
        )?;
    }
    record_gaps(sources, states, metrics);
    Ok(())
}

fn signal_metadata(signal: &SourceSignal) -> Option<&StreamMetadata> {
    match signal {
        SourceSignal::Data(envelope) => Some(envelope.metadata()),
        SourceSignal::Gap(gap) => gap.metadata.as_ref(),
        SourceSignal::Heartbeat(heartbeat) => Some(&heartbeat.metadata),
        SourceSignal::Watermark(_) | SourceSignal::Eof => None,
    }
}

fn capture_merge_signal(
    capture: Option<&crate::capture::CaptureRecorder>,
    signal: &SourceSignal,
) -> Result<(), FeedError> {
    if matches!(
        signal,
        SourceSignal::Data(_) | SourceSignal::Gap(_) | SourceSignal::Heartbeat(_)
    ) {
        crate::capture::capture_merged_source_signal(capture, signal)?;
    }
    Ok(())
}

fn record_gaps(
    sources: BTreeSet<String>,
    states: &mut HashMap<String, SourceState>,
    metrics: Option<&RunMetrics>,
) {
    for source in sources {
        let state = states.entry(source).or_default();
        state.gap_count = state.gap_count.saturating_add(1);
    }
    report_health(metrics, states);
}

fn report_health(metrics: Option<&RunMetrics>, states: &HashMap<String, SourceState>) {
    let Some(metrics) = metrics else {
        return;
    };
    let frontier = merge_frontier(states);
    let mut health = BTreeMap::new();
    for (source, state) in states {
        health.insert(
            source.clone(),
            FeedHealthSnapshot {
                source: source.clone(),
                last_event_timestamp_ms: state.last_event_timestamp_ms,
                watermark_ms: state.watermark,
                logical_lag_ms: frontier.zip(state.last_event_timestamp_ms).map(
                    |(frontier, event_timestamp_ms)| {
                        frontier.saturating_sub(event_timestamp_ms).max(0)
                    },
                ),
                gap_count: state.gap_count,
            },
        );
    }
    metrics.set_feed_health(health.into_values().collect());
}

fn validate_source_name(
    name: &str,
    source_names: &mut BTreeSet<String>,
) -> Result<(), DataSourceError> {
    if name.trim().is_empty() {
        return Err(replay_gap("source name cannot be blank"));
    }
    if !source_names.insert(name.to_owned()) {
        return Err(replay_gap(&format!("duplicate source name: {name}")));
    }
    Ok(())
}

fn replay_gap(message: &str) -> DataSourceError {
    DataSourceError::ReplayGap {
        message: message.to_owned(),
    }
}

fn compare_pm_envelopes(left: &SourceEnvelope, right: &SourceEnvelope) -> Ordering {
    match (left, right) {
        (SourceEnvelope::PmMarket(left), SourceEnvelope::PmMarket(right)) => left
            .fact
            .timestamp_ms()
            .cmp(&right.fact.timestamp_ms())
            .then_with(|| {
                market_event_market_key(&left.fact).cmp(&market_event_market_key(&right.fact))
            })
            .then_with(|| {
                outcome_rank(market_event_outcome(&left.fact))
                    .cmp(&outcome_rank(market_event_outcome(&right.fact)))
            })
            .then_with(|| {
                market_event_kind_rank(&left.fact).cmp(&market_event_kind_rank(&right.fact))
            })
            .then_with(|| compare_market_event_detail(&left.fact, &right.fact)),
        (SourceEnvelope::PmAccount(left), SourceEnvelope::PmAccount(right)) => left
            .portfolio
            .to_string()
            .cmp(&right.portfolio.to_string())
            .then_with(|| {
                account_event_kind_rank(&left.fact).cmp(&account_event_kind_rank(&right.fact))
            })
            .then_with(|| {
                account_event_detail_key(&left.fact).cmp(&account_event_detail_key(&right.fact))
            }),
        (SourceEnvelope::CexReference(_), SourceEnvelope::CexReference(_)) => Ordering::Equal,
        (
            SourceEnvelope::PmMarket(_),
            SourceEnvelope::PmAccount(_) | SourceEnvelope::CexReference(_),
        )
        | (SourceEnvelope::PmAccount(_), SourceEnvelope::CexReference(_)) => Ordering::Less,
        (
            SourceEnvelope::PmAccount(_) | SourceEnvelope::CexReference(_),
            SourceEnvelope::PmMarket(_),
        )
        | (SourceEnvelope::CexReference(_), SourceEnvelope::PmAccount(_)) => Ordering::Greater,
    }
}

fn market_event_market_key(event: &pmkit_event::MarketEvent) -> String {
    match event {
        pmkit_event::MarketEvent::BookUpdate { market, .. }
        | pmkit_event::MarketEvent::BestBidAsk { market, .. }
        | pmkit_event::MarketEvent::LastTrade { market, .. }
        | pmkit_event::MarketEvent::Fill { market, .. } => market.to_string(),
        pmkit_event::MarketEvent::OrderAck { .. } | pmkit_event::MarketEvent::Tick { .. } => {
            String::new()
        }
    }
}

const fn market_event_outcome(event: &pmkit_event::MarketEvent) -> pmkit_market::Outcome {
    match event {
        pmkit_event::MarketEvent::BookUpdate { outcome, .. }
        | pmkit_event::MarketEvent::BestBidAsk { outcome, .. }
        | pmkit_event::MarketEvent::LastTrade { outcome, .. }
        | pmkit_event::MarketEvent::Fill { outcome, .. } => *outcome,
        pmkit_event::MarketEvent::OrderAck { .. } | pmkit_event::MarketEvent::Tick { .. } => {
            pmkit_market::Outcome::Up
        }
    }
}

const fn outcome_rank(outcome: pmkit_market::Outcome) -> u8 {
    match outcome {
        pmkit_market::Outcome::Up => 0,
        pmkit_market::Outcome::Down => 1,
    }
}

const fn market_event_kind_rank(event: &pmkit_event::MarketEvent) -> u8 {
    match event {
        pmkit_event::MarketEvent::BookUpdate { .. } => 0,
        pmkit_event::MarketEvent::BestBidAsk { .. } => 1,
        pmkit_event::MarketEvent::LastTrade { .. } => 2,
        pmkit_event::MarketEvent::Fill { .. } => 3,
        pmkit_event::MarketEvent::OrderAck { .. } => 4,
        pmkit_event::MarketEvent::Tick { .. } => 5,
    }
}

fn market_event_detail_key(event: &pmkit_event::MarketEvent) -> String {
    match event {
        pmkit_event::MarketEvent::BookUpdate { .. } => "book".to_owned(),
        pmkit_event::MarketEvent::BestBidAsk { bid, ask, .. } => format!("{bid}:{ask}"),
        pmkit_event::MarketEvent::LastTrade {
            price, side, size, ..
        } => format!("{price}:{side:?}:{size}"),
        pmkit_event::MarketEvent::Fill {
            order_id,
            price,
            size,
            side,
            fee,
            ..
        } => format!("{order_id}:{price}:{size}:{side:?}:{fee}"),
        pmkit_event::MarketEvent::OrderAck { order_id, .. } => order_id.clone(),
        pmkit_event::MarketEvent::Tick { .. } => "tick".to_owned(),
    }
}

fn compare_market_event_detail(
    left: &pmkit_event::MarketEvent,
    right: &pmkit_event::MarketEvent,
) -> Ordering {
    match (left, right) {
        (
            pmkit_event::MarketEvent::BookUpdate {
                bids: left_bids,
                asks: left_asks,
                ..
            },
            pmkit_event::MarketEvent::BookUpdate {
                bids: right_bids,
                asks: right_asks,
                ..
            },
        ) => left_bids
            .cmp(right_bids)
            .then_with(|| left_asks.cmp(right_asks)),
        _ => market_event_detail_key(left).cmp(&market_event_detail_key(right)),
    }
}

const fn account_event_kind_rank(event: &pmkit_event::PmAccountEvent) -> u8 {
    match event {
        pmkit_event::PmAccountEvent::Fill { .. } => 0,
        pmkit_event::PmAccountEvent::OrderAck { .. } => 1,
        pmkit_event::PmAccountEvent::OrderCancelled { .. } => 2,
        pmkit_event::PmAccountEvent::OrderRejected { .. } => 3,
        pmkit_event::PmAccountEvent::OrderStatus { .. } => 4,
        pmkit_event::PmAccountEvent::Settlement { .. } => 5,
    }
}

fn account_event_detail_key(event: &pmkit_event::PmAccountEvent) -> String {
    match event {
        pmkit_event::PmAccountEvent::Fill {
            identity,
            order_id,
            market,
            outcome,
            price,
            size,
            side,
            fee,
            timestamp_ms,
            ..
        } => {
            format!(
                "{identity:?}:{order_id}:{market}:{outcome:?}:{price}:{size}:{side:?}:{fee}:{timestamp_ms}"
            )
        }
        pmkit_event::PmAccountEvent::OrderAck {
            order_id,
            timestamp_ms,
            ..
        }
        | pmkit_event::PmAccountEvent::OrderCancelled {
            order_id,
            timestamp_ms,
            ..
        } => format!("{order_id}:{timestamp_ms}"),
        pmkit_event::PmAccountEvent::OrderRejected {
            order_id,
            reason,
            timestamp_ms,
            ..
        } => format!("{order_id}:{reason}:{timestamp_ms}"),
        pmkit_event::PmAccountEvent::OrderStatus {
            order_id,
            status,
            timestamp_ms,
            ..
        } => format!("{order_id}:{status}:{timestamp_ms}"),
        pmkit_event::PmAccountEvent::Settlement {
            identity,
            market,
            outcome,
            settled_size,
            proceeds,
            timestamp_ms,
        } => format!("{identity:?}:{market}:{outcome:?}:{settled_size}:{proceeds}:{timestamp_ms}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{QueuedFact, merged_fact};
    use pmkit_core::MarketId;
    use pmkit_event::{
        CexReferenceEnvelope, CexReferenceEvent, MarketEvent, PmMarketEnvelope, SourceEnvelope,
        StreamMetadata,
    };
    use pmkit_market::{Asset, Exchange, Outcome};
    use rust_decimal::Decimal;
    use std::cmp::Ordering;

    fn metadata() -> StreamMetadata {
        StreamMetadata {
            schema_version: 1,
            source_id: "pm".into(),
            source_time_ms: 10,
            canonical_source_rank: 1,
            receipt_time_ms: 10,
            connection_id: "shared".into(),
            connection_epoch: 7,
            frame_sequence: 11,
            ingest_sequence: 99,
        }
    }

    fn queued(
        market: &str,
        outcome: Outcome,
        bid: Decimal,
    ) -> Result<QueuedFact, Box<dyn std::error::Error>> {
        let envelope = SourceEnvelope::PmMarket(PmMarketEnvelope {
            metadata: metadata(),
            raw_frame: Vec::new(),
            fact: MarketEvent::BookUpdate {
                market: MarketId::new(market)?,
                outcome,
                bids: vec![(bid, Decimal::ONE)],
                asks: vec![(Decimal::new(51, 2), Decimal::ONE)],
                timestamp_ms: 10,
            },
        });
        Ok(QueuedFact {
            source: "pm".to_owned(),
            key: envelope.canonical_key(),
            fact: merged_fact(envelope),
        })
    }

    #[test]
    fn cross_market_key_collision_regression() -> Result<(), Box<dyn std::error::Error>> {
        // Given: two PM envelopes with identical durable replay metadata but different markets.
        let left = queued("btc-5m", Outcome::Up, Decimal::new(49, 2))?;
        let right = queued("eth-5m", Outcome::Up, Decimal::new(49, 2))?;

        // When: the merge queue compares them for release ordering.
        let ordering = left.cmp(&right);

        // Then: the in-process merge must not treat them as the same sortable item.
        assert_ne!(ordering, Ordering::Equal);
        assert_ne!(left, right);
        Ok(())
    }

    #[test]
    fn opposite_outcomes_have_distinct_stream_keys() -> Result<(), Box<dyn std::error::Error>> {
        // Given: one market's outcomes with otherwise identical transport metadata.
        let up = queued("btc-5m", Outcome::Up, Decimal::new(49, 2))?;
        let down = queued("btc-5m", Outcome::Down, Decimal::new(49, 2))?;

        // When: their canonical source keys are compared.
        let ordering = up.key.cmp(&down.key);

        // Then: market plus outcome identifies two distinct PM streams.
        assert_ne!(ordering, Ordering::Equal);
        Ok(())
    }

    #[test]
    fn distinct_same_market_books_have_total_order() -> Result<(), Box<dyn std::error::Error>> {
        // Given: two same-stream books whose depth differs.
        let left = queued("btc-5m", Outcome::Up, Decimal::new(48, 2))?;
        let right = queued("btc-5m", Outcome::Up, Decimal::new(49, 2))?;

        // When: the merge queue compares them.
        let ordering = left.cmp(&right);

        // Then: distinct book content cannot collapse to equal ordering identities.
        assert_ne!(ordering, Ordering::Equal);
        assert_ne!(left, right);
        Ok(())
    }

    #[tokio::test]
    async fn equal_key_reference_order_follows_receipt_in_live_and_sorts_in_backtest()
    -> Result<(), Box<dyn std::error::Error>> {
        fn cex_envelope(source_id: &str) -> SourceEnvelope {
            let mut metadata = metadata();
            metadata.source_id = source_id.to_owned();
            SourceEnvelope::CexReference(CexReferenceEnvelope {
                metadata,
                raw_frame: Vec::new(),
                fact: CexReferenceEvent::Trade {
                    asset: Asset::Btc,
                    exchange: Exchange::Binance,
                    aggregate_trade_id: 7,
                    price: Decimal::ONE,
                    qty: Decimal::ONE,
                    is_buyer_maker: false,
                    timestamp_ms: 10,
                },
            })
        }
        let run = |mode: super::FeedMode, first_delay: u64| async move {
            let (tx, mut rx) = tokio::sync::mpsc::channel(8);
            let feed = super::MergedFeed::from_tasks(
                mode,
                vec![
                    super::SourceTaskDefinition::new("alpha", move |sink| async move {
                        tokio::time::sleep(std::time::Duration::from_millis(first_delay)).await;
                        sink.send(pmkit_data::SourceSignal::Data(Box::new(cex_envelope(
                            "alpha",
                        ))))
                        .await
                        .map_err(|_| pmkit_data::DataSourceError::SinkClosed)?;
                        sink.send(pmkit_data::SourceSignal::Watermark(i64::MAX))
                            .await
                            .map_err(|_| pmkit_data::DataSourceError::SinkClosed)?;
                        sink.send(pmkit_data::SourceSignal::Eof)
                            .await
                            .map_err(|_| pmkit_data::DataSourceError::SinkClosed)
                    }),
                    super::SourceTaskDefinition::new("beta", move |sink| async move {
                        tokio::time::sleep(std::time::Duration::from_millis(10 - first_delay))
                            .await;
                        sink.send(pmkit_data::SourceSignal::Data(Box::new(cex_envelope(
                            "beta",
                        ))))
                        .await
                        .map_err(|_| pmkit_data::DataSourceError::SinkClosed)?;
                        sink.send(pmkit_data::SourceSignal::Watermark(i64::MAX))
                            .await
                            .map_err(|_| pmkit_data::DataSourceError::SinkClosed)?;
                        sink.send(pmkit_data::SourceSignal::Eof)
                            .await
                            .map_err(|_| pmkit_data::DataSourceError::SinkClosed)
                    }),
                ],
                None,
            );
            let task = tokio::spawn(feed.forward(tx));
            let mut ids = Vec::new();
            while let Some(fact) = rx.recv().await {
                ids.push(fact.source.metadata().source_id.clone());
            }
            task.await??;
            Ok::<_, Box<dyn std::error::Error>>(ids)
        };
        let alpha_first = vec!["alpha".to_owned(), "beta".to_owned()];
        let beta_first = vec!["beta".to_owned(), "alpha".to_owned()];
        assert_eq!(run(super::FeedMode::Live, 10).await?, beta_first);
        assert_eq!(run(super::FeedMode::Paper, 10).await?, beta_first);
        assert_eq!(run(super::FeedMode::Backtest, 0).await?, alpha_first);
        assert_eq!(run(super::FeedMode::Backtest, 10).await?, alpha_first);
        Ok(())
    }
}
