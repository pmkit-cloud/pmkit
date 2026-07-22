use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};
use std::future::Future;
use std::pin::Pin;

use pmkit_data::{DataSourceError, SourceSignal};
use pmkit_event::{CanonicalSourceKey, SourceEnvelope, StrategyFact, StreamMetadata};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

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
    key: CanonicalSourceKey,
    fact: MergedFact,
}

impl PartialEq for QueuedFact {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
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
        self.key.cmp(&other.key)
    }
}

#[derive(Debug, Default)]
struct SourceState {
    watermark: Option<i64>,
    eof_seen: bool,
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
        }
    }

    /// Streams causally safe facts. Source errors, early completion, and stale coverage fail closed.
    ///
    /// # Errors
    ///
    /// Returns [`DataSourceError::ReplayGap`] for an incomplete lifecycle and
    /// propagates source and output-channel failures.
    pub async fn forward(self, output: mpsc::Sender<MergedFact>) -> Result<(), DataSourceError> {
        let (tx, mut rx) = mpsc::channel(128);
        let mut tasks = JoinSet::new();
        let mut states = HashMap::new();
        match self.sources {
            Sources::Fixtures(definitions) => {
                for definition in definitions {
                    states.insert(definition.name.clone(), SourceState::default());
                    let sender = tx.clone();
                    tasks.spawn(async move {
                        for signal in definition.signals {
                            sender
                                .send((definition.name.clone(), signal))
                                .await
                                .map_err(|_| DataSourceError::SinkClosed)?;
                        }
                        Ok::<_, DataSourceError>(definition.name)
                    });
                }
            }
            Sources::Tasks(definitions) => {
                for definition in definitions {
                    states.insert(definition.name.clone(), SourceState::default());
                    let sender = tx.clone();
                    tasks.spawn(async move {
                    let (source_tx, mut source_rx) = mpsc::channel(128);
                    let source = (definition.task)(source_tx);
                    tokio::pin!(source);
                    loop {
                        tokio::select! {
                            signal = source_rx.recv() => match signal {
                                Some(signal) => sender.send((definition.name.clone(), signal)).await.map_err(|_| DataSourceError::SinkClosed)?,
                                None => return source.await.map(|()| definition.name),
                            },
                            result = &mut source => {
                                while let Ok(signal) = source_rx.try_recv() {
                                    sender.send((definition.name.clone(), signal)).await.map_err(|_| DataSourceError::SinkClosed)?;
                                }
                                return result.map(|()| definition.name);
                            },
                        }
                    }
                });
                }
            }
        }
        drop(tx);
        let source_count = states.len();
        let mut completed = 0_usize;
        let mut queued = BinaryHeap::new();
        while completed < source_count || !queued.is_empty() {
            tokio::select! {
                biased;
                Some((source, signal)) = rx.recv(), if completed < source_count => {
                    let state = states.get_mut(&source).ok_or_else(|| replay_gap("unknown source"))?;
                    match signal {
                        SourceSignal::Data(envelope) => {
                            let envelope = *envelope;
                            let key = envelope.canonical_key().ok_or_else(|| replay_gap("CEX BBO is not strategy input"))?;
                            if state.watermark.is_some_and(|watermark| key.timestamp_ms() <= watermark) { return abort(&mut tasks, replay_gap("late record")).await; }
                            queued.push(Reverse(QueuedFact { key, fact: merged_fact(envelope) }));
                        }
                        SourceSignal::Watermark(watermark) => {
                            if state.watermark.is_some_and(|previous| watermark < previous) { return abort(&mut tasks, replay_gap("watermark regressed")).await; }
                            state.watermark = Some(watermark);
                        }
                        SourceSignal::Eof => state.eof_seen = true,
                    }
                    release_safe(&mut queued, &states, &output).await?;
                }
                joined = tasks.join_next(), if completed < source_count => {
                    let source = joined.ok_or_else(|| replay_gap("source task set ended early"))?.map_err(|error| replay_gap(&format!("source task failed: {error}")))??;
                    let state = states.get(&source).ok_or_else(|| replay_gap("completed unknown source"))?;
                    if !state.eof_seen { return abort(&mut tasks, replay_gap("premature EOF")).await; }
                    if matches!(self.mode, FeedMode::Backtest) && self.replay_end_ms.is_some_and(|end| state.watermark.unwrap_or(i64::MIN) < end) { return abort(&mut tasks, replay_gap("historical coverage ends before replay end")).await; }
                    completed += 1;
                    release_safe(&mut queued, &states, &output).await?;
                }
                else => return Err(replay_gap("source channel closed before source completion")),
            }
        }
        if states.values().any(|state| state.watermark.is_none()) {
            return Err(replay_gap("source did not warm up"));
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

fn merged_fact(envelope: SourceEnvelope) -> MergedFact {
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
) -> Result<(), DataSourceError> {
    let watermark = states.values().filter_map(|state| state.watermark).min();
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
    tasks: &mut JoinSet<Result<String, DataSourceError>>,
    error: DataSourceError,
) -> Result<(), DataSourceError> {
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    Err(error)
}

fn replay_gap(message: &str) -> DataSourceError {
    DataSourceError::ReplayGap {
        message: message.to_owned(),
    }
}
