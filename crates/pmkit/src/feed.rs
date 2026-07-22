use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};

use pmkit_data::{DataSourceError, SourceSignal};
use pmkit_event::{CanonicalSourceKey, StrategyFact};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

/// The lifecycle policy applied to one deterministic source merge.
#[derive(Debug, Clone, Copy)]
pub enum FeedMode {
    /// Historical replay requires watermarks through the requested end.
    Backtest,
    /// Paper fixtures may close after an explicit EOF.
    Paper,
    /// Deterministic live fixtures may close after an explicit EOF.
    LiveFixture,
}

/// A finite source stream and its explicit lifecycle signals.
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

#[derive(Debug)]
struct QueuedFact {
    key: CanonicalSourceKey,
    fact: StrategyFact,
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
    data_seen: bool,
    watermark: Option<i64>,
    eof_seen: bool,
    completed: bool,
}

/// An envelope-aware merge that releases normalized strategy facts safely.
#[derive(Debug)]
pub struct MergedFeed {
    mode: FeedMode,
    definitions: Vec<SourceDefinition>,
    replay_end_ms: Option<i64>,
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
            definitions,
            replay_end_ms,
        }
    }

    /// Returns causally safe normalized facts in canonical source order.
    ///
    /// # Errors
    ///
    /// Returns `ReplayGap` when a source is late, incomplete, or cannot warm up.
    pub async fn collect(self) -> Result<Vec<StrategyFact>, DataSourceError> {
        let source_count = self.definitions.len();
        let (tx, mut rx) = mpsc::channel(128);
        let mut tasks = JoinSet::new();
        let mut states = HashMap::new();
        for definition in self.definitions {
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
        drop(tx);

        let mut output = Vec::new();
        let mut queued = BinaryHeap::new();
        let mut completed = 0_usize;
        while completed < source_count || !queued.is_empty() {
            tokio::select! {
                biased;
                Some((source, signal)) = rx.recv(), if completed < source_count => {
                    let state = states.get_mut(&source).ok_or_else(|| replay_gap("unknown source"))?;
                    match signal {
                        SourceSignal::Data(envelope) => {
                            let envelope = *envelope;
                            let key = envelope.canonical_key().ok_or_else(|| replay_gap("unsupported CEX reference fact"))?;
                            if state.watermark.is_some_and(|watermark| key.timestamp_ms() <= watermark) {
                                return abort_and_drain(&mut tasks, replay_gap("late record"), &mut output).await;
                            }
                            state.data_seen = true;
                            queued.push(Reverse(QueuedFact { key, fact: envelope.into_strategy_fact() }));
                        }
                        SourceSignal::Watermark(watermark) => {
                            if state.watermark.is_some_and(|previous| watermark < previous) {
                                return abort_and_drain(&mut tasks, replay_gap("watermark regressed"), &mut output).await;
                            }
                            state.watermark = Some(watermark);
                        }
                        SourceSignal::Eof => state.eof_seen = true,
                    }
                    release_safe(&mut queued, &states, &mut output);
                }
                joined = tasks.join_next(), if completed < source_count => {
                    let joined = joined.ok_or_else(|| replay_gap("source task set ended early"))?;
                    let source = joined
                        .map_err(|error| replay_gap(&format!("source task failed: {error}")))??;
                    let state = states.get_mut(&source).ok_or_else(|| replay_gap("completed unknown source"))?;
                    if !state.eof_seen {
                        return abort_and_drain(&mut tasks, replay_gap("premature EOF"), &mut output).await;
                    }
                    if matches!(self.mode, FeedMode::Backtest)
                        && self.replay_end_ms.is_some_and(|end| state.watermark.unwrap_or(i64::MIN) < end)
                    {
                        return abort_and_drain(&mut tasks, replay_gap("historical coverage ends before replay end"), &mut output).await;
                    }
                    state.completed = true;
                    completed += 1;
                    release_safe(&mut queued, &states, &mut output);
                }
                else => return Err(replay_gap("source channel closed before source completion")),
            }
        }
        if states
            .values()
            .any(|state| !state.data_seen || state.watermark.is_none())
        {
            return Err(replay_gap("source did not warm up"));
        }
        Ok(output)
    }
}

fn release_safe(
    queued: &mut BinaryHeap<Reverse<QueuedFact>>,
    states: &HashMap<String, SourceState>,
    output: &mut Vec<StrategyFact>,
) {
    let watermark = states.values().filter_map(|state| state.watermark).min();
    while watermark.is_some_and(|watermark| {
        queued
            .peek()
            .is_some_and(|next| next.0.key.timestamp_ms() <= watermark)
    }) {
        if let Some(Reverse(next)) = queued.pop() {
            output.push(next.fact);
        }
    }
}

async fn abort_and_drain(
    tasks: &mut JoinSet<Result<String, DataSourceError>>,
    error: DataSourceError,
    _output: &mut Vec<StrategyFact>,
) -> Result<Vec<StrategyFact>, DataSourceError> {
    tasks.abort_all();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(_) | Err(_)) | Err(_) => {}
        }
    }
    Err(error)
}

fn replay_gap(message: &str) -> DataSourceError {
    DataSourceError::ReplayGap {
        message: message.to_owned(),
    }
}
