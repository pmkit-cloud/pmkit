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
                            let key = envelope.canonical_key();
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
    let watermark = states
        .values()
        .map(|state| state.watermark)
        .collect::<Option<Vec<_>>>()
        .and_then(|watermarks| watermarks.into_iter().min());
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
            .then_with(|| {
                market_event_detail_key(&left.fact).cmp(&market_event_detail_key(&right.fact))
            }),
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
            format!("{order_id}:{market}:{outcome:?}:{price}:{size}:{side:?}:{fee}:{timestamp_ms}")
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
            market,
            outcome,
            settled_size,
            proceeds,
            timestamp_ms,
        } => format!("{market}:{outcome:?}:{settled_size}:{proceeds}:{timestamp_ms}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{QueuedFact, merged_fact};
    use pmkit_core::MarketId;
    use pmkit_event::{MarketEvent, PmMarketEnvelope, SourceEnvelope, StreamMetadata};
    use pmkit_market::Outcome;
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

    fn queued(market: &str) -> Result<QueuedFact, Box<dyn std::error::Error>> {
        let envelope = SourceEnvelope::PmMarket(PmMarketEnvelope {
            metadata: metadata(),
            raw_frame: Vec::new(),
            fact: MarketEvent::BookUpdate {
                market: MarketId::new(market)?,
                outcome: Outcome::Up,
                bids: vec![(Decimal::new(49, 2), Decimal::ONE)],
                asks: vec![(Decimal::new(51, 2), Decimal::ONE)],
                timestamp_ms: 10,
            },
        });
        Ok(QueuedFact {
            key: envelope.canonical_key(),
            fact: merged_fact(envelope),
        })
    }

    #[test]
    fn cross_market_key_collision_regression() -> Result<(), Box<dyn std::error::Error>> {
        // Given: two PM envelopes with identical durable replay metadata but different markets.
        let left = queued("btc-5m")?;
        let right = queued("eth-5m")?;

        // When: the merge queue compares them for release ordering.
        let ordering = left.cmp(&right);

        // Then: the in-process merge must not treat them as the same sortable item.
        assert_ne!(ordering, Ordering::Equal);
        assert_ne!(left, right);
        Ok(())
    }
}
