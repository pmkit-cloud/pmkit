//! Read-only Orbscan order-book history for research and backtests.
//!
//! Orbscan is exposed as a separate, uncorroborated source. Its global coverage
//! bounds are advisory and do not prove per-market completeness. Use
//! `EvidenceRequirement::AllowSingleSource` for exploratory runs; this adapter
//! rejects `CorroboratedOnly` rather than implying trust it cannot establish.
//! Equal-time records from separate outcome streams use `PMKit`'s canonical key
//! as a deterministic replay order; this is not an Orbscan causal-order claim.

use std::{
    cmp::{Ordering, Reverse},
    collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque},
    env, fmt,
    time::Duration,
};

use async_trait::async_trait;
use pmkit_book::OrderBookL2;
use pmkit_core::MarketId;
use pmkit_data::{DataSourceError, HistoricalDataSource, ReplayQuery, SourceSignal};
use pmkit_event::{MarketEvent, PmMarketEnvelope, SourceEnvelope, StreamMetadata};
use pmkit_market::Outcome;
use pmkit_run::EvidenceRequirement;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::sync::mpsc::Sender;

const API_BASE: &str = "https://api.orbscan.com";
const MARKET_PAGE_LIMIT: &str = "1000";
const EVENT_PAGE_LIMIT: &str = "1000";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Fail-closed errors returned by the Orbscan adapter.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum OrbscanError {
    /// `ORBSCAN_API_KEY` was absent or blank.
    #[error("ORBSCAN_API_KEY is missing or blank")]
    MissingApiKey,
    /// The HTTP client could not be built.
    #[error("Orbscan HTTP client could not be built")]
    ClientBuild,
    /// A request could not be completed.
    #[error("Orbscan request failed")]
    RequestFailed,
    /// Orbscan returned an unsuccessful HTTP status.
    #[error("Orbscan returned HTTP status {0}")]
    HttpStatus(u16),
    /// Orbscan rate-limited this credential; no automatic retry was attempted.
    #[error("Orbscan rate limit exceeded (HTTP 429)")]
    RateLimited,
    /// The response did not match the documented success envelope.
    #[error("Orbscan response is malformed")]
    MalformedResponse,
    /// Pagination failed to make progress.
    #[error("Orbscan pagination repeated a cursor")]
    RepeatedCursor,
    /// Orbscan returned an empty page with a continuation cursor.
    #[error("Orbscan pagination returned an empty page with a continuation cursor")]
    EmptyPage,
    /// An Orbscan market or outcome could not be mapped unambiguously.
    #[error("Orbscan market mapping is invalid: {0}")]
    InvalidMapping(&'static str),
    /// An Orbscan market record failed validation.
    #[error("Orbscan market record is malformed: {0}")]
    MalformedMarket(&'static str),
    /// An Orbscan event failed validation or book reconstruction.
    #[error("Orbscan order-book event is malformed: {0}")]
    MalformedEvent(&'static str),
    /// No book snapshot exists at or before the requested start time.
    #[error("Orbscan has no order-book snapshot at or before the replay start")]
    MissingSnapshot,
}

/// Orbscan's availability status, distinct from Gamma's market status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrbscanMarketStatus {
    /// Orbscan currently tracks the market as active.
    Active,
    /// Orbscan does not currently track the market as active.
    Closed,
}

/// One named outcome token returned by Orbscan market discovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrbscanOutcome {
    /// Provider outcome label; never implicitly treated as Up or Down.
    pub name: String,
    /// Positive uint256 token ID encoded as a decimal string.
    pub token_id: String,
}

/// A market listed by Orbscan's order-book API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrbscanMarket {
    /// Gamma market ID as a decimal string.
    pub market_id: String,
    /// Polymarket condition ID.
    pub condition_id: String,
    /// Market question.
    pub question: String,
    /// Market slug, retained as provider metadata only.
    pub slug: String,
    /// Orbscan tracking status; this is not Gamma's `closed` flag.
    pub status: OrbscanMarketStatus,
    /// Market open time in Unix milliseconds.
    pub open_at: i64,
    /// Market close time in Unix milliseconds.
    pub close_at: i64,
    /// Named outcome tokens reported by Orbscan.
    pub outcomes: Vec<OrbscanOutcome>,
}

impl OrbscanMarket {
    fn validate(&self) -> Result<(), OrbscanError> {
        if !valid_market_id(&self.market_id) || !valid_condition_id(&self.condition_id) {
            return Err(OrbscanError::MalformedMarket("invalid market identity"));
        }
        if self.open_at > self.close_at || self.outcomes.len() < 2 {
            return Err(OrbscanError::MalformedMarket(
                "invalid market window or outcomes",
            ));
        }
        let mut names = HashSet::new();
        let mut tokens = HashSet::new();
        for outcome in &self.outcomes {
            if outcome.name.trim().is_empty()
                || !valid_uint256(&outcome.token_id)
                || !names.insert(outcome.name.as_str())
                || !tokens.insert(outcome.token_id.as_str())
            {
                return Err(OrbscanError::MalformedMarket("invalid outcome identity"));
            }
        }
        Ok(())
    }

    /// Explicitly maps two outcome labels to `PMKit`'s binary outcomes.
    ///
    /// No inference is made from the question, slug, or label. Both labels must
    /// exactly match the two provider outcomes.
    ///
    /// # Errors
    ///
    /// Returns [`OrbscanError::InvalidMapping`] unless the named outcomes are
    /// distinct, unambiguous, and cover a binary Orbscan market.
    pub fn map_to(
        &self,
        market: MarketId,
        up_label: &str,
        down_label: &str,
    ) -> Result<OrbscanMarketMapping, OrbscanError> {
        self.validate()?;
        if self.outcomes.len() != 2 || up_label == down_label {
            return Err(OrbscanError::InvalidMapping(
                "PMKit requires two distinct binary outcome labels",
            ));
        }
        let up = self
            .outcomes
            .iter()
            .find(|outcome| outcome.name == up_label)
            .ok_or(OrbscanError::InvalidMapping(
                "Up label is not an exact outcome",
            ))?;
        let down = self
            .outcomes
            .iter()
            .find(|outcome| outcome.name == down_label)
            .ok_or(OrbscanError::InvalidMapping(
                "Down label is not an exact outcome",
            ))?;
        if up.token_id == down.token_id {
            return Err(OrbscanError::InvalidMapping("outcome tokens must differ"));
        }
        Ok(OrbscanMarketMapping {
            market,
            orbscan_market_id: self.market_id.clone(),
            condition_id: self.condition_id.to_ascii_lowercase(),
            up: MappedOutcome {
                name: up.name.clone(),
                token_id: up.token_id.clone(),
            },
            down: MappedOutcome {
                name: down.name.clone(),
                token_id: down.token_id.clone(),
            },
        })
    }
}

/// Global Orbscan order-book time bounds in Unix milliseconds.
///
/// These bounds describe the stored table only. They do not establish that a
/// requested market or every timestamp inside the range is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrbscanCoverage {
    /// Earliest source timestamp across Orbscan's order-book table.
    #[serde(rename = "availableFrom")]
    pub available_from_ms: i64,
    /// Latest source timestamp across Orbscan's order-book table.
    #[serde(rename = "availableTo")]
    pub available_to_ms: i64,
}

/// A reusable explicit mapping from provider outcome labels to `PMKit` outcomes.
#[derive(Debug, Clone)]
pub struct OrbscanMarketMapping {
    market: MarketId,
    orbscan_market_id: String,
    condition_id: String,
    up: MappedOutcome,
    down: MappedOutcome,
}

#[derive(Debug, Clone)]
struct MappedOutcome {
    name: String,
    token_id: String,
}

/// Read-only client for Orbscan's order-book discovery and history endpoints.
#[derive(Clone)]
pub struct OrbscanClient {
    http: reqwest::Client,
    api_key: String,
    base_url: String,
}

impl fmt::Debug for OrbscanClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OrbscanClient")
            .field("base_url", &self.base_url)
            .field("api_key", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl OrbscanClient {
    /// Creates a client pinned to Orbscan's documented HTTPS endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`OrbscanError::MissingApiKey`] for a blank key or
    /// [`OrbscanError::ClientBuild`] if the HTTP client cannot be initialized.
    pub fn new(api_key: impl Into<String>) -> Result<Self, OrbscanError> {
        Self::with_endpoint(api_key, API_BASE)
    }

    /// Reads the dedicated `ORBSCAN_API_KEY` setting without logging it.
    ///
    /// # Errors
    ///
    /// Returns [`OrbscanError::MissingApiKey`] when the setting is absent or
    /// blank, or [`OrbscanError::ClientBuild`] if the HTTP client cannot start.
    pub fn from_env() -> Result<Self, OrbscanError> {
        let api_key = env::var("ORBSCAN_API_KEY").map_err(|_| OrbscanError::MissingApiKey)?;
        Self::new(api_key)
    }

    fn with_endpoint(
        api_key: impl Into<String>,
        base_url: impl Into<String>,
    ) -> Result<Self, OrbscanError> {
        let api_key = api_key.into();
        if api_key.trim().is_empty() {
            return Err(OrbscanError::MissingApiKey);
        }
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| OrbscanError::ClientBuild)?;
        Ok(Self {
            http,
            api_key,
            base_url: base_url.into().trim_end_matches('/').to_owned(),
        })
    }

    /// Lists all Orbscan-tracked crypto markets using cursor pagination.
    ///
    /// # Errors
    ///
    /// Returns an [`OrbscanError`] for HTTP failures, malformed records, or
    /// pagination that is empty or fails to advance.
    pub async fn markets(&self) -> Result<Vec<OrbscanMarket>, OrbscanError> {
        let mut markets = Vec::new();
        let mut market_ids = HashSet::new();
        let mut seen_cursors = HashSet::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut query = vec![("limit".to_owned(), MARKET_PAGE_LIMIT.to_owned())];
            if let Some(value) = &cursor {
                query.push(("cursor".to_owned(), value.clone()));
            }
            let page: ApiPage<OrbscanMarket> =
                self.get_json("/v1/orderbook/markets", &query).await?;
            if page.items.is_empty() && page.next_cursor.is_some() {
                return Err(OrbscanError::EmptyPage);
            }
            for market in page.items {
                market.validate()?;
                if !market_ids.insert(market.market_id.clone()) {
                    return Err(OrbscanError::MalformedMarket("duplicate market ID"));
                }
                markets.push(market);
            }
            let Some(next) = page.next_cursor else {
                return Ok(markets);
            };
            if next.is_empty() || !seen_cursors.insert(next.clone()) {
                return Err(OrbscanError::RepeatedCursor);
            }
            cursor = Some(next);
        }
    }

    /// Reads Orbscan's global order-book coverage bounds.
    ///
    /// This is advisory only; it does not prove per-market completeness.
    ///
    /// # Errors
    ///
    /// Returns an [`OrbscanError`] when the endpoint fails or returns invalid bounds.
    pub async fn coverage(&self) -> Result<OrbscanCoverage, OrbscanError> {
        let data: CoverageData = self.get_json("/v1/orderbook/coverage", &[]).await?;
        let coverage = data.polymarket.orderbook;
        if coverage.available_from_ms > coverage.available_to_ms {
            return Err(OrbscanError::MalformedResponse);
        }
        Ok(coverage)
    }

    async fn event_page(
        &self,
        token_id: &str,
        event_type: &str,
        to_ms: i64,
        order: &str,
        limit: &str,
        cursor: Option<&str>,
    ) -> Result<ApiPage<OrbscanEvent>, OrbscanError> {
        let mut query = vec![
            ("tokenId".to_owned(), token_id.to_owned()),
            ("eventType".to_owned(), event_type.to_owned()),
            ("to".to_owned(), to_ms.to_string()),
            ("order".to_owned(), order.to_owned()),
            ("limit".to_owned(), limit.to_owned()),
        ];
        if let Some(cursor) = cursor {
            query.push(("cursor".to_owned(), cursor.to_owned()));
        }
        self.get_json("/v1/orderbook/events", &query).await
    }

    async fn get_json<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(String, String)],
    ) -> Result<T, OrbscanError> {
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .http
            .get(url)
            .bearer_auth(&self.api_key)
            .query(query)
            .send()
            .await
            .map_err(|_| OrbscanError::RequestFailed)?;
        let status = response.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(OrbscanError::RateLimited);
        }
        if !status.is_success() {
            return Err(OrbscanError::HttpStatus(status.as_u16()));
        }
        let body = response
            .bytes()
            .await
            .map_err(|_| OrbscanError::RequestFailed)?;
        let envelope: ApiEnvelope<T> =
            serde_json::from_slice(&body).map_err(|_| OrbscanError::MalformedResponse)?;
        if envelope.message != "OK" || envelope.status_code != "1" {
            return Err(OrbscanError::MalformedResponse);
        }
        Ok(envelope.data)
    }
}

/// A historical Orbscan source for explicitly mapped binary markets.
#[derive(Debug, Clone)]
pub struct OrbscanHistoricalDataSource {
    client: OrbscanClient,
    mappings: HashMap<MarketId, OrbscanMarketMapping>,
}

impl OrbscanHistoricalDataSource {
    /// Creates a source from explicit market and outcome mappings.
    ///
    /// # Errors
    ///
    /// Returns [`OrbscanError::InvalidMapping`] for duplicate `PMKit` markets or
    /// token IDs. Each mapping must originate from a validated Orbscan market.
    pub fn new(
        client: OrbscanClient,
        mappings: impl IntoIterator<Item = OrbscanMarketMapping>,
    ) -> Result<Self, OrbscanError> {
        let mut by_market = HashMap::new();
        let mut tokens = HashSet::new();
        for mapping in mappings {
            if !tokens.insert(mapping.up.token_id.clone())
                || !tokens.insert(mapping.down.token_id.clone())
                || by_market.insert(mapping.market.clone(), mapping).is_some()
            {
                return Err(OrbscanError::InvalidMapping(
                    "market or token identity is duplicated",
                ));
            }
        }
        Ok(Self {
            client,
            mappings: by_market,
        })
    }
}

#[async_trait]
impl HistoricalDataSource for OrbscanHistoricalDataSource {
    async fn replay(
        &self,
        query: ReplayQuery,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        let cancellation = sink.clone();
        let replay_result = tokio::select! {
            biased;
            () = cancellation.closed() => Err(DataSourceError::SinkClosed),
            result = self.replay_inner(query, sink.clone()) => result,
        };
        replay_result?;
        sink.send(SourceSignal::Eof)
            .await
            .map_err(|_| DataSourceError::SinkClosed)
    }
}

impl OrbscanHistoricalDataSource {
    async fn replay_inner(
        &self,
        query: ReplayQuery,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        if query.evidence == EvidenceRequirement::CorroboratedOnly {
            return Err(DataSourceError::ReplayGap {
                message: "Orbscan is uncorroborated research data; opt into AllowSingleSource"
                    .into(),
            });
        }
        let from_ms = query.from.timestamp_millis();
        let to_ms = query.to.timestamp_millis();
        if from_ms >= to_ms {
            sink.send(SourceSignal::Watermark(to_ms))
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            return Ok(());
        }

        let mut selected = HashSet::new();
        let mut streams = Vec::new();
        for market in query.markets {
            if !selected.insert(market.clone()) {
                continue;
            }
            let mapping = self
                .mappings
                .get(&market)
                .ok_or(DataSourceError::NotAvailable)?;
            streams.push(
                TokenReplay::new(&self.client, mapping, Outcome::Up, from_ms, to_ms)
                    .await
                    .map_err(|error| to_data_error(&error))?,
            );
            streams.push(
                TokenReplay::new(&self.client, mapping, Outcome::Down, from_ms, to_ms)
                    .await
                    .map_err(|error| to_data_error(&error))?,
            );
        }

        let mut pending = BinaryHeap::new();
        for (stream, replay) in streams.iter_mut().enumerate() {
            if let Some(envelope) = replay
                .next_book_update()
                .await
                .map_err(|error| to_data_error(&error))?
            {
                pending.push(Reverse(PendingEvent::new(stream, envelope)));
            }
        }
        let mut last_watermark = None;
        while let Some(Reverse(event)) = pending.pop() {
            sink.send(SourceSignal::Data(Box::new(event.envelope)))
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            if let Some(envelope) = streams[event.stream]
                .next_book_update()
                .await
                .map_err(|error| to_data_error(&error))?
            {
                pending.push(Reverse(PendingEvent::new(event.stream, envelope)));
            }
            let watermark = match pending.peek() {
                Some(Reverse(next)) => next
                    .key
                    .timestamp_ms()
                    .checked_sub(1)
                    .ok_or_else(|| replay_gap("watermark underflow"))?,
                None => to_ms,
            };
            if last_watermark.is_some_and(|previous| watermark < previous) {
                return Err(replay_gap("Orbscan replay watermark regressed"));
            }
            if last_watermark != Some(watermark) {
                sink.send(SourceSignal::Watermark(watermark))
                    .await
                    .map_err(|_| DataSourceError::SinkClosed)?;
                last_watermark = Some(watermark);
            }
        }
        if last_watermark.is_none_or(|watermark| watermark < to_ms) {
            sink.send(SourceSignal::Watermark(to_ms))
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
        }
        Ok(())
    }
}

struct PendingEvent {
    stream: usize,
    key: pmkit_event::CanonicalSourceKey,
    envelope: SourceEnvelope,
}

impl PendingEvent {
    fn new(stream: usize, envelope: SourceEnvelope) -> Self {
        Self {
            stream,
            key: envelope.canonical_key(),
            envelope,
        }
    }
}

impl PartialEq for PendingEvent {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.stream == other.stream
    }
}
impl Eq for PendingEvent {}
impl PartialOrd for PendingEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for PendingEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.stream.cmp(&other.stream))
    }
}

struct TokenReplay<'a> {
    client: &'a OrbscanClient,
    market: MarketId,
    orbscan_market_id: &'a str,
    condition_id: &'a str,
    token: &'a MappedOutcome,
    outcome: Outcome,
    from_ms: i64,
    to_ms: i64,
    snapshot_timestamp_ms: i64,
    book: BookLevels,
    next_cursor: Option<String>,
    pages: VecDeque<OrbscanEvent>,
    seen_page_cursors: HashSet<String>,
    last_event_cursor: Option<String>,
    last_timestamp_ms: i64,
    exhausted: bool,
    baseline_emitted: bool,
    pending_event: Option<OrbscanEvent>,
    frame_sequence: i64,
}

impl<'a> TokenReplay<'a> {
    async fn new(
        client: &'a OrbscanClient,
        mapping: &'a OrbscanMarketMapping,
        outcome: Outcome,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Self, OrbscanError> {
        let token = match outcome {
            Outcome::Up => &mapping.up,
            Outcome::Down => &mapping.down,
        };
        let page = client
            .event_page(&token.token_id, "book", from_ms, "desc", "1", None)
            .await?;
        if page.items.is_empty() {
            return Err(OrbscanError::MissingSnapshot);
        }
        if page.items.len() != 1 {
            return Err(OrbscanError::MalformedEvent(
                "snapshot query returned multiple rows",
            ));
        }
        let snapshot = page
            .items
            .into_iter()
            .next()
            .ok_or(OrbscanError::MissingSnapshot)?;
        validate_event_identity(&snapshot, mapping, token, "book")?;
        if snapshot.timestamp > from_ms
            || snapshot.cursor.is_empty()
            || snapshot.side.is_some()
            || snapshot.price.is_some()
            || snapshot.size_after.is_some()
        {
            return Err(OrbscanError::MalformedEvent("invalid book snapshot"));
        }
        let snapshot_cursor = snapshot.cursor.clone();
        let snapshot_timestamp_ms = snapshot.timestamp;
        let book = BookLevels::from_snapshot(&snapshot)?;
        Ok(Self {
            client,
            market: mapping.market.clone(),
            orbscan_market_id: &mapping.orbscan_market_id,
            condition_id: &mapping.condition_id,
            token,
            outcome,
            from_ms,
            to_ms,
            snapshot_timestamp_ms,
            book,
            next_cursor: Some(snapshot_cursor.clone()),
            pages: VecDeque::new(),
            seen_page_cursors: HashSet::from([snapshot_cursor.clone()]),
            last_event_cursor: Some(snapshot_cursor),
            last_timestamp_ms: snapshot_timestamp_ms,
            exhausted: false,
            baseline_emitted: false,
            pending_event: None,
            frame_sequence: 0,
        })
    }

    async fn next_book_update(&mut self) -> Result<Option<SourceEnvelope>, OrbscanError> {
        if !self.baseline_emitted {
            loop {
                let Some(event) = self.next_delta().await? else {
                    self.baseline_emitted = true;
                    return self.make_envelope(self.from_ms, Vec::new()).map(Some);
                };
                if event.timestamp < self.from_ms {
                    self.book.apply(&event)?;
                } else {
                    self.pending_event = Some(event);
                    self.baseline_emitted = true;
                    return self.make_envelope(self.from_ms, Vec::new()).map(Some);
                }
            }
        }
        let event = match self.pending_event.take() {
            Some(event) => Some(event),
            None => self.next_delta().await?,
        };
        let Some(event) = event else {
            return Ok(None);
        };
        self.book.apply(&event)?;
        let raw_frame = serde_json::to_vec(&event).map_err(|_| OrbscanError::MalformedResponse)?;
        self.make_envelope(event.timestamp, raw_frame).map(Some)
    }

    async fn next_delta(&mut self) -> Result<Option<OrbscanEvent>, OrbscanError> {
        loop {
            if let Some(event) = self.pages.pop_front() {
                return Ok(Some(event));
            }
            if self.exhausted {
                return Ok(None);
            }
            let page = self
                .client
                .event_page(
                    &self.token.token_id,
                    "price_change",
                    self.to_ms - 1,
                    "asc",
                    EVENT_PAGE_LIMIT,
                    self.next_cursor.as_deref(),
                )
                .await?;
            if page.items.is_empty() && page.next_cursor.is_some() {
                return Err(OrbscanError::EmptyPage);
            }
            let mut page_cursors = HashSet::new();
            for event in &page.items {
                validate_event_identity_view(
                    event,
                    self.mapping_view(),
                    self.token,
                    "price_change",
                )?;
                if event.timestamp < self.snapshot_timestamp_ms
                    || event.timestamp < self.last_timestamp_ms
                    || event.timestamp >= self.to_ms
                    || event.cursor.is_empty()
                    || self.last_event_cursor.as_deref() == Some(event.cursor.as_str())
                    || !page_cursors.insert(event.cursor.as_str())
                {
                    return Err(OrbscanError::MalformedEvent(
                        "price-change identity, cursor, or timestamp regressed",
                    ));
                }
                self.last_timestamp_ms = event.timestamp;
                self.last_event_cursor = Some(event.cursor.clone());
                validate_delta(event)?;
            }
            if let Some(next) = &page.next_cursor {
                if next.is_empty() || !self.seen_page_cursors.insert(next.clone()) {
                    return Err(OrbscanError::RepeatedCursor);
                }
            } else {
                self.exhausted = true;
            }
            self.next_cursor = page.next_cursor;
            self.pages = page.items.into();
        }
    }

    const fn mapping_view(&self) -> OrbscanMarketMappingView<'_> {
        OrbscanMarketMappingView {
            market_id: self.orbscan_market_id,
            condition_id: self.condition_id,
        }
    }

    fn make_envelope(
        &mut self,
        timestamp_ms: i64,
        raw_frame: Vec<u8>,
    ) -> Result<SourceEnvelope, OrbscanError> {
        let frame_sequence = self.frame_sequence;
        self.frame_sequence = self
            .frame_sequence
            .checked_add(1)
            .ok_or(OrbscanError::MalformedEvent("frame sequence overflow"))?;
        let book = self.book.to_order_book(timestamp_ms);
        Ok(SourceEnvelope::PmMarket(PmMarketEnvelope {
            metadata: StreamMetadata {
                schema_version: 1,
                source_id: "orbscan-orderbook".to_owned(),
                source_time_ms: timestamp_ms,
                canonical_source_rank: 0,
                receipt_time_ms: timestamp_ms,
                connection_id: format!("token:{}", self.token.token_id),
                connection_epoch: 0,
                frame_sequence,
                ingest_sequence: u64::try_from(frame_sequence)
                    .map_err(|_| OrbscanError::MalformedEvent("negative frame sequence"))?,
            },
            // Event items are re-serialized JSON, not byte-identical HTTP wire data.
            raw_frame,
            fact: MarketEvent::BookUpdate {
                market: self.market.clone(),
                outcome: self.outcome,
                bids: book.bids,
                asks: book.asks,
                timestamp_ms,
            },
        }))
    }
}

#[derive(Clone, Copy)]
struct OrbscanMarketMappingView<'a> {
    market_id: &'a str,
    condition_id: &'a str,
}

fn validate_event_identity(
    event: &OrbscanEvent,
    mapping: &OrbscanMarketMapping,
    token: &MappedOutcome,
    event_type: &str,
) -> Result<(), OrbscanError> {
    validate_event_identity_view(
        event,
        OrbscanMarketMappingView {
            market_id: &mapping.orbscan_market_id,
            condition_id: &mapping.condition_id,
        },
        token,
        event_type,
    )
}

fn validate_event_identity_view(
    event: &OrbscanEvent,
    mapping: OrbscanMarketMappingView<'_>,
    token: &MappedOutcome,
    event_type: &str,
) -> Result<(), OrbscanError> {
    if event.market_id != mapping.market_id
        || !event
            .condition_id
            .eq_ignore_ascii_case(mapping.condition_id)
        || event.token_id != token.token_id
        || event.event_type != event_type
        || event
            .outcome
            .as_ref()
            .is_some_and(|name| name != &token.name)
    {
        return Err(OrbscanError::MalformedEvent(
            "provider identity does not match mapping",
        ));
    }
    Ok(())
}

fn validate_delta(event: &OrbscanEvent) -> Result<(), OrbscanError> {
    if event.cursor.is_empty() {
        return Err(OrbscanError::MalformedEvent("price change lacks cursor"));
    }
    let side = event
        .side
        .as_deref()
        .ok_or(OrbscanError::MalformedEvent("price change lacks side"))?;
    if !matches!(side, "BUY" | "SELL") {
        return Err(OrbscanError::MalformedEvent("unknown price-change side"));
    }
    let price = parse_decimal(
        event
            .price
            .as_deref()
            .ok_or(OrbscanError::MalformedEvent("price change lacks price"))?,
    )?;
    let level_size = parse_decimal(
        event
            .size_after
            .as_deref()
            .ok_or(OrbscanError::MalformedEvent("price change lacks sizeAfter"))?,
    )?;
    if price < Decimal::ZERO || price > Decimal::ONE || level_size < Decimal::ZERO {
        return Err(OrbscanError::MalformedEvent(
            "price or size is out of range",
        ));
    }
    Ok(())
}

#[derive(Default)]
struct BookLevels {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

impl BookLevels {
    fn from_snapshot(event: &OrbscanEvent) -> Result<Self, OrbscanError> {
        let bids = parse_levels(
            event
                .bids
                .as_deref()
                .ok_or(OrbscanError::MalformedEvent("book snapshot lacks bids"))?,
        )?;
        let asks = parse_levels(
            event
                .asks
                .as_deref()
                .ok_or(OrbscanError::MalformedEvent("book snapshot lacks asks"))?,
        )?;
        Ok(Self { bids, asks })
    }

    fn apply(&mut self, event: &OrbscanEvent) -> Result<(), OrbscanError> {
        validate_delta(event)?;
        let price = parse_decimal(
            event
                .price
                .as_deref()
                .ok_or(OrbscanError::MalformedEvent("price change lacks price"))?,
        )?;
        let size = parse_decimal(
            event
                .size_after
                .as_deref()
                .ok_or(OrbscanError::MalformedEvent("price change lacks sizeAfter"))?,
        )?;
        let levels = match event.side.as_deref() {
            Some("BUY") => &mut self.bids,
            Some("SELL") => &mut self.asks,
            _ => return Err(OrbscanError::MalformedEvent("unknown price-change side")),
        };
        if size.is_zero() {
            levels.remove(&price);
        } else {
            levels.insert(price, size);
        }
        Ok(())
    }

    fn to_order_book(&self, timestamp_ms: i64) -> OrderBookL2 {
        OrderBookL2 {
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(price, size)| (*price, *size))
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(price, size)| (*price, *size))
                .collect(),
            timestamp_ms,
            last_trade_price: None,
        }
    }
}

fn parse_levels(levels: &[[String; 2]]) -> Result<BTreeMap<Decimal, Decimal>, OrbscanError> {
    let mut parsed = BTreeMap::new();
    for [raw_price, raw_size] in levels {
        let price = parse_decimal(raw_price)?;
        let size = parse_decimal(raw_size)?;
        if price < Decimal::ZERO
            || price > Decimal::ONE
            || size <= Decimal::ZERO
            || parsed.insert(price, size).is_some()
        {
            return Err(OrbscanError::MalformedEvent(
                "invalid or duplicate book level",
            ));
        }
    }
    Ok(parsed)
}

fn parse_decimal(value: &str) -> Result<Decimal, OrbscanError> {
    Decimal::from_str_exact(value).map_err(|_| OrbscanError::MalformedEvent("invalid decimal"))
}

fn valid_market_id(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && value.parse::<i64>().is_ok()
}

fn valid_condition_id(value: &str) -> bool {
    value.strip_prefix("0x").is_some_and(|digits| {
        digits.len() == 64 && digits.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

fn valid_uint256(value: &str) -> bool {
    const MAX: &str =
        "115792089237316195423570985008687907853269984665640564039457584007913129639935";
    !value.is_empty()
        && value.len() <= MAX.len()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && value.bytes().any(|byte| byte != b'0')
        && (value.len() < MAX.len() || value <= MAX)
}

fn to_data_error(error: &OrbscanError) -> DataSourceError {
    let message = error.to_string();
    match error {
        OrbscanError::MissingSnapshot => DataSourceError::NotAvailable,
        OrbscanError::RequestFailed
        | OrbscanError::ClientBuild
        | OrbscanError::HttpStatus(_)
        | OrbscanError::RateLimited => DataSourceError::Unavailable { message },
        OrbscanError::MissingApiKey
        | OrbscanError::MalformedResponse
        | OrbscanError::RepeatedCursor
        | OrbscanError::EmptyPage
        | OrbscanError::InvalidMapping(_)
        | OrbscanError::MalformedMarket(_)
        | OrbscanError::MalformedEvent(_) => DataSourceError::ReplayGap { message },
    }
}

fn replay_gap(message: &str) -> DataSourceError {
    DataSourceError::ReplayGap {
        message: message.to_owned(),
    }
}

#[derive(Deserialize)]
struct ApiEnvelope<T> {
    message: String,
    status_code: String,
    data: T,
}

#[derive(Deserialize)]
struct ApiPage<T> {
    items: Vec<T>,
    #[serde(rename = "nextCursor", deserialize_with = "required_nullable")]
    next_cursor: Option<String>,
}

fn required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[derive(Deserialize)]
struct CoverageData {
    polymarket: ProviderCoverage,
}

#[derive(Deserialize)]
struct ProviderCoverage {
    orderbook: OrbscanCoverage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrbscanEvent {
    cursor: String,
    market_id: String,
    condition_id: String,
    token_id: String,
    outcome: Option<String>,
    event_type: String,
    timestamp: i64,
    indexed_timestamp: i64,
    side: Option<String>,
    price: Option<String>,
    size_after: Option<String>,
    bids: Option<Vec<[String; 2]>>,
    asks: Option<Vec<[String; 2]>>,
    source_hash: Option<String>,
}

#[cfg(test)]
mod tests;
