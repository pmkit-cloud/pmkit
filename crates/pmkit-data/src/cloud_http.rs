use std::{collections::HashSet, io::Cursor};

use reqwest::{StatusCode, Url};
use serde::Deserialize;
use tokio::sync::mpsc::Sender;

use super::{
    CloudReplayQuery, PmKitCloudDataSource, cloud_cache, cloud_decode,
    cloud_types::{
        CloudCoverage, CloudCoverageStatus, CloudReplayError, CloudReplaySelector, RetrievalState,
    },
};
use crate::SourceSignal;

#[derive(Debug, Deserialize)]
struct SegmentPage {
    next_cursor: Option<String>,
    segments: Vec<Segment>,
}

#[derive(Debug, Deserialize)]
pub(super) struct Segment {
    pub(super) bytes: u64,
    pub(super) encoded_bytes: u64,
    pub(super) encoded_sha256: String,
    pub(super) from_ts_ms: i64,
    pub(super) release_id: String,
    #[serde(rename = "segment_id")]
    pub(super) id: String,
    pub(super) sha256: String,
    pub(super) to_ts_ms: i64,
    pub(super) market_id: String,
    pub(super) series_id: String,
    pub(super) asset: Option<String>,
    pub(super) duration_seconds: Option<u64>,
    pub(super) outcome_tokens: Vec<OutcomeToken>,
    availability: Availability,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OutcomeToken {
    pub(super) outcome: String,
    pub(super) token_id: String,
}

#[derive(Debug, Deserialize)]
struct Availability {
    state: RetrievalState,
}

pub(super) async fn coverage(
    source: &PmKitCloudDataSource,
    query: &CloudReplayQuery,
) -> Result<CloudCoverage, CloudReplayError> {
    let coverage_url = range_url(source, "coverage", query, None)?;
    json::<CloudCoverage>(source, coverage_url).await
}

pub(super) async fn replay(
    source: &PmKitCloudDataSource,
    query: CloudReplayQuery,
    sink: Sender<SourceSignal>,
) -> Result<(), CloudReplayError> {
    replay_segments(source, &query, &sink).await?;
    finish(&sink, query.to.timestamp_millis()).await
}

pub(super) async fn replay_markets(
    source: &PmKitCloudDataSource,
    markets: Vec<pmkit_core::MarketId>,
    from: chrono::DateTime<chrono::Utc>,
    to: chrono::DateTime<chrono::Utc>,
    sink: Sender<SourceSignal>,
) -> Result<(), CloudReplayError> {
    for market in markets {
        replay_segments(
            source,
            &CloudReplayQuery {
                selector: CloudReplaySelector::Market(market),
                from,
                to,
            },
            &sink,
        )
        .await?;
    }
    finish(&sink, to.timestamp_millis()).await
}

async fn replay_segments(
    source: &PmKitCloudDataSource,
    query: &CloudReplayQuery,
    sink: &Sender<SourceSignal>,
) -> Result<(), CloudReplayError> {
    query.validate()?;
    let coverage = coverage(source, query).await?;
    validate_coverage(query, &coverage)?;
    let sealed_end = query
        .to
        .timestamp_millis()
        .checked_sub(1)
        .ok_or(CloudReplayError::MalformedResponse)?;
    if coverage
        .sealed_through_ms
        .is_none_or(|sealed| sealed < sealed_end)
    {
        return Err(CloudReplayError::Unsealed);
    }

    let mut cursor = None;
    let mut seen_segments = HashSet::new();
    let mut saw_segment = false;
    loop {
        let page_url = range_url(source, "replay/segments", query, cursor.as_deref())?;
        let page = json::<SegmentPage>(source, page_url).await?;
        for segment in page.segments {
            saw_segment = true;
            if !seen_segments.insert((segment.release_id.clone(), segment.id.clone())) {
                return Err(CloudReplayError::MalformedResponse);
            }
            validate_identity(query, &segment)?;
            match segment.availability.state {
                RetrievalState::Hot | RetrievalState::ReadyUntil => {}
                state => return Err(CloudReplayError::RetrievalRequired { state }),
            }
            let encoded = cloud_cache::encoded_segment(source, &segment).await?;
            let logical = zstd::stream::decode_all(Cursor::new(encoded.as_ref()))
                .map_err(|_| CloudReplayError::IntegrityMismatch)?;
            verify_logical(&segment, &logical)?;
            for signal in cloud_decode::decode(&segment, &logical)? {
                sink.send(signal)
                    .await
                    .map_err(|_| CloudReplayError::Transport)?;
            }
        }
        match page.next_cursor {
            Some(next) if cursor.as_ref() != Some(&next) => cursor = Some(next),
            Some(_) => return Err(CloudReplayError::MalformedResponse),
            None => break,
        }
    }
    if saw_segment {
        Ok(())
    } else {
        Err(CloudReplayError::MalformedResponse)
    }
}

async fn finish(sink: &Sender<SourceSignal>, to_ms: i64) -> Result<(), CloudReplayError> {
    sink.send(SourceSignal::Watermark(to_ms))
        .await
        .map_err(|_| CloudReplayError::Transport)?;
    sink.send(SourceSignal::Eof)
        .await
        .map_err(|_| CloudReplayError::Transport)
}

async fn json<T: serde::de::DeserializeOwned>(
    source: &PmKitCloudDataSource,
    url: Url,
) -> Result<T, CloudReplayError> {
    request(source, url)
        .await?
        .json()
        .await
        .map_err(|_| CloudReplayError::MalformedResponse)
}

pub(super) async fn request(
    source: &PmKitCloudDataSource,
    url: Url,
) -> Result<reqwest::Response, CloudReplayError> {
    let response = source
        .client
        .get(url)
        .bearer_auth(source.api_key.expose())
        .send()
        .await
        .map_err(|_| CloudReplayError::Transport)?;
    match response.status() {
        StatusCode::OK => Ok(response),
        StatusCode::UNAUTHORIZED => Err(CloudReplayError::Unauthorized),
        StatusCode::FORBIDDEN => Err(CloudReplayError::Forbidden),
        StatusCode::CONFLICT => Err(CloudReplayError::RetrievalRequired {
            state: RetrievalState::RestoreRequired,
        }),
        StatusCode::TOO_MANY_REQUESTS => Err(CloudReplayError::QuotaExceeded),
        StatusCode::SERVICE_UNAVAILABLE => Err(CloudReplayError::ServiceUnavailable),
        _ => Err(CloudReplayError::MalformedResponse),
    }
}

fn range_url(
    source: &PmKitCloudDataSource,
    path: &str,
    query: &CloudReplayQuery,
    cursor: Option<&str>,
) -> Result<Url, CloudReplayError> {
    let mut url = Url::parse(&format!("{}/{}", source.base_url, path))
        .map_err(|_| CloudReplayError::InvalidConfiguration)?;
    {
        let mut pairs = url.query_pairs_mut();
        match &query.selector {
            CloudReplaySelector::Market(market) => {
                pairs.append_pair("market_id", &market.to_string());
            }
            CloudReplaySelector::Series(series) => {
                pairs.append_pair("series_id", series);
            }
            CloudReplaySelector::Asset { asset, duration } => {
                pairs.append_pair("asset", &asset.to_string().to_ascii_uppercase());
                pairs.append_pair("duration", &duration.seconds().to_string());
            }
        }
        pairs.append_pair("from", &query.from.to_rfc3339());
        pairs.append_pair("to", &query.to.to_rfc3339());
        pairs.append_pair("limit", "100");
        if let Some(cursor) = cursor {
            pairs.append_pair("cursor", cursor);
        }
    }
    Ok(url)
}

fn validate_coverage(
    query: &CloudReplayQuery,
    coverage: &CloudCoverage,
) -> Result<(), CloudReplayError> {
    if coverage.intervals.is_empty() {
        return Err(CloudReplayError::KnownGap);
    }
    let from_ms = query.from.timestamp_millis();
    let to_ms = query.to.timestamp_millis();
    let mut intervals = coverage.intervals.iter().collect::<Vec<_>>();
    intervals.sort_unstable_by_key(|interval| interval.from_ts_ms);
    let mut covered_until = from_ms;
    for interval in intervals {
        if interval.from_ts_ms > interval.to_ts_ms {
            return Err(CloudReplayError::MalformedResponse);
        }
        let interval_end = interval
            .to_ts_ms
            .checked_add(1)
            .ok_or(CloudReplayError::MalformedResponse)?;
        if interval_end <= from_ms || interval.from_ts_ms >= to_ms {
            continue;
        }
        if matches!(interval.status, CloudCoverageStatus::KnownGap) {
            return Err(CloudReplayError::KnownGap);
        }
        if interval.from_ts_ms > covered_until {
            return Err(CloudReplayError::KnownGap);
        }
        covered_until = covered_until.max(interval_end.min(to_ms));
        if covered_until == to_ms {
            return Ok(());
        }
    }
    Err(CloudReplayError::KnownGap)
}

fn validate_identity(query: &CloudReplayQuery, segment: &Segment) -> Result<(), CloudReplayError> {
    let query_from = query.from.timestamp_millis();
    let query_to = query
        .to
        .timestamp_millis()
        .checked_sub(1)
        .ok_or(CloudReplayError::MalformedResponse)?;
    let matches = match &query.selector {
        CloudReplaySelector::Market(market) => segment.market_id == market.to_string(),
        CloudReplaySelector::Series(series) => &segment.series_id == series,
        CloudReplaySelector::Asset { asset, duration } => {
            segment
                .asset
                .as_deref()
                .is_some_and(|value| value.eq_ignore_ascii_case(&asset.to_string()))
                && segment.duration_seconds == u64::try_from(duration.seconds()).ok()
        }
    };
    if matches
        && !segment.id.is_empty()
        && !segment.release_id.is_empty()
        && !segment.series_id.trim().is_empty()
        && !segment.outcome_tokens.is_empty()
        && segment.from_ts_ms <= segment.to_ts_ms
        && segment.from_ts_ms >= query_from
        && segment.to_ts_ms <= query_to
    {
        Ok(())
    } else {
        Err(CloudReplayError::MalformedResponse)
    }
}

fn verify_logical(segment: &Segment, bytes: &[u8]) -> Result<(), CloudReplayError> {
    if u64::try_from(bytes.len()).ok() == Some(segment.bytes)
        && cloud_cache::digest(bytes) == segment.sha256
    {
        Ok(())
    } else {
        Err(CloudReplayError::IntegrityMismatch)
    }
}
