use std::collections::{BTreeMap, BTreeSet};

use polymarket_client_sdk_v2::{
    gamma::{
        Client as SdkGammaClient,
        types::{request::MarketsRequest, response::Market},
    },
    types::U256,
};
use sha2::Digest;

use crate::discovery::normalize_snapshot;
use crate::{DiscoveryError, DiscoverySnapshot, GammaMarket, GammaOutcome, RecurringFamily};

/// SDK-backed Gamma discovery at the Polymarket adapter boundary.
#[derive(Debug, Clone)]
pub struct GammaDiscovery {
    client: SdkGammaClient,
}

impl Default for GammaDiscovery {
    fn default() -> Self {
        Self::new()
    }
}

impl GammaDiscovery {
    /// Creates a discovery client for Gamma's default endpoint.
    #[must_use]
    pub fn new() -> Self {
        Self {
            client: SdkGammaClient::default(),
        }
    }

    /// Wraps an SDK Gamma client, primarily for controlled transports and tests.
    #[must_use]
    pub const fn with_client(client: SdkGammaClient) -> Self {
        Self { client }
    }

    /// Fetches a complete Gamma snapshot using fixed `limit` and `offset` pages.
    ///
    /// Family configuration may enrich a structured Gamma series with typed asset and duration;
    /// it never derives a series identity from a title or slug.
    ///
    /// # Errors
    ///
    /// Returns [`DiscoveryError`] when Gamma is unavailable or supplies an incomplete page.
    pub async fn snapshot(
        &self,
        page_size: usize,
        shard_size: usize,
        families: &BTreeMap<String, RecurringFamily>,
    ) -> Result<DiscoverySnapshot, DiscoveryError> {
        if page_size == 0 || shard_size == 0 {
            return Err(DiscoveryError::MalformedPage {
                reason: "page and shard sizes must be positive",
            });
        }
        let limit = i32::try_from(page_size).map_err(|_| DiscoveryError::MalformedPage {
            reason: "page size exceeds Gamma range",
        })?;
        let mut offset = 0_usize;
        let mut fingerprints = BTreeSet::new();
        let mut markets = Vec::new();
        loop {
            let request = MarketsRequest::builder()
                .closed(false)
                .limit(limit)
                .offset(
                    i32::try_from(offset).map_err(|_| DiscoveryError::OffsetOverflow { offset })?,
                )
                .build();
            let page = self.client.markets(&request).await.map_err(|_| {
                if offset == 0 {
                    DiscoveryError::Unavailable
                } else {
                    DiscoveryError::IncompletePagination { offset }
                }
            })?;
            let page = page
                .into_iter()
                .map(|market| gamma_market(market, families))
                .collect::<Result<Vec<_>, _>>()?;
            let count = page.len();
            let fingerprint = serde_json::to_vec(&page)
                .map(|bytes| format!("{:x}", sha2::Sha256::digest(bytes)))
                .map_err(|_| DiscoveryError::MalformedPage {
                    reason: "page cannot be canonicalized",
                })?;
            if count == page_size && !fingerprints.insert(fingerprint) {
                return Err(DiscoveryError::RepeatedPage { offset });
            }
            markets.extend(page);
            if count < page_size {
                break;
            }
            offset = offset
                .checked_add(count)
                .ok_or(DiscoveryError::OffsetOverflow { offset })?;
        }
        normalize_snapshot(markets, shard_size)
    }
}

fn gamma_market(
    market: Market,
    families: &BTreeMap<String, RecurringFamily>,
) -> Result<GammaMarket, DiscoveryError> {
    let condition_id = market
        .condition_id
        .ok_or(DiscoveryError::MalformedPage {
            reason: "market lacks condition id",
        })?
        .to_string();
    let open_time_ms = market
        .start_date
        .ok_or(DiscoveryError::MalformedPage {
            reason: "market lacks start time",
        })?
        .timestamp_millis();
    let close_time_ms = market
        .end_date
        .ok_or(DiscoveryError::MalformedPage {
            reason: "market lacks end time",
        })?
        .timestamp_millis();
    let active = market.active.ok_or(DiscoveryError::MalformedPage {
        reason: "market lacks activity status",
    })?;
    let closed = market.closed.ok_or(DiscoveryError::MalformedPage {
        reason: "market lacks closed status",
    })?;
    let family = market
        .events
        .as_ref()
        .and_then(|events| events.first())
        .and_then(|event| event.series.as_ref())
        .and_then(|series| series.first())
        .map(|series| {
            families
                .get(&series.id)
                .cloned()
                .unwrap_or_else(|| RecurringFamily::new(series.id.clone(), None, None))
        });
    let outcomes = outcomes(market.outcomes, market.clob_token_ids)?;
    Ok(GammaMarket {
        market_id: market.id,
        condition_id,
        open_time_ms,
        close_time_ms,
        active: active && !closed,
        family,
        outcomes,
    })
}

fn outcomes(
    outcomes: Option<Vec<String>>,
    tokens: Option<Vec<U256>>,
) -> Result<Vec<GammaOutcome>, DiscoveryError> {
    let outcomes = outcomes.ok_or(DiscoveryError::MalformedPage {
        reason: "market lacks outcomes",
    })?;
    let tokens = tokens.ok_or(DiscoveryError::MalformedPage {
        reason: "market lacks outcome tokens",
    })?;
    if outcomes.len() != tokens.len() {
        return Err(DiscoveryError::MalformedPage {
            reason: "outcomes and tokens differ in length",
        });
    }
    Ok(outcomes
        .into_iter()
        .zip(tokens)
        .map(|(outcome, token)| GammaOutcome::new(outcome, token.to_string()))
        .collect())
}
