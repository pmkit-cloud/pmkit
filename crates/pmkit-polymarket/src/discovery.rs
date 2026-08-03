use std::collections::BTreeSet;

use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::subscription::{
    DiscoverySnapshot, GammaMarket, PublicSubscription, ReplicaSubscriptionPlan, SubscriptionShard,
};

/// A fixed Gamma request page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GammaPageRequest {
    /// Maximum records requested from Gamma.
    pub limit: usize,
    /// Zero-based Gamma offset.
    pub offset: usize,
}

/// An all-or-nothing discovery failure.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DiscoveryError {
    /// Gamma was unavailable before any page could be accepted.
    #[error("Gamma discovery is unavailable")]
    Unavailable,
    /// A later page failed after discovery had begun.
    #[error("Gamma discovery became incomplete at offset {offset}")]
    IncompletePagination {
        /// First offset whose failure invalidated the complete snapshot.
        offset: usize,
    },
    /// A full page repeated an earlier full page.
    #[error("Gamma repeated a full page at offset {offset}")]
    RepeatedPage {
        /// Offset at which Gamma repeated an already-seen full page.
        offset: usize,
    },
    /// The next offset cannot fit in a platform usize.
    #[error("Gamma offset overflow at {offset}")]
    OffsetOverflow {
        /// Last successfully requested offset.
        offset: usize,
    },
    /// A market identity occurred more than once.
    #[error("Gamma returned duplicate market id {market_id}")]
    DuplicateMarketId {
        /// Repeated concrete market identifier.
        market_id: String,
    },
    /// An outcome or token identity occurred more than once in one market.
    #[error("Gamma returned duplicate outcome identity in market {market_id}")]
    DuplicateOutcomeId {
        /// Concrete market containing the duplicate mapping.
        market_id: String,
    },
    /// A market lacked durable structured family identity.
    #[error("Gamma market {market_id} lacks recurring-family metadata")]
    MissingFamilyMetadata {
        /// Concrete market missing durable family metadata.
        market_id: String,
    },
    /// Gamma supplied a structurally invalid page.
    #[error("Gamma page is malformed: {reason}")]
    MalformedPage {
        /// Stable reason class for malformed external input.
        reason: &'static str,
    },
}

/// Fetches, validates, and publishes one complete fixed-offset Gamma snapshot.
///
/// # Errors
///
/// Returns [`DiscoveryError`] when any page is unavailable, malformed, repeated, or incomplete.
pub fn discover_subscription_snapshot<F>(
    page_size: usize,
    shard_size: usize,
    mut fetch: F,
) -> Result<DiscoverySnapshot, DiscoveryError>
where
    F: FnMut(GammaPageRequest) -> Result<Vec<GammaMarket>, DiscoveryError>,
{
    if page_size == 0 || shard_size == 0 {
        return Err(DiscoveryError::MalformedPage {
            reason: "page and shard sizes must be positive",
        });
    }
    let mut offset = 0;
    let mut fingerprints = BTreeSet::new();
    let mut markets = Vec::new();
    loop {
        let page = fetch(GammaPageRequest {
            limit: page_size,
            offset,
        })
        .map_err(|error| match (offset, error) {
            (0, error) => error,
            (_, _) => DiscoveryError::IncompletePagination { offset },
        })?;
        validate_page(&page)?;
        if page.len() == page_size && !fingerprints.insert(page_digest(&page)?) {
            return Err(DiscoveryError::RepeatedPage { offset });
        }
        let count = page.len();
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

fn validate_page(page: &[GammaMarket]) -> Result<(), DiscoveryError> {
    for market in page {
        if market.market_id.trim().is_empty()
            || market.condition_id.trim().is_empty()
            || market.open_time_ms > market.close_time_ms
            || market.outcomes.is_empty()
        {
            return Err(DiscoveryError::MalformedPage {
                reason: "market identity or window is invalid",
            });
        }
    }
    Ok(())
}

pub fn normalize_snapshot(
    mut markets: Vec<GammaMarket>,
    shard_size: usize,
) -> Result<DiscoverySnapshot, DiscoveryError> {
    markets.retain(|market| market.active);
    markets.sort_by(|left, right| left.market_id.cmp(&right.market_id));
    let mut market_ids = BTreeSet::new();
    let mut asset_ids = Vec::new();
    for market in &markets {
        if !market_ids.insert(&market.market_id) {
            return Err(DiscoveryError::DuplicateMarketId {
                market_id: market.market_id.clone(),
            });
        }
        let Some(family) = &market.family else {
            return Err(DiscoveryError::MissingFamilyMetadata {
                market_id: market.market_id.clone(),
            });
        };
        if family.series_id().trim().is_empty() {
            return Err(DiscoveryError::MissingFamilyMetadata {
                market_id: market.market_id.clone(),
            });
        }
        let mut outcome_ids = BTreeSet::new();
        for outcome in &market.outcomes {
            if outcome.outcome_id().trim().is_empty()
                || outcome.token_id().trim().is_empty()
                || !outcome_ids.insert(outcome.outcome_id())
            {
                return Err(DiscoveryError::DuplicateOutcomeId {
                    market_id: market.market_id.clone(),
                });
            }
            asset_ids.push(outcome.token_id().to_owned());
        }
    }
    asset_ids.sort();
    if asset_ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(DiscoveryError::MalformedPage {
            reason: "token identity appears in multiple outcomes",
        });
    }
    let shards = asset_ids
        .chunks(shard_size)
        .enumerate()
        .map(|(index, assets)| SubscriptionShard {
            index,
            subscription: PublicSubscription::new(assets.to_vec()),
        })
        .collect::<Vec<_>>();
    let lane_a = ReplicaSubscriptionPlan { shards };
    let lane_b = lane_a.clone();
    Ok(DiscoverySnapshot {
        digest: page_digest(&markets)?,
        markets,
        lane_a,
        lane_b,
    })
}

fn page_digest<T: serde::Serialize>(value: &T) -> Result<String, DiscoveryError> {
    serde_json::to_vec(value)
        .map(|bytes| format!("{:x}", Sha256::digest(bytes)))
        .map_err(|_| DiscoveryError::MalformedPage {
            reason: "page cannot be canonicalized",
        })
}
