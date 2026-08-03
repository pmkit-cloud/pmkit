use pmkit_core::{EmptyIdError, MarketId};
use polymarket_client_sdk_v2::error::Error as SdkError;
use polymarket_client_sdk_v2::gamma::Client as SdkGammaClient;
use polymarket_client_sdk_v2::gamma::types::request::MarketsRequest;
use polymarket_client_sdk_v2::gamma::types::response::Market;
use polymarket_client_sdk_v2::types::U256;
use rust_decimal::Decimal;
use thiserror::Error;

/// Market metadata returned by Gamma.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GammaMarket {
    /// Parent event identifier.
    pub event_id: String,
    /// Market end date as provided by Gamma.
    pub end_date_iso: Option<String>,
    /// Whether the market uses negative risk.
    pub negative_risk: bool,
    /// Human-readable outcomes in token order.
    pub outcomes: Vec<String>,
    /// CLOB token identifiers in outcome order.
    pub clob_token_ids: Vec<String>,
    /// Whether Gamma marks the market closed.
    pub closed: bool,
    /// Unix timestamp when the market closed, if known.
    pub closed_time: Option<i64>,
    /// Resolution prices in token order.
    pub outcome_prices: Vec<Decimal>,
    /// Market question.
    pub title: String,
    /// Market slug.
    pub slug: String,
    /// Event slug.
    pub event_slug: String,
}

impl GammaMarket {
    /// Returns the token paired with `token_id` for a binary market.
    #[must_use]
    pub fn opposite_token(&self, token_id: &str) -> Option<&str> {
        (self.clob_token_ids.len() == 2)
            .then(|| {
                self.clob_token_ids
                    .iter()
                    .find(|candidate| candidate.as_str() != token_id)
            })
            .flatten()
            .map(String::as_str)
    }

    /// Returns the outcome paired with `outcome` for a binary market.
    #[must_use]
    pub fn opposite_outcome(&self, outcome: &str) -> Option<&str> {
        (self.outcomes.len() == 2)
            .then(|| {
                self.outcomes
                    .iter()
                    .find(|candidate| candidate.as_str() != outcome)
            })
            .flatten()
            .map(String::as_str)
    }

    /// Resolves a token identifier to its human-readable outcome.
    #[must_use]
    pub fn outcome_for_token(&self, token_id: &str) -> Option<&str> {
        self.clob_token_ids
            .iter()
            .position(|candidate| candidate == token_id)
            .and_then(|index| self.outcomes.get(index))
            .map(String::as_str)
    }

    /// Returns the closed-market payout for a token, when resolved.
    #[must_use]
    pub fn resolution_price(&self, token_id: &str) -> Option<Decimal> {
        if !self.closed {
            return None;
        }
        self.clob_token_ids
            .iter()
            .position(|candidate| candidate == token_id)
            .and_then(|index| self.outcome_prices.get(index))
            .copied()
    }
}

/// Failure returned while loading or converting a Gamma market.
#[derive(Debug, Error)]
pub enum GammaError {
    /// The Gamma API request or response decoding failed.
    #[error("gamma market request failed: {source}")]
    Request {
        #[from]
        #[source]
        source: SdkError,
    },

    /// Gamma omitted a field required by [`GammaMarket`].
    #[error("gamma market payload omitted required field `{field}`")]
    MissingField {
        /// Missing Gamma response field.
        field: &'static str,
    },

    /// Gamma supplied a closed timestamp that was not RFC 3339.
    #[error("gamma market closed timestamp `{value}` was invalid: {source}")]
    InvalidClosedTime {
        /// Invalid timestamp text.
        value: String,
        /// Timestamp parsing failure.
        #[source]
        source: chrono::ParseError,
    },
}

/// Errors returned while requesting or validating an active Gamma market page.
#[derive(Debug, Error)]
pub enum DiscoveryError {
    /// The Gamma API returned a transport or request failure.
    #[error("gamma market listing failed: {source}")]
    Listing {
        /// SDK request or response failure.
        #[source]
        source: SdkError,
    },

    /// A discovered market contained an empty or whitespace-only identifier.
    #[error("gamma market identifier was invalid: {source}")]
    InvalidMarketId {
        /// Rejection produced by the `PMKit` market identifier constructor.
        #[source]
        source: EmptyIdError,
    },

    /// A requested Gamma page cannot be represented by the upstream API.
    #[error("gamma page request is invalid: {reason}")]
    InvalidPageRequest {
        /// Stable reason class for the invalid pagination input.
        reason: &'static str,
    },
}

/// A validated fixed-offset Gamma market request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GammaPageRequest {
    limit: i32,
    offset: i32,
}

impl GammaPageRequest {
    /// Creates a nonempty Gamma page request with a nonnegative offset.
    ///
    /// # Errors
    ///
    /// Returns [`DiscoveryError::InvalidPageRequest`] when either coordinate exceeds Gamma's
    /// signed range or the requested page is empty.
    pub fn new(limit: u32, offset: u32) -> Result<Self, DiscoveryError> {
        if limit == 0 {
            return Err(DiscoveryError::InvalidPageRequest {
                reason: "limit must be positive",
            });
        }
        Ok(Self {
            limit: i32::try_from(limit).map_err(|_| DiscoveryError::InvalidPageRequest {
                reason: "limit exceeds Gamma range",
            })?,
            offset: i32::try_from(offset).map_err(|_| DiscoveryError::InvalidPageRequest {
                reason: "offset exceeds Gamma range",
            })?,
        })
    }

    /// Returns the fixed maximum row count requested from Gamma.
    #[must_use]
    pub const fn limit(&self) -> i32 {
        self.limit
    }

    /// Returns the zero-based Gamma row offset.
    #[must_use]
    pub const fn offset(&self) -> i32 {
        self.offset
    }
}

/// A Gamma client using the official Polymarket SDK.
#[derive(Debug, Clone)]
pub struct GammaClient {
    client: SdkGammaClient,
}

impl Default for GammaClient {
    fn default() -> Self {
        Self::new()
    }
}

impl GammaClient {
    /// Creates a client using the SDK's default Gamma endpoint.
    #[must_use]
    pub fn new() -> Self {
        Self {
            client: SdkGammaClient::default(),
        }
    }

    /// Lists one explicitly requested page of active Gamma markets.
    ///
    /// # Errors
    ///
    /// Returns a transport error if the SDK request fails, a malformed payload error if Gamma
    /// returns invalid market data, or an invalid market id error if a market id is empty.
    pub async fn list_active_market_page(
        &self,
        page: GammaPageRequest,
    ) -> Result<Vec<MarketId>, DiscoveryError> {
        let request = MarketsRequest::builder()
            .closed(false)
            .limit(page.limit())
            .offset(page.offset())
            .build();
        let markets = self
            .client
            .markets(&request)
            .await
            .map_err(|source| DiscoveryError::Listing { source })?;

        markets
            .into_iter()
            .map(|market| {
                MarketId::new(market.id)
                    .map_err(|source| DiscoveryError::InvalidMarketId { source })
            })
            .collect()
    }

    /// Finds metadata by CLOB token id.
    ///
    /// # Errors
    ///
    /// Returns a typed error when the request, response decoding, or required-field conversion
    /// fails.
    pub async fn market_by_token(&self, token_id: &str) -> Result<Option<GammaMarket>, GammaError> {
        let Ok(token) = token_id.parse::<U256>() else {
            return Ok(None);
        };
        let request = MarketsRequest::builder()
            .clob_token_ids(vec![token])
            .build();
        let markets = self.client.markets(&request).await?;
        markets
            .into_iter()
            .next()
            .map(GammaMarket::try_from)
            .transpose()
    }
}

impl TryFrom<Market> for GammaMarket {
    type Error = GammaError;

    fn try_from(market: Market) -> Result<Self, Self::Error> {
        let event = market
            .events
            .as_ref()
            .and_then(|events| events.first())
            .ok_or(GammaError::MissingField { field: "events[0]" })?;
        let event_id = event.id.clone();
        let event_slug = event.slug.clone().ok_or(GammaError::MissingField {
            field: "event.slug",
        })?;
        let closed_time = market
            .closed_time
            .map(|value| {
                parse_timestamp(&value)
                    .map_err(|source| GammaError::InvalidClosedTime { value, source })
            })
            .transpose()?;

        Ok(Self {
            event_id,
            end_date_iso: market.end_date_iso.map(|date| date.to_string()),
            negative_risk: market
                .neg_risk
                .ok_or(GammaError::MissingField { field: "negRisk" })?,
            outcomes: market
                .outcomes
                .ok_or(GammaError::MissingField { field: "outcomes" })?,
            clob_token_ids: market
                .clob_token_ids
                .ok_or(GammaError::MissingField {
                    field: "clobTokenIds",
                })?
                .into_iter()
                .map(|token| token.to_string())
                .collect(),
            closed: market
                .closed
                .ok_or(GammaError::MissingField { field: "closed" })?,
            closed_time,
            outcome_prices: market.outcome_prices.ok_or(GammaError::MissingField {
                field: "outcomePrices",
            })?,
            title: market
                .question
                .ok_or(GammaError::MissingField { field: "question" })?,
            slug: market
                .slug
                .ok_or(GammaError::MissingField { field: "slug" })?,
            event_slug,
        })
    }
}

fn parse_timestamp(value: &str) -> Result<i64, chrono::ParseError> {
    chrono::DateTime::parse_from_rfc3339(value).map(|date| date.timestamp())
}

#[cfg(test)]
mod tests;
