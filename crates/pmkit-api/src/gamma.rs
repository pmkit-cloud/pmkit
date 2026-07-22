use polymarket_client_sdk_v2::gamma::Client as SdkGammaClient;
use polymarket_client_sdk_v2::gamma::types::request::MarketsRequest;
use polymarket_client_sdk_v2::gamma::types::response::Market;
use polymarket_client_sdk_v2::types::U256;

/// Market metadata returned by Gamma.
#[derive(Debug, Clone, PartialEq)]
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
    pub outcome_prices: Vec<f64>,
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
    pub fn resolution_price(&self, token_id: &str) -> Option<f64> {
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

    /// Finds metadata by CLOB token id.
    ///
    /// # Errors
    ///
    /// Returns the SDK's Gamma transport or decoding error.
    pub async fn market_by_token(
        &self,
        token_id: &str,
    ) -> Result<Option<GammaMarket>, polymarket_client_sdk_v2::error::Error> {
        let Ok(token) = token_id.parse::<U256>() else {
            return Ok(None);
        };
        let request = MarketsRequest::builder()
            .clob_token_ids(vec![token])
            .build();
        let markets = self.client.markets(&request).await?;
        Ok(markets.into_iter().next().map(GammaMarket::from))
    }
}

impl From<Market> for GammaMarket {
    fn from(market: Market) -> Self {
        let event = market.events.as_ref().and_then(|events| events.first());
        Self {
            event_id: event.map_or_else(String::new, |event| event.id.clone()),
            end_date_iso: market.end_date_iso.map(|date| date.to_string()),
            negative_risk: market.neg_risk.unwrap_or(false),
            outcomes: market.outcomes.unwrap_or_default(),
            clob_token_ids: market
                .clob_token_ids
                .unwrap_or_default()
                .into_iter()
                .map(|token| token.to_string())
                .collect(),
            closed: market.closed.unwrap_or(false),
            closed_time: market.closed_time.as_deref().and_then(parse_timestamp),
            outcome_prices: market
                .outcome_prices
                .unwrap_or_default()
                .into_iter()
                .filter_map(|price| price.to_string().parse().ok())
                .collect(),
            title: market.question.unwrap_or_default(),
            slug: market.slug.unwrap_or_default(),
            event_slug: event
                .and_then(|event| event.slug.clone())
                .unwrap_or_default(),
        }
    }
}

fn parse_timestamp(value: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|date| date.timestamp())
}

#[cfg(test)]
mod tests {
    use super::GammaMarket;

    #[test]
    fn binary_market_helpers_use_token_order() {
        let market = GammaMarket {
            event_id: "event".into(),
            end_date_iso: None,
            negative_risk: false,
            outcomes: vec!["Yes".into(), "No".into()],
            clob_token_ids: vec!["a".into(), "b".into()],
            closed: true,
            closed_time: Some(1),
            outcome_prices: vec![1.0, 0.0],
            title: "Question".into(),
            slug: "market".into(),
            event_slug: "event".into(),
        };
        assert_eq!(market.opposite_token("a"), Some("b"));
        assert_eq!(market.outcome_for_token("b"), Some("No"));
        assert_eq!(market.resolution_price("a"), Some(1.0));
    }
}
