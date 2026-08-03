use serde::Serialize;

/// Structured recurring identity supplied by Gamma family metadata or caller configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecurringFamily {
    series_id: String,
    asset: Option<String>,
    duration: Option<String>,
}

impl RecurringFamily {
    /// Creates structured recurring-family metadata.
    #[must_use]
    pub fn new(series_id: impl Into<String>, asset: Option<&str>, duration: Option<&str>) -> Self {
        Self {
            series_id: series_id.into(),
            asset: asset.map(str::to_owned),
            duration: duration.map(str::to_owned),
        }
    }

    /// Returns the durable family identifier.
    #[must_use]
    pub fn series_id(&self) -> &str {
        &self.series_id
    }

    /// Returns the structured recurring-family asset when Gamma provides one.
    #[must_use]
    pub fn asset(&self) -> Option<&str> {
        self.asset.as_deref()
    }

    /// Returns the structured recurring-family duration when Gamma provides one.
    #[must_use]
    pub fn duration(&self) -> Option<&str> {
        self.duration.as_deref()
    }
}

/// One ordered outcome/token pair returned by Gamma.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GammaOutcome {
    outcome_id: String,
    token_id: String,
}

impl GammaOutcome {
    /// Creates one ordered outcome/token mapping.
    #[must_use]
    pub fn new(outcome_id: impl Into<String>, token_id: impl Into<String>) -> Self {
        Self {
            outcome_id: outcome_id.into(),
            token_id: token_id.into(),
        }
    }

    /// Returns the CLOB token identifier.
    #[must_use]
    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    /// Returns the provider outcome identifier in the preserved outcome order.
    #[must_use]
    pub fn outcome_id(&self) -> &str {
        &self.outcome_id
    }
}

/// A Gamma market normalized before it can enter a discovery snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GammaMarket {
    /// Concrete Gamma market identifier.
    pub market_id: String,
    /// Concrete CLOB condition identifier.
    pub condition_id: String,
    /// Market opening instant in milliseconds.
    pub open_time_ms: i64,
    /// Market closing instant in milliseconds.
    pub close_time_ms: i64,
    /// Whether Gamma reports the market as active.
    pub active: bool,
    /// Durable structured recurring identity.
    pub family: Option<RecurringFamily>,
    /// Ordered outcome/token mappings.
    pub outcomes: Vec<GammaOutcome>,
}

/// A serialized market-channel subscription request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PublicSubscription {
    #[serde(rename = "assets_ids")]
    asset_ids: Vec<String>,
    custom_feature_enabled: bool,
}

impl PublicSubscription {
    pub(crate) const fn new(asset_ids: Vec<String>) -> Self {
        Self {
            asset_ids,
            custom_feature_enabled: true,
        }
    }

    /// Returns whether custom public-market features are enabled.
    #[must_use]
    pub const fn custom_feature_enabled(&self) -> bool {
        self.custom_feature_enabled
    }

    pub(crate) fn asset_ids(&self) -> &[String] {
        &self.asset_ids
    }
}

/// One deterministic physical-socket-independent subscription shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SubscriptionShard {
    pub(crate) index: usize,
    pub(crate) subscription: PublicSubscription,
}

impl SubscriptionShard {
    /// Returns the encoded subscription payload.
    #[must_use]
    pub const fn subscription(&self) -> &PublicSubscription {
        &self.subscription
    }
}

/// The logical shard plan for one capture replica.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReplicaSubscriptionPlan {
    pub(crate) shards: Vec<SubscriptionShard>,
}

impl ReplicaSubscriptionPlan {
    /// Returns deterministic subscription shards without a physical connection identity.
    #[must_use]
    pub fn shards(&self) -> &[SubscriptionShard] {
        &self.shards
    }
}

/// A complete normalized discovery snapshot and two identical logical lane plans.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DiscoverySnapshot {
    pub(crate) markets: Vec<GammaMarket>,
    pub(crate) digest: String,
    pub(crate) lane_a: ReplicaSubscriptionPlan,
    pub(crate) lane_b: ReplicaSubscriptionPlan,
}

impl DiscoverySnapshot {
    /// Returns normalized active markets in stable market-identity order.
    #[must_use]
    pub fn markets(&self) -> &[GammaMarket] {
        &self.markets
    }

    /// Returns the deterministic SHA-256 snapshot digest.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Returns the first replica's logical shard plan.
    #[must_use]
    pub const fn lane_a(&self) -> &ReplicaSubscriptionPlan {
        &self.lane_a
    }

    /// Returns the second replica's logical shard plan.
    #[must_use]
    pub const fn lane_b(&self) -> &ReplicaSubscriptionPlan {
        &self.lane_b
    }
}

impl GammaMarket {
    /// Returns structured recurring-family metadata after discovery validation.
    #[must_use]
    pub const fn family(&self) -> Option<&RecurringFamily> {
        self.family.as_ref()
    }

    /// Returns ordered outcome/token mappings after discovery validation.
    #[must_use]
    pub fn outcomes(&self) -> &[GammaOutcome] {
        &self.outcomes
    }
}
