use std::time::Duration;

use pmkit_exec::MarketLimits;
use pmkit_sim::{FeeModel, MarketCategory};

/// Configuration for the conservative-V1 fill model.
#[derive(Debug, Clone)]
pub struct ConservativeV1Config {
    /// Delay before a newly submitted order can act on fresh data.
    pub activation_latency: Duration,
    /// Share of crossed maker liquidity assumed ahead in queue, in basis points.
    pub maker_queue_ahead_bps: u16,
    /// Adverse taker slippage applied to simulated fills, in basis points.
    pub slippage_bps: u16,
    /// Adverse taker market impact applied to simulated fills, in basis points.
    pub market_impact_bps: u16,
    /// Optional maker/taker fee override; unset preserves legacy Crypto fees.
    pub fee_model: Option<FeeModel>,
    /// Venue market limits (minimum order size in shares, price tick);
    /// orders that violate them are rejected instead of filled. Unset skips
    /// the checks.
    pub market_limits: Option<MarketLimits>,
}

impl ConservativeV1Config {
    /// Returns the configured fee model or the legacy Crypto default.
    #[must_use]
    pub const fn resolved_fee_model(&self) -> FeeModel {
        match self.fee_model {
            Some(fee_model) => fee_model,
            None => FeeModel::for_category(MarketCategory::Crypto),
        }
    }
}
