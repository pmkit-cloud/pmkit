use std::time::Duration;

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
}
