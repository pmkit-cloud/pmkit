use std::time::Duration;

/// Configuration for the conservative-V1 fill model.
#[derive(Debug, Clone)]
pub struct ConservativeV1Config {
    /// Delay before a newly submitted order can act on fresh data.
    pub activation_latency: Duration,
}
