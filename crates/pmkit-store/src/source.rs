use async_trait::async_trait;

use crate::{CanonicalLogSegment, ChainCheckpoint};

pub use crate::raw::{
    BlockHead, ChainSourceError, FinalizedBlockCoverage, FinalizedBlockRange,
    FinalizedProviderHead, FinalizedRawLogBatch, FinalizedRawLogProvider, ProviderIdentity,
    RawLogIdentity, RawRpcLog, agree_on_finalized_heads,
};

/// A trait-first source of parsed canonical logs; no RPC client is implied.
#[async_trait]
pub trait CanonicalLogSource: Send + Sync {
    /// Returns replacement logs after the supplied source checkpoint.
    async fn canonical_segment(
        &self,
        after: Option<&ChainCheckpoint>,
    ) -> Result<CanonicalLogSegment, ChainSourceError>;
}

/// A deterministic parsed-log fixture source for tests and offline backfills.
#[derive(Debug, Clone)]
pub struct FixtureCanonicalLogSource {
    segment: CanonicalLogSegment,
}

impl FixtureCanonicalLogSource {
    /// Creates a source from one typed canonical segment.
    #[must_use]
    pub const fn new(segment: CanonicalLogSegment) -> Self {
        Self { segment }
    }
}

#[async_trait]
impl CanonicalLogSource for FixtureCanonicalLogSource {
    async fn canonical_segment(
        &self,
        _after: Option<&ChainCheckpoint>,
    ) -> Result<CanonicalLogSegment, ChainSourceError> {
        Ok(self.segment.clone())
    }
}
