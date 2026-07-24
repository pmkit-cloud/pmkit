use async_trait::async_trait;

use crate::{CanonicalLogSegment, ChainCheckpoint, StoreError};

pub use crate::raw::{
    BlockHead, ChainSourceError, FinalizedBlockCoverage, FinalizedBlockRange,
    FinalizedProviderHead, FinalizedRawLogBatch, FinalizedRawLogProvider, ProviderIdentity,
    RawLogIdentity, RawRpcLog, agree_on_finalized_heads, verify_block_header_linkage,
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

pub fn verify_finalized_progression(
    persisted: Option<&ChainCheckpoint>,
    common_ancestor: &ChainCheckpoint,
    batch: &FinalizedRawLogBatch,
) -> Result<Option<ChainCheckpoint>, StoreError> {
    batch.verify()?;
    if common_ancestor.chain_id != batch.range.chain_id {
        return Err(StoreError::InvalidCanonicalSegment);
    }

    let proposed = ChainCheckpoint::new(
        batch.finalized.chain_id,
        batch.finalized.block_number,
        batch.finalized.block_hash.clone(),
    );
    if let Some(persisted) = persisted {
        if proposed.block_number < persisted.block_number {
            return Err(StoreError::FinalizedHeadRegression {
                chain_id: proposed.chain_id.get(),
                persisted_block_number: persisted.block_number,
                proposed_block_number: proposed.block_number,
            });
        }
        if proposed.block_number == persisted.block_number
            && proposed.block_hash != persisted.block_hash
        {
            return Err(StoreError::FinalizedHeadNotLinked {
                chain_id: proposed.chain_id.get(),
                block_number: proposed.block_number,
            });
        }
    }

    let Some(finalized_header) = batch.coverage.blocks.last() else {
        return Ok(None);
    };
    if finalized_header.block_number < proposed.block_number {
        return Ok(None);
    }
    if finalized_header != &batch.finalized {
        return Err(StoreError::FinalizedHeadNotLinked {
            chain_id: proposed.chain_id.get(),
            block_number: proposed.block_number,
        });
    }

    let anchor = persisted.unwrap_or(common_ancestor);
    if proposed.block_number < anchor.block_number {
        return Err(StoreError::InvalidCanonicalSegment);
    }
    if proposed.block_number == anchor.block_number {
        return (proposed.block_hash == anchor.block_hash)
            .then_some(Some(proposed))
            .ok_or_else(|| StoreError::FinalizedHeadNotLinked {
                chain_id: anchor.chain_id.get(),
                block_number: anchor.block_number,
            });
    }

    let starts_after_anchor = anchor
        .block_number
        .checked_add(1)
        .is_some_and(|next| batch.range.from_block == next);
    if starts_after_anchor {
        let Some(first_header) = batch.coverage.blocks.first() else {
            return Ok(None);
        };
        let boundary = [
            BlockHead::new(
                anchor.chain_id,
                anchor.block_number,
                anchor.block_hash.clone(),
                String::new(),
            ),
            first_header.clone(),
        ];
        verify_block_header_linkage(&boundary)?;
        return Ok(Some(proposed));
    }

    let Some(offset) = anchor
        .block_number
        .checked_sub(batch.range.from_block)
        .and_then(|value| usize::try_from(value).ok())
    else {
        return Ok(None);
    };
    let Some(covered_anchor) = batch.coverage.blocks.get(offset) else {
        return Ok(None);
    };
    if covered_anchor.block_hash != anchor.block_hash {
        return Err(StoreError::FinalizedHeadNotLinked {
            chain_id: anchor.chain_id.get(),
            block_number: anchor.block_number,
        });
    }
    Ok(Some(proposed))
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
