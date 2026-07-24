use std::collections::HashSet;

use async_trait::async_trait;
use thiserror::Error;

use crate::{Address, ChainId};

/// The identity of an upstream chain-data provider.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ProviderIdentity(String);

impl ProviderIdentity {
    /// Creates a provider identity from its stable operator-facing name.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the stable provider name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A block reference returned by a chain provider.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BlockHead {
    /// The chain containing the block.
    pub chain_id: ChainId,
    /// The block height.
    pub block_number: u64,
    /// The provider-reported block hash.
    pub block_hash: String,
}

impl BlockHead {
    /// Creates a provider-reported block reference.
    #[must_use]
    pub fn new(chain_id: ChainId, block_number: u64, block_hash: impl Into<String>) -> Self {
        Self {
            chain_id,
            block_number,
            block_hash: block_hash.into(),
        }
    }
}

/// One provider's current and finalized chain observations.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FinalizedProviderHead {
    /// The provider that made the observation.
    pub provider: ProviderIdentity,
    /// The provider's current head.
    pub head: BlockHead,
    /// The provider's finalized block.
    pub finalized: BlockHead,
}

/// An inclusive raw-log query range known to be requested from a finalized provider.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FinalizedBlockRange {
    /// The chain containing the requested blocks.
    pub chain_id: ChainId,
    /// The inclusive first block.
    pub from_block: u64,
    /// The inclusive last block.
    pub to_block: u64,
}

impl FinalizedBlockRange {
    /// Creates an inclusive block range.
    ///
    /// # Errors
    ///
    /// Returns [`ChainSourceError::InvalidRange`] when the end precedes the start.
    pub const fn new(
        chain_id: ChainId,
        from_block: u64,
        to_block: u64,
    ) -> Result<Self, ChainSourceError> {
        if from_block > to_block {
            return Err(ChainSourceError::InvalidRange {
                from_block,
                to_block,
            });
        }
        Ok(Self {
            chain_id,
            from_block,
            to_block,
        })
    }
}

/// A complete finalized block-header coverage proof for an inclusive range.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FinalizedBlockCoverage {
    /// The range covered by the headers.
    pub range: FinalizedBlockRange,
    /// One header for every block in the range, in ascending order.
    pub blocks: Vec<BlockHead>,
}

impl FinalizedBlockCoverage {
    /// Validates complete, ordered block-header coverage.
    ///
    /// # Errors
    ///
    /// Returns [`ChainSourceError::MissingBlockCoverage`] when a block is
    /// missing, duplicated, out of order, or belongs to another chain.
    pub fn new(
        range: FinalizedBlockRange,
        blocks: Vec<BlockHead>,
    ) -> Result<Self, ChainSourceError> {
        let expected = range
            .to_block
            .checked_sub(range.from_block)
            .and_then(|width| width.checked_add(1))
            .ok_or_else(|| ChainSourceError::MissingBlockCoverage {
                message: "block range size overflowed".into(),
            })?;
        if usize::try_from(expected).ok() != Some(blocks.len())
            || !blocks.iter().enumerate().all(|(index, block)| {
                block.chain_id == range.chain_id
                    && block.block_number == range.from_block + index as u64
            })
        {
            return Err(ChainSourceError::MissingBlockCoverage {
                message: "coverage does not contain exactly one header per block".into(),
            });
        }
        Ok(Self { range, blocks })
    }
}

/// Confirms that two provider observations agree on head and finality.
///
/// # Errors
///
/// Returns [`ChainSourceError::ProviderDisagreement`] when either block
/// reference differs.
pub fn agree_on_finalized_heads(
    left: FinalizedProviderHead,
    right: &FinalizedProviderHead,
) -> Result<FinalizedProviderHead, ChainSourceError> {
    if left.head != right.head || left.finalized != right.finalized {
        return Err(ChainSourceError::ProviderDisagreement {
            left: left.provider,
            right: right.provider.clone(),
        });
    }
    Ok(left)
}

/// The provider-preserved identity of one raw EVM log.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct RawLogIdentity {
    /// The provider that supplied this observation.
    pub provider: ProviderIdentity,
    /// The chain containing the log.
    pub chain_id: ChainId,
    /// The canonical block height.
    pub block_number: u64,
    /// The canonical block hash.
    pub block_hash: String,
    /// The transaction hash.
    pub transaction_hash: String,
    /// The transaction's position in its block.
    pub transaction_index: u64,
    /// The log's position in its transaction.
    pub log_index: u64,
}

/// A raw RPC log before ABI decoding into [`crate::ChainEvent`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RawRpcLog {
    /// The provider and chain ordering identity.
    pub identity: RawLogIdentity,
    /// The contract that emitted the log.
    pub contract_address: Address,
    /// The un-decoded topic values as returned by the provider.
    pub topics: Vec<String>,
    /// The un-decoded data value as returned by the provider.
    pub data: String,
}

/// A finalized provider response carrying raw logs and both chain heights.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FinalizedRawLogBatch {
    /// The provider that produced the batch.
    pub provider: ProviderIdentity,
    /// The requested inclusive range.
    pub range: FinalizedBlockRange,
    /// The provider's current head.
    pub head: BlockHead,
    /// The provider's finalized block, which must cover `range.to_block`.
    pub finalized: BlockHead,
    /// The raw logs in the requested range.
    pub logs: Vec<RawRpcLog>,
}

impl FinalizedRawLogBatch {
    /// Validates a provider response before it reaches decoding or storage.
    ///
    /// # Errors
    ///
    /// Returns a typed source error when the provider reports inconsistent chain,
    /// finality, range, or raw-log identity data.
    pub fn new(
        provider: ProviderIdentity,
        range: FinalizedBlockRange,
        head: BlockHead,
        finalized: BlockHead,
        logs: Vec<RawRpcLog>,
    ) -> Result<Self, ChainSourceError> {
        if head.chain_id != range.chain_id || finalized.chain_id != range.chain_id {
            return Err(ChainSourceError::InvalidRawLog {
                message: "provider block heads disagree with requested chain".into(),
            });
        }
        if finalized.block_number > head.block_number {
            return Err(ChainSourceError::InvalidRawLog {
                message: "finalized block is ahead of provider head".into(),
            });
        }
        if range.to_block > finalized.block_number {
            return Err(ChainSourceError::FinalityViolation {
                requested_to_block: range.to_block,
                finalized_block: finalized.block_number,
            });
        }
        if logs.iter().any(|log| {
            log.identity.provider != provider
                || log.identity.chain_id != range.chain_id
                || log.identity.block_number < range.from_block
                || log.identity.block_number > range.to_block
        }) {
            return Err(ChainSourceError::InvalidRawLog {
                message: "raw log identity is outside the finalized response".into(),
            });
        }
        let mut identities = HashSet::with_capacity(logs.len());
        if logs.iter().any(|log| !identities.insert(&log.identity)) {
            return Err(ChainSourceError::DuplicateRawLog);
        }
        Ok(Self {
            provider,
            range,
            head,
            finalized,
            logs,
        })
    }
}

/// Error returned by a canonical-log source before durable ingestion.
#[derive(Debug, Error)]
pub enum ChainSourceError {
    /// The source cannot produce a canonical segment.
    #[error("canonical log source failed: {message}")]
    Unavailable {
        /// Source-specific detail.
        message: String,
    },
    /// A provider transport or JSON-RPC response failed.
    #[error("provider {provider:?} failed: {message}")]
    ProviderFailure {
        /// The provider identity.
        provider: ProviderIdentity,
        /// The transport or protocol detail.
        message: String,
    },
    /// The requested block range is reversed.
    #[error("invalid block range: {from_block}..={to_block}")]
    InvalidRange {
        /// The inclusive first block.
        from_block: u64,
        /// The inclusive last block.
        to_block: u64,
    },
    /// The requested range exceeds the provider's configured bound.
    #[error("requested block range has {requested_blocks} blocks; maximum is {maximum_blocks}")]
    RangeTooLarge {
        /// The requested block count.
        requested_blocks: u64,
        /// The configured maximum block count.
        maximum_blocks: u64,
    },
    /// The provider returned data beyond its finalized height.
    #[error(
        "provider returned an unfinalized range ending at {requested_to_block}; finalized block is {finalized_block}"
    )]
    FinalityViolation {
        /// The requested inclusive range end.
        requested_to_block: u64,
        /// The provider-reported finalized height.
        finalized_block: u64,
    },
    /// The provider returned internally inconsistent raw data.
    #[error("invalid raw provider response: {message}")]
    InvalidRawLog {
        /// The validation detail.
        message: String,
    },
    /// Two providers disagreed on a finalized head or height.
    #[error("providers disagree on finalized chain state: {left:?} vs {right:?}")]
    ProviderDisagreement {
        /// The first provider identity.
        left: ProviderIdentity,
        /// The second provider identity.
        right: ProviderIdentity,
    },
    /// Provider evidence did not cover every requested block exactly once.
    #[error("finalized block coverage is incomplete: {message}")]
    MissingBlockCoverage {
        /// The coverage validation detail.
        message: String,
    },
    /// A raw batch repeated one provider log identity.
    #[error("raw provider batch contains a duplicate log identity")]
    DuplicateRawLog,
}

/// A provider-neutral source of finalized raw EVM logs.
#[async_trait]
pub trait FinalizedRawLogProvider: Send + Sync {
    /// Returns the provider's current head and finalized block.
    async fn finalized_head(&self) -> Result<(BlockHead, BlockHead), ChainSourceError>;

    /// Fetches raw logs for an inclusive range that must be finalized.
    async fn fetch_finalized_logs(
        &self,
        range: &FinalizedBlockRange,
    ) -> Result<FinalizedRawLogBatch, ChainSourceError>;
}

#[cfg(test)]
#[path = "raw_tests.rs"]
mod tests;
