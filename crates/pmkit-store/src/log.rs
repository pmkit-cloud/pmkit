use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{Address, ChainId};

/// The durable source identity of one canonical EVM log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalLogIdentity {
    /// The EVM chain that emitted the log.
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

/// A parsed outcome-token amount from an ERC-1155 batch transfer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeTokenAmount {
    /// The outcome token identifier.
    pub asset_id: String,
    /// The exact token amount after ABI parsing.
    pub amount: Decimal,
}

/// A typed Polymarket event parsed from a canonical EVM log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChainEvent {
    /// An ERC-20 transfer of pUSD collateral.
    CollateralTransfer {
        /// The collateral sender.
        from: Address,
        /// The collateral recipient.
        to: Address,
        /// The transferred collateral.
        amount: Decimal,
    },
    /// An ERC-1155 single outcome-token transfer.
    OutcomeTransferSingle {
        /// The token sender.
        from: Address,
        /// The token recipient.
        to: Address,
        /// The outcome token identifier.
        asset_id: String,
        /// The transferred outcome tokens.
        amount: Decimal,
    },
    /// An ERC-1155 batch outcome-token transfer.
    OutcomeTransferBatch {
        /// The token sender.
        from: Address,
        /// The token recipient.
        to: Address,
        /// The batched outcome-token values.
        transfers: Vec<OutcomeTokenAmount>,
    },
    /// A CTF collateral split.
    PositionSplit {
        /// The splitting wallet.
        stakeholder: Address,
        /// The resolved CTF condition.
        condition_id: String,
        /// The split collateral.
        amount: Decimal,
    },
    /// A CTF outcome-token merge.
    PositionsMerge {
        /// The merging wallet.
        stakeholder: Address,
        /// The resolved CTF condition.
        condition_id: String,
        /// The merged collateral.
        amount: Decimal,
    },
    /// A CTF payout redemption.
    PayoutRedemption {
        /// The redeeming wallet.
        redeemer: Address,
        /// The resolved CTF condition.
        condition_id: String,
        /// The redeemed collateral payout.
        payout: Decimal,
    },
    /// An exchange fill with exact maker/taker amounts.
    OrderFilled {
        /// The maker wallet.
        maker: Address,
        /// The taker wallet.
        taker: Address,
        /// The outcome token identifier.
        asset_id: String,
        /// The maker amount filled.
        maker_amount: Decimal,
        /// The taker amount filled.
        taker_amount: Decimal,
        /// The charged fee.
        fee: Decimal,
    },
    /// An exchange match without enough evidence to synthesize an order lifecycle.
    OrdersMatched {
        /// The matched wallet.
        trader: Address,
        /// The outcome token identifier.
        asset_id: String,
        /// The matched outcome tokens.
        amount: Decimal,
    },
    /// An exchange fee charge.
    FeeCharged {
        /// The charged wallet.
        payer: Address,
        /// The fee recipient.
        recipient: Address,
        /// The exact charged collateral.
        amount: Decimal,
    },
}

/// One parsed event plus its canonical source identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalChainLog {
    /// The full canonical log identity.
    pub identity: CanonicalLogIdentity,
    /// The contract that emitted this event.
    pub contract_address: Address,
    /// The ABI-parsed protocol event.
    pub event: ChainEvent,
}

impl CanonicalChainLog {
    /// Creates a compact typed fixture log on Polygon mainnet.
    #[must_use]
    pub fn fixture(
        block_number: u64,
        block_hash: impl Into<String>,
        transaction_index: u64,
        log_index: u64,
        contract_address: Address,
        event: ChainEvent,
    ) -> Self {
        Self {
            identity: CanonicalLogIdentity {
                chain_id: ChainId::POLYGON,
                block_number,
                block_hash: block_hash.into(),
                transaction_hash: format!("0xfixture-{block_number}-{transaction_index}"),
                transaction_index,
                log_index,
            },
            contract_address,
            event,
        }
    }
}

/// A canonical block checkpoint used as a deterministic reorg ancestor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainCheckpoint {
    /// The checkpoint chain.
    pub chain_id: ChainId,
    /// The finalized or common-ancestor height.
    pub block_number: u64,
    /// The canonical hash at that height.
    pub block_hash: String,
}

impl ChainCheckpoint {
    /// Creates an explicit canonical checkpoint.
    #[must_use]
    pub fn new(chain_id: ChainId, block_number: u64, block_hash: impl Into<String>) -> Self {
        Self {
            chain_id,
            block_number,
            block_hash: block_hash.into(),
        }
    }
}

/// Replacement logs following a source-proven common ancestor checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalLogSegment {
    /// The last mutually canonical checkpoint before this segment.
    pub common_ancestor: ChainCheckpoint,
    /// Replacement canonical logs strictly after the ancestor.
    pub logs: Vec<CanonicalChainLog>,
}

impl CanonicalLogSegment {
    /// Creates a canonical replacement segment from parsed typed logs.
    #[must_use]
    pub const fn new(common_ancestor: ChainCheckpoint, logs: Vec<CanonicalChainLog>) -> Self {
        Self {
            common_ancestor,
            logs,
        }
    }
}
