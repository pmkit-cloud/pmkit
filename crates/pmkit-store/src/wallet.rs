use rust_decimal::Decimal;

use crate::{Address, CanonicalChainLog, ChainCheckpoint, TradeSide};

use crate::wallet_reducer::reconstruct_wallet;

/// A request to rebuild one wallet over an optional inclusive block range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletQuery {
    /// The wallet whose chain state is rebuilt.
    pub wallet: Address,
    /// The optional first canonical block.
    pub from_block: Option<u64>,
    /// The optional final canonical block.
    pub to_block: Option<u64>,
}

impl WalletQuery {
    /// Creates an unbounded wallet reconstruction query.
    #[must_use]
    pub const fn new(wallet: Address) -> Self {
        Self {
            wallet,
            from_block: None,
            to_block: None,
        }
    }
}

/// A nonzero outcome-token position proved by ERC-1155 transfers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletPosition {
    /// The outcome token identifier.
    pub asset_id: String,
    /// The net canonical token balance.
    pub size: Decimal,
}

/// A reconstructed exchange fill involving the queried wallet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletTrade {
    /// The canonical transaction hash.
    pub transaction_hash: String,
    /// The canonical block height.
    pub block_number: u64,
    /// The traded outcome token.
    pub asset_id: String,
    /// Whether the queried wallet was the maker.
    pub maker: bool,
    /// The queried wallet's outcome-token direction.
    pub side: TradeSide,
    /// The wallet's filled amount.
    pub size: Decimal,
    /// The paired filled amount.
    pub counter_amount: Decimal,
    /// The exact protocol fee.
    pub fee: Decimal,
}

/// A protocol-level activity kind with direct chain evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalletActivityKind {
    /// A CTF position split.
    Split,
    /// A CTF position merge.
    Merge,
    /// A CTF payout redemption.
    Redemption,
    /// An exchange order fill.
    Trade,
    /// An exchange match without an order lifecycle projection.
    Match,
    /// A protocol fee charge.
    Fee,
}

/// One chain-proven wallet activity record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletActivity {
    /// The canonical transaction hash.
    pub transaction_hash: String,
    /// The canonical block height.
    pub block_number: u64,
    /// The direct protocol activity kind.
    pub kind: WalletActivityKind,
    /// The CTF condition when the event contains one.
    pub condition_id: Option<String>,
    /// The outcome token when the event contains one.
    pub asset_id: Option<String>,
    /// The directly emitted amount.
    pub amount: Decimal,
}

/// A deterministic wallet reconstruction from canonical logs only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalletSnapshot {
    /// The reconstructed wallet.
    pub wallet: Address,
    /// The canonical tip used by this rebuild, if the range contained logs.
    pub canonical_tip: Option<ChainCheckpoint>,
    /// The net pUSD balance proved by collateral transfer events.
    pub collateral_balance: Decimal,
    /// The nonzero outcome-token balances in ascending asset-id order.
    pub positions: Vec<WalletPosition>,
    /// Collateral paid by CTF redemption events.
    pub settled_collateral: Decimal,
    /// Exchange fill records in canonical log order.
    pub trades: Vec<WalletTrade>,
    /// Protocol activity records in canonical log order.
    pub activity: Vec<WalletActivity>,
}

/// Rebuilds one wallet from logs already known to be canonical and validated.
#[must_use]
pub fn rebuild_wallet(query: &WalletQuery, logs: &[CanonicalChainLog]) -> WalletSnapshot {
    reconstruct_wallet(query, logs)
}
