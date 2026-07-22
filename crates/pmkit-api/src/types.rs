use rust_decimal::Decimal;
use serde::Serialize;
use thiserror::Error;

/// A versioned offset-paginated chain-truth response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChainTruthPage<T> {
    /// The `PMKit` API schema version.
    pub version: &'static str,
    /// The applied request limit.
    pub limit: usize,
    /// The applied request offset.
    pub offset: usize,
    /// The chain-proven response rows.
    pub data: Vec<T>,
}

/// A position query with the official Data API offset shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionsQuery {
    /// The required proxy wallet.
    pub user: String,
    /// The page size, capped at the official 500 rows.
    pub limit: usize,
    /// The zero-based page offset.
    pub offset: usize,
}

impl PositionsQuery {
    /// Validates a position page query.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError`] for an empty user or a limit above 500.
    pub fn new(user: impl Into<String>, limit: usize, offset: usize) -> Result<Self, QueryError> {
        page_query(user, limit, offset, 500).map(|(user, limit, offset)| Self {
            user,
            limit,
            offset,
        })
    }
}

/// A closed-position query with the official Data API offset shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedPositionsQuery {
    /// The required proxy wallet.
    pub user: String,
    /// The page size, capped at the official 50 rows.
    pub limit: usize,
    /// The zero-based page offset.
    pub offset: usize,
}

impl ClosedPositionsQuery {
    /// Validates a closed-position page query.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError`] for an empty user or a limit above 50.
    pub fn new(user: impl Into<String>, limit: usize, offset: usize) -> Result<Self, QueryError> {
        page_query(user, limit, offset, 50).map(|(user, limit, offset)| Self {
            user,
            limit,
            offset,
        })
    }
}

/// A trade query with the official Data API offset shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradesQuery {
    /// The required proxy wallet.
    pub user: String,
    /// The page size, capped at the official 10,000 rows.
    pub limit: usize,
    /// The zero-based page offset.
    pub offset: usize,
}

impl TradesQuery {
    /// Validates a trade page query.
    ///
    /// # Errors
    /// Returns [`QueryError`] for an empty user or a limit above 10,000.
    pub fn new(user: impl Into<String>, limit: usize, offset: usize) -> Result<Self, QueryError> {
        page_query(user, limit, offset, 10_000).map(|(user, limit, offset)| Self {
            user,
            limit,
            offset,
        })
    }
}

/// An activity query with the official Data API offset shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityQuery {
    /// The required proxy wallet.
    pub user: String,
    /// The page size, capped at the official 500 rows.
    pub limit: usize,
    /// The zero-based page offset.
    pub offset: usize,
}

impl ActivityQuery {
    /// Validates an activity page query.
    ///
    /// # Errors
    /// Returns [`QueryError`] for an empty user or a limit above 500.
    pub fn new(user: impl Into<String>, limit: usize, offset: usize) -> Result<Self, QueryError> {
        page_query(user, limit, offset, 500).map(|(user, limit, offset)| Self {
            user,
            limit,
            offset,
        })
    }
}

/// The CLOB `/data/orders` query shape that cannot be satisfied from chain data.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DataOrdersQuery {
    /// The optional CLOB order identifier.
    pub id: Option<String>,
    /// The optional market identifier.
    pub market: Option<String>,
    /// The optional outcome token identifier.
    pub asset_id: Option<String>,
}

/// The CLOB `/data/order/{id}` query shape that cannot be satisfied from chain data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataOrderQuery {
    /// The offchain CLOB order identifier.
    pub id: String,
}

/// A chain-proven current outcome-token position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChainTruthPosition {
    /// The proxy wallet.
    pub proxy_wallet: String,
    /// The outcome token identifier.
    pub asset: String,
    /// The canonical ERC-1155 balance.
    pub size: Decimal,
}

/// A chain-proven settled condition, without offchain display metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChainTruthClosedPosition {
    /// The proxy wallet.
    pub proxy_wallet: String,
    /// The CTF condition identifier.
    pub condition_id: String,
    /// The redeemed CTF collateral.
    pub settled_collateral: Decimal,
}

/// A chain-proven exchange fill.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChainTruthTrade {
    /// The wallet that participated in the fill.
    pub proxy_wallet: String,
    /// The outcome token identifier.
    pub asset: String,
    /// The canonical transaction hash.
    pub transaction_hash: String,
    /// The canonical block height.
    pub block_number: u64,
    /// Whether the wallet was the maker.
    pub maker: bool,
    /// The wallet's filled amount.
    pub size: Decimal,
    /// The paired filled amount.
    pub counter_amount: Decimal,
    /// The protocol fee.
    pub fee: Decimal,
}

/// A chain-proven protocol activity row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChainTruthActivity {
    /// The proxy wallet.
    pub proxy_wallet: String,
    /// The direct protocol event kind.
    pub kind: String,
    /// The canonical transaction hash.
    pub transaction_hash: String,
    /// The canonical block height.
    pub block_number: u64,
    /// The optional CTF condition identifier.
    pub condition_id: Option<String>,
    /// The optional outcome token identifier.
    pub asset: Option<String>,
    /// The directly emitted amount.
    pub amount: Decimal,
}

/// A typed refusal for CLOB concepts that require offchain lifecycle data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum NotReconstructibleFromChain {
    /// CLOB `/data/orders` needs offchain signed-order lifecycle state.
    #[error("CLOB /data/orders is not reconstructible from canonical chain logs")]
    Orders,
    /// CLOB `/data/order/{{id}}` needs offchain signed-order lifecycle state.
    #[error("CLOB /data/order/{{id}} is not reconstructible from canonical chain logs")]
    Order,
}

/// Query validation failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum QueryError {
    /// The wallet query was empty.
    #[error("user must not be empty")]
    EmptyUser,
    /// The page size exceeds the endpoint's documented cap.
    #[error("limit exceeds endpoint maximum of {maximum}")]
    LimitTooLarge {
        /// The endpoint-specific maximum.
        maximum: usize,
    },
}

fn page_query(
    user: impl Into<String>,
    limit: usize,
    offset: usize,
    maximum: usize,
) -> Result<(String, usize, usize), QueryError> {
    let user = user.into();
    if user.trim().is_empty() {
        return Err(QueryError::EmptyUser);
    }
    if limit > maximum {
        return Err(QueryError::LimitTooLarge { maximum });
    }
    Ok((user, limit, offset))
}
