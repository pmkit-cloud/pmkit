use rust_decimal::Decimal;
use serde::Serialize;
use thiserror::Error;

/// A lifecycle status accepted by the v2 positions route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PositionStatus {
    /// A held position, including holdings whose market has resolved.
    Open,
    /// A position eligible for redemption.
    Redeemable,
    /// A losing position held after resolution.
    RedeemableLost,
    /// Complementary positions that can be merged.
    Mergeable,
    /// A position that has been exited or settled.
    Closed,
}

/// A v2 unified positions query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionsQuery {
    /// The required proxy wallet.
    pub user: String,
    /// The page size, capped at 1,000 rows.
    pub limit: usize,
    /// Optional v2 position lifecycle filter (defaults to `OPEN`).
    pub status: Option<PositionStatus>,
    /// Optional CTF condition IDs; at most 20 distinct values are accepted.
    pub condition: Vec<String>,
    /// Optional event ID filter.
    pub event_id: Option<String>,
    /// Opaque cursor returned by a previous page.
    pub cursor: Option<String>,
}

impl PositionsQuery {
    /// Creates a positions query for `user` with no filters or cursor.
    ///
    /// # Errors
    /// Returns [`QueryError`] when the user is empty or `limit` exceeds 1,000.
    pub fn new(user: impl Into<String>, limit: usize) -> Result<Self, QueryError> {
        let (user, limit) = page_query(user, limit)?;
        Ok(Self {
            user,
            limit,
            status: None,
            condition: Vec::new(),
            event_id: None,
            cursor: None,
        })
    }
}

/// A v2 trades query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradesQuery {
    /// The required proxy wallet.
    pub user: String,
    /// The page size, capped at 1,000 rows.
    pub limit: usize,
    /// Optional CTF condition IDs; at most 20 distinct values are accepted.
    pub condition: Vec<String>,
    /// Optional event ID filter.
    pub event_id: Option<String>,
    /// Opaque cursor returned by a previous page.
    pub cursor: Option<String>,
}

impl TradesQuery {
    /// Creates a trades query for `user` with no filters or cursor.
    ///
    /// # Errors
    /// Returns [`QueryError`] when the user is empty or `limit` exceeds 1,000.
    pub fn new(user: impl Into<String>, limit: usize) -> Result<Self, QueryError> {
        let (user, limit) = page_query(user, limit)?;
        Ok(Self {
            user,
            limit,
            condition: Vec::new(),
            event_id: None,
            cursor: None,
        })
    }
}

/// A v2 activity query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityQuery {
    /// The required proxy wallet.
    pub user: String,
    /// The page size, capped at 1,000 rows.
    pub limit: usize,
    /// Optional CTF condition IDs; at most 20 distinct values are accepted.
    pub condition: Vec<String>,
    /// Optional event ID filter.
    pub event_id: Option<String>,
    /// Opaque cursor returned by a previous page.
    pub cursor: Option<String>,
}

impl ActivityQuery {
    /// Creates an activity query for `user` with no filters or cursor.
    ///
    /// # Errors
    /// Returns [`QueryError`] when the user is empty or `limit` exceeds 1,000.
    pub fn new(user: impl Into<String>, limit: usize) -> Result<Self, QueryError> {
        let (user, limit) = page_query(user, limit)?;
        Ok(Self {
            user,
            limit,
            condition: Vec::new(),
            event_id: None,
            cursor: None,
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

/// A v2 response envelope for non-paginated data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DataResponse<T> {
    /// The response data, or `null` for a miss.
    pub data: T,
}

/// A page of v2-shaped chain-truth rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CursorPage<T> {
    /// The page data; empty results serialize as `[]`.
    pub data: Vec<T>,
    /// Cursor metadata for the next page.
    pub pagination: Pagination,
}

/// Cursor and display-only offset metadata for a paginated response.
///
/// Cursors are non-secret, deterministic, build-local consistency markers; they
/// are not signed and must not be used for authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Pagination {
    /// The page size.
    pub limit: usize,
    /// The display-only position of the page in its filtered snapshot.
    pub offset: usize,
    /// Whether another page is available.
    pub has_more: bool,
    /// The opaque cursor for the next page, or `null` when exhausted.
    pub next_cursor: Option<String>,
}

/// A sparse chain-proven position row using v2 field names.
///
/// Only held-token fields are reconstructed here. Valuation fields are omitted;
/// condition IDs and lifecycle flags are omitted when chain truth cannot establish them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ChainTruthPosition {
    /// The proxy wallet.
    pub proxy_wallet: String,
    /// The CTF condition identifier, omitted when asset metadata is unavailable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition_id: Option<String>,
    /// The outcome token identifier.
    pub token_id: String,
    /// The canonical ERC-1155 balance.
    pub current_size: Decimal,
    /// The v2 lifecycle status supported by this chain-truth projection.
    pub status: PositionStatus,
    /// Whether the position can currently be redeemed, omitted when unknown.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redeemable: Option<bool>,
    /// Whether the position can currently be merged, omitted when unknown.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mergeable: Option<bool>,
}

/// A chain-proven exchange fill.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
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
    /// The chain-proven fill direction for this wallet.
    pub side: String,
    /// The wallet's filled amount.
    pub size: Decimal,
    /// The paired filled amount.
    pub counter_amount: Decimal,
    /// The protocol fee.
    pub fee: Decimal,
}

/// A chain-proven protocol activity row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ChainTruthActivity {
    /// The proxy wallet.
    pub proxy_wallet: String,
    /// The direct protocol event kind.
    #[serde(rename = "type")]
    pub activity_type: String,
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

/// A collateral balance that can be reconstructed from canonical transfers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ChainTruthBalance {
    /// The collateral asset used by the canonical registry.
    pub asset: &'static str,
    /// The exact reconstructed collateral balance.
    pub balance: Decimal,
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

/// Query validation or chain-truth reconstruction failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum QueryError {
    /// The wallet query was empty.
    #[error("user must not be empty")]
    EmptyUser,
    /// A page size is outside the v2 range.
    #[error("limit must be between 1 and {maximum}")]
    InvalidLimit {
        /// The v2 maximum page size.
        maximum: usize,
    },
    /// A condition filter contained an empty ID.
    #[error("condition IDs must not be empty")]
    EmptyCondition,
    /// The event ID filter was empty.
    #[error("event_id must not be empty")]
    EmptyEventId,
    /// Too many distinct condition IDs were requested.
    #[error("condition filter exceeds the maximum of {maximum} distinct IDs")]
    TooManyConditions {
        /// The maximum number of distinct condition IDs.
        maximum: usize,
    },
    /// The requested user is not the wallet used to build this projection.
    #[error("user does not match the reconstructed wallet")]
    WalletMismatch,
    /// The cursor is invalid or belongs to a different snapshot, route, or filter.
    #[error("cursor is invalid or does not match this snapshot, route, or filter")]
    InvalidCursor,
    /// Position lifecycle requires resolution and asset-condition data absent from chain truth.
    #[error("requested position status is not reconstructible from this chain-truth snapshot")]
    PositionLifecycleNotReconstructible,
    /// A route's condition filter cannot be evaluated from its chain-truth records.
    #[error("condition filter is not reconstructible for {route}")]
    ConditionFilterNotReconstructible {
        /// The v2 route whose condition filter cannot be evaluated.
        route: &'static str,
    },
    /// Event IDs are not present in the chain-truth records for a route.
    #[error("event_id filter is not reconstructible for {route}")]
    EventIdFilterNotReconstructible {
        /// The v2 route whose event ID filter cannot be evaluated.
        route: &'static str,
    },
}

fn page_query(user: impl Into<String>, limit: usize) -> Result<(String, usize), QueryError> {
    let user = user.into();
    if user.trim().is_empty() {
        return Err(QueryError::EmptyUser);
    }
    if !(1..=1_000).contains(&limit) {
        return Err(QueryError::InvalidLimit { maximum: 1_000 });
    }
    Ok((user, limit))
}
