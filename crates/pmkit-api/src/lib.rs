//! Versioned Polymarket chain-truth read types.

mod api;
mod gamma;
mod types;

pub use api::ChainTruthApiV2;
pub use gamma::{
    DiscoveryError as GammaDiscoveryError, GammaClient, GammaMarket, GammaPageRequest,
};
pub use types::{
    ActivityQuery, ChainTruthActivity, ChainTruthBalance, ChainTruthPosition, ChainTruthTrade,
    CursorPage, DataOrderQuery, DataOrdersQuery, DataResponse, NotReconstructibleFromChain,
    Pagination, PositionStatus, PositionsQuery, QueryError, TradesQuery,
};

#[cfg(test)]
mod tests;
