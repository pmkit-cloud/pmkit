//! Versioned Polymarket chain-truth read types.

mod api;
mod gamma;
mod types;

pub use api::ChainTruthApiV1;
pub use gamma::{
    DiscoveryError as GammaDiscoveryError, GammaClient, GammaMarket, GammaPageRequest,
};
pub use types::{
    ActivityQuery, ChainTruthActivity, ChainTruthBalance, ChainTruthClosedPosition,
    ChainTruthPosition, ChainTruthTrade, ClosedPositionsQuery, DataOrderQuery, DataOrdersQuery,
    NotReconstructibleFromChain, PositionsQuery, QueryError, TradesQuery,
};

#[cfg(test)]
mod tests;
