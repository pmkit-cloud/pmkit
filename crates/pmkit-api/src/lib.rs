//! Versioned Polymarket chain-truth read types.

mod api;
mod types;

pub use api::ChainTruthApiV1;
pub use types::{
    ActivityQuery, ChainTruthActivity, ChainTruthClosedPosition, ChainTruthPage,
    ChainTruthPosition, ChainTruthTrade, ClosedPositionsQuery, DataOrderQuery, DataOrdersQuery,
    NotReconstructibleFromChain, PositionsQuery, QueryError, TradesQuery,
};

#[cfg(test)]
mod tests;
