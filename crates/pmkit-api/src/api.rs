use pmkit_store::{WalletActivityKind, WalletSnapshot};

use crate::{
    ActivityQuery, ChainTruthActivity, ChainTruthClosedPosition, ChainTruthPage,
    ChainTruthPosition, ChainTruthTrade, ClosedPositionsQuery, DataOrderQuery, DataOrdersQuery,
    NotReconstructibleFromChain, PositionsQuery, TradesQuery,
};

/// Version 1 of `PMKit`'s chain-truth API projection.
#[derive(Debug, Clone)]
pub struct ChainTruthApiV1 {
    snapshot: WalletSnapshot,
}

impl ChainTruthApiV1 {
    /// Creates an API projection from one deterministic wallet reconstruction.
    #[must_use]
    pub const fn from_snapshot(snapshot: WalletSnapshot) -> Self {
        Self { snapshot }
    }

    /// Returns current outcome-token positions using Data API offset pagination.
    #[must_use]
    pub fn positions(&self, query: &PositionsQuery) -> ChainTruthPage<ChainTruthPosition> {
        let wallet = self.snapshot.wallet.as_str();
        let data = self
            .snapshot
            .positions
            .iter()
            .map(|position| ChainTruthPosition {
                proxy_wallet: wallet.into(),
                asset: position.asset_id.clone(),
                size: position.size,
            });
        page(query.limit, query.offset, data.collect())
    }

    /// Returns CTF-redemption records using Data API offset pagination.
    #[must_use]
    pub fn closed_positions(
        &self,
        query: &ClosedPositionsQuery,
    ) -> ChainTruthPage<ChainTruthClosedPosition> {
        let wallet = self.snapshot.wallet.as_str();
        let data = self
            .snapshot
            .activity
            .iter()
            .filter(|activity| activity.kind == WalletActivityKind::Redemption)
            .map(|activity| ChainTruthClosedPosition {
                proxy_wallet: wallet.into(),
                condition_id: activity.condition_id.clone().unwrap_or_default(),
                settled_collateral: activity.amount,
            })
            .collect();
        page(query.limit, query.offset, data)
    }

    /// Returns exchange fills using Data API offset pagination.
    #[must_use]
    pub fn trades(&self, query: &TradesQuery) -> ChainTruthPage<ChainTruthTrade> {
        let wallet = self.snapshot.wallet.as_str();
        let data = self
            .snapshot
            .trades
            .iter()
            .map(|trade| ChainTruthTrade {
                proxy_wallet: wallet.into(),
                asset: trade.asset_id.clone(),
                transaction_hash: trade.transaction_hash.clone(),
                block_number: trade.block_number,
                maker: trade.maker,
                size: trade.size,
                counter_amount: trade.counter_amount,
                fee: trade.fee,
            })
            .collect();
        page(query.limit, query.offset, data)
    }

    /// Returns CTF and exchange protocol activity using Data API offset pagination.
    #[must_use]
    pub fn activity(&self, query: &ActivityQuery) -> ChainTruthPage<ChainTruthActivity> {
        let wallet = self.snapshot.wallet.as_str();
        let data = self
            .snapshot
            .activity
            .iter()
            .map(|activity| ChainTruthActivity {
                proxy_wallet: wallet.into(),
                kind: format!("{:?}", activity.kind),
                transaction_hash: activity.transaction_hash.clone(),
                block_number: activity.block_number,
                condition_id: activity.condition_id.clone(),
                asset: activity.asset_id.clone(),
                amount: activity.amount,
            })
            .collect();
        page(query.limit, query.offset, data)
    }

    /// Refuses CLOB order-list reconstruction because signed order state is offchain.
    ///
    /// # Errors
    /// Always returns [`NotReconstructibleFromChain::Orders`].
    pub const fn data_orders(
        &self,
        _query: &DataOrdersQuery,
    ) -> Result<(), NotReconstructibleFromChain> {
        Err(NotReconstructibleFromChain::Orders)
    }

    /// Refuses CLOB single-order reconstruction because signed order state is offchain.
    ///
    /// # Errors
    /// Always returns [`NotReconstructibleFromChain::Order`].
    pub const fn data_order(
        &self,
        _query: &DataOrderQuery,
    ) -> Result<(), NotReconstructibleFromChain> {
        Err(NotReconstructibleFromChain::Order)
    }
}

fn page<T>(limit: usize, offset: usize, mut data: Vec<T>) -> ChainTruthPage<T> {
    let end = offset.saturating_add(limit).min(data.len());
    let data = if offset >= data.len() {
        Vec::new()
    } else {
        data.drain(offset..end).collect()
    };
    ChainTruthPage {
        version: "v1",
        limit,
        offset,
        data,
    }
}
