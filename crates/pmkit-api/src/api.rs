use pmkit_store::{Address, TradeSide, WalletActivityKind, WalletSnapshot};

use crate::{
    ActivityQuery, ChainTruthActivity, ChainTruthBalance, ChainTruthClosedPosition,
    ChainTruthPosition, ChainTruthTrade, ClosedPositionsQuery, DataOrderQuery, DataOrdersQuery,
    NotReconstructibleFromChain, PositionsQuery, QueryError, TradesQuery,
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
    ///
    /// # Errors
    /// Returns [`QueryError::WalletMismatch`] when `user` differs from the reconstructed wallet.
    pub fn positions(&self, query: &PositionsQuery) -> Result<Vec<ChainTruthPosition>, QueryError> {
        self.validate_user(&query.user)?;
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
        Ok(page(query.limit, query.offset, data.collect()))
    }

    /// Returns CTF-redemption records using Data API offset pagination.
    ///
    /// # Errors
    /// Returns [`QueryError::WalletMismatch`] when `user` differs from the reconstructed wallet.
    pub fn closed_positions(
        &self,
        query: &ClosedPositionsQuery,
    ) -> Result<Vec<ChainTruthClosedPosition>, QueryError> {
        self.validate_user(&query.user)?;
        let wallet = self.snapshot.wallet.as_str();
        let data = self
            .snapshot
            .activity
            .iter()
            .filter(|activity| activity.kind == WalletActivityKind::Redemption)
            .filter_map(|activity| {
                activity
                    .condition_id
                    .clone()
                    .map(|condition_id| ChainTruthClosedPosition {
                        proxy_wallet: wallet.into(),
                        condition_id,
                        settled_collateral: activity.amount,
                    })
            })
            .collect();
        Ok(page(query.limit, query.offset, data))
    }

    /// Returns exchange fills using Data API offset pagination.
    ///
    /// # Errors
    /// Returns [`QueryError::WalletMismatch`] when `user` differs from the reconstructed wallet.
    pub fn trades(&self, query: &TradesQuery) -> Result<Vec<ChainTruthTrade>, QueryError> {
        self.validate_user(&query.user)?;
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
                side: side(trade.side).into(),
                size: trade.size,
                counter_amount: trade.counter_amount,
                fee: trade.fee,
            })
            .collect();
        Ok(page(query.limit, query.offset, data))
    }

    /// Returns CTF and exchange protocol activity using Data API offset pagination.
    ///
    /// # Errors
    /// Returns [`QueryError::WalletMismatch`] when `user` differs from the reconstructed wallet.
    pub fn activity(&self, query: &ActivityQuery) -> Result<Vec<ChainTruthActivity>, QueryError> {
        self.validate_user(&query.user)?;
        let wallet = self.snapshot.wallet.as_str();
        let data = self
            .snapshot
            .activity
            .iter()
            .filter_map(|activity| {
                activity_type(activity.kind).map(|activity_type| ChainTruthActivity {
                    proxy_wallet: wallet.into(),
                    activity_type: activity_type.into(),
                    transaction_hash: activity.transaction_hash.clone(),
                    block_number: activity.block_number,
                    condition_id: activity.condition_id.clone(),
                    asset: activity.asset_id.clone(),
                    amount: activity.amount,
                })
            })
            .collect();
        Ok(page(query.limit, query.offset, data))
    }

    /// Returns the collateral balance with no offchain valuation fabricated.
    #[must_use]
    pub const fn balance(&self) -> ChainTruthBalance {
        ChainTruthBalance {
            asset: "USDC",
            balance: self.snapshot.collateral_balance,
        }
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

    fn validate_user(&self, user: &str) -> Result<(), QueryError> {
        let user = Address::new(user).map_err(|_| QueryError::WalletMismatch)?;
        (user == self.snapshot.wallet)
            .then_some(())
            .ok_or(QueryError::WalletMismatch)
    }
}

fn page<T>(limit: usize, offset: usize, mut data: Vec<T>) -> Vec<T> {
    let end = offset.saturating_add(limit).min(data.len());
    if offset >= data.len() {
        Vec::new()
    } else {
        data.drain(offset..end).collect()
    }
}

const fn side(side: TradeSide) -> &'static str {
    match side {
        TradeSide::Buy => "BUY",
        TradeSide::Sell => "SELL",
    }
}

const fn activity_type(kind: WalletActivityKind) -> Option<&'static str> {
    match kind {
        WalletActivityKind::Trade => Some("TRADE"),
        WalletActivityKind::Split => Some("SPLIT"),
        WalletActivityKind::Merge => Some("MERGE"),
        WalletActivityKind::Redemption => Some("REDEEM"),
        WalletActivityKind::Match | WalletActivityKind::Fee => None,
    }
}
