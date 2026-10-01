use std::{collections::HashMap, fmt::Debug};

use pmkit_store::{Address, TradeSide, WalletActivityKind, WalletSnapshot};

use crate::{
    ActivityQuery, ChainTruthActivity, ChainTruthBalance, ChainTruthPosition, ChainTruthTrade,
    CursorPage, DataOrderQuery, DataOrdersQuery, NotReconstructibleFromChain, Pagination,
    PositionStatus, PositionsQuery, QueryError, TradesQuery,
};

const MAX_CONDITIONS: usize = 20;

/// Version 2 of `PMKit`'s chain-truth API projection.
#[derive(Debug, Clone)]
pub struct ChainTruthApiV2 {
    snapshot: WalletSnapshot,
    snapshot_fingerprint: u64,
}

impl ChainTruthApiV2 {
    /// Creates an API projection from one deterministic wallet reconstruction.
    #[must_use]
    pub fn from_snapshot(snapshot: WalletSnapshot) -> Self {
        let snapshot_fingerprint = stable_hash(format!("{snapshot:?}").as_bytes());
        Self {
            snapshot,
            snapshot_fingerprint,
        }
    }

    /// Returns held positions using the v2 cursor envelope.
    ///
    /// `OPEN` represents all currently held token balances, including positions
    /// in resolved markets. Resolution flags and condition IDs are omitted when
    /// the chain-truth snapshot cannot establish them. `REDEEMABLE`,
    /// `REDEEMABLE_LOST`, `MERGEABLE`, and `CLOSED` filters are refused: they
    /// require resolution, outcome, or full exit economics absent from the snapshot.
    ///
    /// # Errors
    /// Returns [`QueryError`] for invalid filters, a wallet mismatch, a stale
    /// cursor, or lifecycle semantics that cannot be reconstructed from chain truth.
    pub fn positions(
        &self,
        query: &PositionsQuery,
    ) -> Result<CursorPage<ChainTruthPosition>, QueryError> {
        let user = self.validate_user(&query.user)?;
        validate_limit(query.limit)?;
        let conditions = normalized_conditions(&query.condition)?;
        validate_event_id_filter(query.event_id.as_deref(), "/v2/positions")?;
        if query
            .status
            .is_some_and(|status| status != PositionStatus::Open)
        {
            return Err(QueryError::PositionLifecycleNotReconstructible);
        }
        if !conditions.is_empty() {
            return Err(QueryError::ConditionFilterNotReconstructible {
                route: "/v2/positions",
            });
        }

        let query_fingerprint = stable_hash(
            format!(
                "{user:?}{conditions:?}{:?}{:?}",
                query.status, query.event_id
            )
            .as_bytes(),
        );
        let wallet = self.snapshot.wallet.as_str();
        let data = self
            .snapshot
            .positions
            .iter()
            .filter(|position| !position.size.is_zero())
            .map(|position| ChainTruthPosition {
                proxy_wallet: wallet.into(),
                condition_id: None,
                token_id: position.asset_id.clone(),
                current_size: position.size,
                status: PositionStatus::Open,
                redeemable: None,
                mergeable: None,
            })
            .collect();
        page(
            "/v2/positions",
            query_fingerprint,
            self.snapshot_fingerprint,
            query.limit,
            query.cursor.as_deref(),
            data,
        )
    }

    /// Returns exchange fills using the v2 cursor envelope.
    ///
    /// # Errors
    /// Returns [`QueryError`] for invalid filters, a wallet mismatch, a stale
    /// cursor, or filters that cannot be reconstructed from chain truth.
    pub fn trades(&self, query: &TradesQuery) -> Result<CursorPage<ChainTruthTrade>, QueryError> {
        let user = self.validate_user(&query.user)?;
        validate_limit(query.limit)?;
        let conditions = normalized_conditions(&query.condition)?;
        validate_event_id_filter(query.event_id.as_deref(), "/v2/trades")?;
        if !conditions.is_empty() {
            return Err(QueryError::ConditionFilterNotReconstructible {
                route: "/v2/trades",
            });
        }

        let query_fingerprint =
            stable_hash(format!("{user:?}{conditions:?}{:?}", query.event_id).as_bytes());
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
        page(
            "/v2/trades",
            query_fingerprint,
            self.snapshot_fingerprint,
            query.limit,
            query.cursor.as_deref(),
            data,
        )
    }

    /// Returns reconstructible CTF and exchange activity using the v2 cursor envelope.
    ///
    /// # Errors
    /// Returns [`QueryError`] for invalid filters, a wallet mismatch, a stale
    /// cursor, or filters that cannot be reconstructed from chain truth.
    pub fn activity(
        &self,
        query: &ActivityQuery,
    ) -> Result<CursorPage<ChainTruthActivity>, QueryError> {
        let user = self.validate_user(&query.user)?;
        validate_limit(query.limit)?;
        let conditions = normalized_conditions(&query.condition)?;
        validate_event_id_filter(query.event_id.as_deref(), "/v2/activity")?;
        if !conditions.is_empty()
            && self.snapshot.activity.iter().any(|activity| {
                activity_type(activity.kind).is_some() && activity.condition_id.is_none()
            })
        {
            return Err(QueryError::ConditionFilterNotReconstructible {
                route: "/v2/activity",
            });
        }

        let query_fingerprint =
            stable_hash(format!("{user:?}{conditions:?}{:?}", query.event_id).as_bytes());
        let wallet = self.snapshot.wallet.as_str();
        let data = self
            .snapshot
            .activity
            .iter()
            .filter(|activity| {
                conditions.is_empty()
                    || activity.condition_id.as_ref().is_some_and(|condition| {
                        conditions
                            .binary_search(&condition.trim().to_ascii_lowercase())
                            .is_ok()
                    })
            })
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
        page(
            "/v2/activity",
            query_fingerprint,
            self.snapshot_fingerprint,
            query.limit,
            query.cursor.as_deref(),
            data,
        )
    }

    /// Returns the collateral balance in the preserved accounting-snapshot shape.
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

    fn validate_user(&self, user: &str) -> Result<&str, QueryError> {
        let user = Address::new(user).map_err(|_| QueryError::WalletMismatch)?;
        (user == self.snapshot.wallet)
            .then_some(self.snapshot.wallet.as_str())
            .ok_or(QueryError::WalletMismatch)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CursorKey {
    row: u64,
    occurrence: u64,
}

fn page<T: Debug>(
    route: &str,
    query_fingerprint: u64,
    snapshot_fingerprint: u64,
    limit: usize,
    cursor: Option<&str>,
    data: Vec<T>,
) -> Result<CursorPage<T>, QueryError> {
    let mut occurrences = HashMap::new();
    let keys = data
        .iter()
        .map(|row| {
            let row = stable_hash(format!("{row:?}").as_bytes());
            let occurrence = occurrences.entry(row).or_insert(0_u64);
            let key = CursorKey {
                row,
                occurrence: *occurrence,
            };
            *occurrence += 1;
            key
        })
        .collect::<Vec<_>>();
    let route_fingerprint = stable_hash(route.as_bytes());
    let (start, limit) = match cursor {
        None => (0, limit),
        Some(cursor) => {
            let (key, limit) = decode_cursor(
                cursor,
                route_fingerprint,
                query_fingerprint,
                snapshot_fingerprint,
            )?;
            let start = keys
                .iter()
                .position(|candidate| *candidate == key)
                .map(|index| index + 1)
                .ok_or(QueryError::InvalidCursor)?;
            (start, limit)
        }
    };
    if start > data.len() {
        return Err(QueryError::InvalidCursor);
    }
    let end = start.saturating_add(limit).min(data.len());
    let has_more = end < data.len();
    let next_cursor = has_more.then(|| {
        encode_cursor(
            route_fingerprint,
            query_fingerprint,
            snapshot_fingerprint,
            limit,
            keys[end - 1],
        )
    });
    let rows = data.into_iter().skip(start).take(end - start).collect();
    Ok(CursorPage {
        data: rows,
        pagination: Pagination {
            limit,
            offset: start,
            has_more,
            next_cursor,
        },
    })
}

fn encode_cursor(route: u64, query: u64, snapshot: u64, limit: usize, key: CursorKey) -> String {
    format!(
        "v2:{route:016x}:{query:016x}:{snapshot:016x}:{limit:04x}:{:016x}:{:016x}",
        key.row, key.occurrence
    )
}

fn decode_cursor(
    cursor: &str,
    route: u64,
    query: u64,
    snapshot: u64,
) -> Result<(CursorKey, usize), QueryError> {
    if cursor.len() != 92 {
        return Err(QueryError::InvalidCursor);
    }
    let mut parts = cursor.split(':');
    if parts.next() != Some("v2") {
        return Err(QueryError::InvalidCursor);
    }
    let parse = |part: Option<&str>| {
        part.and_then(|part| u64::from_str_radix(part, 16).ok())
            .ok_or(QueryError::InvalidCursor)
    };
    if parse(parts.next())? != route
        || parse(parts.next())? != query
        || parse(parts.next())? != snapshot
    {
        return Err(QueryError::InvalidCursor);
    }
    let limit = usize::try_from(parse(parts.next())?).map_err(|_| QueryError::InvalidCursor)?;
    if !(1..=1_000).contains(&limit) {
        return Err(QueryError::InvalidCursor);
    }
    let key = CursorKey {
        row: parse(parts.next())?,
        occurrence: parse(parts.next())?,
    };
    if parts.next().is_some() {
        return Err(QueryError::InvalidCursor);
    }
    Ok((key, limit))
}

fn normalized_conditions(conditions: &[String]) -> Result<Vec<String>, QueryError> {
    let normalized = conditions
        .iter()
        .map(|condition| condition.trim().to_ascii_lowercase())
        .collect::<std::collections::BTreeSet<_>>();
    if normalized.contains("") {
        return Err(QueryError::EmptyCondition);
    }
    if normalized.len() > MAX_CONDITIONS {
        return Err(QueryError::TooManyConditions {
            maximum: MAX_CONDITIONS,
        });
    }
    Ok(normalized.into_iter().collect())
}

fn validate_limit(limit: usize) -> Result<(), QueryError> {
    if !(1..=1_000).contains(&limit) {
        return Err(QueryError::InvalidLimit { maximum: 1_000 });
    }
    Ok(())
}

fn validate_event_id_filter(event_id: Option<&str>, route: &'static str) -> Result<(), QueryError> {
    let Some(event_id) = event_id else {
        return Ok(());
    };
    if event_id.trim().is_empty() {
        return Err(QueryError::EmptyEventId);
    }
    Err(QueryError::EventIdFilterNotReconstructible { route })
}

fn stable_hash(value: &[u8]) -> u64 {
    value.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    })
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

#[cfg(test)]
#[path = "api_tests.rs"]
mod tests;
