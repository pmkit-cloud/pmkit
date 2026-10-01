use pmkit_store::{Address, ChainCheckpoint, ChainId, WalletPosition, WalletSnapshot};
use rust_decimal::Decimal;

use crate::{
    ChainTruthApiV2, DataOrdersQuery, NotReconstructibleFromChain, PositionStatus, PositionsQuery,
    QueryError,
};

#[test]
fn v2_positions_use_cursor_pages_and_keep_accounting_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let api = ChainTruthApiV2::from_snapshot(WalletSnapshot {
        wallet: Address::new("0x00000000000000000000000000000000000000aa")?,
        canonical_tip: Some(ChainCheckpoint::new(ChainId::POLYGON, 1, "0xblock")),
        collateral_balance: Decimal::ZERO,
        positions: vec![
            WalletPosition {
                asset_id: "no".into(),
                size: Decimal::ONE,
            },
            WalletPosition {
                asset_id: "yes".into(),
                size: Decimal::ONE,
            },
        ],
        settled_collateral: Decimal::ZERO,
        trades: Vec::new(),
        activity: Vec::new(),
    });
    let query = PositionsQuery::new("0x00000000000000000000000000000000000000aa", 1)?;

    let first = api.positions(&query)?;
    assert_eq!(first.data.len(), 1);
    assert_eq!(first.data[0].token_id, "no");
    assert_eq!(first.data[0].status, PositionStatus::Open);
    assert_eq!(first.data[0].current_size, Decimal::ONE);
    assert_eq!(first.data[0].condition_id, None);
    assert_eq!(first.data[0].redeemable, None);
    assert!(first.pagination.has_more);
    assert_eq!(first.pagination.offset, 0);
    let cursor = first.pagination.next_cursor.ok_or("missing next cursor")?;

    let mut next_query = query.clone();
    next_query.cursor = Some(cursor);
    let second = api.positions(&next_query)?;
    assert_eq!(second.data.len(), 1);
    assert_eq!(second.data[0].token_id, "yes");
    assert_eq!(second.pagination.offset, 1);
    assert!(!second.pagination.has_more);
    assert_eq!(second.pagination.next_cursor, None);
    assert_eq!(api.balance().balance, Decimal::ZERO);

    let mut mismatched = query;
    mismatched.user = "0x00000000000000000000000000000000000000bb".into();
    assert_eq!(api.positions(&mismatched), Err(QueryError::WalletMismatch));
    Ok(())
}

#[test]
fn unsupported_position_status_is_not_guessed() -> Result<(), Box<dyn std::error::Error>> {
    let api = ChainTruthApiV2::from_snapshot(WalletSnapshot {
        wallet: Address::new("0x00000000000000000000000000000000000000aa")?,
        canonical_tip: None,
        collateral_balance: Decimal::ZERO,
        positions: Vec::new(),
        settled_collateral: Decimal::ZERO,
        trades: Vec::new(),
        activity: Vec::new(),
    });
    let mut query = PositionsQuery::new("0x00000000000000000000000000000000000000aa", 100)?;
    query.status = Some(PositionStatus::Mergeable);
    assert_eq!(
        api.positions(&query),
        Err(QueryError::PositionLifecycleNotReconstructible)
    );
    Ok(())
}

#[test]
fn offchain_order_is_not_reconstructible_from_chain() -> Result<(), Box<dyn std::error::Error>> {
    let api = ChainTruthApiV2::from_snapshot(WalletSnapshot {
        wallet: Address::new("0x00000000000000000000000000000000000000aa")?,
        canonical_tip: None,
        collateral_balance: Decimal::ZERO,
        positions: Vec::new(),
        settled_collateral: Decimal::ZERO,
        trades: Vec::new(),
        activity: Vec::new(),
    });
    assert_eq!(
        api.data_orders(&DataOrdersQuery::default()),
        Err(NotReconstructibleFromChain::Orders)
    );
    Ok(())
}
