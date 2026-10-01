use pmkit_store::{
    Address, WalletActivity, WalletActivityKind, WalletPosition, WalletSnapshot, WalletTrade,
};
use rust_decimal::Decimal;

use crate::{
    ActivityQuery, ChainTruthApiV2, DataResponse, PositionStatus, PositionsQuery, QueryError,
    TradesQuery,
};

const USER: &str = "0x00000000000000000000000000000000000000aa";

#[test]
fn nonzero_collateral_balance_keeps_accounting_shape_and_precision()
-> Result<(), Box<dyn std::error::Error>> {
    let collateral_balance = Decimal::new(123_456_789, 6);
    let mut wallet_snapshot = snapshot(Vec::new(), Vec::new(), Vec::new())?;
    wallet_snapshot.collateral_balance = collateral_balance;
    assert_eq!(wallet_snapshot.collateral_balance, collateral_balance);

    let api = ChainTruthApiV2::from_snapshot(wallet_snapshot);
    let balance = api.balance();
    assert_eq!(balance.asset, "USDC");
    assert_eq!(balance.balance, collateral_balance);
    let expected: serde_json::Value =
        serde_json::from_str(r#"{"asset":"USDC","balance":"123.456789"}"#)?;
    assert_eq!(serde_json::to_value(balance)?, expected);
    Ok(())
}

#[test]
fn closed_position_filter_is_explicitly_unsupported() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        serde_json::to_value(PositionStatus::Closed)?,
        serde_json::json!("CLOSED")
    );

    let api = ChainTruthApiV2::from_snapshot(snapshot(
        Vec::new(),
        Vec::new(),
        vec![WalletPosition {
            asset_id: "token-1".into(),
            size: Decimal::ONE,
        }],
    )?);
    let mut query = PositionsQuery::new(USER, 100)?;
    query.status = Some(PositionStatus::Closed);
    assert_eq!(
        api.positions(&query),
        Err(QueryError::PositionLifecycleNotReconstructible)
    );
    Ok(())
}

#[test]
fn cursor_pages_are_repeatable_keyset_bound_and_exhaust_cleanly()
-> Result<(), Box<dyn std::error::Error>> {
    let trades: Vec<_> = (0_u64..5).map(trade).collect();
    let activity = vec![
        activity(0, "condition-a"),
        activity(1, "condition-a"),
        activity(2, "condition-b"),
    ];
    let api = ChainTruthApiV2::from_snapshot(snapshot(trades.clone(), activity, Vec::new())?);
    let query = TradesQuery::new(USER, 2)?;
    let first = api.trades(&query)?;
    assert_eq!(first, api.trades(&query)?);
    assert_eq!(first.data.len(), 2);
    assert_eq!(first.pagination.limit, 2);
    assert_eq!(first.pagination.offset, 0);
    assert!(first.pagination.has_more);
    let cursor = first.pagination.next_cursor.ok_or("missing next cursor")?;

    let mut next_query = query;
    next_query.cursor = Some(cursor.clone());
    next_query.limit = 100;
    let last = api.trades(&next_query)?;
    assert_eq!(last.data.len(), 2);
    assert_eq!(last.pagination.limit, 2);
    assert_eq!(last.pagination.offset, 2);
    assert!(last.pagination.has_more);

    for invalid_limit_cursor in [
        cursor.replace(":0002:", ":0000:"),
        cursor.replace(":0002:", ":03e9:"),
    ] {
        let mut invalid_limit = next_query.clone();
        invalid_limit.cursor = Some(invalid_limit_cursor);
        assert_eq!(api.trades(&invalid_limit), Err(QueryError::InvalidCursor));
    }

    let mut continued_query = next_query.clone();
    continued_query.cursor = last.pagination.next_cursor;
    let final_page = api.trades(&continued_query)?;
    assert_eq!(final_page.data.len(), 1);
    assert_eq!(final_page.pagination.limit, 2);
    assert_eq!(final_page.pagination.offset, 4);
    assert!(!final_page.pagination.has_more);

    let mut changed_trades = trades;
    changed_trades.push(trade(5));
    let changed = ChainTruthApiV2::from_snapshot(snapshot(changed_trades, Vec::new(), Vec::new())?);
    assert_eq!(changed.trades(&next_query), Err(QueryError::InvalidCursor));

    let mut wrong_route = ActivityQuery::new(USER, 2)?;
    wrong_route.cursor = Some(cursor);
    assert_eq!(api.activity(&wrong_route), Err(QueryError::InvalidCursor));

    let mut filtered = ActivityQuery::new(USER, 1)?;
    filtered.condition = vec!["condition-a".into()];
    let filtered_page = api.activity(&filtered)?;
    filtered.condition = vec!["condition-b".into()];
    filtered.cursor = filtered_page.pagination.next_cursor;
    assert_eq!(api.activity(&filtered), Err(QueryError::InvalidCursor));
    Ok(())
}

#[test]
fn empty_paginated_miss_uses_empty_data_and_exhausted_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let api = ChainTruthApiV2::from_snapshot(snapshot(Vec::new(), Vec::new(), Vec::new())?);
    let page = api.trades(&TradesQuery::new(USER, 100)?)?;
    assert!(page.data.is_empty());
    assert_eq!(page.pagination.limit, 100);
    assert_eq!(page.pagination.offset, 0);
    assert!(!page.pagination.has_more);
    assert_eq!(page.pagination.next_cursor, None);
    assert_eq!(
        serde_json::to_value(&page)?,
        serde_json::json!({
            "data": [],
            "pagination": {
                "limit": 100,
                "offset": 0,
                "has_more": false,
                "next_cursor": null,
            },
        })
    );
    Ok(())
}

#[test]
fn detail_miss_serializes_as_a_v2_null_data_envelope() -> Result<(), Box<dyn std::error::Error>> {
    let miss: DataResponse<Option<crate::ChainTruthPosition>> = DataResponse { data: None };
    assert_eq!(
        serde_json::to_value(miss)?,
        serde_json::json!({ "data": null })
    );
    Ok(())
}

#[test]
fn sparse_position_page_matches_the_served_v2_fixture() -> Result<(), Box<dyn std::error::Error>> {
    let api = ChainTruthApiV2::from_snapshot(snapshot(
        Vec::new(),
        Vec::new(),
        vec![WalletPosition {
            asset_id: "token-1".into(),
            size: Decimal::ONE,
        }],
    )?);
    let page = api.positions(&PositionsQuery::new(USER, 100)?)?;
    let actual = serde_json::to_value(page)?;
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/chain_truth_position_page.json"))?;
    assert_eq!(actual, expected);
    Ok(())
}

#[test]
fn condition_limit_counts_distinct_ids_and_accepts_duplicates()
-> Result<(), Box<dyn std::error::Error>> {
    let api = ChainTruthApiV2::from_snapshot(snapshot(
        Vec::new(),
        vec![WalletActivity {
            transaction_hash: "0xtx".into(),
            block_number: 1,
            kind: WalletActivityKind::Split,
            condition_id: Some("condition-a".into()),
            asset_id: None,
            amount: Decimal::ONE,
        }],
        Vec::new(),
    )?);

    let mut too_many = ActivityQuery::new(USER, 100)?;
    too_many.condition = (0..21).map(|index| format!("condition-{index}")).collect();
    assert_eq!(
        api.activity(&too_many),
        Err(QueryError::TooManyConditions { maximum: 20 })
    );

    let mut duplicates = ActivityQuery::new(USER, 100)?;
    duplicates.condition = vec!["condition-a".into(); 21];
    let result = api.activity(&duplicates)?;
    assert_eq!(result.data.len(), 1);
    assert_eq!(result.data[0].condition_id.as_deref(), Some("condition-a"));
    Ok(())
}

fn snapshot(
    trades: Vec<WalletTrade>,
    activity: Vec<WalletActivity>,
    positions: Vec<WalletPosition>,
) -> Result<WalletSnapshot, pmkit_store::AddressError> {
    Ok(WalletSnapshot {
        wallet: Address::new(USER)?,
        canonical_tip: None,
        collateral_balance: Decimal::ZERO,
        positions,
        settled_collateral: Decimal::ZERO,
        trades,
        activity,
    })
}

fn activity(number: u64, condition: &str) -> WalletActivity {
    WalletActivity {
        transaction_hash: format!("0x{number:064x}"),
        block_number: number,
        kind: WalletActivityKind::Split,
        condition_id: Some(condition.into()),
        asset_id: None,
        amount: Decimal::ONE,
    }
}

fn trade(number: u64) -> WalletTrade {
    WalletTrade {
        transaction_hash: format!("0x{number:064x}"),
        block_number: number,
        asset_id: format!("token-{number}"),
        maker: true,
        side: pmkit_store::TradeSide::Buy,
        size: Decimal::ONE,
        counter_amount: Decimal::ONE,
        fee: Decimal::ZERO,
    }
}
