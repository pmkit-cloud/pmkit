use pmkit_store::{Address, ChainCheckpoint, ChainId, WalletPosition, WalletSnapshot};
use rust_decimal::Decimal;

use crate::{ChainTruthApiV1, DataOrdersQuery, NotReconstructibleFromChain, PositionsQuery};

#[test]
fn clob_compatible_wallet_reads_match_fixtures() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a deterministic, chain-reconstructed wallet snapshot.
    let snapshot = WalletSnapshot {
        wallet: Address::new("0x00000000000000000000000000000000000000aa")?,
        canonical_tip: Some(ChainCheckpoint::new(ChainId::POLYGON, 1, "0xblock")),
        collateral_balance: Decimal::ZERO,
        positions: vec![WalletPosition {
            asset_id: "yes".into(),
            size: Decimal::ONE,
        }],
        settled_collateral: Decimal::ZERO,
        trades: Vec::new(),
        activity: Vec::new(),
    };
    let api = ChainTruthApiV1::from_snapshot(snapshot);

    // When: positions are requested with Data API offset semantics.
    let response = api.positions(&PositionsQuery::new(
        "0x00000000000000000000000000000000000000aa",
        100,
        0,
    )?);

    // Then: the response preserves only chain-provable CLOB-compatible fields.
    assert_eq!(response.version, "v1");
    assert_eq!(response.data.len(), 1);
    assert_eq!(
        response.data[0].proxy_wallet,
        "0x00000000000000000000000000000000000000aa"
    );
    Ok(())
}

#[test]
fn offchain_order_is_not_reconstructible_from_chain() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a chain-truth API without offchain CLOB order lifecycle data.
    let api = ChainTruthApiV1::from_snapshot(WalletSnapshot {
        wallet: Address::new("0x00000000000000000000000000000000000000aa")?,
        canonical_tip: None,
        collateral_balance: Decimal::ZERO,
        positions: Vec::new(),
        settled_collateral: Decimal::ZERO,
        trades: Vec::new(),
        activity: Vec::new(),
    });

    // When: CLOB orders are requested.
    let result = api.data_orders(&DataOrdersQuery::default());

    // Then: the API refuses fabrication with its typed result.
    assert_eq!(result, Err(NotReconstructibleFromChain::Orders));
    Ok(())
}
