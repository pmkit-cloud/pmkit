use std::path::PathBuf;

use rust_decimal::Decimal;

use crate::wallet::rebuild_wallet;
use crate::{
    Address, CanonicalChainLog, CanonicalLogSegment, CanonicalLogStore, ChainCheckpoint,
    ChainEvent, ChainId, ContractRegistry, TradeSide, TursoTapeStore, WalletQuery,
};

fn database_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("pmkit-chain-{name}.db"))
}

#[expect(
    clippy::expect_used,
    reason = "invalid literal means the typed fixture is malformed"
)]
fn address(value: &str) -> Address {
    Address::new(value).expect("fixture address")
}

#[expect(
    clippy::too_many_lines,
    reason = "one complete typed canonical chain fixture is easiest to audit"
)]
fn fixture_segment(
    registry: &ContractRegistry,
    block_hash: &str,
    opening_collateral: Decimal,
) -> CanonicalLogSegment {
    let wallet = address("0x00000000000000000000000000000000000000aa");
    let ctf = registry.conditional_tokens.clone();
    let collateral = registry.collateral.clone();
    let exchange = registry.ctf_exchange.clone();
    CanonicalLogSegment::new(
        ChainCheckpoint::new(ChainId::POLYGON, 0, "0xgenesis"),
        vec![
            CanonicalChainLog::fixture(
                1,
                "0xblock1",
                0,
                0,
                collateral.clone(),
                ChainEvent::CollateralTransfer {
                    from: address("0x0000000000000000000000000000000000000001"),
                    to: wallet.clone(),
                    amount: opening_collateral,
                },
            ),
            CanonicalChainLog::fixture(
                2,
                block_hash,
                0,
                0,
                ctf.clone(),
                ChainEvent::PositionSplit {
                    stakeholder: wallet.clone(),
                    condition_id: "0xcondition".into(),
                    amount: Decimal::from(50),
                },
            ),
            CanonicalChainLog::fixture(
                2,
                block_hash,
                0,
                1,
                collateral.clone(),
                ChainEvent::CollateralTransfer {
                    from: wallet.clone(),
                    to: ctf.clone(),
                    amount: Decimal::from(50),
                },
            ),
            CanonicalChainLog::fixture(
                2,
                block_hash,
                0,
                2,
                ctf.clone(),
                ChainEvent::OutcomeTransferSingle {
                    from: ctf.clone(),
                    to: wallet.clone(),
                    asset_id: "yes".into(),
                    amount: Decimal::from(50),
                },
            ),
            CanonicalChainLog::fixture(
                3,
                "0xblock3",
                0,
                0,
                exchange.clone(),
                ChainEvent::OrderFilled {
                    maker: wallet.clone(),
                    taker: address("0x0000000000000000000000000000000000000002"),
                    maker_asset_id: "yes".into(),
                    taker_asset_id: "USDC".into(),
                    maker_side: TradeSide::Sell,
                    maker_amount: Decimal::from(10),
                    taker_amount: Decimal::from(4),
                    fee: Decimal::new(1, 1),
                },
            ),
            CanonicalChainLog::fixture(
                3,
                "0xblock3",
                0,
                1,
                ctf.clone(),
                ChainEvent::OutcomeTransferSingle {
                    from: wallet.clone(),
                    to: exchange.clone(),
                    asset_id: "yes".into(),
                    amount: Decimal::from(10),
                },
            ),
            CanonicalChainLog::fixture(
                3,
                "0xblock3",
                0,
                2,
                collateral.clone(),
                ChainEvent::CollateralTransfer {
                    from: exchange,
                    to: wallet.clone(),
                    amount: Decimal::from(4),
                },
            ),
            CanonicalChainLog::fixture(
                4,
                "0xblock4",
                0,
                0,
                ctf.clone(),
                ChainEvent::PayoutRedemption {
                    redeemer: wallet.clone(),
                    condition_id: "0xcondition".into(),
                    payout: Decimal::from(20),
                },
            ),
            CanonicalChainLog::fixture(
                4,
                "0xblock4",
                0,
                1,
                ctf,
                ChainEvent::OutcomeTransferSingle {
                    from: wallet.clone(),
                    to: address("0x0000000000000000000000000000000000000003"),
                    asset_id: "yes".into(),
                    amount: Decimal::from(40),
                },
            ),
            CanonicalChainLog::fixture(
                4,
                "0xblock4",
                0,
                2,
                collateral,
                ChainEvent::CollateralTransfer {
                    from: address("0x0000000000000000000000000000000000000003"),
                    to: wallet,
                    amount: Decimal::from(20),
                },
            ),
        ],
    )
}

#[tokio::test]
async fn wallet_reconstruction_matches_canonical_logs() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a typed Polygon fixture chain and a file-backed canonical store.
    let path = database_path("reconstruction");
    let registry = ContractRegistry::polygon();
    let store = TursoTapeStore::open_local(&path).await?;
    let segment = fixture_segment(&registry, "0xblock2a", Decimal::from(100));

    // When: the fixture is persisted, reopened, and rebuilt for its wallet.
    store.replace_canonical_segment(&registry, &segment).await?;
    drop(store);
    let store = TursoTapeStore::open_local(&path).await?;
    let snapshot = store
        .wallet_snapshot(&WalletQuery::new(address(
            "0x00000000000000000000000000000000000000aa",
        )))
        .await?;

    // Then: restart reconstruction preserves canonical balances, settlement, and activity.
    assert_eq!(snapshot.collateral_balance, Decimal::from(74));
    assert!(snapshot.positions.is_empty());
    assert_eq!(snapshot.settled_collateral, Decimal::from(20));
    assert_eq!(snapshot.trades.len(), 1);
    assert_eq!(snapshot.activity.len(), 3);
    store.delete_database()?;
    drop(store);
    Ok(())
}

#[tokio::test]
async fn reorg_replaces_orphaned_wallet_events() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a canonical segment whose block two will be orphaned.
    let path = database_path("reorg-verified");
    let registry = ContractRegistry::polygon();
    let store = TursoTapeStore::open_local(&path).await?;
    store
        .replace_canonical_segment(
            &registry,
            &fixture_segment(&registry, "0xblock2a", Decimal::from(100)),
        )
        .await?;

    // When: the source reports a common ancestor and replacement canonical block.
    let mut replacement = fixture_segment(&registry, "0xblock2b", Decimal::from(200));
    replacement.common_ancestor = ChainCheckpoint::new(ChainId::POLYGON, 1, "0xblock1");
    replacement.logs.remove(0);
    store
        .replace_canonical_segment(&registry, &replacement)
        .await?;
    let snapshot = store
        .wallet_snapshot(&WalletQuery::new(address(
            "0x00000000000000000000000000000000000000aa",
        )))
        .await?;

    // Then: orphaned records never contribute to the rebuilt wallet state.
    assert_eq!(
        snapshot.canonical_tip.ok_or("canonical tip")?.block_hash,
        "0xblock4"
    );
    assert_eq!(snapshot.collateral_balance, Decimal::from(74));
    store.delete_database()?;
    drop(store);
    Ok(())
}

#[test]
fn order_fills_preserve_maker_buy_and_sell_token_semantics() {
    // Given: one wallet makes both a collateral-for-token buy and a token-for-collateral sell.
    let registry = ContractRegistry::polygon();
    let wallet = address("0x00000000000000000000000000000000000000aa");
    let taker = address("0x0000000000000000000000000000000000000002");
    let logs = vec![
        CanonicalChainLog::fixture(
            1,
            "0xblock1",
            0,
            0,
            registry.ctf_exchange.clone(),
            ChainEvent::OrderFilled {
                maker: wallet.clone(),
                taker: taker.clone(),
                maker_asset_id: "USDC".into(),
                taker_asset_id: "yes".into(),
                maker_side: TradeSide::Buy,
                maker_amount: Decimal::from(4),
                taker_amount: Decimal::from(10),
                fee: Decimal::ZERO,
            },
        ),
        CanonicalChainLog::fixture(
            2,
            "0xblock2",
            0,
            0,
            registry.ctf_exchange,
            ChainEvent::OrderFilled {
                maker: wallet.clone(),
                taker,
                maker_asset_id: "no".into(),
                taker_asset_id: "USDC".into(),
                maker_side: TradeSide::Sell,
                maker_amount: Decimal::from(7),
                taker_amount: Decimal::from(3),
                fee: Decimal::ZERO,
            },
        ),
    ];

    // When: canonical logs are reduced for the maker wallet.
    let snapshot = rebuild_wallet(&WalletQuery::new(wallet), &logs);

    // Then: size always expresses outcome tokens and direction follows the maker asset flow.
    assert_eq!(snapshot.trades[0].asset_id, "yes");
    assert_eq!(snapshot.trades[0].side, TradeSide::Buy);
    assert_eq!(snapshot.trades[0].size, Decimal::from(10));
    assert_eq!(snapshot.trades[1].asset_id, "no");
    assert_eq!(snapshot.trades[1].side, TradeSide::Sell);
    assert_eq!(snapshot.trades[1].size, Decimal::from(7));
}

#[tokio::test]
async fn canonical_segments_reject_descending_and_unknown_ancestors()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: a stored canonical segment and replacement data with invalid source evidence.
    let path = database_path("validation");
    let registry = ContractRegistry::polygon();
    let store = TursoTapeStore::open_local(&path).await?;
    store
        .replace_canonical_segment(
            &registry,
            &fixture_segment(&registry, "0xblock2a", Decimal::from(100)),
        )
        .await?;
    let mut descending = fixture_segment(&registry, "0xblock2b", Decimal::from(100));
    descending.logs.reverse();
    let mut competing = fixture_segment(&registry, "0xblock2b", Decimal::from(100));
    competing.logs[2].identity.block_hash = "0xcompeting-block2".into();

    // When: the source supplies descending logs or a non-canonical ancestor.
    let descending_result = store
        .replace_canonical_segment(&registry, &descending)
        .await;
    let competing_result = store.replace_canonical_segment(&registry, &competing).await;
    let unknown = CanonicalLogSegment::new(
        ChainCheckpoint::new(ChainId::POLYGON, 2, "0xnot-canonical"),
        Vec::new(),
    );
    let unknown_result = store.replace_canonical_segment(&registry, &unknown).await;

    // Then: neither source can alter the canonical evidence.
    assert!(matches!(
        descending_result,
        Err(crate::StoreError::InvalidCanonicalSegment)
    ));
    assert!(matches!(
        competing_result,
        Err(crate::StoreError::InvalidCanonicalSegment)
    ));
    assert!(matches!(
        unknown_result,
        Err(crate::StoreError::InvalidCanonicalSegment)
    ));
    store.delete_database()?;
    Ok(())
}

#[tokio::test]
async fn bounded_wallet_snapshot_reports_its_own_canonical_tip()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: canonical evidence extending past the requested block range.
    let path = database_path("bounded-tip");
    let registry = ContractRegistry::polygon();
    let store = TursoTapeStore::open_local(&path).await?;
    store
        .replace_canonical_segment(
            &registry,
            &fixture_segment(&registry, "0xblock2a", Decimal::from(100)),
        )
        .await?;

    // When: reconstructing only block two.
    let snapshot = store
        .wallet_snapshot(&WalletQuery {
            wallet: address("0x00000000000000000000000000000000000000aa"),
            from_block: Some(2),
            to_block: Some(2),
        })
        .await?;

    // Then: the range-local tip is not overwritten by the global checkpoint.
    assert_eq!(
        snapshot.canonical_tip,
        Some(ChainCheckpoint::new(ChainId::POLYGON, 2, "0xblock2a"))
    );
    store.delete_database()?;
    Ok(())
}
