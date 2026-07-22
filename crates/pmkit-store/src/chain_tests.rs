use std::path::PathBuf;

use rust_decimal::Decimal;

use crate::{
    Address, CanonicalChainLog, CanonicalLogSegment, CanonicalLogStore, ChainCheckpoint,
    ChainEvent, ChainId, ContractRegistry, TursoTapeStore, WalletQuery,
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
                    asset_id: "yes".into(),
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
    let path = database_path("reorg");
    let registry = ContractRegistry::polygon();
    let store = TursoTapeStore::open_local(&path).await?;
    store
        .replace_canonical_segment(
            &registry,
            &fixture_segment(&registry, "0xblock2a", Decimal::from(100)),
        )
        .await?;

    // When: the source reports a common ancestor and replacement canonical block.
    store
        .replace_canonical_segment(
            &registry,
            &fixture_segment(&registry, "0xblock2b", Decimal::from(200)),
        )
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
    assert_eq!(snapshot.collateral_balance, Decimal::from(174));
    store.delete_database()?;
    drop(store);
    Ok(())
}
