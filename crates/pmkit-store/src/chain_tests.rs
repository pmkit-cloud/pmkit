#![allow(clippy::significant_drop_tightening)]
use std::path::PathBuf;

use rust_decimal::Decimal;

use crate::wallet::rebuild_wallet;
use crate::{
    Address, BlockHead, CanonicalChainLog, CanonicalLogSegment, CanonicalLogStore, ChainCheckpoint,
    ChainEvent, ChainId, ContractRegistry, FinalizedBlockCoverage, FinalizedBlockRange,
    FinalizedRawLogBatch, ProviderIdentity, QuorumVerifiedFinalizedLogBatch, RawLogIdentity,
    RawRpcLog, TradeSide, TursoTapeStore, WalletQuery, agree_on_finalized_log_batches,
    ingest_finalized_batch,
};

fn database_path(name: &str) -> Result<(tempfile::TempDir, PathBuf), std::io::Error> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join(format!("pmkit-chain-{name}.db"));
    Ok((dir, path))
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
    let (_dir, path) = database_path("reconstruction")?;
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
    assert!(!path.exists());

    Ok(())
}

#[tokio::test]
async fn reorg_replaces_orphaned_wallet_events() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a canonical segment whose block two will be orphaned.
    let (_dir, path) = database_path("reorg-verified")?;
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
    assert!(!path.exists());

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
    let (_dir, path) = database_path("validation")?;
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
    let (_dir, path) = database_path("bounded-tip")?;
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

#[tokio::test]
async fn finalized_raw_batch_is_decoded_and_ingested_transactionally()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: one finalized ERC-20 collateral transfer from a provider boundary.
    let (_dir, path) = database_path("raw-ingestion")?;
    let registry = ContractRegistry::polygon();
    let wallet = address("0x00000000000000000000000000000000000000aa");
    let provider = ProviderIdentity::new("fixture-rpc");
    let raw = RawRpcLog {
        identity: RawLogIdentity {
            provider: provider.clone(),
            chain_id: ChainId::POLYGON,
            block_number: 1,
            block_hash: "0xblock1".into(),
            transaction_hash: "0xtx1".into(),
            transaction_index: 0,
            log_index: 0,
        },
        contract_address: registry.collateral.clone(),
        topics: vec![
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a8df523b3ef".into(),
            "0x0000000000000000000000000000000000000000000000000000000000000001".into(),
            "0x00000000000000000000000000000000000000000000000000000000000000aa".into(),
        ],
        data: format!("0x{:064x}", 42),
    };
    let range = FinalizedBlockRange::new(ChainId::POLYGON, 1, 1)?;
    let coverage = FinalizedBlockCoverage::new(
        range.clone(),
        vec![BlockHead::new(ChainId::POLYGON, 1, "0xblock1", "0xgenesis")],
    )?;
    let batch = quorum_verified_batch(FinalizedRawLogBatch::new(
        provider,
        range,
        BlockHead::new(ChainId::POLYGON, 2, "0xhead", "0xblock1"),
        BlockHead::new(ChainId::POLYGON, 1, "0xblock1", "0xgenesis"),
        coverage,
        vec![raw],
    )?)?;
    let store = TursoTapeStore::open_local(&path).await?;

    // When: the validated batch is decoded and committed through the canonical store.
    ingest_finalized_batch(
        &store,
        &registry,
        &batch,
        ChainCheckpoint::new(ChainId::POLYGON, 0, "0xgenesis"),
    )
    .await?;
    let snapshot = store.wallet_snapshot(&WalletQuery::new(wallet)).await?;

    // Then: the durable wallet view reflects the decoded canonical event.
    assert_eq!(snapshot.collateral_balance, Decimal::from(42));
    assert_eq!(
        snapshot.canonical_tip,
        Some(ChainCheckpoint::new(ChainId::POLYGON, 1, "0xblock1"))
    );
    store.delete_database()?;
    Ok(())
}

fn raw_transfer(block_number: u64, block_hash: &str, amount: u64) -> RawRpcLog {
    RawRpcLog {
        identity: RawLogIdentity {
            provider: ProviderIdentity::new("fixture-rpc"),
            chain_id: ChainId::POLYGON,
            block_number,
            block_hash: block_hash.into(),
            transaction_hash: format!("0xtx{block_number}"),
            transaction_index: 0,
            log_index: 0,
        },
        contract_address: ContractRegistry::polygon().collateral,
        topics: vec![
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a8df523b3ef".into(),
            "0x0000000000000000000000000000000000000000000000000000000000000001".into(),
            "0x00000000000000000000000000000000000000000000000000000000000000aa".into(),
        ],
        data: format!("0x{amount:064x}"),
    }
}

fn quorum_verified_batch(
    batch: FinalizedRawLogBatch,
) -> Result<QuorumVerifiedFinalizedLogBatch, crate::ChainSourceError> {
    let mut corroborating = batch.clone();
    corroborating.provider = ProviderIdentity::new("corroborating-rpc");
    for log in &mut corroborating.logs {
        log.identity.provider = corroborating.provider.clone();
    }
    agree_on_finalized_log_batches(2, &[batch, corroborating])
}

fn finalized_batch(
    blocks: Vec<BlockHead>,
    finalized: BlockHead,
    logs: Vec<RawRpcLog>,
) -> Result<QuorumVerifiedFinalizedLogBatch, Box<dyn std::error::Error>> {
    let from_block = blocks
        .first()
        .ok_or("finalized batch fixture needs a first block")?
        .block_number;
    let to_block = blocks
        .last()
        .ok_or("finalized batch fixture needs a last block")?
        .block_number;
    let range = FinalizedBlockRange::new(ChainId::POLYGON, from_block, to_block)?;
    let coverage = FinalizedBlockCoverage::new(range.clone(), blocks)?;
    Ok(quorum_verified_batch(FinalizedRawLogBatch::new(
        ProviderIdentity::new("fixture-rpc"),
        range,
        finalized.clone(),
        finalized,
        coverage,
        logs,
    )?)?)
}

#[tokio::test]
async fn finality_progresses_across_restart() -> Result<(), Box<dyn std::error::Error>> {
    // Given: finalized block two persisted even though its batch has no block-two log.
    let (_dir, path) = database_path("finality-restart")?;
    let registry = ContractRegistry::polygon();
    let store = TursoTapeStore::open_local(&path).await?;
    let first = finalized_batch(
        vec![
            BlockHead::new(ChainId::POLYGON, 1, "0xblock1", "0xgenesis"),
            BlockHead::new(ChainId::POLYGON, 2, "0xblock2", "0xblock1"),
        ],
        BlockHead::new(ChainId::POLYGON, 2, "0xblock2", "0xblock1"),
        vec![raw_transfer(1, "0xblock1", 10)],
    )?;
    ingest_finalized_batch(
        &store,
        &registry,
        &first,
        ChainCheckpoint::new(ChainId::POLYGON, 0, "0xgenesis"),
    )
    .await?;
    drop(store);

    // When: the store reopens, holds an incompletely proven advance, then receives linked batches.
    let store = TursoTapeStore::open_local(&path).await?;
    assert_eq!(
        store.finalized_checkpoint(ChainId::POLYGON).await?,
        Some(ChainCheckpoint::new(ChainId::POLYGON, 2, "0xblock2"))
    );
    let held = finalized_batch(
        vec![BlockHead::new(ChainId::POLYGON, 3, "0xblock3", "0xblock2")],
        BlockHead::new(ChainId::POLYGON, 4, "0xblock4", "0xblock3"),
        vec![raw_transfer(3, "0xblock3", 20)],
    )?;
    ingest_finalized_batch(
        &store,
        &registry,
        &held,
        ChainCheckpoint::new(ChainId::POLYGON, 2, "0xblock2"),
    )
    .await?;
    assert_eq!(
        store.finalized_checkpoint(ChainId::POLYGON).await?,
        Some(ChainCheckpoint::new(ChainId::POLYGON, 2, "0xblock2"))
    );
    let held_snapshot = store
        .wallet_snapshot(&WalletQuery::new(address(
            "0x00000000000000000000000000000000000000aa",
        )))
        .await?;
    assert_eq!(held_snapshot.collateral_balance, Decimal::from(10));

    let second = finalized_batch(
        vec![
            BlockHead::new(ChainId::POLYGON, 3, "0xblock3", "0xblock2"),
            BlockHead::new(ChainId::POLYGON, 4, "0xblock4", "0xblock3"),
        ],
        BlockHead::new(ChainId::POLYGON, 4, "0xblock4", "0xblock3"),
        vec![raw_transfer(3, "0xblock3", 20)],
    )?;
    ingest_finalized_batch(
        &store,
        &registry,
        &second,
        ChainCheckpoint::new(ChainId::POLYGON, 2, "0xblock2"),
    )
    .await?;
    let third = finalized_batch(
        vec![BlockHead::new(ChainId::POLYGON, 5, "0xblock5", "0xblock4")],
        BlockHead::new(ChainId::POLYGON, 5, "0xblock5", "0xblock4"),
        vec![raw_transfer(5, "0xblock5", 30)],
    )?;
    ingest_finalized_batch(
        &store,
        &registry,
        &third,
        ChainCheckpoint::new(ChainId::POLYGON, 4, "0xblock4"),
    )
    .await?;

    // Then: progression resumes from the durable head and only linked finalized logs surface.
    assert_eq!(
        store.finalized_checkpoint(ChainId::POLYGON).await?,
        Some(ChainCheckpoint::new(ChainId::POLYGON, 5, "0xblock5"))
    );
    let snapshot = store
        .wallet_snapshot(&WalletQuery::new(address(
            "0x00000000000000000000000000000000000000aa",
        )))
        .await?;
    assert_eq!(snapshot.collateral_balance, Decimal::from(60));
    store.delete_database()?;
    Ok(())
}

#[tokio::test]
async fn finality_regression_rejected() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a store whose durable finalized head is block two.
    let (_dir, path) = database_path("finality-regression")?;
    let registry = ContractRegistry::polygon();
    let store = TursoTapeStore::open_local(&path).await?;
    let first = finalized_batch(
        vec![
            BlockHead::new(ChainId::POLYGON, 1, "0xblock1", "0xgenesis"),
            BlockHead::new(ChainId::POLYGON, 2, "0xblock2", "0xblock1"),
        ],
        BlockHead::new(ChainId::POLYGON, 2, "0xblock2", "0xblock1"),
        vec![raw_transfer(1, "0xblock1", 10)],
    )?;
    ingest_finalized_batch(
        &store,
        &registry,
        &first,
        ChainCheckpoint::new(ChainId::POLYGON, 0, "0xgenesis"),
    )
    .await?;
    let regressed = finalized_batch(
        vec![BlockHead::new(ChainId::POLYGON, 1, "0xblock1", "0xgenesis")],
        BlockHead::new(ChainId::POLYGON, 1, "0xblock1", "0xgenesis"),
        Vec::new(),
    )?;

    // When: a provider attempts to move finality backward to block one.
    let result = ingest_finalized_batch(
        &store,
        &registry,
        &regressed,
        ChainCheckpoint::new(ChainId::POLYGON, 1, "0xblock1"),
    )
    .await;

    // Then: the typed error rejects the move and preserves the durable checkpoint.
    assert!(matches!(
        result,
        Err(crate::StoreError::FinalizedHeadRegression {
            chain_id: 137,
            persisted_block_number: 2,
            proposed_block_number: 1,
        })
    ));
    assert_eq!(
        store.finalized_checkpoint(ChainId::POLYGON).await?,
        Some(ChainCheckpoint::new(ChainId::POLYGON, 2, "0xblock2"))
    );
    store.delete_database()?;
    Ok(())
}
