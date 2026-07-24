#![expect(
    clippy::expect_used,
    reason = "typed fixture construction should fail loudly when malformed"
)]

use super::{
    BlockHead, ChainSourceError, FinalizedBlockCoverage, FinalizedBlockRange,
    FinalizedProviderHead, FinalizedRawLogBatch, ProviderIdentity, RawLogIdentity, RawRpcLog,
    agree_on_finalized_heads,
};
use crate::{Address, ChainId};

fn provider() -> ProviderIdentity {
    ProviderIdentity::new("fixture-rpc")
}

fn address() -> Address {
    Address::new("0x0000000000000000000000000000000000000001").expect("fixture address is valid")
}

#[test]
fn finalized_batch_preserves_provider_log_identity_and_rejects_duplicates() {
    // Given: a finalized Polygon range and one raw log from one provider.
    let provider = provider();
    let range =
        FinalizedBlockRange::new(ChainId::POLYGON, 10, 12).expect("fixture range is ordered");
    let log = RawRpcLog {
        identity: RawLogIdentity {
            provider: provider.clone(),
            chain_id: ChainId::POLYGON,
            block_number: 11,
            block_hash: "0xblock11".into(),
            transaction_hash: "0xtx".into(),
            transaction_index: 2,
            log_index: 3,
        },
        contract_address: address(),
        topics: vec!["0xtopic".into()],
        data: "0xdata".into(),
    };

    // When: the raw provider response crosses the validation boundary.
    let batch = FinalizedRawLogBatch::new(
        provider.clone(),
        range,
        BlockHead::new(ChainId::POLYGON, 20, "0xhead"),
        BlockHead::new(ChainId::POLYGON, 12, "0xf12"),
        vec![log.clone()],
    )
    .expect("finalized response is valid");

    // Then: provider identity is lossless and duplicate logs fail closed.
    assert_eq!(batch.provider, provider);
    assert_eq!(batch.logs, vec![log]);
    let duplicate = FinalizedRawLogBatch::new(
        batch.provider.clone(),
        batch.range.clone(),
        batch.head.clone(),
        batch.finalized.clone(),
        vec![batch.logs[0].clone(), batch.logs[0].clone()],
    );
    assert!(matches!(duplicate, Err(ChainSourceError::DuplicateRawLog)));
}

#[test]
fn finalized_batch_rejects_unfinalized_and_reversed_ranges() {
    // Given: a range ending after the provider's finalized height.
    let provider = provider();

    // When: the provider response claims finality only through block eleven.
    let result = FinalizedRawLogBatch::new(
        provider,
        FinalizedBlockRange::new(ChainId::POLYGON, 10, 12).expect("fixture range is ordered"),
        BlockHead::new(ChainId::POLYGON, 20, "0xhead"),
        BlockHead::new(ChainId::POLYGON, 11, "0xf11"),
        Vec::new(),
    );

    // Then: the boundary rejects both invalid lifecycle states.
    assert!(matches!(
        result,
        Err(ChainSourceError::FinalityViolation { .. })
    ));
    assert!(matches!(
        FinalizedBlockRange::new(ChainId::POLYGON, 12, 10),
        Err(ChainSourceError::InvalidRange { .. })
    ));
}

#[test]
fn finality_evidence_rejects_disagreement_and_gaps() {
    // Given: two providers disagree on finality and a range misses block eleven.
    let left = FinalizedProviderHead {
        provider: ProviderIdentity::new("left"),
        head: BlockHead::new(ChainId::POLYGON, 12, "0xhead"),
        finalized: BlockHead::new(ChainId::POLYGON, 11, "0xf11"),
    };
    let right = FinalizedProviderHead {
        provider: ProviderIdentity::new("right"),
        head: BlockHead::new(ChainId::POLYGON, 12, "0xhead"),
        finalized: BlockHead::new(ChainId::POLYGON, 10, "0xf10"),
    };

    // When: finality evidence is compared and coverage is validated.
    let disagreement = agree_on_finalized_heads(left, &right);
    let missing = FinalizedBlockCoverage::new(
        FinalizedBlockRange::new(ChainId::POLYGON, 10, 12).expect("fixture range is ordered"),
        vec![
            BlockHead::new(ChainId::POLYGON, 10, "0xf10"),
            BlockHead::new(ChainId::POLYGON, 12, "0xf12"),
        ],
    );

    // Then: disagreement and missing block coverage fail closed.
    assert!(matches!(
        disagreement,
        Err(ChainSourceError::ProviderDisagreement { .. })
    ));
    assert!(matches!(
        missing,
        Err(ChainSourceError::MissingBlockCoverage { .. })
    ));
}
