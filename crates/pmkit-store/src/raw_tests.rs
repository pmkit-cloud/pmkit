// allow: SIZE_OK — raw boundary tests stay co-located with their shared fixtures.
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

fn block(block_number: u64, block_hash: &str, parent_hash: &str) -> BlockHead {
    BlockHead::new(ChainId::POLYGON, block_number, block_hash, parent_hash)
}

fn provider_head(
    provider: &str,
    finalized_block_number: u64,
    finalized_block_hash: &str,
) -> FinalizedProviderHead {
    FinalizedProviderHead {
        provider: ProviderIdentity::new(provider),
        head: block(20, "0xhead", "0xhead-parent"),
        finalized: block(
            finalized_block_number,
            finalized_block_hash,
            "0xfinalized-parent",
        ),
    }
}

fn provider_batch(provider: &str, data: &str) -> FinalizedRawLogBatch {
    let provider = ProviderIdentity::new(provider);
    let range = FinalizedBlockRange {
        chain_id: ChainId::POLYGON,
        from_block: 11,
        to_block: 11,
    };
    FinalizedRawLogBatch {
        provider: provider.clone(),
        range: range.clone(),
        head: block(20, "0xhead", "0xhead-parent"),
        finalized: block(11, "0xf11", "0xfinalized-parent"),
        coverage: FinalizedBlockCoverage {
            range,
            blocks: vec![block(11, "0xf11", "0xfinalized-parent")],
        },
        logs: vec![RawRpcLog {
            identity: RawLogIdentity {
                provider,
                chain_id: ChainId::POLYGON,
                block_number: 11,
                block_hash: "0xf11".into(),
                transaction_hash: "0xtx".into(),
                transaction_index: 0,
                log_index: 0,
            },
            contract_address: address(),
            topics: vec!["0xtopic".into()],
            data: data.into(),
        }],
    }
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
        range.clone(),
        block(20, "0xhead", "0xf19"),
        block(12, "0xf12", "0xf11"),
        FinalizedBlockCoverage::new(
            range,
            vec![
                block(10, "0xf10", "0xf9"),
                block(11, "0xf11", "0xf10"),
                block(12, "0xf12", "0xf11"),
            ],
        )
        .expect("fixture coverage is complete"),
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
        batch.coverage.clone(),
        vec![batch.logs[0].clone(), batch.logs[0].clone()],
    );
    assert!(matches!(duplicate, Err(ChainSourceError::DuplicateRawLog)));
}

#[test]
fn finalized_batch_rejects_unfinalized_and_reversed_ranges() {
    // Given: a range ending after the provider's finalized height.
    let provider = provider();
    let range =
        FinalizedBlockRange::new(ChainId::POLYGON, 10, 12).expect("fixture range is ordered");
    let coverage = FinalizedBlockCoverage::new(
        range.clone(),
        vec![
            block(10, "0xf10", "0xf9"),
            block(11, "0xf11", "0xf10"),
            block(12, "0xf12", "0xf11"),
        ],
    )
    .expect("fixture coverage is complete");

    // When: the provider response claims finality only through block eleven.
    let result = FinalizedRawLogBatch::new(
        provider,
        range,
        block(20, "0xhead", "0xf19"),
        block(11, "0xf11", "0xf10"),
        coverage,
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
        head: block(12, "0xhead", "0xf11"),
        finalized: block(11, "0xf11", "0xf10"),
    };
    let right = FinalizedProviderHead {
        provider: ProviderIdentity::new("right"),
        head: block(12, "0xhead", "0xf11"),
        finalized: block(10, "0xf10", "0xf9"),
    };

    // When: finality evidence is compared and coverage is validated.
    let disagreement = agree_on_finalized_heads(2, &[left, right]);
    let missing = FinalizedBlockCoverage::new(
        FinalizedBlockRange::new(ChainId::POLYGON, 10, 12).expect("fixture range is ordered"),
        vec![block(10, "0xf10", "0xf9"), block(12, "0xf12", "0xf11")],
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

#[test]
fn quorum_agreement_proceeds() {
    // Given: two available providers from a three-provider configuration agree.
    let observations = [
        provider_head("first", 11, "0xf11"),
        provider_head("second", 11, "0xf11"),
    ];

    // When: the unavailable third provider is handled by strict-majority failover.
    let result = agree_on_finalized_heads(3, &observations);

    // Then: the corroborated finalized height and hash proceed.
    assert!(matches!(
        result,
        Ok(head) if head == block(11, "0xf11", "0xfinalized-parent")
    ));
}

#[test]
fn subquorum_fails_closed() {
    // Given: only two of four configured providers agree and one observation diverges.
    let observations = [
        provider_head("first", 11, "0xf11"),
        provider_head("second", 11, "0xf11"),
        provider_head("divergent", 11, "0xother"),
    ];

    // When: corroboration is checked against the configured strict majority.
    let result = agree_on_finalized_heads(4, &observations);

    // Then: the source fails closed instead of trusting the largest sub-quorum.
    assert!(matches!(
        result,
        Err(ChainSourceError::ProviderQuorumNotReached {
            configured_provider_count: 4,
            required_provider_count: 3,
            observed_provider_count: 3,
            largest_agreement_count: 2,
        })
    ));
}

#[test]
fn two_provider_head_divergence_preserves_fail_closed_behavior() {
    // Given: two providers agree on finality but report different current heads.
    let first = provider_head("first", 11, "0xf11");
    let mut second = provider_head("second", 11, "0xf11");
    second.head = block(19, "0xother-head", "0xother-parent");

    // When: the legacy two-provider configuration is checked.
    let result = agree_on_finalized_heads(2, &[first, second]);

    // Then: exact two-provider disagreement still fails closed.
    assert!(matches!(
        result,
        Err(ChainSourceError::ProviderDisagreement { .. })
    ));
}

#[test]
fn log_batch_quorum_compares_agreed_height() {
    // Given: three finalized-head peers where two return the same log batch.
    let batches = [
        provider_batch("first", "0xagreed"),
        provider_batch("second", "0xagreed"),
        provider_batch("divergent", "0xother"),
    ];

    // When: batches are compared at the strict-majority finalized height.
    let result = crate::source::agree_on_finalized_log_batches(3, &batches);

    // Then: the corroborated log batch proceeds without trusting the divergent peer.
    assert!(matches!(
        result,
        Ok(batch)
            if batch
                .as_raw_batch()
                .logs
                .first()
                .is_some_and(|log| log.data == "0xagreed")
    ));
}

#[test]
fn log_batch_subquorum_fails_closed() {
    // Given: only two of four configured providers return the same log batch.
    let batches = [
        provider_batch("first", "0xagreed"),
        provider_batch("second", "0xagreed"),
        provider_batch("divergent", "0xother"),
    ];

    // When: finalized heads agree but log evidence does not meet quorum.
    let result = crate::source::agree_on_finalized_log_batches(4, &batches);

    // Then: log ingestion fails closed at the source boundary.
    assert!(matches!(
        result,
        Err(ChainSourceError::ProviderQuorumNotReached {
            configured_provider_count: 4,
            required_provider_count: 3,
            observed_provider_count: 3,
            largest_agreement_count: 2,
        })
    ));
}

#[test]
fn header_linkage_verifies() {
    // Given: complete finalized coverage whose parent hashes form one chain.
    let range =
        FinalizedBlockRange::new(ChainId::POLYGON, 10, 12).expect("fixture range is ordered");
    let coverage = FinalizedBlockCoverage::new(
        range.clone(),
        vec![
            block(10, "0xf10", "0xf9"),
            block(11, "0xf11", "0xf10"),
            block(12, "0xf12", "0xf11"),
        ],
    )
    .expect("linked fixture coverage is valid");

    // When: the linked evidence crosses the finalized batch boundary.
    let result = FinalizedRawLogBatch::new(
        provider(),
        range,
        block(20, "0xhead", "0xf19"),
        block(12, "0xf12", "0xf11"),
        coverage,
        Vec::new(),
    );

    // Then: the complete, linked batch is accepted.
    assert!(result.is_ok());
}

#[test]
fn broken_linkage_rejected() {
    // Given: complete range coverage with block twelve linked to the wrong parent.
    let range =
        FinalizedBlockRange::new(ChainId::POLYGON, 10, 12).expect("fixture range is ordered");
    let coverage = FinalizedBlockCoverage {
        range: range.clone(),
        blocks: vec![
            block(10, "0xf10", "0xf9"),
            block(11, "0xf11", "0xf10"),
            block(12, "0xf12", "0xwrong"),
        ],
    };

    // When: the broken evidence crosses the finalized batch boundary.
    let result = FinalizedRawLogBatch::new(
        provider(),
        range,
        block(20, "0xhead", "0xf19"),
        block(12, "0xf12", "0xf11"),
        coverage,
        Vec::new(),
    );

    // Then: linkage failure is a typed, fail-closed source error.
    assert!(matches!(
        result,
        Err(ChainSourceError::BrokenBlockLinkage {
            block_number: 12,
            ..
        })
    ));
}
