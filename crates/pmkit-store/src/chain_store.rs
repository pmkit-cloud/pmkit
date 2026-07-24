use async_trait::async_trait;

use crate::{
    CanonicalChainLog, CanonicalLogSegment, ChainCheckpoint, ContractRegistry, StoreError,
    TursoTapeStore, WalletQuery, WalletSnapshot,
    schema::{
        DELETE_CANONICAL_LOGS_AFTER, INSERT_CANONICAL_LOG, READ_CANONICAL_CHECKPOINT,
        READ_CANONICAL_LOGS, READ_CANONICAL_TIP, UPSERT_CANONICAL_CHECKPOINT,
    },
    wallet::rebuild_wallet,
};

/// Durable canonical-log operations and deterministic wallet reconstruction.
#[async_trait]
pub trait CanonicalLogStore: Send + Sync {
    /// Deletes orphaned logs after the common ancestor and stores the replacement segment.
    async fn replace_canonical_segment(
        &self,
        registry: &ContractRegistry,
        segment: &CanonicalLogSegment,
    ) -> Result<(), StoreError>;

    /// Replays canonical stored logs to rebuild one wallet without an RPC dependency.
    async fn wallet_snapshot(&self, query: &WalletQuery) -> Result<WalletSnapshot, StoreError>;
}

#[async_trait]
impl CanonicalLogStore for TursoTapeStore {
    async fn replace_canonical_segment(
        &self,
        registry: &ContractRegistry,
        segment: &CanonicalLogSegment,
    ) -> Result<(), StoreError> {
        validate_segment(registry, segment)?;
        let chain_id =
            i64::try_from(registry.chain_id.get()).map_err(|_| StoreError::LimitTooLarge)?;
        validate_stored_chain(&self.connection, chain_id, &segment.common_ancestor).await?;
        let transaction = self.connection.unchecked_transaction().await?;
        write_canonical_segment(&transaction, registry, segment).await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn wallet_snapshot(&self, query: &WalletQuery) -> Result<WalletSnapshot, StoreError> {
        let chain_id = i64::try_from(ContractRegistry::polygon().chain_id.get())
            .map_err(|_| StoreError::LimitTooLarge)?;
        let from_block = query
            .from_block
            .map(i64::try_from)
            .transpose()
            .map_err(|_| StoreError::LimitTooLarge)?;
        let to_block = query
            .to_block
            .map(i64::try_from)
            .transpose()
            .map_err(|_| StoreError::LimitTooLarge)?;
        let mut rows = self
            .connection
            .query(READ_CANONICAL_LOGS, (chain_id, from_block, to_block))
            .await?;
        let mut logs = Vec::new();
        while let Some(row) = rows.next().await? {
            logs.push(decode_log(&row)?);
        }
        Ok(rebuild_wallet(query, &logs))
    }
}

pub async fn write_canonical_segment(
    transaction: &turso::transaction::Transaction<'_>,
    registry: &ContractRegistry,
    segment: &CanonicalLogSegment,
) -> Result<(), StoreError> {
    let chain_id = i64::try_from(registry.chain_id.get()).map_err(|_| StoreError::LimitTooLarge)?;
    transaction
        .execute(
            DELETE_CANONICAL_LOGS_AFTER,
            (
                chain_id,
                i64::try_from(segment.common_ancestor.block_number)
                    .map_err(|_| StoreError::LimitTooLarge)?,
            ),
        )
        .await?;
    for log in &segment.logs {
        transaction
            .execute(
                INSERT_CANONICAL_LOG,
                (
                    i64::try_from(log.identity.chain_id.get())
                        .map_err(|_| StoreError::LimitTooLarge)?,
                    i64::try_from(log.identity.block_number)
                        .map_err(|_| StoreError::LimitTooLarge)?,
                    log.identity.block_hash.as_str(),
                    log.identity.transaction_hash.as_str(),
                    i64::try_from(log.identity.transaction_index)
                        .map_err(|_| StoreError::LimitTooLarge)?,
                    i64::try_from(log.identity.log_index).map_err(|_| StoreError::LimitTooLarge)?,
                    log.contract_address.as_str(),
                    serde_json::to_string(&log.event).map_err(|error| {
                        StoreError::CanonicalLogDecode {
                            message: error.to_string(),
                        }
                    })?,
                ),
            )
            .await?;
    }
    let checkpoint = segment_tip(segment);
    transaction
        .execute(
            UPSERT_CANONICAL_CHECKPOINT,
            (
                i64::try_from(checkpoint.chain_id.get()).map_err(|_| StoreError::LimitTooLarge)?,
                i64::try_from(checkpoint.block_number).map_err(|_| StoreError::LimitTooLarge)?,
                checkpoint.block_hash.as_str(),
            ),
        )
        .await?;
    Ok(())
}

pub fn validate_segment(
    registry: &ContractRegistry,
    segment: &CanonicalLogSegment,
) -> Result<(), StoreError> {
    if segment.common_ancestor.chain_id != registry.chain_id
        || !segment.logs.windows(2).all(|logs| {
            let previous = &logs[0].identity;
            let next = &logs[1].identity;
            (
                previous.block_number,
                previous.transaction_index,
                previous.log_index,
            ) < (next.block_number, next.transaction_index, next.log_index)
                && (previous.block_number != next.block_number
                    || previous.block_hash == next.block_hash)
        })
        || segment.logs.iter().any(|log| {
            log.identity.block_number <= segment.common_ancestor.block_number
                || !registry.accepts(log)
        })
    {
        return Err(StoreError::InvalidCanonicalSegment);
    }
    Ok(())
}

pub async fn validate_stored_chain(
    connection: &turso::Connection,
    chain_id: i64,
    ancestor: &ChainCheckpoint,
) -> Result<(), StoreError> {
    let checkpoint = read_chain_checkpoint(connection, chain_id, READ_CANONICAL_CHECKPOINT).await?;
    let tip = read_chain_checkpoint(connection, chain_id, READ_CANONICAL_TIP).await?;
    if checkpoint != tip {
        return Err(StoreError::InvalidCanonicalSegment);
    }
    let Some(tip) = tip else {
        return (ancestor.block_number == 0)
            .then_some(())
            .ok_or(StoreError::InvalidCanonicalSegment);
    };
    if ancestor.block_number > tip.block_number {
        return Err(StoreError::InvalidCanonicalSegment);
    }
    let mut hashes = connection
        .query(
            crate::schema::READ_CANONICAL_BLOCK_HASH,
            (
                chain_id,
                i64::try_from(ancestor.block_number).map_err(|_| StoreError::LimitTooLarge)?,
            ),
        )
        .await?;
    let Some(row) = hashes.next().await? else {
        return Err(StoreError::InvalidCanonicalSegment);
    };
    let hash: String = row.get(0)?;
    if hash != ancestor.block_hash || hashes.next().await?.is_some() {
        return Err(StoreError::InvalidCanonicalSegment);
    }
    Ok(())
}

fn segment_tip(segment: &CanonicalLogSegment) -> ChainCheckpoint {
    segment.logs.last().map_or_else(
        || segment.common_ancestor.clone(),
        |log| {
            ChainCheckpoint::new(
                log.identity.chain_id,
                log.identity.block_number,
                log.identity.block_hash.clone(),
            )
        },
    )
}

fn decode_log(row: &turso::Row) -> Result<CanonicalChainLog, StoreError> {
    let block_number: i64 = row.get(0)?;
    let transaction_index: i64 = row.get(3)?;
    let log_index: i64 = row.get(4)?;
    let contract_address_text = row.get::<String>(5)?;
    let contract_address = crate::Address::new(&contract_address_text).map_err(|error| {
        StoreError::CanonicalLogDecode {
            message: format!("invalid stored contract address {contract_address_text}: {error}"),
        }
    })?;
    let event = serde_json::from_str(&row.get::<String>(6)?).map_err(|error| {
        StoreError::CanonicalLogDecode {
            message: error.to_string(),
        }
    })?;
    Ok(CanonicalChainLog {
        identity: crate::CanonicalLogIdentity {
            chain_id: crate::ChainId::POLYGON,
            block_number: u64::try_from(block_number).map_err(|_| {
                StoreError::CanonicalLogDecode {
                    message: "negative block number".into(),
                }
            })?,
            block_hash: row.get(1)?,
            transaction_hash: row.get(2)?,
            transaction_index: u64::try_from(transaction_index).map_err(|_| {
                StoreError::CanonicalLogDecode {
                    message: "negative transaction index".into(),
                }
            })?,
            log_index: u64::try_from(log_index).map_err(|_| StoreError::CanonicalLogDecode {
                message: "negative log index".into(),
            })?,
        },
        contract_address,
        event,
    })
}

async fn read_chain_checkpoint(
    connection: &turso::Connection,
    chain_id: i64,
    sql: &str,
) -> Result<Option<ChainCheckpoint>, StoreError> {
    let mut rows = connection.query(sql, [chain_id]).await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    Ok(Some(ChainCheckpoint::new(
        crate::ChainId::POLYGON,
        u64::try_from(row.get::<i64>(0)?).map_err(|_| StoreError::CanonicalLogDecode {
            message: "negative canonical tip block".into(),
        })?,
        row.get::<String>(1)?,
    )))
}

#[cfg(test)]
mod rpc_batch_tests {
    use rust_decimal::Decimal;

    use crate::{
        Address, BlockHead, CanonicalLogStore, ChainCheckpoint, ChainId, ContractRegistry,
        FinalizedBlockCoverage, FinalizedBlockRange, FinalizedRawLogBatch, ProviderIdentity,
        RawLogIdentity, RawRpcLog, StoreError, TursoTapeStore, WalletQuery, ingest_finalized_batch,
    };

    const TRANSFER_TOPIC: &str =
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a8df523b3ef";

    fn transfer_log(
        provider: &ProviderIdentity,
        contract_address: Address,
        transaction_index: u64,
        data: String,
    ) -> RawRpcLog {
        RawRpcLog {
            identity: RawLogIdentity {
                provider: provider.clone(),
                chain_id: ChainId::POLYGON,
                block_number: 1,
                block_hash: "0xblock1".into(),
                transaction_hash: format!("0xtx{transaction_index}"),
                transaction_index,
                log_index: 0,
            },
            contract_address,
            topics: vec![
                TRANSFER_TOPIC.into(),
                format!("0x{:064x}", 1),
                format!("0x{:064x}", 0xaa),
            ],
            data,
        }
    }

    fn batch(
        provider: ProviderIdentity,
        logs: Vec<RawRpcLog>,
    ) -> Result<FinalizedRawLogBatch, crate::ChainSourceError> {
        let range = FinalizedBlockRange::new(ChainId::POLYGON, 1, 1)?;
        let finalized = BlockHead::new(ChainId::POLYGON, 1, "0xblock1", "0xgenesis");
        FinalizedRawLogBatch::new(
            provider,
            range.clone(),
            BlockHead::new(ChainId::POLYGON, 2, "0xhead", "0xblock1"),
            finalized.clone(),
            FinalizedBlockCoverage::new(range, vec![finalized])?,
            logs,
        )
    }

    #[tokio::test]
    async fn consistent_batch_ingests() -> Result<(), Box<dyn std::error::Error>> {
        // Given: complete linked coverage and one registered, recognized log.
        let directory = tempfile::tempdir()?;
        let store = TursoTapeStore::open_local(directory.path().join("consistent.db")).await?;
        let registry = ContractRegistry::polygon();
        let provider = ProviderIdentity::new("safe-provider");
        let batch = batch(
            provider.clone(),
            vec![transfer_log(
                &provider,
                registry.collateral.clone(),
                0,
                format!("0x{:064x}", 42),
            )],
        )?;

        // When: the finalized RPC batch crosses the durable ingestion boundary.
        ingest_finalized_batch(
            &store,
            &registry,
            &batch,
            ChainCheckpoint::new(ChainId::POLYGON, 0, "0xgenesis"),
        )
        .await?;

        // Then: the complete batch is committed and visible canonically.
        let wallet = Address::new("0x00000000000000000000000000000000000000aa")?;
        assert_eq!(
            store
                .wallet_snapshot(&WalletQuery::new(wallet))
                .await?
                .collateral_balance,
            Decimal::from(42)
        );
        drop(store);
        Ok(())
    }

    #[tokio::test]
    async fn inconsistent_batch_rejected_redacted() -> Result<(), Box<dyn std::error::Error>> {
        // Given: a batch mixing one valid log with one unregistered-contract payload.
        let directory = tempfile::tempdir()?;
        let store = TursoTapeStore::open_local(directory.path().join("inconsistent.db")).await?;
        let registry = ContractRegistry::polygon();
        let provider = ProviderIdentity::new("safe-provider");
        let secret_url = "https://rpc.example/v1/url-api-key";
        let auth_header = "Bearer header-secret";
        let raw_payload = "0xraw-payload-secret";
        let batch = batch(
            provider.clone(),
            vec![
                transfer_log(
                    &provider,
                    registry.collateral.clone(),
                    0,
                    format!("0x{:064x}", 42),
                ),
                transfer_log(
                    &provider,
                    Address::new("0x0000000000000000000000000000000000000001")?,
                    1,
                    raw_payload.into(),
                ),
            ],
        )?;

        // When: registry sanity rejects one member before the transaction begins.
        let result = ingest_finalized_batch(
            &store,
            &registry,
            &batch,
            ChainCheckpoint::new(ChainId::POLYGON, 0, "0xgenesis"),
        )
        .await;
        let Err(error) = result else {
            return Err("inconsistent batch unexpectedly ingested".into());
        };

        // Then: the typed error exposes only provider identity and sanitized detail.
        assert!(matches!(error, StoreError::CanonicalLogDecode { .. }));
        let display = error.to_string();
        let debug = format!("{error:?}");
        assert!(display.contains("safe-provider"));
        assert!(display.contains("unregistered contract"));
        for secret in [secret_url, auth_header, raw_payload, TRANSFER_TOPIC] {
            assert!(!display.contains(secret));
            assert!(!debug.contains(secret));
        }
        assert_eq!(store.finalized_checkpoint(ChainId::POLYGON).await?, None);
        drop(store);
        Ok(())
    }
}
