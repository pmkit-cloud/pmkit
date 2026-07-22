use async_trait::async_trait;

use crate::{
    CanonicalChainLog, CanonicalLogSegment, ChainCheckpoint, ContractRegistry, StoreError,
    TursoTapeStore, WalletQuery, WalletSnapshot,
    schema::{
        DELETE_CANONICAL_LOGS_AFTER, INSERT_CANONICAL_LOG, READ_CANONICAL_CHECKPOINT,
        READ_CANONICAL_LOGS, UPSERT_CANONICAL_CHECKPOINT,
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
        let transaction = self.connection.unchecked_transaction().await?;
        transaction
            .execute(
                DELETE_CANONICAL_LOGS_AFTER,
                (
                    i64::try_from(registry.chain_id.get())
                        .map_err(|_| StoreError::LimitTooLarge)?,
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
                        i64::try_from(log.identity.log_index)
                            .map_err(|_| StoreError::LimitTooLarge)?,
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
                    i64::try_from(checkpoint.chain_id.get())
                        .map_err(|_| StoreError::LimitTooLarge)?,
                    i64::try_from(checkpoint.block_number)
                        .map_err(|_| StoreError::LimitTooLarge)?,
                    checkpoint.block_hash.as_str(),
                ),
            )
            .await?;
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
        let mut snapshot = rebuild_wallet(query, &logs);
        snapshot.canonical_tip = read_checkpoint(&self.connection, chain_id).await?;
        Ok(snapshot)
    }
}

fn validate_segment(
    registry: &ContractRegistry,
    segment: &CanonicalLogSegment,
) -> Result<(), StoreError> {
    if segment.common_ancestor.chain_id != registry.chain_id
        || segment.logs.iter().any(|log| {
            log.identity.block_number <= segment.common_ancestor.block_number
                || !registry.accepts(log)
        })
    {
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

async fn read_checkpoint(
    connection: &turso::Connection,
    chain_id: i64,
) -> Result<Option<ChainCheckpoint>, StoreError> {
    let mut rows = connection
        .query(READ_CANONICAL_CHECKPOINT, [chain_id])
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let block_number: i64 = row.get(0)?;
    Ok(Some(ChainCheckpoint::new(
        crate::ChainId::POLYGON,
        u64::try_from(block_number).map_err(|_| StoreError::CanonicalLogDecode {
            message: "negative checkpoint block".into(),
        })?,
        row.get::<String>(1)?,
    )))
}
