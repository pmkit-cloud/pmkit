use crate::{
    CanonicalLogSegment, ChainCheckpoint, ChainId, ContractRegistry,
    QuorumVerifiedFinalizedLogBatch, StoreError, TursoTapeStore,
    chain_store::{validate_segment, validate_stored_chain, write_canonical_segment},
    decode_raw_log,
    schema::{READ_FINALIZED_CHAIN_CHECKPOINT, UPSERT_FINALIZED_CHAIN_CHECKPOINT},
    source::verify_finalized_progression,
};

/// Decodes and transactionally ingests one validated finalized raw-log batch.
///
/// The common ancestor is supplied by the caller that owns provider/checkpoint
/// coordination. The batch is held without canonical writes until its coverage
/// proves a finalized head linked to the durable checkpoint.
///
/// A single-provider batch cannot cross this durable boundary:
///
/// ```compile_fail
/// use pmkit_store::{
///     ChainCheckpoint, ContractRegistry, FinalizedRawLogBatch, TursoTapeStore,
///     ingest_finalized_batch,
/// };
///
/// fn bypass_quorum(
///     store: &TursoTapeStore,
///     registry: &ContractRegistry,
///     batch: &FinalizedRawLogBatch,
///     common_ancestor: ChainCheckpoint,
/// ) {
///     let _ = ingest_finalized_batch(store, registry, batch, common_ancestor);
/// }
/// ```
///
/// # Errors
///
/// Returns [`StoreError`] when finality regresses, header evidence is invalid,
/// a raw log cannot be decoded, or durable storage fails.
pub async fn ingest_finalized_batch(
    store: &TursoTapeStore,
    registry: &ContractRegistry,
    batch: &QuorumVerifiedFinalizedLogBatch,
    common_ancestor: ChainCheckpoint,
) -> Result<(), StoreError> {
    let batch = batch.as_raw_batch();
    let persisted = store.finalized_checkpoint(registry.chain_id).await?;
    let Some(finalized) =
        verify_finalized_progression(persisted.as_ref(), &common_ancestor, batch)?
    else {
        return Ok(());
    };
    let logs = batch
        .logs
        .iter()
        .filter(|raw| {
            raw.identity.block_number > common_ancestor.block_number
                && raw.identity.block_number <= finalized.block_number
        })
        .map(|raw| {
            decode_raw_log(registry, raw).map_err(|error| StoreError::CanonicalLogDecode {
                message: error.to_string(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let segment = CanonicalLogSegment::new(common_ancestor, logs);
    validate_segment(registry, &segment)?;
    let chain_id = i64::try_from(registry.chain_id.get()).map_err(|_| StoreError::LimitTooLarge)?;
    if persisted.as_ref() != Some(&segment.common_ancestor) {
        validate_stored_chain(&store.connection, chain_id, &segment.common_ancestor).await?;
    }

    let transaction = store.connection.unchecked_transaction().await?;
    write_canonical_segment(&transaction, registry, &segment).await?;
    let written = transaction
        .execute(
            UPSERT_FINALIZED_CHAIN_CHECKPOINT,
            (
                chain_id,
                i64::try_from(finalized.block_number).map_err(|_| StoreError::LimitTooLarge)?,
                finalized.block_hash.as_str(),
            ),
        )
        .await?;
    if written == 0 {
        transaction.rollback().await?;
        return Err(StoreError::FinalizedHeadNotLinked {
            chain_id: registry.chain_id.get(),
            block_number: finalized.block_number,
        });
    }
    transaction.commit().await?;
    Ok(())
}

impl TursoTapeStore {
    /// Reads the durable finalized head for one chain.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the checkpoint cannot be read or decoded.
    pub async fn finalized_checkpoint(
        &self,
        chain_id: ChainId,
    ) -> Result<Option<ChainCheckpoint>, StoreError> {
        let database_chain_id =
            i64::try_from(chain_id.get()).map_err(|_| StoreError::LimitTooLarge)?;
        let mut rows = self
            .connection
            .query(READ_FINALIZED_CHAIN_CHECKPOINT, [database_chain_id])
            .await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        let block_number =
            u64::try_from(row.get::<i64>(0)?).map_err(|_| StoreError::CanonicalLogDecode {
                message: "negative finalized checkpoint block".into(),
            })?;
        Ok(Some(ChainCheckpoint::new(
            chain_id,
            block_number,
            row.get::<String>(1)?,
        )))
    }
}
