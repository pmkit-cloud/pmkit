//! Live oracle adapter for exact `PolyBolt` 60-second TWAP records.

use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use pmkit_data::{
    DataSourceError, GapRange, GapSequenceDomain, LiveOracleDataSource, SourceGap, SourceHeartbeat,
    SourceSignal,
};
use pmkit_event::{CexReferenceEnvelope, CexReferenceEvent, SourceEnvelope, StreamMetadata};
use pmkit_market::Asset;
use polymarket_client_sdk_v2::auth::{Credentials, ExposeSecret as _};
use thiserror::Error;
use tokio::sync::mpsc::Sender;

use crate::{
    POLYBOLT_SOURCE_ID, PolyBoltClient, PolyBoltControl, PolyBoltError, PolyBoltEvent,
    PolyBoltItem, PolyBoltRawFrame, PolyBoltRecord,
};

/// Stable source identity used by run specifications for `PolyBolt` oracle data.
pub const POLYBOLT_ORACLE_SOURCE_ID: &str = POLYBOLT_SOURCE_ID;
const CANONICAL_SOURCE_RANK: i64 = 2;
static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(0);

/// An error constructing the authenticated `PolyBolt` oracle source.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PolyBoltOracleSourceError {
    /// Caller-provided SDK credentials were absent or empty.
    #[error("PolyBolt oracle credentials are required")]
    MissingCredentials,
}

/// Authenticated BTC/USD 60-second oracle TWAP source.
pub struct PolyBoltOracleDataSource {
    credentials: Credentials,
}

impl fmt::Debug for PolyBoltOracleDataSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PolyBoltOracleDataSource")
            .finish_non_exhaustive()
    }
}

impl PolyBoltOracleDataSource {
    /// Creates a source using the caller's existing Polymarket SDK credentials.
    ///
    /// Passing absent or empty credentials returns a typed error without making a network call.
    ///
    /// # Errors
    ///
    /// Returns [`PolyBoltOracleSourceError::MissingCredentials`] when credentials are missing.
    pub fn new(credentials: Option<Credentials>) -> Result<Self, PolyBoltOracleSourceError> {
        let credentials = credentials
            .filter(|credentials| {
                !credentials.key().is_nil()
                    && !credentials.secret().expose_secret().is_empty()
                    && !credentials.passphrase().expose_secret().is_empty()
            })
            .ok_or(PolyBoltOracleSourceError::MissingCredentials)?;
        Ok(Self { credentials })
    }
}

#[async_trait]
impl LiveOracleDataSource for PolyBoltOracleDataSource {
    async fn subscribe_oracle(
        &self,
        asset: Asset,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        validate_asset(asset)?;
        let session_id = NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed);
        let mut client = PolyBoltClient::connect(self.credentials.clone())
            .await
            .map_err(|error| transport_error(&error))?;
        let mut next_ingest_sequence = 0;

        loop {
            match client
                .next_with_control()
                .await
                .map_err(|error| transport_error(&error))?
            {
                PolyBoltItem::Record {
                    record,
                    frame_sequence,
                } => {
                    deliver_record(
                        record,
                        frame_sequence,
                        session_id,
                        &mut next_ingest_sequence,
                        &sink,
                    )
                    .await?;
                }
                PolyBoltItem::Control(control) => {
                    deliver_control(control, session_id, &mut next_ingest_sequence, &sink).await?;
                }
            }
        }
    }
}

fn validate_asset(asset: Asset) -> Result<(), DataSourceError> {
    if asset == Asset::Btc {
        Ok(())
    } else {
        Err(DataSourceError::Unavailable {
            message: format!("PolyBolt oracle supports BTC/USD only; requested {asset}/USD"),
        })
    }
}

fn transport_error(error: &PolyBoltError) -> DataSourceError {
    DataSourceError::Unavailable {
        message: format!("PolyBolt oracle stream failed: {error}"),
    }
}

async fn deliver_record(
    record: PolyBoltRecord,
    frame_sequence: u64,
    session_id: u64,
    next_ingest_sequence: &mut u64,
    sink: &Sender<SourceSignal>,
) -> Result<(), DataSourceError> {
    let PolyBoltRecord {
        raw,
        connection_epoch,
        server_timestamp_ms,
        event,
        ..
    } = record;
    match event {
        PolyBoltEvent::Snapshot { prices } => {
            for price in prices {
                let ingest_sequence = take_ingest_sequence(next_ingest_sequence)?;
                send_fact(
                    sink,
                    envelope(
                        &raw,
                        connection_epoch,
                        frame_sequence,
                        session_id,
                        ingest_sequence,
                        price.timestamp_ms,
                        CexReferenceEvent::Twap60sSnapshot {
                            asset: Asset::Btc,
                            price: price.exact_value,
                            timestamp_ms: price.timestamp_ms,
                        },
                    )?,
                )
                .await?;
            }
        }
        PolyBoltEvent::Update { price } => {
            let ingest_sequence = take_ingest_sequence(next_ingest_sequence)?;
            send_fact(
                sink,
                envelope(
                    &raw,
                    connection_epoch,
                    frame_sequence,
                    session_id,
                    ingest_sequence,
                    price.timestamp_ms,
                    CexReferenceEvent::Twap60sUpdate {
                        asset: Asset::Btc,
                        price: price.exact_value,
                        timestamp_ms: price.timestamp_ms,
                    },
                )?,
            )
            .await?;
        }
        PolyBoltEvent::EmptySnapshot { .. }
        | PolyBoltEvent::OtherSymbol { .. }
        | PolyBoltEvent::OtherChannel { .. } => {
            send_heartbeat(
                raw,
                connection_epoch,
                frame_sequence,
                session_id,
                next_ingest_sequence,
                server_timestamp_ms,
                sink,
            )
            .await?;
        }
        PolyBoltEvent::Gap(gap) => {
            let missing_range = gap.missing_sequence_range();
            let reason = gap_reason(missing_range, gap.observed_sequence, gap.dropped);
            let ingest_sequence = take_ingest_sequence(next_ingest_sequence)?;
            let metadata = metadata(
                &raw,
                connection_epoch,
                frame_sequence,
                session_id,
                ingest_sequence,
                server_timestamp_ms,
            )?;
            let detected_at_ms = metadata.receipt_time_ms;
            sink.send(SourceSignal::Gap(SourceGap {
                source_id: POLYBOLT_ORACLE_SOURCE_ID.to_owned(),
                detected_at_ms,
                metadata: Some(metadata),
                raw_frame: raw.bytes,
                range: missing_range.map(|(first_missing, last_missing)| GapRange {
                    domain: GapSequenceDomain::Channel,
                    first_missing,
                    last_missing,
                }),
                unresolved_count: (gap.dropped > 0).then_some(gap.dropped),
                reason: reason.clone(),
            }))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
            return Err(DataSourceError::ReplayGap { message: reason });
        }
    }
    Ok(())
}

async fn deliver_control(
    control: PolyBoltControl,
    session_id: u64,
    next_ingest_sequence: &mut u64,
    sink: &Sender<SourceSignal>,
) -> Result<(), DataSourceError> {
    let source_time_ms = control.raw.receipt_time_ms;
    send_heartbeat(
        control.raw,
        control.connection_epoch,
        control.frame_sequence,
        session_id,
        next_ingest_sequence,
        source_time_ms,
        sink,
    )
    .await
}

async fn send_heartbeat(
    raw: PolyBoltRawFrame,
    connection_epoch: u64,
    frame_sequence: u64,
    session_id: u64,
    next_ingest_sequence: &mut u64,
    source_time_ms: i64,
    sink: &Sender<SourceSignal>,
) -> Result<(), DataSourceError> {
    let ingest_sequence = take_ingest_sequence(next_ingest_sequence)?;
    sink.send(SourceSignal::Heartbeat(SourceHeartbeat {
        metadata: metadata(
            &raw,
            connection_epoch,
            frame_sequence,
            session_id,
            ingest_sequence,
            source_time_ms,
        )?,
        raw_frame: raw.bytes,
    }))
    .await
    .map_err(|_| DataSourceError::SinkClosed)
}

async fn send_fact(
    sink: &Sender<SourceSignal>,
    envelope: CexReferenceEnvelope,
) -> Result<(), DataSourceError> {
    sink.send(SourceSignal::Data(Box::new(SourceEnvelope::CexReference(
        envelope,
    ))))
    .await
    .map_err(|_| DataSourceError::SinkClosed)
}

fn envelope(
    raw: &PolyBoltRawFrame,
    connection_epoch: u64,
    frame_sequence: u64,
    session_id: u64,
    ingest_sequence: u64,
    source_time_ms: i64,
    fact: CexReferenceEvent,
) -> Result<CexReferenceEnvelope, DataSourceError> {
    Ok(CexReferenceEnvelope {
        metadata: metadata(
            raw,
            connection_epoch,
            frame_sequence,
            session_id,
            ingest_sequence,
            source_time_ms,
        )?,
        raw_frame: raw.bytes.clone(),
        fact,
    })
}

fn metadata(
    raw: &PolyBoltRawFrame,
    connection_epoch: u64,
    frame_sequence: u64,
    session_id: u64,
    ingest_sequence: u64,
    source_time_ms: i64,
) -> Result<StreamMetadata, DataSourceError> {
    Ok(StreamMetadata {
        schema_version: 1,
        source_id: POLYBOLT_SOURCE_ID.to_owned(),
        source_time_ms,
        canonical_source_rank: CANONICAL_SOURCE_RANK,
        receipt_time_ms: raw.receipt_time_ms,
        connection_id: format!("{POLYBOLT_SOURCE_ID}#{session_id}:{connection_epoch}"),
        connection_epoch: i64::try_from(connection_epoch).map_err(|_| {
            DataSourceError::ReplayGap {
                message: "PolyBolt connection epoch exceeds i64 range".into(),
            }
        })?,
        frame_sequence: i64::try_from(frame_sequence).map_err(|_| DataSourceError::ReplayGap {
            message: "PolyBolt frame sequence exceeds i64 range".into(),
        })?,
        ingest_sequence,
    })
}

fn gap_reason(missing_range: Option<(u64, u64)>, observed_sequence: u64, dropped: u64) -> String {
    match missing_range {
        Some((first, last)) => format!(
            "PolyBolt TWAP sequence discontinuity: missing {first}..={last} (observed {observed_sequence}, server dropped {dropped})"
        ),
        None => format!(
            "PolyBolt reported {dropped} dropped TWAP frames at sequence {observed_sequence} without an identifiable missing sequence range"
        ),
    }
}

fn take_ingest_sequence(next: &mut u64) -> Result<u64, DataSourceError> {
    let sequence = *next;
    *next = sequence
        .checked_add(1)
        .ok_or_else(|| DataSourceError::ReplayGap {
            message: "PolyBolt ingest sequence exceeds u64 range".into(),
        })?;
    Ok(sequence)
}

#[cfg(test)]
#[path = "polybolt_source_tests.rs"]
mod tests;
