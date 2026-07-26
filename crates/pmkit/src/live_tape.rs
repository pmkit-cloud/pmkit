use std::collections::HashSet;
use std::fs::{self, File};

use pmkit_event::{FillIdentity, MarketEvent, PmAccountEnvelope, PmAccountEvent, StreamMetadata};
use pmkit_exec::OrderId;
use pmkit_run::TapePolicy;
use pmkit_runtime::RuntimeConfig;
use pmkit_spec::LiveRun;
use pmkit_tape::{JsonLinesTape, UserTapeSink};

use super::StartError;

pub(super) struct LiveTape {
    policy: Option<TapePolicy>,
    tape: Option<JsonLinesTape<File>>,
    ingest_sequence: u64,
}

impl LiveTape {
    pub(super) fn open(run: &LiveRun, runtime: &RuntimeConfig) -> Result<Self, StartError> {
        let Some(policy) = run.tape_policy() else {
            return Ok(Self {
                policy: None,
                tape: None,
                ingest_sequence: 0,
            });
        };
        let path = tape_path(run, runtime);
        let tape = match fs::create_dir_all(&runtime.manifest_dir).and_then(|()| File::create(path))
        {
            Ok(file) => Some(JsonLinesTape::new(file)),
            Err(source) => match policy {
                TapePolicy::Required => {
                    return Err(StartError::Tape {
                        run: run.id().clone(),
                        source,
                    });
                }
                TapePolicy::BestEffort => None,
            },
        };
        Ok(Self {
            policy: Some(policy),
            tape,
            ingest_sequence: 0,
        })
    }

    pub(super) fn append(&mut self, run: &LiveRun, event: &MarketEvent) -> Result<(), StartError> {
        let timestamp_ms = event.timestamp_ms();
        let frame_sequence = i64::try_from(self.ingest_sequence).map_err(|_| StartError::Tape {
            run: run.id().clone(),
            source: std::io::Error::other("live tape frame sequence exceeds signed range"),
        })?;
        let metadata = StreamMetadata {
            schema_version: 4,
            source_id: "pmkit-live".into(),
            source_time_ms: timestamp_ms,
            canonical_source_rank: 0,
            receipt_time_ms: timestamp_ms,
            connection_id: run.id().to_string(),
            connection_epoch: 0,
            frame_sequence,
            ingest_sequence: self.ingest_sequence,
        };
        let fact = match event {
            MarketEvent::Fill {
                strategy,
                order_id,
                market,
                outcome,
                price,
                size,
                side,
                fee,
                liquidity,
                timestamp_ms,
            } => PmAccountEvent::Fill {
                identity: FillIdentity::transport(&metadata),
                strategy: strategy.clone(),
                order_id: order_id.clone(),
                market: market.clone(),
                outcome: *outcome,
                price: *price,
                size: *size,
                side: *side,
                fee: *fee,
                liquidity: *liquidity,
                timestamp_ms: *timestamp_ms,
            },
            MarketEvent::OrderAck {
                strategy,
                order_id,
                timestamp_ms,
            } => PmAccountEvent::OrderAck {
                strategy: strategy.clone(),
                order_id: order_id.clone(),
                timestamp_ms: *timestamp_ms,
            },
            MarketEvent::BookUpdate { .. }
            | MarketEvent::BestBidAsk { .. }
            | MarketEvent::LastTrade { .. }
            | MarketEvent::Tick { .. } => return Ok(()),
        };
        let Some(tape) = &mut self.tape else {
            return Ok(());
        };
        let envelope = PmAccountEnvelope {
            portfolio: run.portfolio().clone(),
            metadata,
            raw_frame: Vec::new(),
            fact,
        };
        self.ingest_sequence += 1;
        if let Err(source) = tape.append(&envelope)
            && self.policy == Some(TapePolicy::Required)
        {
            return Err(StartError::Tape {
                run: run.id().clone(),
                source,
            });
        }
        Ok(())
    }

    pub(super) fn append_account(
        &mut self,
        run: &LiveRun,
        envelope: &PmAccountEnvelope,
    ) -> Result<(), StartError> {
        let Some(tape) = &mut self.tape else {
            return Ok(());
        };
        if let Err(source) = tape.append(envelope)
            && self.policy == Some(TapePolicy::Required)
        {
            return Err(StartError::Tape {
                run: run.id().clone(),
                source,
            });
        }
        Ok(())
    }

    pub(super) fn flush(&mut self, run: &LiveRun) -> Result<(), StartError> {
        let Some(tape) = &mut self.tape else {
            return Ok(());
        };
        if let Err(source) = tape.flush()
            && self.policy == Some(TapePolicy::Required)
        {
            return Err(StartError::Tape {
                run: run.id().clone(),
                source,
            });
        }
        Ok(())
    }

    pub(super) async fn finish(
        &mut self,
        run: &LiveRun,
        runtime: &RuntimeConfig,
        open_orders: &HashSet<OrderId>,
    ) -> Result<(), StartError> {
        self.flush(run)?;
        super::shutdown_orders(run, runtime, open_orders).await
    }
}

fn tape_path(run: &LiveRun, runtime: &RuntimeConfig) -> std::path::PathBuf {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let run = run.id().to_string();
    let mut encoded_run = String::with_capacity(run.len() * 2);
    for byte in run.bytes() {
        encoded_run.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded_run.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    runtime
        .manifest_dir
        .join(format!("live-{encoded_run}.jsonl"))
}
