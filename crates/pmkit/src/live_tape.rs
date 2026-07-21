use std::collections::HashSet;
use std::fs::{self, File};

use pmkit_event::MarketEvent;
use pmkit_exec::OrderId;
use pmkit_run::TapePolicy;
use pmkit_runtime::RuntimeConfig;
use pmkit_spec::LiveRun;
use pmkit_tape::{JsonLinesTape, UserTapeSink};

use super::StartError;

pub(super) struct LiveTape {
    policy: Option<TapePolicy>,
    tape: Option<JsonLinesTape<File>>,
}

impl LiveTape {
    pub(super) fn open(run: &LiveRun, runtime: &RuntimeConfig) -> Result<Self, StartError> {
        let Some(policy) = run.tape_policy() else {
            return Ok(Self {
                policy: None,
                tape: None,
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
        })
    }

    pub(super) fn append(&mut self, run: &LiveRun, event: &MarketEvent) -> Result<(), StartError> {
        let Some(tape) = &mut self.tape else {
            return Ok(());
        };
        if let Err(source) = tape.append(event) {
            if self.policy == Some(TapePolicy::Required) {
                return Err(StartError::Tape {
                    run: run.id().clone(),
                    source,
                });
            }
        }
        Ok(())
    }

    pub(super) fn flush(&mut self, run: &LiveRun) -> Result<(), StartError> {
        let Some(tape) = &mut self.tape else {
            return Ok(());
        };
        if let Err(source) = tape.flush() {
            if self.policy == Some(TapePolicy::Required) {
                return Err(StartError::Tape {
                    run: run.id().clone(),
                    source,
                });
            }
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
