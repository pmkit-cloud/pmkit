# PMKit generic run-capture tape

This document describes the generic source-capture JSONL tape written by `pmkit-tape` (`RAW_CAPTURE_SCHEMA_VERSION = 4`). It is distinct from PM-specific causal persistence through the optional `TapeStore`. `CaptureConfig::new(directory)` selects the generic capture directory; `path_for(run)` names each run file `run-<run>.jsonl`.

## Record format

Each newline-terminated JSON record contains `body` and `checksum_sha256`. The body fields are `schema_version`, `ingest_sequence`, `projection_index`, and `capture`. `body.schema_version` is the tape version (4); when source metadata is present, `capture.schema_version` is the source-envelope schema version. The lowercase checksum is SHA-256 of the serialized body.

`capture` contains `source_id`, `canonical_source_rank`, `receipt_time_ms`, `transport`, `normalized_projection`, and `payload`. `receipt_time_ms` is the local receipt time, or local detection time for an unwitnessed gap. When present, `transport` contains optional source time plus `connection_id`, `connection_epoch`, `frame_sequence`, and the source-local `ingest_sequence`. That source-local counter is distinct from the record's run-wide `ingest_sequence`, assigned contiguously by `RawCaptureWriter` in append order.

`projection_index` is zero-based within a source frame. Multiple projections of one frame have consecutive indexes and must retain the same frame kind, bytes, receipt time, and physical transport identity (source ID, connection ID, connection epoch, and frame sequence); source-local `transport.ingest_sequence` may differ per fact. Each projection witness must be distinct. `normalized_projection` is a typed-fact witness, not a replacement for the captured bytes.

`RawCapturePayload` is tagged by `kind` and has four forms:

- `frame` and `snapshot`: `raw_bytes` are the original source payload bytes before adaptation/decoding. For WebSocket sources these are the logical WebSocket payload bytes—not transport headers or JSON reconstructed from an SDK object.
- `control`: a `name` and original `raw_bytes` (for example, a heartbeat).
- `gap`: optional `range`, optional `unresolved_count`, `reason`, and `raw_frame` containing the exact bytes of a reporting frame, if one exists. The optional range and count are independent; either, both, or neither may be known.

Frame, snapshot, and control records require `transport`. A gap with `transport: Some(...)` was reported by an observed frame, even if `raw_frame` is empty. A gap with `transport: None` is locally detected and unwitnessed: it must have no range and an empty `raw_frame`. For such a gap, `capture.schema_version: 1` and `canonical_source_rank: 0` are local placeholders, not evidence of a native protocol version or source priority. Consumers must treat `transport: null` and `normalized_projection: null` as absent witness metadata; the writer-assigned run-wide `ingest_sequence` is authoritative for order. Empty bytes alone never identify provenance.

## Gap ranges and continuity

`GapRange` bounds are inclusive. `GapSequenceDomain::Transport` names physical frame ordinals; `Channel` names a source-level sequence and is not interchangeable with transport numbering. A missing range and an unresolved count describe loss; they do not replace the reporting frame's physical sequence.

For a witnessed transport-range gap, when prior continuity is known, `first_missing` must equal the expected frame. Its reporting frame must be exactly `last_missing + 1` (checked for overflow), and the next expected physical frame follows that report. A witnessed channel-range or unknown-range gap follows ordinary physical continuity and consumes exactly its reporting frame; channel sequence values and unresolved counts do not advance transport history. An unwitnessed local gap has no connection identity and does not seed, advance, or excuse physical-frame continuity. It cannot make a later physical sequence jump valid.

## Validation and replay

`RawCaptureReader::new` is strict and fails on an explicit gap. `RawCaptureReader::new_audit` returns the gap record and continues validating subsequent records. `CaptureReplayReader` uses audit validation: `next_signal` returns the gap once, then returns terminal `CaptureReplayError::Gap` on the next read. Complete records are checked for checksum, run-wide ingest order, projection indexes, and per-source physical continuity. Prefix recovery may discard an unterminated final tail only; complete malformed records fail closed, and resuming after a discarded tail requires an explicit gap record.

Replay checks witnesses against capture metadata and re-decodes recognized Binance aggregate-trade, PolyBolt TWAP, and native Polymarket market-WebSocket frames with production decoders. Other source IDs remain witness-backed; they are not thereby claimed to use those native protocols. Native Polymarket market-WebSocket replay requires caller-supplied `MarketTokens` through `CaptureReplayReader::new_with_market_tokens` or `CaptureReplaySource::new_with_market_tokens`. Token/outcome mappings are not inferred from a witness; replay does not guess an opposite asset or token.

When capture is enabled, the shared feed path records each accepted data signal once before mode-specific queueing or delivery, and records heartbeat and source/local-gap signals there too; live and paper consumers do not duplicate capture. Ordinary backtests still order facts by native event time; the capture's writer-assigned `ingest_sequence` preserves append order and does not guarantee strategy evaluation or downstream processing completion. `CaptureReplaySource::replay_recorded_order` and `RecordedBacktest::replay_capture` consume records directly in writer-assigned ingest order, not through the event-time `MergedFeed`. In recorded backtests, strategy time remains the fact's native timestamp; simulation activation time uses captured receipt time, with configured activation latency applied to placed orders. Cancel and cancel-all actions are immediate; cancellation latency is not simulated.

Historical facts remain usable in ordinary native-time backtests; receipt-time provenance is needed only for raw capture/recorded-arrival replay. Binance Vision's historical source preserves its original CSV row bytes, including delimiters, but its archive has no original local receipt time; `BINANCE_VISION_SOURCE_ID` (`binance:vision:aggTrades`) is distinct from live `binance:aggTrade`. The `pmkit-cloud` historical source likewise lacks original receipt-time provenance, and its export ordinals are not physical native frame numbers. The capture writer rejects these source IDs with `capture::CaptureError::UnknownReceiptTime`; `CaptureReplayReader` rejects their recorded entries with `CaptureReplayError::UnsupportedBinanceVisionArchive` (the current public variant name for both), surfaced by `CaptureReplaySource` as `DataSourceError::ReplayGap`. This is source-specific, not a blanket restriction on historical inputs: other sources with genuine original receipt-time provenance can be captured and replayed.

## Compatibility

Only schema 4 is supported by this tape reader. Older schemas (including v2/v3) fail with `RawCaptureError::UnsupportedSchemaVersion`; there are no RTDS/RTDK aliases, legacy-schema `serde` defaults, or implicit compatibility fallbacks. Moving an old capture to this format requires a new v4 recording; this implementation does not provide a transcoder.
