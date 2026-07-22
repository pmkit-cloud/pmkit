use crate::feed::{FeedMode, MergedFeed, SourceDefinition};
use pmkit_core::MarketId;
use pmkit_data::{DataSourceError, SourceSignal};
use pmkit_event::{
    CexReferenceEnvelope, CexReferenceEvent, MarketEvent, PmMarketEnvelope, SourceEnvelope,
    StrategyFact, StreamMetadata,
};
use pmkit_market::{Asset, Exchange, Outcome};
use rust_decimal::Decimal;

fn metadata(source_id: &str, timestamp_ms: i64, rank: i64, sequence: i64) -> StreamMetadata {
    StreamMetadata {
        schema_version: 1,
        source_id: source_id.to_owned(),
        source_time_ms: timestamp_ms,
        canonical_source_rank: rank,
        receipt_time_ms: timestamp_ms,
        connection_id: source_id.to_owned(),
        connection_epoch: 0,
        frame_sequence: sequence,
        ingest_sequence: 999,
    }
}

fn pm(timestamp_ms: i64, sequence: i64) -> Result<SourceSignal, Box<dyn std::error::Error>> {
    Ok(SourceSignal::Data(Box::new(SourceEnvelope::PmMarket(
        PmMarketEnvelope {
            metadata: metadata("pm", timestamp_ms, 1, sequence),
            fact: MarketEvent::BookUpdate {
                market: MarketId::new("btc-5m")?,
                outcome: Outcome::Up,
                bids: vec![(Decimal::new(49, 2), Decimal::ONE)],
                asks: vec![(Decimal::new(51, 2), Decimal::ONE)],
                timestamp_ms,
            },
        },
    ))))
}

fn cex(timestamp_ms: i64, aggregate_trade_id: u64) -> SourceSignal {
    SourceSignal::Data(Box::new(SourceEnvelope::CexReference(
        CexReferenceEnvelope {
            metadata: metadata("binance", timestamp_ms, 2, 0),
            fact: CexReferenceEvent::Trade {
                asset: Asset::Btc,
                exchange: Exchange::Binance,
                aggregate_trade_id,
                price: Decimal::new(100_000, 2),
                qty: Decimal::ONE,
                is_buyer_maker: false,
                timestamp_ms,
            },
        },
    )))
}

#[tokio::test]
async fn deterministic_source_merge_matches_all_modes() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = vec![
        SourceDefinition::finite(
            "pm",
            vec![pm(10, 1)?, SourceSignal::Watermark(20), SourceSignal::Eof],
        ),
        SourceDefinition::finite(
            "binance",
            vec![cex(10, 7), SourceSignal::Watermark(20), SourceSignal::Eof],
        ),
    ];
    let mut expected = None;
    for mode in [FeedMode::Backtest, FeedMode::Paper, FeedMode::LiveFixture] {
        let facts = MergedFeed::from_fixture(mode, fixture.clone(), Some(20))
            .collect()
            .await?;
        if let Some(expected) = &expected {
            assert_eq!(&facts, expected);
        } else {
            expected = Some(facts);
        }
    }
    assert!(matches!(
        expected.as_deref(),
        Some([StrategyFact::Market(_), StrategyFact::Reference(_)])
    ));
    Ok(())
}

#[tokio::test]
async fn source_gap_aborts_before_strategy_evaluation() -> Result<(), Box<dyn std::error::Error>> {
    for signals in [
        vec![pm(10, 1)?, SourceSignal::Watermark(10), SourceSignal::Eof],
        vec![SourceSignal::Watermark(20), pm(10, 1)?, SourceSignal::Eof],
    ] {
        let result = MergedFeed::from_fixture(
            FeedMode::Backtest,
            vec![SourceDefinition::finite("pm", signals)],
            Some(20),
        )
        .collect()
        .await;
        assert!(matches!(result, Err(DataSourceError::ReplayGap { .. })));
    }
    Ok(())
}
