use super::*;

type ReceiptActivationObservations = Arc<Mutex<Vec<(i64, Decimal, Option<Decimal>)>>>;

struct ReceiptActivationProbe {
    placed: bool,
    observations: ReceiptActivationObservations,
    orders: Arc<Mutex<Vec<(Decimal, Decimal, bool)>>>,
}

#[derive(Clone)]
struct ReceiptActivationProbeFactory {
    observations: ReceiptActivationObservations,
    orders: Arc<Mutex<Vec<(Decimal, Decimal, bool)>>>,
}

impl Strategy for ReceiptActivationProbe {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        let quantity = context
            .positions
            .iter()
            .map(|position| position.qty)
            .sum::<Decimal>();
        let average_entry = context.positions.first().map(|position| position.avg_entry);
        self.observations
            .lock()
            .map_err(|_| StrategyError {
                message: "receipt activation observation lock poisoned".into(),
            })?
            .push((context.now.as_millis(), quantity, average_entry));

        if self.placed {
            return Ok(Actions::none());
        }
        self.placed = true;
        let price = Decimal::new(4, 1);
        let quantity = Decimal::ONE;
        let post_only = true;
        self.orders
            .lock()
            .map_err(|_| StrategyError {
                message: "receipt activation order lock poisoned".into(),
            })?
            .push((price, quantity, post_only));
        Ok(Actions::place(pmkit_exec::PlaceOrder {
            market: context.market.clone(),
            outcome: Outcome::Up,
            side: pmkit_book::Side::Buy,
            price,
            qty: quantity,
            post_only,
            tif: pmkit_exec::TimeInForce::Gtc,
        }))
    }
}

impl StrategyFactory for ReceiptActivationProbeFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(ReceiptActivationProbe {
            placed: false,
            observations: Arc::clone(&self.observations),
            orders: Arc::clone(&self.orders),
        }))
    }
}

#[tokio::test]
async fn recorded_maker_activation_uses_receipt_clock() -> Result<(), Box<dyn std::error::Error>> {
    let market = MarketId::new("btc-5m")?;
    let mut writer = RawCaptureWriter::new(Vec::new());
    writer.append(native_book_capture(
        &market,
        100,
        1_000,
        Decimal::new(5, 1),
        0,
    )?)?;
    writer.append(native_book_capture(
        &market,
        10_000,
        1_010,
        Decimal::new(35, 2),
        1,
    )?)?;
    writer.append(native_book_capture(
        &market,
        20_000,
        1_060,
        Decimal::new(35, 2),
        2,
    )?)?;

    let observations = Arc::new(Mutex::new(Vec::new()));
    let orders = Arc::new(Mutex::new(Vec::new()));
    let replay = ReplaySpec::new(
        Arc::new(ScriptedHistory { ticks: Vec::new() }),
        "2026-01-01T00:00:00Z".parse()?,
        "2026-02-01T00:00:00Z".parse()?,
        EvidenceRequirement::CorroboratedOnly,
        RetrievalWait::ReturnPending,
    );
    let run = BacktestRun::new(
        RunId::new("bt-recorded-receipt-activation")?,
        PortfolioId::new("research")?,
        replay,
        Money::usdc(1_000),
        risk()?,
        ConservativeV1Config {
            activation_latency: Duration::from_millis(50),
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("receipt-activation-probe")?,
        market.clone(),
        Arc::new(ReceiptActivationProbeFactory {
            observations: Arc::clone(&observations),
            orders: Arc::clone(&orders),
        }),
    ));
    let tokens = MarketTokens::new(market.clone(), "1".parse()?, "2".parse()?);
    let report = crate::RecordedBacktest::replay_capture(
        &run,
        crate::capture_replay::CaptureReplaySource::new_with_market_tokens(
            Cursor::new(writer.into_inner()),
            HashMap::from([(market, tokens)]),
        ),
    )
    .drive(None)
    .await?;

    assert_eq!(report.events_processed, 3);
    assert_eq!(report.fills, 1);
    assert_eq!(
        observations
            .lock()
            .map_err(|_| "receipt activation observation lock poisoned")?
            .as_slice(),
        &[
            (100, Decimal::ZERO, None),
            (10_000, Decimal::ZERO, None),
            (20_000, Decimal::ONE, Some(Decimal::new(4, 1))),
        ]
    );
    assert_eq!(
        orders
            .lock()
            .map_err(|_| "receipt activation order lock poisoned")?
            .as_slice(),
        &[(Decimal::new(4, 1), Decimal::ONE, true)]
    );
    Ok(())
}
