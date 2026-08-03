use std::{cell::RefCell, collections::BTreeSet};

use crate::{
    DiscoveryError, GammaMarket, GammaOutcome, RecurringFamily, discover_subscription_snapshot,
};

fn market(index: usize) -> GammaMarket {
    let time_slot = i64::try_from(index).unwrap_or_default();
    GammaMarket {
        market_id: format!("market-{index:05}"),
        condition_id: format!("condition-{index:05}"),
        open_time_ms: time_slot * 300_000,
        close_time_ms: (time_slot + 1) * 300_000,
        active: true,
        family: Some(RecurringFamily::new("btc-5m", Some("BTC"), Some("5m"))),
        outcomes: vec![
            GammaOutcome::new("Up", format!("up-{index}")),
            GammaOutcome::new("Down", format!("down-{index}")),
        ],
    }
}

#[test]
fn discovery_subscription_exact_multiple_then_short_page_is_complete_and_deterministic()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: an exact full-page boundary followed by the required short terminator page.
    let requests = RefCell::new(Vec::new());
    let snapshot = discover_subscription_snapshot(2, 2, |request| {
        requests.borrow_mut().push(request.offset);
        Ok(match request.offset {
            0 => vec![market(0), market(1)],
            2 => vec![market(2), market(3)],
            4 => vec![market(4)],
            _ => vec![],
        })
    })?;

    // When: discovery has consumed every page through its short final page.
    let repeated = discover_subscription_snapshot(2, 2, |request| {
        Ok(match request.offset {
            0 => vec![market(0), market(1)],
            2 => vec![market(2), market(3)],
            4 => vec![market(4)],
            _ => vec![],
        })
    })?;

    // Then: all outcomes are retained and both replicas have the same custom-feature shards.
    assert_eq!(*requests.borrow(), vec![0, 2, 4]);
    assert_eq!(snapshot.markets().len(), 5);
    eprintln!("exact-multiple snapshot digest: {}", snapshot.digest());
    assert_eq!(snapshot.digest(), repeated.digest());
    assert_eq!(snapshot.lane_a().shards(), snapshot.lane_b().shards());
    assert!(
        snapshot
            .lane_a()
            .shards()
            .iter()
            .all(|shard| shard.subscription().custom_feature_enabled())
    );
    Ok(())
}

#[test]
fn discovery_subscription_rejects_partial_snapshot_after_nonzero_failure_and_invalid_pages() {
    // Given: each fixture becomes invalid only after the first complete page.
    let transport = discover_subscription_snapshot(2, 2, |request| match request.offset {
        0 => Ok(vec![market(0), market(1)]),
        2 => Ok(vec![market(2), market(3)]),
        _ => Err(DiscoveryError::Unavailable),
    });
    let repeated = discover_subscription_snapshot(2, 2, |_| Ok(vec![market(0), market(1)]));
    let duplicate = discover_subscription_snapshot(2, 2, |request| {
        Ok(if request.offset == 0 {
            vec![market(0), market(1)]
        } else {
            vec![market(1)]
        })
    });
    let missing = discover_subscription_snapshot(2, 2, |request| {
        Ok(if request.offset == 0 {
            let mut invalid = market(0);
            invalid.family = None;
            vec![invalid]
        } else {
            vec![]
        })
    });
    let malformed = discover_subscription_snapshot(2, 2, |request| {
        let mut invalid = market(request.offset);
        invalid.condition_id.clear();
        Ok(vec![invalid])
    });

    // When/Then: an invalid attempt has no publishable partial snapshot.
    assert!(matches!(
        transport,
        Err(DiscoveryError::IncompletePagination { offset: 4 })
    ));
    assert!(matches!(repeated, Err(DiscoveryError::RepeatedPage { .. })));
    assert!(matches!(
        duplicate,
        Err(DiscoveryError::DuplicateMarketId { .. })
    ));
    assert!(matches!(
        missing,
        Err(DiscoveryError::MissingFamilyMetadata { .. })
    ));
    assert!(matches!(
        malformed,
        Err(DiscoveryError::MalformedPage { .. })
    ));
}

#[test]
fn discovery_subscription_has_no_silent_cap_and_preserves_recurring_identity()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: 3,012 concrete BTC five-minute markets in fixed pages.
    let markets = (0..3_012).map(market).collect::<Vec<_>>();
    let snapshot = discover_subscription_snapshot(100, 64, |request| {
        Ok(markets
            .iter()
            .skip(request.offset)
            .take(request.limit)
            .cloned()
            .collect())
    })?;
    let repeated = discover_subscription_snapshot(100, 64, |request| {
        Ok(markets
            .iter()
            .skip(request.offset)
            .take(request.limit)
            .cloned()
            .collect())
    })?;

    // When/Then: all identities remain concrete while their structured family is stable.
    assert_eq!(snapshot.markets().len(), 3_012);
    assert_eq!(snapshot.digest(), repeated.digest());
    let first_twelve = snapshot.markets().iter().take(12).collect::<Vec<_>>();
    assert!(first_twelve.iter().all(|entry| {
        entry.family().is_some_and(|family| {
            family.series_id() == "btc-5m"
                && family.asset() == Some("BTC")
                && family.duration() == Some("5m")
        })
    }));
    assert_eq!(
        first_twelve
            .iter()
            .map(|entry| entry.market_id.as_str())
            .collect::<BTreeSet<_>>()
            .len(),
        12
    );
    assert_eq!(
        first_twelve
            .iter()
            .map(|entry| entry.condition_id.as_str())
            .collect::<BTreeSet<_>>()
            .len(),
        12
    );
    assert!(first_twelve.windows(2).all(|window| {
        window[0].close_time_ms == window[1].open_time_ms
            && window[0].close_time_ms - window[0].open_time_ms == 300_000
    }));
    assert_eq!(
        first_twelve
            .iter()
            .flat_map(|entry| entry.outcomes().iter().map(GammaOutcome::token_id))
            .collect::<BTreeSet<_>>()
            .len(),
        24
    );
    assert!(first_twelve.iter().all(|entry| {
        entry.outcomes()[0].outcome_id() == "Up" && entry.outcomes()[1].outcome_id() == "Down"
    }));
    assert_eq!(snapshot.markets()[0].outcomes()[0].token_id(), "up-0");
    Ok(())
}

#[test]
fn discovery_subscription_rejects_duplicate_outcome_ids() {
    // Given: a Gamma page whose concrete market repeats one outcome identity with another token.
    let result = discover_subscription_snapshot(2, 2, |_| {
        let mut duplicate = market(0);
        duplicate
            .outcomes
            .push(GammaOutcome::new("Up", "third-token"));
        Ok(vec![duplicate])
    });

    // When/Then: the malformed active market cannot produce a partial subscription plan.
    assert!(matches!(
        result,
        Err(DiscoveryError::DuplicateOutcomeId { .. })
    ));
}
