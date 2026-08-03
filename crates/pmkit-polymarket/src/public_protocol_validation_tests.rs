use crate::{PublicProtocolError, decode_public_inbound};

const MARKET: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";

fn assert_malformed(cases: &[(&str, String)], detail: &'static str) {
    for (name, raw) in cases {
        let result = decode_public_inbound(raw.as_bytes());

        assert!(
            matches!(
                result,
                Err(PublicProtocolError::Malformed { detail: actual, raw: error_raw })
                    if actual == detail && error_raw == raw.as_bytes()
            ),
            "{name}"
        );
    }
}

#[test]
fn public_event_surface_rejects_non_string_event_type() {
    // Given: syntactically valid JSON with an invalid provider discriminator type.
    let raw = br#"{"event_type":1}"#;

    // When: it crosses the public protocol boundary.
    let result = decode_public_inbound(raw);

    // Then: the malformed event type has its own typed error class and raw evidence.
    assert!(matches!(
        result,
        Err(PublicProtocolError::Malformed {
            detail: "event_type is invalid",
            raw: error_raw,
        }) if error_raw == raw
    ));
}

#[test]
fn public_event_surface_rejects_missing_required_fields_for_every_known_market_event() {
    // Given: one otherwise-valid payload per known event, each missing its required payload field.
    let cases = [
        (
            "book asset_id",
            format!(
                r#"{{"event_type":"book","market":"{MARKET}","timestamp":"42","bids":[],"asks":[]}}"#
            ),
        ),
        (
            "price_change timestamp",
            format!(
                r#"{{"event_type":"price_change","market":"{MARKET}","price_changes":[{{"asset_id":"1","price":"0.5","side":"BUY"}}]}}"#
            ),
        ),
        (
            "last_trade_price price",
            format!(
                r#"{{"event_type":"last_trade_price","market":"{MARKET}","asset_id":"1","timestamp":"42"}}"#
            ),
        ),
        (
            "tick_size_change old_tick_size",
            format!(
                r#"{{"event_type":"tick_size_change","market":"{MARKET}","asset_id":"1","new_tick_size":"0.001","timestamp":"42"}}"#
            ),
        ),
        (
            "best_bid_ask spread",
            format!(
                r#"{{"event_type":"best_bid_ask","market":"{MARKET}","asset_id":"1","best_bid":"0.49","best_ask":"0.51","timestamp":"42"}}"#
            ),
        ),
        (
            "new_market question",
            format!(
                r#"{{"event_type":"new_market","id":"market-1","market":"{MARKET}","slug":"market-1","description":"Description","assets_ids":["1","2"],"outcomes":["Yes","No"],"timestamp":"42"}}"#
            ),
        ),
        (
            "market_resolved winning_asset_id",
            format!(
                r#"{{"event_type":"market_resolved","id":"market-1","market":"{MARKET}","assets_ids":["1","2"],"winning_outcome":"Yes","timestamp":"42"}}"#
            ),
        ),
    ];

    // When: the payloads cross the only public protocol boundary.
    // Then: malformed known frames cannot produce typed output.
    assert_malformed(&cases, "market event payload is invalid");
}

#[test]
fn public_event_surface_rejects_invalid_market_ids_for_every_known_market_event() {
    // Given: each known event has all required fields but an invalid condition identifier.
    let cases = [
        (
            "book market",
            r#"{"event_type":"book","market":"c","asset_id":"1","timestamp":"42","bids":[],"asks":[]}"#.to_owned(),
        ),
        (
            "price_change market",
            r#"{"event_type":"price_change","market":"c","timestamp":"42","price_changes":[{"asset_id":"1","price":"0.5","side":"BUY"}]}"#.to_owned(),
        ),
        (
            "last_trade_price market",
            r#"{"event_type":"last_trade_price","market":"c","asset_id":"1","price":"0.5","timestamp":"42"}"#.to_owned(),
        ),
        (
            "tick_size_change market",
            r#"{"event_type":"tick_size_change","market":"c","asset_id":"1","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"42"}"#.to_owned(),
        ),
        (
            "best_bid_ask market",
            r#"{"event_type":"best_bid_ask","market":"c","asset_id":"1","best_bid":"0.49","best_ask":"0.51","spread":"0.02","timestamp":"42"}"#.to_owned(),
        ),
        (
            "new_market market",
            r#"{"event_type":"new_market","id":"market-1","question":"Question","market":"c","slug":"market-1","description":"Description","assets_ids":["1","2"],"outcomes":["Yes","No"],"timestamp":"42"}"#.to_owned(),
        ),
        (
            "market_resolved market",
            r#"{"event_type":"market_resolved","id":"market-1","market":"c","assets_ids":["1","2"],"winning_asset_id":"1","winning_outcome":"Yes","timestamp":"42"}"#.to_owned(),
        ),
    ];

    // When: the invalid condition identifiers are decoded.
    // Then: no known market frame with an invalid identity becomes typed output.
    assert_malformed(&cases, "market event payload is invalid");
}

#[test]
fn public_event_surface_rejects_malformed_fields_for_every_known_market_event() {
    // Given: each known event contains an invalid type in an event-specific payload field.
    let cases = [
        (
            "book asset_id",
            format!(
                r#"{{"event_type":"book","market":"{MARKET}","asset_id":true,"timestamp":"42","bids":[],"asks":[]}}"#
            ),
        ),
        (
            "price_change price",
            format!(
                r#"{{"event_type":"price_change","market":"{MARKET}","timestamp":"42","price_changes":[{{"asset_id":"1","price":true,"side":"BUY"}}]}}"#
            ),
        ),
        (
            "last_trade_price price",
            format!(
                r#"{{"event_type":"last_trade_price","market":"{MARKET}","asset_id":"1","price":true,"timestamp":"42"}}"#
            ),
        ),
        (
            "tick_size_change old_tick_size",
            format!(
                r#"{{"event_type":"tick_size_change","market":"{MARKET}","asset_id":"1","old_tick_size":true,"new_tick_size":"0.001","timestamp":"42"}}"#
            ),
        ),
        (
            "best_bid_ask best_bid",
            format!(
                r#"{{"event_type":"best_bid_ask","market":"{MARKET}","asset_id":"1","best_bid":true,"best_ask":"0.51","spread":"0.02","timestamp":"42"}}"#
            ),
        ),
        (
            "new_market assets_ids",
            format!(
                r#"{{"event_type":"new_market","id":"market-1","question":"Question","market":"{MARKET}","slug":"market-1","description":"Description","assets_ids":[true],"outcomes":["Yes","No"],"timestamp":"42"}}"#
            ),
        ),
        (
            "market_resolved winning_asset_id",
            format!(
                r#"{{"event_type":"market_resolved","id":"market-1","market":"{MARKET}","assets_ids":["1","2"],"winning_asset_id":true,"winning_outcome":"Yes","timestamp":"42"}}"#
            ),
        ),
    ];

    // When: malformed typed fields cross the protocol boundary.
    // Then: every malformed known frame remains an error with exact raw evidence.
    assert_malformed(&cases, "market event payload is invalid");
}

#[test]
fn public_event_surface_distinguishes_missing_market_identity_for_known_events() {
    // Given: every recognized market event has a payload but no market identity.
    let cases = [
        (
            "book",
            r#"{"event_type":"book","asset_id":"1","timestamp":"42","bids":[],"asks":[]}"#.to_owned(),
        ),
        (
            "price_change",
            r#"{"event_type":"price_change","timestamp":"42","price_changes":[{"asset_id":"1","price":"0.5","side":"BUY"}]}"#.to_owned(),
        ),
        (
            "last_trade_price",
            r#"{"event_type":"last_trade_price","asset_id":"1","price":"0.5","timestamp":"42"}"#.to_owned(),
        ),
        (
            "tick_size_change",
            r#"{"event_type":"tick_size_change","asset_id":"1","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"42"}"#.to_owned(),
        ),
        (
            "best_bid_ask",
            r#"{"event_type":"best_bid_ask","asset_id":"1","best_bid":"0.49","best_ask":"0.51","spread":"0.02","timestamp":"42"}"#.to_owned(),
        ),
        (
            "new_market",
            r#"{"event_type":"new_market","id":"market-1","question":"Question","slug":"market-1","description":"Description","assets_ids":["1","2"],"outcomes":["Yes","No"],"timestamp":"42"}"#.to_owned(),
        ),
        (
            "market_resolved",
            r#"{"event_type":"market_resolved","id":"market-1","assets_ids":["1","2"],"winning_asset_id":"1","winning_outcome":"Yes","timestamp":"42"}"#.to_owned(),
        ),
    ];

    // When: market identity is absent at the boundary.
    // Then: the error distinguishes it from the malformed event payload class.
    assert_malformed(&cases, "market event lacks market identity");
}
