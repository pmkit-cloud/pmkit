use crate::{
    PublicInboundFrame, PublicMarketEvent, PublicOutboundFrame, PublicProtocolError,
    decode_public_inbound, encode_public_outbound,
};

#[test]
fn public_event_surface_round_trips_every_public_frame_and_preserves_raw_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: every public control and market event type from the unauthenticated wire protocol.
    let frames = [
        br#"{"event_type":"subscription_update","assets_ids":["1"]}"#.as_slice(),
        br#"{"event_type":"ping"}"#.as_slice(),
        br#"{"event_type":"pong"}"#.as_slice(),
        br#"{"event_type":"book","market":"c","asset_id":"1"}"#.as_slice(),
        br#"{"event_type":"price_change","market":"c","price_changes":[{"asset_id":"1"}]}"#
            .as_slice(),
        br#"{"event_type":"last_trade_price","market":"c","asset_id":"1"}"#.as_slice(),
        br#"{"event_type":"tick_size_change","market":"c","asset_id":"1"}"#.as_slice(),
        br#"{"event_type":"best_bid_ask","market":"c","asset_id":"1"}"#.as_slice(),
        br#"{"event_type":"new_market","market":"c","assets_ids":["1"]}"#.as_slice(),
        br#"{"event_type":"market_resolved","market":"c","assets_ids":["1"]}"#.as_slice(),
    ];

    // When: each wire payload crosses the one public decoder seam.
    let decoded = frames
        .iter()
        .map(|raw| decode_public_inbound(raw))
        .collect::<Result<Vec<_>, _>>()?;

    // Then: every typed frame keeps byte-identical evidence and outbound subscriptions opt in.
    assert!(
        decoded
            .iter()
            .zip(frames)
            .all(|(frame, raw)| frame.raw() == raw)
    );
    let market_events = decoded
        .iter()
        .filter_map(|frame| match frame {
            PublicInboundFrame::Market { event, .. } => Some(event),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        market_events,
        vec![
            &PublicMarketEvent::OrderbookSnapshot,
            &PublicMarketEvent::PriceChange,
            &PublicMarketEvent::LastTradePrice,
            &PublicMarketEvent::TickSizeChange,
            &PublicMarketEvent::BestBidAsk,
            &PublicMarketEvent::NewMarket,
            &PublicMarketEvent::MarketResolved,
        ]
    );
    let subscription = encode_public_outbound(&PublicOutboundFrame::SubscriptionUpdate(
        decoded[0].subscription_update()?.clone(),
    ))?;
    assert!(
        subscription
            .windows(b"\"custom_feature_enabled\":true".len())
            .any(|window| window == b"\"custom_feature_enabled\":true")
    );
    Ok(())
}

#[test]
fn public_event_surface_encodes_outbound_ping_and_pong() -> Result<(), Box<dyn std::error::Error>> {
    // Given: both client keepalive control directions.
    let frames = [
        (PublicOutboundFrame::Ping, "ping"),
        (PublicOutboundFrame::Pong, "pong"),
    ];

    // When: each typed control frame is serialized at the public protocol seam.
    let encoded = frames
        .iter()
        .map(|(frame, _)| encode_public_outbound(frame))
        .collect::<Result<Vec<_>, _>>()?;

    // Then: wire payloads preserve the provider control discriminator.
    for ((_, event_type), raw) in frames.iter().zip(encoded) {
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&raw)?["event_type"],
            *event_type
        );
    }
    Ok(())
}

#[test]
fn public_event_surface_retains_unknown_control_but_blocks_unknown_market_event()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: one unknown control payload and one unknown payload carrying a market identity.
    let control = br#"{"event_type":"server_notice"}"#;
    let market = br#"{"event_type":"future_market_event","market":"c","asset_id":"1"}"#;

    // When: they are decoded through the raw protocol boundary.
    let control_frame = decode_public_inbound(control)?;
    let market_result = decode_public_inbound(market);

    // Then: raw control evidence is retained, but a market-bearing unknown creates a typed gap.
    assert!(matches!(
        control_frame,
        PublicInboundFrame::UnknownControl { ref event_type, ref raw }
            if event_type == "server_notice" && raw == control
    ));
    assert!(
        matches!(market_result, Err(PublicProtocolError::UnsupportedMarketEvent { raw, .. }) if raw == market)
    );
    Ok(())
}

#[test]
fn public_event_surface_rejects_malformed_json() {
    // Given: bytes that cannot cross the JSON trust boundary.
    let raw = b"{";

    // When: the public decoder sees malformed JSON.
    let result = decode_public_inbound(raw);

    // Then: typed output is withheld while the exact original bytes remain in the error.
    assert!(matches!(
        result,
        Err(PublicProtocolError::Malformed { detail: "not JSON", raw: error_raw }) if error_raw == raw
    ));
}

#[test]
fn public_event_surface_rejects_missing_event_type() {
    // Given: valid JSON with no provider frame discriminator.
    let raw = br#"{"market":"c"}"#;

    // When: the public decoder receives the frame.
    let result = decode_public_inbound(raw);

    // Then: no typed frame is returned and the original evidence is retained.
    assert!(matches!(
        result,
        Err(PublicProtocolError::Malformed { detail: "missing event_type", raw: error_raw }) if error_raw == raw
    ));
}

#[test]
fn public_event_surface_rejects_malformed_subscription_assets() {
    // Given: a subscription update with a non-string assets array member.
    let raw = br#"{"event_type":"subscription_update","assets_ids":[1]}"#;

    // When: the public decoder processes the inbound update.
    let result = decode_public_inbound(raw);

    // Then: a typed subscription is not exposed from malformed external data.
    assert!(matches!(
        result,
        Err(PublicProtocolError::Malformed { detail: "subscription assets_ids are invalid", raw: error_raw }) if error_raw == raw
    ));
}

#[test]
fn public_event_surface_rejects_market_event_without_market_identity() {
    // Given: a known market event that lacks its required market identity.
    let raw = br#"{"event_type":"book","asset_id":"1"}"#;

    // When: the public decoder processes the inbound event.
    let result = decode_public_inbound(raw);

    // Then: the frame produces no typed market event and preserves its evidence.
    assert!(matches!(
        result,
        Err(PublicProtocolError::Malformed { detail: "market event lacks market identity", raw: error_raw }) if error_raw == raw
    ));
}
