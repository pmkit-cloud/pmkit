use crate::{
    CexReferenceEnvelope, CexReferenceEvent, PmAccountEnvelope, PmMarketEnvelope,
    PolymarketReferenceEnvelope, StrategyFact, StreamMetadata,
};

/// A canonical ordering key derived from envelope metadata only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalSourceKey {
    /// PM market or account ordering identity.
    Pm {
        /// Provider timestamp in milliseconds.
        source_timestamp_ms: i64,
        /// Configured deterministic rank.
        canonical_source_rank: i64,
        /// Stable typed PM stream identity.
        stream_id: String,
        /// Connection generation.
        connection_epoch: i64,
        /// Frame position in the connection generation.
        frame_sequence: i64,
    },
    /// CEX aggregate-trade ordering identity.
    Cex {
        /// Exchange timestamp in milliseconds.
        source_timestamp_ms: i64,
        /// Configured deterministic rank.
        canonical_source_rank: i64,
        /// Exchange aggregate-trade identity.
        aggregate_trade_id: u64,
    },
    /// Polymarket RTDS reference ordering identity.
    Polymarket {
        /// Provider observation timestamp in milliseconds.
        source_timestamp_ms: i64,
        /// Configured deterministic rank.
        canonical_source_rank: i64,
        /// Stable RTDS symbol/window stream identity.
        stream_id: String,
        /// Connection generation.
        connection_epoch: i64,
        /// Frame position in the connection generation.
        frame_sequence: i64,
    },
}

impl PartialOrd for CanonicalSourceKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CanonicalSourceKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let ordering = self
            .timestamp_ms()
            .cmp(&other.timestamp_ms())
            .then_with(|| {
                self.canonical_source_rank()
                    .cmp(&other.canonical_source_rank())
            });
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
        match (self, other) {
            (
                Self::Pm {
                    stream_id: left_stream,
                    connection_epoch: left_epoch,
                    frame_sequence: left_frame,
                    ..
                },
                Self::Pm {
                    stream_id: right_stream,
                    connection_epoch: right_epoch,
                    frame_sequence: right_frame,
                    ..
                },
            ) => left_stream
                .cmp(right_stream)
                .then_with(|| left_epoch.cmp(right_epoch))
                .then_with(|| left_frame.cmp(right_frame)),
            (
                Self::Cex {
                    aggregate_trade_id: left_id,
                    ..
                },
                Self::Cex {
                    aggregate_trade_id: right_id,
                    ..
                },
            ) => left_id.cmp(right_id),
            (
                Self::Polymarket {
                    stream_id: left_stream,
                    connection_epoch: left_epoch,
                    frame_sequence: left_frame,
                    ..
                },
                Self::Polymarket {
                    stream_id: right_stream,
                    connection_epoch: right_epoch,
                    frame_sequence: right_frame,
                    ..
                },
            ) => left_stream
                .cmp(right_stream)
                .then_with(|| left_epoch.cmp(right_epoch))
                .then_with(|| left_frame.cmp(right_frame)),
            (Self::Pm { .. }, Self::Polymarket { .. } | Self::Cex { .. }) => {
                std::cmp::Ordering::Less
            }
            (Self::Polymarket { .. }, Self::Cex { .. }) => std::cmp::Ordering::Less,
            (Self::Cex { .. }, Self::Polymarket { .. } | Self::Pm { .. }) => {
                std::cmp::Ordering::Greater
            }
            (Self::Polymarket { .. }, Self::Pm { .. }) => std::cmp::Ordering::Greater,
        }
    }
}

impl CanonicalSourceKey {
    /// Returns the timestamp used for watermark eligibility.
    #[must_use]
    pub const fn timestamp_ms(&self) -> i64 {
        match self {
            Self::Pm {
                source_timestamp_ms,
                ..
            }
            | Self::Cex {
                source_timestamp_ms,
                ..
            }
            | Self::Polymarket {
                source_timestamp_ms,
                ..
            } => *source_timestamp_ms,
        }
    }

    const fn canonical_source_rank(&self) -> i64 {
        match self {
            Self::Pm {
                canonical_source_rank,
                ..
            }
            | Self::Cex {
                canonical_source_rank,
                ..
            }
            | Self::Polymarket {
                canonical_source_rank,
                ..
            } => *canonical_source_rank,
        }
    }
}

/// A transport envelope accepted only by the merge boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceEnvelope {
    /// A public PM market fact with raw transport identity.
    PmMarket(PmMarketEnvelope),
    /// An authenticated PM account fact with raw transport identity.
    PmAccount(PmAccountEnvelope),
    /// A CEX reference trade with raw transport identity.
    CexReference(CexReferenceEnvelope),
    /// A Polymarket-owned RTDS reference fact with transport metadata.
    PolymarketReference(PolymarketReferenceEnvelope),
}

impl SourceEnvelope {
    /// Returns the transport metadata retained by the merge boundary.
    #[must_use]
    pub const fn metadata(&self) -> &StreamMetadata {
        match self {
            Self::PmMarket(envelope) => &envelope.metadata,
            Self::PmAccount(envelope) => &envelope.metadata,
            Self::CexReference(envelope) => &envelope.metadata,
            Self::PolymarketReference(envelope) => &envelope.metadata,
        }
    }

    /// Returns the canonical ordering key for this envelope.
    #[must_use]
    pub fn canonical_key(&self) -> CanonicalSourceKey {
        let metadata = self.metadata();
        match self {
            Self::PmMarket(envelope) => CanonicalSourceKey::Pm {
                source_timestamp_ms: metadata.source_time_ms,
                canonical_source_rank: metadata.canonical_source_rank,
                stream_id: market_stream_id(envelope),
                connection_epoch: metadata.connection_epoch,
                frame_sequence: metadata.frame_sequence,
            },
            Self::PmAccount(envelope) => CanonicalSourceKey::Pm {
                source_timestamp_ms: metadata.source_time_ms,
                canonical_source_rank: metadata.canonical_source_rank,
                stream_id: format!("account:{}", envelope.portfolio),
                connection_epoch: metadata.connection_epoch,
                frame_sequence: metadata.frame_sequence,
            },
            Self::CexReference(CexReferenceEnvelope {
                fact:
                    CexReferenceEvent::Trade {
                        aggregate_trade_id, ..
                    },
                ..
            }) => CanonicalSourceKey::Cex {
                source_timestamp_ms: metadata.source_time_ms,
                canonical_source_rank: metadata.canonical_source_rank,
                aggregate_trade_id: *aggregate_trade_id,
            },
            Self::PolymarketReference(envelope) => CanonicalSourceKey::Polymarket {
                source_timestamp_ms: metadata.source_time_ms,
                canonical_source_rank: metadata.canonical_source_rank,
                stream_id: format!(
                    "polymarket:rtds:{}:{}",
                    envelope.fact.symbol, envelope.fact.window_s
                ),
                connection_epoch: metadata.connection_epoch,
                frame_sequence: metadata.frame_sequence,
            },
        }
    }

    /// Drops all transport metadata before a fact reaches a strategy.
    #[must_use]
    pub fn into_strategy_fact(self) -> StrategyFact {
        match self {
            Self::PmMarket(envelope) => StrategyFact::Market(envelope.fact),
            Self::PmAccount(envelope) => StrategyFact::Account(envelope.fact),
            Self::CexReference(envelope) => StrategyFact::Reference(envelope.fact),
            Self::PolymarketReference(envelope) => StrategyFact::PolymarketReference(envelope.fact),
        }
    }
}

fn market_stream_id(envelope: &PmMarketEnvelope) -> String {
    match &envelope.fact {
        crate::MarketEvent::BookUpdate {
            market, outcome, ..
        }
        | crate::MarketEvent::BestBidAsk {
            market, outcome, ..
        }
        | crate::MarketEvent::LastTrade {
            market, outcome, ..
        }
        | crate::MarketEvent::Fill {
            market, outcome, ..
        } => format!("market:{market}:{}", outcome.to_string().to_lowercase()),
        crate::MarketEvent::OrderAck { .. } | crate::MarketEvent::Tick { .. } => {
            format!("market-source:{}", envelope.metadata.source_id)
        }
    }
}
