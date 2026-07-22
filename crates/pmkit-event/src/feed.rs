use crate::{
    CexReferenceEnvelope, CexReferenceEvent, PmAccountEnvelope, PmMarketEnvelope, StrategyFact,
    StreamMetadata,
};

/// A canonical ordering key derived from envelope metadata only.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum CanonicalSourceKey {
    /// PM market or account ordering identity.
    Pm {
        /// Provider timestamp in milliseconds.
        source_timestamp_ms: i64,
        /// Configured deterministic rank.
        canonical_source_rank: i64,
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
            } => *source_timestamp_ms,
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
}

impl SourceEnvelope {
    /// Returns the transport metadata retained by the merge boundary.
    #[must_use]
    pub const fn metadata(&self) -> &StreamMetadata {
        match self {
            Self::PmMarket(envelope) => &envelope.metadata,
            Self::PmAccount(envelope) => &envelope.metadata,
            Self::CexReference(envelope) => &envelope.metadata,
        }
    }

    /// Returns the canonical key, rejecting unsupported CEX reference shapes.
    #[must_use]
    pub const fn canonical_key(&self) -> Option<CanonicalSourceKey> {
        let metadata = self.metadata();
        match self {
            Self::PmMarket(_) | Self::PmAccount(_) => Some(CanonicalSourceKey::Pm {
                source_timestamp_ms: metadata.source_time_ms,
                canonical_source_rank: metadata.canonical_source_rank,
                connection_epoch: metadata.connection_epoch,
                frame_sequence: metadata.frame_sequence,
            }),
            Self::CexReference(CexReferenceEnvelope {
                fact:
                    CexReferenceEvent::Trade {
                        aggregate_trade_id, ..
                    },
                ..
            }) => Some(CanonicalSourceKey::Cex {
                source_timestamp_ms: metadata.source_time_ms,
                canonical_source_rank: metadata.canonical_source_rank,
                aggregate_trade_id: *aggregate_trade_id,
            }),
            Self::CexReference(CexReferenceEnvelope {
                fact: CexReferenceEvent::BestBidOffer { .. },
                ..
            }) => None,
        }
    }

    /// Drops all transport metadata before a fact reaches a strategy.
    #[must_use]
    pub fn into_strategy_fact(self) -> StrategyFact {
        match self {
            Self::PmMarket(envelope) => StrategyFact::Market(envelope.fact),
            Self::PmAccount(envelope) => StrategyFact::Account(envelope.fact),
            Self::CexReference(envelope) => StrategyFact::Reference(envelope.fact),
        }
    }
}
