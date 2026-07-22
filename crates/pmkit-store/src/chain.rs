use thiserror::Error;

use crate::log::{CanonicalChainLog, ChainEvent};

/// The only supported onchain network for `PMKit` wallet reconstruction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChainId(u64);

impl ChainId {
    /// Polygon mainnet, the only chain accepted by the Polymarket registry.
    pub const POLYGON: Self = Self(137);

    /// Returns the EVM chain identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A normalized EVM address suitable for stable equality and storage.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Address(String);

impl Address {
    /// Parses a 20-byte hex EVM address and normalizes it to lowercase.
    ///
    /// # Errors
    ///
    /// Returns [`AddressError`] when `value` is not a 0x-prefixed 20-byte hex address.
    pub fn new(value: impl Into<String>) -> Result<Self, AddressError> {
        let value = value.into().to_ascii_lowercase();
        let is_valid = value.len() == 42
            && value.starts_with("0x")
            && value.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit);
        if !is_valid {
            return Err(AddressError);
        }
        Ok(Self(value))
    }

    /// Returns the normalized hexadecimal address.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Address parse failure.
#[derive(Debug, Clone, Copy, Error)]
#[error("address must be a 0x-prefixed 20-byte hex value")]
pub struct AddressError;

/// Optional historical V1 exchange addresses for an explicit backfill only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyV1Contracts {
    /// The historical V1 CTF exchange address.
    pub ctf_exchange: Address,
    /// The historical V1 negative-risk exchange address.
    pub neg_risk_exchange: Address,
}

/// The explicit Polygon contract registry used to validate parsed logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractRegistry {
    /// The registry chain, always Polygon mainnet.
    pub chain_id: ChainId,
    /// The pUSD proxy used as CTF collateral.
    pub collateral: Address,
    /// The Polymarket `ConditionalTokens` contract.
    pub conditional_tokens: Address,
    /// The current V2 CTF exchange.
    pub ctf_exchange: Address,
    /// The current V2 negative-risk exchange.
    pub neg_risk_exchange: Address,
    legacy_v1: Option<LegacyV1Contracts>,
}

impl ContractRegistry {
    /// Returns the current official Polygon Polymarket contract registry.
    #[must_use]
    pub fn polygon() -> Self {
        Self {
            chain_id: ChainId::POLYGON,
            collateral: Address("0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb".into()),
            conditional_tokens: Address("0x4d97dcd97ec945f40cf65f87097ace5ea0476045".into()),
            ctf_exchange: Address("0xe111180000d2663c0091e4f400237545b87b996b".into()),
            neg_risk_exchange: Address("0xe2222d279d744050d28e00520010520000310f59".into()),
            legacy_v1: None,
        }
    }

    /// Enables explicitly supplied historical V1 exchange addresses for a backfill.
    #[must_use]
    pub fn with_legacy_v1(mut self, legacy_v1: LegacyV1Contracts) -> Self {
        self.legacy_v1 = Some(legacy_v1);
        self
    }

    /// Returns whether this registry accepts the log's chain, contract, and event family.
    #[must_use]
    pub fn accepts(&self, log: &CanonicalChainLog) -> bool {
        if log.identity.chain_id != self.chain_id {
            return false;
        }
        match &log.event {
            ChainEvent::CollateralTransfer { .. } => log.contract_address == self.collateral,
            ChainEvent::OutcomeTransferSingle { .. }
            | ChainEvent::OutcomeTransferBatch { .. }
            | ChainEvent::PositionSplit { .. }
            | ChainEvent::PositionsMerge { .. }
            | ChainEvent::PayoutRedemption { .. } => {
                log.contract_address == self.conditional_tokens
            }
            ChainEvent::OrderFilled { .. }
            | ChainEvent::OrdersMatched { .. }
            | ChainEvent::FeeCharged { .. } => {
                log.contract_address == self.ctf_exchange
                    || log.contract_address == self.neg_risk_exchange
                    || self.legacy_v1.as_ref().is_some_and(|legacy| {
                        log.contract_address == legacy.ctf_exchange
                            || log.contract_address == legacy.neg_risk_exchange
                    })
            }
        }
    }
}
