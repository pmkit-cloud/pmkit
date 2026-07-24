use rust_decimal::Decimal;
use thiserror::Error;

use crate::{
    Address, CanonicalChainLog, CanonicalLogIdentity, ChainEvent, ContractRegistry, RawRpcLog,
};

const ERC20_TRANSFER_TOPIC: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a8df523b3ef";

/// A typed failure raised while decoding one raw provider log.
#[derive(Debug, Error)]
pub enum DecodeError {
    /// The raw log belongs to a chain outside the supplied registry.
    #[error("raw log belongs to an unsupported chain")]
    UnsupportedChain,
    /// The raw log contract is not accepted by the supplied registry.
    #[error("raw log contract is not registered for this event family")]
    UnregisteredContract,
    /// The topic has no verified decoder in this release.
    #[error("raw log topic is not supported: {topic}")]
    UnsupportedTopic {
        /// The first topic identifying the event.
        topic: String,
    },
    /// The raw log has an invalid ABI field.
    #[error("raw log is malformed: {message}")]
    Malformed {
        /// The validation detail.
        message: String,
    },
}

/// Decodes the verified standard ERC-20 subset of raw Polygon logs.
///
/// Unknown topics and unregistered contracts fail closed. Polymarket-specific
/// exchange, conditional-token, and ERC-1155 events remain outside this decoder
/// until their ABI signatures are verified and fixture-backed.
///
/// # Errors
///
/// Returns [`DecodeError`] when the chain, contract, topic, or ABI fields are
/// not part of the verified decoder boundary.
pub fn decode_raw_log(
    registry: &ContractRegistry,
    raw: &RawRpcLog,
) -> Result<CanonicalChainLog, DecodeError> {
    if raw.identity.chain_id != registry.chain_id {
        return Err(DecodeError::UnsupportedChain);
    }
    let topic = raw.topics.first().ok_or_else(|| DecodeError::Malformed {
        message: "missing event topic".into(),
    })?;
    let event = if topic.eq_ignore_ascii_case(ERC20_TRANSFER_TOPIC) {
        if raw.contract_address != registry.collateral {
            return Err(DecodeError::UnregisteredContract);
        }
        decode_erc20_transfer(raw)?
    } else {
        return Err(DecodeError::UnsupportedTopic {
            topic: topic.clone(),
        });
    };
    let log = CanonicalChainLog {
        identity: CanonicalLogIdentity {
            chain_id: raw.identity.chain_id,
            block_number: raw.identity.block_number,
            block_hash: raw.identity.block_hash.clone(),
            transaction_hash: raw.identity.transaction_hash.clone(),
            transaction_index: raw.identity.transaction_index,
            log_index: raw.identity.log_index,
        },
        contract_address: raw.contract_address.clone(),
        event,
    };
    if registry.accepts(&log) {
        Ok(log)
    } else {
        Err(DecodeError::UnregisteredContract)
    }
}

fn decode_erc20_transfer(raw: &RawRpcLog) -> Result<ChainEvent, DecodeError> {
    if raw.topics.len() != 3 {
        return Err(malformed("ERC-20 Transfer requires three topics"));
    }
    Ok(ChainEvent::CollateralTransfer {
        from: topic_address(&raw.topics[1])?,
        to: topic_address(&raw.topics[2])?,
        amount: word_decimal(&raw.data)?,
    })
}

fn topic_address(topic: &str) -> Result<Address, DecodeError> {
    let word = topic
        .strip_prefix("0x")
        .ok_or_else(|| malformed("topic is not hex"))?;
    if word.len() != 64 || !word.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(malformed("address topic is not a 32-byte word"));
    }
    Address::new(format!("0x{}", &word[24..])).map_err(|_| malformed("invalid address word"))
}

fn word_decimal(word: &str) -> Result<Decimal, DecodeError> {
    let word = abi_word(word)?;
    let value = u128::from_str_radix(word, 16).map_err(|error| malformed_parse("word", &error))?;
    Ok(Decimal::from(value))
}

fn abi_word(value: &str) -> Result<&str, DecodeError> {
    let word = value
        .strip_prefix("0x")
        .ok_or_else(|| malformed("word is not hex"))?;
    if word.len() != 64 || !word.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(malformed("ABI value is not one 32-byte word"));
    }
    Ok(word)
}

fn malformed(message: &str) -> DecodeError {
    DecodeError::Malformed {
        message: message.into(),
    }
}

fn malformed_parse(field: &str, error: &std::num::ParseIntError) -> DecodeError {
    DecodeError::Malformed {
        message: format!("{field} is invalid: {error}"),
    }
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::expect_used,
        reason = "typed fixture construction should fail loudly when malformed"
    )]

    use super::{DecodeError, ERC20_TRANSFER_TOPIC, decode_raw_log};
    use crate::{Address, ContractRegistry, ProviderIdentity, RawLogIdentity, RawRpcLog};

    const FROM: &str = "00000000000000000000000000000000000000000000000000000000000000aa";
    const TO: &str = "00000000000000000000000000000000000000000000000000000000000000bb";

    fn raw(contract_address: Address, topic: &str) -> RawRpcLog {
        RawRpcLog {
            identity: RawLogIdentity {
                provider: ProviderIdentity::new("fixture-rpc"),
                chain_id: crate::ChainId::POLYGON,
                block_number: 10,
                block_hash: "0xblock".into(),
                transaction_hash: "0xtx".into(),
                transaction_index: 0,
                log_index: 0,
            },
            contract_address,
            topics: vec![topic.into(), format!("0x{FROM}"), format!("0x{TO}")],
            data: format!("0x{:064x}", 42),
        }
    }

    #[test]
    fn decodes_registered_collateral_transfer() {
        // Given: a standard ERC-20 transfer emitted by the registered collateral contract.
        let registry = ContractRegistry::polygon();

        // When: the raw log crosses the decoder boundary.
        let result = decode_raw_log(
            &registry,
            &raw(registry.collateral.clone(), ERC20_TRANSFER_TOPIC),
        );

        // Then: only the existing typed collateral event is emitted.
        let log = result.expect("valid transfer fixture");
        assert!(
            matches!(log.event, crate::ChainEvent::CollateralTransfer { amount, .. } if amount == 42.into())
        );
    }

    #[test]
    fn rejects_unknown_topics_and_unregistered_contracts() {
        // Given: a standard topic with an unregistered emitter and an unknown topic.
        let registry = ContractRegistry::polygon();
        let unregistered = Address::new("0x0000000000000000000000000000000000000001")
            .expect("fixture address is valid");

        // When: both raw logs cross the decoder boundary.
        let wrong_contract = decode_raw_log(&registry, &raw(unregistered, ERC20_TRANSFER_TOPIC));
        let unknown_topic =
            decode_raw_log(&registry, &raw(registry.collateral.clone(), "0xunknown"));

        // Then: both are rejected before any guessed event is created.
        assert!(matches!(
            wrong_contract,
            Err(DecodeError::UnregisteredContract)
        ));
        assert!(matches!(
            unknown_topic,
            Err(DecodeError::UnsupportedTopic { .. })
        ));
    }
}
