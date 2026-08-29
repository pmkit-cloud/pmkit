// allow: SIZE_OK — verified ABI decoding and its signature fixtures are constrained to this file.
use rust_decimal::Decimal;
use thiserror::Error;

use crate::{
    Address, CanonicalChainLog, CanonicalLogIdentity, ChainEvent, ContractRegistry,
    OutcomeTokenAmount, ProviderIdentity, RawRpcLog, TradeSide,
};

const ERC20_TRANSFER_TOPIC: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a8df523b3ef";
const ERC1155_TRANSFER_SINGLE_SIGNATURE_FIXTURE: (&str, &str) = (
    "TransferSingle(address,address,address,uint256,uint256)",
    "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62",
);
const ERC1155_TRANSFER_BATCH_SIGNATURE_FIXTURE: (&str, &str) = (
    "TransferBatch(address,address,address,uint256[],uint256[])",
    "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb",
);
const CTF_POSITION_SPLIT_SIGNATURE_FIXTURE: (&str, &str) = (
    "PositionSplit(address,address,bytes32,bytes32,uint256[],uint256)",
    "0x2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298",
);
const CTF_POSITIONS_MERGE_SIGNATURE_FIXTURE: (&str, &str) = (
    "PositionsMerge(address,address,bytes32,bytes32,uint256[],uint256)",
    "0x6f13ca62553fcc2bcd2372180a43949c1e4cebba603901ede2f4e14f36b282ca",
);
const EXCHANGE_V1_ORDER_FILLED_SIGNATURE_FIXTURE: (&str, &str) = (
    "OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)",
    "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6",
);
const EXCHANGE_V2_ORDER_FILLED_SIGNATURE_FIXTURE: (&str, &str) = (
    "OrderFilled(bytes32,address,address,uint8,uint256,uint256,uint256,uint256,bytes32,bytes32)",
    "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee",
);
const DECIMAL_MAX_MANTISSA: u128 = 79_228_162_514_264_337_593_543_950_335;
const ZERO_ASSET_ID: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// A typed failure raised while decoding one raw provider log.
#[derive(Debug, Error)]
pub enum DecodeError {
    /// The raw log belongs to a chain outside the supplied registry.
    #[error("provider {provider:?} returned a raw log for an unsupported chain")]
    UnsupportedChain {
        /// The stable identity of the provider that returned the log.
        provider: ProviderIdentity,
    },
    /// The raw log contract is not accepted by the supplied registry.
    #[error("provider {provider:?} returned a raw log from an unregistered contract")]
    UnregisteredContract {
        /// The stable identity of the provider that returned the log.
        provider: ProviderIdentity,
    },
    /// The topic has no verified decoder in this release.
    #[error("provider {provider:?} returned a raw log with an unsupported topic")]
    UnsupportedTopic {
        /// The stable identity of the provider that returned the log.
        provider: ProviderIdentity,
    },
    /// The raw log has an invalid ABI field.
    #[error("raw log is malformed: {message}")]
    Malformed {
        /// The validation detail.
        message: String,
    },
}

/// Decodes the fixture-verified ERC-20, ERC-1155, CTF, and exchange log subset.
///
/// Unknown topics, unregistered contracts, and exchange topics from the wrong
/// contract version fail closed.
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
        return Err(DecodeError::UnsupportedChain {
            provider: raw.identity.provider.clone(),
        });
    }
    let topic = raw.topics.first().ok_or_else(|| DecodeError::Malformed {
        message: "missing event topic".into(),
    })?;
    let event = if topic.eq_ignore_ascii_case(ERC20_TRANSFER_TOPIC) {
        if raw.contract_address != registry.collateral {
            return Err(DecodeError::UnregisteredContract {
                provider: raw.identity.provider.clone(),
            });
        }
        decode_erc20_transfer(raw)?
    } else if topic.eq_ignore_ascii_case(ERC1155_TRANSFER_SINGLE_SIGNATURE_FIXTURE.1) {
        if !registry.is_conditional_tokens(&raw.contract_address) {
            return Err(DecodeError::UnregisteredContract {
                provider: raw.identity.provider.clone(),
            });
        }
        decode_erc1155_transfer_single(raw)?
    } else if topic.eq_ignore_ascii_case(ERC1155_TRANSFER_BATCH_SIGNATURE_FIXTURE.1) {
        if !registry.is_conditional_tokens(&raw.contract_address) {
            return Err(DecodeError::UnregisteredContract {
                provider: raw.identity.provider.clone(),
            });
        }
        decode_erc1155_transfer_batch(raw)?
    } else if topic.eq_ignore_ascii_case(CTF_POSITION_SPLIT_SIGNATURE_FIXTURE.1) {
        if !registry.is_conditional_tokens(&raw.contract_address) {
            return Err(DecodeError::UnregisteredContract {
                provider: raw.identity.provider.clone(),
            });
        }
        let (stakeholder, condition_id, amount) =
            decode_ctf_position_change(raw, &registry.collateral)?;
        ChainEvent::PositionSplit {
            stakeholder,
            condition_id,
            amount,
        }
    } else if topic.eq_ignore_ascii_case(CTF_POSITIONS_MERGE_SIGNATURE_FIXTURE.1) {
        if !registry.is_conditional_tokens(&raw.contract_address) {
            return Err(DecodeError::UnregisteredContract {
                provider: raw.identity.provider.clone(),
            });
        }
        let (stakeholder, condition_id, amount) =
            decode_ctf_position_change(raw, &registry.collateral)?;
        ChainEvent::PositionsMerge {
            stakeholder,
            condition_id,
            amount,
        }
    } else if topic.eq_ignore_ascii_case(EXCHANGE_V1_ORDER_FILLED_SIGNATURE_FIXTURE.1) {
        if !registry.is_legacy_exchange(&raw.contract_address) {
            return Err(DecodeError::UnregisteredContract {
                provider: raw.identity.provider.clone(),
            });
        }
        decode_exchange_v1_order_filled(raw)?
    } else if topic.eq_ignore_ascii_case(EXCHANGE_V2_ORDER_FILLED_SIGNATURE_FIXTURE.1) {
        if !registry.is_current_exchange(&raw.contract_address) {
            return Err(DecodeError::UnregisteredContract {
                provider: raw.identity.provider.clone(),
            });
        }
        decode_exchange_v2_order_filled(raw)?
    } else {
        return Err(DecodeError::UnsupportedTopic {
            provider: raw.identity.provider.clone(),
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
        Err(DecodeError::UnregisteredContract {
            provider: raw.identity.provider.clone(),
        })
    }
}

fn decode_erc1155_transfer_single(raw: &RawRpcLog) -> Result<ChainEvent, DecodeError> {
    if raw.topics.len() != 4 {
        return Err(malformed("ERC-1155 TransferSingle requires four topics"));
    }
    topic_address(&raw.topics[1])?;
    let data = fixed_abi_data(&raw.data, 2)?;
    Ok(ChainEvent::OutcomeTransferSingle {
        from: topic_address(&raw.topics[2])?,
        to: topic_address(&raw.topics[3])?,
        asset_id: format!("0x{}", data[..64].to_ascii_lowercase()),
        amount: hex_decimal(&data[64..])?,
    })
}

fn decode_erc1155_transfer_batch(raw: &RawRpcLog) -> Result<ChainEvent, DecodeError> {
    if raw.topics.len() != 4 {
        return Err(malformed("ERC-1155 TransferBatch requires four topics"));
    }
    topic_address(&raw.topics[1])?;
    let data = abi_data(&raw.data)?;
    let ids = dynamic_abi_words(data, 0, 64)?;
    let values_offset = 96_usize
        .checked_add(
            ids.len()
                .checked_div(2)
                .ok_or_else(|| malformed("ERC-1155 ID length is invalid"))?,
        )
        .ok_or_else(|| malformed("ERC-1155 value offset overflows"))?;
    let values = dynamic_abi_words(data, 1, values_offset)?;
    if ids.len() != values.len()
        || data.len()
            != values_offset
                .checked_add(32)
                .and_then(|offset| offset.checked_mul(2))
                .and_then(|offset| offset.checked_add(values.len()))
                .ok_or_else(|| malformed("ERC-1155 batch length overflows"))?
    {
        return Err(malformed(
            "ERC-1155 TransferBatch IDs and values must have one canonical layout",
        ));
    }
    let transfers = ids
        .as_bytes()
        .as_chunks::<64>()
        .0
        .iter()
        .zip(values.as_bytes().as_chunks::<64>().0.iter())
        .map(|(asset_id, amount)| {
            let asset_id = std::str::from_utf8(asset_id)
                .map_err(|_| malformed("ERC-1155 asset ID is not hex"))?;
            let amount =
                std::str::from_utf8(amount).map_err(|_| malformed("ERC-1155 amount is not hex"))?;
            Ok(OutcomeTokenAmount {
                asset_id: format!("0x{}", asset_id.to_ascii_lowercase()),
                amount: hex_decimal(amount)?,
            })
        })
        .collect::<Result<Vec<_>, DecodeError>>()?;
    Ok(ChainEvent::OutcomeTransferBatch {
        from: topic_address(&raw.topics[2])?,
        to: topic_address(&raw.topics[3])?,
        transfers,
    })
}

fn decode_ctf_position_change(
    raw: &RawRpcLog,
    expected_collateral: &Address,
) -> Result<(Address, String, Decimal), DecodeError> {
    if raw.topics.len() != 4 {
        return Err(malformed("CTF position change requires four topics"));
    }
    let stakeholder = topic_address(&raw.topics[1])?;
    abi_word(&raw.topics[2])?;
    let condition_id = format!("0x{}", abi_word(&raw.topics[3])?.to_ascii_lowercase());
    let data = abi_data(&raw.data)?;
    if hex_address(abi_word_at(data, 0)?)? != *expected_collateral {
        return Err(malformed("CTF collateral token is not registered"));
    }
    let partition = dynamic_abi_words(data, 1, 96)?;
    let expected_len = 128_usize
        .checked_mul(2)
        .and_then(|head| head.checked_add(partition.len()))
        .ok_or_else(|| malformed("CTF partition length overflows"))?;
    if data.len() != expected_len {
        return Err(malformed("CTF position change has trailing ABI data"));
    }
    Ok((
        stakeholder,
        condition_id,
        hex_decimal(abi_word_at(data, 2)?)?,
    ))
}

fn decode_exchange_v1_order_filled(raw: &RawRpcLog) -> Result<ChainEvent, DecodeError> {
    if raw.topics.len() != 4 {
        return Err(malformed("V1 OrderFilled requires four topics"));
    }
    abi_word(&raw.topics[1])?;
    let data = fixed_abi_data(&raw.data, 5)?;
    let maker_asset = abi_word_at(data, 0)?;
    let taker_asset = abi_word_at(data, 1)?;
    let maker_side =
        TradeSide::from_collateral_flow(is_zero_word(maker_asset), is_zero_word(taker_asset))
            .ok_or_else(|| {
                malformed("V1 OrderFilled must exchange collateral for one outcome token")
            })?;
    Ok(ChainEvent::OrderFilled {
        maker: topic_address(&raw.topics[2])?,
        taker: topic_address(&raw.topics[3])?,
        maker_asset_id: normalized_asset_id(maker_asset),
        taker_asset_id: normalized_asset_id(taker_asset),
        maker_side,
        maker_amount: hex_decimal(abi_word_at(data, 2)?)?,
        taker_amount: hex_decimal(abi_word_at(data, 3)?)?,
        fee: hex_decimal(abi_word_at(data, 4)?)?,
    })
}

fn decode_exchange_v2_order_filled(raw: &RawRpcLog) -> Result<ChainEvent, DecodeError> {
    if raw.topics.len() != 4 {
        return Err(malformed("V2 OrderFilled requires four topics"));
    }
    abi_word(&raw.topics[1])?;
    let data = fixed_abi_data(&raw.data, 7)?;
    let token_id = abi_word_at(data, 1)?;
    if is_zero_word(token_id) {
        return Err(malformed("V2 OrderFilled token ID cannot be collateral"));
    }
    let (maker_asset, taker_asset, maker_side) = match hex_u128(abi_word_at(data, 0)?, "side")? {
        0 => (ZERO_ASSET_ID, token_id, TradeSide::Buy),
        1 => (token_id, ZERO_ASSET_ID, TradeSide::Sell),
        _ => return Err(malformed("V2 OrderFilled side must be BUY or SELL")),
    };
    Ok(ChainEvent::OrderFilled {
        maker: topic_address(&raw.topics[2])?,
        taker: topic_address(&raw.topics[3])?,
        maker_asset_id: normalized_asset_id(maker_asset),
        taker_asset_id: normalized_asset_id(taker_asset),
        maker_side,
        maker_amount: hex_decimal(abi_word_at(data, 2)?)?,
        taker_amount: hex_decimal(abi_word_at(data, 3)?)?,
        fee: hex_decimal(abi_word_at(data, 4)?)?,
    })
}

fn normalized_asset_id(word: &str) -> String {
    format!("0x{}", word.to_ascii_lowercase())
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
    hex_address(abi_word(topic)?)
}

fn hex_address(word: &str) -> Result<Address, DecodeError> {
    if !word[..24].bytes().all(|byte| byte == b'0') {
        return Err(malformed("address word has a nonzero prefix"));
    }
    Address::new(format!("0x{}", &word[24..])).map_err(|_| malformed("invalid address word"))
}

fn word_decimal(word: &str) -> Result<Decimal, DecodeError> {
    let word = abi_word(word)?;
    hex_decimal(word)
}

fn hex_decimal(word: &str) -> Result<Decimal, DecodeError> {
    let value = hex_u128(word, "word")?;
    if value > DECIMAL_MAX_MANTISSA {
        return Err(malformed("word exceeds the exact decimal range"));
    }
    Ok(Decimal::from(value))
}

fn hex_u128(word: &str, field: &str) -> Result<u128, DecodeError> {
    u128::from_str_radix(word, 16).map_err(|error| malformed_parse(field, &error))
}

fn fixed_abi_data(value: &str, words: usize) -> Result<&str, DecodeError> {
    let data = abi_data(value)?;
    let expected_len = words
        .checked_mul(64)
        .ok_or_else(|| malformed("ABI word count overflows"))?;
    if data.len() != expected_len {
        return Err(malformed("ABI data has an invalid fixed-word layout"));
    }
    Ok(data)
}

fn abi_data(value: &str) -> Result<&str, DecodeError> {
    let data = value
        .strip_prefix("0x")
        .ok_or_else(|| malformed("ABI data is not hex"))?;
    if data.len() % 64 != 0 || !data.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(malformed("ABI data is not a sequence of 32-byte words"));
    }
    Ok(data)
}

fn abi_word_at(data: &str, index: usize) -> Result<&str, DecodeError> {
    let start = index
        .checked_mul(64)
        .ok_or_else(|| malformed("ABI word offset overflows"))?;
    let end = start
        .checked_add(64)
        .ok_or_else(|| malformed("ABI word end overflows"))?;
    data.get(start..end)
        .ok_or_else(|| malformed("ABI word is missing"))
}

fn dynamic_abi_words(
    data: &str,
    offset_word: usize,
    expected_offset: usize,
) -> Result<&str, DecodeError> {
    let offset = usize::from_str_radix(abi_word_at(data, offset_word)?, 16)
        .map_err(|error| malformed_parse("offset", &error))?;
    if offset != expected_offset {
        return Err(malformed("dynamic ABI offset is not canonical"));
    }
    let length_word = offset / 32;
    let length = usize::from_str_radix(abi_word_at(data, length_word)?, 16)
        .map_err(|error| malformed_parse("array length", &error))?;
    let start = length_word
        .checked_add(1)
        .and_then(|word| word.checked_mul(64))
        .ok_or_else(|| malformed("dynamic ABI start overflows"))?;
    let end = length
        .checked_mul(64)
        .and_then(|length| start.checked_add(length))
        .ok_or_else(|| malformed("dynamic ABI length overflows"))?;
    data.get(start..end)
        .ok_or_else(|| malformed("dynamic ABI array is truncated"))
}

fn is_zero_word(word: &str) -> bool {
    word.bytes().all(|byte| byte == b'0')
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
    use super::{
        CTF_POSITION_SPLIT_SIGNATURE_FIXTURE, CTF_POSITIONS_MERGE_SIGNATURE_FIXTURE, DecodeError,
        ERC20_TRANSFER_TOPIC, ERC1155_TRANSFER_BATCH_SIGNATURE_FIXTURE,
        ERC1155_TRANSFER_SINGLE_SIGNATURE_FIXTURE, EXCHANGE_V1_ORDER_FILLED_SIGNATURE_FIXTURE,
        EXCHANGE_V2_ORDER_FILLED_SIGNATURE_FIXTURE, decode_raw_log,
    };
    use crate::{
        Address, ChainEvent, ContractRegistry, LegacyV1Contracts, OutcomeTokenAmount,
        ProviderIdentity, RawLogIdentity, RawRpcLog, TradeSide,
    };

    const OPERATOR: &str = "0000000000000000000000000000000000000000000000000000000000000011";
    const FROM: &str = "00000000000000000000000000000000000000000000000000000000000000aa";
    const TO: &str = "00000000000000000000000000000000000000000000000000000000000000bb";

    fn raw(contract_address: Address, topics: Vec<String>, data: String) -> RawRpcLog {
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
            topics,
            data,
        }
    }

    #[test]
    fn decodes_registered_collateral_transfer() -> Result<(), Box<dyn std::error::Error>> {
        // Given: a standard ERC-20 transfer emitted by the registered collateral contract.
        let registry = ContractRegistry::polygon();

        // When: the raw log crosses the decoder boundary.
        let result = decode_raw_log(
            &registry,
            &raw(
                registry.collateral.clone(),
                vec![
                    ERC20_TRANSFER_TOPIC.into(),
                    format!("0x{FROM}"),
                    format!("0x{TO}"),
                ],
                format!("0x{:064x}", 42),
            ),
        );

        // Then: only the existing typed collateral event is emitted.
        let log = result?;
        assert!(
            matches!(log.event, crate::ChainEvent::CollateralTransfer { amount, .. } if amount == 42.into())
        );
        Ok(())
    }

    #[test]
    fn decode_erc1155_single() -> Result<(), Box<dyn std::error::Error>> {
        // Given: a verified standard TransferSingle log from ConditionalTokens.
        let registry = ContractRegistry::polygon();
        let from = Address::new(format!("0x{}", &FROM[24..]))?;
        let to = Address::new(format!("0x{}", &TO[24..]))?;
        let fixture = raw(
            registry.conditional_tokens.clone(),
            vec![
                ERC1155_TRANSFER_SINGLE_SIGNATURE_FIXTURE.1.into(),
                format!("0x{OPERATOR}"),
                format!("0x{FROM}"),
                format!("0x{TO}"),
            ],
            format!("0x{:064x}{:064x}", 7, 42),
        );

        // When: the raw log crosses the decoder boundary.
        let log = decode_raw_log(&registry, &fixture)?;

        // Then: every represented ABI field is preserved without fabrication.
        assert_eq!(
            log.event,
            ChainEvent::OutcomeTransferSingle {
                from,
                to,
                asset_id: format!("0x{:064x}", 7),
                amount: 42.into(),
            }
        );
        Ok(())
    }

    #[test]
    fn decode_erc1155_batch() -> Result<(), Box<dyn std::error::Error>> {
        // Given: a verified standard TransferBatch log with two token amounts.
        let registry = ContractRegistry::polygon();
        let from = Address::new(format!("0x{}", &FROM[24..]))?;
        let to = Address::new(format!("0x{}", &TO[24..]))?;
        let fixture = raw(
            registry.conditional_tokens.clone(),
            vec![
                ERC1155_TRANSFER_BATCH_SIGNATURE_FIXTURE.1.into(),
                format!("0x{OPERATOR}"),
                format!("0x{FROM}"),
                format!("0x{TO}"),
            ],
            format!(
                "0x{:064x}{:064x}{:064x}{:064x}{:064x}{:064x}{:064x}{:064x}",
                64, 160, 2, 7, 8, 2, 42, 43
            ),
        );

        // When: the raw log crosses the decoder boundary.
        let log = decode_raw_log(&registry, &fixture)?;

        // Then: IDs and amounts retain their ABI pairing.
        assert_eq!(
            log.event,
            ChainEvent::OutcomeTransferBatch {
                from,
                to,
                transfers: vec![
                    OutcomeTokenAmount {
                        asset_id: format!("0x{:064x}", 7),
                        amount: 42.into(),
                    },
                    OutcomeTokenAmount {
                        asset_id: format!("0x{:064x}", 8),
                        amount: 43.into(),
                    },
                ],
            }
        );
        Ok(())
    }

    #[test]
    fn decode_position_split() -> Result<(), Box<dyn std::error::Error>> {
        decode_position_fixture(false)
    }

    #[test]
    fn decode_positions_merge() -> Result<(), Box<dyn std::error::Error>> {
        decode_position_fixture(true)
    }

    fn decode_position_fixture(merge: bool) -> Result<(), Box<dyn std::error::Error>> {
        // Given: a verified CTF position event with its complete dynamic partition.
        let registry = ContractRegistry::polygon();
        let stakeholder = Address::new(format!("0x{}", &FROM[24..]))?;
        let condition_id = format!("0x{:064x}", 2);
        let topic = if merge {
            CTF_POSITIONS_MERGE_SIGNATURE_FIXTURE.1
        } else {
            CTF_POSITION_SPLIT_SIGNATURE_FIXTURE.1
        };
        let fixture = raw(
            registry.conditional_tokens.clone(),
            vec![
                topic.into(),
                format!("0x{FROM}"),
                format!("0x{:064x}", 1),
                condition_id.clone(),
            ],
            format!(
                "0x{:0>64}{:064x}{:064x}{:064x}{:064x}{:064x}",
                &registry.collateral.as_str()[2..],
                96,
                50,
                2,
                1,
                2
            ),
        );

        // When: the raw log crosses the decoder boundary.
        let log = decode_raw_log(&registry, &fixture)?;

        // Then: the represented stakeholder, condition, and amount are exact.
        let expected = if merge {
            ChainEvent::PositionsMerge {
                stakeholder,
                condition_id,
                amount: 50.into(),
            }
        } else {
            ChainEvent::PositionSplit {
                stakeholder,
                condition_id,
                amount: 50.into(),
            }
        };
        assert_eq!(log.event, expected);
        Ok(())
    }

    #[test]
    fn decode_current_exchange_order_filled() -> Result<(), Box<dyn std::error::Error>> {
        // Given: a verified V2 sell fill emitted by the current exchange.
        let registry = ContractRegistry::polygon();
        let fixture = raw(
            registry.ctf_exchange.clone(),
            vec![
                EXCHANGE_V2_ORDER_FILLED_SIGNATURE_FIXTURE.1.into(),
                format!("0x{:064x}", 1),
                format!("0x{FROM}"),
                format!("0x{TO}"),
            ],
            format!(
                "0x{:064x}{:064x}{:064x}{:064x}{:064x}{:064x}{:064x}",
                1, 7, 42, 10, 1, 3, 4
            ),
        );

        // When: the raw log crosses the decoder boundary.
        let log = decode_raw_log(&registry, &fixture)?;

        // Then: V2 side and token ID map to exact maker/taker assets.
        assert_order_filled(&log.event, TradeSide::Sell)?;
        Ok(())
    }

    #[test]
    fn decode_legacy_exchange_order_filled() -> Result<(), Box<dyn std::error::Error>> {
        // Given: a verified V1 fill emitted by an explicitly enabled legacy exchange.
        let legacy_exchange = Address::new("0x0000000000000000000000000000000000000001")?;
        let registry = ContractRegistry::polygon().with_legacy_v1(LegacyV1Contracts {
            ctf_exchange: legacy_exchange.clone(),
            neg_risk_exchange: Address::new("0x0000000000000000000000000000000000000002")?,
        });
        let fixture = raw(
            legacy_exchange,
            vec![
                EXCHANGE_V1_ORDER_FILLED_SIGNATURE_FIXTURE.1.into(),
                format!("0x{:064x}", 1),
                format!("0x{FROM}"),
                format!("0x{TO}"),
            ],
            format!("0x{:064x}{:064x}{:064x}{:064x}{:064x}", 7, 0, 42, 10, 1),
        );

        // When: the raw log crosses the decoder boundary.
        let log = decode_raw_log(&registry, &fixture)?;

        // Then: V1 asset IDs determine the same typed sell direction.
        assert_order_filled(&log.event, TradeSide::Sell)?;
        Ok(())
    }

    fn assert_order_filled(
        event: &ChainEvent,
        maker_side: TradeSide,
    ) -> Result<(), crate::AddressError> {
        assert_eq!(
            event,
            &ChainEvent::OrderFilled {
                maker: Address::new(format!("0x{}", &FROM[24..]))?,
                taker: Address::new(format!("0x{}", &TO[24..]))?,
                maker_asset_id: format!("0x{:064x}", 7),
                taker_asset_id: format!("0x{:064x}", 0),
                maker_side,
                maker_amount: 42.into(),
                taker_amount: 10.into(),
                fee: 1.into(),
            }
        );
        Ok(())
    }

    #[test]
    fn rejects_unregistered_contract() -> Result<(), Box<dyn std::error::Error>> {
        // Given: a standard topic emitted by an unregistered contract.
        let registry = ContractRegistry::polygon();
        let unregistered = Address::new("0x0000000000000000000000000000000000000001")?;

        // When: the raw log crosses the decoder boundary.
        let wrong_contract = decode_raw_log(
            &registry,
            &raw(
                unregistered,
                vec![
                    ERC20_TRANSFER_TOPIC.into(),
                    format!("0x{FROM}"),
                    format!("0x{TO}"),
                ],
                format!("0x{:064x}", 42),
            ),
        );

        // Then: it is rejected before any event is created.
        assert!(matches!(
            wrong_contract,
            Err(DecodeError::UnregisteredContract { provider })
                if provider.as_str() == "fixture-rpc"
        ));
        Ok(())
    }

    #[test]
    fn unverified_topic_rejected() {
        // Given: a well-formed but unverified event topic.
        let registry = ContractRegistry::polygon();
        let unknown_topic = format!("0x{}", "ff".repeat(32));

        // When: the raw log crosses the decoder boundary.
        let result = decode_raw_log(
            &registry,
            &raw(
                registry.conditional_tokens.clone(),
                vec![unknown_topic],
                "0x".into(),
            ),
        );

        // Then: the typed rejection retains provider identity without the raw topic.
        assert!(matches!(
            result,
            Err(DecodeError::UnsupportedTopic { provider })
                if provider.as_str() == "fixture-rpc"
        ));
    }
}
