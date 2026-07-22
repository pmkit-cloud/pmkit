use std::collections::BTreeMap;

use rust_decimal::Decimal;

use crate::{
    Address, CanonicalChainLog, ChainCheckpoint, ChainEvent, WalletActivity, WalletActivityKind,
    WalletPosition, WalletQuery, WalletSnapshot, WalletTrade,
};

pub fn reconstruct_wallet(query: &WalletQuery, logs: &[CanonicalChainLog]) -> WalletSnapshot {
    let mut state = ReconstructionState::new(query.wallet.clone());
    for log in logs {
        if in_range(log, query) {
            state.apply(log);
        }
    }
    state.finish()
}

struct ReconstructionState {
    wallet: Address,
    canonical_tip: Option<ChainCheckpoint>,
    collateral_balance: Decimal,
    positions: BTreeMap<String, Decimal>,
    settled_collateral: Decimal,
    trades: Vec<WalletTrade>,
    activity: Vec<WalletActivity>,
}

impl ReconstructionState {
    const fn new(wallet: Address) -> Self {
        Self {
            wallet,
            canonical_tip: None,
            collateral_balance: Decimal::ZERO,
            positions: BTreeMap::new(),
            settled_collateral: Decimal::ZERO,
            trades: Vec::new(),
            activity: Vec::new(),
        }
    }

    fn apply(&mut self, log: &CanonicalChainLog) {
        self.canonical_tip = Some(ChainCheckpoint::new(
            log.identity.chain_id,
            log.identity.block_number,
            log.identity.block_hash.clone(),
        ));
        self.apply_event(log);
    }

    fn apply_event(&mut self, log: &CanonicalChainLog) {
        match &log.event {
            ChainEvent::CollateralTransfer { from, to, amount } => {
                self.apply_collateral(from, to, amount);
            }
            ChainEvent::OutcomeTransferSingle {
                from,
                to,
                asset_id,
                amount,
            } => {
                self.apply_position(from, to, asset_id, amount);
            }
            ChainEvent::OutcomeTransferBatch {
                from,
                to,
                transfers,
            } => {
                for transfer in transfers {
                    self.apply_position(from, to, &transfer.asset_id, &transfer.amount);
                }
            }
            ChainEvent::PositionSplit {
                stakeholder,
                condition_id,
                amount,
            } if stakeholder == &self.wallet => {
                self.push_activity(
                    log,
                    WalletActivityKind::Split,
                    Some(condition_id.clone()),
                    None,
                    *amount,
                );
            }
            ChainEvent::PositionsMerge {
                stakeholder,
                condition_id,
                amount,
            } if stakeholder == &self.wallet => {
                self.push_activity(
                    log,
                    WalletActivityKind::Merge,
                    Some(condition_id.clone()),
                    None,
                    *amount,
                );
            }
            ChainEvent::PayoutRedemption {
                redeemer,
                condition_id,
                payout,
            } if redeemer == &self.wallet => {
                self.settled_collateral += *payout;
                self.push_activity(
                    log,
                    WalletActivityKind::Redemption,
                    Some(condition_id.clone()),
                    None,
                    *payout,
                );
            }
            ChainEvent::OrderFilled {
                maker,
                taker,
                asset_id,
                maker_amount,
                taker_amount,
                fee,
            } if maker == &self.wallet || taker == &self.wallet => {
                self.apply_trade(
                    log,
                    maker == &self.wallet,
                    asset_id,
                    maker_amount,
                    taker_amount,
                    fee,
                );
            }
            ChainEvent::OrdersMatched {
                trader,
                asset_id,
                amount,
            } if trader == &self.wallet => {
                self.push_activity(
                    log,
                    WalletActivityKind::Match,
                    None,
                    Some(asset_id.clone()),
                    *amount,
                );
            }
            ChainEvent::FeeCharged { payer, amount, .. } if payer == &self.wallet => {
                self.push_activity(log, WalletActivityKind::Fee, None, None, *amount);
            }
            _ => {}
        }
    }

    fn apply_collateral(&mut self, from: &Address, to: &Address, amount: &Decimal) {
        if from == &self.wallet {
            self.collateral_balance -= *amount;
        }
        if to == &self.wallet {
            self.collateral_balance += *amount;
        }
    }

    fn apply_position(&mut self, from: &Address, to: &Address, asset_id: &str, amount: &Decimal) {
        let position = self.positions.entry(asset_id.to_owned()).or_default();
        if from == &self.wallet {
            *position -= *amount;
        }
        if to == &self.wallet {
            *position += *amount;
        }
    }

    fn apply_trade(
        &mut self,
        log: &CanonicalChainLog,
        maker: bool,
        asset_id: &str,
        maker_amount: &Decimal,
        taker_amount: &Decimal,
        fee: &Decimal,
    ) {
        let (size, counter_amount) = if maker {
            (*maker_amount, *taker_amount)
        } else {
            (*taker_amount, *maker_amount)
        };
        self.trades.push(WalletTrade {
            transaction_hash: log.identity.transaction_hash.clone(),
            block_number: log.identity.block_number,
            asset_id: asset_id.to_owned(),
            maker,
            size,
            counter_amount,
            fee: *fee,
        });
        self.push_activity(
            log,
            WalletActivityKind::Trade,
            None,
            Some(asset_id.to_owned()),
            size,
        );
    }

    fn push_activity(
        &mut self,
        log: &CanonicalChainLog,
        kind: WalletActivityKind,
        condition_id: Option<String>,
        asset_id: Option<String>,
        amount: Decimal,
    ) {
        self.activity.push(WalletActivity {
            transaction_hash: log.identity.transaction_hash.clone(),
            block_number: log.identity.block_number,
            kind,
            condition_id,
            asset_id,
            amount,
        });
    }

    fn finish(self) -> WalletSnapshot {
        WalletSnapshot {
            wallet: self.wallet,
            canonical_tip: self.canonical_tip,
            collateral_balance: self.collateral_balance,
            positions: self
                .positions
                .into_iter()
                .filter_map(|(asset_id, size)| {
                    (!size.is_zero()).then_some(WalletPosition { asset_id, size })
                })
                .collect(),
            settled_collateral: self.settled_collateral,
            trades: self.trades,
            activity: self.activity,
        }
    }
}

fn in_range(log: &CanonicalChainLog, query: &WalletQuery) -> bool {
    query
        .from_block
        .is_none_or(|from| log.identity.block_number >= from)
        && query
            .to_block
            .is_none_or(|to| log.identity.block_number <= to)
}
