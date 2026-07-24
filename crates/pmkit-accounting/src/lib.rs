//! Chain- and venue-independent portfolio accounting.

use pmkit_book::Side;
use pmkit_core::MarketId;
use pmkit_market::Outcome;
use pmkit_money::Money;
use rust_decimal::Decimal;
use thiserror::Error;

/// One normalized fill accepted by the accounting ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerFill {
    /// The market containing the traded outcome.
    pub market: MarketId,
    /// The outcome token traded.
    pub outcome: Outcome,
    /// Buy or sell direction.
    pub side: Side,
    /// Fill price in USDC per share.
    pub price: Decimal,
    /// Filled share quantity.
    pub quantity: Decimal,
    /// Fee charged in USDC.
    pub fee: Decimal,
}

/// One normalized settlement instruction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settlement {
    /// The settled market.
    pub market: MarketId,
    /// The winning or settled outcome.
    pub outcome: Outcome,
    /// USDC paid per share of the settled outcome.
    pub payout_per_share: Decimal,
}

/// A mark used to calculate unrealized value and equity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mark {
    /// The marked market.
    pub market: MarketId,
    /// The marked outcome.
    pub outcome: Outcome,
    /// Current USDC price per share.
    pub price: Decimal,
}

/// A current position with average entry accounting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerPosition {
    /// The market containing the position.
    pub market: MarketId,
    /// The held outcome.
    pub outcome: Outcome,
    /// Shares currently held.
    pub quantity: Decimal,
    /// Average entry price in USDC per share.
    pub average_entry: Decimal,
}

/// A typed accounting failure.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AccountingError {
    /// A sell or settlement attempted to consume more shares than held.
    #[error(
        "insufficient {outcome:?} position in market {market}: requested {requested}, held {held}"
    )]
    InsufficientPosition {
        /// The market containing the position.
        market: MarketId,
        /// The outcome being consumed.
        outcome: Outcome,
        /// The requested quantity.
        requested: Decimal,
        /// The held quantity.
        held: Decimal,
    },
    /// A monetary or quantity value was negative.
    #[error("accounting value must not be negative: {field}={value}")]
    NegativeValue {
        /// The invalid field name.
        field: &'static str,
        /// The invalid value.
        value: Decimal,
    },
}

/// A deterministic portfolio ledger over normalized fills and settlements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortfolioLedger {
    cash: Money,
    fees: Money,
    realized_pnl: Money,
    positions: Vec<LedgerPosition>,
}

impl PortfolioLedger {
    /// Creates an empty ledger with the supplied cash balance.
    #[must_use]
    pub const fn new(initial_cash: Money) -> Self {
        Self {
            cash: initial_cash,
            fees: Money::ZERO,
            realized_pnl: Money::ZERO,
            positions: Vec::new(),
        }
    }

    /// Applies one normalized fill.
    ///
    /// # Errors
    ///
    /// Returns [`AccountingError::NegativeValue`] for invalid inputs or
    /// [`AccountingError::InsufficientPosition`] when a sell exceeds holdings.
    pub fn apply_fill(&mut self, fill: LedgerFill) -> Result<(), AccountingError> {
        validate_non_negative(fill.price, "price")?;
        validate_non_negative(fill.quantity, "quantity")?;
        validate_non_negative(fill.fee, "fee")?;
        let notional = fill.price * fill.quantity;
        self.fees += Money::from_decimal(fill.fee);
        match fill.side {
            Side::Buy => {
                self.cash -= Money::from_decimal(notional + fill.fee);
                self.add_position(fill.market.clone(), fill.outcome, fill.quantity, fill.price);
            }
            Side::Sell => {
                let position = self.position(fill.market.clone(), fill.outcome);
                if position.quantity < fill.quantity {
                    return Err(AccountingError::InsufficientPosition {
                        market: fill.market,
                        outcome: fill.outcome,
                        requested: fill.quantity,
                        held: position.quantity,
                    });
                }
                self.realized_pnl +=
                    Money::from_decimal((fill.price - position.average_entry) * fill.quantity);
                self.cash += Money::from_decimal(notional - fill.fee);
                self.remove_position(&fill.market, fill.outcome, fill.quantity);
            }
        }
        Ok(())
    }

    /// Applies one normalized settlement and closes the settled position.
    ///
    /// # Errors
    ///
    /// Returns [`AccountingError::NegativeValue`] when the payout is invalid.
    pub fn settle(&mut self, settlement: &Settlement) -> Result<(), AccountingError> {
        validate_non_negative(settlement.payout_per_share, "payout_per_share")?;
        let position = self.position(settlement.market.clone(), settlement.outcome);
        self.realized_pnl += Money::from_decimal(
            (settlement.payout_per_share - position.average_entry) * position.quantity,
        );
        self.cash += Money::from_decimal(settlement.payout_per_share * position.quantity);
        self.remove_position(&settlement.market, settlement.outcome, position.quantity);
        Ok(())
    }

    /// Returns cash after fills, fees, and settlements.
    #[must_use]
    pub const fn cash(&self) -> Money {
        self.cash
    }

    /// Returns cumulative fees charged by fills.
    #[must_use]
    pub const fn fees(&self) -> Money {
        self.fees
    }

    /// Returns realized profit and loss from closed fills and settlements.
    #[must_use]
    pub const fn realized_pnl(&self) -> Money {
        self.realized_pnl
    }

    /// Returns current open positions.
    #[must_use]
    pub fn positions(&self) -> &[LedgerPosition] {
        &self.positions
    }

    /// Calculates unrealized profit and loss from supplied marks.
    #[must_use]
    pub fn unrealized_pnl(&self, marks: &[Mark]) -> Money {
        Money::from_decimal(
            self.positions
                .iter()
                .map(|position| {
                    marks
                        .iter()
                        .find(|mark| {
                            mark.market == position.market && mark.outcome == position.outcome
                        })
                        .map_or(Decimal::ZERO, |mark| {
                            (mark.price - position.average_entry) * position.quantity
                        })
                })
                .sum(),
        )
    }

    /// Calculates marked equity as cash plus current position value.
    #[must_use]
    pub fn equity(&self, marks: &[Mark]) -> Money {
        self.cash
            + Money::from_decimal(
                self.positions
                    .iter()
                    .map(|position| {
                        marks
                            .iter()
                            .find(|mark| {
                                mark.market == position.market && mark.outcome == position.outcome
                            })
                            .map_or(Decimal::ZERO, |mark| mark.price * position.quantity)
                    })
                    .sum(),
            )
    }

    fn position(&self, market: MarketId, outcome: Outcome) -> LedgerPosition {
        self.positions
            .iter()
            .find(|position| position.market == market && position.outcome == outcome)
            .cloned()
            .unwrap_or(LedgerPosition {
                market,
                outcome,
                quantity: Decimal::ZERO,
                average_entry: Decimal::ZERO,
            })
    }

    fn add_position(
        &mut self,
        market: MarketId,
        outcome: Outcome,
        quantity: Decimal,
        price: Decimal,
    ) {
        if let Some(position) = self
            .positions
            .iter_mut()
            .find(|position| position.market == market && position.outcome == outcome)
        {
            let total = position.quantity + quantity;
            position.average_entry =
                (position.average_entry * position.quantity + price * quantity) / total;
            position.quantity = total;
        } else {
            self.positions.push(LedgerPosition {
                market,
                outcome,
                quantity,
                average_entry: price,
            });
        }
    }

    fn remove_position(&mut self, market: &MarketId, outcome: Outcome, quantity: Decimal) {
        if let Some(position) = self
            .positions
            .iter_mut()
            .find(|position| position.market == *market && position.outcome == outcome)
        {
            position.quantity -= quantity;
        }
        self.positions
            .retain(|position| !position.quantity.is_zero());
    }
}

const fn validate_non_negative(value: Decimal, field: &'static str) -> Result<(), AccountingError> {
    if value.is_sign_negative() {
        return Err(AccountingError::NegativeValue { field, value });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{LedgerFill, Mark, PortfolioLedger, Settlement};
    use pmkit_book::Side;
    use pmkit_core::MarketId;
    use pmkit_market::Outcome;
    use pmkit_money::Money;
    use rust_decimal::Decimal;

    fn market() -> Result<MarketId, Box<dyn std::error::Error>> {
        Ok(MarketId::new("btc-5m")?)
    }

    #[test]
    fn fills_track_cash_fees_realized_and_unrealized_pnl() -> Result<(), Box<dyn std::error::Error>>
    {
        let market = market()?;
        let mut ledger = PortfolioLedger::new(Money::usdc(100));
        ledger.apply_fill(LedgerFill {
            market: market.clone(),
            outcome: Outcome::Up,
            side: Side::Buy,
            price: Decimal::new(40, 2),
            quantity: Decimal::from(10),
            fee: Decimal::new(1, 2),
        })?;
        assert_eq!(ledger.cash(), Money::from_decimal(Decimal::new(9599, 2)));
        assert_eq!(ledger.fees(), Money::from_decimal(Decimal::new(1, 2)));
        assert_eq!(
            ledger.unrealized_pnl(&[Mark {
                market: market.clone(),
                outcome: Outcome::Up,
                price: Decimal::new(50, 2),
            }]),
            Money::usdc(1),
        );
        ledger.apply_fill(LedgerFill {
            market,
            outcome: Outcome::Up,
            side: Side::Sell,
            price: Decimal::new(60, 2),
            quantity: Decimal::from(10),
            fee: Decimal::new(1, 2),
        })?;
        assert_eq!(ledger.realized_pnl(), Money::usdc(2));
        assert!(ledger.positions().is_empty());
        Ok(())
    }

    #[test]
    fn settlement_closes_position_and_credits_payout() -> Result<(), Box<dyn std::error::Error>> {
        let market = market()?;
        let mut ledger = PortfolioLedger::new(Money::usdc(0));
        ledger.apply_fill(LedgerFill {
            market: market.clone(),
            outcome: Outcome::Down,
            side: Side::Buy,
            price: Decimal::new(25, 2),
            quantity: Decimal::from(4),
            fee: Decimal::ZERO,
        })?;
        ledger.settle(&Settlement {
            market,
            outcome: Outcome::Down,
            payout_per_share: Decimal::ONE,
        })?;
        assert_eq!(ledger.cash(), Money::from_decimal(Decimal::new(3, 0)));
        assert_eq!(
            ledger.realized_pnl(),
            Money::from_decimal(Decimal::new(3, 0))
        );
        assert!(ledger.positions().is_empty());
        Ok(())
    }

    #[test]
    fn overselling_fails_without_mutating_the_ledger() -> Result<(), Box<dyn std::error::Error>> {
        let market = market()?;
        let mut ledger = PortfolioLedger::new(Money::usdc(10));
        ledger.apply_fill(LedgerFill {
            market: market.clone(),
            outcome: Outcome::Up,
            side: Side::Buy,
            price: Decimal::new(1, 1),
            quantity: Decimal::ONE,
            fee: Decimal::ZERO,
        })?;
        let result = ledger.apply_fill(LedgerFill {
            market,
            outcome: Outcome::Up,
            side: Side::Sell,
            price: Decimal::ONE,
            quantity: Decimal::from(2),
            fee: Decimal::ZERO,
        });
        assert!(result.is_err());
        assert_eq!(ledger.positions()[0].quantity, Decimal::ONE);
        Ok(())
    }
}
