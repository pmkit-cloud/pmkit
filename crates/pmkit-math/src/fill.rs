//! Order-book fill simulation.

use rust_decimal::Decimal;

/// Walks a sorted book side, filling up to `remaining` size at prices no worse
/// than `limit_price`.
///
/// `levels` are `(price, quantity)` pairs in best-first order. Returns the
/// volume-weighted average fill price and the filled quantity, or `None` when
/// nothing fills.
#[must_use]
pub fn walk_book(
    levels: &[(Decimal, Decimal)],
    mut remaining: Decimal,
    limit_price: Decimal,
    is_buy: bool,
) -> Option<(Decimal, Decimal)> {
    let mut total_cost = Decimal::ZERO;
    let mut total_qty = Decimal::ZERO;

    for &(price, qty) in levels {
        if remaining <= Decimal::ZERO {
            break;
        }
        if is_buy && price > limit_price {
            break;
        }
        if !is_buy && price < limit_price {
            break;
        }
        let fill = remaining.min(qty);
        total_cost += fill * price;
        total_qty += fill;
        remaining -= fill;
    }

    if total_qty > Decimal::ZERO {
        Some((total_cost / total_qty, total_qty))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::walk_book;
    use rust_decimal::Decimal;

    #[test]
    fn buy_walks_asks() -> Result<(), &'static str> {
        let asks = vec![
            (Decimal::new(45, 2), Decimal::from(10)),
            (Decimal::new(46, 2), Decimal::from(10)),
        ];
        let (vwap, qty) =
            walk_book(&asks, Decimal::from(15), Decimal::ONE, true).ok_or("expected fill")?;
        assert_eq!(qty, Decimal::from(15));
        assert!(vwap > Decimal::new(45, 2));
        assert!(vwap < Decimal::new(46, 2));
        Ok(())
    }

    #[test]
    fn respects_limit_price() -> Result<(), &'static str> {
        let asks = vec![
            (Decimal::new(45, 2), Decimal::from(10)),
            (Decimal::new(50, 2), Decimal::from(10)),
        ];
        let (_, qty) = walk_book(&asks, Decimal::from(20), Decimal::new(46, 2), true)
            .ok_or("expected fill")?;
        assert_eq!(qty, Decimal::from(10));
        Ok(())
    }

    #[test]
    fn no_fill_when_no_depth() {
        let asks: Vec<(Decimal, Decimal)> = vec![];
        assert!(walk_book(&asks, Decimal::from(10), Decimal::ONE, true).is_none());
    }
}
