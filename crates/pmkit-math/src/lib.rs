//! Pure pricing and fee math for binary prediction markets.
//!
//! Every function is stateless and operates on [`rust_decimal::Decimal`]. There
//! is no engine state, network, or venue coupling.

pub mod fair_value;
pub mod fees;
pub mod fill;
pub mod signals;
