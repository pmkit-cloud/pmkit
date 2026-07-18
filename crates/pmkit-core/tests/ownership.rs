//! Public ownership contract tests.

use std::collections::HashSet;

use pmkit_core::{
    EmptyIdError, MarketId, Mode, PortfolioId, PortfolioKey, StrategyId, StrategyKey,
};

#[test]
fn portfolio_keys_are_distinct_when_modes_differ() -> Result<(), EmptyIdError> {
    // Given
    let portfolio = PortfolioId::new("alice")?;

    // When
    let keys = HashSet::from([
        PortfolioKey::new(portfolio.clone(), Mode::Backtest),
        PortfolioKey::new(portfolio.clone(), Mode::Paper),
        PortfolioKey::new(portfolio, Mode::Live),
    ]);

    // Then
    assert_eq!(keys.len(), 3);
    Ok(())
}

#[test]
fn strategy_keys_are_distinct_when_markets_differ() -> Result<(), EmptyIdError> {
    // Given
    let portfolio = PortfolioKey::paper("alice")?;
    let strategy = StrategyId::new("maker")?;

    // When
    let keys = HashSet::from([
        StrategyKey::new(
            portfolio.clone(),
            MarketId::new("market-a")?,
            strategy.clone(),
        ),
        StrategyKey::new(portfolio, MarketId::new("market-b")?, strategy),
    ]);

    // Then
    assert_eq!(keys.len(), 2);
    Ok(())
}

#[test]
fn empty_ids_return_typed_errors() {
    // Given
    let empty = "";

    // When
    let portfolio = PortfolioId::new(empty);
    let market = MarketId::new(empty);
    let strategy = StrategyId::new(empty);

    // Then
    assert_eq!(portfolio, Err(EmptyIdError::Portfolio));
    assert_eq!(market, Err(EmptyIdError::Market));
    assert_eq!(strategy, Err(EmptyIdError::Strategy));
}

#[test]
fn whitespace_only_ids_return_typed_errors() {
    // Given
    let whitespace = "  ";

    // When
    let portfolio = PortfolioId::new(whitespace);
    let market = MarketId::new(whitespace);
    let strategy = StrategyId::new(whitespace);

    // Then
    assert_eq!(portfolio, Err(EmptyIdError::Portfolio));
    assert_eq!(market, Err(EmptyIdError::Market));
    assert_eq!(strategy, Err(EmptyIdError::Strategy));
}
