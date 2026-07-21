use std::{fmt, num::NonZeroUsize, path::Path};

use async_trait::async_trait;
use pmkit_book::Side;
use pmkit_event::{CexReferenceEnvelope, PmAccountEnvelope, PmMarketEnvelope};
use pmkit_exec::PlaceOrder;
use pmkit_strategy::{Action, Actions};
use serde_json::{Value, json};

use crate::{StoreError, StoredEvent, StrategyDecision, TapeStore};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS market_events (id INTEGER PRIMARY KEY, timestamp_ms INTEGER NOT NULL, payload TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS market_events_timestamp ON market_events(timestamp_ms);
CREATE TABLE IF NOT EXISTS user_events (id INTEGER PRIMARY KEY, timestamp_ms INTEGER NOT NULL, payload TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS user_events_timestamp ON user_events(timestamp_ms);
CREATE TABLE IF NOT EXISTS reference_events (id INTEGER PRIMARY KEY, timestamp_ms INTEGER NOT NULL, payload TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS reference_events_timestamp ON reference_events(timestamp_ms);
CREATE TABLE IF NOT EXISTS strategy_decisions (id INTEGER PRIMARY KEY, run_id TEXT NOT NULL, strategy_id TEXT NOT NULL, market_id TEXT NOT NULL, timestamp_ms INTEGER NOT NULL, payload TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS strategy_decisions_lookup ON strategy_decisions(run_id, strategy_id, timestamp_ms);
";

/// A local Turso-backed implementation of [`TapeStore`].
pub struct TursoTapeStore {
    _database: turso::Database,
    connection: turso::Connection,
}

impl fmt::Debug for TursoTapeStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TursoTapeStore")
            .finish_non_exhaustive()
    }
}

impl TursoTapeStore {
    /// Opens a local Turso database file and creates `PMKit`'s append-only tables.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the database cannot be opened or migrated.
    pub async fn open_local(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let database = turso::Builder::new_local(&path.as_ref().to_string_lossy())
            .build()
            .await?;
        let connection = database.connect()?;
        connection.execute_batch(SCHEMA).await?;
        Ok(Self {
            _database: database,
            connection,
        })
    }
}

#[async_trait]
impl TapeStore for TursoTapeStore {
    async fn append_market(&self, envelope: &PmMarketEnvelope) -> Result<(), StoreError> {
        append_event(
            &self.connection,
            "market_events",
            envelope.metadata.source_time_ms,
            pmkit_tape::market_envelope_json(envelope),
        )
        .await
    }

    async fn append_account(&self, envelope: &PmAccountEnvelope) -> Result<(), StoreError> {
        append_event(
            &self.connection,
            "user_events",
            envelope.metadata.source_time_ms,
            pmkit_tape::account_envelope_json(envelope),
        )
        .await
    }

    async fn append_reference(&self, envelope: &CexReferenceEnvelope) -> Result<(), StoreError> {
        append_event(
            &self.connection,
            "reference_events",
            envelope.metadata.source_time_ms,
            pmkit_tape::reference_envelope_json(envelope),
        )
        .await
    }

    async fn append_decision(&self, decision: &StrategyDecision) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO strategy_decisions (run_id, strategy_id, market_id, timestamp_ms, payload) VALUES (?1, ?2, ?3, ?4, ?5)",
            (
                decision.run.to_string(),
                decision.strategy.to_string(),
                decision.market.to_string(),
                decision.timestamp_ms,
                serde_json::to_string(&actions_json(&decision.actions))?,
            ),
        ).await?;
        Ok(())
    }

    async fn market_events(&self, limit: NonZeroUsize) -> Result<Vec<StoredEvent>, StoreError> {
        read_events(&self.connection, "market_events", limit).await
    }

    async fn user_events(&self, limit: NonZeroUsize) -> Result<Vec<StoredEvent>, StoreError> {
        read_events(&self.connection, "user_events", limit).await
    }

    async fn reference_events(&self, limit: NonZeroUsize) -> Result<Vec<StoredEvent>, StoreError> {
        read_events(&self.connection, "reference_events", limit).await
    }
}

async fn append_event(
    connection: &turso::Connection,
    table: &str,
    timestamp_ms: i64,
    payload: Value,
) -> Result<(), StoreError> {
    let sql = format!("INSERT INTO {table} (timestamp_ms, payload) VALUES (?1, ?2)");
    connection
        .execute(sql, (timestamp_ms, serde_json::to_string(&payload)?))
        .await?;
    Ok(())
}

async fn read_events(
    connection: &turso::Connection,
    table: &str,
    limit: NonZeroUsize,
) -> Result<Vec<StoredEvent>, StoreError> {
    let limit = i64::try_from(limit.get()).map_err(|_| StoreError::LimitTooLarge)?;
    let sql = format!("SELECT timestamp_ms, payload FROM {table} ORDER BY id LIMIT ?1");
    let mut rows = connection.query(sql, (limit,)).await?;
    let mut events = Vec::new();
    while let Some(row) = rows.next().await? {
        events.push(StoredEvent {
            timestamp_ms: row.get(0)?,
            payload: serde_json::from_str(&row.get::<String>(1)?)?,
        });
    }
    Ok(events)
}

fn actions_json(actions: &Actions) -> Value {
    Value::Array(actions.as_slice().iter().map(action_json).collect())
}

fn action_json(action: &Action) -> Value {
    match action {
        Action::Place(order) => json!({ "kind": "place", "order": order_json(order) }),
        Action::Cancel(order) => json!({ "kind": "cancel", "order_id": order.0.clone() }),
        Action::ReplaceQuotes { cancel, place } => json!({
            "kind": "replace_quotes",
            "cancel": cancel.iter().map(|order| order.0.clone()).collect::<Vec<_>>(),
            "place": place.iter().map(order_json).collect::<Vec<_>>(),
        }),
        Action::CancelAll => json!({ "kind": "cancel_all" }),
    }
}

fn order_json(order: &PlaceOrder) -> Value {
    json!({
        "outcome": order.outcome.to_string(),
        "side": side_json(order.side),
        "price": order.price.to_string(),
        "qty": order.qty.to_string(),
        "post_only": order.post_only,
    })
}

const fn side_json(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

#[cfg(test)]
mod tests {
    use super::TursoTapeStore;
    use crate::{StrategyDecision, TapeStore};
    use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
    use pmkit_event::{
        CexReferenceEnvelope, CexReferenceEvent, PmAccountEnvelope, PmAccountEvent,
        PmMarketEnvelope, StreamMetadata,
    };
    use pmkit_market::{Asset, Exchange};
    use pmkit_strategy::Actions;
    use std::num::NonZeroUsize;

    #[tokio::test]
    async fn turso_records_separated_streams() -> Result<(), Box<dyn std::error::Error>> {
        let store = TursoTapeStore::open_local(":memory:").await?;
        let metadata = StreamMetadata {
            schema_version: 1,
            source_id: "source".into(),
            source_time_ms: 7,
            receipt_time_ms: 8,
            connection_id: "connection".into(),
            ingest_sequence: 9,
        };
        store
            .append_market(&PmMarketEnvelope {
                metadata: metadata.clone(),
                fact: pmkit_event::MarketEvent::Tick { timestamp_ms: 7 },
            })
            .await?;
        store
            .append_account(&PmAccountEnvelope {
                portfolio: PortfolioId::new("paper")?,
                metadata: metadata.clone(),
                fact: PmAccountEvent::OrderAck {
                    strategy: None,
                    order_id: "order".into(),
                    timestamp_ms: 7,
                },
            })
            .await?;
        store
            .append_reference(&CexReferenceEnvelope {
                metadata,
                fact: CexReferenceEvent::Trade {
                    asset: Asset::Btc,
                    exchange: Exchange::Binance,
                    aggregate_trade_id: 1,
                    price: 1.into(),
                    qty: 2.into(),
                    is_buyer_maker: false,
                    timestamp_ms: 7,
                },
            })
            .await?;
        store
            .append_decision(&StrategyDecision::new(
                PortfolioId::new("paper")?,
                RunId::new("run")?,
                StrategyId::new("maker")?,
                MarketId::new("btc-5m")?,
                7,
                Actions::cancel_all(),
            ))
            .await?;

        let limit = NonZeroUsize::new(1).ok_or("nonzero limit")?;
        assert_eq!(store.market_events(limit).await?.len(), 1);
        assert_eq!(
            store.user_events(limit).await?[0].payload["portfolio"],
            "paper"
        );
        assert_eq!(
            store.reference_events(limit).await?[0].payload["payload"]["kind"],
            "reference_trade"
        );
        drop(store);
        Ok(())
    }
}
