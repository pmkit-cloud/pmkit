use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use pmkit_data::HistoricalDataSource;
use pmkit_run::{EvidenceRequirement, RetrievalWait};

/// A bounded historical replay specification.
#[derive(Clone)]
pub struct ReplaySpec {
    source: Arc<dyn HistoricalDataSource>,
    reference_source: Option<Arc<dyn HistoricalDataSource>>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    evidence: EvidenceRequirement,
    retrieval_wait: RetrievalWait,
}

impl ReplaySpec {
    /// Creates a replay specification over `source` for `[from, to)`.
    #[must_use]
    pub const fn new(
        source: Arc<dyn HistoricalDataSource>,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        evidence: EvidenceRequirement,
        retrieval_wait: RetrievalWait,
    ) -> Self {
        Self {
            source,
            reference_source: None,
            from,
            to,
            evidence,
            retrieval_wait,
        }
    }

    /// Adds an optional historical CEX reference source for parity-aware runs.
    #[must_use]
    pub fn reference_source(mut self, source: Arc<dyn HistoricalDataSource>) -> Self {
        self.reference_source = Some(source);
        self
    }

    /// Returns the historical data source.
    #[must_use]
    pub const fn source(&self) -> &Arc<dyn HistoricalDataSource> {
        &self.source
    }

    /// Returns the optional historical CEX reference source.
    #[must_use]
    pub const fn reference_source_ref(&self) -> Option<&Arc<dyn HistoricalDataSource>> {
        self.reference_source.as_ref()
    }

    /// Returns the inclusive window start.
    #[must_use]
    pub const fn from(&self) -> DateTime<Utc> {
        self.from
    }

    /// Returns the exclusive window end.
    #[must_use]
    pub const fn to(&self) -> DateTime<Utc> {
        self.to
    }

    /// Returns the required corroboration.
    #[must_use]
    pub const fn evidence(&self) -> EvidenceRequirement {
        self.evidence
    }

    /// Returns the retrieval-wait policy.
    #[must_use]
    pub const fn retrieval_wait(&self) -> RetrievalWait {
        self.retrieval_wait
    }
}

impl fmt::Debug for ReplaySpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplaySpec")
            .field("from", &self.from)
            .field("to", &self.to)
            .field(
                "reference_source",
                &self.reference_source.as_ref().map(|_| "configured"),
            )
            .field("evidence", &self.evidence)
            .field("retrieval_wait", &self.retrieval_wait)
            .finish_non_exhaustive()
    }
}
