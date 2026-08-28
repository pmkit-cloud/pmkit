use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use pmkit_data::HistoricalDataSource;
use pmkit_run::{EvidenceRequirement, RetrievalWait};

/// A bounded historical replay specification.
#[derive(Clone)]
pub struct ReplaySpec {
    source: Arc<dyn HistoricalDataSource>,
    reference_sources: Vec<(String, Arc<dyn HistoricalDataSource>)>,
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
            reference_sources: Vec::new(),
            from,
            to,
            evidence,
            retrieval_wait,
        }
    }

    /// Adds a historical CEX reference source for parity-aware runs.
    #[must_use]
    pub fn reference_source(self, source: Arc<dyn HistoricalDataSource>) -> Self {
        let name = format!("cex-{}", self.reference_sources.len());
        self.reference_source_named(name, source)
    }

    /// Adds a named historical CEX reference source.
    #[must_use]
    pub fn reference_source_named(
        mut self,
        name: impl Into<String>,
        source: Arc<dyn HistoricalDataSource>,
    ) -> Self {
        self.reference_sources.push((name.into(), source));
        self
    }

    /// Returns the historical data source.
    #[must_use]
    pub const fn source(&self) -> &Arc<dyn HistoricalDataSource> {
        &self.source
    }

    /// Returns the first historical CEX reference source for compatibility.
    #[must_use]
    pub const fn reference_source_ref(&self) -> Option<&Arc<dyn HistoricalDataSource>> {
        match self.reference_sources.as_slice() {
            [] => None,
            [(_, source), ..] => Some(source),
        }
    }

    /// Returns all named historical CEX reference sources in registration order.
    #[must_use]
    pub fn reference_source_refs(&self) -> &[(String, Arc<dyn HistoricalDataSource>)] {
        &self.reference_sources
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
                "reference_sources",
                &self
                    .reference_sources
                    .iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>(),
            )
            .field("evidence", &self.evidence)
            .field("retrieval_wait", &self.retrieval_wait)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::ReplaySpec;
    use crate::test_support::NoHistory;
    use pmkit_run::{EvidenceRequirement, RetrievalWait};
    use std::sync::Arc;

    #[test]
    fn reference_source_registrations_append_with_stable_names()
    -> Result<(), Box<dyn std::error::Error>> {
        let replay = ReplaySpec::new(
            Arc::new(NoHistory),
            "2026-01-01T00:00:00Z".parse()?,
            "2026-02-01T00:00:00Z".parse()?,
            EvidenceRequirement::CorroboratedOnly,
            RetrievalWait::ReturnPending,
        )
        .reference_source(Arc::new(NoHistory))
        .reference_source(Arc::new(NoHistory))
        .reference_source_named("twap", Arc::new(NoHistory));

        let names = replay
            .reference_source_refs()
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, ["cex-0", "cex-1", "twap"]);
        assert!(replay.reference_source_ref().is_some());
        Ok(())
    }
}
