//! Run-specification primitives for `PMKit`.
//!
//! Small, fully-specified value types that gate and describe a run: replay
//! evidence, archive-retrieval waiting, user-tape policy, and the explicit
//! live-trading consent gate.

use std::env;
use std::time::Duration;

use thiserror::Error;

/// Evidence requirement for historical replay data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceRequirement {
    /// Only intervals corroborated by multiple sources are acceptable.
    CorroboratedOnly,
    /// A single source is acceptable.
    AllowSingleSource,
}

/// Whether a backtest waits for archive retrieval or returns pending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetrievalWait {
    /// Wait up to `timeout` for retrieval to complete.
    Wait {
        /// Maximum time to wait.
        timeout: Duration,
    },
    /// Return immediately with a pending report.
    ReturnPending,
}

/// Whether a user tape is mandatory or best-effort.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TapePolicy {
    /// Tape-write failure blocks live startup and new live orders.
    Required,
    /// Tape-write failure is reported but does not stop the run.
    BestEffort,
}

/// Proof that the operator explicitly consented to live trading.
///
/// The inner field is private, so a value can only be obtained through
/// [`LiveConsent::from_env`] with the exact required environment value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveConsent(());

/// Raised when live consent is missing or incorrect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum LiveConsentError {
    /// The consent environment variable is not set.
    #[error("PMKIT_ENABLE_LIVE is not set")]
    Missing,
    /// The consent environment variable does not hold the required value.
    #[error("PMKIT_ENABLE_LIVE does not equal the required consent value")]
    Mismatch,
}

impl LiveConsent {
    /// The environment variable that must carry consent.
    pub const ENV: &'static str = "PMKIT_ENABLE_LIVE";
    /// The exact value the environment variable must hold.
    pub const REQUIRED_VALUE: &'static str = "I_UNDERSTAND_THIS_PLACES_REAL_ORDERS";

    /// Reads consent from the process environment.
    ///
    /// # Errors
    ///
    /// Returns [`LiveConsentError`] if [`LiveConsent::ENV`] is unset or not
    /// exactly [`LiveConsent::REQUIRED_VALUE`].
    pub fn from_env() -> Result<Self, LiveConsentError> {
        Self::check(env::var(Self::ENV).ok().as_deref())
    }

    fn check(value: Option<&str>) -> Result<Self, LiveConsentError> {
        match value {
            Some(v) if v == Self::REQUIRED_VALUE => Ok(Self(())),
            Some(_) => Err(LiveConsentError::Mismatch),
            None => Err(LiveConsentError::Missing),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{EvidenceRequirement, LiveConsent, LiveConsentError, RetrievalWait, TapePolicy};
    use std::time::Duration;

    #[test]
    fn consent_requires_exact_value() {
        assert!(LiveConsent::check(Some(LiveConsent::REQUIRED_VALUE)).is_ok());
        assert_eq!(
            LiveConsent::check(Some("yes")),
            Err(LiveConsentError::Mismatch)
        );
        assert_eq!(LiveConsent::check(None), Err(LiveConsentError::Missing));
    }

    #[test]
    fn run_spec_enums_compare() {
        assert_ne!(
            EvidenceRequirement::CorroboratedOnly,
            EvidenceRequirement::AllowSingleSource
        );
        assert_ne!(TapePolicy::Required, TapePolicy::BestEffort);
        assert_ne!(
            RetrievalWait::Wait {
                timeout: Duration::from_secs(1)
            },
            RetrievalWait::ReturnPending
        );
    }
}
