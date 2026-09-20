//! What a job is, and the only way one changes.
//!
//! Everything here is a value. No handles, no futures, nothing that cannot be written to a
//! row — which is what lets the store behind it be a `HashMap` now and something durable
//! later without any of this moving. What is *not* here is the running: the abort handle and
//! the caller's credentials live process-local beside the runner, because a restart has no
//! running jobs by definition and a credential must never reach a store.
//!
//! **A job changes by [`Change`] and by nothing else.** A closed set of transitions, applied
//! by [`Job::apply`], is what lets one `Mutex<HashMap>` and one `UPDATE … WHERE phase = ?`
//! both be atomic without a version column and without an optimistic-retry loop at every call
//! site. The phase rules are therefore in one place that every store implementation calls,
//! rather than in each of them.

use std::fmt;

use chrono::{DateTime, TimeDelta, Utc};

use crate::error::ApiError;
use crate::tap::jobs::id::JobId;

/// Where a job is in its life (UWS §2.1.3).
///
/// Six of the ten. `HELD` and `SUSPENDED` describe a scheduler this has not got; `UNKNOWN`
/// describes a service that has lost track of a job, which an in-process store cannot do;
/// and `ARCHIVED` is "an alternative that the server may choose" at destruction time, so
/// nothing requires it and a phase kept for one eviction path is a state every client and
/// every test would have to know about. Over its quota a job is destroyed instead, which is
/// what §2.1.7 describes in full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Accepted, and not yet committed to run.
    Pending,
    /// Committed, and waiting for a slot.
    Queued,
    /// Running.
    Executing,
    /// Finished, with a result.
    Completed,
    /// Finished, with an error document instead.
    Error,
    /// Stopped, by the caller or by the clock.
    Aborted,
}

impl Phase {
    /// UWS's own spelling, which is what the documents carry and what `/phase` answers.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Queued => "QUEUED",
            Self::Executing => "EXECUTING",
            Self::Completed => "COMPLETED",
            Self::Error => "ERROR",
            Self::Aborted => "ABORTED",
        }
    }

    /// Whether the job is finished. A terminal phase is never left.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Error | Self::Aborted)
    }

    /// Whether UWS 1.1 allows a `WAIT` to block here — the "active" phases, and only those.
    pub fn is_active(self) -> bool {
        matches!(self, Self::Pending | Self::Queued | Self::Executing)
    }
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Where a finished job's rows are.
///
/// The bytes are not here and never will be: they are a file, and this is what says which.
/// A row-backed store writes these five fields and the file stays where it is.
#[derive(Debug, Clone)]
pub struct Product {
    /// The file's name inside this run's results directory.
    pub file: String,
    pub content_type: String,
    pub bytes: u64,
    pub rows: usize,
    /// Whether the rows stopped at the bound. A VOTable says so in the document; the
    /// delimited formats have nowhere to put it, so the record is the only place it is kept.
    pub overflow: bool,
}

/// One job, as a store holds it.
#[derive(Debug, Clone)]
pub struct Job {
    pub id: JobId,
    /// The caller's `RUNID`, if they sent one (DALI §3.4.6).
    pub run_id: Option<String>,
    /// Who created it, where that can be said.
    ///
    /// **Always `None` today, and the field is where authentication would land.** UWS
    /// §2.1.8 has the owner exist only "in cases where the access to the service is
    /// authenticated", and §3 asks an authenticated service to "set the owner object to the
    /// identity obtained by the authentication" — so this is the standard's own shape rather
    /// than a guess at one, and `/owner` and `<uws:ownerId>` read it rather than each
    /// deciding that there is nobody.
    ///
    /// What would change beside it: the job list, which is empty because an anonymous
    /// caller's security context holds nothing, and the `404` that an id naming somebody
    /// else's job gets — §3 asks for a `403` there, which only means something once two
    /// callers can be told apart.
    pub owner: Option<String>,
    pub phase: Phase,
    pub created: DateTime<Utc>,
    pub started: Option<DateTime<Utc>>,
    pub ended: Option<DateTime<Utc>>,
    /// How long the job may run once it starts. UWS defines it "in real clock seconds", and
    /// `0` means unlimited — which this service does not offer, so a request for it is
    /// answered with the operator's maximum rather than refused.
    pub execution_duration: TimeDelta,
    /// When the record and its result go, whatever phase it is in.
    pub destruction: DateTime<Utc>,
    /// The parameters as the caller sent them, in order, less `UPLOAD_STORAGE_OPTION` —
    /// which is a credential and is held process-local instead. UWS §2.1.11 asks for "an
    /// enumeration of the Job parameters" and requires no completeness, so leaving one out
    /// needs no masking spelling invented for it.
    pub parameters: Vec<(String, String)>,
    /// What went wrong, for the `errorSummary` and the `/error` document.
    pub error: Option<String>,
    pub product: Option<Product>,
}

/// The only ways a job changes.
#[derive(Debug, Clone)]
pub enum Change {
    /// `PHASE=RUN`: the caller commits it. UWS §2.1.3 puts a committed job in `QUEUED`
    /// until something picks it up, which is honest here — a slot may not be free.
    Queue,
    /// The runner took a slot and began.
    Start,
    Complete(Product),
    /// The query, a bound, or a panic. Anything that leaves the caller an error document.
    Fail(String),
    /// `PHASE=ABORT`, or the clock. UWS §2.1 makes exceeding the execution duration "the
    /// same effect as when a manual 'Abort' is requested", which is why the clock arrives
    /// here and not at [`Change::Fail`].
    Abort {
        reason: Option<String>,
    },
    Destruction(DateTime<Utc>),
    ExecutionDuration(TimeDelta),
    Parameter {
        name: String,
        value: String,
    },
}

impl Job {
    /// A job as `POST /async` creates it: `PENDING`, and nothing run yet.
    pub fn new(
        id: JobId,
        parameters: Vec<(String, String)>,
        run_id: Option<String>,
        execution_duration: TimeDelta,
        destruction: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            run_id,
            owner: None,
            phase: Phase::Pending,
            created: now,
            started: None,
            ended: None,
            execution_duration,
            destruction,
            parameters,
            error: None,
            product: None,
        }
    }

    /// Apply one transition, or say why it does not apply.
    ///
    /// **A refusal is a `403`**, which is the status UWS §3 uses for something a caller may
    /// not do to a job — and every refusal here is that rather than a fault: the phase moved
    /// under a caller who was looking at an older one.
    pub fn apply(&mut self, change: Change, now: DateTime<Utc>) -> Result<(), ApiError> {
        match change {
            Change::Queue => {
                self.only_in(&[Phase::Pending], "run")?;
                self.phase = Phase::Queued;
            }
            Change::Start => {
                self.only_in(&[Phase::Queued], "start")?;
                self.phase = Phase::Executing;
                self.started = Some(now);
            }
            Change::Complete(product) => {
                self.only_in(&[Phase::Queued, Phase::Executing], "complete")?;
                self.phase = Phase::Completed;
                self.product = Some(product);
                self.ended = Some(now);
            }
            Change::Fail(message) => {
                self.only_in(&[Phase::Queued, Phase::Executing], "fail")?;
                self.phase = Phase::Error;
                self.error = Some(message);
                self.ended = Some(now);
            }
            Change::Abort { reason } => {
                self.only_in(&[Phase::Pending, Phase::Queued, Phase::Executing], "abort")?;
                self.phase = Phase::Aborted;
                self.error = reason;
                self.ended = Some(now);
            }
            // Any phase, including a finished one: asking for a result to be kept longer is
            // the main reason a client writes this at all.
            Change::Destruction(when) => self.destruction = when,
            Change::ExecutionDuration(duration) => {
                // Refused once the job has started, rather than accepted and ignored. UWS
                // §2.1 lets a service forbid a change; what it does not allow is taking one
                // and leaving the value as it was, which a client reads as having set a
                // clock it has not set.
                self.only_in(&[Phase::Pending, Phase::Queued], "reschedule")?;
                self.execution_duration = duration;
            }
            Change::Parameter { name, value } => {
                // DALI: "Job parameters may only be POSTed while the job is in the PENDING
                // phase; once execution has been requested ... job parameters may not be
                // modified."
                self.only_in(&[Phase::Pending], "take another parameter")?;
                match self
                    .parameters
                    .iter_mut()
                    .find(|(existing, _)| existing.eq_ignore_ascii_case(&name))
                {
                    Some(slot) => slot.1 = value,
                    None => self.parameters.push((name, value)),
                }
            }
        }
        Ok(())
    }

    /// When the clock runs out on a job that has started, if it has.
    ///
    /// `None` where it has not started: the duration is time spent running, so a job waiting
    /// for a slot is not spending it.
    pub fn deadline(&self) -> Option<DateTime<Utc>> {
        self.started
            .map(|started| started + self.execution_duration)
    }

    fn only_in(&self, phases: &[Phase], doing: &str) -> Result<(), ApiError> {
        if phases.contains(&self.phase) {
            return Ok(());
        }
        Err(ApiError::forbidden(format!(
            "this job is {} and cannot {doing}",
            self.phase
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job() -> Job {
        let now = Utc::now();
        Job::new(
            JobId::new().unwrap(),
            vec![("QUERY".to_owned(), "SELECT 1".to_owned())],
            None,
            TimeDelta::seconds(60),
            now + TimeDelta::hours(1),
            now,
        )
    }

    fn product() -> Product {
        Product {
            file: "x".to_owned(),
            content_type: "application/x-votable+xml".to_owned(),
            bytes: 10,
            rows: 1,
            overflow: false,
        }
    }

    /// The phases are UWS's own strings, which is what every document carries.
    #[test]
    fn a_phase_is_spelled_the_way_uws_spells_it() {
        assert_eq!(Phase::Pending.as_str(), "PENDING");
        assert_eq!(Phase::Executing.as_str(), "EXECUTING");
        assert_eq!(Phase::Aborted.as_str(), "ABORTED");
        for phase in [Phase::Completed, Phase::Error, Phase::Aborted] {
            assert!(phase.is_terminal(), "{phase}");
            assert!(!phase.is_active(), "{phase}");
        }
        for phase in [Phase::Pending, Phase::Queued, Phase::Executing] {
            assert!(!phase.is_terminal(), "{phase}");
            assert!(phase.is_active(), "{phase}");
        }
    }

    #[test]
    fn a_job_runs_from_pending_to_completed() {
        let now = Utc::now();
        let mut job = job();
        assert_eq!(job.phase, Phase::Pending);
        assert_eq!(job.deadline(), None);
        job.apply(Change::Queue, now).unwrap();
        assert_eq!(job.phase, Phase::Queued);
        job.apply(Change::Start, now).unwrap();
        assert_eq!(job.phase, Phase::Executing);
        assert_eq!(job.deadline(), Some(now + TimeDelta::seconds(60)));
        job.apply(Change::Complete(product()), now).unwrap();
        assert_eq!(job.phase, Phase::Completed);
        assert!(job.product.is_some());
        assert_eq!(job.ended, Some(now));
    }

    /// A terminal phase is never left, whichever transition tries.
    #[test]
    fn a_finished_job_does_not_start_again() {
        let now = Utc::now();
        for reached in [
            Change::Complete(product()),
            Change::Fail("no".to_owned()),
            Change::Abort { reason: None },
        ] {
            let mut job = job();
            job.apply(Change::Queue, now).unwrap();
            job.apply(Change::Start, now).unwrap();
            job.apply(reached, now).unwrap();
            let was = job.phase;
            for again in [
                Change::Queue,
                Change::Start,
                Change::Complete(product()),
                Change::Fail("no".to_owned()),
                Change::Abort { reason: None },
            ] {
                assert!(job.apply(again, now).is_err(), "{was} moved");
            }
            assert_eq!(job.phase, was);
        }
    }

    /// UWS §2.1: exceeding the execution duration has "the same effect as when a manual
    /// 'Abort' is requested", so the clock produces ABORTED and not ERROR.
    #[test]
    fn the_clock_aborts_rather_than_erroring() {
        let now = Utc::now();
        let mut job = job();
        job.apply(Change::Queue, now).unwrap();
        job.apply(Change::Start, now).unwrap();
        job.apply(
            Change::Abort {
                reason: Some("ran longer than 60 seconds".to_owned()),
            },
            now,
        )
        .unwrap();
        assert_eq!(job.phase, Phase::Aborted);
        assert!(job.error.as_deref().unwrap().contains("60"));
    }

    /// A job may be given up before it is committed, which is the case a client uses to
    /// throw away a job it built and changed its mind about.
    #[test]
    fn a_pending_job_can_be_aborted() {
        let mut job = job();
        job.apply(Change::Abort { reason: None }, Utc::now())
            .unwrap();
        assert_eq!(job.phase, Phase::Aborted);
        assert_eq!(job.error, None);
    }

    /// DALI: parameters may only be POSTed while PENDING. Refused rather than dropped —
    /// a parameter taken and ignored is a query the caller cannot tell from the one they
    /// asked for.
    #[test]
    fn a_parameter_is_taken_only_while_pending() {
        let now = Utc::now();
        let mut job = job();
        job.apply(
            Change::Parameter {
                name: "MAXREC".to_owned(),
                value: "10".to_owned(),
            },
            now,
        )
        .unwrap();
        // The same name again replaces rather than repeating, so the job carries one value.
        job.apply(
            Change::Parameter {
                name: "maxrec".to_owned(),
                value: "20".to_owned(),
            },
            now,
        )
        .unwrap();
        assert_eq!(
            job.parameters
                .iter()
                .filter(|(name, _)| name.eq_ignore_ascii_case("MAXREC"))
                .map(|(_, value)| value.as_str())
                .collect::<Vec<_>>(),
            ["20"]
        );

        job.apply(Change::Queue, now).unwrap();
        let refused = job
            .apply(
                Change::Parameter {
                    name: "MAXREC".to_owned(),
                    value: "30".to_owned(),
                },
                now,
            )
            .unwrap_err();
        assert_eq!(refused.status(), http::StatusCode::FORBIDDEN);
    }

    /// The destruction time is writable whatever the phase — a client extending the life of
    /// a result it has not collected yet is the main reason the resource exists. The
    /// execution duration is not, once the job has started: taking it and leaving the clock
    /// as it was is what UWS forbids, and forbidding the change is what it allows.
    #[test]
    fn what_may_be_rewritten_depends_on_the_phase() {
        let now = Utc::now();
        let mut job = job();
        job.apply(Change::Queue, now).unwrap();
        job.apply(Change::ExecutionDuration(TimeDelta::seconds(30)), now)
            .unwrap();
        assert_eq!(job.execution_duration, TimeDelta::seconds(30));

        job.apply(Change::Start, now).unwrap();
        assert!(
            job.apply(Change::ExecutionDuration(TimeDelta::seconds(5)), now)
                .is_err()
        );
        assert_eq!(job.execution_duration, TimeDelta::seconds(30));

        let later = now + TimeDelta::days(2);
        job.apply(Change::Destruction(later), now).unwrap();
        job.apply(Change::Complete(product()), now).unwrap();
        let later_still = now + TimeDelta::days(3);
        job.apply(Change::Destruction(later_still), now).unwrap();
        assert_eq!(job.destruction, later_still);
    }
}
