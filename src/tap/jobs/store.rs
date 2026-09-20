//! Where jobs are kept.
//!
//! One trait, so that the in-process map here can become a table later without anything
//! above it moving. What makes that possible is that a [`Job`] is entirely values and that
//! it changes only by [`Change`]: every method below maps onto one statement, and
//! [`JobStore::apply`] in particular is a single `UPDATE … WHERE` rather than a read, a
//! decision and a write — so no version column and no retry loop at any call site.
//!
//! **There is no method that lists every job**, and that is the visibility policy rather
//! than an omission. A job is visible to whoever holds its id; UWS §2.2.2.1 asks for the
//! jobs "that the client can see in the current security context" and §3 leaves what that
//! means to the service, so with no authentication an anonymous caller's context holds
//! nothing and the job list is empty. A method handing out every job would exist only to be
//! misused. What the service does need to enumerate for — expiry and the disk quota — is
//! [`JobStore::expire`] and [`JobStore::shed`], which return what they destroyed rather than
//! what exists.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::error::ApiError;
use crate::tap::jobs::id::JobId;
use crate::tap::jobs::job::{Change, Job};

#[async_trait]
pub trait JobStore: std::fmt::Debug + Send + Sync {
    /// Keep a new job.
    ///
    /// **Full is a `503` and not a refusal of the request**, which is nothing to do with the
    /// request: the caller's right move is to wait rather than to ask for less, and the same
    /// submission later is answered. Reaching the record bound never destroys an existing
    /// job — a burst of submissions must not be able to take away a result somebody has
    /// already been promised.
    async fn create(&self, job: Job) -> Result<(), ApiError>;

    /// The job, or nothing where no job has that id.
    ///
    /// Nothing rather than an error, so that the route answers a `404` the same way for an
    /// id that never existed, one that has been destroyed and one somebody guessed.
    async fn get(&self, id: &JobId) -> Result<Option<Job>, ApiError>;

    /// Apply one transition and hand back the job as it now is.
    ///
    /// Returning the record is what removes the read-then-write race: nothing above this has
    /// to look again, so nothing can act on a phase that moved in between.
    async fn apply(&self, id: &JobId, change: Change) -> Result<Job, ApiError>;

    /// Forget a job, returning it so its result file can go too.
    ///
    /// UWS §2.1.7: destroying one means "the service forgets that the job existed". Aborting
    /// whatever is running is the runner's half and happens before this.
    async fn delete(&self, id: &JobId) -> Result<Option<Job>, ApiError>;

    /// Forget every job whose destruction time has passed.
    async fn expire(&self, now: DateTime<Utc>) -> Result<Vec<Job>, ApiError>;

    /// Forget finished jobs, oldest first, until the results held fit in the quota.
    ///
    /// The oldest completed rather than the newest: the old one has already had its chance
    /// to be collected, and the new one is what somebody is waiting for.
    async fn shed(&self) -> Result<Vec<Job>, ApiError>;
}
