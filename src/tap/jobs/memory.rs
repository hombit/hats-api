//! The in-process store: a map behind a mutex, and the bounds it enforces.
//!
//! What a deployment gets from this is that a job lives as long as the process and no
//! longer. A restart loses every record, which is the destroyed-job case UWS already
//! describes and answers `404`, and two replicas do not share jobs at all — so a job created
//! on one is a `404` on the other until something durable takes this one's place. That is
//! the reason [`JobStore`] exists before there is a second implementation.
//!
//! A `std::sync::Mutex` and not an async one. Every critical section here is a map lookup
//! and a field write with no `await` in it, so the lock is never held across a suspension and
//! an async mutex would buy a slower uncontended path and nothing else.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::error::ApiError;
use crate::tap::jobs::id::JobId;
use crate::tap::jobs::job::{Change, Job, Phase};
use crate::tap::jobs::store::JobStore;

/// What the store will hold.
#[derive(Debug, Clone, Copy)]
pub struct Bounds {
    /// How many records, in any phase. Reaching it refuses a new job with a `503`.
    pub max_jobs: usize,
    /// How many bytes of held results together, across every job.
    pub max_result_bytes_total: u64,
}

/// Jobs kept in this process and nowhere else.
#[derive(Debug)]
pub struct MemoryJobStore {
    jobs: Mutex<HashMap<JobId, Job>>,
    bounds: Bounds,
}

impl MemoryJobStore {
    pub fn new(bounds: Bounds) -> Self {
        Self {
            jobs: Mutex::new(HashMap::new()),
            bounds,
        }
    }

    /// The lock, or the one failure a `Mutex` has.
    ///
    /// A poisoned lock means a thread panicked while holding it, which here means a panic
    /// between a map lookup and a field write. The map is still structurally sound —
    /// `HashMap` is not left half-updated by a panic in this code, there being no user
    /// callback inside the critical sections — so the poison is cleared and the work goes
    /// on. Refusing every job for the rest of the process's life is the worse answer.
    fn locked(&self) -> std::sync::MutexGuard<'_, HashMap<JobId, Job>> {
        self.jobs.lock().unwrap_or_else(|poisoned| {
            self.jobs.clear_poison();
            poisoned.into_inner()
        })
    }
}

#[async_trait]
impl JobStore for MemoryJobStore {
    async fn create(&self, job: Job) -> Result<(), ApiError> {
        let mut jobs = self.locked();
        if jobs.len() >= self.bounds.max_jobs {
            return Err(ApiError::unavailable(format!(
                "this service is holding the {} jobs it keeps at once; try again once some \
                 have been collected or have expired",
                self.bounds.max_jobs
            )));
        }
        jobs.insert(job.id.clone(), job);
        Ok(())
    }

    async fn get(&self, id: &JobId) -> Result<Option<Job>, ApiError> {
        Ok(self.locked().get(id).cloned())
    }

    async fn apply(&self, id: &JobId, change: Change) -> Result<Job, ApiError> {
        let mut jobs = self.locked();
        let job = jobs
            .get_mut(id)
            .ok_or_else(|| ApiError::not_found("no job of that name"))?;
        job.apply(change, Utc::now())?;
        Ok(job.clone())
    }

    async fn delete(&self, id: &JobId) -> Result<Option<Job>, ApiError> {
        Ok(self.locked().remove(id))
    }

    async fn expire(&self, now: DateTime<Utc>) -> Result<Vec<Job>, ApiError> {
        let mut jobs = self.locked();
        let expired: Vec<_> = jobs
            .values()
            .filter(|job| job.destruction <= now)
            .map(|job| job.id.clone())
            .collect();
        Ok(expired.iter().filter_map(|id| jobs.remove(id)).collect())
    }

    async fn shed(&self) -> Result<Vec<Job>, ApiError> {
        let mut jobs = self.locked();
        let mut held: u64 = jobs
            .values()
            .filter_map(|job| job.product.as_ref())
            .map(|product| product.bytes)
            .sum();
        if held <= self.bounds.max_result_bytes_total {
            return Ok(Vec::new());
        }
        // Oldest first by when the job ended, which is when its result started taking up
        // room — not by when it was created, since a long job that finished a moment ago is
        // newer than a short one created after it.
        let mut finished: Vec<_> = jobs
            .values()
            .filter(|job| job.phase == Phase::Completed && job.product.is_some())
            .map(|job| (job.ended.unwrap_or(job.created), job.id.clone()))
            .collect();
        finished.sort();

        let mut shed = Vec::new();
        for (_, id) in finished {
            if held <= self.bounds.max_result_bytes_total {
                break;
            }
            if let Some(job) = jobs.remove(&id) {
                held = held.saturating_sub(job.product.as_ref().map_or(0, |held| held.bytes));
                shed.push(job);
            }
        }
        Ok(shed)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;
    use crate::tap::jobs::job::Product;

    fn store(max_jobs: usize, max_result_bytes_total: u64) -> MemoryJobStore {
        MemoryJobStore::new(Bounds {
            max_jobs,
            max_result_bytes_total,
        })
    }

    fn job(now: DateTime<Utc>) -> Job {
        Job::new(
            JobId::new().unwrap(),
            vec![("QUERY".to_owned(), "SELECT 1".to_owned())],
            None,
            TimeDelta::seconds(60),
            now + TimeDelta::hours(1),
            now,
        )
    }

    fn product(bytes: u64) -> Product {
        Product {
            file: "x".to_owned(),
            content_type: "application/x-votable+xml".to_owned(),
            bytes,
            rows: 1,
            overflow: false,
        }
    }

    /// Finish a job with a result of this size, ended at this moment.
    async fn completed(store: &MemoryJobStore, bytes: u64, ended: DateTime<Utc>) -> JobId {
        let job = job(ended);
        let id = job.id.clone();
        store.create(job).await.unwrap();
        store.apply(&id, Change::Queue).await.unwrap();
        store.apply(&id, Change::Start).await.unwrap();
        store
            .apply(&id, Change::Complete(product(bytes)))
            .await
            .unwrap();
        // The store stamps `ended` with the wall clock, which these tests need to control.
        let mut jobs = store.locked();
        if let Some(job) = jobs.get_mut(&id) {
            job.ended = Some(ended);
        }
        drop(jobs);
        id
    }

    #[tokio::test]
    async fn a_job_goes_in_and_comes_back() {
        let store = store(4, 1 << 20);
        let job = job(Utc::now());
        let id = job.id.clone();
        store.create(job).await.unwrap();
        assert_eq!(store.get(&id).await.unwrap().unwrap().phase, Phase::Pending);

        let after = store.apply(&id, Change::Queue).await.unwrap();
        assert_eq!(after.phase, Phase::Queued);
        // What apply returned is what a later read says, so nothing has to look again.
        assert_eq!(store.get(&id).await.unwrap().unwrap().phase, Phase::Queued);
    }

    /// An id naming no job is the same answer whether it never existed or has gone, which
    /// is what keeps a guess from learning anything.
    #[tokio::test]
    async fn an_id_naming_no_job_is_not_found() {
        let store = store(4, 1 << 20);
        let missing = JobId::new().unwrap();
        assert!(store.get(&missing).await.unwrap().is_none());
        assert!(store.delete(&missing).await.unwrap().is_none());
        let refused = store.apply(&missing, Change::Queue).await.unwrap_err();
        assert_eq!(refused.status(), http::StatusCode::NOT_FOUND);
    }

    /// Full refuses the new job and keeps every old one: a burst of submissions must not be
    /// able to take away a result somebody has already been promised.
    #[tokio::test]
    async fn a_full_store_refuses_rather_than_evicting() {
        let store = store(2, 1 << 20);
        let now = Utc::now();
        let first = job(now);
        let kept = first.id.clone();
        store.create(first).await.unwrap();
        store.create(job(now)).await.unwrap();

        let refused = store.create(job(now)).await.unwrap_err();
        assert_eq!(refused.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        assert!(store.get(&kept).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn a_job_past_its_destruction_time_is_forgotten() {
        let store = store(8, 1 << 20);
        let now = Utc::now();
        let live = job(now);
        let live_id = live.id.clone();
        store.create(live).await.unwrap();

        let mut stale = job(now);
        stale.destruction = now - TimeDelta::seconds(1);
        let stale_id = stale.id.clone();
        store.create(stale).await.unwrap();

        let gone = store.expire(now).await.unwrap();
        assert_eq!(gone.len(), 1);
        assert_eq!(gone[0].id, stale_id);
        // Returned so the caller can delete the file, and really gone from the store.
        assert!(store.get(&stale_id).await.unwrap().is_none());
        assert!(store.get(&live_id).await.unwrap().is_some());
    }

    /// Over the quota the oldest finished job goes, and only as many as it takes.
    #[tokio::test]
    async fn shedding_takes_the_oldest_finished_jobs_until_it_fits() {
        let store = store(8, 250);
        let now = Utc::now();
        let oldest = completed(&store, 100, now - TimeDelta::seconds(30)).await;
        let middle = completed(&store, 100, now - TimeDelta::seconds(20)).await;
        let newest = completed(&store, 100, now - TimeDelta::seconds(10)).await;

        let shed = store.shed().await.unwrap();
        assert_eq!(shed.len(), 1, "300 bytes over a 250 quota is one job");
        assert_eq!(shed[0].id, oldest);
        assert!(store.get(&oldest).await.unwrap().is_none());
        assert!(store.get(&middle).await.unwrap().is_some());
        assert!(store.get(&newest).await.unwrap().is_some());

        // Under the quota nothing is taken, however many jobs there are.
        assert!(store.shed().await.unwrap().is_empty());
    }

    /// A job holding no result takes no room, so shedding never reaches for one — which is
    /// what stops a pending job being destroyed to make space for a finished one.
    #[tokio::test]
    async fn shedding_leaves_a_job_that_holds_nothing() {
        let store = store(8, 10);
        let now = Utc::now();
        let waiting = job(now);
        let waiting_id = waiting.id.clone();
        store.create(waiting).await.unwrap();
        let finished = completed(&store, 100, now).await;

        let shed = store.shed().await.unwrap();
        assert_eq!(shed.len(), 1);
        assert_eq!(shed[0].id, finished);
        assert!(store.get(&waiting_id).await.unwrap().is_some());
    }
}
