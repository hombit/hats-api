//! Running a job: the slot it waits for, the clock over it, and how it stops.
//!
//! **This is the half that is not data.** The abort handles and the callers' credentials are
//! a table in this process and reach no store — a restart has no running jobs by definition,
//! so there is nothing for a durable store to reconstruct, and a credential that never
//! reaches a store is one a durable store can never write to disk.
//!
//! What a job actually *does* is [`Work`], supplied by the layer that knows what a job is
//! for. The dependency points that way round because `tap::jobs` is the job model and the
//! query is the HTTP layer's; it also means the things worth testing here — the clock, an
//! abort, a panic, the slots — are testable without a query, a catalog or a service.
//!
//! **Every failure lands on a phase.** Nobody is waiting on a request, so there is no status
//! to return: a job that fails without recording it polls as `EXECUTING` until its clock runs
//! out, which a client cannot tell from a slow query. The five ways one ends are each turned
//! into a transition below, and the panic is the one that would otherwise leave no trace.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::Semaphore;
use tokio::task::AbortHandle;

use crate::error::ApiError;
use crate::tap::jobs::id::JobId;
use crate::tap::jobs::job::{Change, Job, Product};
use crate::tap::jobs::results::{Results, Writing};
use crate::tap::jobs::store::JobStore;

/// What a job's work produced, beside the bytes it wrote.
///
/// **The document is not here.** It has gone into the file as it was made, which is what
/// keeps a large answer from existing twice — once as rows and once as a `String` — before
/// anything can say it is too large. What is left is what the record holds about it.
#[derive(Debug)]
pub struct Rendered {
    pub content_type: String,
    pub rows: usize,
    pub overflow: bool,
}

/// Whatever it is that a job does.
///
/// One method, taking the job, the credentials held for it, and the sink its answer goes
/// into. The credentials arrive as an argument rather than off the record precisely because
/// they are not on the record; the sink arrives as one because where a job's answer is kept
/// is the runner's business and not the work's.
#[async_trait]
pub trait Work: std::fmt::Debug + Send + Sync + 'static {
    async fn run(
        &self,
        job: Job,
        credentials: Vec<String>,
        into: &mut Writing,
    ) -> Result<Rendered, ApiError>;
}

/// What the runner will spend.
#[derive(Debug, Clone, Copy)]
pub struct Slots {
    /// How many jobs execute at once. The rest stay `QUEUED`, which is what that phase is
    /// for. This is also the only thing bounding CPU: there is no per-task CPU measure, so
    /// what a deployment actually limits is this against DataFusion's `target_partitions`.
    pub max_running: usize,
    /// The largest answer one job may leave on disk.
    pub max_result_bytes: u64,
}

/// The jobs this process is running.
#[derive(Debug)]
pub struct Runner {
    store: Arc<dyn JobStore>,
    results: Arc<Results>,
    work: Arc<dyn Work>,
    slots: Arc<Semaphore>,
    limits: Slots,
    /// How a running job is stopped. Present only while it runs.
    running: Mutex<HashMap<JobId, AbortHandle>>,
    /// The `UPLOAD_STORAGE_OPTION` values a job was created with.
    ///
    /// Here and nowhere else. A job outlives the request that made it, so the value has to
    /// be kept somewhere, and the store is the one place it must not be: a row-backed store
    /// would write a caller's secret to disk, and a job document would hand it to whoever
    /// holds the id.
    credentials: Mutex<HashMap<JobId, Vec<String>>>,
}

/// How one run ended, which is the whole of what decides the phase.
enum Ending {
    /// The answer is written, and the sink is handed back to be put into place.
    Done(Rendered, Writing),
    Failed(ApiError),
    /// The execution duration ran out.
    Clock,
    /// Somebody aborted it, or the job was destroyed underneath it.
    Cancelled,
    Panicked(String),
}

impl Runner {
    pub fn new(
        store: Arc<dyn JobStore>,
        results: Arc<Results>,
        work: Arc<dyn Work>,
        limits: Slots,
    ) -> Self {
        Self {
            store,
            results,
            work,
            slots: Arc::new(Semaphore::new(limits.max_running)),
            limits,
            running: Mutex::new(HashMap::new()),
            credentials: Mutex::new(HashMap::new()),
        }
    }

    /// Keep a job's credentials for as long as this process runs it.
    pub fn hold(&self, id: &JobId, credentials: Vec<String>) {
        if credentials.is_empty() {
            return;
        }
        locked(&self.credentials).insert(id.clone(), credentials);
    }

    /// `PHASE=RUN`: commit the job and set it going.
    ///
    /// The phase moves to `QUEUED` here, synchronously, so the caller's next read cannot see
    /// a job it just committed still saying `PENDING`. Whether a slot is free is the spawned
    /// task's problem, which is exactly what `QUEUED` means.
    pub async fn start(self: &Arc<Self>, id: &JobId) -> Result<Job, ApiError> {
        let queued = self.store.apply(id, Change::Queue).await?;
        let runner = Arc::clone(self);
        let id = id.clone();
        tokio::spawn(async move { runner.execute(id).await });
        Ok(queued)
    }

    /// `PHASE=ABORT`, or a `DELETE` on a job that is still going.
    ///
    /// The record moves first and the task is stopped after, so a caller who reads back
    /// immediately sees `ABORTED` rather than a job that is still `EXECUTING`. The task
    /// notices it was cancelled and records nothing further — its transition would be
    /// refused anyway, a terminal phase never being left.
    pub async fn abort(&self, id: &JobId) -> Result<Job, ApiError> {
        let aborted = self.store.apply(id, Change::Abort { reason: None }).await?;
        self.stop(id);
        Ok(aborted)
    }

    /// Stop whatever is running for this job and forget everything process-local about it.
    pub fn stop(&self, id: &JobId) {
        if let Some(handle) = locked(&self.running).remove(id) {
            handle.abort();
        }
        locked(&self.credentials).remove(id);
    }

    /// The whole of one job's run, from waiting for a slot to recording how it ended.
    async fn execute(self: Arc<Self>, id: JobId) {
        // Dropped at the end of this function whichever way it goes, which is what hands the
        // slot to the next queued job even when this one panicked.
        let Ok(_slot) = Arc::clone(&self.slots).acquire_owned().await else {
            // Only when the semaphore is closed, which is shutdown.
            return;
        };
        // Between the commit and the slot the job may have been aborted or destroyed. Both
        // refuse this transition, and both mean there is nothing left to do.
        let Ok(job) = self.store.apply(&id, Change::Start).await else {
            return;
        };
        let credentials = locked(&self.credentials)
            .get(&id)
            .cloned()
            .unwrap_or_default();

        let work = Arc::clone(&self.work);
        // Made here rather than inside the work, because the ceiling and the directory are
        // the runner's; handed back with the answer, so that what is put into place is the
        // same sink the rows went into.
        let mut into = self.results.writing(&id, self.limits.max_result_bytes);
        let mut running = tokio::spawn({
            let job = job.clone();
            async move {
                let rendered = work.run(job, credentials, &mut into).await;
                (rendered, into)
            }
        });
        locked(&self.running).insert(id.clone(), running.abort_handle());

        // The clock starts here and not at the commit: the duration is time spent running,
        // so a job waiting for a slot is not spending it.
        let duration = job
            .execution_duration
            .to_std()
            .unwrap_or(std::time::Duration::MAX);
        let ending = match tokio::time::timeout(duration, &mut running).await {
            Err(_elapsed) => {
                // Stopped, and waited for: a timeout drops the handle rather than the task,
                // and a task still holding its sink would write again after what it had
                // written was swept.
                running.abort();
                let _ = running.await;
                Ending::Clock
            }
            Ok(Ok((Ok(rendered), into))) => Ending::Done(rendered, into),
            // The sink is dropped here and what it had written is swept below, as it is for
            // every other ending that keeps nothing.
            Ok(Ok((Err(refused), _))) => Ending::Failed(refused),
            // A task that was aborted and one that panicked are both a `JoinError`, and the
            // difference is the whole reason this is not one arm: a cancelled job has
            // already recorded why it stopped, and a panicked one has recorded nothing.
            Ok(Err(join)) if join.is_cancelled() => Ending::Cancelled,
            Ok(Err(join)) => Ending::Panicked(join.to_string()),
        };
        self.finish(&id, ending, &job).await;
        self.stop(&id);
    }

    /// Turn how the run ended into the phase it leaves behind.
    async fn finish(&self, id: &JobId, ending: Ending, job: &Job) {
        let change = match ending {
            Ending::Done(rendered, into) => match self.keep(rendered, into).await {
                Ok(product) => Change::Complete(product),
                Err(refused) => Change::Fail(refused.to_string()),
            },
            Ending::Failed(refused) => Change::Fail(refused.to_string()),
            // UWS §2.1: exceeding the execution duration has "the same effect as when a
            // manual 'Abort' is requested", so this is ABORTED and not ERROR.
            Ending::Clock => Change::Abort {
                reason: Some(format!(
                    "this job ran for longer than the {} seconds allowed it",
                    job.execution_duration.num_seconds()
                )),
            },
            // Already recorded by whoever cancelled it. The sink went with the aborted
            // task, so what it had written is swept here rather than by it.
            Ending::Cancelled => {
                self.results.abandon(id).await;
                return;
            }
            Ending::Panicked(how) => {
                // The message a caller gets says nothing about this machine; the log says
                // everything. A panic reaching here is a bug in this service, and the one
                // ending that would otherwise leave a job EXECUTING with nothing to read.
                tracing::error!(job = %id, panic = %how, "a job panicked");
                Change::Fail("this job failed unexpectedly".to_owned())
            }
        };
        // Every ending but a kept answer leaves a half-written file under a name no record
        // points at — the ceiling refused part-way, the clock ran out, the work failed.
        let kept = matches!(change, Change::Complete(_));
        if !kept {
            self.results.abandon(id).await;
        }
        // A job destroyed or aborted while it was finishing refuses this, which is right —
        // a terminal phase is never left. What it leaves behind is a file nothing points at,
        // so that goes too.
        if let Err(refused) = self.store.apply(id, change).await {
            tracing::debug!(job = %id, %refused, "a finished job was already gone");
            if kept {
                self.results.remove(&id.to_string()).await;
            }
            return;
        }
        if kept {
            self.reclaim().await;
        }
    }

    /// Put the written answer under the job's own name.
    ///
    /// Nothing is measured here: the bytes went through the sink, which holds the ceiling
    /// and refuses at the byte that passes it — while the answer is being made rather than
    /// once all of it exists.
    async fn keep(&self, rendered: Rendered, mut into: Writing) -> Result<Product, ApiError> {
        let written = into.finish().await?;
        Ok(Product {
            file: written.file,
            content_type: rendered.content_type,
            bytes: written.bytes,
            rows: rendered.rows,
            overflow: rendered.overflow,
        })
    }

    /// Bring the results held back under the quota, deleting what the store gave up.
    async fn reclaim(&self) {
        let shed = match self.store.shed().await {
            Ok(shed) => shed,
            Err(error) => {
                tracing::error!(%error, "the job store could not be swept");
                return;
            }
        };
        for job in shed {
            tracing::info!(job = %job.id, "a job was destroyed to stay within the result quota");
            if let Some(product) = &job.product {
                self.results.remove(&product.file).await;
            }
            self.stop(&job.id);
        }
    }

    /// Destroy every job whose time has come, and the files with them.
    pub async fn expire(&self) {
        let expired = match self.store.expire(Utc::now()).await {
            Ok(expired) => expired,
            Err(error) => {
                tracing::error!(%error, "the job store could not be swept");
                return;
            }
        };
        for job in expired {
            self.stop(&job.id);
            if let Some(product) = &job.product {
                self.results.remove(&product.file).await;
            }
        }
    }
}

/// A poisoned lock is cleared rather than propagated: every critical section here is a map
/// operation with no `await` in it, so a panic cannot have left one half-written, and
/// refusing every job for the rest of the process's life is the worse answer.
fn locked<T>(lock: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| {
        lock.clear_poison();
        poisoned.into_inner()
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::TimeDelta;

    use super::*;
    use crate::tap::jobs::job::Phase;
    use crate::tap::jobs::memory::{Bounds, MemoryJobStore};

    /// What a job does, in these tests.
    #[derive(Debug)]
    enum Doing {
        Answer(&'static str),
        Refuse,
        Panic,
        /// Run until something stops it, which is what an abort and the clock both need.
        Forever,
        /// Long enough to still be running while another job is submitted.
        Slowly,
    }

    #[derive(Debug)]
    struct Fake {
        doing: Doing,
        /// The credentials the work was handed, so a test can see they arrived.
        seen: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl Work for Fake {
        async fn run(
            &self,
            _job: Job,
            credentials: Vec<String>,
            into: &mut Writing,
        ) -> Result<Rendered, ApiError> {
            locked(&self.seen).extend(credentials);
            match self.doing {
                // A byte at a time, so that a ceiling reached part-way is reached where a
                // real answer would reach it rather than on one whole write.
                Doing::Answer(body) => {
                    for byte in body.as_bytes() {
                        into.write(&[*byte]).await?;
                    }
                    Ok(Rendered {
                        content_type: "text/csv".to_owned(),
                        rows: 1,
                        overflow: false,
                    })
                }
                Doing::Refuse => Err(ApiError::bad_request("that column is not there")),
                #[expect(
                    clippy::panic,
                    reason = "the panic is the thing under test: a job that panics must reach \
                              ERROR rather than sit in EXECUTING with nothing to read"
                )]
                Doing::Panic => panic!("the query exploded"),
                Doing::Forever => {
                    std::future::pending::<()>().await;
                    unreachable!()
                }
                Doing::Slowly => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    into.write(b"slow").await?;
                    Ok(Rendered {
                        content_type: "text/csv".to_owned(),
                        rows: 1,
                        overflow: false,
                    })
                }
            }
        }
    }

    struct Harness {
        runner: Arc<Runner>,
        store: Arc<MemoryJobStore>,
        results: Arc<Results>,
        work: Arc<Fake>,
    }

    fn harness(doing: Doing, max_running: usize, max_result_bytes: u64) -> Harness {
        let store = Arc::new(MemoryJobStore::new(Bounds {
            max_jobs: 16,
            max_result_bytes_total: 1 << 20,
        }));
        let results = Arc::new(Results::open(None).unwrap());
        let work = Arc::new(Fake {
            doing,
            seen: Mutex::new(Vec::new()),
        });
        let runner = Arc::new(Runner::new(
            Arc::clone(&store) as Arc<dyn JobStore>,
            Arc::clone(&results),
            Arc::clone(&work) as Arc<dyn Work>,
            Slots {
                max_running,
                max_result_bytes,
            },
        ));
        Harness {
            runner,
            store,
            results,
            work,
        }
    }

    async fn submitted(harness: &Harness, seconds: i64) -> JobId {
        let now = Utc::now();
        let job = Job::new(
            JobId::new().unwrap(),
            vec![("QUERY".to_owned(), "SELECT 1".to_owned())],
            None,
            TimeDelta::seconds(seconds),
            now + TimeDelta::hours(1),
            now,
        );
        let id = job.id.clone();
        harness.store.create(job).await.unwrap();
        id
    }

    /// Poll until the job stops, so a test never depends on how long the runtime took.
    async fn settled(harness: &Harness, id: &JobId) -> Job {
        for _ in 0..500 {
            let job = harness.store.get(id).await.unwrap().unwrap();
            if job.phase.is_terminal() {
                return job;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let job = harness.store.get(id).await.unwrap().unwrap();
        unreachable!("the job was still {} after five seconds", job.phase);
    }

    #[tokio::test]
    async fn a_job_that_answers_completes_and_leaves_its_rows_in_a_file() {
        let harness = harness(Doing::Answer("a,b\n1,2\n"), 2, 1 << 20);
        let id = submitted(&harness, 30).await;
        harness.runner.start(&id).await.unwrap();

        let job = settled(&harness, &id).await;
        assert_eq!(job.phase, Phase::Completed);
        let product = job.product.unwrap();
        assert_eq!(product.bytes, 8);
        assert_eq!(product.content_type, "text/csv");
        let path = harness.results.path(&product.file);
        assert_eq!(std::fs::read_to_string(path).unwrap(), "a,b\n1,2\n");
    }

    /// A refusal is the job's, not the request's: ERROR, with the message a client reads out
    /// of the error document.
    #[tokio::test]
    async fn a_job_that_is_refused_ends_in_error_with_the_reason() {
        let harness = harness(Doing::Refuse, 2, 1 << 20);
        let id = submitted(&harness, 30).await;
        harness.runner.start(&id).await.unwrap();

        let job = settled(&harness, &id).await;
        assert_eq!(job.phase, Phase::Error);
        assert!(job.error.unwrap().contains("that column is not there"));
        assert!(job.product.is_none());
    }

    /// The ending that would otherwise leave no trace at all. A spawned task nobody joins
    /// takes its panic with it, and the job sits in EXECUTING until its clock runs out —
    /// which a client cannot tell from a slow query.
    #[tokio::test]
    async fn a_job_that_panics_ends_in_error_rather_than_running_forever() {
        let harness = harness(Doing::Panic, 2, 1 << 20);
        let id = submitted(&harness, 30).await;
        harness.runner.start(&id).await.unwrap();

        let job = settled(&harness, &id).await;
        assert_eq!(job.phase, Phase::Error);
        // What the caller is told says nothing about this machine.
        let said = job.error.unwrap();
        assert_eq!(said, "this job failed unexpectedly");
        assert!(!said.contains("exploded"), "{said}");
    }

    /// UWS §2.1: an exceeded execution duration has "the same effect as when a manual
    /// 'Abort' is requested", so the phase is ABORTED and the reason names the bound.
    #[tokio::test]
    async fn the_clock_aborts_a_job_that_runs_too_long() {
        let harness = harness(Doing::Forever, 2, 1 << 20);
        let now = Utc::now();
        let job = Job::new(
            JobId::new().unwrap(),
            Vec::new(),
            None,
            // The shortest the record can express, so the test does not wait a second.
            TimeDelta::milliseconds(50),
            now + TimeDelta::hours(1),
            now,
        );
        let id = job.id.clone();
        harness.store.create(job).await.unwrap();
        harness.runner.start(&id).await.unwrap();

        let job = settled(&harness, &id).await;
        assert_eq!(job.phase, Phase::Aborted);
        assert!(job.error.unwrap().contains("longer than"));
    }

    /// The record moves first, so a caller reading back immediately sees ABORTED rather than
    /// a job still claiming to execute — and the work really does stop.
    #[tokio::test]
    async fn aborting_stops_the_work_and_says_so_at_once() {
        let harness = harness(Doing::Forever, 2, 1 << 20);
        let id = submitted(&harness, 300).await;
        harness.runner.start(&id).await.unwrap();
        // Let it reach EXECUTING, so what is aborted is a running job.
        for _ in 0..200 {
            if harness.store.get(&id).await.unwrap().unwrap().phase == Phase::Executing {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let aborted = harness.runner.abort(&id).await.unwrap();
        assert_eq!(aborted.phase, Phase::Aborted);
        // And nothing overwrites it afterwards: the task notices it was cancelled and
        // records nothing, a terminal phase never being left.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let job = harness.store.get(&id).await.unwrap().unwrap();
        assert_eq!(job.phase, Phase::Aborted);
        assert_eq!(job.error, None);
    }

    /// One slot means the second job waits in QUEUED, which is what that phase is for.
    #[tokio::test]
    async fn a_second_job_waits_for_a_slot() {
        let harness = harness(Doing::Slowly, 1, 1 << 20);
        let first = submitted(&harness, 30).await;
        let second = submitted(&harness, 30).await;
        harness.runner.start(&first).await.unwrap();
        harness.runner.start(&second).await.unwrap();

        // While the first is running the second has not started, so it has no start time.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let waiting = harness.store.get(&second).await.unwrap().unwrap();
        assert_eq!(waiting.phase, Phase::Queued);
        assert_eq!(waiting.started, None);

        // The slot is handed on, so both finish.
        assert_eq!(settled(&harness, &first).await.phase, Phase::Completed);
        assert_eq!(settled(&harness, &second).await.phase, Phase::Completed);
    }

    /// An answer too large to keep is the job's failure and not a file left on disk — and
    /// it is refused at the byte that passes the ceiling, so the rest is never written.
    #[tokio::test]
    async fn an_answer_over_the_ceiling_is_refused_and_written_nowhere() {
        let harness = harness(Doing::Answer("a,b\n1,2\n"), 2, 4);
        let id = submitted(&harness, 30).await;
        harness.runner.start(&id).await.unwrap();

        let job = settled(&harness, &id).await;
        assert_eq!(job.phase, Phase::Error);
        assert!(job.error.unwrap().contains("4 bytes"));
        assert!(job.product.is_none());
        assert_eq!(
            std::fs::read_dir(harness.results.path("")).unwrap().count(),
            0
        );
    }

    /// The credentials reach the work and never the record — which is the whole reason they
    /// are held here rather than on the job.
    #[tokio::test]
    async fn credentials_reach_the_work_and_not_the_store() {
        let harness = harness(Doing::Answer("x"), 2, 1 << 20);
        let id = submitted(&harness, 30).await;
        harness
            .runner
            .hold(&id, vec!["t,secret_access_key,hunter2".to_owned()]);
        harness.runner.start(&id).await.unwrap();

        let job = settled(&harness, &id).await;
        assert_eq!(job.phase, Phase::Completed);
        assert_eq!(
            locked(&harness.work.seen).as_slice(),
            ["t,secret_access_key,hunter2".to_owned()]
        );
        // Not on the record, and not in what a Debug of it would print.
        assert!(!format!("{job:?}").contains("hunter2"));
        // And forgotten once the job is over, so nothing holds it longer than the run.
        assert!(locked(&harness.runner.credentials).is_empty());
    }

    /// A job destroyed while it was finishing keeps no file: the transition is refused, a
    /// terminal phase never being left, and what it would have pointed at goes too.
    #[tokio::test]
    async fn a_job_destroyed_mid_flight_leaves_no_file_behind() {
        let harness = harness(Doing::Slowly, 2, 1 << 20);
        let id = submitted(&harness, 30).await;
        harness.runner.start(&id).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        harness.store.delete(&id).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(harness.store.get(&id).await.unwrap().is_none());
        assert_eq!(
            std::fs::read_dir(harness.results.path("")).unwrap().count(),
            0
        );
    }
}
