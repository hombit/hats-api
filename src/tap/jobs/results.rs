//! Where a finished job's rows are kept: one directory, one file per answer.
//!
//! The bytes never go in the store. A process holding every live job's answer at once is
//! what would exhaust it — the retention rather than the peak, a job living for minutes to a
//! day — and a row-backed store would want a blob column for something that is already a
//! file. So the record carries a name and the file carries the rows, and serving one is
//! serving a file, with the `Content-Length` and the ranged reads that come with that.
//!
//! # Whose leftovers are whose
//!
//! Each run takes a directory of its own under `[limits] scratch_dir`, with a lock file
//! beside it held open for the life of the process. A starting service sweeps the others: a
//! lock it can take is one no live process holds, so that run is dead and its directory
//! goes.
//!
//! **`flock` is the whole of why this works, and a destructor is not.** The kernel releases
//! a lock however the process ended — a clean exit, `SIGTERM`, the OOM killer, `kill -9`,
//! the power going out — and a reboot leaves none at all. A `TempDir` was the first answer
//! and cleans up only on an orderly `Drop`, which is exactly the case that does not need
//! cleaning: `SIGTERM` is how a container is stopped and runs no destructor, so results
//! would leak on every ordinary deployment rather than only on a crash.
//!
//! What a sweep cannot do is guess. Deleting by age, or deleting every directory that is not
//! this run's, both destroy the results of a process that is still serving them — a restart
//! overlapping a draining old one, which a rolling update does every time. The lock is what
//! turns that from a timing question into an answered one.
//!
//! Where there is no `flock` — anything that is not unix — the sweep is skipped and the
//! service says so once at startup. A crash there leaves a directory nothing reclaims, which
//! is worth a warning and is not worth refusing to run over.

use std::fs::File;
use std::path::{Path, PathBuf};

use tokio::io::AsyncWriteExt as _;

use crate::error::ApiError;
use crate::tap::jobs::id::JobId;

/// The directory under `scratch_dir` that every run's own directory sits in.
///
/// Named rather than using `scratch_dir` itself, because the sweep deletes what it finds:
/// it must look only at directories this service made, never at whatever else an operator
/// keeps beside them.
const PARENT: &str = "hats-api-jobs";

/// What a lock file is called, given its run's name.
fn lock_name(run: &str) -> String {
    format!("{run}.lock")
}

/// What an answer is called while it is still being written.
///
/// Beside the answer rather than elsewhere, so the rename into place cannot cross a
/// filesystem and stop being atomic — and under a name derived from the job's, so a run that
/// died mid-write leaves something its own cleanup can name.
fn partial(file: &str) -> String {
    format!("{file}.partial")
}

/// One run's results directory.
#[derive(Debug)]
pub struct Results {
    directory: PathBuf,
    /// Held open, and locked, for as long as this process runs. Dropping it releases the
    /// lock, which is what tells the next service that this run is over.
    #[expect(dead_code, reason = "the value is the lock; nothing reads the handle")]
    lock: File,
}

/// A written answer, as much of a [`super::Product`] as this module can say.
#[derive(Debug, Clone)]
pub struct Written {
    /// The file's name inside the directory.
    pub file: String,
    pub bytes: u64,
}

impl Results {
    /// Take this run's directory, and reclaim what dead runs left behind.
    ///
    /// **This is also the check that results can be written at all**, and it is why it
    /// happens at startup: a service that cannot write one cannot answer `/async`, which TAP
    /// §2.2 does not make optional, so it is an operator's mistake to hear before any caller
    /// meets it. Creating the directory asks what a probe file would ask.
    pub fn open(scratch_dir: Option<&Path>) -> Result<Self, ApiError> {
        let parent = match scratch_dir {
            Some(dir) => dir.join(PARENT),
            None => std::env::temp_dir().join(PARENT),
        };
        let failed = |what: &str, error: std::io::Error| {
            // The operator's path, in the operator's log. There is no caller yet to tell.
            ApiError::internal(format!("cannot {what} for job results: {error}"))
        };
        std::fs::create_dir_all(&parent).map_err(|error| failed("make a directory", error))?;

        // The lock before the directory, never the other way round: a process that died
        // between the two would otherwise leave a directory nothing could prove was dead.
        let run = JobId::new()?.to_string();
        let lock = File::create(parent.join(lock_name(&run)))
            .map_err(|error| failed("make a lock file", error))?;
        take(&lock).map_err(|error| failed("lock a lock file", error))?;
        let directory = parent.join(&run);
        std::fs::create_dir(&directory).map_err(|error| failed("make a directory", error))?;

        reclaim(&parent, &run);
        tracing::info!(directory = %directory.display(), "job results");
        Ok(Self { directory, lock })
    }

    /// Where a written answer is.
    pub fn path(&self, file: &str) -> PathBuf {
        self.directory.join(file)
    }

    /// Start writing one job's answer, a piece at a time.
    ///
    /// **What this is for is the peak.** An answer built whole and then written holds the
    /// document and the rows it was built from at once, and `max_result_bytes` — a gigabyte
    /// by default — could only be applied once all of it was in memory, so a job too large
    /// to keep cost that memory before anything could say so. Written as it is encoded, a
    /// job holds a batch, and the ceiling refuses at the byte that passes it.
    ///
    /// **Written under a temporary name and renamed into place**, so that a file existing
    /// means a whole answer. A crash midway leaves a partial file under a name no record
    /// points at, where the alternative is a truncated document served as a complete one —
    /// which a client cannot tell from the rows really ending there.
    pub fn writing(&self, id: &JobId, ceiling: u64) -> Writing {
        let file = id.to_string();
        Writing {
            destination: self.directory.join(&file),
            scratch: self.directory.join(partial(&file)),
            file,
            handle: None,
            bytes: 0,
            ceiling,
        }
    }

    /// Drop what a run that did not finish left half-written.
    ///
    /// Every ending but a kept answer comes through here, including the two that keep no
    /// [`Writing`] to speak for them: an aborted task and a panicked one are gone with
    /// their sink, and the bytes they had written are still on the disk.
    pub async fn abandon(&self, id: &JobId) {
        self.remove(&partial(&id.to_string())).await;
    }

    /// Drop one job's answer, a destroyed job's file going with it.
    ///
    /// Best effort and deliberately so: the record is already gone, the caller cannot be told
    /// anything, and a file that will not delete is the operator's to hear about. It is not a
    /// reason to leave the job in the store.
    pub async fn remove(&self, file: &str) {
        let path = self.path(file);
        if let Err(error) = tokio::fs::remove_file(&path).await
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(path = %path.display(), %error, "a job result could not be removed");
        }
    }
}

impl Drop for Results {
    /// Take this run's own directory with it on an orderly shutdown.
    ///
    /// Not what the design rests on — the sweep is, precisely because this does not run when
    /// the process is killed — but it means the ordinary case leaves nothing for the next
    /// service to find.
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
        if let Some(run) = self.directory.file_name().and_then(|run| run.to_str())
            && let Some(parent) = self.directory.parent()
        {
            let _ = std::fs::remove_file(parent.join(lock_name(run)));
        }
    }
}

/// Delete the directories of runs no process is holding any more.
///
/// Quiet about every failure: this is housekeeping, and a service that would not start
/// because somebody else's leftovers would not delete is worse than the leftovers.
fn reclaim(parent: &Path, ours: &str) {
    if !supported() {
        tracing::warn!(
            "job results left by a crash are not reclaimed on this platform, which has no \
             flock; remove stale directories under {} by hand",
            parent.display()
        );
        return;
    }
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(run) = name.strip_suffix(".lock") else {
            continue;
        };
        if run == ours {
            continue;
        }
        // A lock that opens and takes is one no live process holds — whatever became of the
        // process that made it, the kernel let this one go.
        let Ok(lock) = File::open(entry.path()) else {
            continue;
        };
        if take(&lock).is_err() {
            continue;
        }
        let directory = parent.join(run);
        if std::fs::remove_dir_all(&directory).is_ok() {
            tracing::info!(directory = %directory.display(), "reclaimed a dead run's results");
        }
        let _ = std::fs::remove_file(entry.path());
    }
}

/// Take an exclusive lock, or say it is held.
#[cfg(unix)]
fn take(file: &File) -> std::io::Result<()> {
    rustix::fs::flock(file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
        .map_err(|errno| std::io::Error::from_raw_os_error(errno.raw_os_error()))
}

#[cfg(not(unix))]
fn take(_file: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "no flock on this platform",
    ))
}

/// Whether a dead run's leftovers can be told from a live one's here.
const fn supported() -> bool {
    cfg!(unix)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A results directory of its own, under a temporary root that goes with the test.
    fn under(root: &Path) -> Results {
        Results::open(Some(root)).unwrap()
    }

    #[tokio::test]
    async fn an_answer_is_written_and_read_back() {
        let root = tempfile::tempdir().unwrap();
        let results = under(root.path());
        let id = JobId::new().unwrap();
        let mut writing = results.writing(&id, 1 << 20);
        writing.write(b"<VOTABLE>").await.unwrap();
        writing.write(b"</VOTABLE>").await.unwrap();
        // Nothing is under the job's own name until the last piece is in: a file that is
        // there is a whole answer.
        assert!(!results.path(&id.to_string()).exists());

        let written = writing.finish().await.unwrap();
        assert_eq!(written.bytes, 19);
        // Named after the job, which is what makes the file findable from the record alone.
        assert_eq!(written.file, id.to_string());
        assert_eq!(
            std::fs::read_to_string(results.path(&written.file)).unwrap(),
            "<VOTABLE></VOTABLE>"
        );
    }

    /// Nothing is left behind but the answer: the temporary the write went through is
    /// renamed rather than copied, so a directory holds one file per finished job.
    #[tokio::test]
    async fn a_write_leaves_one_file() {
        let root = tempfile::tempdir().unwrap();
        let results = under(root.path());
        for _ in 0..3 {
            let mut writing = results.writing(&JobId::new().unwrap(), 1 << 20);
            writing.write(b"rows").await.unwrap();
            writing.finish().await.unwrap();
        }
        assert_eq!(std::fs::read_dir(&results.directory).unwrap().count(), 3);
    }

    /// The ceiling stops the write at the piece that would pass it, rather than once the
    /// whole answer exists — which is the point of writing it as it is encoded.
    #[tokio::test]
    async fn a_write_over_the_ceiling_is_refused_where_it_passes_it() {
        let root = tempfile::tempdir().unwrap();
        let results = under(root.path());
        let id = JobId::new().unwrap();
        let mut writing = results.writing(&id, 6);
        writing.write(b"rows\n").await.unwrap();
        let refused = writing.write(b"and more rows\n").await.unwrap_err();
        assert!(refused.to_string().contains("6 bytes"), "{refused}");

        // What it had written is under a name no record points at, and goes when the run
        // that did not finish is swept.
        assert!(!results.path(&id.to_string()).exists());
        results.abandon(&id).await;
        assert_eq!(std::fs::read_dir(&results.directory).unwrap().count(), 0);
        // A job that never wrote a byte has nothing to sweep, which is not a failure.
        results.abandon(&JobId::new().unwrap()).await;
    }

    /// An answer of no bytes is still an answer: an empty table is a document with a header
    /// and no rows, and the record points at a file either way.
    #[tokio::test]
    async fn an_answer_of_no_bytes_is_still_a_file() {
        let root = tempfile::tempdir().unwrap();
        let results = under(root.path());
        let written = results
            .writing(&JobId::new().unwrap(), 1 << 20)
            .finish()
            .await
            .unwrap();
        assert_eq!(written.bytes, 0);
        assert!(results.path(&written.file).is_file());
    }

    #[tokio::test]
    async fn a_removed_answer_is_gone_and_removing_it_twice_is_quiet() {
        let root = tempfile::tempdir().unwrap();
        let results = under(root.path());
        let mut writing = results.writing(&JobId::new().unwrap(), 1 << 20);
        writing.write(b"rows").await.unwrap();
        let written = writing.finish().await.unwrap();
        results.remove(&written.file).await;
        assert!(!results.path(&written.file).exists());
        // A destroyed job whose file has already gone is not a failure to report.
        results.remove(&written.file).await;
    }

    /// The orderly case: a shutdown that runs destructors leaves nothing at all.
    #[test]
    fn a_clean_shutdown_takes_its_own_directory_and_lock() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join(PARENT);
        {
            let results = under(root.path());
            assert!(results.directory.is_dir());
        }
        assert_eq!(std::fs::read_dir(&parent).unwrap().count(), 0);
    }

    /// The case a destructor cannot cover, which is the one that matters: a run whose
    /// process is gone without unwinding. Its lock is not held, so the next service takes
    /// it and the directory goes.
    #[cfg(unix)]
    #[test]
    fn a_dead_run_s_results_are_reclaimed() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join(PARENT);
        std::fs::create_dir_all(&parent).unwrap();

        // A run that died: a directory with rows in it, and a lock nobody holds.
        let dead = "aaaaaaaaaaaaaaaaaaaaaa";
        std::fs::create_dir(parent.join(dead)).unwrap();
        std::fs::write(parent.join(dead).join("rows"), "x").unwrap();
        File::create(parent.join(lock_name(dead))).unwrap();

        let results = under(root.path());
        assert!(!parent.join(dead).exists(), "the dead run's rows survived");
        assert!(!parent.join(lock_name(dead)).exists());
        assert!(results.directory.is_dir(), "our own run went too");
    }

    /// The case a sweep by age or by name gets wrong: a service starting while another is
    /// still serving. The live run holds its lock, so its results are left alone.
    #[cfg(unix)]
    #[test]
    fn a_live_run_s_results_are_left_alone() {
        let root = tempfile::tempdir().unwrap();
        let live = under(root.path());
        let live_file = live.directory.join("rows");
        std::fs::write(&live_file, "x").unwrap();

        let starting = under(root.path());
        assert!(
            live_file.exists(),
            "a starting service destroyed a running one's results"
        );
        assert!(starting.directory.is_dir());
        assert_ne!(live.directory, starting.directory);
    }

    /// A scratch directory that cannot be made is an operator's mistake, and one they hear
    /// about at startup rather than on the first job.
    #[test]
    fn a_directory_that_cannot_be_made_is_a_startup_failure() {
        let refused = Results::open(Some(Path::new("/dev/null/nowhere"))).unwrap_err();
        assert_eq!(
            refused.status(),
            http::StatusCode::INTERNAL_SERVER_ERROR,
            "{refused}"
        );
    }
}

/// One job's answer, being written.
///
/// Opened lazily: a job that fails before its first byte leaves no file at all, which is
/// what a failed job should leave. Every write counts against the ceiling, and the first one
/// past it refuses — what was written by then is the runner's to sweep, with
/// [`Results::abandon`].
#[derive(Debug)]
pub struct Writing {
    /// The name a finished answer is served under.
    file: String,
    destination: PathBuf,
    /// Where the bytes go until they are all there.
    scratch: PathBuf,
    handle: Option<tokio::fs::File>,
    bytes: u64,
    ceiling: u64,
}

impl Writing {
    /// Add a piece of the document.
    ///
    /// The ceiling is checked before the bytes are written rather than after: what it
    /// bounds is what a job may leave on disk, and a file that went over it and was then
    /// deleted still cost the disk it was written to.
    pub async fn write(&mut self, chunk: &[u8]) -> Result<(), ApiError> {
        if chunk.is_empty() {
            return Ok(());
        }
        let would_be = self.bytes.saturating_add(chunk.len() as u64);
        if would_be > self.ceiling {
            return Err(ApiError::too_much_work(format!(
                "this job's answer is over {} bytes and this service keeps at most that per \
                 job; ask for fewer columns or fewer rows",
                self.ceiling
            )));
        }
        let failed = |error: std::io::Error| {
            ApiError::internal(format!("cannot write a job's result: {error}"))
        };
        let handle = match &mut self.handle {
            Some(handle) => handle,
            // Made on the first piece, which is why a job that fails before one leaves no
            // file at all rather than an empty one.
            none => none.insert(
                tokio::fs::File::create(&self.scratch)
                    .await
                    .map_err(failed)?,
            ),
        };
        handle.write_all(chunk).await.map_err(failed)?;
        self.bytes = would_be;
        Ok(())
    }

    /// Put the finished answer under its own name.
    ///
    /// Taking `&mut self` rather than `self` so that the sink is still there to be swept
    /// when this fails: the rename is the last thing that can go wrong, and what it leaves
    /// behind when it does is the same partial file every other ending leaves.
    pub async fn finish(&mut self) -> Result<Written, ApiError> {
        let failed = |error: std::io::Error| {
            ApiError::internal(format!("cannot keep a job's result: {error}"))
        };
        // An answer of no bytes is still an answer — an empty table is a document with a
        // header and no rows, and for a format that writes nothing at all it is a file of
        // no bytes rather than no file.
        let mut handle = match self.handle.take() {
            Some(handle) => handle,
            None => tokio::fs::File::create(&self.scratch)
                .await
                .map_err(failed)?,
        };
        handle.flush().await.map_err(failed)?;
        drop(handle);
        // Within the directory, so the rename cannot cross a filesystem and stop being
        // atomic.
        tokio::fs::rename(&self.scratch, &self.destination)
            .await
            .map_err(failed)?;
        Ok(Written {
            file: self.file.clone(),
            bytes: self.bytes,
        })
    }
}
