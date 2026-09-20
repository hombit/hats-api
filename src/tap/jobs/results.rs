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

use tempfile::NamedTempFile;

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

    /// Write one job's answer.
    ///
    /// **Written under a temporary name and renamed into place**, so that a file existing
    /// means a whole answer. A crash midway leaves a partial file under a name no record
    /// points at, where the alternative is a truncated document served as a complete one —
    /// which a client cannot tell from the rows really ending there.
    ///
    /// On the blocking pool: the body can be hundreds of megabytes, and writing it is the
    /// one genuinely blocking thing a job does.
    pub async fn write(&self, id: &JobId, body: String) -> Result<Written, ApiError> {
        let directory = self.directory.clone();
        let file = id.to_string();
        let destination = directory.join(&file);
        let bytes = tokio::task::spawn_blocking(move || {
            use std::io::Write as _;

            let mut scratch = NamedTempFile::new_in(&directory)?;
            scratch.write_all(body.as_bytes())?;
            scratch.flush()?;
            // Renamed rather than written in place, and within the same directory so the
            // rename cannot cross a filesystem and stop being atomic.
            scratch.persist(&destination).map_err(|error| error.error)?;
            Ok::<_, std::io::Error>(body.len() as u64)
        })
        .await
        .map_err(|error| ApiError::internal(format!("writing a job's result failed: {error}")))?
        .map_err(|error| ApiError::internal(format!("cannot write a job's result: {error}")))?;
        Ok(Written { file, bytes })
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
        let written = results.write(&id, "<VOTABLE/>".to_owned()).await.unwrap();
        assert_eq!(written.bytes, 10);
        // Named after the job, which is what makes the file findable from the record alone.
        assert_eq!(written.file, id.to_string());
        assert_eq!(
            std::fs::read_to_string(results.path(&written.file)).unwrap(),
            "<VOTABLE/>"
        );
    }

    /// Nothing is left behind but the answer: the temporary the write went through is
    /// renamed rather than copied, so a directory holds one file per finished job.
    #[tokio::test]
    async fn a_write_leaves_one_file() {
        let root = tempfile::tempdir().unwrap();
        let results = under(root.path());
        for _ in 0..3 {
            results
                .write(&JobId::new().unwrap(), "rows".to_owned())
                .await
                .unwrap();
        }
        assert_eq!(std::fs::read_dir(&results.directory).unwrap().count(), 3);
    }

    #[tokio::test]
    async fn a_removed_answer_is_gone_and_removing_it_twice_is_quiet() {
        let root = tempfile::tempdir().unwrap();
        let results = under(root.path());
        let written = results
            .write(&JobId::new().unwrap(), "rows".to_owned())
            .await
            .unwrap();
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
