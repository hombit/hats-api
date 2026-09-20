//! Where a finished job's rows are kept: one directory, one file per answer.
//!
//! The bytes never go in the store. A process holding every live job's answer at once is
//! what would exhaust it — the retention rather than the peak, a job living for minutes to a
//! day — and a row-backed store would want a blob column for something that is already a
//! file. So the record carries a name and the file carries the rows, and serving one is
//! serving a file, with the `Content-Length` and the ranged reads that come with that.
//!
//! **The directory is a [`TempDir`], and that is the whole of the lifetime story.** It is
//! created under `[limits] scratch_dir` beside the copies `MaterializingStore` already puts
//! there, and it removes itself and everything in it when the service shuts down.
//!
//! The alternative, and why it is not here: a fixed directory swept at startup. Sweeping
//! cannot tell a dead process's leftovers from a live process's working files, so a restart
//! that overlaps a draining old process — a rolling update, or any graceful shutdown — would
//! delete results that old process is still serving. Naming each run's directory separately
//! does not fix it either, since the newcomer still cannot tell which of the other names is
//! alive. What would is a lock file per run, and a private temporary directory is the same
//! guarantee for none of the machinery.
//!
//! What it costs is that a crash leaks one directory, nothing being left to drop it. That is
//! the leak `NamedTempFile` already has here for a materialized copy, handled the same way:
//! under the system temporary directory something else clears it, and an operator who points
//! `scratch_dir` at a volume of their own has taken that on already.

use std::path::{Path, PathBuf};

use tempfile::{NamedTempFile, TempDir};

use crate::error::ApiError;
use crate::tap::jobs::id::JobId;

/// One run's results directory.
#[derive(Debug)]
pub struct Results {
    directory: TempDir,
}

/// A written answer, as much of a [`super::Product`] as this module can say.
#[derive(Debug, Clone)]
pub struct Written {
    /// The file's name inside the directory.
    pub file: String,
    pub bytes: u64,
}

impl Results {
    /// Make this run's directory.
    ///
    /// **This is also the check that results can be written at all**, and it is why it
    /// happens at startup: a service that cannot write one cannot answer `/async`, which TAP
    /// §2.2 does not make optional, so it is an operator's mistake to hear before any caller
    /// meets it. Creating the directory asks the question that a probe file would ask.
    pub fn open(scratch_dir: Option<&Path>) -> Result<Self, ApiError> {
        let directory = match scratch_dir {
            Some(dir) => TempDir::new_in(dir),
            None => TempDir::new(),
        }
        .map_err(|error| {
            // The operator's path, in the operator's log. What a caller is told about this
            // is nothing, there being no caller yet.
            ApiError::internal(format!("cannot make a directory for job results: {error}"))
        })?;
        tracing::info!(directory = %directory.path().display(), "job results");
        Ok(Self { directory })
    }

    /// Where a written answer is.
    pub fn path(&self, file: &str) -> PathBuf {
        self.directory.path().join(file)
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
        let directory = self.directory.path().to_owned();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_answer_is_written_and_read_back() {
        let results = Results::open(None).unwrap();
        let id = JobId::new().unwrap();
        let written = results.write(&id, "<VOTABLE/>".to_owned()).await.unwrap();
        assert_eq!(written.bytes, 10);
        // Named after the job, which is what makes the file findable from the record alone.
        assert_eq!(written.file, id.to_string());
        let path = results.path(&written.file);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "<VOTABLE/>");
    }

    /// Nothing is left behind but the answer: the temporary the write went through is
    /// renamed rather than copied, so a directory holds one file per finished job.
    #[tokio::test]
    async fn a_write_leaves_one_file() {
        let results = Results::open(None).unwrap();
        for _ in 0..3 {
            let id = JobId::new().unwrap();
            results.write(&id, "rows".to_owned()).await.unwrap();
        }
        let left = std::fs::read_dir(results.directory.path()).unwrap().count();
        assert_eq!(left, 3);
    }

    #[tokio::test]
    async fn a_removed_answer_is_gone_and_removing_it_twice_is_quiet() {
        let results = Results::open(None).unwrap();
        let id = JobId::new().unwrap();
        let written = results.write(&id, "rows".to_owned()).await.unwrap();
        results.remove(&written.file).await;
        assert!(!results.path(&written.file).exists());
        // A destroyed job whose file has already gone is not a failure to report.
        results.remove(&written.file).await;
    }

    /// The directory and everything in it goes when the service does, which is what stops a
    /// restart inheriting the last run's answers.
    #[test]
    fn the_directory_goes_when_the_service_does() {
        let path = {
            let results = Results::open(None).unwrap();
            let path = results.directory.path().to_owned();
            assert!(path.is_dir());
            path
        };
        assert!(!path.exists());
    }

    /// A scratch directory that is not there is an operator's mistake, and one they hear
    /// about at startup rather than on the first job.
    #[test]
    fn a_directory_that_cannot_be_made_is_a_startup_failure() {
        let missing = Path::new("/nonexistent-hats-api-scratch/deeper");
        let refused = Results::open(Some(missing)).unwrap_err();
        assert_eq!(
            refused.status(),
            http::StatusCode::INTERNAL_SERVER_ERROR,
            "{refused}"
        );
    }
}
