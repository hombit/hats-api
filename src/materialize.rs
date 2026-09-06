//! Reading parquet from a server that will not serve byte ranges.
//!
//! Reading one parquet file is a sequence of ranged reads: the footer length, the
//! footer, the page index, then a chunk per column per surviving row group — tens of
//! requests against small, scattered offsets. Every store here honours `Range`, with one
//! exception: an `http(s)://` url points at whatever the caller has, and a plain HTTP
//! server may answer `200` with the whole body to a request that asked for eight bytes.
//!
//! Passing that failure through is not an option, and neither is ignoring it. The reader
//! would take the first bytes of the object as though they were the last, and read a
//! footer out of the file's header; and each of those tens of requests would transfer
//! the whole file, so one query would cost *N* times the file size. So an object whose
//! server does not range is fetched once into a scratch file, and every read after that
//! is a `pread` from local disk.
//!
//! **The whole object goes to disk, never to memory.** Partitions run to gigabytes and
//! requests arrive together, so a buffer per object is how the process dies.
//!
//! # Deciding, in one request
//!
//! The verdict and the copy come from the same `GET`, asking for the last eight bytes:
//!
//! - **`206`** — the server honours ranges. The eight bytes are dropped and every later
//!   read goes straight to it. `Content-Range` also carries the object's total size.
//! - **`200`** — it does not, and the body arriving is already the whole object, so it
//!   is streamed to the scratch file then and there rather than fetched a second time.
//!
//! A server that honours bounded ranges but not suffix ranges is judged non-ranging and
//! merely reads slower, which is the direction to be wrong in.
//!
//! Asking is per object rather than per host. A host is not one answer: the same server
//! can hand back static files that range and generated responses that do not, and a
//! generated response is also one whose bytes may differ between requests — so nothing
//! here is remembered under a url, and two requests for one object each ask again.
//! Making that cheaper is a caching question, and belongs wherever the rest of the
//! caching does.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use url::Url;

use crate::config::LimitsConfig;

/// The suffix asked for by the probe. Small enough to be free when it is thrown away,
/// and it is the tail every parquet reader wants first in any case.
const PROBE_SUFFIX: u64 = 8;

/// What the process may spend on scratch copies, shared by every request.
///
/// One of these is built at startup and handed to every [`MaterializingStore`], so the
/// budget is the deployment's rather than each request's: the per-object cap alone would
/// let enough concurrent requests fill the disk between them.
#[derive(Debug)]
pub struct Transfers {
    max_bytes: u64,
    max_total_bytes: u64,
    /// How many copies may be running at once. A waiting request is better than several
    /// transfers sharing one disk and one upstream link badly.
    permits: tokio::sync::Semaphore,
    /// Bytes currently held by scratch files, released when each one is dropped — which
    /// is when the request that made it ends, cancelled or not.
    resident: AtomicU64,
    scratch_dir: Option<PathBuf>,
}

impl Transfers {
    pub fn new(config: &LimitsConfig) -> Self {
        Self {
            max_bytes: config.max_materialize_bytes.as_u64(),
            max_total_bytes: config.max_materialize_total_bytes.as_u64(),
            permits: tokio::sync::Semaphore::new(config.max_concurrent_materializations),
            resident: AtomicU64::new(0),
            scratch_dir: config.scratch_dir.clone(),
        }
    }

    /// Whether copying is switched on at all. A zero cap is an operator saying this
    /// service does not copy objects to disk, which makes a non-ranging server
    /// unreadable rather than expensive.
    fn enabled(&self) -> bool {
        self.max_bytes > 0
    }

    /// An empty claim on the process-wide budget, to be grown as bytes arrive.
    fn claim(self: &Arc<Self>) -> Claim {
        Claim {
            transfers: Arc::clone(self),
            held: 0,
        }
    }
}

/// Bytes of the process-wide budget held by one scratch file. A guard rather than a
/// pair of calls, so that giving the budget back is not something an error path can
/// forget to do.
#[derive(Debug)]
struct Claim {
    transfers: Arc<Transfers>,
    held: u64,
}

impl Claim {
    /// Take `extra` more bytes, or fail leaving the claim as it was.
    ///
    /// A compare-and-swap loop rather than a lock: this runs once per chunk of every
    /// transfer in flight, and the whole critical section is one addition.
    fn grow(&mut self, extra: u64) -> Result<(), TooLarge> {
        let max = self.transfers.max_total_bytes;
        let mut resident = self.transfers.resident.load(Ordering::Relaxed);
        loop {
            let wanted = resident.saturating_add(extra);
            if wanted > max {
                return Err(TooLarge::Total { wanted, max });
            }
            match self.transfers.resident.compare_exchange_weak(
                resident,
                wanted,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.held = self.held.saturating_add(extra);
                    return Ok(());
                }
                Err(actual) => resident = actual,
            }
        }
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        self.transfers
            .resident
            .fetch_sub(self.held, Ordering::Relaxed);
    }
}

/// Why a copy was refused. Separate from the transport failures so that the two get
/// different statuses: over a limit is the request asking for more than this service
/// gives, not the origin misbehaving.
#[derive(Debug, thiserror::Error)]
pub enum TooLarge {
    #[error(
        "the server will not serve byte ranges, so the whole object has to be copied, \
         and it declares {declared} bytes against a limit of {max}"
    )]
    Declared { declared: u64, max: u64 },
    #[error(
        "the server will not serve byte ranges and did not say how large the object is; \
         copying it passed the limit of {max} bytes"
    )]
    WhileStreaming { max: u64 },
    #[error(
        "the server will not serve byte ranges, and copying {wanted} bytes would pass \
         this server's total scratch limit of {max}; try again"
    )]
    Total { wanted: u64, max: u64 },
    #[error(
        "the server will not serve byte ranges, and this server is configured not to \
         copy objects to disk; set limits.max_materialize_bytes to change that"
    )]
    Disabled,
}

/// An [`ObjectStore`] that reads through to `inner`, unless the server behind it turns
/// out not to honour `Range` — in which case the object is copied to scratch and served
/// from there.
///
/// Only `get_opts` is intercepted, which is the whole of reading: `get`, `get_range`,
/// `get_ranges` and `head` are all defined in terms of it. Everything else is delegated,
/// and the write paths are the inner store's refusals rather than new ones, since the
/// service never writes.
#[derive(Debug)]
pub struct MaterializingStore {
    inner: Arc<dyn ObjectStore>,
    /// `scheme://authority` of the server, which is what a key is resolved against to
    /// get the url the probe asks for.
    origin: Url,
    client: reqwest::Client,
    transfers: Arc<Transfers>,
    /// One cell per key, so that the tens of concurrent ranged reads a parquet scan
    /// opens with collapse into a single probe rather than a race to copy the same
    /// object several times over.
    objects: Mutex<HashMap<Path, Arc<tokio::sync::OnceCell<Source>>>>,
}

/// Where the bytes for one key come from.
#[derive(Debug, Clone)]
enum Source {
    /// The server answered the probe with the range it was asked for.
    Ranged,
    /// It answered with the whole object, which is now here.
    Scratch(Arc<Scratch>),
}

/// The copy, and the budget it holds. Dropping it deletes the file and returns the
/// bytes, so a cancelled request cleans up on the way out with nothing to remember.
#[derive(Debug)]
struct Scratch {
    file: tempfile::NamedTempFile,
    size: u64,
    _claim: Claim,
}

impl MaterializingStore {
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        origin: Url,
        client: reqwest::Client,
        transfers: Arc<Transfers>,
    ) -> Self {
        Self {
            inner,
            origin,
            client,
            transfers,
            objects: Mutex::new(HashMap::new()),
        }
    }

    /// Where this key's bytes come from, deciding once per key.
    async fn source(&self, location: &Path) -> object_store::Result<Source> {
        let cell = {
            let mut objects = self
                .objects
                .lock()
                // Held only across the map operations below, none of which can panic,
                // so a poisoned lock would mean a panic somewhere that cannot reach it.
                .unwrap_or_else(PoisonError::into_inner);
            Arc::clone(objects.entry(location.clone()).or_default())
        };
        cell.get_or_try_init(|| self.probe(location)).await.cloned()
    }

    /// The one request that settles it. See the module docs for why it is a suffix range
    /// and why the answer is not remembered per host.
    async fn probe(&self, location: &Path) -> object_store::Result<Source> {
        let url = self.object_url(location)?;
        let response = self
            .client
            .get(url)
            .header(reqwest::header::RANGE, format!("bytes=-{PROBE_SUFFIX}"))
            .send()
            .await
            .map_err(|error| self.failed(location, error))?;

        let status = response.status();
        if status == reqwest::StatusCode::PARTIAL_CONTENT {
            return Ok(Source::Ranged);
        }
        if !status.is_success() {
            // Let the inner store make the request again and turn the status into the
            // error it already knows how to build: it distinguishes 404 from 403 from a
            // gateway failure, and this would only be a second opinion about the same
            // response. The extra request is the price of not having two mappings.
            return Ok(Source::Ranged);
        }
        // A success that is not a `206`: the range was ignored and this body is the whole
        // object, arriving now.
        let declared = response.content_length();
        self.absorb(location, response, declared)
            .await
            .map(|scratch| Source::Scratch(Arc::new(scratch)))
    }

    /// Stream a whole-object response to scratch, refusing to go past the limits.
    ///
    /// `declared` is `Content-Length` when the server sent one. It is a claim rather than
    /// a fact, so it is used to refuse early and never to decide when to stop: a service
    /// generating parquet on the fly answers chunked with no length at all, and one that
    /// understates its length would otherwise be a way past the cap. The running count is
    /// what enforces it.
    async fn absorb(
        &self,
        location: &Path,
        response: reqwest::Response,
        declared: Option<u64>,
    ) -> object_store::Result<Scratch> {
        let max = self.transfers.max_bytes;
        if !self.transfers.enabled() {
            return Err(too_large(location, TooLarge::Disabled));
        }
        if let Some(declared) = declared.filter(|declared| *declared > max) {
            return Err(too_large(location, TooLarge::Declared { declared, max }));
        }
        let _permit = self
            .transfers
            .permits
            .acquire()
            .await
            .map_err(|error| self.failed(location, error))?;

        let mut file = match &self.transfers.scratch_dir {
            Some(dir) => tempfile::NamedTempFile::new_in(dir),
            None => tempfile::NamedTempFile::new(),
        }
        .map_err(|error| self.failed(location, error))?;

        let mut size = 0u64;
        // Grown as the bytes arrive rather than reserved up front, since the length is
        // often not known until the body ends. Dropped with `file` on every exit path,
        // including the ones below and a cancelled request.
        let mut claim = self.transfers.claim();
        let mut body = response.bytes_stream();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|error| self.failed(location, error))?;
            let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
            size = size.saturating_add(len);
            if size > max {
                return Err(too_large(location, TooLarge::WhileStreaming { max }));
            }
            claim
                .grow(len)
                .map_err(|error| too_large(location, error))?;
            file.write_all(&chunk)
                .map_err(|error| self.failed(location, error))?;
        }
        file.flush().map_err(|error| self.failed(location, error))?;
        Ok(Scratch {
            file,
            size,
            _claim: claim,
        })
    }

    /// The absolute url of one key, which is the origin with the key hung off it.
    fn object_url(&self, location: &Path) -> object_store::Result<Url> {
        self.origin
            .join(location.as_ref())
            .map_err(|error| object_store::Error::Generic {
                store: STORE,
                source: Box::new(error),
            })
    }

    fn failed(
        &self,
        location: &Path,
        error: impl std::error::Error + Send + Sync + 'static,
    ) -> object_store::Error {
        object_store::Error::Generic {
            store: STORE,
            source: Box::new(Failed {
                path: location.to_string(),
                source: Box::new(error),
            }),
        }
    }
}

const STORE: &str = "http";

/// A transport failure while probing or copying, named so that a reader of the log can
/// tell it from a failure of the inner store's own requests.
#[derive(Debug, thiserror::Error)]
#[error("could not copy {path} from a server that will not serve byte ranges: {source}")]
struct Failed {
    path: String,
    source: Box<dyn std::error::Error + Send + Sync>,
}

fn too_large(location: &Path, error: TooLarge) -> object_store::Error {
    object_store::Error::Generic {
        store: STORE,
        source: Box::new(Refused {
            path: location.to_string(),
            source: error,
        }),
    }
}

/// A refusal on the service's own limits rather than anything the origin did. Carried as
/// its own type so that [`crate::error`] can find it by downcasting and answer 413,
/// rather than reporting the origin as a bad gateway for a rule of ours.
#[derive(Debug, thiserror::Error)]
#[error("cannot read {path}: {source}")]
pub struct Refused {
    pub path: String,
    #[source]
    pub source: TooLarge,
}

impl Scratch {
    /// Serve a read from the copy. `object_store` does the `pread` itself given a file
    /// and a range, and dispatches it to a blocking pool.
    fn get_opts(&self, location: &Path, options: &GetOptions) -> object_store::Result<GetResult> {
        let meta = ObjectMeta {
            location: location.clone(),
            // The copy is this request's own, so its age says nothing about the object.
            // Reporting the fetch time would look like a validator and is not one.
            last_modified: chrono::DateTime::UNIX_EPOCH,
            size: self.size,
            e_tag: None,
            version: None,
        };
        // A `head` wants the metadata and no bytes.
        let range = match (options.head, &options.range) {
            (true, _) => 0..0,
            (false, None) => 0..self.size,
            (false, Some(wanted)) => {
                wanted
                    .as_range(self.size)
                    .map_err(|error| object_store::Error::Generic {
                        store: STORE,
                        source: Box::new(error),
                    })?
            }
        };
        let path = self.file.path();
        let file = std::fs::File::open(path).map_err(|error| object_store::Error::Generic {
            store: STORE,
            source: Box::new(error),
        })?;
        Ok(GetResult {
            // A handle of its own per read: `object_store` seeks the one it is given, so
            // a shared handle would have concurrent reads moving each other's cursor.
            payload: GetResultPayload::File(file, path.to_owned()),
            meta,
            range,
            attributes: Attributes::default(),
        })
    }
}

impl std::fmt::Display for MaterializingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MaterializingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for MaterializingStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        match self.source(location).await? {
            Source::Ranged => self.inner.get_opts(location, options).await,
            Source::Scratch(scratch) => scratch.get_opts(location, &options),
        }
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
