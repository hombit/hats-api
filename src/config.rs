//! The TOML configuration file, and nothing else.
//!
//! Every field has a default, so the file is optional and every key in it is optional
//! too. Unknown keys are an error rather than a silent no-op: this file decides what
//! the service is allowed to read, and a typo in it must not quietly widen or narrow
//! that.

use std::fmt;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use bytesize::ByteSize;
use http::HeaderValue;
use serde::Deserialize;

use crate::storage::StorageOptions;

/// Where to look for the file when `--config` is not given.
pub const CONFIG_ENV_VAR: &str = "HATS_API_CONFIG";

/// What this service calls itself, in the form a `User-Agent` or a `Server` header takes.
///
/// One string for both directions and for the foot of a generated page, so that what an
/// operator reads on a listing is what an origin's log will hold.
pub const PRODUCT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub server: ServerConfig,
    pub api: ApiConfig,
    /// Zero or more `[[mount]]` tables. Each publishes a local directory under a url
    /// prefix; with none, the service is the API alone.
    #[serde(rename = "mount")]
    pub mounts: Vec<MountConfig>,
    pub data: DataConfig,
    pub tap: TapConfig,
    pub limits: LimitsConfig,
    pub log: LogConfig,
}

/// The tables this service publishes over IVOA's Table Access Protocol.
///
/// Empty is a service with no TAP surface at all, which is what a deployment serving only
/// the API and the mounts wants: a TAP resource with no table to name answers nothing.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TapConfig {
    /// Zero or more `[[tap.table]]` tables.
    #[serde(rename = "table")]
    pub tables: Vec<TapTableConfig>,
    /// What a job may spend, under `[tap.async]`.
    #[serde(rename = "async")]
    pub jobs: AsyncConfig,
}

/// What the `/async` job resource will spend.
///
/// **There is no switch turning it on.** TAP §2.2 makes `/async` a MUST alongside §2.1's
/// `/sync`, so a TAP surface has both or is not one — publishing a `[[tap.table]]` is
/// publishing a job resource, and what is left to say is only what it may cost. An operator
/// with little room sets these low; one who cannot host jobs at all cannot publish TAP, and
/// that is the standard's answer rather than this service's.
///
/// Where the results go is not here either: that is `[limits] scratch_dir`, which already
/// means "where this service puts bytes on local disk". A second path key would be a second
/// answer to one question, and an operator pointing one at a volume and forgetting the other
/// is the failure it would buy.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AsyncConfig {
    /// How many job records are kept, in any phase. Reaching it refuses a new job with a
    /// `503` and destroys nothing: a burst of submissions must not be able to take away a
    /// result somebody has already been promised.
    pub max_jobs: usize,
    /// How many jobs execute at once; the rest wait in `QUEUED`. Also the only thing
    /// bounding CPU, there being no per-task measure to offer — what a deployment really
    /// limits is this against DataFusion's `target_partitions`.
    pub max_running: usize,
    /// How much disk every held result may take together. Over it the oldest finished job
    /// is destroyed, that one having had its chance to be collected.
    pub max_result_bytes_total: ByteSize,
    /// The largest answer one job may leave on disk.
    pub max_result_bytes: ByteSize,
    /// How long a job may run when it says nothing, and the most it may ask for. UWS lets a
    /// client write to `executionduration`, and lets a service cap what it writes.
    pub default_execution_seconds: u64,
    pub max_execution_seconds: u64,
    /// How long a finished job and its result are kept, and the most a client may ask for.
    pub default_destruction_seconds: u64,
    pub max_destruction_seconds: u64,
    /// The longest a `WAIT` may block. UWS 1.1 lets a service "impose a maximum blocking
    /// time"; this one must stay under `max_request_seconds`, or the router's own clock cuts
    /// the poll before the bound the client asked for is reached.
    pub max_wait_seconds: u64,
    /// What a job's query may spend, where it differs from `[limits]`.
    pub limits: AsyncLimitsConfig,
}

impl Default for AsyncConfig {
    fn default() -> Self {
        Self {
            max_jobs: 256,
            // Two, because a job is a whole query engine: each one is already reading
            // partitions in parallel, so the concurrency that matters is inside one job.
            max_running: 2,
            max_result_bytes_total: ByteSize::gib(20),
            max_result_bytes: ByteSize::gib(1),
            default_execution_seconds: 600,
            max_execution_seconds: 3600,
            default_destruction_seconds: 86_400,
            max_destruction_seconds: 604_800,
            max_wait_seconds: 30,
            limits: AsyncLimitsConfig::default(),
        }
    }
}

/// The `[limits]` fields a job's query answers to differently.
///
/// **Every field is an override and absent means the sync value**, so there is one list of
/// bound names rather than two that drift, and an operator writes only the difference. What
/// this exists for is `max_partitions`: a job is what a request too wide for one response
/// future turns into, so the partition count is what async buys. The clock is not here —
/// a job's is `executionduration`, which the client can read and raise.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AsyncLimitsConfig {
    pub max_partitions: Option<usize>,
    pub max_rows: Option<usize>,
    pub max_query_memory_bytes: Option<ByteSize>,
}

/// One published table: the name a query writes, and the catalog it reads.
///
/// Both are required and there is nothing else. In particular there are no storage
/// options: a published table names a place in this service's own url space, and whatever
/// it takes to reach that place was written once, on the `[[mount]]` that publishes it.
/// So an operator's credential is in one place in the file rather than repeated beside
/// every surface that reads through it, and a reader of this section sees what is
/// published rather than what it is authenticated with.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TapTableConfig {
    /// The fully qualified name, `schema.table`, as a query writes it and as
    /// `TAP_SCHEMA` publishes it.
    pub name: String,
    /// Where the catalog is, as a path under a `[[mount]]` — the same address the file
    /// server publishes it at and an API request names it by, e.g. `/hats/gaia_dr3`.
    pub path: String,
    /// Queries to offer against this table, as `[[tap.table.example]]` sections. The
    /// default is one cone search per table, written from the catalog; any written here
    /// replace it.
    #[serde(rename = "example", default)]
    pub examples: Vec<TapExampleConfig>,
}

/// One query offered against a published table, for a client to put in front of a user.
///
/// What it buys over the generated one is that somebody looked at the data: a predicate
/// that matches something, and a position worth looking at, are things no amount of
/// metadata says.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TapExampleConfig {
    /// What a client shows in its menu, e.g. `"Bright stars near M13"`.
    pub name: String,
    /// The query, as a client would send it.
    pub query: String,
}

/// Which files this service will read as data, in both modes.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DataConfig {
    /// Glob patterns, matched against a file's own name and never against the path it
    /// sits in — a directory called `catalog.parquet` does not make the files under it
    /// data, and a name is the same name wherever it was found.
    ///
    /// The default is what a HATS catalog contains. `_metadata` and `_common_metadata`
    /// have no extension at all, which is why this is a list of names rather than a list
    /// of suffixes.
    pub filenames: Vec<String>,
}

impl Default for DataConfig {
    fn default() -> Self {
        Self {
            filenames: [
                "*.parq",
                "*.parquet",
                "*.pq",
                "_metadata",
                "_common_metadata",
            ]
            .map(str::to_owned)
            .to_vec(),
        }
    }
}

/// The mode where the caller names the location of the data.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ApiConfig {
    pub enabled: bool,
    /// The url subtree the API answers under.
    pub prefix: String,
    pub access: AccessConfig,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            prefix: "/api/v1".to_owned(),
            access: AccessConfig::default(),
        }
    }
}

/// One directory this service will read, and where it sits in the url space.
///
/// `path` and `source` are required: a mount with either missing is not a mount with a
/// sensible default, it is an unfinished sentence. `path` is the mount's address in both
/// modes — the API names a file by the url space rather than by where it really is — so a
/// mount that the file server does not publish still needs one.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MountConfig {
    /// The url prefix it answers under, e.g. `/` or `/hats`.
    pub path: String,
    /// The directory it publishes: an absolute path, or a url in any scheme this service
    /// reads — `file://`, `s3://`, `gs://`, `az://`, `https://`, `webdav://`, `hf://`.
    pub source: String,
    /// What it takes to reach a `source` that is a store: the same options a request
    /// carries beside its url, with the same names and the same rules. Empty for a local
    /// directory, and an error to write for one.
    ///
    /// This is the one credential in the file, and it is the operator's own: it reaches
    /// their store and is never echoed, logged, or offered to a caller. A caller reaches
    /// the mount by its `path` and supplies nothing.
    #[serde(default)]
    pub storage: StorageOptions,
    /// Whether the file server publishes it at `path`. Off is API-only: the directory is
    /// still readable, and only by a request that asks a question about a file in it.
    ///
    /// For a store-backed mount, on means this service stands in front of that store: a
    /// request for a name under `path` becomes a ranged read against the origin, and one
    /// for a directory becomes a listing.
    #[serde(default)]
    pub serve: bool,
    /// Whether a path under it may go through a symlink. Per mount rather than global,
    /// so publishing a directory of links does not decide the question for every other
    /// mount. A store has no symlinks, so it is an error to write on one.
    #[serde(default)]
    pub follow_symlinks: bool,
    /// Whether what is published never changes once published, which is what lets a
    /// cached copy be served without revalidating it.
    #[serde(default)]
    pub immutable: bool,
    /// Which files under it are read as data, in place of `[data] filenames`. Absent is
    /// that list; an empty list is a mount with no query surface at all.
    pub filenames: Option<Vec<String>>,
}

/// What one request, and the process as a whole, may spend.
///
/// What one request may cost before it is refused rather than served.
///
/// The materialization limits exist because a server that ignores `Range` forces the
/// whole object onto local disk before any of it can be read, which is the one read path
/// here whose cost is not bounded by what the caller asked for. The expression limits
/// bound the other input a caller writes freely.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LimitsConfig {
    /// The most this service will copy to disk for one object whose server will not
    /// serve byte ranges. `0` refuses to copy at all, which makes such a server
    /// unreadable rather than expensive.
    pub max_materialize_bytes: ByteSize,
    /// The same across every copy resident at once. One request cannot exceed the limit
    /// above; this is what stops many of them together from filling the disk.
    pub max_materialize_total_bytes: ByteSize,
    /// How many copies may be in flight at once. Beyond this a request waits, since the
    /// transfers are competing for the same disk and the same upstream bandwidth.
    pub max_concurrent_materializations: usize,
    /// Where the copies go. Absent is the system temporary directory.
    pub scratch_dir: Option<PathBuf>,
    /// The most of a catalog's `_metadata` this service will fetch to learn its partition
    /// list. Over this the partitions are listed instead, which costs the per-partition
    /// sizes and nothing else.
    pub max_catalog_metadata_bytes: ByteSize,
    /// How many partitions of a catalog one request may read.
    ///
    /// The only one of the three known before anything is read: the partition list comes
    /// from whichever discovery source answered, so this is checked and refused without a
    /// single byte of data fetched. The other two can only be watched as they accumulate.
    pub max_partitions: usize,
    /// How many bytes of data one request may fetch from the store, across every partition.
    ///
    /// What a request costs the origin, which the partition count does not bound: one dense
    /// partition can be larger than a thousand sparse ones.
    ///
    /// Checked between files, since the counter is only final once a scan has finished, so
    /// a request overshoots by at most the file that carried it past.
    pub max_bytes_fetched: ByteSize,
    /// How many rows one request may return.
    ///
    /// Not the caller's `limit`, which is them asking for fewer. This is the operator's
    /// ceiling on the answer, and it binds a request that set no limit at all.
    pub max_rows: usize,
    /// How much memory one ADQL statement's own working set may take.
    ///
    /// The ADQL route's alone. Every other route evaluates one expression per row and holds
    /// the answer; a statement builds a join's hash table, an aggregate's groups and a sort's
    /// heap, none of which `max_rows` bounds — a `GROUP BY` over a million distinct values
    /// holds them all while returning one row at a time.
    ///
    /// Reached, the query is refused. It does not spill: the disk manager is off on that
    /// route, so this bound cannot quietly become a bound on the operator's scratch space
    /// instead. It is per request and not across them, so a server answering several at once
    /// may hold several of these.
    pub max_query_memory_bytes: ByteSize,
    /// How many partitions of one request are read at once.
    ///
    /// A performance knob rather than a bound: the read is over a network whose latency is
    /// what a request spends most of its time on, so the partitions go out together. It is
    /// also what decides how far `max_bytes_fetched` can overshoot, since the reads already
    /// in flight when it trips are not stopped.
    pub max_concurrent_partitions: usize,
    /// How long one request may take to produce an answer, in seconds. `0` is no bound.
    ///
    /// The clock runs while the answer is being made and stops when it is ready to send,
    /// so a large file served off a mount is not bounded by it — the bytes go out after
    /// the response exists. What it bounds is work: a read that is waiting on a store
    /// that has stopped answering, or a query over more data than the other limits
    /// happen to catch.
    ///
    /// It is the bound that acts first for anything slow. `max_bytes_fetched` allows
    /// gigabytes, which over an unhurried link is longer than this, so a request that
    /// trips the clock never reaches the counters — and unlike them it has no plan to
    /// answer with, the work having already been done.
    pub max_request_seconds: u64,
    /// The widest circle a query string may ask for, in arcseconds.
    ///
    /// The file-server mode's bound and not the API's. A url is something a browser follows
    /// and a page offers, so what it asks for has to be answerable in one response — while a
    /// wide cone against a catalog is a fan-out, which is what the API's plan route hands
    /// back and a query string has no way to express.
    ///
    /// The other three bounds still apply behind it; this is the one that acts on the
    /// request's own numbers rather than on what reading it turns out to cost. Raising it is
    /// how an operator opens the file server up to wider searches, and `0` closes the
    /// circle surface entirely, no radius being smaller than none — which leaves the plain
    /// `limit` request, a circle being what a catalog's url narrows rather than what makes
    /// it answerable.
    pub max_query_radius_arcsec: f64,
    /// How long a generated answer is kept for the requests that read it. `0` keeps
    /// nothing, which turns the cache off.
    ///
    /// A parquet reader opens one url three times — a size probe, the footer, then the
    /// data — and each of those is a separate request that would otherwise re-run the
    /// query and re-read the same bytes from the store. This is how long the answer to a
    /// query waits for the rest of the reads that belong to it.
    ///
    /// **Deliberately short, and time is the only validator.** The key is the request's own
    /// path and query string; nothing asks the store whether the object has changed, since
    /// that would be the round trip this exists to avoid. So the window is how long a
    /// replaced file may still be answered with, and it wants to be the length of a read
    /// rather than of a session.
    pub query_cache_seconds: u64,
    /// The most this service will hold in answers waiting to be read again.
    ///
    /// An answer larger than this is never kept, and the oldest are let go of when a new
    /// one does not fit. Without it a service answering large queries would hold every one
    /// of them for the whole of `query_cache_seconds`.
    pub max_query_cache_bytes: ByteSize,
    /// How large a request body may be. `0` is no bound.
    ///
    /// It bounds the bytes a caller sends, which the expression limits cannot: those are
    /// counted after the body has been read and parsed, so something far too large to plan
    /// is refused here rather than after it has been held in memory.
    ///
    /// It is also the only bound on `region`, which `max_expression_nodes` never sees: that
    /// counts the caller's SQL, and a region is a structured field lowered straight to a
    /// predicate. Both of the shapes that get large live there — a serialized `moc`, which is
    /// a coverage map in one string, and a long list of circles, which is how a caller
    /// cross-matches a catalog of their own against one served here. Those two set this
    /// default rather than anything about SQL: a circle is about seventy bytes, so this holds
    /// a couple of hundred thousand of them.
    ///
    /// Nothing here streams a body, so this is also how much memory one request may occupy
    /// before any of its work begins — and, `region` having no count of its own, it is what
    /// decides how many shapes one request may carry into the covering and the predicate.
    pub max_request_body_bytes: ByteSize,
    /// How deeply a `columns`, `filters` or ADQL `query` may nest. The parser enforces it, so
    /// a pathological one is refused while it is still text rather than after it has
    /// grown a stack of planner frames.
    pub max_expression_depth: usize,
    /// How many terms a `columns`, `filters` or ADQL `query` may have, counted after planning.
    /// Depth does not bound this: an `IN` list is one node wide and arbitrarily long,
    /// and a chain of `OR`s is shallow. Nothing to do with how many rows come back.
    pub max_expression_nodes: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            // Multi-GiB HATS partitions are ordinary, so a smaller cap would refuse
            // ordinary data rather than protecting against anything.
            max_materialize_bytes: ByteSize::gib(2),
            max_materialize_total_bytes: ByteSize::gib(8),
            max_concurrent_materializations: 4,
            scratch_dir: None,
            // A wide schema over many partitions runs to hundreds of MB, and this is the
            // whole file rather than a range of it: `_metadata` holds no rows, so its
            // footer is the file. Generous enough for a real catalog and short of the
            // sizes that would be a download rather than a lookup.
            max_catalog_metadata_bytes: ByteSize::mib(256),
            // A cone of a few degrees over a deep catalog, or a crossmatch side of that size.
            // A partition runs to gigabytes, but a query projects a few columns of it and
            // prunes row groups by the covering, so this bounds the fan-out rather than the
            // bytes — `max_bytes_fetched` and the clock are what bound those. Wider than this
            // is the plan route's, fanning the partitions out as separate requests.
            max_partitions: 128,
            max_bytes_fetched: ByteSize::gib(10),
            max_rows: 1_000_000,
            // Room for a real aggregate — a `GROUP BY` over a few million distinct values,
            // or a join whose build side is a catalog's worth of positions — while leaving a
            // server that answers several requests at once well short of its memory. An
            // operator who has given the process more can raise it.
            max_query_memory_bytes: ByteSize::gib(1),
            max_concurrent_partitions: 4,
            // Long enough for a wide catalog query against a cold remote store, and short
            // enough that a caller waiting on one finds out rather than holding a
            // connection open until something else closes it.
            max_request_seconds: 90,
            // Ten minutes of arc: a field around a source rather than a single position,
            // which is the size of question a person browsing actually asks — and still
            // small enough against a partition that it lands in a couple of them.
            max_query_radius_arcsec: 600.0,
            // Four minutes: long enough to cover the three requests a parquet reader makes
            // of one url, including a slow first read of a large partition, and short
            // enough that a replaced file is answered from here for one read rather than
            // for a session. It is a window over one client's reads, not a cache anyone
            // else is expected to hit.
            query_cache_seconds: 240,
            // Sized against what an answer actually weighs rather than against a tidy
            // number. A re-encoded partition of a real catalog runs to hundreds of
            // megabytes, and several readers are reading at once, so a cap that holds one
            // of them evicts on every second request and buys nothing. A cap rather than
            // an allocation: what is held is what has been asked for in the last few
            // minutes, which for a service nobody is reading is nothing.
            max_query_cache_bytes: ByteSize::gib(2),
            // Axum's own default, which is what this replaces rather than widens. A body
            // here is a query and not an upload, so the figure is set by the largest thing a
            // query legitimately carries: a `region`, either as a serialized MOC or as one
            // circle per source of a catalog being cross-matched. At about seventy bytes a
            // circle this is some tens of thousands of them, which is already more than one
            // request can run: the cost of a `region` is linear in its shapes, so a few
            // thousand circles reach `max_request_seconds` before they reach this.
            max_request_body_bytes: ByteSize::mib(2),
            // DataFusion's own default for the same limit.
            max_expression_depth: 50,
            // Generous, because a list of ten thousand object ids is a request this
            // service exists to answer: it refuses the absurd rather than budgeting.
            max_expression_nodes: 50_000,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// Address to listen on. The default is every interface, which is what a container
    /// needs; `127.0.0.1` keeps it on the machine.
    pub address: IpAddr,
    pub port: u16,
    /// Whether this service signs what it answers — the `Server` header, and the foot of
    /// a generated directory page — the way `apache` and `nginx` do.
    ///
    /// On by default, because it is what someone reporting that an answer looks wrong
    /// needs to be able to say, and a caller of the API never sees the page. An operator
    /// who would rather not publish which version is running turns it off — the same call
    /// as nginx's `server_tokens`, and worth as much: it hides the number from a reader,
    /// not from anyone fingerprinting the service.
    pub show_version: bool,
    /// Whether a directory holding an `index.html` is served that file in place of a
    /// generated listing.
    ///
    /// On by default, which is what every other file server does. Off publishes the
    /// generated page for every directory, and the `index.html` stays an ordinary file
    /// that answers to its own name — so a tree carrying pages written for some other
    /// reader is browsable here as data, which is what a HATS catalog under one is.
    pub serve_mounted_index_html: bool,
    /// Whether a `robots.txt` at the root of whichever mount answers for it is served in
    /// place of the generated default.
    ///
    /// On by default, which is `serve_mounted_index_html`'s own rule applied to this file: a
    /// mount that carries one wrote it on purpose, and it wins. Off publishes the
    /// generated default everywhere instead — `Disallow: /` for every path, and, where
    /// the API is on, `Allow` for its own docs page, health check and `openapi.json`,
    /// since none of those is data and a crawler refused all three could not even
    /// describe what it was refused. Either way, a mount with no `robots.txt` of its own
    /// gets the generated default — this only ever chooses between the two, and never
    /// answers with nothing.
    pub serve_mounted_robots_txt: bool,
    /// Who runs this deployment, written after the name wherever the service identifies
    /// itself.
    ///
    /// No default. An origin's operator needs to reach whoever installed the service,
    /// and the project's own url would point them at whoever wrote it.
    pub contact: Option<String>,
    /// What this service calls itself to an origin it reads from.
    pub user_agent: UserAgent,
}

/// `[server] user_agent`, where each of the three values means something.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum UserAgent {
    /// `true` for `hats-api/x.y.z (contact)`, `false` for no header at all.
    Product(bool),
    /// Sent as written, with neither the name nor the contact added to it.
    Named(String),
}

impl Default for UserAgent {
    fn default() -> Self {
        Self::Product(true)
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: 80,
            show_version: true,
            serve_mounted_index_html: true,
            serve_mounted_robots_txt: true,
            contact: None,
            user_agent: UserAgent::default(),
        }
    }
}

impl ServerConfig {
    pub fn listen_addr(&self) -> SocketAddr {
        SocketAddr::new(self.address, self.port)
    }

    /// The `User-Agent` every request this service makes carries, or `None` where the
    /// operator asked for none.
    ///
    /// Read once, at startup, so a string a header cannot carry is a service that does
    /// not start rather than one that fails on its first read.
    pub fn user_agent(&self) -> Result<Option<HeaderValue>, ConfigError> {
        let text = match &self.user_agent {
            UserAgent::Product(false) => return Ok(None),
            UserAgent::Product(true) => self.signed()?,
            UserAgent::Named(text) => text.clone(),
        };
        header_value("user_agent", &text).map(Some)
    }

    /// How the service signs what it answers: the `Server` header and the foot of a
    /// generated page. `None` where `show_version` says not to sign at all.
    ///
    /// Separate from [`ServerConfig::user_agent`] because the two face different readers.
    /// An operator may want an archive to know exactly who is reading it while telling
    /// the public internet nothing about which version is running, or the reverse.
    pub fn signature(&self) -> Result<Option<HeaderValue>, ConfigError> {
        match self.show_version {
            false => Ok(None),
            true => header_value("contact", &self.signed()?).map(Some),
        }
    }

    /// Who runs this deployment, checked.
    ///
    /// Not gated on `show_version`: that key is about publishing which build is running,
    /// and a contact is about who to reach — an operator who hides the number still wants
    /// their address on `/docs`.
    pub fn contact(&self) -> Result<Option<&str>, ConfigError> {
        let Some(contact) = &self.contact else {
            return Ok(None);
        };
        let contact = contact.trim();
        // It goes inside a header comment, where a parenthesis of the operator's own
        // would close the comment early and leave the rest of what they wrote reading as
        // a second product token.
        let printable = |c: char| c.is_ascii_graphic() || c == ' ';
        if contact.is_empty() || contact.contains(['(', ')']) || !contact.chars().all(printable) {
            return Err(ConfigError::Rule(
                contact.to_owned(),
                "expected printable ASCII with no parentheses, such as \"ops@example.org\" \
                 or \"+https://data.example.org/\""
                    .to_owned(),
            ));
        }
        Ok(Some(contact))
    }

    /// The product token with the operator's contact after it, where there is one.
    fn signed(&self) -> Result<String, ConfigError> {
        Ok(match self.contact()? {
            Some(contact) => format!("{PRODUCT} ({contact})"),
            None => PRODUCT.to_owned(),
        })
    }
}

/// A configured string as something that can go on the wire, or the key to fix.
fn header_value(key: &str, text: &str) -> Result<HeaderValue, ConfigError> {
    HeaderValue::from_str(text).map_err(|_| {
        ConfigError::Rule(
            text.to_owned(),
            format!("is not a value a header can carry, so [server] {key} cannot be it"),
        )
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LogConfig {
    /// A tracing filter, in `RUST_LOG` syntax: `hats_api=debug`, `warn`, and so on.
    /// `RUST_LOG` itself overrides this when it is set.
    pub filter: String,
    pub format: LogFormat,
    /// Colour. Off when the output is not a terminal, whatever this says.
    pub ansi: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// One human-readable line per event.
    Text,
    /// One JSON object per event, for a log collector to parse.
    Json,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            filter: "hats_api=info,tower_http=info".to_owned(),
            format: LogFormat::Text,
            ansi: true,
        }
    }
}

/// What the service may read when the request says where to read from. See
/// [`crate::access`] for what the entries mean.
///
/// The default is every remote endpoint on the public internet. It says nothing about
/// local files: `[[mount]]` is the only thing that makes a directory readable, and a
/// caller reaches one by its `path` rather than by where it is on the disk.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AccessConfig {
    pub network: NetworkConfig,
    pub s3: EndpointConfig,
    pub gcs: EndpointConfig,
    pub azure: EndpointConfig,
    pub http: HttpConfig,
    pub webdav: EndpointConfig,
    pub hf: EndpointConfig,
}

/// Which addresses a request may reach, whatever backend it goes through: the
/// destination is a property of the address rather than of the protocol. See
/// [`crate::access::network`].
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct NetworkConfig {
    /// `127.0.0.0/8`, `::1`, and `localhost`.
    pub allow_loopback: bool,
    /// Everything else that is not the public internet: RFC1918, link-local — which is
    /// where the cloud metadata services are — unique-local, and the reserved ranges.
    pub allow_private: bool,
    /// Names that only resolve inside a network: single-label ones, and anything whose
    /// last label is not a top-level domain IANA has delegated.
    pub allow_local_names: bool,
    /// Networks to allow whatever the switches above say, e.g. `["10.1.2.0/24"]`.
    pub allow_cidrs: Vec<String>,
    /// Hosts to allow whatever the switches above say, and whatever they resolve to.
    pub allow_hosts: Vec<String>,
}

/// One remote backend's endpoint rules. Every backend has the same shape, under its own
/// section: `[api.access.s3]`, `[api.access.gcs]`, `[api.access.azure]`.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EndpointConfig {
    /// Endpoints a request may point the service at. The provider's own name —
    /// `"aws"`, `"gcp"`, `"azure"` — is that provider's own service, which is what a
    /// url with no `endpoint` option means; anything else is a URL, e.g.
    /// `"https://minio.example.com"`.
    ///
    /// Three states, and the difference between the first two matters: absent is any
    /// endpoint, an empty list turns the backend off, and a list is exactly those
    /// endpoints.
    pub endpoints: Option<Vec<String>>,
}

/// `http://` and `https://` urls, which are one backend reached two ways. The endpoint
/// list has the same three states as every other backend's; what is extra here is that
/// the url carries the scheme, so whether cleartext is acceptable is a question about
/// the request rather than about an option beside it.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct HttpConfig {
    /// Servers a request may read from, as urls: `["https://data.example.com"]`. There
    /// is no provider name here — an `http(s)://` url names its own server, so there is
    /// no "the provider's own service" for one to mean.
    ///
    /// Three states, as elsewhere: absent is any server, an empty list turns `http://`
    /// and `https://` off, and a list is exactly those.
    pub endpoints: Option<Vec<String>>,
    /// Whether an `http://` url may be read when no endpoint list names one. Off by
    /// default: over cleartext nothing says the bytes came from the host the url names,
    /// and a parquet file that something on the path rewrote is a wrong answer rather
    /// than a failed request. Distinct from `network.allow_loopback`, which is about
    /// which address may be reached rather than what may be spoken to it.
    pub allow_plain_http: bool,
}

#[derive(Debug)]
pub enum ConfigError {
    Read(PathBuf, io::Error),
    Parse(PathBuf, toml::de::Error),
    /// An `[api.access]` endpoint entry that means nothing.
    Rule(String, String),
    /// A `[[mount]]` that cannot be read: a source that is not a local directory, or a
    /// prefix that is not a prefix, or two mounts claiming the same subtree. Named by
    /// its `path`, which is what the operator wrote and what tells the two apart.
    Mount(String, String),
    /// The two modes do not divide the url space between them: an `api.prefix` that is
    /// not a prefix, a mount inside it where no request could reach it, or a
    /// configuration that would serve nothing at all.
    Route(String),
    /// A `filenames` pattern that is not a glob, named as the operator wrote it. Either
    /// list can hold one, and both spell it the same way.
    Data(String, String),
    /// A `[[tap.table]]` that cannot be published: a name no query could write, a name
    /// twice, or a url the access policy refuses. Named by the `name`, which is what the
    /// operator wrote and what a caller would have queried.
    Tap(String, String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(path, error) => write!(f, "cannot read {}: {error}", path.display()),
            Self::Parse(path, error) => write!(f, "invalid config {}: {error}", path.display()),
            Self::Rule(entry, reason) => {
                write!(f, "invalid [api.access] entry {entry:?}: {reason}")
            }
            Self::Mount(path, reason) => write!(f, "invalid [[mount]] {path:?}: {reason}"),
            Self::Route(reason) => write!(f, "invalid routing: {reason}"),
            Self::Data(pattern, reason) => {
                write!(f, "invalid filenames entry {pattern:?}: {reason}")
            }
            Self::Tap(name, reason) => write!(f, "invalid [[tap.table]] {name:?}: {reason}"),
        }
    }
}

impl std::error::Error for ConfigError {}

pub fn load(path: &Path) -> Result<Config, ConfigError> {
    let text =
        std::fs::read_to_string(path).map_err(|error| ConfigError::Read(path.to_owned(), error))?;
    toml::from_str(&text).map_err(|error| ConfigError::Parse(path.to_owned(), error))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(toml: &str) -> Result<Config, toml::de::Error> {
        toml::from_str(toml)
    }

    fn server(toml: &str) -> ServerConfig {
        parse(toml).unwrap().server
    }

    /// The three values `user_agent` takes, and the one composed form the contact makes.
    /// Each of the three has to mean something different, or a config that means "say
    /// nothing" and one that means "say the usual thing" are the same config.
    #[test]
    fn a_deployment_names_itself_three_ways() {
        // Nothing configured: the product token, and no contact to write after it.
        let bare = server("").user_agent().unwrap().unwrap();
        assert_eq!(bare, PRODUCT);

        let signed = server("[server]\ncontact = \"ops@example.org\"")
            .user_agent()
            .unwrap()
            .unwrap();
        assert_eq!(signed, format!("{PRODUCT} (ops@example.org)"));

        // A string is the whole header: neither the token nor the contact is added, so
        // an operator who wrote one knows what goes out.
        let named =
            server("[server]\ncontact = \"ops@example.org\"\nuser_agent = \"archive-mirror/2\"")
                .user_agent()
                .unwrap()
                .unwrap();
        assert_eq!(named, "archive-mirror/2");

        assert_eq!(
            server("[server]\nuser_agent = false").user_agent().unwrap(),
            None
        );
    }

    /// `user_agent = false` silences the outbound header and nothing else: the contact
    /// still signs what this service answers, so an operator who configured one has not
    /// had it quietly dropped.
    #[test]
    fn the_two_directions_are_configured_apart() {
        let config = server("[server]\ncontact = \"ops@example.org\"\nuser_agent = false");
        assert_eq!(config.user_agent().unwrap(), None);
        assert_eq!(
            config.signature().unwrap().unwrap(),
            format!("{PRODUCT} (ops@example.org)")
        );

        // And the other way: named to an origin, silent to a caller.
        let config = server("[server]\nshow_version = false");
        assert_eq!(config.user_agent().unwrap().unwrap(), PRODUCT);
        assert_eq!(config.signature().unwrap(), None);

        // `show_version` is about the build, so an operator who hides it is still
        // reachable: the contact stays, for `/docs` to publish.
        let config = server("[server]\nshow_version = false\ncontact = \"ops@example.org\"");
        assert_eq!(config.signature().unwrap(), None);
        assert_eq!(config.contact().unwrap(), Some("ops@example.org"));
    }

    /// A contact that would not survive the header it goes in is refused at startup
    /// rather than truncating the operator's own text on every request.
    #[test]
    fn a_contact_a_header_cannot_carry_is_refused() {
        for contact in [
            // Would close the comment early and leave the rest reading as a product token.
            "ops@example.org (daytime)",
            "ops@example.org\nX-Evil: 1",
            // Not ASCII, which a header value is not.
            "ops@examplé.org",
            "   ",
        ] {
            let config = ServerConfig {
                contact: Some(contact.to_owned()),
                ..Default::default()
            };
            assert!(config.user_agent().is_err(), "{contact:?} was accepted");
            assert!(config.signature().is_err(), "{contact:?} was accepted");
        }
    }

    #[test]
    fn an_empty_file_is_the_default_configuration() {
        let config = parse("").unwrap();
        assert_eq!(config.server.listen_addr().to_string(), "0.0.0.0:80");
        let access = &config.api.access;
        // Absent, not empty: any endpoint rather than none.
        assert_eq!(access.s3.endpoints, None);
        assert_eq!(access.gcs.endpoints, None);
        assert_eq!(access.azure.endpoints, None);
        assert!(!access.network.allow_loopback);
        assert!(!access.network.allow_private);
        assert!(!access.network.allow_local_names);
        assert!(access.network.allow_cidrs.is_empty());
        assert!(access.network.allow_hosts.is_empty());
        // The API on its own: no directory is readable until a mount says so.
        assert!(config.api.enabled);
        assert_eq!(config.api.prefix, "/api/v1");
        assert!(config.mounts.is_empty());
    }

    #[test]
    fn every_key_can_be_set_on_its_own() {
        let config = parse("[server]\naddress = \"127.0.0.1\"").unwrap();
        assert_eq!(config.server.listen_addr().to_string(), "127.0.0.1:80");
        let config = parse("[server]\nport = 8080").unwrap();
        assert_eq!(config.server.listen_addr().to_string(), "0.0.0.0:8080");
        let config = parse("[api.access.http]\nallow_plain_http = true").unwrap();
        assert!(config.api.access.http.allow_plain_http);
        // Setting one access key must not disturb the others.
        assert_eq!(config.api.access.s3.endpoints, None);
        // Nor the mode switches: naming one part of [api] leaves the rest default.
        assert!(config.api.enabled);
        assert_eq!(config.api.prefix, "/api/v1");
    }

    #[test]
    fn a_mount_needs_both_halves() {
        let config = parse("[[mount]]\npath = \"/hats\"\nsource = \"/data/hats\"").unwrap();
        let [mount] = config.mounts.as_slice() else {
            panic!("expected one mount, got {:?}", config.mounts)
        };
        assert_eq!(mount.path, "/hats");
        assert_eq!(mount.source, "/data/hats");
        assert!(!mount.serve);
        assert!(!mount.follow_symlinks);
        assert!(!mount.immutable);
        // Absent is `[data] filenames`, which an empty list is not.
        assert_eq!(mount.filenames, None);
        // Neither half has a default that could stand in for the other.
        assert!(parse("[[mount]]\npath = \"/hats\"").is_err());
        assert!(parse("[[mount]]\nsource = \"/data/hats\"").is_err());
        assert!(parse("[[mount]]\npath = \"/a\"\nsource = \"/b\"\nimmutabe = true").is_err());
    }

    /// Several mounts are the ordinary case, and each keeps its own rules.
    #[test]
    fn mounts_are_a_list_and_do_not_share_their_rules() {
        let config = parse(
            "[[mount]]\npath = \"/\"\nsource = \"/srv/data\"\nfollow_symlinks = true\n\
             filenames = [\"*.parquet\"]\n\
             [[mount]]\npath = \"/hats\"\nsource = \"/data/hats\"\nimmutable = true\n\
             serve = true",
        )
        .unwrap();
        let [first, second] = config.mounts.as_slice() else {
            panic!("expected two mounts, got {:?}", config.mounts)
        };
        assert!(first.follow_symlinks && !first.immutable && !first.serve);
        assert_eq!(
            first.filenames.as_deref(),
            Some(["*.parquet".to_owned()].as_slice())
        );
        assert!(!second.follow_symlinks && second.immutable && second.serve);
        assert_eq!(second.filenames, None);
    }

    /// A published table is a name and a path, and both halves are required. No `storage`
    /// and no `url`: what reaches the catalog was written on the mount the path lands in.
    #[test]
    fn a_published_table_is_a_name_and_a_path() {
        let config = parse(
            "[[tap.table]]\nname = \"gaia_dr3.gaia_source\"\npath = \"/hats/gaia_dr3\"\n\
             [[tap.table]]\nname = \"ztf.dr24_lc\"\npath = \"/hats/ztf\"",
        )
        .unwrap();
        let [first, second] = config.tap.tables.as_slice() else {
            panic!("expected two tables, got {:?}", config.tap.tables)
        };
        assert_eq!(first.name, "gaia_dr3.gaia_source");
        assert_eq!(first.path, "/hats/gaia_dr3");
        assert_eq!(second.name, "ztf.dr24_lc");
        // No examples is the ordinary case, and is what makes the generated one the default.
        assert!(first.examples.is_empty());

        assert!(parse("[[tap.table]]\nname = \"a.b\"").is_err());
        assert!(parse("[[tap.table]]\npath = \"/a\"").is_err());
        assert!(parse("[[tap.table]]\nname = \"a.b\"\npath = \"/a\"\nstorage = {}").is_err());
        // The key this replaced, so a config written against the old shape fails loudly
        // rather than publishing nothing.
        assert!(parse("[[tap.table]]\nname = \"a.b\"\nurl = \"file:///a\"").is_err());
        // No tables is the default, and is a service with no TAP surface.
        assert!(parse("").unwrap().tap.tables.is_empty());
    }

    /// An operator's own examples, which replace the generated one for that table. Both
    /// halves are required: a query with no name is one a client has nothing to label in
    /// its menu, and a name with no query is a menu entry that does nothing.
    #[test]
    fn a_table_may_carry_examples_of_its_own() {
        let config = parse(
            "[[tap.table]]\nname = \"gaia_dr3.gaia_source\"\npath = \"/hats/gaia_dr3\"\n\
             [[tap.table.example]]\nname = \"Bright stars\"\nquery = \"SELECT TOP 1 ra\"\n\
             [[tap.table.example]]\nname = \"Nearby\"\nquery = \"SELECT TOP 1 dec\"",
        )
        .unwrap();
        let [table] = config.tap.tables.as_slice() else {
            panic!("expected one table, got {:?}", config.tap.tables)
        };
        let names: Vec<&str> = table
            .examples
            .iter()
            .map(|example| example.name.as_str())
            .collect();
        assert_eq!(names, ["Bright stars", "Nearby"]);
        assert_eq!(table.examples[0].query, "SELECT TOP 1 ra");

        let table = "[[tap.table]]\nname = \"a.b\"\npath = \"/a\"\n";
        assert!(parse(&format!("{table}[[tap.table.example]]\nname = \"n\"")).is_err());
        assert!(parse(&format!("{table}[[tap.table.example]]\nquery = \"q\"")).is_err());
        assert!(
            parse(&format!(
                "{table}[[tap.table.example]]\nname = \"n\"\nquery = \"q\"\ntable = \"a.b\""
            ))
            .is_err()
        );
        // One section rather than a list of them, which would be a different shape with the
        // same spelling.
        assert!(parse(&format!("{table}[tap.table.example]\nname = \"n\"")).is_err());
    }

    #[test]
    fn the_api_can_be_turned_off() {
        let config = parse("[api]\nenabled = false\nprefix = \"/query\"").unwrap();
        assert!(!config.api.enabled);
        assert_eq!(config.api.prefix, "/query");
    }

    #[test]
    fn logging_has_defaults_and_can_be_set() {
        let config = parse("").unwrap();
        assert_eq!(config.log.filter, "hats_api=info,tower_http=info");
        assert_eq!(config.log.format, LogFormat::Text);
        assert!(config.log.ansi);

        let config = parse("[log]\nformat = \"json\"\nfilter = \"warn\"\nansi = false").unwrap();
        assert_eq!(config.log.format, LogFormat::Json);
        assert_eq!(config.log.filter, "warn");
        assert!(!config.log.ansi);

        assert!(parse("[log]\nformat = \"xml\"").is_err());
    }

    /// The one distinction in this file that a reader could get wrong, so it is
    /// pinned: no key at all is every endpoint, an empty list is none. Every backend's
    /// section, since they share the type and could stop sharing the behaviour.
    #[test]
    fn an_absent_endpoint_list_is_not_an_empty_one() {
        for section in ["s3", "gcs", "azure"] {
            let any = parse(&format!("[api.access.{section}]"))
                .unwrap()
                .api
                .access;
            let none = parse(&format!("[api.access.{section}]\nendpoints = []"))
                .unwrap()
                .api
                .access;
            let (present, absent) = match section {
                "s3" => (none.s3.endpoints, any.s3.endpoints),
                "gcs" => (none.gcs.endpoints, any.gcs.endpoints),
                _ => (none.azure.endpoints, any.azure.endpoints),
            };
            assert_eq!(absent, None, "{section}");
            assert_eq!(present, Some(Vec::new()), "{section}");
        }
    }

    /// A backend's section is set on its own, without disturbing the others.
    #[test]
    fn each_backend_has_its_own_section() {
        let access = parse("[api.access.gcs]\nendpoints = [\"gcp\"]")
            .unwrap()
            .api
            .access;
        assert_eq!(
            access.gcs.endpoints.as_deref(),
            Some(["gcp".to_owned()].as_slice())
        );
        assert_eq!(access.s3.endpoints, None);
        assert_eq!(access.azure.endpoints, None);
    }

    #[test]
    fn a_misspelled_key_is_an_error_rather_than_a_silent_default() {
        for toml in [
            "[server]\nadress = \"127.0.0.1\"",
            // Every remote backend has an `[api.access.<backend>]` section, so a local
            // one is a plausible thing to write. There is none: `[[mount]]` is where a
            // directory is named, and a config that says otherwise does not start.
            "[api.access.local]\npaths = [\"/srv/data\"]",
            "[api.access.local]\nfollow_symlinks = true",
            "[api.access.s3]\nendpoint = \"aws\"",
            "[api.access.gcs]\nendpoint = \"gcp\"",
            "[api.access.azure]\nendpoitns = []",
            "[api.access.network]\nallow_privte = true",
            "[api.access.network]\nallow_cidr = []",
            "[api.acess.network]\nallow_loopback = true",
            "[api]\nenable = true",
            "[api]\nrefix = \"/v2\"",
            // Not sections this file has: the schemes are `gs` and `az`, but the
            // sections are named for the services.
            "[api.access.gs]\nendpoints = []",
            "[api.access.az]\nendpoints = []",
            // The keys these replaced, so an old config fails loudly.
            "[api.access]\nallow = []",
            "[api.access]\nfollow_symlinks = true",
            // Loopback is one case of a destination rule, and lives with the rest.
            "[api.access]\nallow_loopback = true",
            // `[access]` is now `[api.access]`, and governs API mode alone; a config
            // written against the old shape must not be read as the new one.
            "[access.local]\npaths = [\"/srv/data\"]",
            "[access.s3]\nendpoints = [\"aws\"]",
            // A mount is `[[mount]]`, not `[mount]`, and not `[[mounts]]`.
            "[[mounts]]\npath = \"/\"\nsource = \"/srv/data\"",
            "[mount]\npath = \"/\"\nsource = \"/srv/data\"",
            // A published table is `[[tap.table]]`, and the list has no other spelling.
            "[tap]\ntables = []",
            "[[tap.tables]]\nname = \"a.b\"\nurl = \"file:///a\"",
        ] {
            assert!(parse(toml).is_err(), "{toml} was accepted");
        }
    }

    /// The example file says every value shown is the default, and that it is complete.
    /// Both halves are checked: it parses under `deny_unknown_fields`, so it cannot name
    /// a key that no longer exists, and it produces the default config, so no value in
    /// it has drifted from what it claims to show.
    #[test]
    fn the_example_config_is_the_default_configuration() {
        let example = parse(include_str!("../hats-api.example.toml")).unwrap();
        let default = Config::default();
        assert_eq!(
            format!("{example:?}"),
            format!("{default:?}"),
            "hats-api.example.toml no longer shows the defaults"
        );
    }

    #[test]
    fn rejects_an_address_that_is_not_one() {
        assert!(parse("[server]\naddress = \"example.com\"").is_err());
        assert!(parse("[server]\nport = 99999").is_err());
    }
}
