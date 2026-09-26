//! The directories the service will read, and where in the url space each one sits.
//!
//! **A mount is the only way a directory becomes readable**, in either mode. The operator
//! names it once in the config, and nothing a request says can widen that: a path either
//! lands inside one of these directories or lands nowhere.
//!
//! ```toml
//! [[mount]]
//! path = "/"
//! source = "/srv/data"
//! serve = true
//!
//! [[mount]]
//! path = "/hats"
//! source = "s3://archive/hats"
//! serve = true
//! storage = {endpoint = "https://minio.example.org", region = "us-east-1"}
//! ```
//!
//! `path` is the mount's address, and both modes use it: the file server publishes the
//! directory there, and an API request naming a file writes that same path —
//! `file:///hats/dr1/x.parquet`, never the `source` it sits in. So where the data really
//! is stays the operator's alone, and moving it changes no url. That is what makes a
//! source in any scheme [`storage::open_dir`](crate::storage::open_dir()) understands work
//! here: what the mode above sees is a prefix and the names under it, and a store answers
//! that as a filesystem does.
//!
//! `serve` is what the file server needs and the API does not. Publishing a directory
//! whole and answering a question about one file in it are different things to be
//! willing to do, and a mount that says nothing is willing to do only the second. **A
//! served mount over a store is this service standing in front of that store** — a
//! request for a name under `path` becomes a ranged read against the origin, and a
//! request for a directory becomes a listing of it. That is the intended shape and not a
//! side effect: what it buys is a HATS catalog browsable and queryable at a url of the
//! operator's choosing, whichever store it is in, with no credential at the caller's end.
//!
//! **A mount's `source` is the operator's own url, and naming it is the permission.**
//! `[api.access]` decides where a *caller* may point this service; there has never been a
//! section of it for a local directory, and a remote source is the same thing in another
//! scheme. See [`NamedBy`](crate::storage::NamedBy) for what that does and does not skip.
//!
//! Two mounts may not claim the same urls, whether or not either is served. First-match
//! wins would make the order of the tables load-bearing, and a file's identity would then
//! depend on where its mount was written rather than on where it is.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use percent_encoding::percent_decode_str;
use url::Url;

use crate::access::data::DataFiles;
use crate::access::local::canonical_root;
use crate::access::{AccessPolicy, LOCAL_SCHEME};
use crate::config::{ConfigError, DataConfig, MountConfig};
use crate::error::ApiError;
use crate::hats::Lifetime;
use crate::storage::materialize::Transfers;
use crate::storage::{self, RemoteDir, StorageOptions};

/// Where a mount's files actually are.
#[derive(Debug)]
pub enum MountSource {
    /// A directory on this machine, canonical, so a path resolved out of a request can
    /// simply be tested for being under it.
    Local(PathBuf),
    /// A prefix in a store.
    ///
    /// Boxed because it is the far larger of the two — [`StorageOptions`] is every
    /// backend's options at once — and a `Mount` is held for the life of the process
    /// whichever kind it is.
    Remote(Box<RemoteSource>),
}

/// A mount's store, as far as the config settles it: where it is, and what reaches it.
///
/// The url and the options are held rather than an opened store, for the reason
/// `[[tap.table]]` holds a url: a store is built per request and costs no connection,
/// while holding one would make this a registry of what the origin turned out to contain.
/// It is opened once at startup all the same — see [`Mounts::check_sources`] — so that an
/// operator hears about a source this service cannot reach before a caller does.
pub struct RemoteSource {
    /// The prefix, with a trailing `/` so that a name under it joins on rather than
    /// replacing its last segment.
    url: Url,
    /// What it takes to reach it: an endpoint, a region, a credential. The same options a
    /// request carries, read the same way, so there is one spelling of them in the
    /// service.
    options: Arc<StorageOptions>,
}

/// `Url`'s own `Debug` prints its parsed fields, `password` among them.
impl std::fmt::Debug for RemoteSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteSource")
            .field("url", &self.url.as_str())
            .field("options", &self.options)
            .finish()
    }
}

impl RemoteSource {
    /// The prefix this mount publishes. The operator's url, so it goes in a log and never
    /// in a response.
    pub fn url(&self) -> &Url {
        &self.url
    }
}

/// Where a mount reads from, for the one line of the startup log that says what this
/// deployment publishes.
///
/// A url is printed with its authority emptied of any userinfo. Nothing accepted here
/// carries one — `storage` refuses a url with credentials in its authority, and every
/// source goes through that at startup — so this is the backstop rather than the
/// guarantee, and it is the reason `Url`'s own `Display` is not what gets called.
impl std::fmt::Display for MountSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local(path) => write!(f, "{}", path.display()),
            Self::Remote(source) => {
                let mut url = source.url.clone();
                let _ = url.set_username("");
                let _ = url.set_password(None);
                f.write_str(url.as_str())
            }
        }
    }
}

/// One readable directory.
#[derive(Debug)]
pub struct Mount {
    /// The url prefix, normalized: `/`, or `/hats` with no trailing slash. Written this
    /// way once so that matching a request against it is a comparison rather than a
    /// second round of parsing.
    prefix: String,
    source: MountSource,
    serve: bool,
    follow_symlinks: bool,
    /// How long a catalog under it is remembered, where the mount says; `None` is
    /// `[limits] catalog_cache_seconds`.
    catalog_cache: Option<Lifetime>,
    /// Which files under it are data, which is the mount's own list where it wrote one
    /// and `[data] filenames` where it did not. Compiled per mount rather than looked up
    /// per request, so both modes ask one object the same question.
    data_files: DataFiles,
}

impl Mount {
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn source(&self) -> &MountSource {
        &self.source
    }

    /// The directory on this machine, or `None` for a mount backed by a store.
    ///
    /// The two are not interchangeable and the difference is not only where the bytes
    /// are: a filesystem has symlinks, a `stat` per name and a server underneath that
    /// serves ranges for free, and a store has none of those. So the paths that need one
    /// ask for it rather than being handed something that stands in for it.
    pub fn local_source(&self) -> Option<&Path> {
        match &self.source {
            MountSource::Local(path) => Some(path),
            MountSource::Remote(_) => None,
        }
    }

    /// Whether the file server publishes it. The API reads it either way.
    pub fn serve(&self) -> bool {
        self.serve
    }

    pub fn follow_symlinks(&self) -> bool {
        self.follow_symlinks
    }

    pub fn data_files(&self) -> &DataFiles {
        &self.data_files
    }

    /// How long a catalog under this mount is remembered, where the mount says rather than
    /// leaving it to `[limits]`. Freshness is a property of the data, and the operator who
    /// wrote the mount is the one who knows how often it changes.
    pub fn catalog_cache(&self) -> Option<Lifetime> {
        self.catalog_cache
    }

    /// The directory this mount publishes, opened.
    ///
    /// One call for both kinds, because everything above this reads a directory the same
    /// way. A local mount opens the path the config resolved at startup and asks the
    /// policy nothing — the mount is the policy — and a store-backed one is built from
    /// the operator's own url and options.
    pub fn open(
        &self,
        policy: &AccessPolicy,
        transfers: &Arc<Transfers>,
    ) -> Result<RemoteDir, ApiError> {
        match &self.source {
            // Not stamped: a `LocalFileSystem` is built with no credentials at all, so it
            // is the same store whichever mount asked for it, and two tables under two
            // local mounts share one correctly.
            MountSource::Local(path) => storage::open_mounted_dir(path),
            // Stamped, because this store carries the operator's credentials and is filed
            // by DataFusion under the origin's authority — where a url the caller wrote
            // could land too. `storage::open_configured_dir` is handed a url and options
            // and has no mount in front of it, so this is the one place that can say.
            MountSource::Remote(source) => {
                storage::open_configured_dir(&source.url, &source.options, policy, transfers)
                    .map(|dir| dir.mounted_by(Arc::clone(&source.options)))
            }
        }
    }
}

/// Every mount, checked against each other. Empty is the ordinary case: the API over
/// remote stores alone.
#[derive(Debug, Default)]
pub struct Mounts(Vec<Mount>);

impl Mounts {
    /// The mounts as configured, with `data` as the list any of them that named none
    /// reads its files by.
    pub fn new(configs: &[MountConfig], data: &DataConfig) -> Result<Self, ConfigError> {
        let default_files = DataFiles::new(&data.filenames)?;
        let mut mounts: Vec<Mount> = Vec::new();
        for config in configs {
            let invalid = |reason: String| ConfigError::Mount(config.path.clone(), reason);
            let prefix = normalize_prefix(&config.path).map_err(&invalid)?;
            let source = source_of(config).map_err(&invalid)?;
            if let Some(other) = mounts.iter().find(|other| overlaps(&other.prefix, &prefix)) {
                return Err(invalid(format!(
                    "claims urls the mount at {:?} already claims; mount prefixes must \
                     not overlap",
                    other.prefix
                )));
            }
            let data_files = match &config.filenames {
                Some(filenames) => DataFiles::new(filenames)?,
                None => default_files.clone(),
            };
            mounts.push(Mount {
                prefix,
                source,
                serve: config.serve,
                follow_symlinks: config.follow_symlinks,
                catalog_cache: config.catalog_cache_seconds,
                data_files,
            });
        }
        Ok(Self(mounts))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Open every store-backed source once, and throw the handle away.
    ///
    /// What that buys is the refusal — an option this backend has no use for, an endpoint
    /// in a scheme nothing speaks, a url with a credential in its authority — reaching the
    /// operator at startup rather than a caller on the first request to a mount that
    /// looked configured. It is the same check a local source gets by being resolved, and
    /// the same one `[[tap.table]]` makes. Nothing is read: a source that is there and
    /// empty is a mount with nothing in it, which is not this file's business.
    ///
    /// Separate from [`Self::new`] because opening a store needs the access policy, and
    /// the policy is built around the mounts.
    pub fn check_sources(
        &self,
        policy: &AccessPolicy,
        transfers: &Arc<Transfers>,
    ) -> Result<(), ConfigError> {
        for mount in &self.0 {
            if matches!(mount.source, MountSource::Remote(_)) {
                mount
                    .open(policy, transfers)
                    .map_err(|error| ConfigError::Mount(mount.prefix.clone(), error.to_string()))?;
            }
        }
        Ok(())
    }

    /// Whether the file server has anything to publish. Not the same question as
    /// [`Self::is_empty`]: a service of API-only mounts has directories to read and no
    /// url space to claim.
    pub fn serves_nothing(&self) -> bool {
        !self.0.iter().any(Mount::serve)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Mount> {
        self.0.iter()
    }

    /// The mount a path belongs to, and the part of the path inside it — with no leading
    /// slash, so it can be joined onto the source directly.
    ///
    /// At most one mount can match, since overlapping prefixes were refused at startup,
    /// so the order of the list decides nothing here.
    ///
    /// Every mount, served or not: this is the address the API names a local file by,
    /// and the file server asks [`Self::published`] instead.
    pub fn resolve<'a>(&self, path: &'a str) -> Option<(&Mount, &'a str)> {
        self.0
            .iter()
            .find_map(|mount| within(&mount.prefix, path).map(|rest| (mount, rest)))
    }

    /// The same, for the file server, which sees only what `serve` published.
    ///
    /// A path inside an unserved mount comes back `None` rather than falling through to
    /// a mount further out — there is no mount further out, since prefixes may not
    /// overlap — so an unserved mount is a hole in the url space and not a redirection
    /// of it.
    pub fn published<'a>(&self, path: &'a str) -> Option<(&Mount, &'a str)> {
        self.resolve(path).filter(|(mount, _)| mount.serve())
    }
}

/// What a `[[mount]]`'s `source` and `storage` spell, checked as far as the config alone
/// can check them.
///
/// The fork is the scheme and nothing else: an absolute path and a `file://` url are the
/// two ways to write a directory on this machine, and everything else is a store. Each
/// side then refuses the keys the other side's shape is the only use for, rather than
/// ignoring them — a `follow_symlinks` on a store-backed mount is an operator who thinks
/// this service is resolving links out there.
fn source_of(config: &MountConfig) -> Result<MountSource, String> {
    // An absolute path is not a url, and it is what someone with a directory in front of
    // them will type.
    let local = match config.source.starts_with('/') {
        true => Some(PathBuf::from(&config.source)),
        false => {
            let url = Url::parse(&config.source).map_err(|error| {
                format!("{error}; expected an absolute path or a url, such as \"/srv/data\" or \"s3://archive/hats\"")
            })?;
            match url.scheme() == LOCAL_SCHEME {
                true => Some(
                    url.to_file_path()
                        .map_err(|()| "not an absolute local path".to_owned())?,
                ),
                false => None,
            }
        }
    };
    if let Some(path) = local {
        // Checked rather than dropped: an operator who wrote a credential for a local
        // directory has misunderstood which half of the config they are in, and reading a
        // secret they meant to be used is worse than refusing it. The check is the one a
        // request gets, so the two say the same thing.
        config
            .storage
            .refuse_for_a_local_source()
            .map_err(|error| error.to_string())?;
        // Resolved now, so that a source that is not there fails at startup rather than
        // on every request to a route that looked configured.
        return canonical_root(&path).map(MountSource::Local);
    }

    let url = Url::parse(&config.source).map_err(|error| error.to_string())?;
    if config.follow_symlinks {
        return Err("follow_symlinks is about a filesystem, and this source is a store".to_owned());
    }
    let mut url = url;
    // The store is addressed relative to this, so it is a prefix rather than a name: a
    // source written without the separator would otherwise have its last segment replaced
    // by the first name joined onto it.
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(MountSource::Remote(Box::new(RemoteSource {
        url,
        options: Arc::new(config.storage.configured()),
    })))
}

/// The url prefix as it will be compared: absolute, with no trailing slash unless it is
/// the root, and with nothing in it that a path resolver would have to interpret.
///
/// The API's own prefix goes through this too. The two claim url space the same way, and
/// deciding whether they collide means comparing them in one spelling.
pub(crate) fn normalize_prefix(raw: &str) -> Result<String, String> {
    if !raw.starts_with('/') {
        return Err(format!(
            "a mount path is an absolute url prefix, so it starts with /, not {raw:?}"
        ));
    }
    let mut segments = Vec::new();
    for segment in raw.split('/').filter(|segment| !segment.is_empty()) {
        // `.` and `..` in a prefix would mean the mount answers under one url and
        // matches another. A request path carrying them is a different question, and one
        // the path resolver already answers.
        if segment == "." || segment == ".." {
            return Err(format!(
                "{segment:?} is not something a url prefix can contain"
            ));
        }
        segments.push(segment);
    }
    Ok(match segments.is_empty() {
        true => "/".to_owned(),
        false => format!("/{}", segments.join("/")),
    })
}

/// Whether two prefixes claim any url in common, which for path prefixes means one
/// contains the other.
fn overlaps(one: &str, other: &str) -> bool {
    within(one, other).is_some() || within(other, one).is_some()
}

/// The part of `path` that lies under `prefix`, or `None` when it does not. `/hats` does
/// not contain `/hatsx`: a prefix matches whole segments or nothing.
pub(crate) fn within<'a>(prefix: &str, path: &'a str) -> Option<&'a str> {
    let rest = path.strip_prefix(prefix)?;
    match prefix {
        // The root already ends in the separator, so what is left is the whole path.
        "/" => Some(rest),
        _ => match rest.is_empty() {
            true => Some(rest),
            false => rest.strip_prefix('/'),
        },
    }
}

/// The path a request names inside a mount, one component per url segment.
///
/// Percent-decoded one segment at a time, so that an encoded separator arrives as part
/// of a name rather than as a separator: `a%2Fb` is one component called `a/b`, which no
/// filesystem has, and not a path into `a`. `.` and `..` are refused outright rather than
/// resolved, since a mount's url space is not a filesystem and has nothing above it.
///
/// Both modes come through here. The API's local urls are addressed in this same url
/// space, so a name that means one file to the file server must not mean another to a
/// query about it.
pub fn path_segments(relative: &str) -> Result<Vec<String>, ApiError> {
    let mut segments = Vec::new();
    for segment in relative.split('/').filter(|segment| !segment.is_empty()) {
        let decoded = percent_decode_str(segment)
            .decode_utf8()
            .map_err(|_| ApiError::bad_request("this path is not valid UTF-8"))?;
        if matches!(decoded.as_ref(), "." | "..") || decoded.contains(['/', '\0']) {
            return Err(ApiError::bad_request(format!(
                "{segment:?} is not something a path here can contain"
            )));
        }
        segments.push(decoded.into_owned());
    }
    Ok(segments)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::config::{AccessConfig, EndpointConfig, LimitsConfig};
    use crate::storage::S3Options;

    use super::*;

    fn config(path: &str, source: &Path) -> MountConfig {
        MountConfig {
            path: path.to_owned(),
            source: source.display().to_string(),
            serve: true,
            follow_symlinks: false,
            catalog_cache_seconds: None,
            storage: StorageOptions::default(),
            filenames: None,
        }
    }

    fn mounts(configs: &[MountConfig]) -> Result<Mounts, ConfigError> {
        Mounts::new(configs, &DataConfig::default())
    }

    #[test]
    fn a_prefix_is_normalized_before_it_is_compared() {
        for (raw, normalized) in [
            ("/", "/"),
            ("//", "/"),
            ("/hats", "/hats"),
            ("/hats/", "/hats"),
            ("/hats//dr1/", "/hats/dr1"),
        ] {
            assert_eq!(normalize_prefix(raw).unwrap(), normalized, "{raw}");
        }
        for raw in ["hats", "", "/hats/../etc", "/./hats"] {
            assert!(normalize_prefix(raw).is_err(), "{raw} was accepted");
        }
    }

    /// A prefix matches whole segments: the mount at `/hats` is not the owner of
    /// `/hatsx`, which would otherwise be served out of the wrong directory.
    #[test]
    fn a_prefix_matches_whole_segments() {
        assert_eq!(
            within("/hats", "/hats/dr1/x.parquet"),
            Some("dr1/x.parquet")
        );
        assert_eq!(within("/hats", "/hats"), Some(""));
        assert_eq!(within("/hats", "/hatsx"), None);
        assert_eq!(within("/hats", "/other"), None);
        assert_eq!(within("/", "/hats/x"), Some("hats/x"));
        assert_eq!(within("/", "/"), Some(""));
    }

    /// An address is an address whether or not the file server publishes it, so an
    /// unserved mount takes part in this like any other.
    #[test]
    fn overlapping_mounts_are_refused_rather_than_ordered() {
        let dir = TempDir::new().unwrap();
        let at = |paths: &[&str]| {
            let configs: Vec<_> = paths.iter().map(|path| config(path, dir.path())).collect();
            mounts(&configs)
        };
        assert!(at(&["/hats", "/data"]).is_ok());
        // The same directory under two prefixes is not an overlap: the urls differ.
        assert!(at(&["/hats", "/hats-dr1"]).is_ok());
        for overlapping in [
            ["/", "/hats"].as_slice(),
            ["/hats", "/"].as_slice(),
            ["/hats", "/hats"].as_slice(),
            ["/hats", "/hats/"].as_slice(),
            ["/hats", "/hats/dr1"].as_slice(),
        ] {
            let error = at(overlapping).unwrap_err().to_string();
            assert!(error.contains("overlap"), "{overlapping:?}: {error}");
            let unserved: Vec<_> = overlapping
                .iter()
                .map(|path| MountConfig {
                    serve: false,
                    ..config(path, dir.path())
                })
                .collect();
            assert!(mounts(&unserved).is_err(), "{overlapping:?} unserved");
        }
    }

    #[test]
    fn a_local_source_must_be_a_directory_that_exists() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("x.parquet");
        std::fs::write(&file, b"").unwrap();
        assert!(mounts(&[config("/", dir.path())]).is_ok());
        // A file:// url says the same thing as the path, and is accepted the same way.
        assert!(
            mounts(&[MountConfig {
                source: format!("file://{}", dir.path().display()),
                ..config("/", dir.path())
            }])
            .is_ok()
        );
        let missing = dir.path().join("nothing");
        for source in [file.as_path(), missing.as_path()] {
            let error = mounts(&[config("/", source)]).unwrap_err().to_string();
            assert!(error.contains("[[mount]]"), "{error}");
        }
    }

    /// A mount over a store is settled from the config alone: the url is a prefix, and
    /// whether anything is at the end of it is a question for the first request, the way
    /// it is for every other url this service reads.
    #[test]
    fn a_source_may_be_a_prefix_in_a_store() {
        let dir = TempDir::new().unwrap();
        for source in [
            "s3://archive/hats",
            "gs://archive/hats",
            "https://data.example.org/hats",
            "hf://datasets/lincc-frameworks/hats",
        ] {
            let found = mounts(&[MountConfig {
                source: source.to_owned(),
                ..config("/hats", dir.path())
            }])
            .unwrap_or_else(|error| panic!("{source}: {error}"));
            let [mount] = found.0.as_slice() else {
                panic!("expected one mount")
            };
            // Nothing on this machine, so the paths that need a filesystem get no answer
            // rather than one about the process's own directory.
            assert_eq!(mount.local_source(), None, "{source}");
            let MountSource::Remote(remote) = mount.source() else {
                panic!("{source} was not read as a store")
            };
            // A prefix, so a name inside it joins on rather than replacing `hats`.
            assert!(remote.url().path().ends_with('/'), "{source}");
        }

        // Neither a path nor a url is not a source.
        for source in ["data/hats", "", "bucket/hats"] {
            assert!(
                mounts(&[MountConfig {
                    source: source.to_owned(),
                    ..config("/hats", dir.path())
                }])
                .is_err(),
                "{source} was accepted"
            );
        }
    }

    /// Each side refuses the key the other side's shape is the only use for, rather than
    /// ignoring it. A credential written for a local directory is an operator who has
    /// misunderstood which half of the file they are in, and one nobody uses is worse than
    /// one nobody wrote.
    #[test]
    fn a_mount_refuses_the_keys_its_own_kind_has_no_use_for() {
        let dir = TempDir::new().unwrap();
        let with_credential = || StorageOptions {
            s3: S3Options {
                access_key_id: Some("AKIA123".to_owned().into()),
                ..Default::default()
            },
            ..StorageOptions::default()
        };
        let error = mounts(&[MountConfig {
            storage: with_credential(),
            ..config("/", dir.path())
        }])
        .unwrap_err()
        .to_string();
        assert!(error.contains("storage options"), "{error}");
        assert!(!error.contains("AKIA123"), "leaked: {error}");

        // And the same options on the mount they are for are accepted.
        assert!(
            mounts(&[MountConfig {
                source: "s3://archive/hats".to_owned(),
                storage: with_credential(),
                ..config("/hats", dir.path())
            }])
            .is_ok()
        );

        // A store has no symlinks, so there is nothing for the switch to decide.
        let error = mounts(&[MountConfig {
            source: "s3://archive/hats".to_owned(),
            follow_symlinks: true,
            ..config("/hats", dir.path())
        }])
        .unwrap_err()
        .to_string();
        assert!(error.contains("symlink"), "{error}");
    }

    /// **A source is reachable without becoming a server a caller may name.**
    ///
    /// The mount is read through the client with no address rules, so a store on an
    /// internal network needs no second entry anywhere. What must not follow is the
    /// widening: the rules a *caller's* url is judged by never hear about the source, so
    /// naming that same server in a request is refused exactly as it was before the mount
    /// existed. The grant stays as wide as the mount and no wider.
    #[test]
    fn a_source_is_reachable_without_becoming_one_a_caller_may_name() {
        let dir = TempDir::new().unwrap();
        let mounts = Arc::new(
            mounts(&[MountConfig {
                source: "s3://archive/hats".to_owned(),
                storage: StorageOptions {
                    endpoint: Some("http://minio.internal:9000".to_owned()),
                    ..StorageOptions::default()
                },
                ..config("/hats", dir.path())
            }])
            .unwrap(),
        );
        let policy =
            AccessPolicy::new(&AccessConfig::default(), Arc::clone(&mounts), None).unwrap();
        let transfers = Arc::new(Transfers::new(&LimitsConfig::default()));
        // The mount opens, which is the half that has to keep working.
        mounts.check_sources(&policy, &transfers).unwrap();

        // And the same server, named by a caller, is refused: `minio.internal` is a name
        // that means something only inside a network, and nothing about the mount changed
        // what the network rules say about it.
        let error = policy
            .authorize_endpoint(
                crate::access::Backend::S3,
                Some(&Url::parse("http://minio.internal:9000").unwrap()),
            )
            .unwrap_err();
        assert!(matches!(error, ApiError::Forbidden(_)), "{error}");
    }

    /// The store is the mount's, and so are the credentials: the caller writes a path
    /// under `path` and names neither.
    #[test]
    fn a_store_backed_mount_opens_its_own_source() {
        let dir = TempDir::new().unwrap();
        let mounts = Arc::new(
            mounts(&[MountConfig {
                source: "s3://archive/hats".to_owned(),
                ..config("/hats", dir.path())
            }])
            .unwrap(),
        );
        let policy =
            AccessPolicy::new(&AccessConfig::default(), Arc::clone(&mounts), None).unwrap();
        let transfers = Arc::new(Transfers::new(&LimitsConfig::default()));
        // Nothing is read, so this needs no server: what it proves is that the source is
        // opened at all, and that the store is keyed by the bucket rather than the prefix.
        let opened = mounts
            .iter()
            .next()
            .unwrap()
            .open(&policy, &transfers)
            .unwrap();
        assert_eq!(opened.base.as_str(), "s3://archive");
        assert_eq!(opened.url.as_str(), "s3://archive/hats/");
        mounts.check_sources(&policy, &transfers).unwrap();
    }

    /// The endpoint rules are about where a *caller* may point this service, so a backend
    /// an operator turned off for callers is still one they may mount — and mounting it
    /// widens nothing, the mount being reachable only through its own `path`.
    #[test]
    fn a_source_is_not_judged_by_the_endpoint_rules() {
        let dir = TempDir::new().unwrap();
        let mounts = Arc::new(
            mounts(&[MountConfig {
                source: "s3://archive/hats".to_owned(),
                ..config("/hats", dir.path())
            }])
            .unwrap(),
        );
        let policy = AccessPolicy::new(
            &AccessConfig {
                // s3 off for every caller.
                s3: EndpointConfig {
                    endpoints: Some(Vec::new()),
                },
                ..Default::default()
            },
            Arc::clone(&mounts),
            None,
        )
        .unwrap();
        let transfers = Arc::new(Transfers::new(&LimitsConfig::default()));
        mounts.check_sources(&policy, &transfers).unwrap();
        // And a caller writing the source url outright is still refused.
        assert!(
            policy
                .authorize(&Url::parse("s3://archive/hats/x.parquet").unwrap())
                .is_err()
        );
    }

    #[test]
    fn a_request_path_finds_its_mount_and_the_rest_of_the_path() {
        let dir = TempDir::new().unwrap();
        let found = mounts(&[config("/hats", dir.path()), config("/raw", dir.path())]).unwrap();
        let (mount, rest) = found.resolve("/hats/dr1/x.parquet").unwrap();
        assert_eq!(mount.prefix(), "/hats");
        assert_eq!(rest, "dr1/x.parquet");
        // The source as resolved, not as written: on macOS the temporary directory is
        // reached through a symlink, and the path that gets opened is the resolved one.
        assert_eq!(
            mount.local_source(),
            Some(std::fs::canonicalize(dir.path()).unwrap().as_path())
        );
        assert!(found.resolve("/other/x.parquet").is_none());
        assert!(Mounts::default().resolve("/hats/x").is_none());
    }

    /// The file server sees a subset of the addresses, and an unserved prefix is a hole
    /// in its url space rather than something a wider mount picks up.
    #[test]
    fn only_a_served_mount_is_published() {
        let dir = TempDir::new().unwrap();
        let found = mounts(&[
            MountConfig {
                serve: false,
                ..config("/private", dir.path())
            },
            config("/hats", dir.path()),
        ])
        .unwrap();
        assert!(found.resolve("/private/x.parquet").is_some());
        assert!(found.published("/private/x.parquet").is_none());
        assert!(found.published("/hats/x.parquet").is_some());
        assert!(!found.is_empty() && !found.serves_nothing());

        let api_only = mounts(&[MountConfig {
            serve: false,
            ..config("/private", dir.path())
        }])
        .unwrap();
        assert!(!api_only.is_empty() && api_only.serves_nothing());
    }

    /// A mount that names no `filenames` reads its files by `[data] filenames`, and one
    /// that names its own reads them by that and does not disturb the others.
    #[test]
    fn a_mount_may_carry_its_own_list_of_data_files() {
        let dir = TempDir::new().unwrap();
        let found = Mounts::new(
            &[
                MountConfig {
                    filenames: Some(vec!["*.fits".to_owned()]),
                    ..config("/fits", dir.path())
                },
                config("/hats", dir.path()),
            ],
            &DataConfig::default(),
        )
        .unwrap();
        let [fits, hats] = found.0.as_slice() else {
            panic!("expected two mounts")
        };
        assert!(fits.data_files().matches("image.fits"));
        assert!(!fits.data_files().matches("part0.parquet"));
        assert!(hats.data_files().matches("part0.parquet"));
        assert!(!hats.data_files().matches("image.fits"));
    }

    #[test]
    fn a_path_is_decoded_one_segment_at_a_time() {
        assert_eq!(
            path_segments("dr1/a%20b.parquet").unwrap(),
            ["dr1", "a b.parquet"]
        );
        // Empty components are dropped, so a doubled separator is not a component.
        assert_eq!(path_segments("//dr1//x").unwrap(), ["dr1", "x"]);
        // An encoded separator is part of a name, and a name is one component.
        for refused in ["a%2Fb", "..", ".", "dr1/../etc", "a%00b"] {
            assert!(path_segments(refused).is_err(), "{refused} was accepted");
        }
    }
}
