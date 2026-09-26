//! A caller's url, opened into something DataFusion can read.
//!
//! The rest of the service only ever sees a [`RemoteFile`]; it does not know that S3 exists,
//! that S3 needs a region, or that a region has to be asked for. Adding a backend means
//! adding a [`Backend`] variant and following the compile errors: every match on one is
//! exhaustive, and the served schemes and option lists are derived from it rather than
//! written out beside it.
//!
//! Storage options arrive beside the URL as [`StorageOptions`], never inside it. A
//! URL's query string is the origin's — a presigned signature, a CDN token, part of
//! what identifies the bytes — and nothing could tell one of those from one of ours.
//! Options never leave `storage`, and a credential never reaches an error message.
//!
//! ```json
//! {
//!   "url": "https://data.example.com/hats/part0.parquet",
//!   "storage": {"headers": {"Authorization": "Bearer …"}}
//! }
//! ```
//!
//! Which URLs may be opened at all is not decided here: [`open`] asks the
//! [`AccessPolicy`] first, and every path into a store goes through that one call.

use std::path::Path as FilePath;
use std::sync::Arc;

use futures::StreamExt;
use http::HeaderMap;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ObjectMeta, ObjectStore, ObjectStoreExt, local::LocalFileSystem,
};
use url::Url;

use crate::access::{AccessPolicy, BACKENDS, Backend, LOCAL_SCHEME, Target};
use crate::error::ApiError;
use crate::storage::backends::{
    Reach, Redirects, azblob_builder, gcs_builder, hf_builder, http_builder, origin, remote_store,
    s3_builder, webdav_builder, webdav_endpoint, webdav_headers,
};
use crate::storage::huggingface::{HfRepo, HfStore, hf_headers};
use crate::storage::materialize::{MaterializingStore, Transfers};
use crate::storage::options::{Credentials, Fingerprint, Opened, StorageOptions, options_clause};

/// Whether [`open`] can serve this scheme at all. Asked of [`Backend`] rather than of a
/// list written out by hand, so a backend cannot be added and then refused here by a
/// list nobody updated. Whether a given url in a served scheme may *actually* be read is
/// the [`AccessPolicy`]'s business, not this one's.
fn is_supported_scheme(scheme: &str) -> bool {
    Backend::from_scheme(scheme).is_some() || scheme == LOCAL_SCHEME
}

/// The same set, spelled out for an error message.
pub fn supported_schemes() -> Vec<&'static str> {
    BACKENDS
        .iter()
        .copied()
        .flat_map(Backend::schemes)
        .copied()
        .chain([LOCAL_SCHEME])
        .collect()
}

/// The options a `[[mount]]`'s own `storage` built a store from, where one did.
///
/// **Carried on the handle rather than worked out again beside it.** Which mount a url
/// lands in is settled once, by the policy, on the way to building the store; deriving it
/// a second time at the place that needs to know — to decide whether two tables may share
/// the one store DataFusion files them under — would be the same fact in two places, and
/// the one that goes wrong is the one nothing checks. `None` is every other store: a
/// caller's own url, and a mount that is a directory on this machine, whose
/// `LocalFileSystem` is built with no credentials at all.
pub type MountedBy = Option<Arc<StorageOptions>>;

/// An opened remote file: the store it lives in, the key DataFusion registers that
/// store under, the object's own URL, and which mount's credentials built the store.
pub struct RemoteFile {
    pub store: Arc<dyn ObjectStore>,
    pub base: Url,
    pub url: Url,
    mounted_by: MountedBy,
    built_with: Fingerprint,
}

impl RemoteFile {
    /// A handle on a store this module did not build, for a test that has one of its own.
    ///
    /// Unmounted by construction, which is the only thing a store made outside `build` can
    /// truthfully be: [`Mount::open`](crate::access::mount::Mount::open) is what stamps the
    /// other case, and nothing here has a mount.
    #[cfg(test)]
    pub(crate) fn over(store: Arc<dyn ObjectStore>, base: Url, url: Url) -> Self {
        Self {
            store,
            base,
            url,
            mounted_by: None,
            #[expect(
                clippy::unwrap_used,
                reason = "a test with no entropy cannot run anyway"
            )]
            built_with: StorageOptions::default().fingerprint().unwrap(),
        }
    }

    /// The same object, read through another store — one that wraps this one to count its
    /// requests, or to record them.
    ///
    /// Everything else is carried over, what built the store included: an instrumented
    /// store is the same store for the purpose of who may share it, and a wrapper that
    /// reset that would be a test measuring something other than what runs.
    pub fn through(&self, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            base: self.base.clone(),
            url: self.url.clone(),
            mounted_by: self.mounted_by.clone(),
            built_with: self.built_with,
        }
    }

    /// Another handle on the same object, sharing the one store.
    ///
    /// Not `Clone`: a store is an `Arc` and a url is a string, so copying one is cheap, but
    /// a derive would also make it cheap to copy something holding a credential around
    /// without meaning to.
    pub fn clone_handle(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            base: self.base.clone(),
            url: self.url.clone(),
            mounted_by: self.mounted_by.clone(),
            built_with: self.built_with,
        }
    }

    /// This store as [`Authorities`](super::Authorities) compares it: the authority it is
    /// registered under, and what built it.
    pub fn opened<'a>(&self, options: &'a StorageOptions) -> Opened<'a> {
        Opened::new(&self.base, &self.mounted_by, options)
    }
}

/// `Url`'s own `Debug` prints its parsed fields, `password` among them.
impl std::fmt::Debug for RemoteFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteFile")
            .field("store", &self.store)
            .field("base", &self.base.as_str())
            .field("url", &self.url.as_str())
            .finish()
    }
}

/// An opened directory: the store it lives in, and the prefix everything under it is
/// addressed relative to.
///
/// A catalog is a directory and not an object — `properties`, `partition_info.csv`,
/// `_metadata` and a tree of parquet files, none of which the caller names — so this is
/// what a caller's catalog url opens as, and every file read out of it is named relative
/// to this rather than by a url of its own.
///
/// `Clone` shares the store rather than building a second one: the handle is an `Arc`, and
/// two of them are the same connection pool and the same policy decision.
#[derive(Clone)]
pub struct RemoteDir {
    pub store: Arc<dyn ObjectStore>,
    pub base: Url,
    /// The prefix, always ending in `/` so that a relative name joins onto it rather than
    /// replacing its last segment.
    pub url: Url,
    mounted_by: MountedBy,
    built_with: Fingerprint,
}

/// One entry of a listing, named relative to the directory that was listed.
#[derive(Debug, Clone)]
pub struct Entry {
    /// The path below the listed prefix. It may contain `/`: a listing is recursive,
    /// which for a catalog is what makes `Norder=…/Dir=…/Npix=….parquet` one request.
    pub name: String,
    pub size: u64,
}

/// One object directly inside a directory, as a file server describes it.
///
/// More than an [`Entry`] carries because a listing is read by a person and by `fsspec`
/// rather than by the catalog reader: the time is what a browser shows and what a client
/// compares, and it is the store's own rather than anything derived here.
#[derive(Debug, Clone)]
pub struct Object {
    pub name: String,
    pub size: u64,
    pub modified: chrono::DateTime<chrono::Utc>,
}

/// One level of a directory in a store: the names directly inside it and nothing below
/// them.
///
/// A store's namespace is flat and has no directories in it, so what stands in for one is
/// the set of keys sharing a prefix up to the next separator. That is what
/// `list_with_delimiter` answers, and it is one request for a directory however large the
/// tree under it is — which [`RemoteDir::list`]'s recursive walk is not.
#[derive(Debug, Default)]
pub struct Level {
    /// The prefixes one step down, as bare names with no separator on either end.
    pub directories: Vec<String>,
    pub files: Vec<Object>,
}

/// `Url`'s own `Debug` prints its parsed fields, `password` among them.
impl std::fmt::Debug for RemoteDir {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteDir")
            .field("store", &self.store)
            .field("base", &self.base.as_str())
            .field("url", &self.url.as_str())
            .finish()
    }
}

impl RemoteDir {
    /// A url that named a directory, normalized so that joining a name onto it appends.
    fn new(file: RemoteFile) -> Self {
        let RemoteFile {
            store,
            base,
            url,
            mounted_by,
            built_with,
        } = file;
        let mut url = url;
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }
        Self {
            store,
            base,
            url,
            mounted_by,
            built_with,
        }
    }

    /// A file inside this directory, as the query layer reads one.
    ///
    /// `relative` is joined as a path and never as a url: a name that parses as one of its
    /// own — `//host/x`, or anything with a scheme — would otherwise address a different
    /// server entirely, and these names come out of a catalog's own files.
    pub fn child(&self, relative: &str) -> Result<RemoteFile, ApiError> {
        Ok(RemoteFile {
            store: Arc::clone(&self.store),
            base: self.base.clone(),
            url: self.join(relative)?,
            mounted_by: self.mounted_by.clone(),
            built_with: self.built_with,
        })
    }

    /// This store as [`Authorities`](super::Authorities) compares it: the authority it is
    /// registered under, and what built it.
    pub fn opened<'a>(&self, options: &'a StorageOptions) -> Opened<'a> {
        Opened::new(&self.base, &self.mounted_by, options)
    }

    /// The options this store was built from, as a digest: the request's own for a url a
    /// caller wrote, and the mount's for one that landed in a store-backed mount.
    ///
    /// Stamped where the store is built, for the reason [`MountedBy`] is — which options
    /// built it is settled there, and working it out again beside the handle would be the
    /// same fact in two places. With the url, it is what says two handles reach the same
    /// bytes the same way.
    pub fn built_with(&self) -> Fingerprint {
        self.built_with
    }

    /// The same handle, stamped as the given mount's.
    ///
    /// [`Mount::open`](crate::access::mount::Mount::open) is the one caller, and it is the
    /// one place that knows: `open_configured_dir` is handed a url and options and has no
    /// mount in front of it.
    #[must_use]
    pub fn mounted_by(self, options: Arc<StorageOptions>) -> Self {
        Self {
            mounted_by: Some(options),
            ..self
        }
    }

    /// A directory inside this one, joined the same way.
    pub fn subdir(&self, relative: &str) -> Result<Self, ApiError> {
        Ok(Self::new(self.child(relative)?))
    }

    /// The bytes of a file inside this directory.
    pub async fn read(&self, relative: &str) -> Result<bytes::Bytes, ApiError> {
        let key = self.key(relative)?;
        Ok(self.store.get(&key).await?.bytes().await?)
    }

    /// The same, for a file a catalog may simply not have. Absence is how one discovery
    /// tier says the next one should be tried, so it is a value here rather than an error.
    pub async fn read_if_present(&self, relative: &str) -> Result<Option<bytes::Bytes>, ApiError> {
        match self.read(relative).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(ApiError::ObjectStore(object_store::Error::NotFound { .. })) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// What the store says about one name inside this directory, or `None` where it holds
    /// no object of that name.
    ///
    /// Absence is a value rather than an error for the reason it is in
    /// [`Self::read_if_present`], and for one more: a store has no directories, so "no
    /// object here" is also how a name that is a prefix answers.
    pub async fn meta(&self, relative: &str) -> Result<Option<ObjectMeta>, ApiError> {
        match self.store.head(&self.key(relative)?).await {
            Ok(meta) => Ok(Some(meta)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// How large a file inside this directory is, without reading it.
    pub async fn size(&self, relative: &str) -> Result<Option<u64>, ApiError> {
        Ok(self.meta(relative).await?.map(|meta| meta.size))
    }

    /// One file inside this directory, as the store hands it over — a range of it where
    /// the options ask for one, and the conditional answer where they carry a validator.
    ///
    /// The bytes are not collected here. A mounted object is served as it arrives, so what
    /// the file server needs is the stream and what the store said about it.
    pub async fn get(&self, relative: &str, options: GetOptions) -> Result<GetResult, ApiError> {
        Ok(self.store.get_opts(&self.key(relative)?, options).await?)
    }

    /// The names directly inside a prefix, one level down.
    ///
    /// One request against an object store, and one `readdir` against a local filesystem.
    /// Distinct from [`Self::list`], which walks the whole tree: a directory page describes
    /// one directory, and a recursive listing of a catalog's `Norder=` level is millions of
    /// keys to build a page of ten entries from.
    pub async fn level(&self, relative: &str) -> Result<Level, ApiError> {
        let prefix = self.key(relative)?;
        let listed = self.store.list_with_delimiter(Some(&prefix)).await?;
        // The store answers with whole keys; what a listing says is names inside this one.
        // A key that is not below the prefix cannot be described as a name in it, so it is
        // dropped rather than rendered as whatever the arithmetic left.
        let inside = |key: &ObjectPath| -> Option<String> {
            let (whole, head) = (key.as_ref(), prefix.as_ref());
            let rest = match head.is_empty() {
                true => whole,
                false => whole.strip_prefix(head)?.strip_prefix('/')?,
            };
            (!rest.is_empty() && !rest.contains('/')).then(|| rest.to_owned())
        };
        Ok(Level {
            directories: listed.common_prefixes.iter().filter_map(inside).collect(),
            files: listed
                .objects
                .iter()
                .filter_map(|meta| {
                    Some(Object {
                        name: inside(&meta.location)?,
                        size: meta.size,
                        modified: meta.last_modified,
                    })
                })
                .collect(),
        })
    }

    /// Every file below a prefix inside this directory, recursively.
    ///
    /// One request against an object store, whose namespace is flat, and one walk against a
    /// local filesystem. Not every backend can do it at all — an `http(s)://` origin has no
    /// listing operation — which is a refusal from the store rather than an empty answer.
    pub async fn list(&self, relative: &str) -> Result<Vec<Entry>, ApiError> {
        let prefix = self.key(relative)?;
        let mut entries = Vec::new();
        let mut listing = self.store.list(Some(&prefix));
        while let Some(meta) = listing.next().await.transpose()? {
            let Some(name) = meta.location.as_ref().strip_prefix(prefix.as_ref()) else {
                continue;
            };
            entries.push(Entry {
                name: name.trim_start_matches('/').to_owned(),
                size: meta.size,
            });
        }
        Ok(entries)
    }

    /// The key a name inside this directory has in the store.
    fn key(&self, relative: &str) -> Result<ObjectPath, ApiError> {
        // `relative` is what the request or the catalog named and is safe to repeat; the
        // error is not, since `object_store`'s path errors print the whole path they were
        // given, which for a local directory is a mount's `source`.
        ObjectPath::from_url_path(self.join(relative)?.path()).map_err(|error| {
            tracing::warn!(%error, "not a valid object path");
            ApiError::bad_request(format!("{relative:?} is not a valid object path"))
        })
    }

    fn join(&self, relative: &str) -> Result<Url, ApiError> {
        let base = self.url.path();
        let mut url = self.url.clone();
        url.set_path(&format!("{base}{}", relative.trim_start_matches('/')));
        // `set_path` percent-encodes what it has to and leaves `/` alone, so a name that
        // tried to climb out is still there to be refused rather than resolved away.
        if url
            .path_segments()
            .is_some_and(|mut segments| segments.any(|segment| segment == ".." || segment == "."))
        {
            return Err(ApiError::bad_request(format!(
                "{relative:?} is not a name inside this directory"
            )));
        }
        Ok(url)
    }
}

/// Who wrote the url, which is what decides whether the endpoint rules have anything to
/// say about it.
///
/// The rules under `[api.access]` are about where a *caller* may point this service. An
/// operator naming a directory in the config is the permission itself — the same reason
/// there is no `[api.access]` section for a local one — and it widens nothing, since a
/// mount is reachable only through its own `path`. Requiring the endpoint to be named as
/// well would be the wrong grant anyway: an entry in `endpoints` opens every bucket at
/// that server to every caller, where the mount opens one prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamedBy {
    /// A url in a request.
    Caller,
    /// A `[[mount]]` source in the config file.
    Operator,
}

pub fn open(
    url: &Url,
    options: &StorageOptions,
    policy: &AccessPolicy,
    transfers: &Arc<Transfers>,
) -> Result<RemoteFile, ApiError> {
    require_object_key(url)?;
    build(url, options, policy, transfers, NamedBy::Caller)
}

/// The same, for a url naming a directory rather than an object.
///
/// A HATS catalog is addressed as a directory — `properties` and a tree of parquet files
/// under one prefix — so the one thing this drops is [`open`]'s refusal of a url naming no
/// object, whose message is written for a caller who meant to name a file. Every other
/// check `open` makes is about the url and the policy rather than about what is at the end
/// of it, and they all still run.
pub fn open_dir(
    url: &Url,
    options: &StorageOptions,
    policy: &AccessPolicy,
    transfers: &Arc<Transfers>,
) -> Result<RemoteDir, ApiError> {
    Ok(RemoteDir::new(build(
        url,
        options,
        policy,
        transfers,
        NamedBy::Caller,
    )?))
}

/// The directory a `[[mount]]`'s `source` names, opened.
///
/// The one caller is [`crate::access::mount::Mount::open`], and what it drops is the
/// endpoint rules — see [`NamedBy`]. Everything else `open_dir` checks still runs, the
/// options included, so a mount carrying another backend's option is a startup error the
/// same way a request carrying one is a 400.
pub fn open_configured_dir(
    url: &Url,
    options: &StorageOptions,
    policy: &AccessPolicy,
    transfers: &Arc<Transfers>,
) -> Result<RemoteDir, ApiError> {
    Ok(RemoteDir::new(build(
        url,
        options,
        policy,
        transfers,
        NamedBy::Operator,
    )?))
}

fn build(
    url: &Url,
    options: &StorageOptions,
    policy: &AccessPolicy,
    transfers: &Arc<Transfers>,
    named_by: NamedBy,
) -> Result<RemoteFile, ApiError> {
    refuse_userinfo(url)?;
    refuse_query_string(url)?;
    if !is_supported_scheme(url.scheme()) {
        return Err(ApiError::bad_request(format!(
            "unsupported URL scheme {:?}: supported schemes are {}",
            url.scheme(),
            supported_schemes().join(", ")
        )));
    }
    // The refusal of another backend's options and the narrowing to this one's are the same
    // step, so what the builders below are handed is a value that could not have been obtained
    // without the check. Before the policy, as the check it replaces was: a `file://` url with
    // a `secret_access_key` is a caller's mistake whatever the policy would have said.
    let credentials = options.resolve(url.scheme())?;
    // Before anything is built, and before the filesystem is touched.
    let target = match named_by {
        NamedBy::Caller => policy.authorize(url)?,
        NamedBy::Operator => Target::Remote(policy.authorize_configured(url)?),
    };
    match target {
        Target::Local(path) => local_file(&path),
        // A mount whose source is a store. The store is the mount's and so are the
        // credentials: the caller wrote a path under `path` and named neither.
        Target::InStore(mount, relative) => mount.open(policy, transfers)?.child(&relative),
        Target::Remote(backend) => {
            refuse_port_on_a_bucket(url, backend)?;
            // `None` is a scheme with no backend, which `authorize` has just said this is not.
            let credentials = credentials
                .ok_or_else(|| ApiError::internal("a remote url whose scheme names no backend"))?;
            // Each arm produces a configured builder and nothing more; `remote_store` is
            // the single place a builder becomes something that can make a request.
            // Every backend takes these; only the http one has anything to put in them.
            let headers = match credentials {
                Credentials::Webdav(webdav) => webdav_headers(webdav)?,
                Credentials::Http(http) => http.headers.to_header_map()?,
                Credentials::Hf(hf) => hf_headers(hf)?,
                _ => HeaderMap::new(),
            };
            // The requests this crate makes itself for this store — a range probe, a Hub
            // listing — go on the same client the transport wraps, so a configured source
            // makes them through the one with no address rules too.
            let client = match named_by {
                NamedBy::Caller => policy.network().client(),
                NamedBy::Operator => policy.network().configured_client(),
            };
            let reach = Reach::of(options, policy, named_by);
            let store: Arc<dyn ObjectStore> = match credentials {
                Credentials::S3(s3) => Arc::new(remote_store(
                    s3_builder(url, s3, reach)?,
                    policy,
                    &headers,
                    Redirects::Refused,
                    named_by,
                )?),
                Credentials::Gcs(gcs) => Arc::new(remote_store(
                    gcs_builder(url, gcs, reach)?,
                    policy,
                    &headers,
                    Redirects::Refused,
                    named_by,
                )?),
                Credentials::Azure(azure) => Arc::new(remote_store(
                    azblob_builder(url, azure, reach)?,
                    policy,
                    &headers,
                    Redirects::Refused,
                    named_by,
                )?),
                // The one backend whose origin hands a file over by redirecting: the Hub
                // answers a `resolve` with a `307` to a presigned url on a CDN for anything
                // in LFS, which is every parquet file in a dataset. What makes following it
                // acceptable is in `storage::redirect`.
                //
                // No `MaterializingStore`: the server is the provider rather than one the
                // caller chose, and it answers a ranged read with a `206`.
                Credentials::Hf(_) => {
                    // Parsed here and thrown away: the store reads the repository back out of
                    // each key, and what this call is for is the refusal a caller sees — a url
                    // naming no repository is a 400 from this service rather than whatever the
                    // Hub says about a path it has never heard of.
                    HfRepo::parse(url)?;
                    let (builder, hub) = hf_builder(reach)?;
                    Arc::new(HfStore::new(
                        Arc::new(remote_store(
                            builder,
                            policy,
                            &headers,
                            Redirects::Followed,
                            named_by,
                        )?),
                        HfRepo::kind_of(url)?,
                        hub,
                        // The listing is this crate's own request rather than the store's, so
                        // it goes on the policy's client — the same one the transport wraps,
                        // with the same resolver behind it.
                        client,
                        headers.clone(),
                    ))
                }
                // The one backend whose server may refuse to serve byte ranges, since it
                // is the one whose server the caller chose rather than the operator.
                Credentials::Http(_) => Arc::new(MaterializingStore::new(
                    Arc::new(remote_store(
                        http_builder(url, reach)?,
                        policy,
                        &headers,
                        Redirects::Refused,
                        named_by,
                    )?),
                    origin(url)?,
                    client,
                    // The probe is a request of this service's own, made outside the
                    // store, so it needs the headers handed to it separately — a server
                    // that authenticates would answer it 401 otherwise, and the object
                    // would look unreadable rather than unauthenticated.
                    headers,
                    Arc::clone(transfers),
                )),
                Credentials::Webdav(webdav) => Arc::new(MaterializingStore::new(
                    Arc::new(remote_store(
                        webdav_builder(url, webdav, reach)?,
                        policy,
                        &headers,
                        Redirects::Refused,
                        named_by,
                    )?),
                    webdav_endpoint(url, webdav)?,
                    client,
                    headers,
                    Arc::clone(transfers),
                )),
            };
            Ok(RemoteFile {
                store,
                base: origin(url)?,
                url: file_url(url),
                // A url the caller wrote; the mount arm above is what stamps one.
                mounted_by: None,
                built_with: options.fingerprint()?,
            })
        }
    }
}

/// A file the file-server mode has already resolved, as something the query layer can
/// read.
///
/// This does not go through [`open`], and there is nothing here for it to decide. `open`
/// exists to judge a url a caller wrote — the scheme, the options, the endpoint, the
/// address behind it — and in file-server mode the caller wrote none of that: they named
/// a path under a mount, and [`crate::access::local::authorize_mounted`] has already answered
/// the only question there was, against the mount, returning the canonical path taken
/// here. Routing it back through a `file://` url would ask the API mode's local-access
/// rules about a file the API mode is not serving.
pub fn open_mounted(path: &FilePath) -> Result<RemoteFile, ApiError> {
    local_file(path)
}

/// The same, for a directory the file-server mode resolved — a catalog under a mount.
///
/// The trailing separator is the whole of the difference, as it is between [`open`] and
/// [`open_dir`]: a name inside the directory then joins onto its url rather than replacing
/// its last segment.
pub fn open_mounted_dir(path: &FilePath) -> Result<RemoteDir, ApiError> {
    Ok(RemoteDir::new(local_file(path)?))
}

/// A file on this machine, already resolved and allowed by the policy. The url is
/// rebuilt from the canonical path, so what the rest of the service reads and logs is
/// the file that was actually opened, not the way the caller spelled it.
#[expect(
    clippy::expect_used,
    reason = "`file://` is a literal, and its parse is checked by every test that opens \
              a local file"
)]
fn local_file(path: &FilePath) -> Result<RemoteFile, ApiError> {
    // The path is the mount's `source` joined with what the caller wrote, so naming it
    // here would put the operator's directory in a response. Only a path that is not
    // absolute reaches this, and a mount's source is canonical.
    let url = Url::from_file_path(path).map_err(|()| {
        tracing::error!(path = %path.display(), "a resolved local path is not a file url");
        ApiError::internal("cannot read this file")
    })?;
    Ok(RemoteFile {
        store: Arc::new(LocalFileSystem::new()),
        base: Url::parse("file://").expect("file:// is a valid url"),
        url,
        // A `LocalFileSystem` is built with no credentials at all, so there is nothing a
        // second table at this authority could be given that it does not already have.
        mounted_by: None,
        built_with: StorageOptions::default().fingerprint()?,
    })
}

/// The URL as far as it is safe to print. `refuse_userinfo` means an opened file never
/// has any, but this also builds the error messages — one of which is that refusal.
///
/// Public because a caller's url exists before it is opened, and so before that refusal has
/// run: anything holding one at that stage prints it through here rather than through a
/// formula of its own.
pub fn file_url(url: &Url) -> Url {
    let mut file = url.clone();
    file.set_query(None);
    file.set_fragment(None);
    let _ = file.set_username("");
    let _ = file.set_password(None);
    file
}

/// `s3://key:secret@bucket/object` would survive into `RemoteFile::url`, the url every
/// layer downstream logs. Refused rather than stripped, so a caller who meant it is
/// told where credentials go instead of getting an unexplained 403.
fn refuse_userinfo(url: &Url) -> Result<(), ApiError> {
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ApiError::bad_request(format!(
            "url {}://{} carries credentials in its authority; {}",
            url.scheme(),
            // Not `file_url`: that keeps the userinfo, which is the thing to not echo.
            url.host_str().unwrap_or_default(),
            options_clause(url)
        )));
    }
    Ok(())
}

/// An object key has no query string in any scheme served here. Dropping one silently
/// would turn a credentialed read into an anonymous one that fails later and elsewhere.
/// A scheme whose objects do have query strings makes this a per-scheme decision.
fn refuse_query_string(url: &Url) -> Result<(), ApiError> {
    if url.query().is_some() {
        return Err(ApiError::bad_request(format!(
            "url {} has a query string; storage options go in \"storage\", and {}",
            file_url(url),
            options_clause(url)
        )));
    }
    Ok(())
}

/// `s3://bucket:9000/key.parquet` is a caller who thinks the host half of the url is the
/// server. It is the bucket, and a bucket has no port — so this is refused rather than
/// dropped, which would read the url as naming a bucket the caller did not write and
/// send it to whatever endpoint the options named instead.
fn refuse_port_on_a_bucket(url: &Url, backend: Backend) -> Result<(), ApiError> {
    match url.port() {
        Some(port) if backend.has_provider() => Err(ApiError::bad_request(format!(
            "url {} names port {port}; a {} url's host is a bucket, so the server goes \
             in the endpoint option",
            file_url(url),
            url.scheme()
        ))),
        _ => Ok(()),
    }
}

fn require_object_key(url: &Url) -> Result<(), ApiError> {
    if url.path().trim_start_matches('/').is_empty() {
        return Err(ApiError::bad_request(format!(
            "url {} points at no object; expected a path to a parquet file",
            file_url(url)
        )));
    }
    Ok(())
}

/// A url as the caller wrote it — the raw string, before [`parse_url`], so it may not
/// even be a url. `Debug` prints it cut at the first `?`, which is the most that can be
/// said about a string nothing has parsed.
// No `ToSchema`: on the wire this is a string, and a named `SourceUrl` component would put a
// Rust newtype into the API description for a reader to wonder about. The field that holds one
// declares `value_type = String` instead.
#[derive(Clone, serde::Deserialize)]
#[serde(transparent)]
pub struct SourceUrl(String);

impl SourceUrl {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The part safe to put in a message: everything before the query string.
    pub fn redacted(&self) -> &str {
        redact(&self.0)
    }
}

impl From<String> for SourceUrl {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl std::fmt::Debug for SourceUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.redacted())
    }
}

/// Cut a url string at its query string. For a url that parses, [`file_url`] strips the
/// query properly; this is for the ones that do not, where there is nothing to strip
/// properly with.
fn redact(raw: &str) -> &str {
    // Also the fragment: `#` before `?` means there is no query string at all, and
    // whatever follows is not something to echo either.
    let end = raw.find(['?', '#']).unwrap_or(raw.len());
    raw.get(..end).unwrap_or(raw)
}

pub fn parse_url(raw: &str) -> Result<Url, ApiError> {
    // An absolute local path is not a URL, but it is what someone with a local file in
    // front of them will type, and it has exactly one reading.
    if raw.starts_with('/') {
        return Url::from_file_path(raw)
            .map_err(|()| ApiError::bad_request(format!("invalid local path {raw:?}")));
    }
    Url::parse(raw).map_err(|error| {
        // Unparseable, so there is no query string to strip properly.
        ApiError::bad_request(format!("invalid url {:?}: {error}", redact(raw)))
    })
}

#[cfg(test)]
pub(super) mod tests {
    use secrecy::{ExposeSecret, SecretString};

    use super::*;

    pub(in crate::storage) const SECRET: &str = "wJalrXUtnFEMIsecretKEY";

    /// A derive on a struct holding a credential prints the rest of it and not that.
    #[test]
    fn a_derived_debug_around_a_secret_does_not_print_it() {
        #[derive(Debug)]
        #[expect(dead_code, reason = "the fields exist to be printed by the derive")]
        struct Holder {
            name: &'static str,
            secret: SecretString,
        }
        let holder = Holder {
            name: "key",
            secret: SecretString::from(SECRET.to_owned()),
        };
        let shown = format!("{holder:?}");
        assert!(shown.contains("key"), "{shown}");
        assert!(!shown.contains(SECRET), "leaked: {shown}");
        assert_eq!(holder.secret.expose_secret(), SECRET);
    }

    #[test]
    fn a_source_url_prints_without_its_query_string() {
        let url = SourceUrl::from(format!(
            "s3://bucket/key.parquet?access_key_id=AKIA1&secret_access_key={SECRET}"
        ));
        let shown = format!("{url:?}");
        assert!(shown.contains("s3://bucket/key.parquet"), "{shown}");
        assert!(!shown.contains(SECRET), "leaked: {shown}");
        // The value itself is untouched; only the printing is.
        assert!(url.as_str().contains(SECRET));
    }

    #[test]
    fn redacting_leaves_a_url_without_a_query_string_alone() {
        assert_eq!(redact("s3://bucket/key.parquet"), "s3://bucket/key.parquet");
        assert_eq!(redact("not-a-url"), "not-a-url");
        assert_eq!(redact(""), "");
        // A fragment is not a query string, and is not ours to echo either.
        assert_eq!(redact("s3://b/k#frag?a=1"), "s3://b/k");
        assert_eq!(redact("?everything"), "");
    }

    /// Storage options built the way a request body spells them, so these tests also
    /// cover the deserialization rather than only the struct behind it.
    pub(in crate::storage) fn options(json: serde_json::Value) -> StorageOptions {
        serde_json::from_value(json).expect("the options should deserialize")
    }

    pub(in crate::storage) fn no_options() -> StorageOptions {
        StorageOptions::default()
    }

    /// These tests are about reading the URL, not about the policy, so they all run
    /// under one that allows every bucket. What the policy itself allows is
    /// [`crate::access`]'s own business, and tested there.
    pub(in crate::storage) fn open(
        url: &Url,
        options: &StorageOptions,
    ) -> Result<RemoteFile, ApiError> {
        super::open(url, options, &AccessPolicy::default(), &transfers())
    }

    /// The scratch budget at its defaults. Only a server that refuses byte ranges
    /// consults it, and none of these tests has one.
    pub(in crate::storage) fn transfers() -> Arc<Transfers> {
        Arc::new(Transfers::new(&crate::config::LimitsConfig::default()))
    }

    #[test]
    fn opens_an_s3_url() {
        let url = parse_url("s3://bucket/some/key.parquet").unwrap();
        let file = open(&url, &no_options()).unwrap();
        assert_eq!(file.base.as_str(), "s3://bucket");
        assert_eq!(file.url.as_str(), "s3://bucket/some/key.parquet");
    }

    #[test]
    fn opens_a_gcs_url() {
        let url = parse_url("gs://bucket/some/key.parquet").unwrap();
        let file = open(&url, &no_options()).unwrap();
        assert_eq!(file.base.as_str(), "gs://bucket");
        assert_eq!(file.url.as_str(), "gs://bucket/some/key.parquet");
    }

    /// The url carries the container; the account is an option, because Azure has no
    /// one host to default to and the url has nowhere to put the other half.
    #[test]
    fn opens_an_azure_url() {
        let url = parse_url("az://container/some/key.parquet").unwrap();
        let file = open(&url, &options(serde_json::json!({"account": "hatsdata"}))).unwrap();
        assert_eq!(file.base.as_str(), "az://container");
        assert_eq!(file.url.as_str(), "az://container/some/key.parquet");
    }

    /// Options are the request's, the url is the object's, and neither reaches the
    /// other: the key registered with DataFusion is what the caller named.
    #[test]
    fn storage_options_stay_out_of_the_object_key() {
        let url = parse_url("s3://bucket/key.parquet").unwrap();
        let file = open(&url, &options(serde_json::json!({"region": "us-west-2"}))).unwrap();
        assert_eq!(file.url.as_str(), "s3://bucket/key.parquet");
    }

    #[test]
    fn errors_never_carry_the_credentials() {
        let credentials = options(serde_json::json!({
            "access_key_id": "AKIA123",
            "secret_access_key": SECRET,
        }));
        // Every failure path that formats a url: no key, no host, an unusable scheme.
        for raw in ["s3://bucket", "ftp://bucket/key.parquet", "s3://bucket/"] {
            let url = parse_url(raw).unwrap();
            let error = open(&url, &credentials).unwrap_err().to_string();
            assert!(!error.contains(SECRET), "leaked in: {error}");
        }
    }

    /// The url is the object's alone, and options in its query string are refused
    /// rather than ignored: ignoring them turns a credentialed read into an anonymous
    /// one, which fails later and somewhere else.
    #[test]
    fn refuses_a_url_that_carries_a_query_string() {
        let url = parse_url(&format!(
            "s3://bucket/key.parquet?secret_access_key={SECRET}"
        ))
        .unwrap();
        let error = open(&url, &no_options()).unwrap_err();
        assert!(matches!(error, ApiError::BadRequest(_)), "{error}");
        assert!(error.to_string().contains("query string"), "{error}");
        assert!(error.to_string().contains("storage"), "{error}");
        assert!(!error.to_string().contains(SECRET), "leaked: {error}");
    }

    #[test]
    fn rejects_schemes_we_cannot_serve_yet() {
        let url = parse_url("ftp://example.com/a.parquet").unwrap();
        let error = open(&url, &no_options()).unwrap_err();
        assert!(matches!(error, ApiError::BadRequest(_)), "{error}");
        assert!(
            error.to_string().contains("unsupported URL scheme"),
            "{error}"
        );
    }

    #[test]
    fn rejects_urls_without_an_object_key() {
        for raw in ["s3://bucket", "s3://bucket/"] {
            let url = parse_url(raw).unwrap();
            let error = open(&url, &no_options()).unwrap_err();
            assert!(error.to_string().contains("no object"), "{raw}: {error}");
        }
    }

    /// Printing the options, and printing the opened file. Both are `Debug` and both
    /// are one `tracing` call away from a log; neither may carry a credential.
    /// `RemoteFile` holds the store, whose own `Debug` is the backend's, so this is
    /// also what would catch a backend that started printing what it was built with.
    #[test]
    fn debug_output_carries_no_credentials() {
        let credentials = options(serde_json::json!({
            "access_key_id": "AKIA123",
            "secret_access_key": SECRET,
            "session_token": "tok",
            "endpoint": "https://minio.example.com",
        }));

        let shown = format!("{credentials:?}");
        assert!(!shown.contains(SECRET), "leaked: {shown}");
        assert!(!shown.contains("AKIA123"), "leaked: {shown}");
        assert!(!shown.contains("tok\""), "leaked: {shown}");
        // Still worth printing: the endpoint is how a misrouted request is diagnosed.
        assert!(shown.contains("minio.example.com"), "{shown}");

        let url = parse_url("s3://bucket/key.parquet").unwrap();
        let file = format!("{:?}", open(&url, &credentials).unwrap());
        assert!(!file.contains(SECRET), "leaked: {file}");
        assert!(!file.contains("AKIA123"), "leaked: {file}");
        assert!(file.contains("s3://bucket/key.parquet"), "{file}");
    }

    /// Credentials in the authority are the other way to spell them into the url, and
    /// the one that would ride along inside the url the whole service logs.
    #[test]
    fn refuses_credentials_in_the_url_authority() {
        for raw in [
            &format!("s3://AKIA123:{SECRET}@bucket/key.parquet"),
            &format!("s3://{SECRET}@bucket/key.parquet"),
        ] {
            let url = parse_url(raw).unwrap();
            let error = open(&url, &no_options()).unwrap_err();
            assert!(matches!(error, ApiError::BadRequest(_)), "{error}");
            assert!(error.to_string().contains("in its authority"), "{error}");
            assert!(!error.to_string().contains(SECRET), "leaked: {error}");
        }
    }

    /// The same url with no object key, which fails earlier and on a different message.
    #[test]
    fn an_error_before_the_userinfo_check_does_not_echo_it_either() {
        let url = parse_url(&format!("s3://AKIA123:{SECRET}@bucket")).unwrap();
        let error = open(&url, &no_options()).unwrap_err().to_string();
        assert!(!error.contains(SECRET), "leaked: {error}");
    }

    /// The key DataFusion actually files a store under, which is scheme and authority
    /// and nothing else. `Url` normalises `https://host` to a `/` path, so comparing the
    /// base url as written would be comparing a spelling rather than the key.
    pub(in crate::storage) fn store_key(file: &RemoteFile) -> String {
        format!(
            "{}://{}",
            file.base.scheme(),
            &file.base[url::Position::BeforeHost..url::Position::AfterPort]
        )
    }

    /// A plain HTTP server: the url is the whole address, so there is no bucket, no
    /// endpoint option and nothing to sign with.
    #[test]
    fn opens_an_https_url() {
        let url = parse_url("https://data.example.com/hats/part0.parquet").unwrap();
        let file = open(&url, &no_options()).unwrap();
        assert_eq!(store_key(&file), "https://data.example.com");
        assert_eq!(
            file.url.as_str(),
            "https://data.example.com/hats/part0.parquet"
        );
    }

    /// The port is part of which server this is, and DataFusion files a store under
    /// scheme and authority — so dropping it would put two servers under one key and
    /// send the second one's reads to the first.
    #[test]
    fn a_port_is_part_of_the_server_a_url_names() {
        let url = parse_url("https://data.example.com:8443/part0.parquet").unwrap();
        let file = open(&url, &no_options()).unwrap();
        assert_eq!(store_key(&file), "https://data.example.com:8443");
    }

    /// The other half: a bucket-addressed url's host is a bucket, and a bucket has no
    /// port. Refused rather than dropped, which would read the url as naming a bucket
    /// the caller did not write.
    #[test]
    fn a_port_on_a_bucket_url_is_refused() {
        let url = parse_url("s3://bucket:9000/key.parquet").unwrap();
        let error = open(&url, &no_options()).unwrap_err();
        assert!(matches!(error, ApiError::BadRequest(_)), "{error}");
        assert!(error.to_string().contains("host is a bucket"), "{error}");
    }

    /// Basic-auth credentials in the authority are the other way to spell a secret into
    /// an `https://` url. Refused, with the message naming the option that does carry
    /// one and never echoing what was written.
    #[test]
    fn credentials_in_an_http_url_authority_are_refused() {
        let url = parse_url(&format!("https://user:{SECRET}@data.example.com/k.parquet")).unwrap();
        let error = open(&url, &no_options()).unwrap_err();
        assert!(error.to_string().contains("in its authority"), "{error}");
        assert!(error.to_string().contains("headers"), "{error}");
        assert!(!error.to_string().contains(SECRET), "leaked: {error}");
    }

    #[test]
    fn keeps_equals_signs_in_hats_paths() {
        let url = parse_url("s3://b/hats/Norder=5/Npix=12240/part0.parquet").unwrap();
        let file = open(&url, &no_options()).unwrap();
        assert_eq!(file.url.path(), url.path());
        assert!(file.url.path().contains("Norder=5"), "{}", file.url.path());
    }
}
