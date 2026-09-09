//! Turning a user-supplied URL into something DataFusion can read.
//!
//! Everything storage-specific lives here. The rest of the service only ever sees a
//! [`RemoteFile`]; it does not know that S3 exists, that S3 needs a region, or that a
//! region has to be asked for. Adding a backend means adding a [`Backend`] variant and
//! following the compile errors: every match on one is exhaustive, and the served
//! schemes and option lists are derived from it rather than written out beside it.
//!
//! Storage options arrive beside the URL as [`StorageOptions`], never inside it. A
//! URL's query string is the origin's — a presigned signature, a CDN token, part of
//! what identifies the bytes — and nothing could tell one of those from one of ours.
//! Options never leave this module, and a credential never reaches an error message.
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

use std::collections::BTreeMap;
use std::path::Path as FilePath;
use std::sync::Arc;

use base64::Engine;
use futures::StreamExt;
use http::{HeaderMap, HeaderName, HeaderValue};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, local::LocalFileSystem};
use object_store_opendal::OpendalStore;
use opendal::layers::RetryLayer;
use opendal::{HttpTransport, HttpTransporter, OperationContext, Operator, services};
use secrecy::{ExposeSecret, SecretString};
use url::Url;

use crate::access::{
    AccessPolicy, BACKENDS, Backend, EndpointScheme, LOCAL_SCHEME, Target,
    describe_endpoint_schemes,
};
use crate::error::ApiError;
use crate::materialize::{MaterializingStore, Transfers};

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

/// S3 offers no way to discover a bucket's region, and object_store will not guess.
pub const DEFAULT_S3_REGION: &str = "us-east-1";

/// How to reach the store the object lives in. Not what the object is — that is the
/// URL, which this service treats as opaque.
///
/// One flat set rather than one per scheme: the URL already says which backend it is,
/// and an option the scheme has no use for is refused rather than ignored. A misspelled
/// option is a 400 rather than a silently anonymous request.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageOptions {
    /// Base URL of a server other than the provider's own: MinIO, Ceph, R2, Azurite.
    pub endpoint: Option<String>,
    /// Permission to send credentials to a cleartext `endpoint`.
    #[serde(default)]
    pub allow_http: bool,

    pub region: Option<String>,
    pub access_key_id: Option<SecretString>,
    pub secret_access_key: Option<SecretString>,
    pub session_token: Option<SecretString>,

    /// A GCS service account key: the JSON Google issues, base64-encoded.
    pub service_account_key: Option<SecretString>,
    /// A GCS OAuth2 access token, for a caller who mints short-lived credentials of
    /// their own rather than handing over a service account key.
    pub access_token: Option<SecretString>,

    /// The Azure storage account the container is in. Required for `az://`: it is the
    /// host half of the address, which the url only carries the container half of.
    pub account: Option<String>,
    /// An Azure storage account key, base64 as Azure issues it.
    pub access_key: Option<SecretString>,
    pub sas_token: Option<SecretString>,

    /// Headers to send with every request to an `http(s)://` server, for a service that
    /// authenticates with one — a bearer token, an API key. Treated as a credential
    /// whatever the caller puts in it, since that is what it is for.
    #[serde(default)]
    pub headers: Headers,
    /// The HTTP transport under a `webdav://` URL. HTTPS is the default.
    #[serde(default)]
    pub transport: Option<WebdavTransport>,
    /// A WebDAV Basic credential, which is the only kind this backend takes. Both halves
    /// or neither: a username alone would authenticate as nobody, which a server answers
    /// the same way it answers an anonymous request.
    pub username: Option<SecretString>,
    pub password: Option<SecretString>,
}

/// The transport a WebDAV server speaks. An HTTP transport must be named exactly in
/// `api.access.webdav.endpoints`; HTTPS is used when this option is absent.
#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WebdavTransport {
    Http,
    Https,
}

impl WebdavTransport {
    fn scheme(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }
}

/// Caller-supplied request headers.
///
/// A newtype rather than a bare map for one reason: the derived `Debug` on a map prints
/// its keys, and both halves of an entry here come from the caller. A token in the value
/// is the point of the option; a token in the *name* is a caller's mistake, but it would
/// be this service's log it landed in. So neither is printed, and what a reader gets is
/// how many there were.
#[derive(Default, Clone, serde::Deserialize)]
#[serde(transparent)]
pub struct Headers(BTreeMap<String, SecretString>);

impl std::fmt::Debug for Headers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{} header(s)>", self.0.len())
    }
}

impl Headers {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The headers as something that can go on a request, with every name checked.
    ///
    /// A name is refused rather than dropped: a caller who asked for a header that does
    /// not arrive has been told their request is authenticated when it is not.
    fn to_header_map(&self) -> Result<HeaderMap, ApiError> {
        let mut map = HeaderMap::with_capacity(self.0.len());
        for (name, value) in &self.0 {
            let parsed = HeaderName::try_from(name.as_str()).map_err(|_| {
                ApiError::bad_request(format!("{name:?} is not a valid header name"))
            })?;
            if let Some(reason) = refused_header(&parsed) {
                return Err(ApiError::bad_request(format!(
                    "header {name:?} cannot be set on a request this service makes: {reason}"
                )));
            }
            // The value is the caller's secret, so a parse failure says nothing about
            // what was in it — only that it cannot go in a header.
            let mut parsed_value = HeaderValue::from_str(value.expose_secret()).map_err(|_| {
                ApiError::bad_request(format!(
                    "the value given for header {name:?} contains characters a header \
                     cannot carry"
                ))
            })?;
            // Marks it for redaction in anything that formats a `HeaderMap`, which is
            // the last line of defence rather than the first.
            parsed_value.set_sensitive(true);
            map.insert(parsed, parsed_value);
        }
        Ok(map)
    }
}

/// Headers a caller may not set, and why. Two kinds: the ones that decide where the
/// request goes or how much of it to read, which are this service's to set, and the
/// hop-by-hop ones, which describe a connection rather than a request and would be a way
/// to confuse the client rather than to authenticate to the server.
fn refused_header(name: &HeaderName) -> Option<&'static str> {
    let hop_by_hop = [
        http::header::CONNECTION,
        http::header::PROXY_AUTHENTICATE,
        http::header::PROXY_AUTHORIZATION,
        http::header::TE,
        http::header::TRAILER,
        http::header::TRANSFER_ENCODING,
        http::header::UPGRADE,
        http::header::CONTENT_LENGTH,
    ];
    // What the backend sets on a read of its own, and would therefore be overridden
    // rather than merged. Every one of these is a header whose value decides which bytes
    // come back, so a caller's copy of it is a caller quietly answering a question this
    // service had already answered.
    let conditional = [
        http::header::IF_MATCH,
        http::header::IF_NONE_MATCH,
        http::header::IF_MODIFIED_SINCE,
        http::header::IF_UNMODIFIED_SINCE,
    ];
    if *name == http::header::HOST {
        return Some(
            "it names the server, which the url already does and the access policy has already judged",
        );
    }
    if *name == http::header::RANGE {
        return Some("every read this service makes is a ranged one, so it sets its own");
    }
    if conditional.contains(name) {
        return Some(
            "it decides whether the server answers with the object or with a 304, which \
             is this service's question to ask",
        );
    }
    if *name == http::header::ACCEPT_ENCODING {
        return Some(
            "a compressed body has different offsets from the object, so a ranged read \
             of it returns the wrong bytes",
        );
    }
    if hop_by_hop.contains(name) || name.as_str().eq_ignore_ascii_case("keep-alive") {
        return Some("it describes the connection rather than the request");
    }
    None
}

/// The caller's half of the cleartext decision, which every remote backend has: each of
/// them can be given a credential, and none may send one over cleartext unless the
/// caller who owns it said so.
const CLEARTEXT_OPTION: &[&str] = &["allow_http"];
/// Naming a server other than the provider's own, which only a backend that has a
/// provider can do.
const ENDPOINT_OPTION: &[&str] = &["endpoint"];
const HTTP_OPTIONS: &[&str] = &["headers"];
const WEBDAV_OPTIONS: &[&str] = &["transport", "username", "password"];
const S3_OPTIONS: &[&str] = &[
    "region",
    "access_key_id",
    "secret_access_key",
    "session_token",
];
const GCS_OPTIONS: &[&str] = &["service_account_key", "access_token"];
const AZURE_OPTIONS: &[&str] = &["account", "access_key", "sas_token"];

/// Whether an option is proof of identity. What separates the two is not the type — an
/// Azure `account` is a `String` and a GCS `access_token` is a `SecretString`, and both
/// are sent — but whether the store would treat it as saying who is asking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Plain,
    Credential,
}

/// One option, as a request spells it.
struct Named {
    name: &'static str,
    set: bool,
    kind: Kind,
}

/// The types a [`Kind::Plain`] option is allowed to have. [`SecretString`] is
/// deliberately not among them, so classifying a secret as plain does not compile —
/// which is the mistake worth catching, since it is the one that makes
/// [`allow_cleartext`] wave a credential through.
trait NotACredential {}
impl NotACredential for String {}
impl NotACredential for WebdavTransport {}

impl Named {
    /// An option that says something about where to go, not about who is asking.
    fn plain<T: NotACredential>(name: &'static str, value: &Option<T>) -> Self {
        Self {
            name,
            set: value.is_some(),
            kind: Kind::Plain,
        }
    }

    /// The same for a bool, which is set by being true rather than by being there.
    fn flag(name: &'static str, value: bool) -> Self {
        Self {
            name,
            set: value,
            kind: Kind::Plain,
        }
    }

    /// Proof of identity. Taking [`SecretString`] and nothing else is the other half of
    /// the check: a credential declared as a plain `String` cannot be classified as one,
    /// and so cannot be declared that way at all.
    fn credential(name: &'static str, value: &Option<SecretString>) -> Self {
        Self {
            name,
            set: value.is_some(),
            kind: Kind::Credential,
        }
    }

    /// Several of them under one option name. [`Headers`] is a credential by
    /// construction — it exists to carry a token — so there is no plain counterpart to
    /// register it with by mistake.
    fn credentials(name: &'static str, value: &Headers) -> Self {
        Self {
            name,
            set: !value.is_empty(),
            kind: Kind::Credential,
        }
    }
}

impl StorageOptions {
    /// Every option, under the name a request spells it, whether it is set, and whether
    /// it is a credential.
    ///
    /// Everything that has to know the full set of options reads this one list: the
    /// per-scheme check, [`Self::is_empty`], and [`Self::has_credentials`]. Two lists
    /// would be two chances to forget a field, and forgetting one in the credential list
    /// is the expensive direction — it makes [`allow_cleartext`] wave through a request
    /// that does carry a secret.
    ///
    /// Two things keep the list honest, both at compile time: the destructuring means a
    /// field added to the struct and not listed here does not compile, and the
    /// constructors mean a field listed under the wrong [`Kind`] does not either.
    fn named(&self) -> [Named; 15] {
        let Self {
            endpoint,
            allow_http,
            region,
            access_key_id,
            secret_access_key,
            session_token,
            service_account_key,
            access_token,
            account,
            access_key,
            sas_token,
            headers,
            transport,
            username,
            password,
        } = self;
        [
            Named::plain("endpoint", endpoint),
            Named::flag("allow_http", *allow_http),
            Named::plain("region", region),
            Named::credential("access_key_id", access_key_id),
            Named::credential("secret_access_key", secret_access_key),
            Named::credential("session_token", session_token),
            Named::credential("service_account_key", service_account_key),
            Named::credential("access_token", access_token),
            // The storage account, which is the host half of an `az://` address rather
            // than anything that authenticates. It is signed *over*, not sent as proof.
            Named::plain("account", account),
            Named::credential("access_key", access_key),
            Named::credential("sas_token", sas_token),
            Named::credentials("headers", headers),
            Named::plain("transport", transport),
            Named::credential("username", username),
            Named::credential("password", password),
        ]
    }

    /// Nothing set at all, which is what a public object needs.
    pub fn is_empty(&self) -> bool {
        self.named().iter().all(|option| !option.set)
    }

    /// A `file://` url with a `secret_access_key`, or an `s3://` one with a `sas_token`,
    /// is a caller who has the wrong url or the wrong options; either reading is worth
    /// saying rather than guessing at, and one of them misdirects a credential.
    fn for_scheme(&self, scheme: &str) -> Result<(), ApiError> {
        let accepted = accepted_options(scheme);
        if accepted.is_empty() {
            return match self.is_empty() {
                true => Ok(()),
                false => Err(ApiError::bad_request(format!(
                    "{scheme:?} urls take no storage options"
                ))),
            };
        }
        match self
            .named()
            .into_iter()
            .find(|option| option.set && !accepted.contains(&option.name))
        {
            None => Ok(()),
            Some(option) => Err(ApiError::bad_request(format!(
                "option {:?} is not one {scheme:?} urls take; they take {}",
                option.name,
                accepted.join(", ")
            ))),
        }
    }

    /// These options written back out, credentials in the clear, for a plan whose caller
    /// asked to have them.
    ///
    /// **The only place a credential is copied out of this struct on purpose.** Everything
    /// else — logs, errors, metrics, the plan by default — sees the stripped url and
    /// nothing more. What makes this safe is not the code here but who asked for it: a
    /// caller gets back the secret they themselves sent, in a response to their own
    /// request, and only when they set `return_storage`. It enables nothing they cannot
    /// already do; what it costs is that the plan is then a document with a secret in it,
    /// which is why it is off unless asked for.
    ///
    /// Destructured like `named`, so a field added to the struct and not written
    /// here does not compile. The guard runs the other way for this one: forgetting a field
    /// hands back a plan missing something the caller asked for, rather than one carrying
    /// what they did not.
    pub fn echo(&self) -> serde_json::Value {
        fn plain(into: &mut serde_json::Map<String, serde_json::Value>, name: &str, value: &str) {
            into.insert(name.to_owned(), value.into());
        }
        fn secret(
            into: &mut serde_json::Map<String, serde_json::Value>,
            name: &str,
            value: &Option<SecretString>,
        ) {
            if let Some(value) = value {
                plain(into, name, value.expose_secret());
            }
        }
        let Self {
            endpoint,
            allow_http,
            region,
            access_key_id,
            secret_access_key,
            session_token,
            service_account_key,
            access_token,
            account,
            access_key,
            sas_token,
            headers,
            transport,
            username,
            password,
        } = self;
        let mut out = serde_json::Map::new();
        for (name, value) in [
            ("endpoint", endpoint),
            ("region", region),
            ("account", account),
        ] {
            if let Some(value) = value {
                plain(&mut out, name, value);
            }
        }
        if *allow_http {
            out.insert("allow_http".to_owned(), true.into());
        }
        if let Some(transport) = transport {
            plain(&mut out, "transport", transport.scheme());
        }
        secret(&mut out, "access_key_id", access_key_id);
        secret(&mut out, "secret_access_key", secret_access_key);
        secret(&mut out, "session_token", session_token);
        secret(&mut out, "service_account_key", service_account_key);
        secret(&mut out, "access_token", access_token);
        secret(&mut out, "access_key", access_key);
        secret(&mut out, "sas_token", sas_token);
        secret(&mut out, "username", username);
        secret(&mut out, "password", password);
        if !headers.is_empty() {
            let mut written = serde_json::Map::new();
            for (name, value) in &headers.0 {
                plain(&mut written, name, value.expose_secret());
            }
            out.insert("headers".to_owned(), written.into());
        }
        out.into()
    }

    /// Whether anything here would be sent to the store as proof of identity — which is
    /// the whole of what `allow_cleartext` is protecting, and what a plan says to re-attach.
    pub fn has_credentials(&self) -> bool {
        self.named()
            .iter()
            .any(|option| option.set && option.kind == Kind::Credential)
    }
}

/// The options a scheme takes. Empty for a scheme that takes none at all, which is both
/// `file://`, where there is no store to reach, and `http(s)://`, where the url is
/// already the whole of the address.
fn accepted_options(scheme: &str) -> Vec<&'static str> {
    let Some(backend) = Backend::from_scheme(scheme) else {
        return Vec::new();
    };
    let specific: &[&str] = match backend {
        Backend::S3 => S3_OPTIONS,
        Backend::Gcs => GCS_OPTIONS,
        Backend::Azure => AZURE_OPTIONS,
        Backend::Http => HTTP_OPTIONS,
        Backend::Webdav => WEBDAV_OPTIONS,
    };
    // A url that is its own endpoint has nothing for `endpoint` to point elsewhere at.
    // `allow_http` is a different question and every backend has it, because every
    // backend can now be handed a credential the caller would not want in cleartext.
    let endpoint: &[&str] = match backend.has_provider() {
        true => ENDPOINT_OPTION,
        false => &[],
    };
    [endpoint, CLEARTEXT_OPTION, specific].concat()
}

/// The clause naming what this url's scheme takes, for the two messages that have to say
/// where a caller's options go. Phrased both ways rather than printing an empty list,
/// which would read as though the scheme's options had been left out of the message.
///
/// Written for a scheme that may not be one this service serves at all: these messages
/// come from checks that run before the scheme is known, and saying "ftp urls take no
/// storage options" is true and is followed by the refusal that matters.
fn options_clause(url: &Url) -> String {
    let scheme = url.scheme();
    match accepted_options(scheme).as_slice() {
        [] => format!("{scheme} urls take no storage options"),
        options => format!("{scheme} urls take {}", options.join(", ")),
    }
}

/// An opened remote file: the store it lives in, the key DataFusion registers that
/// store under, and the object's own URL.
pub struct RemoteFile {
    pub store: Arc<dyn ObjectStore>,
    pub base: Url,
    pub url: Url,
}

impl RemoteFile {
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
        }
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
pub struct RemoteDir {
    pub store: Arc<dyn ObjectStore>,
    pub base: Url,
    /// The prefix, always ending in `/` so that a relative name joins onto it rather than
    /// replacing its last segment.
    pub url: Url,
}

/// One entry of a listing, named relative to the directory that was listed.
#[derive(Debug, Clone)]
pub struct Entry {
    /// The path below the listed prefix. It may contain `/`: a listing is recursive,
    /// which for a catalog is what makes `Norder=…/Dir=…/Npix=….parquet` one request.
    pub name: String,
    pub size: u64,
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
        let RemoteFile { store, base, url } = file;
        let mut url = url;
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }
        Self { store, base, url }
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
        })
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

    /// How large a file inside this directory is, without reading it.
    pub async fn size(&self, relative: &str) -> Result<Option<u64>, ApiError> {
        match self.store.head(&self.key(relative)?).await {
            Ok(meta) => Ok(Some(meta.size)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
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

pub fn open(
    url: &Url,
    options: &StorageOptions,
    policy: &AccessPolicy,
    transfers: &Arc<Transfers>,
) -> Result<RemoteFile, ApiError> {
    require_object_key(url)?;
    build(url, options, policy, transfers)
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
    Ok(RemoteDir::new(build(url, options, policy, transfers)?))
}

fn build(
    url: &Url,
    options: &StorageOptions,
    policy: &AccessPolicy,
    transfers: &Arc<Transfers>,
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
    options.for_scheme(url.scheme())?;
    // Before anything is built, and before the filesystem is touched.
    match policy.authorize(url)? {
        Target::Local(path) => local_file(&path),
        Target::Remote(backend) => {
            refuse_port_on_a_bucket(url, backend)?;
            // Each arm produces a configured builder and nothing more; `remote_store` is
            // the single place a builder becomes something that can make a request.
            // Every backend takes these; only the http one has anything to put in them.
            let headers = match backend {
                Backend::Webdav => webdav_headers(options)?,
                _ => options.headers.to_header_map()?,
            };
            let store: Arc<dyn ObjectStore> = match backend {
                Backend::S3 => Arc::new(remote_store(
                    s3_builder(url, options, policy)?,
                    policy,
                    &headers,
                )?),
                Backend::Gcs => Arc::new(remote_store(
                    gcs_builder(url, options, policy)?,
                    policy,
                    &headers,
                )?),
                Backend::Azure => Arc::new(remote_store(
                    azblob_builder(url, options, policy)?,
                    policy,
                    &headers,
                )?),
                // The one backend whose server may refuse to serve byte ranges, since it
                // is the one whose server the caller chose rather than the operator.
                Backend::Http => Arc::new(MaterializingStore::new(
                    Arc::new(remote_store(
                        http_builder(url, options, policy)?,
                        policy,
                        &headers,
                    )?),
                    origin(url)?,
                    policy.network().client(),
                    // The probe is a request of this service's own, made outside the
                    // store, so it needs the headers handed to it separately — a server
                    // that authenticates would answer it 401 otherwise, and the object
                    // would look unreadable rather than unauthenticated.
                    headers,
                    Arc::clone(transfers),
                )),
                Backend::Webdav => Arc::new(MaterializingStore::new(
                    Arc::new(remote_store(
                        webdav_builder(url, options, policy)?,
                        policy,
                        &headers,
                    )?),
                    webdav_endpoint(url, options)?,
                    policy.network().client(),
                    headers,
                    Arc::clone(transfers),
                )),
            };
            Ok(RemoteFile {
                store,
                base: origin(url)?,
                url: file_url(url),
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
/// a path under a mount, and [`crate::access::authorize_mounted`] has already answered
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
    })
}

/// The URL as far as it is safe to print. [`refuse_userinfo`] means an opened file never
/// has any, but this also builds the error messages — one of which is that refusal.
fn file_url(url: &Url) -> Url {
    let mut file = url.clone();
    file.set_query(None);
    file.set_fragment(None);
    let _ = file.set_username("");
    let _ = file.set_password(None);
    file
}

/// The host part of the url, which for a bucket-addressed backend is the bucket or
/// container name.
fn host(url: &Url) -> Result<&str, ApiError> {
    url.host_str()
        .filter(|host| !host.is_empty())
        .ok_or_else(|| ApiError::bad_request(format!("url {} has no host", file_url(url))))
}

/// The same, plus the port when the url names one the scheme does not imply.
///
/// The port is only ever part of an address, so it belongs to [`origin`] and not to the
/// three backends above, whose host is a bucket name. A bucket cannot have a port, and
/// quietly accepting one into a bucket name would be a way to write something into a
/// signed request that is not a bucket.
fn authority(url: &Url) -> Result<String, ApiError> {
    let host = host(url)?;
    Ok(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    })
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

fn parse_endpoint(endpoint: &str) -> Result<Url, ApiError> {
    Url::parse(endpoint)
        .map_err(|error| ApiError::bad_request(format!("invalid endpoint {endpoint:?}: {error}")))
}

/// Which server this request would have us talk to, decided before anything is built.
/// `None` is a request with no `endpoint` option, which means the provider's own
/// service — and naming no endpoint is a choice the policy gets to refuse too.
fn resolve_endpoint(
    backend: Backend,
    options: &StorageOptions,
    policy: &AccessPolicy,
) -> Result<Option<Url>, ApiError> {
    let Some(raw) = options.endpoint.as_deref() else {
        policy.authorize_endpoint(backend, None)?;
        // A provider's own service is https, so there is nothing here for `allow_http`
        // to permit, and a caller who set it has misunderstood what it does.
        if options.allow_http {
            return Err(ApiError::bad_request(
                "allow_http only applies together with endpoint",
            ));
        }
        return Ok(None);
    };

    let endpoint = parse_endpoint(raw)?;
    // Before the policy: an endpoint in a scheme this service does not speak is not a
    // request the policy has anything useful to say about, and saying which host it
    // will not reach would answer a question the caller did not ask.
    let scheme = require_endpoint_scheme(&endpoint)?;
    policy.authorize_endpoint(backend, Some(&endpoint))?;
    allow_cleartext(
        &endpoint,
        scheme,
        options.allow_http,
        options.has_credentials(),
    )?;
    Ok(Some(endpoint))
}

/// A caller's string that has been checked to be safe inside a hostname. The check is
/// [`require_label`], and this is the only thing it hands back — so the value a backend
/// passes on is one that came from the check rather than one that merely had it run
/// nearby.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HostLabel<'a>(&'a str);

impl HostLabel<'_> {
    fn as_str(&self) -> &str {
        self.0
    }
}

impl std::fmt::Display for HostLabel<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// Anything that ends up inside a hostname the service then connects to has to be
/// checked before it gets there. `format!` does not care that a `/` in the middle of
/// what was meant to be a subdomain moves the host to whatever came before it, so a
/// region or an account name is a way past the endpoint policy unless it is restricted
/// to characters that cannot mean anything else.
fn require_label<'a>(name: &str, value: &'a str, extra: &str) -> Result<HostLabel<'a>, ApiError> {
    let ok = !value.is_empty()
        && value.len() <= 63
        && value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || extra.contains(c));
    match ok {
        true => Ok(HostLabel(value)),
        false => Err(ApiError::bad_request(format!(
            "{name} {value:?} may only hold lowercase letters, digits{}",
            match extra.is_empty() {
                true => String::new(),
                false => format!(" and {extra:?}"),
            }
        ))),
    }
}

/// An endpoint is an HTTP service whatever the storage behind it is. The scheme is handed
/// back rather than merely approved, so the cleartext decision below reads a parsed value
/// instead of comparing the string again.
fn require_endpoint_scheme(endpoint: &Url) -> Result<EndpointScheme, ApiError> {
    EndpointScheme::parse(endpoint.scheme()).ok_or_else(|| {
        ApiError::bad_request(format!(
            "endpoint {endpoint} has scheme {:?}, expected {}",
            endpoint.scheme(),
            describe_endpoint_schemes()
        ))
    })
}

/// Decide whether this endpoint may be spoken to over cleartext. The backend would not
/// ask — it just follows the endpoint's scheme — so this is the whole of the decision,
/// and it happens before a connection is opened rather than after one fails.
fn allow_cleartext(
    endpoint: &Url,
    scheme: EndpointScheme,
    allow_http: bool,
    has_credentials: bool,
) -> Result<(), ApiError> {
    // Nothing to expose when the request is anonymous, and that is the common case of a
    // local MinIO or a test server.
    match !scheme.is_cleartext() || !has_credentials || allow_http {
        true => Ok(()),
        // Display, not Debug: Debug on a Url prints the whole parsed struct. The
        // endpoint is safe to echo either way — credentials are separate options.
        false => Err(ApiError::bad_request(format!(
            "endpoint {endpoint} is not https and credentials were given, which would \
             be sent in cleartext; pass allow_http=true to do it anyway"
        ))),
    }
}

/// The only way a configured builder becomes a store, and so the only place in the crate
/// that calls `Operator::new` — which `clippy.toml` forbids everywhere else. A backend
/// function hands its builder here and never holds an [`Operator`] of its own, so it has
/// nothing to attach the wrong transport to.
///
/// What gets attached: the policy's own HTTP transport, whose resolver decides which
/// addresses may be connected to, and the retries. OpenDAL would otherwise reach for the
/// process-wide default transport — a plain `reqwest::Client` that resolves and connects
/// to whatever it is given, which is the whole of what [`crate::network`] exists to stop.
#[expect(
    clippy::disallowed_methods,
    reason = "the one permitted call; the lint exists to send every other one here"
)]
fn remote_store(
    builder: impl opendal::Builder,
    policy: &AccessPolicy,
    headers: &HeaderMap,
) -> Result<OpendalStore, ApiError> {
    let operator = Operator::new(builder)?
        .with_context(OperationContext::new().with_http_transport(transport(policy, headers)))
        .layer(retries());
    Ok(OpendalStore::new(operator))
}

/// The policy's transport, with the caller's headers on it if they gave any.
///
/// Wrapped per operator rather than per process: the headers are one request's
/// credentials, and the transport underneath is shared by every store in the process.
/// Putting them on the shared one would send one caller's token to every other caller's
/// server.
fn transport(policy: &AccessPolicy, headers: &HeaderMap) -> HttpTransporter {
    let inner = policy.network().transport();
    match headers.is_empty() {
        true => inner,
        false => HttpTransporter::new(WithHeaders {
            inner,
            headers: headers.clone(),
        }),
    }
}

/// Adds the caller's headers to every request an operator makes.
///
/// Below the store and above the policy's client, which is the only layer that sees a
/// whole request and still belongs to one caller. It cannot reach another store: the
/// wrapper is built per operator and holds that operator's headers alone.
struct WithHeaders {
    inner: HttpTransporter,
    headers: HeaderMap,
}

/// Never derived: the headers are the caller's credentials, and this type is one field
/// of something a `tracing` call could print.
impl std::fmt::Debug for WithHeaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WithHeaders(<{} header(s)>)", self.headers.len())
    }
}

impl HttpTransport for WithHeaders {
    async fn fetch(
        &self,
        mut request: http::Request<opendal::Buffer>,
    ) -> opendal::Result<http::Response<opendal::HttpBody>> {
        // `insert`, so a caller cannot append a second value to a header the backend
        // already set and leave the server to choose between them.
        //
        // It can only ever be replacing nothing. Every header the backend sets on a read
        // — the range, and the four conditional ones — is refused by `refused_header`, so
        // by the time a name reaches here it is one the backend does not use. Keep that
        // true: a header this service starts sending for itself has to be refused there
        // in the same change, or a caller silently overrides it.
        for (name, value) in &self.headers {
            request.headers_mut().insert(name, value.clone());
        }
        self.inner.fetch(request).await
    }
}

/// Retry the failures OpenDAL marks temporary: a connection an origin closed between
/// requests, a 503, a truncated body. Reading one parquet file is a sequence of ranged
/// requests over a pooled connection, so a single closed socket would otherwise fail the
/// whole query — and to the caller that looks like the file is unreadable rather than
/// like a hiccup.
///
/// Jitter because those ranged requests are in flight together and would otherwise all
/// come back at the same instant. The delays are short and few: a request is waiting on
/// this, and a store that is really down should be a 502 rather than a hang.
fn retries() -> RetryLayer {
    RetryLayer::new()
        .with_jitter()
        .with_max_times(3)
        .with_min_delay(std::time::Duration::from_millis(100))
        .with_max_delay(std::time::Duration::from_secs(2))
}

fn s3_builder(
    url: &Url,
    options: &StorageOptions,
    policy: &AccessPolicy,
) -> Result<services::S3, ApiError> {
    let bucket = host(url)?;
    // Virtual-host addressing puts the region in the hostname, so a region is one of
    // the strings `require_label` exists for.
    let region = require_label(
        "region",
        options.region.as_deref().unwrap_or(DEFAULT_S3_REGION),
        "-",
    )?;

    let mut builder = services::S3::default()
        .bucket(bucket)
        .region(region.as_str())
        // The request is the only source of credentials. Without these two, OpenDAL
        // reads `AWS_*` from the environment, `~/.aws/{config,credentials}`, and the
        // EC2 metadata service — so a caller who sent none would be answered with the
        // service's own identity and every bucket this deployment can reach.
        // `disable_config_load` also stops `AWS_ENDPOINT_URL` from redirecting a
        // request the policy already decided was going to AWS.
        .disable_config_load()
        .disable_ec2_metadata();

    builder = match resolve_endpoint(Backend::S3, options, policy)? {
        // OpendDAL addresses path-style unless told otherwise, which is what an
        // S3-compatible server on a named endpoint wants: MinIO, Ceph and the rest
        // serve `endpoint/bucket/key`, and virtual-host style would need a wildcard
        // DNS entry per bucket.
        Some(endpoint) => builder.endpoint(endpoint.as_str()),
        // AWS itself is the other way round: path-style addressing is deprecated
        // there, so a bucket is a subdomain.
        None => builder.enable_virtual_host_style(),
    };

    builder = match (&options.access_key_id, &options.secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => {
            let builder = builder
                .access_key_id(access_key_id.expose_secret())
                .secret_access_key(secret_access_key.expose_secret());
            match &options.session_token {
                Some(token) => builder.session_token(token.expose_secret()),
                None => builder,
            }
        }
        (None, None) => {
            if options.session_token.is_some() {
                return Err(ApiError::bad_request(
                    "session_token needs access_key_id and secret_access_key",
                ));
            }
            // No credentials given: ask anonymously rather than signing with nothing.
            builder.skip_signature()
        }
        _ => {
            return Err(ApiError::bad_request(
                "access_key_id and secret_access_key must be given together",
            ));
        }
    };
    Ok(builder)
}

fn gcs_builder(
    url: &Url,
    options: &StorageOptions,
    policy: &AccessPolicy,
) -> Result<services::Gcs, ApiError> {
    let mut builder = services::Gcs::default()
        .bucket(host(url)?)
        // The request is the only source of credentials. Without these two, OpenDAL
        // reads `GOOGLE_APPLICATION_CREDENTIALS`, `~/.config/gcloud` and the GCE
        // metadata server — so a caller who sent none would be answered with the
        // service's own identity and every bucket this deployment can reach.
        .disable_config_load()
        .disable_vm_metadata();

    if let Some(endpoint) = resolve_endpoint(Backend::Gcs, options, policy)? {
        builder = builder.endpoint(endpoint.as_str());
    }

    builder = match (&options.service_account_key, &options.access_token) {
        (Some(_), Some(_)) => {
            return Err(ApiError::bad_request(
                "send service_account_key or access_token, not both",
            ));
        }
        (Some(key), None) => {
            // OpenDAL wants the key base64-encoded, and silently ignores it when it is
            // not — which, with ambient discovery off, turns a mistyped credential into
            // an unexplained 403 from Google rather than a 400 from here.
            let key = key.expose_secret();
            if !is_base64(key) {
                return Err(ApiError::bad_request(
                    "service_account_key is not base64; it is the service account JSON \
                     Google issues, base64-encoded",
                ));
            }
            builder.credential(key)
        }
        (None, Some(token)) => builder.token(token.expose_secret().to_owned()),
        // No credentials given: ask anonymously rather than signing with nothing.
        (None, None) => builder.skip_signature(),
    };
    Ok(builder)
}

fn azblob_builder(
    url: &Url,
    options: &StorageOptions,
    policy: &AccessPolicy,
) -> Result<services::Azblob, ApiError> {
    // Azure has no one host to default to: every account is its own. The url carries
    // the container, so the account has to come from the options — and naming it is
    // also what keeps a credential from being ignored, since OpenDAL only installs a
    // shared key when it has an account name to pair it with, and falls back to the
    // environment when it does not.
    let account = options.account.as_deref().ok_or_else(|| {
        ApiError::bad_request(
            "az:// urls need an account option: the storage account the container is in",
        )
    })?;
    let account = require_label("account", account, "")?;

    let mut builder = services::Azblob::default()
        .container(host(url)?)
        .account_name(account.as_str());

    builder = match resolve_endpoint(Backend::Azure, options, policy)? {
        // Azurite and the rest are addressed as they are written; the account is still
        // sent, because it is half of the shared-key signature.
        Some(endpoint) => builder.endpoint(endpoint.as_str()),
        None => builder.endpoint(&format!("https://{account}.blob.core.windows.net")),
    };

    builder = match (&options.access_key, &options.sas_token) {
        (Some(_), Some(_)) => {
            return Err(ApiError::bad_request(
                "send access_key or sas_token, not both",
            ));
        }
        (Some(key), None) => builder.account_key(key.expose_secret()),
        (None, Some(token)) => builder.sas_token(token.expose_secret()),
        // No credentials given: ask anonymously. Azure's own signer would otherwise
        // reach for `AZURE_*` and the managed-identity endpoint, which is the service's
        // identity rather than the caller's.
        (None, None) => builder.skip_signature(),
    };
    Ok(builder)
}

/// A plain HTTP server, where the url is the whole address: no bucket, no endpoint
/// option, no credentials.
///
/// The two things `CLAUDE.md` requires of a backend before it is served here are met by
/// this one having no identity at all. OpenDAL's http service sends an `Authorization`
/// header only when the builder was given one, and reads no environment variable, no
/// config file and no metadata service on its way to deciding that — so an anonymous
/// request stays anonymous without a switch having to say so.
///
/// What it cannot do is list, so a catalog served this way is discoverable only through
/// the files it names rather than by walking its directories.
fn http_builder(
    url: &Url,
    options: &StorageOptions,
    policy: &AccessPolicy,
) -> Result<services::Http, ApiError> {
    let origin = origin(url)?;
    // The url *is* the endpoint here, so this is the same gate the other backends reach
    // through their `endpoint` option — asked about the address the caller wrote.
    policy.authorize_endpoint(Backend::Http, Some(&origin))?;
    // The operator has already allowed cleartext by this point, or the line above
    // refused it. This is the other half, and it is the caller's: `headers` may carry a
    // token, and whether that goes out in the clear is not the operator's to decide.
    allow_cleartext(
        &origin,
        require_endpoint_scheme(&origin)?,
        options.allow_http,
        options.has_credentials(),
    )?;
    // `Url` prints an empty path as a trailing `/`, and OpenDAL joins the endpoint to a
    // key that already starts with one. Left in, every request would go to `//key`.
    Ok(services::Http::default().endpoint(origin.as_str().trim_end_matches('/')))
}

/// Turn WebDAV's Basic authentication into request headers. The materialization probe
/// and the WebDAV operator share this map, so they authenticate identically.
fn webdav_headers(options: &StorageOptions) -> Result<HeaderMap, ApiError> {
    let mut headers = HeaderMap::new();
    match (&options.username, &options.password) {
        (Some(username), Some(password)) => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(format!(
                "{}:{}",
                username.expose_secret(),
                password.expose_secret()
            ));
            let mut value =
                HeaderValue::from_str(&format!("Basic {encoded}")).map_err(|error| {
                    ApiError::bad_request(format!(
                        "WebDAV basic authentication is not a valid header: {error}"
                    ))
                })?;
            value.set_sensitive(true);
            headers.insert(http::header::AUTHORIZATION, value);
        }
        (None, None) => {}
        _ => {
            return Err(ApiError::bad_request(
                "username and password must be given together",
            ));
        }
    }
    Ok(headers)
}

/// A WebDAV server, addressed by its own url the way [`http_builder`]'s is.
///
/// Like the http service, this one has no identity of its own: OpenDAL's WebDAV config
/// holds an endpoint, a username, a password and a token, and reads no environment
/// variable, no config file and no metadata service on its way to deciding it has none.
/// So the two switches `CLAUDE.md` requires of a backend are satisfied by construction
/// rather than by a call, and there is nothing here for an ambient credential to be
/// picked up from.
///
/// The credential is not given to the builder. It goes out as a header instead, from
/// [`webdav_headers`], so that the materialization probe — which is this crate's own
/// request rather than OpenDAL's — authenticates as the operator does. Setting both
/// would be one credential in two places to keep in step.
fn webdav_builder(
    url: &Url,
    options: &StorageOptions,
    policy: &AccessPolicy,
) -> Result<services::Webdav, ApiError> {
    let endpoint = webdav_endpoint(url, options)?;
    // The url is the endpoint here, as it is for http, so this is the same gate the
    // provider-backed backends reach through their `endpoint` option.
    policy.authorize_endpoint(Backend::Webdav, Some(&endpoint))?;
    // The caller's half of the cleartext decision: a username and password over `http`
    // are Basic authentication in the clear, which is the credential itself and not
    // merely a token derived from it.
    allow_cleartext(
        &endpoint,
        require_endpoint_scheme(&endpoint)?,
        options.allow_http,
        options.has_credentials(),
    )?;
    // As in `http_builder`: OpenDAL joins this to a key that already starts with `/`.
    Ok(services::Webdav::default().endpoint(endpoint.as_str().trim_end_matches('/')))
}

/// The server a `webdav://` url names, under the transport the request chose.
///
/// `webdav` is a scheme for the protocol and says nothing about what carries it, so the
/// transport is named rather than guessed — and defaults to TLS, since a default of
/// cleartext would be one that silently costs the caller their password. The path is not
/// part of this: it names the object, which is what OpenDAL joins to the endpoint, so a
/// server rooted under a prefix like `/remote.php/dav` is reached by writing that prefix
/// into the url.
fn webdav_endpoint(url: &Url, options: &StorageOptions) -> Result<Url, ApiError> {
    Url::parse(&format!(
        "{}://{}",
        options.transport.unwrap_or(WebdavTransport::Https).scheme(),
        authority(url)?
    ))
    .map_err(|error| {
        ApiError::bad_request(format!(
            "cannot derive the WebDAV server from {}: {error}",
            file_url(url)
        ))
    })
}

/// `scheme://host[:port]` — the url with the object taken off it.
///
/// Two jobs, and they want the same value: it is the key DataFusion registers a store
/// under, and for [`http_builder`] it is also the server itself, which is what makes it
/// the thing the endpoint policy judges.
fn origin(url: &Url) -> Result<Url, ApiError> {
    Url::parse(&format!("{}://{}", url.scheme(), authority(url)?)).map_err(|error| {
        ApiError::bad_request(format!(
            "cannot derive the server from {}: {error}",
            file_url(url)
        ))
    })
}

/// Whether a string is base64. Not a decode: what the caller needs to know is that
/// OpenDAL will not discard the credential, and the bytes behind it are none of this
/// module's business. Both alphabets, since which one an encoder used is not something
/// to make a caller find out by trial.
fn is_base64(value: &str) -> bool {
    let body = value.trim_end_matches('=');
    value.len() - body.len() <= 2
        && !body.is_empty()
        && body.len() % 4 != 1
        && body
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"+/-_".contains(&b))
}

/// A url as the caller wrote it — the raw string, before [`parse_url`], so it may not
/// even be a url. `Debug` prints it cut at the first `?`, which is the most that can be
/// said about a string nothing has parsed.
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
mod tests {
    use super::*;

    const SECRET: &str = "wJalrXUtnFEMIsecretKEY";
    /// Azure account keys are base64, and OpenDAL rejects one that is not at build
    /// time — so a test about anything else needs a well-formed one.
    const AZURE_KEY: &str = "c2VjcmV0LWF6dXJlLWFjY291bnQta2V5";

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
    fn options(json: serde_json::Value) -> StorageOptions {
        serde_json::from_value(json).expect("the options should deserialize")
    }

    fn no_options() -> StorageOptions {
        StorageOptions::default()
    }

    /// These tests are about reading the URL, not about the policy, so they all run
    /// under one that allows every bucket. What the policy itself allows is
    /// [`crate::access`]'s own business, and tested there.
    fn open(url: &Url, options: &StorageOptions) -> Result<RemoteFile, ApiError> {
        super::open(url, options, &AccessPolicy::default(), &transfers())
    }

    /// The scratch budget at its defaults. Only a server that refuses byte ranges
    /// consults it, and none of these tests has one.
    fn transfers() -> Arc<Transfers> {
        Arc::new(Transfers::new(&crate::config::LimitsConfig::default()))
    }

    /// The same, for a server configured to let requests reach the loopback interface
    /// — which is what running against a local MinIO means.
    fn open_loopback(url: &Url, options: &StorageOptions) -> Result<RemoteFile, ApiError> {
        let config = crate::config::AccessConfig {
            network: crate::config::NetworkConfig {
                allow_loopback: true,
                ..Default::default()
            },
            ..Default::default()
        };
        super::open(
            url,
            options,
            &AccessPolicy::new(&config, Arc::default()).unwrap(),
            &transfers(),
        )
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

    #[test]
    fn an_azure_url_without_an_account_says_so() {
        let url = parse_url("az://container/key.parquet").unwrap();
        let error = open(&url, &no_options()).unwrap_err();
        assert!(matches!(error, ApiError::BadRequest(_)), "{error}");
        assert!(error.to_string().contains("account"), "{error}");
    }

    /// Each scheme takes its own options and refuses the rest. An option in the wrong
    /// place is a caller who has confused two backends, and where it is a credential
    /// that means a credential sent to the wrong service.
    #[test]
    fn an_option_belonging_to_another_backend_is_refused() {
        for (raw, option, expected) in [
            (
                "gs://b/k.parquet",
                serde_json::json!({"region": "us-west-2"}),
                "region",
            ),
            (
                "gs://b/k.parquet",
                serde_json::json!({"secret_access_key": SECRET}),
                "secret_access_key",
            ),
            (
                "s3://b/k.parquet",
                serde_json::json!({"sas_token": "sv=2021"}),
                "sas_token",
            ),
            (
                "s3://b/k.parquet",
                serde_json::json!({"service_account_key": SECRET}),
                "service_account_key",
            ),
            (
                "az://c/k.parquet",
                serde_json::json!({"account": "hatsdata", "access_key_id": "AKIA123"}),
                "access_key_id",
            ),
        ] {
            let url = parse_url(raw).unwrap();
            let error = open(&url, &options(option.clone())).unwrap_err();
            assert!(matches!(error, ApiError::BadRequest(_)), "{raw}: {error}");
            assert!(error.to_string().contains(expected), "{raw}: {error}");
            // The message names what the scheme does take, and never the value.
            assert!(error.to_string().contains("they take"), "{raw}: {error}");
            assert!(!error.to_string().contains(SECRET), "leaked: {error}");
        }
    }

    /// Options that end up inside a hostname are the way past the endpoint policy: with
    /// a `/` in it, `{region}.amazonaws.com` stops being under `amazonaws.com` at all
    /// and the request goes wherever the caller wrote.
    #[test]
    fn an_option_that_becomes_a_hostname_may_not_carry_a_path() {
        for (raw, option) in [
            (
                "s3://bucket/k.parquet",
                serde_json::json!({"region": "us-east-1.evil.example.com/"}),
            ),
            (
                "s3://bucket/k.parquet",
                serde_json::json!({"region": "US-EAST-1"}),
            ),
            (
                "az://container/k.parquet",
                serde_json::json!({"account": "hatsdata/evil.example.com/"}),
            ),
            (
                "az://container/k.parquet",
                serde_json::json!({"account": "hats data"}),
            ),
            (
                "az://container/k.parquet",
                serde_json::json!({"account": ""}),
            ),
        ] {
            let url = parse_url(raw).unwrap();
            let error = open(&url, &options(option.clone())).unwrap_err();
            assert!(
                matches!(error, ApiError::BadRequest(_)),
                "{option}: {error}"
            );
            assert!(
                error.to_string().contains("lowercase letters"),
                "{option}: {error}"
            );
        }
    }

    /// OpenDAL discards a `credential` it cannot base64-decode, and with ambient
    /// discovery off that turns a mistyped key into an unexplained refusal from Google
    /// rather than a 400 from here.
    #[test]
    fn a_service_account_key_that_is_not_base64_is_refused() {
        let url = parse_url("gs://bucket/k.parquet").unwrap();
        let raw_json =
            serde_json::json!({"service_account_key": "{\"type\": \"service_account\"}"});
        let error = open(&url, &options(raw_json)).unwrap_err();
        assert!(error.to_string().contains("base64"), "{error}");
        assert!(!error.to_string().contains("service_account\""), "{error}");

        let encoded =
            serde_json::json!({"service_account_key": "eyJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCJ9"});
        assert!(open(&url, &options(encoded)).is_ok());
    }

    /// Two credentials are two answers to one question, and picking one for the caller
    /// would mean the other was silently ignored.
    #[test]
    fn two_credentials_for_one_backend_are_refused() {
        for (raw, option) in [
            (
                "gs://b/k.parquet",
                serde_json::json!({
                    "service_account_key": "eyJhIjogMX0=",
                    "access_token": "ya29.token",
                }),
            ),
            (
                "az://c/k.parquet",
                serde_json::json!({
                    "account": "hatsdata",
                    "access_key": AZURE_KEY,
                    "sas_token": "sv=2021",
                }),
            ),
        ] {
            let url = parse_url(raw).unwrap();
            let error = open(&url, &options(option)).unwrap_err();
            assert!(error.to_string().contains("not both"), "{raw}: {error}");
        }
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
    fn accepts_credentials_as_storage_options() {
        let url = parse_url("s3://bucket/key.parquet").unwrap();
        let file = open(
            &url,
            &options(serde_json::json!({
                "access_key_id": "AKIA123",
                "secret_access_key": SECRET,
                "session_token": "tok",
                "region": "us-west-2",
            })),
        )
        .unwrap();
        assert_eq!(file.url.as_str(), "s3://bucket/key.parquet");
    }

    #[test]
    fn accepts_a_custom_endpoint() {
        let url = parse_url("s3://data/key.parquet").unwrap();
        let endpoint = options(serde_json::json!({"endpoint": "https://minio.example.com"}));
        let file = open(&url, &endpoint).unwrap();
        assert_eq!(file.url.as_str(), "s3://data/key.parquet");
        assert_eq!(file.base.as_str(), "s3://data");
    }

    #[test]
    fn anonymous_requests_may_use_a_plain_http_endpoint() {
        let url = parse_url("s3://data/key.parquet").unwrap();
        let endpoint = options(serde_json::json!({"endpoint": "http://minio.example.com"}));
        assert!(open(&url, &endpoint).is_ok());
    }

    /// The usual local-MinIO endpoint is on the loopback interface, which the caller
    /// does not get to reach unless the server was configured for it.
    #[test]
    fn a_loopback_endpoint_needs_the_server_to_allow_it() {
        let url = parse_url("s3://data/key.parquet").unwrap();
        let endpoint = options(serde_json::json!({"endpoint": "http://127.0.0.1:9000"}));
        let error = open(&url, &endpoint).unwrap_err();
        assert!(matches!(error, ApiError::Forbidden(_)), "{error}");
        assert!(error.to_string().contains("loopback"), "{error}");
        assert!(open_loopback(&url, &endpoint).is_ok());
    }

    #[test]
    fn credentials_over_a_plain_http_endpoint_need_saying_so() {
        let url = parse_url("s3://data/key.parquet").unwrap();
        let credentialed = serde_json::json!({
            "endpoint": "http://127.0.0.1:9000",
            "access_key_id": "AKIA123",
            "secret_access_key": SECRET,
        });

        let error = open_loopback(&url, &options(credentialed.clone())).unwrap_err();
        assert!(error.to_string().contains("cleartext"), "{error}");
        assert!(error.to_string().contains("allow_http"), "{error}");
        assert!(!error.to_string().contains(SECRET), "leaked: {error}");

        let mut allowed = credentialed;
        allowed["allow_http"] = serde_json::json!(true);
        assert!(open_loopback(&url, &options(allowed)).is_ok());
    }

    #[test]
    fn rejects_nonsense_endpoints_and_flags() {
        for (option, expected) in [
            (
                serde_json::json!({"endpoint": "ftp://host"}),
                "expected http or https",
            ),
            (
                serde_json::json!({"endpoint": "not a url"}),
                "invalid endpoint",
            ),
            (
                serde_json::json!({"allow_http": true}),
                "only applies together with endpoint",
            ),
        ] {
            let url = parse_url("s3://data/key.parquet").unwrap();
            let error = open(&url, &options(option.clone())).unwrap_err();
            assert!(error.to_string().contains(expected), "{option}: {error}");
        }
    }

    /// `allow_http` is a bool in the body, so a string is a deserialization failure
    /// rather than something this module has to parse.
    #[test]
    fn allow_http_must_be_a_boolean() {
        let error =
            serde_json::from_value::<StorageOptions>(serde_json::json!({"allow_http": "yes"}))
                .unwrap_err();
        assert!(error.to_string().contains("boolean"), "{error}");
    }

    #[test]
    fn credentials_must_come_in_pairs() {
        for option in [
            serde_json::json!({"access_key_id": "AKIA123"}),
            serde_json::json!({"secret_access_key": SECRET}),
            serde_json::json!({"session_token": "tok"}),
        ] {
            let url = parse_url("s3://bucket/key.parquet").unwrap();
            let error = open(&url, &options(option.clone())).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("access_key_id and secret_access_key")
                    || error.to_string().contains("session_token needs"),
                "{option}: {error}"
            );
            assert!(!error.to_string().contains(SECRET), "leaked: {error}");
        }
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

    /// A misspelled option is a 400 rather than a silently anonymous request. Held by
    /// `deny_unknown_fields`, and the point is that it holds at all.
    #[test]
    fn rejects_unknown_storage_options_instead_of_ignoring_them() {
        let error =
            serde_json::from_value::<StorageOptions>(serde_json::json!({"regoin": "us-west-2"}))
                .unwrap_err();
        assert!(error.to_string().contains("unknown field"), "{error}");
        assert!(error.to_string().contains("regoin"), "{error}");
    }

    /// A local file has no store to reach, so options with it mean the caller has the
    /// wrong url or the wrong options — and either way the credential is misdirected.
    #[test]
    fn refuses_storage_options_for_a_scheme_that_has_none() {
        let path = std::env::temp_dir().join("hats-api-nonexistent.parquet");
        let url = Url::from_file_path(&path).unwrap();
        let credentials = options(serde_json::json!({"secret_access_key": SECRET}));
        let error = open(&url, &credentials).unwrap_err();
        assert!(error.to_string().contains("no storage options"), "{error}");
        assert!(!error.to_string().contains(SECRET), "leaked: {error}");
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

    /// A one-shot HTTP server on the loopback interface: it takes one request, hands
    /// the head back to the test and answers 404. Whether a request was signed is not
    /// visible on the builder, only on the wire, so this is where it gets checked.
    fn capture_one_request() -> (u16, std::sync::mpsc::Receiver<String>) {
        // 404 rather than a hang or a reset: a retry would find nothing listening.
        serve_one("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_owned())
    }

    /// The same, answering whatever the caller wants answered.
    fn serve_one(response: String) -> (u16, std::sync::mpsc::Receiver<String>) {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                match stream.read(&mut byte) {
                    Ok(1) => head.push(byte[0]),
                    _ => break,
                }
            }
            let _ = stream.write_all(response.as_bytes());
            let _ = sender.send(String::from_utf8_lossy(&head).into_owned());
        });
        (port, receiver)
    }

    /// The request head one `GET` through the store puts on the wire, lowercased —
    /// header names are case-insensitive and the comparisons below do not care.
    ///
    /// The endpoint is filled in here, because it is the port the capturing server got.
    async fn request_head(raw: &str, mut option: serde_json::Value) -> String {
        let (port, receiver) = capture_one_request();
        option["endpoint"] = serde_json::json!(format!("http://127.0.0.1:{port}"));
        let url = parse_url(raw).unwrap();
        let file = open_loopback(&url, &options(option)).unwrap();
        // The 404 is the point: the request reached the server, which is all the test
        // needs to see.
        use object_store::ObjectStoreExt;
        let _ = file
            .store
            .get(&object_store::path::Path::from("key.parquet"))
            .await;
        receiver
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the store made no request")
            .to_ascii_lowercase()
    }

    /// A caller who sends no credentials gets an anonymous request, never the
    /// service's own identity. The builder disables every ambient source OpenDAL
    /// would otherwise consult; this checks the result of that.
    #[tokio::test]
    async fn a_request_without_credentials_is_unsigned() {
        let head = request_head("s3://bucket/key.parquet", serde_json::json!({})).await;
        assert!(head.contains("get /bucket/key.parquet"), "{head}");
        assert!(!head.contains("authorization:"), "signed anyway: {head}");
        assert!(!head.contains("x-amz-security-token:"), "{head}");
    }

    /// And the other half: the credentials the request carried are the ones that sign.
    /// The secret itself never goes on the wire — SigV4 sends a signature and the key
    /// id.
    #[tokio::test]
    async fn credentials_from_the_request_are_the_ones_that_sign() {
        let head = request_head(
            "s3://bucket/key.parquet",
            serde_json::json!({
                "access_key_id": "AKIA123",
                "secret_access_key": SECRET,
                "allow_http": true,
            }),
        )
        .await;
        assert!(head.contains("authorization:"), "unsigned: {head}");
        assert!(head.contains("akia123"), "{head}");
        assert!(
            !head.contains(&SECRET.to_ascii_lowercase()),
            "leaked: {head}"
        );
    }

    /// The same guarantee for GCS: no credentials in the request means an unsigned
    /// request, never the service's own identity from the environment or the metadata
    /// server.
    #[tokio::test]
    async fn a_gcs_request_without_credentials_is_unsigned() {
        let head = request_head("gs://bucket/key.parquet", serde_json::json!({})).await;
        assert!(head.contains("key.parquet"), "{head}");
        assert!(!head.contains("authorization:"), "signed anyway: {head}");
    }

    /// And the credential the caller sent is the one that signs. An access token is the
    /// one GCS credential that signs without asking Google for anything first, which is
    /// why it is the one on the wire here.
    #[tokio::test]
    async fn a_gcs_access_token_is_the_one_that_signs() {
        let head = request_head(
            "gs://bucket/key.parquet",
            serde_json::json!({"access_token": "ya29.token-value", "allow_http": true}),
        )
        .await;
        assert!(
            head.contains("authorization: bearer ya29.token-value"),
            "{head}"
        );
    }

    #[tokio::test]
    async fn an_azure_request_without_credentials_is_unsigned() {
        let head = request_head(
            "az://container/key.parquet",
            serde_json::json!({"account": "hatsdata"}),
        )
        .await;
        assert!(head.contains("container/key.parquet"), "{head}");
        assert!(!head.contains("authorization:"), "signed anyway: {head}");
    }

    /// Azure's shared key is an HMAC computed here, so the key itself never goes on the
    /// wire — the account name and the signature do.
    #[tokio::test]
    async fn azure_credentials_from_the_request_are_the_ones_that_sign() {
        let head = request_head(
            "az://container/key.parquet",
            serde_json::json!({
                "account": "hatsdata",
                // Azure account keys are base64, and OpenDAL refuses one that is not.
                "access_key": AZURE_KEY,
                "allow_http": true,
            }),
        )
        .await;
        assert!(
            head.contains("authorization: sharedkey hatsdata:"),
            "{head}"
        );
        assert!(
            !head.contains(&AZURE_KEY.to_ascii_lowercase()),
            "leaked: {head}"
        );
    }

    /// A SAS token authenticates by riding in the query string, which is the one place
    /// a credential is meant to be on the wire.
    #[tokio::test]
    async fn an_azure_sas_token_is_sent_as_the_query_string() {
        let head = request_head(
            "az://container/key.parquet",
            serde_json::json!({
                "account": "hatsdata",
                "sas_token": "sv=2021-06-08&sig=abc",
                "allow_http": true,
            }),
        )
        .await;
        assert!(head.contains("sig=abc"), "the token was not sent: {head}");
        assert!(!head.contains("authorization:"), "{head}");
    }

    /// The stores are built on the access policy's own HTTP transport, whose resolver is
    /// the network policy. A store on OpenDAL's process-wide default client would
    /// resolve and connect to whatever it was handed, so this checks that a name a store
    /// is pointed at goes through a resolver that works — the refusing half is
    /// [`crate::network`]'s own test.
    #[tokio::test]
    async fn a_store_reaches_a_named_host_through_the_policys_own_resolver() {
        let (port, receiver) = capture_one_request();
        let url = parse_url("s3://bucket/key.parquet").unwrap();
        let endpoint = options(serde_json::json!({
            "endpoint": format!("http://localhost:{port}"),
        }));
        let file = open_loopback(&url, &endpoint).unwrap();

        use object_store::ObjectStoreExt;
        let _ = file
            .store
            .get(&object_store::path::Path::from("key.parquet"))
            .await;
        let head = receiver
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the store made no request");
        assert!(head.contains("/bucket/key.parquet"), "{head}");
    }

    /// A redirect is the origin picking the next destination, and the next destination
    /// is the one thing only the config gets to pick: the hop would carry the caller's
    /// credentials to a host no endpoint rule named.
    #[tokio::test]
    async fn a_redirect_is_not_followed() {
        let (elsewhere, never_reached) = capture_one_request();
        let (port, _redirected) = serve_one(format!(
            "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{elsewhere}/bucket/key.parquet\r\n\
             Content-Length: 0\r\n\r\n"
        ));

        let url = parse_url("s3://bucket/key.parquet").unwrap();
        let endpoint = options(serde_json::json!({
            "endpoint": format!("http://127.0.0.1:{port}"),
        }));
        let file = open_loopback(&url, &endpoint).unwrap();

        use object_store::ObjectStoreExt;
        let error = file
            .store
            .get(&object_store::path::Path::from("key.parquet"))
            .await
            .expect_err("a 302 is not a response the store can read");
        assert!(!format!("{error}").is_empty());
        assert!(
            never_reached
                .recv_timeout(std::time::Duration::from_secs(2))
                .is_err(),
            "the redirect was followed"
        );
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
    fn store_key(file: &RemoteFile) -> String {
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

    #[test]
    fn a_webdav_url_defaults_to_https() {
        let url = parse_url("webdav://data.example.com/hats/part0.parquet").unwrap();
        let file = open(&url, &no_options()).unwrap();
        assert_eq!(store_key(&file), "webdav://data.example.com");
        assert_eq!(
            webdav_endpoint(&url, &no_options()).unwrap().as_str(),
            "https://data.example.com/"
        );
    }

    #[test]
    fn webdav_http_is_explicit_and_needs_a_matching_endpoint_rule() {
        let url = parse_url("webdav://data.example.com/hats/part0.parquet").unwrap();
        let cleartext = options(serde_json::json!({"transport": "http"}));
        let error = open(&url, &cleartext).unwrap_err();
        assert!(matches!(error, ApiError::Forbidden(_)), "{error}");
        assert!(
            error.to_string().contains("access.webdav.endpoints"),
            "{error}"
        );

        let policy = AccessPolicy::new(
            &crate::config::AccessConfig {
                webdav: crate::config::EndpointConfig {
                    endpoints: Some(vec!["http://data.example.com".to_owned()]),
                },
                ..Default::default()
            },
            Arc::default(),
        )
        .unwrap();
        assert!(super::open(&url, &cleartext, &policy, &transfers()).is_ok());
        assert_eq!(
            webdav_endpoint(&url, &cleartext).unwrap().as_str(),
            "http://data.example.com/"
        );
    }

    #[test]
    fn webdav_basic_auth_requires_both_values_and_uses_no_generic_headers() {
        let url = parse_url("webdav://data.example.com/hats/part0.parquet").unwrap();
        let username_only = options(serde_json::json!({"username": "reader"}));
        let error = open(&url, &username_only).unwrap_err();
        assert!(
            error.to_string().contains("username and password"),
            "{error}"
        );

        let with_headers = options(serde_json::json!({"headers": {"X-Test": "no"}}));
        let error = open(&url, &with_headers).unwrap_err();
        assert!(error.to_string().contains("they take"), "{error}");

        let basic = options(serde_json::json!({
            "username": "reader",
            "password": SECRET,
        }));
        let headers = webdav_headers(&basic).unwrap();
        assert!(headers.contains_key(http::header::AUTHORIZATION));
        assert!(!format!("{headers:?}").contains(SECRET));
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

    /// The url is its own server, so there is nothing for `endpoint` to point elsewhere
    /// at and no bucket-backend option that means anything here. What it does take is
    /// `headers`, and `allow_http` to say those may go over cleartext.
    #[test]
    fn an_http_url_takes_only_headers_and_the_cleartext_flag() {
        let url = parse_url("https://data.example.com/part0.parquet").unwrap();
        for option in [
            serde_json::json!({"endpoint": "https://elsewhere.example.com"}),
            serde_json::json!({"region": "us-west-2"}),
            serde_json::json!({"secret_access_key": SECRET}),
            serde_json::json!({"account": "hatsdata"}),
        ] {
            let error = open(&url, &options(option.clone())).unwrap_err();
            assert!(
                matches!(error, ApiError::BadRequest(_)),
                "{option}: {error}"
            );
            assert!(error.to_string().contains("they take"), "{option}: {error}");
            assert!(!error.to_string().contains(SECRET), "leaked: {error}");
        }

        assert!(open(&url, &options(serde_json::json!({"allow_http": true}))).is_ok());
        assert!(
            open(
                &url,
                &options(serde_json::json!({"headers": {"Authorization": "Bearer t"}})),
            )
            .is_ok()
        );
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
    fn a_cleartext_http_url_needs_the_server_to_allow_it() {
        let url = parse_url("http://data.example.com/part0.parquet").unwrap();
        let error = open(&url, &no_options()).unwrap_err();
        assert!(matches!(error, ApiError::Forbidden(_)), "{error}");
        assert!(error.to_string().contains("allow_plain_http"), "{error}");
    }

    /// The whole path for the new backend: a real server, a real ranged `GET` through
    /// the store, and no identity of ours on it.
    #[tokio::test]
    async fn an_http_request_carries_no_credentials_and_asks_for_the_key() {
        let (port, receiver) = capture_one_request();
        let config = crate::config::AccessConfig {
            network: crate::config::NetworkConfig {
                allow_loopback: true,
                ..Default::default()
            },
            http: crate::config::HttpConfig {
                endpoints: None,
                allow_plain_http: true,
            },
            ..Default::default()
        };
        let policy = AccessPolicy::new(&config, Arc::default()).unwrap();
        let url = parse_url(&format!("http://127.0.0.1:{port}/hats/part0.parquet")).unwrap();
        let file = super::open(&url, &no_options(), &policy, &transfers()).unwrap();
        assert_eq!(store_key(&file), format!("http://127.0.0.1:{port}"));

        use object_store::ObjectStoreExt;
        let _ = file
            .store
            .get(&object_store::path::Path::from("hats/part0.parquet"))
            .await;
        let head = receiver
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the store made no request")
            .to_ascii_lowercase();
        // One slash, not two: the endpoint has the trailing one trimmed off it.
        assert!(head.contains("get /hats/part0.parquet"), "{head}");
        assert!(!head.contains("authorization:"), "signed anyway: {head}");
    }

    /// A token the caller gave goes on the request the store makes. Checked on the wire
    /// rather than on the builder, since the builder has nowhere to put one — the
    /// headers ride on a transport wrapped around this operator alone.
    #[tokio::test]
    async fn caller_headers_are_sent_to_an_http_server() {
        let heads = http_request_heads(serde_json::json!({
            "headers": {"Authorization": "Bearer secret-token", "X-Api-Key": "key-value"},
            "allow_http": true,
        }))
        .await;
        // Both requests: the probe, which asks for a suffix range, and the store's own
        // read afterwards. A server that authenticates would refuse either one alone.
        let requests: Vec<&str> = heads.split("--- next request ---").collect();
        assert_eq!(requests.len(), 2, "{heads}");
        for request in &requests {
            assert!(
                request.contains("authorization: bearer secret-token"),
                "a request went out without the caller's token: {request}"
            );
            assert!(request.contains("x-api-key: key-value"), "{request}");
        }
        // And the two really are the probe and the read, not the same one twice.
        assert!(heads.contains("range: bytes=-8"), "{heads}");
    }

    /// And a store built without them sends none, so the headers above came from the
    /// option rather than from anything ambient.
    #[tokio::test]
    async fn no_headers_are_sent_when_the_caller_gave_none() {
        let heads = http_request_heads(serde_json::json!({"allow_http": true})).await;
        assert!(!heads.contains("authorization:"), "{heads}");
        assert!(!heads.contains("x-api-key:"), "{heads}");
    }

    /// The headers are one caller's credentials, so they must not reach a store built
    /// for another. The wrapper is per operator; this is what says so.
    #[tokio::test]
    async fn one_callers_headers_do_not_reach_another_callers_store() {
        let with_token = http_request_heads(serde_json::json!({
            "headers": {"Authorization": "Bearer secret-token"},
            "allow_http": true,
        }))
        .await;
        assert!(with_token.contains("bearer secret-token"), "{with_token}");

        // A second store, built afterwards from the same policy and the same shared
        // transport underneath it.
        let without = http_request_heads(serde_json::json!({"allow_http": true})).await;
        assert!(
            !without.contains("secret-token"),
            "a previous caller's token leaked into another store: {without}"
        );
    }

    /// Headers this service decides for itself, and headers that describe a connection
    /// rather than a request. Refused rather than dropped: a caller whose header does
    /// not arrive has been told their request is authenticated when it is not.
    #[test]
    fn headers_the_service_owns_cannot_be_set_by_a_caller() {
        let url = parse_url("https://data.example.com/k.parquet").unwrap();
        for (name, expected) in [
            ("Host", "names the server"),
            ("Range", "ranged one"),
            // Set by the backend on a read, so a caller's copy would replace it.
            ("If-Match", "304"),
            ("If-None-Match", "304"),
            ("If-Modified-Since", "304"),
            ("If-Unmodified-Since", "304"),
            // A gzipped body has different offsets from the object it encodes.
            ("Accept-Encoding", "wrong bytes"),
            ("Content-Length", "describes the connection"),
            ("Transfer-Encoding", "describes the connection"),
            ("Connection", "describes the connection"),
            ("Keep-Alive", "describes the connection"),
            ("TE", "describes the connection"),
            ("Upgrade", "describes the connection"),
        ] {
            let option = serde_json::json!({"headers": {name: "whatever"}});
            let error = open(&url, &options(option)).unwrap_err();
            assert!(matches!(error, ApiError::BadRequest(_)), "{name}: {error}");
            assert!(error.to_string().contains(expected), "{name}: {error}");
            // Case is not a distinction a header name makes, and the refusal must not
            // depend on how the caller spelled it.
            let shouted = serde_json::json!({"headers": {name.to_uppercase(): "whatever"}});
            assert!(
                open(&url, &options(shouted)).is_err(),
                "{name} in upper case"
            );
        }

        // And the list is not so wide that it refuses what the option is for. These are
        // the headers a service actually authenticates with.
        for name in ["Authorization", "X-Api-Key", "Cookie", "X-Auth-Token"] {
            let option = serde_json::json!({"headers": {name: "value"}});
            assert!(open(&url, &options(option)).is_ok(), "{name} was refused");
        }
    }

    #[test]
    fn a_header_that_is_not_one_is_refused_without_echoing_its_value() {
        let url = parse_url("https://data.example.com/k.parquet").unwrap();
        let bad_name = serde_json::json!({"headers": {"not a header": SECRET}});
        let error = open(&url, &options(bad_name)).unwrap_err();
        assert!(
            error.to_string().contains("not a valid header name"),
            "{error}"
        );
        assert!(!error.to_string().contains(SECRET), "leaked: {error}");

        // A newline in the value is header injection, and the message must not quote it
        // back either.
        let bad_value =
            serde_json::json!({"headers": {"X-Token": format!("{SECRET}\r\nX-Evil: 1")}});
        let error = open(&url, &options(bad_value)).unwrap_err();
        assert!(error.to_string().contains("cannot carry"), "{error}");
        assert!(!error.to_string().contains(SECRET), "leaked: {error}");
    }

    /// A token over cleartext is the caller's own secret in the open, so it needs their
    /// say-so — the operator's `allow_plain_http` is a different question and answering
    /// it does not answer this one.
    #[test]
    fn headers_over_cleartext_need_the_caller_to_say_so() {
        let url = parse_url("http://data.example.com/k.parquet").unwrap();
        let policy = policy_allowing_plain_http();
        let with_token = options(serde_json::json!({
            "headers": {"Authorization": format!("Bearer {SECRET}")},
        }));

        let error = super::open(&url, &with_token, &policy, &transfers()).unwrap_err();
        assert!(error.to_string().contains("cleartext"), "{error}");
        assert!(error.to_string().contains("allow_http"), "{error}");
        assert!(!error.to_string().contains(SECRET), "leaked: {error}");

        let allowed = options(serde_json::json!({
            "headers": {"Authorization": format!("Bearer {SECRET}")},
            "allow_http": true,
        }));
        assert!(super::open(&url, &allowed, &policy, &transfers()).is_ok());

        // And with no headers there is no secret to protect, so cleartext is fine.
        let anonymous = options(serde_json::json!({}));
        assert!(super::open(&url, &anonymous, &policy, &transfers()).is_ok());
    }

    /// Neither half of a header is printed. Both come from the caller, and a token in
    /// the name is a mistake that would still land in this service's log.
    #[test]
    fn debug_output_carries_no_header_name_or_value() {
        let with_headers = options(serde_json::json!({
            "headers": {SECRET: format!("Bearer {SECRET}")},
        }));
        let shown = format!("{with_headers:?}");
        assert!(!shown.contains(SECRET), "leaked: {shown}");
        // Still says there were some, which is what a reader needs to know.
        assert!(shown.contains("1 header(s)"), "{shown}");

        let none = format!("{:?}", no_options());
        assert!(none.contains("0 header(s)"), "{none}");
    }

    /// A policy that allows the loopback interface and cleartext, which is what a test
    /// server on `127.0.0.1` needs.
    fn policy_allowing_plain_http() -> AccessPolicy {
        AccessPolicy::new(
            &crate::config::AccessConfig {
                network: crate::config::NetworkConfig {
                    allow_loopback: true,
                    ..Default::default()
                },
                http: crate::config::HttpConfig {
                    endpoints: None,
                    allow_plain_http: true,
                },
                ..Default::default()
            },
            Arc::default(),
        )
        .unwrap()
    }

    /// Every request head one read through an `http://` store puts on the wire,
    /// lowercased and joined.
    ///
    /// There are two, and both have to be checked. `materialize` probes with a request of
    /// its own before the store makes any, and the two get their headers by different
    /// routes — the probe from the client call, the store's from the transport wrapped
    /// around its operator. Capturing only the first would pass with the wrapper missing
    /// entirely.
    async fn http_request_heads(mut option: serde_json::Value) -> String {
        let (port, receiver) = capture_requests(2);
        let url = parse_url(&format!("http://127.0.0.1:{port}/key.parquet")).unwrap();
        if option.get("allow_http").is_none() {
            option["allow_http"] = serde_json::json!(true);
        }
        let file = super::open(
            &url,
            &options(option),
            &policy_allowing_plain_http(),
            &transfers(),
        )
        .unwrap();

        use object_store::ObjectStoreExt;
        let _ = file
            .store
            .get(&object_store::path::Path::from("key.parquet"))
            .await;
        let mut heads = Vec::new();
        for _ in 0..2 {
            heads.push(
                receiver
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .expect("the store made too few requests")
                    .to_ascii_lowercase(),
            );
        }
        heads.join("\n--- next request ---\n")
    }

    /// A server that answers `count` requests with a 404 and hands each head back. Each
    /// request gets its own connection, which is what the store does anyway once the
    /// first answer closes.
    fn capture_requests(count: usize) -> (u16, std::sync::mpsc::Receiver<String>) {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for _ in 0..count {
                let Ok((mut stream, _)) = listener.accept() else {
                    return;
                };
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    match stream.read(&mut byte) {
                        Ok(1) => head.push(byte[0]),
                        _ => break,
                    }
                }
                // `Connection: close` so the store opens a fresh one for the next
                // request rather than reusing this and never being accepted again.
                let _ = stream.write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                );
                let _ = sender.send(String::from_utf8_lossy(&head).into_owned());
            }
        });
        (port, receiver)
    }

    #[test]
    fn keeps_equals_signs_in_hats_paths() {
        let url = parse_url("s3://b/hats/Norder=5/Npix=12240/part0.parquet").unwrap();
        let file = open(&url, &no_options()).unwrap();
        assert_eq!(file.url.path(), url.path());
        assert!(file.url.path().contains("Norder=5"), "{}", file.url.path());
    }
}
