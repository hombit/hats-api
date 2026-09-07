//! What a request is allowed to read.
//!
//! The service takes the location of the data from the caller, so without a policy it
//! would read anything the machine it runs on can reach — every S3 server on the
//! network, every file on disk, and whatever is listening on localhost. [`AccessPolicy`]
//! is what it may read instead, built once at startup from `[access]` in the config.
//!
//! For an object store the thing worth deciding is **which endpoint** may be contacted,
//! not which bucket may be read: the bucket name says nothing about the host, since a
//! request carries its own `endpoint` option and can point the service anywhere. So the
//! rules are endpoints, and any bucket at an allowed endpoint is readable.
//!
//! Every remote backend has the same three-state list, under its own section. Most of
//! them also have a name for "the provider's own service", which is what a url carrying
//! no `endpoint` option means. `http(s)://` is the exception: its url names the server
//! outright, so there is no endpoint option to default and no provider to default to —
//! only the list, and whether cleartext is acceptable when there is no list.
//!
//! Which *address* a request ends up reaching is a second question, and one the endpoint
//! rules cannot answer: a name allowed here may still resolve onto the network this
//! process happens to sit on. That is [`crate::network`]'s, under `[access.network]`.
//!
//! ```toml
//! [access.network]
//! allow_loopback = false
//!
//! [access.s3]
//! # "aws" is AWS S3 itself: a url with no `endpoint` option of its own.
//! endpoints = ["aws", "https://minio.example.com"]
//!
//! [access.gcs]
//! endpoints = ["gcp"]
//!
//! [access.azure]
//! endpoints = ["azure"]
//!
//! [access.http]
//! # No provider name: an http(s) url names its own server, so every entry is a url.
//! endpoints = ["https://data.example.com"]
//! allow_plain_http = false
//!
//! [access.local]
//! paths = ["/srv/hats"]
//! follow_symlinks = false
//! ```
//!
//! Local paths get two checks an endpoint does not need, because a filesystem has ways
//! of pointing outside itself. A path is resolved before it is matched, so neither `..`
//! nor a symlink inside an allowed directory can lead out of one; and unless
//! `follow_symlinks` is on, a path that goes through a symlink at all is refused.

use std::path::{Component, Path, PathBuf};

use url::{Host, Url};

use crate::config::{AccessConfig, ConfigError};
use crate::error::ApiError;
use crate::mount::{Mount, Mounts};
use crate::network::NetworkPolicy;

/// What a URL turned out to be, once it was allowed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// Read it through this backend's object store. Carrying the backend rather than
    /// leaving the caller to work it out again from the scheme is what makes the match
    /// on the other side exhaustive: a backend with no arm is a compile error instead of
    /// a scheme that got this far and then had nothing to open it with.
    ///
    /// Not the whole answer for a remote backend: the endpoint is only known once the
    /// url's options are parsed, so [`AccessPolicy::authorize_endpoint`] is the second
    /// half.
    Remote(Backend),
    /// Read this local file. Absolute, with every symlink already resolved and the
    /// result checked against the policy.
    Local(PathBuf),
}

/// A remote backend, which is to say a scheme with an endpoint behind it. `file` is not
/// one: it has no host to decide about, and the directory rules are its whole policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    S3,
    Gcs,
    Azure,
    Http,
    Webdav,
}

/// Every remote backend, so that a caller listing or looping over them cannot miss one
/// a later phase adds.
pub const BACKENDS: &[Backend] = &[
    Backend::S3,
    Backend::Gcs,
    Backend::Azure,
    Backend::Http,
    Backend::Webdav,
];

/// The one scheme with no [`Backend`] behind it, named here because it is the other half
/// of that enum rather than a string three places happen to agree on.
pub const LOCAL_SCHEME: &str = "file";

impl Backend {
    /// The url schemes that name this backend. More than one where the scheme is also
    /// the transport: `http` and `https` are one backend reached two ways, and which of
    /// the two a request used is [`AccessPolicy`]'s business rather than a separate
    /// backend's.
    pub fn schemes(self) -> &'static [&'static str] {
        match self {
            Self::S3 => &["s3"],
            Self::Gcs => &["gs"],
            Self::Azure => &["az"],
            Self::Http => &["http", "https"],
            Self::Webdav => &["webdav"],
        }
    }

    pub fn from_scheme(scheme: &str) -> Option<Self> {
        BACKENDS
            .iter()
            .copied()
            .find(|backend| backend.schemes().contains(&scheme))
    }

    /// What an endpoint entry says to mean the provider's own service, which is the
    /// endpoint a url with no `endpoint` option is asking for.
    ///
    /// `None` for a backend whose url names the server itself. That is one property with
    /// several consequences — no default server to fall back to, no `endpoint` option to
    /// override it with, and no provider name an endpoint list can hold — so it is asked
    /// once here rather than decided again at each of them.
    fn provider(self) -> Option<&'static str> {
        match self {
            Self::S3 => Some("aws"),
            Self::Gcs => Some("gcp"),
            Self::Azure => Some("azure"),
            // `https://data.example.com/x.parquet` says which server as plainly as a url
            // can. There is nothing left for an option to name.
            Self::Http => None,
            Self::Webdav => None,
        }
    }

    /// Whether the url names a bucket at a server the request may choose, rather than
    /// naming the server itself. The two shapes differ in more than addressing: only the
    /// first has an `endpoint` option, a provider to default to, or a cleartext decision
    /// that belongs to the caller.
    pub fn has_provider(self) -> bool {
        self.provider().is_some()
    }

    /// The endpoint a request means when it names none, which only a backend with a
    /// provider has.
    fn default_endpoint(self) -> Option<Endpoint> {
        self.provider().map(Endpoint::Provider)
    }

    /// The config section its rules live in, for saying where to change them.
    fn section(self) -> &'static str {
        match self {
            Self::S3 => "access.s3",
            Self::Gcs => "access.gcs",
            Self::Azure => "access.azure",
            Self::Http => "access.http",
            Self::Webdav => "access.webdav",
        }
    }
}

#[derive(Debug)]
pub struct AccessPolicy {
    s3: EndpointRules,
    gcs: EndpointRules,
    azure: EndpointRules,
    http: EndpointRules,
    webdav: EndpointRules,
    /// Whether an `http://` url may be read when the http rules name no endpoints. A
    /// decision of the operator's rather than the caller's: the caller sends no
    /// credential to an `http(s)://` url, so what cleartext costs here is not a secret
    /// but the assurance that the bytes came from the host the url names — and only the
    /// operator knows whether the deployment's network makes that acceptable.
    allow_plain_http: bool,
    /// Allowed directories, canonical, so that a resolved request path can simply be
    /// tested for being under one of them.
    local: Vec<LocalRule>,
    network: NetworkPolicy,
}

/// One allowed directory and the resolution rules that come with it.
///
/// The rules are per directory rather than one switch over all of them because a mount
/// brings its own: publishing a directory of symlinks must not decide the question for
/// every other directory this policy allows.
#[derive(Debug)]
struct LocalRule {
    root: PathBuf,
    follow_symlinks: bool,
}

#[derive(Debug)]
enum EndpointRules {
    /// No list was configured: any endpoint, subject to the network rules.
    Any,
    /// Exactly these, and an empty list turns the backend off. An entry here is the
    /// operator naming a host, so the network rules have nothing left to decide.
    Only(Vec<Endpoint>),
}

/// The schemes an endpoint can have: it is an HTTP service whatever the storage behind
/// it is. An enum rather than a string because `http` is a decision — it is the one that
/// puts a credential where anything on the path can read it — and a decision compared by
/// spelling is one that a typo silently reverses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointScheme {
    Http,
    Https,
}

impl EndpointScheme {
    pub const ALL: [Self; 2] = [Self::Http, Self::Https];

    pub fn name(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }

    pub fn parse(scheme: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|known| known.name() == scheme)
    }

    /// Whether what goes over it can be read by anything on the path.
    pub fn is_cleartext(self) -> bool {
        matches!(self, Self::Http)
    }
}

impl std::fmt::Display for EndpointScheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Endpoint {
    /// The provider's own service, which is what a url with no `endpoint` option means.
    /// Built only from [`Backend::provider`], never from the entry as written, so a
    /// refusal cannot quote a spelling the parse did not accept — and a backend that has
    /// no provider has no way to reach this variant at all.
    Provider(&'static str),
    /// One server speaking the backend's protocol. The port is kept resolved so that
    /// `https://host` and `https://host:443` are the one endpoint they are.
    Url {
        scheme: EndpointScheme,
        host: Host<String>,
        port: Option<u16>,
    },
}

impl Default for AccessPolicy {
    #[expect(
        clippy::expect_used,
        reason = "the default config names no endpoint and no directory, so there is \
                  no rule for `new` to reject; a panic here would be a bug in this file"
    )]
    fn default() -> Self {
        Self::new(&AccessConfig::default(), &Mounts::default())
            .expect("the default access config is valid")
    }
}

impl AccessPolicy {
    /// The rules as configured, plus what the mounts grant.
    ///
    /// A mount is passed in rather than looked up later because the grant is not
    /// optional: the bytes under a mount are already served whole over its own route, so
    /// refusing to query them through the API would withhold nothing. Taking the mounts
    /// as an argument is what makes that a thing this function does rather than a thing
    /// each caller has to remember.
    pub fn new(config: &AccessConfig, mounts: &Mounts) -> Result<Self, ConfigError> {
        // Every host the operator named, collected as the rules are built rather than by
        // walking them again afterwards: a second pass would be a second list of
        // backends to keep in step, and a backend missing from it would have its own
        // configured endpoint refused by the network rules.
        //
        // Naming a host is what puts it here, and that is the point — the network rules
        // govern what a caller may point the service at, not what the deployment was set
        // up for. A MinIO on RFC1918 space must not need saying twice.
        let mut named: Vec<Host<String>> = Vec::new();
        let mut build = |endpoints, backend| -> Result<EndpointRules, ConfigError> {
            let rules = EndpointRules::new(endpoints, backend)?;
            named.extend(rules.hosts());
            Ok(rules)
        };
        let s3 = build(&config.s3.endpoints, Backend::S3)?;
        let gcs = build(&config.gcs.endpoints, Backend::Gcs)?;
        let azure = build(&config.azure.endpoints, Backend::Azure)?;
        let http = build(&config.http.endpoints, Backend::Http)?;
        let webdav = build(&config.webdav.endpoints, Backend::Webdav)?;

        let network = NetworkPolicy::new(&config.network, &named)?;
        let mut local = config
            .local
            .paths
            .iter()
            // Resolved now so that startup fails on a directory that is not there,
            // rather than every request failing later for a reason nobody can see.
            .map(|entry| {
                canonical_root(entry)
                    .map(|root| LocalRule {
                        root,
                        follow_symlinks: config.local.follow_symlinks,
                    })
                    .map_err(|reason| ConfigError::Rule(entry.to_owned(), reason))
            })
            .collect::<Result<Vec<_>, _>>()?;
        // Scoped to what the mount publishes and no wider, and under the mount's own
        // resolution rules, so both routes agree about the same file.
        local.extend(mounts.iter().map(|mount| LocalRule {
            root: mount.source().to_owned(),
            follow_symlinks: mount.follow_symlinks(),
        }));
        Ok(Self {
            s3,
            gcs,
            azure,
            http,
            webdav,
            allow_plain_http: config.http.allow_plain_http,
            local,
            network,
        })
    }

    /// The destination rules, and the HTTP transport built from them.
    pub fn network(&self) -> &NetworkPolicy {
        &self.network
    }

    /// Every scheme this policy can serve, for logs and error messages.
    pub fn allowed_schemes(&self) -> Vec<&'static str> {
        let mut schemes: Vec<&'static str> = BACKENDS
            .iter()
            .copied()
            .filter(|backend| self.rules(*backend).enabled())
            .flat_map(Backend::schemes)
            .copied()
            // A backend's schemes are otherwise all served or all not; `http` is the one
            // that has a second decision behind it, and claiming to read a scheme that
            // every url in gets refused would send a caller looking for the wrong rule.
            .filter(|scheme| *scheme != EndpointScheme::Http.name() || self.reads_cleartext_http())
            .collect();
        if !self.local.is_empty() {
            schemes.push(LOCAL_SCHEME);
        }
        schemes
    }

    /// Whether any `http://` url could be read. With a list, the operator writing a
    /// cleartext entry in it is the decision; without one, `allow_plain_http` is.
    fn reads_cleartext_http(&self) -> bool {
        match &self.http {
            EndpointRules::Any => self.allow_plain_http,
            EndpointRules::Only(endpoints) => endpoints.iter().any(|endpoint| {
                matches!(
                    endpoint,
                    Endpoint::Url {
                        scheme: EndpointScheme::Http,
                        ..
                    }
                )
            }),
        }
    }

    fn rules(&self, backend: Backend) -> &EndpointRules {
        match backend {
            Backend::S3 => &self.s3,
            Backend::Gcs => &self.gcs,
            Backend::Azure => &self.azure,
            Backend::Http => &self.http,
            Backend::Webdav => &self.webdav,
        }
    }

    /// The first gate: is this the *kind* of thing the service reads at all? For a
    /// local file that settles it. For a remote backend the endpoint is still to come,
    /// because it lives in the url's options and only `storage` knows how to read those.
    pub fn authorize(&self, url: &Url) -> Result<Target, ApiError> {
        let scheme = url.scheme();
        if scheme == LOCAL_SCHEME {
            return self.authorize_local(url).map(Target::Local);
        }
        match Backend::from_scheme(scheme) {
            Some(backend) if self.rules(backend).enabled() => Ok(Target::Remote(backend)),
            _ => Err(ApiError::forbidden(format!(
                "this server does not read {scheme}:// urls; it reads {}",
                self.describe_schemes()
            ))),
        }
    }

    /// The second gate for a remote backend: which server the request would have us
    /// talk to. `None` is a request with no `endpoint` option, which means the
    /// provider's own service.
    pub fn authorize_endpoint(
        &self,
        backend: Backend,
        endpoint: Option<&Url>,
    ) -> Result<(), ApiError> {
        match self.rules(backend) {
            // An unrestricted policy still has a destination to decide about: the
            // service can reach a great deal that its callers cannot.
            EndpointRules::Any => {
                let Some(url) = endpoint else { return Ok(()) };
                self.refuse_unwanted_cleartext(backend, url)?;
                match url.host() {
                    Some(host) => self.network.authorize_host(&host),
                    None => Ok(()),
                }
            }
            EndpointRules::Only(allowed) => {
                let wanted = match endpoint {
                    Some(url) => Endpoint::from_url(url)?,
                    None => backend.default_endpoint().ok_or_else(|| {
                        ApiError::bad_request(format!(
                            "a {} url names its own server, so there is no default \
                             endpoint to ask about",
                            backend.schemes().join(" or ")
                        ))
                    })?,
                };
                match allowed.contains(&wanted) {
                    true => Ok(()),
                    false => Err(ApiError::forbidden(format!(
                        "{wanted} is not an endpoint this server will contact; {} \
                         contacts {}",
                        backend.section(),
                        describe(allowed)
                    ))),
                }
            }
        }
    }

    /// The cleartext half, for a backend whose url is its own endpoint. Only under
    /// [`EndpointRules::Any`]: an operator who wrote an `http://` entry in a list has
    /// already made this decision for that server, the same way naming an endpoint
    /// settles the network rules for it.
    ///
    /// The bucket-addressed backends do not come through here. Their cleartext decision
    /// is about a credential the caller attached, which only `storage` can see, and it is
    /// the caller's to make with `allow_http` — there is nothing for the operator to
    /// decide about a secret that is not theirs.
    fn refuse_unwanted_cleartext(&self, backend: Backend, endpoint: &Url) -> Result<(), ApiError> {
        let cleartext =
            EndpointScheme::parse(endpoint.scheme()).is_some_and(EndpointScheme::is_cleartext);
        if backend.provider().is_some() || !cleartext {
            return Ok(());
        }
        if backend == Backend::Http && self.allow_plain_http {
            return Ok(());
        }
        let change = match backend {
            Backend::Http => format!(
                "set {}.allow_plain_http, or name the server in {}.endpoints",
                backend.section(),
                backend.section()
            ),
            Backend::Webdav => format!("name the server in {}.endpoints", backend.section()),
            _ => unreachable!("provider-backed backends returned above"),
        };
        Err(ApiError::forbidden(format!(
            "{endpoint} is cleartext http, so nothing guarantees the bytes came from \
             the host it names; {change}, to change that"
        )))
    }

    fn authorize_local(&self, url: &Url) -> Result<PathBuf, ApiError> {
        if self.local.is_empty() {
            return Err(ApiError::forbidden(
                "this server reads no local files; add a directory to \
                 api.access.local.paths, or mount one, to change that",
            ));
        }
        let path = url
            .to_file_path()
            .map_err(|()| ApiError::bad_request(format!("url {url} is not a local path")))?;
        let lexical = lexically_clean(&path)
            .ok_or_else(|| ApiError::bad_request(format!("path {} escapes /", path.display())))?;

        resolve_local(&self.local, &lexical).map_err(|refusal| match refusal {
            LocalRefusal::NotAllowed => self.local_refusal(url),
            LocalRefusal::Symlink(path) => ApiError::forbidden(format!(
                "{} goes through a symlink, which this server does not follow; set \
                 api.access.local.follow_symlinks, or the mount's own, to change that",
                path.display()
            )),
            LocalRefusal::NotFound(path) => {
                ApiError::not_found(format!("{} does not exist", path.display()))
            }
            LocalRefusal::Unreadable(path, error) => {
                ApiError::forbidden(format!("cannot read {}: {error}", path.display()))
            }
        })
    }

    fn local_refusal(&self, url: &Url) -> ApiError {
        let roots: Vec<String> = self
            .local
            .iter()
            .map(|rule| rule.root.display().to_string())
            .collect();
        ApiError::forbidden(format!(
            "{url} is not under any allowed directory; this server reads {}",
            describe(&roots)
        ))
    }

    fn describe_schemes(&self) -> String {
        match self.allowed_schemes().as_slice() {
            [] => "nothing".to_owned(),
            schemes => schemes.join(", "),
        }
    }
}

impl EndpointRules {
    /// The list as the config spells it: absent is any endpoint, present is exactly
    /// those. Taking the list rather than the section it came from is what lets the http
    /// section carry a second key without every other backend growing one.
    fn new(endpoints: &Option<Vec<String>>, backend: Backend) -> Result<Self, ConfigError> {
        match endpoints {
            None => Ok(Self::Any),
            Some(entries) => entries
                .iter()
                .map(|entry| parse_endpoint(entry, backend))
                .collect::<Result<_, _>>()
                .map(Self::Only),
        }
    }

    /// An empty list is the operator turning the backend off, which is not the same as
    /// no list at all.
    fn enabled(&self) -> bool {
        !matches!(self, Self::Only(endpoints) if endpoints.is_empty())
    }

    /// The hosts the operator wrote here. A provider entry has none: it stands for the
    /// provider's own service, which is on the public internet by construction.
    fn hosts(&self) -> Vec<Host<String>> {
        match self {
            Self::Any => Vec::new(),
            Self::Only(endpoints) => endpoints
                .iter()
                .filter_map(|endpoint| match endpoint {
                    Endpoint::Provider(_) => None,
                    Endpoint::Url { host, .. } => Some(host.clone()),
                })
                .collect(),
        }
    }
}

impl Endpoint {
    fn from_url(url: &Url) -> Result<Self, ApiError> {
        let scheme = EndpointScheme::parse(url.scheme()).ok_or_else(|| {
            ApiError::bad_request(format!(
                "endpoint {url} has scheme {:?}, expected {}",
                url.scheme(),
                describe_endpoint_schemes()
            ))
        })?;
        let host = url
            .host()
            .filter(|host| !matches!(host, Host::Domain(name) if name.is_empty()))
            .ok_or_else(|| ApiError::bad_request(format!("endpoint {url} has no host")))?;
        Ok(Self::Url {
            scheme,
            // `Url` has already lowercased a domain and canonicalized an address, so
            // two spellings of one endpoint compare equal here.
            host: host.to_owned(),
            port: url.port_or_known_default(),
        })
    }
}

/// The endpoint schemes, for a refusal that has to name them.
pub fn describe_endpoint_schemes() -> String {
    EndpointScheme::ALL.map(EndpointScheme::name).join(" or ")
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Provider(name) => f.write_str(name),
            Self::Url { scheme, host, port } => {
                write!(f, "{scheme}://{host}")?;
                match port {
                    Some(port) => write!(f, ":{port}"),
                    None => Ok(()),
                }
            }
        }
    }
}

fn describe(items: &[impl std::fmt::Display]) -> String {
    match items.is_empty() {
        true => "nothing".to_owned(),
        false => items
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", "),
    }
}

fn parse_endpoint(entry: &str, backend: Backend) -> Result<Endpoint, ConfigError> {
    let invalid = |reason: String| ConfigError::Rule(entry.to_owned(), reason);

    let provider = backend.provider();
    if let Some(name) = provider.filter(|name| entry.eq_ignore_ascii_case(name)) {
        return Ok(Endpoint::Provider(name));
    }
    let url = Url::parse(entry).map_err(|error| {
        invalid(match provider {
            Some(provider) => {
                format!("{error}; expected {provider:?} or a url like https://minio.example.com")
            }
            None => format!("{error}; expected a url like https://data.example.com"),
        })
    })?;
    if EndpointScheme::parse(url.scheme()).is_none() {
        return Err(invalid(format!(
            "scheme {:?} is not one an endpoint can have; use {}",
            url.scheme(),
            describe_endpoint_schemes()
        )));
    }
    // An endpoint is a server, not a place in one: a path here would be silently
    // ignored, and quietly meaning less than it says is the one thing this file
    // must not do.
    if !matches!(url.path(), "" | "/") || url.query().is_some() || url.fragment().is_some() {
        return Err(invalid(
            "an endpoint is just a scheme and a host, with no path, query or fragment".to_owned(),
        ));
    }
    Endpoint::from_url(&url).map_err(|error| invalid(error.to_string()))
}

/// The directory an entry names, resolved: an absolute path or a `file://` url, and a
/// directory that is there now rather than a rule that silently never matches.
///
/// Returns the reason rather than a [`ConfigError`], because the entry means the same
/// thing in `[api.access.local]` and in a `[[mount]]` while the two name it differently,
/// and a message that says the wrong section is worse than one that says none.
pub(crate) fn canonical_root(entry: &str) -> Result<PathBuf, String> {
    let path = if entry.starts_with('/') {
        PathBuf::from(entry)
    } else {
        let url = Url::parse(entry)
            .map_err(|error| format!("{error}; expected an absolute path or a file:// url"))?;
        if url.scheme() != LOCAL_SCHEME {
            return Err(format!(
                "scheme {:?} is not a local path; expected an absolute path or a \
                 file:// url",
                url.scheme()
            ));
        }
        url.to_file_path()
            .map_err(|()| "not an absolute local path".to_owned())?
    };

    let canonical = std::fs::canonicalize(&path)
        .map_err(|error| format!("cannot resolve {}: {error}", path.display()))?;
    match canonical.is_dir() {
        true => Ok(canonical),
        false => Err(format!("{} is not a directory", canonical.display())),
    }
}

/// The file a path under a mount names, resolved under that mount's own rule and
/// nothing else — so one mount cannot serve a file out of another's directory, and a
/// mount cannot serve one out of a directory `[api.access.local]` happens to allow.
///
/// Every refusal is the same answer. A caller here named a place in the url space rather
/// than a file on a disk, so which file is missing, which is outside the mount and which
/// is behind a symlink are all distinctions about a filesystem they were never shown.
pub fn authorize_mounted(mount: &Mount, path: &Path) -> Result<PathBuf, ApiError> {
    let rules = [LocalRule {
        root: mount.source().to_owned(),
        follow_symlinks: mount.follow_symlinks(),
    }];
    resolve_local(&rules, path).map_err(|refusal| {
        // The reason belongs in the log, where the operator can see it, and not in the
        // response.
        tracing::debug!(mount = mount.prefix(), reason = %refusal, "not served");
        ApiError::not_found("no such file")
    })
}

/// Why a path is not a file that may be read. Separate from [`ApiError`] because how
/// much a refusal may say differs by mode: a caller who named the path is told which
/// directories exist, and one who walked into it through a mount is not.
enum LocalRefusal {
    /// Under none of the rules.
    NotAllowed,
    /// Under a rule, but nothing is there.
    NotFound(PathBuf),
    /// It goes through a symlink and the rule that governs it does not follow them.
    Symlink(PathBuf),
    Unreadable(PathBuf, std::io::Error),
}

/// For the log, which is the operator's. What a caller is told is decided where the
/// refusal is turned into an [`ApiError`].
impl std::fmt::Display for LocalRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAllowed => f.write_str("outside every allowed directory"),
            Self::NotFound(path) => write!(f, "{} does not exist", path.display()),
            Self::Symlink(path) => write!(f, "{} goes through a symlink", path.display()),
            Self::Unreadable(path, error) => write!(f, "cannot read {}: {error}", path.display()),
        }
    }
}

/// The file a path names, resolved and checked against the rules. `lexical` must already
/// be [`lexically_clean`], so that comparing it with the canonical path is a question
/// about symlinks and nothing else.
fn resolve_local(rules: &[LocalRule], lexical: &Path) -> Result<PathBuf, LocalRefusal> {
    // The rule the path was written under. Without symlink resolution the path as
    // written is the path that gets opened, so having none settles it before the
    // filesystem is touched at all — and a path outside every allowed directory then
    // gets the same answer whether or not it exists.
    let named = rules.iter().find(|rule| lexical.starts_with(&rule.root));
    if named.is_none() && !rules.iter().any(|rule| rule.follow_symlinks) {
        return Err(LocalRefusal::NotAllowed);
    }
    let canonical = std::fs::canonicalize(lexical).map_err(|error| match error.kind() {
        // Saying "no such file" about a path the caller was never allowed to name would
        // answer a question they did not get to ask.
        _ if named.is_none() => LocalRefusal::NotAllowed,
        std::io::ErrorKind::NotFound => LocalRefusal::NotFound(lexical.to_owned()),
        _ => LocalRefusal::Unreadable(lexical.to_owned(), error),
    })?;
    // Again on the resolved path: a link inside an allowed directory must still not
    // lead out of every allowed directory.
    let destination = rules
        .iter()
        .find(|rule| canonical.starts_with(&rule.root))
        .ok_or(LocalRefusal::NotAllowed)?;
    // The rule the path was written under is the one that decides about the link, so a
    // directory whose rules allow symlinks cannot become a way of following one out of
    // a directory whose rules do not.
    if !named.unwrap_or(destination).follow_symlinks && canonical != lexical {
        return Err(LocalRefusal::Symlink(lexical.to_owned()));
    }
    Ok(canonical)
}

/// Resolve `.` and `..` without touching the filesystem, so that the result can be
/// compared with the canonical path to tell whether a symlink was involved.
fn lexically_clean(path: &Path) -> Option<PathBuf> {
    let mut clean = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                // Above the root: there is nothing there to allow.
                if !clean.pop() {
                    return None;
                }
            }
            other => clean.push(other),
        }
    }
    Some(clean)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use crate::config::{EndpointConfig, HttpConfig, LocalConfig, NetworkConfig};

    use super::*;

    fn policy(config: &AccessConfig) -> AccessPolicy {
        AccessPolicy::new(config, &Mounts::default()).unwrap()
    }

    fn entries(entries: &[&str]) -> EndpointConfig {
        EndpointConfig {
            endpoints: Some(entries.iter().map(|e| (*e).to_owned()).collect()),
        }
    }

    fn with_endpoints(list: &[&str]) -> AccessPolicy {
        policy(&AccessConfig {
            s3: entries(list),
            ..Default::default()
        })
    }

    /// The same list under every backend's own section, for the tests that are about
    /// the rules rather than about one backend. Only for entries every section accepts:
    /// a provider's name is not one, since each section knows only its own.
    fn everywhere(list: &[&str]) -> AccessPolicy {
        policy(&AccessConfig {
            s3: entries(list),
            gcs: entries(list),
            azure: entries(list),
            http: HttpConfig {
                endpoints: entries(list).endpoints,
                allow_plain_http: false,
            },
            webdav: entries(list),
            ..Default::default()
        })
    }

    /// A list under one backend's section, the others left unrestricted.
    fn under(backend: Backend, list: &[&str]) -> AccessPolicy {
        let list = entries(list);
        policy(&match backend {
            Backend::S3 => AccessConfig {
                s3: list,
                ..Default::default()
            },
            Backend::Gcs => AccessConfig {
                gcs: list,
                ..Default::default()
            },
            Backend::Azure => AccessConfig {
                azure: list,
                ..Default::default()
            },
            Backend::Http => AccessConfig {
                http: HttpConfig {
                    endpoints: list.endpoints,
                    // The list is the operator naming servers, which settles cleartext
                    // for the ones in it; this switch is only about the case where
                    // there is no list.
                    allow_plain_http: false,
                },
                ..Default::default()
            },
            Backend::Webdav => AccessConfig {
                webdav: list,
                ..Default::default()
            },
        })
    }

    fn with_paths(paths: &[&Path], follow_symlinks: bool) -> AccessPolicy {
        policy(&AccessConfig {
            local: LocalConfig {
                paths: paths.iter().map(|p| p.display().to_string()).collect(),
                follow_symlinks,
            },
            ..Default::default()
        })
    }

    fn url(raw: &str) -> Url {
        Url::parse(raw).unwrap()
    }

    /// The endpoint as it reaches the policy: `None` when the request named none.
    fn endpoint(raw: Option<&str>) -> Option<Url> {
        raw.map(url)
    }

    /// A temp dir, canonical: on macOS the temp root is itself reached through a
    /// symlink, and a test about symlinks must not trip over that one.
    fn temp_dir() -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let root = fs::canonicalize(dir.path()).unwrap();
        (dir, root)
    }

    fn file_url(path: &Path) -> Url {
        Url::from_file_path(path).unwrap()
    }

    #[test]
    fn the_default_policy_is_any_endpoint_and_no_local_files() {
        let policy = AccessPolicy::default();
        assert!(
            policy
                .authorize(&url("s3://any-bucket/key.parquet"))
                .is_ok()
        );
        assert!(policy.authorize_endpoint(Backend::S3, None).is_ok());
        assert!(
            policy
                .authorize_endpoint(
                    Backend::S3,
                    endpoint(Some("https://minio.example.com")).as_ref()
                )
                .is_ok()
        );

        let error = policy.authorize(&url("file:///etc/passwd")).unwrap_err();
        assert!(matches!(error, ApiError::Forbidden(_)), "{error}");
        assert!(error.to_string().contains("no local files"), "{error}");
    }

    /// The bucket is not the question, and never was: what matters is which server the
    /// request would have us talk to.
    #[test]
    fn any_bucket_is_readable_at_an_allowed_endpoint() {
        let policy = with_endpoints(&["aws"]);
        for bucket in [
            "s3://ipac-irsa-ztf/k.parquet",
            "s3://anything-else/k.parquet",
        ] {
            assert!(policy.authorize(&url(bucket)).is_ok(), "{bucket}");
        }
    }

    #[test]
    fn an_endpoint_list_allows_those_endpoints_and_no_others() {
        let policy = with_endpoints(&["https://minio.example.com"]);
        assert!(
            policy
                .authorize_endpoint(
                    Backend::S3,
                    endpoint(Some("https://minio.example.com")).as_ref()
                )
                .is_ok()
        );
        // A default port is the same endpoint spelled out.
        assert!(
            policy
                .authorize_endpoint(
                    Backend::S3,
                    endpoint(Some("https://minio.example.com:443")).as_ref()
                )
                .is_ok()
        );
        for refused in [
            "https://evil.example.com",
            "http://minio.example.com",
            "https://minio.example.com:9000",
            "https://minio.example.com.evil.net",
        ] {
            let error = policy
                .authorize_endpoint(Backend::S3, endpoint(Some(refused)).as_ref())
                .unwrap_err();
            assert!(matches!(error, ApiError::Forbidden(_)), "{refused}");
        }
    }

    /// A list that does not say "aws" does not allow AWS, and a url with no endpoint
    /// option is exactly that request.
    #[test]
    fn aws_is_an_endpoint_like_any_other() {
        let only_minio = with_endpoints(&["https://minio.example.com"]);
        let error = only_minio
            .authorize_endpoint(Backend::S3, None)
            .unwrap_err();
        assert!(error.to_string().contains("aws"), "{error}");

        let only_aws = with_endpoints(&["aws"]);
        assert!(only_aws.authorize_endpoint(Backend::S3, None).is_ok());
        assert!(
            only_aws
                .authorize_endpoint(
                    Backend::S3,
                    endpoint(Some("https://minio.example.com")).as_ref()
                )
                .is_err()
        );
    }

    /// An empty list is the operator turning a backend off, and it turns off only that
    /// backend: the sections are separate rules, not one rule written three times.
    #[test]
    fn an_empty_endpoint_list_turns_one_backend_off() {
        let no_s3 = with_endpoints(&[]);
        let error = no_s3.authorize(&url("s3://bucket/k.parquet")).unwrap_err();
        assert!(matches!(error, ApiError::Forbidden(_)), "{error}");
        assert_eq!(no_s3.allowed_schemes(), ["gs", "az", "https", "webdav"]);

        assert!(everywhere(&[]).allowed_schemes().is_empty());
    }

    /// Each backend reads its own section, so a rule written under one does not admit
    /// or refuse a url addressed to another.
    #[test]
    fn the_sections_do_not_reach_into_each_other() {
        let policy = policy(&AccessConfig {
            s3: entries(&["aws"]),
            gcs: entries(&[]),
            azure: entries(&["https://azurite.example.com"]),
            ..Default::default()
        });
        assert_eq!(policy.allowed_schemes(), ["s3", "az", "https", "webdav"]);

        assert!(policy.authorize(&url("s3://b/k.parquet")).is_ok());
        assert!(policy.authorize(&url("gs://b/k.parquet")).is_err());
        assert!(policy.authorize(&url("az://c/k.parquet")).is_ok());

        // s3 names the provider and azure names a server, so each refuses what the
        // other allows.
        assert!(policy.authorize_endpoint(Backend::S3, None).is_ok());
        assert!(policy.authorize_endpoint(Backend::Azure, None).is_err());
        let azurite = endpoint(Some("https://azurite.example.com"));
        assert!(
            policy
                .authorize_endpoint(Backend::Azure, azurite.as_ref())
                .is_ok()
        );
        assert!(
            policy
                .authorize_endpoint(Backend::S3, azurite.as_ref())
                .is_err()
        );
    }

    /// Each backend spells "the provider's own service" its own way, and a refusal says
    /// which section to change.
    #[test]
    fn each_backend_has_its_own_name_for_its_provider() {
        for (backend, provider, section) in [
            (Backend::S3, "aws", "access.s3"),
            (Backend::Gcs, "gcp", "access.gcs"),
            (Backend::Azure, "azure", "access.azure"),
        ] {
            let policy = under(backend, &[provider]);
            assert!(
                policy.authorize_endpoint(backend, None).is_ok(),
                "{provider}"
            );

            // A list that names a server does not thereby allow the provider.
            let refuses = under(backend, &["https://elsewhere.example.com"]);
            let error = refuses.authorize_endpoint(backend, None).unwrap_err();
            assert!(error.to_string().contains(provider), "{error}");
            assert!(error.to_string().contains(section), "{error}");
        }
    }

    #[test]
    fn the_loopback_interface_is_off_by_default() {
        let policy = AccessPolicy::default();
        for raw in [
            "http://127.0.0.1:9000",
            "http://localhost:9000",
            "http://sub.localhost",
            "https://[::1]:9000",
            "http://0.0.0.0:9000",
            "http://127.13.14.15",
        ] {
            let error = policy
                .authorize_endpoint(Backend::S3, endpoint(Some(raw)).as_ref())
                .unwrap_err();
            assert!(matches!(error, ApiError::Forbidden(_)), "{raw}");
            assert!(error.to_string().contains("loopback"), "{raw}: {error}");
        }
        // Not loopback, whatever the name suggests.
        assert!(
            policy
                .authorize_endpoint(
                    Backend::S3,
                    endpoint(Some("https://localhost.example.com")).as_ref()
                )
                .is_ok()
        );
    }

    /// Loopback was never the whole of it. Everything else this machine can reach and
    /// its callers cannot is refused by the same default, and by the same gate.
    #[test]
    fn an_unrestricted_policy_still_refuses_a_destination_that_is_not_public() {
        let policy = AccessPolicy::default();
        for raw in [
            // The instance metadata service, which is where the machine's own IAM
            // credentials are.
            "http://169.254.169.254",
            "http://[::ffff:169.254.169.254]",
            "http://10.0.0.5:9000",
            "http://192.168.1.1",
            // And the name half, since an internal host on public address space would
            // otherwise walk straight through.
            "http://minio",
            "https://metadata.google.internal",
            "https://store.svc",
        ] {
            let error = policy
                .authorize_endpoint(Backend::S3, endpoint(Some(raw)).as_ref())
                .unwrap_err();
            assert!(matches!(error, ApiError::Forbidden(_)), "{raw}: {error}");
        }
        assert!(
            policy
                .authorize_endpoint(
                    Backend::S3,
                    endpoint(Some("https://minio.example.com")).as_ref()
                )
                .is_ok()
        );
    }

    /// The same rule for every backend: the destination is a property of the address,
    /// not of the protocol that would be spoken to it.
    #[test]
    fn the_network_rules_apply_to_every_backend() {
        let policy = AccessPolicy::default();
        for &backend in BACKENDS {
            let error = policy
                .authorize_endpoint(backend, endpoint(Some("http://169.254.169.254")).as_ref())
                .unwrap_err();
            assert!(
                matches!(error, ApiError::Forbidden(_)),
                "{}: {error}",
                backend.schemes().join(", ")
            );
        }
    }

    /// Naming an endpoint is the operator pointing at it deliberately, and that is
    /// permission enough — the network rules are about what a *caller* may point the
    /// service at. Otherwise the ordinary deployment, a MinIO on an internal network,
    /// would have to say so twice.
    #[test]
    fn a_configured_endpoint_is_not_subject_to_the_network_rules() {
        let policy = with_endpoints(&["http://10.0.0.5:9000", "https://minio.internal"]);
        for raw in ["http://10.0.0.5:9000", "https://minio.internal"] {
            assert!(
                policy
                    .authorize_endpoint(Backend::S3, endpoint(Some(raw)).as_ref())
                    .is_ok(),
                "{raw}"
            );
        }
        // And nothing else on those networks came along with it.
        assert!(
            policy
                .authorize_endpoint(Backend::S3, endpoint(Some("http://10.0.0.6:9000")).as_ref())
                .is_err()
        );
    }

    #[test]
    fn the_loopback_interface_can_be_turned_on() {
        let policy = policy(&AccessConfig {
            network: NetworkConfig {
                allow_loopback: true,
                ..Default::default()
            },
            ..Default::default()
        });
        assert!(
            policy
                .authorize_endpoint(
                    Backend::S3,
                    endpoint(Some("http://127.0.0.1:9000")).as_ref()
                )
                .is_ok()
        );
    }

    /// Naming an endpoint in the config is the operator pointing at it deliberately,
    /// which is all `allow_loopback` was ever standing in for.
    #[test]
    fn a_named_loopback_endpoint_needs_no_further_permission() {
        let policy = with_endpoints(&["http://127.0.0.1:9000"]);
        assert!(
            policy
                .authorize_endpoint(
                    Backend::S3,
                    endpoint(Some("http://127.0.0.1:9000")).as_ref()
                )
                .is_ok()
        );
        assert!(
            policy
                .authorize_endpoint(
                    Backend::S3,
                    endpoint(Some("http://127.0.0.1:9001")).as_ref()
                )
                .is_err()
        );
    }

    #[test]
    fn a_local_root_allows_what_is_under_it_and_nothing_else() {
        let (_dir, root) = temp_dir();
        let inside = root.join("part0.parquet");
        fs::write(&inside, b"").unwrap();
        let outside = root.parent().unwrap().join("outside.parquet");
        fs::write(&outside, b"").unwrap();

        let policy = with_paths(&[&root], false);
        assert_eq!(
            policy.authorize(&file_url(&inside)).unwrap(),
            Target::Local(inside)
        );
        let error = policy.authorize(&file_url(&outside)).unwrap_err();
        assert!(matches!(error, ApiError::Forbidden(_)), "{error}");

        fs::remove_file(&outside).unwrap();
    }

    #[test]
    fn a_local_root_may_be_written_as_a_file_url() {
        let (_dir, root) = temp_dir();
        let inside = root.join("part0.parquet");
        fs::write(&inside, b"").unwrap();
        let policy = policy(&AccessConfig {
            local: LocalConfig {
                paths: vec![file_url(&root).to_string()],
                follow_symlinks: false,
            },
            ..Default::default()
        });
        assert!(policy.authorize(&file_url(&inside)).is_ok());
    }

    /// A sibling directory whose name starts with the root's name is not under it.
    #[test]
    fn a_local_root_is_matched_by_path_component() {
        let (_dir, root) = temp_dir();
        let allowed = root.join("data");
        let sibling = root.join("data-private");
        fs::create_dir(&allowed).unwrap();
        fs::create_dir(&sibling).unwrap();
        let target = sibling.join("part0.parquet");
        fs::write(&target, b"").unwrap();

        let policy = with_paths(&[&allowed], false);
        assert!(policy.authorize(&file_url(&target)).is_err());
    }

    #[test]
    fn a_path_cannot_climb_out_of_an_allowed_root() {
        let (_dir, root) = temp_dir();
        let policy = with_paths(&[&root], false);
        // `..` is resolved before the path is matched, and never on the filesystem.
        let climb = format!("{}/../../etc/passwd", root.display());
        let error = policy
            .authorize(&Url::from_file_path(&climb).unwrap())
            .unwrap_err();
        assert!(matches!(error, ApiError::Forbidden(_)), "{error}");
    }

    #[test]
    fn a_missing_file_under_an_allowed_root_is_a_404() {
        let (_dir, root) = temp_dir();
        let policy = with_paths(&[&root], false);
        let error = policy
            .authorize(&file_url(&root.join("nope.parquet")))
            .unwrap_err();
        assert!(matches!(error, ApiError::NotFound(_)), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_is_refused_unless_the_server_follows_them() {
        let (_dir, root) = temp_dir();
        let real = root.join("part0.parquet");
        fs::write(&real, b"").unwrap();
        let link = root.join("link.parquet");
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let refuses = with_paths(&[&root], false);
        let error = refuses.authorize(&file_url(&link)).unwrap_err();
        assert!(matches!(error, ApiError::Forbidden(_)), "{error}");
        assert!(error.to_string().contains("symlink"), "{error}");

        // Following it lands on the real file, which is what gets opened.
        let follows = with_paths(&[&root], true);
        assert_eq!(
            follows.authorize(&file_url(&link)).unwrap(),
            Target::Local(real)
        );
    }

    /// The spelling of a path may go through a symlink without the *file* being
    /// anywhere unexpected — `/tmp` is a link to `/private/tmp` on macOS. A server
    /// that follows symlinks has to resolve first and judge second, or it refuses
    /// files that are plainly inside an allowed directory.
    #[cfg(unix)]
    #[test]
    fn following_symlinks_allows_a_linked_spelling_of_an_allowed_directory() {
        let (_dir, root) = temp_dir();
        let real = root.join("data");
        fs::create_dir(&real).unwrap();
        let target = real.join("part0.parquet");
        fs::write(&target, b"").unwrap();
        let linked_root = root.join("link");
        std::os::unix::fs::symlink(&real, &linked_root).unwrap();

        let follows = with_paths(&[&real], true);
        assert_eq!(
            follows
                .authorize(&file_url(&linked_root.join("part0.parquet")))
                .unwrap(),
            Target::Local(target)
        );
    }

    /// The point of resolving before matching: a link inside an allowed directory is
    /// still not a way out of it.
    #[cfg(unix)]
    #[test]
    fn following_symlinks_does_not_let_one_escape_the_allowed_roots() {
        let (_dir, root) = temp_dir();
        let allowed = root.join("allowed");
        fs::create_dir(&allowed).unwrap();
        let secret = root.join("secret.parquet");
        fs::write(&secret, b"").unwrap();
        let link = allowed.join("innocent.parquet");
        std::os::unix::fs::symlink(&secret, &link).unwrap();

        let follows = with_paths(&[&allowed], true);
        let error = follows.authorize(&file_url(&link)).unwrap_err();
        assert!(matches!(error, ApiError::Forbidden(_)), "{error}");
    }

    fn mounted(source: &Path, follow_symlinks: bool) -> Mounts {
        Mounts::new(&[crate::config::MountConfig {
            path: "/".to_owned(),
            source: source.display().to_string(),
            follow_symlinks,
            immutable: false,
        }])
        .unwrap()
    }

    /// The bytes under a mount are already served whole over its own route, so the API
    /// can read them too — without `api.access.local.paths` naming the directory again.
    #[test]
    fn a_mount_lets_the_api_read_what_it_publishes() {
        let (_dir, root) = temp_dir();
        let published = root.join("published");
        fs::create_dir(&published).unwrap();
        let file = published.join("part0.parquet");
        fs::write(&file, b"").unwrap();
        let elsewhere = root.join("elsewhere.parquet");
        fs::write(&elsewhere, b"").unwrap();

        let policy =
            AccessPolicy::new(&AccessConfig::default(), &mounted(&published, false)).unwrap();
        assert_eq!(
            policy.authorize(&file_url(&file)).unwrap(),
            Target::Local(file.clone())
        );
        // Scoped to what the mount publishes: mounting a directory is not a way to read
        // its parent.
        let error = policy.authorize(&file_url(&elsewhere)).unwrap_err();
        assert!(matches!(error, ApiError::Forbidden(_)), "{error}");
        // And the grant is one way: the API's own table says nothing about the mount.
        assert!(AccessPolicy::default().authorize(&file_url(&file)).is_err());
    }

    /// Two directories, two answers about the same question, because the rules that
    /// decide it are the mount's rather than the service's.
    #[cfg(unix)]
    #[test]
    fn a_mount_brings_its_own_symlink_rule() {
        let (_dir, root) = temp_dir();
        let published = root.join("published");
        fs::create_dir(&published).unwrap();
        let real = published.join("part0.parquet");
        fs::write(&real, b"").unwrap();
        let link = published.join("link.parquet");
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let refuses = AccessPolicy::new(&AccessConfig::default(), &mounted(&published, false))
            .unwrap()
            .authorize(&file_url(&link));
        assert!(refuses.is_err(), "{refuses:?}");

        let follows = AccessPolicy::new(&AccessConfig::default(), &mounted(&published, true))
            .unwrap()
            .authorize(&file_url(&link))
            .unwrap();
        assert_eq!(follows, Target::Local(real));
    }

    /// A directory the operator listed keeps its own answer whatever a mount says, so a
    /// mount that follows symlinks cannot become a way of following one out of a
    /// directory that does not.
    #[cfg(unix)]
    #[test]
    fn a_mount_that_follows_symlinks_does_not_widen_the_api_table() {
        let (_dir, root) = temp_dir();
        let listed = root.join("listed");
        let published = root.join("published");
        fs::create_dir(&listed).unwrap();
        fs::create_dir(&published).unwrap();
        let target = published.join("part0.parquet");
        fs::write(&target, b"").unwrap();
        let link = listed.join("innocent.parquet");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let policy = AccessPolicy::new(
            &AccessConfig {
                local: LocalConfig {
                    paths: vec![listed.display().to_string()],
                    follow_symlinks: false,
                },
                ..Default::default()
            },
            &mounted(&published, true),
        )
        .unwrap();
        let error = policy.authorize(&file_url(&link)).unwrap_err();
        assert!(error.to_string().contains("symlink"), "{error}");
        // The file itself is still readable where it actually lives.
        assert_eq!(
            policy.authorize(&file_url(&target)).unwrap(),
            Target::Local(target)
        );
    }

    #[test]
    fn a_rule_that_could_never_match_is_a_startup_error() {
        let (_dir, root) = temp_dir();
        let missing = root.join("not-there");
        let a_file = root.join("a-file");
        fs::write(&a_file, b"").unwrap();

        for entry in [
            // An endpoint is a server, not a place in one.
            "https://minio.example.com/bucket",
            "https://minio.example.com?region=us-west-2",
            // Not an endpoint at all.
            "s3://bucket",
            "minio.example.com",
            "aws-ish",
            // Another backend's name for its provider, which this one has no use for.
            "gcp",
            "azure",
        ] {
            let config = AccessConfig {
                s3: entries(&[entry]),
                ..Default::default()
            };
            assert!(
                AccessPolicy::new(&config, &Mounts::default()).is_err(),
                "{entry} was accepted as an s3 endpoint"
            );
        }

        // And the same the other way round, so that no section quietly accepts a name
        // that means a different provider.
        assert!(
            AccessPolicy::new(
                &AccessConfig {
                    gcs: entries(&["aws"]),
                    ..Default::default()
                },
                &Mounts::default()
            )
            .is_err(),
            "the gcs section accepted \"aws\""
        );
        assert!(
            AccessPolicy::new(
                &AccessConfig {
                    azure: entries(&["gcp"]),
                    ..Default::default()
                },
                &Mounts::default()
            )
            .is_err(),
            "the azure section accepted \"gcp\""
        );

        for entry in [
            "relative/path",
            "s3://bucket",
            "file://relative/path",
            &missing.display().to_string(),
            &a_file.display().to_string(),
        ] {
            let config = AccessConfig {
                local: LocalConfig {
                    paths: vec![entry.to_owned()],
                    follow_symlinks: false,
                },
                ..Default::default()
            };
            assert!(
                AccessPolicy::new(&config, &Mounts::default()).is_err(),
                "{entry} was accepted as a local directory"
            );
        }
    }

    /// Every backend, against the narrowest config that should allow it and against the
    /// one that should refuse it. One table rather than a test per backend, so a
    /// backend added without a rule of its own fails here rather than passing quietly.
    #[test]
    fn every_backend_is_allowed_only_by_a_config_that_names_it() {
        for &backend in BACKENDS {
            for &scheme in backend.schemes() {
                let object = url(&format!("{scheme}://container/key.parquet"));

                // The narrowest thing that should allow it. For a backend with a
                // provider that is the provider's own name; for one whose url is its own
                // address it is that address, there being nothing else to write.
                let entry = backend.provider().map_or_else(
                    || match backend {
                        Backend::Webdav => "https://container".to_owned(),
                        _ => format!("{scheme}://container"),
                    },
                    str::to_owned,
                );
                let narrowest = under(backend, &[&entry]);
                assert!(narrowest.authorize(&object).is_ok(), "{scheme}");
                assert!(
                    narrowest
                        .authorize_endpoint(
                            backend,
                            endpoint(Some("https://other.example.com")).as_ref()
                        )
                        .is_err(),
                    "{scheme}: {entry} allowed a server it does not name"
                );

                // And the config that turns it off refuses it outright, at the first
                // gate.
                let off = under(backend, &[]);
                let error = off.authorize(&object).unwrap_err();
                assert!(matches!(error, ApiError::Forbidden(_)), "{scheme}: {error}");
                assert!(!off.allowed_schemes().contains(&scheme), "{scheme}");
            }

            // A provider entry is also what a url carrying no endpoint option asks for,
            // which is a question only a backend that has a provider can be asked.
            if let Some(provider) = backend.provider() {
                assert!(
                    under(backend, &[provider])
                        .authorize_endpoint(backend, None)
                        .is_ok(),
                    "{provider}"
                );
            }
        }
    }

    #[test]
    fn the_schemes_a_policy_serves_are_the_ones_it_was_given() {
        let (_dir, root) = temp_dir();
        // `http` is absent by default and `https` is not: the http backend is on, and
        // its cleartext half is the one thing an operator has to ask for.
        assert_eq!(
            AccessPolicy::default().allowed_schemes(),
            ["s3", "gs", "az", "https", "webdav"]
        );
        assert_eq!(
            with_paths(&[&root], false).allowed_schemes(),
            ["s3", "gs", "az", "https", "webdav", "file"]
        );
        assert_eq!(everywhere(&[]).allowed_schemes(), Vec::<&str>::new());
    }

    /// The scheme a caller writes is half the http rules: `https://` is served out of the
    /// box and `http://` is not, because over cleartext nothing says the parquet file
    /// came from the host the url named.
    #[test]
    fn plain_http_is_off_until_the_config_asks_for_it() {
        let default = AccessPolicy::default();
        assert!(
            default
                .authorize(&url("https://data.example.com/k.parquet"))
                .is_ok()
        );
        let error = default
            .authorize_endpoint(
                Backend::Http,
                endpoint(Some("http://data.example.com")).as_ref(),
            )
            .unwrap_err();
        assert!(matches!(error, ApiError::Forbidden(_)), "{error}");
        assert!(error.to_string().contains("allow_plain_http"), "{error}");
        // https over the same policy is not affected by the switch.
        assert!(
            default
                .authorize_endpoint(
                    Backend::Http,
                    endpoint(Some("https://data.example.com")).as_ref()
                )
                .is_ok()
        );

        let allowed = policy(&AccessConfig {
            http: HttpConfig {
                endpoints: None,
                allow_plain_http: true,
            },
            ..Default::default()
        });
        assert!(
            allowed
                .authorize_endpoint(
                    Backend::Http,
                    endpoint(Some("http://data.example.com")).as_ref()
                )
                .is_ok()
        );
        assert!(allowed.allowed_schemes().contains(&"http"));
    }

    /// Naming a cleartext server is the operator making the same decision deliberately,
    /// and it does not open cleartext to anywhere else.
    #[test]
    fn a_named_http_endpoint_needs_no_further_permission() {
        let policy = under(Backend::Http, &["http://data.example.com"]);
        assert!(
            policy
                .authorize_endpoint(
                    Backend::Http,
                    endpoint(Some("http://data.example.com")).as_ref()
                )
                .is_ok()
        );
        assert!(
            policy
                .authorize_endpoint(
                    Backend::Http,
                    endpoint(Some("http://other.example.com")).as_ref()
                )
                .is_err()
        );
        assert!(policy.allowed_schemes().contains(&"http"));
    }

    /// The other backends' cleartext decision is the caller's `allow_http`, about a
    /// credential only `storage` can see. `allow_plain_http` must not reach into it, in
    /// either direction.
    #[test]
    fn the_cleartext_switch_is_the_http_backends_alone() {
        let policy = policy(&AccessConfig {
            http: HttpConfig {
                endpoints: None,
                allow_plain_http: false,
            },
            network: NetworkConfig {
                allow_private: true,
                ..Default::default()
            },
            ..Default::default()
        });
        for &backend in BACKENDS {
            let cleartext = endpoint(Some("http://minio.example.com"));
            let refused = policy
                .authorize_endpoint(backend, cleartext.as_ref())
                .is_err();
            assert_eq!(
                refused,
                !backend.has_provider(),
                "{:?} disagreed about cleartext",
                backend
            );
        }
    }
}
