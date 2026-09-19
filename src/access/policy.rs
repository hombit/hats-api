//! Which endpoint a request may name.
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
//! process happens to sit on. That is [`network`](super::network)'s, under
//! `[access.network]`.
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
//! ```
//!
//! **There is no section for a mount, in any scheme.** A `[[mount]]` is the whole of what
//! makes a directory readable, and a `file://` url in a request is addressed in the
//! mounts' url space rather than in a store or on a disk: `file:///hats/dr1/x.parquet` is
//! the mount at `/hats`, and whatever `source` that mount holds. So there is no second
//! list of directories to keep in step with the mounts, and no spelling of a path that
//! reaches a directory no mount named. What a path under a mount may reach is
//! [`local`](super::local)'s.
//!
//! That is also why these rules say nothing about a mount's own `source`, whatever scheme
//! it is in: they are about where a *caller* may point this service, and an operator
//! writing a directory into the config is the permission for that directory.
//! [`AccessPolicy::authorize_configured`] is the gate a source goes through instead, and
//! the mount stays reachable only through its own `path`.

use std::path::PathBuf;
use std::sync::Arc;

use http::HeaderValue;
use url::{Host, Url};

use crate::access::mount::{Mount, Mounts};
use crate::access::network::NetworkPolicy;
use crate::config::{AccessConfig, ConfigError};
use crate::error::ApiError;

/// What a URL turned out to be, once it was allowed.
#[derive(Debug)]
pub enum Target<'a> {
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
    /// Read this name out of a mount whose `source` is a store: the mount, and the path
    /// below it.
    ///
    /// The mount rather than a store, because opening one needs the policy and the
    /// scratch budget, and neither is this gate's to hold. What is settled here is the
    /// same thing [`Self::Local`] settles — which mount the url named, and that the path
    /// inside it is a path — and the store is the mount's own.
    InStore(&'a Mount, String),
}

impl Target<'_> {
    /// The file on this machine, for a caller that only reads local ones.
    pub fn local(&self) -> Option<&std::path::Path> {
        match self {
            Self::Local(path) => Some(path),
            _ => None,
        }
    }
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
    Hf,
}

/// Every remote backend, so that a caller listing or looping over them cannot miss one
/// a later phase adds.
pub const BACKENDS: &[Backend] = &[
    Backend::S3,
    Backend::Gcs,
    Backend::Azure,
    Backend::Http,
    Backend::Webdav,
    Backend::Hf,
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
            Self::Hf => &["hf"],
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
            // An `hf://` url names a repository, not a server; the Hub is the server, and
            // an organisation running a private Hub points a request at it with the same
            // `endpoint` option the bucket-addressed backends take.
            Self::Hf => Some("hf"),
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
            Self::Hf => "access.hf",
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
    hf: EndpointRules,
    /// Whether an `http://` url may be read when the http rules name no endpoints. A
    /// decision of the operator's rather than the caller's: the caller sends no
    /// credential to an `http(s)://` url, so what cleartext costs here is not a secret
    /// but the assurance that the bytes came from the host the url names — and only the
    /// operator knows whether the deployment's network makes that acceptable.
    allow_plain_http: bool,
    /// Every readable directory, which is every mount. Shared with the rest of the
    /// service rather than copied, so a mount's rules cannot be two things at once.
    pub(super) mounts: Arc<Mounts>,
    network: NetworkPolicy,
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
        Self::new(&AccessConfig::default(), Arc::default(), None)
            .expect("the default access config is valid")
    }
}

impl AccessPolicy {
    /// The endpoint rules as configured, over the directories the mounts name.
    ///
    /// The mounts are the local half of the policy rather than an addition to it, so
    /// they are an argument: a policy that could be built without them would be one that
    /// reads no local files, and every caller would have to remember to say otherwise.
    ///
    /// `user_agent` is what the network policy's client names this deployment as, from
    /// `[server]`. It is passed in rather than read here because `[api.access]` is about
    /// what may be reached and this is about what the request looks like when it gets
    /// there; `None` sends no such header.
    pub fn new(
        config: &AccessConfig,
        mounts: Arc<Mounts>,
        user_agent: Option<HeaderValue>,
    ) -> Result<Self, ConfigError> {
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
        let hf = build(&config.hf.endpoints, Backend::Hf)?;
        // A `[[mount]]`'s own source is deliberately *not* added here. It is reached
        // through `NetworkPolicy::configured_transport` instead, which has no address
        // rules — so the mount stays reachable without its server becoming one a caller
        // may name, which is what putting it in this list would do. The grant stays
        // exactly as wide as the mount.
        let network = NetworkPolicy::new(&config.network, &named, user_agent)?;
        Ok(Self {
            s3,
            gcs,
            azure,
            http,
            webdav,
            hf,
            allow_plain_http: config.http.allow_plain_http,
            mounts,
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
        if !self.mounts.is_empty() {
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
            Backend::Hf => &self.hf,
        }
    }

    /// The first gate: is this the *kind* of thing the service reads at all? For a
    /// local file that settles it. For a remote backend the endpoint is still to come,
    /// because it lives in the url's options and only `storage` knows how to read those.
    pub fn authorize(&self, url: &Url) -> Result<Target<'_>, ApiError> {
        let scheme = url.scheme();
        if scheme == LOCAL_SCHEME {
            return self.authorize_local(url);
        }
        match Backend::from_scheme(scheme) {
            Some(backend) if self.rules(backend).enabled() => Ok(Target::Remote(backend)),
            _ => Err(ApiError::forbidden(format!(
                "this server does not read {scheme}:// urls; it reads {}",
                self.describe_schemes()
            ))),
        }
    }

    /// The same gate for a `[[mount]]`'s own `source`, which is the operator's url rather
    /// than a caller's.
    ///
    /// Only the scheme is asked about. Which endpoints a caller may name is
    /// `[api.access]`'s, and an operator writing a directory into the config is the
    /// permission for that directory — the same reason there is no `[api.access]` section
    /// for a local one. A backend an operator turned off for callers is therefore still
    /// mountable, and the mount is still reachable only through its own `path`.
    pub fn authorize_configured(&self, url: &Url) -> Result<Backend, ApiError> {
        Backend::from_scheme(url.scheme()).ok_or_else(|| {
            ApiError::bad_request(format!(
                "a mount source in scheme {:?} names no backend",
                url.scheme()
            ))
        })
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
                            "a {} url names its own server and takes no endpoint option",
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
            "{endpoint} is cleartext http; {change}"
        )))
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

pub(super) fn describe(items: &[impl std::fmt::Display]) -> String {
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

#[cfg(test)]
pub(super) mod tests {
    use std::fs;
    use std::path::Path;

    use tempfile::TempDir;

    use crate::config::{DataConfig, EndpointConfig, HttpConfig, MountConfig, NetworkConfig};
    use crate::storage::StorageOptions;

    use super::*;

    fn policy(config: &AccessConfig) -> AccessPolicy {
        AccessPolicy::new(config, Arc::default(), None).unwrap()
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
            hf: entries(list),
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
            Backend::Hf => AccessConfig {
                hf: list,
                ..Default::default()
            },
        })
    }

    /// A policy over mounts, which is the only kind that reads a local file. Each
    /// directory is published under `/<n>`, so a test writes `file:///0/x.parquet` for
    /// the first of them.
    pub(in crate::access) fn with_paths(paths: &[&Path], follow_symlinks: bool) -> AccessPolicy {
        let configs: Vec<MountConfig> = paths
            .iter()
            .enumerate()
            .map(|(n, path)| MountConfig {
                path: format!("/{n}"),
                source: path.display().to_string(),
                serve: false,
                follow_symlinks,
                immutable: false,
                storage: StorageOptions::default(),
                filenames: None,
            })
            .collect();
        let mounts = Mounts::new(&configs, &DataConfig::default()).unwrap();
        AccessPolicy::new(&AccessConfig::default(), Arc::new(mounts), None).unwrap()
    }

    pub(in crate::access) fn url(raw: &str) -> Url {
        Url::parse(raw).unwrap()
    }

    /// The endpoint as it reaches the policy: `None` when the request named none.
    fn endpoint(raw: Option<&str>) -> Option<Url> {
        raw.map(url)
    }

    /// A temp dir, canonical: on macOS the temp root is itself reached through a
    /// symlink, and a test about symlinks must not trip over that one.
    pub(in crate::access) fn temp_dir() -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let root = fs::canonicalize(dir.path()).unwrap();
        (dir, root)
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
        assert_eq!(
            no_s3.allowed_schemes(),
            ["gs", "az", "https", "webdav", "hf"]
        );

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
        assert_eq!(
            policy.allowed_schemes(),
            ["s3", "az", "https", "webdav", "hf"]
        );

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
    fn a_rule_that_could_never_match_is_a_startup_error() {
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
                AccessPolicy::new(&config, Arc::default(), None).is_err(),
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
                Arc::default(),
                None
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
                Arc::default(),
                None
            )
            .is_err(),
            "the azure section accepted \"gcp\""
        );
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
            ["s3", "gs", "az", "https", "webdav", "hf"]
        );
        assert_eq!(
            with_paths(&[&root], false).allowed_schemes(),
            ["s3", "gs", "az", "https", "webdav", "hf", "file"]
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
