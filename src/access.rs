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
//! Every remote backend has the same three-state list, under its own section, with its
//! own name for "the provider's own service" — which is what a url carrying no
//! `endpoint` option means.
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
}

/// Every remote backend, so that a caller listing or looping over them cannot miss one
/// a later phase adds.
pub const BACKENDS: &[Backend] = &[Backend::S3, Backend::Gcs, Backend::Azure];

/// The one scheme with no [`Backend`] behind it, named here because it is the other half
/// of that enum rather than a string three places happen to agree on.
pub const LOCAL_SCHEME: &str = "file";

impl Backend {
    /// The url scheme that names this backend.
    pub fn scheme(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::Gcs => "gs",
            Self::Azure => "az",
        }
    }

    pub fn from_scheme(scheme: &str) -> Option<Self> {
        BACKENDS
            .iter()
            .copied()
            .find(|backend| backend.scheme() == scheme)
    }

    /// What an endpoint entry says to mean the provider's own service, which is the
    /// endpoint a url with no `endpoint` option is asking for.
    fn provider(self) -> &'static str {
        match self {
            Self::S3 => "aws",
            Self::Gcs => "gcp",
            Self::Azure => "azure",
        }
    }

    /// The config section its rules live in, for saying where to change them.
    fn section(self) -> &'static str {
        match self {
            Self::S3 => "access.s3",
            Self::Gcs => "access.gcs",
            Self::Azure => "access.azure",
        }
    }
}

#[derive(Debug)]
pub struct AccessPolicy {
    s3: EndpointRules,
    gcs: EndpointRules,
    azure: EndpointRules,
    /// Allowed directories, canonical, so that a resolved request path can simply be
    /// tested for being under one of them.
    local: Vec<PathBuf>,
    network: NetworkPolicy,
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
    /// The backend it belongs to, not the name it is written under: the name is one of
    /// the backend's own properties, and looking it up is what keeps a refusal from
    /// being able to quote a different one than the parse accepted.
    Provider(Backend),
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
        Self::new(&AccessConfig::default()).expect("the default access config is valid")
    }
}

impl AccessPolicy {
    pub fn new(config: &AccessConfig) -> Result<Self, ConfigError> {
        let s3 = EndpointRules::new(&config.s3, Backend::S3)?;
        let gcs = EndpointRules::new(&config.gcs, Backend::Gcs)?;
        let azure = EndpointRules::new(&config.azure, Backend::Azure)?;
        // Every host the operator named, so that the network rules let the deployment
        // reach the endpoints it was configured for. A MinIO on RFC1918 space is the
        // ordinary case of this, and it must not need saying twice.
        let named: Vec<Host<String>> = [&s3, &gcs, &azure]
            .into_iter()
            .flat_map(EndpointRules::hosts)
            .collect();
        let network = NetworkPolicy::new(&config.network, &named)?;
        let local = config
            .local
            .paths
            .iter()
            // Resolved now so that startup fails on a directory that is not there,
            // rather than every request failing later for a reason nobody can see.
            .map(|entry| canonical_root(entry))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            s3,
            gcs,
            azure,
            local,
            network,
            follow_symlinks: config.local.follow_symlinks,
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
            .map(Backend::scheme)
            .collect();
        if !self.local.is_empty() {
            schemes.push(LOCAL_SCHEME);
        }
        schemes
    }

    fn rules(&self, backend: Backend) -> &EndpointRules {
        match backend {
            Backend::S3 => &self.s3,
            Backend::Gcs => &self.gcs,
            Backend::Azure => &self.azure,
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
            EndpointRules::Any => match endpoint.and_then(Url::host) {
                Some(host) => self.network.authorize_host(&host),
                None => Ok(()),
            },
            EndpointRules::Only(allowed) => {
                let wanted = match endpoint {
                    Some(url) => Endpoint::from_url(url)?,
                    None => Endpoint::Provider(backend),
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

    fn authorize_local(&self, url: &Url) -> Result<PathBuf, ApiError> {
        if self.local.is_empty() {
            return Err(ApiError::forbidden(
                "this server reads no local files; add a directory to \
                 access.local.paths to change that",
            ));
        }
        let path = url
            .to_file_path()
            .map_err(|()| ApiError::bad_request(format!("url {url} is not a local path")))?;
        let lexical = lexically_clean(&path)
            .ok_or_else(|| ApiError::bad_request(format!("path {} escapes /", path.display())))?;

        // Without symlink resolution the path as written is the path that gets opened,
        // so it can be judged before the filesystem is touched at all — and a path
        // outside every allowed directory then gets the same answer whether or not it
        // exists. With resolution on, only the resolved path can be judged, so the
        // check moves below and this only decides how much a refusal may say.
        let named_an_allowed_path = self.contains(&lexical);
        if !self.follow_symlinks && !named_an_allowed_path {
            return Err(self.local_refusal(url));
        }
        let canonical = std::fs::canonicalize(&lexical).map_err(|error| match error.kind() {
            // Saying "no such file" about a path the caller was never allowed to name
            // would answer a question they did not get to ask.
            _ if !named_an_allowed_path => self.local_refusal(url),
            std::io::ErrorKind::NotFound => {
                ApiError::not_found(format!("{} does not exist", lexical.display()))
            }
            _ => ApiError::forbidden(format!("cannot read {}: {error}", lexical.display())),
        })?;
        if !self.follow_symlinks && canonical != lexical {
            return Err(ApiError::forbidden(format!(
                "{} goes through a symlink, which this server does not follow; set \
                 access.local.follow_symlinks to change that",
                lexical.display()
            )));
        }
        // Again on the resolved path: with follow_symlinks on, a link inside an allowed
        // directory must still not land outside every allowed directory.
        if !self.contains(&canonical) {
            return Err(self.local_refusal(url));
        }
        Ok(canonical)
    }

    fn contains(&self, path: &Path) -> bool {
        self.local.iter().any(|root| path.starts_with(root))
    }

    fn local_refusal(&self, url: &Url) -> ApiError {
        let roots: Vec<String> = self
            .local
            .iter()
            .map(|root| root.display().to_string())
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
    fn new(config: &crate::config::EndpointConfig, backend: Backend) -> Result<Self, ConfigError> {
        match &config.endpoints {
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
            Self::Provider(backend) => f.write_str(backend.provider()),
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
    if entry.eq_ignore_ascii_case(provider) {
        return Ok(Endpoint::Provider(backend));
    }
    let url = Url::parse(entry).map_err(|error| {
        invalid(format!(
            "{error}; expected {provider:?} or a url like https://minio.example.com"
        ))
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

fn canonical_root(entry: &str) -> Result<PathBuf, ConfigError> {
    let invalid = |reason: String| ConfigError::Rule(entry.to_owned(), reason);

    let path = if entry.starts_with('/') {
        PathBuf::from(entry)
    } else {
        let url = Url::parse(entry).map_err(|error| {
            invalid(format!(
                "{error}; expected an absolute path or a file:// url"
            ))
        })?;
        if url.scheme() != LOCAL_SCHEME {
            return Err(invalid(format!(
                "scheme {:?} is not a local path; expected an absolute path or a \
                 file:// url",
                url.scheme()
            )));
        }
        url.to_file_path()
            .map_err(|()| invalid("not an absolute local path".to_owned()))?
    };

    let canonical = std::fs::canonicalize(&path)
        .map_err(|error| invalid(format!("cannot resolve {}: {error}", path.display())))?;
    match canonical.is_dir() {
        true => Ok(canonical),
        false => Err(invalid(format!(
            "{} is not a directory",
            canonical.display()
        ))),
    }
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

    use crate::config::{EndpointConfig, LocalConfig, NetworkConfig};

    use super::*;

    fn policy(config: &AccessConfig) -> AccessPolicy {
        AccessPolicy::new(config).unwrap()
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
        assert_eq!(no_s3.allowed_schemes(), ["gs", "az"]);

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
        assert_eq!(policy.allowed_schemes(), ["s3", "az"]);

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
                backend.scheme()
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
                AccessPolicy::new(&config).is_err(),
                "{entry} was accepted as an s3 endpoint"
            );
        }

        // And the same the other way round, so that no section quietly accepts a name
        // that means a different provider.
        assert!(
            AccessPolicy::new(&AccessConfig {
                gcs: entries(&["aws"]),
                ..Default::default()
            })
            .is_err(),
            "the gcs section accepted \"aws\""
        );
        assert!(
            AccessPolicy::new(&AccessConfig {
                azure: entries(&["gcp"]),
                ..Default::default()
            })
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
                AccessPolicy::new(&config).is_err(),
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
            let scheme = backend.scheme();
            let object = url(&format!("{scheme}://container/key.parquet"));

            // The narrowest thing that should allow it: its provider, and nothing else.
            let narrowest = under(backend, &[backend.provider()]);
            assert!(narrowest.authorize(&object).is_ok(), "{scheme}");
            assert!(
                narrowest.authorize_endpoint(backend, None).is_ok(),
                "{scheme}"
            );
            assert!(
                narrowest
                    .authorize_endpoint(
                        backend,
                        endpoint(Some("https://other.example.com")).as_ref()
                    )
                    .is_err(),
                "{scheme}: a provider entry allowed a server it does not name"
            );

            // And the config that turns it off refuses it outright, at the first gate.
            let off = under(backend, &[]);
            let error = off.authorize(&object).unwrap_err();
            assert!(matches!(error, ApiError::Forbidden(_)), "{scheme}: {error}");
            assert!(!off.allowed_schemes().contains(&scheme), "{scheme}");
        }
    }

    #[test]
    fn the_schemes_a_policy_serves_are_the_ones_it_was_given() {
        let (_dir, root) = temp_dir();
        assert_eq!(
            AccessPolicy::default().allowed_schemes(),
            ["s3", "gs", "az"]
        );
        assert_eq!(
            with_paths(&[&root], false).allowed_schemes(),
            ["s3", "gs", "az", "file"]
        );
        assert_eq!(everywhere(&[]).allowed_schemes(), Vec::<&str>::new());
    }
}
