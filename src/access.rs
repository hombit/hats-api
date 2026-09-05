//! What a request is allowed to read.
//!
//! The service takes the location of the data from the caller, so without a policy it
//! would read anything the machine it runs on can reach — every S3 server on the
//! network, every file on disk, and whatever is listening on localhost. [`AccessPolicy`]
//! is what it may read instead, built once at startup from `[access]` in the config.
//!
//! For s3 the thing worth deciding is **which endpoint** may be contacted, not which
//! bucket may be read: the bucket name says nothing about the host, since a request
//! carries its own `endpoint` option and can point the service anywhere. So the rules
//! are endpoints, and any bucket at an allowed endpoint is readable.
//!
//! ```toml
//! [access]
//! allow_loopback = false
//!
//! [access.s3]
//! # "aws" is AWS S3 itself: a url with no `endpoint` option of its own.
//! endpoints = ["aws", "https://minio.example.com"]
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

/// What a URL turned out to be, once it was allowed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// Read it through the object store for its scheme. For s3 this is not the whole
    /// answer: the endpoint is only known once the url's options are parsed, so
    /// [`AccessPolicy::authorize_s3_endpoint`] is the second half of the decision.
    Remote,
    /// Read this local file. Absolute, with every symlink already resolved and the
    /// result checked against the policy.
    Local(PathBuf),
}

#[derive(Debug)]
pub struct AccessPolicy {
    s3: S3Rules,
    /// Allowed directories, canonical, so that a resolved request path can simply be
    /// tested for being under one of them.
    local: Vec<PathBuf>,
    allow_loopback: bool,
    follow_symlinks: bool,
}

#[derive(Debug)]
enum S3Rules {
    /// No list was configured: any endpoint, subject to `allow_loopback`.
    Any,
    /// Exactly these, and an empty list is no s3 at all. An entry here is the
    /// operator naming a host, so `allow_loopback` has nothing left to decide.
    Only(Vec<Endpoint>),
}

#[derive(Debug, PartialEq, Eq)]
enum Endpoint {
    /// AWS S3 itself, which is what a url with no `endpoint` option means.
    Aws,
    /// One S3-compatible server. The port is kept resolved so that
    /// `https://host` and `https://host:443` are the one endpoint they are.
    Url {
        scheme: String,
        host: String,
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
        let s3 = match &config.s3.endpoints {
            None => S3Rules::Any,
            Some(entries) => S3Rules::Only(
                entries
                    .iter()
                    .map(|entry| parse_endpoint(entry))
                    .collect::<Result<_, _>>()?,
            ),
        };
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
            local,
            allow_loopback: config.allow_loopback,
            follow_symlinks: config.local.follow_symlinks,
        })
    }

    /// Every scheme this policy can serve, for logs and error messages.
    pub fn allowed_schemes(&self) -> Vec<&'static str> {
        let mut schemes = Vec::new();
        if self.s3_enabled() {
            schemes.push("s3");
        }
        if !self.local.is_empty() {
            schemes.push("file");
        }
        schemes
    }

    fn s3_enabled(&self) -> bool {
        !matches!(&self.s3, S3Rules::Only(endpoints) if endpoints.is_empty())
    }

    /// The first gate: is this the *kind* of thing the service reads at all? For a
    /// local file that settles it. For s3 the endpoint is still to come, because it
    /// lives in the url's options and only `storage` knows how to read those.
    pub fn authorize(&self, url: &Url) -> Result<Target, ApiError> {
        match url.scheme() {
            "file" => self.authorize_local(url).map(Target::Local),
            "s3" if self.s3_enabled() => Ok(Target::Remote),
            scheme => Err(ApiError::forbidden(format!(
                "this server does not read {scheme}:// urls; it reads {}",
                self.describe_schemes()
            ))),
        }
    }

    /// The second gate for s3: which server the request would have us talk to. `None`
    /// is a request with no `endpoint` option, which means AWS.
    pub fn authorize_s3_endpoint(&self, endpoint: Option<&Url>) -> Result<(), ApiError> {
        match &self.s3 {
            S3Rules::Any => match endpoint {
                // The one thing an unrestricted policy still refuses: the service can
                // reach things on its own machine that its callers cannot.
                Some(url)
                    if url.host().is_some_and(|host| is_loopback(&host))
                        && !self.allow_loopback =>
                {
                    Err(ApiError::forbidden(format!(
                        "endpoint {url} is on the loopback interface, which this server \
                         does not allow; set access.allow_loopback to change that"
                    )))
                }
                _ => Ok(()),
            },
            S3Rules::Only(allowed) => {
                let wanted = match endpoint {
                    Some(url) => Endpoint::from_url(url)?,
                    None => Endpoint::Aws,
                };
                match allowed.contains(&wanted) {
                    true => Ok(()),
                    false => Err(ApiError::forbidden(format!(
                        "{wanted} is not an endpoint this server will contact; it \
                         contacts {}",
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

impl Endpoint {
    fn from_url(url: &Url) -> Result<Self, ApiError> {
        let host = url
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or_else(|| ApiError::bad_request(format!("endpoint {url} has no host")))?;
        Ok(Self::Url {
            scheme: url.scheme().to_owned(),
            host: host.to_ascii_lowercase(),
            port: url.port_or_known_default(),
        })
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Aws => f.write_str("aws"),
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

/// Schemes an endpoint entry may use. An endpoint is an HTTP service whatever the
/// storage behind it is.
const ENDPOINT_SCHEMES: &[&str] = &["http", "https"];

fn parse_endpoint(entry: &str) -> Result<Endpoint, ConfigError> {
    let invalid = |reason: String| ConfigError::Rule(entry.to_owned(), reason);

    if entry.eq_ignore_ascii_case("aws") {
        return Ok(Endpoint::Aws);
    }
    let url = Url::parse(entry).map_err(|error| {
        invalid(format!(
            "{error}; expected \"aws\" or a url like https://minio.example.com"
        ))
    })?;
    if !ENDPOINT_SCHEMES.contains(&url.scheme()) {
        return Err(invalid(format!(
            "scheme {:?} is not one an endpoint can have; use {}",
            url.scheme(),
            ENDPOINT_SCHEMES.join(" or ")
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
        if url.scheme() != "file" {
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

fn is_loopback(host: &Host<&str>) -> bool {
    match host {
        // `.localhost` is reserved for the loopback interface, and a resolver is free
        // to answer for all of it.
        Host::Domain(name) => {
            let name = name.trim_end_matches('.').to_ascii_lowercase();
            name == "localhost" || name.ends_with(".localhost")
        }
        // Unspecified as well as loopback: connecting to 0.0.0.0 reaches this machine.
        Host::Ipv4(ip) => ip.is_loopback() || ip.is_unspecified(),
        Host::Ipv6(ip) => ip.is_loopback() || ip.is_unspecified(),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use crate::config::{LocalConfig, S3Config};

    use super::*;

    fn policy(config: &AccessConfig) -> AccessPolicy {
        AccessPolicy::new(config).unwrap()
    }

    fn with_endpoints(entries: &[&str]) -> AccessPolicy {
        policy(&AccessConfig {
            s3: S3Config {
                endpoints: Some(entries.iter().map(|e| (*e).to_owned()).collect()),
            },
            ..Default::default()
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
        assert!(policy.authorize_s3_endpoint(None).is_ok());
        assert!(
            policy
                .authorize_s3_endpoint(endpoint(Some("https://minio.example.com")).as_ref())
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
                .authorize_s3_endpoint(endpoint(Some("https://minio.example.com")).as_ref())
                .is_ok()
        );
        // A default port is the same endpoint spelled out.
        assert!(
            policy
                .authorize_s3_endpoint(endpoint(Some("https://minio.example.com:443")).as_ref())
                .is_ok()
        );
        for refused in [
            "https://evil.example.com",
            "http://minio.example.com",
            "https://minio.example.com:9000",
            "https://minio.example.com.evil.net",
        ] {
            let error = policy
                .authorize_s3_endpoint(endpoint(Some(refused)).as_ref())
                .unwrap_err();
            assert!(matches!(error, ApiError::Forbidden(_)), "{refused}");
        }
    }

    /// A list that does not say "aws" does not allow AWS, and a url with no endpoint
    /// option is exactly that request.
    #[test]
    fn aws_is_an_endpoint_like_any_other() {
        let only_minio = with_endpoints(&["https://minio.example.com"]);
        let error = only_minio.authorize_s3_endpoint(None).unwrap_err();
        assert!(error.to_string().contains("aws"), "{error}");

        let only_aws = with_endpoints(&["aws"]);
        assert!(only_aws.authorize_s3_endpoint(None).is_ok());
        assert!(
            only_aws
                .authorize_s3_endpoint(endpoint(Some("https://minio.example.com")).as_ref())
                .is_err()
        );
    }

    #[test]
    fn an_empty_endpoint_list_turns_s3_off() {
        let policy = with_endpoints(&[]);
        let error = policy.authorize(&url("s3://bucket/k.parquet")).unwrap_err();
        assert!(matches!(error, ApiError::Forbidden(_)), "{error}");
        assert!(policy.allowed_schemes().is_empty());
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
                .authorize_s3_endpoint(endpoint(Some(raw)).as_ref())
                .unwrap_err();
            assert!(matches!(error, ApiError::Forbidden(_)), "{raw}");
            assert!(error.to_string().contains("loopback"), "{raw}: {error}");
        }
        // Not loopback, whatever the name suggests.
        assert!(
            policy
                .authorize_s3_endpoint(endpoint(Some("https://localhost.example.com")).as_ref())
                .is_ok()
        );
    }

    #[test]
    fn the_loopback_interface_can_be_turned_on() {
        let policy = policy(&AccessConfig {
            allow_loopback: true,
            ..Default::default()
        });
        assert!(
            policy
                .authorize_s3_endpoint(endpoint(Some("http://127.0.0.1:9000")).as_ref())
                .is_ok()
        );
    }

    /// Naming an endpoint in the config is the operator pointing at it deliberately,
    /// which is all `allow_loopback` was ever standing in for.
    #[test]
    fn a_named_loopback_endpoint_needs_no_further_permission() {
        let policy = with_endpoints(&["http://127.0.0.1:9000"]);
        assert!(!policy.allow_loopback);
        assert!(
            policy
                .authorize_s3_endpoint(endpoint(Some("http://127.0.0.1:9000")).as_ref())
                .is_ok()
        );
        assert!(
            policy
                .authorize_s3_endpoint(endpoint(Some("http://127.0.0.1:9001")).as_ref())
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
        ] {
            let config = AccessConfig {
                s3: S3Config {
                    endpoints: Some(vec![entry.to_owned()]),
                },
                ..Default::default()
            };
            assert!(
                AccessPolicy::new(&config).is_err(),
                "{entry} was accepted as an endpoint"
            );
        }

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

    #[test]
    fn the_schemes_a_policy_serves_are_the_ones_it_was_given() {
        let (_dir, root) = temp_dir();
        assert_eq!(AccessPolicy::default().allowed_schemes(), ["s3"]);
        assert_eq!(
            with_paths(&[&root], false).allowed_schemes(),
            ["s3", "file"]
        );
        assert_eq!(with_endpoints(&[]).allowed_schemes(), Vec::<&str>::new());
    }
}
