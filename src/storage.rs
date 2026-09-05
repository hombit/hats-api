//! Turning a user-supplied URL into something DataFusion can read.
//!
//! Everything storage-specific lives here. The rest of the service only ever sees a
//! [`RemoteFile`]; it does not know that S3 exists, that S3 needs a region, or that a
//! region has to be asked for. Adding HTTPS, GCS or Azure later means adding a match
//! arm to [`open`] and a name to [`SUPPORTED_SCHEMES`].
//!
//! Storage options arrive beside the URL as [`StorageOptions`], never inside it. A
//! URL's query string is the origin's — a presigned signature, a CDN token, part of
//! what identifies the bytes — and nothing could tell one of those from one of ours.
//! Options never leave this module, and a credential never reaches an error message.
//!
//! Which URLs may be opened at all is not decided here: [`open`] asks the
//! [`AccessPolicy`] first, and every path into a store goes through that one call.

use std::path::Path as FilePath;
use std::sync::Arc;

use object_store::{ObjectStore, local::LocalFileSystem};
use object_store_opendal::OpendalStore;
use opendal::layers::RetryLayer;
use opendal::{Operator, services};
use secrecy::{ExposeSecret, SecretString};
use url::Url;

use crate::access::{AccessPolicy, Target};
use crate::error::ApiError;

/// Schemes [`open`] can serve today. Whether a given URL in one of them may actually be
/// read is the [`AccessPolicy`]'s business, not this list's.
pub const SUPPORTED_SCHEMES: &[&str] = &["s3", "file"];

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
    pub region: Option<String>,
    /// Base URL of a non-AWS S3 implementation (MinIO, Ceph, R2, ...).
    pub endpoint: Option<String>,
    /// Permission to send credentials to a cleartext `endpoint`.
    #[serde(default)]
    pub allow_http: bool,
    pub access_key_id: Option<SecretString>,
    pub secret_access_key: Option<SecretString>,
    pub session_token: Option<SecretString>,
}

/// The option names, for saying which are accepted in an error.
const S3_OPTIONS: &[&str] = &[
    "region",
    "endpoint",
    "allow_http",
    "access_key_id",
    "secret_access_key",
    "session_token",
];

impl StorageOptions {
    /// Nothing set at all, which is what a public object needs.
    pub fn is_empty(&self) -> bool {
        self.region.is_none()
            && self.endpoint.is_none()
            && !self.allow_http
            && self.access_key_id.is_none()
            && self.secret_access_key.is_none()
            && self.session_token.is_none()
    }

    /// A `file://` url with a `secret_access_key` is a caller who has the wrong url or
    /// the wrong options; either reading is worth saying rather than guessing at.
    fn for_scheme(&self, scheme: &str) -> Result<(), ApiError> {
        match scheme {
            "s3" => Ok(()),
            _ if self.is_empty() => Ok(()),
            other => Err(ApiError::bad_request(format!(
                "storage options are not accepted for {other:?} urls, which have none"
            ))),
        }
    }

    fn has_credentials(&self) -> bool {
        self.access_key_id.is_some()
            || self.secret_access_key.is_some()
            || self.session_token.is_some()
    }
}

/// An opened remote file: the store it lives in, the key DataFusion registers that
/// store under, and the object's own URL.
pub struct RemoteFile {
    pub store: Arc<dyn ObjectStore>,
    pub base: Url,
    pub url: Url,
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

pub fn open(
    url: &Url,
    options: &StorageOptions,
    policy: &AccessPolicy,
) -> Result<RemoteFile, ApiError> {
    require_object_key(url)?;
    refuse_userinfo(url)?;
    refuse_query_string(url)?;
    if !SUPPORTED_SCHEMES.contains(&url.scheme()) {
        return Err(ApiError::bad_request(format!(
            "unsupported URL scheme {:?}: supported schemes are {}",
            url.scheme(),
            SUPPORTED_SCHEMES.join(", ")
        )));
    }
    options.for_scheme(url.scheme())?;
    // Before anything is built, and before the filesystem is touched.
    match policy.authorize(url)? {
        Target::Local(path) => local_file(&path),
        Target::Remote => {
            let store: Arc<dyn ObjectStore> = match url.scheme() {
                "s3" => Arc::new(s3_store(url, options, policy)?),
                // Every supported remote scheme has an arm above, and `file` went to
                // the local branch.
                scheme => {
                    return Err(ApiError::bad_request(format!(
                        "unsupported URL scheme {scheme:?}: supported schemes are {}",
                        SUPPORTED_SCHEMES.join(", ")
                    )));
                }
            };
            Ok(RemoteFile {
                store,
                base: base_url(url)?,
                url: file_url(url),
            })
        }
    }
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
    let url = Url::from_file_path(path).map_err(|()| {
        ApiError::bad_request(format!("{} is not a valid file url", path.display()))
    })?;
    Ok(RemoteFile {
        store: Arc::new(LocalFileSystem::new()),
        base: Url::parse("file://").expect("file:// is a valid url"),
        url,
    })
}

/// `scheme://authority` — DataFusion looks stores up by that, without the object path.
fn base_url(url: &Url) -> Result<Url, ApiError> {
    let authority = authority(url)?;
    Url::parse(&format!("{}://{authority}", url.scheme())).map_err(|error| {
        ApiError::bad_request(format!(
            "cannot derive store url from {}: {error}",
            file_url(url)
        ))
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

fn authority(url: &Url) -> Result<&str, ApiError> {
    url.host_str()
        .filter(|host| !host.is_empty())
        .ok_or_else(|| ApiError::bad_request(format!("url {} has no host", file_url(url))))
}

/// `s3://key:secret@bucket/object` would survive into `RemoteFile::url`, the url every
/// layer downstream logs. Refused rather than stripped, so a caller who meant it is
/// told where credentials go instead of getting an unexplained 403.
fn refuse_userinfo(url: &Url) -> Result<(), ApiError> {
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ApiError::bad_request(format!(
            "url {}://{} carries credentials in its authority; pass them as {} instead",
            url.scheme(),
            // Not `file_url`: that keeps the userinfo, which is the thing to not echo.
            url.host_str().unwrap_or_default(),
            S3_OPTIONS.join(", ")
        )));
    }
    Ok(())
}

/// An `s3://` key has no query string. Dropping one silently would turn a credentialed
/// read into an anonymous one that fails later and elsewhere. A scheme whose objects do
/// have query strings makes this a per-scheme decision.
fn refuse_query_string(url: &Url) -> Result<(), ApiError> {
    if url.query().is_some() {
        return Err(ApiError::bad_request(format!(
            "url {} has a query string; storage options go in \"storage\", which takes {}",
            file_url(url),
            S3_OPTIONS.join(", ")
        )));
    }
    Ok(())
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

/// Decide whether this endpoint may be spoken to over cleartext. The backend would not
/// ask — it just follows the endpoint's scheme — so this is the whole of the decision,
/// and it happens before a connection is opened rather than after one fails.
fn allow_http_endpoint(
    endpoint: &Url,
    allow_http: bool,
    has_credentials: bool,
) -> Result<(), ApiError> {
    match endpoint.scheme() {
        "https" => Ok(()),
        // Nothing to expose when the request is anonymous, and that is the common case
        // of a local MinIO or a test server.
        "http" if !has_credentials || allow_http => Ok(()),
        // Display, not Debug: Debug on a Url prints the whole parsed struct. The
        // endpoint is safe to echo either way — credentials are separate options.
        "http" => Err(ApiError::bad_request(format!(
            "endpoint {endpoint} is not https and credentials were given, which would \
             be sent in cleartext; pass allow_http=true to do it anyway"
        ))),
        scheme => Err(ApiError::bad_request(format!(
            "endpoint {endpoint} has scheme {scheme:?}, expected http or https"
        ))),
    }
}

/// OpenDAL has no HTTP client until one is installed process-wide, and a store built
/// without one fails on its first request rather than at build time. Installing is
/// idempotent and first-one-wins, so every remote backend calls this before building.
fn install_http_transport() {
    static INSTALLED: std::sync::Once = std::sync::Once::new();
    INSTALLED.call_once(opendal::install_default);
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

fn s3_store(
    url: &Url,
    options: &StorageOptions,
    policy: &AccessPolicy,
) -> Result<OpendalStore, ApiError> {
    install_http_transport();
    let bucket = authority(url)?;

    let mut builder = services::S3::default()
        .bucket(bucket)
        .region(options.region.as_deref().unwrap_or(DEFAULT_S3_REGION))
        // The request is the only source of credentials. Without these two, OpenDAL
        // reads `AWS_*` from the environment, `~/.aws/{config,credentials}`, and the
        // EC2 metadata service — so a caller who sent none would be answered with the
        // service's own identity and every bucket this deployment can reach.
        // `disable_config_load` also stops `AWS_ENDPOINT_URL` from redirecting a
        // request the policy already decided was going to AWS.
        .disable_config_load()
        .disable_ec2_metadata();

    // Which server this request would have us talk to is the policy's decision, and
    // naming no endpoint is a choice too: it means AWS.
    let endpoint = options
        .endpoint
        .as_deref()
        .map(parse_endpoint)
        .transpose()?;
    policy.authorize_s3_endpoint(endpoint.as_ref())?;

    builder = match &endpoint {
        // OpendDAL addresses path-style unless told otherwise, which is what an
        // S3-compatible server on a named endpoint wants: MinIO, Ceph and the rest
        // serve `endpoint/bucket/key`, and virtual-host style would need a wildcard
        // DNS entry per bucket. Cleartext is decided here rather than by OpenDAL,
        // which would simply follow the endpoint's own scheme.
        Some(endpoint) => {
            allow_http_endpoint(endpoint, options.allow_http, options.has_credentials())?;
            builder.endpoint(endpoint.as_str())
        }
        // AWS itself is the other way round: path-style addressing is deprecated
        // there, so a bucket is a subdomain.
        None => {
            if options.allow_http {
                return Err(ApiError::bad_request(
                    "allow_http only applies together with endpoint",
                ));
            }
            builder.enable_virtual_host_style()
        }
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
    Ok(OpendalStore::new(Operator::new(builder)?.layer(retries())))
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
        super::open(url, options, &AccessPolicy::default())
    }

    /// The same, for a server configured to let requests reach the loopback interface
    /// — which is what running against a local MinIO means.
    fn open_loopback(url: &Url, options: &StorageOptions) -> Result<RemoteFile, ApiError> {
        let config = crate::config::AccessConfig {
            allow_loopback: true,
            ..Default::default()
        };
        super::open(url, options, &AccessPolicy::new(&config).unwrap())
    }

    #[test]
    fn opens_an_s3_url() {
        let url = parse_url("s3://bucket/some/key.parquet").unwrap();
        let file = open(&url, &no_options()).unwrap();
        assert_eq!(file.base.as_str(), "s3://bucket");
        assert_eq!(file.url.as_str(), "s3://bucket/some/key.parquet");
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
        assert!(error.to_string().contains("not accepted"), "{error}");
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
        let url = parse_url("https://example.com/a.parquet").unwrap();
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
            // 404 rather than a hang or a reset: a retry would find nothing listening.
            let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
            let _ = sender.send(String::from_utf8_lossy(&head).into_owned());
        });
        (port, receiver)
    }

    /// The request head one `GET` through the store puts on the wire, lowercased —
    /// header names are case-insensitive and the comparisons below do not care.
    async fn request_head(mut option: serde_json::Value) -> String {
        let (port, receiver) = capture_one_request();
        option["endpoint"] = serde_json::json!(format!("http://127.0.0.1:{port}"));
        let url = parse_url("s3://bucket/key.parquet").unwrap();
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
        let head = request_head(serde_json::json!({})).await;
        assert!(head.contains("get /bucket/key.parquet"), "{head}");
        assert!(!head.contains("authorization:"), "signed anyway: {head}");
        assert!(!head.contains("x-amz-security-token:"), "{head}");
    }

    /// And the other half: the credentials the request carried are the ones that sign.
    /// The secret itself never goes on the wire — SigV4 sends a signature and the key
    /// id.
    #[tokio::test]
    async fn credentials_from_the_request_are_the_ones_that_sign() {
        let head = request_head(serde_json::json!({
            "access_key_id": "AKIA123",
            "secret_access_key": SECRET,
            "allow_http": true,
        }))
        .await;
        assert!(head.contains("authorization:"), "unsigned: {head}");
        assert!(head.contains("akia123"), "{head}");
        assert!(
            !head.contains(&SECRET.to_ascii_lowercase()),
            "leaked: {head}"
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

    #[test]
    fn keeps_equals_signs_in_hats_paths() {
        let url = parse_url("s3://b/hats/Norder=5/Npix=12240/part0.parquet").unwrap();
        let file = open(&url, &no_options()).unwrap();
        assert_eq!(file.url.path(), url.path());
        assert!(file.url.path().contains("Norder=5"), "{}", file.url.path());
    }
}
