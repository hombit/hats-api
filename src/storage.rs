//! Turning a user-supplied URL into something DataFusion can read.
//!
//! Everything storage-specific lives here. The rest of the service only ever sees a
//! [`RemoteFile`]; it does not know that S3 exists, that S3 needs a region, or that a
//! region has to be asked for. Adding HTTPS, GCS or Azure later means adding a match
//! arm to [`open`] and a name to [`SUPPORTED_SCHEMES`].
//!
//! Storage-specific options travel in the URL's own query string, where storage
//! details belong, e.g. `s3://bucket/key.parquet?region=us-west-2`. Credentials are
//! such options. They never leave this module: the URL the rest of the service sees
//! and logs has the query string stripped, and error messages are built from that
//! stripped URL so a secret cannot escape in a 400.
//!
//! Which URLs may be opened at all is not decided here: [`open`] asks the
//! [`AccessPolicy`] first, and every path into a store goes through that one call.

use std::path::Path as FilePath;
use std::sync::Arc;

use object_store::{ObjectStore, local::LocalFileSystem};
use object_store_opendal::OpendalStore;
use opendal::{Operator, services};
use url::Url;

use crate::access::{AccessPolicy, Target};
use crate::error::ApiError;

/// Schemes [`open`] can serve today. Whether a given URL in one of them may actually be
/// read is the [`AccessPolicy`]'s business, not this list's.
pub const SUPPORTED_SCHEMES: &[&str] = &["s3", "file"];

/// S3 offers no way to discover a bucket's region, and object_store will not guess.
pub const DEFAULT_S3_REGION: &str = "us-east-1";

const S3_OPTIONS: &[&str] = &[
    "region",
    "endpoint",
    "allow_http",
    "access_key_id",
    "secret_access_key",
    "session_token",
];

/// An opened remote file: the store it lives in, the key DataFusion registers that
/// store under, and the file's own URL stripped of any storage options.
#[derive(Debug)]
pub struct RemoteFile {
    pub store: Arc<dyn ObjectStore>,
    pub base: Url,
    pub url: Url,
}

pub fn open(url: &Url, policy: &AccessPolicy) -> Result<RemoteFile, ApiError> {
    require_object_key(url)?;
    if !SUPPORTED_SCHEMES.contains(&url.scheme()) {
        return Err(ApiError::bad_request(format!(
            "unsupported URL scheme {:?}: supported schemes are {}",
            url.scheme(),
            SUPPORTED_SCHEMES.join(", ")
        )));
    }
    // Before anything is built, and before the filesystem is touched.
    match policy.authorize(url)? {
        Target::Local(path) => local_file(&path),
        Target::Remote => {
            let store: Arc<dyn ObjectStore> = match url.scheme() {
                "s3" => Arc::new(s3_store(url, policy)?),
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

/// The URL without its storage options, which are ours and not part of the object key.
fn file_url(url: &Url) -> Url {
    let mut file = url.clone();
    file.set_query(None);
    file.set_fragment(None);
    file
}

fn authority(url: &Url) -> Result<&str, ApiError> {
    url.host_str()
        .filter(|host| !host.is_empty())
        .ok_or_else(|| ApiError::bad_request(format!("url {} has no host", file_url(url))))
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

#[derive(Default)]
struct S3Options {
    region: Option<String>,
    /// Base URL of a non-AWS S3 implementation (MinIO, Ceph, R2, ...).
    endpoint: Option<String>,
    allow_http: bool,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
}

fn s3_options(url: &Url) -> Result<S3Options, ApiError> {
    let mut options = S3Options::default();
    for (key, value) in url.query_pairs() {
        let value = value.into_owned();
        match key.as_ref() {
            "region" => options.region = Some(value),
            "endpoint" => options.endpoint = Some(value),
            "allow_http" => options.allow_http = parse_bool("allow_http", &value)?,
            "access_key_id" => options.access_key_id = Some(value),
            "secret_access_key" => options.secret_access_key = Some(value),
            "session_token" => options.session_token = Some(value),
            // The name is safe to echo; a value never is.
            other => {
                return Err(ApiError::bad_request(format!(
                    "unknown s3 option {other:?} in url; supported options are {}",
                    S3_OPTIONS.join(", ")
                )));
            }
        }
    }
    Ok(options)
}

fn parse_bool(name: &str, value: &str) -> Result<bool, ApiError> {
    value
        .parse()
        .map_err(|_| ApiError::bad_request(format!("{name} must be true or false, got {value:?}")))
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

fn s3_store(url: &Url, policy: &AccessPolicy) -> Result<OpendalStore, ApiError> {
    install_http_transport();
    let bucket = authority(url)?;
    let options = s3_options(url)?;

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

    let has_credentials = options.access_key_id.is_some()
        || options.secret_access_key.is_some()
        || options.session_token.is_some();

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
            allow_http_endpoint(endpoint, options.allow_http, has_credentials)?;
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

    builder = match (options.access_key_id, options.secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => {
            let builder = builder
                .access_key_id(&access_key_id)
                .secret_access_key(&secret_access_key);
            match options.session_token {
                Some(token) => builder.session_token(&token),
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
    Ok(OpendalStore::new(Operator::new(builder)?))
}

pub fn parse_url(raw: &str) -> Result<Url, ApiError> {
    // An absolute local path is not a URL, but it is what someone with a local file in
    // front of them will type, and it has exactly one reading.
    if raw.starts_with('/') {
        return Url::from_file_path(raw)
            .map_err(|()| ApiError::bad_request(format!("invalid local path {raw:?}")));
    }
    Url::parse(raw).map_err(|error| {
        // Unparseable, so there is no query string to strip properly; cut at the first
        // `?` so a secret cannot ride out in the error.
        let shown = raw.split('?').next().unwrap_or_default();
        ApiError::bad_request(format!("invalid url {shown:?}: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// These tests are about reading the URL, not about the policy, so they all run
    /// under one that allows every bucket. What the policy itself allows is
    /// [`crate::access`]'s own business, and tested there.
    fn open(url: &Url) -> Result<RemoteFile, ApiError> {
        super::open(url, &AccessPolicy::default())
    }

    /// The same, for a server configured to let requests reach the loopback interface
    /// — which is what running against a local MinIO means.
    fn open_loopback(url: &Url) -> Result<RemoteFile, ApiError> {
        let config = crate::config::AccessConfig {
            allow_loopback: true,
            ..Default::default()
        };
        super::open(url, &AccessPolicy::new(&config).unwrap())
    }

    #[test]
    fn opens_an_s3_url() {
        let url = parse_url("s3://bucket/some/key.parquet").unwrap();
        let file = open(&url).unwrap();
        assert_eq!(file.base.as_str(), "s3://bucket");
        assert_eq!(file.url.as_str(), "s3://bucket/some/key.parquet");
    }

    #[test]
    fn storage_options_stay_out_of_the_object_key() {
        let url = parse_url("s3://bucket/key.parquet?region=us-west-2").unwrap();
        let file = open(&url).unwrap();
        assert_eq!(file.url.as_str(), "s3://bucket/key.parquet");
    }

    const SECRET: &str = "wJalrXUtnFEMIsecretKEY";

    #[test]
    fn accepts_credentials_as_storage_options() {
        let url = parse_url(&format!(
            "s3://bucket/key.parquet?access_key_id=AKIA123&secret_access_key={SECRET}\
             &session_token=tok&region=us-west-2"
        ))
        .unwrap();
        let file = open(&url).unwrap();
        assert_eq!(file.url.as_str(), "s3://bucket/key.parquet");
    }

    #[test]
    fn accepts_a_custom_endpoint() {
        let url = parse_url("s3://data/key.parquet?endpoint=https://minio.example.com").unwrap();
        let file = open(&url).unwrap();
        assert_eq!(file.url.as_str(), "s3://data/key.parquet");
        assert_eq!(file.base.as_str(), "s3://data");
    }

    #[test]
    fn anonymous_requests_may_use_a_plain_http_endpoint() {
        let url = parse_url("s3://data/key.parquet?endpoint=http://minio.example.com").unwrap();
        assert!(open(&url).is_ok());
    }

    /// The usual local-MinIO endpoint is on the loopback interface, which the caller
    /// does not get to reach unless the server was configured for it.
    #[test]
    fn a_loopback_endpoint_needs_the_server_to_allow_it() {
        let url = parse_url("s3://data/key.parquet?endpoint=http://127.0.0.1:9000").unwrap();
        let error = open(&url).unwrap_err();
        assert!(matches!(error, ApiError::Forbidden(_)), "{error}");
        assert!(error.to_string().contains("loopback"), "{error}");
        assert!(open_loopback(&url).is_ok());
    }

    #[test]
    fn credentials_over_a_plain_http_endpoint_need_saying_so() {
        let with_creds = format!(
            "endpoint=http://127.0.0.1:9000&access_key_id=AKIA123&secret_access_key={SECRET}"
        );
        let url = parse_url(&format!("s3://data/key.parquet?{with_creds}")).unwrap();
        let error = open_loopback(&url).unwrap_err();
        assert!(error.to_string().contains("cleartext"), "{error}");
        assert!(error.to_string().contains("allow_http=true"), "{error}");
        assert!(!error.to_string().contains(SECRET), "leaked: {error}");

        let url = parse_url(&format!(
            "s3://data/key.parquet?{with_creds}&allow_http=true"
        ))
        .unwrap();
        assert!(open_loopback(&url).is_ok());
    }

    #[test]
    fn rejects_nonsense_endpoints_and_flags() {
        for (query, expected) in [
            ("endpoint=ftp://host", "expected http or https"),
            ("endpoint=not a url", "invalid endpoint"),
            ("allow_http=yes&endpoint=https://h", "must be true or false"),
            ("allow_http=true", "only applies together with endpoint"),
        ] {
            let url = parse_url(&format!("s3://data/key.parquet?{query}")).unwrap();
            let error = open(&url).unwrap_err();
            assert!(error.to_string().contains(expected), "{query}: {error}");
        }
    }

    #[test]
    fn credentials_must_come_in_pairs() {
        for query in [
            "access_key_id=AKIA123",
            "secret_access_key=abc",
            "session_token=tok",
        ] {
            let url = parse_url(&format!("s3://bucket/key.parquet?{query}")).unwrap();
            let error = open(&url).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("access_key_id and secret_access_key")
                    || error.to_string().contains("session_token needs"),
                "{query}: {error}"
            );
        }
    }

    #[test]
    fn errors_never_carry_the_credentials() {
        // Every failure path that formats a url: no key, no host, unknown option.
        for raw in [
            &format!("s3://bucket?access_key_id=AKIA123&secret_access_key={SECRET}"),
            &format!("s3://bucket/key.parquet?nope=1&secret_access_key={SECRET}"),
            &format!("ftp://bucket/key.parquet?secret_access_key={SECRET}"),
        ] {
            let url = parse_url(raw).unwrap();
            let error = open(&url).unwrap_err().to_string();
            assert!(!error.contains(SECRET), "leaked in: {error}");
        }
    }

    #[test]
    fn rejects_unknown_storage_options_instead_of_ignoring_them() {
        let url = parse_url("s3://bucket/key.parquet?regoin=us-west-2").unwrap();
        let error = open(&url).unwrap_err();
        assert!(error.to_string().contains("unknown s3 option"), "{error}");
    }

    #[test]
    fn rejects_schemes_we_cannot_serve_yet() {
        let url = parse_url("https://example.com/a.parquet").unwrap();
        let error = open(&url).unwrap_err();
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
            let error = open(&url).unwrap_err();
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
    async fn request_head(options: &str) -> String {
        let (port, receiver) = capture_one_request();
        let url = parse_url(&format!(
            "s3://bucket/key.parquet?endpoint=http://127.0.0.1:{port}&{options}"
        ))
        .unwrap();
        let file = open_loopback(&url).unwrap();
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
        let head = request_head("").await;
        assert!(head.contains("get /bucket/key.parquet"), "{head}");
        assert!(!head.contains("authorization:"), "signed anyway: {head}");
        assert!(!head.contains("x-amz-security-token:"), "{head}");
    }

    /// And the other half: credentials in the url are the ones that sign. The secret
    /// itself never goes on the wire — SigV4 sends a signature and the key id.
    #[tokio::test]
    async fn credentials_from_the_url_are_the_ones_that_sign() {
        let head = request_head(&format!(
            "access_key_id=AKIA123&secret_access_key={SECRET}&allow_http=true"
        ))
        .await;
        assert!(head.contains("authorization:"), "unsigned: {head}");
        assert!(head.contains("akia123"), "{head}");
        assert!(
            !head.contains(&SECRET.to_ascii_lowercase()),
            "leaked: {head}"
        );
    }

    #[test]
    fn keeps_equals_signs_in_hats_paths() {
        let url = parse_url("s3://b/hats/Norder=5/Npix=12240/part0.parquet").unwrap();
        let file = open(&url).unwrap();
        assert_eq!(file.url.path(), url.path());
        assert!(file.url.path().contains("Norder=5"), "{}", file.url.path());
    }
}
