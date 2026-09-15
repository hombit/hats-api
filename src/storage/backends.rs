//! Each backend's builder, and the one place a builder becomes a store.
//!
//! A backend function is handed its own group of options and a [`Reach`], and hands back a
//! configured builder; [`remote_store`] puts the policy's transport on it. Where a request may
//! connect — the endpoint, the address behind it, whether cleartext is acceptable — is decided
//! here before anything is built.

use base64::Engine;
use http::{HeaderMap, HeaderValue};
use object_store_opendal::OpendalStore;
use opendal::layers::RetryLayer;
use opendal::{HttpTransport, HttpTransporter, OperationContext, Operator, services};
use secrecy::ExposeSecret;
use url::Url;

use crate::access::{AccessPolicy, Backend, EndpointScheme, describe_endpoint_schemes};
use crate::error::ApiError;
use crate::storage::options::{
    AzureOptions, GcsOptions, S3Options, StorageOptions, WebdavOptions, WebdavTransport,
};
use crate::storage::store::file_url;

/// S3 offers no way to discover a bucket's region, and object_store will not guess.
pub const DEFAULT_S3_REGION: &str = "us-east-1";

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

fn parse_endpoint(endpoint: &str) -> Result<Url, ApiError> {
    Url::parse(endpoint)
        .map_err(|error| ApiError::bad_request(format!("invalid endpoint {endpoint:?}: {error}")))
}

/// What a backend function needs besides its own options: where it may connect, and whether
/// anything in the request is a credential.
///
/// A bundle rather than the whole of [`StorageOptions`], so a backend function is handed its
/// own group and this, and cannot reach another backend's fields at all. `credentials` spans
/// every group by design — it is the answer to "is there a secret in this request", which is
/// what the cleartext rule turns on, and no one group can answer it.
#[derive(Clone, Copy)]
pub(super) struct Reach<'a> {
    endpoint: Option<&'a str>,
    allow_http: bool,
    credentials: bool,
    policy: &'a AccessPolicy,
}

impl<'a> Reach<'a> {
    pub(super) fn of(options: &'a StorageOptions, policy: &'a AccessPolicy) -> Self {
        Self {
            endpoint: options.endpoint.as_deref(),
            allow_http: options.allow_http,
            credentials: options.has_credentials(),
            policy,
        }
    }
}

/// Which server this request would have us talk to, decided before anything is built.
/// `None` is a request with no `endpoint` option, which means the provider's own
/// service — and naming no endpoint is a choice the policy gets to refuse too.
fn resolve_endpoint(backend: Backend, reach: Reach<'_>) -> Result<Option<Url>, ApiError> {
    let Some(raw) = reach.endpoint else {
        reach.policy.authorize_endpoint(backend, None)?;
        // A provider's own service is https, so there is nothing here for `allow_http`
        // to permit, and a caller who set it has misunderstood what it does.
        if reach.allow_http {
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
    reach.policy.authorize_endpoint(backend, Some(&endpoint))?;
    allow_cleartext(&endpoint, scheme, reach.allow_http, reach.credentials)?;
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
/// to whatever it is given, which is the whole of what [`crate::access::network`] exists to stop.
#[expect(
    clippy::disallowed_methods,
    reason = "the one permitted call; the lint exists to send every other one here"
)]
pub(super) fn remote_store(
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

pub(super) fn s3_builder(
    url: &Url,
    options: &S3Options,
    reach: Reach<'_>,
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

    builder = match resolve_endpoint(Backend::S3, reach)? {
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

pub(super) fn gcs_builder(
    url: &Url,
    options: &GcsOptions,
    reach: Reach<'_>,
) -> Result<services::Gcs, ApiError> {
    let mut builder = services::Gcs::default()
        .bucket(host(url)?)
        // The request is the only source of credentials. Without these two, OpenDAL
        // reads `GOOGLE_APPLICATION_CREDENTIALS`, `~/.config/gcloud` and the GCE
        // metadata server — so a caller who sent none would be answered with the
        // service's own identity and every bucket this deployment can reach.
        .disable_config_load()
        .disable_vm_metadata();

    if let Some(endpoint) = resolve_endpoint(Backend::Gcs, reach)? {
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

pub(super) fn azblob_builder(
    url: &Url,
    options: &AzureOptions,
    reach: Reach<'_>,
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

    builder = match resolve_endpoint(Backend::Azure, reach)? {
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
pub(super) fn http_builder(url: &Url, reach: Reach<'_>) -> Result<services::Http, ApiError> {
    let origin = origin(url)?;
    // The url *is* the endpoint here, so this is the same gate the other backends reach
    // through their `endpoint` option — asked about the address the caller wrote.
    reach
        .policy
        .authorize_endpoint(Backend::Http, Some(&origin))?;
    // The operator has already allowed cleartext by this point, or the line above
    // refused it. This is the other half, and it is the caller's: `headers` may carry a
    // token, and whether that goes out in the clear is not the operator's to decide.
    allow_cleartext(
        &origin,
        require_endpoint_scheme(&origin)?,
        reach.allow_http,
        reach.credentials,
    )?;
    // `Url` prints an empty path as a trailing `/`, and OpenDAL joins the endpoint to a
    // key that already starts with one. Left in, every request would go to `//key`.
    Ok(services::Http::default().endpoint(origin.as_str().trim_end_matches('/')))
}

/// Turn WebDAV's Basic authentication into request headers. The materialization probe
/// and the WebDAV operator share this map, so they authenticate identically.
pub(super) fn webdav_headers(options: &WebdavOptions) -> Result<HeaderMap, ApiError> {
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
pub(super) fn webdav_builder(
    url: &Url,
    options: &WebdavOptions,
    reach: Reach<'_>,
) -> Result<services::Webdav, ApiError> {
    let endpoint = webdav_endpoint(url, options)?;
    // The url is the endpoint here, as it is for http, so this is the same gate the
    // provider-backed backends reach through their `endpoint` option.
    reach
        .policy
        .authorize_endpoint(Backend::Webdav, Some(&endpoint))?;
    // The caller's half of the cleartext decision: a username and password over `http`
    // are Basic authentication in the clear, which is the credential itself and not
    // merely a token derived from it.
    allow_cleartext(
        &endpoint,
        require_endpoint_scheme(&endpoint)?,
        reach.allow_http,
        reach.credentials,
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
pub(super) fn webdav_endpoint(url: &Url, options: &WebdavOptions) -> Result<Url, ApiError> {
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
pub(super) fn origin(url: &Url) -> Result<Url, ApiError> {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::storage::store::tests::{SECRET, no_options, open, options, store_key, transfers};
    use crate::storage::{self, RemoteFile, parse_url};

    use super::*;

    /// Azure account keys are base64, and OpenDAL rejects one that is not at build
    /// time — so a test about anything else needs a well-formed one.
    const AZURE_KEY: &str = "c2VjcmV0LWF6dXJlLWFjY291bnQta2V5";

    /// The same as `open`, for a server configured to let requests reach the loopback
    /// interface — which is what running against a local MinIO means.
    fn open_loopback(url: &Url, options: &StorageOptions) -> Result<RemoteFile, ApiError> {
        let config = crate::config::AccessConfig {
            network: crate::config::NetworkConfig {
                allow_loopback: true,
                ..Default::default()
            },
            ..Default::default()
        };
        storage::open(
            url,
            options,
            &AccessPolicy::new(&config, Arc::default()).unwrap(),
            &transfers(),
        )
    }

    #[test]
    fn an_azure_url_without_an_account_says_so() {
        let url = parse_url("az://container/key.parquet").unwrap();
        let error = open(&url, &no_options()).unwrap_err();
        assert!(matches!(error, ApiError::BadRequest(_)), "{error}");
        assert!(error.to_string().contains("account"), "{error}");
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
    /// [`crate::access::network`]'s own test.
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

    #[test]
    fn a_webdav_url_defaults_to_https() {
        let url = parse_url("webdav://data.example.com/hats/part0.parquet").unwrap();
        let file = open(&url, &no_options()).unwrap();
        assert_eq!(store_key(&file), "webdav://data.example.com");
        assert_eq!(
            webdav_endpoint(&url, &no_options().webdav)
                .unwrap()
                .as_str(),
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
        assert!(storage::open(&url, &cleartext, &policy, &transfers()).is_ok());
        assert_eq!(
            webdav_endpoint(&url, &cleartext.webdav).unwrap().as_str(),
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
        let headers = webdav_headers(&basic.webdav).unwrap();
        assert!(headers.contains_key(http::header::AUTHORIZATION));
        assert!(!format!("{headers:?}").contains(SECRET));
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
        let file = storage::open(&url, &no_options(), &policy, &transfers()).unwrap();
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

        let error = storage::open(&url, &with_token, &policy, &transfers()).unwrap_err();
        assert!(error.to_string().contains("cleartext"), "{error}");
        assert!(error.to_string().contains("allow_http"), "{error}");
        assert!(!error.to_string().contains(SECRET), "leaked: {error}");

        let allowed = options(serde_json::json!({
            "headers": {"Authorization": format!("Bearer {SECRET}")},
            "allow_http": true,
        }));
        assert!(storage::open(&url, &allowed, &policy, &transfers()).is_ok());

        // And with no headers there is no secret to protect, so cleartext is fine.
        let anonymous = options(serde_json::json!({}));
        assert!(storage::open(&url, &anonymous, &policy, &transfers()).is_ok());
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
        let file = storage::open(
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
}
