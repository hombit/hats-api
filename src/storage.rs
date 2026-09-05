//! Turning a user-supplied URL into something DataFusion can read.
//!
//! Everything storage-specific lives here. The rest of the service only ever sees a
//! [`RemoteFile`]; it does not know that S3 exists, that S3 needs a region, or that a
//! region has to be asked for. Adding a backend means adding a match arm to [`open`], a
//! name to [`SUPPORTED_SCHEMES`], an option list, and a [`Backend`] variant.
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

use crate::access::{AccessPolicy, Backend, Target};
use crate::error::ApiError;

/// Schemes [`open`] can serve today. Whether a given URL in one of them may actually be
/// read is the [`AccessPolicy`]'s business, not this list's.
pub const SUPPORTED_SCHEMES: &[&str] = &["s3", "gs", "az", "file"];

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
}

/// Options every remote backend takes, since every one of them has a host to name and a
/// cleartext decision to make about it.
const SHARED_OPTIONS: &[&str] = &["endpoint", "allow_http"];
const S3_OPTIONS: &[&str] = &[
    "region",
    "access_key_id",
    "secret_access_key",
    "session_token",
];
const GCS_OPTIONS: &[&str] = &["service_account_key", "access_token"];
const AZURE_OPTIONS: &[&str] = &["account", "access_key", "sas_token"];

impl StorageOptions {
    /// Every option, under the name a request spells it, and whether it is set.
    ///
    /// The per-scheme check is driven off this one list rather than a match per scheme,
    /// so a field added without a scheme to belong to is refused everywhere instead of
    /// being quietly accepted everywhere.
    fn named(&self) -> [(&'static str, bool); 11] {
        [
            ("endpoint", self.endpoint.is_some()),
            ("allow_http", self.allow_http),
            ("region", self.region.is_some()),
            ("access_key_id", self.access_key_id.is_some()),
            ("secret_access_key", self.secret_access_key.is_some()),
            ("session_token", self.session_token.is_some()),
            ("service_account_key", self.service_account_key.is_some()),
            ("access_token", self.access_token.is_some()),
            ("account", self.account.is_some()),
            ("access_key", self.access_key.is_some()),
            ("sas_token", self.sas_token.is_some()),
        ]
    }

    /// Nothing set at all, which is what a public object needs.
    pub fn is_empty(&self) -> bool {
        self.named().iter().all(|(_, set)| !set)
    }

    /// A `file://` url with a `secret_access_key`, or an `s3://` one with a `sas_token`,
    /// is a caller who has the wrong url or the wrong options; either reading is worth
    /// saying rather than guessing at, and one of them misdirects a credential.
    fn for_scheme(&self, scheme: &str) -> Result<(), ApiError> {
        let Some(accepted) = accepted_options(scheme) else {
            return match self.is_empty() {
                true => Ok(()),
                false => Err(ApiError::bad_request(format!(
                    "storage options are not accepted for {scheme:?} urls, which have none"
                ))),
            };
        };
        match self
            .named()
            .into_iter()
            .find(|(name, set)| *set && !accepted.contains(name))
        {
            None => Ok(()),
            Some((name, _)) => Err(ApiError::bad_request(format!(
                "option {name:?} is not one {scheme:?} urls take; they take {}",
                accepted.join(", ")
            ))),
        }
    }

    /// Whether anything here would be sent to the store as proof of identity — which is
    /// the whole of what [`allow_http_endpoint`] is protecting.
    fn has_credentials(&self) -> bool {
        self.access_key_id.is_some()
            || self.secret_access_key.is_some()
            || self.session_token.is_some()
            || self.service_account_key.is_some()
            || self.access_token.is_some()
            || self.access_key.is_some()
            || self.sas_token.is_some()
    }
}

/// The options a scheme takes, or `None` for a scheme that takes none at all.
fn accepted_options(scheme: &str) -> Option<Vec<&'static str>> {
    let backend = Backend::from_scheme(scheme)?;
    let specific = match backend {
        Backend::S3 => S3_OPTIONS,
        Backend::Gcs => GCS_OPTIONS,
        Backend::Azure => AZURE_OPTIONS,
    };
    Some([SHARED_OPTIONS, specific].concat())
}

/// The options this url's scheme takes, for an error message that has to name them
/// before the scheme is known to be one we serve.
fn options_for(url: &Url) -> String {
    match accepted_options(url.scheme()) {
        Some(options) => options.join(", "),
        None => SHARED_OPTIONS.join(", "),
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
            let store: Arc<dyn ObjectStore> = match Backend::from_scheme(url.scheme()) {
                Some(Backend::S3) => Arc::new(s3_store(url, options, policy)?),
                Some(Backend::Gcs) => Arc::new(gcs_store(url, options, policy)?),
                Some(Backend::Azure) => Arc::new(azblob_store(url, options, policy)?),
                // Every supported remote scheme has an arm above, and `file` went to
                // the local branch.
                None => {
                    let scheme = url.scheme();
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
            options_for(url)
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
            "url {} has a query string; storage options go in \"storage\", which takes {}",
            file_url(url),
            options_for(url)
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

/// Which server this request would have us talk to, decided before anything is built.
/// `None` is a request with no `endpoint` option, which means the provider's own
/// service — and naming no endpoint is a choice the policy gets to refuse too.
fn resolve_endpoint(
    backend: Backend,
    options: &StorageOptions,
    policy: &AccessPolicy,
) -> Result<Option<Url>, ApiError> {
    let endpoint = options
        .endpoint
        .as_deref()
        .map(parse_endpoint)
        .transpose()?;
    policy.authorize_endpoint(backend, endpoint.as_ref())?;
    match &endpoint {
        Some(endpoint) => {
            allow_http_endpoint(endpoint, options.allow_http, options.has_credentials())?;
        }
        // A provider's own service is https, so there is nothing here for `allow_http`
        // to permit, and a caller who set it has misunderstood what it does.
        None if options.allow_http => {
            return Err(ApiError::bad_request(
                "allow_http only applies together with endpoint",
            ));
        }
        None => {}
    }
    Ok(endpoint)
}

/// Anything that ends up inside a hostname the service then connects to has to be
/// checked before it gets there. `format!` does not care that a `/` in the middle of
/// what was meant to be a subdomain moves the host to whatever came before it, so a
/// region or an account name is a way past the endpoint policy unless it is restricted
/// to characters that cannot mean anything else.
fn require_label(name: &str, value: &str, extra: &str) -> Result<(), ApiError> {
    let ok = !value.is_empty()
        && value.len() <= 63
        && value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || extra.contains(c));
    match ok {
        true => Ok(()),
        false => Err(ApiError::bad_request(format!(
            "{name} {value:?} is not a name this backend has: it becomes part of a \
             hostname, so it may only hold lowercase letters, digits{}",
            match extra.is_empty() {
                true => String::new(),
                false => format!(" and {extra:?}"),
            }
        ))),
    }
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
    let region = options.region.as_deref().unwrap_or(DEFAULT_S3_REGION);
    // Virtual-host addressing puts the region in the hostname, so a region is one of
    // the strings `require_label` exists for.
    require_label("region", region, "-")?;

    let mut builder = services::S3::default()
        .bucket(bucket)
        .region(region)
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
    Ok(OpendalStore::new(Operator::new(builder)?.layer(retries())))
}

fn gcs_store(
    url: &Url,
    options: &StorageOptions,
    policy: &AccessPolicy,
) -> Result<OpendalStore, ApiError> {
    install_http_transport();

    let mut builder = services::Gcs::default()
        .bucket(authority(url)?)
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
                "service_account_key and access_token are two ways to say who is asking; \
                 give one",
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
    Ok(OpendalStore::new(Operator::new(builder)?.layer(retries())))
}

fn azblob_store(
    url: &Url,
    options: &StorageOptions,
    policy: &AccessPolicy,
) -> Result<OpendalStore, ApiError> {
    install_http_transport();

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
    require_label("account", account, "")?;

    let mut builder = services::Azblob::default()
        .container(authority(url)?)
        .account_name(account);

    builder = match resolve_endpoint(Backend::Azure, options, policy)? {
        // Azurite and the rest are addressed as they are written; the account is still
        // sent, because it is half of the shared-key signature.
        Some(endpoint) => builder.endpoint(endpoint.as_str()),
        None => builder.endpoint(&format!("https://{account}.blob.core.windows.net")),
    };

    builder = match (&options.access_key, &options.sas_token) {
        (Some(_), Some(_)) => {
            return Err(ApiError::bad_request(
                "access_key and sas_token are two ways to say who is asking; give one",
            ));
        }
        (Some(key), None) => builder.account_key(key.expose_secret()),
        (None, Some(token)) => builder.sas_token(token.expose_secret()),
        // No credentials given: ask anonymously. Azure's own signer would otherwise
        // reach for `AZURE_*` and the managed-identity endpoint, which is the service's
        // identity rather than the caller's.
        (None, None) => builder.skip_signature(),
    };
    Ok(OpendalStore::new(Operator::new(builder)?.layer(retries())))
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
            assert!(error.to_string().contains("hostname"), "{option}: {error}");
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
            assert!(error.to_string().contains("give one"), "{raw}: {error}");
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
