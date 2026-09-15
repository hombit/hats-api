//! What a request says about how to reach its store: the options each backend takes, which
//! of them are credentials, and the refusal of every option the url's scheme has no use for.

use std::collections::BTreeMap;

use http::{HeaderMap, HeaderName, HeaderValue};
use secrecy::{ExposeSecret, SecretString};
use url::Url;

use crate::access::{BACKENDS, Backend};
use crate::error::ApiError;

/// How to reach the store. The url says which object to read; these say what is needed to get
/// at it — a server, a region, credentials.
///
/// The url's scheme decides which of them apply, so a body carries the options of one backend.
/// An option belonging to another is refused, and so is one no backend has.
//
// Flat on the wire, grouped in the type. The groups are what a backend function is handed, so
// `gcs_builder` cannot reach `sas_token` — which stops "the options s3 takes" from being two
// facts, the list and whatever the builder happens to read, that nothing keeps in step.
//
// `flatten` is why this cannot use `deny_unknown_fields`: serde ignores it on a struct that has
// one, and drops unmatched keys in silence. A misspelled `secret_acces_key` dropped that way is
// an anonymous request the caller reads as an authenticated one, so the leftovers are collected
// and refused by `for_scheme`. Anything added here must keep both halves — flat outside, and
// nothing dropped.
#[derive(Debug, Default, serde::Deserialize, utoipa::ToSchema)]
pub struct StorageOptions {
    /// Base URL of a server other than the provider's own: MinIO, Ceph, R2, Azurite. Taken by
    /// the backends addressed by bucket, and by no other: a url that is its own address has
    /// nothing for this to point elsewhere at.
    pub endpoint: Option<String>,
    /// Permission to send credentials to a server reached over cleartext HTTP. Taken by every
    /// backend: any of them can be handed a credential, and none may send one in the clear
    /// unless the caller who owns it said so.
    #[serde(default)]
    pub allow_http: bool,
    #[serde(flatten)]
    pub s3: S3Options,
    #[serde(flatten)]
    pub gcs: GcsOptions,
    #[serde(flatten)]
    pub azure: AzureOptions,
    #[serde(flatten)]
    pub http: HttpOptions,
    #[serde(flatten)]
    pub webdav: WebdavOptions,
    /// Every key the body carried that no backend has a field for.
    ///
    /// Filled by deserialization, and refused when the options are checked against the url's
    /// scheme. Not something to set: it is where a caller's mistakes are caught, and anything
    /// put here makes the request a 400. Public only because a struct literal elsewhere in the
    /// workspace cannot use `..Default::default()` without it.
    #[serde(flatten)]
    #[schema(ignore)]
    pub unknown: BTreeMap<String, serde::de::IgnoredAny>,
}

/// The two options that belong to no backend in particular. Named here because
/// `accepted_options` needs them and `StorageOptions::named` registers them, and those are the
/// two places that must agree.
const ENDPOINT: &str = "endpoint";
const ALLOW_HTTP: &str = "allow_http";

#[derive(Debug, Default, serde::Deserialize, utoipa::ToSchema)]
pub struct S3Options {
    /// The bucket's region. Defaults to `us-east-1` when not given.
    pub region: Option<String>,
    /// Access key id. Give it with `secret_access_key`; without both, the object is read
    /// anonymously.
    // A credential is a `SecretString` so that nothing prints it, and that is a type the
    // document has no way to derive from. Named as a string here, which is what it is on the
    // wire; the schema is what a caller sends, not how it is held.
    #[schema(value_type = Option<String>)]
    pub access_key_id: Option<SecretString>,
    /// Secret access key, alongside `access_key_id`.
    #[schema(value_type = Option<String>)]
    pub secret_access_key: Option<SecretString>,
    /// Session token, for temporary credentials. Only with the other two.
    #[schema(value_type = Option<String>)]
    pub session_token: Option<SecretString>,
}

#[derive(Debug, Default, serde::Deserialize, utoipa::ToSchema)]
pub struct GcsOptions {
    /// A GCS service account key: the JSON Google issues, base64-encoded.
    #[schema(value_type = Option<String>)]
    pub service_account_key: Option<SecretString>,
    /// A GCS OAuth2 access token, as an alternative to `service_account_key` for a caller who
    /// mints short-lived credentials of their own.
    #[schema(value_type = Option<String>)]
    pub access_token: Option<SecretString>,
}

#[derive(Debug, Default, serde::Deserialize, utoipa::ToSchema)]
pub struct AzureOptions {
    /// The Azure storage account the container is in. Required for `az://`: it is the
    /// host half of the address, which the url only carries the container half of.
    pub account: Option<String>,
    /// An Azure storage account key, base64 as Azure issues it.
    #[schema(value_type = Option<String>)]
    pub access_key: Option<SecretString>,
    /// An Azure shared access signature, as an alternative to `access_key`.
    #[schema(value_type = Option<String>)]
    pub sas_token: Option<SecretString>,
}

#[derive(Debug, Default, serde::Deserialize, utoipa::ToSchema)]
pub struct HttpOptions {
    /// Headers to send with every request to an `http(s)://` server, for a service that
    /// authenticates with one — a bearer token, an API key. Whatever is put here is treated as
    /// a credential.
    #[serde(default)]
    #[schema(value_type = std::collections::HashMap<String, String>)]
    pub headers: Headers,
}

#[derive(Debug, Default, serde::Deserialize, utoipa::ToSchema)]
pub struct WebdavOptions {
    /// The HTTP transport under a `webdav://` URL. HTTPS is the default.
    #[serde(default)]
    pub transport: Option<WebdavTransport>,
    /// The WebDAV username. Basic authentication is the only kind this backend takes, and both
    /// halves are required together: give this with `password`, or give neither.
    #[schema(value_type = Option<String>)]
    pub username: Option<SecretString>,
    /// The WebDAV password, alongside `username`.
    #[schema(value_type = Option<String>)]
    pub password: Option<SecretString>,
}

/// The transport a WebDAV server speaks. An HTTP transport must be named exactly in
/// `api.access.webdav.endpoints`; HTTPS is used when this option is absent.
#[derive(Debug, Clone, Copy, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum WebdavTransport {
    Http,
    Https,
}

impl WebdavTransport {
    pub(super) fn scheme(self) -> &'static str {
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
    pub(super) fn to_header_map(&self) -> Result<HeaderMap, ApiError> {
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

/// The names one group of options answers to.
///
/// Read off the group's own [`Group::named`] rather than written beside it, so "which options
/// are S3's" is one fact. A list written separately is one that can disagree with the fields a
/// builder actually reads, and the disagreement is silent in the direction that accepts an
/// option and never uses it.
fn names_of<G: Group + Default>() -> Vec<&'static str> {
    G::default()
        .named()
        .into_iter()
        .map(|option| option.name)
        .collect()
}

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
/// `backends::allow_cleartext` wave a credential through.
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

/// The options of the one backend a request is for.
///
/// A request names one url, the url's scheme names one backend, and that backend's options are
/// the only ones that mean anything — so past the point where the scheme is known, this is the
/// shape, and "s3 options and azure options at once" is not a value that exists.
///
/// [`StorageOptions`] stays a struct because it is the wire form, and the wire form is
/// deserialized before the scheme is known: the url is a sibling field, so serde has nothing to
/// pick a variant by. This is what that flat form is turned into, by [`StorageOptions::resolve`].
pub(super) enum Credentials<'a> {
    S3(&'a S3Options),
    Gcs(&'a GcsOptions),
    Azure(&'a AzureOptions),
    Http(&'a HttpOptions),
    Webdav(&'a WebdavOptions),
}

/// One backend's options, and the two things everything else needs of them.
///
/// Implemented by destructuring, which is what makes the guarantees compile-time: a field added
/// to a group and not registered in `named` does not compile, and a field registered under the
/// wrong [`Kind`] does not either, because [`Named::credential`] takes a [`SecretString`] and
/// [`Named::plain`] refuses one.
trait Group {
    /// Every option in this group, under the name a request spells it.
    fn named(&self) -> Vec<Named>;

    /// These options written back out for a plan whose caller asked to have them, credentials
    /// in the clear. Destructured for the same reason, with the guard running the other way:
    /// forgetting a field hands back a plan missing something the caller sent.
    fn echo_into(&self, out: &mut serde_json::Map<String, serde_json::Value>);
}

fn echo_plain(out: &mut serde_json::Map<String, serde_json::Value>, name: &str, value: &str) {
    out.insert(name.to_owned(), value.into());
}

fn echo_secret(
    out: &mut serde_json::Map<String, serde_json::Value>,
    name: &str,
    value: &Option<SecretString>,
) {
    if let Some(value) = value {
        echo_plain(out, name, value.expose_secret());
    }
}

impl Group for S3Options {
    fn named(&self) -> Vec<Named> {
        let Self {
            region,
            access_key_id,
            secret_access_key,
            session_token,
        } = self;
        vec![
            Named::plain("region", region),
            Named::credential("access_key_id", access_key_id),
            Named::credential("secret_access_key", secret_access_key),
            Named::credential("session_token", session_token),
        ]
    }

    fn echo_into(&self, out: &mut serde_json::Map<String, serde_json::Value>) {
        let Self {
            region,
            access_key_id,
            secret_access_key,
            session_token,
        } = self;
        if let Some(region) = region {
            echo_plain(out, "region", region);
        }
        echo_secret(out, "access_key_id", access_key_id);
        echo_secret(out, "secret_access_key", secret_access_key);
        echo_secret(out, "session_token", session_token);
    }
}

impl Group for GcsOptions {
    fn named(&self) -> Vec<Named> {
        let Self {
            service_account_key,
            access_token,
        } = self;
        vec![
            Named::credential("service_account_key", service_account_key),
            Named::credential("access_token", access_token),
        ]
    }

    fn echo_into(&self, out: &mut serde_json::Map<String, serde_json::Value>) {
        let Self {
            service_account_key,
            access_token,
        } = self;
        echo_secret(out, "service_account_key", service_account_key);
        echo_secret(out, "access_token", access_token);
    }
}

impl Group for AzureOptions {
    fn named(&self) -> Vec<Named> {
        let Self {
            account,
            access_key,
            sas_token,
        } = self;
        vec![
            // The storage account, which is the host half of an `az://` address rather
            // than anything that authenticates. It is signed *over*, not sent as proof.
            Named::plain("account", account),
            Named::credential("access_key", access_key),
            Named::credential("sas_token", sas_token),
        ]
    }

    fn echo_into(&self, out: &mut serde_json::Map<String, serde_json::Value>) {
        let Self {
            account,
            access_key,
            sas_token,
        } = self;
        if let Some(account) = account {
            echo_plain(out, "account", account);
        }
        echo_secret(out, "access_key", access_key);
        echo_secret(out, "sas_token", sas_token);
    }
}

impl Group for HttpOptions {
    fn named(&self) -> Vec<Named> {
        let Self { headers } = self;
        vec![Named::credentials("headers", headers)]
    }

    fn echo_into(&self, out: &mut serde_json::Map<String, serde_json::Value>) {
        let Self { headers } = self;
        if !headers.is_empty() {
            let mut written = serde_json::Map::new();
            for (name, value) in &headers.0 {
                echo_plain(&mut written, name, value.expose_secret());
            }
            out.insert("headers".to_owned(), written.into());
        }
    }
}

impl Group for WebdavOptions {
    fn named(&self) -> Vec<Named> {
        let Self {
            transport,
            username,
            password,
        } = self;
        vec![
            Named::plain("transport", transport),
            Named::credential("username", username),
            Named::credential("password", password),
        ]
    }

    fn echo_into(&self, out: &mut serde_json::Map<String, serde_json::Value>) {
        let Self {
            transport,
            username,
            password,
        } = self;
        if let Some(transport) = transport {
            echo_plain(out, "transport", transport.scheme());
        }
        echo_secret(out, "username", username);
        echo_secret(out, "password", password);
    }
}

impl StorageOptions {
    /// Every backend's group, so that anything needing the whole set walks them rather than
    /// listing the fields again.
    ///
    /// Destructured, which is what makes a field added to this struct a compile error until it
    /// is placed: a new group has to be listed here, and a new bare field beside `endpoint` has
    /// to be named in the arms of [`Self::named`] and [`Self::echo`] below.
    fn groups(&self) -> [&dyn Group; 5] {
        let Self {
            endpoint: _,
            allow_http: _,
            s3,
            gcs,
            azure,
            http,
            webdav,
            unknown: _,
        } = self;
        [s3, gcs, azure, http, webdav]
    }

    /// Every option, under the name a request spells it, whether it is set, and whether
    /// it is a credential.
    ///
    /// Everything that has to know the full set of options reads this one list: the
    /// per-scheme check, [`Self::is_empty`], and [`Self::has_credentials`]. Two lists
    /// would be two chances to forget a field, and forgetting one in the credential list
    /// is the expensive direction — it makes `backends::allow_cleartext` wave through a
    /// request that does carry a secret.
    ///
    /// The list is honest because each group builds its own by destructuring: a field added to
    /// a group and not registered does not compile, a field registered under the wrong [`Kind`]
    /// does not either, and [`Self::groups`] is destructured in turn, so a field added to this
    /// struct — group or not — does not compile until it is placed.
    fn named(&self) -> Vec<Named> {
        let mut all = vec![
            Named::plain(ENDPOINT, &self.endpoint),
            Named::flag(ALLOW_HTTP, self.allow_http),
        ];
        all.extend(self.groups().iter().flat_map(|group| group.named()));
        all
    }

    /// Nothing set at all, which is what a public object needs.
    pub fn is_empty(&self) -> bool {
        self.named().iter().all(|option| !option.set)
    }

    /// A `file://` url with a `secret_access_key`, or an `s3://` one with a `sas_token`,
    /// is a caller who has the wrong url or the wrong options; either reading is worth
    /// saying rather than guessing at, and one of them misdirects a credential.
    ///
    /// This also refuses an option no backend has, which `deny_unknown_fields` used to do and
    /// cannot any more: serde ignores it on a struct with a flattened field. A misspelled
    /// `secret_acces_key` silently dropped is an anonymous request the caller reads as an
    /// authenticated one — the wrong answer rather than an error.
    fn for_scheme(&self, scheme: &str) -> Result<(), ApiError> {
        if let Some(name) = self.unknown.keys().next() {
            return Err(ApiError::bad_request(format!(
                "{name:?} is not a storage option; {}",
                options_clause_for(scheme)
            )));
        }
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

    /// The one backend's options this request is actually for, once the url has said which
    /// backend that is — and a refusal of everything belonging to another.
    ///
    /// The two happen together on purpose. Checking and then reading the groups separately
    /// leaves a path where the narrowed value is taken without the check having run; here the
    /// check is how the narrowed value is obtained, so there is no such path to take.
    /// `None` for a scheme with no backend behind it — `file://`, where there is no store to
    /// reach. That case is not an error here: the refusal it needs is that it takes no options
    /// at all, which `for_scheme` has already made.
    pub(super) fn resolve(&self, scheme: &str) -> Result<Option<Credentials<'_>>, ApiError> {
        self.for_scheme(scheme)?;
        Ok(Backend::from_scheme(scheme).map(|backend| match backend {
            Backend::S3 => Credentials::S3(&self.s3),
            Backend::Gcs => Credentials::Gcs(&self.gcs),
            Backend::Azure => Credentials::Azure(&self.azure),
            Backend::Http => Credentials::Http(&self.http),
            Backend::Webdav => Credentials::Webdav(&self.webdav),
        }))
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
    /// Each group writes its own, by destructuring, so a field added to a group and not
    /// written here does not compile. The guard runs the other way for this one: forgetting a
    /// field hands back a plan missing something the caller asked for, rather than one
    /// carrying what they did not.
    pub fn echo(&self) -> serde_json::Value {
        let mut out = serde_json::Map::new();
        if let Some(endpoint) = &self.endpoint {
            echo_plain(&mut out, ENDPOINT, endpoint);
        }
        if self.allow_http {
            out.insert(ALLOW_HTTP.to_owned(), true.into());
        }
        for group in self.groups() {
            group.echo_into(&mut out);
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
    // The group's own fields, so this list and the fields the backend function reads are the
    // same fact rather than two that have to be kept in step.
    let mut accepted = match backend {
        Backend::S3 => names_of::<S3Options>(),
        Backend::Gcs => names_of::<GcsOptions>(),
        Backend::Azure => names_of::<AzureOptions>(),
        Backend::Http => names_of::<HttpOptions>(),
        Backend::Webdav => names_of::<WebdavOptions>(),
    };
    // A url that is its own address has nothing for `endpoint` to point elsewhere at, so it
    // joins the list only for a backend addressed by bucket. `allow_http` is a different
    // question and every backend has it, because every backend can be handed a credential the
    // caller would not want in cleartext.
    if backend.has_provider() {
        accepted.push(ENDPOINT);
    }
    accepted.push(ALLOW_HTTP);
    accepted
}

/// Which url schemes each storage option applies to, for the API description.
///
/// Derived from the same lists `accepted_options` refuses by, so what the document says an
/// option is for and what the service accepts it for cannot come apart. A backend added to
/// `BACKENDS` appears here without anything being written twice.
pub fn option_schemes(option: &str) -> Vec<&'static str> {
    BACKENDS
        .iter()
        .flat_map(|backend| backend.schemes())
        .copied()
        .filter(|scheme| accepted_options(scheme).contains(&option))
        .collect()
}

/// The clause naming what this url's scheme takes, for the two messages that have to say
/// where a caller's options go. Phrased both ways rather than printing an empty list,
/// which would read as though the scheme's options had been left out of the message.
///
/// Written for a scheme that may not be one this service serves at all: these messages
/// come from checks that run before the scheme is known, and saying "ftp urls take no
/// storage options" is true and is followed by the refusal that matters.
pub(super) fn options_clause(url: &Url) -> String {
    options_clause_for(url.scheme())
}

fn options_clause_for(scheme: &str) -> String {
    match accepted_options(scheme).as_slice() {
        [] => format!("{scheme} urls take no storage options"),
        options => format!("{scheme} urls take {}", options.join(", ")),
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::parse_url;
    use crate::storage::tests::{SECRET, no_options, open, options};

    use super::*;

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

    /// `allow_http` is a bool in the body, so a string is a deserialization failure
    /// rather than something this module has to parse.
    #[test]
    fn allow_http_must_be_a_boolean() {
        let error =
            serde_json::from_value::<StorageOptions>(serde_json::json!({"allow_http": "yes"}))
                .unwrap_err();
        assert!(error.to_string().contains("boolean"), "{error}");
    }

    /// A misspelled option is a 400 rather than a silently anonymous request.
    ///
    /// Asserted against opening rather than against deserializing: the groups are flattened
    /// into [`StorageOptions`], and serde ignores `deny_unknown_fields` on a struct with a
    /// flattened field, so the leftovers are collected and refused by `for_scheme` instead.
    /// What must hold is the refusal, not which layer produces it — `secret_acces_key` quietly
    /// dropped would be an anonymous request the caller reads as an authenticated one.
    #[test]
    fn rejects_unknown_storage_options_instead_of_ignoring_them() {
        let url = parse_url("s3://bucket/key.parquet").unwrap();
        for (misspelled, value) in [
            ("regoin", serde_json::json!("us-west-2")),
            ("secret_acces_key", serde_json::json!(SECRET)),
        ] {
            let options = options(serde_json::json!({ misspelled: value }));
            let error = open(&url, &options).unwrap_err().to_string();
            assert!(error.contains(misspelled), "{error}");
            assert!(error.contains("not a storage option"), "{error}");
            assert!(!error.contains(SECRET), "leaked: {error}");
        }
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
}
