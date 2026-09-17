//! Following one origin's redirect to another, where a backend cannot be read without it.
//!
//! The service does not follow redirects. A 3xx is the origin choosing the next destination,
//! and where the request goes is the one decision only the config makes — so
//! [`crate::access::network`] builds its client with `redirect::Policy::none()` and a 3xx comes
//! back as the response it is.
//!
//! **One backend cannot be read under that rule.** The Hugging Face Hub answers
//! `GET …/resolve/…` with a `307` to a presigned url on a CDN for anything stored in LFS, which
//! is every parquet file in a dataset. There is no second route to the bytes: the redirect is
//! how the Hub hands a file over.
//!
//! So the hop is built here rather than by turning the client's policy back on, because what
//! makes it acceptable is a set of conditions a redirect policy has no way to express:
//!
//! - **The origin that named the target is one the policy already allowed.** A caller cannot
//!   reach this code without [`crate::access::AccessPolicy::authorize_endpoint`] having agreed
//!   to the endpoint the request went to, so the host that picks the next hop is the
//!   operator's choice and the target is that host's. A caller never names it.
//! - **A hop may not go from `https` to `http`.** What cleartext costs is the assurance that
//!   the bytes came from the host the url named, and a hop that gave that up would be an
//!   origin downgrading a request the operator made over TLS.
//! - **A credential does not cross an origin.** The token goes to the Hub, and the CDN url is
//!   presigned and wants none — so dropping it costs a caller nothing and is what keeps the
//!   Hub's choice of CDN from being a choice about where the token goes.
//! - **Every address is still judged.** The follow-up request is made through the same client,
//!   so its target goes through the policy's resolver exactly as the first one did. That is
//!   the check the hop cannot be allowed to skip, and it is skipped by nothing here.
//!
//! **What a credential is, here, is an `Authorization` header.** This transport is installed on
//! one backend, whose only credential is the token it sends that way; the two other names
//! stripped below are the ones that would be a credential if anything sent them. A backend
//! whose credential is a signature over the request — S3's, Azure's — must not be given this
//! layer without deciding what a hop does to the signature, which is a different question from
//! what it does to a header.

use http::{HeaderMap, Method, Uri};
use opendal::{Buffer, Error, ErrorKind, HttpBody, HttpTransport, HttpTransporter};
use url::Url;

/// How many hops a read may take before it is a loop rather than a redirect.
///
/// The Hub takes one to the CDN, sometimes two by way of its own cache route. The bound is
/// what stops two origins pointing at each other from becoming a request that never ends; it
/// is deliberately small, because a chain longer than this is not a shape any origin here has.
const MAX_HOPS: usize = 4;

/// Headers that must not cross an origin.
///
/// `Authorization` is the one this service sends. The other two are here because they are what
/// a credential looks like when something else sends one, and a list that named only what is
/// sent today is one that goes stale silently in the direction that leaks.
const CREDENTIAL_HEADERS: [http::HeaderName; 3] = [
    http::header::AUTHORIZATION,
    http::header::COOKIE,
    http::header::PROXY_AUTHORIZATION,
];

/// Follows a redirect from an origin the policy allowed, under the conditions in this module's
/// documentation.
///
/// Wrapped around the policy's own transport and below the caller's headers, which is what
/// makes both halves work: the first request carries the token, and this layer is what decides
/// whether the second one does.
pub(super) struct FollowingRedirects {
    inner: HttpTransporter,
}

impl FollowingRedirects {
    pub(super) fn new(inner: HttpTransporter) -> Self {
        Self { inner }
    }
}

/// Never derived: the headers being carried are the caller's credentials, and this type is one
/// field of something a `tracing` call could print.
impl std::fmt::Debug for FollowingRedirects {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FollowingRedirects")
    }
}

impl HttpTransport for FollowingRedirects {
    async fn fetch(
        &self,
        request: http::Request<Buffer>,
    ) -> opendal::Result<http::Response<HttpBody>> {
        // The request that arrived is sent as it arrived; only a hop is rebuilt. What that
        // keeps is whatever the layer above put on it that is not a method, a uri, a header
        // or a body — extensions, which a rebuild would silently drop on the first request
        // and so on every request, redirect or not.
        let mut request = request;
        for _ in 0..=MAX_HOPS {
            // Taken before the request is consumed, and needed after: a hop is the same
            // request somewhere else.
            let method = request.method().clone();
            let uri = request.uri().clone();
            let headers = request.headers().clone();
            // Cheap: a read carries no body, and `Buffer` is a handle rather than the bytes.
            let body = request.body().clone();

            let response = self.inner.fetch(request).await?;
            let Some(hop) = hop(
                &uri,
                &method,
                &headers,
                response.status(),
                response.headers(),
            )?
            else {
                return Ok(response);
            };
            request = rebuild(&hop.method, &hop.uri, &hop.headers, &body)?;
        }

        Err(Error::new(
            ErrorKind::Unexpected,
            "this url redirects more times than a read will follow",
        ))
    }
}

/// The request to make next.
struct Hop {
    uri: Uri,
    method: Method,
    headers: HeaderMap,
}

/// Whether this response is a redirect worth following, and what the next request looks like.
///
/// Separated from the transport so that the rules can be asked about directly: what this
/// decides is a security boundary, and a test that has to stand up two servers to reach it is
/// one that only covers the cases somebody thought to serve.
fn hop(
    from: &Uri,
    method: &Method,
    headers: &HeaderMap,
    status: http::StatusCode,
    response_headers: &HeaderMap,
) -> opendal::Result<Option<Hop>> {
    if !status.is_redirection() {
        return Ok(None);
    }
    // A 304 is in the 3xx range and is an answer about the object rather than a destination.
    let Some(location) = response_headers.get(http::header::LOCATION) else {
        return Ok(None);
    };
    let location = location.to_str().map_err(|_| {
        Error::new(
            ErrorKind::Unexpected,
            "this url redirects somewhere its Location header cannot spell",
        )
    })?;

    // Resolved against the url that answered, because a `Location` may be relative — the Hub's
    // first hop is, and a relative one stays on the origin that is already allowed.
    let here = Url::parse(&from.to_string()).map_err(|error| {
        Error::new(
            ErrorKind::Unexpected,
            "cannot read the url that was requested",
        )
        .set_source(error)
    })?;
    let target = here.join(location).map_err(|error| {
        Error::new(
            ErrorKind::Unexpected,
            "this url redirects somewhere unreadable",
        )
        .set_source(error)
    })?;

    // A hop may not give up TLS. An origin reached over https that sends the read to an
    // http url is one asking for the rest of the transfer in the clear, and the bytes of a
    // parquet file are an answer this service would then be repeating on trust.
    if here.scheme() == "https" && target.scheme() != "https" {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "this url redirects from https to a url that is not, which is a hop \
             that gives up the assurance the first one had",
        ));
    }
    if !matches!(target.scheme(), "http" | "https") {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "this url redirects to something that is not an http url",
        ));
    }

    let mut headers = headers.clone();
    if !same_origin(&here, &target) {
        for name in CREDENTIAL_HEADERS {
            headers.remove(name);
        }
    }

    let uri = Uri::try_from(target.as_str()).map_err(|error| {
        Error::new(
            ErrorKind::Unexpected,
            "this url redirects somewhere unreadable",
        )
        .set_source(error)
    })?;
    Ok(Some(Hop {
        uri,
        // A 303 says to ask again with a GET, which for the two methods a read uses means
        // turning a HEAD into one. Every other redirect keeps the method it was answering.
        method: match status == http::StatusCode::SEE_OTHER {
            true => Method::GET,
            false => method.clone(),
        },
        headers,
    }))
}

/// Whether two urls are the same server: scheme, host and port, which is what decides
/// whether a credential may go on. The port is compared as the scheme implies it, so
/// `https://host` and `https://host:443` are the one origin they are.
fn same_origin(here: &Url, target: &Url) -> bool {
    here.scheme() == target.scheme()
        && here.host() == target.host()
        && here.port_or_known_default() == target.port_or_known_default()
}

fn rebuild(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &Buffer,
) -> opendal::Result<http::Request<Buffer>> {
    let mut request = http::Request::builder()
        .method(method.clone())
        .uri(uri.clone());
    match request.headers_mut() {
        Some(into) => *into = headers.clone(),
        None => {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "cannot rebuild the request to follow a redirect",
            ));
        }
    }
    request.body(body.clone()).map_err(|error| {
        Error::new(ErrorKind::Unexpected, "cannot follow a redirect").set_source(error)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "hf_secret-token-value";

    fn headers_with_token() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_str(&format!("Bearer {SECRET}")).unwrap(),
        );
        headers.insert(
            http::header::RANGE,
            http::HeaderValue::from_static("bytes=0-7"),
        );
        headers
    }

    fn redirect_to(location: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::LOCATION,
            http::HeaderValue::from_str(location).unwrap(),
        );
        headers
    }

    fn follow(from: &str, location: &str) -> opendal::Result<Option<Hop>> {
        hop(
            &Uri::try_from(from).unwrap(),
            &Method::GET,
            &headers_with_token(),
            http::StatusCode::TEMPORARY_REDIRECT,
            &redirect_to(location),
        )
    }

    /// The hop the Hub actually asks for: an absolute url on a CDN, which is a different
    /// origin. The token is what must not go with it — the target is presigned and the
    /// caller's secret would be travelling to a host the operator never named.
    #[test]
    fn a_credential_does_not_cross_an_origin() {
        let hop = follow(
            "https://huggingface.co/datasets/o/n/resolve/main/x.parquet",
            "https://us.aws.cdn.hf.co/xet-bridge-us/abc?Signature=def",
        )
        .unwrap()
        .expect("a 307 with a Location is a hop");
        assert_eq!(hop.uri.host(), Some("us.aws.cdn.hf.co"));
        assert!(!hop.headers.contains_key(http::header::AUTHORIZATION));
        // The range is not a credential and has to survive, or the hop reads the whole
        // object where the first request asked for eight bytes.
        assert_eq!(hop.headers.get(http::header::RANGE).unwrap(), "bytes=0-7");
    }

    /// The Hub's other hop is relative and stays on the origin that was already allowed, so
    /// the token goes on: a private repository answers the second request the way it
    /// answered the first, or the read fails as unauthenticated halfway through.
    #[test]
    fn a_credential_stays_within_one_origin() {
        let hop = follow(
            "https://huggingface.co/datasets/o/n/resolve/main/x.parquet",
            "/api/resolve-cache/datasets/o/n/abc/x.parquet?etag=def",
        )
        .unwrap()
        .expect("a relative Location is a hop");
        assert_eq!(hop.uri.host(), Some("huggingface.co"));
        assert_eq!(
            hop.headers.get(http::header::AUTHORIZATION).unwrap(),
            &format!("Bearer {SECRET}")
        );
    }

    /// A hop that gives up TLS is refused rather than followed. What it would cost is the
    /// assurance that the bytes came from the host the url named — and a parquet file
    /// something on the path rewrote is a wrong answer rather than a failed request.
    /// `Hop` holds the headers a follow-up would carry, so it has no `Debug` on purpose —
    /// which means a test cannot unwrap the error out of one and has to take it this way.
    fn refusal(from: &str, location: &str) -> Error {
        match follow(from, location) {
            Err(error) => error,
            Ok(_) => panic!("{location} was followed"),
        }
    }

    #[test]
    fn a_hop_may_not_give_up_tls() {
        let error = refusal(
            "https://huggingface.co/datasets/o/n/resolve/main/x.parquet",
            "http://cdn.example.com/x.parquet",
        );
        assert!(error.to_string().contains("gives up"), "{error}");
        assert!(!error.to_string().contains(SECRET), "leaked: {error}");
    }

    /// And a scheme that is not http at all — `file:///etc/passwd` is the one worth naming,
    /// since a transport that resolved it would be reading the disk on an origin's say-so.
    #[test]
    fn a_hop_goes_nowhere_but_http() {
        for location in ["file:///etc/passwd", "ftp://host/x"] {
            let error = refusal(
                "http://127.0.0.1:1/datasets/o/n/resolve/main/x.parquet",
                location,
            );
            assert!(error.to_string().contains("not an http url"), "{error}");
        }
    }

    /// A 3xx with nothing to go on is the response it is, not a hop. A `304` is the case
    /// that matters: it is an answer about the object, and taking it for a redirect would
    /// turn a conditional read into a failure.
    #[test]
    fn a_redirect_without_a_destination_is_the_answer() {
        let answered = hop(
            &Uri::try_from("https://huggingface.co/x").unwrap(),
            &Method::GET,
            &headers_with_token(),
            http::StatusCode::NOT_MODIFIED,
            &HeaderMap::new(),
        )
        .unwrap();
        assert!(answered.is_none());
    }

    /// A 303 says to ask again with a GET, which is the one redirect that changes the
    /// method. The other kinds keep it, or a HEAD would quietly become a read of the object.
    #[test]
    fn only_a_303_changes_the_method() {
        let kept = hop(
            &Uri::try_from("https://huggingface.co/x").unwrap(),
            &Method::HEAD,
            &HeaderMap::new(),
            http::StatusCode::TEMPORARY_REDIRECT,
            &redirect_to("https://huggingface.co/y"),
        )
        .unwrap()
        .unwrap();
        assert_eq!(kept.method, Method::HEAD);

        let changed = hop(
            &Uri::try_from("https://huggingface.co/x").unwrap(),
            &Method::HEAD,
            &HeaderMap::new(),
            http::StatusCode::SEE_OTHER,
            &redirect_to("https://huggingface.co/y"),
        )
        .unwrap()
        .unwrap();
        assert_eq!(changed.method, Method::GET);
    }

    /// The port is part of an origin, and one spelled out is the same origin as one implied.
    #[test]
    fn an_implied_port_is_the_same_origin() {
        let here = Url::parse("https://huggingface.co/x").unwrap();
        assert!(same_origin(
            &here,
            &Url::parse("https://huggingface.co:443/y").unwrap()
        ));
        assert!(!same_origin(
            &here,
            &Url::parse("https://huggingface.co:8443/y").unwrap()
        ));
        assert!(!same_origin(
            &here,
            &Url::parse("https://other.example.com/y").unwrap()
        ));
    }
}
