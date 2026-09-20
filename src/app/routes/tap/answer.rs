//! What every TAP answer has in common: how a refusal is said, and where this service
//! tells a client it is.

use axum::http::{HeaderMap, header};
use axum::response::{IntoResponse, Response};

use crate::error::ApiError;
use crate::output::votable;

/// Whatever a resource produced, as a TAP answer.
///
/// TAP §3.3 asks for an error document with an appropriate HTTP status, and DALI §4.4.2
/// says what the document is. The status is [`ApiError`]'s own — the same one this service
/// answers every other route with — and only the body is replaced: `ApiError` renders JSON,
/// which a client that reads `QUERY_STATUS` has nothing to say about.
pub(super) fn answered(outcome: Result<Response, ApiError>) -> Response {
    match outcome {
        Ok(response) => response,
        Err(error) => {
            let status = error.status();
            let message = error.to_string();
            if status.is_server_error() {
                tracing::error!(error = %message, "a tap resource failed");
            } else {
                tracing::debug!(error = %message, %status, "a tap request was rejected");
            }
            (
                status,
                [(header::CONTENT_TYPE, votable::CONTENT_TYPE)],
                votable::error(&message),
            )
                .into_response()
        }
    }
}

/// Whether the answer stopped at the row bound, for the formats with nowhere to say it.
///
/// **Only a VOTable can say it was truncated**, `OVERFLOW` being a VOTable marker — so `csv`
/// and `tsv` carry this instead. It is this service's own and no client reads it; what it is
/// for is that the fact is stated somewhere rather than nowhere, a delimited body having no
/// room for it and TAP defining nothing for one.
///
/// Both query resources send it, from the same const: a job's answer is collected later and
/// by a client that did not see the request, so it is if anything the one that needs it more.
pub(super) const OVERFLOW_HEADER: &str = "x-hats-overflow";

/// What the XML documents are served as. VOSI's own media type, and the one every
/// reference service answers these three resources with.
pub(super) const XML_CONTENT_TYPE: &str = "text/xml";

/// An XML document, as a response.
pub(super) fn document(body: String) -> Response {
    ([(header::CONTENT_TYPE, XML_CONTENT_TYPE)], body).into_response()
}

/// Where a client should write to reach this service, as this request reached it.
///
/// Built from the request's own `Host`, because that is the only thing that knows: the
/// service may be behind any number of proxies and its configuration says nothing about
/// the name it is published under. `X-Forwarded-Proto` is honoured where a proxy set one,
/// a deployment behind TLS termination otherwise advertising `http://` urls that redirect.
///
/// What this is used for is the access urls in the capabilities document, which a client
/// follows — so a wrong `Host` gives that client urls pointing at whatever it already
/// wrote. Nothing this service does follows them.
pub(super) fn base_url(headers: &HeaderMap, prefix: &str) -> String {
    let text = |name: header::HeaderName| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    };
    let scheme = text(header::HeaderName::from_static("x-forwarded-proto"))
        // A proxy may list every hop; the first is the client's own.
        .and_then(|value| value.split(',').next().map(|first| first.trim().to_owned()))
        .filter(|scheme| scheme == "http" || scheme == "https")
        .unwrap_or_else(|| "http".to_owned());
    let host = text(header::HOST).unwrap_or_else(|| "localhost".to_owned());
    let prefix = prefix.trim_end_matches('/');
    format!("{scheme}://{host}{prefix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(
                header::HeaderName::from_static(name),
                value.parse().unwrap(),
            );
        }
        headers
    }

    #[test]
    fn the_base_url_is_where_the_request_arrived() {
        assert_eq!(
            base_url(&headers(&[("host", "data.example.org")]), "/api/v1"),
            "http://data.example.org/api/v1"
        );
        // Behind TLS termination, which is the ordinary deployment.
        assert_eq!(
            base_url(
                &headers(&[
                    ("host", "data.example.org"),
                    ("x-forwarded-proto", "https, http"),
                ]),
                "/api/v1"
            ),
            "https://data.example.org/api/v1"
        );
        // A prefix of `/` leaves no trailing slash to double up.
        assert_eq!(
            base_url(&headers(&[("host", "localhost:8080")]), "/"),
            "http://localhost:8080"
        );
        // Anything but the two schemes is not one this service would be reached over.
        assert_eq!(
            base_url(
                &headers(&[("host", "h"), ("x-forwarded-proto", "gopher")]),
                "/api/v1"
            ),
            "http://h/api/v1"
        );
    }
}
