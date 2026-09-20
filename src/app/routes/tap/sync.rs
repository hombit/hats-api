//! `{api.prefix}/tap/sync`: one ADQL statement against the published tables, answered in
//! the same response.
//!
//! A DALI-sync resource, which TAP §2.1 says `/sync` must be. `GET` and `POST` both, and
//! the two differ only in where the parameters are read from — §2.1 notes that a `GET` may
//! be answered from a cache and that a client needing current data must `POST`, and nothing
//! here caches either way.
//!
//! Running the statement is [`super::run`]'s and is shared with the job resource; what is
//! here is the HTTP around it — reading the pairs off either carrier, and turning one answer
//! into one response.
//!
//! **Every answer this resource gives is a VOTable or the format the request asked for,
//! including the refusals.** `ApiError` renders JSON, which is right for the routes a
//! caller of this service's own API writes against and is not what a TAP client parses: it
//! reads a document, looks for `QUERY_STATUS`, and has nothing to tell a user where it
//! finds neither.

use std::time::Instant;

use axum::extract::rejection::StringRejection;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};

use crate::app::routes::tap::answer::answered;
use crate::app::routes::tap::format;
use crate::app::routes::tap::parameters::Parameters;
use crate::app::routes::tap::run::{self, Answered};
use crate::app::service::Service;
use crate::error::ApiError;

/// The parameters in a query string.
pub(in crate::app) async fn tap_sync_get(
    State(service): State<Service>,
    RawQuery(query): RawQuery,
) -> Response {
    let pairs = pairs(query.as_deref().unwrap_or_default());
    answered(sync(&service, &pairs).await)
}

/// The same parameters in a form body, which is what a client sends for a statement too
/// long to put in a url.
pub(in crate::app) async fn tap_sync_post(
    State(service): State<Service>,
    headers: HeaderMap,
    body: Result<String, StringRejection>,
) -> Response {
    answered(posted(&service, &headers, body).await)
}

async fn posted(
    service: &Service,
    headers: &HeaderMap,
    body: Result<String, StringRejection>,
) -> Result<Response, ApiError> {
    // A `multipart/form-data` body is how TAP carries an inline `UPLOAD`, which this
    // service does not implement — so saying that is more use than reading the bytes as
    // form-encoded and reporting that they hold no QUERY.
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if content_type.starts_with("multipart/") {
        return Err(ApiError::bad_request(
            "this service takes a POST as application/x-www-form-urlencoded; multipart is \
             for an inline UPLOAD, which it does not implement",
        ));
    }
    // Asked of the rejection's status rather than by naming a variant: the body limit
    // surfaces through whichever buffering error the extractor wraps, and the status is the
    // part of that axum promises.
    let body = body.map_err(|rejection| match rejection.status() {
        StatusCode::PAYLOAD_TOO_LARGE => ApiError::body_too_large(
            "the request body is larger than this service accepts; send a shorter statement",
        ),
        _ => ApiError::bad_request("the request body is not text this service can read"),
    })?;
    sync(service, &pairs(&body)).await
}

/// One statement, from the parameters to the response.
async fn sync(service: &Service, pairs: &[(String, String)]) -> Result<Response, ApiError> {
    let started = Instant::now();
    let parameters = Parameters::read(pairs)?;
    // Before the statement is even parsed: a format this service cannot write makes the
    // rest of the request moot, and refusing costs nothing.
    //
    // Unlike the job resource, which cannot refuse here: TAP §2.7 enforces a parameter's
    // value only when the query is run, and for a job that is after the redirect.
    let answering = format::resolve(parameters.format.as_deref())?;
    let answer = run::run(service, &parameters, answering, service.adql_limits).await?;
    tracing::info!(
        tables = %answer.tables.join(","),
        query_bytes = parameters.query.len(),
        format = answering.format.name(),
        runid = parameters.runid.as_deref().unwrap_or(""),
        num_rows = answer.num_rows,
        overflow = answer.overflow,
        data_bytes_read = answer.data_bytes_read,
        elapsed_ms = started.elapsed().as_millis(),
        "tap sync"
    );
    Ok(response(answer))
}

/// The bytes, labelled, and said to be cut where the format cannot say it itself.
///
/// **Only a VOTable can say it was truncated**, `OVERFLOW` being a VOTable marker — so the
/// other two carry [`OVERFLOW_HEADER`] instead. That is this service's own and no client
/// reads it; what it is for is that the fact is stated somewhere rather than nowhere, a
/// delimited body having no room for it and TAP defining nothing for one.
fn response(answer: Answered) -> Response {
    (
        [
            (header::CONTENT_TYPE, answer.content_type.to_owned()),
            (
                header::HeaderName::from_static(OVERFLOW_HEADER),
                answer.overflow.to_string(),
            ),
        ],
        answer.body,
    )
        .into_response()
}

/// Whether the answer stopped at the row bound, for the formats with nowhere to say it.
const OVERFLOW_HEADER: &str = "x-hats-overflow";

/// The pairs of a query string or a form body, which are the same encoding.
pub(super) fn pairs(text: &str) -> Vec<(String, String)> {
    url::form_urlencoded::parse(text.as_bytes())
        .into_owned()
        .collect()
}
