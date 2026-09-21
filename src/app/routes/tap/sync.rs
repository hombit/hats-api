//! `{api.prefix}/tap/sync`: one ADQL statement against the published tables, answered in
//! the same response.
//!
//! A DALI-sync resource, which TAP §2.1 says `/sync` must be. `GET` and `POST` both, and
//! the two differ only in where the parameters are read from — §2.1 notes that a `GET` may
//! be answered from a cache and that a client needing current data must `POST`, and nothing
//! here caches either way.
//!
//! Almost nothing is this resource's own. Running the statement is [`super::run`]'s and
//! reading the parameters is [`super::parameters`]'s, both shared with the job resource;
//! what is left here is one answer becoming one response.
//!
//! **Every answer this resource gives is a VOTable or the format the request asked for,
//! including the refusals.** `ApiError` renders JSON, which is right for the routes a
//! caller of this service's own API writes against and is not what a TAP client parses: it
//! reads a document, looks for `QUERY_STATUS`, and has nothing to tell a user where it
//! finds neither.

use std::time::Instant;

use axum::extract::rejection::StringRejection;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, header};
use axum::response::{IntoResponse, Response};

use crate::app::routes::tap::answer::{OVERFLOW_HEADER, answered};
use crate::app::routes::tap::format;
use crate::app::routes::tap::parameters::{self, Parameters};
use crate::app::routes::tap::run::{self, Answered};
use crate::app::service::Service;
use crate::error::ApiError;

/// The parameters in a query string.
pub(in crate::app) async fn tap_sync_get(
    State(service): State<Service>,
    RawQuery(query): RawQuery,
) -> Response {
    let pairs = parameters::pairs(query.as_deref().unwrap_or_default());
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
    let pairs = parameters::posted(headers, body)?;
    sync(service, &pairs).await
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
    let (body, answer) = run::run(service, &parameters, answering, service.adql_limits).await?;
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
    Ok(response(body, &answer))
}

/// The bytes, labelled, and said to be cut where the format cannot say it itself.
fn response(body: String, answer: &Answered) -> Response {
    (
        [
            (header::CONTENT_TYPE, answer.content_type.to_owned()),
            (
                header::HeaderName::from_static(OVERFLOW_HEADER),
                answer.overflow.to_string(),
            ),
        ],
        body,
    )
        .into_response()
}
