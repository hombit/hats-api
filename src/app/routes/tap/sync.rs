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
//! what is left here is one answer becoming one response — of which there are two, a body
//! built and measured and a body sent as it is read, and `STREAMING` is which.
//!
//! **Every answer this resource gives is a VOTable or the format the request asked for,
//! including the refusals.** `ApiError` renders JSON, which is right for the routes a
//! caller of this service's own API writes against and is not what a TAP client parses: it
//! reads a document, looks for `QUERY_STATUS`, and has nothing to tell a user where it
//! finds neither.

use std::time::Instant;

use axum::extract::rejection::StringRejection;
use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, header};
use axum::response::{IntoResponse, Response};

use crate::app::answer::Generated;
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
    if parameters.streaming {
        let answer = run::stream(service, &parameters, answering, service.adql_limits).await?;
        // Before a row has been read, so it carries no counts: what a streamed answer cost
        // is known as the body ends, which is after this handler has returned. What is here
        // is what the request asked for.
        tracing::info!(
            tables = %answer.tables.join(","),
            query_bytes = parameters.query.len(),
            format = answering.format.name(),
            runid = parameters.runid.as_deref().unwrap_or(""),
            streaming = true,
            elapsed_ms = started.elapsed().as_millis(),
            "tap sync"
        );
        return Ok(streaming(answer));
    }
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
///
/// Bytes rather than text, parquet being a format that is not one — and the body is built
/// whole here, so this answer carries a `Content-Length`. That is what a parquet reader
/// opening the url needs, and it is what `STREAMING=true` gives up.
fn response(body: Vec<u8>, answer: &Answered) -> Response {
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

/// The same answer, sent as it is read.
///
/// Two headers a collected answer has are missing here and neither can be supplied. There is
/// no `Content-Length`, which is why the body says `Accept-Ranges: none` — a client that sent
/// a `Range` and got a plain `200` may otherwise read that many bytes off the front and treat
/// them as the range it asked for. And there is no `x-hats-overflow`: whether the row bound
/// cut the answer is known once the rows have run out, and these headers went before the
/// first of them was read. What says it instead is the end of the document — `OVERFLOW` in a
/// VOTable, and a body that stops without its terminator in the three formats that have
/// nowhere to write one.
///
/// `Generated` is what puts the body under the request clock. The rows are read as it is
/// sent, so the work this request came to do is not over when the handler returns; without
/// the mark a streamed query would be the one request `max_request_seconds` does not bound.
fn streaming(answer: run::Streamed) -> Response {
    let mut response = ([(header::CONTENT_TYPE, answer.content_type)], answer.body).into_response();
    response
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("none"));
    response.extensions_mut().insert(Generated);
    response
}
