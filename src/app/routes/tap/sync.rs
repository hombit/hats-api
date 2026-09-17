//! `{api.prefix}/tap/sync`: one ADQL statement against the published tables, answered in
//! the same response.
//!
//! A DALI-sync resource, which TAP §2.1 says `/sync` must be. `GET` and `POST` both, and
//! the two differ only in where the parameters are read from — §2.1 notes that a `GET` may
//! be answered from a cache and that a client needing current data must `POST`, and nothing
//! here caches either way.
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

use crate::access::data::DataFiles;
use crate::adql;
use crate::adql::query::{Answer, Rows, Source, Table};
use crate::app::request::Format;
use crate::app::routes::tap::answer::answered;
use crate::app::routes::tap::format::{self, Answering};
use crate::app::routes::tap::parameters::Parameters;
use crate::app::routes::tap::published::describe;
use crate::app::routes::tap::upload::{Kind, UPLOAD_SCHEMA, Upload};
use crate::app::service::Service;
use crate::error::ApiError;
use crate::output::{dsv, votable};
use crate::storage::{self, StorageOptions};
use crate::tap::schema;

/// What a null is written as in `csv` and `tsv` here.
///
/// Empty, which is `arrow-csv`'s own. TAP has no parameter for it, so there is nothing for
/// a caller to choose and no reason to answer differently from the writer's default.
const DSV_NULL: &str = "";

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

/// One statement, from the parameters to the bytes of the answer.
async fn sync(service: &Service, pairs: &[(String, String)]) -> Result<Response, ApiError> {
    let started = Instant::now();
    let parameters = Parameters::read(pairs)?;
    // Before the statement is even parsed: a format this service cannot write makes the
    // rest of the request moot, and refusing costs nothing.
    let answering = format::resolve(parameters.format.as_deref())?;
    let translated = adql::translate(&parameters.query, service.sql_limits)?;

    // Read once, and only where the statement names one of TAP_SCHEMA's tables: describing
    // what is published costs a few small reads per catalog, which a query that names none
    // of it should not pay.
    let described = match translated
        .tables
        .iter()
        .any(|spelling| schema::resolve(spelling).is_some())
    {
        true => describe(service).await?,
        false => Vec::new(),
    };

    let mut tables = Vec::new();
    let mut data_files: Option<DataFiles> = None;
    for spelling in &translated.tables {
        let source = match schema::resolve(spelling) {
            Some(fixed) => {
                let table = fixed.rsplit('.').next().unwrap_or_default();
                Source::Memory(schema::provider(table, &described)?)
            }
            None if names_an_upload(spelling) => {
                let upload = parameters.uploads.named(spelling).ok_or_else(|| {
                    ApiError::bad_request(format!(
                        "query: {spelling} names no table this request uploaded; it uploads {}",
                        match parameters.uploads.is_empty() {
                            true => "nothing".to_owned(),
                            false => parameters.uploads.names().join(", "),
                        }
                    ))
                })?;
                let (source, files) = uploaded(service, upload)?;
                if let Some(files) = files {
                    data_files = Some(files);
                }
                source
            }
            None => {
                let published = service.tap_tables.lookup(spelling).ok_or_else(|| {
                    ApiError::bad_request(format!(
                        "query: this service publishes no table named {spelling}; it \
                         publishes {}",
                        match service.tap_tables.is_empty() {
                            true => "none".to_owned(),
                            false => service.tap_tables.names().join(", "),
                        }
                    ))
                })?;
                let url = published.url();
                // Whichever mount governs the catalog, which is what says which names
                // inside it hold rows. A published table carries no storage options, so a
                // catalog that needs a credential is not one this resource can read.
                data_files = Some(service.data_files_for(url).clone());
                Source::Catalog(storage::open_dir(
                    url,
                    &StorageOptions::default(),
                    &service.policy,
                    &service.transfers,
                )?)
            }
        };
        tables.push(Table {
            name: spelling.clone(),
            source,
        });
    }
    let data_files = data_files.unwrap_or_else(|| service.data_files.as_ref().clone());

    // The row bound is the smaller of what the caller asked for and what the operator
    // allows, and reaching it is a truncation rather than a refusal — this bound alone, and
    // on this route alone: `OVERFLOW` is the in-band statement whose absence makes a cut
    // answer indistinguishable from a whole one, which is why every other route refuses.
    // `MAXREC` is *not* applied to the statement's own `TOP`: TAP §2.7.4 has the
    // truncation happen "after any limitations imposed by the query", so `TOP 5` with
    // `MAXREC=10` is five rows and no overflow.
    let ceiling = service.adql_limits.max_rows;
    let limits = adql::query::Limits {
        max_rows: parameters
            .maxrec
            .map_or(ceiling, |asked| asked.min(ceiling)),
        rows: Rows::Truncate,
        ..service.adql_limits
    };
    let answer = adql::query::run(&translated, &tables, &data_files, limits).await?;
    let response = encode(&answer, answering)?;
    tracing::info!(
        tables = %translated.tables.iter().cloned().collect::<Vec<_>>().join(","),
        query_bytes = parameters.query.len(),
        format = answering.format.name(),
        runid = parameters.runid.as_deref().unwrap_or(""),
        num_rows = answer.result.num_rows(),
        overflow = answer.overflow,
        data_bytes_read = answer.result.data_bytes_read,
        elapsed_ms = started.elapsed().as_millis(),
        "tap sync"
    );
    Ok(response)
}

/// Whether a statement's table name is one of `TAP_UPLOAD`'s.
///
/// Asked before the uploads are searched so that a name under that schema is answered by
/// what the request uploaded, rather than falling through to a refusal naming the tables
/// this service publishes — which is not where the caller's table was going to be.
fn names_an_upload(spelling: &str) -> bool {
    spelling
        .split_once('.')
        .is_some_and(|(schema, _)| schema.eq_ignore_ascii_case(UPLOAD_SCHEMA))
}

/// One uploaded table as something a statement can read, and the data-file list a catalog
/// brings with it.
///
/// **Where `UPLOAD_TYPE` said nothing, the url's own name decides**: one matching the
/// data-file globs is a parquet file and anything else is a catalog directory. A directory
/// that is not one fails where a catalog is opened, which is the read that looks for
/// `hats.properties`, `properties` and `collection.properties` and says so by name.
fn uploaded(service: &Service, upload: &Upload) -> Result<(Source, Option<DataFiles>), ApiError> {
    let files = service.data_files_for(&upload.url);
    let kind = upload.kind.unwrap_or(match files.matches_url(&upload.url) {
        true => Kind::Parquet,
        false => Kind::Hats,
    });
    match kind {
        Kind::Parquet => {
            if !files.matches_url(&upload.url) {
                return Err(ApiError::not_found(format!(
                    "UPLOAD {} names no data file; a parquet url ends in a name matching {}",
                    upload.name,
                    files.describe()
                )));
            }
            let file = storage::open(
                &upload.url,
                &upload.storage,
                &service.policy,
                &service.transfers,
            )?;
            Ok((Source::File(file), None))
        }
        Kind::Hats => {
            let files = files.clone();
            let dir = storage::open_dir(
                &upload.url,
                &upload.storage,
                &service.policy,
                &service.transfers,
            )?;
            Ok((Source::Catalog(dir), Some(files)))
        }
    }
}

/// The rows, in the format the request asked for.
///
/// **Only a VOTable can say it was truncated**, `OVERFLOW` being a VOTable marker — so the
/// other two carry [`OVERFLOW_HEADER`] instead. That is this service's own and no client
/// reads it; what it is for is that the fact is stated somewhere rather than nowhere, a
/// delimited body having no room for it and TAP defining nothing for one.
fn encode(answer: &Answer, answering: Answering) -> Result<Response, ApiError> {
    let body = match (answering.format, answer.overflow) {
        (Format::Votable, false) => votable::encode(&answer.result)?,
        (Format::Votable, true) => votable::encode_truncated(&answer.result)?,
        (Format::Dsv(kind), _) => dsv::encode(&answer.result, kind, DSV_NULL)?,
        // The spelling table is the only source of a format here, and it holds these two.
        (other, _) => {
            return Err(ApiError::internal(format!(
                "{} is not a format this resource writes",
                other.name()
            )));
        }
    };
    Ok((
        [
            (header::CONTENT_TYPE, answering.content_type.to_owned()),
            (
                header::HeaderName::from_static(OVERFLOW_HEADER),
                answer.overflow.to_string(),
            ),
        ],
        body,
    )
        .into_response())
}

/// Whether the answer stopped at the row bound, for the formats with nowhere to say it.
const OVERFLOW_HEADER: &str = "x-hats-overflow";

/// The pairs of a query string or a form body, which are the same encoding.
fn pairs(text: &str) -> Vec<(String, String)> {
    url::form_urlencoded::parse(text.as_bytes())
        .into_owned()
        .collect()
}
