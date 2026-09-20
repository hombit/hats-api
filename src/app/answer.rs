//! What a query answers with: the response bodies, the encodings, and the counts that travel
//! as headers where a body has no room for them.

use std::time::Instant;

use axum::Json;
use axum::http::request::Parts;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::app::request::{Format, Output};
use crate::app::service::PARQUET_CONTENT_TYPE;
use crate::engine::query::QueryResult;
use crate::error::ApiError;
use crate::hats;
use crate::output::{dsv, json, parquet, votable};
use crate::storage::RemoteFile;

/// One column of the answer. Sent even when no rows matched, so it is where the answer's shape
/// can always be read.
// Rows do not describe themselves, and an empty answer looks exactly like a file that has not
// got the column — hence sending this for an empty answer and for `limit=0`.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(in crate::app) struct Column {
    /// The column's name, spelled as the file spells it.
    name: String,
    /// The arrow type, in arrow's own spelling — `Int64`, `Float32`,
    /// `List(Float32, field: 'element')`. This is what says whether a value needs quoting
    /// when you write it into a predicate.
    r#type: String,
    /// A struct column's own fields, one level down — the parts of a packed light curve, say.
    ///
    /// Ask for one by joining its name to the column's with a dot: `lightcurve.mag`. Quote each
    /// part on its own where it needs quoting: `"lightcurve"."mag"`. A field that is itself a
    /// struct is listed here; its own fields are not.
    // Named rather than left to the type string, which spells them only in arrow's `Display`.
    // One level is what a projection can address and what the value carries, and it is also
    // what stops the schema, which is recursive, from being walked forever.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[schema(no_recursion)]
    fields: Vec<Column>,
}

/// A catalog's answer: the rows, and what it cost to find them.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(in crate::app) struct HatsResponse {
    /// How many rows are in `rows`.
    num_rows: usize,
    /// How many of the catalog's partitions were read.
    ///
    /// This is how to tell whether a `region` narrowed anything: four partitions and the whole
    /// catalog can return the same rows, and only this says which of the two happened.
    num_partitions: usize,
    /// The columns of the answer, in order.
    schema: Vec<Column>,
    /// Bytes read from the store to answer this. Two queries that return the same rows can
    /// read very different amounts, and this is where the difference shows.
    data_bytes_read: u64,
    /// How long the query took, in milliseconds.
    #[schema(value_type = u64)]
    elapsed_ms: u128,
    /// One object per row, keyed by the names in `schema`.
    #[schema(value_type = Vec<Object>)]
    rows: Vec<serde_json::Value>,
}

/// One file's answer: the rows, and what it cost to read them.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(in crate::app) struct SelectResponse {
    /// How many rows are in `rows`.
    num_rows: usize,
    /// The columns of the answer, in order: what the projection asked for, or the file's whole
    /// schema if it did not ask.
    schema: Vec<Column>,
    /// Bytes read from the file to answer this. Two queries that return the same rows can read
    /// very different amounts, and this is where the difference shows.
    data_bytes_read: u64,
    /// How long the query took, in milliseconds.
    #[schema(value_type = u64)]
    elapsed_ms: u128,
    /// One object per row, keyed by the names in `schema`.
    #[schema(value_type = Vec<Object>)]
    rows: Vec<serde_json::Value>,
}

/// The counts and the timing are part of the JSON body; a parquet body has no room for
/// them, so they travel as headers instead and both formats report the same numbers.
pub(in crate::app) const NUM_ROWS_HEADER: &str = "x-hats-num-rows";
pub(in crate::app) const DATA_BYTES_READ_HEADER: &str = "x-hats-data-bytes-read";
const ELAPSED_MS_HEADER: &str = "x-hats-elapsed-ms";

/// How many partitions of the catalog the answer was read from, which is the number that
/// says whether a region pruned. A parquet body has no room for it, so it travels beside the
/// other three.
const NUM_PARTITIONS_HEADER: &str = "x-hats-num-partitions";

/// The catalog's answer, in whichever encoding was asked for.
///
/// A parquet body copies its layout from one of the partitions actually read, which is the
/// nearest thing to "the source file" a catalog has. A request that read nothing gets the
/// writer's own defaults, there being no file to copy from.
pub(in crate::app) async fn hats_answer(
    result: &hats::query::CatalogResult,
    output: &Output,
    started: Instant,
    parts: Option<&Parts>,
) -> Result<Response, ApiError> {
    let num_rows = result.rows.num_rows();
    let response = match output.format {
        Format::Json => {
            let rows = json::to_json(&result.rows)?;
            let schema = columns_of(&result.rows);
            Json(HatsResponse {
                num_rows: rows.len(),
                num_partitions: result.partitions_read,
                schema,
                data_bytes_read: result.rows.data_bytes_read,
                elapsed_ms: started.elapsed().as_millis(),
                rows,
            })
            .into_response()
        }
        Format::Parquet => {
            let layout = match &result.source {
                Some(file) => parquet::read_layout(file).await?,
                None => parquet::SourceLayout::default(),
            };
            let body = parquet::encode(&result.rows, &layout)?;
            return Ok(seekable(
                body,
                (
                    attachment(PARQUET_CONTENT_TYPE, "selection.parquet"),
                    hats_counters(result, num_rows, started),
                ),
                parts,
            ));
        }
        Format::Votable => (
            attachment(votable::CONTENT_TYPE, "selection.vot"),
            hats_counters(result, num_rows, started),
            votable::encode(&result.rows)?,
        )
            .into_response(),
        Format::Dsv(kind) => (
            attachment(kind.content_type(), &format!("selection.{}", kind.name())),
            hats_counters(result, num_rows, started),
            dsv::encode(&result.rows, kind, &output.dsv_null)?,
        )
            .into_response(),
    };
    Ok(without_ranges(response))
}

/// An answer no client may seek in: generated for this request, and not parquet.
///
/// Saying so matters because the alternative failure is silent. A client that sends `Range`
/// and gets a plain `200` — legal under RFC 7233 for a server without ranges — may still
/// trust the byte count it asked for and read that many bytes off the front of the whole
/// body, taking the head of the answer for whatever slice it wanted.
fn without_ranges(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("none"));
    response
}

/// A parquet answer, offered as something a reader can seek in.
///
/// **Parquet is read footer-first or not at all**, so a body that refuses ranges is a body
/// `pyarrow` cannot open: `fsspec` reports such a url as `partial: False`, hands back a
/// streaming file, and the read ends at `Cannot seek streaming HTTP file`. That is what an
/// `lsdb` client meets on every answer larger than one of its blocks — which is every
/// partition of a real catalog — so the query surface this service publishes is unusable
/// from the client it was built for unless the answer can be sliced.
///
/// The slice comes from the body this request generated: there is no cache, so a client
/// reading a footer and then three column chunks runs the query four times. That is the
/// price of the seek, and it is bounded by what the client asks for rather than by
/// anything here. Two consequences worth knowing:
///
/// - The answer has to be **the same bytes each time** for the slices to agree, which holds
///   because the same query over the same file is answered in the file's own order by a
///   writer that is given the same layout. A source file that changes under the client is
///   the one case where it does not, and no validator here could make it.
/// - Only parquet is offered this way. It is the one format read by seeking, and — being
///   excluded from the compression layer — the one whose `Content-Length` a client can
///   trust. A ranged JSON body would be a slice of something a gzip layer above may then
///   re-encode.
fn seekable(
    bytes: Vec<u8>,
    headers: impl axum::response::IntoResponseParts,
    parts: Option<&Parts>,
) -> Response {
    // The API mode answers a `POST` carrying a body, where a `Range` is not a request for
    // part of anything, so it passes no parts and keeps the old refusal. Advertising ranges
    // there would be a claim nothing on that route can honour.
    let Some(parts) = parts else {
        return without_ranges((headers, bytes).into_response());
    };
    let size = bytes.len() as u64;
    let mut response = match parts.headers.get(header::RANGE) {
        None => (headers, bytes).into_response(),
        Some(asked) => match crate::app::files::wanted_range(asked, size) {
            // Refused rather than answered whole: a client that asked for the tail and got
            // the head cannot tell the two apart.
            None => (
                StatusCode::RANGE_NOT_SATISFIABLE,
                [(header::CONTENT_RANGE, format!("bytes */{size}"))],
            )
                .into_response(),
            // `wanted_range` validated the range against this very body, so the slice is
            // there; `get` rather than an index so that a future caller who validates
            // against something else gets a refusal instead of a panic.
            Some(range) => match usize::try_from(range.start)
                .ok()
                .zip(usize::try_from(range.end).ok())
                .and_then(|(start, end)| bytes.get(start..end))
            {
                None => ApiError::internal("cannot cut this range").into_response(),
                Some(slice) => (
                    StatusCode::PARTIAL_CONTENT,
                    headers,
                    [(
                        header::CONTENT_RANGE,
                        format!("bytes {}-{}/{size}", range.start, range.end - 1),
                    )],
                    slice.to_vec(),
                )
                    .into_response(),
            },
        },
    };
    response
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response
}

/// What a body that is not JSON is served as, and what to call it once it is saved. The
/// counts have no room in any of those bodies, so they travel as headers instead.
pub(in crate::app) fn attachment(
    content_type: &str,
    name: &str,
) -> [(header::HeaderName, String); 2] {
    [
        (header::CONTENT_TYPE, content_type.to_owned()),
        (
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{name}\""),
        ),
    ]
}

/// What a catalog request reports beside its rows, whichever encoding carries them.
fn hats_counters(
    result: &hats::query::CatalogResult,
    num_rows: usize,
    started: Instant,
) -> [(&'static str, String); 4] {
    [
        (NUM_ROWS_HEADER, num_rows.to_string()),
        (NUM_PARTITIONS_HEADER, result.partitions_read.to_string()),
        (
            DATA_BYTES_READ_HEADER,
            result.rows.data_bytes_read.to_string(),
        ),
        (ELAPSED_MS_HEADER, started.elapsed().as_millis().to_string()),
    ]
}

/// The columns of an answer, as the schema describes them.
fn columns_of(result: &QueryResult) -> Vec<Column> {
    result.schema.fields().iter().map(column_of).collect()
}

/// One field, with a struct's own fields under it.
///
/// Only a struct: `sources.mjd` is planned as a compound identifier, and that resolves
/// through a struct and through nothing else. A list of structs holds the same names and is
/// not reachable by writing one, so listing its fields would offer a name that does not
/// answer — worse than not listing it, since the caller cannot tell which kind they have
/// without reading the type.
fn column_of(field: &datafusion::arrow::datatypes::FieldRef) -> Column {
    Column {
        name: field.name().clone(),
        r#type: field.data_type().to_string(),
        fields: match field.data_type() {
            datafusion::arrow::datatypes::DataType::Struct(fields) => fields
                .iter()
                .map(|inner| Column {
                    name: inner.name().clone(),
                    r#type: inner.data_type().to_string(),
                    // One level. The walk stops here rather than recursing.
                    fields: Vec::new(),
                })
                .collect(),
            _ => Vec::new(),
        },
    }
}

/// The result, in whichever encoding was asked for. Both modes answer through here, so
/// the same query returns the same bytes whichever one carried it.
///
/// `layout` is the source file's, fetched by the caller alongside the row query rather
/// than after it — `read_layout` needs only `file`, not the rows — and is `None`
/// whenever `output.format` is not `Parquet`, there being nothing to copy for any other
/// encoding.
pub(in crate::app) async fn answer(
    result: &QueryResult,
    file: &RemoteFile,
    output: &Output,
    started: Instant,
    layout: Option<parquet::SourceLayout>,
    parts: Option<&Parts>,
) -> Result<Response, ApiError> {
    let response = match output.format {
        Format::Json => json_response(result, started)?,
        Format::Parquet => {
            return parquet_response(result, file, result.num_rows(), started, layout, parts);
        }
        Format::Votable => (
            attachment(votable::CONTENT_TYPE, &download_name(file, "vot")),
            counters(result, result.num_rows(), started),
            votable::encode(result)?,
        )
            .into_response(),
        Format::Dsv(kind) => (
            attachment(kind.content_type(), &download_name(file, kind.name())),
            counters(result, result.num_rows(), started),
            dsv::encode(result, kind, &output.dsv_null)?,
        )
            .into_response(),
    };
    Ok(without_ranges(response))
}

/// What a single-file request reports beside its rows, whichever encoding carries them.
pub(in crate::app) fn counters(
    result: &QueryResult,
    num_rows: usize,
    started: Instant,
) -> [(&'static str, String); 3] {
    [
        (NUM_ROWS_HEADER, num_rows.to_string()),
        (DATA_BYTES_READ_HEADER, result.data_bytes_read.to_string()),
        (ELAPSED_MS_HEADER, started.elapsed().as_millis().to_string()),
    ]
}

pub(in crate::app) fn json_response(
    result: &QueryResult,
    started: Instant,
) -> Result<Response, ApiError> {
    let rows = json::to_json(result)?;
    let schema = columns_of(result);
    Ok(Json(SelectResponse {
        num_rows: rows.len(),
        schema,
        data_bytes_read: result.data_bytes_read,
        elapsed_ms: started.elapsed().as_millis(),
        rows,
    })
    .into_response())
}

/// The answer as a parquet file laid out like the file it came from, which costs one
/// extra footer read of that file.
fn parquet_response(
    result: &QueryResult,
    file: &RemoteFile,
    num_rows: usize,
    started: Instant,
    layout: Option<parquet::SourceLayout>,
    parts: Option<&Parts>,
) -> Result<Response, ApiError> {
    let body = parquet::encode(result, &layout.unwrap_or_default())?;
    Ok(parquet_answer(
        body,
        file,
        Counts {
            num_rows,
            data_bytes_read: result.data_bytes_read,
        },
        started,
        parts,
    ))
}

/// What a parquet answer reports beside its bytes. Carried apart from the rows so that an
/// answer served again from the cache reports what reading it actually cost, rather than
/// the nothing this request spent.
#[derive(Debug, Clone, Copy)]
pub(in crate::app) struct Counts {
    pub num_rows: usize,
    pub data_bytes_read: u64,
}

/// A parquet body, named after the file it came from and offered as a seekable resource.
///
/// The one place a parquet answer becomes a response, so an answer generated now and one
/// handed back from the cache are the same bytes under the same headers.
pub(in crate::app) fn parquet_answer(
    body: Vec<u8>,
    file: &RemoteFile,
    counts: Counts,
    started: Instant,
    parts: Option<&Parts>,
) -> Response {
    seekable(
        body,
        (
            attachment(PARQUET_CONTENT_TYPE, &download_name(file, "parquet")),
            [
                (NUM_ROWS_HEADER, counts.num_rows.to_string()),
                (DATA_BYTES_READ_HEADER, counts.data_bytes_read.to_string()),
                (ELAPSED_MS_HEADER, started.elapsed().as_millis().to_string()),
            ],
        ),
        parts,
    )
}

/// Name the download after the source object, so a directory of these files says which
/// partition each came from. Falls back to a fixed name for a url that ends in a slash
/// — `open` already rejected the ones with no object at all.
///
/// The source's own `.parquet` is dropped rather than kept, so that one partition
/// answered in two encodings is two files with two names rather than one name on a body
/// that is not parquet at all.
fn download_name(file: &RemoteFile, extension: &str) -> String {
    let name = file
        .url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .filter(|segment| !segment.is_empty() && !segment.contains('"'))
        .unwrap_or("selection");
    let stem = name.strip_suffix(".parquet").unwrap_or(name);
    format!("{stem}.{extension}")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::access::AccessPolicy;
    use crate::config::LimitsConfig;
    use crate::storage::materialize::Transfers;
    use crate::storage::{self, StorageOptions, parse_url};

    use super::*;

    #[test]
    fn names_the_download_after_the_source_object() {
        let policy = AccessPolicy::default();
        let transfers = Arc::new(Transfers::new(&LimitsConfig::default()));
        let name = |raw: &str, extension: &str| {
            let url = parse_url(raw).unwrap();
            download_name(
                &storage::open(&url, &StorageOptions::default(), &policy, &transfers).unwrap(),
                extension,
            )
        };
        assert_eq!(
            name("s3://b/dir/part0.snappy.parquet", "parquet"),
            "part0.snappy.parquet"
        );
        // HATS partition paths, and anything else that is not already a parquet name.
        assert_eq!(
            name("s3://b/Norder=5/Npix=12240/part0", "parquet"),
            "part0.parquet"
        );
        // One partition in two encodings is two names, rather than one name on a body
        // that is not parquet.
        assert_eq!(name("s3://b/dir/part0.parquet", "vot"), "part0.vot");
    }

    /// A column's `type` is arrow's own `Display`, which makes an arrow upgrade able to
    /// change this API without changing a line of this crate. These are the spellings as
    /// published, so a failure here is that change arriving rather than a mistake in the
    /// test: decide whether to publish the new spelling or to map to one of our own, and
    /// say so in the answer, `/docs` and `openapi.json` all at once.
    #[test]
    fn the_published_column_types_are_arrows_spelling_and_it_has_not_moved() {
        use datafusion::arrow::datatypes::{DataType, Field, Fields};

        // `element` is the name arrow's parquet writer gives a list's item field, so it is
        // the one a caller's file arrives with.
        let float = || Field::new("element", DataType::Float32, true);
        let sources = || {
            Fields::from(vec![
                Field::new("mjd", DataType::Float64, true),
                Field::new("band", DataType::Utf8, true),
            ])
        };
        // Every type an astronomy parquet puts in front of a caller: the scalars, a name,
        // the two shapes a light curve is packed into, and a fixed-width vector.
        let published = [
            (DataType::Boolean, "Boolean"),
            (DataType::Int32, "Int32"),
            (DataType::Int64, "Int64"),
            (DataType::UInt64, "UInt64"),
            (DataType::Float32, "Float32"),
            (DataType::Float64, "Float64"),
            (DataType::Utf8, "Utf8"),
            (
                DataType::List(Arc::new(float())),
                "List(Float32, field: 'element')",
            ),
            (
                DataType::FixedSizeList(Arc::new(float()), 3),
                "FixedSizeList(3 x Float32, field: 'element')",
            ),
            (
                DataType::Struct(sources()),
                "Struct(\"mjd\": Float64, \"band\": Utf8)",
            ),
        ];
        // Compared as one list rather than one at a time, so a respelling shows every
        // type it touched instead of stopping at the first.
        let spelled: Vec<String> = published
            .iter()
            .map(|(data_type, _)| {
                column_of(&Arc::new(Field::new("c", data_type.clone(), true))).r#type
            })
            .collect();
        let expected: Vec<String> = published
            .iter()
            .map(|(_, spelling)| (*spelling).to_owned())
            .collect();
        assert_eq!(spelled, expected);
    }
}
