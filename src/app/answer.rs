//! What a query answers with: the response bodies, the encodings, and the counts that travel
//! as headers where a body has no room for them.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Instant;

use axum::Json;
use axum::body::Body;
use axum::http::request::Parts;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::app::request::{Format, Output};
use crate::app::service::PARQUET_CONTENT_TYPE;
use crate::engine::query::{self, QueryResult};
use crate::error::ApiError;
use crate::hats;
use crate::output::{dsv, json, parquet, stream, votable};
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
    columns_of_schema(&result.schema)
}

/// The same, from the schema alone — which is what a streamed answer has before it has
/// read a row.
fn columns_of_schema(schema: &datafusion::arrow::datatypes::SchemaRef) -> Vec<Column> {
    schema.fields().iter().map(column_of).collect()
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

/// The answer sent as it is read, in whichever of the streamable encodings was asked for.
///
/// **What a stream cannot carry is a header it has not earned yet.** The counts are known
/// only once the last row is in, and the headers go out before the first one, so a streamed
/// answer has none of them. JSON puts them after its rows instead — a JSON object does not
/// care what order its keys arrive in — which is why the envelope here is written by hand
/// rather than serialized from [`SelectResponse`].
///
/// Parquet is not streamable and is refused before this is reached: it is read footer-first,
/// so a body with no length is one `pyarrow` cannot open at all.
pub(in crate::app) fn streamed(
    planned: query::Planned,
    output: &Output,
    started: Instant,
) -> Result<Response, ApiError> {
    let schema = Arc::clone(&planned.schema);
    let batches = planned.batches()?;
    let mut encoder = encoder_for(output, &schema)?;
    // While a status can still be chosen: `csv` refuses a nested column here, and a
    // refusal after the `200` is a dropped connection rather than a message.
    let head = encoder.begin(&schema)?;
    let content_type = content_type_for(output);
    // The plan outlives the rows, which is what makes the ending readable: `data_bytes_read`
    // is a counter on the scan and is final only once the last batch has come off it.
    let ending = move |rows: usize| stream::Ending {
        rows,
        data_bytes_read: planned.data_bytes_read(),
        elapsed: started.elapsed(),
        overflow: false,
        // One file, and its bounds are the expression limits, which are checked before
        // anything is read. There is nothing here that can stop part-way.
        refused: None,
    };
    let body = Body::from_stream(stream::streamed(encoder, head, batches, ending));
    Ok((
        [
            (header::CONTENT_TYPE, content_type.to_owned()),
            // Said out loud rather than left out: a body with no length is one a client
            // must not seek in, and the alternative is a client discovering that by
            // reading the head of the answer where it asked for the tail.
            (header::ACCEPT_RANGES, "none".to_owned()),
        ],
        body,
    )
        .into_response())
}

/// A catalog's rows, sent as its partitions land.
///
/// The counterpart of [`streamed`] for the route whose rows come from many files rather
/// than one — so what the ending costs is not one plan's counter but what the read
/// accumulated, and a bound reached part-way is a `refused` the encoder renders rather
/// than the `422` with a work list a collected answer would have given.
pub(in crate::app) fn streamed_rows<S>(
    schema: &datafusion::arrow::datatypes::SchemaRef,
    batches: S,
    refused: Arc<std::sync::Mutex<Option<hats::query::Exceeded>>>,
    output: &Output,
    started: Instant,
) -> Result<Response, ApiError>
where
    S: futures::Stream<Item = Result<datafusion::arrow::array::RecordBatch, ApiError>>
        + Send
        + 'static,
{
    let mut encoder = encoder_for(output, schema)?;
    // While a status can still be chosen, for the reason [`streamed`] gives.
    let head = encoder.begin(schema)?;
    let content_type = content_type_for(output);
    let ending = move |rows: usize| stream::Ending {
        rows,
        // A streamed catalog read reports no byte count: it is summed as the partitions
        // land, and the sum belongs to the read rather than to any one plan. What it cost
        // is in the log line, which is the operator's rather than the caller's.
        data_bytes_read: 0,
        elapsed: started.elapsed(),
        overflow: false,
        refused: refused
            .lock()
            .ok()
            .and_then(|held| held.map(|why| why.to_string())),
    };
    let body = Body::from_stream(stream::streamed(encoder, head, batches, ending));
    Ok((
        [
            (header::CONTENT_TYPE, content_type.to_owned()),
            (header::ACCEPT_RANGES, "none".to_owned()),
        ],
        body,
    )
        .into_response())
}

/// Which encoder writes this answer, and the one format that has no streamed form.
fn encoder_for(
    output: &Output,
    schema: &datafusion::arrow::datatypes::SchemaRef,
) -> Result<Box<dyn stream::Encoder>, ApiError> {
    Ok(match output.format {
        Format::Json => Box::new(JsonBody::new(columns_of_schema(schema))),
        Format::Dsv(kind) => Box::new(dsv::Delimited::new(kind, &output.dsv_null)),
        Format::Votable => Box::new(votable::Document::new(None)),
        Format::Parquet => {
            return Err(ApiError::bad_request(
                "parquet cannot be streamed: it is read from its footer backwards, so a \
                 reader needs the whole file; ask for json, csv, tsv or votable, or leave \
                 streaming out",
            ));
        }
    })
}

fn content_type_for(output: &Output) -> &'static str {
    match output.format {
        Format::Json => "application/json",
        Format::Dsv(kind) => kind.content_type(),
        Format::Votable => votable::CONTENT_TYPE,
        Format::Parquet => PARQUET_CONTENT_TYPE,
    }
}

/// The same body [`SelectResponse`] is, written around rows that have not arrived yet.
///
/// The fields are the same fields and the counts are the same counts; what moves is where
/// they sit. `schema` is known before the first row and goes first, as it does today, and
/// the three counts are known only after the last one and go after `rows` — which is a
/// difference no JSON reader can see, keys in an object being unordered.
struct JsonBody {
    columns: Vec<Column>,
    rows: json::Rows,
    wrote: bool,
}

impl JsonBody {
    fn new(columns: Vec<Column>) -> Self {
        Self {
            columns,
            rows: json::Rows::new(),
            wrote: false,
        }
    }
}

impl stream::Encoder for JsonBody {
    fn begin(
        &mut self,
        schema: &datafusion::arrow::datatypes::SchemaRef,
    ) -> Result<Vec<u8>, ApiError> {
        let columns = serde_json::to_string(&self.columns)?;
        let mut out = format!("{{\"schema\":{columns},\"rows\":").into_bytes();
        out.extend_from_slice(&self.rows.begin(schema)?);
        Ok(out)
    }

    fn rows(&mut self, batch: &datafusion::arrow::array::RecordBatch) -> Result<Vec<u8>, ApiError> {
        let bytes = self.rows.rows(batch)?;
        self.wrote |= !bytes.is_empty();
        Ok(bytes)
    }

    fn end(&mut self, ending: stream::Ending) -> Result<Vec<u8>, ApiError> {
        let mut out = self.rows.end(ending.clone())?;
        // An answer of no rows at all: the row writer never opened its array, and an empty
        // one still has to be there for `rows` to be a list.
        if !self.wrote {
            out.extend_from_slice(b"[]");
        }
        let mut counts = format!(
            ",\"num_rows\":{},\"data_bytes_read\":{},\"elapsed_ms\":{}",
            ending.rows,
            ending.data_bytes_read,
            ending.elapsed.as_millis(),
        );
        // Where a bound stopped the rows, the body says so rather than ending as though
        // it were whole. A collected answer never reaches here: there, a bound is a `422`
        // carrying the work list, which is what a caller can act on and what a stream that
        // has already sent its status cannot go back and offer.
        if let Some(why) = &ending.refused {
            let _ = write!(
                counts,
                ",\"refused\":{}",
                serde_json::Value::from(why.as_str())
            );
        }
        counts.push('}');
        out.extend_from_slice(counts.as_bytes());
        Ok(out)
    }
}

/// The same body, written straight out rather than through `serde_json::Value`.
///
/// **The detour was most of what a JSON answer cost.** Running the arrow writer to bytes,
/// parsing those into a `Value` per cell and serializing them again held 481 MB for a
/// 53 MB answer and took 460 ms where the writing alone takes 57 — so the collected path
/// now drives the same encoder a streamed one does, and the `Value`s are gone from both.
///
/// [`SelectResponse`] stays as the type the description publishes: it is what the body
/// *is*, and `utoipa` reads it to say so.
pub(in crate::app) fn json_response(
    result: &QueryResult,
    started: Instant,
) -> Result<Response, ApiError> {
    let mut body = JsonBody::new(columns_of(result));
    let bytes = stream::collected(
        &mut body,
        result,
        stream::Ending {
            rows: result.num_rows(),
            data_bytes_read: result.data_bytes_read,
            elapsed: started.elapsed(),
            overflow: false,
            refused: None,
        },
    )?;
    Ok((
        [(header::CONTENT_TYPE, "application/json")],
        Body::from(bytes),
    )
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
