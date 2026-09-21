//! `POST {api.prefix}/simple/parquet`: a projection and a predicate against one parquet file the
//! url names outright.

use std::collections::BTreeMap;
use std::time::Instant;

use axum::extract::{State, rejection::JsonRejection};
use axum::response::{Json, Response};
use serde::Deserialize;
use serde::de::IgnoredAny;

use futures::TryStreamExt as _;
use futures::future::OptionFuture;

use crate::app::answer::{answer, streamable, streamed};
use crate::app::request::{
    Format, Output, body_error, predicate_of, projection_of, refuse_unknown, takes,
};
use crate::app::service::Service;
use crate::engine::query::{self, Order, Selection};
use crate::error::ApiError;
use crate::output::parquet;
use crate::sky::region::{Healpix, Region, Spatial};
use crate::storage::{self, SourceUrl, StorageOptions, parse_url};

/// A query against one parquet file, which the url names outright.
///
/// Its own type, not a shared body with the catalog's: a file says nothing about which of its
/// columns are a position or an index, and a catalog answers both for itself, so the two
/// endpoints take different fields. Being different types is what makes that structural — the
/// column names are not fields of a catalog request at all, so there is nothing there to drop
/// in silence or to honour by a later change, and the description shows each endpoint the
/// fields it takes rather than every field either takes with a sentence about which.
///
/// What the endpoints share is the code below them, not the body above them: each lowers to
/// the internal selection its own query layer takes.
///
/// The fields are declared in the order a body is written in — what to read, how to reach it,
/// where on the sky, what to ask of it, and how the answer comes back — which is the order the
/// description lists them in.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub(in crate::app) struct ParquetQuery {
    /// The parquet file to read. Its scheme picks the backend — `s3`, `gs`, `az`, `https`,
    /// `webdav`, `hf` or `file` — and which of those a deployment answers for is the operator's to
    /// configure.
    // Treated as opaque: whatever query string it has belongs to the origin, not to us.
    // No `example` on the field: every operation's own example carries a url this route
    // answers, and a second one here is a second thing to keep true.
    #[schema(value_type = String)]
    url: SourceUrl,
    /// How to reach the store: an endpoint, a region, credentials. Leave it out for a public
    /// object read anonymously, which is the common case. Which options apply is decided by
    /// the url's scheme, and one that does not apply is refused rather than ignored.
    #[serde(default)]
    storage: StorageOptions,
    /// One or more shapes on the sky. A row inside any of them qualifies — the array is a
    /// union — and the whole field is ANDed with the predicate. `ra_column` and `dec_column`
    /// are required alongside it.
    region: Option<Vec<Region>>,
    /// Which column holds right ascension, in degrees. Required whenever `region` is given.
    // A parquet file carries nothing that says which of its columns are a position, so a guess
    // from conventional names would answer a different question than the one asked.
    #[schema(example = "objra")]
    ra_column: Option<String>,
    /// Which column holds declination, in degrees. Required alongside `ra_column`.
    #[schema(example = "objdec")]
    dec_column: Option<String>,
    /// A HEALPix index column, if the file has one — `_healpix_29` for a HATS catalog that
    /// took the recommendation. Purely an accelerator: it changes what a query costs and never
    /// which rows come back. Give `healpix_order` with it.
    #[schema(example = "_healpix_29")]
    healpix_column: Option<String>,
    /// The order the values in `healpix_column` are written at. Never inferred from the
    /// column's name — read at the wrong order, every bound is one no row satisfies, which
    /// returns nothing rather than failing.
    #[schema(example = 29)]
    healpix_order: Option<u8>,
    /// The columns to return, one name per element, and nothing computed — for that, write an
    /// ADQL query. Write a name as the file spells it, in double quotes where the spelling needs
    /// them. A dotted name reaches inside a struct column, and comes back as that column
    /// carrying the fields you named. Leave the field out for every column; an empty list is
    /// refused rather than read as one or the other.
    #[schema(example = json!(["objectid", "objra", "objdec", "lightcurve.mag"]))]
    columns: Option<Vec<String>>,
    /// The row condition: one boolean expression over this file's columns, the condition that
    /// would follow a `WHERE` keyword. Write a column as the file spells it, in double quotes
    /// where the spelling needs them — `"Gmag" < 20`. Leave it out for every row.
    #[schema(example = "objdec > 60 AND nepochs > 100")]
    filters: Option<String>,
    /// `json`, the default; `parquet` for the answer as a parquet file laid out like the file
    /// it came from; `votable` for a VOTable, `csv` for comma-separated text and `tsv` for
    /// tab-separated. The last three take flat columns only and refuse a nested one by name.
    /// Anything but `json` carries its counts in `x-hats-*` response headers, there being no
    /// room in the body.
    #[schema(example = "json")]
    format: Option<String>,
    /// What a null is written as in `csv` and `tsv`. Absent, a null is an empty field — the
    /// spelling an empty string also has, so the two cannot be told apart until this is set.
    /// At most 128 bytes, and no `,`, tab, newline or carriage return, whichever of the two
    /// formats was asked for.
    #[schema(example = "NULL")]
    dsv_null_value: Option<String>,
    /// At most this many rows. The order is not promised, but the same request returns the
    /// same rows.
    #[schema(example = 100)]
    limit: Option<usize>,
    /// Send the answer as it is read, rather than reading it all and then sending it.
    ///
    /// The rows are the same rows. What changes is that the body starts arriving sooner and
    /// this service holds less of it at once — and that the answer arrives without the
    /// `x-hats-*` counts, which are headers and so are sent before the first row is known.
    /// `json` reports them at the end of its body instead; a streamed `votable` leaves
    /// `nrows` off its `TABLE`, which is optional for this reason.
    ///
    /// **A read that fails after the first row is said at the end of the body**, there being
    /// no status left to send it as: `json` ends with a `refused` naming it, `votable` with
    /// `QUERY_STATUS="ERROR"`, and `csv` and `tsv` end without the transfer completing.
    ///
    /// Not for `parquet`, which is read footer-first: a body with no length is one a
    /// reader cannot seek in, so asking for both is refused rather than quietly answered
    /// whole.
    #[serde(default)]
    #[schema(example = false)]
    streaming: bool,
    /// Every key the body carried that this endpoint has no field for.
    #[serde(flatten)]
    #[schema(ignore)]
    unknown: BTreeMap<String, IgnoredAny>,
}

impl ParquetQuery {
    /// Every field this endpoint takes, in the order a body is written in, for a refusal to
    /// name. `each_route_describes_its_own_body` holds it to the fields the description lists,
    /// in this order.
    pub(in crate::app) fn fields() -> Vec<&'static str> {
        vec![
            "url",
            "storage",
            "region",
            "ra_column",
            "dec_column",
            "healpix_column",
            "healpix_order",
            "columns",
            "filters",
            "format",
            "dsv_null_value",
            "limit",
            "streaming",
        ]
    }

    /// The same list, as the sentence a refusal ends with.
    fn takes() -> String {
        takes(&Self::fields())
    }

    /// What to read: the request lowered to what the query layer runs, which is the same type
    /// the catalog endpoint's own lowering feeds.
    fn selection(&self) -> Result<Selection<'_>, ApiError> {
        Ok(Selection {
            projection: projection_of(self.columns.as_deref()),
            predicate: predicate_of(self.filters.as_deref()),
            spatial: self.spatial()?,
            limit: self.limit,
        })
    }

    /// The region and the two columns it is tested against, which travel together or not
    /// at all.
    ///
    /// No part of this is dropped for want of another. A `region` with no columns named has
    /// nothing to test and would return every row in the file, which a caller cannot tell
    /// from a region that contained them all; a column named with no region does nothing,
    /// so a request carrying one is a caller who believes otherwise.
    ///
    /// The index column is the part that may be left out of a region search, since a file
    /// need not have one and a query is answered the same either way. What it may not be is
    /// half-given: the column without the order, or the order without the column, is a
    /// request that means nothing until both are known. Named with no region at all it is
    /// refused like the coordinates are — it would accelerate nothing.
    fn spatial(&self) -> Result<Option<Spatial<'_>>, ApiError> {
        let healpix = match (self.healpix_column.as_deref(), self.healpix_order) {
            (None, None) => None,
            // The caller wrote it, so a file without it is their mistake and not a file
            // that happens to have no index.
            (Some(column), Some(order)) => Some(Healpix { column, order }),
            _ => return Err(ApiError::bad_request(HEALPIX_PAIR)),
        };
        if self.region.is_none() && healpix.is_some() {
            return Err(ApiError::bad_request(
                "healpix_column needs a region; to filter on that column alone, use filters",
            ));
        }
        let Some(regions) = self.region.as_deref() else {
            return match self.ra_column.is_some() || self.dec_column.is_some() {
                true => Err(ApiError::bad_request(
                    "ra_column and dec_column need a region",
                )),
                false => Ok(None),
            };
        };
        // Which columns a region needs is the region's business: a `moc` is cells and reads
        // no position, so it needs neither. Asked here rather than left to the planner, so
        // that a request missing a column it needs is refused from its own body — before a
        // store is opened for a request that was never going to run.
        let needs_coordinates = regions.iter().any(Region::needs_coordinates);
        if needs_coordinates && (self.ra_column.is_none() || self.dec_column.is_none()) {
            return Err(ApiError::bad_request(
                "region needs ra_column and dec_column",
            ));
        }
        Ok(Some(Spatial {
            regions,
            ra_column: self.ra_column.as_deref(),
            dec_column: self.dec_column.as_deref(),
            healpix,
            // A url naming one file names no catalog, so nothing here says the file is a
            // partition of one. The HATS routes fill this in.
            partition: None,
            // And names one table, so a column needs no qualifier.
            relation: None,
        }))
    }
}

/// Half of the pair is not half a request. The column may be called anything and be written
/// at any order, so the order is what says which cell a value names — and reading a column
/// at the wrong order puts every bound where no row is, which returns nothing rather than
/// failing. None of which the caller needs; they need to send the other field.
const HEALPIX_PAIR: &str = "healpix_column and healpix_order must be given together";

pub(in crate::app) async fn query_parquet(
    State(service): State<Service>,
    body: Result<Json<ParquetQuery>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(params) = body.map_err(|rejection| body_error(&rejection, &ParquetQuery::takes()))?;
    let started = Instant::now();
    refuse_unknown(&params.unknown, &ParquetQuery::takes())?;
    // Everything decidable from the request alone, before anything is opened.
    let output = Output::parse(
        params.format.as_deref(),
        params.dsv_null_value.as_deref(),
        Format::Json,
    )?;
    // A format with no streamed form is a fault in the request itself, so it is answered
    // here with the rest of what the body alone decides rather than after a file is opened.
    if params.streaming {
        streamable(&output)?;
    }
    let selection = params.selection()?;
    let url = parse_url(params.url.as_str())?;
    // The API has only one thing to do with an object, so a url naming something it does
    // not read as data names nothing this route serves. Answered before the store is
    // built, so a request for the wrong object costs no connection.
    let data_files = service.data_files_for(&url);
    if !data_files.matches_url(&url) {
        return Err(ApiError::not_found(format!(
            "this url does not name a data file; url must end in a name matching {}",
            data_files.describe()
        )));
    }
    let file = storage::open(&url, &params.storage, &service.policy, &service.transfers)?;
    // A local url resolved to a place on the disk that the caller did not write and must
    // not be shown: they named a mount's `path`, and a store's message names its
    // `source`. So a local answer is mapped the way a mounted one is. A remote one keeps
    // its own message, where the path in it is the caller's own url.
    let on_disk = file.url.to_file_path().ok();
    let hide_the_path = |error: ApiError| match &on_disk {
        Some(path) => error.from_mount(path),
        None => error,
    };
    // No order promised: the caller named a url and asked for rows, not for a view of a
    // file's layout. A `limit` is still answered reproducibly — that is `query`'s own
    // rule, since which rows come back is a different question from what order they are
    // in.
    //
    // The layout read needs only `file`, not the rows, so it runs alongside the query
    // rather than after it — the two round trips overlap instead of adding up. Fetched
    // only when the answer will actually be parquet, there being nothing to copy for
    // any other encoding.
    // Streamed, where the caller asked for it: the rows go out as they come off the scan,
    // so nothing here holds the whole answer and the body starts before the last row is
    // read. The log line is the one thing that suffers — what a streamed answer cost is
    // known only once it has been sent, and by then this handler has returned.
    if params.streaming {
        let planned = query::plan(&file, &selection, service.sql_limits, Order::Unspecified)
            .await
            .map_err(hide_the_path)?;
        // What a failed read says reaches the caller at the end of the document, so it is
        // shaped the way a refusal would have been — and by a copy of the rule, the body
        // outliving this handler.
        let hide_in_body = {
            let on_disk = on_disk.clone();
            move |error: ApiError| match &on_disk {
                Some(path) => error.from_mount(path),
                None => error,
            }
        };
        let batches = planned
            .batches()
            .map_err(hide_the_path)?
            .map_err(hide_in_body);
        tracing::info!(
            url = %file.url,
            selected = params.columns.is_some(),
            filtered = params.filters.is_some(),
            regions = params.region.as_ref().map_or(0, Vec::len),
            format = output.format.name(),
            streaming = true,
            elapsed_ms = started.elapsed().as_millis(),
            "query"
        );
        return streamed(planned, batches, &output, started).map_err(hide_the_path);
    }
    let layout_future: OptionFuture<_> = matches!(output.format, Format::Parquet)
        .then(|| parquet::read_layout(&file))
        .into();
    let (result, layout) = tokio::join!(
        query::run(&file, &selection, service.sql_limits, Order::Unspecified),
        layout_future,
    );
    let result = result.map_err(hide_the_path)?;
    let layout = layout.transpose().map_err(hide_the_path)?;

    let num_rows = result.num_rows();
    let data_bytes_read = result.data_bytes_read;
    // Both of them, the way the mounted path does it: encoding a parquet answer reads
    // the source file's layout, so it raises the store's messages too.
    let response = answer(&result, &file, &output, started, layout, None)
        .await
        .map_err(hide_the_path)?;
    tracing::info!(
        // file.url, not the parameter: the parameter may carry credentials. The two
        // expressions are the caller's own text and can be megabytes of `IN` list, so
        // what is logged is that they were there.
        url = %file.url,
        selected = params.columns.is_some(),
        filtered = params.filters.is_some(),
        // How many shapes, not what they were: a region is small, but logging the
        // numbers would be logging the caller's own coordinates for no purpose the
        // count does not already serve.
        regions = params.region.as_ref().map_or(0, Vec::len),
        format = output.format.name(),
        num_rows,
        // Over a remote store this is also what the request cost the origin, which the
        // elapsed time on its own does not distinguish from a slow network.
        data_bytes_read,
        elapsed_ms = started.elapsed().as_millis(),
        "query"
    );
    Ok(response)
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use crate::app::testing::{SECRET, ask, get, mounted, post_parquet};
    use crate::config::ApiConfig;

    use super::*;

    /// The credential-bearing shape is not reachable by a method that puts its
    /// parameters in a url.
    #[tokio::test]
    async fn the_query_endpoint_is_not_a_get() {
        let (status, _) = get("/api/v1/simple/parquet?url=s3://b/k.parquet&filters=x%3D1").await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn a_misspelled_field_is_named_rather_than_ignored() {
        let (status, body) = post_parquet(serde_json::json!({
            "url": "s3://b/k.parquet",
            "storage": {"regoin": "us-west-2"},
        }))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("regoin"), "{body}");
    }

    /// A `region` and the two columns it is tested against travel together. Any of the
    /// three on its own is refused rather than dropped: a region with nothing to test
    /// would return every row in the file, which a caller cannot tell from a region that
    /// held them all, and a column named with no region does nothing at all.
    #[tokio::test]
    async fn a_region_and_its_coordinate_columns_travel_together() {
        let circle = serde_json::json!([
            {"type": "circle", "ra": 320.6, "dec": -12.4, "radius_deg": 0.01}
        ]);
        for (what, body) in [
            (
                "no columns",
                serde_json::json!({"url": "s3://b/k.parquet", "region": circle}),
            ),
            (
                "one column",
                serde_json::json!({
                    "url": "s3://b/k.parquet", "region": circle, "ra_column": "objra",
                }),
            ),
            (
                "columns and no region",
                serde_json::json!({
                    "url": "s3://b/k.parquet", "ra_column": "objra", "dec_column": "objdec",
                }),
            ),
        ] {
            let (status, body) = post_parquet(body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{what}: {body}");
            assert!(body.contains("region"), "{what}: {body}");
        }
    }

    /// A radius with no unit in its name is refused rather than read as either one: both
    /// readings are legal radii, differing by a factor of 3600, and no answer would say
    /// which one it had used.
    #[tokio::test]
    async fn a_radius_says_its_unit() {
        let (status, body) = post_parquet(serde_json::json!({
            "url": "s3://b/k.parquet",
            "region": [{"type": "circle", "ra": 320.6, "dec": -12.4, "radius": 0.01}],
            "ra_column": "objra",
            "dec_column": "objdec",
        }))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("radius"), "{body}");
    }

    /// The shapes are part of the request's closed shape, so a misspelled field inside
    /// one is named rather than defaulted — a `radus` that became a `radius` of zero
    /// would be a region matching nothing.
    #[tokio::test]
    async fn a_misspelled_field_inside_a_region_is_named() {
        let (status, body) = post_parquet(serde_json::json!({
            "url": "s3://b/k.parquet",
            "region": [{"type": "circle", "ra": 320.6, "dec": -12.4, "radus": 0.01}],
        }))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("radus"), "{body}");
    }

    #[tokio::test]
    async fn unsupported_schemes_are_rejected() {
        let (status, body) = post_parquet(serde_json::json!({
            "url": "ftp://example.com/a.parquet",
        }))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("unsupported URL scheme"), "{body}");
    }

    /// The url is the object's, so options in it are a caller using the old shape —
    /// and dropping them would turn a credentialed read into an anonymous one.
    #[tokio::test]
    async fn storage_options_in_the_url_are_refused() {
        let (status, body) = post_parquet(serde_json::json!({
            "url": format!("s3://b/k.parquet?secret_access_key={SECRET}"),
        }))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("query string"), "{body}");
        assert!(!body.contains(SECRET), "leaked: {body}");
    }

    /// The request as a struct, printed. Nothing logs it today, but the derive is what
    /// makes that a choice rather than a rule to remember.
    #[test]
    fn printing_the_request_leaks_nothing() {
        let params = ParquetQuery {
            url: "s3://b/k.parquet".to_owned().into(),
            storage: StorageOptions {
                s3: storage::S3Options {
                    region: Some("us-west-2".to_owned()),
                    secret_access_key: Some(SECRET.to_owned().into()),
                    ..Default::default()
                },
                ..Default::default()
            },
            region: None,
            ra_column: None,
            dec_column: None,
            healpix_column: None,
            healpix_order: None,
            columns: None,
            filters: Some("objectid = 1".to_owned()),
            format: None,
            dsv_null_value: None,
            limit: None,
            streaming: false,
            unknown: BTreeMap::new(),
        };
        let shown = format!("{params:?}");
        assert!(!shown.contains(SECRET), "leaked: {shown}");
        assert!(shown.contains("s3://b/k.parquet"), "{shown}");
        assert!(shown.contains("us-west-2"), "{shown}");
    }

    /// A streamed answer is the same answer, in every format that streams.
    ///
    /// The rows are the point: what streaming changes is when the bytes leave, not which
    /// ones. The counts move rather than disappear — out of the headers, which are gone by
    /// the time the first row is read, and into the end of the JSON body, where a reader
    /// finds them by name rather than by position.
    #[tokio::test]
    async fn a_streamed_answer_is_the_answer() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), query::tests::fixture()).unwrap();
        let service = || mounted(dir.path(), &ApiConfig::default());

        for format in ["json", "csv", "tsv", "votable"] {
            let body = |streaming: bool| {
                serde_json::json!({
                    "url": "file:///part0.parquet",
                    "columns": ["objectid", "band"],
                    "filters": "objectid >= 4",
                    "format": format,
                    "streaming": streaming,
                })
            };
            let (whole_status, whole) = ask(service(), body(false)).await;
            let (streamed_status, streamed) = ask(service(), body(true)).await;
            assert_eq!(whole_status, StatusCode::OK, "{format}: {whole}");
            assert_eq!(streamed_status, StatusCode::OK, "{format}: {streamed}");

            match format {
                // Key order is not what a JSON reader reads, so the two are compared as
                // documents: same schema, same rows, same counts, wherever they sit.
                "json" => {
                    let whole: serde_json::Value = serde_json::from_str(&whole).unwrap();
                    let streamed: serde_json::Value = serde_json::from_str(&streamed).unwrap();
                    assert_eq!(whole["rows"], streamed["rows"]);
                    assert_eq!(whole["schema"], streamed["schema"]);
                    assert_eq!(whole["num_rows"], streamed["num_rows"]);
                    assert_eq!(
                        whole["data_bytes_read"], streamed["data_bytes_read"],
                        "a streamed read cost something else: {streamed}"
                    );
                }
                // `nrows` is the one thing a streamed VOTable leaves out, being an
                // attribute of the tag that opens before the rows are known.
                "votable" => {
                    assert!(streamed.contains("<TABLE>"), "{streamed}");
                    assert_eq!(
                        whole.replace(&format!("<TABLE nrows=\"{}\">", 6), "<TABLE>"),
                        streamed,
                    );
                }
                _ => assert_eq!(whole, streamed, "{format}"),
            }
        }
    }

    /// Parquet is read footer-first, so a body with no length is one no reader can open.
    /// Refused rather than answered whole: a caller who asked for both said something this
    /// service cannot do, and doing the other thing silently is how they find out late.
    #[tokio::test]
    async fn parquet_and_streaming_together_are_refused() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), query::tests::fixture()).unwrap();
        let (status, body) = ask(
            mounted(dir.path(), &ApiConfig::default()),
            serde_json::json!({
                "url": "file:///part0.parquet",
                "format": "parquet",
                "streaming": true,
            }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("streaming"), "{body}");
    }

    /// The single-file route never produces a plan, so a field about what a plan carries is
    /// not one of its fields, and a body carrying it is refused rather than quietly doing
    /// nothing.
    #[tokio::test]
    async fn return_storage_is_refused_where_there_is_no_plan() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), query::tests::fixture()).unwrap();
        let (status, body) = ask(
            mounted(dir.path(), &ApiConfig::default()),
            serde_json::json!({"url": "file:///part0.parquet", "return_storage": true}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("return_storage"), "{body}");
    }

    /// The API has only one thing to do with an object, so a url naming something it
    /// does not read as data names nothing this route serves.
    #[tokio::test]
    async fn the_api_refuses_a_url_that_is_not_a_data_file() {
        for url in [
            "s3://b/hats/properties",
            "s3://b/hats/part0.csv",
            "s3://b/hats/",
        ] {
            let (status, body) = post_parquet(serde_json::json!({"url": url})).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{url}");
            // The refusal says what would have been read, so a caller can see why.
            assert!(body.contains("*.parquet"), "{url}: {body}");
        }
    }

    #[tokio::test]
    async fn unparseable_urls_are_rejected() {
        let (status, body) = post_parquet(serde_json::json!({"url": "not-a-url"})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("invalid url"), "{body}");
    }
}
