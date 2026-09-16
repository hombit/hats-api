//! `POST {api.prefix}/simple/hats` and `/simple/hats/plan`: the same projection and predicate
//! against a whole HATS catalog, answered with the rows or with the work that would read them.

use std::collections::BTreeMap;
use std::time::Instant;

use axum::extract::{State, rejection::JsonRejection};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::app::answer::hats_answer;
use crate::app::request::{
    Format, Output, body_error, predicate_of, projection_of, refuse_unknown, takes,
};
use crate::app::service::{QUERY_SEGMENT, Service, route};
use crate::error::ApiError;
use crate::hats::query::{CatalogSelection, Exceeded, Outcome, Search};
use crate::sky::healpix::Cover;
use crate::sky::region::Region;
use crate::storage::{self, SourceUrl, StorageOptions, parse_url};

/// A query against a whole HATS catalog: the url names the catalog and the partitions to
/// read are chosen from the region.
///
/// It carries none of [`ParquetQuery`](super::parquet::ParquetQuery)'s column names.
/// `hats_col_ra`, `hats_col_dec` and `hats_col_healpix` are the catalog's statement about its
/// own files, and it can see more of them than a caller can; a caller who wants their own pair
/// names one of the files, where the single-file route takes them.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub(in crate::app) struct CatalogQuery {
    /// The HATS catalog to read: the directory holding `hats.properties`, or a collection's,
    /// which is followed to its primary table. Its scheme picks the backend — `s3`, `gs`,
    /// `az`, `https`, `webdav`, `hf` or `file` — and which of those a deployment answers for is the
    /// operator's to configure.
    // Treated as opaque: whatever query string it has belongs to the origin, not to us.
    // No `example` on the field, for the reason `ParquetQuery::url` has none.
    #[schema(value_type = String)]
    url: SourceUrl,
    /// How to reach the store: an endpoint, a region, credentials. Leave it out for a public
    /// catalog read anonymously, which is the common case. Which options apply is decided by
    /// the url's scheme, and one that does not apply is refused rather than ignored.
    #[serde(default)]
    storage: StorageOptions,
    /// One or more shapes on the sky. A row inside any of them qualifies — the array is a
    /// union — and the whole field is ANDed with the predicate. It is also what chooses the
    /// partitions: without one, every partition of the catalog is read.
    ///
    /// The columns it is tested against are the catalog's own, from its `properties`.
    region: Option<Vec<Region>>,
    /// The columns to return, one name per element, and nothing computed — for that, write an
    /// ADQL query. Write a name as the catalog spells it, in double quotes where the spelling
    /// needs them. A dotted name reaches inside a struct column, and comes back as that column
    /// carrying the fields you named. Leave the field out for every column; an empty list is
    /// refused rather than read as one or the other.
    #[schema(example = json!(["source_id", "ra", "dec", "phot_g_mean_mag"]))]
    columns: Option<Vec<String>>,
    /// The row condition: one boolean expression over the catalog's columns, the condition
    /// that would follow a `WHERE` keyword. Write a column as the catalog spells it, in double
    /// quotes where the spelling needs them — `"Gmag" < 20`. Leave it out for every row.
    #[schema(example = "parallax > 1")]
    filters: Option<String>,
    /// `json`, the default; `parquet` for the answer as a parquet file laid out like the
    /// partitions it came from; `votable` for a VOTable, `csv` for comma-separated text and
    /// `tsv` for tab-separated. The last three take flat columns only and refuse a nested one
    /// by name. Anything but `json` carries its counts in `x-hats-*` response headers, there
    /// being no room in the body.
    #[schema(example = "json")]
    format: Option<String>,
    /// What a null is written as in `csv` and `tsv`. Absent, a null is an empty field — the
    /// spelling an empty string also has, so the two cannot be told apart until this is set.
    /// At most 128 bytes, and no `,`, tab, newline or carriage return, whichever of the two
    /// formats was asked for.
    #[schema(example = "NULL")]
    dsv_null_value: Option<String>,
    /// At most this many rows, taken from the front of the catalog's own order. The same
    /// request returns the same rows.
    #[schema(example = 100)]
    limit: Option<usize>,
    /// Every key the body carried that this endpoint has no field for.
    #[serde(flatten)]
    #[schema(ignore)]
    unknown: BTreeMap<String, IgnoredAny>,
}

impl CatalogQuery {
    /// Every field this endpoint takes, in the order a body is written in.
    pub(in crate::app) fn fields() -> Vec<&'static str> {
        vec![
            "url",
            "storage",
            "region",
            "columns",
            "filters",
            "format",
            "dsv_null_value",
            "limit",
        ]
    }

    /// The same list, as the sentence a refusal ends with.
    fn takes() -> String {
        takes(&Self::fields())
    }

    /// This request, lowered. No entry of a plan this produces carries a credential: there is
    /// no field here to have asked with, so the `None` is the type's rather than a check's.
    fn lowered(&self) -> Lowered<'_> {
        Lowered {
            url: &self.url,
            storage: &self.storage,
            columns: self.columns.as_deref(),
            filters: self.filters.as_deref(),
            dsv_null_value: self.dsv_null_value.as_deref(),
            region: self.region.as_deref(),
            format: self.format.as_deref(),
            limit: self.limit,
            echo: None,
        }
    }
}

/// The same query against the same catalog, answered with the work rather than the rows.
///
/// Its own type rather than a flag on [`CatalogQuery`], and written out rather than composed:
/// what the two endpoints take is the same today and is not the same thing, and a shape shared
/// until it diverges is one that diverges by growing a field that means nothing on one route.
/// The one difference today is `return_storage`, which only means something where a plan is
/// the answer — so a request for rows has no way to spell it.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub(in crate::app) struct CatalogPlanQuery {
    /// The HATS catalog to resolve the request against: the directory holding
    /// `hats.properties`, or a collection's, which is followed to its primary table.
    // No `example` on the field, for the reason `ParquetQuery::url` has none.
    #[schema(value_type = String)]
    url: SourceUrl,
    /// How to reach the store: an endpoint, a region, credentials. Only the catalog's own
    /// files are read here, but they are read the same way the rows would be.
    #[serde(default)]
    storage: StorageOptions,
    /// One or more shapes on the sky, which is what chooses the partitions and so what
    /// decides how long the work list is. Without one, every partition is an entry.
    ///
    /// An entry the region contains whole carries no region: every row of it qualifies.
    region: Option<Vec<Region>>,
    /// The columns to return, as the catalog route takes them. Carried into every entry as you
    /// wrote it; nothing here is planned against a file.
    #[schema(example = json!(["source_id", "ra", "dec", "phot_g_mean_mag"]))]
    columns: Option<Vec<String>>,
    /// The row condition, as the catalog route takes it. Carried into every entry as you wrote
    /// it.
    #[schema(example = "parallax > 1")]
    filters: Option<String>,
    /// Written into each entry, so the answers arrive in the encoding you asked for. It is
    /// not the plan's own: a plan is JSON.
    #[schema(example = "json")]
    format: Option<String>,
    /// What a null is written as in `csv` and `tsv`. Absent, a null is an empty field — the
    /// spelling an empty string also has, so the two cannot be told apart until this is set.
    /// At most 128 bytes, and no `,`, tab, newline or carriage return, whichever of the two
    /// formats was asked for.
    #[schema(example = "NULL")]
    dsv_null_value: Option<String>,
    /// Written into each entry as you wrote it. Each answers at most this many rows, and the
    /// first `limit` of the concatenation is what this service would have returned.
    #[schema(example = 100)]
    limit: Option<usize>,
    /// Write this request's own `storage` — credentials included — into each entry, so the
    /// entries can be sent as they stand. Off by default: it discloses nothing, being your own
    /// secret handed back to you, but it makes the plan a document with a credential in it,
    /// and a plan is the sort of thing that gets logged, cached and pasted into an issue.
    // The default writes the stripped url and `requires_credentials`, so a client that would
    // rather re-attach them itself — which is most of them — never has to think about it.
    #[serde(default)]
    return_storage: bool,
    /// Every key the body carried that this endpoint has no field for.
    #[serde(flatten)]
    #[schema(ignore)]
    unknown: BTreeMap<String, IgnoredAny>,
}

impl CatalogPlanQuery {
    /// Every field this endpoint takes, in the order a body is written in: the catalog
    /// query's, and last the one field that is this endpoint's own.
    pub(in crate::app) fn fields() -> Vec<&'static str> {
        let mut fields = CatalogQuery::fields();
        fields.push("return_storage");
        fields
    }

    /// The same list, as the sentence a refusal ends with.
    fn takes() -> String {
        takes(&Self::fields())
    }

    /// This request, lowered.
    fn lowered(&self) -> Lowered<'_> {
        Lowered {
            url: &self.url,
            storage: &self.storage,
            columns: self.columns.as_deref(),
            filters: self.filters.as_deref(),
            dsv_null_value: self.dsv_null_value.as_deref(),
            region: self.region.as_deref(),
            format: self.format.as_deref(),
            limit: self.limit,
            // Both halves, and both are the caller's doing: they asked for it, and they sent
            // something to hand back. Asked for with nothing to return writes no field rather
            // than an empty object, which would read as "these are the options" and they are
            // not.
            echo: (self.return_storage && !self.storage.is_empty()).then(|| self.storage.echo()),
        }
    }
}

/// A catalog request, lowered: what everything below the routes works in.
///
/// The two catalog endpoints are two wire types and one of these, so what they have in common
/// is code they share rather than a body they share. A third — a catalog read written some
/// other way — is a wire type and a `lowered`, and nothing below here changes.
struct Lowered<'a> {
    url: &'a SourceUrl,
    storage: &'a StorageOptions,
    columns: Option<&'a [String]>,
    filters: Option<&'a str>,
    region: Option<&'a [Region]>,
    format: Option<&'a str>,
    dsv_null_value: Option<&'a str>,
    limit: Option<usize>,
    /// The caller's own storage options, to be written into every entry of a plan. `Some`
    /// only from an endpoint that has a `return_storage` to have asked with.
    echo: Option<serde_json::Value>,
}

impl Lowered<'_> {
    /// What to read. The catalog supplies the columns the region is tested against, so there
    /// is nothing here a caller named.
    fn selection(&self) -> CatalogSelection<'_> {
        CatalogSelection {
            projection: projection_of(self.columns),
            predicate: predicate_of(self.filters),
            regions: self.region,
            limit: self.limit,
        }
    }
}

/// Everything both catalog routes settle before either of them does its own work.
///
/// They have to choose the same partitions to be answers to the same question, so they ask
/// for them through one function rather than two that look alike.
struct Opened {
    search: Search,
    /// The catalog as the *caller* spelled it, which is what a plan entry's url is built
    /// from. The opened directory's url is the store's, and for a mount a store's url is the
    /// operator's path on disk.
    url: Url,
    output: Output,
}

async fn open_catalog(service: &Service, params: &Lowered<'_>) -> Result<Opened, ApiError> {
    let output = Output::parse(params.format, params.dsv_null_value, Format::Json)?;
    let url = parse_url(params.url.as_str())?;
    // A directory rather than an object: `open_dir` drops only the refusal of a url naming
    // no object, and every policy check `open` makes still runs. There is no `[data]`
    // question to ask of the url either — the caller names a catalog, and which files inside
    // it are read is the catalog's own answer.
    let dir = storage::open_dir(&url, params.storage, &service.policy, &service.transfers)?;
    let on_disk = dir.url.to_file_path().ok();
    let search = Search::resolve(dir, params.region, service.catalog_limits)
        .await
        .map_err(|error| match &on_disk {
            Some(path) => error.from_mount(path),
            None => error,
        })?;
    Ok(Opened {
        search,
        url,
        output,
    })
}

/// A query against a catalog: the url names a HATS directory, and the partitions to read
/// are chosen from the region rather than named by the caller.
///
/// Its body is [`CatalogQuery`] rather than the single-file route's: what a caller may say
/// about a catalog is not what they may say about one file, so the two are different types on
/// different routes rather than one body with a sentence about which fields apply where.
pub(in crate::app) async fn query_hats(
    State(service): State<Service>,
    body: Result<Json<CatalogQuery>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(body) = body.map_err(|rejection| body_error(&rejection, &CatalogQuery::takes()))?;
    let started = Instant::now();
    refuse_unknown(&body.unknown, &CatalogQuery::takes())?;
    let params = body.lowered();
    let Opened {
        search,
        url,
        output,
    } = open_catalog(&service, &params).await?;
    let on_disk = url.to_file_path().ok();
    let hide_the_path = |error: ApiError| match &on_disk {
        Some(path) => error.from_mount(path),
        None => error,
    };

    let selection = params.selection();
    let outcome = search
        .run(
            &selection,
            service.data_files_for(&url),
            service.sql_limits,
            service.catalog_limits,
        )
        .await
        .map_err(&hide_the_path)?;

    let result = match outcome {
        Outcome::Rows(result) => result,
        // The work list rather than a sentence: a caller who asked for more than this server
        // will do in one request needs the requests it would take, not to be told to try
        // something smaller and guess what.
        Outcome::TooMuchWork(why) => {
            let plan = plan_of(&service, &search, &params, Some(why)).await?;
            return Ok((StatusCode::UNPROCESSABLE_ENTITY, Json(plan)).into_response());
        }
    };

    let num_rows = result.rows.num_rows();
    let data_bytes_read = result.rows.data_bytes_read;
    let partitions_read = result.partitions_read;
    let response = hats_answer(&result, &output, started)
        .await
        .map_err(&hide_the_path)?;
    tracing::info!(
        // The catalog's url, not the parameter, which may carry credentials.
        url = %search.catalog().dir().url,
        partitions = search.catalog().partitions().len(),
        source = search.catalog().partitions().source().name(),
        chosen = search.chosen().len(),
        partitions_read,
        selected = params.columns.is_some(),
        filtered = params.filters.is_some(),
        // How many shapes, not what they were: logging the numbers would be logging the
        // caller's own coordinates for no purpose the count does not already serve.
        regions = params.region.map_or(0, <[Region]>::len),
        format = output.format.name(),
        num_rows,
        data_bytes_read,
        elapsed_ms = started.elapsed().as_millis(),
        "catalog query"
    );
    Ok(response)
}

/// The same request, resolved and handed back as a work list rather than run.
///
/// It reads the catalog's own files and no data at all, so the limits that bound
/// [`query_hats`] do not apply: the whole point is to answer a request too large to run.
pub(in crate::app) async fn query_hats_plan(
    State(service): State<Service>,
    body: Result<Json<CatalogPlanQuery>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(body) =
        body.map_err(|rejection| body_error(&rejection, &CatalogPlanQuery::takes()))?;
    let started = Instant::now();
    // Planned, not run — but a body this route cannot read as written is refused here all
    // the same, or the plan would hand back entries every one of which is a 400 the client
    // discovers one at a time.
    refuse_unknown(&body.unknown, &CatalogPlanQuery::takes())?;
    let params = body.lowered();
    let Opened { search, .. } = open_catalog(&service, &params).await?;
    let plan = plan_of(&service, &search, &params, None).await?;
    tracing::info!(
        url = %search.catalog().dir().url,
        partitions = search.catalog().partitions().len(),
        chosen = search.chosen().len(),
        requests = plan.requests.len(),
        regions = params.region.map_or(0, <[Region]>::len),
        elapsed_ms = started.elapsed().as_millis(),
        "catalog plan"
    );
    Ok(Json(plan).into_response())
}

/// A work list: one request per partition, for you to send yourself.
///
/// Send them in the order given and concatenate the answers, and you get what the catalog
/// route would have returned — but you choose the concurrency, and you can stop early.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub(in crate::app) struct PlanResponse {
    /// Why a plan came back instead of rows. Present only when a catalog query was refused
    /// for being too large; the plan routes, which were asked for a plan, leave it out.
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    /// The catalog, spelled the way you spelled it.
    catalog: String,
    /// How many entries are in `requests`.
    num_partitions: usize,
    /// Roughly how many bytes the whole plan would read, summed over the entries. Left out
    /// unless every entry knew its own size — a partial sum would read as a total.
    #[serde(skip_serializing_if = "Option::is_none")]
    estimated_bytes: Option<u64>,
    /// Whether the request that produced this plan carried credentials — meaning you must
    /// attach your own `storage` to each entry before sending it.
    ///
    /// The entries do not carry them unless you asked with `return_storage`.
    requires_credentials: bool,
    /// The entries, in the catalog's own order.
    requests: Vec<PlanRequest>,
}

/// One entry of a plan: a request to send to this service, for one partition of the catalog.
#[derive(Debug, Serialize, utoipa::ToSchema)]
struct PlanRequest {
    /// The HEALPix order of the partition this reads.
    order: u8,
    /// The HEALPix pixel of the partition this reads, at `order`.
    pixel: u64,
    /// The HTTP method to use — always `POST` here.
    // A field rather than part of `path`, so a file-server entry, which is a `GET` under a
    // mount, has the same shape as this one.
    method: &'static str,
    /// The path on this service to send `body` to.
    path: String,
    /// Roughly how many bytes this entry would read, where the catalog said.
    #[serde(skip_serializing_if = "Option::is_none")]
    estimated_bytes: Option<u64>,
    /// The body to send.
    body: PlanBody,
}

/// The body of one plan entry: an ordinary single-file request, ready to send unchanged.
///
/// It carries what you wrote that still applies, plus the coordinate and index column names the
/// catalog supplied.
// The column names are stated here because the single-file route has no catalog to ask.
#[derive(Debug, Serialize, utoipa::ToSchema)]
struct PlanBody {
    /// The partition's own url, below the catalog url you gave.
    url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filters: Option<String>,
    /// Present only where this partition straddles the region's edge. An entry without it is
    /// one the region contains whole, so every row qualifies and no test is needed.
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<Vec<Region>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ra_column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dec_column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    healpix_column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    healpix_order: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dsv_null_value: Option<String>,
    /// Carried as the caller wrote it. Each entry returns at most this many, and the client
    /// takes the first `limit` of the concatenation — which is the same rows this service
    /// would have returned, the entries being in the same order.
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<usize>,
    /// The caller's own storage options, credentials included, and only where they asked
    /// for them with `return_storage`. Absent is the default and means the client attaches
    /// what it already holds.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Object>)]
    storage: Option<serde_json::Value>,
}

async fn plan_of(
    service: &Service,
    search: &Search,
    params: &Lowered<'_>,
    reason: Option<Exceeded>,
) -> Result<PlanResponse, ApiError> {
    let url = parse_url(params.url.as_str())?;
    let data = service.data_files_for(&url);
    let entries = search.entries(data).await?;
    // The route an entry is sent to. `None` cannot happen — the API is how this request
    // arrived — but a prefix is what the caller must be told, not what this can assume.
    let path = service
        .api_prefix
        .as_deref()
        .map(|prefix| route(prefix, &format!("{QUERY_SEGMENT}/parquet")))
        .ok_or_else(|| ApiError::internal("the API has no prefix"))?;

    let columns = search.columns();
    // Only where the catalog named one, which is the only case `Columns` carries. Where it
    // did not, each entry's file is asked for `_healpix_29` itself — the same discovery the
    // catalog route did — so there is nothing to pass on and nothing lost by not passing it.
    let healpix = columns.and_then(|columns| columns.healpix.as_ref());

    let mut estimated_bytes = Some(0);
    let requests = entries
        .iter()
        .map(|entry| {
            estimated_bytes = match (estimated_bytes, entry.estimated_bytes) {
                (Some(total), Some(bytes)) => Some(total + bytes),
                _ => None,
            };
            // The region only where the rows still need it, and the columns only where the
            // region is there to be tested.
            let tested = entry.cover != Cover::Inside;
            let region = tested
                .then(|| params.region.map(<[Region]>::to_vec))
                .flatten();
            let named = region.is_some().then_some(columns).flatten();
            Ok(PlanRequest {
                order: entry.order,
                pixel: entry.pixel,
                method: "POST",
                path: path.clone(),
                estimated_bytes: entry.estimated_bytes,
                body: PlanBody {
                    url: below(&url, &entry.path)?,
                    columns: params.columns.map(<[String]>::to_vec),
                    filters: params.filters.map(str::to_owned),
                    region,
                    ra_column: named.map(|columns| columns.ra.clone()),
                    dec_column: named.map(|columns| columns.dec.clone()),
                    healpix_column: named.and(healpix).map(|(column, _)| column.clone()),
                    healpix_order: named.and(healpix).map(|(_, order)| *order),
                    format: params.format.map(str::to_owned),
                    dsv_null_value: params.dsv_null_value.map(str::to_owned),
                    limit: params.limit,
                    storage: params.echo.clone(),
                },
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;

    Ok(PlanResponse {
        reason: reason.map(|why| why.to_string()),
        catalog: url.to_string(),
        num_partitions: search.chosen().len(),
        estimated_bytes,
        requires_credentials: params.storage.has_credentials(),
        requests,
    })
}

/// A path below the catalog, as a url in the caller's own spelling.
///
/// Built from the url the caller wrote and never from the store's. For a mounted catalog
/// the store's url is the operator's absolute path on disk, so an entry built from it would
/// publish the one thing a response may not carry — and would hand back a url the caller
/// could not send back to this service anyway, a local file being addressed by its mount.
fn below(catalog: &Url, path: &str) -> Result<String, ApiError> {
    let mut url = catalog.clone();
    url.path_segments_mut()
        .map_err(|()| ApiError::bad_request("this url cannot name a catalog"))?
        .pop_if_empty()
        .extend(path.split('/'));
    Ok(url.to_string())
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    use crate::access::AccessPolicy;
    use crate::access::mount::Mounts;
    use crate::app::router;
    use crate::app::testing::{SECRET, ask, ask_hats, body_of, mounted, post_json, serving};
    use crate::config::{ApiConfig, DataConfig, LimitsConfig, ServerConfig};
    use crate::engine::query;
    use crate::hats;

    use super::*;

    /// The route end to end: a body naming a catalog by a mount's path comes back with the
    /// rows the region holds, and with the count of partitions they came out of.
    ///
    /// What `hats::query` tests is which partitions get read and which rows come back. What
    /// this adds is that the request shape reaches it — the same body the parquet route
    /// takes, with the column names left to the catalog.
    #[tokio::test]
    async fn the_hats_route_answers_a_region_over_a_catalog() {
        let dir = hats::query::tests::fixture(true);
        let region = hats::query::tests::regions()[0].clone();
        let expected = hats::query::tests::inside(&region);
        assert!(!expected.is_empty(), "the cone selects nothing");

        let (status, body) = ask_hats(
            mounted(dir.path(), &ApiConfig::default()),
            serde_json::json!({
                "url": "file:///",
                "columns": ["id"],
                "region": [region],
            }),
        )
        .await;

        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(answer["num_rows"], expected.len());
        assert_eq!(
            answer["num_partitions"], 1,
            "a cone inside one partition read others: {body}"
        );
        let ids: Vec<i64> = answer["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_i64().unwrap())
            .collect();
        assert_eq!(ids, expected);
    }

    /// The route end to end with a cross-match's worth of circles on it.
    ///
    /// [`crate::sky::region::tests::many_shapes_do_not_build_a_tree_as_deep_as_they_are_long`] is
    /// the same guarantee stated about the expression; this is the one that would actually
    /// have taken the process down. The circles are clustered inside one partition on
    /// purpose, so that the partition bound does not refuse the request before the predicate
    /// those shapes build is ever put together.
    #[tokio::test]
    async fn a_cross_matchs_worth_of_circles_is_answered() {
        let dir = hats::query::tests::fixture(true);
        let inside = hats::query::tests::regions()[0].clone();
        let Region::Circle { ra, dec, .. } = inside else {
            panic!("the fixture's first region is a circle");
        };

        // Past the few hundred that overflowed a left-deep union, and no further: the cost
        // of planning is linear in the terms, so a larger figure here buys the same
        // guarantee and spends the test's whole budget on arithmetic.
        let circles: Vec<serde_json::Value> = (0..500)
            .map(|i| {
                let offset = f64::from(i) / 500.0;
                serde_json::json!({
                    "type": "circle",
                    "ra": ra + offset * 0.01,
                    "dec": dec + offset * 0.01,
                    "radius_arcsec": 1.0,
                })
            })
            .collect();

        let (status, body) = ask_hats(
            mounted(dir.path(), &ApiConfig::default()),
            serde_json::json!({"url": "file:///", "columns": ["id"], "region": circles}),
        )
        .await;

        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            answer["num_partitions"], 1,
            "the cluster reached past its own partition: {body}"
        );
    }

    /// A `zone` against a pole is answered rather than dropping the connection.
    ///
    /// The covering turns a declination band into a ring about the pole, and a band this
    /// close to one makes a ring smaller than a cell — which `cdshealpix` panics on. A panic
    /// is the one failure that reaches no caller: it unwinds past the handler, and what
    /// arrives is a reset connection rather than a status. So what is asserted is only that
    /// there is a response at all; which rows it holds is
    /// `healpix::tests::a_covering_brackets_its_shape`'s question.
    ///
    /// See <https://github.com/cds-astro/cds-healpix-rust/issues/27>.
    #[tokio::test]
    async fn a_zone_at_a_pole_is_answered() {
        let dir = hats::query::tests::fixture(true);
        let zones = [
            // A polar cap, reaching the pole from either side.
            serde_json::json!({"type": "zone", "ra": [0.0, 360.0], "dec": [89.9, 90.0]}),
            serde_json::json!({"type": "zone", "ra": [0.0, 360.0], "dec": [-90.0, -89.9]}),
            // A band near a pole that does not reach it: the ring's size is what matters,
            // not whether the pole is in the zone.
            serde_json::json!({"type": "zone", "ra": [0.0, 359.999], "dec": [89.999, 89.9999]}),
            // And one narrow in right ascension as well, which takes the other arm of the
            // zone covering.
            serde_json::json!({"type": "zone", "ra": [0.0, 10.0], "dec": [89.999, 90.0]}),
        ];

        for zone in zones {
            let (status, body) = ask_hats(
                mounted(dir.path(), &ApiConfig::default()),
                serde_json::json!({"url": "file:///", "columns": ["id"], "region": [zone.clone()]}),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{zone}: {body}");
        }
    }

    /// A catalog that says nothing about its position columns cannot be region-searched, and
    /// says so rather than answering without a spatial test.
    #[tokio::test]
    async fn a_catalog_that_names_no_position_columns_cannot_be_searched() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("hats.properties"), "obs_collection=x\n").unwrap();
        std::fs::write(dir.path().join("partition_info.csv"), "Norder,Npix\n0,0\n").unwrap();

        let (status, body) = ask_hats(
            mounted(dir.path(), &ApiConfig::default()),
            serde_json::json!({
                "url": "file:///",
                "region": [{"type": "circle", "ra": 0.0, "dec": 0.0, "radius_deg": 1.0}],
            }),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(
            body.contains("does not name its position columns"),
            "{body}"
        );
    }

    /// The four column names are the catalog's to answer, so a catalog request has no field
    /// for one and a body carrying one is refused.
    ///
    /// Refused and not ignored: silently dropping one would test the rows against different
    /// columns than the caller wrote, which is an answer they cannot tell from the one they
    /// asked for. What makes that structural is [`CatalogQuery`] not having the fields; this
    /// is the catch-all proving the leftovers really are refused rather than dropped, on both
    /// catalog routes.
    #[tokio::test]
    async fn a_catalog_route_refuses_column_names() {
        let dir = hats::query::tests::fixture(true);
        for field in ["ra_column", "dec_column", "healpix_column", "healpix_order"] {
            let value = match field {
                "healpix_order" => serde_json::json!(29),
                _ => serde_json::json!("whatever"),
            };
            for route in ["simple/hats", "simple/hats/plan"] {
                let request = Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/{route}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"url": "file:///", field: value}).to_string(),
                    ))
                    .unwrap();
                let response = router(mounted(dir.path(), &ApiConfig::default()))
                    .oneshot(request)
                    .await
                    .unwrap();
                let status = response.status();
                let body = body_of(response).await;
                assert_eq!(status, StatusCode::BAD_REQUEST, "{route} {field}: {body}");
                assert!(body.contains(field), "{route} {field}: {body}");
            }
        }
    }

    async fn ask_plan(
        service: Service,
        path: &str,
        body: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let (status, body) = post_json(service, path, body).await;
        (status, serde_json::from_str(&body).unwrap())
    }

    /// A collection is followed one hop down to its primary table, and every url handed back
    /// has to carry that hop.
    ///
    /// The entries are built from the url the *caller* wrote, which is the collection's, while
    /// the partitions are found below the *catalog's* — so a path joined on without the hop
    /// names a file that is not there, and the client discovers it one 404 at a time.
    #[tokio::test]
    async fn a_plan_for_a_collection_names_the_catalog_inside_it() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("collection.properties"),
            "obs_collection=c\nhats_primary_table_url=inside\n",
        )
        .unwrap();
        let inside = dir.path().join("inside");
        std::fs::create_dir(&inside).unwrap();
        let fixture = hats::query::tests::fixture(true);
        for name in std::fs::read_dir(fixture.path()).unwrap() {
            let name = name.unwrap().path();
            let to = inside.join(name.file_name().unwrap());
            match name.is_dir() {
                true => copy_tree(&name, &to),
                false => std::fs::copy(&name, &to).map(|_| ()).unwrap(),
            }
        }

        let region = hats::query::tests::regions()[0].clone();
        let (status, plan) = ask_plan(
            mounted(dir.path(), &ApiConfig::default()),
            "/api/v1/simple/hats/plan",
            serde_json::json!({"url": "file:///", "region": [region]}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{plan}");

        let url = plan["requests"][0]["body"]["url"].as_str().unwrap();
        assert!(url.contains("/inside/dataset/"), "{url}");
        // And it is a url this service answers, which is the whole of what an entry is for.
        let (status, body) = ask(
            mounted(dir.path(), &ApiConfig::default()),
            plan["requests"][0]["body"].clone(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    fn copy_tree(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap().path();
            let into = to.join(entry.file_name().unwrap());
            match entry.is_dir() {
                true => copy_tree(&entry, &into),
                false => {
                    std::fs::copy(&entry, &into).unwrap();
                }
            }
        }
    }

    /// The plan is the same work as separate requests, and every url in it is one the caller
    /// could send back to this service.
    ///
    /// **No disk path may appear in it.** A mounted catalog's store urls are the operator's
    /// absolute paths, so an entry built from those would publish them — and would hand back
    /// a url that does not name anything, a local file being addressed by its mount.
    #[tokio::test]
    async fn a_plan_is_requests_the_caller_could_send() {
        let dir = hats::query::tests::fixture(true);
        let region = hats::query::tests::regions()[1].clone();
        let (status, plan) = ask_plan(
            mounted(dir.path(), &ApiConfig::default()),
            "/api/v1/simple/hats/plan",
            serde_json::json!({"url": "file:///", "columns": ["id"], "region": [region]}),
        )
        .await;

        assert_eq!(status, StatusCode::OK, "{plan}");
        assert_eq!(plan["catalog"], "file:///");
        assert!(
            plan["reason"].is_null(),
            "a plan that was asked for has none"
        );
        assert_eq!(plan["requires_credentials"], false);
        let requests = plan["requests"].as_array().unwrap();
        assert_eq!(
            u64::try_from(requests.len()).unwrap(),
            plan["num_partitions"].as_u64().unwrap()
        );
        assert!(!requests.is_empty(), "the region reached nothing");

        let source = dir.path().display().to_string();
        for entry in requests {
            assert_eq!(entry["method"], "POST");
            // A plan a client can send back unchanged has to name a route that reads the
            // fields it carries.
            assert_eq!(entry["path"], "/api/v1/simple/parquet");
            let url = entry["body"]["url"].as_str().unwrap();
            assert!(url.starts_with("file:///dataset/Norder="), "{url}");
            assert!(
                !plan.to_string().contains(&source),
                "the plan names the disk"
            );
            assert_eq!(entry["body"]["columns"], serde_json::json!(["id"]));
            // A region is carried only where the rows still need it, and the columns only
            // where the region is there to test.
            match entry["body"]["region"].is_null() {
                true => assert!(entry["body"]["ra_column"].is_null(), "{entry}"),
                false => assert_eq!(entry["body"]["ra_column"], "ra", "{entry}"),
            }
        }
    }

    /// Every entry of a plan is a request this service answers, and together they are the
    /// rows the catalog route would have returned.
    #[tokio::test]
    async fn following_a_plan_gives_the_same_rows() {
        let dir = hats::query::tests::fixture(true);
        let region = hats::query::tests::regions()[1].clone();
        let expected = hats::query::tests::inside(&region);
        let body = serde_json::json!({"url": "file:///", "columns": ["id"], "region": [region]});

        let (_, plan) = ask_plan(
            mounted(dir.path(), &ApiConfig::default()),
            "/api/v1/simple/hats/plan",
            body.clone(),
        )
        .await;
        let mut ids = Vec::new();
        for entry in plan["requests"].as_array().unwrap() {
            // Sent to the route the entry itself names, which is what a client following a
            // plan does. Naming the route here instead would let an entry point somewhere
            // that cannot read it and the test would never notice.
            let (status, answer) = post_json(
                mounted(dir.path(), &ApiConfig::default()),
                entry["path"].as_str().unwrap(),
                entry["body"].clone(),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{answer}");
            let answer: serde_json::Value = serde_json::from_str(&answer).unwrap();
            ids.extend(
                answer["rows"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| row["id"].as_i64().unwrap()),
            );
        }
        assert_eq!(ids, expected, "the plan and the query disagree");
    }

    /// A plan carries no credential, and says that one is needed.
    ///
    /// [`PlanBody`] has no `storage` field, so this cannot regress by an entry gaining one —
    /// but it can by a credential reaching the body some other way, and a url is where it
    /// would. Rendered rather than routed: `file://` refuses storage options before a plan
    /// would ever be built, and the rendering is the part that could copy one.
    #[tokio::test]
    async fn a_plan_never_carries_the_credentials() {
        let dir = hats::query::tests::fixture(true);
        let service = mounted(dir.path(), &ApiConfig::default());
        let params = CatalogQuery {
            url: "file:///".to_owned().into(),
            storage: StorageOptions {
                s3: storage::S3Options {
                    region: Some("us-west-2".to_owned()),
                    secret_access_key: Some(SECRET.to_owned().into()),
                    ..Default::default()
                },
                ..Default::default()
            },
            region: Some(vec![hats::query::tests::regions()[1].clone()]),
            columns: Some(vec!["id".to_owned()]),
            filters: None,
            format: None,
            dsv_null_value: None,
            limit: None,
            unknown: BTreeMap::new(),
        };
        let url = parse_url(params.url.as_str()).unwrap();
        let dir_handle = storage::open_dir(
            &url,
            &StorageOptions::default(),
            &service.policy,
            &service.transfers,
        )
        .unwrap();
        let search = Search::resolve(dir_handle, params.region.as_deref(), service.catalog_limits)
            .await
            .unwrap();

        let plan = plan_of(&service, &search, &params.lowered(), None)
            .await
            .unwrap();
        let shown = serde_json::to_string(&plan).unwrap();
        assert!(!shown.contains(SECRET), "leaked: {shown}");
        assert!(!shown.contains("us-west-2"), "leaked: {shown}");
        assert!(!shown.contains("secret_access_key"), "{shown}");
        assert!(!shown.contains("storage"), "{shown}");
        // The flag is how the client knows to re-attach what it already holds, rather than
        // finding out from a 403.
        assert!(plan.requires_credentials, "{shown}");
        assert!(!plan.requests.is_empty(), "{shown}");
    }

    /// `return_storage` writes the caller's own options into every entry, and only when they
    /// both asked for it and sent something.
    ///
    /// Two conditions rather than one: asking for it with nothing to return writes no field
    /// at all, since an empty object would read as "these are the options" and they are not.
    #[tokio::test]
    async fn a_plan_returns_the_credentials_only_when_asked() {
        let dir = hats::query::tests::fixture(true);
        let service = mounted(dir.path(), &ApiConfig::default());
        let plan = async |storage: StorageOptions, return_storage: bool| {
            // The plan route's own body: it is the one that has a `return_storage` to set.
            let params = CatalogPlanQuery {
                url: "file:///".to_owned().into(),
                storage,
                region: None,
                columns: Some(vec!["id".to_owned()]),
                filters: None,
                format: None,
                dsv_null_value: None,
                limit: None,
                return_storage,
                unknown: BTreeMap::new(),
            };
            let url = parse_url(params.url.as_str()).unwrap();
            let opened = storage::open_dir(
                &url,
                &StorageOptions::default(),
                &service.policy,
                &service.transfers,
            )
            .unwrap();
            let search = Search::resolve(opened, None, service.catalog_limits)
                .await
                .unwrap();
            serde_json::to_value(
                plan_of(&service, &search, &params.lowered(), None)
                    .await
                    .unwrap(),
            )
            .unwrap()
        };
        let given = || StorageOptions {
            s3: storage::S3Options {
                region: Some("us-west-2".to_owned()),
                secret_access_key: Some(SECRET.to_owned().into()),
                ..Default::default()
            },
            ..Default::default()
        };

        // Asked for, and something to give.
        let asked = plan(given(), true).await;
        let body = &asked["requests"][0]["body"];
        assert_eq!(body["storage"]["secret_access_key"], SECRET, "{asked}");
        assert_eq!(body["storage"]["region"], "us-west-2", "{asked}");
        assert!(asked["requires_credentials"].as_bool().unwrap(), "{asked}");

        // Not asked for, though there is something to give.
        let unasked = plan(given(), false).await;
        assert!(
            unasked["requests"][0]["body"]["storage"].is_null(),
            "{unasked}"
        );
        assert!(!unasked.to_string().contains(SECRET), "leaked: {unasked}");

        // Asked for, with nothing to give: no field rather than an empty one.
        let nothing = plan(StorageOptions::default(), true).await;
        assert!(
            nothing["requests"][0]["body"]["storage"].is_null(),
            "{nothing}"
        );
        assert!(
            !nothing["requires_credentials"].as_bool().unwrap(),
            "{nothing}"
        );
    }

    /// A request over more than the server will do comes back as the plan for it, with the
    /// bound that stopped it named.
    #[tokio::test]
    async fn too_much_work_is_answered_with_the_plan_for_it() {
        let dir = hats::query::tests::fixture(true);
        let limits = LimitsConfig {
            max_partitions: 2,
            ..LimitsConfig::default()
        };
        let mounts = Arc::new(Mounts::new(&[serving(dir.path())], &DataConfig::default()).unwrap());
        let policy = AccessPolicy::new(
            &crate::config::AccessConfig::default(),
            Arc::clone(&mounts),
            None,
        )
        .unwrap();
        let service = Service::new(
            policy,
            &limits,
            mounts,
            &ApiConfig::default(),
            &DataConfig::default(),
            &crate::config::TapConfig::default(),
            &ServerConfig::default(),
        )
        .unwrap();

        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/simple/hats")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({"url": "file:///"}).to_string(),
            ))
            .unwrap();
        let response = router(service).oneshot(request).await.unwrap();
        let status = response.status();
        let plan: serde_json::Value = serde_json::from_str(&body_of(response).await).unwrap();

        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{plan}");
        assert!(
            plan["reason"].as_str().unwrap().contains("at most 2"),
            "{plan}"
        );
        assert_eq!(plan["num_partitions"], 4);
        assert_eq!(plan["requests"].as_array().unwrap().len(), 4);
    }

    /// A url that is not a catalog is a 404, the same as a missing file: the caller named a
    /// place this server can reach and there is nothing of the kind at it.
    #[tokio::test]
    async fn a_directory_that_is_not_a_catalog_is_not_found() {
        // No properties file under any of its names, which is the whole of what makes a
        // directory not a catalog.
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), query::tests::fixture()).unwrap();
        let (status, body) = ask_hats(
            mounted(dir.path(), &ApiConfig::default()),
            serde_json::json!({"url": "file:///"}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    }

    /// A region reaching more partitions than the server will read is refused, and says the
    /// two numbers rather than failing part way through.
    #[tokio::test]
    async fn a_request_over_too_many_partitions_is_refused() {
        let dir = hats::query::tests::fixture(true);
        let limits = LimitsConfig {
            max_partitions: 2,
            ..LimitsConfig::default()
        };
        let mounts = Arc::new(Mounts::new(&[serving(dir.path())], &DataConfig::default()).unwrap());
        let policy = AccessPolicy::new(
            &crate::config::AccessConfig::default(),
            Arc::clone(&mounts),
            None,
        )
        .unwrap();
        let service = Service::new(
            policy,
            &limits,
            mounts,
            &ApiConfig::default(),
            &DataConfig::default(),
            &crate::config::TapConfig::default(),
            &ServerConfig::default(),
        )
        .unwrap();

        let (status, body) = ask_hats(service, serde_json::json!({"url": "file:///"})).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert!(body.contains("at most 2"), "{body}");
    }
}
