//! `POST {api.prefix}/adql`: one ADQL statement over the tables the request declares.

use std::collections::BTreeMap;
use std::time::Instant;

use axum::extract::{State, rejection::JsonRejection};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use serde::Deserialize;
use serde::de::IgnoredAny;

use crate::adql;
use crate::app::answer::{attachment, counters, json_response};
use crate::app::request::{Format, Output, refuse_unknown, takes};
use crate::app::service::{PARQUET_CONTENT_TYPE, Service};
use crate::engine::query::QueryResult;
use crate::error::ApiError;
use crate::output::{dsv, parquet, votable};
use crate::storage::{self, SourceUrl, StorageOptions, parse_url};

/// A query written in IVOA's ADQL, over tables this request declares.
///
/// Its own body rather than the query routes': `columns` and `filters` are a projection and a
/// predicate against a target the url names, and a statement carries its own targets, its own
/// joins and its own ordering.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub(in crate::app) struct AdqlQuery {
    /// The ADQL statement. One `SELECT`, over the tables `tables` declares — `SELECT TOP 100
    /// source_id, ra, dec FROM gaia WHERE 1 = CONTAINS(POINT(ra, dec), CIRCLE(45.0, -20.0,
    /// 0.1))`. Grouping, ordering, joins and subqueries are answered; the geometry this
    /// service does not test is refused by name.
    #[schema(example = "SELECT TOP 100 objectid, objra, objdec FROM ztf WHERE nepochs > 10")]
    query: String,
    /// The tables the statement may read, each under the name it is written as in the query.
    /// A name the statement reads and this does not declare is an error.
    tables: BTreeMap<String, AdqlTable>,
    /// Which language the statement is written in: `ADQL`, or a version after the name —
    /// `ADQL-2.0` or `ADQL-2.1`. Absent is `ADQL`. It is TAP's `LANG` under another carrier,
    /// and answers to the same values.
    #[schema(example = "ADQL")]
    lang: Option<String>,
    /// `json`, the default; `parquet` for the answer as a parquet file; `votable` for a
    /// VOTable, `csv` for comma-separated text and `tsv` for tab-separated. The last three
    /// take flat columns only and refuse a nested one by name. Anything but `json` carries
    /// its counts in `x-hats-*` response headers, there being no room in the body.
    #[schema(example = "json")]
    format: Option<String>,
    /// What a null is written as in `csv` and `tsv`. Absent, a null is an empty field — the
    /// spelling an empty string also has, so the two cannot be told apart until this is set.
    /// At most 128 bytes, and no `,`, tab, newline or carriage return, whichever of the two
    /// formats was asked for.
    #[schema(example = "NULL")]
    dsv_null_value: Option<String>,
    /// Every key the body carried that this endpoint has no field for.
    #[serde(flatten)]
    #[schema(ignore)]
    unknown: BTreeMap<String, IgnoredAny>,
}

/// One table of an ADQL request: what kind of thing it is, and where.
///
/// The three fields are the same whichever kind it is — a catalog is a url and storage
/// options exactly as a file is — so `type` is a field rather than a tag over two variants
/// that would hold the same pair.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
struct AdqlTable {
    /// `parquet` for one file the url names outright, `hats` for a whole catalog.
    r#type: TableKind,
    /// The object to read. Its scheme picks the backend — `s3`, `gs`, `az`, `https`, `webdav`, `hf`
    /// or `file` — and which of those a deployment answers for is the operator's to configure.
    #[schema(value_type = String)]
    url: SourceUrl,
    /// How to reach the store: an endpoint, a region, credentials. Leave it out for a public
    /// object read anonymously.
    #[serde(default)]
    storage: StorageOptions,
}

/// What a table's url names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
enum TableKind {
    /// One parquet file.
    Parquet,
    /// A whole HATS catalog: the directory holding `hats.properties`, or a collection's, which
    /// is followed to its primary table. Only the partitions the query's region reaches are
    /// read, and a query reaching more than the server's partition limit is refused rather
    /// than started.
    Hats,
}

impl AdqlQuery {
    /// Every field this endpoint takes, in the order a body is written in.
    fn fields() -> Vec<&'static str> {
        vec!["query", "tables", "lang", "format", "dsv_null_value"]
    }

    /// The same list, as the sentence a refusal ends with.
    fn takes() -> String {
        takes(&Self::fields())
    }
}

/// Names a table may not be given, because a TAP layer over this will need them for itself.
///
/// Reserved now rather than when that layer arrives: a caller who has already written
/// `FROM TAP_SCHEMA` against this service would find it meaning something else the day it
/// does, which is worse than not being able to use the name today.
const RESERVED_TABLES: [&str; 2] = ["TAP_SCHEMA", "TAP_UPLOAD"];

/// One ADQL statement, planned and run over the tables the request declared.
pub(in crate::app) async fn query_adql(
    State(service): State<Service>,
    body: Result<Json<AdqlQuery>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(params) = body.map_err(|rejection| adql_body_error(&rejection))?;
    let started = Instant::now();
    refuse_unknown(&params.unknown, &AdqlQuery::takes())?;
    let output = Output::parse(
        params.format.as_deref(),
        params.dsv_null_value.as_deref(),
        Format::Json,
    )?;
    if let Some(asked) = &params.lang {
        adql::language::check("lang", asked)?;
    }
    // Everything decidable from the request alone, before a store is built. The statement is
    // read first because it says which of the declared tables are even needed, and because a
    // statement this service will not answer costs nothing to refuse.
    let translated = adql::translate(&params.query, service.sql_limits)?;
    for name in params.tables.keys() {
        if RESERVED_TABLES
            .iter()
            .any(|reserved| reserved.eq_ignore_ascii_case(name))
        {
            return Err(ApiError::bad_request(format!(
                "tables: {name} is reserved for a future TAP layer and cannot name a table here"
            )));
        }
    }

    let mut tables = Vec::new();
    let mut data_files = None;
    for (name, table) in &params.tables {
        let url = parse_url(table.url.as_str())?;
        let source = match table.r#type {
            TableKind::Parquet => {
                // The same question the single-file route asks: a url naming something this
                // service does not read as data names nothing it serves, and answering it here
                // costs no connection.
                let files = service.data_files_for(&url);
                if !files.matches_url(&url) {
                    return Err(ApiError::not_found(format!(
                        "tables: {name} does not name a data file; url must end in a name \
                         matching {}",
                        files.describe()
                    )));
                }
                adql::query::Source::File(storage::open(
                    &url,
                    &table.storage,
                    &service.policy,
                    &service.transfers,
                )?)
            }
            // A directory rather than an object, and no name to match: a catalog's own files
            // are what its metadata names, and which of those are rows is the question
            // `data_files` answers below rather than one about this url.
            TableKind::Hats => {
                data_files = Some(service.data_files_for(&url).clone());
                adql::query::Source::Catalog(storage::open_dir(
                    &url,
                    &table.storage,
                    &service.policy,
                    &service.transfers,
                )?)
            }
        };
        tables.push(adql::query::Table {
            name: name.clone(),
            source,
        });
    }

    // Whichever mount governs a catalog this request named, else the service's own list. A
    // request with no catalog never asks.
    let data_files = data_files.unwrap_or_else(|| service.data_files.as_ref().clone());
    let answer = adql::query::run(&translated, &tables, &data_files, service.adql_limits).await?;
    let result = answer.result;
    let num_rows = result.num_rows();
    let data_bytes_read = result.data_bytes_read;
    let response = adql_answer(&result, &output, started)?;
    tracing::info!(
        // The names and not the urls: a url may carry credentials, and which tables a
        // statement read is what a log is for. The statement itself is the caller's own text
        // and can be large, so what is recorded is its size.
        tables = %translated.tables.iter().cloned().collect::<Vec<_>>().join(","),
        query_bytes = params.query.len(),
        format = output.format.name(),
        num_rows,
        data_bytes_read,
        elapsed_ms = started.elapsed().as_millis(),
        "adql"
    );
    Ok(response)
}

/// The answer to a statement, in the encoding the request asked for.
///
/// Its own function because the parquet case differs: a single-file answer is laid out like
/// the file it came from, and a statement may have read several files or none whose layout
/// means anything for a set of groups. So the writer's own defaults, which is what
/// [`parquet::SourceLayout::default`] is.
fn adql_answer(
    result: &QueryResult,
    output: &Output,
    started: Instant,
) -> Result<Response, ApiError> {
    match output.format {
        Format::Json => json_response(result, started),
        Format::Parquet => Ok((
            attachment(PARQUET_CONTENT_TYPE, "query.parquet"),
            counters(result, result.num_rows(), started),
            parquet::encode(result, &parquet::SourceLayout::default())?,
        )
            .into_response()),
        Format::Votable => Ok((
            attachment(votable::CONTENT_TYPE, "query.vot"),
            counters(result, result.num_rows(), started),
            votable::encode(result)?,
        )
            .into_response()),
        Format::Dsv(kind) => Ok((
            attachment(kind.content_type(), &format!("query.{}", kind.name())),
            counters(result, result.num_rows(), started),
            dsv::encode(result, kind, &output.dsv_null)?,
        )
            .into_response()),
    }
}

/// A body this route could not read, said without quoting it back.
///
/// [`body_error`](crate::app::request::body_error) is written for the three query routes and
/// names their `columns` and `filters`; this one has neither, so the note it ends with is about
/// the shape a table entry takes — which is what a caller writing this body for the first time
/// gets wrong.
fn adql_body_error(rejection: &JsonRejection) -> ApiError {
    let takes = AdqlQuery::takes();
    if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
        return ApiError::too_much_work(
            "the request body is larger than this service accepts; send a shorter query, or \
             ask the operator to raise the body limit",
        );
    }
    match rejection {
        JsonRejection::JsonSyntaxError(error) => {
            ApiError::bad_request(format!("the request body is not valid JSON: {error}"))
        }
        _ => ApiError::bad_request(format!(
            "expected a JSON object with {takes}; query is one ADQL statement and tables maps \
             each name it reads to {{\"type\": \"parquet\", \"url\": …}}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::app::testing::{ask_hats, mounted, post_json};
    use crate::config::{ApiConfig, LimitsConfig};
    use crate::engine::query;
    use crate::hats;
    use crate::sky::region::Region;

    use super::*;

    /// One ADQL statement over a mounted parquet file, under the name `t`.
    async fn ask_adql(dir: &Path, query: &str) -> (StatusCode, String) {
        post_json(
            mounted(dir, &ApiConfig::default()),
            "/api/v1/adql",
            serde_json::json!({
                "query": query,
                "tables": {"t": {"type": "parquet", "url": "file:///part0.parquet"}},
            }),
        )
        .await
    }

    /// A directory holding the ten-row fixture, which every ADQL case below reads.
    fn adql_fixture() -> tempfile::TempDir {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), query::tests::fixture()).unwrap();
        dir
    }

    /// The route end to end, and the point of it: what a statement asks for is the planner's
    /// to answer, so the cases here are the ones the other routes refuse — a grouped
    /// aggregate, an ordering, a join of a table with itself.
    #[tokio::test]
    async fn the_adql_route_answers_what_the_planner_plans() {
        let dir = adql_fixture();

        let (status, body) = ask_adql(dir.path(), "SELECT TOP 3 objectid FROM t").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(answer["num_rows"], 3);

        // `TOP` became a `LIMIT` and the ordering is the statement's own, so the last three
        // ids come back rather than whichever rows the scan reached first.
        let (status, body) = ask_adql(
            dir.path(),
            "SELECT TOP 3 objectid FROM t ORDER BY objectid DESC",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(
            answer["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| row["objectid"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            [9, 8, 7]
        );

        // An aggregate over a group, which every other route refuses as an expression over
        // more than one row.
        let (status, body) = ask_adql(
            dir.path(),
            "SELECT band, COUNT(*) AS n FROM t GROUP BY band ORDER BY band",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(answer["num_rows"], 2);
        assert_eq!(answer["rows"][0]["n"], 5);

        // A join, which needs both sides at once.
        let (status, body) = ask_adql(
            dir.path(),
            "SELECT COUNT(*) AS n FROM t AS a JOIN t AS b ON a.objectid = b.objectid",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(answer["rows"][0]["n"], 10);
    }

    /// Two tables, which is what `tables` being a map is for: each is opened and registered
    /// under its own name, and the join is the planner's.
    #[tokio::test]
    async fn an_adql_statement_reads_more_than_one_table() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("a.parquet"), query::tests::fixture()).unwrap();
        std::fs::write(dir.path().join("b.parquet"), query::tests::sky_fixture()).unwrap();

        let (status, body) = post_json(
            mounted(dir.path(), &ApiConfig::default()),
            "/api/v1/adql",
            serde_json::json!({
                "query": "SELECT a.band, b.ra FROM a JOIN b ON a.objectid = b.objectid \
                          WHERE b.ra > 44.5 ORDER BY b.ra",
                "tables": {
                    "a": {"type": "parquet", "url": "file:///a.parquet"},
                    "b": {"type": "parquet", "url": "file:///b.parquet"},
                },
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        // The sky fixture runs 40..50 in right ascension against ids 0..10, so five rows are
        // past 44.5 and each carries the band its id has in the other file.
        assert_eq!(answer["num_rows"], 5, "{body}");
        assert_eq!(answer["rows"][0]["ra"], 45.0);
    }

    /// **An unquoted name is case-insensitive here, which is ADQL's own rule** (§2.1.3) and
    /// not the one the `simple` routes follow. It has to be applied by hand because
    /// identifier normalization is off: with it on DataFusion would lowercase `Gmag` and put
    /// every mixed-case astronomy column out of reach.
    ///
    /// A delimited name is exact, which is the other half of the same rule and what makes a
    /// column whose spelling a client read out of `TAP_SCHEMA` reachable unambiguously.
    #[tokio::test]
    async fn an_adql_name_is_case_insensitive_unless_it_is_quoted() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("part0.parquet"),
            query::tests::mixed_case_fixture(),
        )
        .unwrap();

        for query in [
            "SELECT Gmag FROM t",
            "SELECT gmag FROM t",
            "SELECT GMAG FROM t",
            "SELECT \"Gmag\" FROM t",
            // Qualified by the table, and by an alias, since the column is the last segment
            // either way.
            "SELECT t.gmag FROM t",
            "SELECT a.GMAG FROM t AS a",
            // And the table's name by the same rule.
            "SELECT objectid FROM T",
        ] {
            let (status, body) = ask_adql(dir.path(), query).await;
            assert_eq!(status, StatusCode::OK, "{query}: {body}");
            let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(answer["num_rows"], 10, "{query}");
        }

        // A delimited name is the file's own spelling and no other.
        let (status, body) = ask_adql(dir.path(), "SELECT \"GMAG\" FROM t").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    }

    /// The volatility rule holds inside a statement. It is the one thing `engine::sql` checks that
    /// a planner will not: `random()` is an ordinary scalar function to DataFusion.
    ///
    /// `LOG` is the case that shows the translation is what decides. `sql::AMBIGUOUS` refuses
    /// `log` where the caller wrote SQL, because base ten and the natural logarithm are both
    /// plausible readings; ADQL says which its own is, so here it becomes `ln` and is answered.
    #[tokio::test]
    async fn an_adql_statement_meets_the_function_rules() {
        let dir = adql_fixture();
        let (status, body) = ask_adql(dir.path(), "SELECT random() FROM t").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("the same way twice"), "{body}");

        // `RAND` is the one function let through that rule, ADQL making it mandatory — and
        // `random()` above is what shows the exception is the name and not the volatility.
        let (status, body) = ask_adql(dir.path(), "SELECT RAND() AS r FROM t").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        let drawn = answer["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["r"].as_f64().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(drawn.len(), 10);
        assert!(
            drawn.windows(2).any(|pair| pair[0] != pair[1]),
            "a constant rather than a column: {drawn:?}"
        );

        let (status, body) = ask_adql(dir.path(), "SELECT LOG(objectid) AS l FROM t").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        // The natural logarithm of 2, which base ten would answer 0.301 for. The row before
        // it is `log(1)`, which both bases answer 0 for and which would prove nothing.
        assert_eq!(answer["rows"][2]["l"], std::f64::consts::LN_2);
    }

    /// A region test through the route, which is the whole path nothing else runs end to end:
    /// the statement is translated, the table registered, `CONTAINS` becomes the function
    /// `geometry` registers, and that rewrites itself into the predicate `sky::region` builds.
    ///
    /// Every spelling ADQL gives a region test, against rows a degree apart, so a test that
    /// read one the wrong way round returns a different row rather than the same count.
    #[tokio::test]
    async fn the_adql_route_answers_a_region() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("part0.parquet"),
            query::tests::sky_fixture(),
        )
        .unwrap();

        for predicate in [
            "1 = CONTAINS(POINT(ra, dec), CIRCLE(42.0, -20.0, 0.1))",
            "CONTAINS(POINT(ra, dec), CIRCLE(42.0, -20.0, 0.1)) = 1",
            "1 = INTERSECTS(POINT(ra, dec), CIRCLE(42.0, -20.0, 0.1))",
            "DISTANCE(POINT(ra, dec), POINT(42.0, -20.0)) < 0.1",
            "DISTANCE(ra, dec, 42.0, -20.0) < 0.1",
        ] {
            let (status, body) = ask_adql(
                dir.path(),
                &format!("SELECT objectid FROM t WHERE {predicate}"),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{predicate}: {body}");
            let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(answer["num_rows"], 1, "{predicate}: {body}");
            // The row two degrees along from the first, which is the one the circle is on.
            assert_eq!(answer["rows"][0]["objectid"], 2, "{predicate}");
        }

        // Compared with 0, which is the same test negated rather than a different one.
        let (status, body) = ask_adql(
            dir.path(),
            "SELECT objectid FROM t WHERE 0 = CONTAINS(POINT(ra, dec), CIRCLE(42.0, -20.0, 0.1))",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(answer["num_rows"], 9);
    }

    /// An ADQL function this service does not implement says so, rather than being reported as
    /// a function nobody has heard of.
    #[tokio::test]
    async fn an_adql_function_this_service_does_not_implement_says_so() {
        let dir = adql_fixture();
        let (status, body) = ask_adql(
            dir.path(),
            "SELECT objectid FROM t WHERE 1 = CONTAINS(POINT(ra, dec), BOX(1, 2, 3, 4))",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(
            body.contains("BOX") && body.contains("not implemented"),
            "{body}"
        );
    }

    /// A table the statement reads and the request did not declare, which is the one thing
    /// standing between `FROM` and a file nobody named.
    #[tokio::test]
    async fn an_adql_statement_reads_only_the_tables_the_request_declared() {
        let dir = adql_fixture();
        let (status, body) = ask_adql(dir.path(), "SELECT objectid FROM somewhere_else").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("declares no table"), "{body}");
    }

    /// A whole catalog as a table, which is the point of the route: the statement names the
    /// catalog and the planner reads the partitions a region reaches.
    #[tokio::test]
    async fn the_adql_route_answers_a_catalog() {
        let dir = hats::query::tests::fixture(true);
        let region = hats::query::tests::regions()[0].clone();
        let expected = hats::query::tests::inside(&region);
        assert!(!expected.is_empty(), "the cone selects nothing");
        let (ra, dec, radius) = match &region {
            Region::Circle {
                ra,
                dec,
                radius_deg,
                ..
            } => (*ra, *dec, radius_deg.unwrap()),
            other => panic!("the fixture's first region is a circle: {other:?}"),
        };

        let (status, body) = post_json(
            mounted(dir.path(), &ApiConfig::default()),
            "/api/v1/adql",
            serde_json::json!({
                "query": format!(
                    "SELECT id FROM c WHERE 1 = CONTAINS(POINT(ra, dec), \
                     CIRCLE({ra}, {dec}, {radius})) ORDER BY id"
                ),
                "tables": {"c": {"type": "hats", "url": "file:///"}},
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        // The same rows the fan-out returns for the same circle, which is the claim: two
        // routes over one catalog are two ways of asking, not two answers.
        assert_eq!(
            answer["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| row["id"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            expected,
            "{body}"
        );
    }

    /// An aggregate over a whole catalog, which is what a statement is for and what no other
    /// route can answer: the partition fan-out cannot combine rows across partitions.
    #[tokio::test]
    async fn the_adql_route_aggregates_a_catalog() {
        let dir = hats::query::tests::fixture(true);
        let (status, body) = post_json(
            mounted(dir.path(), &ApiConfig::default()),
            "/api/v1/adql",
            serde_json::json!({
                "query": "SELECT COUNT(*) AS n, MIN(id) AS lowest FROM c",
                "tables": {"c": {"type": "hats", "url": "file:///"}},
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();

        // Against what the fan-out returns for the same catalog, which is the only number
        // worth comparing to: an aggregate nobody can check is an aggregate of anything.
        let (status, counted) = ask_hats(
            mounted(dir.path(), &ApiConfig::default()),
            serde_json::json!({"url": "file:///", "columns": ["id"]}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{counted}");
        let counted: serde_json::Value = serde_json::from_str(&counted).unwrap();
        assert_eq!(answer["rows"][0]["n"], counted["num_rows"], "{body}");
        assert!(answer["rows"][0]["n"].as_i64().unwrap() > 0);
    }

    /// **The bound that acts before any work.** The memory pool bounds memory and the clock
    /// bounds time; neither refuses a scan of every partition before it starts, and a
    /// statement has no plan route to be answered with instead.
    #[tokio::test]
    async fn a_catalog_scan_wider_than_the_partition_bound_is_refused() {
        let dir = hats::query::tests::fixture(true);
        let limits = LimitsConfig {
            max_partitions: 2,
            ..LimitsConfig::default()
        };
        let mut service = mounted(dir.path(), &ApiConfig::default());
        service.adql_limits = (&limits).into();
        let (status, body) = post_json(
            service,
            "/api/v1/adql",
            serde_json::json!({
                "query": "SELECT COUNT(*) AS n FROM c",
                "tables": {"c": {"type": "hats", "url": "file:///"}},
            }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("partitions"), "{body}");
    }

    /// **The partition bound counts partitions opened, so a `TOP` the first partition can
    /// fill is answered** over a catalog with more partitions than are allowed — and one that
    /// needs a second partition is refused on reaching it, never answered short.
    ///
    /// Both halves run with one partition allowed. The first shows the scan is lazy: an eager
    /// one would have refused before reading. The second shows the count still binds: a lazy
    /// one without it would have read on and answered.
    #[tokio::test]
    async fn a_limit_is_answered_from_the_partitions_it_needs() {
        let dir = hats::query::tests::fixture(true);
        let limits = LimitsConfig {
            max_partitions: 1,
            ..LimitsConfig::default()
        };
        let ask = async |query: &str| {
            let mut service = mounted(dir.path(), &ApiConfig::default());
            service.adql_limits = (&limits).into();
            post_json(
                service,
                "/api/v1/adql",
                serde_json::json!({
                    "query": query,
                    "tables": {"c": {"type": "hats", "url": "file:///"}},
                }),
            )
            .await
        };

        let (status, body) = ask("SELECT TOP 1 id FROM c").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(answer["num_rows"], 1, "{body}");
        // What the partitions' own scans fetched, which a scan planned while this one runs has
        // to report itself or the answer says nothing was read.
        assert!(answer["data_bytes_read"].as_u64().unwrap() > 0, "{body}");

        // More rows than any one partition of the fixture holds, so it needs a second.
        let (status, body) = ask("SELECT TOP 100000 id FROM c").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("partitions"), "{body}");

        // The same when the limit reaches the scan only as a filter that stops pulling.
        let (status, body) = ask("SELECT TOP 1 id FROM c WHERE id >= 0").await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    /// `ORDER BY` the index is answered by the order partitions are read in, so a `TOP` over
    /// it reads the partitions at that end of the catalog and no others. With one partition
    /// allowed, a sort that consumed its whole input would be refused.
    ///
    /// The fixture's ids ascend with the index across the catalog and within each partition,
    /// so the ids say both which partition was read and that its rows were sorted: the
    /// descending answer has to reverse the order the file holds them in.
    #[tokio::test]
    async fn an_order_by_the_index_reads_from_that_end_of_the_catalog() {
        let dir = hats::query::tests::fixture(true);
        let limits = LimitsConfig {
            max_partitions: 1,
            ..LimitsConfig::default()
        };
        let ask = async |query: &str| {
            let mut service = mounted(dir.path(), &ApiConfig::default());
            service.adql_limits = (&limits).into();
            post_json(
                service,
                "/api/v1/adql",
                serde_json::json!({
                    "query": query,
                    "tables": {"c": {"type": "hats", "url": "file:///"}},
                }),
            )
            .await
        };
        let ids = |body: &str| -> Vec<i64> {
            let answer: serde_json::Value = serde_json::from_str(body).unwrap();
            answer["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| row["id"].as_i64().unwrap())
                .collect()
        };
        let total = i64::try_from(hats::query::tests::fixture_rows()).unwrap();

        let (status, body) = ask("SELECT TOP 3 id FROM c ORDER BY _healpix_29 DESC").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(ids(&body), [total, total - 1, total - 2], "{body}");

        let (status, body) = ask("SELECT TOP 3 id FROM c ORDER BY _healpix_29 ASC").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(ids(&body), [1, 2, 3], "{body}");

        // Through a filter and under an alias for the column, which is the planner's to see.
        let (status, body) =
            ask("SELECT TOP 2 id, _healpix_29 AS h FROM c WHERE id > 0 ORDER BY h DESC").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(ids(&body), [total, total - 1], "{body}");

        // A second key keeps the sort, over rows already in order by the first, and the sort
        // stops once the first partition's rows are past the ones it holds.
        let (status, body) = ask("SELECT TOP 3 id FROM c ORDER BY _healpix_29 DESC, id DESC").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(ids(&body), [total, total - 1, total - 2], "{body}");

        // A first key that is not the index gives the walk nothing to go by, so the sort reads
        // every partition.
        let (status, body) = ask("SELECT TOP 3 id FROM c ORDER BY id DESC, _healpix_29").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("partitions"), "{body}");
    }

    /// **That the pruning has teeth**, which the tests above cannot show: they would pass
    /// whether or not a partition was skipped, since skipping one changes what a query costs
    /// and not what it answers.
    ///
    /// The bound is what makes the difference visible. With fewer partitions allowed than the
    /// catalog has, a query carrying a region still answers — which it can only do if the
    /// region removed partitions before the bound was checked — while the same query without
    /// one is refused for reaching them all.
    #[tokio::test]
    async fn a_region_prunes_the_partitions_a_statement_reads() {
        let dir = hats::query::tests::fixture(true);
        let region = hats::query::tests::regions()[0].clone();
        let (ra, dec, radius) = match &region {
            Region::Circle {
                ra,
                dec,
                radius_deg,
                ..
            } => (*ra, *dec, radius_deg.unwrap()),
            other => panic!("the fixture's first region is a circle: {other:?}"),
        };
        let limits = LimitsConfig {
            max_partitions: 1,
            ..LimitsConfig::default()
        };
        let service = || {
            let mut service = mounted(dir.path(), &ApiConfig::default());
            service.adql_limits = (&limits).into();
            service
        };

        let (status, body) = post_json(
            service(),
            "/api/v1/adql",
            serde_json::json!({
                "query": format!(
                    "SELECT COUNT(*) AS n FROM c WHERE 1 = CONTAINS(POINT(ra, dec), \
                     CIRCLE({ra}, {dec}, {radius}))"
                ),
                "tables": {"c": {"type": "hats", "url": "file:///"}},
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "the region did not prune: {body}");

        let (status, body) = post_json(
            service(),
            "/api/v1/adql",
            serde_json::json!({
                "query": "SELECT COUNT(*) AS n FROM c",
                "tables": {"c": {"type": "hats", "url": "file:///"}},
            }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    }

    /// **A region outside a catalog's coverage answers with no rows and the columns asked
    /// for.** It is the ordinary case — a cone where the survey did not look — and the scan
    /// that answers it reads nothing, so what it hands back is a schema rather than a file.
    /// That schema has to be the projected one: the projection above it carries indices into
    /// what the scan returns, and the whole catalog's schema resolves every one of them
    /// against the wrong column.
    ///
    /// The column asked for is deliberately not the catalog's first. With `id` the mistake
    /// is invisible, index 0 being right by luck.
    #[tokio::test]
    async fn a_region_that_reaches_no_partition_answers_the_columns_asked_for() {
        let dir = hats::query::tests::fixture(true);
        for region in [
            // Nowhere near the fixture's cells.
            "CIRCLE(180.0, 60.0, 0.001)",
            // A MOC of cells the catalog does not hold, which has no coordinate test at
            // all — so the scan is chosen by the covering alone.
            "MOC('3/3 10')",
        ] {
            let (status, body) = post_json(
                mounted(dir.path(), &ApiConfig::default()),
                "/api/v1/adql",
                serde_json::json!({
                    "query": format!(
                        "SELECT ra, dec FROM c WHERE 1 = CONTAINS(POINT(ra, dec), {region})"
                    ),
                    "tables": {"c": {"type": "hats", "url": "file:///"}},
                }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{region}: {body}");
            let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(answer["num_rows"], 0, "{region}: {body}");
            assert_eq!(
                answer["schema"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|column| column["name"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                ["ra", "dec"],
                "{region}: {body}"
            );
        }
    }

    /// **A catalog whose coordinates are `Float32` answers a region**, the same rows as the
    /// `Float64` one and pruned the same way.
    ///
    /// A region in a statement is built after type coercion has run, so nothing widened the
    /// columns: the bounds compared a `Float32` column with an `f64` literal and arrow refused
    /// the whole query. One partition allowed is what shows the covering still prunes through
    /// the cast; a `DISTANCE` value and a crossmatch are the two other ways a coordinate
    /// column reaches the geometry, and the projection shows the column keeps its own type.
    #[tokio::test]
    async fn a_catalog_with_narrow_coordinates_answers_a_region() {
        let dir = hats::query::tests::narrow_fixture();
        let region = hats::query::tests::regions()[0].clone();
        let expected = hats::query::tests::inside(&region);
        let (ra, dec, radius) = match &region {
            Region::Circle {
                ra,
                dec,
                radius_deg,
                ..
            } => (*ra, *dec, radius_deg.unwrap()),
            other => panic!("the fixture's first region is a circle: {other:?}"),
        };
        let limits = LimitsConfig {
            max_partitions: 1,
            ..LimitsConfig::default()
        };
        let ask = async |query: String, tables: serde_json::Value| {
            let mut service = mounted(dir.path(), &ApiConfig::default());
            service.adql_limits = (&limits).into();
            post_json(
                service,
                "/api/v1/adql",
                serde_json::json!({"query": query, "tables": tables}),
            )
            .await
        };
        let one = serde_json::json!({"c": {"type": "hats", "url": "file:///"}});

        let (status, body) = ask(
            format!(
                "SELECT id, ra FROM c WHERE 1 = CONTAINS(POINT(ra, dec), \
                 CIRCLE({ra}, {dec}, {radius})) ORDER BY id"
            ),
            one.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        // Compared with the f64 positions the fixture was written from, so a row within
        // single precision of the edge may land on either side of it.
        let ids = answer["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_i64().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(ids, expected, "{body}");
        assert_eq!(answer["schema"][1]["type"], "Float32", "{body}");

        let (status, body) = ask(
            format!(
                "SELECT TOP 1 DISTANCE(POINT(ra, dec), POINT({ra}, {dec})) AS sep FROM c \
                 WHERE 1 = CONTAINS(POINT(ra, dec), CIRCLE({ra}, {dec}, {radius})) ORDER BY sep"
            ),
            one,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let (status, body) = ask(
            format!(
                "SELECT a.id AS aid, b.id AS bid FROM left AS a JOIN right AS b \
                   ON 1 = CONTAINS(POINT(b.ra, b.dec), CIRCLE(a.ra, a.dec, 0.0001)) \
                 WHERE 1 = CONTAINS(POINT(a.ra, a.dec), CIRCLE({ra}, {dec}, {radius})) \
                   AND 1 = CONTAINS(POINT(b.ra, b.dec), CIRCLE({ra}, {dec}, {radius}))"
            ),
            serde_json::json!({
                "left": {"type": "hats", "url": "file:///"},
                "right": {"type": "hats", "url": "file:///"},
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(answer["num_rows"], expected.len(), "{body}");
    }

    /// A crossmatch: two catalogs joined on the separation between their rows, which is
    /// ADQL's own spelling of one — a circle whose centre is a row of the other side.
    ///
    /// Each side carries its own region, which is what chooses the partitions; the join
    /// condition only says which of the surviving pairs match, and nothing about it prunes.
    /// The fixture's rows are cell centres well apart, so at a radius far below that spacing
    /// the only pairs are each row with itself — an answer a test can state exactly.
    ///
    /// **Run with one partition allowed**, which is what shows each side's own region still
    /// pruning. With two catalogs in scope a bare `ra` belongs to both and so does
    /// `_healpix_29`; a predicate that lost track of which side it was about would either
    /// fail to plan or quietly drop the covering, and dropping it reaches every partition and
    /// is refused here rather than answered slowly.
    #[tokio::test]
    async fn the_adql_route_crossmatches_two_catalogs() {
        let dir = hats::query::tests::fixture(true);
        let limits = LimitsConfig {
            max_partitions: 1,
            ..LimitsConfig::default()
        };
        let region = hats::query::tests::regions()[0].clone();
        let expected = hats::query::tests::inside(&region);
        let (ra, dec, radius) = match &region {
            Region::Circle {
                ra,
                dec,
                radius_deg,
                ..
            } => (*ra, *dec, radius_deg.unwrap()),
            other => panic!("the fixture's first region is a circle: {other:?}"),
        };

        let mut service = mounted(dir.path(), &ApiConfig::default());
        service.adql_limits = (&limits).into();
        let (status, body) = post_json(
            service,
            "/api/v1/adql",
            serde_json::json!({
                "query": format!(
                    "SELECT a.id AS aid, b.id AS bid \
                     FROM left AS a JOIN right AS b \
                       ON 1 = CONTAINS(POINT(b.ra, b.dec), CIRCLE(a.ra, a.dec, 0.0001)) \
                     WHERE 1 = CONTAINS(POINT(a.ra, a.dec), CIRCLE({ra}, {dec}, {radius})) \
                       AND 1 = CONTAINS(POINT(b.ra, b.dec), CIRCLE({ra}, {dec}, {radius})) \
                     ORDER BY aid"
                ),
                "tables": {
                    "left": {"type": "hats", "url": "file:///"},
                    "right": {"type": "hats", "url": "file:///"},
                },
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        let rows = answer["rows"].as_array().unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row["aid"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            expected,
            "{body}"
        );
        // Each row matched itself and nothing else, which is what makes the count above a
        // statement about the join rather than about the two regions.
        assert!(rows.iter().all(|row| row["aid"] == row["bid"]), "{body}");
    }

    /// A separation as a value, which is what a crossmatch reports beside the pair.
    #[tokio::test]
    async fn the_adql_route_answers_a_separation() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("part0.parquet"),
            query::tests::sky_fixture(),
        )
        .unwrap();
        let (status, body) = ask_adql(
            dir.path(),
            "SELECT TOP 1 DISTANCE(POINT(ra, dec), POINT(42.0, -20.0)) AS sep FROM t \
             ORDER BY sep",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        let separation = answer["rows"][0]["sep"].as_f64().unwrap();
        // The fixture's rows are a degree apart along a meridian and the circle is on one of
        // them, so the nearest is that row itself.
        assert!(separation < 1e-9, "{body}");
    }

    /// A catalog's positions are where the catalog says they are, and a region over any other
    /// pair of its columns is refused rather than answered from the wrong partitions.
    ///
    /// Swapped is the case worth testing because it is the one that happens, and because it
    /// fails silently without the check: the partitions are chosen by an index over `ra` and
    /// `dec`, so a cone at the transposed position finds none of them and the answer is
    /// empty — fewer rows than the shape holds, with nothing saying why.
    #[tokio::test]
    async fn a_region_over_a_catalogs_other_columns_is_refused() {
        let dir = hats::query::tests::fixture(true);
        let (status, body) = post_json(
            mounted(dir.path(), &ApiConfig::default()),
            "/api/v1/adql",
            serde_json::json!({
                "query": "SELECT id FROM c WHERE 1 = CONTAINS(POINT(dec, ra), \
                          CIRCLE(10.0, 10.0, 0.5))",
                "tables": {"c": {"type": "hats", "url": "file:///"}},
            }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("ra") && body.contains("dec"), "{body}");
    }

    /// The same statement against one file, which answers it. A file says nothing about which
    /// of its columns hold a position, so the caller naming them is the only claim there is
    /// and there is nothing for this to contradict.
    #[tokio::test]
    async fn a_region_over_a_files_columns_is_the_callers_to_choose() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("part0.parquet"),
            query::tests::sky_fixture(),
        )
        .unwrap();
        let (status, body) = ask_adql(
            dir.path(),
            "SELECT objectid FROM t WHERE 1 = CONTAINS(POINT(dec, ra), CIRCLE(42.0, -20.0, 0.1))",
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    /// `lang` is TAP's `LANG` under another carrier, and answers to the same values — a
    /// query moved between the two routes carries the same one. A language this service
    /// does not answer is refused rather than parsed as ADQL and failed later.
    #[tokio::test]
    async fn an_adql_request_may_say_which_language_it_wrote() {
        let dir = adql_fixture();
        let ask = async |lang: serde_json::Value| {
            post_json(
                mounted(dir.path(), &ApiConfig::default()),
                "/api/v1/adql",
                serde_json::json!({
                    "query": "SELECT TOP 1 objectid FROM t",
                    "tables": {"t": {"type": "parquet", "url": "file:///part0.parquet"}},
                    "lang": lang,
                }),
            )
            .await
        };

        // Absent is ADQL, and a version after the name is the same language.
        for lang in [
            serde_json::Value::Null,
            "ADQL".into(),
            "ADQL-2.0".into(),
            "adql-2.1".into(),
        ] {
            let (status, body) = ask(lang.clone()).await;
            assert_eq!(status, StatusCode::OK, "{lang}: {body}");
        }

        let (status, body) = ask("PQL".into()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("PQL") && body.contains("ADQL"), "{body}");
    }

    /// Reserved now, before a TAP layer needs them, so that no caller writes a statement
    /// against this service that would mean something else the day it arrives.
    #[tokio::test]
    async fn an_adql_table_may_not_take_a_reserved_name() {
        let dir = adql_fixture();
        let (status, body) = post_json(
            mounted(dir.path(), &ApiConfig::default()),
            "/api/v1/adql",
            serde_json::json!({
                "query": "SELECT objectid FROM TAP_SCHEMA",
                "tables": {"TAP_SCHEMA": {"type": "parquet", "url": "file:///part0.parquet"}},
            }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("reserved"), "{body}");
    }

    /// The operator's ceiling on an answer, which a statement can reach without asking for
    /// many rows: a cross join of ten rows with themselves is a hundred.
    #[tokio::test]
    async fn an_adql_answer_larger_than_the_cap_is_refused() {
        let dir = adql_fixture();
        let limits = LimitsConfig {
            max_rows: 50,
            ..LimitsConfig::default()
        };
        let mut service = mounted(dir.path(), &ApiConfig::default());
        service.adql_limits = (&limits).into();
        let (status, body) = post_json(
            service,
            "/api/v1/adql",
            serde_json::json!({
                "query": "SELECT a.objectid FROM t AS a, t AS b",
                "tables": {"t": {"type": "parquet", "url": "file:///part0.parquet"}},
            }),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert!(body.contains("more than 50 rows"), "{body}");
    }
}
