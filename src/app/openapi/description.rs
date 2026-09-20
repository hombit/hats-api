//! This service's own document: the operations its routes answer, the bodies they take, and an
//! example of each that runs.

use crate::app::answer::{HatsResponse, SelectResponse};
use crate::app::openapi;
use crate::app::routes::adql::AdqlQuery;
use crate::app::routes::hats::{CatalogPlanQuery, CatalogQuery, PlanResponse};
use crate::app::routes::parquet::ParquetQuery;
use crate::app::service::{QUERY_SEGMENT, route};

/// One line saying what those three routes take, for the API description.
const QUERY_SUMMARY: &str = "Names and a condition: `columns` is a list of column names, \
    `filters` one row condition. A computed column or an alias is written in ADQL.";

/// The whole document: the health route, the three query routes, and ADQL.
///
/// `contact` is `[server] contact`, which is who runs this deployment rather than anything
/// about its routes — so it reaches the document here rather than being built into it.
pub(in crate::app) fn describe(prefix: &str, contact: Option<&str>) -> utoipa::openapi::OpenApi {
    let mut paths = utoipa::openapi::Paths::new();
    let mut schemas = Vec::new();
    openapi::health(&mut paths, &route(prefix, "health"));
    describe_queries(&mut paths, &mut schemas, prefix);
    describe_adql(&mut paths, &mut schemas, prefix);
    let components = utoipa::openapi::ComponentsBuilder::new()
        .schemas_from_iter(schemas)
        .build();
    openapi::document(paths, components, contact)
}

/// The three query operations, alongside
/// [`with_queries`](crate::app::service::with_queries)'s three routes.
fn describe_queries(
    paths: &mut utoipa::openapi::Paths,
    schemas: &mut Vec<(String, utoipa::openapi::RefOr<utoipa::openapi::Schema>)>,
    prefix: &str,
) {
    // One body per endpoint rather than one for all three, which is what makes the description
    // say of each route exactly what that route takes.
    let file_body = named::<ParquetQuery>(schemas);
    let catalog_body = named::<CatalogQuery>(schemas);
    let plan_body = named::<CatalogPlanQuery>(schemas);
    let plan = named::<PlanResponse>(schemas);
    let rows = named::<SelectResponse>(schemas);
    let catalog_rows = named::<HatsResponse>(schemas);
    let path = |target: &str| route(prefix, &format!("{QUERY_SEGMENT}/{target}"));

    openapi::post(
        paths,
        &path("parquet"),
        openapi::operation(
            QUERY_SEGMENT,
            "Query one parquet file",
            QUERY_SUMMARY,
            file_body,
            example(
                EXAMPLE_PARTITION,
                PARTITION_COLUMNS,
                PARTITION_FILTERS,
                false,
            ),
            "The rows, or a parquet file or a VOTable where `format` asked for one",
            rows,
        ),
    );
    openapi::post(
        paths,
        &path("hats"),
        openapi::operation(
            QUERY_SEGMENT,
            "Query a HATS catalog",
            QUERY_SUMMARY,
            catalog_body,
            example(EXAMPLE_CATALOG, CATALOG_COLUMNS, CATALOG_FILTERS, true),
            "The rows, in the catalog's own order, with the partitions they came from",
            catalog_rows,
        ),
    );
    openapi::post(
        paths,
        &path("hats/plan"),
        openapi::operation(
            QUERY_SEGMENT,
            "Resolve a catalog query without running it",
            QUERY_SUMMARY,
            plan_body,
            example(EXAMPLE_CATALOG, CATALOG_COLUMNS, CATALOG_FILTERS, true),
            "One request per partition, for the client to send itself",
            plan,
        ),
    );
}

/// The one ADQL operation.
///
/// Not part of [`describe_queries`]: there is one route rather than a set, and a statement
/// carries its own targets and projection rather than taking them from the url and the body.
fn describe_adql(
    paths: &mut utoipa::openapi::Paths,
    schemas: &mut Vec<(String, utoipa::openapi::RefOr<utoipa::openapi::Schema>)>,
    prefix: &str,
) {
    let body = named::<AdqlQuery>(schemas);
    let rows = named::<SelectResponse>(schemas);
    openapi::post(
        paths,
        &route(prefix, "adql"),
        openapi::operation(
            "adql",
            "Query with ADQL",
            "IVOA's query language over tables this request declares, each one a parquet file \
             or a whole HATS catalog. Grouping, ordering, joins and subqueries are answered; a \
             region is `1 = CONTAINS(POINT(ra, dec), CIRCLE(…))`, which chooses the partitions \
             read rather than being tested over everything.",
            body,
            serde_json::json!({
                "query": format!(
                    "SELECT TOP 10 {} FROM gaia \
                     WHERE 1 = CONTAINS(POINT(ra, dec), \
                     CIRCLE({ADQL_EXAMPLE_RA}, {ADQL_EXAMPLE_DEC}, {ADQL_EXAMPLE_RADIUS}))",
                    CATALOG_COLUMNS.join(", ")
                ),
                "tables": {
                    "gaia": {"type": "hats", "url": EXAMPLE_CATALOG},
                },
                "format": "json",
            }),
            "The rows, or a parquet file or a VOTable where `format` asked for one",
            rows,
        ),
    );
}

/// Gaia DR3, which a reader can send the catalog examples at as written: a real collection,
/// published anonymously, all-sky, and with partitions even enough that a reader who moves the
/// circle gets the same answer in the same time.
const EXAMPLE_CATALOG: &str = "s3://stpubdata/gaia/gaia_dr3/public/hats";
const CATALOG_COLUMNS: &[&str] = &["source_id", "ra", "dec", "phot_g_mean_mag"];
const CATALOG_FILTERS: &str = "parallax > 1";

/// Anywhere will do — the catalog is all-sky — and this is away from the galactic plane, where
/// a five-arcminute circle is a handful of rows rather than a crowd.
const EXAMPLE_RA: f64 = 30.0;
const EXAMPLE_DEC: f64 = 5.0;
const EXAMPLE_RADIUS_ARCSEC: u32 = 300;

/// One partition of ZTF DR24's light curves, which the catalogs above have nothing like: a
/// `lightcurve` column holding every epoch of a source. That is what makes this the example
/// worth spending on the single-file route — a dotted name reaching into a struct column is
/// the one thing a reader cannot see demonstrated anywhere else on the page.
///
/// The smallest partition ZTF has, at 180 KB against nearly 4 GB for its largest. A partition
/// is named outright here rather than reached through the catalog, so picking a small one
/// costs the example nothing and buys it a second.
const EXAMPLE_PARTITION: &str = "s3://ipac-irsa-ztf/ztf/enhanced/dr24/lc/hats/\
    ztf_dr24_lc-hats/dataset/Norder=6/Dir=30000/Npix=34623/part0.snappy.parquet";
const PARTITION_COLUMNS: &[&str] = &["objectid", "objra", "objdec", "lightcurve.mag"];
const PARTITION_FILTERS: &str = "nepochs > 10";

/// A position with a bright source an arcsecond away, so the ADQL example returns a row rather
/// than an empty answer a reader would read as a fault. In degrees, which is what ADQL's
/// `CIRCLE` takes; the radius is one arcsecond.
///
/// The circle is what makes the example cheap against the whole catalog: it reaches one
/// partition of 230 MB, of which the query reads 7 — the region test is a covering over
/// `_healpix_29` before it is trigonometry, so what a small circle costs is the row groups it
/// reaches. About a second.
const ADQL_EXAMPLE_RA: f64 = 254.45754;
const ADQL_EXAMPLE_DEC: f64 = 35.34235;
const ADQL_EXAMPLE_RADIUS: f64 = 0.000278;

/// A body the route it is shown on will actually answer, in about a second: the url, a few
/// columns, a condition, a limit, and for a catalog a circle.
///
/// Everything is a parameter because the two targets name different files: a catalog gets the
/// circle, since without one a catalog query reads every partition — against a real catalog
/// that is minutes, and a reader pressing the button would conclude the service was broken —
/// and the single file gets the columns of the one catalog here with anything nested in it.
///
/// **A few named columns.** What a request against a real catalog costs is the columns it
/// projects and not the rows it returns: a nested column holding every epoch of a light curve
/// is seconds where four flat ones are under one.
///
/// A `limit` alone would not stand in for the circle: it stops the read once enough rows are
/// found, and a predicate that most partitions fail keeps it reading.
fn example(url: &str, columns: &[&str], filters: &str, region: bool) -> serde_json::Value {
    let mut body = serde_json::Map::new();
    body.insert("url".to_owned(), serde_json::Value::String(url.to_owned()));
    body.insert("columns".to_owned(), serde_json::json!(columns));
    body.insert("filters".to_owned(), serde_json::json!(filters));
    if region {
        body.insert(
            "region".to_owned(),
            serde_json::json!([{
                "type": "circle",
                "ra": EXAMPLE_RA,
                "dec": EXAMPLE_DEC,
                "radius_arcsec": EXAMPLE_RADIUS_ARCSEC,
            }]),
        );
    }
    body.insert("limit".to_owned(), serde_json::json!(10));
    serde_json::Value::Object(body)
}

/// Register a type's schema under its name, and everything it refers to, and hand back the
/// reference to it.
///
/// A component and a `$ref` rather than the schema written into each operation: a response is
/// several routes' here, and a reader wants to see one name for it.
///
/// Only for a type with no generic parameter. `ToSchema::schemas` composes a type argument into
/// the name — `PlanBody_T` — while `ToSchema::name` drops it and answers `PlanBody` for every
/// instantiation, so two instantiations registered here would land at one key and the second
/// would silently replace the first.
fn named<T: utoipa::ToSchema>(
    schemas: &mut Vec<(String, utoipa::openapi::RefOr<utoipa::openapi::Schema>)>,
) -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
    let name = T::name().to_string();
    T::schemas(schemas);
    schemas.push((name.clone(), <T as utoipa::PartialSchema>::schema()));
    utoipa::openapi::Ref::from_schema_name(name).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each route's description offers exactly the fields that route takes.
    ///
    /// This is what the request types are for: `ra_column` is not a catalog's to refuse
    /// because it is not a catalog's to send, and the document says so rather than saying
    /// it in prose beside a field that is there. The lists below are the endpoints' own
    /// contracts, so a field added to one type and meant for another fails here.
    #[test]
    fn each_route_describes_its_own_body() {
        let document = serde_json::to_value(describe("/api/v1", None)).unwrap();
        let fields = |path: &str| {
            let reference = document["paths"][path]["post"]["requestBody"]["content"]
                ["application/json"]["schema"]["$ref"]
                .as_str()
                .unwrap_or_else(|| panic!("{path} has no request body"))
                .rsplit('/')
                .next()
                .unwrap()
                .to_owned();
            document["components"]["schemas"][&reference]["properties"]
                .as_object()
                .unwrap_or_else(|| panic!("{reference} is not an object"))
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        };
        // Compared in order rather than as a set: the order is what a reader meets the fields
        // in, so this is what holds it. What to read, how to reach it, where on the sky, what
        // to ask of it, and last how the answer comes back.
        let common = [
            "url",
            "storage",
            "region",
            "columns",
            "filters",
            "format",
            "dsv_null_value",
            "limit",
        ];
        let file = [
            &["url", "storage", "region"][..],
            &["ra_column", "dec_column", "healpix_column", "healpix_order"],
            &["columns", "filters", "format", "dsv_null_value", "limit", "streaming"],
        ]
        .concat();
        let plan = [common.as_slice(), &["return_storage"]].concat();
        assert_eq!(fields("/api/v1/simple/parquet"), file);
        assert_eq!(fields("/api/v1/simple/hats"), common);
        assert_eq!(fields("/api/v1/simple/hats/plan"), plan);
        // And a refusal names them in the same order the description lists them.
        assert_eq!(ParquetQuery::fields(), file);
        assert_eq!(CatalogQuery::fields(), common);
        assert_eq!(CatalogPlanQuery::fields(), plan);
    }

    /// Who runs the deployment, in the document and on the page it renders. A `name` and
    /// never an `email` or a `url`: the operator writes one free string, and either typed
    /// field would be this code guessing which of the two it was.
    #[test]
    fn the_description_says_who_runs_the_deployment() {
        let described = describe("/api/v1", Some("ops@example.org"));
        let document = serde_json::to_value(&described).unwrap();
        assert_eq!(document["info"]["contact"]["name"], "ops@example.org");
        assert!(document["info"]["contact"]["email"].is_null());
        assert!(document["info"]["contact"]["url"].is_null());
        assert!(openapi::page(&described, "/api/v1/openapi.json").contains("ops@example.org"));

        // And a deployment that named nobody carries no contact at all, rather than an
        // empty one a client generator would render as a blank line.
        let anonymous = serde_json::to_value(describe("/api/v1", None)).unwrap();
        assert!(anonymous["info"]["contact"].is_null());
    }

    /// Every `$ref` in the description names a component the description carries.
    ///
    /// The page and any generated client both resolve these, and a dangling one is a
    /// component that was flattened into its users and removed while something still
    /// pointed at it — which reads as a body with no fields rather than as an error.
    #[test]
    fn the_description_refers_to_nothing_it_does_not_carry() {
        let document = serde_json::to_value(describe("/api/v1", None)).unwrap();
        let carried = document["components"]["schemas"].as_object().unwrap();
        let mut missing = Vec::new();
        let mut stack = vec![&document];
        while let Some(value) = stack.pop() {
            match value {
                serde_json::Value::Object(object) => {
                    if let Some(serde_json::Value::String(reference)) = object.get("$ref") {
                        let name = reference.rsplit('/').next().unwrap();
                        if !carried.contains_key(name) {
                            missing.push(name.to_owned());
                        }
                    }
                    stack.extend(object.values());
                }
                serde_json::Value::Array(items) => stack.extend(items),
                _ => {}
            }
        }
        assert!(missing.is_empty(), "dangling: {missing:?}");
    }
}
