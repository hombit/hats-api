use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    Router,
    extract::{Request, State, rejection::JsonRejection},
    http::{Method, StatusCode, header, request::Parts},
    response::{Html, IntoResponse, Json, Response},
    routing::{get, post},
};
use serde::de::{DeserializeOwned, IgnoredAny};
use serde::{Deserialize, Serialize};
use tower_http::compression::CompressionLayer;
use tower_http::compression::predicate::{
    And, DefaultPredicate, NotForContentType, Predicate as _,
};
use tower_http::services::ServeFile;
use tower_http::trace::TraceLayer;
// The query-string reading of percent encoding, which is not the path's: `+` is a space
// here and is a literal `+` in a path segment.
use url::{Url, form_urlencoded};

use crate::access::{self, AccessPolicy};
use crate::config::{ApiConfig, ConfigError, DataConfig, LimitsConfig, ServerConfig};
use crate::data::DataFiles;
use crate::error::ApiError;
use crate::hats;
use crate::hats_query::{CatalogLimits, CatalogSelection, Exceeded, Outcome, Search};
use crate::healpix::Cover;
use crate::listing::{self, Listing};
use crate::materialize::Transfers;
use crate::mount::{self, Mount, Mounts};
use crate::openapi;
use crate::parquet_out;
use crate::query::{self, Order, Predicate, Projection, QueryResult, Selection};
use crate::region::{self, Healpix, Region, Spatial};
use crate::sql;
use crate::storage::{self, RemoteFile, SourceUrl, StorageOptions, parse_url};
use crate::votable;

/// What every request needs and no request may change: the rules, the shared scratch
/// budget, and the url space each mode claims. All built once at startup, so a request
/// carries a handle rather than a copy and two requests cannot disagree about any of it.
#[derive(Debug, Clone)]
pub struct Service {
    pub policy: Arc<AccessPolicy>,
    pub transfers: Arc<Transfers>,
    pub mounts: Arc<Mounts>,
    /// Which files are read as data where no mount governs the question, which in API
    /// mode is every remote url. A mount answers it with [`Mount::data_files`] instead.
    pub data_files: Arc<DataFiles>,
    /// How much SQL one request may carry.
    pub sql_limits: sql::Limits,
    /// What a request against a whole catalog may spend.
    pub catalog_limits: CatalogLimits,
    /// The widest circle a query string may ask for. The file-server mode's bound alone: a
    /// url is followed rather than fanned out, so what it asks for has to fit in one answer.
    max_query_radius_arcsec: f64,
    /// Whether a generated listing says which software and version produced it.
    show_version: bool,
    /// The subtree the API answers under, normalized; `None` when API mode is off.
    api_prefix: Option<Arc<str>>,
}

impl Service {
    /// Fails when the two modes do not divide the url space between them, which is a
    /// question about the configuration as a whole rather than about either half of it.
    pub fn new(
        policy: AccessPolicy,
        limits: &LimitsConfig,
        mounts: Arc<Mounts>,
        api: &ApiConfig,
        data: &DataConfig,
        server: &ServerConfig,
    ) -> Result<Self, ConfigError> {
        let data_files = DataFiles::new(&data.filenames)?;
        let api_prefix = match api.enabled {
            true => Some(mount::normalize_prefix(&api.prefix).map_err(|reason| {
                ConfigError::Route(format!("api.prefix {:?}: {reason}", api.prefix))
            })?),
            false => None,
        };
        if let Some(prefix) = &api_prefix {
            // Every mount, served or not: a mount's `path` is its address in both modes,
            // so one buried under the API's routes is unreachable either way.
            //
            // The other way round is the expected arrangement — a mount at `/` with the
            // API inside it — and needs no rule: a route wins over the fallback that
            // reaches the mounts.
            if let Some(mount) = mounts
                .iter()
                .find(|mount| mount::within(prefix, mount.prefix()).is_some())
            {
                return Err(ConfigError::Route(format!(
                    "the mount at {:?} is inside the API's own subtree {prefix:?}, so \
                     nothing would ever reach it",
                    mount.prefix()
                )));
            }
        } else if mounts.serves_nothing() {
            // A mount the file server does not publish is not something the file server
            // can serve, so a config of nothing but those has the same hole in it as a
            // config with no mounts at all.
            return Err(ConfigError::Route(
                "api.enabled is false and no [[mount]] sets serve, so there would be \
                 nothing to serve"
                    .to_owned(),
            ));
        }
        Ok(Self {
            policy: Arc::new(policy),
            transfers: Arc::new(Transfers::new(limits)),
            mounts,
            data_files: Arc::new(data_files),
            sql_limits: limits.into(),
            catalog_limits: limits.into(),
            max_query_radius_arcsec: limits.max_query_radius_arcsec,
            show_version: server.show_version,
            api_prefix: api_prefix.map(Arc::from),
        })
    }

    /// A path under the API's own subtree belongs to API mode whether or not a route
    /// matched it, so a mistyped API path cannot fall through to a mount at `/`.
    fn is_api_path(&self, path: &str) -> bool {
        self.api_prefix
            .as_ref()
            .is_some_and(|prefix| mount::within(prefix, path).is_some())
    }

    /// Which files a url's own location reads as data. A `file://` url is addressed in
    /// the mounts' url space, so the mount it names answers for it; everything else is
    /// remote and has only `[data] filenames` to go on.
    ///
    /// Asked before the store is built, so it is a question about the url rather than
    /// about the file — a url under no mount gets the default list and is refused a
    /// moment later by the policy, which is where saying so belongs.
    fn data_files_for(&self, url: &Url) -> &DataFiles {
        if url.scheme() != access::LOCAL_SCHEME {
            return &self.data_files;
        }
        self.mounts
            .resolve(url.path())
            .map_or(&self.data_files, |(mount, _)| mount.data_files())
    }
}

pub fn router(service: Service) -> Router {
    let mut router = Router::new();
    if let Some(prefix) = service.api_prefix.clone() {
        router = router.route(&route(&prefix, "health"), get(health));
        // Two axes, and each is a segment of its own: the vocabulary the body is written in,
        // and what the url names. The spatial constraint is in neither — it is one clause of
        // a query, so a `{target}/{predicate}` path set would grow as the product of the
        // predicate kinds rather than their sum.
        //
        // The vocabulary is a path segment rather than a pair of fields the body may or may
        // not carry, so that "which of these did the caller mean" is answered by which route
        // they sent it to. What that costs is a route per target per vocabulary; what it
        // buys is that adding one is an implementation and three lines here, rather than a
        // wider body and another pairwise refusal everywhere the fields are read.
        router = with_dialect::<Expr>(router, &prefix);
        router = with_dialect::<Simple>(router, &prefix);
        router = with_description(router, &prefix);
    }
    router
        // Mounts claim whatever the API's routes did not, so a mount at `/` and the API
        // at `/api/v1` divide the url space without either being nested in the other.
        .fallback(serve_mounted)
        .with_state(service)
        .layer(compression())
        // Method and path only. The default span carries the whole URI, including a
        // query string this service does not read but a caller may still have put
        // something in.
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &Request| {
                tracing::debug_span!(
                    "request",
                    method = %request.method(),
                    path = request.uri().path()
                )
            }),
        )
}

/// Compress what is worth compressing, which is everything this service answers with
/// except parquet.
///
/// A JSON answer is repetitive by construction — the same keys on every row — and a
/// generated directory page inlines its whole stylesheet and script before it lists an
/// entry. Both go over the wire many times smaller for a few hundred microseconds of CPU.
///
/// A parquet body is the exception, and it has to be named: it carries per-column
/// compression of its own, so a second pass over it spends CPU at both ends to save a
/// percent or two — on the largest answers produced here. [`DefaultPredicate`] excludes
/// gRPC, images and `text/event-stream` and knows nothing about parquet. The rule is written
/// against the response's own content type rather than against a route, which is what makes
/// one line cover both a parquet file served off a mount and one encoded from a query.
///
/// A partial response needs no rule of its own: the layer leaves anything carrying a
/// `Content-Range` alone, so a ranged read of a mounted file comes back as the bytes that
/// were asked for whatever its type is.
///
/// Three encodings are offered, and the client's `Accept-Encoding` picks among them: gzip,
/// which every client already asks for, and brotli and zstd, which browsers prefer and
/// which compress a page or a row-heavy answer measurably smaller. The two extra ones are
/// enabled because they are close to free here — `brotli` and `zstd` are already linked in,
/// being what parquet and arrow-ipc read their own compressed blocks with, so turning on
/// the feature adds no dependency and only the encoder wrappers to the binary. Deflate is
/// left off: nothing asks for it that does not also ask for gzip.
///
/// Two consequences worth knowing. `Content-Length` goes where a body is compressed, the
/// body becoming chunked — a client that sized a buffer from the header cannot any more,
/// while the `x-hats-*` counters say exactly what they said before. And a compressed body
/// that mixes a secret with attacker-chosen text is the shape BREACH exploits: the one such
/// body here is a plan answered with `return_storage`, which echoes the caller's own storage
/// options. What makes it not that attack is that the secret is the caller's own, returned
/// on their own `POST`, over a route no third-party page can make a browser send with those
/// options attached.
fn compression() -> CompressionLayer<And<DefaultPredicate, NotForContentType>> {
    CompressionLayer::new().compress_when(
        DefaultPredicate::new().and(NotForContentType::const_new(PARQUET_CONTENT_TYPE)),
    )
}

/// One vocabulary's three routes.
///
/// They are registered together because they are the same request against three targets, and
/// a vocabulary that answered on only some of them would be one a caller has to remember the
/// exceptions to.
///
/// `POST`, not `GET`: the request carries credentials, and a query string is written to
/// every proxy's access log and the caller's shell history on the way. A body also has no
/// url-length limit — a long `IN` list and a wide select list both run past nginx's 8 KB
/// header buffer — and needs no url nested inside a url.
fn with_dialect<D: Dialect>(router: Router<Service>, prefix: &str) -> Router<Service> {
    let path = |target| route(prefix, &format!("{}/{target}", D::SEGMENT));
    router
        .route(&path("parquet"), post(query_parquet::<D>))
        // The same body, against a catalog instead of a file: the url names a HATS
        // directory and this chooses the partitions to read out of it.
        .route(&path("hats"), post(query_hats::<D>))
        // The same body again, resolved and not run. Two routes rather than one with a
        // mode: rows and a work list are different kinds of thing, and a field saying
        // which arrived is one more value a caller has to look at the body to trust.
        .route(&path("hats/plan"), post(query_hats_plan::<D>))
}

/// The two routes that describe the rest: the document, and a page rendering it.
///
/// The document is built per request rather than once, because it names the prefix and the
/// prefix is the operator's. It is a few hundred microseconds of `serde_json` on a route
/// nothing calls in a loop.
fn with_description(router: Router<Service>, prefix: &str) -> Router<Service> {
    let document = route(prefix, "openapi.json");
    let page = openapi::page(&describe(prefix), &document);
    router
        .route(
            &document,
            get({
                let prefix = prefix.to_owned();
                move || {
                    let prefix = prefix.clone();
                    async move { Json(describe(&prefix)) }
                }
            }),
        )
        .route(
            &route(prefix, "docs"),
            get(move || {
                let page = page.clone();
                async move { Html(page) }
            }),
        )
}

/// The whole document: the health route, then every vocabulary's own.
fn describe(prefix: &str) -> utoipa::openapi::OpenApi {
    let mut paths = utoipa::openapi::Paths::new();
    let mut schemas = Vec::new();
    openapi::health(&mut paths, &route(prefix, "health"));
    describe_dialect::<Expr>(&mut paths, &mut schemas, prefix);
    describe_dialect::<Simple>(&mut paths, &mut schemas, prefix);
    let components = utoipa::openapi::ComponentsBuilder::new()
        .schemas_from_iter(schemas)
        .build();
    let mut document = openapi::document(paths, components);
    // Last, because it is the flattened body that is ordered: the vocabulary's fields are not
    // in the body's own schema until `document` has folded them in.
    order_bodies::<Expr>(&mut document);
    order_bodies::<Simple>(&mut document);
    document
}

/// Each request body's fields in the order its own endpoint says a body is written in.
fn order_bodies<D: Dialect>(document: &mut utoipa::openapi::OpenApi) {
    openapi::order_fields(
        document,
        &component_of::<D>("ParquetQuery"),
        &ParquetQuery::<D>::fields(),
    );
    openapi::order_fields(
        document,
        &component_of::<D>("CatalogQuery"),
        &CatalogQuery::<D>::fields(),
    );
    openapi::order_fields(
        document,
        &component_of::<D>("CatalogPlanQuery"),
        &CatalogPlanQuery::<D>::fields(),
    );
}

/// What a generic component is called in the document.
///
/// Every named schema is registered and referred to by `$ref`, including the generic ones: the
/// derive composes the argument into the name, so `ParquetQuery<Expr>` and
/// `ParquetQuery<Simple>` are two components rather than one that is wrong for one route.
/// Spelled the way utoipa spells a generic it composes itself, so the two kinds of name in the
/// document read alike.
fn component_of<D: utoipa::ToSchema>(base: &str) -> String {
    format!("{base}_{}", <D as utoipa::ToSchema>::name())
}

/// One vocabulary's three operations, alongside [`with_dialect`]'s three routes.
///
/// Generic over the same trait for the same reason: a vocabulary that was served and not
/// described, or described and not served, would be a difference nobody notices until a
/// caller does.
fn describe_dialect<D: Dialect>(
    paths: &mut utoipa::openapi::Paths,
    schemas: &mut Vec<(String, utoipa::openapi::RefOr<utoipa::openapi::Schema>)>,
    prefix: &str,
) {
    let of = component_of::<D>;
    // The vocabulary itself, which nothing else registers: it reaches the body through a
    // `flatten` on a generic parameter, and walking a type's references does not cross one.
    // Left out, every request body `$ref`s a component that is not in the document.
    named::<D>(schemas, <D as utoipa::ToSchema>::name().to_string());
    // One body per endpoint rather than one for all three, which is what makes the description
    // say of each route exactly what that route takes.
    let file_body = named::<ParquetQuery<D>>(schemas, of("ParquetQuery"));
    let catalog_body = named::<CatalogQuery<D>>(schemas, of("CatalogQuery"));
    let plan_body = named::<CatalogPlanQuery<D>>(schemas, of("CatalogPlanQuery"));
    let plan = named::<PlanResponse<D>>(schemas, of("PlanResponse"));
    let rows = named::<SelectResponse>(
        schemas,
        <SelectResponse as utoipa::ToSchema>::name().to_string(),
    );
    let catalog_rows = named::<HatsResponse>(
        schemas,
        <HatsResponse as utoipa::ToSchema>::name().to_string(),
    );
    let path = |target: &str| route(prefix, &format!("{}/{target}", D::SEGMENT));

    openapi::post(
        paths,
        &path("parquet"),
        openapi::operation(
            D::SEGMENT,
            "Query one parquet file",
            D::SUMMARY,
            file_body,
            example::<D>(EXAMPLE_PARTITION, PARTITION_SELECT, PARTITION_WHERE, false),
            "The rows, or a parquet file or a VOTable where `format` asked for one",
            rows,
        ),
    );
    openapi::post(
        paths,
        &path("hats"),
        openapi::operation(
            D::SEGMENT,
            "Query a HATS catalog",
            D::SUMMARY,
            catalog_body,
            example::<D>(EXAMPLE_CATALOG, CATALOG_SELECT, CATALOG_WHERE, true),
            "The rows, in the catalog's own order, with the partitions they came from",
            catalog_rows,
        ),
    );
    openapi::post(
        paths,
        &path("hats/plan"),
        openapi::operation(
            D::SEGMENT,
            "Resolve a catalog query without running it",
            D::SUMMARY,
            plan_body,
            example::<D>(EXAMPLE_CATALOG, CATALOG_SELECT, CATALOG_WHERE, true),
            "One request per partition, for the client to send itself",
            plan,
        ),
    );
}

/// Gaia DR3, which a reader can send the catalog examples at as written: a real collection,
/// published anonymously, all-sky, and with partitions even enough that a reader who moves the
/// circle gets the same answer in the same time.
const EXAMPLE_CATALOG: &str = "s3://stpubdata/gaia/gaia_dr3/public/hats";
const CATALOG_SELECT: &str = "source_id, ra, dec, phot_g_mean_mag";
const CATALOG_WHERE: &str = "parallax > 1";

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
const PARTITION_SELECT: &str = "objectid, objra, objdec, lightcurve.mag";
const PARTITION_WHERE: &str = "nepochs > 10";

/// A body the route it is shown on will actually answer, in about a second: the url, this
/// vocabulary's own two fields, a limit, and for a catalog a circle.
///
/// Everything is a parameter because the two targets name different files: a catalog gets the
/// circle, since without one a catalog query reads every partition — against a real catalog
/// that is minutes, and a reader pressing the button would conclude the service was broken —
/// and the single file gets the columns of the one catalog here with anything nested in it.
///
/// A `limit` alone would not stand in for the circle: it stops the read once enough rows are
/// found, and a predicate that most partitions fail keeps it reading.
fn example<D: Dialect>(
    url: &str,
    projection: &str,
    predicate: &str,
    region: bool,
) -> serde_json::Value {
    let mut body = serde_json::Map::new();
    body.insert("url".to_owned(), serde_json::Value::String(url.to_owned()));
    if let serde_json::Value::Object(query) = D::example(projection, predicate) {
        body.extend(query);
    }
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

/// Register a type's schema under `name`, and everything it refers to, and hand back the
/// reference to it.
///
/// A component and a `$ref` rather than the schema written into each operation: the same body
/// is three routes' here, and a reader comparing two vocabularies wants to see one name twice.
///
/// The name is a parameter because a generic type has two of them. `ToSchema::schemas` composes
/// the argument in — `PlanBody_Expr` — while `ToSchema::name` drops it and answers `PlanBody`
/// for every instantiation. Registering a generic under the latter puts both dialects' schemas
/// at one key, where the second silently replaces the first and every route ends up describing
/// whichever was built last.
fn named<T: utoipa::ToSchema>(
    schemas: &mut Vec<(String, utoipa::openapi::RefOr<utoipa::openapi::Schema>)>,
    name: String,
) -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
    T::schemas(schemas);
    schemas.push((name.clone(), <T as utoipa::PartialSchema>::schema()));
    utoipa::openapi::Ref::from_schema_name(name).into()
}

/// The health response's schema, for the route that has no body to derive one from.
pub fn health_schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
    <HealthResponse as utoipa::PartialSchema>::schema()
}

/// One route under a prefix. The root prefix already ends in the separator, so joining
/// it the same way as any other would give `//health`.
fn route(prefix: &str, name: &str) -> String {
    match prefix {
        "/" => format!("/{name}"),
        _ => format!("{prefix}/{name}"),
    }
}

/// What a parquet file is served as, whether it was read off a mount or encoded from a
/// query. `mime_guess` has no answer for the extension.
const PARQUET_CONTENT_TYPE: &str = "application/vnd.apache.parquet";

/// The file a directory is served as when it has one, in place of a generated listing.
const DIRECTORY_INDEX: &str = "index.html";

/// The file-server side: a url path, a mount, and the file inside it.
///
/// Everything the request may decide is decided here; everything about *how* an ordinary
/// file is served over HTTP — byte ranges, `ETag` and `Last-Modified`, the conditional
/// requests, `HEAD` — is [`ServeFile`]'s, against a path this function has already
/// resolved. A client reading one partition out of a mount is doing so with ranged
/// requests, so this is not an optional part of being a file server.
async fn serve_mounted(
    State(service): State<Service>,
    request: Request,
) -> Result<Response, ApiError> {
    // The head on its own, so that a listing can be built from it while a `Body` — which
    // is not `Sync`, and would make this future unable to cross a thread — is set aside.
    let (parts, body) = request.into_parts();
    let path = parts.uri.path().to_owned();
    if service.is_api_path(&path) {
        return Err(ApiError::not_found(format!("{path} is not a route")));
    }
    // `published`, not `resolve`: a mount that did not opt in claims no url space, and a
    // request for one of its paths is a request for a route that is not there.
    let Some((mount, relative)) = service.mounts.published(&path) else {
        return Err(ApiError::not_found(format!("{path} is not a route")));
    };
    let segments = mount::path_segments(relative)?;
    let radius = service.max_query_radius_arcsec;
    let mut requested = mount.source().to_owned();
    requested.extend(&segments);
    let mut file = access::authorize_mounted(mount, &requested)?;
    if file.is_dir() {
        // A catalog is the one directory that answers a question about itself, and the
        // question comes before the page: a directory with an `index.html` still has
        // partitions to search, and serving the page instead would drop the query.
        //
        // Any parameter this service reads makes it a question — the circle narrows the
        // answer rather than being what makes one possible, the way `columns` and `limit`
        // are against a file. Which directory this is decides whether there is a query
        // surface at all, so a directory that is not a catalog is listed with its query
        // string ignored, and one that is answers or refuses but never drops it.
        if hats::local::describes_a_catalog(&file)
            && let Some(query) = FileQuery::parse(parts.uri.query().unwrap_or_default(), radius)?
        {
            return query_catalog_mounted(&service, mount, &file, &query, &parts).await;
        }
        // A directory that publishes its own page says what it wants said about itself,
        // and the generated listing is only the fallback. Through `authorize_mounted`
        // like any other file, so a link the mount does not follow is not followed here
        // either.
        match access::authorize_mounted(mount, &file.join(DIRECTORY_INDEX)) {
            Ok(index) if index.is_file() => file = index,
            _ => return list_directory(&service, mount, &segments, &file, &parts).await,
        }
    }
    // A query string turns a data file into a question about itself. Anything else keeps
    // going out verbatim, parameters and all: a file server that has no use for a
    // parameter ignores it, and `index.html?v=3` is a request for `index.html`.
    if mount.data_files().matches_path(&file)
        && let Some(query) = FileQuery::parse(parts.uri.query().unwrap_or_default(), radius)?
    {
        return query_mounted(&service, &file, &query, &parts).await;
    }
    let mut response = ServeFile::new(&file)
        .try_call(Request::from_parts(parts, body))
        .await
        .map_err(|error| {
            tracing::warn!(%error, "serving a mounted file failed");
            ApiError::internal("cannot read this file")
        })?
        .into_response();
    // mime_guess has no answer for `.parquet`, and the clients that read these files
    // look at the content type.
    if mount.data_files().matches_path(&file) {
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static(PARQUET_CONTENT_TYPE),
        );
    }
    Ok(response)
}

/// The question a file-server request asks about a file, if it asks one.
///
/// The names are vizcat's, so a client written against that service reads a mount here
/// without changing anything but the host. What they mean is this service's own: a
/// `filters` that does not parse, or that names a column the file does not have, is
/// refused rather than dropped — a request whose predicate went missing returns every
/// row, and the caller cannot tell that from a predicate that matched them all.
///
/// `format` and `limit` have no vizcat equivalent and so take names of our own, which is
/// what keeps a name from meaning two things depending on which service answered.
///
/// The circle is the API's `region` flattened into a query string — the one shape a url can
/// carry, since it is four numbers rather than a structure. The rest of the shapes stay with
/// the API: a `box` is two ordered pairs and a `moc` is a document, and neither reads as a
/// parameter. What a url gains in return is that it can be linked to, pasted and handed to a
/// reader that takes one, which is what the file-server mode is for.
#[derive(Debug, Default)]
struct FileQuery {
    /// The same pair the `simple` route takes, in the text form a url can carry: one
    /// `columns=` holding names separated by commas, one `filters=` holding conditions
    /// joined by `&&`, `,` or `;`. A body writes each as a list and needs no separator,
    /// which is the only difference between the two — both lower to the same expression.
    columns: Option<String>,
    filters: Option<String>,
    format: Option<String>,
    limit: Option<String>,
    /// The circle, built and checked at parse time so that everything downstream can borrow
    /// it — a [`Selection`] holds the shapes rather than owning them. One element: a query
    /// string names one centre, and the union the API's array expresses needs a body.
    region: Option<Vec<Region>>,
    /// Which columns hold the position, for a file. Refused against a catalog, which names
    /// its own.
    ra_column: Option<String>,
    dec_column: Option<String>,
}

impl FileQuery {
    /// `None` when the query string asks nothing this service answers, which is what
    /// keeps a file with a cache-buster on its url an ordinary download.
    ///
    /// Anything unrecognised is ignored rather than refused, the way an ordinary HTTP
    /// server ignores what it has no use for. The last of a repeated parameter wins,
    /// which is what a browser and a form both produce.
    ///
    /// `max_radius_arcsec` is the operator's ceiling on the circle. It is applied here, on
    /// the request's own numbers, rather than left to the bounds that watch what a read
    /// costs: those answer a fan-out, and a url has no way to express one.
    fn parse(raw: &str, max_radius_arcsec: f64) -> Result<Option<Self>, ApiError> {
        let mut query = Self::default();
        let mut circle = Circle::default();
        let mut asked = false;
        for (name, value) in form_urlencoded::parse(raw.as_bytes()) {
            let field = match name.as_ref() {
                "columns" => &mut query.columns,
                "filters" => &mut query.filters,
                "format" => &mut query.format,
                "limit" => &mut query.limit,
                "ra_column" => &mut query.ra_column,
                "dec_column" => &mut query.dec_column,
                "ra" => &mut circle.ra,
                "dec" => &mut circle.dec,
                "radius_deg" => &mut circle.radius_deg,
                "radius_arcsec" => &mut circle.radius_arcsec,
                _ => continue,
            };
            *field = Some(value.into_owned());
            asked = true;
        }
        if !asked {
            return Ok(None);
        }
        query.region = circle.region(max_radius_arcsec)?;
        Ok(Some(query))
    }

    /// The projection, the predicate and the limit, which every route reads the same way.
    fn common(&self) -> Result<(Projection<'_>, Predicate<'_>, Option<usize>), ApiError> {
        Ok((
            match self.columns.as_deref() {
                Some(list) => Projection::ColumnText(list),
                None => Projection::All,
            },
            match self.filters.as_deref() {
                Some(text) => Predicate::FilterText(text),
                None => Predicate::All,
            },
            match self.limit.as_deref() {
                // Said as a number rather than left to mean "no limit": a caller who
                // wrote one and got every row would have no way to notice.
                Some(raw) => Some(raw.parse().map_err(|_| {
                    ApiError::bad_request("limit takes a number of rows".to_owned())
                })?),
                None => None,
            },
        ))
    }

    /// What to read from one file of a mount.
    ///
    /// A file says nothing about which of its columns are a position, so a circle here needs
    /// both column names — the same rule, and the same message, as the API's single-file
    /// route. A request that names a partition itself and asks no spatial question is the
    /// ordinary case and unchanged.
    fn selection(&self) -> Result<Selection<'_>, ApiError> {
        let (projection, predicate, limit) = self.common()?;
        let spatial = match &self.region {
            None => {
                if self.ra_column.is_some() || self.dec_column.is_some() {
                    return Err(ApiError::bad_request(NEEDS_A_CIRCLE));
                }
                None
            }
            Some(regions) => {
                if self.ra_column.is_none() || self.dec_column.is_none() {
                    return Err(ApiError::bad_request(
                        "a circle over a file needs ra_column and dec_column; a catalog's \
                         own url names them for you",
                    ));
                }
                Some(Spatial {
                    regions,
                    ra_column: self.ra_column.as_deref(),
                    dec_column: self.dec_column.as_deref(),
                    // The `_healpix_29` a HATS partition carries is found in the file's own
                    // schema, so the accelerator costs a url nothing. An index column under
                    // any other name has to be named along with its order, which is a pair
                    // this vocabulary does not carry — the API's route takes it.
                    healpix: None,
                    // A url naming one file names no catalog above it.
                    partition: None,
                })
            }
        };
        Ok(Selection {
            projection,
            predicate,
            spatial,
            limit,
        })
    }

    /// What to read from a catalog, whose own properties answer for the columns.
    fn catalog_selection(&self) -> Result<CatalogSelection<'_>, ApiError> {
        let (projection, predicate, limit) = self.common()?;
        // Refused rather than ignored, for the reason the API's catalog route refuses them:
        // a dropped one returns rows tested against columns the caller did not write, which
        // they cannot tell from the ones they asked for.
        let named = [
            ("ra_column", self.ra_column.is_some()),
            ("dec_column", self.dec_column.is_some()),
        ]
        .into_iter()
        .filter_map(|(name, given)| given.then_some(name))
        .collect::<Vec<_>>();
        if !named.is_empty() {
            return Err(ApiError::bad_request(format!(
                "{} not accepted against a catalog, which names its own columns; to choose \
                 them, query one of its files",
                named.join(", ")
            )));
        }
        Ok(CatalogSelection {
            projection,
            predicate,
            regions: self.region.as_deref(),
            limit,
        })
    }
}

/// A column named with no circle to test with it. It accelerates nothing and constrains
/// nothing, so a request carrying one is a caller who believes otherwise.
const NEEDS_A_CIRCLE: &str = "ra_column and dec_column need ra, dec and a radius";

/// The circle as it arrives: four parameters, each of them text.
#[derive(Debug, Default)]
struct Circle {
    ra: Option<String>,
    dec: Option<String>,
    radius_deg: Option<String>,
    radius_arcsec: Option<String>,
}

impl Circle {
    /// The shape these four spell, or `None` where none of them was written.
    ///
    /// Validated through [`Region::shape`] rather than field by field, so a circle means the
    /// same thing whether it arrived in a url or in a body — including which radius spellings
    /// are legal and what a radius may be.
    fn region(&self, max_radius_arcsec: f64) -> Result<Option<Vec<Region>>, ApiError> {
        let given = [&self.ra, &self.dec, &self.radius_deg, &self.radius_arcsec];
        if given.iter().all(|value| value.is_none()) {
            return Ok(None);
        }
        let region = Region::Circle {
            ra: number("ra", self.ra.as_deref())?,
            dec: number("dec", self.dec.as_deref())?,
            radius_deg: optional_number("radius_deg", self.radius_deg.as_deref())?,
            radius_arcsec: optional_number("radius_arcsec", self.radius_arcsec.as_deref())?,
        };
        // Which also settles the radius in degrees, whichever spelling carried it.
        let region::Shape::Circle { radius, .. } = region.shape()? else {
            return Err(ApiError::internal("a circle did not resolve to a circle"));
        };
        let asked = radius * 3600.0;
        if asked > max_radius_arcsec {
            return Err(ApiError::bad_request(format!(
                "radius {asked}\u{2033} is over the {max_radius_arcsec}\u{2033} this url \
                 answers; the API's catalog route takes a larger one, and its plan route \
                 answers one too large to run"
            )));
        }
        Ok(Some(vec![region]))
    }
}

/// One parameter of the circle, which the other three make required.
fn number(name: &str, raw: Option<&str>) -> Result<f64, ApiError> {
    let raw = raw.ok_or_else(|| {
        ApiError::bad_request(format!(
            "a circle takes ra, dec and one radius; {name} is missing"
        ))
    })?;
    optional_number(name, Some(raw))?.ok_or_else(|| ApiError::internal("a number went missing"))
}

fn optional_number(name: &str, raw: Option<&str>) -> Result<Option<f64>, ApiError> {
    raw.map(|raw| {
        raw.trim()
            .parse()
            .map_err(|_| ApiError::bad_request(format!("{name} takes a number")))
    })
    .transpose()
}

/// A parquet file under a mount, asked for less of itself.
///
/// The file was authorized by the mount before it got here, and the caller named no
/// store and supplied no credential — that is the whole difference from the API mode,
/// which is why this reads through [`storage::open_mounted`] rather than through the
/// url-judging path.
async fn query_mounted(
    service: &Service,
    file: &Path,
    query: &FileQuery,
    request: &Parts,
) -> Result<Response, ApiError> {
    if !matches!(request.method, Method::GET | Method::HEAD) {
        return Err(ApiError::method_not_allowed("a query is read, not written"));
    }
    let started = Instant::now();
    let format = Format::parse(query.format.as_deref(), Format::Parquet)?;
    let selection = query.selection()?;
    let opened = storage::open_mounted(file)?;
    // Through `from_mount`, both of them: a store's own message about a local file names
    // the path it was reading, and that path is the operator's.
    // The rows come back in the file's own order. The request named a file and asked for
    // less of it, so the answer describes that file, and a client that reads a partition
    // twice gets the same rows in the same places both times.
    let result = query::run(&opened, &selection, service.sql_limits, Order::File)
        .await
        .map_err(|error| error.from_mount(file))?;

    let num_rows = result.num_rows();
    let data_bytes_read = result.data_bytes_read;
    let response = answer(&result, &opened, format, started)
        .await
        .map_err(|error| error.from_mount(file))?;
    tracing::info!(
        // The url path, not the local path: what is on disk is the operator's business.
        // Both parameters are the caller's own text and can be megabytes of `IN` list,
        // so what is logged is that they were there.
        path = request.uri.path(),
        projected = query.columns.is_some(),
        filtered = query.filters.is_some(),
        format = format.name(),
        num_rows,
        // What the pruning was worth, next to the time it took. Free to record and the
        // one number that says whether a slow request was slow because it read the file.
        data_bytes_read,
        elapsed_ms = started.elapsed().as_millis(),
        "query"
    );
    Ok(response)
}

/// A catalog under a mount, asked for the rows inside a circle.
///
/// The same work [`query_hats`] does, reached by a url rather than by a body: the catalog
/// chooses its partitions from the region, names its own position columns, and reads them
/// several at a time. What a url cannot carry is a fan-out — so a request too large for one
/// answer is refused here rather than answered with a work list, and the message says which
/// route hands one back.
///
/// **The circle is optional, and a `limit` is what makes it so.** Without a region the
/// request is the whole catalog in its own order, which a `limit` turns into the front of it
/// — read partition by partition until there are enough rows, which for the first ten is the
/// first partition. Without either, the whole catalog is what it says, and the partition
/// bound refuses it before anything is read.
async fn query_catalog_mounted(
    service: &Service,
    mount: &Mount,
    dir: &Path,
    query: &FileQuery,
    request: &Parts,
) -> Result<Response, ApiError> {
    if !matches!(request.method, Method::GET | Method::HEAD) {
        return Err(ApiError::method_not_allowed("a query is read, not written"));
    }
    let started = Instant::now();
    // Parquet, as it is for a file: adding a query string to a url should not change what
    // media type it answers with, and the page asks for JSON by name.
    let format = Format::parse(query.format.as_deref(), Format::Parquet)?;
    let selection = query.catalog_selection()?;
    // Every message from here down names the operator's directory, this being a local store.
    let hide_the_path = |error: ApiError| error.from_mount(dir);

    let opened = storage::open_mounted_dir(dir)?;
    let search = Search::resolve(opened, selection.regions, service.catalog_limits)
        .await
        .map_err(hide_the_path)?;
    let outcome = search
        .run(
            &selection,
            mount.data_files(),
            service.sql_limits,
            service.catalog_limits,
        )
        .await
        .map_err(hide_the_path)?;
    let result = match outcome {
        Outcome::Rows(result) => result,
        Outcome::TooMuchWork(why) => return Err(too_much_for_a_url(&why)),
    };

    let num_rows = result.rows.num_rows();
    let data_bytes_read = result.rows.data_bytes_read;
    let partitions_read = result.partitions_read;
    let response = hats_answer(&result, format, started)
        .await
        .map_err(hide_the_path)?;
    tracing::info!(
        // The url path, not the local path: what is on disk is the operator's business.
        path = request.uri.path(),
        partitions = search.catalog().partitions().len(),
        chosen = search.chosen().len(),
        partitions_read,
        projected = query.columns.is_some(),
        filtered = query.filters.is_some(),
        format = format.name(),
        num_rows,
        data_bytes_read,
        elapsed_ms = started.elapsed().as_millis(),
        "catalog query"
    );
    Ok(response)
}

/// A bound reached on a route that has no work list to hand back.
///
/// The API's catalog route answers this with the plan, which is the useful answer and the
/// reason the bound exists. A url cannot carry one — a plan is a document — so what this can
/// do is say which bound was reached and where the request that fans out is written.
fn too_much_for_a_url(why: &Exceeded) -> ApiError {
    ApiError::too_much_work(format!(
        "{why}; add a limit or a radius, or use the API's plan route, which lists the \
         requests this takes"
    ))
}

/// A directory, as a page or as JSON. Which one is [`listing::wants_html`]'s decision.
async fn list_directory(
    service: &Service,
    mount: &Mount,
    segments: &[String],
    dir: &Path,
    request: &Parts,
) -> Result<Response, ApiError> {
    if !matches!(request.method, Method::GET | Method::HEAD) {
        return Err(ApiError::method_not_allowed(
            "a listing is read, not written",
        ));
    }
    // The url as this service spells it, built from the decoded segments rather than
    // from the request path, so every entry's url has one spelling whatever the request
    // used to get here.
    let path = listing::url(mount.prefix(), segments);
    let root = mount.prefix().to_owned();

    let (dir, follow_symlinks) = (dir.to_owned(), mount.follow_symlinks());
    // How far the mount's root is, which is as far up as the catalog may be looked for:
    // a listing goes no higher than its mount, and neither does what it offers.
    let depth = segments.len();
    // `read_dir` and a `stat` per entry are blocking calls, and a HATS `Dir=` level is
    // ten thousand of them. The catalog probe is a handful more, on the same thread.
    let read = tokio::task::spawn_blocking(move || {
        let listing = Listing::read(&dir, &root, &path, follow_symlinks)?;
        // The catalog this directory is inside, and what it says about itself — both read
        // here rather than beside the page, `about` being another small file off the disk.
        let found = hats::local::enclosing(&dir, depth).map(|levels| {
            let at = dir.ancestors().nth(levels).unwrap_or(&dir);
            (levels, hats::local::about(at))
        });
        Ok::<_, std::io::Error>((listing, found))
    })
    .await
    .map_err(|error| {
        tracing::error!(%error, "listing a directory panicked");
        ApiError::internal("cannot read this directory")
    })?;
    let (listing, found) = read.map_err(|error| {
        // The path is the operator's business and not the caller's, so what comes back
        // is the same answer as for a directory that is not published at all.
        tracing::warn!(%error, mount = mount.prefix(), "cannot list");
        ApiError::not_found("no such directory")
    })?;
    // The catalog's own url, which is this directory's with as many levels trimmed off it as
    // the walk climbed. Built from the decoded segments the same way this directory's was, so
    // the two agree on how a name is spelled in a url.
    let (catalog, about) = match found {
        Some((levels, about)) => (
            segments
                .get(..segments.len().saturating_sub(levels))
                .map(|above| listing::url(mount.prefix(), above)),
            Some(about),
        ),
        None => (None, None),
    };
    // Where the catalog's columns are, as a url under this mount. Only where the catalog has
    // the file: one without it is answered by the page a different way rather than offered a
    // url that is a 404. The path is the catalog's to give — a collection's is inside its
    // primary table — and the encoding is `listing`'s.
    let schema = catalog
        .as_deref()
        .zip(about.as_ref())
        .and_then(|(at, about)| {
            let path = about.schema.as_deref()?;
            Some(listing::below(at, path))
        });

    Ok(match listing::wants_html(&request.headers) {
        true => Html(listing.to_html(
            mount.data_files(),
            service.api_prefix.as_deref(),
            &listing::Catalog {
                url: catalog.as_deref(),
                name: about.as_ref().and_then(|about| about.name.as_deref()),
                rows: about.as_ref().and_then(|about| about.rows),
                order: about.as_ref().and_then(|about| about.order),
                schema_url: schema.as_deref(),
                max_radius_arcsec: service.max_query_radius_arcsec,
            },
            service.show_version,
        ))
        .into_response(),
        false => Json(listing).into_response(),
    })
}

#[derive(Debug, Serialize, utoipa::ToSchema)]
struct HealthResponse {
    status: &'static str,
}

async fn health() -> (StatusCode, Json<HealthResponse>) {
    (StatusCode::OK, Json(HealthResponse { status: "ok" }))
}

/// One of the vocabularies a request may be written in, which is what the first segment of
/// the path names.
///
/// Both say the same thing and lower to the same planned expression; they differ in what a
/// field may hold. Putting them on separate routes is what lets each be a plain pair of
/// fields — a body carrying a field of both is not a request this service has to have an
/// opinion about, because no route accepts one.
///
/// A third vocabulary is a third implementation and three more route lines, rather than two
/// more fields and another pairwise refusal in every body that reads them.
// `Sync` as well as `Send`: a handler holds a reference to a request written in this
// vocabulary across an await, and a shared reference is only `Send` where what it points at
// is `Sync`.
trait Dialect:
    Debug + Clone + Serialize + DeserializeOwned + utoipa::ToSchema + Send + Sync + 'static
{
    /// The path segment this vocabulary is reached at. A plan's entries name it too: they
    /// are written in the vocabulary the request that produced them was written in.
    const SEGMENT: &'static str;

    /// One line saying what this vocabulary is, for the API description. Beside `SEGMENT`
    /// because they are the two things a route set needs to know about a vocabulary, and a
    /// third one added without either is a route nobody can find or read.
    const SUMMARY: &'static str;

    /// This vocabulary's own two fields, in the order a body is written in. Every endpoint
    /// splices them into its own field list, so a vocabulary is named in one place and the
    /// routes that carry it say the same thing about it.
    const FIELDS: &'static [&'static str];

    /// What a caller most often gets wrong about these two, said in the refusal a body that
    /// would not deserialize meets. Serde's own message names the field and then quotes the
    /// value, which is why it is not the one shown — so the type has to be said here or not
    /// at all.
    const NOTE: &'static str;

    /// A projection and a predicate written in this vocabulary's own two fields, so that the
    /// description's runner starts from a request that returns rows rather than a 400.
    ///
    /// The columns are the caller's because the two targets are different files. What stays
    /// this method's business is the spelling — which pair of field names the body carries.
    ///
    /// **A few named columns.** What a request against a real catalog costs is the columns it
    /// projects and not the rows it returns: a nested column holding every epoch of a light
    /// curve is seconds where four flat ones are under one.
    fn example(projection: &str, predicate: &str) -> serde_json::Value;

    fn projection(&self) -> Projection<'_>;

    fn predicate(&self) -> Predicate<'_>;

    /// Whether the caller narrowed the answer, which is what the log records. Read off the
    /// lowered form rather than from the fields, so a vocabulary cannot report this
    /// differently from how it is actually planned.
    fn selects(&self) -> bool {
        !matches!(self.projection(), Projection::All)
    }

    fn filtered(&self) -> bool {
        !matches!(self.predicate(), Predicate::All)
    }
}

/// Expressions: each field is one SQL expression, and nothing wider.
///
/// Named for what a field holds rather than for SQL, because a statement is refused —
/// [`crate::sql`] parses each field on its own and requires the parser to reach the end of
/// the string, so `SELECT … FROM …` is not a longer form of this that happens to be
/// rejected, it is a different thing.
// The `description`s are the caller's text and the doc comments are the next maintainer's.
// Two audiences rather than two copies: a comment here says why the design is what it is,
// which is the wrong thing to read when you are trying to write a request. What must not be
// written twice is the shape — fields, types, which are required — and that is derived.
#[derive(Debug, Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[schema(description = "A projection and a predicate written as SQL expressions.")]
struct Expr {
    /// The select list that would follow `SELECT`: names, expressions over them, and aliases.
    /// Leave it out to get every column. Write a column as the file spells it, in double quotes
    /// where the spelling needs them — `"Gmag"`. Each expression is evaluated one row at a
    /// time, so aggregates and window functions are refused.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "objectid, objra, objdec, lightcurve.mag")]
    select: Option<String>,
    /// One boolean expression over this file's columns: the condition that would follow a
    /// `WHERE` keyword. Leave it out to get every row. Write a column as the file spells it, in
    /// double quotes where the spelling needs them — `"Gmag" < 20`.
    // The raw identifier is the field name in both directions: serde reads and writes it as
    // `where`, which is what a caller sends.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "objdec > 60")]
    r#where: Option<String>,
}

impl Dialect for Expr {
    const SEGMENT: &'static str = "expr";
    const SUMMARY: &'static str = "SQL expressions: `select` is a select list, `where` one \
        boolean expression. Neither is a statement — each is parsed on its own and must \
        parse to its end.";
    const FIELDS: &'static [&'static str] = &["select", "where"];
    const NOTE: &'static str = "select and where are each one string";

    fn example(projection: &str, predicate: &str) -> serde_json::Value {
        serde_json::json!({ "select": projection, "where": predicate })
    }

    fn projection(&self) -> Projection<'_> {
        match self.select.as_deref() {
            Some(sql) => Projection::Select(sql),
            None => Projection::All,
        }
    }

    fn predicate(&self) -> Predicate<'_> {
        match self.r#where.as_deref() {
            Some(sql) => Predicate::Where(sql),
            None => Predicate::All,
        }
    }
}

/// A list of column names and one row condition — the narrower pair, and the same one a
/// url's query string carries.
///
/// The projection is a list because a body has arrays and a comma between names is a
/// separator a caller would otherwise have to quote around. The predicate stays one string:
/// it is one expression whichever way it is carried, and a list of them would be a second
/// way to write the `AND` the expression already has.
#[derive(Debug, Clone, Default, Deserialize, Serialize, utoipa::ToSchema)]
#[schema(
    description = "A projection and a predicate in the narrower forms: a list of column \
                   names, and one row condition. The same meaning as the expression \
                   vocabulary, and the same limits."
)]
struct Simple {
    /// The columns to return, one name per element, and nothing computed — for that, use the
    /// expression vocabulary. Write a name as the file spells it, in double quotes where the
    /// spelling needs them. A dotted name reaches inside a struct column, and comes back as
    /// that column carrying the fields you named. Leave the field out for every column; an
    /// empty list is refused rather than read as one or the other.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = json!(["objectid", "objra", "objdec", "lightcurve.mag"]))]
    columns: Option<Vec<String>>,
    /// The row condition: one boolean expression over this file's columns, which is the same
    /// language as the other vocabulary's `where`. Leave it out for every row.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = "objdec > 60 AND nepochs > 100")]
    filters: Option<String>,
}

impl Dialect for Simple {
    const SEGMENT: &'static str = "simple";
    const SUMMARY: &'static str = "Names and a condition: `columns` is a list of column \
        names, `filters` one row condition. A caller who wants a computed column or an \
        alias uses the `expr` routes.";
    const FIELDS: &'static [&'static str] = &["columns", "filters"];
    const NOTE: &'static str = "columns is a list of names and filters one condition";

    fn example(projection: &str, predicate: &str) -> serde_json::Value {
        // The projection arrives as the select list the other vocabulary writes, since a
        // target names the same columns for either, and its commas are this vocabulary's
        // list. The predicate is one expression in both and goes across as it is.
        let names = projection
            .split(',')
            .map(str::trim)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        serde_json::json!({ "columns": names, "filters": predicate })
    }

    fn projection(&self) -> Projection<'_> {
        match self.columns.as_deref() {
            Some(names) => Projection::Columns(names),
            None => Projection::All,
        }
    }

    fn predicate(&self) -> Predicate<'_> {
        match self.filters.as_deref() {
            Some(text) => Predicate::Filters(text),
            None => Predicate::All,
        }
    }
}

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
/// the internal selection its own query layer takes, and both vocabularies lower to one
/// planned expression the way they always did.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct ParquetQuery<D> {
    /// The parquet file to read. Its scheme picks the backend — `s3`, `gs`, `az`, `https`,
    /// `webdav` or `file` — and which of those a deployment answers for is the operator's to
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
    /// The projection and the predicate, in this route's own vocabulary.
    #[serde(flatten)]
    query: D,
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
    /// `json`, the default; `parquet` for the answer as a parquet file laid out like the file
    /// it came from; `votable` for a VOTable, which takes flat columns only and refuses a
    /// nested one by name. Anything but `json` carries its counts in `x-hats-*` response
    /// headers, there being no room in the body.
    #[schema(example = "json")]
    format: Option<String>,
    /// At most this many rows. The order is not promised, but the same request returns the
    /// same rows.
    #[schema(example = 100)]
    limit: Option<usize>,
    /// Every key the body carried that this endpoint has no field for.
    #[serde(flatten)]
    #[schema(ignore)]
    unknown: BTreeMap<String, IgnoredAny>,
}

impl<D: Dialect> ParquetQuery<D> {
    /// Every field this endpoint takes, in the order a body is written in: what to read, how
    /// to reach it, where on the sky, what to ask of it, and how the answer comes back.
    ///
    /// Stated rather than read off the type, because the order cannot be: a `#[serde(flatten)]`
    /// is an `allOf` in the schema and its part always lands first, which would put the
    /// projection above the url it is a projection of. It is what the description orders each
    /// table by and what a refusal names, and `each_route_describes_its_own_body` holds it to
    /// the fields the type actually has, in this order.
    fn fields() -> Vec<&'static str> {
        let mut fields = vec![
            "url",
            "storage",
            "region",
            "ra_column",
            "dec_column",
            "healpix_column",
            "healpix_order",
        ];
        fields.extend(D::FIELDS);
        fields.extend(["format", "limit"]);
        fields
    }

    /// The same list, as the sentence a refusal ends with.
    fn takes() -> String {
        takes(&Self::fields())
    }

    /// What to read, in this route's vocabulary: the request lowered to what the query layer
    /// runs, which is the same type the catalog endpoint's own lowering feeds.
    fn selection(&self) -> Result<Selection<'_>, ApiError> {
        Ok(Selection {
            projection: self.query.projection(),
            predicate: self.query.predicate(),
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
                "healpix_column needs a region; to filter on that column alone, use where",
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
        }))
    }
}

/// A query against a whole HATS catalog: the url names the catalog and the partitions to
/// read are chosen from the region.
///
/// It carries none of [`ParquetQuery`]'s column names. `hats_col_ra`, `hats_col_dec` and
/// `hats_col_healpix` are the catalog's statement about its own files, and it can see more of
/// them than a caller can; a caller who wants their own pair names one of the files, where
/// [`ParquetQuery`] takes them.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
struct CatalogQuery<D> {
    /// The HATS catalog to read: the directory holding `hats.properties`, or a collection's,
    /// which is followed to its primary table. Its scheme picks the backend — `s3`, `gs`,
    /// `az`, `https`, `webdav` or `file` — and which of those a deployment answers for is the
    /// operator's to configure.
    // Treated as opaque: whatever query string it has belongs to the origin, not to us.
    // No `example` on the field, for the reason [`ParquetQuery::url`] has none.
    #[schema(value_type = String)]
    url: SourceUrl,
    /// How to reach the store: an endpoint, a region, credentials. Leave it out for a public
    /// catalog read anonymously, which is the common case. Which options apply is decided by
    /// the url's scheme, and one that does not apply is refused rather than ignored.
    #[serde(default)]
    storage: StorageOptions,
    /// The projection and the predicate, in this route's own vocabulary.
    #[serde(flatten)]
    query: D,
    /// One or more shapes on the sky. A row inside any of them qualifies — the array is a
    /// union — and the whole field is ANDed with the predicate. It is also what chooses the
    /// partitions: without one, every partition of the catalog is read.
    ///
    /// The columns it is tested against are the catalog's own, from its `properties`.
    region: Option<Vec<Region>>,
    /// `json`, the default; `parquet` for the answer as a parquet file laid out like the
    /// partitions it came from; `votable` for a VOTable, which takes flat columns only and
    /// refuses a nested one by name. Anything but `json` carries its counts in `x-hats-*`
    /// response headers, there being no room in the body.
    #[schema(example = "json")]
    format: Option<String>,
    /// At most this many rows, taken from the front of the catalog's own order. The same
    /// request returns the same rows.
    #[schema(example = 100)]
    limit: Option<usize>,
    /// Every key the body carried that this endpoint has no field for.
    #[serde(flatten)]
    #[schema(ignore)]
    unknown: BTreeMap<String, IgnoredAny>,
}

impl<D: Dialect> CatalogQuery<D> {
    /// Every field this endpoint takes, in the order a body is written in.
    /// [`ParquetQuery::fields`] says why the order is stated rather than read off the type.
    fn fields() -> Vec<&'static str> {
        let mut fields = vec!["url", "storage", "region"];
        fields.extend(D::FIELDS);
        fields.extend(["format", "limit"]);
        fields
    }

    /// The same list, as the sentence a refusal ends with.
    fn takes() -> String {
        takes(&Self::fields())
    }

    /// This request, lowered. No entry of a plan this produces carries a credential: there is
    /// no field here to have asked with, so the `None` is the type's rather than a check's.
    fn lowered(&self) -> Lowered<'_, D> {
        Lowered {
            url: &self.url,
            storage: &self.storage,
            query: &self.query,
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
struct CatalogPlanQuery<D> {
    /// The HATS catalog to resolve the request against: the directory holding
    /// `hats.properties`, or a collection's, which is followed to its primary table.
    // No `example` on the field, for the reason [`ParquetQuery::url`] has none.
    #[schema(value_type = String)]
    url: SourceUrl,
    /// How to reach the store: an endpoint, a region, credentials. Only the catalog's own
    /// files are read here, but they are read the same way the rows would be.
    #[serde(default)]
    storage: StorageOptions,
    /// The projection and the predicate, in this route's own vocabulary. Carried into every
    /// entry as you wrote it; nothing here is planned against a file.
    #[serde(flatten)]
    query: D,
    /// One or more shapes on the sky, which is what chooses the partitions and so what
    /// decides how long the work list is. Without one, every partition is an entry.
    ///
    /// An entry the region contains whole carries no region: every row of it qualifies.
    region: Option<Vec<Region>>,
    /// Written into each entry, so the answers arrive in the encoding you asked for. It is
    /// not the plan's own: a plan is JSON.
    #[schema(example = "json")]
    format: Option<String>,
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

impl<D: Dialect> CatalogPlanQuery<D> {
    /// Every field this endpoint takes, in the order a body is written in: the catalog
    /// query's, and last the one field that is this endpoint's own.
    fn fields() -> Vec<&'static str> {
        let mut fields = CatalogQuery::<D>::fields();
        fields.push("return_storage");
        fields
    }

    /// The same list, as the sentence a refusal ends with.
    fn takes() -> String {
        takes(&Self::fields())
    }

    /// This request, lowered.
    fn lowered(&self) -> Lowered<'_, D> {
        Lowered {
            url: &self.url,
            storage: &self.storage,
            query: &self.query,
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
struct Lowered<'a, D> {
    url: &'a SourceUrl,
    storage: &'a StorageOptions,
    query: &'a D,
    region: Option<&'a [Region]>,
    format: Option<&'a str>,
    limit: Option<usize>,
    /// The caller's own storage options, to be written into every entry of a plan. `Some`
    /// only from an endpoint that has a `return_storage` to have asked with.
    echo: Option<serde_json::Value>,
}

impl<D: Dialect> Lowered<'_, D> {
    /// What to read, in the vocabulary the request was written in. The catalog supplies the
    /// columns the region is tested against, so there is nothing here a caller named.
    fn selection(&self) -> CatalogSelection<'_> {
        CatalogSelection {
            projection: self.query.projection(),
            predicate: self.query.predicate(),
            regions: self.region,
            limit: self.limit,
        }
    }
}

/// An endpoint's field list as the sentence a refusal ends with. The url is the one field a
/// body must carry, and it is first in every list, so the two halves are said apart.
fn takes(fields: &[&str]) -> String {
    let [url, optional @ ..] = fields else {
        return String::new();
    };
    format!("{url}, and optionally {}", optional.join(", "))
}

/// A body carrying a name this endpoint has no field for, refused rather than ignored.
///
/// `deny_unknown_fields` cannot say this: serde does not apply it to a struct that has a
/// flattened field, and every request here flattens its vocabulary. A `flatten` with nothing
/// to catch the leftovers drops them in silence, and a `filters` dropped on the expression
/// route would return every row — which the caller cannot tell from a predicate that matched
/// them all, the failure this service keeps finding in a new place. So each request type
/// collects them and every route refuses them before it does anything: a request this service
/// cannot read as written is not one it may answer part of.
///
/// The refusal names what the endpoint does take, which is also where a caller who wrote
/// another route's field reads that it is not this one's.
fn refuse_unknown(unknown: &BTreeMap<String, IgnoredAny>, takes: &str) -> Result<(), ApiError> {
    match unknown.is_empty() {
        true => Ok(()),
        false => Err(ApiError::bad_request(format!(
            "not accepted here: {}; this route takes {takes}",
            unknown.keys().cloned().collect::<Vec<_>>().join(", ")
        ))),
    }
}

/// Half of the pair is not half a request. The column may be called anything and be written
/// at any order, so the order is what says which cell a value names — and reading a column
/// at the wrong order puts every bound where no row is, which returns nothing rather than
/// failing. None of which the caller needs; they need to send the other field.
const HEALPIX_PAIR: &str = "healpix_column and healpix_order must be given together";

/// What the caller wants back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Format {
    Json,
    Parquet,
    /// The XML table format IVOA tools read. Flat columns only — `votable.rs` says which
    /// ones are refused and why.
    Votable,
}

impl Format {
    /// Every format, in the order a refusal lists them. The default is the first.
    const ALL: [Self; 3] = [Self::Json, Self::Parquet, Self::Votable];

    /// The one place a format's name is written. [`Self::parse`] and the list in a
    /// refusal are both derived from it, so a format cannot be renamed in one and not
    /// the others, or added and left unparseable.
    fn name(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Parquet => "parquet",
            Self::Votable => "votable",
        }
    }

    /// The default is the caller's mode, not this type's: an API request asks for rows
    /// and gets JSON, while a file-server request asks a parquet file for less of itself
    /// and gets a parquet file back. Adding a query string should not change what media
    /// type a path answers with.
    fn parse(raw: Option<&str>, default: Self) -> Result<Self, ApiError> {
        let Some(raw) = raw else {
            return Ok(default);
        };
        Self::ALL
            .into_iter()
            .find(|format| format.name() == raw)
            .ok_or_else(|| {
                ApiError::bad_request(format!(
                    "unknown format {raw:?}; supported formats are {}",
                    Self::ALL.map(Self::name).join(", ")
                ))
            })
    }
}

/// A body we could not read, said without quoting it back.
///
/// The body carries the credentials, and serde's type errors quote the offending value
/// — `invalid type: string "AKIA…"`. So the message is ours, except for the two serde
/// phrasings that name a key rather than a value. The rest state what was expected,
/// which is what the caller needed anyway.
///
/// Recognising those two by their wording is the weak part: serde could reword them, and
/// the only cost would be a caller who stops being told which key they misspelled. It
/// fails towards the safe message, and the two tests below are what notice.
fn body_error<D: Dialect>(rejection: &JsonRejection, takes: &str) -> ApiError {
    // What this endpoint takes, and not what any endpoint takes: the two differ, and a
    // sentence naming both leaves the caller to work out which half is theirs. The
    // vocabulary's note is here because this is the message a wrong *type* lands on, and
    // serde's own — which does say the type — quotes the value beside it.
    let shape = format!("expected a JSON object with {takes}; {}", D::NOTE);

    match rejection {
        // A parse failure quotes the position, not the contents.
        JsonRejection::JsonSyntaxError(error) => {
            ApiError::bad_request(format!("the request body is not valid JSON: {error}"))
        }
        JsonRejection::JsonDataError(error) => {
            let message = error.body_text();
            // `unknown field \`regoin\`` and `missing field \`url\`` name a key. Every
            // other message may quote a value.
            let names_a_key = ["unknown field", "missing field"]
                .iter()
                .any(|prefix| message.contains(prefix));
            match names_a_key {
                true => ApiError::bad_request(message),
                false => ApiError::bad_request(format!("the request body does not fit: {shape}")),
            }
        }
        _ => ApiError::bad_request(format!("{}; {shape}", rejection.body_text())),
    }
}

/// One column of the answer. Sent even when no rows matched, so it is where the answer's shape
/// can always be read.
// Rows do not describe themselves, and an empty answer looks exactly like a file that has not
// got the column — hence sending this for an empty answer and for `limit=0`.
#[derive(Debug, Serialize, utoipa::ToSchema)]
struct Column {
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
struct HatsResponse {
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
struct SelectResponse {
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
const NUM_ROWS_HEADER: &str = "x-hats-num-rows";
const DATA_BYTES_READ_HEADER: &str = "x-hats-data-bytes-read";
const ELAPSED_MS_HEADER: &str = "x-hats-elapsed-ms";

async fn query_parquet<D: Dialect>(
    State(service): State<Service>,
    body: Result<Json<ParquetQuery<D>>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(params) =
        body.map_err(|rejection| body_error::<D>(&rejection, &ParquetQuery::<D>::takes()))?;
    let started = Instant::now();
    refuse_unknown(&params.unknown, &ParquetQuery::<D>::takes())?;
    // Everything decidable from the request alone, before anything is opened.
    let format = Format::parse(params.format.as_deref(), Format::Json)?;
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
    let result = query::run(&file, &selection, service.sql_limits, Order::Unspecified)
        .await
        .map_err(hide_the_path)?;

    let num_rows = result.num_rows();
    let data_bytes_read = result.data_bytes_read;
    // Both of them, the way the mounted path does it: encoding a parquet answer reads
    // the source file's layout, so it raises the store's messages too.
    let response = answer(&result, &file, format, started)
        .await
        .map_err(hide_the_path)?;
    tracing::info!(
        // file.url, not the parameter: the parameter may carry credentials. The two
        // expressions are the caller's own text and can be megabytes of `IN` list, so
        // what is logged is that they were there.
        url = %file.url,
        vocabulary = D::SEGMENT,
        selected = params.query.selects(),
        filtered = params.query.filtered(),
        // How many shapes, not what they were: a region is small, but logging the
        // numbers would be logging the caller's own coordinates for no purpose the
        // count does not already serve.
        regions = params.region.as_ref().map_or(0, Vec::len),
        format = format.name(),
        num_rows,
        // Over a remote store this is also what the request cost the origin, which the
        // elapsed time on its own does not distinguish from a slow network.
        data_bytes_read,
        elapsed_ms = started.elapsed().as_millis(),
        "query"
    );
    Ok(response)
}

/// How many partitions of the catalog the answer was read from, which is the number that
/// says whether a region pruned. A parquet body has no room for it, so it travels beside the
/// other three.
const NUM_PARTITIONS_HEADER: &str = "x-hats-num-partitions";

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
    format: Format,
}

async fn open_catalog<D: Dialect>(
    service: &Service,
    params: &Lowered<'_, D>,
) -> Result<Opened, ApiError> {
    let format = Format::parse(params.format, Format::Json)?;
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
        format,
    })
}

/// A query against a catalog: the url names a HATS directory, and the partitions to read
/// are chosen from the region rather than named by the caller.
///
/// Its body is [`CatalogQuery`] rather than [`query_parquet`]'s: what a caller may say about
/// a catalog is not what they may say about one file, so the two are different types on
/// different routes rather than one body with a sentence about which fields apply where.
async fn query_hats<D: Dialect>(
    State(service): State<Service>,
    body: Result<Json<CatalogQuery<D>>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(body) =
        body.map_err(|rejection| body_error::<D>(&rejection, &CatalogQuery::<D>::takes()))?;
    let started = Instant::now();
    refuse_unknown(&body.unknown, &CatalogQuery::<D>::takes())?;
    let params = body.lowered();
    let Opened {
        search,
        url,
        format,
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
            return Ok((StatusCode::PAYLOAD_TOO_LARGE, Json(plan)).into_response());
        }
    };

    let num_rows = result.rows.num_rows();
    let data_bytes_read = result.rows.data_bytes_read;
    let partitions_read = result.partitions_read;
    let response = hats_answer(&result, format, started)
        .await
        .map_err(&hide_the_path)?;
    tracing::info!(
        // The catalog's url, not the parameter, which may carry credentials.
        url = %search.catalog().dir().url,
        partitions = search.catalog().partitions().len(),
        source = search.catalog().partitions().source().name(),
        chosen = search.chosen().len(),
        partitions_read,
        vocabulary = D::SEGMENT,
        selected = params.query.selects(),
        filtered = params.query.filtered(),
        // How many shapes, not what they were: logging the numbers would be logging the
        // caller's own coordinates for no purpose the count does not already serve.
        regions = params.region.map_or(0, <[Region]>::len),
        format = format.name(),
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
async fn query_hats_plan<D: Dialect>(
    State(service): State<Service>,
    body: Result<Json<CatalogPlanQuery<D>>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(body) =
        body.map_err(|rejection| body_error::<D>(&rejection, &CatalogPlanQuery::<D>::takes()))?;
    let started = Instant::now();
    // Planned, not run — but a body this route cannot read as written is refused here all
    // the same, or the plan would hand back entries every one of which is a 400 the client
    // discovers one at a time.
    refuse_unknown(&body.unknown, &CatalogPlanQuery::<D>::takes())?;
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
struct PlanResponse<D> {
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
    requests: Vec<PlanRequest<D>>,
}

/// One entry of a plan: a request to send to this service, for one partition of the catalog.
#[derive(Debug, Serialize, utoipa::ToSchema)]
struct PlanRequest<D> {
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
    body: PlanBody<D>,
}

/// The body of one plan entry: an ordinary single-file request, ready to send unchanged.
///
/// It carries what you wrote that still applies, plus the coordinate and index column names the
/// catalog supplied, and it is written in the vocabulary you used.
// The column names are stated here because the single-file route has no catalog to ask.
#[derive(Debug, Serialize, utoipa::ToSchema)]
struct PlanBody<D> {
    /// The partition's own url, below the catalog url you gave.
    url: String,
    #[serde(flatten)]
    query: D,
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

async fn plan_of<D: Dialect>(
    service: &Service,
    search: &Search,
    params: &Lowered<'_, D>,
    reason: Option<Exceeded>,
) -> Result<PlanResponse<D>, ApiError> {
    let url = parse_url(params.url.as_str())?;
    let data = service.data_files_for(&url);
    let entries = search.entries(data).await?;
    // The route an entry is sent to. `None` cannot happen — the API is how this request
    // arrived — but a prefix is what the caller must be told, not what this can assume.
    let path = service
        .api_prefix
        .as_deref()
        .map(|prefix| route(prefix, &format!("{}/parquet", D::SEGMENT)))
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
                    query: (*params.query).clone(),
                    region,
                    ra_column: named.map(|columns| columns.ra.clone()),
                    dec_column: named.map(|columns| columns.dec.clone()),
                    healpix_column: named.and(healpix).map(|(column, _)| column.clone()),
                    healpix_order: named.and(healpix).map(|(_, order)| *order),
                    format: params.format.map(str::to_owned),
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

/// The catalog's answer, in whichever encoding was asked for.
///
/// A parquet body copies its layout from one of the partitions actually read, which is the
/// nearest thing to "the source file" a catalog has. A request that read nothing gets the
/// writer's own defaults, there being no file to copy from.
async fn hats_answer(
    result: &crate::hats_query::CatalogResult,
    format: Format,
    started: Instant,
) -> Result<Response, ApiError> {
    let num_rows = result.rows.num_rows();
    match format {
        Format::Json => {
            let rows = query::to_json(&result.rows)?;
            let schema = columns_of(&result.rows);
            Ok(Json(HatsResponse {
                num_rows: rows.len(),
                num_partitions: result.partitions_read,
                schema,
                data_bytes_read: result.rows.data_bytes_read,
                elapsed_ms: started.elapsed().as_millis(),
                rows,
            })
            .into_response())
        }
        Format::Parquet => {
            let layout = match &result.source {
                Some(file) => parquet_out::read_layout(file).await?,
                None => parquet_out::SourceLayout::default(),
            };
            let body = parquet_out::encode(&result.rows, &layout)?;
            Ok((
                attachment(PARQUET_CONTENT_TYPE, "selection.parquet"),
                hats_counters(result, num_rows, started),
                body,
            )
                .into_response())
        }
        Format::Votable => Ok((
            attachment(votable::CONTENT_TYPE, "selection.vot"),
            hats_counters(result, num_rows, started),
            votable::encode(&result.rows)?,
        )
            .into_response()),
    }
}

/// What a body that is not JSON is served as, and what to call it once it is saved. The
/// counts have no room in any of those bodies, so they travel as headers instead.
fn attachment(content_type: &str, name: &str) -> [(header::HeaderName, String); 2] {
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
    result: &crate::hats_query::CatalogResult,
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
async fn answer(
    result: &QueryResult,
    file: &RemoteFile,
    format: Format,
    started: Instant,
) -> Result<Response, ApiError> {
    match format {
        Format::Json => json_response(result, started),
        Format::Parquet => parquet_response(result, file, result.num_rows(), started).await,
        Format::Votable => Ok((
            attachment(votable::CONTENT_TYPE, &download_name(file, "vot")),
            counters(result, result.num_rows(), started),
            votable::encode(result)?,
        )
            .into_response()),
    }
}

/// What a single-file request reports beside its rows, whichever encoding carries them.
fn counters(
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

fn json_response(result: &QueryResult, started: Instant) -> Result<Response, ApiError> {
    let rows = query::to_json(result)?;
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
async fn parquet_response(
    result: &QueryResult,
    file: &RemoteFile,
    num_rows: usize,
    started: Instant,
) -> Result<Response, ApiError> {
    let layout = parquet_out::read_layout(file).await?;
    let body = parquet_out::encode(result, &layout)?;
    Ok((
        attachment(PARQUET_CONTENT_TYPE, &download_name(file, "parquet")),
        counters(result, num_rows, started),
        body,
    )
        .into_response())
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
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;

    const SECRET: &str = "wJalrXUtnFEMIsecretKEY";

    /// The API alone, at its default prefix and with nothing mounted.
    fn api_only() -> Service {
        Service::new(
            AccessPolicy::default(),
            &LimitsConfig::default(),
            Arc::default(),
            &ApiConfig::default(),
            &DataConfig::default(),
            &ServerConfig::default(),
        )
        .unwrap()
    }

    async fn get(uri: &str) -> (StatusCode, String) {
        send(Request::builder().uri(uri), Body::empty()).await
    }

    /// A `POST /api/v1/expr/parquet` with the given body, under a policy that allows
    /// everything — what the policy allows is `access.rs`'s business.
    async fn select_with(body: serde_json::Value) -> (StatusCode, String) {
        send(
            Request::builder()
                .method("POST")
                .uri("/api/v1/expr/parquet")
                .header("content-type", "application/json"),
            Body::from(body.to_string()),
        )
        .await
    }

    /// The status and the body as text; axum's own rejections are plain text, ours are
    /// JSON.
    async fn send(request: http::request::Builder, body: Body) -> (StatusCode, String) {
        let response = router(api_only())
            .oneshot(request.body(body).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn health_is_ok() {
        let (status, body) = get("/api/v1/health").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&body).unwrap()["status"],
            "ok"
        );
    }

    /// The credential-bearing shape is not reachable by a method that puts its
    /// parameters in a url.
    #[tokio::test]
    async fn the_query_endpoint_is_not_a_get() {
        let (status, _) = get("/api/v1/expr/parquet?url=s3://b/k.parquet&where=x%3D1").await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn a_missing_field_is_named() {
        let (status, body) = select_with(serde_json::json!({"where": "x = 1"})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("url"), "{body}");
    }

    #[tokio::test]
    async fn a_misspelled_field_is_named_rather_than_ignored() {
        let (status, body) = select_with(serde_json::json!({
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
            let (status, body) = select_with(body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{what}: {body}");
            assert!(body.contains("region"), "{what}: {body}");
        }
    }

    /// A radius with no unit in its name is refused rather than read as either one: both
    /// readings are legal radii, differing by a factor of 3600, and no answer would say
    /// which one it had used.
    #[tokio::test]
    async fn a_radius_says_its_unit() {
        let (status, body) = select_with(serde_json::json!({
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
        let (status, body) = select_with(serde_json::json!({
            "url": "s3://b/k.parquet",
            "region": [{"type": "circle", "ra": 320.6, "dec": -12.4, "radus": 0.01}],
        }))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("radus"), "{body}");
    }

    /// A body that does not fit is described, never quoted: a mistyped `storage` is
    /// exactly where a secret would be sitting.
    #[tokio::test]
    async fn a_body_that_does_not_fit_is_not_quoted_back() {
        let (status, body) = select_with(serde_json::json!({
            "url": "s3://b/k.parquet",
            "storage": SECRET,
        }))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(!body.contains(SECRET), "leaked: {body}");
        assert!(body.contains("storage"), "{body}");
    }

    #[tokio::test]
    async fn a_body_that_is_not_json_is_rejected() {
        let (status, body) = send(
            Request::builder()
                .method("POST")
                .uri("/api/v1/expr/parquet")
                .header("content-type", "application/json"),
            Body::from(format!("{{\"url\": \"{SECRET}\"")),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(!body.contains(SECRET), "leaked: {body}");
    }

    #[tokio::test]
    async fn unsupported_schemes_are_rejected() {
        let (status, body) = select_with(serde_json::json!({
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
        let (status, body) = select_with(serde_json::json!({
            "url": format!("s3://b/k.parquet?secret_access_key={SECRET}"),
        }))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("query string"), "{body}");
        assert!(!body.contains(SECRET), "leaked: {body}");
    }

    #[test]
    fn parses_the_format_parameter() {
        assert_eq!(
            Format::parse(Some("json"), Format::Parquet).unwrap(),
            Format::Json
        );
        assert_eq!(
            Format::parse(Some("parquet"), Format::Json).unwrap(),
            Format::Parquet
        );
        // Absent is the mode's own default, which is why it is passed in.
        assert_eq!(Format::parse(None, Format::Json).unwrap(), Format::Json);
        assert_eq!(
            Format::parse(None, Format::Parquet).unwrap(),
            Format::Parquet
        );
    }

    #[tokio::test]
    async fn unknown_formats_are_rejected() {
        let (status, body) = select_with(serde_json::json!({
            "url": "s3://b/k.parquet", "format": "csv",
        }))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("unknown format"), "{body}");
        assert!(body.contains("json, parquet, votable"), "{body}");
    }

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
            query: Expr {
                select: None,
                r#where: Some("objectid = 1".to_owned()),
            },
            region: None,
            ra_column: None,
            dec_column: None,
            healpix_column: None,
            healpix_order: None,
            format: None,
            limit: None,
            unknown: BTreeMap::new(),
        };
        let shown = format!("{params:?}");
        assert!(!shown.contains(SECRET), "leaked: {shown}");
        assert!(shown.contains("s3://b/k.parquet"), "{shown}");
        assert!(shown.contains("us-west-2"), "{shown}");
    }

    /// One `[[mount]]`, and a service built around it.
    fn with_mount(config: crate::config::MountConfig, api: &ApiConfig) -> Service {
        with_limits(config, api, &LimitsConfig::default())
    }

    /// The same, for the cases that are about a bound rather than about a route.
    fn with_limits(
        config: crate::config::MountConfig,
        api: &ApiConfig,
        limits: &LimitsConfig,
    ) -> Service {
        let mounts = Arc::new(Mounts::new(&[config], &DataConfig::default()).unwrap());
        let policy =
            AccessPolicy::new(&crate::config::AccessConfig::default(), Arc::clone(&mounts))
                .unwrap();
        Service::new(
            policy,
            limits,
            mounts,
            api,
            &DataConfig::default(),
            &ServerConfig::default(),
        )
        .unwrap()
    }

    /// A `[[mount]]` publishing `dir` at `/`, which is what most of these want.
    fn serving(dir: &Path) -> crate::config::MountConfig {
        crate::config::MountConfig {
            path: "/".to_owned(),
            source: dir.display().to_string(),
            serve: true,
            follow_symlinks: false,
            immutable: false,
            filenames: None,
        }
    }

    /// A directory with one file in it, and a service that publishes it at `/`.
    fn mounted(dir: &Path, api: &ApiConfig) -> Service {
        with_mount(serving(dir), api)
    }

    async fn respond(service: Service, request: http::request::Builder) -> Response {
        router(service)
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    /// A body a client asked for gzip on, and what the answer was in bytes.
    ///
    /// The encoding is read off the response rather than guessed from the request: a
    /// predicate that declined leaves the body as it was and says nothing, which is the
    /// case half of these tests are about.
    async fn fetch(
        service: Service,
        uri: &str,
        accept_encoding: Option<&str>,
    ) -> (Response, Vec<u8>) {
        let mut request = Request::builder().uri(uri);
        if let Some(encoding) = accept_encoding {
            request = request.header(header::ACCEPT_ENCODING, encoding);
        }
        let response = respond(service, request).await;
        let (parts, body) = response.into_parts();
        let bytes = body.collect().await.unwrap().to_bytes().to_vec();
        (Response::from_parts(parts, Body::empty()), bytes)
    }

    /// The two bytes a gzip member starts with. The point is that the body was actually
    /// encoded, not merely labelled.
    const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

    /// The JSON answers are the case this exists for: the same keys on every row, and a
    /// description document that repeats a schema's vocabulary throughout.
    #[tokio::test]
    async fn json_is_compressed_for_a_client_that_asks() {
        let uri = "/api/v1/openapi.json";
        let (plain, identity) = fetch(api_only(), uri, None).await;
        let (response, body) = fetch(api_only(), uri, Some("gzip")).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_ENCODING], "gzip");
        assert_eq!(body[..2], GZIP_MAGIC, "not gzip: {:?}", &body[..2]);
        assert!(
            body.len() < identity.len(),
            "{} compressed is {} bytes",
            identity.len(),
            body.len()
        );
        // A compressed body is chunked, so the length a client would have sized a buffer
        // from is gone — and is there for the client that did not ask.
        assert!(!response.headers().contains_key(header::CONTENT_LENGTH));
        assert!(!plain.headers().contains_key(header::CONTENT_ENCODING));
        serde_json::from_slice::<serde_json::Value>(&identity).unwrap();
    }

    /// What a browser asks for, and what it gets: the encoding is negotiated rather than
    /// fixed, so the three that are compiled in are the three a client may choose from.
    #[tokio::test]
    async fn the_encoding_is_the_clients_to_choose() {
        for (accepted, chosen) in [("br", "br"), ("zstd", "zstd"), ("gzip", "gzip")] {
            let (response, body) = fetch(api_only(), "/api/v1/openapi.json", Some(accepted)).await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()[header::CONTENT_ENCODING], chosen);
            assert!(!body.is_empty(), "{chosen}: empty body");
        }
        // One this service does not offer, alongside one it does: the answer is the one it
        // offers rather than an unencoded body.
        let (response, _) = fetch(
            api_only(),
            "/api/v1/openapi.json",
            Some("deflate;q=1.0, gzip;q=0.5"),
        )
        .await;
        assert_eq!(response.headers()[header::CONTENT_ENCODING], "gzip");
    }

    /// The generated directory page, which inlines its own stylesheet and script before it
    /// lists a single entry.
    #[tokio::test]
    async fn a_directory_page_is_compressed() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), b"0123456789").unwrap();

        let service = mounted(dir.path(), &ApiConfig::default());
        let (response, body) = fetch(service, "/", Some("gzip")).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_ENCODING], "gzip");
        assert_eq!(body[..2], GZIP_MAGIC);
    }

    /// Parquet is the exclusion, and it is the response's content type that carries it —
    /// so a file served off a mount and a query answered as parquet are one rule. The
    /// bytes come back verbatim, which is what a client reading a footer depends on.
    #[tokio::test]
    async fn a_parquet_body_is_never_compressed() {
        let dir = tempfile::TempDir::new().unwrap();
        // Long and repetitive, so that nothing but the predicate could be what declined:
        // it is well over the size the default predicate ignores, and gzip would flatten it.
        let contents = b"PAR1".repeat(1024);
        std::fs::write(dir.path().join("part0.parquet"), &contents).unwrap();

        let service = mounted(dir.path(), &ApiConfig::default());
        let (response, body) = fetch(service, "/part0.parquet", Some("gzip")).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            PARQUET_CONTENT_TYPE
        );
        assert!(!response.headers().contains_key(header::CONTENT_ENCODING));
        assert_eq!(response.headers()[header::CONTENT_LENGTH], "4096");
        assert_eq!(body, contents);
    }

    /// The bytes, the length, the type, and the header that says a client may ask for
    /// part of it — which is how an `lsdb` client reads one partition without
    /// downloading it.
    #[tokio::test]
    async fn a_mounted_file_is_served_whole() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), b"0123456789").unwrap();

        let response = respond(
            mounted(dir.path(), &ApiConfig::default()),
            Request::builder().uri("/part0.parquet"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            PARQUET_CONTENT_TYPE
        );
        assert_eq!(response.headers()[header::CONTENT_LENGTH], "10");
        assert!(response.headers().contains_key(header::ACCEPT_RANGES));
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"0123456789");
    }

    #[tokio::test]
    async fn a_mounted_file_is_served_by_range() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), b"0123456789").unwrap();

        let response = respond(
            mounted(dir.path(), &ApiConfig::default()),
            Request::builder()
                .uri("/part0.parquet")
                .header(header::RANGE, "bytes=-4"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"6789");
    }

    /// A range is answered with the bytes that were asked for, whatever the file is and
    /// whatever encoding the client would accept. The content-type exclusion does not
    /// cover this — the file here is text, and text is what compresses best — so it is
    /// the layer's own refusal to touch a partial response that holds.
    #[tokio::test]
    async fn a_ranged_read_is_not_compressed() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("notes.txt"), "a".repeat(1024)).unwrap();

        let response = respond(
            mounted(dir.path(), &ApiConfig::default()),
            Request::builder()
                .uri("/notes.txt")
                .header(header::RANGE, "bytes=-4")
                .header(header::ACCEPT_ENCODING, "gzip"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert!(!response.headers().contains_key(header::CONTENT_ENCODING));
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"aaaa");
    }

    /// Nothing about the filesystem comes back: a file outside the mount, one that is
    /// not there and one behind a symlink the mount does not follow are one answer.
    #[tokio::test]
    async fn what_a_mount_will_not_serve_is_not_described() {
        let dir = tempfile::TempDir::new().unwrap();
        let published = dir.path().join("published");
        std::fs::create_dir(&published).unwrap();
        let secret = dir.path().join("secret.parquet");
        std::fs::write(&secret, b"secret").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&secret, published.join("innocent.parquet")).unwrap();

        let service = || mounted(&published, &ApiConfig::default());
        for uri in [
            "/missing.parquet",
            #[cfg(unix)]
            "/innocent.parquet",
        ] {
            let response = respond(service(), Request::builder().uri(uri)).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{uri}");
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let body = String::from_utf8(body.to_vec()).unwrap();
            assert!(!body.contains("secret"), "{uri} leaked: {body}");
            assert!(!body.contains("symlink"), "{uri} leaked: {body}");
            assert!(
                !body.contains(&dir.path().display().to_string()),
                "{uri} leaked a local path: {body}"
            );
        }
    }

    /// A request path is not a place to climb from: `..` is refused whichever way it is
    /// spelled, rather than being cleaned up and then found to be outside the mount.
    #[tokio::test]
    async fn a_path_cannot_climb_out_of_a_mount() {
        let dir = tempfile::TempDir::new().unwrap();
        let published = dir.path().join("published");
        std::fs::create_dir(&published).unwrap();
        std::fs::write(dir.path().join("secret.parquet"), b"secret").unwrap();

        for uri in ["/../secret.parquet", "/%2e%2e/secret.parquet"] {
            let response = respond(
                mounted(&published, &ApiConfig::default()),
                Request::builder().uri(uri),
            )
            .await;
            // Refused for what it says, not for where it would have landed: the
            // containment check behind this would also refuse it, and a 400 is what
            // says the segment never became a path component at all.
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert!(
                !String::from_utf8_lossy(&body).contains("secret"),
                "{uri} leaked"
            );
        }
    }

    /// A tree with something at two levels, so a listing has a parent to point at.
    fn tree() -> tempfile::TempDir {
        let dir = tempfile::TempDir::new().unwrap();
        let inner = dir.path().join("Norder=5");
        std::fs::create_dir(&inner).unwrap();
        std::fs::write(inner.join("Npix=12240.parquet"), b"0123456789").unwrap();
        std::fs::write(dir.path().join("properties"), b"x").unwrap();
        dir
    }

    async fn body_of(response: Response) -> String {
        let body = response.into_body().collect().await.unwrap().to_bytes();
        String::from_utf8(body.to_vec()).unwrap()
    }

    /// The default reading: a client walking the tree gets the names, the types and the
    /// urls to ask for next, without having to know how a name becomes a url.
    #[tokio::test]
    async fn a_directory_is_listed_as_json() {
        let dir = tree();
        let response = respond(
            mounted(dir.path(), &ApiConfig::default()),
            Request::builder().uri("/Norder=5"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response.headers()[header::CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("application/json")
        );
        let listing: serde_json::Value = serde_json::from_str(&body_of(response).await).unwrap();
        assert_eq!(listing["path"], "/Norder=5");
        assert_eq!(listing["parent"], "/");
        assert_eq!(listing["entries"][0]["name"], "Npix=12240.parquet");
        assert_eq!(listing["entries"][0]["type"], "file");
        assert_eq!(listing["entries"][0]["size"], 10);
        assert_eq!(listing["entries"][0]["url"], "/Norder=5/Npix=12240.parquet");
    }

    /// The top of a mount has nothing above it, whatever is above it on disk.
    #[tokio::test]
    async fn a_listing_does_not_point_above_its_mount() {
        let dir = tree();
        let response = respond(
            mounted(dir.path(), &ApiConfig::default()),
            Request::builder().uri("/"),
        )
        .await;
        let listing: serde_json::Value = serde_json::from_str(&body_of(response).await).unwrap();
        assert_eq!(listing["path"], "/");
        assert_eq!(listing["parent"], serde_json::Value::Null);
    }

    /// Only a browser gets the page. Everything else — and `*/*` above all, which is
    /// what every client library sends — gets the reading it can parse.
    #[tokio::test]
    async fn a_browser_gets_a_page_and_a_client_does_not() {
        let dir = tree();
        let service = || mounted(dir.path(), &ApiConfig::default());
        let accepting = |accept: &'static str| {
            Request::builder()
                .uri("/Norder=5")
                .header(header::ACCEPT, accept)
        };

        let response = respond(service(), accepting("text/html,application/xhtml+xml")).await;
        assert!(
            response.headers()[header::CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("text/html")
        );
        let body = body_of(response).await;
        // The href is what a browser clicks and what `fsspec` scrapes.
        assert!(
            body.contains("href=\"/Norder=5/Npix=12240.parquet\""),
            "{body}"
        );

        for accept in ["*/*", "application/json"] {
            let response = respond(service(), accepting(accept)).await;
            assert!(
                response.headers()[header::CONTENT_TYPE]
                    .to_str()
                    .unwrap()
                    .starts_with("application/json"),
                "{accept}"
            );
        }
    }

    /// A directory that has its own page is served it, and the generated listing is what
    /// happens when it does not.
    #[tokio::test]
    async fn a_directory_with_an_index_is_served_it() {
        let dir = tree();
        std::fs::write(dir.path().join("index.html"), b"<p>the catalog</p>").unwrap();

        let response = respond(
            mounted(dir.path(), &ApiConfig::default()),
            Request::builder().uri("/"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_of(response).await, "<p>the catalog</p>");
    }

    /// Nothing here is written, so a verb that would write is refused at the listing
    /// rather than answered with one.
    #[tokio::test]
    async fn a_listing_is_not_written_to() {
        let dir = tree();
        let response = respond(
            mounted(dir.path(), &ApiConfig::default()),
            Request::builder().method("POST").uri("/"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    /// The API's subtree belongs to the API even where a mount covers everything else,
    /// so a mistyped API path is a 404 rather than a file.
    #[tokio::test]
    async fn the_api_prefix_wins_over_a_mount_that_covers_it() {
        let dir = tempfile::TempDir::new().unwrap();
        // Files that would be served if the API's subtree were the mount's to answer.
        let inside = dir.path().join("api/v1");
        std::fs::create_dir_all(&inside).unwrap();
        std::fs::write(inside.join("health"), b"not the health route").unwrap();
        std::fs::write(inside.join("part0.parquet"), b"0123456789").unwrap();
        std::fs::write(dir.path().join("part0.parquet"), b"0123456789").unwrap();
        let service = || mounted(dir.path(), &ApiConfig::default());

        let response = respond(service(), Request::builder().uri("/api/v1/health")).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(String::from_utf8_lossy(&body).contains("\"ok\""));

        // And a path under the prefix that no route matched does not fall through to
        // the mount, even where the mount has a file with exactly that name.
        let response = respond(service(), Request::builder().uri("/api/v1/part0.parquet")).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // The mount still serves everything outside it.
        let response = respond(service(), Request::builder().uri("/part0.parquet")).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// A pure file server: the API's own routes are not there to be found.
    #[tokio::test]
    async fn the_api_can_be_turned_off_and_the_mount_still_serves() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), b"0123456789").unwrap();
        let service = || {
            mounted(
                dir.path(),
                &ApiConfig {
                    enabled: false,
                    ..Default::default()
                },
            )
        };

        let response = respond(service(), Request::builder().uri("/api/v1/health")).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let response = respond(service(), Request::builder().uri("/part0.parquet")).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// A `POST` of `body` to one route of a service's own API, as JSON.
    ///
    /// The path is given rather than assumed, because which vocabulary a body is written in
    /// is now which route it goes to — a test that sends `columns` to an `expr` route is
    /// asking a different question than it means to.
    async fn post_json(
        service: Service,
        path: &str,
        body: serde_json::Value,
    ) -> (StatusCode, String) {
        let request = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = router(service).oneshot(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    /// A `POST` of `body` to a service's own API route, as JSON.
    async fn ask(service: Service, body: serde_json::Value) -> (StatusCode, String) {
        post_json(service, "/api/v1/expr/parquet", body).await
    }

    async fn ask_hats(service: Service, body: serde_json::Value) -> (StatusCode, String) {
        post_json(service, "/api/v1/expr/hats", body).await
    }

    /// The route end to end: a body naming a catalog by a mount's path comes back with the
    /// rows the region holds, and with the count of partitions they came out of.
    ///
    /// What `hats_query` tests is which partitions get read and which rows come back. What
    /// this adds is that the request shape reaches it — the same body the parquet route
    /// takes, with the column names left to the catalog.
    #[tokio::test]
    async fn the_hats_route_answers_a_region_over_a_catalog() {
        let dir = crate::hats_query::tests::fixture(true);
        let region = crate::hats_query::tests::regions()[0].clone();
        let expected = crate::hats_query::tests::inside(&region);
        assert!(!expected.is_empty(), "the cone selects nothing");

        let (status, body) = ask_hats(
            mounted(dir.path(), &ApiConfig::default()),
            serde_json::json!({
                "url": "file:///",
                "select": "id",
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
        let dir = crate::hats_query::tests::fixture(true);
        for field in ["ra_column", "dec_column", "healpix_column", "healpix_order"] {
            let value = match field {
                "healpix_order" => serde_json::json!(29),
                _ => serde_json::json!("whatever"),
            };
            for route in ["expr/hats", "expr/hats/plan"] {
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

    /// The centre of the fixture's first cone, which is where its rows are.
    fn centre() -> (f64, f64) {
        let Region::Circle { ra, dec, .. } = crate::hats_query::tests::regions()[0] else {
            unreachable!("the fixture's regions are circles")
        };
        (ra, dec)
    }

    /// A service over the fixture whose query strings may ask for a cone this wide.
    fn catalog_server(dir: &Path, max_radius_arcsec: f64) -> Service {
        with_limits(
            serving(dir),
            &ApiConfig::default(),
            &LimitsConfig {
                max_query_radius_arcsec: max_radius_arcsec,
                ..LimitsConfig::default()
            },
        )
    }

    /// The catalog's own url answers the same search the API's route does, and picks the
    /// same partitions to answer it from.
    ///
    /// The vocabularies differ — a url carries one circle where a body carries an array of
    /// shapes — but they lower to the same request, so what this holds is that the rows are
    /// the geometry's and not the route's.
    #[tokio::test]
    async fn a_catalog_url_answers_a_cone_search() {
        let dir = crate::hats_query::tests::fixture(true);
        let region = crate::hats_query::tests::regions()[0].clone();
        let expected = crate::hats_query::tests::inside(&region);
        assert!(!expected.is_empty(), "the cone selects nothing");
        let (ra, dec) = centre();

        let response = respond(
            // The fixture's cone is a degree across, which is well past what a deployment
            // offers by default; the bound itself is what the next test is about.
            catalog_server(dir.path(), 3600.0),
            Request::builder().uri(format!(
                "/?ra={ra}&dec={dec}&radius_arcsec=3600&columns=id&format=json"
            )),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_of(response).await;
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(answer["num_rows"], expected.len(), "{body}");
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

    /// A url answers what fits in one answer, and says where a wider search goes.
    ///
    /// Checked on the request's own numbers rather than on what reading it costs: the other
    /// bounds answer a fan-out, which is exactly what a url cannot carry. Both spellings of
    /// the radius are the same radius, so both are measured against it.
    #[tokio::test]
    async fn a_cone_wider_than_the_url_answers_is_refused() {
        let dir = crate::hats_query::tests::fixture(true);
        let (ra, dec) = centre();
        let service = || mounted(dir.path(), &ApiConfig::default());

        for radius in ["radius_arcsec=1200", "radius_deg=1"] {
            let response = respond(
                service(),
                Request::builder().uri(format!("/?ra={ra}&dec={dec}&{radius}")),
            )
            .await;
            let status = response.status();
            let body = body_of(response).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{radius}: {body}");
            assert!(body.contains("600"), "{radius}: {body}");
            assert!(body.contains("plan route"), "{radius}: {body}");
        }

        // And what is inside the bound is answered, so the refusal is the radius and not
        // the route.
        let response = respond(
            service(),
            Request::builder().uri(format!("/?ra={ra}&dec={dec}&radius_arcsec=600&format=json")),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// The catalog names its own position columns, so a url naming them is refused for the
    /// reason the API's body is: a dropped one tests the rows against columns the caller
    /// did not write, and they cannot tell that from the ones they asked for.
    #[tokio::test]
    async fn a_catalog_url_refuses_the_column_names() {
        let dir = crate::hats_query::tests::fixture(true);
        let (ra, dec) = centre();
        for field in ["ra_column", "dec_column"] {
            let response = respond(
                mounted(dir.path(), &ApiConfig::default()),
                Request::builder().uri(format!(
                    "/?ra={ra}&dec={dec}&radius_arcsec=10&{field}=whatever"
                )),
            )
            .await;
            let status = response.status();
            let body = body_of(response).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{field}: {body}");
            assert!(body.contains(field), "{field}: {body}");
        }
    }

    /// Which directory this is decides whether there is a query surface at all, and it is
    /// decided before any parameter is read.
    ///
    /// So a directory that is not a catalog is listed with its query string ignored, the way
    /// any file server ignores what it has no use for — and a catalog asked nothing this
    /// service reads is listed too, there being no question in it.
    #[tokio::test]
    async fn a_directory_with_no_query_surface_is_listed_parameters_and_all() {
        let catalog = crate::hats_query::tests::fixture(true);
        let plain = tempfile::TempDir::new().unwrap();
        std::fs::write(plain.path().join("part0.parquet"), b"x").unwrap();
        let (ra, dec) = centre();

        for (dir, query) in [
            // A catalog, asked nothing at all, and asked something it has no use for.
            (catalog.path(), ""),
            (catalog.path(), "?v=3"),
            // Not a catalog, asked a search: nothing here answers one, and the listing is
            // what this url has always been.
            (
                plain.path(),
                &*format!("?ra={ra}&dec={dec}&radius_arcsec=10"),
            ),
        ] {
            let response = respond(
                mounted(dir, &ApiConfig::default()),
                Request::builder().uri(format!("/{query}")),
            )
            .await;
            let status = response.status();
            let body = body_of(response).await;
            assert_eq!(status, StatusCode::OK, "{query:?}: {body}");
            let listing: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert!(listing["entries"].is_array(), "{query:?}: {body}");
        }
    }

    /// The front of a catalog, with no circle in it.
    ///
    /// A `limit` is what makes that a bounded question: the partitions are read in the
    /// catalog's own order and the read stops once there are enough rows, so the first few
    /// rows cost the first partition rather than all of them. Without a limit the request
    /// really is the whole catalog, and the partition bound says so before anything is read.
    #[tokio::test]
    async fn a_limit_asks_a_catalog_for_its_front() {
        let dir = crate::hats_query::tests::fixture(true);
        // Tighter than the fixture has partitions, so a request that read them all would be
        // refused and one that stops early is not.
        let service = || {
            with_limits(
                serving(dir.path()),
                &ApiConfig::default(),
                &LimitsConfig {
                    max_partitions: 2,
                    ..LimitsConfig::default()
                },
            )
        };
        let front = || Request::builder().uri("/?limit=3&format=json");

        let response = respond(service(), front()).await;
        let status = response.status();
        let body = body_of(response).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(answer["num_rows"], 3, "{body}");
        assert_eq!(
            answer["num_partitions"], 1,
            "the front of a catalog read past the partition holding it: {body}"
        );

        // The same request twice is the same rows: the catalog's order decides where the
        // read stops, and that does not depend on which partition finished first.
        let again = body_of(respond(service(), front()).await).await;
        let again: serde_json::Value = serde_json::from_str(&again).unwrap();
        assert_eq!(answer["rows"], again["rows"]);

        // With no limit the request is every partition, and that is what the bound is for.
        let response = respond(service(), Request::builder().uri("/?format=json")).await;
        let status = response.status();
        let body = body_of(response).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
        assert!(body.contains("partitions"), "{body}");
    }

    /// One file of a catalog answers the same circle, and needs to be told which columns
    /// hold a position — a parquet file says nothing about that, and the catalog that would
    /// have is not what this url named.
    #[tokio::test]
    async fn a_parquet_url_answers_a_cone_search_when_it_is_told_the_columns() {
        let dir = crate::hats_query::tests::fixture(true);
        let (ra, dec) = centre();
        let file = format!("/{}", hats::HatsPartition::new(3, 64).path(".parquet"));
        let service = || mounted(dir.path(), &ApiConfig::default());

        let response = respond(
            service(),
            Request::builder().uri(format!(
                "{file}?ra={ra}&dec={dec}&radius_arcsec=60&ra_column=ra&dec_column=dec\
                 &columns=id&format=json"
            )),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_of(response).await;
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(answer["num_rows"].as_u64().is_some(), "{body}");

        // Without them there is nothing to test the circle against, and answering every row
        // would be indistinguishable from a circle that held them all.
        let response = respond(
            service(),
            Request::builder().uri(format!("{file}?ra={ra}&dec={dec}&radius_arcsec=60")),
        )
        .await;
        let status = response.status();
        let body = body_of(response).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("ra_column"), "{body}");
    }

    /// A catalog is browsed from the inside, so its page carries the search wherever in it
    /// the reader is standing — and the url the search goes to is the catalog's, not the
    /// directory's.
    #[tokio::test]
    async fn a_catalog_s_page_offers_the_search_from_anywhere_inside_it() {
        let dir = crate::hats_query::tests::fixture(true);
        let plain = tempfile::TempDir::new().unwrap();
        std::fs::create_dir(plain.path().join("Norder=3")).unwrap();

        for (mount, path) in [
            // The catalog's own directory, and every layer of the layout below it.
            (dir.path(), "/"),
            (dir.path(), "/dataset"),
            (dir.path(), "/dataset/Norder=3"),
            (dir.path(), "/dataset/Norder=3/Dir=0"),
        ] {
            let response = respond(
                mounted(mount, &ApiConfig::default()),
                Request::builder()
                    .uri(path)
                    .header(header::ACCEPT, "text/html"),
            )
            .await;
            let body = body_of(response).await;
            assert!(body.contains("data-catalog=\"/\""), "{path}: {body}");
            assert!(body.contains("HATS catalog"), "{path}: {body}");
            // What the page offers without a script, which is the url itself.
            assert!(body.contains("radius_arcsec=10"), "{path}: {body}");
            assert!(body.contains("data-max-radius=\"600\""), "{path}: {body}");
            // The catalog's own word for itself, which a reader inside it cannot see.
            assert!(body.contains("<code>fixture</code>"), "{path}: {body}");
            assert!(body.contains("order 3"), "{path}: {body}");
            // The fixture writes no `_common_metadata`, so no url is offered for one: a
            // catalog without the file gets its columns from the first answer instead of a
            // link to a 404.
            assert!(!body.contains("data-schema"), "{path}: {body}");
        }

        // And where the catalog does have one, that is where the page reads its columns.
        std::fs::create_dir_all(dir.path().join("dataset")).unwrap();
        std::fs::write(dir.path().join("dataset/_common_metadata"), b"x").unwrap();
        let response = respond(
            mounted(dir.path(), &ApiConfig::default()),
            Request::builder()
                .uri("/dataset")
                .header(header::ACCEPT, "text/html"),
        )
        .await;
        let body = body_of(response).await;
        assert!(
            body.contains("data-schema=\"/dataset/_common_metadata\""),
            "{body}"
        );

        // A directory named like one of the layers, with no catalog over it, is an ordinary
        // directory: the walk climbs the layout looking for a catalog and does not assume
        // one from the names alone.
        let response = respond(
            mounted(plain.path(), &ApiConfig::default()),
            Request::builder()
                .uri("/Norder=3")
                .header(header::ACCEPT, "text/html"),
        )
        .await;
        let body = body_of(response).await;
        assert!(!body.contains("data-catalog"), "{body}");
        assert!(!body.contains("HATS catalog"), "{body}");
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
        let fixture = crate::hats_query::tests::fixture(true);
        for name in std::fs::read_dir(fixture.path()).unwrap() {
            let name = name.unwrap().path();
            let to = inside.join(name.file_name().unwrap());
            match name.is_dir() {
                true => copy_tree(&name, &to),
                false => std::fs::copy(&name, &to).map(|_| ()).unwrap(),
            }
        }

        let region = crate::hats_query::tests::regions()[0].clone();
        let (status, plan) = ask_plan(
            mounted(dir.path(), &ApiConfig::default()),
            "/api/v1/expr/hats/plan",
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
        let dir = crate::hats_query::tests::fixture(true);
        let region = crate::hats_query::tests::regions()[1].clone();
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
            // The vocabulary the request was written in, which is the one the entry is
            // written in: a plan a client can send back unchanged has to name a route that
            // reads the fields it carries.
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
        let dir = crate::hats_query::tests::fixture(true);
        let region = crate::hats_query::tests::regions()[1].clone();
        let expected = crate::hats_query::tests::inside(&region);
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
        let dir = crate::hats_query::tests::fixture(true);
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
            query: Simple {
                columns: Some(vec!["id".to_owned()]),
                filters: None,
            },
            region: Some(vec![crate::hats_query::tests::regions()[1].clone()]),
            format: None,
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
        let dir = crate::hats_query::tests::fixture(true);
        let service = mounted(dir.path(), &ApiConfig::default());
        let plan = async |storage: StorageOptions, return_storage: bool| {
            // The plan route's own body: it is the one that has a `return_storage` to set.
            let params = CatalogPlanQuery {
                url: "file:///".to_owned().into(),
                storage,
                query: Simple {
                    columns: Some(vec!["id".to_owned()]),
                    filters: None,
                },
                region: None,
                format: None,
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

    /// A request over more than the server will do comes back as the plan for it, with the
    /// bound that stopped it named.
    #[tokio::test]
    async fn too_much_work_is_answered_with_the_plan_for_it() {
        let dir = crate::hats_query::tests::fixture(true);
        let limits = LimitsConfig {
            max_partitions: 2,
            ..LimitsConfig::default()
        };
        let mounts = Arc::new(Mounts::new(&[serving(dir.path())], &DataConfig::default()).unwrap());
        let policy =
            AccessPolicy::new(&crate::config::AccessConfig::default(), Arc::clone(&mounts))
                .unwrap();
        let service = Service::new(
            policy,
            &limits,
            mounts,
            &ApiConfig::default(),
            &DataConfig::default(),
            &ServerConfig::default(),
        )
        .unwrap();

        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/expr/hats")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({"url": "file:///"}).to_string(),
            ))
            .unwrap();
        let response = router(service).oneshot(request).await.unwrap();
        let status = response.status();
        let plan: serde_json::Value = serde_json::from_str(&body_of(response).await).unwrap();

        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{plan}");
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
        let dir = crate::hats_query::tests::fixture(true);
        let limits = LimitsConfig {
            max_partitions: 2,
            ..LimitsConfig::default()
        };
        let mounts = Arc::new(Mounts::new(&[serving(dir.path())], &DataConfig::default()).unwrap());
        let policy =
            AccessPolicy::new(&crate::config::AccessConfig::default(), Arc::clone(&mounts))
                .unwrap();
        let service = Service::new(
            policy,
            &limits,
            mounts,
            &ApiConfig::default(),
            &DataConfig::default(),
            &ServerConfig::default(),
        )
        .unwrap();

        let (status, body) = ask_hats(service, serde_json::json!({"url": "file:///"})).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
        assert!(body.contains("at most 2"), "{body}");
    }

    /// `serve` is what the file server needs and the API does not, and `path` is the
    /// address for both. So an unserved mount is a 404 to a browser and a readable file
    /// to a query — and the query writes the mount's path, not the disk's.
    #[tokio::test]
    async fn an_unserved_mount_is_read_by_the_api_and_by_nothing_else() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), query::tests::fixture()).unwrap();
        let service = || {
            with_mount(
                crate::config::MountConfig {
                    path: "/staging".to_owned(),
                    serve: false,
                    ..serving(dir.path())
                },
                &ApiConfig::default(),
            )
        };

        // Nothing of it is published: not the file, not the directory it is in.
        for path in ["/staging/part0.parquet", "/staging/", "/staging"] {
            let response = respond(service(), Request::builder().uri(path)).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        }

        // The API reads it, by the mount's path.
        let (status, body) = ask(
            service(),
            serde_json::json!({"url": "file:///staging/part0.parquet", "limit": 1}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        // And not by where it is on the disk, which is not a url this service publishes.
        let on_disk = dir.path().canonicalize().unwrap().join("part0.parquet");
        let (status, body) = ask(
            service(),
            serde_json::json!({"url": Url::from_file_path(&on_disk).unwrap().to_string()}),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    }

    /// One question, and the mount's own list answers it for both routes: the file
    /// server decides whether a query string means anything, and the API decides whether
    /// the url names data at all.
    #[tokio::test]
    async fn a_mount_s_own_filenames_govern_both_routes() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.pq"), query::tests::fixture()).unwrap();
        // `*.pq` is on `[data] filenames` by default, and this mount takes it off.
        let service = || {
            with_mount(
                crate::config::MountConfig {
                    filenames: Some(vec!["*.parquet".to_owned()]),
                    ..serving(dir.path())
                },
                &ApiConfig::default(),
            )
        };

        // The file server hands the bytes over and ignores a query it has no use for.
        let response = respond(
            service(),
            Request::builder().uri("/part0.pq?columns=objectid&format=json"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_ne!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            PARQUET_CONTENT_TYPE
        );

        // The API has nothing to say about it, and says which names it would have read.
        let (status, body) = ask(
            service(),
            serde_json::json!({"url": "file:///part0.pq", "limit": 1}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert!(body.contains("*.parquet"), "{body}");
    }

    /// The two modes have to divide the url space between them, and a configuration
    /// where they do not is a startup error rather than a route nothing reaches.
    #[test]
    fn a_url_space_that_serves_nothing_is_a_startup_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let service = |mount_path: &str, serve: bool, api_enabled: bool| {
            let api = ApiConfig {
                enabled: api_enabled,
                ..Default::default()
            };
            let mounts = Mounts::new(
                &[crate::config::MountConfig {
                    path: mount_path.to_owned(),
                    serve,
                    ..serving(dir.path())
                }],
                &DataConfig::default(),
            )
            .unwrap();
            Service::new(
                AccessPolicy::default(),
                &LimitsConfig::default(),
                Arc::new(mounts),
                &api,
                &DataConfig::default(),
                &ServerConfig::default(),
            )
        };

        // A mount inside the API's subtree is one no request could reach. `path` is the
        // mount's address in both modes, so an unserved one is no more reachable there
        // than a served one.
        for serve in [true, false] {
            let error = service("/api/v1/hats", serve, true)
                .unwrap_err()
                .to_string();
            assert!(error.contains("/api/v1"), "serve={serve}: {error}");
        }
        // The same directory one level up is the expected arrangement.
        assert!(service("/hats", true, true).is_ok());
        // The API off, with a served mount, is a file server.
        assert!(service("/api/v1/hats", true, false).is_ok());
        // The API off, and a mount only the API could have read, serves nothing.
        let error = service("/hats", false, false).unwrap_err().to_string();
        assert!(error.contains("nothing to serve"), "{error}");

        // And the API off with nothing mounted, likewise.
        let error = Service::new(
            AccessPolicy::default(),
            &LimitsConfig::default(),
            Arc::default(),
            &ApiConfig {
                enabled: false,
                ..Default::default()
            },
            &DataConfig::default(),
            &ServerConfig::default(),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("nothing to serve"), "{error}");
    }

    /// A field of the other vocabulary is refused, and the refusal names the fields this
    /// route does take, so the caller can see which of the two they wrote.
    ///
    /// This is the whole of what the split buys, and it is the one thing serde cannot say
    /// for us: `deny_unknown_fields` is ignored on a struct with a flattened field, so
    /// without the catch-all a `filters` sent here would be dropped and every row returned —
    /// which the caller cannot tell from a predicate that matched them all.
    #[tokio::test]
    async fn a_field_of_the_other_vocabulary_is_refused_and_placed() {
        for (route, field, takes) in [
            ("/api/v1/expr/parquet", "columns", "select"),
            ("/api/v1/expr/parquet", "filters", "where"),
            ("/api/v1/simple/parquet", "select", "columns"),
            ("/api/v1/simple/parquet", "where", "filters"),
        ] {
            let (status, body) = post_json(
                api_only(),
                route,
                serde_json::json!({"url": "s3://b/k.parquet", field: "objectid"}),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{route} {field}: {body}");
            assert!(body.contains(field), "{route} {field}: {body}");
            assert!(body.contains(takes), "{route} {field}: {body}");
        }
    }

    /// `columns` is a list in a body, and the refusal says so when it is handed the comma
    /// separated text a url carries.
    ///
    /// A client moving a url's parameters into a body would otherwise be asking for a column
    /// named `objectid, band` — and the refusal has to name the shape itself, since serde's
    /// own message says it beside the value it quotes, which is the one thing this service
    /// does not repeat back.
    #[tokio::test]
    async fn the_simple_vocabulary_takes_a_list_of_columns() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), query::tests::fixture()).unwrap();
        let ask = async |body| {
            post_json(
                mounted(dir.path(), &ApiConfig::default()),
                "/api/v1/simple/parquet",
                body,
            )
            .await
        };

        let (status, body) = ask(serde_json::json!({
            "url": "file:///part0.parquet",
            "columns": ["objectid", "band"],
            "filters": "objectid < 3 AND band = 'g'",
        }))
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let answer: serde_json::Value = serde_json::from_str(&body).unwrap();
        let names = answer["schema"]
            .as_array()
            .unwrap()
            .iter()
            .map(|column| column["name"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(names, ["objectid", "band"], "{body}");
        // The columns came back in the order they were named, and the condition held.
        for row in answer["rows"].as_array().unwrap() {
            assert!(row["objectid"].as_i64().unwrap() < 3, "{row}");
            assert_eq!(row["band"], "g", "{row}");
        }

        // The url's spelling of the same request, sent to the body's route.
        let (status, body) = ask(serde_json::json!({
            "url": "file:///part0.parquet",
            "columns": "objectid, band",
        }))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("list of names"), "{body}");
    }

    /// A field of no vocabulary at all is named too, rather than ignored.
    #[tokio::test]
    async fn an_unknown_field_is_named_rather_than_dropped() {
        let (status, body) = select_with(serde_json::json!({
            "url": "s3://b/k.parquet",
            "wehre": "objectid = 1",
        }))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("wehre"), "{body}");
    }

    /// Each route's description offers exactly the fields that route takes.
    ///
    /// This is what the request types are for: `ra_column` is not a catalog's to refuse
    /// because it is not a catalog's to send, and the document says so rather than saying
    /// it in prose beside a field that is there. The lists below are the endpoints' own
    /// contracts, so a field added to one type and meant for another fails here.
    #[test]
    fn each_route_describes_its_own_body() {
        let document = serde_json::to_value(describe("/api/v1")).unwrap();
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
        // in and is stated by hand, so this is what holds it. What to read, how to reach it,
        // where on the sky, what to ask of it, and last how the answer comes back.
        for (vocabulary, projection, predicate) in [
            ("expr", "select", "where"),
            ("simple", "columns", "filters"),
        ] {
            let common = [
                "url", "storage", "region", projection, predicate, "format", "limit",
            ];
            assert_eq!(
                fields(&format!("/api/v1/{vocabulary}/parquet")),
                [
                    &["url", "storage", "region"][..],
                    &["ra_column", "dec_column", "healpix_column", "healpix_order"],
                    &[projection, predicate, "format", "limit"],
                ]
                .concat(),
            );
            assert_eq!(fields(&format!("/api/v1/{vocabulary}/hats")), common);
            assert_eq!(
                fields(&format!("/api/v1/{vocabulary}/hats/plan")),
                [common.as_slice(), &["return_storage"]].concat(),
            );
        }
    }

    /// Every `$ref` in the description names a component the description carries.
    ///
    /// The page and any generated client both resolve these, and a dangling one is a
    /// component that was flattened into its users and removed while something still
    /// pointed at it — which reads as a body with no fields rather than as an error.
    #[test]
    fn the_description_refers_to_nothing_it_does_not_carry() {
        let document = serde_json::to_value(describe("/api/v1")).unwrap();
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

    /// A query string on a mounted parquet file is a question about it, and the answer
    /// is a parquet file — the same media type the path serves without one.
    #[tokio::test]
    async fn a_mounted_parquet_file_answers_a_query() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), query::tests::fixture()).unwrap();
        let service = || mounted(dir.path(), &ApiConfig::default());

        // vizcat's two parameter names, with `&&` sent the way a query string requires.
        let response = respond(
            service(),
            Request::builder().uri(
                "/part0.parquet?columns=objectid,band&filters=objectid%3C3%20%26%26%20band%3D'g'",
            ),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            PARQUET_CONTENT_TYPE
        );
        assert_eq!(response.headers()[NUM_ROWS_HEADER], "2");
        // A parquet body has no room for the counts, so they are headers here and fields
        // in the JSON below — the same numbers either way.
        let scanned: u64 = response.headers()[DATA_BYTES_READ_HEADER]
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        assert!(scanned > 0);

        // And the same question answered as rows, for a client that wants them.
        let response = respond(
            service(),
            Request::builder().uri("/part0.parquet?filters=objectid=1&format=json"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_str(&body_of(response).await).unwrap();
        assert_eq!(body["num_rows"], 1);
        assert!(body["data_bytes_read"].as_u64().unwrap() > 0, "{body}");
        assert_eq!(body["rows"][0]["objectid"], 1);
    }

    /// A query that cannot run says so. Telling a caller their file is not parquet, when
    /// what is wrong is the predicate they wrote, sends them to look at the one thing that
    /// is not the matter — and the file's own path must not come back with the message
    /// either, which is why every failure against a mounted file used to be flattened
    /// into one sentence.
    #[tokio::test]
    async fn a_query_that_cannot_run_is_not_reported_as_a_bad_file() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), query::tests::fixture()).unwrap();

        // An Int64 column against a string: nothing is wrong with the file, and the
        // planner is the only thing that can say what is wrong with the query.
        let response = respond(
            mounted(dir.path(), &ApiConfig::default()),
            Request::builder().uri("/part0.parquet?filters=objectid%20%3D%20'x'&format=json"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = body_of(response).await;
        assert!(!body.contains("not a parquet file"), "{body}");
        assert!(body.contains("Int64"), "{body}");
        assert!(
            !body.contains(&dir.path().display().to_string()),
            "leaked a local path: {body}"
        );
    }

    /// A parquet file with no rows in it is a parquet file, and an empty answer is the
    /// right answer about it — in either format, with or without a predicate. Only a file
    /// that cannot be read as parquet at all is the caller's mistake, which is what the
    /// zero-byte case above is: the two are one line apart in the code and nothing in the
    /// answer would tell them apart.
    #[tokio::test]
    async fn a_parquet_file_with_no_rows_is_answered_rather_than_refused() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("no_rows.parquet"),
            query::tests::fixture_of(0),
        )
        .unwrap();

        for uri in [
            "/no_rows.parquet?format=json",
            // The default format here, so the parquet writer answers with an empty file
            // rather than refusing to write one.
            "/no_rows.parquet",
            "/no_rows.parquet?filters=objectid%3E0&format=json",
            "/no_rows.parquet?columns=band&format=parquet",
        ] {
            let service = mounted(dir.path(), &ApiConfig::default());
            let response = respond(service, Request::builder().uri(uri)).await;
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
        }

        // And it still describes itself, which is what an empty file cannot do.
        let service = mounted(dir.path(), &ApiConfig::default());
        let response = respond(
            service,
            Request::builder().uri("/no_rows.parquet?format=json"),
        )
        .await;
        let body: serde_json::Value = serde_json::from_str(&body_of(response).await).unwrap();
        assert_eq!(body["num_rows"], 0);
        assert_eq!(body["schema"][0]["name"], "objectid");
    }

    /// Rows do not describe themselves, so the answer says what its columns are — which
    /// is the only thing a request matching no row has to say, and what makes `limit=0`
    /// a description of a file rather than an empty answer.
    #[tokio::test]
    async fn an_answer_says_what_its_columns_are() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), query::tests::fixture()).unwrap();
        let asked = |uri: &'static str| {
            let service = mounted(dir.path(), &ApiConfig::default());
            async move {
                let response = respond(service, Request::builder().uri(uri)).await;
                assert_eq!(response.status(), StatusCode::OK);
                serde_json::from_str::<serde_json::Value>(&body_of(response).await).unwrap()
            }
        };

        let described = asked("/part0.parquet?limit=0&format=json").await;
        assert_eq!(described["num_rows"], 0);
        // And it costs no data: asking what a file holds is not reading it.
        assert_eq!(described["data_bytes_read"], 0);
        assert_eq!(described["schema"][0]["name"], "objectid");
        assert_eq!(described["schema"][0]["type"], "Int64");

        // A projection narrows it, so what comes back describes the answer rather than
        // the file.
        let projected = asked("/part0.parquet?columns=band&limit=1&format=json").await;
        assert_eq!(projected["schema"].as_array().unwrap().len(), 1);
        assert_eq!(projected["schema"][0]["name"], "band");
    }

    /// A VOTable comes back as one, with its counts where a body that is not JSON has to
    /// put them — and a nested column is refused by name rather than dropped from the
    /// answer, which is the half of this format that is not built yet.
    #[tokio::test]
    async fn a_votable_answer_is_a_document_with_its_counts_in_the_headers() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), query::tests::fixture()).unwrap();
        std::fs::write(
            dir.path().join("nested.parquet"),
            query::tests::nested_fixture(),
        )
        .unwrap();

        let response = respond(
            mounted(dir.path(), &ApiConfig::default()),
            Request::builder().uri("/part0.parquet?columns=objectid,band&limit=2&format=votable"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let headers = response.headers().clone();
        assert_eq!(headers[header::CONTENT_TYPE], votable::CONTENT_TYPE);
        // Named after the partition it came from, and not as the parquet file it is not.
        assert_eq!(
            headers[header::CONTENT_DISPOSITION],
            "attachment; filename=\"part0.vot\""
        );
        assert_eq!(headers[NUM_ROWS_HEADER], "2");
        let body = body_of(response).await;
        assert!(body.contains("<VOTABLE version=\"1.4\""), "{body}");
        assert!(
            body.contains("<FIELD name=\"objectid\" datatype=\"long\"/>"),
            "{body}"
        );
        assert!(body.contains("<TD>0</TD><TD>g</TD>"), "{body}");

        // The whole column, said by name: a caller who cannot tell which column stopped
        // the request cannot narrow their way past it.
        let response = respond(
            mounted(dir.path(), &ApiConfig::default()),
            Request::builder().uri("/nested.parquet?format=votable"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = body_of(response).await;
        assert!(body.contains("sources"), "{body}");
        assert!(body.contains("nested"), "{body}");
    }

    /// A struct column names its own fields, and the name that is built out of them
    /// resolves.
    ///
    /// Both halves matter and neither is enough alone. A HATS catalog packs a light curve
    /// into a struct, so the column is `sources` and what a reader wants is `sources.mjd`;
    /// the names are in the type string, but reading them off it means parsing arrow's
    /// `Display`, so they are named in `fields` instead. And a field's name is its own
    /// rather than the path: the parts are quoted one at a time, because `"sources"."mjd"`
    /// names the field while `"sources.mjd"` names a column no file has got.
    #[tokio::test]
    async fn a_struct_column_names_its_fields_and_they_can_be_asked_for() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("part0.parquet"),
            query::tests::nested_fixture(),
        )
        .unwrap();
        let asked = |uri: String| {
            let service = mounted(dir.path(), &ApiConfig::default());
            async move {
                let response = respond(service, Request::builder().uri(uri.as_str())).await;
                assert_eq!(response.status(), StatusCode::OK);
                serde_json::from_str::<serde_json::Value>(&body_of(response).await).unwrap()
            }
        };

        let described = asked("/part0.parquet?limit=0&format=json".to_owned()).await;
        let columns = described["schema"].as_array().unwrap();
        // A scalar names no fields at all rather than an empty list, which would read as a
        // struct that has none.
        assert_eq!(columns[0]["name"], "objectid");
        assert!(columns[0]["fields"].is_null(), "{described}");
        assert_eq!(columns[1]["name"], "sources");
        let fields = columns[1]["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0]["name"], "mjd");
        assert_eq!(fields[1]["name"], "band");
        // One level: a field lists no fields of its own.
        assert!(fields[0]["fields"].is_null(), "{described}");

        // The spelling the page builds out of those two names, quoted a part at a time. It
        // comes back as the column it names into, holding the one field that was asked for:
        // a row's light curve is one value, and this asked for less of that value rather
        // than for a column beside it.
        let picked =
            asked("/part0.parquet?columns=%22sources%22.%22mjd%22&limit=1&format=json".to_owned())
                .await;
        assert_eq!(picked["schema"].as_array().unwrap().len(), 1);
        assert_eq!(picked["schema"][0]["name"], "sources");
        let packed = picked["schema"][0]["fields"].as_array().unwrap();
        assert_eq!(packed.len(), 1, "{picked}");
        assert_eq!(packed[0]["name"], "mjd");
        assert_eq!(picked["rows"][0]["sources"]["mjd"][0], 0.0);

        // And the path quoted whole is a different name, which the file has not got. It is
        // refused rather than answered, so a page that got this wrong could not look right.
        let service = mounted(dir.path(), &ApiConfig::default());
        let response = respond(
            service,
            Request::builder().uri("/part0.parquet?columns=%22sources.mjd%22&limit=1&format=json"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
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

    /// The point of taking the parameter names rather than the behaviour: a predicate
    /// that cannot run is refused, never dropped. A caller cannot tell an ignored filter
    /// from one that matched every row.
    #[tokio::test]
    async fn a_filter_that_cannot_run_is_refused_rather_than_ignored() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), query::tests::fixture()).unwrap();
        let service = || mounted(dir.path(), &ApiConfig::default());

        for uri in [
            "/part0.parquet?filters=nosuchcolumn%3E0",
            "/part0.parquet?filters=this%20is%20not%20sql",
            "/part0.parquet?columns=nosuchcolumn",
            // A name, not an expression: that is what `select` is for.
            "/part0.parquet?columns=objectid%20-%201",
            "/part0.parquet?limit=lots",
        ] {
            let response = respond(service(), Request::builder().uri(uri)).await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        }
    }

    /// A file server ignores a parameter it has no use for, and a file with nothing but
    /// such parameters on its url is still a download.
    #[tokio::test]
    async fn an_unrecognised_parameter_is_ignored() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), b"0123456789").unwrap();

        let response = respond(
            mounted(dir.path(), &ApiConfig::default()),
            // Not a parquet file at all, so anything that read it as one would fail.
            Request::builder().uri("/part0.parquet?v=3&_=1712345678"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_of(response).await, "0123456789");
    }

    /// A file that is not a data file has no query surface, and a file server that has
    /// no use for a parameter still has the bytes: it goes out whole, parameters and
    /// all. A directory is the same case — a listing takes no parameters of its own.
    #[tokio::test]
    async fn a_file_that_is_not_data_is_served_rather_than_queried() {
        let dir = tree();
        std::fs::write(dir.path().join("notes.txt"), b"plain").unwrap();
        let service = || mounted(dir.path(), &ApiConfig::default());

        let response = respond(
            service(),
            Request::builder().uri("/notes.txt?columns=objectid&filters=x%3E1"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_of(response).await, "plain");

        // `properties` sits beside a catalog's partitions and is not one of them.
        let response = respond(service(), Request::builder().uri("/properties?columns=x")).await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = respond(service(), Request::builder().uri("/?columns=objectid")).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(body_of(response).await.contains("Norder=5"));
    }

    /// The names a HATS catalog actually uses, which is why the list is names rather
    /// than suffixes: `_metadata` and `_common_metadata` have no extension at all, and
    /// DataFusion's own reader filters on `.parquet` unless told otherwise.
    #[tokio::test]
    async fn every_name_on_the_list_answers_a_query() {
        let dir = tempfile::TempDir::new().unwrap();
        let names = [
            "_metadata",
            "_common_metadata",
            "part0.parq",
            "part0.parquet",
        ];
        for name in names {
            std::fs::write(dir.path().join(name), query::tests::fixture()).unwrap();
        }
        let service = || mounted(dir.path(), &ApiConfig::default());

        for name in names {
            let response = respond(
                service(),
                Request::builder().uri(format!("/{name}?columns=objectid&format=json")),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK, "{name}");
            let body: serde_json::Value = serde_json::from_str(&body_of(response).await).unwrap();
            assert_eq!(body["num_rows"], 10, "{name}");
        }
    }

    /// The list is the operator's, so a mount of something else is served by naming it.
    #[tokio::test]
    async fn the_list_of_data_files_is_configurable() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.pq"), query::tests::fixture()).unwrap();
        let data = DataConfig {
            filenames: vec!["*.pq".to_owned()],
        };
        let mounts = Arc::new(Mounts::new(&[serving(dir.path())], &data).unwrap());
        let policy =
            AccessPolicy::new(&crate::config::AccessConfig::default(), Arc::clone(&mounts))
                .unwrap();
        let service = Service::new(
            policy,
            &LimitsConfig::default(),
            mounts,
            &ApiConfig::default(),
            &data,
            &ServerConfig::default(),
        )
        .unwrap();

        let response = respond(
            service,
            Request::builder().uri("/part0.pq?columns=objectid&format=json"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&body_of(response).await).unwrap()["num_rows"],
            10
        );
    }

    /// A parquet footer describing data that is not in this file, which is the shape of a
    /// HATS `_metadata`: its row groups are the ones in the partition files beside it.
    ///
    /// A file with the shape of a HATS `_metadata`: a valid footer whose row groups
    /// describe data that is not in this file, because there the data is in the partition
    /// files beside it.
    ///
    /// Built by keeping a file's footer and dropping the data it points at, so the reader
    /// parses the metadata, believes there are rows, and asks the store for a range past
    /// the end. That comes back wrapped in the store's own error, which is a different
    /// arm of the status match from a footer that will not parse at all — and the arm
    /// that answered `502` with the store's path in it.
    ///
    /// The source has to have more data than footer: the recorded offsets are what must
    /// end up beyond the stripped file's length, and a ten-row file's footer is bigger
    /// than its data.
    fn metadata_only(file: &[u8]) -> Vec<u8> {
        const MAGIC: &[u8] = b"PAR1";
        // `… metadata | u32 length | PAR1`, so the length sits in the four bytes before
        // the trailing magic, and the metadata is that many bytes before those.
        let length_at = file.len() - MAGIC.len() - size_of::<u32>();
        let length = u32::from_le_bytes(
            file[length_at..length_at + size_of::<u32>()]
                .try_into()
                .unwrap(),
        ) as usize;
        let mut stripped = MAGIC.to_vec();
        stripped.extend_from_slice(&file[length_at - length..]);
        stripped
    }

    /// A name on the list whose bytes are not parquet: the reader is what decides, so
    /// this is the caller's file being wrong rather than this service failing.
    #[tokio::test]
    async fn a_data_file_that_is_not_parquet_is_the_callers_mistake() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("liar.parquet"), b"not parquet at all").unwrap();
        std::fs::write(dir.path().join("empty.parquet"), b"").unwrap();
        std::fs::write(
            dir.path().join("metadata_only.parquet"),
            metadata_only(&query::tests::fixture_of(5000)),
        )
        .unwrap();
        let service = || mounted(dir.path(), &ApiConfig::default());

        for uri in [
            "/liar.parquet?columns=objectid",
            "/empty.parquet?limit=1",
            "/metadata_only.parquet?limit=1",
            // The same three as rows rather than as parquet. The refusal belongs to the
            // file and not to what was asked of it: reading a file's footer to copy its
            // layout is what catches an empty one on the parquet path, and a JSON answer
            // never does that — so this is where that path would quietly answer "no
            // rows" for a file that is not a parquet file at all.
            "/liar.parquet?columns=objectid&format=json",
            "/empty.parquet?limit=1&format=json",
            "/metadata_only.parquet?limit=1&format=json",
        ] {
            let response = respond(service(), Request::builder().uri(uri)).await;
            // A mount has no origin behind it, so `502` would be this service blaming a
            // gateway that does not exist for a file it published itself.
            let status = response.status();
            // Whatever is on disk is the operator's business, and a refusal is where a
            // path would otherwise get written into a message.
            let body = body_of(response).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}: {body}");
            assert!(
                !body.contains(&dir.path().display().to_string()),
                "{uri} leaked a local path: {body}"
            );
        }
        // And without a query they are still ordinary files.
        let response = respond(service(), Request::builder().uri("/liar.parquet")).await;
        assert_eq!(response.status(), StatusCode::OK);

        // The same three through the API, which reaches the same files by the mount's
        // path. Every one of these messages is raised by a store or a reader that knows
        // only where the file is on the disk, so this is where that would be repeated.
        for name in ["liar.parquet", "empty.parquet", "metadata_only.parquet"] {
            for format in ["parquet", "json"] {
                let (status, body) = ask(
                    service(),
                    serde_json::json!({
                        "url": format!("file:///{name}"),
                        "format": format,
                        "limit": 1,
                    }),
                )
                .await;
                assert_eq!(
                    status,
                    StatusCode::BAD_REQUEST,
                    "{name} as {format}: {body}"
                );
                assert!(
                    !body.contains(&dir.path().display().to_string()),
                    "{name} as {format} leaked a local path: {body}"
                );
            }
        }
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
            let (status, body) = select_with(serde_json::json!({"url": url})).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{url}");
            // The refusal says what would have been read, so a caller can see why.
            assert!(body.contains("*.parquet"), "{url}: {body}");
        }
    }

    /// Nothing here is written, whichever shape the request took.
    #[tokio::test]
    async fn a_query_is_not_written_to() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), query::tests::fixture()).unwrap();

        let response = respond(
            mounted(dir.path(), &ApiConfig::default()),
            Request::builder()
                .method("POST")
                .uri("/part0.parquet?columns=objectid"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn unparseable_urls_are_rejected() {
        let (status, body) = select_with(serde_json::json!({"url": "not-a-url"})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("invalid url"), "{body}");
    }
}
