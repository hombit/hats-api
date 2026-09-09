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
use serde::{Deserialize, Serialize};
use tower_http::services::ServeFile;
use tower_http::trace::TraceLayer;
// The query-string reading of percent encoding, which is not the path's: `+` is a space
// here and is a literal `+` in a path segment.
use url::{Url, form_urlencoded};

use crate::access::{self, AccessPolicy};
use crate::config::{ApiConfig, ConfigError, DataConfig, LimitsConfig, ServerConfig};
use crate::data::DataFiles;
use crate::error::ApiError;
use crate::hats_query::{CatalogLimits, CatalogSelection, Exceeded, Outcome, Search};
use crate::healpix::Cover;
use crate::listing::{self, Listing};
use crate::materialize::Transfers;
use crate::mount::{self, Mount, Mounts};
use crate::parquet_out;
use crate::query::{self, Order, Predicate, Projection, QueryResult, Selection};
use crate::region::{Healpix, Region, Spatial};
use crate::sql;
use crate::storage::{self, RemoteFile, SourceUrl, StorageOptions, parse_url};

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
        router = router
            .route(&route(&prefix, "health"), get(health))
            // The path names the target, and the predicate never appears in it: a
            // spatial constraint is one clause of a query, so a `{target}/{predicate}`
            // path set would grow as the product of the predicate kinds rather than
            // their sum.
            //
            // `POST`, not `GET`: the request carries credentials, and a query string is
            // written to every proxy's access log and the caller's shell history on the
            // way. A body also has no url-length limit — a long `IN` list and a wide
            // select list both run past nginx's 8 KB header buffer — and needs no url
            // nested inside a url.
            .route(&route(&prefix, "parquet"), post(query_parquet))
            // The same body, against a catalog instead of a file: the url names a HATS
            // directory and this chooses the partitions to read out of it.
            .route(&route(&prefix, "hats"), post(query_hats))
            // The same body again, resolved and not run. Two routes rather than one with a
            // mode: rows and a work list are different kinds of thing, and a field saying
            // which arrived is one more value a caller has to look at the body to trust.
            .route(&route(&prefix, "hats/plan"), post(query_hats_plan));
    }
    router
        // Mounts claim whatever the API's routes did not, so a mount at `/` and the API
        // at `/api/v1` divide the url space without either being nested in the other.
        .fallback(serve_mounted)
        .with_state(service)
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
    let mut requested = mount.source().to_owned();
    requested.extend(&segments);
    let mut file = access::authorize_mounted(mount, &requested)?;
    if file.is_dir() {
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
        && let Some(query) = FileQuery::parse(parts.uri.query().unwrap_or_default())?
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
#[derive(Debug, Default)]
struct FileQuery {
    columns: Option<String>,
    filters: Option<String>,
    format: Option<String>,
    limit: Option<String>,
}

impl FileQuery {
    /// `None` when the query string asks nothing this service answers, which is what
    /// keeps a file with a cache-buster on its url an ordinary download.
    ///
    /// Anything unrecognised is ignored rather than refused, the way an ordinary HTTP
    /// server ignores what it has no use for. The last of a repeated parameter wins,
    /// which is what a browser and a form both produce.
    fn parse(raw: &str) -> Result<Option<Self>, ApiError> {
        let mut query = Self::default();
        let mut asked = false;
        for (name, value) in form_urlencoded::parse(raw.as_bytes()) {
            let field = match name.as_ref() {
                "columns" => &mut query.columns,
                "filters" => &mut query.filters,
                "format" => &mut query.format,
                "limit" => &mut query.limit,
                _ => continue,
            };
            *field = Some(value.into_owned());
            asked = true;
        }
        match asked {
            true => Ok(Some(query)),
            false => Ok(None),
        }
    }

    fn selection(&self) -> Result<Selection<'_>, ApiError> {
        Ok(Selection {
            projection: match self.columns.as_deref() {
                Some(list) => Projection::Columns(list),
                None => Projection::All,
            },
            predicate: match self.filters.as_deref() {
                Some(text) => Predicate::Filters(text),
                None => Predicate::All,
            },
            // Spatial selection here is by path — a request names `Norder=k/Npix=p`
            // itself. A caller who wants the catalog to choose partitions uses the API
            // against the same data.
            spatial: None,
            limit: match self.limit.as_deref() {
                // Said as a number rather than left to mean "no limit": a caller who
                // wrote one and got every row would have no way to notice.
                Some(raw) => Some(raw.parse().map_err(|_| {
                    ApiError::bad_request("limit takes a number of rows".to_owned())
                })?),
                None => None,
            },
        })
    }
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
    // `read_dir` and a `stat` per entry are blocking calls, and a HATS `Dir=` level is
    // ten thousand of them.
    let listing =
        tokio::task::spawn_blocking(move || Listing::read(&dir, &root, &path, follow_symlinks))
            .await
            .map_err(|error| {
                tracing::error!(%error, "listing a directory panicked");
                ApiError::internal("cannot read this directory")
            })?
            .map_err(|error| {
                // The path is the operator's business and not the caller's, so what comes back
                // is the same answer as for a directory that is not published at all.
                tracing::warn!(%error, mount = mount.prefix(), "cannot list");
                ApiError::not_found("no such directory")
            })?;

    Ok(match listing::wants_html(&request.headers) {
        true => Html(listing.to_html(
            mount.data_files(),
            service.api_prefix.as_deref(),
            service.show_version,
        ))
        .into_response(),
        false => Json(listing).into_response(),
    })
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

async fn health() -> (StatusCode, Json<HealthResponse>) {
    (StatusCode::OK, Json(HealthResponse { status: "ok" }))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryRequest {
    /// Where the data is, treated as opaque: whatever query string it has belongs to
    /// the origin, not to us.
    url: SourceUrl,
    /// How to reach the store — a region, an endpoint, credentials. Absent means a
    /// public object read anonymously, which is the common case.
    #[serde(default)]
    storage: StorageOptions,
    /// The projection, as a SQL select list, so `mag - 0.1 AS mag_corr` works. Absent
    /// returns every column.
    select: Option<String>,
    /// One boolean SQL expression over this file's columns. Absent returns every row.
    r#where: Option<String>,
    /// The projection as plain column names, which is what a file-server client writes.
    /// The narrower of the two: it takes names, never expressions.
    columns: Option<String>,
    /// The row predicate in the same vocabulary, which additionally spells `AND` as
    /// `&&`.
    filters: Option<String>,
    /// A shape on the sky, or several. A row inside any of them qualifies — the array is
    /// a union — and the whole field is conjoined with the predicate.
    region: Option<Vec<Region>>,
    /// Which columns hold the position a `region` is tested against. Required alongside
    /// one: a parquet file carries nothing that says which of its columns are a position.
    ra_column: Option<String>,
    dec_column: Option<String>,
    /// The file's HEALPix index column, and the order its values are at — `_healpix_29`
    /// and 29 for a HATS catalog that took the recommendation. Optional, and an
    /// accelerator only: they change what a query costs and never which rows come back.
    healpix_column: Option<String>,
    healpix_order: Option<u8>,
    /// `json` (the default) or `parquet`.
    format: Option<String>,
    /// Most rows to return.
    limit: Option<usize>,
    /// Whether a plan's entries carry `storage` — this request's own, credentials included
    /// — so that they can be sent as they stand.
    ///
    /// **Off unless asked for, and only ever the caller's own secret handed back to them.**
    /// It enables nothing they cannot already do: they sent it. What it costs is that the
    /// plan becomes a document with a credential in it, and a plan is the sort of thing that
    /// gets logged, cached and pasted into an issue. So the default writes the stripped url
    /// and `requires_credentials`, and a client that would rather re-attach them itself —
    /// which is most of them — never has to think about it.
    #[serde(default)]
    return_storage: bool,
}

impl QueryRequest {
    /// The one pair of fields this request actually used.
    ///
    /// Both pairs say the same thing, so a caller may write either. Not both: a body
    /// carrying `select` and `columns` together is one written by someone who thinks
    /// they differ, and quietly picking either would be answering the question they got
    /// wrong.
    fn selection(&self) -> Result<Selection<'_>, ApiError> {
        let (projection, predicate) = self.expressions()?;
        Ok(Selection {
            projection,
            predicate,
            spatial: self.spatial()?,
            limit: self.limit,
        })
    }

    /// The projection and the predicate, in whichever of the two vocabularies was used.
    ///
    /// Both routes read them the same way, which is the point: a field means one thing
    /// whichever route the request arrived on, and there is one place that decides what.
    fn expressions(&self) -> Result<(Projection<'_>, Predicate<'_>), ApiError> {
        let projection = match (self.select.as_deref(), self.columns.as_deref()) {
            (Some(_), Some(_)) => return Err(one_of_two("select", "columns")),
            (Some(sql), None) => Projection::Select(sql),
            (None, Some(list)) => Projection::Columns(list),
            (None, None) => Projection::All,
        };
        let predicate = match (self.r#where.as_deref(), self.filters.as_deref()) {
            (Some(_), Some(_)) => return Err(one_of_two("where", "filters")),
            (Some(sql), None) => Predicate::Where(sql),
            (None, Some(text)) => Predicate::Filters(text),
            (None, None) => Predicate::All,
        };
        Ok((projection, predicate))
    }

    /// The four column names a catalog answers for itself, refused rather than honoured.
    ///
    /// `hats_col_ra`, `hats_col_dec` and `hats_col_healpix` are the catalog's statement
    /// about its own files, and it can see more of them than a caller can. Accepting an
    /// override would let a request test a pair of columns the catalog does not call a
    /// position and get an answer that looks like a cone search.
    ///
    /// They are refused rather than ignored: a parameter this service acts on is honoured or
    /// refused, and one silently dropped here returns rows tested against different columns
    /// than the caller wrote — which they could not tell from the ones they asked for. A
    /// caller who does want their own pair names the file, where the route takes them.
    fn refuse_catalog_columns(&self) -> Result<(), ApiError> {
        let named = [
            ("ra_column", self.ra_column.is_some()),
            ("dec_column", self.dec_column.is_some()),
            ("healpix_column", self.healpix_column.is_some()),
            ("healpix_order", self.healpix_order.is_some()),
        ]
        .into_iter()
        .filter_map(|(name, given)| given.then_some(name))
        .collect::<Vec<_>>();
        match named.is_empty() {
            true => Ok(()),
            false => Err(ApiError::bad_request(format!(
                "{} not accepted against a catalog, which names its own columns; \
                 to choose them, query one of its files",
                named.join(", ")
            ))),
        }
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

/// Half of the pair is not half a request. The column may be called anything and be written
/// at any order, so the order is what says which cell a value names — and reading a column
/// at the wrong order puts every bound where no row is, which returns nothing rather than
/// failing. None of which the caller needs; they need to send the other field.
const HEALPIX_PAIR: &str = "healpix_column and healpix_order must be given together";

fn one_of_two(one: &str, other: &str) -> ApiError {
    ApiError::bad_request(format!("send {one} or {other}, not both"))
}

/// What the caller wants back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Format {
    Json,
    Parquet,
}

impl Format {
    /// Every format, in the order a refusal lists them. The default is the first.
    const ALL: [Self; 2] = [Self::Json, Self::Parquet];

    /// The one place a format's name is written. [`Self::parse`] and the list in a
    /// refusal are both derived from it, so a format cannot be renamed in one and not
    /// the others, or added and left unparseable.
    fn name(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Parquet => "parquet",
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
fn body_error(rejection: &JsonRejection) -> ApiError {
    const SHAPE: &str = "expected a JSON object with url, and optionally storage, \
                         select or columns, where or filters, region with ra_column and \
                         dec_column, format, limit";

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
                false => ApiError::bad_request(format!("the request body does not fit: {SHAPE}")),
            }
        }
        _ => ApiError::bad_request(format!("{}; {SHAPE}", rejection.body_text())),
    }
}

/// One column of the answer, as the schema describes it.
///
/// What it is for is that rows do not describe themselves. An answer that matched
/// nothing, and one asked for with `limit=0`, are the same shape as a file that has not
/// got the column — so a caller reading column names off the first row learns nothing
/// from either. The type is arrow's own spelling, which is what says whether a value
/// needs quoting in a predicate.
#[derive(Debug, Serialize)]
struct Column {
    name: String,
    r#type: String,
}

/// A catalog's answer: [`SelectResponse`] plus which partitions it came out of.
///
/// The count is what says whether the region pruned. Without it a caller cannot tell a
/// region that reached four partitions from one that read the whole catalog and matched the
/// same rows — the same failure `data_bytes_read` answers one file at a time.
#[derive(Debug, Serialize)]
struct HatsResponse {
    num_rows: usize,
    num_partitions: usize,
    schema: Vec<Column>,
    data_bytes_read: u64,
    elapsed_ms: u128,
    rows: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct SelectResponse {
    num_rows: usize,
    /// The columns of the answer, which is the projection where the request made one and
    /// the file's own schema where it did not.
    schema: Vec<Column>,
    /// How much of the source file was read to answer this. What it is for is telling a
    /// caller whether their predicate pruned: the same query written two ways returns the
    /// same rows, and this is where the difference between them shows.
    data_bytes_read: u64,
    elapsed_ms: u128,
    rows: Vec<serde_json::Value>,
}

/// The counts and the timing are part of the JSON body; a parquet body has no room for
/// them, so they travel as headers instead and both formats report the same numbers.
const NUM_ROWS_HEADER: &str = "x-hats-num-rows";
const DATA_BYTES_READ_HEADER: &str = "x-hats-data-bytes-read";
const ELAPSED_MS_HEADER: &str = "x-hats-elapsed-ms";

async fn query_parquet(
    State(service): State<Service>,
    body: Result<Json<QueryRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(params) = body.map_err(|rejection| body_error(&rejection))?;
    let started = Instant::now();
    // Refused rather than ignored: this route answers with rows and never with a plan, so
    // there is nothing here for it to have done.
    if params.return_storage {
        return Err(ApiError::bad_request(
            "return_storage writes credentials into a plan, and this route returns rows",
        ));
    }
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
        selected = params.select.is_some(),
        filtered = params.r#where.is_some(),
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

async fn open_catalog(service: &Service, params: &QueryRequest) -> Result<Opened, ApiError> {
    let format = Format::parse(params.format.as_deref(), Format::Json)?;
    params.refuse_catalog_columns()?;
    let url = parse_url(params.url.as_str())?;
    // A directory rather than an object: `open_dir` drops only the refusal of a url naming
    // no object, and every policy check `open` makes still runs. There is no `[data]`
    // question to ask of the url either — the caller names a catalog, and which files inside
    // it are read is the catalog's own answer.
    let dir = storage::open_dir(&url, &params.storage, &service.policy, &service.transfers)?;
    let on_disk = dir.url.to_file_path().ok();
    let search = Search::resolve(dir, params.region.as_deref(), service.catalog_limits)
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
/// The body is the same one [`query_parquet`] takes, and every field means what it means
/// there. What differs is the four column names, which the catalog answers for itself and
/// this route therefore refuses.
async fn query_hats(
    State(service): State<Service>,
    body: Result<Json<QueryRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(params) = body.map_err(|rejection| body_error(&rejection))?;
    let started = Instant::now();
    let (projection, predicate) = params.expressions()?;
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

    let selection = CatalogSelection {
        projection,
        predicate,
        regions: params.region.as_deref(),
        limit: params.limit,
    };
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
        selected = params.select.is_some() || params.columns.is_some(),
        filtered = params.r#where.is_some() || params.filters.is_some(),
        // How many shapes, not what they were: logging the numbers would be logging the
        // caller's own coordinates for no purpose the count does not already serve.
        regions = params.region.as_ref().map_or(0, Vec::len),
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
async fn query_hats_plan(
    State(service): State<Service>,
    body: Result<Json<QueryRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(params) = body.map_err(|rejection| body_error(&rejection))?;
    let started = Instant::now();
    // Planned, not run — but the expressions still have to parse, or the plan would hand
    // back entries every one of which is a 400 the client discovers one at a time.
    params.expressions()?;
    let Opened { search, .. } = open_catalog(&service, &params).await?;
    let plan = plan_of(&service, &search, &params, None).await?;
    tracing::info!(
        url = %search.catalog().dir().url,
        partitions = search.catalog().partitions().len(),
        chosen = search.chosen().len(),
        requests = plan.requests.len(),
        regions = params.region.as_ref().map_or(0, Vec::len),
        elapsed_ms = started.elapsed().as_millis(),
        "catalog plan"
    );
    Ok(Json(plan).into_response())
}

/// A work list: the requests this one resolves to, for a client to send itself.
#[derive(Debug, Serialize)]
struct PlanResponse {
    /// Why this came back instead of rows, where it did. Absent on the plan route, which
    /// was asked for.
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    /// The catalog, spelled the way the caller spelled it.
    catalog: String,
    num_partitions: usize,
    /// The sum over the entries, where every one of them knew. Absent otherwise: a partial
    /// sum would read as a total.
    #[serde(skip_serializing_if = "Option::is_none")]
    estimated_bytes: Option<u64>,
    /// Whether the original request carried credentials. The entries never do — copying a
    /// secret into a body that gets logged, cached and pasted enables nothing the client
    /// cannot already do, since it is the client's own secret. This says to re-attach them.
    requires_credentials: bool,
    requests: Vec<PlanRequest>,
}

/// One entry: a request against this service, for one file of the catalog.
#[derive(Debug, Serialize)]
struct PlanRequest {
    order: u8,
    pixel: u64,
    /// Separate fields so that a file-server entry, which is a `GET` under a mount, has the
    /// same shape as this one.
    method: &'static str,
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    estimated_bytes: Option<u64>,
    body: PlanBody,
}

/// The body of one entry, which is an ordinary [`query_parquet`] request.
///
/// Every field the caller wrote that still applies, and the column names they were refused
/// — because the single-file route has no catalog to ask and needs them stated.
#[derive(Debug, Serialize)]
struct PlanBody {
    url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    select: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    columns: Option<String>,
    #[serde(rename = "where", skip_serializing_if = "Option::is_none")]
    where_: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filters: Option<String>,
    /// Only where the region does not contain this partition whole. An entry without it is
    /// one whose every row qualifies, and testing them again would only cost time.
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
    storage: Option<serde_json::Value>,
}

async fn plan_of(
    service: &Service,
    search: &Search,
    params: &QueryRequest,
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
        .map(|prefix| route(prefix, "parquet"))
        .ok_or_else(|| ApiError::internal("the API has no prefix"))?;

    let columns = search.columns();
    // Only where the catalog named one, which is the only case `Columns` carries. Where it
    // did not, each entry's file is asked for `_healpix_29` itself — the same discovery the
    // catalog route did — so there is nothing to pass on and nothing lost by not passing it.
    let healpix = columns.and_then(|columns| columns.healpix.as_ref());

    // Both halves, and both are the caller's doing: they asked for it, and they sent
    // something to hand back. Asked for with nothing to return writes no field rather than
    // an empty object, which would read as "these are the options" and they are not.
    let storage =
        (params.return_storage && !params.storage.is_empty()).then(|| params.storage.echo());

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
            let region = tested.then(|| params.region.clone()).flatten();
            let named = region.is_some().then_some(columns).flatten();
            Ok(PlanRequest {
                order: entry.order,
                pixel: entry.pixel,
                method: "POST",
                path: path.clone(),
                estimated_bytes: entry.estimated_bytes,
                body: PlanBody {
                    url: below(&url, &entry.path)?,
                    select: params.select.clone(),
                    columns: params.columns.clone(),
                    where_: params.r#where.clone(),
                    filters: params.filters.clone(),
                    region,
                    ra_column: named.map(|columns| columns.ra.clone()),
                    dec_column: named.map(|columns| columns.dec.clone()),
                    healpix_column: named.and(healpix).map(|(column, _)| column.clone()),
                    healpix_order: named.and(healpix).map(|(_, order)| *order),
                    format: params.format.clone(),
                    limit: params.limit,
                    storage: storage.clone(),
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
                [
                    (header::CONTENT_TYPE, PARQUET_CONTENT_TYPE.to_owned()),
                    (
                        header::CONTENT_DISPOSITION,
                        "attachment; filename=\"selection.parquet\"".to_owned(),
                    ),
                ],
                [
                    (NUM_ROWS_HEADER, num_rows.to_string()),
                    (NUM_PARTITIONS_HEADER, result.partitions_read.to_string()),
                    (
                        DATA_BYTES_READ_HEADER,
                        result.rows.data_bytes_read.to_string(),
                    ),
                    (ELAPSED_MS_HEADER, started.elapsed().as_millis().to_string()),
                ],
                body,
            )
                .into_response())
        }
    }
}

/// The columns of an answer, as the schema describes them.
fn columns_of(result: &QueryResult) -> Vec<Column> {
    result
        .schema
        .fields()
        .iter()
        .map(|field| Column {
            name: field.name().clone(),
            r#type: field.data_type().to_string(),
        })
        .collect()
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
    }
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
        [
            (header::CONTENT_TYPE, PARQUET_CONTENT_TYPE.to_owned()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{}\"", download_name(file)),
            ),
        ],
        [
            (NUM_ROWS_HEADER, num_rows.to_string()),
            (DATA_BYTES_READ_HEADER, result.data_bytes_read.to_string()),
            (ELAPSED_MS_HEADER, started.elapsed().as_millis().to_string()),
        ],
        body,
    )
        .into_response())
}

/// Name the download after the source object, so a directory of these files says which
/// partition each came from. Falls back to a fixed name for a url that ends in a slash
/// — `open` already rejected the ones with no object at all.
fn download_name(file: &RemoteFile) -> String {
    let name = file
        .url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .filter(|segment| !segment.is_empty() && !segment.contains('"'))
        .unwrap_or("selection.parquet");
    match name.ends_with(".parquet") {
        true => name.to_owned(),
        false => format!("{name}.parquet"),
    }
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

    /// A `POST /api/v1/parquet` with the given body, under a policy that allows
    /// everything — what the policy allows is `access.rs`'s business.
    async fn select_with(body: serde_json::Value) -> (StatusCode, String) {
        send(
            Request::builder()
                .method("POST")
                .uri("/api/v1/parquet")
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
        let (status, _) = get("/api/v1/parquet?url=s3://b/k.parquet&where=x%3D1").await;
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
                .uri("/api/v1/parquet")
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
        assert!(body.contains("json, parquet"), "{body}");
    }

    #[test]
    fn names_the_download_after_the_source_object() {
        let policy = AccessPolicy::default();
        let transfers = Arc::new(Transfers::new(&LimitsConfig::default()));
        let name = |raw: &str| {
            let url = parse_url(raw).unwrap();
            download_name(
                &storage::open(&url, &StorageOptions::default(), &policy, &transfers).unwrap(),
            )
        };
        assert_eq!(
            name("s3://b/dir/part0.snappy.parquet"),
            "part0.snappy.parquet"
        );
        // HATS partition paths, and anything else that is not already a parquet name.
        assert_eq!(name("s3://b/Norder=5/Npix=12240/part0"), "part0.parquet");
    }

    /// The request as a struct, printed. Nothing logs it today, but the derive is what
    /// makes that a choice rather than a rule to remember.
    #[test]
    fn printing_the_request_leaks_nothing() {
        let params = QueryRequest {
            url: "s3://b/k.parquet".to_owned().into(),
            storage: StorageOptions {
                region: Some("us-west-2".to_owned()),
                secret_access_key: Some(SECRET.to_owned().into()),
                ..Default::default()
            },
            select: None,
            r#where: Some("objectid = 1".to_owned()),
            columns: None,
            filters: None,
            region: None,
            ra_column: None,
            dec_column: None,
            healpix_column: None,
            healpix_order: None,
            format: None,
            limit: None,
            return_storage: false,
        };
        let shown = format!("{params:?}");
        assert!(!shown.contains(SECRET), "leaked: {shown}");
        assert!(shown.contains("s3://b/k.parquet"), "{shown}");
        assert!(shown.contains("us-west-2"), "{shown}");
    }

    /// One `[[mount]]`, and a service built around it.
    fn with_mount(config: crate::config::MountConfig, api: &ApiConfig) -> Service {
        let mounts = Arc::new(Mounts::new(&[config], &DataConfig::default()).unwrap());
        let policy =
            AccessPolicy::new(&crate::config::AccessConfig::default(), Arc::clone(&mounts))
                .unwrap();
        Service::new(
            policy,
            &LimitsConfig::default(),
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

    /// A `POST` of `body` to a service's own API route, as JSON.
    async fn ask(service: Service, body: serde_json::Value) -> (StatusCode, String) {
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/parquet")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = router(service).oneshot(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    async fn ask_hats(service: Service, body: serde_json::Value) -> (StatusCode, String) {
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/hats")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = router(service).oneshot(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
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
                "columns": "id",
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

    /// The four column names are the catalog's to answer, so a request naming one is refused
    /// rather than obeyed.
    ///
    /// Refused and not ignored: silently dropping one would test the rows against different
    /// columns than the caller wrote, which is an answer they cannot tell from the one they
    /// asked for. The refusal names the field and says where a caller who means it should go.
    #[tokio::test]
    async fn a_catalog_route_refuses_column_names() {
        let dir = crate::hats_query::tests::fixture(true);
        for field in ["ra_column", "dec_column", "healpix_column", "healpix_order"] {
            let value = match field {
                "healpix_order" => serde_json::json!(29),
                _ => serde_json::json!("whatever"),
            };
            for route in ["hats", "hats/plan"] {
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
        body: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/hats/plan")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = router(service).oneshot(request).await.unwrap();
        let status = response.status();
        let body = body_of(response).await;
        (status, serde_json::from_str(&body).unwrap())
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
            serde_json::json!({"url": "file:///", "columns": "id", "region": [region]}),
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
            assert_eq!(entry["path"], "/api/v1/parquet");
            let url = entry["body"]["url"].as_str().unwrap();
            assert!(url.starts_with("file:///dataset/Norder="), "{url}");
            assert!(
                !plan.to_string().contains(&source),
                "the plan names the disk"
            );
            assert_eq!(entry["body"]["columns"], "id");
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
        let body = serde_json::json!({"url": "file:///", "columns": "id", "region": [region]});

        let (_, plan) = ask_plan(mounted(dir.path(), &ApiConfig::default()), body.clone()).await;
        let mut ids = Vec::new();
        for entry in plan["requests"].as_array().unwrap() {
            let (status, answer) = ask(
                mounted(dir.path(), &ApiConfig::default()),
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
        let params = QueryRequest {
            url: "file:///".to_owned().into(),
            storage: StorageOptions {
                region: Some("us-west-2".to_owned()),
                secret_access_key: Some(SECRET.to_owned().into()),
                ..Default::default()
            },
            select: None,
            r#where: None,
            columns: Some("id".to_owned()),
            filters: None,
            region: Some(vec![crate::hats_query::tests::regions()[1].clone()]),
            ra_column: None,
            dec_column: None,
            healpix_column: None,
            healpix_order: None,
            format: None,
            limit: None,
            return_storage: false,
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

        let plan = plan_of(&service, &search, &params, None).await.unwrap();
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
            let params = QueryRequest {
                url: "file:///".to_owned().into(),
                storage,
                select: None,
                r#where: None,
                columns: Some("id".to_owned()),
                filters: None,
                region: None,
                ra_column: None,
                dec_column: None,
                healpix_column: None,
                healpix_order: None,
                format: None,
                limit: None,
                return_storage,
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
            serde_json::to_value(plan_of(&service, &search, &params, None).await.unwrap()).unwrap()
        };
        let given = || StorageOptions {
            region: Some("us-west-2".to_owned()),
            secret_access_key: Some(SECRET.to_owned().into()),
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

    /// The rows route never produces a plan, so a field about what a plan carries is refused
    /// there rather than quietly doing nothing.
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
            .uri("/api/v1/hats")
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

    /// The two vocabularies are two ways of saying one thing, so a body carrying both
    /// halves of one pair is a caller who thinks otherwise.
    #[tokio::test]
    async fn the_api_body_refuses_both_spellings_at_once() {
        for (one, other) in [("select", "columns"), ("where", "filters")] {
            let (status, body) = select_with(serde_json::json!({
                "url": "s3://b/k.parquet",
                one: "objectid",
                other: "objectid",
            }))
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{one}/{other}");
            assert!(body.contains(one) && body.contains(other), "{body}");
        }
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
