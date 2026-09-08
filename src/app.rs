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
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use tower_http::services::ServeFile;
use tower_http::trace::TraceLayer;
// The query-string reading of percent encoding, which is not the path's: `+` is a space
// here and is a literal `+` in a path segment.
use url::form_urlencoded;

use crate::access::{self, AccessPolicy};
use crate::config::{ApiConfig, ConfigError, DataConfig, LimitsConfig, ServerConfig};
use crate::data::DataFiles;
use crate::error::ApiError;
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
    /// Which files either mode will read as data.
    pub data_files: Arc<DataFiles>,
    /// How much SQL one request may carry.
    pub sql_limits: sql::Limits,
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
        mounts: Mounts,
        api: &ApiConfig,
        data: &DataConfig,
        server: &ServerConfig,
    ) -> Result<Self, ConfigError> {
        let data_files = DataFiles::new(data)?;
        let api_prefix = match api.enabled {
            true => Some(mount::normalize_prefix(&api.prefix).map_err(|reason| {
                ConfigError::Route(format!("api.prefix {:?}: {reason}", api.prefix))
            })?),
            false => None,
        };
        if let Some(prefix) = &api_prefix {
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
        } else if mounts.is_empty() {
            return Err(ConfigError::Route(
                "api.enabled is false and there is no [[mount]], so there would be \
                 nothing to serve"
                    .to_owned(),
            ));
        }
        Ok(Self {
            policy: Arc::new(policy),
            transfers: Arc::new(Transfers::new(limits)),
            mounts: Arc::new(mounts),
            data_files: Arc::new(data_files),
            sql_limits: limits.into(),
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
            .route(&route(&prefix, "parquet"), post(query_parquet));
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
    let Some((mount, relative)) = service.mounts.resolve(&path) else {
        return Err(ApiError::not_found(format!("{path} is not a route")));
    };
    let segments = path_segments(relative)?;
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
    if service.data_files.matches_path(&file)
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
    if service.data_files.matches_path(&file) {
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
        true => Html(listing.to_html(&service.data_files, service.show_version)).into_response(),
        false => Json(listing).into_response(),
    })
}

/// The path a request names inside a mount, one component per url segment.
///
/// Percent-decoded one segment at a time, so that an encoded separator arrives as part
/// of a name rather than as a separator, and `..` is refused outright rather than left
/// for the resolver to clean up: a request path is not a place to be climbing from.
fn path_segments(relative: &str) -> Result<Vec<String>, ApiError> {
    let mut segments = Vec::new();
    for segment in relative.split('/').filter(|segment| !segment.is_empty()) {
        let decoded = percent_decode_str(segment)
            .decode_utf8()
            .map_err(|_| ApiError::bad_request("this path is not valid UTF-8"))?;
        if matches!(decoded.as_ref(), "." | "..") || decoded.contains(['/', '\0']) {
            return Err(ApiError::bad_request(format!(
                "{segment:?} is not something a path here can contain"
            )));
        }
        segments.push(decoded.into_owned());
    }
    Ok(segments)
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
}

impl QueryRequest {
    /// The one pair of fields this request actually used.
    ///
    /// Both pairs say the same thing, so a caller may write either. Not both: a body
    /// carrying `select` and `columns` together is one written by someone who thinks
    /// they differ, and quietly picking either would be answering the question they got
    /// wrong.
    fn selection(&self) -> Result<Selection<'_>, ApiError> {
        Ok(Selection {
            projection: match (self.select.as_deref(), self.columns.as_deref()) {
                (Some(_), Some(_)) => return Err(one_of_two("select", "columns")),
                (Some(sql), None) => Projection::Select(sql),
                (None, Some(list)) => Projection::Columns(list),
                (None, None) => Projection::All,
            },
            predicate: match (self.r#where.as_deref(), self.filters.as_deref()) {
                (Some(_), Some(_)) => return Err(one_of_two("where", "filters")),
                (Some(sql), None) => Predicate::Where(sql),
                (None, Some(text)) => Predicate::Filters(text),
                (None, None) => Predicate::All,
            },
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
            (Some(column), Some(order)) => Some(Healpix { column, order }),
            _ => {
                return Err(ApiError::bad_request(
                    "healpix_column and healpix_order travel together: a HEALPix index \
                     column may be called anything and be written at any order, so the \
                     order is what says which cell a value is",
                ));
            }
        };
        if self.region.is_none() && healpix.is_some() {
            return Err(ApiError::bad_request(
                "healpix_column speeds up a region search, and this request carries no \
                 region; a query on that column alone is a filters or where expression",
            ));
        }
        match (
            self.region.as_deref(),
            self.ra_column.as_deref(),
            self.dec_column.as_deref(),
        ) {
            (None, None, None) => Ok(None),
            (Some(regions), Some(ra_column), Some(dec_column)) => Ok(Some(Spatial {
                regions,
                ra_column,
                dec_column,
                healpix,
            })),
            (Some(_), _, _) => Err(ApiError::bad_request(
                "region is tested against two columns of the file, so a request carrying \
                 one must also name ra_column and dec_column",
            )),
            (None, _, _) => Err(ApiError::bad_request(
                "ra_column and dec_column say which columns a region is tested against, \
                 and this request carries no region",
            )),
        }
    }
}

fn one_of_two(one: &str, other: &str) -> ApiError {
    ApiError::bad_request(format!(
        "{one} and {other} are two ways of saying the same thing; send one of them"
    ))
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
    // Everything decidable from the request alone, before anything is opened.
    let format = Format::parse(params.format.as_deref(), Format::Json)?;
    let selection = params.selection()?;
    let url = parse_url(params.url.as_str())?;
    // The API has only one thing to do with an object, so a url naming something it does
    // not read as data names nothing this route serves. Answered before the store is
    // built, so a request for the wrong object costs no connection.
    if !service.data_files.matches_url(&url) {
        return Err(ApiError::not_found(format!(
            "this url does not name a data file; url must end in a name matching {}",
            service.data_files.describe()
        )));
    }
    let file = storage::open(&url, &params.storage, &service.policy, &service.transfers)?;
    // No order promised: the caller named a url and asked for rows, not for a view of a
    // file's layout. A `limit` is still answered reproducibly — that is `query`'s own
    // rule, since which rows come back is a different question from what order they are
    // in.
    let result = query::run(&file, &selection, service.sql_limits, Order::Unspecified).await?;

    let num_rows = result.num_rows();
    let data_bytes_read = result.data_bytes_read;
    let response = answer(&result, &file, format, started).await?;
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
    let schema = result
        .schema
        .fields()
        .iter()
        .map(|field| Column {
            name: field.name().clone(),
            r#type: field.data_type().to_string(),
        })
        .collect();
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
            Mounts::default(),
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
        };
        let shown = format!("{params:?}");
        assert!(!shown.contains(SECRET), "leaked: {shown}");
        assert!(shown.contains("s3://b/k.parquet"), "{shown}");
        assert!(shown.contains("us-west-2"), "{shown}");
    }

    /// A directory with one file in it, and a service that publishes it at `/`.
    fn mounted(dir: &Path, api: &ApiConfig) -> Service {
        let mounts = Mounts::new(&[crate::config::MountConfig {
            path: "/".to_owned(),
            source: dir.display().to_string(),
            follow_symlinks: false,
            immutable: false,
        }])
        .unwrap();
        let policy = AccessPolicy::new(&crate::config::AccessConfig::default(), &mounts).unwrap();
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

    /// The two modes have to divide the url space between them, and a configuration
    /// where they do not is a startup error rather than a route nothing reaches.
    #[test]
    fn a_url_space_that_serves_nothing_is_a_startup_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let service = |mount_path: &str, api: ApiConfig| {
            let mounts = Mounts::new(&[crate::config::MountConfig {
                path: mount_path.to_owned(),
                source: dir.path().display().to_string(),
                follow_symlinks: false,
                immutable: false,
            }])
            .unwrap();
            Service::new(
                AccessPolicy::default(),
                &LimitsConfig::default(),
                mounts,
                &api,
                &DataConfig::default(),
                &ServerConfig::default(),
            )
        };

        // A mount inside the API's subtree is one no request could reach.
        let error = service("/api/v1/hats", ApiConfig::default())
            .unwrap_err()
            .to_string();
        assert!(error.contains("/api/v1"), "{error}");
        // The same directory one level up is the expected arrangement.
        assert!(service("/hats", ApiConfig::default()).is_ok());
        // The API off, with a mount, is a file server.
        let off = ApiConfig {
            enabled: false,
            ..Default::default()
        };
        assert!(service("/api/v1/hats", off).is_ok());

        // The API off with nothing mounted serves nothing at all.
        let error = Service::new(
            AccessPolicy::default(),
            &LimitsConfig::default(),
            Mounts::default(),
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
        let mounts = Mounts::new(&[crate::config::MountConfig {
            path: "/".to_owned(),
            source: dir.path().display().to_string(),
            follow_symlinks: false,
            immutable: false,
        }])
        .unwrap();
        let policy = AccessPolicy::new(&crate::config::AccessConfig::default(), &mounts).unwrap();
        let service = Service::new(
            policy,
            &LimitsConfig::default(),
            mounts,
            &ApiConfig::default(),
            &DataConfig {
                filenames: vec!["*.pq".to_owned()],
            },
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
