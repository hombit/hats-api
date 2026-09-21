//! The file-server mode: a url path under a mount, and the file or directory it names — served
//! as it is, listed, or asked a question through its query string.

use std::path::Path;
use std::time::Instant;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header, request::Parts};
use axum::response::{Html, IntoResponse, Json, Response};
use bytes::Bytes;
use object_store::{GetOptions, GetRange, ObjectMeta};
use tower_http::services::ServeFile;
// The query-string reading of percent encoding, which is not the path's: `+` is a space
// here and is a literal `+` in a path segment.
use url::{Url, form_urlencoded};

use crate::access::mount::{self, Mount, MountSource, RemoteSource};
use crate::access::{self};
use crate::app::answer::{self, answer, hats_answer};
use crate::app::cache;
use crate::app::listing::{self, Listing};
use crate::app::request::{Format, Output};
use crate::app::service::{PARQUET_CONTENT_TYPE, Service};
use crate::engine::query::{self, Order, Predicate, Projection, Selection};
use crate::engine::whole;
use crate::error::ApiError;
use crate::hats;
use crate::hats::query::{CatalogSelection, Exceeded, Outcome, Search};
use crate::output::parquet;
use crate::sky::region::{self, Region, Spatial};
use crate::storage::{self, RemoteDir};

/// The file a directory is served as when it has one, in place of a generated listing.
const DIRECTORY_INDEX: &str = "index.html";

/// The file-server side: a url path, a mount, and the file inside it.
///
/// Everything the request may decide is decided here; everything about *how* an ordinary
/// file is served over HTTP — byte ranges, `ETag` and `Last-Modified`, the conditional
/// requests, `HEAD` — is [`ServeFile`]'s, against a path this function has already
/// resolved. A client reading one partition out of a mount is doing so with ranged
/// requests, so this is not an optional part of being a file server.
pub(in crate::app) async fn serve_mounted(
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
    match mount.source() {
        MountSource::Local(root) => {
            serve_from_disk(&service, mount, root, &segments, parts, body).await
        }
        // Everything below the url space is different: a store has no symlinks and no
        // directories, its names come back from a listing rather than a `readdir`, and
        // the server that would answer a `Range` is the origin's rather than this one.
        MountSource::Remote(source) => {
            serve_from_store(&service, mount, source, &segments, &parts).await
        }
    }
}

/// A path under a mount that is a directory on this machine.
async fn serve_from_disk(
    service: &Service,
    mount: &Mount,
    root: &Path,
    segments: &[String],
    parts: Parts,
    body: Body,
) -> Result<Response, ApiError> {
    let radius = service.max_query_radius_arcsec;
    let mut requested = root.to_owned();
    requested.extend(segments);
    let mut file = access::local::authorize_mounted(mount, &requested)?;
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
        if hats::browse::describes_a_catalog(&file)
            && let Some(query) = FileQuery::parse(parts.uri.query().unwrap_or_default(), radius)?
        {
            let opened = storage::open_mounted_dir(&file)?;
            return query_catalog(service, mount, opened, Hide::Local(&file), &query, &parts).await;
        }
        // A directory that publishes its own page says what it wants said about itself,
        // and the generated listing is only the fallback — unless the operator turned
        // that around with `serve_mounted_index_html`, which is for a tree whose pages were
        // written for some other reader and say nothing about the data under them.
        // Through `authorize_mounted` like any other file, so a link the mount does not
        // follow is not followed here either.
        //
        // The file itself stays served under its own name either way: what the setting
        // decides is what a request for the *directory* answers with, not whether an
        // `index.html` exists.
        let index = match service.serve_mounted_index_html {
            true => access::local::authorize_mounted(mount, &file.join(DIRECTORY_INDEX)).ok(),
            false => None,
        };
        match index {
            Some(index) if index.is_file() => file = index,
            _ => return list_directory(service, mount, segments, &file, &parts).await,
        }
    }
    // A query string turns a data file into a question about itself. Anything else keeps
    // going out verbatim, parameters and all: a file server that has no use for a
    // parameter ignores it, and `index.html?v=3` is a request for `index.html`.
    if mount.data_files().matches_path(&file)
        && let Some(query) = FileQuery::parse(parts.uri.query().unwrap_or_default(), radius)?
    {
        let opened = storage::open_mounted(&file)?;
        // `None` is "the answer is the file", which is what the plain serve below does.
        if let Some(response) =
            query_file(service, &opened, Hide::Local(&file), &query, &parts).await?
        {
            return Ok(response);
        }
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
            HeaderValue::from_static(PARQUET_CONTENT_TYPE),
        );
    }
    Ok(response)
}

/// A path under a mount whose `source` is a store — this service standing in front of
/// that store, which is what `serve` on such a mount means.
///
/// **A store has no directories.** Its namespace is flat, so a name is a file where the
/// store holds an object of exactly that name, and a directory otherwise — and "a prefix
/// nothing is under" is not a case it can tell from "a prefix nobody wrote". So an empty
/// listing is what an unknown path answers with, which is also the answer a mount gives
/// for anything it will not serve.
async fn serve_from_store(
    service: &Service,
    mount: &Mount,
    source: &RemoteSource,
    segments: &[String],
    parts: &Parts,
) -> Result<Response, ApiError> {
    let hide = Hide::Store(source.url());
    let root = mount.open(&service.policy, &service.transfers)?;
    let relative = segments.join("/");
    // The mount's own root is a directory by construction and is never an object, so it
    // is not asked about — a store may perfectly well hold a zero-length key at the
    // prefix a mount publishes, and serving that as the mount would publish a file where
    // the operator published a tree.
    let object = match relative.is_empty() {
        true => None,
        false => root
            .meta(&relative)
            .await
            .map_err(|error| hide.apply(error))?,
    };
    let Some(object) = object else {
        return serve_stored_directory(service, mount, &root, segments, parts, &hide).await;
    };
    // A query string turns a data file into a question about itself; anything else is the
    // bytes, parameters and all.
    if mount
        .data_files()
        .matches(object.location.filename().unwrap_or_default())
        && let Some(query) = FileQuery::parse(
            parts.uri.query().unwrap_or_default(),
            service.max_query_radius_arcsec,
        )?
    {
        let opened = root.child(&relative)?;
        if let Some(response) = query_file(service, &opened, hide, &query, parts).await? {
            return Ok(response);
        }
    }
    serve_object(&root, &relative, &object, mount, parts, &hide).await
}

/// A directory of a store-backed mount: a catalog asked a question, its own `index.html`,
/// or the listing.
///
/// One listing answers all three. Against a filesystem each of those is a `stat` that
/// costs nothing; against a store each would be a request, and the page is already one.
async fn serve_stored_directory(
    service: &Service,
    mount: &Mount,
    root: &RemoteDir,
    segments: &[String],
    parts: &Parts,
    hide: &Hide<'_>,
) -> Result<Response, ApiError> {
    let relative = segments.join("/");
    let dir = root.subdir(&relative)?;
    // The catalog probe is a request of its own, so it is made only where there is a
    // question for it to be about. Which directory this is still decides whether there is
    // a query surface at all — a directory that is not a catalog is listed with its query
    // string ignored, exactly as on disk.
    if let Some(query) = FileQuery::parse(
        parts.uri.query().unwrap_or_default(),
        service.max_query_radius_arcsec,
    )? && hats::browse::in_store::describes_a_catalog(&dir)
        .await
        .map_err(|error| hide.apply(error))?
    {
        return query_catalog(service, mount, dir, *hide, &query, parts).await;
    }

    if !matches!(parts.method, Method::GET | Method::HEAD) {
        return Err(ApiError::method_not_allowed(
            "a listing is read, not written",
        ));
    }
    let level = dir.level("").await.map_err(|error| {
        // The same answer a directory that is not published gets: which prefixes a store
        // will not list is something about the origin, and the operator reads it in the
        // log. An `http(s)://` source is the ordinary case — an origin with no listing
        // operation serves its files here and cannot be browsed.
        tracing::warn!(error = %error, mount = mount.prefix(), "cannot list");
        ApiError::not_found("no such directory")
    })?;
    // **A store has no empty directories**, so a prefix with nothing under it is a name
    // that is not there rather than a directory that happens to be bare — and answering it
    // `200` with an empty listing is this service inventing a resource. What that costs is
    // not cosmetic: `hats` probes for `hats.properties`, `properties` and
    // `collection.properties` in turn, and an empty listing answered `200` is a file as far
    // as it can tell, so it parses the JSON as a properties file and a catalog that is
    // perfectly readable fails to open with a validation error naming fields no listing has.
    // A local mount is the other case and keeps its empty directories: there, the directory
    // really is there.
    if level.files.is_empty() && level.directories.is_empty() {
        return Err(ApiError::not_found("no such directory"));
    }
    // A directory that publishes its own page says what it wants said about itself, and
    // the generated listing is the fallback — the same rule, and the same switch, as on
    // disk. Read out of the listing rather than asked for, which is what makes it free.
    if service.serve_mounted_index_html
        && let Some(index) = level.files.iter().find(|file| file.name == DIRECTORY_INDEX)
    {
        let within = listing::relative_path(segments, &index.name);
        let object = root
            .meta(&within)
            .await
            .map_err(|error| hide.apply(error))?
            .ok_or_else(|| ApiError::not_found("no such file"))?;
        return serve_object(root, &within, &object, mount, parts, hide).await;
    }

    let path = listing::url(mount.prefix(), segments);
    let listed = Listing::of_store(&level, mount.prefix(), &path);
    let (catalog, about) = match listing::wants_html(&parts.headers) {
        // Only for the page. The walk is a listing per level it climbs, and nothing but
        // the page renders what it finds.
        true => stored_catalog(&dir, root, segments, mount, hide).await?,
        false => (None, None),
    };
    Ok(render(
        service,
        mount,
        &listed,
        catalog.as_deref(),
        about.as_ref(),
        &parts.headers,
    ))
}

/// The catalog this directory belongs to, for the page: its url under the mount, and the
/// little it says about itself.
async fn stored_catalog(
    dir: &RemoteDir,
    root: &RemoteDir,
    segments: &[String],
    mount: &Mount,
    hide: &Hide<'_>,
) -> Result<(Option<String>, Option<hats::browse::About>), ApiError> {
    let found = hats::browse::in_store::enclosing(root, segments)
        .await
        .map_err(|error| hide.apply(error))?;
    let Some(levels) = found else {
        return Ok((None, None));
    };
    let above = segments.get(..segments.len().saturating_sub(levels));
    let at = match levels {
        0 => dir.clone(),
        _ => root.subdir(&above.unwrap_or_default().join("/"))?,
    };
    let about = hats::browse::in_store::about(&at)
        .await
        .map_err(|error| hide.apply(error))?;
    Ok((
        above.map(|above| listing::url(mount.prefix(), above)),
        Some(about),
    ))
}

/// One object off a store-backed mount, over HTTP.
///
/// What [`ServeFile`] does for a local file, done against a store: the range the client
/// asked for, the validators it will ask with next time, and the type the readers above
/// this look at. **The ranges are not optional.** An `lsdb` or `fsspec` client reads one
/// partition of a catalog with ranged requests, and a server that answered `200` to one of
/// those would hand it the head of the file for every slice it asked for — a wrong answer
/// with nothing in it to say so, which is the same failure this service refuses everywhere
/// else.
///
/// The bytes are streamed as they arrive rather than collected. A partition runs to
/// gigabytes, and nothing here has any reason to hold one.
async fn serve_object(
    dir: &RemoteDir,
    relative: &str,
    object: &ObjectMeta,
    mount: &Mount,
    parts: &Parts,
    hide: &Hide<'_>,
) -> Result<Response, ApiError> {
    if !matches!(parts.method, Method::GET | Method::HEAD) {
        return Err(ApiError::method_not_allowed("a mounted file is read"));
    }
    let mut headers = HeaderMap::new();
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Some(tag) = object.e_tag.as_deref()
        && let Ok(value) = HeaderValue::from_str(tag)
    {
        headers.insert(header::ETAG, value);
    }
    if let Ok(value) = HeaderValue::from_str(&httpdate::fmt_http_date(object.last_modified.into()))
    {
        headers.insert(header::LAST_MODIFIED, value);
    }
    headers.insert(header::CONTENT_TYPE, content_type(mount, relative));

    // The origin's own validator, handed back to it: a client that has the object already
    // gets a `304` and the bytes stay where they are.
    if let Some(tag) = object.e_tag.as_deref()
        && let Some(asked) = parts.headers.get(header::IF_NONE_MATCH)
        && asked.to_str().is_ok_and(|asked| matches_etag(asked, tag))
    {
        return Ok((StatusCode::NOT_MODIFIED, headers).into_response());
    }

    let range = match parts.headers.get(header::RANGE) {
        None => None,
        Some(asked) => match wanted_range(asked, object.size) {
            Some(range) => Some(range),
            // Refused rather than answered whole: a client that asked for the tail and got
            // the head cannot tell the two apart.
            None => {
                headers.insert(
                    header::CONTENT_RANGE,
                    HeaderValue::from_str(&format!("bytes */{}", object.size))
                        .unwrap_or_else(|_| HeaderValue::from_static("bytes */0")),
                );
                return Ok((StatusCode::RANGE_NOT_SATISFIABLE, headers).into_response());
            }
        },
    };
    let (status, length) = match &range {
        None => (StatusCode::OK, object.size),
        Some(range) => {
            headers.insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!(
                    "bytes {}-{}/{}",
                    range.start,
                    range.end - 1,
                    object.size
                ))
                .map_err(|_| ApiError::internal("cannot describe this range"))?,
            );
            (StatusCode::PARTIAL_CONTENT, range.end - range.start)
        }
    };
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from(length));
    // A `HEAD` is the same headers with no body, which is what the store is not asked for.
    if parts.method == Method::HEAD {
        return Ok((status, headers).into_response());
    }
    let options = GetOptions {
        range: range.map(GetRange::Bounded),
        ..GetOptions::default()
    };
    let result = dir
        .get(relative, options)
        .await
        .map_err(|error| hide.apply(error))?;
    Ok((status, headers, Body::from_stream(result.into_stream())).into_response())
}

/// The one range a request asked for, or `None` where it asked for something this cannot
/// answer.
///
/// Several ranges at once is one of those. A multipart response is a form of answer
/// nothing reading these files sends, and half-answering it — the first range under a
/// `206` claiming all of them — is the mislabelling this whole path exists to avoid.
pub(in crate::app) fn wanted_range(asked: &HeaderValue, size: u64) -> Option<std::ops::Range<u64>> {
    let text = asked.to_str().ok()?;
    let ranges = http_range_header::parse_range_header(text)
        .ok()?
        .validate(size)
        .ok()?;
    match ranges.as_slice() {
        [one] => Some(*one.start()..one.end().checked_add(1)?),
        _ => None,
    }
}

/// Whether an `If-None-Match` value covers this tag. `*` is every representation, and a
/// list is any of them; a weak comparison is what a `GET` uses, so the `W/` prefix is not
/// part of the comparison.
fn matches_etag(asked: &str, tag: &str) -> bool {
    let weak = |value: &str| value.trim().trim_start_matches("W/").to_owned();
    asked.trim() == "*"
        || asked
            .split(',')
            .any(|candidate| weak(candidate) == weak(tag))
}

/// What a mounted file is served as. The mount's own list decides the parquet half, the
/// way it decides everything else about which of its files are data.
fn content_type(mount: &Mount, relative: &str) -> HeaderValue {
    let name = relative.rsplit('/').next().unwrap_or(relative);
    if mount.data_files().matches(name) {
        return HeaderValue::from_static(PARQUET_CONTENT_TYPE);
    }
    mime_guess::from_path(name)
        .first_raw()
        .and_then(|mime| HeaderValue::from_str(mime).ok())
        .unwrap_or_else(|| HeaderValue::from_static("application/octet-stream"))
}

/// How a failure against one mount is told to a caller, the mount's own location being no
/// part of what they wrote.
///
/// Two rules rather than one, and the difference is not cosmetic: a local mount has no
/// origin behind it, so a store's complaint about it is a statement about the file and
/// answers `400`, while a store-backed mount has one and keeps the status it really got.
#[derive(Clone, Copy)]
enum Hide<'a> {
    Local(&'a Path),
    Store(&'a Url),
}

impl Hide<'_> {
    fn apply(&self, error: ApiError) -> ApiError {
        match self {
            Self::Local(path) => error.from_mount(path),
            Self::Store(url) => error.from_mounted_store(url),
        }
    }
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
/// the API: a `zone` is two ordered pairs and a `moc` is a document, and neither reads as a
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
    dsv_null_value: Option<String>,
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
                "dsv_null_value" => &mut query.dsv_null_value,
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
                    // a query string does not carry — the API's route takes it.
                    healpix: None,
                    // A url naming one file names no catalog above it.
                    partition: None,
                    // And names one table, so a column needs no qualifier.
                    relation: None,
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
/// which is why the handle is opened by the mount rather than through the url-judging
/// path.
///
/// **`None` means the answer is the file itself**, and the caller serves it the way it
/// would have without a query string. That is not an optimisation of a query, it is the
/// observation that the query is not one: `engine::whole` proves from the footer that the
/// projection is every column and the predicate keeps every row, which is exactly what an
/// `lsdb` client sends for a partition it has not projected. Answering it as a query is a
/// full read and a full re-encode, three times over, for bytes that are already sitting in
/// the store.
async fn query_file(
    service: &Service,
    opened: &storage::RemoteFile,
    hide: Hide<'_>,
    query: &FileQuery,
    request: &Parts,
) -> Result<Option<Response>, ApiError> {
    if !matches!(request.method, Method::GET | Method::HEAD) {
        return Err(ApiError::method_not_allowed("a query is read, not written"));
    }
    let started = Instant::now();
    let output = Output::parse(
        query.format.as_deref(),
        query.dsv_null_value.as_deref(),
        Format::Parquet,
    )?;
    let selection = query.selection()?;
    // Through `hide`, all of them: a store's own message names where it was reading, and
    // where a mount's files really are is the operator's.
    //
    // The footer is read before the rows rather than alongside them, which costs one round
    // trip on the way to a query and is what buys the two decisions below: whether this is
    // a query at all, and — for the answer that is one — the layout to write it in. Both
    // need the same footer, so the read is not an extra one either way.
    let layout = match output.format {
        Format::Parquet => {
            let (layout, metadata) = parquet::read_source(opened)
                .await
                .map_err(|error| hide.apply(error))?;
            if whole::answers_with_the_file(&selection, &metadata, service.sql_limits) {
                return Ok(None);
            }
            Some(layout)
        }
        _ => None,
    };

    // The answer this request would make may already have been made: a parquet reader opens
    // one url three times, and without this each of those re-runs the query and re-reads
    // the file. Keyed by the url and bounded by a few minutes — see `app::cache`.
    let asked = cache::Asked::new(request.uri.path(), request.uri.query());
    if matches!(output.format, Format::Parquet)
        && let Some(held) = service.answers.get(&asked)
    {
        return Ok(Some(answer::parquet_answer(
            held.body.to_vec(),
            opened,
            held.counts,
            started,
            Some(request),
        )));
    }

    // The rows come back in the file's own order. The request named a file and asked for
    // less of it, so the answer describes that file, and a client that reads a partition
    // twice gets the same rows in the same places both times.
    let result = query::run(opened, &selection, service.sql_limits, Order::File)
        .await
        .map_err(|error| hide.apply(error))?;

    let num_rows = result.num_rows();
    let data_bytes_read = result.data_bytes_read;
    let response = match output.format {
        Format::Parquet => {
            let body = parquet::encode(&result, layout.unwrap_or_default())
                .map_err(|error| hide.apply(error))?;
            let counts = answer::Counts {
                num_rows,
                data_bytes_read,
            };
            service.answers.insert(
                asked,
                cache::Held {
                    body: Bytes::from(body.clone()),
                    counts,
                },
            );
            answer::parquet_answer(body, opened, counts, started, Some(request))
        }
        _ => answer(&result, opened, &output, started, None, Some(request))
            .await
            .map_err(|error| hide.apply(error))?,
    };
    tracing::info!(
        // The url path, not the mount's own: where the file really is is the operator's
        // business.
        // Both parameters are the caller's own text and can be megabytes of `IN` list,
        // so what is logged is that they were there.
        path = request.uri.path(),
        projected = query.columns.is_some(),
        filtered = query.filters.is_some(),
        format = output.format.name(),
        num_rows,
        // What the pruning was worth, next to the time it took. Free to record and the
        // one number that says whether a slow request was slow because it read the file.
        data_bytes_read,
        elapsed_ms = started.elapsed().as_millis(),
        "query"
    );
    Ok(Some(response))
}

/// A catalog under a mount, asked for the rows inside a circle.
///
/// The same work [`query_hats`](crate::app::routes::hats::query_hats) does, reached by a url
/// rather than by a body: the catalog chooses its partitions from the region, names its own
/// position columns, and reads them several at a time. What a url cannot carry is a fan-out —
/// so a request too large for one answer is refused here rather than answered with a work
/// list, and the message says which route hands one back.
///
/// **The circle is optional, and a `limit` is what makes it so.** Without a region the
/// request is the whole catalog in its own order, which a `limit` turns into the front of it
/// — read partition by partition until there are enough rows, which for the first ten is the
/// first partition. Without either, the whole catalog is what it says, and the partition
/// bound refuses it before anything is read.
async fn query_catalog(
    service: &Service,
    mount: &Mount,
    dir: RemoteDir,
    hide: Hide<'_>,
    query: &FileQuery,
    request: &Parts,
) -> Result<Response, ApiError> {
    if !matches!(request.method, Method::GET | Method::HEAD) {
        return Err(ApiError::method_not_allowed("a query is read, not written"));
    }
    let started = Instant::now();
    // Parquet, as it is for a file: adding a query string to a url should not change what
    // media type it answers with, and the page asks for JSON by name.
    let output = Output::parse(
        query.format.as_deref(),
        query.dsv_null_value.as_deref(),
        Format::Parquet,
    )?;
    let selection = query.catalog_selection()?;
    // Every message from here down names where the mount's data really is, which is the
    // operator's and no part of what the caller wrote.
    let hide_the_path = |error: ApiError| hide.apply(error);

    let search = Search::resolve(dir, selection.regions, service.catalog_limits)
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
    let response = hats_answer(&result, &output, started, Some(request))
        .await
        .map_err(hide_the_path)?;
    tracing::info!(
        // The url path, not the mount's own: where the catalog really is is the
        // operator's business.
        path = request.uri.path(),
        partitions = search.catalog().partitions().len(),
        chosen = search.chosen().len(),
        partitions_read,
        projected = query.columns.is_some(),
        filtered = query.filters.is_some(),
        format = output.format.name(),
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
    // Only for the page, which is the one thing that renders what the walk finds. On disk
    // it is a handful of `stat`s; the store's half of this is a request per level, and one
    // rule for both is what keeps them from drifting.
    let wants_catalog = listing::wants_html(&request.headers);
    // `read_dir` and a `stat` per entry are blocking calls, and a HATS `Dir=` level is
    // ten thousand of them. The catalog probe is a handful more, on the same thread.
    let read = tokio::task::spawn_blocking(move || {
        let listing = Listing::read(&dir, &root, &path, follow_symlinks)?;
        // The catalog this directory is inside, and what it says about itself — both read
        // here rather than beside the page, `about` being another small file off the disk.
        let found = wants_catalog
            .then(|| hats::browse::enclosing(&dir, depth))
            .flatten()
            .map(|levels| {
                let at = dir.ancestors().nth(levels).unwrap_or(&dir);
                (levels, hats::browse::about(at))
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
    Ok(render(
        service,
        mount,
        &listing,
        catalog.as_deref(),
        about.as_ref(),
        &request.headers,
    ))
}

/// A listing as a page or as JSON, whichever the request asked for, with what its catalog
/// says about itself where the walk found one.
///
/// Both mount kinds land here. What differs between them is how a directory is read and
/// how a catalog above it is recognised; what a directory *answers with* is one thing, and
/// a second copy of it is how the two modes come to describe one directory differently.
fn render(
    service: &Service,
    mount: &Mount,
    listing: &Listing,
    catalog: Option<&str>,
    about: Option<&hats::browse::About>,
    headers: &HeaderMap,
) -> Response {
    // Where the catalog's columns are, as a url under this mount. Only where the catalog has
    // the file: one without it is answered by the page a different way rather than offered a
    // url that is a 404. The path is the catalog's to give — a collection's is inside its
    // primary table — and the encoding is `listing`'s.
    let schema = catalog.zip(about).and_then(|(at, about)| {
        let path = about.schema.as_deref()?;
        Some(listing::below(at, path))
    });
    match listing::wants_html(headers) {
        true => Html(
            listing.to_html(
                mount.data_files(),
                service.api_prefix.as_deref(),
                &listing::Catalog {
                    url: catalog,
                    name: about.and_then(|about| about.name.as_deref()),
                    rows: about.and_then(|about| about.rows),
                    order: about.and_then(|about| about.order),
                    schema_url: schema.as_deref(),
                    max_radius_arcsec: service.max_query_radius_arcsec,
                },
                service
                    .signature
                    .as_ref()
                    .and_then(|value| value.to_str().ok()),
            ),
        )
        .into_response(),
        false => Json(listing).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;

    use crate::app::answer::{DATA_BYTES_READ_HEADER, NUM_ROWS_HEADER};
    use crate::app::testing::{ask, body_of, mounted, respond, serving, with_limits, with_server};
    use crate::config::{ApiConfig, LimitsConfig, ServerConfig};
    use crate::output::{dsv, votable};

    use super::*;

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

    /// `serve_mounted_index_html = false` turns that around: the directory answers with the
    /// generated listing, and the file is still there under its own name — which is what
    /// makes this a choice about the directory rather than about hiding a file.
    #[tokio::test]
    async fn an_index_is_ignored_where_the_operator_turned_it_off() {
        let dir = tree();
        std::fs::write(dir.path().join("index.html"), b"<p>hand-written</p>").unwrap();
        let service = || {
            with_server(
                serving(dir.path()),
                &ApiConfig::default(),
                &LimitsConfig::default(),
                &ServerConfig {
                    serve_mounted_index_html: false,
                    ..ServerConfig::default()
                },
            )
        };

        let listing = respond(
            service(),
            Request::builder()
                .uri("/")
                .header(header::ACCEPT, "text/html"),
        )
        .await;
        assert_eq!(listing.status(), StatusCode::OK);
        let body = body_of(listing).await;
        assert!(!body.contains("hand-written"), "{body}");
        assert!(body.contains("href=\"/index.html\""), "{body}");

        let file = respond(service(), Request::builder().uri("/index.html")).await;
        assert_eq!(file.status(), StatusCode::OK);
        assert_eq!(body_of(file).await, "<p>hand-written</p>");
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

    /// The centre of the fixture's first cone, which is where its rows are.
    fn centre() -> (f64, f64) {
        let Region::Circle { ra, dec, .. } = hats::query::tests::regions()[0] else {
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
    /// The carriers differ — a url carries one circle where a body carries an array of
    /// shapes — but they lower to the same request, so what this holds is that the rows are
    /// the geometry's and not the route's.
    #[tokio::test]
    async fn a_catalog_url_answers_a_cone_search() {
        let dir = hats::query::tests::fixture(true);
        let region = hats::query::tests::regions()[0].clone();
        let expected = hats::query::tests::inside(&region);
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

    /// A catalog's answer is a parquet body like any other, so it is sliced like one.
    #[tokio::test]
    async fn a_catalog_query_answer_serves_a_range() {
        let dir = hats::query::tests::fixture(true);
        let whole = respond(
            catalog_server(dir.path(), 3600.0),
            Request::builder().uri("/?limit=1&format=parquet"),
        )
        .await;
        assert_eq!(whole.headers()[header::ACCEPT_RANGES], "bytes");
        let whole = whole.into_body().collect().await.unwrap().to_bytes();

        let response = respond(
            catalog_server(dir.path(), 3600.0),
            Request::builder()
                .uri("/?limit=1&format=parquet")
                .header(header::RANGE, "bytes=-4"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        // The parquet magic, which is the read every client starts with.
        assert_eq!(&body[..], b"PAR1");
        assert_eq!(&body[..], &whole[whole.len() - 4..]);
    }

    /// A url answers what fits in one answer, and says where a wider search goes.
    ///
    /// Checked on the request's own numbers rather than on what reading it costs: the other
    /// bounds answer a fan-out, which is exactly what a url cannot carry. Both spellings of
    /// the radius are the same radius, so both are measured against it.
    #[tokio::test]
    async fn a_cone_wider_than_the_url_answers_is_refused() {
        let dir = hats::query::tests::fixture(true);
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
        let dir = hats::query::tests::fixture(true);
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
        let catalog = hats::query::tests::fixture(true);
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
        let dir = hats::query::tests::fixture(true);
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
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert!(body.contains("partitions"), "{body}");
    }

    /// One file of a catalog answers the same circle, and needs to be told which columns
    /// hold a position — a parquet file says nothing about that, and the catalog that would
    /// have is not what this url named.
    #[tokio::test]
    async fn a_parquet_url_answers_a_cone_search_when_it_is_told_the_columns() {
        let dir = hats::query::tests::fixture(true);
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
        let dir = hats::query::tests::fixture(true);
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

    /// A query that narrows nothing is answered with the file, byte for byte.
    ///
    /// This is the request an `lsdb` client sends for a partition it has not projected:
    /// every column named, and a predicate that is the partition's own cell bounds, which
    /// every row satisfies. Answered as a query it is a full read and a full re-encode, and
    /// three times over, for bytes already sitting in the store.
    ///
    /// The check is that the body *is* the file — not merely that it holds the same rows —
    /// since that is the difference between serving bytes and generating them.
    #[tokio::test]
    async fn a_query_that_narrows_nothing_answers_with_the_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let fixture = query::tests::fixture();
        std::fs::write(dir.path().join("part0.parquet"), &fixture).unwrap();
        let service = || mounted(dir.path(), &ApiConfig::default());

        for url in [
            "/part0.parquet?columns=objectid,band",
            // The column order is not the file's, which is the order `lsdb` writes.
            "/part0.parquet?columns=band,objectid",
            // Every row satisfies it, proved from the footer rather than by reading.
            "/part0.parquet?columns=objectid,band&filters=objectid%3E=0",
        ] {
            let response = respond(service(), Request::builder().uri(url)).await;
            assert_eq!(response.status(), StatusCode::OK, "{url}");
            assert_eq!(
                response.headers()[header::CONTENT_TYPE],
                PARQUET_CONTENT_TYPE
            );
            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(
                &body[..],
                &fixture[..],
                "{url} was re-encoded rather than served"
            );
        }

        // And a query that does narrow something is still a query: the answer is smaller
        // than the file and is not it.
        let response = respond(
            service(),
            Request::builder().uri("/part0.parquet?columns=objectid&filters=objectid%3E5"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_ne!(&body[..], &fixture[..]);
    }

    /// A query's answer is generated for this request, and a client still has to seek in
    /// it: parquet is read footer-first, so a body that refuses ranges is a body `pyarrow`
    /// cannot open at all. `fsspec` reports such a url as `partial: False` and hands back a
    /// streaming file, and every `lsdb` read of an answer bigger than one block ends at
    /// `Cannot seek streaming HTTP file`.
    ///
    /// So the slice is real, and it is the slice of the body this request generated — the
    /// magic at the end, which is where a parquet reader starts.
    #[tokio::test]
    async fn a_query_answer_serves_a_range() {
        let dir = tempfile::TempDir::new().unwrap();
        let fixture = query::tests::fixture();
        std::fs::write(dir.path().join("part0.parquet"), &fixture).unwrap();
        let service = || mounted(dir.path(), &ApiConfig::default());

        let whole = respond(
            service(),
            Request::builder().uri("/part0.parquet?columns=objectid"),
        )
        .await;
        assert_eq!(whole.status(), StatusCode::OK);
        assert_eq!(whole.headers()[header::ACCEPT_RANGES], "bytes");
        let whole = whole.into_body().collect().await.unwrap().to_bytes();

        let response = respond(
            service(),
            Request::builder()
                .uri("/part0.parquet?columns=objectid")
                .header(header::RANGE, "bytes=-4"),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::PARTIAL_CONTENT,
            "a range was answered whole rather than sliced"
        );
        assert_eq!(response.headers()[header::ACCEPT_RANGES], "bytes");
        assert_eq!(
            response.headers()[header::CONTENT_RANGE],
            format!(
                "bytes {}-{}/{}",
                whole.len() - 4,
                whole.len() - 1,
                whole.len()
            )
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"PAR1");
        assert_eq!(&body[..], &whole[whole.len() - 4..]);

        // A range past the end is refused rather than answered with what there is: a
        // client that asked for bytes that do not exist has to hear so.
        let response = respond(
            service(),
            Request::builder()
                .uri("/part0.parquet?columns=objectid")
                .header(header::RANGE, format!("bytes={}-", whole.len() + 1)),
        )
        .await;
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(
            response.headers()[header::CONTENT_RANGE],
            format!("bytes */{}", whole.len())
        );

        // The plain file, with no query, still answers the same range for real.
        let response = respond(
            service(),
            Request::builder()
                .uri("/part0.parquet")
                .header(header::RANGE, "bytes=-4"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()[header::ACCEPT_RANGES], "bytes");
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
            body.contains("<FIELD name=\"objectid\" ID=\"objectid\" datatype=\"long\"/>"),
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

    /// Delimiter-separated values off the same route, which is what says the format reached
    /// the answer rather than only the parser. The nested refusal is `output::dsv`'s and lands as
    /// a `400` here for the same reason the VOTable one does: a body that has begun cannot
    /// take back a column it could not write.
    #[tokio::test]
    async fn a_dsv_answer_carries_a_header_row_and_its_counts() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), query::tests::fixture()).unwrap();
        std::fs::write(
            dir.path().join("nested.parquet"),
            query::tests::nested_fixture(),
        )
        .unwrap();

        for (format, content_type, name, separator) in [
            ("csv", dsv::Dsv::Csv.content_type(), "part0.csv", ','),
            ("tsv", dsv::Dsv::Tsv.content_type(), "part0.tsv", '\t'),
        ] {
            let response = respond(
                mounted(dir.path(), &ApiConfig::default()),
                Request::builder().uri(format!(
                    "/part0.parquet?columns=objectid,band&limit=2&format={format}"
                )),
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK, "{format}");
            let headers = response.headers().clone();
            assert_eq!(headers[header::CONTENT_TYPE], content_type, "{format}");
            assert_eq!(
                headers[header::CONTENT_DISPOSITION],
                format!("attachment; filename=\"{name}\""),
                "{format}"
            );
            assert_eq!(headers[NUM_ROWS_HEADER], "2", "{format}");

            let body = body_of(response).await;
            let mut lines = body.lines();
            assert_eq!(
                lines.next().unwrap(),
                format!("objectid{separator}band"),
                "{format}"
            );
            assert_eq!(lines.next().unwrap(), format!("0{separator}g"), "{format}");
        }

        for format in ["csv", "tsv"] {
            let response = respond(
                mounted(dir.path(), &ApiConfig::default()),
                Request::builder().uri(format!("/nested.parquet?format={format}")),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{format}");
            let body = body_of(response).await;
            assert!(body.contains("sources"), "{format}: {body}");
        }
    }

    /// `dsv_null_value` reaches the encoder, and the three formats that have a null of their
    /// own refuse it rather than dropping it — a caller who named one and got the default
    /// spelling back cannot tell that from the service having honoured it.
    #[tokio::test]
    async fn a_null_value_is_honoured_by_the_two_formats_that_take_one() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("nulls.parquet"),
            query::tests::null_fixture(),
        )
        .unwrap();
        let ask = async |query: &str| {
            let response = respond(
                mounted(dir.path(), &ApiConfig::default()),
                Request::builder().uri(format!("/nulls.parquet?{query}")),
            )
            .await;
            (response.status(), body_of(response).await)
        };

        // The null takes the sentinel and the empty string does not, which is the whole of
        // what the option is for.
        let (status, body) = ask("format=csv&dsv_null_value=%5CN").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body.lines().nth(1).unwrap(), "0,\\N", "{body}");
        assert_eq!(body.lines().nth(2).unwrap(), "1,", "{body}");

        // Left out, the two share a spelling — which is what the option is for.
        let (_, body) = ask("format=csv").await;
        assert_eq!(body.lines().nth(1).unwrap(), "0,", "{body}");
        assert_eq!(body.lines().nth(2).unwrap(), "1,", "{body}");

        for format in ["json", "parquet", "votable"] {
            let (status, body) = ask(&format!("format={format}&dsv_null_value=X")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{format}: {body}");
            assert!(body.contains("csv and tsv"), "{format}: {body}");
        }

        // Checked before it is used, by the rule `output::dsv` states — and a tab is refused for
        // a csv answer too, the sentinel not knowing which format will carry it.
        let (status, body) = ask("format=csv&dsv_null_value=a%09b").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body.contains("separates"), "{body}");
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
            // A name, not an expression: that is what ADQL is for.
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
        use std::sync::Arc;

        use crate::access::AccessPolicy;
        use crate::access::mount::Mounts;
        use crate::config::DataConfig;

        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.pq"), query::tests::fixture()).unwrap();
        let data = DataConfig {
            filenames: vec!["*.pq".to_owned()],
        };
        let mounts = Arc::new(Mounts::new(&[serving(dir.path())], &data).unwrap());
        let policy = AccessPolicy::new(
            &crate::config::AccessConfig::default(),
            Arc::clone(&mounts),
            None,
        )
        .unwrap();
        let service = Service::new(
            policy,
            &LimitsConfig::default(),
            mounts,
            &ApiConfig::default(),
            &data,
            &crate::config::TapConfig::default(),
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
}
