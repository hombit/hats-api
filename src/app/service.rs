//! The service as a whole: what every request shares, and the router that divides the url space
//! between the API's routes and the mounts.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    body::Body,
    extract::{DefaultBodyLimit, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{Html, IntoResponse, Json, Response},
    routing::{get, post},
};
use futures::StreamExt as _;
use http::{HeaderValue, header};
use serde::Serialize;
use tower_http::compression::CompressionLayer;
use tower_http::compression::predicate::{
    And, DefaultPredicate, NotForContentType, Predicate as _,
};
use tower_http::decompression::RequestDecompressionLayer;
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::trace::TraceLayer;
use url::Url;

use crate::access::data::DataFiles;
use crate::access::mount::{self, Mount, MountSource, Mounts};
use crate::access::{self, AccessPolicy};
use crate::adql;
use crate::app::answer;
use crate::app::cache;
use crate::app::files::serve_mounted;
use crate::app::openapi::{self, description::describe};
use crate::app::routes::tap::Jobs;
use crate::app::routes::{adql::query_adql, hats, parquet::query_parquet, tap};
use crate::config::{ApiConfig, ConfigError, DataConfig, LimitsConfig, ServerConfig, TapConfig};
use crate::engine::sql;
use crate::error::ApiError;
use crate::hats::query::CatalogLimits;
use crate::storage::materialize::Transfers;
use crate::tap::TapTableList;

/// What every request needs and no request may change: the rules, the shared scratch
/// budget, and the url space each mode claims. All built once at startup, so a request
/// carries a handle rather than a copy and two requests cannot disagree about any of it.
#[derive(Debug, Clone)]
pub struct Service {
    pub policy: Arc<AccessPolicy>,
    pub transfers: Arc<Transfers>,
    pub mounts: Arc<Mounts>,
    /// Which files are read as data where no mount governs the question, which in API
    /// mode is every remote url. A mount answers it with [`mount::Mount::data_files`] instead.
    pub data_files: Arc<DataFiles>,
    /// The tables the TAP resources publish. Read from the config at startup, which is
    /// what keeps it config rather than a registry: nothing adds to it per request.
    pub tap_tables: Arc<TapTableList>,
    /// How much SQL one request may carry.
    pub sql_limits: sql::Limits,
    /// What a request against a whole catalog may spend.
    pub catalog_limits: CatalogLimits,
    /// What one ADQL statement may spend.
    pub adql_limits: adql::query::Limits,
    /// The widest circle a query string may ask for. The file-server mode's bound alone: a
    /// url is followed rather than fanned out, so what it asks for has to fit in one answer.
    pub(in crate::app) max_query_radius_arcsec: f64,
    /// Answers held for the rest of the requests that read them; see [`cache`].
    pub(in crate::app) answers: Arc<cache::Answers>,
    /// How long a request has to produce an answer; `None` where the operator set no bound.
    request_timeout: Option<Duration>,
    /// How large a request body may be, in bytes; `None` where the operator set no bound.
    request_body_limit: Option<usize>,
    /// How the service signs what it answers — the `Server` header, and the foot of a
    /// generated listing. `None` where `[server] show_version` says not to sign at all.
    pub(in crate::app) signature: Option<HeaderValue>,
    /// Who runs this deployment, for the API description's `info.contact`. Separate from
    /// the signature above because it survives `show_version` being off.
    pub(in crate::app) contact: Option<Arc<str>>,
    /// Whether a directory's own `index.html` is served in place of a generated listing.
    pub(in crate::app) serve_mounted_index_html: bool,
    /// Whether a mounted `robots.txt` is served in place of the generated default.
    pub(in crate::app) serve_mounted_robots_txt: bool,
    /// The subtree the API answers under, normalized; `None` when API mode is off.
    pub(in crate::app) api_prefix: Option<Arc<str>>,
}

/// What the router hands a handler: the service, and the job resource where there is one.
///
/// **Two fields rather than a `jobs` on [`Service`], and the reason is a cycle.** The job
/// runner has to run a statement, so it holds a `Service`; a `Service` holding the runner
/// back would be an `Arc` cycle, and what that costs is not memory — it is the results
/// directory's `Drop`, which would then never run and would leave every clean shutdown
/// behaving like a crash.
#[derive(Debug, Clone)]
pub struct AppState {
    pub service: Service,
    /// `None` where this deployment publishes no TAP table, there being no resource then.
    pub(in crate::app) jobs: Option<Arc<Jobs>>,
}

impl AppState {
    /// The job resource, or the refusal a deployment without one answers.
    ///
    /// Unreachable through the router, which registers these routes only where there is a
    /// resource — so this is the belt to that braces, and says something true either way.
    pub(in crate::app) fn jobs(&self) -> Result<&Jobs, ApiError> {
        self.jobs
            .as_deref()
            .ok_or_else(|| ApiError::not_found("this service publishes no TAP tables"))
    }
}

impl axum::extract::FromRef<AppState> for Service {
    fn from_ref(state: &AppState) -> Self {
        state.service.clone()
    }
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
        tap: &TapConfig,
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
        // The TAP resources are siblings under the API's own prefix, so with API mode off
        // there is nowhere for a published table to be queried from. Refused rather than
        // ignored: an operator who wrote the list meant it to be reachable.
        if api_prefix.is_none() && !tap.tables.is_empty() {
            return Err(ConfigError::Route(
                "api.enabled is false, so the [[tap.table]] entries would be published at \
                 no url"
                    .to_owned(),
            ));
        }
        let transfers = Arc::new(Transfers::new(limits));
        // After the policy, which is what opening a store needs, and before the first
        // request: a mount over a store this deployment cannot reach is an operator's
        // mistake to hear about at startup, the way a local source that is not there is.
        mounts.check_sources(&policy, &transfers)?;
        let tap_tables = TapTableList::new(&tap.tables, &policy, &transfers)?;
        Ok(Self {
            policy: Arc::new(policy),
            transfers,
            mounts,
            data_files: Arc::new(data_files),
            tap_tables: Arc::new(tap_tables),
            sql_limits: limits.into(),
            catalog_limits: limits.into(),
            adql_limits: limits.into(),
            max_query_radius_arcsec: limits.max_query_radius_arcsec,
            answers: Arc::new(cache::Answers::new(
                limits.query_cache_seconds,
                limits.max_query_cache_bytes.as_u64(),
            )),
            request_timeout: (limits.max_request_seconds > 0)
                .then(|| Duration::from_secs(limits.max_request_seconds)),
            // Saturating rather than refusing: a 32-bit host cannot hold a body that large
            // anyway, so the cap it lands on is the one that machine could have honoured.
            request_body_limit: match limits.max_request_body_bytes.as_u64() {
                0 => None,
                bytes => Some(usize::try_from(bytes).unwrap_or(usize::MAX)),
            },
            signature: server.signature()?,
            contact: server.contact()?.map(Arc::from),
            serve_mounted_index_html: server.serve_mounted_index_html,
            serve_mounted_robots_txt: server.serve_mounted_robots_txt,
            api_prefix: api_prefix.map(Arc::from),
        })
    }

    /// A path under the API's own subtree belongs to API mode whether or not a route
    /// matched it, so a mistyped API path cannot fall through to a mount at `/`.
    pub(in crate::app) fn is_api_path(&self, path: &str) -> bool {
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
    pub(in crate::app) fn data_files_for(&self, url: &Url) -> &DataFiles {
        if url.scheme() != access::LOCAL_SCHEME {
            return &self.data_files;
        }
        self.mounts
            .resolve(url.path())
            .map_or(&self.data_files, |(mount, _)| mount.data_files())
    }
}

/// The router with no job resource behind it, which is what a test that is not about jobs
/// wants: building one makes a results directory and sweeps for dead runs, and a check about
/// a listing or a mount should pay for neither.
pub fn router(service: Service) -> Router {
    router_with(service, None)
}

/// The router, and the job resource it needs built first.
///
/// `jobs` is `None` where nothing is published over TAP. Building it is what creates the
/// results directory and reclaims what dead runs left, so a deployment that cannot write
/// results finds out at startup rather than on its first job — and that failure is fatal,
/// `/async` being a resource TAP §2.2 does not let an operator decline.
pub fn router_with(service: Service, jobs: Option<Arc<Jobs>>) -> Router {
    let timeout = service.request_timeout;
    let body_limit = service.request_body_limit;
    let signature = service.signature.clone();
    // Ahead of both modes and gated on neither: a crawler asks for this at the root
    // whether the deployment is an API, a file server, or both, and it is one answer
    // rather than one per mount.
    let mut router = Router::new().route(ROBOTS_TXT_PATH, get(robots_txt));
    if let Some(prefix) = service.api_prefix.clone() {
        router = router.route(&route(&prefix, "health"), get(health));
        // What the url names is a segment of its own. The spatial constraint is not — it is
        // one clause of a query, so a `{target}/{predicate}` path set would grow as the
        // product of the predicate kinds rather than their sum.
        router = with_queries(router, &prefix);
        // Not one of `with_queries`: a statement carries its own targets and its own
        // projection, which is the pair the other three routes take from the url and the body
        // separately.
        router = router.route(&route(&prefix, "adql"), post(query_adql));
        // Only where an operator published something. A TAP service with no table is one a
        // client can learn nothing from, so this deployment says it has none by not
        // answering at all rather than by answering every query with a refusal.
        if !service.tap_tables.is_empty() {
            router = with_tap(router, &prefix);
        }
        router = with_description(router, &prefix, service.contact.clone());
    }
    let mut router = router
        // Mounts claim whatever the API's routes did not, so a mount at `/` and the API
        // at `/api/v1` divide the url space without either being nested in the other.
        .fallback(serve_mounted)
        .with_state(AppState { service, jobs })
        .layer(compression());
    if let Some(limit) = timeout {
        router = router.layer(middleware::from_fn_with_state(limit, deadline));
    }
    // Replaces axum's own default rather than adding to it: whichever of the two is set
    // last is the one an extractor reads, so an operator raising this really does raise it.
    // Outside the deadline, since refusing an oversized body is not work the clock is about.
    router = router.layer(match body_limit {
        Some(bytes) => DefaultBodyLimit::max(bytes),
        None => DefaultBodyLimit::disable(),
    });
    let router = router
        .layer(decompression())
        // Method and path only. The default span carries the whole URI, including a
        // query string this service does not read but a caller may still have put
        // something in.
        //
        // Outside the deadline, so a request the clock cut off is logged as the `504` it
        // answered with rather than as a span that stops mid-request.
        .layer(
            TraceLayer::new_for_http().make_span_with(|request: &Request| {
                tracing::debug_span!(
                    "request",
                    method = %request.method(),
                    path = request.uri().path()
                )
            }),
        );
    // Outermost, so that it reaches the answers no handler produced: a `504` from the
    // deadline, a body the limit refused, a path no route matched. Those are exactly the
    // answers someone reporting that this deployment misbehaves has in front of them.
    match signature {
        Some(value) => router.layer(SetResponseHeaderLayer::overriding(header::SERVER, value)),
        None => router,
    }
}

/// Give up on a request that has run past `[limits] max_request_seconds`.
///
/// **The clock covers the query and not the sending of a file.** Which of those a body is
/// decides whether it is under the clock, and the body says so itself: `app::answer` marks
/// one it is still generating with [`answer::Generated`], and only those are read under
/// what is left of the deadline. A mounted file's bytes are not work — the handler's future
/// was finished before the first one went out — and bounding them would cut a slow download
/// this service is happy to serve.
///
/// A collected query is therefore bounded by the handler's own future, and a streamed one
/// by that future and then its body, which together are the whole of the request. The two
/// are one clock: the deadline is an instant taken when the request arrives, so time spent
/// planning is time the rows do not get.
///
/// Dropping the future is what stops the work, before the response and after it alike. The
/// reads below are DataFusion streams and object-store requests, none of which is polled
/// again once this returns, so a request nobody is waiting for stops costing the origin.
///
/// **A body cut here ends mid-chunk**, with the error the stream yields. The status and the
/// head of the document have gone, so there is no `504` left to send, and a truncated
/// transfer is the one thing a reader cannot mistake for a whole answer.
async fn deadline(State(limit): State<Duration>, request: Request, next: Next) -> Response {
    let until = tokio::time::Instant::now() + limit;
    let Ok(mut response) = tokio::time::timeout_at(until, next.run(request)).await else {
        return expired(limit).into_response();
    };
    if response
        .extensions_mut()
        .remove::<answer::Generated>()
        .is_none()
    {
        return response;
    }
    let (parts, body) = response.into_parts();
    let rest = futures::stream::unfold(
        Some(Box::pin(body.into_data_stream())),
        move |state| async move {
            let mut chunks = state?;
            match tokio::time::timeout_at(until, chunks.next()).await {
                Ok(Some(Ok(bytes))) => Some((Ok(bytes), Some(chunks))),
                Ok(Some(Err(error))) => Some((Err(ApiError::internal(error.to_string())), None)),
                Ok(None) => None,
                Err(_) => Some((Err(expired(limit)), None)),
            }
        },
    );
    Response::from_parts(parts, Body::from_stream(rest))
}

/// What a request that ran out of time is told, wherever it ran out.
fn expired(limit: Duration) -> ApiError {
    ApiError::timeout(format!(
        "the request took longer than {} s; narrow the region, the columns or the limit, or \
         send it to the plan route",
        limit.as_secs()
    ))
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

/// Read a body the caller compressed, in the same three encodings this service answers in.
///
/// The bodies that get large here are `region` — a serialized MOC, or one circle per source
/// of a cross-match — and both are repetitive text that gzip takes down by an order of
/// magnitude. There is no negotiating it: `Accept-Encoding` is the server saying what it can
/// send back, and HTTP has no counterpart for a request, so a caller cannot discover this and
/// has to be told. That is also why it is not the ordinary case and why nothing here depends
/// on it — `[limits] max_request_body_bytes` is sized for a body sent as it was written.
///
/// **The limit is measured on the expanded bytes, and that is what makes this safe.**
/// [`DefaultBodyLimit`] is enforced by the extractor, on whatever body the request holds by
/// then, so a small compressed body that expands without end trips the limit mid-decode and
/// the decoder stops being polled. The layer also drops `Content-Length` when it decodes, so
/// nothing downstream reads the compressed size as the body's size.
///
/// An encoding this service does not have is a `415` naming the ones it does, which is the
/// layer's own behaviour and the right one: a caller who guessed wrong learns that from the
/// status rather than from a complaint about byte 0 not being JSON.
fn decompression() -> RequestDecompressionLayer {
    RequestDecompressionLayer::new()
}

/// The first path segment of the three routes a projection and a predicate are sent to. A
/// plan's entries name it too.
pub(in crate::app) const QUERY_SEGMENT: &str = "simple";

/// The three routes a projection and a predicate are sent to.
///
/// They are registered together because they are the same request against three targets, and
/// a request that answered on only some of them would be one a caller has to remember the
/// exceptions to.
///
/// `POST`, not `GET`: the request carries credentials, and a query string is written to
/// every proxy's access log and the caller's shell history on the way. A body also has no
/// url-length limit — a long `IN` list and a wide column list both run past nginx's 8 KB
/// header buffer — and needs no url nested inside a url.
pub(in crate::app) fn with_queries(router: Router<AppState>, prefix: &str) -> Router<AppState> {
    let path = |target| route(prefix, &format!("{QUERY_SEGMENT}/{target}"));
    router
        .route(&path("parquet"), post(query_parquet))
        // The same body, against a catalog instead of a file: the url names a HATS
        // directory and this chooses the partitions to read out of it.
        .route(&path("hats"), post(hats::query_hats))
        // The same body again, resolved and not run. Two routes rather than one with a
        // mode: rows and a work list are different kinds of thing, and a field saying
        // which arrived is one more value a caller has to look at the body to trust.
        .route(&path("hats/plan"), post(hats::query_hats_plan))
}

/// The TAP resources. `/sync` answers `GET` and `POST` alike, which TAP §2.1 asks of a
/// DALI-sync resource: the two differ only in where the parameters are read from.
fn with_tap(router: Router<AppState>, prefix: &str) -> Router<AppState> {
    let job = |child: &str| route(prefix, &format!("tap/async/{{id}}{child}"));
    router
        .route(
            &route(prefix, "tap/sync"),
            get(tap::tap_sync_get).post(tap::tap_sync_post),
        )
        .route(&route(prefix, "tap/availability"), get(tap::availability))
        .route(&route(prefix, "tap/capabilities"), get(tap::capabilities))
        .route(&route(prefix, "tap/tables"), get(tap::tables))
        // Queries a client offers a user, which is the one resource here that exists to be
        // read by a person rather than by a program.
        .route(&route(prefix, "tap/examples"), get(tap::examples))
        // One table by name, which is how a client that has the name already avoids
        // fetching every column of every table.
        .route(&route(prefix, "tap/tables/{name}"), get(tap::table))
        // The job list, and one job. `DELETE` and a `POST` carrying `ACTION=DELETE` are
        // the same thing said two ways, UWS §2.2.3.2 offering the second for a client that
        // cannot send the first.
        .route(
            &route(prefix, "tap/async"),
            get(tap::list).post(tap::create),
        )
        .route(&job(""), get(tap::show).post(tap::act).delete(tap::destroy))
        // The child resources. Each is a value a client reads on its own and, where UWS
        // allows it, writes — and a write is honoured, clamped or refused, never dropped.
        .route(&job("/phase"), get(tap::phase).post(tap::set_phase))
        .route(
            &job("/executionduration"),
            get(tap::execution_duration).post(tap::set_execution_duration),
        )
        .route(
            &job("/destruction"),
            get(tap::destruction).post(tap::set_destruction),
        )
        .route(&job("/quote"), get(tap::quote))
        .route(&job("/owner"), get(tap::owner))
        .route(&job("/error"), get(tap::error))
        .route(
            &job("/parameters"),
            get(tap::job_parameters).post(tap::set_job_parameters),
        )
        .route(&job("/results"), get(tap::results))
        // Named rather than fixed at `result`, so that asking for a name this job has not
        // got says so instead of falling through to whatever the router matches next.
        .route(&job("/results/{name}"), get(tap::result))
}

/// The two routes that describe the rest: the document, and a page rendering it.
///
/// The document is built per request rather than once, because it names the prefix and the
/// prefix is the operator's. It is a few hundred microseconds of `serde_json` on a route
/// nothing calls in a loop.
fn with_description(
    router: Router<AppState>,
    prefix: &str,
    contact: Option<Arc<str>>,
) -> Router<AppState> {
    let document = route(prefix, "openapi.json");
    let page = openapi::page(&describe(prefix, contact.as_deref()), &document);
    router
        .route(
            &document,
            get({
                let prefix = prefix.to_owned();
                move || {
                    let prefix = prefix.clone();
                    let contact = contact.clone();
                    async move { Json(describe(&prefix, contact.as_deref())) }
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

/// The health response's schema, for the route that has no body to derive one from.
pub fn health_schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
    <HealthResponse as utoipa::PartialSchema>::schema()
}

/// One route under a prefix. The root prefix already ends in the separator, so joining
/// it the same way as any other would give `//health`.
pub(in crate::app) fn route(prefix: &str, name: &str) -> String {
    match prefix {
        "/" => format!("/{name}"),
        _ => format!("{prefix}/{name}"),
    }
}

/// What a parquet file is served as, whether it was read off a mount or encoded from a
/// query. `mime_guess` has no answer for the extension.
pub(in crate::app) const PARQUET_CONTENT_TYPE: &str = "application/vnd.apache.parquet";

#[derive(Debug, Serialize, utoipa::ToSchema)]
struct HealthResponse {
    status: &'static str,
}

async fn health() -> (StatusCode, Json<HealthResponse>) {
    (StatusCode::OK, Json(HealthResponse { status: "ok" }))
}

/// Where a crawler asks for the policy, whichever mode answers it.
const ROBOTS_TXT_PATH: &str = "/robots.txt";

/// The default: every path refused, except the three routes that describe the API rather
/// than serve data — a crawler that could not even read those could not say what it was
/// refused. `None` where the API is off, since there is then nothing to allow.
fn default_robots_txt(api_prefix: Option<&str>) -> String {
    let mut body = String::from("User-agent: *\nDisallow: /\n");
    if let Some(prefix) = api_prefix {
        for name in ["docs", "health", "openapi.json"] {
            body.push_str("Allow: ");
            body.push_str(&route(prefix, name));
            body.push('\n');
        }
    }
    body
}

/// `text/plain`, the way every `robots.txt` on the web is served.
fn robots_response(body: Vec<u8>) -> Response {
    ([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], body).into_response()
}

/// `/robots.txt`, answered by a mounted file where the operator opted in and one is
/// there, and by the generated default otherwise — the same fallback `index.html` gets.
///
/// Read fresh on every request rather than cached at startup, the way `index.html` is:
/// an operator publishing one expects editing it to take effect without a restart.
async fn robots_txt(State(service): State<Service>) -> Response {
    if service.serve_mounted_robots_txt
        && let Some((mount, relative)) = service.mounts.published(ROBOTS_TXT_PATH)
        && let Ok(segments) = mount::path_segments(relative)
        && let Some(contents) = mounted_robots_txt(&service, mount, &segments).await
    {
        return robots_response(contents);
    }
    robots_response(default_robots_txt(service.api_prefix.as_deref()).into_bytes())
}

/// The `robots.txt` a mount carries, or `None` where it carries none.
///
/// Every failure is a `None`: what this decides is which of two answers the root gives,
/// and a mount that cannot be read has not published a policy. The reason goes nowhere —
/// a crawler is not owed one, and the generated default is a complete answer.
async fn mounted_robots_txt(
    service: &Service,
    mount: &Mount,
    segments: &[String],
) -> Option<Vec<u8>> {
    match mount.source() {
        MountSource::Local(root) => {
            let mut requested = root.to_owned();
            requested.extend(segments);
            let file = access::local::authorize_mounted(mount, &requested).ok()?;
            file.is_file().then_some(())?;
            tokio::fs::read(&file).await.ok()
        }
        MountSource::Remote(_) => {
            let dir = mount.open(&service.policy, &service.transfers).ok()?;
            let bytes = dir.read_if_present(&segments.join("/")).await.ok()??;
            Some(bytes.to_vec())
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, header};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use crate::app::testing::{
        api_only, ask, ask_hats, body_of, get, mounted, respond, serving, with_limits, with_mount,
        with_server,
    };
    use crate::engine::query;

    use super::*;

    /// Everything is disallowed, except the three routes that describe the API rather than
    /// serve data — a crawler could not even say what it was refused without those.
    #[tokio::test]
    async fn robots_txt_disallows_everything_but_the_api_description() {
        let (status, body) = get("/robots.txt").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("Disallow: /\n"), "{body}");
        for path in ["/api/v1/docs", "/api/v1/health", "/api/v1/openapi.json"] {
            assert!(body.contains(&format!("Allow: {path}\n")), "{body}");
        }
    }

    /// The API off leaves nothing to allow, so the default is the bare refusal.
    #[tokio::test]
    async fn robots_txt_allows_nothing_where_the_api_is_off() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), b"x").unwrap();
        let service = mounted(
            dir.path(),
            &ApiConfig {
                enabled: false,
                ..Default::default()
            },
        );
        let response = respond(service, Request::builder().uri("/robots.txt")).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_of(response).await;
        assert!(body.contains("Disallow: /\n"), "{body}");
        assert!(!body.contains("Allow:"), "{body}");
    }

    /// A mounted file wins by default — the same rule `serve_mounted_index_html` applies to
    /// `index.html` — and only where one is actually there: a mount with none still gets
    /// the generated default rather than an empty answer.
    #[tokio::test]
    async fn a_mounted_robots_txt_wins_by_default_where_it_is_there() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("robots.txt"), b"User-agent: *\nAllow: /\n").unwrap();

        // Present, and taken by default: the mount's own text, verbatim.
        let response = respond(
            mounted(dir.path(), &ApiConfig::default()),
            Request::builder().uri("/robots.txt"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_of(response).await, "User-agent: *\nAllow: /\n");

        // Absent: the generated default, not an empty answer.
        std::fs::remove_file(dir.path().join("robots.txt")).unwrap();
        let response = respond(
            mounted(dir.path(), &ApiConfig::default()),
            Request::builder().uri("/robots.txt"),
        )
        .await;
        assert!(body_of(response).await.contains("Disallow: /\n"));
    }

    /// `serve_mounted_robots_txt = false` turns that around, the way
    /// `serve_mounted_index_html = false` does for the directory page: the mount's file
    /// stays there under its own name, and the root answers with the generated default
    /// regardless.
    #[tokio::test]
    async fn turning_it_off_ignores_a_mounted_robots_txt() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("robots.txt"), b"User-agent: *\nAllow: /\n").unwrap();
        let service = with_server(
            serving(dir.path()),
            &ApiConfig::default(),
            &LimitsConfig::default(),
            &ServerConfig {
                serve_mounted_robots_txt: false,
                ..ServerConfig::default()
            },
        );

        let response = respond(service.clone(), Request::builder().uri("/robots.txt")).await;
        assert!(body_of(response).await.contains("Disallow: /\n"));

        // Still there under its own name.
        let file = respond(service, Request::builder().uri("/robots.txt")).await;
        assert_eq!(file.status(), StatusCode::OK);
    }

    /// An unserved mount at `/` — API-only — offers nothing at the root for `robots.txt`
    /// to be read from, so the generated default is what answers even with a file there.
    #[tokio::test]
    async fn an_unserved_mount_offers_no_robots_txt_to_read() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("robots.txt"), b"User-agent: *\nAllow: /\n").unwrap();
        let service = with_server(
            crate::config::MountConfig {
                serve: false,
                ..serving(dir.path())
            },
            &ApiConfig::default(),
            &LimitsConfig::default(),
            &ServerConfig::default(),
        );
        let response = respond(service, Request::builder().uri("/robots.txt")).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(body_of(response).await.contains("Disallow: /\n"));
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

    /// The service signs what it answers, and the contact rides with it. Checked on a
    /// path no route matched, because the layer is outermost for exactly that reason:
    /// the answers a reporter has in front of them are as often a refusal as a body.
    #[tokio::test]
    async fn an_answer_says_what_produced_it() {
        let dir = tempfile::TempDir::new().unwrap();
        let signed = ServerConfig {
            contact: Some("ops@example.org".to_owned()),
            ..Default::default()
        };
        let service = with_server(
            serving(dir.path()),
            &ApiConfig::default(),
            &LimitsConfig::default(),
            &signed,
        );
        let expected = format!("{} (ops@example.org)", crate::config::PRODUCT);
        let (response, _) = fetch(service.clone(), "/", None).await;
        assert_eq!(response.headers()[header::SERVER], expected);
        let (missing, _) = fetch(service, "/nothing-here", None).await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        assert_eq!(missing.headers()[header::SERVER], expected);

        // And an operator who would rather not publish which version is running gets no
        // header at all, rather than one with the version taken out of it.
        let quiet = ServerConfig {
            show_version: false,
            contact: Some("ops@example.org".to_owned()),
            ..Default::default()
        };
        let service = with_server(
            serving(dir.path()),
            &ApiConfig::default(),
            &LimitsConfig::default(),
            &quiet,
        );
        let (response, _) = fetch(service, "/", None).await;
        assert!(!response.headers().contains_key(header::SERVER));
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

    /// `body`, gzipped, as a client that set `Content-Encoding` would send it.
    fn gzipped(body: &serde_json::Value) -> Vec<u8> {
        use std::io::Write as _;

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(body.to_string().as_bytes()).unwrap();
        encoder.finish().unwrap()
    }

    async fn post_bytes(
        service: Service,
        body: Vec<u8>,
        encoding: &str,
    ) -> (StatusCode, http::HeaderMap, String) {
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/simple/parquet")
            .header("content-type", "application/json")
            .header("content-encoding", encoding)
            .body(Body::from(body))
            .unwrap();
        let response = router(service).oneshot(request).await.unwrap();
        let (status, headers) = (response.status(), response.headers().clone());
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, headers, String::from_utf8_lossy(&body).into_owned())
    }

    /// A caller may compress a body, and it means exactly what the same body means sent as it
    /// was written. Worth a test of its own because nothing in the answer would say which way
    /// it arrived — a decode that silently produced different bytes would read as the
    /// caller's own mistake.
    #[tokio::test]
    async fn a_compressed_body_asks_the_same_question_as_a_plain_one() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), query::tests::fixture()).unwrap();
        let body = serde_json::json!({
            "url": "file:///part0.parquet",
            "columns": ["objectid"],
            "filters": "objectid < 4",
        });
        let service = || mounted(dir.path(), &ApiConfig::default());

        let (plain_status, plain) = ask(service(), body.clone()).await;
        let (status, _, answer) = post_bytes(service(), gzipped(&body), "gzip").await;

        assert_eq!(plain_status, StatusCode::OK, "{plain}");
        assert_eq!(status, StatusCode::OK, "{answer}");
        // Everything but how long it took, which is a measurement of this machine rather than
        // anything the request asked for.
        let without_timing = |body: &str| {
            let mut answer: serde_json::Value = serde_json::from_str(body).unwrap();
            answer.as_object_mut().unwrap().remove("elapsed_ms");
            answer
        };
        assert_eq!(without_timing(&answer), without_timing(&plain));
    }

    /// An encoding this service has not got is said so, rather than handed to the parser as
    /// bytes it cannot read. The two are a `415` and a `400` about byte 0, and only the first
    /// tells a caller what is actually wrong.
    #[tokio::test]
    async fn an_encoding_this_service_cannot_read_is_refused_as_one() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), query::tests::fixture()).unwrap();
        let body = serde_json::json!({"url": "file:///part0.parquet", "columns": ["objectid"]});

        let (status, headers, answer) = post_bytes(
            mounted(dir.path(), &ApiConfig::default()),
            gzipped(&body),
            "snappy",
        )
        .await;

        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE, "{answer}");
        // Which encodings it does have, since the caller has no other way to find out: there
        // is no negotiation for a request body.
        let accepted = headers[header::ACCEPT_ENCODING].to_str().unwrap();
        assert!(accepted.contains("gzip"), "{accepted}");
    }

    /// The bound is on the body the service ends up holding, never on the bytes that arrived.
    /// A compressed body that expands past it is refused the same as one sent that size —
    /// which is what stops a kilobyte from costing the process a gigabyte.
    #[tokio::test]
    async fn a_compressed_body_is_measured_after_it_is_expanded() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), query::tests::fixture()).unwrap();
        // Compresses to a few hundred bytes and expands to a megabyte, so the two ends of the
        // decode fall on opposite sides of the bound.
        let body = serde_json::json!({
            "url": "file:///part0.parquet",
            "filters": format!("'{}'", "x".repeat(1 << 20)),
        });
        let sent = gzipped(&body);
        assert!(sent.len() < 4096, "the fixture is not compressible enough");
        let limits = LimitsConfig {
            max_request_body_bytes: bytesize::ByteSize::kib(64),
            ..LimitsConfig::default()
        };

        let (status, _, answer) = post_bytes(
            with_limits(serving(dir.path()), &ApiConfig::default(), &limits),
            sent,
            "gzip",
        )
        .await;

        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{answer}");
    }

    /// A body past `[limits] max_request_body_bytes` is refused whole, and the bound is the
    /// operator's to raise: the same body goes through against a larger one.
    ///
    /// The refusal is a `413` rather than the `400` every other malformed body gets, since
    /// nothing was read: there is no field to name and nothing for the caller to respell.
    #[tokio::test]
    async fn a_request_body_is_bounded_by_what_the_operator_allows() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("part0.parquet"), query::tests::fixture()).unwrap();
        // An `IN` list is what a caller's body actually grows by, and this one is some
        // kilobytes — well past the small bound and well short of the large one.
        let ids = (0..2000)
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let body = serde_json::json!({
            "url": "file:///part0.parquet",
            "columns": ["objectid"],
            "filters": format!("objectid IN ({ids})"),
        });
        let bounded = |bytes| LimitsConfig {
            max_request_body_bytes: bytesize::ByteSize::b(bytes),
            ..LimitsConfig::default()
        };
        let service =
            |limits: &LimitsConfig| with_limits(serving(dir.path()), &ApiConfig::default(), limits);

        let (status, answer) = ask(service(&bounded(1024)), body.clone()).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{answer}");
        assert!(
            answer.contains("larger than this service accepts"),
            "{answer}"
        );

        let (status, answer) = ask(service(&bounded(1 << 20)), body).await;
        assert_eq!(status, StatusCode::OK, "{answer}");
    }

    /// The two bounds a caller is most likely to meet answer with different statuses, and
    /// telling them apart is the whole of what the distinction buys.
    ///
    /// A query-cost bound is reached by a body of a couple of hundred bytes — a handful of
    /// one-arcsecond circles scattered across a catalog touch a partition apiece — so a
    /// caller answered `413` for it reads "payload too large" about a payload that is
    /// nothing of the sort. The move that follows from that reading is raising a proxy's
    /// `client_max_body_size`, which changes nothing, and the bound that actually stopped
    /// them is never looked at. Hence `422` here and `413` only for the bytes.
    #[tokio::test]
    async fn a_costly_query_and_an_oversized_body_are_told_apart() {
        let dir = crate::hats::query::tests::fixture(true);
        let tiny = LimitsConfig {
            max_partitions: 1,
            max_request_body_bytes: bytesize::ByteSize::b(256),
            ..LimitsConfig::default()
        };
        let service = || {
            let mounts =
                Arc::new(Mounts::new(&[serving(dir.path())], &DataConfig::default()).unwrap());
            let policy = AccessPolicy::new(
                &crate::config::AccessConfig::default(),
                Arc::clone(&mounts),
                None,
            )
            .unwrap();
            Service::new(
                policy,
                &tiny,
                mounts,
                &ApiConfig::default(),
                &DataConfig::default(),
                &TapConfig::default(),
                &ServerConfig::default(),
            )
            .unwrap()
        };

        // Well inside the body bound, and over the partition bound.
        let small = serde_json::json!({"url": "file:///"});
        assert!(serde_json::to_vec(&small).unwrap().len() < 256);
        let (status, body) = ask_hats(service(), small).await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "a query-cost bound answered as a payload-size one: {body}"
        );

        // Over the body bound, and never planned at all.
        let large = serde_json::json!({
            "url": "file:///",
            "filters": format!("'{}'", "x".repeat(512)),
        });
        assert!(serde_json::to_vec(&large).unwrap().len() > 256);
        let (status, body) = ask_hats(service(), large).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
        assert!(body.contains("larger than this service accepts"), "{body}");
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
                &TapConfig::default(),
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
            &TapConfig::default(),
            &ServerConfig::default(),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("nothing to serve"), "{error}");
    }

    /// TAP's resources are siblings under the API's prefix, so with the API off a
    /// published table has no url — which is an operator's mistake to hear about rather
    /// than a list to quietly drop.
    #[tokio::test]
    async fn a_published_table_needs_the_api_to_be_on() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join("gaia")).unwrap();
        let tap = TapConfig {
            tables: vec![crate::config::TapTableConfig {
                name: "gaia_dr3.gaia_source".to_owned(),
                path: "/gaia".to_owned(),
                examples: Vec::new(),
            }],
            jobs: Default::default(),
        };
        let service = |enabled| {
            let mounts =
                Arc::new(Mounts::new(&[serving(dir.path())], &DataConfig::default()).unwrap());
            let policy = AccessPolicy::new(
                &crate::config::AccessConfig::default(),
                Arc::clone(&mounts),
                None,
            )
            .unwrap();
            Service::new(
                policy,
                &LimitsConfig::default(),
                mounts,
                &ApiConfig {
                    enabled,
                    ..Default::default()
                },
                &DataConfig::default(),
                &tap,
                &ServerConfig::default(),
            )
        };
        let error = service(false).unwrap_err().to_string();
        assert!(error.contains("no url"), "{error}");
        assert_eq!(
            service(true).unwrap().tap_tables.names(),
            ["gaia_dr3.gaia_source"]
        );
    }

    /// A handler that never finishes, behind the deadline. Nothing this service answers
    /// can be made slow to order, so the middleware is exercised over a route of the
    /// test's own — what is being checked is the layer and the status it produces.
    ///
    /// The last two are bodies rather than handlers: one this service is still generating
    /// and one it is only sending, both of which answer at once and then stall.
    fn timed(limit: Duration) -> Router {
        Router::new()
            .route("/slow", axum::routing::get(std::future::pending::<&str>))
            .route("/fast", axum::routing::get(|| async { "answered" }))
            .route(
                "/generated",
                axum::routing::get(|| async { stalling(true) }),
            )
            .route(
                "/download",
                axum::routing::get(|| async { stalling(false) }),
            )
            .layer(middleware::from_fn_with_state(limit, deadline))
    }

    /// A body that sends one chunk and then never sends another.
    fn stalling(generated: bool) -> Response {
        let chunks =
            futures::stream::once(async { Ok::<_, ApiError>(bytes::Bytes::from_static(b"rows")) })
                .chain(futures::stream::pending());
        let mut response = Body::from_stream(chunks).into_response();
        if generated {
            response.extensions_mut().insert(answer::Generated);
        }
        response
    }

    /// The clock covers the rows of a streamed answer, which is the whole of the work a
    /// request of that shape came to do.
    ///
    /// A handler that streams returns as soon as it has a plan, so a deadline over the
    /// handler alone would bound the planning and leave the reading unbounded — the one
    /// request with no time limit at all. The body is cut instead, mid-chunk: the `200` and
    /// the head of the document have gone, so a truncated transfer is what is left to say
    /// it with.
    #[tokio::test]
    async fn the_clock_covers_a_body_this_service_is_still_generating() {
        let response = timed(Duration::from_millis(10))
            .oneshot(
                Request::builder()
                    .uri("/generated")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Answered: the status went out with the first chunk, long before the clock ran.
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response.into_body().collect().await.is_err(),
            "a streamed body past the deadline has to end as a failure, not as an answer"
        );
    }

    /// And not the bytes of a file, which are not work.
    ///
    /// A mounted file's handler was finished before its first byte went out, so the clock
    /// has nothing left to bound — and cutting a slow download this service is happy to
    /// serve is what a bound over every body would do.
    #[tokio::test]
    async fn the_clock_does_not_cut_a_download() {
        let response = timed(Duration::from_millis(10))
            .oneshot(
                Request::builder()
                    .uri("/download")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), response.into_body().collect())
                .await
                .is_err(),
            "the deadline reached into a body it has no business bounding"
        );
    }

    async fn hit(router: Router, uri: &str) -> (StatusCode, String) {
        let response = router
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn a_request_that_runs_past_the_deadline_is_a_gateway_timeout() {
        let (status, body) = hit(timed(Duration::from_millis(10)), "/slow").await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert!(body.contains("took longer than"), "{body}");
    }

    #[tokio::test]
    async fn a_request_that_beats_the_deadline_is_answered_untouched() {
        let (status, body) = hit(timed(Duration::from_secs(30)), "/fast").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "answered");
    }

    /// `0` seconds is the operator turning the clock off, which has to be the absence of
    /// the layer rather than a deadline of no time at all.
    #[test]
    fn a_deadline_of_zero_seconds_is_no_deadline() {
        let with_zero = LimitsConfig {
            max_request_seconds: 0,
            ..LimitsConfig::default()
        };
        assert_eq!(service_with(&with_zero).request_timeout, None);
        assert_eq!(
            service_with(&LimitsConfig::default()).request_timeout,
            Some(Duration::from_secs(90))
        );
    }

    fn service_with(limits: &LimitsConfig) -> Service {
        Service::new(
            AccessPolicy::default(),
            limits,
            Arc::default(),
            &ApiConfig::default(),
            &DataConfig::default(),
            &TapConfig::default(),
            &ServerConfig::default(),
        )
        .unwrap()
    }
}
