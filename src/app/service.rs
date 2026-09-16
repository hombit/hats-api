//! The service as a whole: what every request shares, and the router that divides the url space
//! between the API's routes and the mounts.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    extract::{DefaultBodyLimit, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{Html, IntoResponse, Json, Response},
    routing::{get, post},
};
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
use crate::access::mount::{self, Mounts};
use crate::access::{self, AccessPolicy};
use crate::adql;
use crate::app::files::serve_mounted;
use crate::app::openapi::{self, description::describe};
use crate::app::routes::{adql::query_adql, hats, parquet::query_parquet};
use crate::config::{ApiConfig, ConfigError, DataConfig, LimitsConfig, ServerConfig};
use crate::engine::sql;
use crate::error::ApiError;
use crate::hats::query::CatalogLimits;
use crate::storage::materialize::Transfers;

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
    /// How much SQL one request may carry.
    pub sql_limits: sql::Limits,
    /// What a request against a whole catalog may spend.
    pub catalog_limits: CatalogLimits,
    /// What one ADQL statement may spend.
    pub adql_limits: adql::query::Limits,
    /// The widest circle a query string may ask for. The file-server mode's bound alone: a
    /// url is followed rather than fanned out, so what it asks for has to fit in one answer.
    pub(in crate::app) max_query_radius_arcsec: f64,
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
    pub(in crate::app) serve_index_html: bool,
    /// The subtree the API answers under, normalized; `None` when API mode is off.
    pub(in crate::app) api_prefix: Option<Arc<str>>,
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
            adql_limits: limits.into(),
            max_query_radius_arcsec: limits.max_query_radius_arcsec,
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
            serve_index_html: server.serve_index_html,
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

pub fn router(service: Service) -> Router {
    let timeout = service.request_timeout;
    let body_limit = service.request_body_limit;
    let signature = service.signature.clone();
    let mut router = Router::new();
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
        router = with_description(router, &prefix, service.contact.clone());
    }
    let mut router = router
        // Mounts claim whatever the API's routes did not, so a mount at `/` and the API
        // at `/api/v1` divide the url space without either being nested in the other.
        .fallback(serve_mounted)
        .with_state(service)
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
/// The clock covers producing the response and not sending it. That is the whole of why
/// this sits here rather than over the body: every query is collected before it answers,
/// so a handler's own future is the work, while a mounted file's is already finished when
/// the first byte goes out. A bound over the body would cut a slow download of a file this
/// service would happily serve, and bound nothing a query does.
///
/// Dropping the future is what stops the work. The reads below it are DataFusion streams
/// and object-store requests, none of which is polled again once this returns, so a request
/// nobody is waiting for stops costing the origin as well.
async fn deadline(State(limit): State<Duration>, request: Request, next: Next) -> Response {
    match tokio::time::timeout(limit, next.run(request)).await {
        Ok(response) => response,
        Err(_) => ApiError::timeout(format!(
            "the request took longer than {} s; narrow the region, the columns or the \
             limit, or send it to the plan route",
            limit.as_secs()
        ))
        .into_response(),
    }
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
pub(in crate::app) fn with_queries(router: Router<Service>, prefix: &str) -> Router<Service> {
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

/// The two routes that describe the rest: the document, and a page rendering it.
///
/// The document is built per request rather than once, because it names the prefix and the
/// prefix is the operator's. It is a few hundred microseconds of `serde_json` on a route
/// nothing calls in a loop.
fn with_description(
    router: Router<Service>,
    prefix: &str,
    contact: Option<Arc<str>>,
) -> Router<Service> {
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

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, header};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use crate::app::testing::{
        api_only, ask, ask_hats, get, mounted, respond, serving, with_limits, with_mount,
        with_server,
    };
    use crate::engine::query;

    use super::*;

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

    /// A handler that never finishes, behind the deadline. Nothing this service answers
    /// can be made slow to order, so the middleware is exercised over a route of the
    /// test's own — what is being checked is the layer and the status it produces.
    fn timed(limit: Duration) -> Router {
        Router::new()
            .route("/slow", axum::routing::get(std::future::pending::<&str>))
            .route("/fast", axum::routing::get(|| async { "answered" }))
            .layer(middleware::from_fn_with_state(limit, deadline))
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
            &ServerConfig::default(),
        )
        .unwrap()
    }
}
