//! What the route tests share: a service built around a mount or around nothing, and a request
//! sent through the whole router.

use std::path::Path;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::Response;
use http_body_util::BodyExt;
use tower::ServiceExt;

use crate::access::AccessPolicy;
use crate::access::mount::Mounts;
use crate::app::{Service, router};
use crate::config::{ApiConfig, DataConfig, LimitsConfig, ServerConfig, TapConfig};
use crate::storage::StorageOptions;

pub(in crate::app) const SECRET: &str = "wJalrXUtnFEMIsecretKEY";

/// The API alone, at its default prefix and with nothing mounted.
pub(in crate::app) fn api_only() -> Service {
    Service::new(
        AccessPolicy::default(),
        &LimitsConfig::default(),
        Arc::default(),
        &ApiConfig::default(),
        &DataConfig::default(),
        &TapConfig::default(),
        &ServerConfig::default(),
    )
    .unwrap()
}

pub(in crate::app) async fn get(uri: &str) -> (StatusCode, String) {
    send(Request::builder().uri(uri), Body::empty()).await
}

/// A `POST /api/v1/simple/parquet` with the given body, under a policy that allows
/// everything — what the policy allows is `access`'s business.
pub(in crate::app) async fn post_parquet(body: serde_json::Value) -> (StatusCode, String) {
    send(
        Request::builder()
            .method("POST")
            .uri("/api/v1/simple/parquet")
            .header("content-type", "application/json"),
        Body::from(body.to_string()),
    )
    .await
}

/// The status and the body as text; axum's own rejections are plain text, ours are
/// JSON.
pub(in crate::app) async fn send(
    request: http::request::Builder,
    body: Body,
) -> (StatusCode, String) {
    let response = router(api_only())
        .oneshot(request.body(body).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

/// One `[[mount]]`, and a service built around it.
pub(in crate::app) fn with_mount(config: crate::config::MountConfig, api: &ApiConfig) -> Service {
    with_limits(config, api, &LimitsConfig::default())
}

/// The same, for the cases that are about a bound rather than about a route.
pub(in crate::app) fn with_limits(
    config: crate::config::MountConfig,
    api: &ApiConfig,
    limits: &LimitsConfig,
) -> Service {
    with_server(config, api, limits, &ServerConfig::default())
}

/// And the same for the cases that are about what the file server publishes.
pub(in crate::app) fn with_server(
    config: crate::config::MountConfig,
    api: &ApiConfig,
    limits: &LimitsConfig,
    server: &ServerConfig,
) -> Service {
    with_tap(config, api, limits, server, &TapConfig::default())
}

/// The same, publishing a list of TAP tables over the mount.
pub(in crate::app) fn with_tap(
    config: crate::config::MountConfig,
    api: &ApiConfig,
    limits: &LimitsConfig,
    server: &ServerConfig,
    tap: &TapConfig,
) -> Service {
    let mounts = Arc::new(Mounts::new(&[config], &DataConfig::default()).unwrap());
    let policy = AccessPolicy::new(
        &crate::config::AccessConfig::default(),
        Arc::clone(&mounts),
        None,
    )
    .unwrap();
    Service::new(
        policy,
        limits,
        mounts,
        api,
        &DataConfig::default(),
        tap,
        server,
    )
    .unwrap()
}

/// A `[[mount]]` publishing `dir` at `/`, which is what most of these want.
pub(in crate::app) fn serving(dir: &Path) -> crate::config::MountConfig {
    crate::config::MountConfig {
        path: "/".to_owned(),
        source: dir.display().to_string(),
        serve: true,
        follow_symlinks: false,
        immutable: false,
        storage: StorageOptions::default(),
        filenames: None,
    }
}

/// A directory with one file in it, and a service that publishes it at `/`.
pub(in crate::app) fn mounted(dir: &Path, api: &ApiConfig) -> Service {
    with_mount(serving(dir), api)
}

pub(in crate::app) async fn respond(service: Service, request: http::request::Builder) -> Response {
    router(service)
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

pub(in crate::app) async fn body_of(response: Response) -> String {
    let body = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(body.to_vec()).unwrap()
}

/// A `POST` of `body` to one route of a service's own API, as JSON.
pub(in crate::app) async fn post_json(
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
pub(in crate::app) async fn ask(service: Service, body: serde_json::Value) -> (StatusCode, String) {
    post_json(service, "/api/v1/simple/parquet", body).await
}

/// The same, for an answer that is not text: a parquet body is read back as a file.
pub(in crate::app) async fn ask_bytes(
    service: Service,
    path: &str,
    body: serde_json::Value,
) -> (StatusCode, bytes::Bytes) {
    let response = respond_to(service, path, body).await;
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, body)
}

/// The whole response, for a test that is about the headers.
pub(in crate::app) async fn respond_to(
    service: Service,
    path: &str,
    body: serde_json::Value,
) -> Response {
    let request = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    router(service).oneshot(request).await.unwrap()
}

pub(in crate::app) async fn ask_hats(
    service: Service,
    body: serde_json::Value,
) -> (StatusCode, String) {
    post_json(service, "/api/v1/simple/hats", body).await
}
