use std::sync::Arc;
use std::time::Instant;

use axum::{
    Router,
    extract::{Request, State, rejection::JsonRejection},
    http::{StatusCode, header},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tower_http::trace::TraceLayer;

use crate::access::AccessPolicy;
use crate::config::LimitsConfig;
use crate::error::ApiError;
use crate::materialize::Transfers;
use crate::parquet_out;
use crate::query::{self, QueryResult, Selection};
use crate::storage::{self, RemoteFile, SourceUrl, StorageOptions, parse_url};

/// What every request needs and no request may change: the rules, and the shared scratch
/// budget. Both are built once at startup, so a request carries a handle rather than a
/// copy and two requests cannot disagree about either.
#[derive(Debug, Clone)]
pub struct Service {
    pub policy: Arc<AccessPolicy>,
    pub transfers: Arc<Transfers>,
}

impl Service {
    pub fn new(policy: AccessPolicy, limits: &LimitsConfig) -> Self {
        Self {
            policy: Arc::new(policy),
            transfers: Arc::new(Transfers::new(limits)),
        }
    }
}

pub fn router(service: Service) -> Router {
    Router::new()
        .route("/api/v1/health", get(health))
        // `POST`, not `GET`: the request carries credentials, and a query string is
        // written to every proxy's access log and the caller's shell history on the way.
        // A body also has no url-length limit and needs no url nested inside a url.
        .route("/api/v1/select", post(select))
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

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

async fn health() -> (StatusCode, Json<HealthResponse>) {
    (StatusCode::OK, Json(HealthResponse { status: "ok" }))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectRequest {
    /// Where the data is, treated as opaque: whatever query string it has belongs to
    /// the origin, not to us.
    url: SourceUrl,
    /// How to reach the store — a region, an endpoint, credentials. Absent means a
    /// public object read anonymously, which is the common case.
    #[serde(default)]
    storage: StorageOptions,
    /// What to select. These are the only domain parameters.
    column: String,
    value: String,
    /// Columns to return, dotted for nested fields (`lightcurve.mag`). Absent returns
    /// every column.
    columns: Option<Vec<String>>,
    /// `json` (the default) or `parquet`.
    format: Option<String>,
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

    fn parse(raw: Option<&str>) -> Result<Self, ApiError> {
        let Some(raw) = raw else {
            return Ok(Self::Json);
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
    const SHAPE: &str = "expected a JSON object with url, column and value, and \
                         optionally storage, columns, format";

    match rejection {
        // A parse failure quotes the position, not the contents.
        JsonRejection::JsonSyntaxError(error) => {
            ApiError::bad_request(format!("the request body is not valid JSON: {error}"))
        }
        JsonRejection::JsonDataError(error) => {
            let message = error.body_text();
            // `unknown field \`regoin\`` and `missing field \`column\`` name a key. Every
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

/// Reject a `columns` list that would silently return the wrong thing: an empty list
/// reads as "no columns" but would be served as "every column".
fn check_columns(columns: Option<&Vec<String>>) -> Result<(), ApiError> {
    let Some(columns) = columns else {
        return Ok(());
    };
    if columns.is_empty() || columns.iter().any(|path| path.trim().is_empty()) {
        return Err(ApiError::bad_request(
            "columns must be a non-empty list of non-empty column names",
        ));
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct SelectResponse {
    num_rows: usize,
    elapsed_ms: u128,
    rows: Vec<serde_json::Value>,
}

/// The count and the timing are part of the JSON body; a parquet body has no room for
/// them, so they travel as headers instead and both formats report the same numbers.
const NUM_ROWS_HEADER: &str = "x-hats-num-rows";
const ELAPSED_MS_HEADER: &str = "x-hats-elapsed-ms";

async fn select(
    State(service): State<Service>,
    body: Result<Json<SelectRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(params) = body.map_err(|rejection| body_error(&rejection))?;
    let started = Instant::now();
    // Everything decidable from the request alone, before anything is opened.
    let format = Format::parse(params.format.as_deref())?;
    check_columns(params.columns.as_ref())?;
    let file = storage::open(
        &parse_url(params.url.as_str())?,
        &params.storage,
        &service.policy,
        &service.transfers,
    )?;
    let result = query::run(
        &file,
        &Selection {
            filter_column: &params.column,
            filter_value: &params.value,
            columns: params.columns.as_deref(),
        },
    )
    .await?;

    let num_rows = result.num_rows();
    let response = match format {
        Format::Json => json_response(&result, started)?,
        Format::Parquet => parquet_response(&result, &file, num_rows, started).await?,
    };
    tracing::info!(
        // file.url, not the parameter: the parameter may carry credentials.
        url = %file.url,
        column = %params.column,
        columns = params
            .columns
            .as_ref()
            .map_or_else(|| "*".to_owned(), |paths| paths.join(",")),
        format = format.name(),
        num_rows,
        elapsed_ms = started.elapsed().as_millis(),
        "select"
    );
    Ok(response)
}

fn json_response(result: &QueryResult, started: Instant) -> Result<Response, ApiError> {
    let rows = query::to_json(result)?;
    Ok(Json(SelectResponse {
        num_rows: rows.len(),
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
            (
                header::CONTENT_TYPE,
                "application/vnd.apache.parquet".to_owned(),
            ),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{}\"", download_name(file)),
            ),
        ],
        [
            (NUM_ROWS_HEADER, num_rows.to_string()),
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

    async fn get(uri: &str) -> (StatusCode, String) {
        send(Request::builder().uri(uri), Body::empty()).await
    }

    /// A `POST /api/v1/select` with the given body, under a policy that allows
    /// everything — what the policy allows is `access.rs`'s business.
    async fn select_with(body: serde_json::Value) -> (StatusCode, String) {
        send(
            Request::builder()
                .method("POST")
                .uri("/api/v1/select")
                .header("content-type", "application/json"),
            Body::from(body.to_string()),
        )
        .await
    }

    /// The status and the body as text; axum's own rejections are plain text, ours are
    /// JSON.
    async fn send(request: axum::http::request::Builder, body: Body) -> (StatusCode, String) {
        let response = router(Service::new(
            AccessPolicy::default(),
            &LimitsConfig::default(),
        ))
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
    async fn select_is_not_a_get() {
        let (status, _) = get("/api/v1/select?url=s3://b/k.parquet&column=x&value=1").await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn a_missing_field_is_named() {
        let (status, body) = select_with(serde_json::json!({"url": "s3://b/k.parquet"})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("column"), "{body}");
    }

    #[tokio::test]
    async fn a_misspelled_field_is_named_rather_than_ignored() {
        let (status, body) = select_with(serde_json::json!({
            "url": "s3://b/k.parquet", "column": "x", "value": "1",
            "storage": {"regoin": "us-west-2"},
        }))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("regoin"), "{body}");
    }

    /// A body that does not fit is described, never quoted: a mistyped `storage` is
    /// exactly where a secret would be sitting.
    #[tokio::test]
    async fn a_body_that_does_not_fit_is_not_quoted_back() {
        let (status, body) = select_with(serde_json::json!({
            "url": "s3://b/k.parquet", "column": "x", "value": "1",
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
                .uri("/api/v1/select")
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
            "url": "ftp://example.com/a.parquet", "column": "x", "value": "1",
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
            "column": "x", "value": "1",
        }))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("query string"), "{body}");
        assert!(!body.contains(SECRET), "leaked: {body}");
    }

    #[test]
    fn rejects_empty_column_lists() {
        assert!(check_columns(None).is_ok());
        assert!(check_columns(Some(&vec!["objectid".to_owned()])).is_ok());
        for columns in [
            vec![],
            vec![String::new()],
            vec!["a".to_owned(), " ".to_owned()],
        ] {
            let error = check_columns(Some(&columns)).unwrap_err();
            assert!(error.to_string().contains("non-empty"), "{error}");
        }
    }

    #[tokio::test]
    async fn empty_column_lists_are_rejected_over_http() {
        let (status, body) = select_with(serde_json::json!({
            "url": "file:///nonexistent.parquet", "column": "x", "value": "1", "columns": [],
        }))
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("non-empty"), "{body}");
    }

    #[test]
    fn parses_the_format_parameter() {
        assert_eq!(Format::parse(None).unwrap(), Format::Json);
        assert_eq!(Format::parse(Some("json")).unwrap(), Format::Json);
        assert_eq!(Format::parse(Some("parquet")).unwrap(), Format::Parquet);
    }

    #[tokio::test]
    async fn unknown_formats_are_rejected() {
        let (status, body) = select_with(serde_json::json!({
            "url": "s3://b/k.parquet", "column": "x", "value": "1", "format": "csv",
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
        let params = SelectRequest {
            url: "s3://b/k.parquet".to_owned().into(),
            storage: StorageOptions {
                region: Some("us-west-2".to_owned()),
                secret_access_key: Some(SECRET.to_owned().into()),
                ..Default::default()
            },
            column: "objectid".to_owned(),
            value: "1".to_owned(),
            columns: None,
            format: None,
        };
        let shown = format!("{params:?}");
        assert!(!shown.contains(SECRET), "leaked: {shown}");
        assert!(shown.contains("s3://b/k.parquet"), "{shown}");
        assert!(shown.contains("us-west-2"), "{shown}");
    }

    #[tokio::test]
    async fn unparseable_urls_are_rejected() {
        let (status, body) =
            select_with(serde_json::json!({"url": "not-a-url", "column": "x", "value": "1"})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("invalid url"), "{body}");
    }
}
