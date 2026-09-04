use std::time::Instant;

use axum::{
    Router,
    extract::{Query, Request},
    http::{StatusCode, header},
    response::{IntoResponse, Json, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use tower_http::trace::TraceLayer;

use crate::error::ApiError;
use crate::parquet_out;
use crate::query::{self, QueryResult, Selection};
use crate::storage::{self, RemoteFile, parse_url};

pub fn router() -> Router {
    Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/select", get(select))
        // Method and path only. The default span carries the whole URI, and our query
        // string can hold S3 credentials.
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
struct SelectQuery {
    /// Where the data is. Storage-specific options ride in this URL's own query
    /// string, e.g. `s3://bucket/key.parquet?region=us-west-2`.
    url: String,
    /// What to select. These are the only domain parameters.
    column: String,
    value: String,
    /// Comma-separated columns to return, dotted for nested fields
    /// (`objectid,lightcurve.mag,objra`). Absent returns every column.
    columns: Option<String>,
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
    const NAMES: &'static [&'static str] = &["json", "parquet"];

    fn parse(raw: Option<&str>) -> Result<Self, ApiError> {
        match raw {
            None | Some("json") => Ok(Self::Json),
            Some("parquet") => Ok(Self::Parquet),
            Some(other) => Err(ApiError::bad_request(format!(
                "unknown format {other:?}; supported formats are {}",
                Self::NAMES.join(", ")
            ))),
        }
    }
}

/// Split the `columns` parameter, rejecting anything that would silently return the
/// wrong thing (an empty list, a stray comma).
fn parse_columns(raw: Option<&str>) -> Result<Option<Vec<String>>, ApiError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let paths: Vec<String> = raw.split(',').map(|p| p.trim().to_owned()).collect();
    if paths.iter().any(|p| p.is_empty()) {
        return Err(ApiError::bad_request(
            "columns must be a comma-separated list of non-empty column names",
        ));
    }
    Ok(Some(paths))
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

async fn select(Query(params): Query<SelectQuery>) -> Result<Response, ApiError> {
    let started = Instant::now();
    let format = Format::parse(params.format.as_deref())?;
    let file = storage::open(&parse_url(&params.url)?)?;
    let columns = parse_columns(params.columns.as_deref())?;
    let result = query::run(
        &file,
        &Selection {
            filter_column: &params.column,
            filter_value: &params.value,
            columns: columns.as_deref(),
        },
    )
    .await?;

    let num_rows: usize = result.batches.iter().map(|batch| batch.num_rows()).sum();
    let response = match format {
        Format::Json => json_response(&result, started)?,
        Format::Parquet => parquet_response(&result, &file, num_rows, started).await?,
    };
    tracing::info!(
        // file.url, not the parameter: the parameter may carry credentials.
        url = %file.url,
        column = %params.column,
        columns = params.columns.as_deref().unwrap_or("*"),
        format = ?format,
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

    /// Returns the status and the body as text; axum's own rejections (a missing
    /// query parameter) are plain text, ours are JSON.
    async fn get(uri: &str) -> (StatusCode, String) {
        let response = router()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
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

    #[tokio::test]
    async fn missing_parameters_are_rejected() {
        let (status, _) = get("/api/v1/select?url=s3://b/k.parquet").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn unsupported_schemes_are_rejected() {
        let (status, body) =
            get("/api/v1/select?url=https://example.com/a.parquet&column=x&value=1").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("unsupported URL scheme"), "{body}");
    }

    #[test]
    fn parses_the_columns_parameter() {
        assert_eq!(parse_columns(None).unwrap(), None);
        assert_eq!(
            parse_columns(Some("objectid, lightcurve.mag ,objra")).unwrap(),
            Some(vec![
                "objectid".to_owned(),
                "lightcurve.mag".to_owned(),
                "objra".to_owned()
            ])
        );
    }

    #[test]
    fn rejects_empty_column_lists() {
        for raw in ["", "objectid,", ",objectid", "objectid,,objra"] {
            let error = parse_columns(Some(raw)).unwrap_err();
            assert!(error.to_string().contains("non-empty"), "{raw}: {error}");
        }
    }

    #[tokio::test]
    async fn empty_columns_parameter_is_rejected() {
        let (status, body) =
            get("/api/v1/select?url=s3://b/k.parquet&column=x&value=1&columns=").await;
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
        let (status, body) =
            get("/api/v1/select?url=s3://b/k.parquet&column=x&value=1&format=csv").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("unknown format"), "{body}");
        assert!(body.contains("json, parquet"), "{body}");
    }

    #[test]
    fn names_the_download_after_the_source_object() {
        let name = |raw: &str| download_name(&storage::open(&parse_url(raw).unwrap()).unwrap());
        assert_eq!(
            name("s3://b/dir/part0.snappy.parquet"),
            "part0.snappy.parquet"
        );
        // HATS partition paths, and anything else that is not already a parquet name.
        assert_eq!(name("s3://b/Norder=5/Npix=12240/part0"), "part0.parquet");
    }

    #[tokio::test]
    async fn unparseable_urls_are_rejected() {
        let (status, body) = get("/api/v1/select?url=not-a-url&column=x&value=1").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.contains("invalid url"), "{body}");
    }
}
