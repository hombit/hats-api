use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use datafusion::error::DataFusionError;
use datafusion::parquet::errors::ParquetError;
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    /// The policy in the config file says no. Distinct from a store's own 403: this
    /// one is the server's own rule, and the caller cannot fix it with credentials.
    #[error("{0}")]
    Forbidden(String),
    #[error("{0}")]
    NotFound(String),
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    /// Raised while building a store, before any request. DataFusion's own reads come
    /// back as [`Self::ObjectStore`], because the adapter translates them.
    #[error("cannot open storage: {0}")]
    Storage(#[from] opendal::Error),
    #[error("query failed: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("failed to encode result as JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("arrow error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    /// Reading the source file's footer, to copy its layout into the answer.
    #[error("failed to read the parquet metadata of the source file: {0}")]
    SourceMetadata(#[source] ParquetError),
    #[error("failed to encode result as parquet: {0}")]
    ParquetWrite(#[source] ParquetError),
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

impl ApiError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::Forbidden(message.into())
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }

    fn status(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::ObjectStore(error) => object_store_status(error),
            Self::Storage(error) => storage_status(error),
            // The remote file being missing or unreadable reaches us wrapped in a
            // DataFusionError, and is the caller's problem, not ours.
            Self::DataFusion(DataFusionError::ObjectStore(error)) => object_store_status(error),
            // Every input to planning — the url, the column, the value, the columns
            // — comes from the caller, so a planning failure is a bad request. A
            // missing file lands here, as a failure to infer a schema from nothing.
            Self::DataFusion(DataFusionError::Plan(_)) => StatusCode::BAD_REQUEST,
            // The source file is the caller's, and a footer we cannot parse is a
            // problem with it, not with us.
            Self::SourceMetadata(ParquetError::External(_)) => StatusCode::BAD_GATEWAY,
            Self::SourceMetadata(_) => StatusCode::BAD_REQUEST,
            Self::DataFusion(_) | Self::Json(_) | Self::Arrow(_) | Self::ParquetWrite(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}

/// Every storage option a store is built from came out of the caller's url, so a
/// configuration the backend will not accept is their mistake to fix, not a fault of
/// the service.
fn storage_status(error: &opendal::Error) -> StatusCode {
    match error.kind() {
        opendal::ErrorKind::NotFound => StatusCode::NOT_FOUND,
        opendal::ErrorKind::PermissionDenied => StatusCode::FORBIDDEN,
        opendal::ErrorKind::ConfigInvalid => StatusCode::BAD_REQUEST,
        _ => StatusCode::BAD_GATEWAY,
    }
}

fn object_store_status(error: &object_store::Error) -> StatusCode {
    match error {
        object_store::Error::NotFound { .. } => StatusCode::NOT_FOUND,
        object_store::Error::PermissionDenied { .. }
        | object_store::Error::Unauthenticated { .. } => StatusCode::FORBIDDEN,
        _ => StatusCode::BAD_GATEWAY,
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();
        let message = self.to_string();
        if status.is_server_error() {
            tracing::error!(error = %message, "request failed");
        } else {
            tracing::debug!(error = %message, %status, "request rejected");
        }
        (status, Json(ErrorResponse { error: message })).into_response()
    }
}
