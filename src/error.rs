use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use std::path::Path;

use datafusion::error::DataFusionError;
use datafusion::parquet::errors::ParquetError;
use serde::Serialize;

use crate::materialize::{Refused, TooLarge};

/// Whether a message says where on disk the file is. A mount publishes a directory, not
/// the machine it is on, so a message that names one is not repeatable to a caller
/// however useful the rest of it is. The directory is checked rather than the file: a
/// message naming any of what is beside it names that too.
fn names_path(message: &str, file: &Path) -> bool {
    [file.parent(), Some(file)]
        .into_iter()
        .flatten()
        .filter_map(Path::to_str)
        .any(|path| !path.is_empty() && message.contains(path))
}

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
    /// The route exists and this verb is not one of its. Nothing here is written, so
    /// every route has the same answer to a verb that would write.
    #[error("{0}")]
    MethodNotAllowed(String),
    /// Something on this side went wrong. The message is ours, and says nothing about
    /// the machine it happened on.
    #[error("{0}")]
    Internal(String),
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

    pub fn method_not_allowed(message: impl Into<String>) -> Self {
        Self::MethodNotAllowed(message.into())
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }

    /// The same failure, told to a caller who named a path in a mount rather than a url
    /// of their own.
    ///
    /// Two things are wrong with a store's own account of itself here. Its message names
    /// the path it was reading, which for a mount is the operator's absolute local path
    /// and no part of what the caller asked for; and its status describes a store that
    /// the caller never named. A mount has no origin behind it, so `502` blames a gateway
    /// that does not exist, and `500` blames this service for a file whose bytes it will
    /// hand over quite happily without a query on the url.
    ///
    /// So every failure a store or a reader raises against a mounted file is the caller
    /// having asked a file that is not queryable data for rows, which is what the parquet
    /// reader is there to decide and what `[data] filenames` cannot: `_metadata` is on
    /// that list by default, and the rows its footer describes are in the files beside
    /// it. The cost is that a genuine local I/O error reads to the caller as a bad
    /// request. The log is where those stay distinguishable, and a disk that cannot be
    /// read will fail the plain byte-serving path too, where nothing dresses it up.
    ///
    /// What must not be swept in with them is the planner's account of a query it cannot
    /// run — a column that is not there, two types that will not compare. That is about
    /// what the caller wrote, and telling them their file is not parquet sends them to
    /// look at the one thing that is not wrong. Those messages are passed through, unless
    /// one names where the file is, which is the operator's business either way.
    ///
    /// Only for a mount. In API mode the path in such a message is the caller's own url,
    /// which they already have, and `502` is the truth about a store that really is one.
    pub fn from_mount(self, file: &Path) -> Self {
        let unreadable = |error: &Self| {
            tracing::warn!(
                error = %error,
                status = %error.status(),
                "cannot read a mounted file as data"
            );
            Self::BadRequest(
                "this file cannot be read as parquet data; it is not a parquet file, \
                 or the rows its footer describes are not in it"
                    .to_owned(),
            )
        };
        match self {
            // Written here rather than by a store, which is what makes them safe to
            // repeat. The planner's account of a misspelled column arrives this way, and
            // it is the most useful message a caller gets.
            ours @ (Self::BadRequest(_)
            | Self::Forbidden(_)
            | Self::NotFound(_)
            | Self::MethodNotAllowed(_)
            | Self::Internal(_)) => ours,
            // Getting the bytes, or reading them as parquet. These are the failures the
            // one sentence is for, and the ones whose messages name the path.
            bytes @ (Self::ObjectStore(_)
            | Self::Storage(_)
            | Self::SourceMetadata(_)
            | Self::DataFusion(
                DataFusionError::ObjectStore(_)
                | DataFusionError::IoError(_)
                | DataFusionError::ParquetError(_),
            )) => unreadable(&bytes),
            // Everything else DataFusion says is about the query rather than the file: a
            // column that is not there, two types that will not compare — the last of
            // which arrives as an optimizer rule wrapping an arrow cast, several layers
            // from anything one would think to match on. So this is a rule about what is
            // *not* the file, with the path guard standing behind it.
            Self::DataFusion(error) if !names_path(&error.to_string(), file) => {
                Self::BadRequest(error.to_string())
            }
            foreign => unreadable(&foreign),
        }
    }

    /// The status this error answers with. Public so that a test can check the status a
    /// caller sees rather than the message, which is the part that has to be right.
    pub fn status(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::MethodNotAllowed(_) => StatusCode::METHOD_NOT_ALLOWED,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ObjectStore(error) => object_store_status(error),
            Self::Storage(error) => storage_status(error),
            // The remote file being missing or unreadable reaches us wrapped in a
            // DataFusionError, and is the caller's problem, not ours.
            Self::DataFusion(DataFusionError::ObjectStore(error)) => object_store_status(error),
            // Every input to planning — the url, the select list, the predicate —
            // comes from the caller, so a planning failure is a bad request. A missing
            // file lands here, as a failure to infer a schema from nothing.
            Self::DataFusion(DataFusionError::Plan(_)) => StatusCode::BAD_REQUEST,
            // The source file is the caller's, and a footer we cannot parse is a
            // problem with it, not with us.
            Self::SourceMetadata(ParquetError::External(_)) => StatusCode::BAD_GATEWAY,
            Self::SourceMetadata(_) => StatusCode::BAD_REQUEST,
            // The same judgement for the read that planning does, and for the same
            // reason: the object the caller named is not a parquet file, or is one that
            // has been truncated. Nothing decides that from the key — an object is not
            // parquet because of what is in it — so this is where a caller who pointed
            // at the wrong thing finds out, and it is their mistake rather than a fault
            // of this service.
            Self::DataFusion(DataFusionError::ParquetError(error)) => match error.as_ref() {
                ParquetError::External(_) => StatusCode::BAD_GATEWAY,
                _ => StatusCode::BAD_REQUEST,
            },
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
        // A store is free to wrap anything in `Generic`, so the default below would
        // report this service's own limits as the origin misbehaving.
        object_store::Error::Generic { source, .. } => match source.downcast_ref::<Refused>() {
            Some(refused) => refused_status(&refused.source),
            None => StatusCode::BAD_GATEWAY,
        },
        _ => StatusCode::BAD_GATEWAY,
    }
}

/// A copy this service would not make. The distinction that matters to a caller is
/// whether asking again could work.
fn refused_status(error: &TooLarge) -> StatusCode {
    match error {
        // The object is larger than this service will ever copy, or it will not copy at
        // all. Retrying changes nothing.
        TooLarge::Declared { .. } | TooLarge::WhileStreaming { .. } | TooLarge::Disabled => {
            StatusCode::PAYLOAD_TOO_LARGE
        }
        // The process-wide budget was full, which is other requests rather than this
        // one. The same request later is a different answer.
        TooLarge::Total { .. } => StatusCode::SERVICE_UNAVAILABLE,
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
