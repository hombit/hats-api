use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use std::path::Path;

use datafusion::error::DataFusionError;
use datafusion::parquet::errors::ParquetError;
use serde::Serialize;
use url::Url;

use crate::storage::materialize::{Refused, TooLarge};

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
    /// The request is well formed and asks for more work than this service will do. Not a
    /// [`Self::BadRequest`]: nothing about it is wrong, and the same request against a
    /// smaller region — or against a deployment configured to allow more — is answered.
    #[error("{0}")]
    TooMuchWork(String),
    /// The bytes the caller sent are past `[limits] max_request_body_bytes`.
    ///
    /// Distinct from [`Self::TooMuchWork`], and the distinction is the whole point of the
    /// variant: that one is about what the request asks this service to *read*, which a
    /// body of a few hundred bytes can ask for, while this one is about the size of the
    /// request itself. Answering both with one status leaves a caller holding a `413` that
    /// says "payload too large" about a payload that is nothing of the sort — and the
    /// obvious next move, raising a proxy's `client_max_body_size`, changes nothing.
    #[error("{0}")]
    BodyTooLarge(String),
    /// The request ran past `[limits] max_request_seconds` and was dropped where it
    /// stood. Distinct from [`Self::TooMuchWork`], which is a bound checked against what
    /// the request asks for and answers with the plan: this one is reached with work
    /// already done and nothing to hand back, so the message is all the caller gets.
    #[error("{0}")]
    Timeout(String),
    /// Something on this side went wrong. The message is ours, and says nothing about
    /// the machine it happened on.
    #[error("{0}")]
    Internal(String),
    /// A failure whose own account of itself named where a mount really is, told without
    /// it. [`ApiError::from_mounted_store`] is the only thing that makes one.
    ///
    /// The status is carried rather than derived, which is the whole of why this is a
    /// variant and not a message: there is an origin behind a store-backed mount, so a
    /// `502` about it and a `404` about an object that is not there are both true, and
    /// flattening them into the `400` a local mount gets would answer "your file is not
    /// parquet" about a store that was down.
    #[error("{1}")]
    Hidden(StatusCode, String),
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

    pub fn too_much_work(message: impl Into<String>) -> Self {
        Self::TooMuchWork(message.into())
    }

    pub fn body_too_large(message: impl Into<String>) -> Self {
        Self::BodyTooLarge(message.into())
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::Timeout(message.into())
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
    /// For every local file, whichever mode reached it. A caller who named one wrote a
    /// mount's `path`, and a store's message names its `source` — so the path in it is
    /// the operator's there too. A remote url keeps its own message: the path in that one
    /// is the caller's own url, which they already have, and `502` is the truth about a
    /// store that really is one.
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
            | Self::TooMuchWork(_)
            | Self::BodyTooLarge(_)
            | Self::Timeout(_)
            | Self::Internal(_)
            | Self::Hidden(..)) => ours,
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

    /// The same failure, told to a caller who named a path under a mount whose `source`
    /// is a store.
    ///
    /// The rule is [`Self::from_mount`]'s and the reason is the same one: a mount
    /// publishes a directory and not where that directory really is, so a store's own
    /// account of a key — which names the operator's bucket, endpoint and prefix, none of
    /// them any part of what the caller wrote — is not repeatable as it stands.
    ///
    /// What differs is everything the local rule turns on. There really is an origin
    /// behind this one, so `502` blames something that exists and `404` is the honest
    /// answer for an object that is not there; and the bytes of an unreadable object are
    /// not something this service would hand over regardless, so a read that failed is not
    /// evidence about the file. So the status is kept and only the message is replaced,
    /// and only where the message names the source at all — a planner's account of a
    /// column that is not there is the caller's own mistake here as it is there.
    pub fn from_mounted_store(self, source: &Url) -> Self {
        // The whole url, its authority, and the prefix under it: a message carrying any of
        // the three says where the mount really is. The path is checked without its
        // separators so that a store spelling a key `hats/dr1/x.parquet` is caught by the
        // `hats` the operator wrote.
        let names_source = |message: &str| {
            let path = source.path().trim_matches('/');
            message.contains(source.as_str())
                || source
                    .host_str()
                    .is_some_and(|host| !host.is_empty() && message.contains(host))
                || (!path.is_empty() && message.contains(path))
        };
        match self {
            // Written here rather than by a store, which is what makes them safe to
            // repeat.
            ours @ (Self::BadRequest(_)
            | Self::Forbidden(_)
            | Self::NotFound(_)
            | Self::MethodNotAllowed(_)
            | Self::TooMuchWork(_)
            | Self::BodyTooLarge(_)
            | Self::Timeout(_)
            | Self::Internal(_)
            | Self::Hidden(..)) => ours,
            foreign if !names_source(&foreign.to_string()) => foreign,
            foreign => {
                let status = foreign.status();
                tracing::warn!(
                    error = %foreign,
                    %status,
                    "a mounted store said where it is; telling the caller the status alone"
                );
                Self::Hidden(
                    status,
                    "this mount could not read the file at that path".to_owned(),
                )
            }
        }
    }

    /// The status this error answers with. Public so that a test can check the status a
    /// caller sees rather than the message, which is the part that has to be right.
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Hidden(status, _) => *status,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::MethodNotAllowed(_) => StatusCode::METHOD_NOT_ALLOWED,
            // Not `413`: the request content is the caller's query, and it is well formed
            // and ordinarily small. What is too large is the read it asks for, which is
            // `422`'s case — a body understood, and a set of instructions this service will
            // not carry out. `413` is `Self::BodyTooLarge`'s alone.
            Self::TooMuchWork(_) => StatusCode::UNPROCESSABLE_ENTITY,
            Self::BodyTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            // What every proxy in front of this service answers when the thing behind it
            // ran long — nginx, HAProxy, Envoy and a load balancer all say `504` — so it
            // is the status an operator's dashboard already counts as a timeout and a
            // client's retry policy already knows. `408` is the other standard code and
            // says the caller was slow to send the request, which is a different event.
            Self::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ObjectStore(error) => object_store_status(error),
            Self::Storage(error) => storage_status(error),
            // The remote file being missing or unreadable reaches us wrapped in a
            // DataFusionError, and is the caller's problem, not ours.
            Self::DataFusion(DataFusionError::ObjectStore(error)) => object_store_status(error),
            // Every input to planning — the url, the columns, the predicate —
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

#[cfg(test)]
mod tests {
    use super::*;

    fn source() -> Url {
        Url::parse("s3://archive-bucket/hats/").expect("a source url")
    }

    /// What a store says about a key names the bucket, the endpoint and the operator's
    /// prefix, and a caller wrote none of the three. The status is kept, which is the
    /// whole of what makes this a different rule from the local one: an object that is
    /// not there is a `404` about an origin that exists.
    #[test]
    fn a_store_s_account_of_a_mounted_key_is_replaced_and_its_status_kept() {
        let missing = ApiError::ObjectStore(object_store::Error::NotFound {
            path: "hats/dr1/x.parquet".to_owned(),
            source: "no such key".into(),
        });
        let told = missing.from_mounted_store(&source());
        assert_eq!(told.status(), StatusCode::NOT_FOUND);
        let message = told.to_string();
        assert!(!message.contains("archive-bucket"), "{message}");
        assert!(!message.contains("hats"), "{message}");
    }

    /// The planner's account of a query it cannot run is about what the caller wrote, and
    /// telling them their file is unreadable sends them to look at the one thing that is
    /// not wrong. It names nothing of the mount's, so it goes through untouched.
    #[test]
    fn a_failure_that_names_nothing_of_the_mount_is_told_as_it_is() {
        let planning = ApiError::DataFusion(DataFusionError::Plan(
            "No field named objectd. Valid fields are objectid, band.".to_owned(),
        ));
        let told = planning.from_mounted_store(&source());
        assert!(told.to_string().contains("objectd"), "{told}");
        assert_eq!(told.status(), StatusCode::BAD_REQUEST);
    }

    /// Ours to repeat, written here rather than by a store — including a refusal that
    /// happens to quote the caller's own url.
    #[test]
    fn a_message_this_crate_wrote_is_not_replaced() {
        let ours = ApiError::bad_request("limit takes a number of rows");
        assert!(
            ours.from_mounted_store(&source())
                .to_string()
                .contains("number of rows")
        );
    }

    /// A mount publishes a directory, not where it is, and the authority is half of where
    /// it is: an endpoint in a message is the server the operator configured.
    #[test]
    fn a_message_naming_only_the_authority_is_replaced_too() {
        let reached = ApiError::ObjectStore(object_store::Error::Generic {
            store: "S3",
            source: "connecting to archive-bucket.s3.example.org timed out".into(),
        });
        let told = reached.from_mounted_store(&source());
        assert!(!told.to_string().contains("archive-bucket"), "{told}");
        // There is an origin behind this mount, so blaming a gateway blames something
        // that exists.
        assert_eq!(told.status(), StatusCode::BAD_GATEWAY);
    }
}
