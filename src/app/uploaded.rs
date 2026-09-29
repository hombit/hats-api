//! A table a request brings with it rather than names in place: a VOTable, sent inline or
//! fetched from a url, and a parquet file sent inline. Each is read into memory for the one
//! request, under [`Budget`].
//!
//! A parquet file or a catalog *named* by url is not one of these. It is read where it is,
//! through a store and under the scan's own bounds, the way a published catalog is — so
//! nothing of it is held here and nothing of it counts against the budget.

use std::sync::Arc;

use axum::body::Bytes;
use datafusion::arrow::array::RecordBatchReader as _;
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use futures::StreamExt as _;
use object_store::{GetOptions, GetRange, ObjectStoreExt as _};
use url::Url;

use crate::access::LOCAL_SCHEME;
use crate::error::ApiError;
use crate::storage::RemoteFile;
use crate::votable;

/// How much of a file is read to recognise it. A VOTable's root element comes after an XML
/// declaration and perhaps a comment or two; a parquet file says so in its first four bytes.
const HEAD: u64 = 4096;

/// What a file is, by its own first bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::app) enum Sniffed {
    Votable,
    Parquet,
    /// Neither: which, for a url, may yet be a catalog's directory, whose url an HTTP server
    /// answers with a page.
    Other,
}

/// Recognise a file from its head.
pub(in crate::app) fn sniff(head: &[u8]) -> Sniffed {
    if head.starts_with(b"PAR1") {
        Sniffed::Parquet
    } else if votable::is_votable(head) {
        Sniffed::Votable
    } else {
        Sniffed::Other
    }
}

/// What a part's own `Content-Type` says it is, where it says anything this service reads.
///
/// `application/x-votable+xml` is VOTable's registered type (VOTable 1.5 §8), and the two
/// XML types are what a client that knows only that it is sending XML writes.
/// `application/octet-stream` and no type at all say nothing, which is what `requests` —
/// and so `pyvo` — sends; those are recognised from the bytes instead.
pub(in crate::app) fn of_media_type(media_type: &str) -> Option<Sniffed> {
    let essence = media_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    match essence.as_str() {
        "application/x-votable+xml" | "text/xml" | "application/xml" => Some(Sniffed::Votable),
        "application/vnd.apache.parquet" | "application/x-parquet" => Some(Sniffed::Parquet),
        _ => None,
    }
}

/// What one request may still bring with it.
#[derive(Debug, Clone, Copy)]
pub(in crate::app) struct Budget {
    left: u64,
    limit: u64,
}

impl Budget {
    pub fn new(limit: u64) -> Self {
        Self { left: limit, limit }
    }

    /// Count bytes against what is left, or refuse the request.
    ///
    /// DALI §3.4.5: "if the service refuses to accept the upload, it must respond with an
    /// error".
    pub fn spend(&mut self, bytes: u64) -> Result<(), ApiError> {
        match self.left.checked_sub(bytes) {
            Some(left) => {
                self.left = left;
                Ok(())
            }
            None => Err(ApiError::body_too_large(format!(
                "the tables this request uploads come to more than the {} bytes this service \
                 accepts in one request",
                self.limit
            ))),
        }
    }
}

/// The head of the object a url names, or `None` where there is no such object — which is
/// how a store answers a url that is a catalog's directory.
///
/// Any other failure is `None` too. This is a question asked to decide what to open, and a
/// store that will not answer it will say why again, more usefully, when the thing it
/// decides is opened.
pub(in crate::app) async fn head_of(file: &RemoteFile) -> Option<Bytes> {
    let path = object_path(file).ok()?;
    let options = GetOptions {
        range: Some(GetRange::Bounded(0..HEAD)),
        ..GetOptions::default()
    };
    let got = file.store.get_opts(&path, options).await.ok()?;
    got.bytes().await.ok()
}

/// The whole of a file, counted against the budget as it arrives so that one larger than
/// what is left is refused at the byte that passes it rather than once it is all in memory.
pub(in crate::app) async fn fetch(
    file: &RemoteFile,
    written: &Url,
    budget: &mut Budget,
) -> Result<Bytes, ApiError> {
    let path = object_path(file)?;
    let got = file
        .store
        .get(&path)
        .await
        .map_err(|error| unreadable(written, error))?;
    let mut stream = got.into_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| unreadable(written, error))?;
        budget.spend(chunk.len() as u64)?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(bytes))
}

/// A VOTable's table, read on a thread of its own.
///
/// Parsing is computation with nothing to wait on, so on a runtime worker it would hold that
/// worker for as long as the document takes; the upload's byte cap is what bounds how long
/// that is.
pub(in crate::app) async fn votable_table(
    bytes: Bytes,
    name: &str,
) -> Result<Arc<dyn TableProvider>, ApiError> {
    let table = tokio::task::spawn_blocking(move || votable::read(&bytes))
        .await
        .map_err(|error| ApiError::internal(format!("reading an upload failed: {error}")))?
        .map_err(|error| ApiError::bad_request(format!("UPLOAD {name}: {error}")))?;
    Ok(Arc::new(MemTable::try_new(
        table.schema,
        vec![table.batches],
    )?))
}

/// A parquet file sent inline, which has no store to be read through and so is read whole.
pub(in crate::app) async fn parquet_table(
    bytes: Bytes,
    name: &str,
) -> Result<Arc<dyn TableProvider>, ApiError> {
    let name = name.to_owned();
    let (schema, batches) = tokio::task::spawn_blocking(move || {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?.build()?;
        let schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        Ok::<_, datafusion::parquet::errors::ParquetError>((schema, batches))
    })
    .await
    .map_err(|error| ApiError::internal(format!("reading an upload failed: {error}")))?
    .map_err(|error| {
        ApiError::bad_request(format!(
            "UPLOAD {name} is not a parquet file this service can read: {error}"
        ))
    })?;
    Ok(Arc::new(MemTable::try_new(schema, vec![batches])?))
}

fn object_path(file: &RemoteFile) -> Result<object_store::path::Path, ApiError> {
    // Neither the url nor the error goes into the message: for a local file `file.url` is
    // where it sits on the disk, and the path error prints what it was given.
    object_store::path::Path::from_url_path(file.url.path()).map_err(|error| {
        tracing::warn!(%error, "an upload url is not a valid object path");
        ApiError::bad_request("this url is not a valid object path")
    })
}

/// A store's failure to hand over a file, said in terms of the url the caller wrote.
///
/// A `file://` url is a mount's `path`, and the store's own message names the mount's source
/// — a directory on this machine, or the operator's bucket — so that one gets a message of
/// this crate's own and the original goes to the log. Any other url is the caller's, and
/// so is what the store said about it.
fn unreadable(written: &Url, error: object_store::Error) -> ApiError {
    if written.scheme() != LOCAL_SCHEME {
        return error.into();
    }
    tracing::warn!(%error, "cannot read a mounted upload");
    match error {
        object_store::Error::NotFound { .. } => {
            ApiError::not_found(format!("{written} names no file this service can read"))
        }
        _ => ApiError::bad_request(format!("{written} cannot be read")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_is_recognised_by_its_head_and_a_part_by_its_type() {
        assert_eq!(sniff(b"PAR1\x15\x00"), Sniffed::Parquet);
        assert_eq!(sniff(b"<?xml version='1.0'?><VOTABLE>"), Sniffed::Votable);
        assert_eq!(sniff(b"<!DOCTYPE html><html>"), Sniffed::Other);
        assert_eq!(
            of_media_type("application/x-votable+xml; charset=UTF-8"),
            Some(Sniffed::Votable)
        );
        assert_eq!(of_media_type("application/octet-stream"), None);
    }

    #[test]
    fn a_budget_refuses_at_the_byte_that_passes_it() {
        let mut budget = Budget::new(10);
        assert!(budget.spend(6).is_ok());
        assert!(budget.spend(4).is_ok());
        assert!(budget.spend(1).is_err());
    }
}
