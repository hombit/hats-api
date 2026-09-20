//! Writing an answer out as its rows arrive, and the same writing when they are all in.
//!
//! **One implementation per format, driven two ways.** A format knows three things: what
//! comes before the rows, how a batch of rows is written, and what comes after them. That
//! is enough to write a collected answer — drive it over the batches that are already in
//! hand — and enough to write a streamed one, where the same calls happen as each batch
//! comes off the scan. Anything less shared than this is two encoders per format that
//! have to be kept saying the same thing.
//!
//! **What a stream cannot carry is anything the head of the answer would have to know.**
//! The counts travel as headers on a collected answer and the headers are sent before the
//! first byte, so a streamed answer has none of them; `nrows` on a VOTable's `TABLE` is
//! the same problem inside the document and is left out, being optional. JSON is the one
//! format that can still report them, because a JSON object does not care what order its
//! keys arrive in — so the counts go after the rows rather than before.
//!
//! Parquet is not here. It is read footer-first, so a reader needs the whole object or
//! ranges into it, and a body with no length is one `fsspec` reports as unseekable — the
//! failure this service fixed rather than one to reintroduce.

use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use futures::{Stream, StreamExt};

use crate::engine::query::QueryResult;
use crate::error::ApiError;

/// What is known only once the last row is in, and what the end of a document may say.
#[derive(Debug, Default, Clone)]
pub struct Ending {
    pub rows: usize,
    pub data_bytes_read: u64,
    pub elapsed: Duration,
    /// Whether a row bound cut the answer short, which DALI §4.4.1 puts *after* the table.
    pub overflow: bool,
    /// Why this answer is not the whole answer, where a bound stopped it part-way.
    ///
    /// A collected answer never has one — a bound reached there is a `422` and a work list,
    /// which is what the caller wants and what a stream cannot go back and send. So this is
    /// the streamed case only, and each format says it the way it can: JSON writes it,
    /// VOTable turns it into `OVERFLOW`, and the delimited formats, having nowhere to put
    /// it, end the body without its terminating chunk rather than look complete.
    pub refused: Option<String>,
}

/// One format, written in three pieces.
///
/// Every method returns the bytes to send now rather than writing into a buffer the caller
/// owns: a streamed answer sends what each batch produced and keeps nothing, and a
/// collected one appends. The same three calls make both.
pub trait Encoder: Send {
    /// What comes before the rows. The schema is known before any row is read, which is
    /// why a streamed answer can describe its columns without having any.
    fn begin(&mut self, schema: &SchemaRef) -> Result<Vec<u8>, ApiError>;

    /// One batch of rows.
    fn rows(&mut self, batch: &RecordBatch) -> Result<Vec<u8>, ApiError>;

    /// What comes after them.
    fn end(&mut self, ending: Ending) -> Result<Vec<u8>, ApiError>;
}

/// Write a result that is already in hand, through the same encoder a stream uses.
pub fn collected(
    encoder: &mut dyn Encoder,
    result: &QueryResult,
    ending: Ending,
) -> Result<Vec<u8>, ApiError> {
    let mut out = encoder.begin(&result.schema)?;
    for batch in &result.batches {
        out.extend_from_slice(&encoder.rows(batch)?);
    }
    out.extend_from_slice(&encoder.end(ending)?);
    Ok(out)
}

/// The same encoder over batches that have not been read yet.
///
/// The ending is computed once the rows run out, by whatever knows what they cost — for a
/// file that is the plan's own metrics, which are final as soon as the last batch is in.
pub fn streamed<S, F>(
    encoder: Box<dyn Encoder>,
    schema: SchemaRef,
    batches: S,
    ending: F,
) -> impl Stream<Item = Result<Bytes, ApiError>> + Send
where
    S: Stream<Item = Result<RecordBatch, ApiError>> + Send + 'static,
    F: FnOnce(usize) -> Ending + Send + 'static,
{
    let writing = Writing {
        encoder,
        schema,
        batches: Box::pin(batches),
        ending: Some(ending),
        rows: 0,
        phase: Phase::Begin,
    };
    futures::stream::unfold(writing, |mut writing| async move {
        let sent = match writing.phase {
            Phase::Begin => {
                writing.phase = Phase::Rows;
                writing.encoder.begin(&writing.schema)
            }
            Phase::Rows => match writing.batches.next().await {
                Some(Ok(batch)) => {
                    writing.rows += batch.num_rows();
                    writing.encoder.rows(&batch)
                }
                Some(Err(error)) => Err(error),
                None => {
                    writing.phase = Phase::End;
                    // Taken rather than borrowed: what the ending costs is read off the
                    // plan, and reading it is the last thing that happens.
                    match writing.ending.take() {
                        Some(ending) => {
                            let ending = ending(writing.rows);
                            writing.encoder.end(ending)
                        }
                        None => Ok(Vec::new()),
                    }
                }
            },
            Phase::End => return None,
        };
        match sent {
            // A failure ends the body where it stands. There is no status left to change
            // — the `200` and the rows before it are already sent — so what a reader gets
            // is a document that stops, which is why nothing streams until its rows are
            // known to be readable.
            Err(error) => Some((Err(error), Writing::done(writing))),
            Ok(bytes) => Some((Ok(Bytes::from(bytes)), writing)),
        }
    })
    // An encoder with nothing to say for a batch is not a chunk of no bytes.
    .filter(|sent| futures::future::ready(sent.as_ref().map_or(true, |bytes| !bytes.is_empty())))
}

/// Where a streamed answer has got to.
#[derive(Debug, Clone, Copy)]
enum Phase {
    Begin,
    Rows,
    End,
}

struct Writing<S, F> {
    encoder: Box<dyn Encoder>,
    schema: SchemaRef,
    batches: std::pin::Pin<Box<S>>,
    ending: Option<F>,
    rows: usize,
    phase: Phase,
}

impl<S, F> Writing<S, F> {
    /// The same state, saying there is nothing more to send.
    fn done(mut self) -> Self {
        self.phase = Phase::End;
        self
    }
}

/// A sink an `arrow` writer writes into, whose bytes can be taken out between batches.
///
/// The writers here — `arrow`'s JSON and CSV ones — are built over a `Write` and emit as
/// they go: the JSON one opens its array on the first batch and closes it on `finish`, and
/// the CSV one writes its header there. Taking the bytes out after each call is what turns
/// that into a stream without asking either writer to do anything it does not already do.
#[derive(Debug, Default, Clone)]
pub struct Pipe(Arc<Mutex<Vec<u8>>>);

impl Pipe {
    /// Everything written since the last call.
    pub fn take(&self) -> Vec<u8> {
        self.0
            .lock()
            .map(|mut held| std::mem::take(&mut *held))
            .unwrap_or_default()
    }
}

impl io::Write for Pipe {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.0.lock() {
            Ok(mut held) => {
                held.extend_from_slice(buf);
                Ok(buf.len())
            }
            Err(_) => Err(io::Error::other("the output buffer was poisoned")),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
