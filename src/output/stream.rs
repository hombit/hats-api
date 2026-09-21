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
//! **Parquet is one of them**, and it is the format streaming is most worth having for: an
//! answer of a few hundred megabytes need not exist twice, once in the writer's buffer and
//! once in the response. It is written a row group at a time with its footer last, so what
//! a reader gets at the end is an ordinary parquet file.
//!
//! What that file is not is a body anyone can seek in. A reader that opens parquet *over
//! HTTP* — `pyarrow` through `fsspec` — needs ranges and a length, and gets them from a
//! collected answer, which is why `streaming` is off unless a request asks. A reader that
//! saves the bytes first, which is `curl` and `requests` and every client writing a file to
//! disk, never needed either.

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
    /// How many of a catalog's partitions were read, where the answer came from a catalog.
    /// `None` for one file, which has none to count — not a zero, which would be a catalog
    /// that answered out of no partition at all.
    pub partitions: Option<usize>,
    pub elapsed: Duration,
    /// Whether a row bound cut the answer short, which DALI §4.4.1 puts *after* the table.
    pub overflow: bool,
    /// Why the rows stopped before the answer did, where they did.
    ///
    /// A collected answer never has one — a bound reached there is a `422` and a work list,
    /// and a read that fails there is the status of the whole response. A stream has sent
    /// both by the time it knows, so it says it at the end of the document instead, and
    /// each format says it the way it can.
    pub stopped: Option<Stopped>,
}

/// Why an answer stopped before it was whole.
///
/// The two are apart because a reader has to tell them apart: one is this service declining
/// to do more work, and the answer so far is sound as far as it goes; the other is the read
/// itself failing, and what came before is whatever had arrived. VOTable spells the first
/// `OVERFLOW` and the second `ERROR` (DALI §4.4), which is the distinction made out loud.
#[derive(Debug, Clone)]
pub enum Stopped {
    /// A bound this service sets was reached, and the rows were cut there.
    Bound(String),
    /// The read failed after the first byte of the answer had gone.
    Failed(String),
}

impl Stopped {
    /// What to tell the caller, which is the same sentence either way.
    pub fn why(&self) -> &str {
        match self {
            Self::Bound(why) | Self::Failed(why) => why,
        }
    }
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
        gathered(&mut out, encoder.rows(batch)?);
    }
    gathered(&mut out, encoder.end(ending)?);
    Ok(out)
}

/// Add one encoder chunk to the document, taking it whole where there is nothing to add it
/// to.
///
/// **A chunk arrives owned**, `Pipe::take` being a `mem::take` of the buffer an `arrow`
/// writer filled, so the first one to arrive becomes the answer rather than being copied
/// into an empty `Vec` beside it. What that is worth depends on how many chunks a format
/// produces: a parquet answer under one row group is written entirely by `end`, so the
/// whole body is adopted and the copy is gone — which is the case the module's own
/// measurements are of, a 27.6 MB projection of Gaia among them. A format that emits per
/// batch still pays for the batches after the first.
///
/// Removing the rest of it means the encoders writing into a buffer this function owns,
/// which is the `Encoder` contract and `Pipe`'s rather than this call's.
fn gathered(out: &mut Vec<u8>, mut chunk: Vec<u8>) {
    match out.is_empty() {
        true => *out = chunk,
        false => out.append(&mut chunk),
    }
}

/// The same encoder over batches that have not been read yet.
///
/// The ending is computed once the rows run out, by whatever knows what they cost — for a
/// file that is the plan's own metrics, which are final as soon as the last batch is in.
/// `head` is what [`Encoder::begin`] already returned, written by the caller rather than
/// here. **That is where a format refuses.** `csv` has no form for a nested column and says
/// so in `begin`; asked for inside the body, that refusal arrives after the `200` and the
/// caller sees a dropped connection instead of the `400` naming the column. So the head is
/// written while a status can still be chosen, and what reaches here cannot fail.
///
/// **A read that fails part-way ends the document rather than the connection.** The status
/// is long gone, but the end of the answer is still to be written, and a format that has
/// somewhere to name the failure names it — which is a caller reading why their answer is
/// short instead of guessing at a dropped transfer. Whatever the error says reaches the
/// caller, so what is handed in here must already be safe to say: the routes map their
/// streams through the same refusal-shaping a collected answer goes through.
pub fn streamed<S, F>(
    encoder: Box<dyn Encoder>,
    head: Vec<u8>,
    batches: S,
    ending: F,
) -> impl Stream<Item = Result<Bytes, ApiError>> + Send
where
    S: Stream<Item = Result<RecordBatch, ApiError>> + Send + 'static,
    F: FnOnce(usize) -> Ending + Send + 'static,
{
    let writing = Writing {
        encoder,
        head: Some(head),
        batches: Box::pin(batches),
        ending: Some(ending),
        rows: 0,
        phase: Phase::Begin,
    };
    futures::stream::unfold(writing, |mut writing| async move {
        let sent = match writing.phase {
            Phase::Begin => {
                writing.phase = Phase::Rows;
                Ok(writing.head.take().unwrap_or_default())
            }
            Phase::Rows => match writing.batches.next().await {
                Some(Ok(batch)) => {
                    writing.rows += batch.num_rows();
                    writing.encoder.rows(&batch)
                }
                // The rows end here, and the end of the document says why. A format with
                // nowhere to say it refuses in `end`, which is the dropped transfer that
                // was the only answer before.
                Some(Err(error)) => writing.finish(Some(Stopped::Failed(error.to_string()))),
                None => writing.finish(None),
            },
            Phase::End => return None,
        };
        match sent {
            // Nothing left to write it with: the encoder itself failed, or had no form for
            // what the ending had to say. The body stops mid-chunk, which is what a reader
            // must not be able to mistake for a whole answer.
            Err(error) => Some((Err(error), Writing::done(writing))),
            Ok(bytes) => Some((Ok(Bytes::from(bytes)), writing)),
        }
    })
    // An encoder with nothing to say for a batch is not a chunk of no bytes.
    .filter(|sent| futures::future::ready(sent.as_ref().map_or(true, |bytes| !bytes.is_empty())))
    // **A body may be polled once more after it has ended.** `StreamBody` does not override
    // `is_end_stream`, so it answers the default `false` and a layer above it asks again to
    // find out — `tower_http`'s compression does, which is every answer to a browser, since
    // that is what sends `accept-encoding`. An `unfold` panics on that poll rather than
    // answering `None` a second time, and it panics on the worker rather than failing the
    // request, so nothing in the response says what happened.
    .fuse()
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
    /// What `begin` wrote, waiting to be the first chunk.
    head: Option<Vec<u8>>,
    batches: std::pin::Pin<Box<S>>,
    ending: Option<F>,
    rows: usize,
    phase: Phase,
}

impl<S, F: FnOnce(usize) -> Ending> Writing<S, F> {
    /// Close the document, saying why the rows stopped where a reason is known.
    ///
    /// A failure is the reason whatever else the ending was going to say: it is why there
    /// are no more rows, and a bound recorded by a read that then failed is not what cut
    /// the answer short.
    fn finish(&mut self, stopped: Option<Stopped>) -> Result<Vec<u8>, ApiError> {
        self.phase = Phase::End;
        // Taken rather than borrowed: what the ending costs is read off the plan, and
        // reading it is the last thing that happens.
        let Some(ending) = self.ending.take() else {
            return Ok(Vec::new());
        };
        let mut ending = ending(self.rows);
        if stopped.is_some() {
            ending.stopped = stopped;
        }
        self.encoder.end(ending)
    }

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

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;

    use super::*;

    /// Enough of an encoder to drive the stream from end to end.
    struct Bracketed;

    impl Encoder for Bracketed {
        fn begin(&mut self, _schema: &SchemaRef) -> Result<Vec<u8>, ApiError> {
            Ok(b"[".to_vec())
        }

        fn rows(&mut self, _batch: &RecordBatch) -> Result<Vec<u8>, ApiError> {
            Ok(b"row".to_vec())
        }

        fn end(&mut self, _ending: Ending) -> Result<Vec<u8>, ApiError> {
            Ok(b"]".to_vec())
        }
    }

    fn one_batch() -> impl Stream<Item = Result<RecordBatch, ApiError>> + Send {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))]).unwrap();
        futures::stream::iter(vec![Ok(batch)])
    }

    /// A finished body answers a further poll instead of panicking.
    ///
    /// `StreamBody` does not override `is_end_stream`, so it answers the default `false` and
    /// whatever wraps it asks once more to find out — `tower_http`'s compression does, which
    /// is every answer to a browser, that being what sends `accept-encoding`. An `unfold`
    /// panics on a poll after `Poll::Ready(None)`, and it panics on the worker rather than
    /// failing the request, so nothing in the response says what happened.
    #[tokio::test]
    async fn a_finished_body_answers_a_further_poll() {
        // The head is the caller's: `sending` writes what `begin` returned while a status
        // can still be chosen, and hands the bytes in here.
        let mut body = Box::pin(streamed(
            Box::new(Bracketed),
            b"[".to_vec(),
            one_batch(),
            |rows| Ending {
                rows,
                ..Ending::default()
            },
        ));

        let mut written = Vec::new();
        while let Some(sent) = body.next().await {
            written.extend_from_slice(&sent.unwrap());
        }
        assert_eq!(&written[..], b"[row]");

        // The polls that took the worker down.
        assert!(body.next().await.is_none());
        assert!(body.next().await.is_none());
    }
}
