//! How a date or a time is written, wherever this service writes one.
//!
//! DALI §3.3.3 opens with a plain requirement about the literal — "Date and time values
//! must be represented using the convention established for FITS and STC for astronomical
//! times: `YYYY-MM-DD['T'hh:mm:ss[.SSS]]`" — and §3.3's own preamble says the section is
//! about values "used as input or output from DAL services: as parameter values when
//! invoking simple services, as data values in response documents (e.g. VOTable), etc."
//! `csv` and `tsv` are two of the response formats §3.4.3 lists beside VOTable, so the form
//! is owed in a delimited body as much as in a document. What is VOTable's alone is the
//! *marking*, `xtype="timestamp"` on a `FIELD`, and that stays in
//! [`crate::output::votable`].
//!
//! **A zone other than UTC has no spelling.** An astronomical value carries no zone
//! indicator at all and a civil one may carry `Z`, which is the whole of what §3.3.3
//! permits. Arrow prints a zoned value in the zone its column's type names, so a `Z` glued
//! onto that would name an hour the value is not — hence [`utc`], which moves the type
//! before anything is formatted.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{
    DataType, Field, Fields, Schema, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};

use crate::error::ApiError;

/// A date, which carries no time and cannot.
pub const DATE: &str = "%Y-%m-%d";

/// A date and a time. `%.f` writes the fraction the value has and nothing where it has
/// none, so a column stored at the second comes out as §3.3.3's own example rather than
/// with zeroes after it.
pub const DATE_TIME: &str = "%Y-%m-%dT%H:%M:%S%.f";

/// The same for a column whose type names a zone, which [`utc`] has already made UTC.
pub const ZONED: &str = "%Y-%m-%dT%H:%M:%S%.fZ";

/// The name of the zone every instant is written in.
const UTC: &str = "UTC";

/// The column, with any zone in its type replaced by UTC.
///
/// Arrow holds the instant itself and the zone is its type's, so this renames the zone the
/// value will be printed in without touching a value. Anything else is handed back as it
/// came.
pub fn utc(array: &ArrayRef) -> ArrayRef {
    let DataType::Timestamp(unit, Some(_)) = array.data_type() else {
        return Arc::clone(array);
    };
    match unit {
        TimeUnit::Second => Arc::new(
            array
                .as_primitive::<TimestampSecondType>()
                .clone()
                .with_timezone(UTC),
        ),
        TimeUnit::Millisecond => Arc::new(
            array
                .as_primitive::<TimestampMillisecondType>()
                .clone()
                .with_timezone(UTC),
        ),
        TimeUnit::Microsecond => Arc::new(
            array
                .as_primitive::<TimestampMicrosecondType>()
                .clone()
                .with_timezone(UTC),
        ),
        TimeUnit::Nanosecond => Arc::new(
            array
                .as_primitive::<TimestampNanosecondType>()
                .clone()
                .with_timezone(UTC),
        ),
    }
}

/// The same over a whole batch, for a writer that takes batches rather than columns.
///
/// A batch holding no zoned column is handed back as it came, which is what makes this free
/// for the catalogs that have none: the check is over the schema and the clone is of two
/// pointers.
pub fn in_utc(batch: &RecordBatch) -> Result<RecordBatch, ApiError> {
    let zoned = |field: &Arc<Field>| matches!(field.data_type(), DataType::Timestamp(_, Some(_)));
    if !batch.schema().fields().iter().any(zoned) {
        return Ok(batch.clone());
    }
    let columns = batch.columns().iter().map(utc).collect::<Vec<_>>();
    let fields = batch
        .schema()
        .fields()
        .iter()
        .zip(&columns)
        .map(|(field, column)| {
            Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(column.data_type().clone()),
            )
        })
        .collect::<Fields>();
    let schema = Arc::new(Schema::new(fields).with_metadata(batch.schema().metadata().clone()));
    RecordBatch::try_new(schema, columns).map_err(|error| {
        ApiError::internal(format!(
            "an instant column could not be read as UTC: {error}"
        ))
    })
}
