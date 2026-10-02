//! A struct's fields that a statement named one by one, and putting them back into the struct.
//!
//! A statement that selects `lc.mag` and `lc.mjd` answers two columns, named by those paths
//! and in the order it wrote them, which is what TAP asks of an answer and what a VOTable
//! writes. Each carries [`PARENT`], naming the struct it came out of. A format that can hold
//! a struct — json and parquet — packs the marked columns back into one, the way `pyarrow`
//! reads a subset of a struct: a client that asked for part of a light curve still gets a
//! light curve.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};

use crate::engine::query::QueryResult;
use crate::error::ApiError;
use crate::output::stream;

/// The field metadata key naming the struct a column is a field of. Its value is the
/// column's name up to its last dot, and what follows the dot is the field's own name.
pub const PARENT: &str = "hats.parent";

/// The struct a column is a field of, where a statement named it by its path.
pub fn parent(field: &Field) -> Option<&str> {
    field.metadata().get(PARENT).map(String::as_str)
}

/// The answer with its marked columns packed, ready for a format that holds a struct.
pub fn packed(result: &QueryResult) -> Result<QueryResult, ApiError> {
    let packing = Packing::of(&result.schema);
    Ok(QueryResult {
        schema: Arc::clone(&packing.schema),
        batches: result
            .batches
            .iter()
            .map(|batch| packing.pack(batch))
            .collect::<Result<_, _>>()?,
        data_bytes_read: result.data_bytes_read,
    })
}

/// An encoder for a format that holds a struct, handed the rows packed.
pub struct Packed {
    inner: Box<dyn stream::Encoder>,
    packing: Option<Packing>,
}

impl Packed {
    pub fn new(inner: Box<dyn stream::Encoder>) -> Self {
        Self {
            inner,
            packing: None,
        }
    }
}

/// By hand: an encoder has no `Debug`, and what is worth printing is whether it has begun.
impl std::fmt::Debug for Packed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Packed")
            .field("begun", &self.packing.is_some())
            .finish()
    }
}

impl stream::Encoder for Packed {
    fn begin(&mut self, schema: &SchemaRef) -> Result<Vec<u8>, ApiError> {
        let packing = Packing::of(schema);
        let out = self.inner.begin(&packing.schema)?;
        self.packing = Some(packing);
        Ok(out)
    }

    fn rows(&mut self, batch: &RecordBatch) -> Result<Vec<u8>, ApiError> {
        let packing = self
            .packing
            .as_ref()
            .ok_or_else(|| ApiError::internal("rows were packed before the answer began"))?;
        let batch = packing.pack(batch)?;
        self.inner.rows(&batch)
    }

    fn end(&mut self, ending: stream::Ending) -> Result<Vec<u8>, ApiError> {
        self.inner.end(ending)
    }
}

/// Which columns of an answer become one, worked out once from its schema.
struct Packing {
    schema: SchemaRef,
    /// Per column of the packed answer, the columns of the unpacked one it is made of.
    slots: Vec<Slot>,
}

enum Slot {
    Column(usize),
    Struct(Vec<usize>),
}

impl Packing {
    /// A struct goes where its first field was, holding its fields in the order they were
    /// named.
    ///
    /// **A struct whose name is already a column is not packed.** `SELECT lc, lc.mag` answers
    /// `lc` whole and `lc.mag` beside it; packing the second would be a second column named
    /// `lc`, which no reader can tell from the first.
    fn of(schema: &SchemaRef) -> Self {
        let taken = schema
            .fields()
            .iter()
            .filter(|field| parent(field).is_none())
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        let mut slots: Vec<Slot> = Vec::new();
        let mut structs: HashMap<&str, usize> = HashMap::new();
        for (at, field) in schema.fields().iter().enumerate() {
            match parent(field).filter(|name| !taken.contains(name)) {
                Some(name) => match structs.get(name) {
                    Some(&slot) => {
                        if let Some(Slot::Struct(members)) = slots.get_mut(slot) {
                            members.push(at);
                        }
                    }
                    None => {
                        structs.insert(name, slots.len());
                        slots.push(Slot::Struct(vec![at]));
                    }
                },
                None => slots.push(Slot::Column(at)),
            }
        }
        let fields = slots
            .iter()
            .filter_map(|slot| match slot {
                Slot::Column(at) => schema.fields().get(*at).cloned(),
                Slot::Struct(members) => {
                    let first = schema.fields().get(*members.first()?)?;
                    let name = parent(first)?;
                    let inner = members
                        .iter()
                        .filter_map(|at| schema.fields().get(*at))
                        .map(|field| unmarked(field, name))
                        .collect::<Fields>();
                    Some(Arc::new(Field::new(name, DataType::Struct(inner), true)))
                }
            })
            .collect::<Vec<FieldRef>>();
        Self {
            schema: Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
            slots,
        }
    }

    fn pack(&self, batch: &RecordBatch) -> Result<RecordBatch, ApiError> {
        let columns = self
            .slots
            .iter()
            .zip(self.schema.fields())
            .map(|(slot, field)| -> Result<ArrayRef, ApiError> {
                let column = |at: &usize| {
                    batch.columns().get(*at).cloned().ok_or_else(|| {
                        ApiError::internal("a batch has fewer columns than its schema")
                    })
                };
                Ok(match (slot, field.data_type()) {
                    (Slot::Struct(members), DataType::Struct(inner)) => {
                        Arc::new(StructArray::try_new(
                            inner.clone(),
                            members.iter().map(column).collect::<Result<_, _>>()?,
                            None,
                        )?)
                    }
                    (Slot::Column(at), _) => column(at)?,
                    (Slot::Struct(_), _) => {
                        return Err(ApiError::internal("a packed column is not a struct"));
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }
}

/// A column as the field of the struct it is packed into: named for what follows the
/// struct's name, and no longer marked.
fn unmarked(field: &Field, parent: &str) -> FieldRef {
    let name = field
        .name()
        .strip_prefix(parent)
        .and_then(|rest| rest.strip_prefix('.'))
        .unwrap_or(field.name());
    let mut metadata = field.metadata().clone();
    metadata.remove(PARENT);
    Arc::new(field.clone().with_name(name).with_metadata(metadata))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, AsArray, Float64Array, Int64Array};
    use datafusion::arrow::datatypes::Float64Type;

    use super::*;

    fn marked(name: &str, parent: &str) -> Field {
        Field::new(name, DataType::Float64, true)
            .with_metadata(HashMap::from([(PARENT.to_owned(), parent.to_owned())]))
    }

    fn result(fields: Vec<Field>) -> QueryResult {
        let schema = Arc::new(Schema::new(fields));
        let columns = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(at, field)| -> ArrayRef {
                let value = u32::try_from(at).unwrap();
                match field.data_type() {
                    DataType::Int64 => Arc::new(Int64Array::from(vec![i64::from(value)])),
                    _ => Arc::new(Float64Array::from(vec![f64::from(value)])),
                }
            })
            .collect();
        QueryResult {
            batches: vec![RecordBatch::try_new(Arc::clone(&schema), columns).unwrap()],
            schema,
            data_bytes_read: 0,
        }
    }

    /// Two fields named apart, with a column between them, come back as one struct where
    /// the first was — holding both, in the order they were named.
    #[test]
    fn the_fields_of_a_struct_are_packed_where_the_first_was() {
        let packed = packed(&result(vec![
            marked("lc.mjd", "lc"),
            Field::new("id", DataType::Int64, false),
            marked("lc.mag", "lc"),
        ]))
        .unwrap();
        let names = |fields: &Fields| fields.iter().map(|f| f.name().clone()).collect::<Vec<_>>();
        assert_eq!(names(packed.schema.fields()), ["lc", "id"]);
        let DataType::Struct(inner) = packed.schema.field(0).data_type() else {
            panic!("{:?}", packed.schema);
        };
        assert_eq!(names(inner), ["mjd", "mag"]);
        assert!(inner.iter().all(|field| parent(field).is_none()));
        let lc = packed.batches[0].column(0).as_struct();
        assert_eq!(lc.column(1).as_primitive::<Float64Type>().value(0), 2.0);
        assert_eq!(packed.batches[0].column(1).len(), 1);
    }

    /// A struct already in the answer under the same name keeps its fields beside it, since
    /// packing them would be a second column of that name.
    #[test]
    fn a_struct_named_like_a_column_is_left_flat() {
        let packed = packed(&result(vec![
            Field::new("lc", DataType::Int64, true),
            marked("lc.mag", "lc"),
        ]))
        .unwrap();
        let names = packed
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        assert_eq!(names, ["lc", "lc.mag"]);
    }

    /// A qualified parent is packed under that name, so two tables' fields of one struct
    /// name stay two columns.
    #[test]
    fn two_tables_fields_pack_into_two_structs() {
        let packed = packed(&result(vec![
            marked("a.lc.mag", "a.lc"),
            marked("b.lc.mag", "b.lc"),
        ]))
        .unwrap();
        let names = packed
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>();
        assert_eq!(names, ["a.lc", "b.lc"]);
    }
}
