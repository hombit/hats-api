//! One column's values as they are read, and the Arrow array they become.
//!
//! Both serializations feed it the same way — an item at a time, and a cell ended — so what
//! a null is, what a magic value means and what shape a list takes is decided here once
//! rather than in each of them.

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, FixedSizeListArray, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, ListArray, StringBuilder, UInt8Builder,
};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field};

use crate::votable::datatype::{Cell, Primitive};
use crate::votable::field;
use crate::votable::tabledata;

/// A column being filled.
#[derive(Debug)]
pub struct Builder {
    pub column: field::Column,
    magic: Option<Magic>,
    values: Values,
    /// Where each cell's items end, for a variable cell.
    offsets: Vec<i32>,
    /// Whether each cell is there, for a cell that is a list.
    valid: Vec<bool>,
    /// Items appended since the table began, which is what the reader's memory budget
    /// counts.
    appended: usize,
}

/// `VALUES`' `null`: a value that, wherever it is found, means no value (VOTable 1.5 §4.7).
///
/// §5.5: "if it is present, the VALUES null attribute must always be respected", which is
/// why it applies to a float too, though its use there is deprecated in favour of `NaN`.
#[derive(Debug, Clone)]
enum Magic {
    Int(i64),
    /// A `NaN` here matches every `NaN`, which is what a writer declaring it meant.
    Float(f64),
    Text(String),
}

#[derive(Debug)]
enum Values {
    Bool(BooleanBuilder),
    U8(UInt8Builder),
    I16(Int16Builder),
    I32(Int32Builder),
    I64(Int64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Str(StringBuilder),
}

impl Builder {
    pub fn new(column: field::Column) -> Result<Self, String> {
        let primitive = column.layout.primitive;
        let magic = match &column.declared.null {
            None => None,
            Some(written) => magic(primitive, written).map_err(|error| {
                format!(
                    "FIELD {} has VALUES null {written:?}, which is not a {primitive}: {error}",
                    column.name
                )
            })?,
        };
        let values = match field::item_type(primitive) {
            DataType::Boolean => Values::Bool(BooleanBuilder::new()),
            DataType::UInt8 => Values::U8(UInt8Builder::new()),
            DataType::Int16 => Values::I16(Int16Builder::new()),
            DataType::Int32 => Values::I32(Int32Builder::new()),
            DataType::Int64 => Values::I64(Int64Builder::new()),
            DataType::Float32 => Values::F32(Float32Builder::new()),
            DataType::Float64 => Values::F64(Float64Builder::new()),
            _ => Values::Str(StringBuilder::new()),
        };
        Ok(Self {
            column,
            magic,
            values,
            offsets: vec![0],
            valid: Vec::new(),
            appended: 0,
        })
    }

    pub fn primitive(&self) -> Primitive {
        self.column.layout.primitive
    }

    pub fn cell(&self) -> Cell {
        self.column.layout.cell
    }

    /// How many Arrow values one item is: two for a complex number.
    fn per_item(&self) -> usize {
        match self.primitive().is_complex() {
            true => 2,
            false => 1,
        }
    }

    pub fn appended(&self) -> usize {
        self.appended
    }

    /// How many items a null cell of this column is, which a fixed-size list holds whether
    /// the cell is there or not.
    pub fn null_cost(&self) -> usize {
        match self.cell() {
            Cell::Scalar => self.per_item(),
            Cell::Fixed(n) => n.saturating_mul(self.per_item()),
            Cell::Variable { .. } => 0,
        }
    }

    /// A cell that is missing, whatever its shape.
    pub fn null_cell(&mut self) {
        match self.cell() {
            Cell::Scalar if self.per_item() == 1 => self.null_item(),
            Cell::Scalar => {
                self.null_items(self.per_item());
                self.valid.push(false);
            }
            // A fixed-size list holds its items whether the cell is there or not.
            Cell::Fixed(n) => {
                self.null_items(n.saturating_mul(self.per_item()));
                self.valid.push(false);
            }
            Cell::Variable { .. } => {
                self.offsets.push(self.last_offset());
                self.valid.push(false);
            }
        }
    }

    /// The end of a cell that was there, for any cell that is a list.
    pub fn end_cell(&mut self) -> Result<(), String> {
        match self.cell() {
            Cell::Scalar if self.per_item() == 1 => {}
            Cell::Variable { .. } => {
                let end = i32::try_from(self.values_len())
                    .map_err(|_| "a column holds more items than a list can".to_owned())?;
                self.offsets.push(end);
                self.valid.push(true);
            }
            Cell::Scalar | Cell::Fixed(_) => self.valid.push(true),
        }
        Ok(())
    }

    fn last_offset(&self) -> i32 {
        self.offsets.last().copied().unwrap_or(0)
    }

    fn null_items(&mut self, count: usize) {
        for _ in 0..count {
            self.null_item();
        }
    }

    /// One item that is missing.
    pub fn null_item(&mut self) {
        self.appended += 1;
        match &mut self.values {
            Values::Bool(values) => values.append_null(),
            Values::U8(values) => values.append_null(),
            Values::I16(values) => values.append_null(),
            Values::I32(values) => values.append_null(),
            Values::I64(values) => values.append_null(),
            Values::F32(values) => values.append_null(),
            Values::F64(values) => values.append_null(),
            Values::Str(values) => values.append_null(),
        }
    }

    pub fn boolean(&mut self, value: Option<bool>) {
        self.appended += 1;
        if let Values::Bool(values) = &mut self.values {
            values.append_option(value);
        }
    }

    /// An integer, already known to fit the column's own type.
    pub fn integer(&mut self, value: i64) {
        if matches!(self.magic, Some(Magic::Int(magic)) if magic == value) {
            return self.null_item();
        }
        self.appended += 1;
        // The readers range-check against the primitive before calling this, so each of
        // these conversions succeeds; the fallback is there for the type system.
        match &mut self.values {
            Values::U8(values) => values.append_option(u8::try_from(value).ok()),
            Values::I16(values) => values.append_option(i16::try_from(value).ok()),
            Values::I32(values) => values.append_option(i32::try_from(value).ok()),
            Values::I64(values) => values.append_value(value),
            _ => {}
        }
    }

    pub fn float(&mut self, value: f64) {
        if let Some(Magic::Float(magic)) = self.magic
            && (magic == value || (magic.is_nan() && value.is_nan()))
        {
            return self.null_item();
        }
        self.appended += 1;
        match &mut self.values {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "only ever handed a value read as an f32, so this is exact"
            )]
            Values::F32(values) => values.append_value(value as f32),
            Values::F64(values) => values.append_value(value),
            _ => {}
        }
    }

    pub fn text(&mut self, value: &str) {
        if matches!(&self.magic, Some(Magic::Text(magic)) if magic == value) {
            return self.null_item();
        }
        self.appended += 1;
        if let Values::Str(values) = &mut self.values {
            values.append_value(value);
        }
    }

    fn values_len(&self) -> usize {
        match &self.values {
            Values::Bool(values) => values.len(),
            Values::U8(values) => values.len(),
            Values::I16(values) => values.len(),
            Values::I32(values) => values.len(),
            Values::I64(values) => values.len(),
            Values::F32(values) => values.len(),
            Values::F64(values) => values.len(),
            Values::Str(values) => values.len(),
        }
    }

    /// Everything read since the last call, as one array, and the builder empty again.
    pub fn finish(&mut self) -> Result<ArrayRef, String> {
        let items: ArrayRef = match &mut self.values {
            Values::Bool(values) => Arc::new(values.finish()),
            Values::U8(values) => Arc::new(values.finish()),
            Values::I16(values) => Arc::new(values.finish()),
            Values::I32(values) => Arc::new(values.finish()),
            Values::I64(values) => Arc::new(values.finish()),
            Values::F32(values) => Arc::new(values.finish()),
            Values::F64(values) => Arc::new(values.finish()),
            Values::Str(values) => Arc::new(values.finish()),
        };
        let valid = std::mem::take(&mut self.valid);
        let nulls = match valid.iter().all(|present| *present) {
            true => None,
            false => Some(NullBuffer::from(valid)),
        };
        let item = Arc::new(Field::new_list_field(items.data_type().clone(), true));
        let fixed = |n: usize| -> Result<ArrayRef, String> {
            let size = i32::try_from(n).map_err(|_| "a cell holds more items than a list can")?;
            FixedSizeListArray::try_new(Arc::clone(&item), size, Arc::clone(&items), nulls.clone())
                .map(|array| Arc::new(array) as ArrayRef)
                .map_err(|error| error.to_string())
        };
        let array = match self.cell() {
            Cell::Scalar if self.per_item() == 1 => items,
            Cell::Scalar => fixed(self.per_item())?,
            Cell::Fixed(n) => fixed(n.saturating_mul(self.per_item()))?,
            Cell::Variable { .. } => {
                let offsets = std::mem::replace(&mut self.offsets, vec![0]);
                let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
                Arc::new(
                    ListArray::try_new(item, offsets, items, nulls)
                        .map_err(|error| error.to_string())?,
                )
            }
        };
        Ok(array)
    }
}

/// A `null` attribute read as the value it stands for.
///
/// §4.7: it "must follow the same rules as the TABLEDATA serialization for the appropriate
/// datatype", so it is read by the same code a `TD` is.
///
/// A boolean "has its own arrangements for representing null" (§5.5) and a bit has no value
/// that is not one, so on either the attribute says nothing and is passed over.
fn magic(primitive: Primitive, written: &str) -> Result<Option<Magic>, String> {
    Ok(Some(match primitive {
        Primitive::UnsignedByte | Primitive::Short | Primitive::Int | Primitive::Long => {
            Magic::Int(tabledata::integer(primitive, written.trim())?)
        }
        // At the column's own width, or `-99.9` would never equal the `f32` written for it.
        Primitive::Float | Primitive::FloatComplex => {
            let value = written
                .trim()
                .parse::<f32>()
                .map_err(|_| format!("{written:?} is not a float"))?;
            Magic::Float(f64::from(value))
        }
        Primitive::Double | Primitive::DoubleComplex => {
            Magic::Float(tabledata::float(written.trim())?)
        }
        Primitive::Char | Primitive::UnicodeChar => Magic::Text(written.to_owned()),
        Primitive::Boolean | Primitive::Bit => return Ok(None),
    }))
}
