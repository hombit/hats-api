//! What a `FIELD` declares, and how that is kept on the Arrow column it becomes.
//!
//! The keys below are the whole contract between this reader and `output::votable`: a
//! column that carries them was declared by a VOTable, and an answer written as one says of
//! it what the document said. A key is dropped wherever DataFusion builds a new field —
//! an expression, a cast — which is exactly where what the document said stops being true.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};

use crate::votable::datatype::{ArraySize, Cell, Layout, Primitive};

/// The `datatype` as the document wrote it.
pub const DATATYPE: &str = "votable.datatype";
/// The `arraysize` as the document wrote it.
pub const ARRAYSIZE: &str = "votable.arraysize";
pub const XTYPE: &str = "votable.xtype";
pub const UNIT: &str = "votable.unit";
pub const UCD: &str = "votable.ucd";
pub const UTYPE: &str = "votable.utype";
/// The `FIELD`'s `DESCRIPTION`.
pub const DESCRIPTION: &str = "votable.description";
/// The `VALUES` `null` a value was read against. An integer array has no other way to say
/// that one of its elements is missing, so an answer writing that array back needs it.
pub const NULL: &str = "votable.null";

/// One `FIELD`, read from its attributes and the elements inside it.
#[derive(Debug, Clone)]
pub struct Declared {
    pub name: Option<String>,
    pub id: Option<String>,
    pub datatype: Option<String>,
    pub arraysize: Option<String>,
    pub xtype: Option<String>,
    pub unit: Option<String>,
    pub ucd: Option<String>,
    pub utype: Option<String>,
    pub description: Option<String>,
    /// `VALUES`' `null`, whether written in the `FIELD` or reached through a `ref`.
    pub null: Option<String>,
}

/// A `FIELD` made into a column: what its cells are, and what to call it.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub layout: Layout,
    pub declared: Declared,
}

impl Declared {
    pub fn from_attributes(attributes: &[(String, String)]) -> Self {
        let get = |key: &str| {
            attributes
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value.clone())
        };
        Self {
            name: get("name"),
            id: get("ID"),
            datatype: get("datatype"),
            arraysize: get("arraysize"),
            xtype: get("xtype"),
            unit: get("unit"),
            ucd: get("ucd"),
            utype: get("utype"),
            description: None,
            null: None,
        }
    }

    /// The column this declares, the `position`th of its table.
    ///
    /// **A column is called by its `name`**, which is TAP §2.7.6's rule for an upload; where
    /// a `FIELD` has none its `ID` stands in, and where it has neither it is named by its
    /// position, `col1` onwards, the way a reader of a headerless table would name it.
    pub fn column(self, position: usize) -> Result<Column, String> {
        let name = self
            .name
            .clone()
            .filter(|name| !name.is_empty())
            .or_else(|| self.id.clone())
            .unwrap_or_else(|| format!("col{}", position + 1));
        let datatype = self
            .datatype
            .as_deref()
            .ok_or_else(|| format!("FIELD {name} has no datatype, which VOTable requires"))?;
        let primitive = Primitive::parse(datatype).ok_or_else(|| {
            format!("FIELD {name} has datatype {datatype:?}, which is not one of VOTable's")
        })?;
        let arraysize = match self.arraysize.as_deref() {
            None => None,
            Some(written) => Some(ArraySize::parse(written).ok_or_else(|| {
                format!("FIELD {name} has arraysize {written:?}, which is not one")
            })?),
        };
        let layout = Layout::new(primitive, arraysize.as_ref())
            .map_err(|error| format!("FIELD {name}: {error}"))?;
        Ok(Column {
            name,
            layout,
            declared: self,
        })
    }
}

impl Column {
    /// The Arrow field this column's values are held in.
    ///
    /// Every column is nullable: an empty `TD`, a `BINARY2` flag and a `VALUES` `null` are
    /// all ways to say a cell is missing, and nothing about a `FIELD` promises none of them
    /// is used.
    pub fn arrow_field(&self) -> Field {
        let mut metadata = HashMap::new();
        let declared = &self.declared;
        for (key, value) in [
            (DATATYPE, &declared.datatype),
            (ARRAYSIZE, &declared.arraysize),
            (XTYPE, &declared.xtype),
            (UNIT, &declared.unit),
            (UCD, &declared.ucd),
            (UTYPE, &declared.utype),
            (DESCRIPTION, &declared.description),
            (NULL, &declared.null),
        ] {
            if let Some(value) = value {
                metadata.insert(key.to_owned(), value.clone());
            }
        }
        Field::new(&self.name, self.data_type(), true).with_metadata(metadata)
    }

    pub fn data_type(&self) -> DataType {
        let item = item_type(self.layout.primitive);
        // A complex number is two floats, so a scalar one is already a list of two.
        let per_item = match self.layout.primitive.is_complex() {
            true => 2,
            false => 1,
        };
        let list = |size: Option<usize>| {
            let field = Arc::new(Field::new_list_field(item.clone(), true));
            match size {
                Some(n) => DataType::FixedSizeList(field, i32::try_from(n).unwrap_or(i32::MAX)),
                None => DataType::List(field),
            }
        };
        match self.layout.cell {
            Cell::Scalar if per_item == 1 => item,
            Cell::Scalar => list(Some(per_item)),
            Cell::Fixed(n) => list(Some(n.saturating_mul(per_item))),
            Cell::Variable { .. } => list(None),
        }
    }
}

/// How many primitives an `arraysize` with no variable dimension holds, or `None` where its
/// last dimension is variable or it is not one.
pub fn fixed_count(arraysize: &str) -> Option<usize> {
    let size = ArraySize::parse(arraysize)?;
    match size.last {
        crate::votable::datatype::Last::Fixed(n) => size
            .leading
            .iter()
            .try_fold(n, |acc, dimension| acc.checked_mul(*dimension)),
        crate::votable::datatype::Last::Variable => None,
    }
}

/// What a two-dimensional character `arraysize` says: the width of one string, which is its
/// first dimension, and how many strings a cell holds — `None` where that is variable.
///
/// `None` for any other `arraysize`: one dimension is one string, and a third says how the
/// strings are grouped, which a list of them no longer carries.
pub fn string_array(arraysize: &str) -> Option<(usize, Option<usize>)> {
    let size = ArraySize::parse(arraysize)?;
    let [width] = size.leading[..] else {
        return None;
    };
    let strings = match size.last {
        crate::votable::datatype::Last::Fixed(n) => Some(n),
        crate::votable::datatype::Last::Variable => None,
    };
    Some((width, strings))
}

/// Whether an `arraysize` is one whose last dimension is variable.
pub fn is_variable(arraysize: &str) -> bool {
    ArraySize::parse(arraysize)
        .is_some_and(|size| size.last == crate::votable::datatype::Last::Variable)
}

/// The Arrow type one item is held as.
///
/// Each integer keeps its own width and VOTable's one unsigned type stays unsigned, so a
/// value reads back as the number the document held; a `bit` is a boolean, and a complex
/// number is its two parts.
pub fn item_type(primitive: Primitive) -> DataType {
    match primitive {
        Primitive::Boolean | Primitive::Bit => DataType::Boolean,
        Primitive::UnsignedByte => DataType::UInt8,
        Primitive::Short => DataType::Int16,
        Primitive::Int => DataType::Int32,
        Primitive::Long => DataType::Int64,
        Primitive::Char | Primitive::UnicodeChar => DataType::Utf8,
        Primitive::Float | Primitive::FloatComplex => DataType::Float32,
        Primitive::Double | Primitive::DoubleComplex => DataType::Float64,
    }
}
