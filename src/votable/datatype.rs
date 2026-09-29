//! What a `FIELD`'s `datatype` and `arraysize` make of one cell (VOTable 1.5 §2.1, §2.2, §6).

use std::fmt;

/// One of the primitives of VOTable 1.5 Table 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Primitive {
    Boolean,
    Bit,
    UnsignedByte,
    Short,
    Int,
    Long,
    Char,
    UnicodeChar,
    Float,
    Double,
    FloatComplex,
    DoubleComplex,
}

impl Primitive {
    /// The primitive a `datatype` attribute names, exactly as Table 1 spells it.
    pub fn parse(datatype: &str) -> Option<Self> {
        Some(match datatype {
            "boolean" => Self::Boolean,
            "bit" => Self::Bit,
            "unsignedByte" => Self::UnsignedByte,
            "short" => Self::Short,
            "int" => Self::Int,
            "long" => Self::Long,
            "char" => Self::Char,
            "unicodeChar" => Self::UnicodeChar,
            "float" => Self::Float,
            "double" => Self::Double,
            "floatComplex" => Self::FloatComplex,
            "doubleComplex" => Self::DoubleComplex,
            _ => return None,
        })
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Bit => "bit",
            Self::UnsignedByte => "unsignedByte",
            Self::Short => "short",
            Self::Int => "int",
            Self::Long => "long",
            Self::Char => "char",
            Self::UnicodeChar => "unicodeChar",
            Self::Float => "float",
            Self::Double => "double",
            Self::FloatComplex => "floatComplex",
            Self::DoubleComplex => "doubleComplex",
        }
    }

    /// Whether a cell of this primitive is text rather than a number.
    pub fn is_text(self) -> bool {
        matches!(self, Self::Char | Self::UnicodeChar)
    }

    /// Whether one of it is two floats.
    pub fn is_complex(self) -> bool {
        matches!(self, Self::FloatComplex | Self::DoubleComplex)
    }
}

impl fmt::Display for Primitive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// An `arraysize`: dimensions separated by `x`, the first changing fastest, and the last one
/// possibly variable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArraySize {
    /// Every dimension but the last.
    pub leading: Vec<usize>,
    pub last: Last,
}

/// The last dimension of an `arraysize`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Last {
    Fixed(usize),
    /// `*` or `n*`: a count precedes the cell in `BINARY`, and `n` is only an upper bound.
    Variable,
}

impl ArraySize {
    /// Read an `arraysize` attribute. `None` is a spelling that is not one.
    pub fn parse(written: &str) -> Option<Self> {
        let mut parts: Vec<&str> = written.trim().split('x').collect();
        let last = parts.pop()?;
        let leading = parts
            .iter()
            .map(|part| part.trim().parse::<usize>().ok())
            .collect::<Option<Vec<_>>>()?;
        let last = match last.trim().strip_suffix('*') {
            Some(bound) if bound.is_empty() || bound.parse::<usize>().is_ok() => Last::Variable,
            Some(_) => return None,
            None => Last::Fixed(last.trim().parse().ok()?),
        };
        Some(Self { leading, last })
    }
}

/// How the text of a string cell is measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Width {
    /// So many characters, padded: in `BINARY` the bytes are always there, and a string
    /// ends at its first NUL or is trimmed of the blanks after it.
    Fixed(usize),
    /// A count of characters precedes the string in `BINARY`.
    Counted,
}

/// How many items one cell holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cell {
    /// One item, which is a scalar column.
    Scalar,
    /// Exactly so many items, which is a fixed-size list.
    Fixed(usize),
    /// A count of slices of so many items, the count read per cell, which is a list.
    Variable { slice: usize },
}

/// A column's primitive, and the cell `arraysize` makes of it.
///
/// An *item* is what the column's Arrow values are made of: one number, one bit, one complex
/// number, or — for text — one string. `arraysize`'s first dimension is the string's width
/// for text, since VOTable has no string primitive and spells one as an array of characters;
/// what is left of it is the cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Layout {
    pub primitive: Primitive,
    /// For text only.
    pub width: Width,
    pub cell: Cell,
}

impl Layout {
    /// The layout a `datatype` and an `arraysize` describe.
    ///
    /// **`arraysize="1"` is a scalar for a number and a one-character string for text.**
    /// VOTable 1.5 §2.2 says it "should not be used, as it is interpreted differently by
    /// different clients", which leaves the reader to pick; those are the readings that give
    /// every writer's intent the same value, since nobody writes a one-element array on
    /// purpose.
    pub fn new(primitive: Primitive, arraysize: Option<&ArraySize>) -> Result<Self, String> {
        let Some(size) = arraysize else {
            return Ok(Self {
                primitive,
                width: Width::Fixed(1),
                cell: Cell::Scalar,
            });
        };
        if primitive.is_text() {
            // The first dimension is the string; the rest, if there is any, is the cell.
            let (width, rest): (Width, &[usize]) = match (size.leading.split_first(), size.last) {
                (None, Last::Fixed(n)) => {
                    return Ok(Self {
                        primitive,
                        width: Width::Fixed(n),
                        cell: Cell::Scalar,
                    });
                }
                (None, Last::Variable) => {
                    return Ok(Self {
                        primitive,
                        width: Width::Counted,
                        cell: Cell::Scalar,
                    });
                }
                (Some((first, rest)), _) => (Width::Fixed(*first), rest),
            };
            let slice = product(rest)?;
            let cell = match size.last {
                Last::Fixed(n) => Cell::Fixed(slice.checked_mul(n).ok_or_else(too_large)?),
                Last::Variable => Cell::Variable { slice },
            };
            return Ok(Self {
                primitive,
                width,
                cell,
            });
        }
        let slice = product(&size.leading)?;
        let cell = match size.last {
            Last::Fixed(1) if size.leading.is_empty() => Cell::Scalar,
            Last::Fixed(n) => Cell::Fixed(slice.checked_mul(n).ok_or_else(too_large)?),
            Last::Variable => Cell::Variable { slice },
        };
        Ok(Self {
            primitive,
            width: Width::Fixed(1),
            cell,
        })
    }
}

fn product(dimensions: &[usize]) -> Result<usize, String> {
    dimensions
        .iter()
        .try_fold(1usize, |acc, n| acc.checked_mul(*n))
        .ok_or_else(too_large)
}

fn too_large() -> String {
    "its arraysize is larger than any cell can be".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout(datatype: &str, arraysize: Option<&str>) -> Layout {
        let size = arraysize.map(|written| ArraySize::parse(written).unwrap());
        Layout::new(Primitive::parse(datatype).unwrap(), size.as_ref()).unwrap()
    }

    #[test]
    fn an_arraysize_is_dimensions_and_a_possibly_variable_last() {
        assert_eq!(
            ArraySize::parse("64x64x10*"),
            Some(ArraySize {
                leading: vec![64, 64],
                last: Last::Variable
            })
        );
        assert_eq!(
            ArraySize::parse("*"),
            Some(ArraySize {
                leading: vec![],
                last: Last::Variable
            })
        );
        for nonsense in ["", "x", "3x", "*x3", "a", "3**", "-1"] {
            assert_eq!(ArraySize::parse(nonsense), None, "{nonsense}");
        }
    }

    /// Text takes its first dimension as the string's width, and every other primitive
    /// counts items.
    #[test]
    fn text_spends_its_first_dimension_on_the_string() {
        assert_eq!(layout("char", None).width, Width::Fixed(1));
        assert_eq!(layout("char", Some("10")).cell, Cell::Scalar);
        assert_eq!(layout("char", Some("10*")).width, Width::Counted);
        let strings = layout("char", Some("8x3"));
        assert_eq!(
            (strings.width, strings.cell),
            (Width::Fixed(8), Cell::Fixed(3))
        );
        let strings = layout("unicodeChar", Some("8x2x*"));
        assert_eq!(
            (strings.width, strings.cell),
            (Width::Fixed(8), Cell::Variable { slice: 2 })
        );

        assert_eq!(layout("double", None).cell, Cell::Scalar);
        assert_eq!(layout("double", Some("1")).cell, Cell::Scalar);
        assert_eq!(layout("int", Some("2x3")).cell, Cell::Fixed(6));
        assert_eq!(layout("int", Some("2x*")).cell, Cell::Variable { slice: 2 });
        assert_eq!(layout("float", Some("*")).cell, Cell::Variable { slice: 1 });
    }
}
