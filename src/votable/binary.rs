//! A cell of a `BINARY` or `BINARY2` stream (VOTable 1.5 §5.3, §5.4 and §6).
//!
//! Every multi-byte value is big-endian. A variable cell is preceded by a four-byte count,
//! and how that count and a variable bit array are laid out is [`Dialect`]'s to say.

use crate::votable::column::Builder;
use crate::votable::datatype::{Cell, Primitive, Width};
use crate::votable::tabledata::fixed_or_counted;

/// The two layouts the writers most uploads come from give the cells the text leaves open.
///
/// **§5.3's count, for an array of more than one dimension.** Worded the same since 1.2, it
/// is "the number of items of the array", and the bytes "the size and number of the
/// primitives" multiplied. STIL reads the items as primitives: for `2x*` a count of six is six
/// numbers, for `char` `8x*` a count of sixteen is two strings. astropy, and the CDS writer
/// after it, read them as slices of the last dimension: a count of three for `2x*` is six
/// numbers. For a one-dimensional array the two are the same number, which is why the
/// difference goes unnoticed until a `2x*` column is uploaded.
///
/// **A variable-length `bit` array.** §6 packs bits eight to a byte, the most significant
/// first, and STIL does. astropy writes the count of bits and then one byte per bit, `0x08`
/// where it is set.
///
/// Neither is refused: astropy is what pyvo uploads with and STIL is what TOPCAT does. The
/// document is read in one dialect and, where that does not parse, in the other — which is
/// how a wrong reading shows itself, every cell after the first misaligned one being read
/// from the middle of another.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Stil,
    Astropy,
}

impl Dialect {
    /// The other one.
    pub fn other(self) -> Self {
        match self {
            Self::Stil => Self::Astropy,
            Self::Astropy => Self::Stil,
        }
    }
}

/// Whether the two dialects read a column's cells differently: a variable array of more than
/// one dimension, or a variable bit array.
pub fn is_ambiguous(builder: &Builder) -> bool {
    matches!(builder.cell(), Cell::Variable { .. })
        && (slice_primitives(builder) > 1 || builder.primitive() == Primitive::Bit)
}

/// How many primitives one slice of a variable cell's last dimension is, which is where the
/// two counts differ; for a one-dimensional array it is one, and they agree.
fn slice_primitives(builder: &Builder) -> usize {
    match builder.cell() {
        Cell::Variable { slice } => match builder.column.layout.width {
            Width::Fixed(width) if builder.primitive().is_text() => slice.saturating_mul(width),
            _ => slice,
        },
        _ => 1,
    }
}

/// The decoded stream, read from the front.
#[derive(Debug)]
pub struct Cursor<'a> {
    data: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, at: 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.at >= self.data.len()
    }

    pub fn position(&self) -> usize {
        self.at
    }

    /// The next `n` bytes, or a refusal saying the stream ended inside a row.
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let ended = || "the stream ends inside a row".to_owned();
        let end = self.at.checked_add(n).ok_or_else(ended)?;
        let taken = self.data.get(self.at..end).ok_or_else(ended)?;
        self.at = end;
        Ok(taken)
    }

    fn count(&mut self) -> Result<usize, String> {
        let bytes = fixed::<4>(self.take(4)?);
        usize::try_from(u32::from_be_bytes(bytes))
            .map_err(|_| "a cell's count is larger than this machine can hold".to_owned())
    }
}

/// The first `N` bytes of a slice already taken at that length.
fn fixed<const N: usize>(bytes: &[u8]) -> [u8; N] {
    bytes.first_chunk::<N>().copied().unwrap_or([0; N])
}

/// One cell. `null` is its `BINARY2` flag: the bytes are still there and are read past.
pub fn push(
    builder: &mut Builder,
    cursor: &mut Cursor<'_>,
    null: bool,
    dialect: Dialect,
) -> Result<(), String> {
    let primitive = builder.primitive();
    let counted = primitive.is_text() && builder.column.layout.width == Width::Counted;
    let variable = matches!(builder.cell(), Cell::Variable { .. });
    let items = match builder.cell() {
        // A counted string is one item whose own length is the count: its characters are
        // the variable last dimension.
        Cell::Scalar if counted => cursor.count()?,
        Cell::Scalar => 1,
        Cell::Fixed(n) => n,
        // Primitives: for a multi-dimensional `char`, characters rather than strings.
        Cell::Variable { .. } => {
            let count = cursor.count()?;
            let slice = slice_primitives(builder);
            match dialect {
                // Held to whole slices, which is half of what tells the dialects apart.
                Dialect::Stil if !count.is_multiple_of(slice.max(1)) => {
                    return Err(format!(
                        "a count of {count} is not a whole number of its {slice}-item slices"
                    ));
                }
                Dialect::Stil => count,
                Dialect::Astropy => count
                    .checked_mul(slice)
                    .ok_or_else(|| "a cell's count is larger than any cell can be".to_owned())?,
            }
        }
    };
    match primitive {
        Primitive::Char | Primitive::UnicodeChar => strings(builder, cursor, items, null),
        Primitive::Bit => {
            // One byte per bit is astropy's variable array; everything else is packed.
            let bytewise = variable && dialect == Dialect::Astropy;
            let length = match bytewise {
                true => items,
                false => items.div_ceil(8),
            };
            let bytes = cursor.take(length)?;
            if null {
                builder.null_cell();
                return Ok(());
            }
            match (bytewise, builder.cell()) {
                (true, _) => {
                    for byte in bytes {
                        builder.boolean(Some(*byte != 0));
                    }
                }
                // One bit in one byte, whose padding §6 has be zero, so any bit set is the
                // value: STIL writes 0x80 and astropy 0x08.
                (false, Cell::Scalar) => builder.boolean(Some(fixed::<1>(bytes)[0] != 0)),
                (false, _) => {
                    for at in 0..items {
                        let byte = bytes.get(at / 8).copied().unwrap_or_default();
                        builder.boolean(Some(byte & (0x80 >> (at % 8)) != 0));
                    }
                }
            }
            builder.end_cell()
        }
        _ => {
            let width = width(primitive);
            let length = items
                .checked_mul(width)
                .ok_or_else(|| "a cell is larger than any stream can be".to_owned())?;
            let bytes = cursor.take(length)?;
            if null {
                builder.null_cell();
                return Ok(());
            }
            for value in bytes.chunks_exact(width) {
                item(builder, primitive, value)?;
            }
            builder.end_cell()
        }
    }
}

/// The bytes of one item, for everything but text and bits.
fn width(primitive: Primitive) -> usize {
    match primitive {
        Primitive::Boolean | Primitive::UnsignedByte | Primitive::Char | Primitive::Bit => 1,
        Primitive::Short | Primitive::UnicodeChar => 2,
        Primitive::Int | Primitive::Float => 4,
        Primitive::Long | Primitive::Double | Primitive::FloatComplex => 8,
        Primitive::DoubleComplex => 16,
    }
}

fn item(builder: &mut Builder, primitive: Primitive, bytes: &[u8]) -> Result<(), String> {
    match primitive {
        Primitive::Boolean => builder.boolean(match fixed::<1>(bytes)[0] {
            b'T' | b't' | b'1' => Some(true),
            b'F' | b'f' | b'0' => Some(false),
            0 | b' ' | b'?' => None,
            other => return Err(format!("byte {other:#04x} is not a boolean")),
        }),
        Primitive::UnsignedByte => builder.integer(i64::from(fixed::<1>(bytes)[0])),
        Primitive::Short => builder.integer(i64::from(i16::from_be_bytes(fixed(bytes)))),
        Primitive::Int => builder.integer(i64::from(i32::from_be_bytes(fixed(bytes)))),
        Primitive::Long => builder.integer(i64::from_be_bytes(fixed(bytes))),
        Primitive::Float => builder.float(f64::from(f32::from_be_bytes(fixed(bytes)))),
        Primitive::Double => builder.float(f64::from_be_bytes(fixed(bytes))),
        Primitive::FloatComplex => {
            for part in bytes.as_chunks::<4>().0 {
                builder.float(f64::from(f32::from_be_bytes(*part)));
            }
        }
        Primitive::DoubleComplex => {
            for part in bytes.as_chunks::<8>().0 {
                builder.float(f64::from_be_bytes(*part));
            }
        }
        // Text and bits are read by their own functions.
        Primitive::Char | Primitive::UnicodeChar | Primitive::Bit => {}
    }
    Ok(())
}

/// Text: one string, or for a two-dimensional `char` so many strings of one width.
fn strings(
    builder: &mut Builder,
    cursor: &mut Cursor<'_>,
    items: usize,
    null: bool,
) -> Result<(), String> {
    let per_char = match builder.primitive() {
        Primitive::UnicodeChar => 2,
        _ => 1,
    };
    let layout = builder.column.layout;
    let too_large = || "a cell is larger than any stream can be".to_owned();
    // How many characters the cell holds, and how many make one string. A counted string's
    // count is its characters, and so is a variable multi-dimensional one's, the count being
    // of primitives.
    let (total, characters) = match (layout.cell, layout.width) {
        (Cell::Scalar, Width::Counted) => (items, items),
        (Cell::Scalar, Width::Fixed(width)) => (width, width),
        (Cell::Fixed(strings), Width::Fixed(width)) => {
            (strings.checked_mul(width).ok_or_else(too_large)?, width)
        }
        (Cell::Variable { .. }, Width::Fixed(width)) => (items, width),
        (_, Width::Counted) => return Err("a multi-dimensional char cell has no width".to_owned()),
    };
    let length = total.checked_mul(per_char).ok_or_else(too_large)?;
    let per_string = characters.checked_mul(per_char).ok_or_else(too_large)?;
    let strings = match (layout.cell, per_string) {
        (Cell::Scalar, _) => 1,
        (_, 0) => 0,
        _ => length.div_ceil(per_string),
    };
    let bytes = cursor.take(length)?;
    if null {
        builder.null_cell();
        return Ok(());
    }
    let string_width = match layout.cell {
        Cell::Scalar => layout.width,
        _ => Width::Fixed(characters),
    };
    // A width of nothing is a cell of empty strings, which `chunks` cannot count out. A
    // variable cell's last string may be short, where the count is not a whole number of
    // them, and is then the characters there are.
    if per_string == 0 {
        for _ in 0..strings {
            builder.text("");
        }
        return builder.end_cell();
    }
    for piece in bytes.chunks(per_string) {
        let text = match per_char {
            2 => {
                let units: Vec<u16> = piece
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|pair| u16::from_be_bytes(*pair))
                    .collect();
                String::from_utf16_lossy(&units)
            }
            _ => ascii(piece),
        };
        builder.text(&fixed_or_counted(&text, string_width));
    }
    builder.end_cell()
}

/// `char` is ASCII by §6, and a byte past it is taken as UTF-8 where the bytes are UTF-8 and
/// as Latin-1 otherwise — the two things a writer that ignored the rule will have written.
fn ascii(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => text.to_owned(),
        Err(_) => bytes.iter().map(|byte| char::from(*byte)).collect(),
    }
}
