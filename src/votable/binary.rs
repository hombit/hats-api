//! A cell of a `BINARY` or `BINARY2` stream (VOTable 1.5 §5.3, §5.4 and §6).
//!
//! Every multi-byte value is big-endian. A variable cell is preceded by a four-byte count,
//! which counts the slices of its last dimension — for `2x*` a count of three is six
//! numbers — the reading astropy and STIL both give it.

use crate::votable::column::Builder;
use crate::votable::datatype::{Cell, Primitive, Width};
use crate::votable::tabledata::fixed_or_counted;

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
pub fn push(builder: &mut Builder, cursor: &mut Cursor<'_>, null: bool) -> Result<(), String> {
    let primitive = builder.primitive();
    let counted = primitive.is_text() && builder.column.layout.width == Width::Counted;
    let items = match builder.cell() {
        // A counted string is one item whose own length is the count: its characters are
        // the variable last dimension.
        Cell::Scalar if counted => cursor.count()?,
        Cell::Scalar => 1,
        Cell::Fixed(n) => n,
        Cell::Variable { slice } => cursor
            .count()?
            .checked_mul(slice)
            .ok_or_else(|| "a cell's count is larger than any cell can be".to_owned())?,
    };
    match primitive {
        Primitive::Char | Primitive::UnicodeChar => strings(builder, cursor, items, null),
        Primitive::Bit => {
            let bytes = cursor.take(items.div_ceil(8))?;
            if null {
                builder.null_cell();
                return Ok(());
            }
            for at in 0..items {
                let byte = bytes.get(at / 8).copied().unwrap_or_default();
                builder.boolean(Some(byte & (0x80 >> (at % 8)) != 0));
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
    // How many strings, and how many characters each. A counted string's count is its
    // characters, that being its variable last dimension.
    let (strings, characters) = match (layout.cell, layout.width) {
        (Cell::Scalar, Width::Counted) => (1, items),
        (Cell::Scalar, Width::Fixed(width)) => (1, width),
        (_, Width::Fixed(width)) => (items, width),
        (_, Width::Counted) => return Err("a multi-dimensional char cell has no width".to_owned()),
    };
    let per_string = characters
        .checked_mul(per_char)
        .ok_or_else(|| "a cell is larger than any stream can be".to_owned())?;
    let length = strings
        .checked_mul(per_string)
        .ok_or_else(|| "a cell is larger than any stream can be".to_owned())?;
    let bytes = cursor.take(length)?;
    if null {
        builder.null_cell();
        return Ok(());
    }
    let string_width = match layout.cell {
        Cell::Scalar => layout.width,
        _ => Width::Fixed(characters),
    };
    // A width of nothing is a cell of empty strings, which `chunks_exact` cannot count out.
    if per_string == 0 {
        for _ in 0..strings {
            builder.text("");
        }
        return builder.end_cell();
    }
    for piece in bytes.chunks_exact(per_string) {
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
