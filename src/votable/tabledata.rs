//! A `TD`'s text as the values of one cell (VOTable 1.5 §5.1 and §6).

use crate::votable::column::Builder;
use crate::votable::datatype::{Cell, Primitive, Width};

/// One cell, from the text of its `TD`. `None` is a `TD` with nothing in it, which is a null
/// whatever the column holds (§5.1) — for text too, so an empty string and a null are one
/// thing here, which §5.5 says of this serialization.
pub fn push(builder: &mut Builder, text: Option<&str>) -> Result<(), String> {
    let Some(text) = text.filter(|text| !text.is_empty()) else {
        builder.null_cell();
        return Ok(());
    };
    match builder.primitive() {
        Primitive::Char | Primitive::UnicodeChar => strings(builder, text)?,
        Primitive::Bit => bits(builder, text)?,
        Primitive::Boolean => {
            let tokens = boolean_tokens(text);
            count(builder, tokens.len(), 1)?;
            for token in tokens {
                builder.boolean(boolean(token)?);
            }
        }
        primitive => {
            let tokens: Vec<&str> = text.split_whitespace().collect();
            // Whitespace alone is not a number, and in a numeric column it is how a writer
            // that pads its cells writes nothing.
            if tokens.is_empty() {
                builder.null_cell();
                return Ok(());
            }
            let per_item = match primitive.is_complex() {
                true => 2,
                false => 1,
            };
            count(builder, tokens.len(), per_item)?;
            for token in tokens {
                number(builder, primitive, token)?;
            }
        }
    }
    builder.end_cell()
}

/// Whether so many values fit the cell, a complex number being two of them.
fn count(builder: &Builder, values: usize, per_item: usize) -> Result<(), String> {
    let fits = match builder.cell() {
        Cell::Scalar => values == per_item,
        Cell::Fixed(n) => values == n.saturating_mul(per_item),
        Cell::Variable { slice } => values.is_multiple_of(slice.saturating_mul(per_item).max(1)),
    };
    match fits {
        true => Ok(()),
        false => Err(format!(
            "holds {values} values, which is not what its arraysize {} allows",
            builder
                .column
                .declared
                .arraysize
                .as_deref()
                .unwrap_or("(none, one value)")
        )),
    }
}

fn number(builder: &mut Builder, primitive: Primitive, token: &str) -> Result<(), String> {
    match primitive {
        Primitive::Float | Primitive::FloatComplex => {
            let value = token
                .parse::<f32>()
                .map_err(|_| format!("{token:?} is not a float"))?;
            builder.float(f64::from(value));
        }
        Primitive::Double | Primitive::DoubleComplex => builder.float(float(token)?),
        integer_type => builder.integer(integer(integer_type, token)?),
    }
    Ok(())
}

/// An integer as §6 writes one: decimal with an optional sign, or `0x` and hexadigits.
///
/// Hexadecimal is the type's own bits, so `0xFFFF` is `-1` as a `short`: §6 allows as many
/// hexadigits as the type has nybbles, which is only a full range if the top bit is the sign.
pub fn integer(primitive: Primitive, token: &str) -> Result<i64, String> {
    let refuse = || format!("{token:?} is not a {primitive}");
    let (low, high, bits): (i64, i64, u32) = match primitive {
        Primitive::UnsignedByte => (0, 255, 8),
        Primitive::Short => (i16::MIN.into(), i16::MAX.into(), 16),
        Primitive::Int => (i32::MIN.into(), i32::MAX.into(), 32),
        _ => (i64::MIN, i64::MAX, 64),
    };
    if let Some(hex) = token
        .strip_prefix("0x")
        .or_else(|| token.strip_prefix("0X"))
    {
        if hex.is_empty() || hex.len() > (bits / 4) as usize {
            return Err(refuse());
        }
        let raw = u64::from_str_radix(hex, 16).map_err(|_| refuse())?;
        // The hexadigits have been counted against the type's width, so each narrowing
        // holds and the reinterpretation is the type's own bits.
        return Ok(match primitive {
            Primitive::UnsignedByte => i64::from(u8::try_from(raw).map_err(|_| refuse())?),
            Primitive::Short => i64::from(u16::try_from(raw).map_err(|_| refuse())? as i16),
            Primitive::Int => i64::from(u32::try_from(raw).map_err(|_| refuse())? as i32),
            _ => raw as i64,
        });
    }
    let value = token.parse::<i64>().map_err(|_| refuse())?;
    match (low..=high).contains(&value) {
        true => Ok(value),
        false => Err(format!("{token:?} is outside what a {primitive} holds")),
    }
}

/// A double as §6 writes one, with `NaN`, `+Inf` and `-Inf`.
///
/// Rust's own parser takes those three in any case, and `inf` and `infinity` too, which is
/// wider than §6 and loses nothing: no number is spelled that way.
pub fn float(token: &str) -> Result<f64, String> {
    token
        .parse::<f64>()
        .map_err(|_| format!("{token:?} is not a double"))
}

/// A boolean cell's values. An array of them is written with whitespace between, or — as
/// `TTF` — without, the way §6 writes a bit array; a lone word is one value either way.
fn boolean_tokens(text: &str) -> Vec<&str> {
    let tokens: Vec<&str> = text.split_whitespace().collect();
    match tokens.as_slice() {
        [] => vec![""],
        [one]
            if one.len() > 1 && !matches!(one.to_ascii_lowercase().as_str(), "true" | "false") =>
        {
            // Each character is ASCII here or the value is refused below anyway.
            (0..one.len())
                .filter_map(|at| one.get(at..at + 1))
                .collect()
        }
        _ => tokens,
    }
}

/// One boolean, as §6 spells it: `T`, `t`, `1` or any casing of `true`; `F`, `f`, `0` or any
/// casing of `false`; and `?`, a blank or nothing for a null.
fn boolean(token: &str) -> Result<Option<bool>, String> {
    match token {
        "T" | "t" | "1" => Ok(Some(true)),
        "F" | "f" | "0" => Ok(Some(false)),
        "" | "?" => Ok(None),
        word if word.eq_ignore_ascii_case("true") => Ok(Some(true)),
        word if word.eq_ignore_ascii_case("false") => Ok(Some(false)),
        _ => Err(format!("{token:?} is not a boolean")),
    }
}

/// A bit array: `0` and `1`, whitespace ignored.
fn bits(builder: &mut Builder, text: &str) -> Result<(), String> {
    let digits: Vec<char> = text.chars().filter(|c| !c.is_whitespace()).collect();
    count(builder, digits.len(), 1)?;
    for digit in digits {
        match digit {
            '0' => builder.boolean(Some(false)),
            '1' => builder.boolean(Some(true)),
            _ => return Err(format!("{digit:?} is not a bit")),
        }
    }
    Ok(())
}

/// Text, which is one string or — for a two-dimensional `char` — several of one width.
fn strings(builder: &mut Builder, text: &str) -> Result<(), String> {
    let width = builder.column.layout.width;
    match (builder.cell(), width) {
        (Cell::Scalar, width) => {
            builder.text(&fixed_or_counted(text, width));
            Ok(())
        }
        (cell, Width::Fixed(width)) => {
            let characters: Vec<char> = text.chars().collect();
            let mut pieces: Vec<String> = characters
                .chunks(width.max(1))
                .map(|chunk| {
                    fixed_or_counted(&chunk.iter().collect::<String>(), Width::Fixed(width))
                })
                .collect();
            // A writer that drops trailing blanks from the cell drops the last strings'
            // padding with them, so a short cell is filled out with empty strings; one that
            // is too long has said something the arraysize does not allow.
            let wanted = match cell {
                Cell::Fixed(n) => n,
                Cell::Variable { slice } => pieces.len().div_ceil(slice.max(1)) * slice.max(1),
                Cell::Scalar => 1,
            };
            if pieces.len() > wanted {
                return Err(format!(
                    "holds {} strings of {width} characters, which is more than its arraysize \
                     allows",
                    pieces.len()
                ));
            }
            pieces.resize(wanted, String::new());
            for piece in &pieces {
                builder.text(piece);
            }
            Ok(())
        }
        (_, Width::Counted) => Err("a multi-dimensional char cell has no width".to_owned()),
    }
}

/// A string as the column declares it.
///
/// **A fixed-width string ends at its first NUL and is trimmed of trailing blanks**: §5.1's
/// own example pads `Apple` to ten characters with them, and §6 has the binary form end at a
/// NUL, both being padding rather than text. A counted string keeps what it was given, the
/// count having said how long it is — but still ends at a NUL, which §6 allows either kind.
/// A single character is taken as it is, a blank being as much a character as any other.
pub fn fixed_or_counted(text: &str, width: Width) -> String {
    let text = text.split('\0').next().unwrap_or_default();
    match width {
        Width::Fixed(n) => {
            // Longer than its width is only possible in TABLEDATA, and is cut to it, the way
            // astropy and STILTS both read it: the width is what the FIELD says the column is.
            let cut = match text.char_indices().nth(n) {
                Some((at, _)) => text.get(..at).unwrap_or(text),
                None => text,
            };
            match n > 1 {
                true => cut.trim_end_matches(' ').to_owned(),
                false => cut.to_owned(),
            }
        }
        Width::Counted => text.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, AsArray, RecordBatch};
    use datafusion::arrow::compute::concat_batches;

    use super::*;
    use crate::votable::document;

    /// The rows of a one-column document, `cells` being each row's `TD`.
    fn column(field: &str, cells: &[&str]) -> Result<RecordBatch, String> {
        let rows: String = cells
            .iter()
            .map(|cell| format!("<TR><TD>{cell}</TD></TR>"))
            .collect();
        let table = document::read(
            format!(
                "<VOTABLE><RESOURCE><TABLE>{field}<DATA><TABLEDATA>{rows}</TABLEDATA></DATA>\
                 </TABLE></RESOURCE></VOTABLE>"
            )
            .as_bytes(),
        )?;
        Ok(concat_batches(&table.schema, &table.batches).unwrap())
    }

    /// A writer that pads its cells writes a missing number as blanks, which is the empty
    /// cell §5.1 makes a null — for a scalar and for an array alike.
    #[test]
    fn a_numeric_cell_of_blanks_is_a_null() {
        for field in [
            "<FIELD name=\"a\" datatype=\"int\"/>",
            "<FIELD name=\"a\" datatype=\"double\"/>",
            "<FIELD name=\"a\" datatype=\"float\" arraysize=\"3\"/>",
            "<FIELD name=\"a\" datatype=\"long\" arraysize=\"*\"/>",
            "<FIELD name=\"a\" datatype=\"doubleComplex\"/>",
        ] {
            let batch = column(field, &["   ", " \n\t ", ""]).unwrap();
            assert_eq!(batch.column(0).null_count(), 3, "{field}");
        }
    }

    /// A two-dimensional `char` is strings of the first dimension's width. A cell short of
    /// the count is filled out with empty strings, the way a writer that trims trailing
    /// blanks leaves it; one with more strings than the count allows is refused.
    #[test]
    fn a_char_array_cell_is_strings_of_its_width_up_to_its_count() {
        let fixed = "<FIELD name=\"a\" datatype=\"char\" arraysize=\"3x2\"/>";
        let batch = column(fixed, &["abcdef", "abcd", "ab"]).unwrap();
        let lists = batch.column(0).as_fixed_size_list();
        let strings = |row: usize| {
            let value = lists.value(row);
            let value = value.as_string::<i32>();
            (0..value.len())
                .map(|at| value.value(at).to_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(strings(0), ["abc", "def"]);
        assert_eq!(strings(1), ["abc", "d"]);
        assert_eq!(strings(2), ["ab", ""]);
        let refused = column(fixed, &["abcdefg"]).unwrap_err();
        assert!(refused.contains("3 strings of 3 characters"), "{refused}");

        // A variable count is filled out to whole slices, and has no upper bound to refuse.
        let variable = "<FIELD name=\"a\" datatype=\"char\" arraysize=\"2x2x*\"/>";
        let batch = column(variable, &["abcde"]).unwrap();
        let lists = batch.column(0).as_list::<i32>();
        assert_eq!(lists.value(0).len(), 4);
    }

    #[test]
    fn an_integer_is_decimal_or_the_types_own_bits_in_hex() {
        assert_eq!(integer(Primitive::Short, "+15"), Ok(15));
        assert_eq!(integer(Primitive::Short, "0xFFFF"), Ok(-1));
        assert_eq!(
            integer(Primitive::Int, "0x7fffffff"),
            Ok(i64::from(i32::MAX))
        );
        assert_eq!(integer(Primitive::UnsignedByte, "0xff"), Ok(255));
        assert_eq!(
            integer(Primitive::Long, "-9223372036854775808"),
            Ok(i64::MIN)
        );
        assert_eq!(integer(Primitive::Long, "0xFFFFFFFFFFFFFFFF"), Ok(-1));
        assert_eq!(integer(Primitive::Long, "0x8000000000000000"), Ok(i64::MIN));
        assert!(integer(Primitive::Long, "0x10000000000000000").is_err());
        assert!(integer(Primitive::Short, "32768").is_err());
        assert!(integer(Primitive::UnsignedByte, "-1").is_err());
        assert!(integer(Primitive::Short, "0x10000").is_err());
        assert!(integer(Primitive::Int, "1.0").is_err());
    }

    #[test]
    fn a_float_has_its_three_special_values() {
        assert!(float("NaN").unwrap().is_nan());
        assert_eq!(float("+Inf"), Ok(f64::INFINITY));
        assert_eq!(float("-Inf"), Ok(f64::NEG_INFINITY));
        assert_eq!(float("+1.5e3"), Ok(1500.0));
        assert!(float("1.5D3").is_err());
    }

    #[test]
    fn a_boolean_is_spelled_every_way_section_six_allows() {
        for (token, value) in [
            ("T", Some(true)),
            ("t", Some(true)),
            ("1", Some(true)),
            ("tRUe", Some(true)),
            ("F", Some(false)),
            ("0", Some(false)),
            ("FalsE", Some(false)),
            ("?", None),
            ("", None),
        ] {
            assert_eq!(boolean(token), Ok(value), "{token}");
        }
        assert!(boolean("yes").is_err());
        assert_eq!(boolean_tokens("TFT"), ["T", "F", "T"]);
        assert_eq!(boolean_tokens("true false"), ["true", "false"]);
        assert_eq!(boolean_tokens("true"), ["true"]);
    }

    #[test]
    fn a_fixed_string_loses_its_padding_and_a_counted_one_keeps_its_blanks() {
        assert_eq!(fixed_or_counted("Apple     ", Width::Fixed(10)), "Apple");
        assert_eq!(fixed_or_counted("ab\0cd", Width::Fixed(5)), "ab");
        assert_eq!(fixed_or_counted(" padded  ", Width::Counted), " padded  ");
        assert_eq!(fixed_or_counted(" ", Width::Fixed(1)), " ");
    }
}
