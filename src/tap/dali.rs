//! A DALI parameter value, read into a type.
//!
//! A parameter's value is a small grammar and always the same one: fields separated by a
//! delimiter, where a leading keyword says how many follow it. DALI writes its shapes and
//! intervals that way — `CIRCLE 12.3 45.6 0.5` is three numbers and `RANGE 12.0 14.0 34.0
//! 36.0` is four (§3.3) — and TAP writes `UPLOAD=name,uri` with a comma (§2.7.6). Several
//! values of one parameter are several parameters (DALI §3.2), so nothing here reads more
//! than one value.
//!
//! This is a `serde` data format over that grammar, so a parameter is a type and the arity,
//! the keyword and the number parsing are the type's to state:
//!
//! ```ignore
//! #[derive(Deserialize)]
//! #[serde(rename_all = "UPPERCASE")]
//! enum Shape {
//!     Circle(f64, f64, f64),
//!     Range(f64, f64, f64, f64),
//!     Polygon(Vec<f64>),
//! }
//! let shape: Shape = dali::value("CIRCLE 12.3 45.6 0.5", Delimiter::Space)?;
//! ```
//!
//! Three things it does that a general-purpose format does not, each because a parameter
//! value is text a caller typed rather than a document a program wrote:
//!
//! - **[`Tail`] is the rest of the value, delimiter and all.** The last field of an option
//!   carrying a credential has to arrive whole: a separator taken seriously inside it
//!   truncates the secret, and a truncated credential is a request that reads as anonymous.
//! - **The delimiter belongs to the parameter**, not to the format. TAP separates `UPLOAD`'s
//!   two fields with a comma and DALI separates a shape's numbers with spaces.
//! - **A value with fields left over is an error.** Arity is what a keyword promised, so
//!   `CIRCLE 1 2 3 4` is a refusal rather than a circle.
//!
//! There is no serializer: nothing here writes a parameter, a caller's request being the
//! only place these values come from.

use std::fmt;

use serde::Deserialize;
use serde::de::{self, DeserializeSeed, EnumAccess, SeqAccess, VariantAccess, Visitor};

/// What separates one field from the next inside a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delimiter {
    /// TAP's, as in `UPLOAD=name,uri`.
    Comma,
    /// DALI's own, as in `CIRCLE 12.3 45.6 0.5`.
    Space,
}

impl Delimiter {
    /// The next field and what is left after it, or `None` where the value is spent.
    fn split(self, rest: &str) -> (&str, Option<&str>) {
        let rest = rest.trim_start();
        match self {
            Self::Comma => match rest.split_once(',') {
                Some((field, tail)) => (field.trim_end(), Some(tail)),
                None => (rest.trim_end(), None),
            },
            Self::Space => match rest.split_once(char::is_whitespace) {
                Some((field, tail)) => (field, Some(tail)),
                None => (rest, None),
            },
        }
    }
}

/// Read one parameter value into `T`.
pub fn value<'de, T>(text: &'de str, delimiter: Delimiter) -> Result<T, Error>
where
    T: Deserialize<'de>,
{
    let mut reader = Reader {
        rest: Some(text),
        delimiter,
    };
    let read = T::deserialize(&mut reader)?;
    match reader.rest {
        Some(left) if !left.trim().is_empty() => {
            Err(Error(format!("{left:?} is more than this value takes")))
        }
        _ => Ok(read),
    }
}

/// The rest of a value, taken whole.
///
/// Whatever is left when this field is reached, delimiter and all, so a credential carrying
/// the delimiter arrives as it was written. It is the last field of whatever holds it; a
/// field after it has nothing to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tail(String);

/// The name this format recognises [`Tail`] by, `serde` having no other way to say it.
const TAIL: &str = "$dali::tail";

impl Tail {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl<'de> Deserialize<'de> for Tail {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Whatever;

        impl Visitor<'_> for Whatever {
            type Value = Tail;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("the rest of the value")
            }

            fn visit_str<E>(self, text: &str) -> Result<Tail, E> {
                Ok(Tail(text.to_owned()))
            }
        }

        deserializer.deserialize_newtype_struct(TAIL, Whatever)
    }
}

/// What went wrong reading a value.
///
/// A caller's own text is what produced it, so it is rendered into the refusal the route
/// writes, with the parameter's name added there — this knows the shape and not the name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

impl de::Error for Error {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self(message.to_string())
    }
}

/// One value, read a field at a time.
struct Reader<'de> {
    /// What is left, or `None` once every field has been taken.
    rest: Option<&'de str>,
    delimiter: Delimiter,
}

impl<'de> Reader<'de> {
    /// The next field.
    fn field(&mut self) -> Result<&'de str, Error> {
        let rest = self
            .rest
            .ok_or_else(|| Error("the value ends before its last field".to_owned()))?;
        let (field, tail) = self.delimiter.split(rest);
        self.rest = tail;
        Ok(field)
    }

    /// Everything that is left, delimiters included.
    fn tail(&mut self) -> Result<&'de str, Error> {
        let rest = self
            .rest
            .ok_or_else(|| Error("the value ends before its last field".to_owned()))?;
        self.rest = None;
        Ok(rest.trim())
    }

    /// Whether anything is left to read.
    fn spent(&self) -> bool {
        self.rest.is_none_or(|rest| rest.trim().is_empty())
    }

    /// The next field as a number, named by what it was being read as.
    fn number<T>(&mut self, what: &str) -> Result<T, Error>
    where
        T: std::str::FromStr,
    {
        let field = self.field()?;
        field
            .parse()
            .map_err(|_| Error(format!("{field:?} is not {what}")))
    }
}

/// Every scalar a parameter value can hold, read from one field.
macro_rules! numbers {
    ($($method:ident => $visit:ident, $what:literal;)*) => {
        $(
            fn $method<V>(self, visitor: V) -> Result<V::Value, Error>
            where
                V: Visitor<'de>,
            {
                visitor.$visit(self.number($what)?)
            }
        )*
    };
}

impl<'de> serde::Deserializer<'de> for &mut Reader<'de> {
    type Error = Error;

    /// A parameter value says nothing about its own shape, so the type has to.
    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        Err(Error(
            "a parameter value is read into a known shape, and this one asks the value what \
             shape it is"
                .to_owned(),
        ))
    }

    numbers! {
        deserialize_i8 => visit_i8, "a whole number";
        deserialize_i16 => visit_i16, "a whole number";
        deserialize_i32 => visit_i32, "a whole number";
        deserialize_i64 => visit_i64, "a whole number";
        deserialize_u8 => visit_u8, "a whole number";
        deserialize_u16 => visit_u16, "a whole number";
        deserialize_u32 => visit_u32, "a whole number";
        deserialize_u64 => visit_u64, "a whole number";
        deserialize_f32 => visit_f32, "a number";
        deserialize_f64 => visit_f64, "a number";
        deserialize_char => visit_char, "one character";
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        let field = self.field()?;
        match field {
            _ if field.eq_ignore_ascii_case("true") => visitor.visit_bool(true),
            _ if field.eq_ignore_ascii_case("false") => visitor.visit_bool(false),
            other => Err(Error(format!("{other:?} is not true or false"))),
        }
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_borrowed_str(self.field()?)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_borrowed_bytes(self.field()?.as_bytes())
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_bytes(visitor)
    }

    /// A field that is there or is not, which is how a value's optional tail is written.
    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        match self.spent() {
            true => visitor.visit_none(),
            false => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    /// [`Tail`] is the one newtype this format knows by name: it takes what is left.
    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        match name == TAIL {
            true => visitor.visit_borrowed_str(self.tail()?),
            false => visitor.visit_newtype_struct(self),
        }
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(Fields { reader: self })
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    /// A value has fields in order and no names, so a map is read as the pairs it is written
    /// as rather than by name.
    fn deserialize_map<V>(self, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        Err(Error(
            "a parameter value is a list of fields, not names and values".to_owned(),
        ))
    }

    /// A struct's fields are read in the order it declares them, the value carrying no names.
    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    /// The leading keyword, and then whatever that keyword's variant takes.
    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_enum(Keyword {
            reader: self,
            variants,
        })
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        let _ = self.field()?;
        visitor.visit_unit()
    }
}

/// The fields of a sequence, a tuple or a struct, taken until the value is spent.
struct Fields<'a, 'de> {
    reader: &'a mut Reader<'de>,
}

impl<'de> SeqAccess<'de> for Fields<'_, 'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.reader.spent() {
            true => Ok(None),
            false => seed.deserialize(&mut *self.reader).map(Some),
        }
    }
}

/// A value whose first field is the keyword saying what the rest of it is.
struct Keyword<'a, 'de> {
    reader: &'a mut Reader<'de>,
    /// The keywords the type knows, which the field is matched against.
    variants: &'static [&'static str],
}

impl<'de> EnumAccess<'de> for Keyword<'_, 'de> {
    type Error = Error;
    type Variant = Self;

    /// **A keyword is matched whatever its case.** DALI writes `CIRCLE` and a caller writes
    /// what they typed; the type's own spelling is what the variant is then read as, so the
    /// case of a value never decides whether a request is answered. A keyword the type does
    /// not know is passed on as it was written, for the error to name.
    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self), Error>
    where
        V: DeserializeSeed<'de>,
    {
        let written = self.reader.field()?;
        let known = self
            .variants
            .iter()
            .find(|variant| variant.eq_ignore_ascii_case(written));
        let keyword = match known {
            Some(variant) => seed.deserialize(de::value::StrDeserializer::new(variant))?,
            None => seed.deserialize(de::value::StrDeserializer::new(written))?,
        };
        Ok((keyword, self))
    }
}

impl<'de> VariantAccess<'de> for Keyword<'_, 'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Error> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Error>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(self.reader)
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(Fields {
            reader: self.reader,
        })
    }

    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(Fields {
            reader: self.reader,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// DALI §3.3's own shapes, which is what this format is for.
    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(rename_all = "UPPERCASE")]
    enum Shape {
        Circle(f64, f64, f64),
        Range(f64, f64, f64, f64),
        Polygon(Vec<f64>),
    }

    #[test]
    fn a_shape_is_its_keyword_and_that_many_numbers() {
        let read = |text| value::<Shape>(text, Delimiter::Space);
        assert_eq!(
            read("CIRCLE 12.3 45.6 0.5").unwrap(),
            Shape::Circle(12.3, 45.6, 0.5)
        );
        assert_eq!(
            read("RANGE 12.0 14.0 34.0 36.0").unwrap(),
            Shape::Range(12.0, 14.0, 34.0, 36.0)
        );
        assert_eq!(
            read("POLYGON 10.0 10.0 10.2 10.0 10.2 10.2").unwrap(),
            Shape::Polygon(vec![10.0, 10.0, 10.2, 10.0, 10.2, 10.2])
        );
        // Runs of whitespace are one delimiter, a client having written it by hand.
        assert_eq!(
            read("CIRCLE  12.3   45.6 0.5").unwrap(),
            Shape::Circle(12.3, 45.6, 0.5)
        );
    }

    /// Arity is what the keyword promised, so a field too few or too many is a refusal
    /// rather than a shape that means something else.
    #[test]
    fn a_shape_with_the_wrong_number_of_fields_is_refused() {
        let read = |text| {
            value::<Shape>(text, Delimiter::Space)
                .unwrap_err()
                .to_string()
        };
        // Too few, which `serde` says for the type it was reading.
        assert!(
            read("CIRCLE 12.3 45.6").contains("3 elements"),
            "{}",
            read("CIRCLE 12.3 45.6")
        );
        assert!(
            read("CIRCLE 1 2 3 4").contains("more than"),
            "{}",
            read("CIRCLE 1 2 3 4")
        );
        assert!(read("CIRCLE 1 2 north").contains("not a number"));
        assert!(read("BOX 1 2 3").contains("BOX"));
    }

    /// TAP §2.7.6's `UPLOAD`, which is the same grammar with a comma.
    #[test]
    fn an_upload_is_a_name_and_the_rest() {
        let (name, uri): (String, Tail) =
            value("mine,s3://bucket/a,b/hats", Delimiter::Comma).unwrap();
        assert_eq!(name, "mine");
        // The url keeps its own comma: only the first one was structure.
        assert_eq!(uri.as_str(), "s3://bucket/a,b/hats");
    }

    /// A credential carries whatever it carries, and what reaches the store has to be what
    /// was written: a value cut at a delimiter is a request that reads as anonymous.
    #[test]
    fn a_tail_keeps_every_delimiter_in_it() {
        let (upload, option, secret): (String, String, Tail) = value(
            "mine,secret_access_key,wJalr,K7/MDENG+bPxRfiCY==",
            Delimiter::Comma,
        )
        .unwrap();
        assert_eq!(
            (upload.as_str(), option.as_str()),
            ("mine", "secret_access_key")
        );
        assert_eq!(secret.as_str(), "wJalr,K7/MDENG+bPxRfiCY==");
    }

    /// A keyword deciding how many fields follow is the whole point, and a hand-written
    /// `Deserialize` reads a keyword this format does not know by name.
    #[test]
    fn a_keyword_chooses_how_many_fields_follow() {
        #[derive(Debug, Deserialize, PartialEq)]
        #[serde(rename_all = "lowercase")]
        enum Setting {
            Header(String, Tail),
            Endpoint(Tail),
        }

        assert_eq!(
            value::<Setting>("header,Authorization,Bearer a,b c", Delimiter::Comma).unwrap(),
            Setting::Header("Authorization".to_owned(), Tail("Bearer a,b c".to_owned()))
        );
        assert_eq!(
            value::<Setting>("endpoint,https://minio.example.com", Delimiter::Comma).unwrap(),
            Setting::Endpoint(Tail("https://minio.example.com".to_owned()))
        );
    }

    /// An interval is two numbers, and `-Inf` is one of them (DALI §3.3.4).
    #[test]
    fn an_interval_is_two_numbers() {
        let (low, high): (f64, f64) = value("0.5 1.0", Delimiter::Space).unwrap();
        assert_eq!((low, high), (0.5, 1.0));
        let (low, high): (f64, f64) = value("-Inf 0.0", Delimiter::Space).unwrap();
        assert!(low.is_infinite() && low.is_sign_negative());
        assert_eq!(high, 0.0);
    }

    /// A value says nothing about its own shape, so reading it into one that asks is an
    /// error here rather than a guess.
    #[test]
    fn a_shape_the_value_cannot_describe_is_refused() {
        let refused = value::<serde_json::Value>("1 2", Delimiter::Space).unwrap_err();
        assert!(refused.to_string().contains("known shape"), "{refused}");
    }
}
