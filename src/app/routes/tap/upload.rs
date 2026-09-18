//! `UPLOAD`: the tables a request names by url, queried as `TAP_UPLOAD.<name>`.
//!
//! TAP §2.7.6's parameter is `name,uri`, and uploads accumulate over repeated parameters. A
//! uri that is an `http(s)` url is the standard's own *referenced* upload, `param:` its
//! inline one.
//!
//! **What this service reads at that url is a HATS catalog or a parquet file, never the
//! VOTable the standard means.** So a client may write the parameter and nothing else about
//! it is borrowed, which is why `/capabilities` declares no `uploadMethod`: declaring one
//! tells a client it may send a VOTable, and this refuses that.
//!
//! Two parameters of this service's own go with it, both named in the README because no
//! capabilities document can describe them, and both written `<upload>,…` the way `UPLOAD`
//! is. Nothing in TAP or DALI keys a value by anything but that one comma, so neither
//! borrows a shape that does not exist: they repeat instead, which is DALI §3.2's own way of
//! saying several.
//!
//! - `UPLOAD_STORAGE_OPTION=<upload>,<option>,<value>`, one option to a value and the
//!   parameter repeated for more. The options are the `/adql` body's `storage`. **Everything
//!   past the second comma is the value**, so a secret carrying a comma, a space, an `=` or a
//!   `;` arrives whole — which is the failure a separator inside the value would cause, and a
//!   truncated credential is a request that reads as anonymous.
//!
//!   **The option's name says how many fields follow it**, the way DALI reads a shape —
//!   `CIRCLE` takes three numbers and `RANGE` four. `header` is the one option that is a map
//!   rather than a value, so it takes a header name and then the value:
//!   `<upload>,header,Authorization,Bearer …`.
//!
//!   It is read on `GET` as on `POST`: TAP gives the two carriers one syntax, and a credential
//!   written into a url has already been sent by the time this service could refuse to read
//!   it. What is left to protect is that it goes no further, which is the log recording a path
//!   and never a query string.
//! - `UPLOAD_TYPE=<upload>,<kind>`, saying which of the two a url is. Absent, [`Upload::kind`]
//!   is `None` and the url is judged by its own name.
//!
//! TAP's own answer to an upload url that needs authentication is credential delegation, a
//! service of its own that hands this one the caller's certificate. These two parameters are
//! this service's answer instead, and are not that.

use std::collections::BTreeMap;
use std::fmt;

use serde::Deserialize;
use serde::de::{self, EnumAccess, SeqAccess, VariantAccess, Visitor};
use url::Url;

use crate::adql::names;
use crate::error::ApiError;
use crate::storage::{self, StorageOptions, parse_url};
use crate::tap::dali::{self, Delimiter, Tail};
use crate::tap::tables::RESERVED_SCHEMAS;

/// The schema an uploaded table is queried under (TAP §2.5).
pub(super) const UPLOAD_SCHEMA: &str = "TAP_UPLOAD";

/// This service's own parameter for one thing a url needs to be opened.
const STORAGE_OPTION: &str = "UPLOAD_STORAGE_OPTION";

/// This service's own parameter for what kind of table a url holds.
const TYPE: &str = "UPLOAD_TYPE";

/// The one option that is a map rather than a value, and so takes a name before its value.
///
/// `<upload>,header,Authorization,Bearer …` sets one of them. What decides how many fields
/// follow is the option's own name, which is how DALI reads a shape: `CIRCLE` takes three
/// numbers and `RANGE` four, the leading word saying which.
const HEADER: &str = "header";

/// What a request uploaded, in the order the parameters named them.
#[derive(Debug, Default)]
pub(super) struct Uploads(Vec<Upload>);

/// One uploaded table.
#[derive(Debug)]
pub(super) struct Upload {
    /// The name it answers to under `TAP_UPLOAD`, as the request spelled it.
    pub name: String,
    /// Where it is. The caller's own url, which is theirs to have written and so may be
    /// named back to them in a refusal.
    pub url: Url,
    /// What the url holds, where `UPLOAD_TYPE` said; otherwise the url is judged by its name.
    pub kind: Option<Kind>,
    /// How to reach the store.
    pub storage: StorageOptions,
}

/// The two things an upload's url may name, which are the `/adql` body's two table types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum Kind {
    /// A whole HATS catalog: the directory holding `hats.properties`.
    Hats,
    /// One parquet file.
    Parquet,
}

/// One setting of one upload's storage options, which its own name decides the shape of.
///
/// The keyword-decides-arity rule DALI writes its shapes with: `header` names a header and
/// then its value, and every other option is a name and the value itself. Both end in a
/// [`Tail`], so whatever punctuation a credential carries arrives with it.
#[derive(Debug)]
enum Setting {
    Header { header: String, value: Tail },
    Named { option: String, value: Tail },
}

impl<'de> Deserialize<'de> for Setting {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ByName;

        impl<'de> Visitor<'de> for ByName {
            type Value = Setting;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(
                    f,
                    "an option and its value, or {HEADER} and a header's name and value"
                )
            }

            /// The option's name is read first, and it says what follows. A name this
            /// service has no field for is carried anyway: `StorageOptions` refuses it where
            /// it knows the backend, and dropping it here would leave a request that reads
            /// as anonymous.
            fn visit_enum<A>(self, data: A) -> Result<Setting, A::Error>
            where
                A: EnumAccess<'de>,
            {
                let (option, rest): (String, _) = data.variant()?;
                match option.eq_ignore_ascii_case(HEADER) {
                    true => {
                        let (header, value) = rest.tuple_variant(2, TwoFields)?;
                        Ok(Setting::Header { header, value })
                    }
                    false => Ok(Setting::Named {
                        option: option.to_ascii_lowercase(),
                        value: rest.newtype_variant()?,
                    }),
                }
            }
        }

        deserializer.deserialize_enum("Setting", &[HEADER], ByName)
    }
}

/// One upload's storage options as the request wrote them: names, and text or headers.
///
/// It deserializes into [`StorageOptions`] itself, so what an option means here is what it
/// means in the `/adql` body — a name for another backend refused, a credential kept out of a
/// `String`, an unknown name carried rather than dropped — and this holds no list of options
/// of its own to fall out of step with that one.
#[derive(Debug, Default)]
struct Declared(Vec<(String, Field)>);

/// One option's value, as the type its field takes.
///
/// A query string carries text and nothing else, so what a value's type is has to come from
/// the option: [`storage::is_flag`] answers that from the same registry the rest of the
/// storage code reads, rather than from what the text happens to look like. Guessing instead
/// makes `region,true` a boolean and then a refusal about a string field.
#[derive(Debug)]
enum Field {
    Text(String),
    Flag(bool),
    Headers(BTreeMap<String, String>),
}

impl Declared {
    /// Add one setting, or name the option that was set twice.
    fn add(&mut self, setting: Setting) -> Result<(), String> {
        let (option, field) = match setting {
            Setting::Header { header, value } => {
                let written = format!("{HEADER} {header}");
                match self.0.iter_mut().find(|(name, _)| name == storage::HEADERS) {
                    Some((_, Field::Headers(headers))) => {
                        match headers.insert(header, value.into_string()) {
                            Some(_) => return Err(written),
                            None => return Ok(()),
                        }
                    }
                    _ => (
                        storage::HEADERS.to_owned(),
                        Field::Headers(BTreeMap::from([(header, value.into_string())])),
                    ),
                }
            }
            Setting::Named { option, value } => {
                let field = match storage::is_flag(&option) {
                    true => Field::Flag(flag(&value)?),
                    false => Field::Text(value.into_string()),
                };
                (option, field)
            }
        };
        if self.0.iter().any(|(name, _)| *name == option) {
            return Err(option);
        }
        self.0.push((option, field));
        Ok(())
    }

    /// The options themselves, read by `StorageOptions`'s own `Deserialize`.
    fn options(self) -> Result<StorageOptions, de::value::Error> {
        StorageOptions::deserialize(de::value::MapDeserializer::new(self.0.into_iter()))
    }
}

/// A switch's value, which is the one thing a query string cannot say by its type.
fn flag(value: &Tail) -> Result<bool, String> {
    match value.as_str() {
        text if text.eq_ignore_ascii_case("true") => Ok(true),
        text if text.eq_ignore_ascii_case("false") => Ok(false),
        other => Err(format!(
            "{other:?}, which is a switch and takes true or false"
        )),
    }
}

impl<'de> de::IntoDeserializer<'de, de::value::Error> for Field {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self {
        self
    }
}

impl<'de> serde::Deserializer<'de> for Field {
    type Error = de::value::Error;

    /// Every field knows what it is, so the one method is enough: `StorageOptions` flattens
    /// its groups, and a flattened struct reads its values through this.
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self {
            Self::Text(text) => visitor.visit_string(text),
            Self::Flag(flag) => visitor.visit_bool(flag),
            Self::Headers(headers) => {
                visitor.visit_map(de::value::MapDeserializer::new(headers.into_iter()))
            }
        }
    }

    /// An option that was written is a value: the field is optional in the struct, not in
    /// the request.
    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_some(self)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
        unit unit_struct newtype_struct seq tuple tuple_struct map struct enum
        identifier ignored_any
    }
}

/// A header's name and then its value, which is what `header` takes.
struct TwoFields;

impl<'de> Visitor<'de> for TwoFields {
    type Value = (String, Tail);

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a header's name and its value")
    }

    fn visit_seq<A>(self, mut fields: A) -> Result<(String, Tail), A::Error>
    where
        A: SeqAccess<'de>,
    {
        let header = fields
            .next_element::<String>()?
            .ok_or_else(|| de::Error::custom(format!("{HEADER} takes a name and then a value")))?;
        let value = fields
            .next_element::<Tail>()?
            .ok_or_else(|| de::Error::custom(format!("{HEADER} {header} takes a value")))?;
        Ok((header, value))
    }
}

impl Uploads {
    /// Read the three parameters into one list.
    ///
    /// Each is every value it was given: TAP §2.7.6 has uploads accumulate over repeated
    /// `UPLOAD` parameters, which is DALI §3.2's rule for several values of anything.
    pub fn read(uploads: &[&str], types: &[&str], storage: &[&str]) -> Result<Self, ApiError> {
        let mut named = Vec::new();
        for value in uploads {
            // TAP 1.0 §2.5.1 joined pairs with `;` and 1.1 dropped it for repetition. Both
            // are read, a client having no way to know which this service grew up on.
            for entry in value.split(';').filter(|entry| !entry.trim().is_empty()) {
                named.push(read_value::<(String, Tail)>(
                    entry,
                    "UPLOAD",
                    "<name>,<url>",
                )?);
            }
        }
        let mut list = Vec::new();
        for (name, uri) in named {
            let uri = uri.into_string();
            check_name(&name)?;
            if list
                .iter()
                .any(|upload: &Upload| upload.name.eq_ignore_ascii_case(&name))
            {
                return Err(ApiError::bad_request(format!(
                    "UPLOAD names {name} twice, and an ADQL name matches either way; give each \
                     uploaded table its own name"
                )));
            }
            list.push(Upload {
                name,
                url: url_of(&uri)?,
                kind: None,
                storage: StorageOptions::default(),
            });
        }

        for value in types {
            let (name, kind) =
                read_value::<(String, Kind)>(value, TYPE, "<upload>,hats or <upload>,parquet")?;
            find(&mut list, &name, TYPE)?.kind = Some(kind);
        }

        // An entry naming no upload is refused rather than dropped: dropped, it is a request
        // whose credentials were never used, which the caller reads as a store that let them
        // in anonymously.
        let mut declared: BTreeMap<String, Declared> = BTreeMap::new();
        for value in storage {
            let (name, setting) = read_value::<(String, Setting)>(
                value,
                STORAGE_OPTION,
                "<upload>,<option>,<value>",
            )?;
            let name = find(&mut list, &name, STORAGE_OPTION)?.name.clone();
            declared
                .entry(name.clone())
                .or_default()
                .add(setting)
                .map_err(|option| {
                    ApiError::bad_request(format!(
                        "{STORAGE_OPTION} sets {option} twice for {name}"
                    ))
                })?;
        }
        // Through the same type the `/adql` body deserializes, so that an option means what it
        // means everywhere else — a name for another backend refused, a credential kept out of
        // a `String`, nothing dropped for being unknown.
        for (name, written) in declared {
            let options = written.options().map_err(|error| {
                ApiError::bad_request(format!(
                    "{STORAGE_OPTION} for {name} is not storage options this service takes: \
                     {error}"
                ))
            })?;
            find(&mut list, &name, STORAGE_OPTION)?.storage = options;
        }
        Ok(Self(list))
    }

    /// The upload a statement's table name refers to, if the name is one of `TAP_UPLOAD`'s.
    ///
    /// The schema and the name are both matched the way ADQL matches an unquoted name, which
    /// is how every other table name on this route is matched.
    pub fn named(&self, spelling: &str) -> Option<&Upload> {
        let (schema, table) = spelling.split_once('.')?;
        if !schema.eq_ignore_ascii_case(UPLOAD_SCHEMA) {
            return None;
        }
        self.0
            .iter()
            .find(|upload| upload.name.eq_ignore_ascii_case(table))
    }

    /// Whether a statement naming `TAP_UPLOAD` has anything to name, for the refusal.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The names given, for a refusal that says what the statement could have named.
    pub fn names(&self) -> Vec<&str> {
        self.0.iter().map(|upload| upload.name.as_str()).collect()
    }
}

/// An option's value as the type its field holds.
///
/// Every storage option is a string but `allow_http`, which is a switch — and a switch
/// written `true` is not a string that happens to spell one. Anything else stays text, so a
/// region that looks like a number is still a region.
/// One parameter's value, read into the shape that parameter takes.
///
/// The comma is `UPLOAD`'s own separator (TAP §2.7.6), so every parameter here is written
/// the way it is; what each one's fields are is the type's to say. `shape` is how the
/// parameter is written, since what a reader of the refusal needs is the form and not the
/// arity a deserializer counted.
fn read_value<'de, T>(value: &'de str, parameter: &str, shape: &str) -> Result<T, ApiError>
where
    T: Deserialize<'de>,
{
    dali::value(value, Delimiter::Comma).map_err(|error| {
        ApiError::bad_request(format!(
            "{parameter} is written {parameter}={shape}: {error}"
        ))
    })
}

/// The upload a parameter's entry names, or a refusal saying which names there are.
fn find<'a>(
    list: &'a mut [Upload],
    name: &str,
    parameter: &str,
) -> Result<&'a mut Upload, ApiError> {
    let names = list
        .iter()
        .map(|upload| upload.name.clone())
        .collect::<Vec<_>>();
    list.iter_mut()
        .find(|upload| upload.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "{parameter} names {name}, which no UPLOAD declares; this request uploads {}",
                match names.is_empty() {
                    true => "nothing".to_owned(),
                    false => names.join(", "),
                }
            ))
        })
}

/// An upload's name, which a statement has to be able to write.
fn check_name(name: &str) -> Result<(), ApiError> {
    if !names::is_plain(name) {
        return Err(ApiError::bad_request(format!(
            "UPLOAD name {name:?} is not one an ADQL query can write unquoted; it starts with \
             a letter and carries letters, digits and underscores"
        )));
    }
    // `TAP_UPLOAD.TAP_SCHEMA` is a name a statement can write, and one nobody means to.
    if RESERVED_SCHEMAS
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(name))
    {
        return Err(ApiError::bad_request(format!(
            "UPLOAD name {name:?} is a schema this service reserves"
        )));
    }
    Ok(())
}

/// The url an upload's uri names.
///
/// `param:` is TAP's inline upload, which this service does not implement, and it is refused
/// by what it is rather than as a url that will not parse.
fn url_of(uri: &str) -> Result<Url, ApiError> {
    if uri.starts_with("param:") {
        return Err(ApiError::bad_request(format!(
            "UPLOAD {uri} is an inline table, which this service does not implement; name a \
             catalog or a parquet file by url instead"
        )));
    }
    parse_url(uri)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(uploads: &[&str], types: &[&str], storage: &[&str]) -> Result<Uploads, String> {
        Uploads::read(uploads, types, storage).map_err(|error| error.to_string())
    }

    /// TAP §2.5.2: several pairs to a value, and the parameter repeatable.
    #[test]
    fn several_uploads_arrive_in_one_value_or_in_several() {
        let uploads = read(
            &[
                "a,s3://bucket/a/hats;b,https://example.org/b.parquet",
                "c,file:///hats/c",
            ],
            &[],
            &[],
        )
        .unwrap();
        assert_eq!(uploads.names(), ["a", "b", "c"]);
        assert_eq!(
            uploads.named("TAP_UPLOAD.b").unwrap().url.as_str(),
            "https://example.org/b.parquet"
        );
        // The schema and the name are matched the way ADQL matches an unquoted name.
        assert!(uploads.named("tap_upload.C").is_some());
        // And a name under any other schema is not an upload at all.
        assert!(uploads.named("gaia_dr3.c").is_none());
        assert!(uploads.named("c").is_none());
    }

    /// One option to a value and the parameter repeated for more, which is DALI §3.2's rule
    /// for several values of anything.
    #[test]
    fn a_type_and_storage_options_reach_the_upload_they_name() {
        let uploads = read(
            &["a,s3://bucket/a/hats;b,s3://bucket/b.parquet"],
            &["b,parquet"],
            &[
                "a,endpoint,https://minio.example.com",
                "a,region,us-east-1",
                "a,allow_http,false",
            ],
        )
        .unwrap();
        let a = uploads.named("TAP_UPLOAD.a").unwrap();
        assert_eq!(a.kind, None);
        assert_eq!(
            a.storage.endpoint.as_deref(),
            Some("https://minio.example.com")
        );
        assert_eq!(
            uploads.named("TAP_UPLOAD.b").unwrap().kind,
            Some(Kind::Parquet)
        );
        assert!(!a.storage.allow_http, "{:?}", a.storage);
    }

    /// **What an option's value means is the option's to say, never the text's.** A query
    /// string carries text and `allow_http` is the one switch among them, so the type comes
    /// from `storage::is_flag` — read from the value instead, a region spelled `true` would
    /// be a boolean handed to a string field, and a switch spelled `yes` would be silently
    /// false.
    #[test]
    fn a_values_type_comes_from_the_option_it_sets() {
        let uploads = read(
            &["a,s3://bucket/a/hats"],
            &[],
            &["a,region,true", "a,allow_http,TRUE"],
        )
        .unwrap();
        let storage = &uploads.named("TAP_UPLOAD.a").unwrap().storage;
        assert_eq!(storage.s3.region.as_deref(), Some("true"));
        assert!(storage.allow_http, "{storage:?}");

        let refused = read(&["a,s3://bucket/a/hats"], &[], &["a,allow_http,yes"]).unwrap_err();
        assert!(refused.contains("true or false"), "{refused}");
    }

    /// Dropped instead, it is a request whose credentials went unused, which the caller
    /// cannot tell from a store that let them in anonymously.
    #[test]
    fn a_parameter_naming_no_upload_is_refused() {
        let refused = read(&["a,s3://bucket/a/hats"], &[], &["b,region,us-east-1"]).unwrap_err();
        assert!(refused.contains("UPLOAD_STORAGE_OPTION"), "{refused}");
        assert!(refused.contains('b') && refused.contains('a'), "{refused}");

        let refused = read(&["a,s3://bucket/a/hats"], &["b,hats"], &[]).unwrap_err();
        assert!(refused.contains("UPLOAD_TYPE"), "{refused}");
    }

    /// Everything past the second comma is the value: a secret is base64 and ends in `=`, a
    /// header value carries commas and spaces, and a truncated credential is a request that
    /// reads as anonymous.
    #[test]
    fn an_option_value_keeps_its_commas_and_equals_signs() {
        let uploads = read(
            &["a,s3://bucket/a/hats"],
            &[],
            &[
                "a,secret_access_key,wJalr/K7MDENG+bPxRfiCY==",
                "a,header,Accept,text/plain, text/csv;q=0.9",
            ],
        )
        .unwrap();
        let storage = &uploads.named("TAP_UPLOAD.a").unwrap().storage;
        // The value itself never leaves a `SecretString`, so what is asked is that the
        // options carry a credential at all and that the header survived beside it.
        assert!(storage.has_credentials(), "{storage:?}");
        assert!(format!("{storage:?}").contains("headers"), "{storage:?}");
    }

    /// A name this service does not take is carried rather than dropped, and refused where
    /// every other route's options are: opening the store, which is what knows the backend.
    /// Dropped here, `secret_acces_key` would be an anonymous request the caller reads as an
    /// authenticated one.
    #[test]
    fn an_option_nobody_takes_is_carried_to_the_store() {
        let uploads = read(&["a,s3://bucket/a/hats"], &[], &["a,secret_acces_key,oops"]).unwrap();
        let storage = &uploads.named("TAP_UPLOAD.a").unwrap().storage;
        assert!(
            storage.unknown.contains_key("secret_acces_key"),
            "{storage:?}"
        );

        // An entry naming an option and no value at all has nothing to carry.
        let refused = read(&["a,s3://bucket/a/hats"], &[], &["a,region"]).unwrap_err();
        assert!(refused.contains("UPLOAD_STORAGE_OPTION"), "{refused}");
    }

    /// The inline form is the standard's, so it says what it is rather than failing as a
    /// url nobody can parse.
    #[test]
    fn an_inline_upload_says_it_is_not_implemented() {
        let refused = read(&["t,param:doc"], &[], &[]).unwrap_err();
        assert!(refused.contains("inline"), "{refused}");
        assert!(refused.contains("url"), "{refused}");
    }

    #[test]
    fn a_name_a_statement_cannot_write_is_refused() {
        for name in ["9lives", "with space", "a-b", "", "TAP_SCHEMA"] {
            let refused = read(&[&format!("{name},file:///hats/x")], &[], &[]).unwrap_err();
            assert!(refused.contains("UPLOAD"), "{name}: {refused}");
        }
    }

    /// Two uploads under one name are two answers to one table reference.
    #[test]
    fn one_name_twice_is_refused() {
        let refused = read(&["a,file:///hats/x;A,file:///hats/y"], &[], &[]).unwrap_err();
        assert!(refused.contains("twice"), "{refused}");
    }

    /// What a caller needs from the refusal is how the parameter is written, so every one
    /// of them says its own shape.
    #[test]
    fn a_value_that_is_not_a_pair_is_refused() {
        let refused = read(&["file:///hats/x"], &[], &[]).unwrap_err();
        assert!(refused.contains("UPLOAD=<name>,<url>"), "{refused}");

        let refused = read(&["a,file:///hats/x"], &["a"], &[]).unwrap_err();
        assert!(refused.contains("<upload>,hats"), "{refused}");

        let refused = read(&["a,file:///hats/x"], &[], &["a,region"]).unwrap_err();
        assert!(refused.contains("<upload>,<option>,<value>"), "{refused}");
    }
}
