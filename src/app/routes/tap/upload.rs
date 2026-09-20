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

use serde::Deserialize;
use serde::de::{self, Visitor};
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
/// numbers and `RANGE` four, the leading word saying which. [`Setting`] is what reads it;
/// this is the word a refusal names, a request having written it rather than `headers`.
const HEADER: &str = "header";

/// What a request uploaded, in the order the parameters named them.
#[derive(Debug, Default)]
pub(super) struct Uploads(Vec<Upload>);

/// One uploaded table.
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

/// `Url`'s own `Debug` prints its `password` field, and this url is the caller's.
///
/// `refuse_userinfo` is what rejects `s3://key:secret@bucket/object`, and it runs where the
/// url is opened rather than where it is read — so between the two an `Upload` holds one
/// that a derive would print in full. [`storage::file_url`] is the same stripping every
/// message about an unopened url already goes through.
impl std::fmt::Debug for Upload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Upload")
            .field("name", &self.name)
            .field("url", &storage::file_url(&self.url).as_str())
            .field("kind", &self.kind)
            .field("storage", &self.storage)
            .finish()
    }
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
/// then its value, and every other option is a name and the value itself. The option names
/// are an open set, so [`dali::OTHER`] is what carries one this service has no field for —
/// carried rather than dropped, since `StorageOptions` refuses it where it knows the backend
/// and dropping it would leave a request that reads as anonymous. Both variants end in a
/// [`Tail`], so whatever punctuation a credential carries arrives with it.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Setting {
    Header {
        header: String,
        value: Tail,
    },
    // `dali::OTHER`, which an attribute cannot be written with;
    // `the_catch_all_is_the_one_dali_declares` is what holds the two spellings together.
    #[serde(rename = "$dali::other")]
    Named {
        option: String,
        value: Tail,
    },
}

/// One upload's storage options as the request wrote them: names, and text or headers.
///
/// It deserializes into [`StorageOptions`] itself, so what an option means here is what it
/// means in the `/adql` body — a name for another backend refused, a credential kept out of a
/// `String`, an unknown name carried rather than dropped — and this holds no list of options
/// of its own to fall out of step with that one.
#[derive(Debug, Default)]
struct Declared(Vec<(String, Field)>);

/// One option's value, as the request wrote it.
///
/// A query string carries text and nothing else, and which type an option takes is
/// `StorageOptions`'s own field to say: `serde` reads a struct's own field with that field's
/// type even where the struct has a flattened group, so the field asks for what it holds and
/// this answers from the text. Deciding from what the text looks like instead is what would
/// make `region,true` a boolean and then a refusal about a string field.
#[derive(Debug)]
enum Field {
    Text(String),
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
            Setting::Named { option, value } => (
                option.to_ascii_lowercase(),
                Field::Text(value.into_string()),
            ),
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

impl<'de> de::IntoDeserializer<'de, de::value::Error> for Field {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self {
        self
    }
}

impl<'de> serde::Deserializer<'de> for Field {
    type Error = de::value::Error;

    /// What a backend's own group holds, which is text or the headers map: a flattened group
    /// is buffered before its struct sees it, so its fields ask the value what it is rather
    /// than what they take.
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self {
            Self::Text(text) => visitor.visit_string(text),
            Self::Headers(headers) => {
                visitor.visit_map(de::value::MapDeserializer::new(headers.into_iter()))
            }
        }
    }

    /// A switch, which is `allow_http` and nothing else today.
    ///
    /// Reached because a field of `StorageOptions` itself is read as the type it holds — only
    /// the keys that belong to a flattened group are buffered — so the text is parsed for the
    /// field that says it wants a bool, and no list here says which option that is. An option
    /// added to a *group* as a switch would not reach this, which is what
    /// `every_storage_option_is_set_from_text` is for.
    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self {
            Self::Text(text) if text.eq_ignore_ascii_case("true") => visitor.visit_bool(true),
            Self::Text(text) if text.eq_ignore_ascii_case("false") => visitor.visit_bool(false),
            Self::Text(text) => Err(de::Error::custom(format!(
                "{text:?}, which is a switch and takes true or false"
            ))),
            headers => serde::Deserializer::deserialize_any(headers, visitor),
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
        i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
        unit unit_struct newtype_struct seq tuple tuple_struct map struct enum
        identifier ignored_any
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

    /// A url carrying userinfo is refused where it is opened, which is after it is read —
    /// so in between an `Upload` holds one, and printing it must not print the password.
    ///
    /// The window is what makes this worth a test rather than a derive: nothing between
    /// `Uploads::read` and `storage::open` has established that the url is clean, and a
    /// `Debug` reached from a log line, a panic or a job document would print whatever the
    /// caller wrote.
    #[test]
    fn a_debug_of_an_upload_does_not_print_the_url_s_password() {
        let uploads = read(
            &["a,https://reader:hunter2@example.org/a.parquet"],
            &[],
            &[],
        )
        .unwrap();
        let shown = format!("{uploads:?}");
        assert!(!shown.contains("hunter2"), "{shown}");
        // The name of the thing is still there, or the Debug has stopped being useful.
        assert!(shown.contains("example.org/a.parquet"), "{shown}");
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
    /// from the field `StorageOptions` declares — read from the value instead, a region
    /// spelled `true` would be a boolean handed to a string field, and a switch spelled `yes`
    /// would be silently false.
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

    /// Every option a body can set is reachable from a query string's text.
    ///
    /// Driven from the registry, so an option added is covered by having been registered, and
    /// one whose field is not a string fails here rather than in a request: text handed to a
    /// field inside a backend's group is buffered before that group sees it, so only a field
    /// of `StorageOptions` itself is read as the type it holds. A switch added to a group
    /// would need what `Field::deserialize_bool` does, and this is what says so.
    #[test]
    fn every_storage_option_is_set_from_text() {
        for name in storage::option_names() {
            let written = match name {
                // A map rather than a value, so it is written with its own keyword, and
                // `an_option_value_keeps_its_commas_and_equals_signs` is where it is read.
                storage::HEADERS => continue,
                "allow_http" => "true",
                "transport" => "https",
                _ => "text",
            };
            let read = read(
                &["a,s3://bucket/a/hats"],
                &[],
                &[&format!("a,{name},{written}")],
            );
            assert!(read.is_ok(), "{name}: {}", read.unwrap_err());
        }
    }

    /// The catch-all variant's name is `dali`'s, an attribute having nowhere to write a const.
    #[test]
    fn the_catch_all_is_the_one_dali_declares() {
        assert_eq!(dali::OTHER, "$dali::other");
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
