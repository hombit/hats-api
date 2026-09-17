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
use url::Url;

use crate::adql::names;
use crate::error::ApiError;
use crate::storage::{StorageOptions, parse_url};
use crate::tap::tables::RESERVED_SCHEMAS;

/// The schema an uploaded table is queried under (TAP §2.5).
pub(super) const UPLOAD_SCHEMA: &str = "TAP_UPLOAD";

/// This service's own parameter for one thing a url needs to be opened.
const STORAGE_OPTION: &str = "UPLOAD_STORAGE_OPTION";

/// This service's own parameter for what kind of table a url holds.
const TYPE: &str = "UPLOAD_TYPE";

/// What an option name is prefixed with to set one header rather than a field.
///
/// `headers` is the one option that is a map of its own, so it is the one that cannot be
/// written as a name and a value. `headers.Authorization=Bearer …` is that map, a header at
/// a time.
const HEADER_PREFIX: &str = "headers.";

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

impl Kind {
    fn parse(value: &str) -> Result<Self, ApiError> {
        match value.trim() {
            name if name.eq_ignore_ascii_case("hats") => Ok(Self::Hats),
            name if name.eq_ignore_ascii_case("parquet") => Ok(Self::Parquet),
            other => Err(ApiError::bad_request(format!(
                "UPLOAD_TYPE {other:?} is not a kind of table this service reads; it takes \
                 hats or parquet"
            ))),
        }
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
                named.push(pair(entry, "UPLOAD")?);
            }
        }
        let mut list = Vec::new();
        for (name, uri) in named {
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
            let (name, kind) = pair(value, TYPE)?;
            let kind = Kind::parse(&kind)?;
            find(&mut list, &name, TYPE)?.kind = Some(kind);
        }

        // An entry naming no upload is refused rather than dropped: dropped, it is a request
        // whose credentials were never used, which the caller reads as a store that let them
        // in anonymously.
        let mut declared: BTreeMap<String, serde_json::Map<String, serde_json::Value>> =
            BTreeMap::new();
        for value in storage {
            let (name, setting) = pair(value, STORAGE_OPTION)?;
            let (option, value) = setting.split_once(',').ok_or_else(|| {
                ApiError::bad_request(format!(
                    "{STORAGE_OPTION} is one upload, one option and its value: \
                     {STORAGE_OPTION}=<upload>,<option>,<value>"
                ))
            })?;
            let name = find(&mut list, &name, STORAGE_OPTION)?.name.clone();
            let written = declared.entry(name.clone()).or_default();
            let option = option.trim();
            // Everything past the second comma is the value, commas and all, so a secret is
            // never cut short by its own punctuation.
            let value = value.trim();
            let replaced = match option.strip_prefix(HEADER_PREFIX) {
                Some(header) => {
                    let serde_json::Value::Object(headers) = written
                        .entry("headers")
                        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()))
                    else {
                        unreachable!("headers is written as an object above")
                    };
                    headers.insert(header.trim().to_owned(), value.into())
                }
                None => written.insert(option.to_owned(), typed(value)),
            };
            if replaced.is_some() {
                return Err(ApiError::bad_request(format!(
                    "{STORAGE_OPTION} sets {option} twice for {name}"
                )));
            }
        }
        // Through the same type the `/adql` body deserializes, so that an option means what it
        // means everywhere else — a name for another backend refused, a credential kept out of
        // a `String`, nothing dropped for being unknown.
        for (name, written) in declared {
            let options =
                serde_json::from_value(serde_json::Value::Object(written)).map_err(|error| {
                    ApiError::bad_request(format!(
                        "{STORAGE_OPTION} for {name} is not storage options this service \
                         takes: {error}"
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
fn typed(value: &str) -> serde_json::Value {
    match value.trim() {
        "true" => serde_json::Value::Bool(true),
        "false" => serde_json::Value::Bool(false),
        _ => value.into(),
    }
}

/// `name,rest` into the two, the rest kept whole.
///
/// One comma, which is `UPLOAD`'s own separator: what follows it is a url there, an option
/// and its value here, and in neither case is it this function's to take apart further.
fn pair(value: &str, parameter: &str) -> Result<(String, String), ApiError> {
    let (name, rest) = value.trim().split_once(',').ok_or_else(|| {
        ApiError::bad_request(format!(
            "{parameter} {value:?} does not start with an upload name and a comma"
        ))
    })?;
    Ok((name.trim().to_owned(), rest.trim().to_owned()))
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
                "a,headers.Accept,text/plain, text/csv;q=0.9",
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
        assert!(refused.contains("<option>"), "{refused}");
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

    #[test]
    fn a_value_that_is_not_a_pair_is_refused() {
        let refused = read(&["file:///hats/x"], &[], &[]).unwrap_err();
        assert!(refused.contains("comma"), "{refused}");
    }
}
