//! The tables an operator publishes, read from `[[tap.table]]` at startup.
//!
//! A TAP query names a table the service already knows: there is nowhere in a TAP request
//! to put a url, so the list is the whole of what a statement may read. Every entry is a
//! HATS catalog — a name and a path and nothing else.
//!
//! **The path is a path under a `[[mount]]`**, which is the address the file server
//! publishes that catalog at and the address an API request names it by. So a table is
//! published at a url space this file does not invent, and whatever it takes to reach the
//! catalog — an endpoint, a region, a credential — was written once on the mount rather
//! than again here. That is what keeps an operator's secret out of the section describing
//! a surface whose answers are public, without keeping a catalog that needs one
//! unpublishable.
//!
//! **Checked at startup, so an operator hears about a mistake before a caller does.** The
//! name has to be one a query can write, and the path goes through the access policy like
//! any other, which is what catches a mount that does not exist.

use std::sync::Arc;

use url::Url;

use crate::access::AccessPolicy;
use crate::access::mount;
use crate::adql::names;
use crate::config::{ConfigError, TapTableConfig};
use crate::storage::materialize::Transfers;
use crate::storage::{self, StorageOptions};

/// The schemas a query may not be given a table in, because TAP defines them itself.
///
/// `TAP_SCHEMA` is the service's own metadata and `TAP_UPLOAD` is where an uploaded table
/// would land. An operator publishing into either would shadow a name every client already
/// knows the meaning of.
pub const RESERVED_SCHEMAS: [&str; 2] = ["TAP_SCHEMA", "TAP_UPLOAD"];

/// One query this service offers against a published table.
///
/// The operator's, where they wrote any; otherwise one generated from what the catalog says
/// about itself. Either way it is a query a client puts in front of a user and a user then
/// sends back, so the only thing this type carries is what the `/examples` document needs:
/// a label and the statement itself.
#[derive(Debug, Clone)]
pub struct TapExample {
    /// What a client's menu shows.
    pub name: String,
    /// The statement, as a client would send it.
    pub query: String,
}

/// One published table: the name a query writes, and the catalog behind it.
#[derive(Debug)]
pub struct TapTable {
    /// The schema half of the name, as the operator wrote it.
    schema: String,
    /// The table half.
    table: String,
    /// The two joined, which is what a `FROM` says and what `TAP_SCHEMA.tables` publishes.
    qualified: String,
    /// Where the catalog is, as the `file://` url its mount path spells. The store is
    /// built per request rather than held: nothing here is a registry of what a catalog
    /// turned out to contain, and a builder costs no connection. It is also what makes the
    /// mount's own options reach this resource without being repeated — the url resolves
    /// through the mounts like any other.
    url: Url,
    /// What `[[tap.table.example]]` said, and empty where it said nothing. Empty is what
    /// makes `/examples` generate one instead, so this is the whole of the override.
    examples: Vec<TapExample>,
}

impl TapTable {
    pub fn schema(&self) -> &str {
        &self.schema
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    /// `schema.table`, which is the name a query writes.
    pub fn qualified(&self) -> &str {
        &self.qualified
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    /// The queries the operator wrote for this table, and nothing where they wrote none.
    pub fn examples(&self) -> &[TapExample] {
        &self.examples
    }
}

/// Every table this service publishes, in the order the config declared them.
///
/// Spelled out rather than `TapTables`, a type and its collection differing by one letter
/// being a pair that reads alike at a use site.
#[derive(Debug, Default)]
pub struct TapTableList(Vec<TapTable>);

impl TapTableList {
    /// Read the list, refusing anything a caller could not go on to query.
    ///
    /// The path is opened and the handle dropped: what that buys is the policy's answer —
    /// a path under no mount, above all — at startup rather than on somebody's query. A
    /// mount on this machine is checked for being there as well, the store being rooted at
    /// it; a store-backed one is not, nothing here making a request. Either way no catalog
    /// is *read*, so one that is malformed is discovered on the first query against it,
    /// the same as everywhere else here.
    pub fn new(
        configured: &[TapTableConfig],
        policy: &AccessPolicy,
        transfers: &Arc<Transfers>,
    ) -> Result<Self, ConfigError> {
        let mut tables: Vec<TapTable> = Vec::with_capacity(configured.len());
        for entry in configured {
            let refuse = |reason: String| ConfigError::Tap(entry.name.clone(), reason);
            let (schema, table) = split(&entry.name).map_err(&refuse)?;
            if let Some(reserved) = RESERVED_SCHEMAS
                .iter()
                .find(|reserved| reserved.eq_ignore_ascii_case(schema))
            {
                return Err(refuse(format!(
                    "{reserved} is TAP's own schema and a table may not be published in it"
                )));
            }
            // Case-insensitively, although a name is matched exactly: an unquoted
            // identifier is case-insensitive in ADQL, so two names that differ only in
            // case are two tables a client has no way to write one of.
            if let Some(clash) = tables
                .iter()
                .find(|published| published.qualified.eq_ignore_ascii_case(&entry.name))
            {
                return Err(refuse(format!(
                    "{} is already published, and two names differing only in case are \
                     two tables a query cannot tell apart",
                    clash.qualified
                )));
            }
            let url = mounted_url(&entry.path).map_err(&refuse)?;
            // No options: the mount the path lands in carries whatever reaching it takes,
            // and a second set written here would be a second answer to one question.
            storage::open_dir(&url, &StorageOptions::default(), policy, transfers)
                .map_err(|error| refuse(error.to_string()))?;
            tables.push(TapTable {
                schema: schema.to_owned(),
                table: table.to_owned(),
                qualified: entry.name.clone(),
                url,
                examples: examples(&entry.examples).map_err(&refuse)?,
            });
        }
        Ok(Self(tables))
    }

    /// The table a statement's `FROM` named, or nothing.
    ///
    /// **Case-insensitively, which is ADQL's rule for a name written without quotes**
    /// (§2.1.3) — and every published name is written without quotes, this list refusing at
    /// startup any name that would need them. So the spelling `TAP_SCHEMA` publishes works,
    /// and so does the one a reader typed out of a paper.
    ///
    /// Two published names differing only in case are refused where they are configured,
    /// which is what leaves this with one answer or none.
    pub fn lookup(&self, name: &str) -> Option<&TapTable> {
        self.0
            .iter()
            .find(|table| table.qualified.eq_ignore_ascii_case(name))
    }

    pub fn iter(&self) -> impl Iterator<Item = &TapTable> {
        self.0.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The published names, for a refusal to list and for the startup log.
    pub fn names(&self) -> Vec<&str> {
        self.0.iter().map(TapTable::qualified).collect()
    }
}

/// The operator's own examples for one table, checked for the two ways they are useless.
///
/// Whether the query *runs* is not checked and cannot usefully be: it would mean planning a
/// statement against a catalog at startup, which is the read this service does per request
/// and declines to do per process. What checks that is the conformance suite, which fetches
/// the published document and sends every query in it.
fn examples(configured: &[crate::config::TapExampleConfig]) -> Result<Vec<TapExample>, String> {
    configured
        .iter()
        .map(|example| {
            let name = example.name.trim();
            let query = example.query.trim();
            // A menu entry with no label, and a label that does nothing. Neither is
            // something a client can render into anything a user could act on.
            if name.is_empty() || query.is_empty() {
                return Err(format!(
                    "an example needs a name and a query, and {:?} has {}",
                    example.name,
                    if name.is_empty() {
                        "no name"
                    } else {
                        "no query"
                    }
                ));
            }
            Ok(TapExample {
                name: name.to_owned(),
                query: query.to_owned(),
            })
        })
        .collect()
}

/// The url a path in this service's own url space is named by, which is the same thing an
/// API request writes for the same catalog.
///
/// Normalized through [`mount::normalize_prefix`], so that `/hats/gaia` and `/hats/gaia/`
/// are the one address they are, and so that a path that is not one — relative, or
/// carrying a `..` — is refused here rather than resolved into some other catalog.
fn mounted_url(path: &str) -> Result<Url, String> {
    let path = mount::normalize_prefix(path)?;
    // Built a segment at a time rather than by formatting, which is what percent-encodes a
    // name: a catalog directory may hold a character a url reserves.
    let mut url = Url::parse("file:///").map_err(|error| error.to_string())?;
    url.path_segments_mut()
        .map_err(|()| "file:// cannot hold a path".to_owned())?
        // `file:///` already has one empty segment, and pushing onto it would double the
        // separator.
        .pop_if_empty()
        .extend(path.split('/').filter(|segment| !segment.is_empty()));
    Ok(url)
}

/// `schema.table`, both halves being names a query can write without quoting them.
///
/// The schema is required because every published table sits in one: `TAP_SCHEMA.schemas`
/// has a row per schema and `TAP_SCHEMA.tables` says which one each table is in, so a bare
/// name would need a schema invented for it here.
///
/// Quoting is ruled out rather than handled. TAP §4.3 has a published name carry its own
/// quotes where it needs them, and a delimited identifier then matches case-sensitively —
/// so a name needing quotes would work only for the client that copied it verbatim out of
/// `TAP_SCHEMA`. Refusing it at startup is one operator reading one message.
fn split(name: &str) -> Result<(&str, &str), String> {
    let shape = "a name is schema.table, such as \"gaia_dr3.gaia_source\"";
    let mut parts = name.split('.');
    let (Some(schema), Some(table), None) = (parts.next(), parts.next(), parts.next()) else {
        return Err(shape.to_owned());
    };
    for (half, part) in [("schema", schema), ("table", table)] {
        if part.is_empty() {
            return Err(format!("the {half} half of the name is empty; {shape}"));
        }
        if !names::is_plain(part) {
            return Err(format!(
                "the {half} half, {part:?}, is not a name a query can write unquoted: \
                 expected a letter followed by letters, digits and underscores"
            ));
        }
    }
    Ok((schema, table))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::access::mount::Mounts;
    use crate::config::{AccessConfig, DataConfig, LimitsConfig, MountConfig};

    use super::*;

    /// A policy with one mount at `/hats`, which is what a published path needs — and a
    /// prefix rather than `/`, so that a path outside it is one nothing publishes.
    fn published(dir: &std::path::Path, tables: &[(&str, &str)]) -> Result<TapTableList, String> {
        publishing(mount_at(dir), tables)
    }

    fn mount_at(dir: &std::path::Path) -> MountConfig {
        // A local store is rooted at its directory, so the ones these paths name have to
        // be there.
        for name in ["gaia", "a", "b"] {
            std::fs::create_dir_all(dir.join(name)).unwrap();
        }
        MountConfig {
            path: "/hats".to_owned(),
            source: dir.display().to_string(),
            serve: false,
            follow_symlinks: false,
            catalog_cache_seconds: None,
            storage: StorageOptions::default(),
            filenames: None,
        }
    }

    fn publishing(mount: MountConfig, tables: &[(&str, &str)]) -> Result<TapTableList, String> {
        let mounts = Arc::new(Mounts::new(&[mount], &DataConfig::default()).unwrap());
        let policy = AccessPolicy::new(&AccessConfig::default(), mounts, None).unwrap();
        let transfers = Arc::new(Transfers::new(&LimitsConfig::default()));
        let configured = tables
            .iter()
            .map(|(name, path)| TapTableConfig {
                name: (*name).to_owned(),
                path: (*path).to_owned(),
                examples: Vec::new(),
            })
            .collect::<Vec<_>>();
        TapTableList::new(&configured, &policy, &transfers).map_err(|error| error.to_string())
    }

    #[test]
    fn a_table_is_published_under_its_qualified_name() {
        let dir = tempfile::TempDir::new().unwrap();
        let list = published(dir.path(), &[("gaia_dr3.gaia_source", "/hats/gaia")]).unwrap();
        let table = list.lookup("gaia_dr3.gaia_source").unwrap();
        assert_eq!(table.schema(), "gaia_dr3");
        assert_eq!(table.table(), "gaia_source");
        // The path, as the url an API request would name the same catalog by.
        assert_eq!(table.url().as_str(), "file:///hats/gaia");
        assert_eq!(list.names(), ["gaia_dr3.gaia_source"]);
    }

    /// ADQL's rule for a name written without quotes, which every published name is.
    #[test]
    fn a_name_is_matched_whatever_its_case() {
        let dir = tempfile::TempDir::new().unwrap();
        let list = published(dir.path(), &[("gaia_dr3.gaia_source", "/hats/gaia")]).unwrap();
        for spelling in [
            "gaia_dr3.gaia_source",
            "GAIA_DR3.GAIA_SOURCE",
            "gaia_dr3.Gaia_Source",
        ] {
            assert!(list.lookup(spelling).is_some(), "{spelling} did not match");
        }
        // Half a name is not the name.
        for miss in ["gaia_source", "gaia_dr3", "gaia_dr3.gaia_sources"] {
            assert!(list.lookup(miss).is_none(), "{miss} matched");
        }
    }

    /// A name no query could write is refused where it is written rather than where it is
    /// read, an operator being the one who can fix it.
    #[test]
    fn a_name_a_query_cannot_write_is_refused() {
        let dir = tempfile::TempDir::new().unwrap();
        for name in [
            // No schema.
            "gaia_source",
            // Empty halves.
            ".gaia_source",
            "gaia_dr3.",
            // Three parts.
            "cat.gaia_dr3.gaia_source",
            // Would need quoting, and a quoted name matches case-sensitively.
            "gaia dr3.gaia_source",
            "gaia-dr3.gaia_source",
            "3gaia.source",
            // ADQL's grammar wants a letter first.
            "_gaia.source",
        ] {
            let refused = published(dir.path(), &[(name, "/hats/gaia")]).unwrap_err();
            assert!(refused.contains(name), "{name}: {refused}");
        }
    }

    /// TAP defines both, so publishing into either would shadow a name every client
    /// already knows.
    #[test]
    fn tap_s_own_schemas_are_not_an_operators_to_publish_in() {
        let dir = tempfile::TempDir::new().unwrap();
        for name in ["TAP_SCHEMA.tables", "tap_schema.mine", "TAP_UPLOAD.t1"] {
            let refused = published(dir.path(), &[(name, "/hats/gaia")]).unwrap_err();
            assert!(refused.contains("schema"), "{name}: {refused}");
        }
    }

    /// Two names differing only in case are two tables a client writing an unquoted
    /// identifier cannot tell apart.
    #[test]
    fn one_name_is_published_once() {
        let dir = tempfile::TempDir::new().unwrap();
        let refused = published(
            dir.path(),
            &[
                ("gaia_dr3.gaia_source", "/hats/a"),
                ("gaia_dr3.GAIA_SOURCE", "/hats/b"),
            ],
        )
        .unwrap_err();
        assert!(refused.contains("already published"), "{refused}");
    }

    /// The path goes through the access policy like any other, so an operator hears about
    /// a directory this service will not read at startup rather than on a query.
    #[test]
    fn a_path_the_policy_refuses_is_a_startup_error() {
        let dir = tempfile::TempDir::new().unwrap();
        // Outside every mount, so nothing may read it.
        let refused = published(dir.path(), &[("a.b", "/elsewhere")]).unwrap_err();
        assert!(refused.contains("a.b"), "{refused}");

        // Not a path in this service's url space at all. A url is the shape this key used
        // to take, so it is the mistake worth naming.
        for written in ["s3://ipac-irsa-ztf/ztf", "file:///hats/gaia", "hats/gaia"] {
            let refused = published(dir.path(), &[("a.b", written)]).unwrap_err();
            assert!(refused.contains("a.b"), "{written}: {refused}");
        }
    }

    /// An example missing either half is refused where it is written, and the whole of
    /// what a valid one costs is being carried.
    #[test]
    fn an_example_needs_both_halves() {
        let wrote = |name: &str, query: &str| {
            examples(&[crate::config::TapExampleConfig {
                name: name.to_owned(),
                query: query.to_owned(),
            }])
        };
        let carried = wrote("  Bright stars  ", "\nSELECT TOP 1 ra FROM a.b\n").unwrap();
        assert_eq!(carried[0].name, "Bright stars");
        assert_eq!(carried[0].query, "SELECT TOP 1 ra FROM a.b");

        for (name, query) in [
            ("", "SELECT 1"),
            ("n", ""),
            ("   ", "SELECT 1"),
            ("n", "\t"),
        ] {
            assert!(wrote(name, query).is_err(), "{name:?}/{query:?}");
        }
    }

    /// A catalog in a store is published the same way, because the mount is what reaches
    /// it: the entry is a path in this service's url space either way, and what it takes
    /// to read the catalog was written once, on the mount.
    #[test]
    fn a_table_over_a_store_is_published_too() {
        let mount = MountConfig {
            path: "/hats".to_owned(),
            source: "s3://ipac-irsa-ztf/hats".to_owned(),
            serve: false,
            follow_symlinks: false,
            catalog_cache_seconds: None,
            storage: StorageOptions::default(),
            filenames: None,
        };
        let list = publishing(mount, &[("ztf.dr24_lc", "/hats/ztf_dr24")]).unwrap();
        let table = list.lookup("ztf.dr24_lc").unwrap();
        // The address, not where it really is: a published table names the url space and
        // the mount answers for the rest.
        assert_eq!(table.url().as_str(), "file:///hats/ztf_dr24");
    }
}
