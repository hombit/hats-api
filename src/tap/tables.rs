//! The tables an operator publishes, read from `[[tap.table]]` at startup.
//!
//! A TAP query names a table the service already knows: there is nowhere in a TAP request
//! to put a url, so the list is the whole of what a statement may read. Every entry is a
//! HATS catalog — a name and a url and nothing else.
//!
//! **Checked at startup, so an operator hears about a mistake before a caller does.** The
//! name has to be one a query can write, and the url goes through the access policy like
//! any other, which is what catches a mount that does not exist and an endpoint the policy
//! refuses.

use std::sync::Arc;

use url::Url;

use crate::access::AccessPolicy;
use crate::adql::names;
use crate::config::{ConfigError, TapTableConfig};
use crate::storage::materialize::Transfers;
use crate::storage::{self, StorageOptions, parse_url};

/// The schemas a query may not be given a table in, because TAP defines them itself.
///
/// `TAP_SCHEMA` is the service's own metadata and `TAP_UPLOAD` is where an uploaded table
/// would land. An operator publishing into either would shadow a name every client already
/// knows the meaning of.
pub const RESERVED_SCHEMAS: [&str; 2] = ["TAP_SCHEMA", "TAP_UPLOAD"];

/// One published table: the name a query writes, and the catalog behind it.
#[derive(Debug)]
pub struct TapTable {
    /// The schema half of the name, as the operator wrote it.
    schema: String,
    /// The table half.
    table: String,
    /// The two joined, which is what a `FROM` says and what `TAP_SCHEMA.tables` publishes.
    qualified: String,
    /// Where the catalog is. The store is built per request rather than held: nothing here
    /// is a registry of what a catalog turned out to contain, and a builder costs no
    /// connection.
    url: Url,
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
    /// The url is opened and the handle dropped: what that buys is the policy's answer —
    /// a mount that does not exist, a scheme this deployment does not serve, an endpoint
    /// the rules refuse — at startup rather than on somebody's query. A local directory
    /// is checked for being there as well, the store being rooted at it; a remote one is
    /// not, nothing here making a request. Either way no catalog is *read*, so one that
    /// is malformed is discovered on the first query against it, the same as everywhere
    /// else here.
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
            let url = parse_url(&entry.url).map_err(|error| refuse(error.to_string()))?;
            storage::open_dir(&url, &StorageOptions::default(), policy, transfers)
                .map_err(|error| refuse(error.to_string()))?;
            tables.push(TapTable {
                schema: schema.to_owned(),
                table: table.to_owned(),
                qualified: entry.name.clone(),
                url,
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

    /// A policy with one mount at `/hats`, which is what a `file://` table needs — and a
    /// prefix rather than `/`, so that a url outside it is a url nothing publishes.
    fn published(dir: &std::path::Path, tables: &[(&str, &str)]) -> Result<TapTableList, String> {
        // A local store is rooted at its directory, so the ones these urls name have to
        // be there.
        for name in ["gaia", "a", "b"] {
            std::fs::create_dir_all(dir.join(name)).unwrap();
        }
        let mount = MountConfig {
            path: "/hats".to_owned(),
            source: dir.display().to_string(),
            serve: false,
            follow_symlinks: false,
            immutable: false,
            filenames: None,
        };
        let mounts = Arc::new(Mounts::new(&[mount], &DataConfig::default()).unwrap());
        let policy = AccessPolicy::new(&AccessConfig::default(), mounts, None).unwrap();
        let transfers = Arc::new(Transfers::new(&LimitsConfig::default()));
        let configured = tables
            .iter()
            .map(|(name, url)| TapTableConfig {
                name: (*name).to_owned(),
                url: (*url).to_owned(),
            })
            .collect::<Vec<_>>();
        TapTableList::new(&configured, &policy, &transfers).map_err(|error| error.to_string())
    }

    #[test]
    fn a_table_is_published_under_its_qualified_name() {
        let dir = tempfile::TempDir::new().unwrap();
        let list = published(dir.path(), &[("gaia_dr3.gaia_source", "file:///hats/gaia")]).unwrap();
        let table = list.lookup("gaia_dr3.gaia_source").unwrap();
        assert_eq!(table.schema(), "gaia_dr3");
        assert_eq!(table.table(), "gaia_source");
        assert_eq!(table.url().as_str(), "file:///hats/gaia");
        assert_eq!(list.names(), ["gaia_dr3.gaia_source"]);
    }

    /// ADQL's rule for a name written without quotes, which every published name is.
    #[test]
    fn a_name_is_matched_whatever_its_case() {
        let dir = tempfile::TempDir::new().unwrap();
        let list = published(dir.path(), &[("gaia_dr3.gaia_source", "file:///hats/gaia")]).unwrap();
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
            let refused = published(dir.path(), &[(name, "file:///hats/gaia")]).unwrap_err();
            assert!(refused.contains(name), "{name}: {refused}");
        }
    }

    /// TAP defines both, so publishing into either would shadow a name every client
    /// already knows.
    #[test]
    fn tap_s_own_schemas_are_not_an_operators_to_publish_in() {
        let dir = tempfile::TempDir::new().unwrap();
        for name in ["TAP_SCHEMA.tables", "tap_schema.mine", "TAP_UPLOAD.t1"] {
            let refused = published(dir.path(), &[(name, "file:///hats/gaia")]).unwrap_err();
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
                ("gaia_dr3.gaia_source", "file:///hats/a"),
                ("gaia_dr3.GAIA_SOURCE", "file:///hats/b"),
            ],
        )
        .unwrap_err();
        assert!(refused.contains("already published"), "{refused}");
    }

    /// The url goes through the access policy like any other, so an operator hears about
    /// a directory this service will not read at startup rather than on a query.
    #[test]
    fn a_url_the_policy_refuses_is_a_startup_error() {
        let dir = tempfile::TempDir::new().unwrap();
        // Outside every mount, so nothing may read it.
        let refused = published(dir.path(), &[("a.b", "file:///elsewhere")]).unwrap_err();
        assert!(refused.contains("a.b"), "{refused}");

        // A scheme this deployment does not serve at all.
        let refused = published(dir.path(), &[("a.b", "ftp://example.org/gaia")]).unwrap_err();
        assert!(refused.contains("a.b"), "{refused}");
    }

    /// A remote catalog is publishable on the same terms, there being no mount involved
    /// and no storage options to carry.
    #[test]
    fn a_remote_table_is_published_too() {
        let dir = tempfile::TempDir::new().unwrap();
        let list = published(dir.path(), &[("ztf.dr24_lc", "s3://ipac-irsa-ztf/ztf")]).unwrap();
        assert_eq!(list.lookup("ztf.dr24_lc").unwrap().url().scheme(), "s3");
    }
}
