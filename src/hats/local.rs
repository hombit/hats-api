//! Recognising a catalog on local disk, for the file-server mode.
//!
//! The API is told what it is looking at: a caller names a catalog's url, and the answer to
//! "is this a catalog" is whatever [`super::Catalog::open`] makes of the directory. A person
//! browsing a mount names nothing — they click, and the page has to know before they ask
//! whether there is a question to offer. So this is a cheap look at a directory, made from
//! names and from one small read, and it is a *hint*: everything it says yes to is opened
//! properly a moment later, and a directory that lied gets the same refusal any other
//! would.
//!
//! It answers a second question the API never has to ask. A catalog is browsed from the
//! inside — `dataset/Norder=5/Dir=0` is where the files are, and where a reader ends up —
//! and the query surface belongs to the catalog rather than to the directory they happen to
//! be standing in. So a directory that is one of a catalog's own layers points at the
//! catalog above it.

use std::io::Read as _;
use std::path::Path;

use super::properties::{self, Properties};
use super::{catalog, partitions};

/// How much of a `properties` file is read to recognise it.
///
/// The file is a few hundred bytes and the keys that identify it are at the front, so this
/// is not a budget so much as a refusal to read whatever else might be sitting under that
/// name — the check runs against a directory chosen by whoever is browsing.
const PEEK: u64 = 64 * 1024;

/// What a catalog's own key names start with, which is what tells its `properties` file
/// from any other file called `properties`.
const PREFIX: &str = "hats_";

/// The layers between a catalog's root and its files.
///
/// `dataset` and the three `Norder`/`Dir`/`Npix` levels, which is as deep as the layout
/// goes — `Npix=` is a directory rather than a file only where `hats_npix_suffix` is `/`.
/// Anything else stops the walk: a directory below a catalog that is not one of these is
/// not a place the catalog's own structure put a reader.
const LAYERS: usize = 4;

/// Whether this directory holds the file that describes a catalog.
///
/// `hats.properties` and `collection.properties` are recognised by name — nothing else is
/// called either, so reading them here would only be reading them twice. A bare
/// `properties` is a name anything may have, so that one is opened and asked whether it
/// carries a catalog's keys.
pub fn describes_a_catalog(dir: &Path) -> bool {
    if dir.join(properties::NAMES[0]).is_file() || dir.join(properties::COLLECTION).is_file() {
        return true;
    }
    holds_catalog_keys(&dir.join(properties::NAMES[1]))
}

/// Whether a file reads as a catalog's properties: a `hats_`-prefixed key at the start of
/// some line.
///
/// Deliberately not the properties parser. This runs against a file nobody claimed was one,
/// so the question is whether it looks like a catalog's, and a parse failure on some other
/// file called `properties` would be a refusal where the answer is simply "no".
fn holds_catalog_keys(path: &Path) -> bool {
    read(path).is_some_and(|text| {
        text.lines()
            .any(|line| line.trim_start().starts_with(PREFIX))
    })
}

/// The front of a file, or `None` for anything that is not a readable text file — a
/// directory of that name included, which is what `properties` may well be.
fn read(path: &Path) -> Option<String> {
    let mut text = String::new();
    std::fs::File::open(path)
        .ok()?
        .take(PEEK)
        .read_to_string(&mut text)
        .ok()?;
    Some(text)
}

/// How many levels above `dir` its catalog is, or `None` if there is no catalog over it.
///
/// `Some(0)` is the catalog's own directory. Above that the walk only ever climbs through
/// `LAYERS`, so a reader inside a partition directory is offered the catalog and a reader
/// in some unrelated subdirectory of one is offered nothing.
///
/// `depth` is how far the mount's root is, which is as far up as this may look: what is
/// above a mount is the operator's business, and a catalog there is not published.
pub fn enclosing(dir: &Path, depth: usize) -> Option<usize> {
    let mut at = dir;
    for level in 0..=depth.min(LAYERS) {
        if describes_a_catalog(at) {
            return Some(level);
        }
        // The name that was just stepped over has to be one of the catalog's own layers,
        // checked before stepping so that the catalog's root is reached only from inside it.
        let name = at.file_name()?.to_str()?;
        if !is_a_layer(name) {
            return None;
        }
        at = at.parent()?;
    }
    None
}

/// The little a page says about a catalog before anyone asks it anything.
///
/// Every field is optional and none of it is checked against anything: this is a catalog
/// introducing itself, so a key it does not carry is a line the page does not write, and a
/// key that will not parse is the same. Reading it is one small file — the properties file
/// is a few hundred bytes — against a directory somebody is already looking at.
#[derive(Debug, Default, Clone)]
pub struct About {
    /// `obs_collection`, which is what a catalog calls itself.
    pub name: Option<String>,
    /// `hats_nrows`, the catalog's own count. Not summed from anywhere and not checked
    /// against anything: it is what the catalog says.
    pub rows: Option<u64>,
    /// `hats_order`, the order the catalog says it is partitioned at. The page is the one
    /// place this is read rather than the partition list — nothing is being planned on it.
    pub order: Option<u8>,
    /// Where this catalog's columns can be read, as the path below the directory the page is
    /// for — `dataset/_common_metadata`, which is every partition's columns and no rows, so
    /// the page gets them without choosing a partition to ask.
    ///
    /// Segments rather than a joined string, because a url is built out of these and the
    /// encoding of a name belongs to `listing`. A collection's is one segment longer: the
    /// file is in the primary table, and the collection's own directory has no `dataset`.
    ///
    /// `None` where the file is not there. A catalog without one is answered by the page a
    /// different way rather than offered a url that is a 404.
    pub schema: Option<Vec<String>>,
}

/// What this catalog says about itself, from whichever of its files describes it.
pub fn about(dir: &Path) -> About {
    let mut about = About::default();
    let properties = properties::NAMES
        .iter()
        .chain([&properties::COLLECTION])
        .find_map(|name| read(&dir.join(name)))
        .and_then(|text| Properties::parse(text.as_bytes()).ok());
    if let Some(properties) = properties {
        about.name = properties.name().map(str::to_owned);
        // A key that will not parse is a key the page leaves out. Nothing is being decided
        // on these, so a broken one is worth less than a refusal would cost.
        about.rows = properties.rows().ok().flatten();
        about.order = properties.order().ok().flatten();
        about.schema = schema(dir, &properties);
    }
    about
}

/// The path to the file the page reads this catalog's columns from, checked to be there.
///
/// A collection has no `dataset` of its own: its partitions are the primary table's, and so
/// is the file that describes them. Without the hop the page finds nothing and shows a
/// catalog with no columns, which reads as a catalog that has none.
///
/// The hop is [`super::catalog::primary_table`]'s to allow, not this module's to repeat: a
/// collection is followed one hop and only ever downwards, and a second copy of that rule is
/// one that can come to disagree with the first. Here it is a hint rather than a request, so
/// a refusal is simply a page with no column list — the same answer as a missing file.
fn schema(dir: &Path, properties: &Properties) -> Option<Vec<String>> {
    // Split rather than kept whole: a primary table may be named as more than one level, and
    // a `/` left inside a segment is percent-encoded into the name when the url is built.
    let below = |within: &str| {
        let path: Vec<String> = within
            .split('/')
            .filter(|segment| !segment.is_empty())
            .chain(partitions::COMMON_METADATA.split('/'))
            .map(str::to_owned)
            .collect();
        let at = path
            .iter()
            .fold(dir.to_path_buf(), |at, segment| at.join(segment));
        at.is_file().then_some(path)
    };
    below("").or_else(|| below(catalog::primary_table(properties).ok()?))
}

/// Whether a directory name is one of a catalog's own layers.
fn is_a_layer(name: &str) -> bool {
    name == "dataset"
        || ["Norder=", "Dir=", "Npix="]
            .iter()
            .any(|level| name.starts_with(level))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    /// A catalog directory, under each of the three names one is described by.
    #[test]
    fn a_catalog_is_recognised_by_the_file_that_describes_it() {
        for (name, content) in [
            ("hats.properties", "anything at all"),
            ("collection.properties", "anything at all"),
            ("properties", "obs_collection=dr1\nhats_order=5\n"),
        ] {
            let dir = TempDir::new().unwrap();
            fs::write(dir.path().join(name), content).unwrap();
            assert!(describes_a_catalog(dir.path()), "{name}");
        }
    }

    /// `properties` is a name anything may have, and the two spelled out in full are not.
    #[test]
    fn a_properties_file_that_is_not_a_catalog_s_is_not_one() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("properties"), "colour=blue\nsize=12\n").unwrap();
        assert!(!describes_a_catalog(dir.path()));

        // Not a file at all, which is the other thing that name can be.
        let other = TempDir::new().unwrap();
        fs::create_dir(other.path().join("properties")).unwrap();
        assert!(!describes_a_catalog(other.path()));
    }

    /// A reader ends up inside the layout, and the query belongs to the catalog above it.
    #[test]
    fn a_directory_inside_a_catalog_finds_the_catalog_over_it() {
        let root = TempDir::new().unwrap();
        let catalog = root.path().join("dr1");
        let inside = catalog.join("dataset/Norder=5/Dir=0/Npix=17");
        fs::create_dir_all(&inside).unwrap();
        fs::write(catalog.join("hats.properties"), "x").unwrap();

        // Depth is how far the mount root is, and every level of the layout is reachable.
        assert_eq!(enclosing(&catalog, 1), Some(0));
        assert_eq!(enclosing(&catalog.join("dataset"), 2), Some(1));
        assert_eq!(enclosing(&catalog.join("dataset/Norder=5"), 3), Some(2));
        assert_eq!(enclosing(&inside, 5), Some(4));
    }

    /// The walk climbs through the catalog's own layers and through nothing else, so a
    /// directory a catalog did not put there is not offered the catalog's query.
    #[test]
    fn the_walk_climbs_only_the_catalog_s_own_layers() {
        let root = TempDir::new().unwrap();
        let catalog = root.path().join("dr1");
        let aside = catalog.join("notes/january");
        fs::create_dir_all(&aside).unwrap();
        fs::write(catalog.join("hats.properties"), "x").unwrap();

        assert_eq!(enclosing(&aside, 5), None);
        assert_eq!(enclosing(&catalog.join("notes"), 5), None);
    }

    /// A listing goes no higher than its mount, so neither does this: a catalog above the
    /// mount is not something the mount published.
    #[test]
    fn the_walk_stops_at_the_mount() {
        let root = TempDir::new().unwrap();
        let inside = root.path().join("dataset/Norder=5");
        fs::create_dir_all(&inside).unwrap();
        fs::write(root.path().join("hats.properties"), "x").unwrap();

        // The mount is the catalog itself, two levels up, and is found from inside it.
        assert_eq!(enclosing(&inside, 2), Some(2));
        // Published from `dataset` down, the catalog's own directory is above the mount.
        assert_eq!(enclosing(&inside, 1), None);
    }

    /// Nothing above a directory that is not itself a catalog and not inside one.
    #[test]
    fn an_ordinary_directory_has_no_catalog() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("part0.parquet"), b"x").unwrap();
        assert_eq!(enclosing(dir.path(), 4), None);
    }

    /// The columns file, for a catalog that holds one directly and for a collection that
    /// holds one in its primary table.
    ///
    /// A collection has no `dataset` of its own, so looking only below its own directory
    /// finds nothing and the page shows a catalog with no columns — which a reader cannot
    /// tell from a catalog that has none.
    #[test]
    fn a_collection_s_columns_are_found_in_its_primary_table() {
        let plain = TempDir::new().unwrap();
        fs::write(plain.path().join("hats.properties"), "hats_order=1\n").unwrap();
        fs::create_dir_all(plain.path().join("dataset")).unwrap();
        fs::write(plain.path().join(partitions::COMMON_METADATA), b"x").unwrap();
        assert_eq!(
            about(plain.path()).schema.as_deref(),
            Some(["dataset".to_owned(), "_common_metadata".to_owned()].as_slice())
        );

        let collection = TempDir::new().unwrap();
        fs::write(
            collection.path().join("collection.properties"),
            "obs_collection=c\nhats_primary_table_url=inside\n",
        )
        .unwrap();
        fs::create_dir_all(collection.path().join("inside/dataset")).unwrap();
        fs::write(
            collection
                .path()
                .join("inside")
                .join(partitions::COMMON_METADATA),
            b"x",
        )
        .unwrap();
        assert_eq!(
            about(collection.path()).schema.as_deref(),
            Some(
                [
                    "inside".to_owned(),
                    "dataset".to_owned(),
                    "_common_metadata".to_owned()
                ]
                .as_slice()
            )
        );
    }

    /// A collection whose primary table points outside itself is not followed, and the page
    /// simply has no column list.
    ///
    /// The hop is the same one the API allows and no wider: a url built from an absolute
    /// path or a `..` would leave the mount, and it would be built out of a name a file this
    /// service read rather than out of anything the caller wrote.
    #[test]
    fn a_collection_pointing_outside_itself_offers_no_columns() {
        for named in ["/etc", "../sibling", "https://example.com/x"] {
            let dir = TempDir::new().unwrap();
            fs::write(
                dir.path().join("collection.properties"),
                format!("obs_collection=c\nhats_primary_table_url={named}\n"),
            )
            .unwrap();
            assert_eq!(about(dir.path()).schema, None, "{named}");
        }
    }

    /// A catalog with no such file offers no url for one, rather than one that is a 404.
    #[test]
    fn a_catalog_without_the_file_offers_no_columns() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("hats.properties"), "hats_order=1\n").unwrap();
        assert_eq!(about(dir.path()).schema, None);
    }
}
