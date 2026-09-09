//! A catalog's `properties` file: what the catalog says about itself.
//!
//! The format is a Java properties file — `key=value` a line at a time, `#` for a
//! comment — and the keys are HATS's. Nothing here validates a catalog: a key that is
//! absent is absent, and what a missing one costs is decided where it is wanted. What is
//! refused is a key that is present and unreadable, since a `hats_order` of `banana` is a
//! catalog saying something rather than saying nothing.

use std::collections::BTreeMap;
use std::str::FromStr;

use java_properties::PropertiesIter;

use crate::error::ApiError;

/// The file a catalog is described by, preferred spelling first.
///
/// A catalog written today carries both, holding the same content. `properties` is the
/// deprecated name, so it is the fallback: reading `hats.properties` first means the
/// ordinary catalog is answered by its first request rather than its second.
pub const NAMES: [&str; 2] = ["hats.properties", "properties"];

/// The file a *collection* is described by: a primary table, its margin catalogs and its
/// indexes, each of which is a catalog directory beside it.
///
/// Asked for last, after both catalog spellings, so that a collection costs an extra
/// request and a catalog costs none.
pub const COLLECTION: &str = "collection.properties";

/// What a partition file is called when the catalog does not say.
const DEFAULT_NPIX_SUFFIX: &str = ".parquet";

/// The parsed file: every key it carried, and typed access to the ones this service acts
/// on.
#[derive(Debug, Clone, Default)]
pub struct Properties {
    entries: BTreeMap<String, String>,
}

impl Properties {
    /// Parse the bytes of a `properties` file.
    ///
    /// `java-properties` does the reading, so the corners of the format come with it:
    /// `\` escapes and `\uXXXX`, a trailing `\` continuing a line, `:` and bare whitespace
    /// as separators beside `=`, and `!` as a second comment marker. None of that appears
    /// in what an importer writes today, which is exactly why it is not worth a parser of
    /// our own — a hand-rolled one agrees with the format only until a catalog uses one of
    /// them, and then it disagrees silently.
    ///
    /// **Read as UTF-8, not as the format's own encoding.** Java specifies ISO-8859-1, and
    /// a HATS properties file is written by Python and is UTF-8; the two agree on ASCII and
    /// part company on everything else. Decoded as Latin-1, a degree sign or an accented
    /// name in `obs_title` comes back as two characters that are not the ones in the file —
    /// data, not an error, and nothing downstream would report it.
    pub fn parse(bytes: &[u8]) -> Result<Self, ApiError> {
        let mut entries = BTreeMap::new();
        PropertiesIter::new_with_encoding(bytes, encoding_rs::UTF_8)
            .read_into(|key, value| {
                entries.insert(key, value);
            })
            .map_err(|error| {
                ApiError::bad_request(format!(
                    "this catalog's properties file cannot be read: {error}"
                ))
            })?;
        Ok(Self { entries })
    }

    /// One key as written, for anything this service does not itself act on.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries
            .get(key)
            .map(String::as_str)
            .filter(|value| !value.is_empty())
    }

    /// The catalog's short name.
    pub fn name(&self) -> Option<&str> {
        self.get("obs_collection")
    }

    /// `object`, `source`, `association`, `index`, `margin` or `map` — left as written,
    /// since what a type this service has not heard of should do is decided by whoever
    /// needs the distinction rather than by refusing to open the catalog.
    pub fn product_type(&self) -> Option<&str> {
        self.get("dataproduct_type")
    }

    /// The columns a `region` is tested against, which a lone parquet file cannot supply
    /// and a catalog can. Both or neither: a position needs two coordinates, and half of
    /// one is a catalog that has not said.
    pub fn coordinate_columns(&self) -> Option<(&str, &str)> {
        Some((self.get("hats_col_ra")?, self.get("hats_col_dec")?))
    }

    /// The HEALPix column and the order its values are at.
    ///
    /// `hats_col_healpix` and `hats_col_healpix_order`, falling back to what HATS
    /// recommends — the column `_healpix_29`, holding order-29 cells. The order is read
    /// and never taken from the column's *name*: `healpix13` is written at order 13, and
    /// read at 29 every bound would be one no row satisfies, which returns no rows rather
    /// than failing.
    ///
    /// Both halves default independently, so what this answers is a candidate rather than
    /// a fact about the catalog. It costs nothing when it is wrong about a catalog that
    /// has no such column: [`crate::healpix::SpatialIndex::resolve`] asks the file's
    /// schema, and a column that is not there means the query runs on the geometry alone.
    /// A request naming its own column and order overrides this.
    /// Whether the catalog names a HEALPix column of its own, as against
    /// [`Self::healpix_column`] falling back to the recommended name.
    ///
    /// The difference is who is making the claim, and it decides what a file without that
    /// column means: a catalog that named one and has not got it is broken, and a catalog
    /// that named none simply has no index.
    pub fn names_healpix_column(&self) -> bool {
        self.get("hats_col_healpix").is_some()
    }

    pub fn healpix_column(&self) -> Result<(&str, u8), ApiError> {
        Ok((
            self.get("hats_col_healpix")
                .unwrap_or(crate::healpix::DEFAULT_HEALPIX_COLUMN_NAME),
            self.number("hats_col_healpix_order")?
                .unwrap_or(crate::healpix::MAX_ORDER),
        ))
    }

    /// The deepest order the catalog is partitioned at, as the catalog states it. The
    /// partition list is what this is checked against, since that is the one that decides
    /// what gets read.
    pub fn order(&self) -> Result<Option<u8>, ApiError> {
        self.number("hats_order")
    }

    /// How many rows the whole catalog holds.
    pub fn rows(&self) -> Result<Option<u64>, ApiError> {
        self.number("hats_nrows")
    }

    /// What a partition file's name ends in.
    ///
    /// `.parquet` unless the catalog says otherwise, and `/` means each partition is a
    /// directory of files rather than one file.
    pub fn npix_suffix(&self) -> &str {
        self.get("hats_npix_suffix").unwrap_or(DEFAULT_NPIX_SUFFIX)
    }

    /// Whether a partition is a directory rather than a single file.
    pub fn partition_is_a_directory(&self) -> bool {
        self.npix_suffix().ends_with('/')
    }

    /// The columns the catalog is sorted by, in order. Space-separated, which is how HATS
    /// writes a list.
    pub fn sort_columns(&self) -> Vec<&str> {
        self.get("hats_cols_sort")
            .map(|value| value.split_whitespace().collect())
            .unwrap_or_default()
    }

    fn number<T>(&self, key: &str) -> Result<Option<T>, ApiError>
    where
        T: FromStr,
        T::Err: std::fmt::Display,
    {
        self.get(key)
            .map(|value| {
                value.parse::<T>().map_err(|error| {
                    ApiError::bad_request(format!(
                        "this catalog's properties file has {key}={value:?}, which is not a \
                         number: {error}"
                    ))
                })
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SMALL_SKY: &str = "\
#HATS catalog
obs_collection=small_sky
dataproduct_type=object
hats_nrows=131
hats_col_ra=ra
hats_col_dec=dec
hats_col_healpix=_healpix_29
hats_col_healpix_order=29
hats_npix_suffix=.parquet
hats_order=0
";

    #[test]
    fn a_catalog_says_its_columns_and_its_order() {
        let properties = Properties::parse(SMALL_SKY.as_bytes()).unwrap();
        assert_eq!(properties.name(), Some("small_sky"));
        assert_eq!(properties.product_type(), Some("object"));
        assert_eq!(properties.coordinate_columns(), Some(("ra", "dec")));
        assert_eq!(properties.healpix_column().unwrap(), ("_healpix_29", 29));
        assert_eq!(properties.order().unwrap(), Some(0));
        assert_eq!(properties.rows().unwrap(), Some(131));
        assert_eq!(properties.npix_suffix(), ".parquet");
        assert!(!properties.partition_is_a_directory());
    }

    /// The order is the catalog's to state, and a catalog stating a different one is not
    /// a catalog whose column is at 29.
    #[test]
    fn the_healpix_order_is_read_and_not_guessed_from_the_name() {
        let properties =
            Properties::parse(b"hats_col_healpix=healpix13\nhats_col_healpix_order=13\n" as &[u8])
                .unwrap();
        assert_eq!(properties.healpix_column().unwrap(), ("healpix13", 13));
    }

    /// A catalog that says neither is taken at the recommendation, which is a candidate
    /// and not a claim: the file's schema is what decides whether there is such a column.
    #[test]
    fn a_catalog_that_says_nothing_is_taken_at_the_recommendation() {
        let properties = Properties::default();
        assert_eq!(properties.healpix_column().unwrap(), ("_healpix_29", 29));
    }

    #[test]
    fn a_partition_may_be_a_directory() {
        let properties = Properties::parse(b"hats_npix_suffix=/\n" as &[u8]).unwrap();
        assert_eq!(properties.npix_suffix(), "/");
        assert!(properties.partition_is_a_directory());
    }

    /// A key that is there and unreadable is the catalog contradicting itself, which is
    /// worth saying; one that is absent is not.
    #[test]
    fn a_number_that_is_not_one_is_refused_and_an_absent_one_is_not() {
        let error = Properties::parse(b"hats_order=banana\n" as &[u8])
            .unwrap()
            .order()
            .unwrap_err()
            .to_string();
        assert!(error.contains("hats_order"), "{error}");
        assert!(
            Properties::parse(b"" as &[u8])
                .unwrap()
                .order()
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn comments_and_blank_lines_are_not_entries() {
        let properties = Properties::parse(
            b"# a comment\n! another\n\n  obs_collection = spaced\nbare_key\n" as &[u8],
        )
        .unwrap();
        assert_eq!(properties.name(), Some("spaced"));
        // A key with nothing after it has an empty value, which reads the same as absent:
        // a catalog that wrote the key and no value has not said anything.
        assert_eq!(properties.get("bare_key"), None);
        assert_eq!(properties.get("# a comment"), None);
    }

    /// **A value runs to the end of its line, trailing spaces and all.** That is the
    /// format, and trimming would be this crate disagreeing with every other reader about
    /// what a file says. What it costs is a hand-edited `hats_col_ra=ra ` naming a column
    /// no file has — an error the caller sees, rather than a value quietly altered.
    #[test]
    fn a_value_keeps_the_whitespace_the_file_gave_it() {
        let properties = Properties::parse(b"hats_col_ra=ra \n" as &[u8]).unwrap();
        assert_eq!(properties.get("hats_col_ra"), Some("ra "));
    }

    /// The corners of the format, which are the reason this is not parsed by hand. None of
    /// them appears in what an importer writes today — and a reader that gets them wrong
    /// gets them wrong silently, on the one catalog that uses them.
    #[test]
    fn the_awkward_corners_of_the_format_are_read_as_the_format_says() {
        let properties = Properties::parse(
            b"obs_title=Survey \\u00b0 north\n\
              hats_cols_sort=_healpix_29 \\\n    id\n\
              obs_collection:colon_separated\n\
              hats_col_ra dec_by_whitespace\n" as &[u8],
        )
        .unwrap();
        assert_eq!(properties.get("obs_title"), Some("Survey ° north"));
        // A trailing backslash continues the line, and the next line's indent is dropped.
        assert_eq!(properties.sort_columns(), vec!["_healpix_29", "id"]);
        assert_eq!(properties.name(), Some("colon_separated"));
        assert_eq!(properties.get("hats_col_ra"), Some("dec_by_whitespace"));
    }

    /// A properties file is UTF-8 here, though the Java format says ISO-8859-1. Read as
    /// Latin-1 this comes back as two characters that are not the one in the file — data
    /// rather than an error, which nothing downstream would report.
    #[test]
    fn a_properties_file_is_read_as_utf8() {
        let properties = Properties::parse("obs_title=Andrés °\n".as_bytes()).unwrap();
        assert_eq!(properties.get("obs_title"), Some("Andrés °"));
    }

    #[test]
    fn a_list_valued_key_is_split_on_whitespace() {
        let properties = Properties::parse(b"hats_cols_sort=_healpix_29 id\n" as &[u8]).unwrap();
        assert_eq!(properties.sort_columns(), vec!["_healpix_29", "id"]);
        assert!(Properties::default().sort_columns().is_empty());
    }
}
