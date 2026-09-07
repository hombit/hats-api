//! Which files this service reads as data, and which are only bytes to hand over.
//!
//! One list, consulted by both modes, because the question is the same one: a caller
//! asking a file-server path for fewer columns and a caller naming a url in the API are
//! both asking this service to parse something. What differs is the answer for a file
//! that is not on the list — the file server still has bytes to send, and the API has
//! nothing to say.

use std::path::Path;

use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use url::Url;

use crate::config::{ConfigError, DataConfig};

/// The configured patterns, compiled once at startup.
///
/// Matched against a file's own name and never against the path above it. A pattern here
/// cannot reach a directory: `*` in a glob would otherwise cross separators in some
/// spellings and not others, and an operator writing `*.parquet` means the files, not a
/// tree that happens to have one of those in its name.
#[derive(Debug, Clone)]
pub struct DataFiles {
    patterns: GlobSet,
    /// The patterns as written, for the refusal that names them. A `GlobSet` does not
    /// keep them.
    written: Vec<String>,
}

impl DataFiles {
    pub fn new(config: &DataConfig) -> Result<Self, ConfigError> {
        let mut builder = GlobSetBuilder::new();
        for pattern in &config.filenames {
            // `literal_separator` is what keeps a pattern to a name: without it `*`
            // crosses `/`, so `*.parquet` would match `catalog.parquet/properties` as
            // readily as a file, and what an operator wrote for a name would silently be
            // a rule about paths.
            let glob = GlobBuilder::new(pattern)
                .literal_separator(true)
                .build()
                .map_err(|error| ConfigError::Data(pattern.clone(), error.to_string()))?;
            builder.add(glob);
        }
        let patterns = builder
            .build()
            .map_err(|error| ConfigError::Data(config.filenames.join(", "), error.to_string()))?;
        Ok(Self {
            patterns,
            written: config.filenames.clone(),
        })
    }

    /// Whether a name is one this service reads as data.
    ///
    /// An empty list matches nothing, which is what an operator who wrote one meant: it
    /// turns every query surface off and leaves the file server serving bytes.
    pub fn matches(&self, name: &str) -> bool {
        self.patterns.is_match(name)
    }

    /// The same, for a path — its last component, which is the file's own name.
    pub fn matches_path(&self, path: &Path) -> bool {
        path.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| self.matches(name))
    }

    /// The same, for the object a url names.
    ///
    /// The url's own last segment, still percent-encoded as the caller wrote it, is not
    /// the object's name: `%2Emetadata` addresses `.metadata`. Decoding is what makes
    /// this the same question the other two answer.
    pub fn matches_url(&self, url: &Url) -> bool {
        self.object_name(url)
            .is_some_and(|name| self.matches(&name))
    }

    /// The name a url's object has, decoded. `None` when the url names no object at all,
    /// which `storage::open` refuses separately.
    pub fn object_name(&self, url: &Url) -> Option<String> {
        let segment = url
            .path_segments()?
            .next_back()
            .filter(|it| !it.is_empty())?;
        Some(
            percent_encoding::percent_decode_str(segment)
                .decode_utf8()
                .ok()?
                .into_owned(),
        )
    }

    /// The list as an operator wrote it, for a message telling a caller what would have
    /// been read.
    pub fn describe(&self) -> String {
        match self.written.is_empty() {
            true => "nothing: data.filenames is empty".to_owned(),
            false => self.written.join(", "),
        }
    }
}

impl Default for DataFiles {
    /// The default list, which cannot fail to compile.
    fn default() -> Self {
        #[expect(
            clippy::expect_used,
            reason = "the default patterns are literals in `DataConfig::default`, and \
                      `the_default_patterns_compile` is what holds them to compiling"
        )]
        Self::new(&DataConfig::default()).expect("the default patterns are valid globs")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn files(patterns: &[&str]) -> DataFiles {
        DataFiles::new(&DataConfig {
            filenames: patterns.iter().map(|it| (*it).to_owned()).collect(),
        })
        .unwrap()
    }

    #[test]
    fn the_default_patterns_compile() {
        let files = DataFiles::default();
        for name in [
            "part0.parquet",
            "part0.parq",
            "_metadata",
            "_common_metadata",
        ] {
            assert!(files.matches(name), "{name}");
        }
        for name in ["properties", "index.html", "partition_info.csv", "part0"] {
            assert!(!files.matches(name), "{name}");
        }
    }

    /// A name, not a path. The pattern is written for files, and a directory that
    /// happens to be called one does not make what is under it data.
    #[test]
    fn a_pattern_matches_a_name_and_not_a_path() {
        let files = files(&["*.parquet"]);
        assert!(files.matches_path(Path::new("/srv/data/Norder=5/part0.parquet")));
        assert!(!files.matches_path(Path::new("/srv/catalog.parquet/properties")));
        // And no pattern reaches across a separator, however it is written.
        assert!(!files.matches("dir/part0.parquet"));
    }

    /// The url carries the name encoded, and what is matched is the name.
    #[test]
    fn a_url_is_matched_on_its_decoded_object_name() {
        let files = files(&["*.parquet", "_metadata"]);
        let url = |raw: &str| Url::parse(raw).unwrap();
        assert!(files.matches_url(&url("s3://bucket/Norder=5/part0.parquet")));
        assert!(files.matches_url(&url("s3://bucket/hats/_metadata")));
        assert!(files.matches_url(&url("https://example.com/a/b/%5Fmetadata")));
        assert!(!files.matches_url(&url("s3://bucket/hats/properties")));
        // A directory url names no object.
        assert!(!files.matches_url(&url("s3://bucket/hats/")));
    }

    /// An operator who wrote an empty list turned the query surface off, which is a
    /// setting rather than a mistake — so it matches nothing rather than everything.
    #[test]
    fn an_empty_list_matches_nothing() {
        let files = files(&[]);
        assert!(!files.matches("part0.parquet"));
        assert!(files.describe().contains("empty"));
    }

    #[test]
    fn a_pattern_that_is_not_a_glob_is_a_startup_error() {
        let error = DataFiles::new(&DataConfig {
            filenames: vec!["[unclosed".to_owned()],
        })
        .unwrap_err()
        .to_string();
        assert!(error.contains("[data]"), "{error}");
    }
}
