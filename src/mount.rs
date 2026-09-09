//! The local directories the service will read, and where in the url space each one
//! sits.
//!
//! **A mount is the only way a local directory becomes readable**, in either mode. The
//! operator names it once in the config, and nothing a request says can widen that: a
//! path either lands inside one of these directories or lands nowhere.
//!
//! ```toml
//! [[mount]]
//! path = "/"
//! source = "/srv/data"
//! serve = true
//!
//! [[mount]]
//! path = "/hats"
//! source = "/data/hats"
//! immutable = true
//! ```
//!
//! `path` is the mount's address, and both modes use it: the file server publishes the
//! directory there, and an API request naming a local file writes that same path —
//! `file:///hats/dr1/x.parquet`, never the `source` it sits in. So the disk layout is
//! the operator's alone, and moving a directory changes no url.
//!
//! `serve` is what the file server needs and the API does not. Publishing a directory
//! whole and answering a question about one file in it are different things to be
//! willing to do, and a mount that says nothing is willing to do only the second.
//!
//! Two mounts may not claim the same urls, whether or not either is served. First-match
//! wins would make the order of the tables load-bearing, and a file's identity would then
//! depend on where its mount was written rather than on where it is.

use std::path::{Path, PathBuf};

use percent_encoding::percent_decode_str;

use crate::access::canonical_root;
use crate::config::{ConfigError, DataConfig, MountConfig};
use crate::data::DataFiles;
use crate::error::ApiError;

/// One readable directory.
#[derive(Debug)]
pub struct Mount {
    /// The url prefix, normalized: `/`, or `/hats` with no trailing slash. Written this
    /// way once so that matching a request against it is a comparison rather than a
    /// second round of parsing.
    prefix: String,
    /// The directory it holds, canonical, so a path resolved out of a request can simply
    /// be tested for being under it.
    source: PathBuf,
    serve: bool,
    follow_symlinks: bool,
    immutable: bool,
    /// Which files under it are data, which is the mount's own list where it wrote one
    /// and `[data] filenames` where it did not. Compiled per mount rather than looked up
    /// per request, so both modes ask one object the same question.
    data_files: DataFiles,
}

impl Mount {
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn source(&self) -> &Path {
        &self.source
    }

    /// Whether the file server publishes it. The API reads it either way.
    pub fn serve(&self) -> bool {
        self.serve
    }

    pub fn follow_symlinks(&self) -> bool {
        self.follow_symlinks
    }

    pub fn data_files(&self) -> &DataFiles {
        &self.data_files
    }

    /// Whether what is published never changes once published, which is what lets a
    /// cached copy be served without asking the filesystem whether it is still current.
    pub fn immutable(&self) -> bool {
        self.immutable
    }
}

/// Every mount, checked against each other. Empty is the ordinary case: the API over
/// remote stores alone.
#[derive(Debug, Default)]
pub struct Mounts(Vec<Mount>);

impl Mounts {
    /// The mounts as configured, with `data` as the list any of them that named none
    /// reads its files by.
    pub fn new(configs: &[MountConfig], data: &DataConfig) -> Result<Self, ConfigError> {
        let default_files = DataFiles::new(&data.filenames)?;
        let mut mounts: Vec<Mount> = Vec::new();
        for config in configs {
            let invalid = |reason: String| ConfigError::Mount(config.path.clone(), reason);
            let prefix = normalize_prefix(&config.path).map_err(&invalid)?;
            // Resolved now, so that a source that is not there fails at startup rather
            // than on every request to a route that looked configured.
            let source = canonical_root(&config.source).map_err(&invalid)?;
            if let Some(other) = mounts.iter().find(|other| overlaps(&other.prefix, &prefix)) {
                return Err(invalid(format!(
                    "claims urls the mount at {:?} already claims; mount prefixes must \
                     not overlap",
                    other.prefix
                )));
            }
            let data_files = match &config.filenames {
                Some(filenames) => DataFiles::new(filenames)?,
                None => default_files.clone(),
            };
            mounts.push(Mount {
                prefix,
                source,
                serve: config.serve,
                follow_symlinks: config.follow_symlinks,
                immutable: config.immutable,
                data_files,
            });
        }
        Ok(Self(mounts))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether the file server has anything to publish. Not the same question as
    /// [`Self::is_empty`]: a service of API-only mounts has directories to read and no
    /// url space to claim.
    pub fn serves_nothing(&self) -> bool {
        !self.0.iter().any(Mount::serve)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Mount> {
        self.0.iter()
    }

    /// The mount a path belongs to, and the part of the path inside it — with no leading
    /// slash, so it can be joined onto the source directly.
    ///
    /// At most one mount can match, since overlapping prefixes were refused at startup,
    /// so the order of the list decides nothing here.
    ///
    /// Every mount, served or not: this is the address the API names a local file by,
    /// and the file server asks [`Self::published`] instead.
    pub fn resolve<'a>(&self, path: &'a str) -> Option<(&Mount, &'a str)> {
        self.0
            .iter()
            .find_map(|mount| within(&mount.prefix, path).map(|rest| (mount, rest)))
    }

    /// The same, for the file server, which sees only what `serve` published.
    ///
    /// A path inside an unserved mount comes back `None` rather than falling through to
    /// a mount further out — there is no mount further out, since prefixes may not
    /// overlap — so an unserved mount is a hole in the url space and not a redirection
    /// of it.
    pub fn published<'a>(&self, path: &'a str) -> Option<(&Mount, &'a str)> {
        self.resolve(path).filter(|(mount, _)| mount.serve())
    }
}

/// The url prefix as it will be compared: absolute, with no trailing slash unless it is
/// the root, and with nothing in it that a path resolver would have to interpret.
///
/// The API's own prefix goes through this too. The two claim url space the same way, and
/// deciding whether they collide means comparing them in one spelling.
pub(crate) fn normalize_prefix(raw: &str) -> Result<String, String> {
    if !raw.starts_with('/') {
        return Err(format!(
            "a mount path is an absolute url prefix, so it starts with /, not {raw:?}"
        ));
    }
    let mut segments = Vec::new();
    for segment in raw.split('/').filter(|segment| !segment.is_empty()) {
        // `.` and `..` in a prefix would mean the mount answers under one url and
        // matches another. A request path carrying them is a different question, and one
        // the path resolver already answers.
        if segment == "." || segment == ".." {
            return Err(format!(
                "{segment:?} is not something a url prefix can contain"
            ));
        }
        segments.push(segment);
    }
    Ok(match segments.is_empty() {
        true => "/".to_owned(),
        false => format!("/{}", segments.join("/")),
    })
}

/// Whether two prefixes claim any url in common, which for path prefixes means one
/// contains the other.
fn overlaps(one: &str, other: &str) -> bool {
    within(one, other).is_some() || within(other, one).is_some()
}

/// The part of `path` that lies under `prefix`, or `None` when it does not. `/hats` does
/// not contain `/hatsx`: a prefix matches whole segments or nothing.
pub(crate) fn within<'a>(prefix: &str, path: &'a str) -> Option<&'a str> {
    let rest = path.strip_prefix(prefix)?;
    match prefix {
        // The root already ends in the separator, so what is left is the whole path.
        "/" => Some(rest),
        _ => match rest.is_empty() {
            true => Some(rest),
            false => rest.strip_prefix('/'),
        },
    }
}

/// The path a request names inside a mount, one component per url segment.
///
/// Percent-decoded one segment at a time, so that an encoded separator arrives as part
/// of a name rather than as a separator: `a%2Fb` is one component called `a/b`, which no
/// filesystem has, and not a path into `a`. `.` and `..` are refused outright rather than
/// resolved, since a mount's url space is not a filesystem and has nothing above it.
///
/// Both modes come through here. The API's local urls are addressed in this same url
/// space, so a name that means one file to the file server must not mean another to a
/// query about it.
pub fn path_segments(relative: &str) -> Result<Vec<String>, ApiError> {
    let mut segments = Vec::new();
    for segment in relative.split('/').filter(|segment| !segment.is_empty()) {
        let decoded = percent_decode_str(segment)
            .decode_utf8()
            .map_err(|_| ApiError::bad_request("this path is not valid UTF-8"))?;
        if matches!(decoded.as_ref(), "." | "..") || decoded.contains(['/', '\0']) {
            return Err(ApiError::bad_request(format!(
                "{segment:?} is not something a path here can contain"
            )));
        }
        segments.push(decoded.into_owned());
    }
    Ok(segments)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    fn config(path: &str, source: &Path) -> MountConfig {
        MountConfig {
            path: path.to_owned(),
            source: source.display().to_string(),
            serve: true,
            follow_symlinks: false,
            immutable: false,
            filenames: None,
        }
    }

    fn mounts(configs: &[MountConfig]) -> Result<Mounts, ConfigError> {
        Mounts::new(configs, &DataConfig::default())
    }

    #[test]
    fn a_prefix_is_normalized_before_it_is_compared() {
        for (raw, normalized) in [
            ("/", "/"),
            ("//", "/"),
            ("/hats", "/hats"),
            ("/hats/", "/hats"),
            ("/hats//dr1/", "/hats/dr1"),
        ] {
            assert_eq!(normalize_prefix(raw).unwrap(), normalized, "{raw}");
        }
        for raw in ["hats", "", "/hats/../etc", "/./hats"] {
            assert!(normalize_prefix(raw).is_err(), "{raw} was accepted");
        }
    }

    /// A prefix matches whole segments: the mount at `/hats` is not the owner of
    /// `/hatsx`, which would otherwise be served out of the wrong directory.
    #[test]
    fn a_prefix_matches_whole_segments() {
        assert_eq!(
            within("/hats", "/hats/dr1/x.parquet"),
            Some("dr1/x.parquet")
        );
        assert_eq!(within("/hats", "/hats"), Some(""));
        assert_eq!(within("/hats", "/hatsx"), None);
        assert_eq!(within("/hats", "/other"), None);
        assert_eq!(within("/", "/hats/x"), Some("hats/x"));
        assert_eq!(within("/", "/"), Some(""));
    }

    /// An address is an address whether or not the file server publishes it, so an
    /// unserved mount takes part in this like any other.
    #[test]
    fn overlapping_mounts_are_refused_rather_than_ordered() {
        let dir = TempDir::new().unwrap();
        let at = |paths: &[&str]| {
            let configs: Vec<_> = paths.iter().map(|path| config(path, dir.path())).collect();
            mounts(&configs)
        };
        assert!(at(&["/hats", "/data"]).is_ok());
        // The same directory under two prefixes is not an overlap: the urls differ.
        assert!(at(&["/hats", "/hats-dr1"]).is_ok());
        for overlapping in [
            ["/", "/hats"].as_slice(),
            ["/hats", "/"].as_slice(),
            ["/hats", "/hats"].as_slice(),
            ["/hats", "/hats/"].as_slice(),
            ["/hats", "/hats/dr1"].as_slice(),
        ] {
            let error = at(overlapping).unwrap_err().to_string();
            assert!(error.contains("overlap"), "{overlapping:?}: {error}");
            let unserved: Vec<_> = overlapping
                .iter()
                .map(|path| MountConfig {
                    serve: false,
                    ..config(path, dir.path())
                })
                .collect();
            assert!(mounts(&unserved).is_err(), "{overlapping:?} unserved");
        }
    }

    #[test]
    fn a_source_must_be_a_directory_that_exists() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("x.parquet");
        std::fs::write(&file, b"").unwrap();
        assert!(mounts(&[config("/", dir.path())]).is_ok());
        // A file:// url says the same thing as the path, and is accepted the same way.
        assert!(
            mounts(&[MountConfig {
                source: format!("file://{}", dir.path().display()),
                ..config("/", dir.path())
            }])
            .is_ok()
        );
        let missing = dir.path().join("nothing");
        for source in [file.as_path(), missing.as_path()] {
            let error = mounts(&[config("/", source)]).unwrap_err().to_string();
            assert!(error.contains("[[mount]]"), "{error}");
        }
        // A remote source is a startup error rather than a directory nothing can find.
        let error = mounts(&[MountConfig {
            source: "s3://bucket/hats".to_owned(),
            ..config("/", dir.path())
        }])
        .unwrap_err()
        .to_string();
        assert!(error.contains("local path"), "{error}");
    }

    #[test]
    fn a_request_path_finds_its_mount_and_the_rest_of_the_path() {
        let dir = TempDir::new().unwrap();
        let found = mounts(&[config("/hats", dir.path()), config("/raw", dir.path())]).unwrap();
        let (mount, rest) = found.resolve("/hats/dr1/x.parquet").unwrap();
        assert_eq!(mount.prefix(), "/hats");
        assert_eq!(rest, "dr1/x.parquet");
        // The source as resolved, not as written: on macOS the temporary directory is
        // reached through a symlink, and the path that gets opened is the resolved one.
        assert_eq!(mount.source(), std::fs::canonicalize(dir.path()).unwrap());
        assert!(found.resolve("/other/x.parquet").is_none());
        assert!(Mounts::default().resolve("/hats/x").is_none());
    }

    /// The file server sees a subset of the addresses, and an unserved prefix is a hole
    /// in its url space rather than something a wider mount picks up.
    #[test]
    fn only_a_served_mount_is_published() {
        let dir = TempDir::new().unwrap();
        let found = mounts(&[
            MountConfig {
                serve: false,
                ..config("/private", dir.path())
            },
            config("/hats", dir.path()),
        ])
        .unwrap();
        assert!(found.resolve("/private/x.parquet").is_some());
        assert!(found.published("/private/x.parquet").is_none());
        assert!(found.published("/hats/x.parquet").is_some());
        assert!(!found.is_empty() && !found.serves_nothing());

        let api_only = mounts(&[MountConfig {
            serve: false,
            ..config("/private", dir.path())
        }])
        .unwrap();
        assert!(!api_only.is_empty() && api_only.serves_nothing());
    }

    /// A mount that names no `filenames` reads its files by `[data] filenames`, and one
    /// that names its own reads them by that and does not disturb the others.
    #[test]
    fn a_mount_may_carry_its_own_list_of_data_files() {
        let dir = TempDir::new().unwrap();
        let found = Mounts::new(
            &[
                MountConfig {
                    filenames: Some(vec!["*.fits".to_owned()]),
                    ..config("/fits", dir.path())
                },
                config("/hats", dir.path()),
            ],
            &DataConfig::default(),
        )
        .unwrap();
        let [fits, hats] = found.0.as_slice() else {
            panic!("expected two mounts")
        };
        assert!(fits.data_files().matches("image.fits"));
        assert!(!fits.data_files().matches("part0.parquet"));
        assert!(hats.data_files().matches("part0.parquet"));
        assert!(!hats.data_files().matches("image.fits"));
    }

    #[test]
    fn a_path_is_decoded_one_segment_at_a_time() {
        assert_eq!(
            path_segments("dr1/a%20b.parquet").unwrap(),
            ["dr1", "a b.parquet"]
        );
        // Empty components are dropped, so a doubled separator is not a component.
        assert_eq!(path_segments("//dr1//x").unwrap(), ["dr1", "x"]);
        // An encoded separator is part of a name, and a name is one component.
        for refused in ["a%2Fb", "..", ".", "dr1/../etc", "a%00b"] {
            assert!(path_segments(refused).is_err(), "{refused} was accepted");
        }
    }
}
