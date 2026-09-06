//! The directories the service publishes, and where in the url space each one sits.
//!
//! A mount is the other mode: instead of the request naming the location of the data,
//! the operator names it once in the config and the request names a path inside it. So
//! there is nothing here for a caller to widen — the rules are fixed at startup, and a
//! request either lands inside one of these directories or lands nowhere.
//!
//! ```toml
//! [[mount]]
//! path = "/"
//! source = "/srv/data"
//!
//! [[mount]]
//! path = "/hats"
//! source = "/data/hats"
//! immutable = true
//! ```
//!
//! Two mounts may not claim the same urls. First-match-wins would make the order of the
//! tables load-bearing, and a file's identity would then depend on where its mount was
//! written rather than on where it is.

use std::path::{Path, PathBuf};

use crate::access::canonical_root;
use crate::config::{ConfigError, MountConfig};

/// One published directory.
#[derive(Debug)]
pub struct Mount {
    /// The url prefix, normalized: `/`, or `/hats` with no trailing slash. Written this
    /// way once so that matching a request against it is a comparison rather than a
    /// second round of parsing.
    prefix: String,
    /// The directory it publishes, canonical, so a path resolved out of a request can
    /// simply be tested for being under it.
    source: PathBuf,
    follow_symlinks: bool,
    immutable: bool,
}

impl Mount {
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn source(&self) -> &Path {
        &self.source
    }

    pub fn follow_symlinks(&self) -> bool {
        self.follow_symlinks
    }

    /// Whether what is published never changes once published, which is what lets a
    /// cached copy be served without asking the filesystem whether it is still current.
    pub fn immutable(&self) -> bool {
        self.immutable
    }
}

/// Every mount, checked against each other. Empty is the ordinary case: the API alone.
#[derive(Debug, Default)]
pub struct Mounts(Vec<Mount>);

impl Mounts {
    pub fn new(configs: &[MountConfig]) -> Result<Self, ConfigError> {
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
            mounts.push(Mount {
                prefix,
                source,
                follow_symlinks: config.follow_symlinks,
                immutable: config.immutable,
            });
        }
        Ok(Self(mounts))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Mount> {
        self.0.iter()
    }

    /// The mount a request path belongs to, and the part of the path inside it — with no
    /// leading slash, so it can be joined onto the source directly.
    ///
    /// At most one mount can match, since overlapping prefixes were refused at startup,
    /// so the order of the list decides nothing here.
    pub fn resolve<'a>(&self, path: &'a str) -> Option<(&Mount, &'a str)> {
        self.0
            .iter()
            .find_map(|mount| within(&mount.prefix, path).map(|rest| (mount, rest)))
    }
}

/// The url prefix as it will be compared: absolute, with no trailing slash unless it is
/// the root, and with nothing in it that a path resolver would have to interpret.
fn normalize_prefix(raw: &str) -> Result<String, String> {
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
fn within<'a>(prefix: &str, path: &'a str) -> Option<&'a str> {
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

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    fn config(path: &str, source: &Path) -> MountConfig {
        MountConfig {
            path: path.to_owned(),
            source: source.display().to_string(),
            follow_symlinks: false,
            immutable: false,
        }
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

    #[test]
    fn overlapping_mounts_are_refused_rather_than_ordered() {
        let dir = TempDir::new().unwrap();
        let mounts = |paths: &[&str]| {
            let configs: Vec<_> = paths.iter().map(|path| config(path, dir.path())).collect();
            Mounts::new(&configs)
        };
        assert!(mounts(&["/hats", "/data"]).is_ok());
        // The same directory under two prefixes is not an overlap: the urls differ.
        assert!(mounts(&["/hats", "/hats-dr1"]).is_ok());
        for overlapping in [
            ["/", "/hats"].as_slice(),
            ["/hats", "/"].as_slice(),
            ["/hats", "/hats"].as_slice(),
            ["/hats", "/hats/"].as_slice(),
            ["/hats", "/hats/dr1"].as_slice(),
        ] {
            let error = mounts(overlapping).unwrap_err().to_string();
            assert!(error.contains("overlap"), "{overlapping:?}: {error}");
        }
    }

    #[test]
    fn a_source_must_be_a_directory_that_exists() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("x.parquet");
        std::fs::write(&file, b"").unwrap();
        assert!(Mounts::new(&[config("/", dir.path())]).is_ok());
        // A file:// url says the same thing as the path, and is accepted the same way.
        assert!(
            Mounts::new(&[MountConfig {
                source: format!("file://{}", dir.path().display()),
                ..config("/", dir.path())
            }])
            .is_ok()
        );
        let missing = dir.path().join("nothing");
        for source in [file.as_path(), missing.as_path()] {
            let error = Mounts::new(&[config("/", source)]).unwrap_err().to_string();
            assert!(error.contains("[[mount]]"), "{error}");
        }
        // A remote source is a startup error rather than a directory nothing can find.
        let error = Mounts::new(&[MountConfig {
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
        let mounts =
            Mounts::new(&[config("/hats", dir.path()), config("/raw", dir.path())]).unwrap();
        let (mount, rest) = mounts.resolve("/hats/dr1/x.parquet").unwrap();
        assert_eq!(mount.prefix(), "/hats");
        assert_eq!(rest, "dr1/x.parquet");
        // The source as resolved, not as written: on macOS the temporary directory is
        // reached through a symlink, and the path that gets opened is the resolved one.
        assert_eq!(mount.source(), std::fs::canonicalize(dir.path()).unwrap());
        assert!(mounts.resolve("/other/x.parquet").is_none());
        assert!(Mounts::default().resolve("/hats/x").is_none());
    }
}
