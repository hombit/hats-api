//! What a request may read from local disk: a path under a mount, and nothing a symlink or
//! a `..` leads to from there.
//!
//! Under the mount, two checks an endpoint does not need, because a filesystem has ways
//! of pointing outside itself. The path is resolved before it is matched, so neither `..`
//! nor a symlink inside the mount can lead out of it; and unless the mount's
//! `follow_symlinks` is on, a path that goes through a symlink at all is refused.

use std::path::{Component, Path, PathBuf};

use url::Url;

use crate::access::mount::{self, Mount};
use crate::access::policy::describe;
use crate::access::{AccessPolicy, LOCAL_SCHEME};
use crate::error::ApiError;

impl AccessPolicy {
    /// A `file://` url, which names a place in the mounts' url space rather than on the
    /// disk. So the answer comes in two steps: which mount the caller named, and then
    /// what that mount's own rules make of the path inside it.
    ///
    /// Nothing the caller is told here names a `source`. A mount's `path` is what the
    /// caller wrote and what an operator published; where it is on the disk is neither.
    pub(super) fn authorize_local(&self, url: &Url) -> Result<PathBuf, ApiError> {
        if self.mounts.is_empty() {
            return Err(ApiError::forbidden(
                "this server reads no local files; add a [[mount]] to change that",
            ));
        }
        // Not `to_file_path`: what follows the authority is a url path, and reading it as
        // one is what keeps `file:///hats/x` the same address in both modes.
        if url.host().is_some() {
            return Err(ApiError::bad_request(format!(
                "url {url} names a host; a local url is file:// followed by a mount's path"
            )));
        }
        let Some((mount, relative)) = self.mounts.resolve(url.path()) else {
            return Err(self.local_refusal(url));
        };
        let mut requested = mount.source().to_owned();
        requested.extend(mount::path_segments(relative)?);

        resolve_under(mount, &requested).map_err(|refusal| {
            // The path is the operator's; the reason is the caller's, since they named
            // the url and can act on every one of these.
            tracing::debug!(mount = mount.prefix(), reason = %refusal, "not read");
            match refusal {
                // Under the mount as written and outside it once resolved: a symlink led
                // out of what the mount publishes.
                LocalRefusal::NotAllowed | LocalRefusal::Symlink(_) => ApiError::forbidden(
                    format!("{url} goes through a symlink this mount does not follow"),
                ),
                LocalRefusal::NotFound(_) => ApiError::not_found(format!("{url} does not exist")),
                LocalRefusal::Unreadable(..) => {
                    ApiError::forbidden(format!("{url} cannot be read"))
                }
            }
        })
    }

    /// A url under no mount. It says which prefixes there are, because those are urls
    /// this server publishes and a caller has to be able to find out what to write.
    fn local_refusal(&self, url: &Url) -> ApiError {
        let prefixes: Vec<String> = self
            .mounts
            .iter()
            .map(|mount| format!("file://{}", mount.prefix()))
            .collect();
        ApiError::forbidden(format!(
            "{url} is not under any mount; this server reads {}",
            describe(&prefixes)
        ))
    }
}

/// The directory an entry names, resolved: an absolute path or a `file://` url, and a
/// directory that is there now rather than a rule that silently never matches.
///
/// Returns the reason rather than a [`ConfigError`](crate::config::ConfigError), so that the
/// caller names the section it read the entry out of.
pub(crate) fn canonical_root(entry: &str) -> Result<PathBuf, String> {
    let path = if entry.starts_with('/') {
        PathBuf::from(entry)
    } else {
        let url = Url::parse(entry)
            .map_err(|error| format!("{error}; expected an absolute path or a file:// url"))?;
        if url.scheme() != LOCAL_SCHEME {
            return Err(format!(
                "scheme {:?} is not a local path; expected an absolute path or a \
                 file:// url",
                url.scheme()
            ));
        }
        url.to_file_path()
            .map_err(|()| "not an absolute local path".to_owned())?
    };

    let canonical = std::fs::canonicalize(&path)
        .map_err(|error| format!("cannot resolve {}: {error}", path.display()))?;
    match canonical.is_dir() {
        true => Ok(canonical),
        false => Err(format!("{} is not a directory", canonical.display())),
    }
}

/// The file a path under a mount names, for the file server.
///
/// Every refusal is the same answer. A caller here walked into the directory rather than
/// naming it, so which file is missing, which is outside the mount and which is behind a
/// symlink are all distinctions about a filesystem they were never shown. The API says
/// more, because there the caller wrote the url and can act on each of them.
pub fn authorize_mounted(mount: &Mount, path: &Path) -> Result<PathBuf, ApiError> {
    resolve_under(mount, path).map_err(|refusal| {
        // The reason belongs in the log, where the operator can see it, and not in the
        // response.
        tracing::debug!(mount = mount.prefix(), reason = %refusal, "not served");
        ApiError::not_found("no such file")
    })
}

/// Why a path is not a file that may be read. Separate from [`ApiError`] because how
/// much a refusal may say differs by mode.
enum LocalRefusal {
    /// Outside the mount.
    NotAllowed,
    /// Inside it, but nothing is there.
    NotFound(PathBuf),
    /// It goes through a symlink and the mount does not follow them.
    Symlink(PathBuf),
    Unreadable(PathBuf, std::io::Error),
}

/// For the log, which is the operator's. What a caller is told is decided where the
/// refusal is turned into an [`ApiError`].
impl std::fmt::Display for LocalRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAllowed => f.write_str("outside the mount"),
            Self::NotFound(path) => write!(f, "{} does not exist", path.display()),
            Self::Symlink(path) => write!(f, "{} goes through a symlink", path.display()),
            Self::Unreadable(path, error) => write!(f, "cannot read {}: {error}", path.display()),
        }
    }
}

/// The file a path names, resolved and checked against the one mount that governs it.
///
/// One mount and not a list of them: a path arrives here having already been matched to
/// its mount by prefix, and resolving it against any other would let one mount serve a
/// file out of another's directory.
fn resolve_under(mount: &Mount, path: &Path) -> Result<PathBuf, LocalRefusal> {
    let root = mount.source();
    // `..` resolved without touching the filesystem, so that comparing the result with
    // the canonical path afterwards is a question about symlinks and nothing else.
    let lexical = lexically_clean(path).ok_or(LocalRefusal::NotAllowed)?;
    // Without symlink resolution the path as written is the path that gets opened, so a
    // path outside the mount is settled before the filesystem is touched at all — and
    // then gets the same answer whether or not it exists.
    let inside = lexical.starts_with(root);
    if !inside && !mount.follow_symlinks() {
        return Err(LocalRefusal::NotAllowed);
    }
    let canonical = std::fs::canonicalize(&lexical).map_err(|error| match error.kind() {
        // Saying "no such file" about a path the caller was never allowed to name would
        // answer a question they did not get to ask.
        _ if !inside => LocalRefusal::NotAllowed,
        std::io::ErrorKind::NotFound => LocalRefusal::NotFound(lexical.clone()),
        _ => LocalRefusal::Unreadable(lexical.clone(), error),
    })?;
    // Again on the resolved path: a link inside the mount must still not lead out of it.
    if !canonical.starts_with(root) {
        return Err(LocalRefusal::NotAllowed);
    }
    if !mount.follow_symlinks() && canonical != lexical {
        return Err(LocalRefusal::Symlink(lexical));
    }
    Ok(canonical)
}

/// Resolve `.` and `..` without touching the filesystem, so that the result can be
/// compared with the canonical path to tell whether a symlink was involved.
fn lexically_clean(path: &Path) -> Option<PathBuf> {
    let mut clean = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                // Above the root: there is nothing there to allow.
                if !clean.pop() {
                    return None;
                }
            }
            other => clean.push(other),
        }
    }
    Some(clean)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use crate::access::Target;
    use crate::access::mount::Mounts;
    use crate::access::policy::tests::{temp_dir, url, with_paths};
    use crate::config::{AccessConfig, DataConfig, MountConfig};

    use super::*;

    /// The url a path under the `n`th of those mounts is named by. Built a segment at a
    /// time, which is what encodes each name — a test may pass one with a space in it.
    fn mounted_url(n: usize, relative: &str) -> Url {
        let mut url = Url::parse("file:///").unwrap();
        url.path_segments_mut()
            .unwrap()
            // `file:///` already has one empty segment, and pushing onto it would give
            // `//0` rather than `/0`.
            .pop_if_empty()
            .push(&n.to_string())
            .extend(relative.split('/'));
        url
    }

    fn file_url(path: &Path) -> Url {
        Url::from_file_path(path).unwrap()
    }

    #[test]
    fn a_mount_allows_what_is_under_it_and_nothing_else() {
        let (_dir, root) = temp_dir();
        let published = root.join("published");
        fs::create_dir(&published).unwrap();
        let inside = published.join("part0.parquet");
        fs::write(&inside, b"").unwrap();
        let outside = root.join("outside.parquet");
        fs::write(&outside, b"").unwrap();

        let policy = with_paths(&[&published], false);
        assert_eq!(
            policy.authorize(&mounted_url(0, "part0.parquet")).unwrap(),
            Target::Local(inside)
        );
        // Mounting a directory is not a way to read its parent. `Url` resolves `..`
        // itself, percent-encoded or not, so what arrives here is already a path outside
        // the mount rather than one that climbs out of it.
        for climb in [
            "file:///0/../outside.parquet",
            "file:///0/%2E%2E/outside.parquet",
        ] {
            let error = policy.authorize(&url(climb)).unwrap_err();
            assert!(matches!(error, ApiError::Forbidden(_)), "{climb}: {error}");
        }
        assert!(policy.authorize(&url("file:///1/x.parquet")).is_err());
    }

    /// The headline of the design: `path` is the address and `source` is not. A caller
    /// writing where the directory really is on the disk is writing a url no mount
    /// claims, whether or not the file is there.
    #[test]
    fn the_api_cannot_name_a_mount_by_where_it_is_on_the_disk() {
        let (_dir, root) = temp_dir();
        let published = root.join("published");
        fs::create_dir(&published).unwrap();
        let inside = published.join("part0.parquet");
        fs::write(&inside, b"").unwrap();

        let policy = with_paths(&[&published], false);
        assert!(policy.authorize(&mounted_url(0, "part0.parquet")).is_ok());
        let error = policy.authorize(&file_url(&inside)).unwrap_err();
        assert!(matches!(error, ApiError::Forbidden(_)), "{error}");
        // The refusal says which urls there are, since those a caller may write.
        assert!(error.to_string().contains("file:///0"), "{error}");
        // And says nothing about where they are. This url quotes back only itself, so
        // the source appearing in the message could only have come from the mount list.
        let error = policy.authorize(&url("file:///elsewhere/x")).unwrap_err();
        assert!(
            !error.to_string().contains(&published.display().to_string()),
            "{error}"
        );
    }

    #[test]
    fn a_missing_file_under_a_mount_is_a_404() {
        let (_dir, root) = temp_dir();
        let policy = with_paths(&[&root], false);
        let error = policy
            .authorize(&mounted_url(0, "nope.parquet"))
            .unwrap_err();
        assert!(matches!(error, ApiError::NotFound(_)), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_is_refused_unless_the_mount_follows_them() {
        let (_dir, root) = temp_dir();
        let real = root.join("part0.parquet");
        fs::write(&real, b"").unwrap();
        let link = root.join("link.parquet");
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let refuses = with_paths(&[&root], false);
        let error = refuses
            .authorize(&mounted_url(0, "link.parquet"))
            .unwrap_err();
        assert!(matches!(error, ApiError::Forbidden(_)), "{error}");
        assert!(error.to_string().contains("symlink"), "{error}");

        // Following it lands on the real file, which is what gets opened.
        let follows = with_paths(&[&root], true);
        assert_eq!(
            follows.authorize(&mounted_url(0, "link.parquet")).unwrap(),
            Target::Local(real)
        );
    }

    /// The spelling of a mount's own source may go through a symlink without the *file*
    /// being anywhere unexpected — `/tmp` is a link to `/private/tmp` on macOS. Which is
    /// why a source is canonicalized at startup: the path that gets opened is the
    /// resolved one, and it is what the file is then judged against.
    #[cfg(unix)]
    #[test]
    fn a_linked_spelling_of_a_source_is_resolved_at_startup() {
        let (_dir, root) = temp_dir();
        let real = root.join("data");
        fs::create_dir(&real).unwrap();
        let target = real.join("part0.parquet");
        fs::write(&target, b"").unwrap();
        let linked_root = root.join("link");
        std::os::unix::fs::symlink(&real, &linked_root).unwrap();

        let policy = with_paths(&[&linked_root], false);
        assert_eq!(
            policy.authorize(&mounted_url(0, "part0.parquet")).unwrap(),
            Target::Local(target)
        );
    }

    /// The point of resolving before matching: a link inside a mount is still not a way
    /// out of it, even for a mount that follows links.
    #[cfg(unix)]
    #[test]
    fn following_symlinks_does_not_let_one_escape_the_mount() {
        let (_dir, root) = temp_dir();
        let published = root.join("published");
        fs::create_dir(&published).unwrap();
        let secret = root.join("secret.parquet");
        fs::write(&secret, b"").unwrap();
        let link = published.join("innocent.parquet");
        std::os::unix::fs::symlink(&secret, &link).unwrap();

        let follows = with_paths(&[&published], true);
        let error = follows
            .authorize(&mounted_url(0, "innocent.parquet"))
            .unwrap_err();
        assert!(matches!(error, ApiError::Forbidden(_)), "{error}");
    }

    /// Two mounts, two answers about the same question, because the rule that decides
    /// it is the mount's rather than the service's — and because a link out of one
    /// mount is refused even when the file it points at is inside another. A mount that
    /// follows links is not a way into a mount that does not.
    #[cfg(unix)]
    #[test]
    fn a_mount_brings_its_own_symlink_rule_and_nobody_else_s_directory() {
        let (_dir, root) = temp_dir();
        let strict = root.join("strict");
        let loose = root.join("loose");
        fs::create_dir(&strict).unwrap();
        fs::create_dir(&loose).unwrap();
        let real = strict.join("part0.parquet");
        fs::write(&real, b"").unwrap();
        fs::write(loose.join("part0.parquet"), b"").unwrap();
        std::os::unix::fs::symlink(&real, strict.join("link.parquet")).unwrap();
        std::os::unix::fs::symlink(&real, loose.join("link.parquet")).unwrap();

        let policy = AccessPolicy::new(
            &AccessConfig::default(),
            Arc::new(
                Mounts::new(
                    &[
                        MountConfig {
                            path: "/0".to_owned(),
                            source: strict.display().to_string(),
                            serve: false,
                            follow_symlinks: false,
                            immutable: false,
                            filenames: None,
                        },
                        MountConfig {
                            path: "/1".to_owned(),
                            source: loose.display().to_string(),
                            serve: false,
                            follow_symlinks: true,
                            immutable: false,
                            filenames: None,
                        },
                    ],
                    &DataConfig::default(),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        // The same name under each, and the mount decides.
        assert!(policy.authorize(&mounted_url(0, "link.parquet")).is_err());
        // The loose mount follows its link, and the link leaves the mount, so it is
        // refused for that instead of quietly reading the strict mount's file.
        assert!(policy.authorize(&mounted_url(1, "link.parquet")).is_err());
        // Each still reads its own file.
        assert_eq!(
            policy.authorize(&mounted_url(0, "part0.parquet")).unwrap(),
            Target::Local(real)
        );
        assert!(policy.authorize(&mounted_url(1, "part0.parquet")).is_ok());
    }
}
