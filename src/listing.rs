//! What a directory inside a mount answers with.
//!
//! Two renderings of one reading. A browser gets a page it can click through; everything
//! else gets JSON, which is what a client walking the tree can parse. The choice is made
//! from `Accept` and nothing else — no `.json` suffix, no query parameter — so the same
//! url is the same directory whoever asks for it.
//!
//! Names come off the filesystem and go into a url and into HTML, so both encodings
//! happen here rather than at the point of use: a name with a `/`, a `%` or a `<` in it
//! is unusual but perfectly legal, and each of the three breaks a different reader.

use std::path::Path;
use std::time::SystemTime;

use axum::http::HeaderMap;
use axum::http::header::ACCEPT;
use bytesize::ByteSize;
use chrono::{DateTime, SecondsFormat, Utc};
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use serde::Serialize;

/// What a url path segment may not carry literally.
///
/// `/` above all: a name containing one must arrive as part of a name rather than as a
/// separator. `%` for the same reason one step removed — an unencoded one turns the two
/// characters after it into something else. The rest are the delimiters that end a path.
///
/// `=` is deliberately absent: HATS directories are named `Norder=5`, and a listing that
/// spells them `Norder%3D5` is unreadable for no gain.
const SEGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}')
    .add(b'/');

/// A cursor rides in a query string, where `=` and `&` are the delimiters, so it is
/// encoded harder than a path segment is.
const QUERY_VALUE: &AsciiSet = percent_encoding::NON_ALPHANUMERIC;

/// Whether the caller asked for HTML by name.
///
/// Only an exact `text/html` with a non-zero weight counts. A wildcard does not: `*/*`
/// is what every program that is not a browser sends, and answering it with a page
/// would give the JSON reading to nobody. That is also why this is not one of the
/// content-negotiation crates — theirs resolve a wildcard *to* `text/html`, which is
/// the opposite of what a data service wants.
pub fn wants_html(headers: &HeaderMap) -> bool {
    let Some(accept) = headers.get(ACCEPT).and_then(|value| value.to_str().ok()) else {
        return false;
    };
    accept.split(',').any(|offer| {
        let mut parts = offer.split(';').map(str::trim);
        if !parts
            .next()
            .is_some_and(|mime| mime.eq_ignore_ascii_case("text/html"))
        {
            return false;
        }
        // `text/html;q=0` names the type in order to refuse it.
        parts.all(|parameter| match parameter.split_once('=') {
            Some((name, weight)) if name.eq_ignore_ascii_case("q") => weight
                .trim()
                .parse::<f32>()
                .is_ok_and(|weight| weight > 0.0),
            _ => true,
        })
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    File,
    Directory,
}

/// One name in a directory. Anything that is neither a file nor a directory is left out
/// rather than described: a socket or a device node is not something this service can
/// serve, so listing it would only promise a 404.
#[derive(Debug, Serialize)]
pub struct Entry {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: Kind,
    /// Absent for a directory, and for a file that went away between the directory being
    /// read and its size being asked for.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified: Option<String>,
    /// Where this entry is, ready to request. A client walking the tree does not have to
    /// know how a name becomes a url, which is the part with the encoding rules in it.
    pub url: String,
}

/// One page of one directory.
#[derive(Debug, Serialize)]
pub struct Listing {
    /// This directory's own url.
    pub path: String,
    /// The directory above, or `None` at the top of the mount — which is as far up as a
    /// listing goes, whatever is above it on disk.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    pub entries: Vec<Entry>,
    /// The rest of the directory, when it did not fit. Absent means this is all of it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

impl Listing {
    /// Read one page of `dir`, which is `path` in the url space.
    ///
    /// Entries are ordered by name, which is what makes `after` a cursor rather than an
    /// offset: a page is decided by the last name on the one before it, so a file
    /// appearing or disappearing in between shifts nothing.
    pub fn read(
        dir: &Path,
        path: &str,
        parent: Option<String>,
        follow_symlinks: bool,
        after: Option<&str>,
        limit: usize,
    ) -> std::io::Result<Self> {
        // Names first and nothing else: `read_dir` hands out the file type for free on
        // the platforms this runs on, while a size costs a `stat` per entry. Sorting has
        // to see every name, so only the page that survives the sort is worth paying for.
        let mut names: Vec<(String, Kind)> = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            // A name that is not UTF-8 cannot be spelled in a url, so there is no
            // request that could ever reach it.
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            if after.is_some_and(|after| name.as_str() <= after) {
                continue;
            }
            let file_type = entry.file_type()?;
            // A mount that does not follow links will not serve one either, so listing
            // it would describe a 404. A mount that does follow them still refuses a
            // link leading out of itself, which is rarer and is left to the fetch.
            let file_type = match file_type.is_symlink() {
                false => file_type,
                true if !follow_symlinks => continue,
                // `DirEntry::metadata` is an `lstat` and would answer "a symlink"
                // again; following the link is what says whether it is a file or a
                // directory, and a link that dangles is left out.
                true => match std::fs::metadata(entry.path()) {
                    Ok(metadata) => metadata.file_type(),
                    Err(_) => continue,
                },
            };
            let kind = match (file_type.is_dir(), file_type.is_file()) {
                (true, _) => Kind::Directory,
                (_, true) => Kind::File,
                _ => continue,
            };
            names.push((name, kind));
        }
        names.sort_by(|(one, _), (other, _)| one.cmp(other));

        // Counted against the whole directory rather than guessed from the page being
        // exactly full, which would offer a second page that turns out to be empty.
        let more = names.len() > limit;
        names.truncate(limit);
        let next = match more {
            true => names.last().map(|(name, _)| name.clone()),
            false => None,
        };
        let entries = names
            .into_iter()
            .map(|(name, kind)| {
                let url = child(path, &name);
                // The entry is kept when its metadata cannot be read: it was in the
                // directory a moment ago, and a listing missing a name is a worse answer
                // than one missing a size.
                let metadata = std::fs::metadata(dir.join(&name)).ok();
                Entry {
                    name,
                    kind,
                    size: match kind {
                        Kind::Directory => None,
                        Kind::File => metadata.as_ref().map(std::fs::Metadata::len),
                    },
                    modified: metadata
                        .as_ref()
                        .and_then(|metadata| metadata.modified().ok())
                        .map(timestamp),
                    url,
                }
            })
            .collect();
        Ok(Self {
            path: path.to_owned(),
            parent,
            entries,
            next: next
                .map(|name| format!("{path}?after={}", utf8_percent_encode(&name, QUERY_VALUE))),
        })
    }

    /// The same listing as a page. Deliberately plain: the hrefs are absolute, so a
    /// client that scrapes them — `fsspec` reads an HTML index this way — sees the
    /// entries of this directory and not the links that navigate away from it.
    pub fn to_html(&self) -> String {
        let title = html_escape::encode_text(&self.path);
        let mut html = format!(
            "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n\
             <title>Index of {title}</title>\n<style>\nbody {{ font-family: sans-serif; \
             margin: 2rem; }}\ntd, th {{ padding: 0.2rem 1.5rem 0.2rem 0; \
             text-align: left; }}\ntd.size {{ text-align: right; }}\n</style>\n</head>\n\
             <body>\n<h1>Index of {title}</h1>\n<table>\n\
             <tr><th>Name</th><th>Size</th><th>Last modified</th></tr>\n"
        );
        if let Some(parent) = &self.parent {
            html.push_str(&format!(
                "<tr><td><a href=\"{}\">../</a></td><td></td><td></td></tr>\n",
                html_escape::encode_double_quoted_attribute(parent)
            ));
        }
        for entry in &self.entries {
            let slash = match entry.kind {
                Kind::Directory => "/",
                Kind::File => "",
            };
            html.push_str(&format!(
                "<tr><td><a href=\"{href}\">{name}{slash}</a></td>\
                 <td class=\"size\">{size}</td><td>{modified}</td></tr>\n",
                href = html_escape::encode_double_quoted_attribute(&entry.url),
                name = html_escape::encode_text(&entry.name),
                size = entry
                    .size
                    .map(|size| ByteSize(size).to_string())
                    .unwrap_or_default(),
                modified = entry.modified.as_deref().unwrap_or_default(),
            ));
        }
        html.push_str("</table>\n");
        if let Some(next) = &self.next {
            html.push_str(&format!(
                "<p><a href=\"{}\">next page</a></p>\n",
                html_escape::encode_double_quoted_attribute(next)
            ));
        }
        html.push_str("</body>\n</html>\n");
        html
    }
}

/// A directory's url, from the mount's prefix and the path inside it. The segments are
/// the decoded ones, so this is the one spelling of the url whatever the request used.
pub fn url(prefix: &str, segments: &[String]) -> String {
    let mut url = match prefix {
        "/" => String::new(),
        _ => prefix.to_owned(),
    };
    for segment in segments {
        url.push('/');
        url.push_str(&utf8_percent_encode(segment, SEGMENT).to_string());
    }
    match url.is_empty() {
        true => "/".to_owned(),
        false => url,
    }
}

/// A url one level below `base`. The root ends in the separator already, so joining it
/// the way any other directory is joined would give `//name`.
fn child(base: &str, name: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        utf8_percent_encode(name, SEGMENT)
    )
}

/// RFC 3339 in UTC, to the second. The filesystem's resolution is finer than that, and
/// nothing here is deciding an ordering from it.
fn timestamp(time: SystemTime) -> String {
    DateTime::<Utc>::from(time).to_rfc3339_opts(SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    fn accepting(accept: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, accept.parse().unwrap());
        headers
    }

    #[test]
    fn only_a_browser_asking_for_a_page_gets_one() {
        // What a browser sends, in the spellings the four major ones use.
        for accept in [
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,*/*;q=0.8",
            "text/html, application/json",
            "application/json;q=0.9, text/html;q=1.0",
            "TEXT/HTML",
        ] {
            assert!(wants_html(&accepting(accept)), "{accept}");
        }
        // What everything else sends. A wildcard is not a request for a page, and a
        // named type with no weight left is a refusal of it.
        for accept in [
            "*/*",
            "application/json",
            "text/*",
            "text/html;q=0",
            "text/html;q=0.0",
            "application/json, */*;q=0.1",
        ] {
            assert!(!wants_html(&accepting(accept)), "{accept}");
        }
        assert!(!wants_html(&HeaderMap::new()));
    }

    #[test]
    fn a_url_is_built_from_the_prefix_and_the_names_inside_it() {
        let segments = |names: &[&str]| -> Vec<String> {
            names.iter().map(|name| (*name).to_owned()).collect()
        };
        assert_eq!(url("/", &[]), "/");
        assert_eq!(url("/hats", &[]), "/hats");
        assert_eq!(url("/", &segments(&["dr1", "Norder=5"])), "/dr1/Norder=5");
        assert_eq!(url("/hats", &segments(&["dr1"])), "/hats/dr1");
        // The characters that would otherwise end the path, or split one name into two.
        assert_eq!(url("/", &segments(&["a/b"])), "/a%2Fb");
        assert_eq!(url("/", &segments(&["100%"])), "/100%25");
        assert_eq!(url("/", &segments(&["a b?c#d"])), "/a%20b%3Fc%23d");
    }

    /// The root is the case a plain join gets wrong.
    #[test]
    fn a_child_url_has_one_separator() {
        assert_eq!(child("/", "hats"), "/hats");
        assert_eq!(child("/hats", "dr1"), "/hats/dr1");
    }

    fn listing(dir: &TempDir, after: Option<&str>, limit: usize) -> Listing {
        Listing::read(dir.path(), "/", None, false, after, limit).unwrap()
    }

    #[test]
    fn a_directory_lists_its_files_and_directories_in_name_order() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("b.parquet"), b"0123456789").unwrap();
        fs::write(dir.path().join("a.parquet"), b"01").unwrap();
        fs::create_dir(dir.path().join("Norder=5")).unwrap();

        let listing = listing(&dir, None, 10);
        let names: Vec<_> = listing.entries.iter().map(|entry| &*entry.name).collect();
        assert_eq!(names, ["Norder=5", "a.parquet", "b.parquet"]);
        assert_eq!(listing.entries[0].kind, Kind::Directory);
        assert_eq!(listing.entries[0].size, None);
        assert_eq!(listing.entries[0].url, "/Norder=5");
        assert_eq!(listing.entries[1].kind, Kind::File);
        assert_eq!(listing.entries[1].size, Some(2));
        assert!(listing.entries[1].modified.is_some());
        assert_eq!(listing.next, None);
    }

    /// A page ends at the cap, and the cursor is a name rather than a count — so the
    /// pages join up even though the directory is read afresh for each one.
    #[test]
    fn a_directory_larger_than_the_page_is_continued_rather_than_truncated() {
        let dir = TempDir::new().unwrap();
        for index in 0..5 {
            fs::write(dir.path().join(format!("part{index}.parquet")), b"x").unwrap();
        }

        let first = listing(&dir, None, 2);
        assert_eq!(first.entries.len(), 2);
        assert_eq!(first.next.as_deref(), Some("/?after=part1%2Eparquet"));

        let second = Listing::read(dir.path(), "/", None, false, Some("part1.parquet"), 2).unwrap();
        let names: Vec<_> = second.entries.iter().map(|entry| &*entry.name).collect();
        assert_eq!(names, ["part2.parquet", "part3.parquet"]);
        assert!(second.next.is_some());

        let last = Listing::read(dir.path(), "/", None, false, Some("part3.parquet"), 2).unwrap();
        let names: Vec<_> = last.entries.iter().map(|entry| &*entry.name).collect();
        assert_eq!(names, ["part4.parquet"]);
        assert_eq!(last.next, None, "the last page does not offer another");
    }

    /// A directory of exactly one page is complete, not the first of two: the reading
    /// looks one entry past the cap rather than inferring it from a full page.
    #[test]
    fn a_directory_that_exactly_fills_a_page_offers_no_next() {
        let dir = TempDir::new().unwrap();
        for index in 0..2 {
            fs::write(dir.path().join(format!("part{index}.parquet")), b"x").unwrap();
        }
        assert_eq!(listing(&dir, None, 2).next, None);
    }

    /// What the mount will not serve is not advertised either.
    #[cfg(unix)]
    #[test]
    fn a_link_is_listed_only_where_it_would_be_served() {
        let dir = TempDir::new().unwrap();
        let published = dir.path().join("published");
        fs::create_dir(&published).unwrap();
        fs::write(dir.path().join("secret.parquet"), b"secret").unwrap();
        std::os::unix::fs::symlink(
            dir.path().join("secret.parquet"),
            published.join("innocent.parquet"),
        )
        .unwrap();

        let listed = |follow| {
            Listing::read(&published, "/", None, follow, None, 10)
                .unwrap()
                .entries
                .len()
        };
        assert_eq!(listed(false), 0);
        assert_eq!(listed(true), 1);
    }

    /// A name is the filesystem's, and the filesystem allows what HTML reads as markup.
    #[test]
    fn a_name_that_looks_like_markup_is_not_markup() {
        let dir = TempDir::new().unwrap();
        // A `/` is the one character a name cannot hold, so the closing tag is spelled
        // the way a filesystem would actually let someone spell it.
        fs::write(dir.path().join("<script>alert(1)<script>"), b"x").unwrap();
        fs::write(dir.path().join("a\"b.parquet"), b"x").unwrap();

        let html = listing(&dir, None, 10).to_html();
        assert!(!html.contains("<script>"), "{html}");
        assert!(html.contains("&lt;script&gt;"), "{html}");
        // And the quote does not end the href it sits in.
        assert!(html.contains("href=\"/a%22b.parquet\""), "{html}");
    }
}
