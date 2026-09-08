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

use std::fmt::Write as _;
use std::path::Path;
use std::time::SystemTime;

use axum::http::HeaderMap;
use axum::http::header::ACCEPT;
use bytesize::ByteSize;
use chrono::{DateTime, SecondsFormat, Utc};
use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str, utf8_percent_encode};
use serde::Serialize;

use crate::data::DataFiles;

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

/// Whether the caller named `text/html`. Everything else, `*/*` included, gets JSON:
/// a wildcard is what every program that is not a browser sends.
pub fn wants_html(headers: &HeaderMap) -> bool {
    headers
        .get(ACCEPT)
        .and_then(|accept| accept.to_str().ok())
        .is_some_and(|accept| {
            accept.split(',').any(|offer| {
                // `text/html;q=0.9` is the type with a weight on it, still the type.
                let mime = offer.split(';').next().unwrap_or(offer);
                mime.trim().eq_ignore_ascii_case("text/html")
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

/// One directory, whole.
#[derive(Debug, Serialize)]
pub struct Listing {
    /// This directory's own url.
    pub path: String,
    /// The directory above, or `None` at the top of the mount — which is as far up as a
    /// listing goes, whatever is above it on disk.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    pub entries: Vec<Entry>,
    /// The mount's own url, which is how far up this listing may point. Not part of the
    /// answer: it says nothing a client walking the tree from the mount does not know.
    #[serde(skip)]
    root: String,
}

impl Listing {
    /// Read `dir`, which is `path` in the url space, below the mount published at `root`.
    ///
    /// Every entry, ordered by name, the way `apache` and `nginx` list a directory. A cap
    /// would have to either truncate — which reads to a client as the files past it not
    /// existing — or page, and a paged listing is one more thing every client has to know
    /// about a plain file server. The cost is a `readdir` and a `stat` per entry, both
    /// paid against a directory the operator chose to publish.
    pub fn read(
        dir: &Path,
        root: &str,
        path: &str,
        follow_symlinks: bool,
    ) -> std::io::Result<Self> {
        // Names first and nothing else: `read_dir` hands out the file type for free on
        // the platforms this runs on, while a size costs a `stat` per entry, and sorting
        // has to see every name before any of them is worth asking about.
        let mut names: Vec<(String, Kind)> = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            // A name that is not UTF-8 cannot be spelled in a url, so there is no
            // request that could ever reach it.
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
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
            parent: parent(root, path),
            entries,
            root: root.to_owned(),
        })
    }

    /// The same listing as a page.
    ///
    /// `fsspec` reads a directory by pulling every `href` out of this markup and keeping
    /// the ones below the url it asked for, which decides two things about the page. Each
    /// entry stays a plain `<a href>` an expression can find, rather than a link a script
    /// builds. And nothing else on the page may link below this directory: the breadcrumb
    /// and the parent row point upwards and are dropped, but a link offering a query on an
    /// entry would be scraped as a file that is not there — which is why the query surface
    /// is described in prose instead.
    ///
    /// Everything the page needs is in it. A style sheet or a font from elsewhere would
    /// leave the page unstyled on exactly the networks this service is built for, and the
    /// script is an improvement on markup that is already complete without it — which is
    /// why the button it wires up is hidden until it runs, while the prose that says the
    /// same thing about the url is served either way.
    pub fn to_html(&self, data_files: &DataFiles, show_version: bool) -> String {
        let title = html_escape::encode_text(&self.path);
        let mut html = format!(
            "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n\
             <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
             <title>Index of {title}</title>\n<style>\n{STYLE}</style>\n</head>\n<body>\n\
             <h1>Index of {breadcrumb}</h1>\n<p class=\"summary\">{summary}</p>\n\
             <table class=\"listing\">\n\
             <thead><tr><th>Name</th><th class=\"ask-cell\"></th>\
             <th class=\"size\">Size</th><th>Last modified</th></tr></thead>\n<tbody>\n",
            breadcrumb = self.breadcrumb(),
            summary = self.summary(),
        );
        // The stripe is a class rather than `nth-child`, since the script inserts a row
        // of its own between two entries and every row below it would change colour.
        let mut stripe = ["", " odd"].into_iter().cycle();
        if let Some(parent) = &self.parent {
            let _ = writeln!(
                html,
                "<tr class=\"entry{stripe}\"><td class=\"name\"><a href=\"{parent}\">../</a>\
                 </td><td class=\"ask-cell\"></td><td></td><td></td></tr>",
                stripe = stripe.next().unwrap_or_default(),
                parent = html_escape::encode_double_quoted_attribute(parent)
            );
        }
        let mut queryable = false;
        for entry in &self.entries {
            let slash = match entry.kind {
                Kind::Directory => "/",
                Kind::File => "",
            };
            // A name on the configured list is one this service will answer a question
            // about. Marking it is the only place a caller browsing a partition could
            // learn that the file's own url takes parameters, and the button is where the
            // script puts the panel that writes one. The button has a column of its own so
            // that a directory of partitions has one line of them rather than a ragged one
            // that follows however long each name happens to be.
            let data = entry.kind == Kind::File && data_files.matches(&entry.name);
            queryable |= data;
            let _ = writeln!(
                html,
                "<tr class=\"entry{stripe}\"><td class=\"name\">\
                 <a{class} href=\"{href}\">{name}{slash}</a></td>\
                 <td class=\"ask-cell\">{ask}</td>\
                 <td class=\"size\">{size}</td><td class=\"modified\">{modified}</td></tr>",
                stripe = stripe.next().unwrap_or_default(),
                class = match data {
                    true => " class=\"data\"",
                    false => "",
                },
                ask = match data {
                    true => format!(
                        "<button class=\"ask\" data-url=\"{href}\">query</button>",
                        href = html_escape::encode_double_quoted_attribute(&entry.url)
                    ),
                    false => String::new(),
                },
                href = html_escape::encode_double_quoted_attribute(&entry.url),
                name = html_escape::encode_text(&entry.name),
                size = entry
                    .size
                    .map(|size| ByteSize(size).to_string())
                    .unwrap_or_default(),
                // UTC, said in the text rather than left to be worked out. A local time
                // is a different instant for every reader and the same string for all of
                // them, which for a timestamp on a data file is worse than a `Z` to read
                // past. The attribute keeps the machine-readable spelling.
                modified = match &entry.modified {
                    Some(modified) => format!(
                        "<time datetime=\"{attribute}\">{shown}</time>",
                        attribute = html_escape::encode_double_quoted_attribute(modified),
                        shown = html_escape::encode_text(&readable(modified)),
                    ),
                    None => String::new(),
                },
            );
        }
        html.push_str("</tbody>\n</table>\n");
        if queryable {
            html.push_str(QUERY_NOTE);
        }
        if show_version {
            let _ = writeln!(html, "<p class=\"footer\">{SERVER}</p>");
        }
        let _ = write!(html, "<script>\n{SCRIPT}</script>\n");
        html.push_str("</body>\n</html>\n");
        html
    }

    /// The heading, with every level above this one a link. Climbing out of a `Npix=`
    /// directory is then one click per level rather than one per level per browser
    /// button.
    fn breadcrumb(&self) -> String {
        let mut html = format!(
            "<a href=\"{href}\">{root}</a>",
            href = html_escape::encode_double_quoted_attribute(&self.root),
            root = html_escape::encode_text(&self.root),
        );
        let mut at = self.root.trim_end_matches('/').to_owned();
        for (index, segment) in relative(&self.root, &self.path).enumerate() {
            at.push('/');
            at.push_str(segment);
            let _ = write!(
                html,
                "{separator}<a href=\"{href}\">{name}</a>",
                // A mount publishing the whole url space is spelled `/`, which is the
                // separator before its first child already.
                separator = match index == 0 && self.root.ends_with('/') {
                    true => "",
                    false => "/",
                },
                href = html_escape::encode_double_quoted_attribute(&at),
                name = html_escape::encode_text(&decode(segment)),
            );
        }
        html
    }

    /// How much is here, which is what a `Dir=` level of ten thousand entries cannot say
    /// by being scrolled.
    fn summary(&self) -> String {
        let files = self
            .entries
            .iter()
            .filter(|entry| entry.kind == Kind::File)
            .count();
        let directories = self.entries.len() - files;
        let bytes: u64 = self.entries.iter().filter_map(|entry| entry.size).sum();
        format!(
            "{directories} {}, {files} {}, {bytes}",
            plural(directories, "directory", "directories"),
            plural(files, "file", "files"),
            bytes = ByteSize(bytes),
        )
    }
}

/// The page's own style sheet and its own script, inlined into every listing. Kept as
/// files so that they are read as what they are; neither is written from anything in a
/// request, so both are constants from this side.
const STYLE: &str = include_str!("listing/page.css");
const SCRIPT: &str = include_str!("listing/page.js");

/// What answered, at the foot of the page, the way `apache` and `nginx` sign a listing
/// they generated. It says which software and which version a page came from, which is
/// what someone reporting that a listing looks wrong has to be able to say. Nothing about
/// the machine: the name and the version are this build's, and the host is the caller's
/// own url.
const SERVER: &str = concat!(
    env!("CARGO_PKG_NAME"),
    " ",
    env!("CARGO_PKG_VERSION"),
    " \u{2014} generated listing"
);

/// What a caller browsing to a partition has no other way to find out. Written once
/// below the table rather than per row: a `Norder=` level is thousands of entries, and
/// the sentence is the same for all of them.
///
/// It describes the url rather than the panel, since the url is what a caller writes into
/// a client — and it is all the page has to offer where the script did not run. Which of
/// the two openings is shown is the style sheet's, so that the sentence names something
/// the reader can actually see: the mark, or the button that replaces it.
const QUERY_NOTE: &str = "<p><span class=\"without-js\">Files marked \u{25c6}</span>\
<span class=\"with-js\">Files with a query button</span> answer a query on their own url: \
<code>?columns=ra,dec&amp;filters=ra&gt;10 AND dec&lt;20</code>, with \
<code>&amp;format=parquet</code> and <code>&amp;limit=</code>.</p>\n";

/// The directory above, or `None` at the top of the mount — which is as far up as a
/// listing goes, whatever is above it on disk. Derived from the two urls rather than
/// passed in, so that it cannot disagree with the breadcrumb over where the mount ends.
fn parent(root: &str, path: &str) -> Option<String> {
    if path.trim_end_matches('/') == root.trim_end_matches('/') {
        return None;
    }
    let above = path.get(..path.rfind('/')?)?;
    match above.len() < root.trim_end_matches('/').len() {
        true => None,
        false => Some(match above.is_empty() {
            true => "/".to_owned(),
            false => above.to_owned(),
        }),
    }
}

/// The segments of `path` below `root`, still encoded as they are in the url.
fn relative<'a>(root: &str, path: &'a str) -> impl Iterator<Item = &'a str> {
    path.get(root.trim_end_matches('/').len()..)
        .unwrap_or_default()
        .split('/')
        .filter(|segment| !segment.is_empty())
}

/// A url segment as the name it stands for. Undecodable bytes cannot have come from a
/// name this service listed, so the encoded form is a truthful last resort.
fn decode(segment: &str) -> String {
    percent_decode_str(segment)
        .decode_utf8()
        .map_or_else(|_| segment.to_owned(), |name| name.into_owned())
}

fn plural<'a>(count: usize, one: &'a str, many: &'a str) -> &'a str {
    match count {
        1 => one,
        _ => many,
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

/// The same instant for a person to read, still UTC and saying so. Parsed back from the
/// answer's own spelling rather than formatted again from the clock, so the page and the
/// JSON cannot come to differ; a timestamp this crate did not write is left as it is.
fn readable(timestamp: &str) -> String {
    DateTime::parse_from_rfc3339(timestamp).map_or_else(
        |_| timestamp.to_owned(),
        |at| {
            at.with_timezone(&Utc)
                .format("%Y-%m-%d %H:%M:%S UTC")
                .to_string()
        },
    )
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

    fn listing(dir: &TempDir) -> Listing {
        Listing::read(dir.path(), "/", "/", false).unwrap()
    }

    #[test]
    fn a_directory_lists_its_files_and_directories_in_name_order() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("b.parquet"), b"0123456789").unwrap();
        fs::write(dir.path().join("a.parquet"), b"01").unwrap();
        fs::create_dir(dir.path().join("Norder=5")).unwrap();

        // A time is UTC in the answer and UTC on the page, and the page says which:
        // a local time is a different instant for every reader and the same text for
        // all of them.
        let html = listing(&dir).to_html(&DataFiles::default(), true);
        assert!(html.contains(" UTC</time>"), "{html}");
        assert!(!html.contains("toLocaleString"), "{html}");

        let listing = listing(&dir);
        let names: Vec<_> = listing.entries.iter().map(|entry| &*entry.name).collect();
        assert_eq!(names, ["Norder=5", "a.parquet", "b.parquet"]);
        assert_eq!(listing.entries[0].kind, Kind::Directory);
        assert_eq!(listing.entries[0].size, None);
        assert_eq!(listing.entries[0].url, "/Norder=5");
        assert_eq!(listing.entries[1].kind, Kind::File);
        assert_eq!(listing.entries[1].size, Some(2));
        assert!(listing.entries[1].modified.is_some());
    }

    /// A directory arrives whole, however many entries it has: nothing is held back for
    /// a second request, so a client that reads one listing has read the directory.
    #[test]
    fn a_large_directory_is_listed_in_full() {
        let dir = TempDir::new().unwrap();
        for index in 0..2_000 {
            fs::write(dir.path().join(format!("Npix={index}.parquet")), b"x").unwrap();
        }
        assert_eq!(listing(&dir).entries.len(), 2_000);
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
            Listing::read(&published, "/", "/", follow)
                .unwrap()
                .entries
                .len()
        };
        assert_eq!(listed(false), 0);
        assert_eq!(listed(true), 1);
    }

    /// A listing goes no higher than the mount, in the heading as in the `parent` field:
    /// what is above a mount on disk is the operator's business, and the url above one is
    /// not this mount's to offer.
    #[test]
    fn a_page_links_every_level_down_to_the_mount_and_no_further() {
        let dir = TempDir::new().unwrap();
        let listing = Listing::read(dir.path(), "/hats", "/hats/dr1/Norder=5", false).unwrap();
        assert_eq!(listing.parent.as_deref(), Some("/hats/dr1"));
        assert_eq!(
            listing.breadcrumb(),
            "<a href=\"/hats\">/hats</a>/<a href=\"/hats/dr1\">dr1</a>\
             /<a href=\"/hats/dr1/Norder=5\">Norder=5</a>"
        );

        // A mount publishing the whole url space is the case a plain join gets wrong at
        // both ends.
        let root = Listing::read(dir.path(), "/", "/", false).unwrap();
        assert_eq!(root.parent, None);
        assert_eq!(root.breadcrumb(), "<a href=\"/\">/</a>");
        let below = Listing::read(dir.path(), "/", "/dr1", false).unwrap();
        assert_eq!(below.parent.as_deref(), Some("/"));
        assert_eq!(
            below.breadcrumb(),
            "<a href=\"/\">/</a><a href=\"/dr1\">dr1</a>"
        );
    }

    /// The one thing on the page that could tell a caller their url takes parameters,
    /// and it is said only where it is true.
    #[test]
    fn a_file_that_can_be_queried_says_so() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("properties"), b"x").unwrap();
        let plain = listing(&dir).to_html(&DataFiles::default(), true);
        assert!(!plain.contains("columns="), "{plain}");

        fs::write(dir.path().join("part0.parquet"), b"x").unwrap();
        let html = listing(&dir).to_html(&DataFiles::default(), true);
        assert!(html.contains("columns="), "{html}");
        assert!(
            html.contains("<a class=\"data\" href=\"/part0.parquet\""),
            "{html}"
        );
        assert!(html.contains("<a href=\"/properties\""), "{html}");
        // The panel is offered for the one file it can ask a question about, and the
        // button is not a link: a client scraping this page would read one as an entry.
        assert_eq!(html.matches("class=\"ask\"").count(), 1, "{html}");
        // The page says what generated it, unless the operator would rather it did not.
        assert!(html.contains(env!("CARGO_PKG_VERSION")), "{html}");
        let unsigned = listing(&dir).to_html(&DataFiles::default(), false);
        assert!(!unsigned.contains(env!("CARGO_PKG_VERSION")), "{unsigned}");
        assert!(
            html.contains("<button class=\"ask\" data-url=\"/part0.parquet\">"),
            "{html}"
        );
    }

    /// `fsspec` reads this page by keeping every href below the url it asked for, so a
    /// link that is not an entry — the breadcrumb, the parent — has to point elsewhere,
    /// and a link offering a query on an entry would read as a file that is not there.
    #[test]
    fn the_only_links_below_this_directory_are_its_entries() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("part0.parquet"), b"x").unwrap();
        fs::create_dir(dir.path().join("Norder=5")).unwrap();
        let listing = Listing::read(dir.path(), "/hats", "/hats/dr1", false).unwrap();

        let html = listing.to_html(&DataFiles::default(), true);
        let scraped: Vec<&str> = html
            .split("<a")
            .skip(1)
            .filter_map(|tag| tag.split_once("href=\""))
            .filter_map(|(_, rest)| rest.split('"').next())
            .filter(|href| href.starts_with("/hats/dr1/"))
            .collect();
        let entries: Vec<&str> = listing.entries.iter().map(|entry| &*entry.url).collect();
        assert_eq!(scraped, entries);
    }

    /// A name is the filesystem's, and the filesystem allows what HTML reads as markup.
    #[test]
    fn a_name_that_looks_like_markup_is_not_markup() {
        let dir = TempDir::new().unwrap();
        // A `/` is the one character a name cannot hold, so the closing tag is spelled
        // the way a filesystem would actually let someone spell it.
        fs::write(dir.path().join("<script>alert(1)<script>"), b"x").unwrap();
        fs::write(dir.path().join("a\"b.parquet"), b"x").unwrap();

        let html = listing(&dir).to_html(&DataFiles::default(), true);
        // The page has a `<script>` of its own, so what says the name did not become
        // markup is that no tag opened where the name is.
        assert!(!html.contains("<script>alert"), "{html}");
        assert!(html.contains("&lt;script&gt;"), "{html}");
        // And the quote does not end the href it sits in.
        assert!(html.contains("href=\"/a%22b.parquet\""), "{html}");
    }
}
