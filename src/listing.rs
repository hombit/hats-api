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

/// The catalog this directory belongs to, as the page needs to describe it.
///
/// A catalog is browsed from the inside — `dataset/Norder=5/Dir=0` is where the files are —
/// so the url here is the catalog's own and not this directory's, and a reader who has
/// climbed down into a partition is still offered the search over the whole thing.
#[derive(Debug, Clone, Copy, Default)]
pub struct Catalog<'a> {
    /// `None` where this directory is neither a catalog nor inside one.
    pub url: Option<&'a str>,
    /// What the catalog calls itself, and how much of it there is. Every part optional: it
    /// is the catalog introducing itself, and a key it does not carry is a line the page
    /// does not write.
    pub name: Option<&'a str>,
    pub rows: Option<u64>,
    pub order: Option<u8>,
    /// Where the catalog's columns can be read without choosing a partition, which is
    /// `dataset/_common_metadata` where the catalog has one. The page asks it the same
    /// question it asks a parquet file, so the column list is there before the first
    /// search rather than after it.
    pub schema_url: Option<&'a str>,
    /// The widest circle that url will answer, which is what the page must not offer more
    /// than: a form that writes a request the service refuses is worse than no form.
    pub max_radius_arcsec: f64,
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
    /// `api_prefix` is the API's own subtree when API mode is on. The page writes the
    /// request a client would send against it; with `None` there is no such request to
    /// write, and the page says nothing about one rather than describing a route that
    /// answers 404.
    pub fn to_html(
        &self,
        data_files: &DataFiles,
        api_prefix: Option<&str>,
        catalog: &Catalog<'_>,
        show_version: bool,
    ) -> String {
        let title = html_escape::encode_text(&self.path);
        let mut html = format!(
            "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n\
             <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
             <title>Index of {title}</title>\n<style>\n{STYLE}</style>\n</head>\n\
             <body{api}{catalog}>\n\
             <h1>Index of {breadcrumb}</h1>\n<p class=\"summary\">{summary}</p>\n\
             {cone}\
             <table class=\"listing\">\n\
             <thead><tr><th>Name</th><th class=\"ask-cell\"></th>\
             <th class=\"size\">Size</th><th>Last modified</th></tr></thead>\n<tbody>\n",
            // An attribute rather than a line of generated script: the prefix is
            // configuration, and the script is a static file that reads it.
            api = match api_prefix {
                Some(prefix) => format!(
                    " data-api=\"{}\"",
                    html_escape::encode_double_quoted_attribute(prefix)
                ),
                None => String::new(),
            },
            catalog = match catalog.url {
                Some(url) => format!(
                    " data-catalog=\"{url}\" data-max-radius=\"{radius}\"{schema}",
                    url = html_escape::encode_double_quoted_attribute(url),
                    radius = catalog.max_radius_arcsec,
                    schema = match catalog.schema_url {
                        Some(schema) => format!(
                            " data-schema=\"{}\"",
                            html_escape::encode_double_quoted_attribute(schema)
                        ),
                        None => String::new(),
                    },
                ),
                None => String::new(),
            },
            cone = catalog
                .url
                .map(|url| cone_note(catalog, url))
                .unwrap_or_default(),
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
            if let Some(prefix) = api_prefix {
                html.push_str(&api_note(prefix));
            }
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
const SERVER: &str = concat!(env!("CARGO_PKG_NAME"), " ", env!("CARGO_PKG_VERSION"));

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

/// What a catalog's url answers, written where a reader browsing one can see it.
///
/// The section is the whole of what the page says about searching a catalog when the script
/// did not run, and the anchor for the form when it did. It says the url form rather than
/// describing the form, for the same reason [`QUERY_NOTE`] does: the url is what someone
/// pastes into a client, and it is all there is without a script.
///
/// The catalog's own url is written out rather than linked. It is this directory or one
/// above, so `fsspec` would drop it either way — but a page whose ancestors are linked
/// somewhere other than the breadcrumb is a page with two answers to where they are.
fn cone_note(catalog: &Catalog<'_>, url: &str) -> String {
    format!(
        "<section class=\"catalog\">\n\
         <h2>Query HATS catalog{named}</h2>\n\
         <p class=\"about\">{about}</p>\n\
         <p class=\"without-js\">The catalog's url answers a cone search: \
         <code>{url}?ra=45.6&amp;dec=-3.2&amp;radius_arcsec=10</code> — with \
         <code>&amp;columns=</code>, <code>&amp;filters=</code>, <code>&amp;limit=</code> \
         and <code>&amp;format=json</code>, parquet otherwise. The catalog names its own \
         position columns and chooses which of its partitions to read. The radius reaches \
         {radius}\u{2033}; a wider search is the API's, whose plan route hands back the \
         requests it fans out into.</p>\n\
         </section>\n",
        // The catalog's own name where it gave one. A catalog that did not is still a
        // catalog, and the heading says what the section is either way.
        named = match catalog.name {
            Some(name) => format!(" <code>{}</code>", html_escape::encode_text(name)),
            None => String::new(),
        },
        about = about(catalog, url),
        url = html_escape::encode_text(url),
        radius = catalog.max_radius_arcsec,
    )
}

/// How much of the catalog there is, and where it is — the two things a reader standing in
/// `Dir=0` cannot see for themselves.
///
/// The url is written out and never linked, for the reason nothing else on this page links
/// upwards: the breadcrumb is where an ancestor belongs, and a second link to one is a
/// second answer to where it is.
///
/// Every number is the catalog's own word for itself. Nothing here counts anything or
/// checks one against another — that is a validator, and this is a heading.
fn about(catalog: &Catalog<'_>, url: &str) -> String {
    let mut said = vec![html_escape::encode_text(url).into_owned()];
    if let Some(rows) = catalog.rows {
        said.push(format!("{} rows", grouped(rows)));
    }
    if let Some(order) = catalog.order {
        said.push(format!("order {order}"));
    }
    said.join(" \u{b7} ")
}

/// A count with its thousands marked off. A catalog's row count runs to ten digits, and
/// ten digits in a row is a number nobody reads — they count the digits instead.
fn grouped(count: u64) -> String {
    let digits = count.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}

/// The same question as a request to the API, which is what a client writes rather than
/// a browser. Prose, and the address is written out rather than linked: an `<a href>`
/// below this directory would be scraped as an entry by a client reading the markup for
/// one.
///
/// The url in the body is this page's own path with a `file://` in front of it, because
/// a mount's `path` is its address in both modes. There is nothing to look up.
///
/// Written into the markup rather than filled in by the script, so that the sentence is
/// complete on a page whose script did not run.
fn api_note(prefix: &str) -> String {
    format!(
        "<p>The same question as an API request: <code>POST {route}</code> with a JSON \
         body naming the file as <code>file://\u{2026}</code> — this page's own path. A \
         file's panel writes the request out for <code>curl</code> and for the Python \
         readers, each with the <code>pip install</code> line it needs.</p>\n",
        route = html_escape::encode_text(&route(prefix, "parquet")),
    )
}

/// One API route under the prefix. The root prefix already ends in the separator, so
/// joining it the way any other is joined would give `//parquet`.
fn route(prefix: &str, name: &str) -> String {
    match prefix {
        "/" => format!("/{name}"),
        _ => format!("{prefix}/{name}"),
    }
}

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

    /// The API's own subtree, as `[api] prefix` defaults to it.
    const API: &str = "/api/v1";

    /// A directory with no catalog over it, which is what most of these are.
    const NO_CATALOG: Catalog<'static> = Catalog {
        url: None,
        name: None,
        rows: None,
        order: None,
        schema_url: None,
        max_radius_arcsec: 60.0,
    };

    /// A catalog that said everything it could about itself, which is what the page has
    /// the most to render from.
    fn a_catalog() -> Catalog<'static> {
        Catalog {
            url: Some("/hats/dr1"),
            name: Some("dr1"),
            rows: Some(17_161),
            order: Some(3),
            schema_url: Some("/hats/dr1/dataset/_common_metadata"),
            max_radius_arcsec: 60.0,
        }
    }

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
        let html = listing(&dir).to_html(&DataFiles::default(), Some(API), &NO_CATALOG, true);
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
        let plain = listing(&dir).to_html(&DataFiles::default(), Some(API), &NO_CATALOG, true);
        assert!(!plain.contains("columns="), "{plain}");

        fs::write(dir.path().join("part0.parquet"), b"x").unwrap();
        let html = listing(&dir).to_html(&DataFiles::default(), Some(API), &NO_CATALOG, true);
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
        let unsigned = listing(&dir).to_html(&DataFiles::default(), Some(API), &NO_CATALOG, false);
        assert!(!unsigned.contains(env!("CARGO_PKG_VERSION")), "{unsigned}");
        assert!(
            html.contains("<button class=\"ask\" data-url=\"/part0.parquet\">"),
            "{html}"
        );
    }

    /// The catalog's search is offered on the catalog's own url, which is where the walk up
    /// from this directory landed rather than the directory itself.
    #[test]
    fn a_catalog_s_page_says_what_its_url_answers() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("part0.parquet"), b"x").unwrap();
        let listing = Listing::read(dir.path(), "/hats", "/hats/dr1/dataset", false).unwrap();
        let html = listing.to_html(&DataFiles::default(), Some(API), &a_catalog(), true);

        // The url the search goes to, for the script and for the reader, and the bound the
        // form must not offer past.
        assert!(html.contains("data-catalog=\"/hats/dr1\""), "{html}");
        assert!(html.contains("data-max-radius=\"60\""), "{html}");
        assert!(html.contains("<code>/hats/dr1?ra="), "{html}");
        // Said as a url, which is the whole of what the page offers with no script — so it
        // is written for a reader rather than hidden behind one.
        assert!(html.contains("radius_arcsec=10"), "{html}");
        assert!(html.contains("60\u{2033}"), "{html}");
        // Which catalog, and how much of it: a reader standing two levels down can see
        // neither for themselves.
        assert!(
            html.contains("Query HATS catalog <code>dr1</code>"),
            "{html}"
        );
        assert!(html.contains("17,161 rows"), "{html}");
        assert!(html.contains("order 3"), "{html}");
        // Where the columns come from, which is the catalog's own schema file rather than
        // whatever a search happened to match.
        assert!(
            html.contains("data-schema=\"/hats/dr1/dataset/_common_metadata\""),
            "{html}"
        );

        // A catalog that says nothing about itself is still a catalog, and every line the
        // page cannot write is simply absent.
        let bare = listing.to_html(
            &DataFiles::default(),
            Some(API),
            &Catalog {
                url: Some("/hats/dr1"),
                ..Catalog::default()
            },
            true,
        );
        assert!(bare.contains("Query HATS catalog</h2>"), "{bare}");
        // The url alone, no count and no order: a line the catalog gave nothing for is a
        // line the page does not write.
        assert!(bare.contains("<p class=\"about\">/hats/dr1</p>"), "{bare}");
        assert!(!bare.contains("data-schema"), "{bare}");

        // A directory with no catalog over it says none of it.
        let plain = listing.to_html(&DataFiles::default(), Some(API), &NO_CATALOG, true);
        assert!(!plain.contains("data-catalog"), "{plain}");
        assert!(!plain.contains("HATS catalog"), "{plain}");
    }

    /// `fsspec` reads this page by keeping every href below the url it asked for, so a
    /// link that is not an entry — the breadcrumb, the parent — has to point elsewhere,
    /// and a link offering a query on an entry would read as a file that is not there.
    ///
    /// The catalog's section is the case that makes this worth re-asking: it names a url in
    /// its prose, and a url named in prose is one word away from being linked.
    #[test]
    fn the_only_links_below_this_directory_are_its_entries() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("part0.parquet"), b"x").unwrap();
        fs::create_dir(dir.path().join("Norder=5")).unwrap();
        let listing = Listing::read(dir.path(), "/hats", "/hats/dr1", false).unwrap();

        let html = listing.to_html(&DataFiles::default(), Some(API), &a_catalog(), true);
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

        let html = listing(&dir).to_html(&DataFiles::default(), Some(API), &NO_CATALOG, true);
        // The page has a `<script>` of its own, so what says the name did not become
        // markup is that no tag opened where the name is.
        assert!(!html.contains("<script>alert"), "{html}");
        assert!(html.contains("&lt;script&gt;"), "{html}");
        // And the quote does not end the href it sits in.
        assert!(html.contains("href=\"/a%22b.parquet\""), "{html}");
    }

    /// The script writes the API request out, and it reads the route off the page rather
    /// than being generated with it in. So the attribute is what carries API mode to the
    /// panel, and a page served with the API off must not offer a route that answers 404.
    #[test]
    fn the_api_route_reaches_the_page_only_when_the_api_is_on() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("part0.parquet"), b"x").unwrap();

        let on = listing(&dir).to_html(&DataFiles::default(), Some(API), &NO_CATALOG, true);
        assert!(on.contains("data-api=\"/api/v1\""), "{on}");
        assert!(on.contains("POST /api/v1/parquet"), "{on}");

        let off = listing(&dir).to_html(&DataFiles::default(), None, &NO_CATALOG, true);
        assert!(!off.contains("data-api"), "{off}");
        assert!(!off.contains("/parquet</code>"), "{off}");
        // The file server's own query surface is not the API's, and is described either
        // way: a mount answers a query string whether or not API mode is on.
        for html in [&on, &off] {
            assert!(html.contains("?columns=ra,dec"), "{html}");
        }
    }

    /// A mount at the root and an API at `/` are both spelled with the one separator the
    /// route already ends in, so joining them the way any other prefix is joined would
    /// give `//parquet`.
    #[test]
    fn the_root_api_prefix_does_not_double_its_separator() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("part0.parquet"), b"x").unwrap();
        let html = listing(&dir).to_html(&DataFiles::default(), Some("/"), &NO_CATALOG, true);
        assert!(html.contains("POST /parquet"), "{html}");
        assert!(!html.contains("//parquet"), "{html}");
    }
}
