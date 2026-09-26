//! The page at the TAP base url, for a person rather than a client.
//!
//! TAP defines no resource at the base url itself, and it is the url a person is handed and
//! opens in a browser. What they need there is what a client would otherwise have to be
//! asked: which tables there are, a query that runs against them, and how to send it from
//! the clients they already have.
//!
//! **The snippets are written against this deployment**: each example is written out for
//! each client with this deployment's own base url, so what a reader copies runs as it
//! stands.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use axum::extract::State;
use axum::http::{HeaderMap, header};
use axum::response::{IntoResponse, Response};

use crate::app::routes::tap::answer::{answered, base_url};
use crate::app::routes::tap::examples::offered;
use crate::app::routes::tap::published::{Published, open_each};
use crate::app::routes::tap::vosi::TAP_SEGMENT;
use crate::app::service::Service;
use crate::error::ApiError;
use crate::tap::TapExample;

/// The page.
pub(in crate::app) async fn page(State(service): State<Service>, headers: HeaderMap) -> Response {
    answered(respond(&service, &headers).await)
}

async fn respond(service: &Service, headers: &HeaderMap) -> Result<Response, ApiError> {
    let base = format!(
        "{}/{TAP_SEGMENT}",
        base_url(headers, service.api_prefix.as_deref().unwrap_or("/"))
    );
    let published = open_each(service).await?;
    let body = render(&base, &published);
    Ok(([(header::CONTENT_TYPE, "text/html; charset=utf-8")], body).into_response())
}

/// The clients an example is written out for, in the order the tabs show them. `ADQL` comes
/// first and is not a client: it is the query the rest of them send, for someone whose client
/// is none of these.
const CLIENTS: [&str; 5] = ["ADQL", "pyvo", "TOPCAT", "STILTS", "curl"];

fn render(base: &str, published: &[Published<'_>]) -> String {
    let examples: Vec<(&str, TapExample)> = published
        .iter()
        .flat_map(|table| {
            offered(table)
                .into_iter()
                .map(move |(_, example)| (table.metadata.qualified.as_str(), example))
        })
        .collect();

    let mut out = String::new();
    let base_text = escape(base);
    let _ = write!(
        out,
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
         <title>TAP</title>\n<style>\n{css}</style>\n</head>\n<body>\n\
         <h1>TAP</h1>\n\
         <p>Base url: <code>{base_text}</code></p>\n\
         <nav><a href=\"#catalogs\">Catalogs</a><a href=\"#examples\">Examples</a></nav>\n",
        css = include_str!("page/page.css"),
    );

    // Few catalogs set `obs_title`, and a column empty on every row is width taken from the
    // ones that say something.
    let titled = published.iter().any(|table| table.title.is_some());
    let _ = write!(
        out,
        "<h2 id=\"catalogs\">Catalogs</h2>\n<div class=\"wide\"><table>\n<thead><tr><th>Table</th>{}\
         <th>Rows</th><th>Columns</th><th>Nested columns</th></tr></thead>\n<tbody>\n",
        if titled { "<th>Title</th>" } else { "" },
    );
    for table in published {
        push_catalog(&mut out, base, table, titled);
    }
    out.push_str(
        "</tbody>\n</table></div>\n\
         <p>Nested columns are answered in parquet only: <code>RESPONSEFORMAT=parquet</code>.</p>\n",
    );

    let _ = writeln!(
        out,
        "<h2 id=\"examples\">Examples</h2>\n\
         <p>Also at <a href=\"{base_text}/examples\"><code>{base_text}/examples</code></a>.</p>"
    );
    for (table, example) in &examples {
        // A generated name says its table already; an operator's may not.
        let named = if example.name.contains(table) {
            String::new()
        } else {
            format!(" <span class=\"table\">{}</span>", escape(table))
        };
        let _ = writeln!(out, "<h3>{}{named}</h3>", escape(&example.name));
        push_clients(&mut out, base, example);
    }

    let _ = write!(
        out,
        "<script>\n{script}</script>\n</body>\n</html>\n",
        script = include_str!("page/page.js"),
    );
    out
}

/// One example as each client sends it: a tab bar and a box per client.
///
/// Every box is in the markup, so without a script the page shows all of them, each under
/// its name; `page.js` hides the bar's names and all but the chosen box.
fn push_clients(out: &mut String, base: &str, example: &TapExample) {
    out.push_str("<div class=\"clients\">\n<div class=\"client-tabs\">");
    for client in CLIENTS {
        let _ = write!(
            out,
            "<button class=\"client-tab\" data-client=\"{client}\">{client}</button>"
        );
    }
    out.push_str(
        "<button class=\"client-copy\" title=\"Copy to the clipboard\">copy</button></div>\n",
    );
    for client in CLIENTS {
        let code = match client {
            "pyvo" => python(base, &example.query),
            "TOPCAT" => topcat(base, &example.name),
            "STILTS" => stilts(base, &example.query),
            "curl" => curl(base, &example.query),
            _ => example.query.clone(),
        };
        let _ = writeln!(
            out,
            "<p class=\"client-label\">{client}</p>\
             <pre class=\"client-code\" data-client=\"{client}\">{}</pre>",
            escape(&code),
        );
    }
    out.push_str("</div>\n");
}

/// One row of the catalog table.
///
/// The column count is the table's own, a nested column counting once: `/tables` lists
/// each leaf of one, which is what a query names and not what the catalog holds.
fn push_catalog(out: &mut String, base: &str, table: &Published<'_>, titled: bool) {
    let name = &table.metadata.qualified;
    let mut flat = 0;
    let mut nested = BTreeSet::new();
    for column in &table.metadata.columns {
        match column.name.split_once('.') {
            Some((parent, _)) => {
                nested.insert(parent);
            }
            None => flat += 1,
        }
    }
    let columns = flat + nested.len();
    let nested = nested
        .iter()
        .map(|parent| format!("<code>{}</code>", escape(parent)))
        .collect::<Vec<_>>()
        .join(", ");
    // A published name is `schema.table`, both halves ADQL identifiers, so it is already a
    // path segment and `/tables/{name}` matches it as written.
    let _ = writeln!(
        out,
        "<tr><td><a href=\"{base}/tables/{name}\"><code>{name}</code></a></td>{title}\
         <td class=\"count\">{rows}</td><td class=\"count\">{columns}</td><td>{nested}</td></tr>",
        base = escape(base),
        name = escape(name),
        title = if titled {
            format!(
                "<td class=\"title\">{}</td>",
                escape(table.title.as_deref().unwrap_or(""))
            )
        } else {
            String::new()
        },
        rows = table.rows.map(grouped).unwrap_or_default(),
    );
}

/// `pyvo` sending the query, and `nested-pandas` reading the parquet it gets back — `/sync`
/// for an answer that fits in one request, a job for one that does not.
///
/// `aiohttp` is what `fsspec` reads the job's result url with, and nothing else here pulls
/// it in.
fn python(base: &str, query: &str) -> String {
    format!(
        "# pip install aiohttp nested-pandas pyvo\n\
         import io\n\
         \n\
         import nested_pandas as npd\n\
         import pyvo\n\
         \n\
         tap = pyvo.dal.TAPService(\"{base}\")\n\
         query = \"\"\"\n{query}\n\"\"\"\n\
         \n\
         # /sync\n\
         answer = tap.create_query(query, RESPONSEFORMAT=\"parquet\").execute_stream()\n\
         nf = npd.read_parquet(io.BytesIO(answer.read()))\n\
         \n\
         # /async, for a query too slow for /sync\n\
         job = tap.submit_job(query, RESPONSEFORMAT=\"parquet\").run().wait()\n\
         job.raise_if_error()\n\
         nf = npd.read_parquet(job.result_uri)\n\
         job.delete()\n\
         \n\
         # VOTable, into astropy\n\
         table = tap.run_sync(query).to_table()\n",
        base = python_string(base),
        query = query
            .replace('\\', "\\\\")
            .replace("\"\"\"", "\\\"\\\"\\\""),
    )
}

/// `stilts tapquery`, which picks parquet from the file name.
fn stilts(base: &str, query: &str) -> String {
    let adql = query.split_whitespace().collect::<Vec<_>>().join(" ");
    format!(
        "stilts tapquery tapurl={} \\\n  adql={} \\\n  out=rows.parquet\n",
        shell_word(base),
        shell_word(&adql),
    )
}

/// Where TOPCAT's TAP window takes the url, and where it lists this example by name. TOPCAT
/// asks for a VOTable, so a nested column is out of its reach.
fn topcat(base: &str, name: &str) -> String {
    format!(
        "VO → Table Access Protocol (TAP) Query\n\
         TAP URL: {base} → Use Service\n\
         Examples → Service Provided → {name}\n"
    )
}

/// A `POST` to `/sync`, the query form-encoded by curl itself.
fn curl(base: &str, query: &str) -> String {
    format!(
        "curl -o rows.parquet {}/sync \\\n  -d LANG=ADQL -d RESPONSEFORMAT=parquet \\\n  \
         --data-urlencode {}\n",
        shell_word(base),
        shell_word(&format!("QUERY={query}")),
    )
}

/// A string inside Python's double quotes.
fn python_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

/// One shell word, quoted where it has to be. An ADQL string literal is in single quotes,
/// so a quote inside is closed, escaped and reopened.
fn shell_word(value: &str) -> String {
    let plain = value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || "-_./:=@%+,".contains(c));
    if plain {
        value.to_owned()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

/// A count with its thousands separated, as a reader compares two of them.
fn grouped(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::new();
    for (at, digit) in digits.chars().enumerate() {
        if at > 0 && (digits.len() - at).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

/// A name out of somebody's parquet file, or a query out of somebody's config, is markup
/// until it is escaped.
fn escape(value: &str) -> String {
    quick_xml::escape::escape(value).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_count_is_grouped_by_thousands() {
        assert_eq!(grouped(0), "0");
        assert_eq!(grouped(999), "999");
        assert_eq!(grouped(1000), "1,000");
        assert_eq!(grouped(1_811_709_771), "1,811,709,771");
    }

    #[test]
    fn a_shell_word_survives_an_adql_string_literal() {
        assert_eq!(shell_word("http://h/api/v1/tap"), "http://h/api/v1/tap");
        assert_eq!(
            shell_word("SELECT 'ICRS' FROM t"),
            "'SELECT '\\''ICRS'\\'' FROM t'"
        );
    }
}
