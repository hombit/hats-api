//! Queries that run, for a client to put in front of a user.
//!
//! DALI §2.3's `/examples`: one XHTML document carrying RDFa, which TOPCAT reads into its
//! "Service Provided" menu and `pyvo` into `TAPService.examples`. It is a SHOULD rather
//! than a MUST — a client works without it — and what it buys is that the first query a new
//! user runs against this service is one that returns rows.
//!
//! **Generated from the catalog, and replaced by the operator.** A `[[tap.table]]` is a name
//! and a path, so which columns a table has, which two hold a position and where on the sky
//! it holds rows are the catalog's to answer; `[[tap.table.example]]` is where somebody who
//! has looked at the data says something better. The two never mix for one table — writing
//! an example replaces the generated one, which is what keeps an operator from having to
//! work around a query they did not write.
//!
//! **The cone is centred on a row the catalog holds**, which is the one thing that makes the
//! answer reliably non-empty; `published::example_position` reads it, and says why nothing
//! cheaper does. Everything else — the columns, the position columns, the length scale — is
//! metadata `/tables` reads anyway.
//!
//! Three things a generated query has to avoid, and each is a way to make the menu useless:
//! `SELECT *`, these catalogs being 150 to 370 columns wide; a nested column, which no
//! format a cone can answer in carries; and a cone with no `TOP` beside it, which is what
//! keeps the read to the partitions the cone names.

use std::fmt::Write as _;

use axum::extract::State;
use axum::http::header;
use axum::response::{IntoResponse, Response};

use crate::adql::names;
use crate::app::routes::tap::answer::answered;
use crate::app::routes::tap::published::{Published, open_each};
use crate::app::service::Service;
use crate::error::ApiError;
use crate::tap::TapExample;

/// Where a client looks for this document, under the TAP base url. DALI §2.3 fixes the
/// name, so there is nothing here to choose.
pub(super) const EXAMPLES_RESOURCE: &str = "examples";

/// What `/capabilities` declares this resource as.
pub(super) const EXAMPLES_STANDARD: &str = "ivo://ivoa.net/std/DALI#examples";

/// The RDFa vocabulary the examples are read under, which DALI §2.3 writes out. It is an
/// `http://` term rather than an `ivo://` one — the identifier above is the capability's,
/// and the two are not interchangeable.
const VOCAB: &str = "http://www.ivoa.net/rdf/examples#";

/// How many of a catalog's columns a generated query names.
///
/// What a request against a real catalog costs is the columns it projects rather than the
/// rows it returns, which is the same measurement `/docs` sizes its examples by: ten to
/// seventy seconds for all of them against about one for four.
const EXAMPLE_COLUMNS: usize = 4;

/// How many rows a generated query asks for.
///
/// The bound that makes the example cheap whatever the cone turned out to cover: a catalog
/// is read partition by partition in HEALPix order and a statement that has its rows stops
/// pulling, so this is what a reader who widens the circle is protected by.
const EXAMPLE_ROWS: usize = 10;

/// The narrowest cone a generated query will ask for, in degrees.
///
/// One arcsecond. Widening cannot make the answer empty — the centre is a row — and a cone
/// narrower than this is a number nobody reads as a field on the sky.
const NARROWEST_RADIUS_DEG: f64 = 1.0 / 3600.0;

/// The widest, in degrees: ten arcminutes.
///
/// The same figure `[limits] max_query_radius_arcsec` defaults to, and for the same reason —
/// it is the size of question somebody browsing actually asks, a field around a source
/// rather than a survey of the sky. A reader who wants more widens it themselves.
const WIDEST_RADIUS_DEG: f64 = 600.0 / 3600.0;

/// What fraction of the deepest partition's own size a cone spans.
///
/// The cell is the one length scale a catalog offers, and a tenth of it is comfortably
/// inside one partition — so the example reads about one file, whether the catalog is
/// partitioned at order 3 or order 12.
const CELL_FRACTION: f64 = 0.1;

/// The examples document.
pub(in crate::app) async fn examples(State(service): State<Service>) -> Response {
    answered(page(&service).await)
}

async fn page(service: &Service) -> Result<Response, ApiError> {
    let published = open_each(service).await?;
    let body = render(&published);
    Ok(([(header::CONTENT_TYPE, "text/html; charset=utf-8")], body).into_response())
}

/// The whole document: one section per published table, each holding that table's examples.
///
/// XHTML, because DALI §2.3 asks for well-formed XML and every client that reads this parses
/// it as XML. So nothing here may write a named entity beyond the five XML defines, and every
/// element closes.
///
/// It carries everything it needs — the stylesheet inline, no script at all — for the reason
/// every page this service serves does: a page that is blank on the network this is deployed
/// on is worse than a plain one.
fn render(published: &[Published<'_>]) -> String {
    let mut out = String::new();
    out.push_str("<!DOCTYPE html>\n<html xmlns=\"http://www.w3.org/1999/xhtml\">\n<head>\n");
    out.push_str("<meta charset=\"utf-8\"/>\n<title>ADQL examples</title>\n");
    out.push_str(
        "<style>\n\
         body { font: 14px/1.5 system-ui, sans-serif; margin: 0 auto; max-width: 48rem; \
         padding: 1.5rem; }\n\
         h2 { font-size: 1.1rem; margin: 2rem 0 0.25rem; }\n\
         p.table { color: #555; margin: 0 0 0.5rem; }\n\
         pre { background: #f4f4f4; border-radius: 4px; overflow-x: auto; padding: 0.75rem; }\n\
         </style>\n",
    );
    out.push_str("</head>\n<body>\n<h1>ADQL examples</h1>\n");
    out.push_str(
        "<p>Queries this service answers, one per published table. Send one to the \
         <code>/sync</code> resource beside this one, or open this service in a client that \
         offers them.</p>\n",
    );
    // Every example is a descendant of the one element carrying the vocabulary, which is
    // what DALI §2.3 asks for: one attribute for the page rather than one per example.
    let _ = writeln!(out, "<div vocab=\"{VOCAB}\">");
    for table in published {
        for (id, example) in offered(table) {
            push_example(&mut out, &id, &table.metadata.qualified, &example);
        }
    }
    out.push_str("</div>\n</body>\n</html>\n");
    out
}

/// What this service offers for one table, and the fragment id each is referenced by.
///
/// The operator's, where they wrote any; otherwise the one generated from the catalog. Not
/// both: an operator who wrote an example for a table has said what that table's example is.
///
/// A table can end up with nothing — a catalog naming no position column, or one whose
/// columns are all nested — and it is then simply absent. An example that would not run is
/// worse than a menu one entry shorter.
fn offered(table: &Published<'_>) -> Vec<(String, TapExample)> {
    let qualified = &table.metadata.qualified;
    let configured = table.table.examples();
    if !configured.is_empty() {
        return configured
            .iter()
            .enumerate()
            .map(|(at, example)| (format!("{qualified}-{}", at + 1), example.clone()))
            .collect();
    }
    generated(table)
        .into_iter()
        .map(|example| (qualified.clone(), example))
        .collect()
}

/// One query written from what the catalog says about itself.
///
/// A cone, because that is the shape a HATS catalog is built to answer and the one nobody
/// guesses the spelling of: `1 = CONTAINS(POINT(ra, dec), CIRCLE(…))` is three nested calls
/// and an equality against a number, and a user meeting ADQL for the first time will not
/// write it. Where the catalog names no position there is no cone to write, and what is left
/// is the first few rows — still useful, and still bounded by the `TOP`.
fn generated(table: &Published<'_>) -> Option<TapExample> {
    let qualified = &table.metadata.qualified;
    let columns = projection(table);
    if columns.is_empty() {
        return None;
    }
    let columns = columns.join(", ");
    let Some((ra, dec, centre_ra, centre_dec, radius)) = circle(table) else {
        return Some(TapExample {
            name: format!("First rows of {qualified}"),
            query: format!("SELECT TOP {EXAMPLE_ROWS} {columns}\nFROM {qualified}"),
        });
    };
    // A line per clause, which is what TAP §2.6 means by extra whitespace for human
    // consumption — and no alignment inside a line, an example being something a reader
    // edits rather than something that has to stay lined up as they do.
    Some(TapExample {
        name: format!("Cone search on {qualified}"),
        query: format!(
            "SELECT TOP {EXAMPLE_ROWS} {columns}\n\
             FROM {qualified}\n\
             WHERE 1 = CONTAINS(POINT({ra}, {dec}), CIRCLE({}, {}, {}))",
            degrees(centre_ra),
            degrees(centre_dec),
            degrees(radius),
        ),
    })
}

/// The two position columns, then the first few others, as a `SELECT` list.
///
/// **The position comes first because the example is a cone.** A cone search whose answer
/// does not say where any of the rows are is a demonstration of the wrong thing — and the
/// front of a real catalog is not where its coordinates are: Gaia DR3 opens with
/// `solution_id`, `designation`, `source_id` and `random_index`, of which one is the same
/// number on every row and another is bookkeeping.
///
/// Three kinds are left out of the rest, and each would otherwise reach a reader:
///
/// - **The HEALPix index**, which is not `principal` — a number an importer wrote to make
///   the tiling work, which nobody opening a menu asked for.
/// - **A leaf of a nested column**, which is dotted. VOTable has no form for one (§7.5's
///   decisions), so naming it would make the first query a new user runs a refusal.
/// - **Anything whose published name needs quoting.** In a HATS catalog that is the rest of
///   the importer's machinery — ZTF DR24 carries `_healpix_19` and `_healpix_9` beside the
///   one the catalog declares, and those are `principal` because the catalog never claimed
///   them. It is a heuristic rather than a rule about astronomy, and what it costs is a
///   column absent from one example rather than a column nobody can query.
fn projection(table: &Published<'_>) -> Vec<String> {
    let position: Vec<String> = table
        .coordinates
        .iter()
        .flat_map(|(ra, dec)| [names::as_written(ra), names::as_written(dec)])
        .collect();
    let room = EXAMPLE_COLUMNS.saturating_sub(position.len());
    let mut chosen = position.clone();
    chosen.extend(
        table
            .metadata
            .columns
            .iter()
            .filter(|column| {
                column.principal
                    && !column.name.contains('.')
                    && !column.name.starts_with('"')
                    && !position.contains(&column.name)
            })
            .map(|column| column.name.clone())
            .take(room),
    );
    chosen
}

/// Where a generated cone looks and how wide it is: the two position columns as a query
/// writes them, then the centre and the radius in degrees.
///
/// **The centre is a row the catalog holds, so the answer cannot be empty.** That is the
/// whole of it, and nothing cheaper works: a partition's centre is empty wherever the data
/// fills a corner of its cell. `published::example_position` is where the row is read.
///
/// The radius is a tenth of the deepest partition's own size, capped at ten arcminutes. Two
/// numbers rather than one because the cell alone spans four orders of magnitude between a
/// catalog partitioned at order 0 and one at order 12 — the fraction keeps the read inside
/// about one partition, and the cap keeps a shallow catalog's example from being a cone over
/// a quarter of the sky. The floor is legibility and costs nothing, the centre being a row.
fn circle(table: &Published<'_>) -> Option<(String, String, f64, f64, f64)> {
    let (ra, dec) = table.coordinates.as_ref()?;
    let (centre_ra, centre_dec) = table.position?;
    let cell = table.cell.as_ref()?;
    let (lon, lat) = cdshealpix::nested::center(cell.order, cell.pixel);
    let span = cdshealpix::largest_center_to_vertex_distance(cell.order, lon, lat).to_degrees();
    let radius = (CELL_FRACTION * span).clamp(NARROWEST_RADIUS_DEG, WIDEST_RADIUS_DEG);
    Some((
        names::as_written(ra),
        names::as_written(dec),
        centre_ra,
        centre_dec,
        radius,
    ))
}

/// An angle as a query would write it: five decimals, which is a third of an arcsecond, and
/// no trailing zeros behind it.
///
/// The trimming is safe because `{:.5}` always writes the point, so there is always a digit
/// the zeros are trimmed back to and a whole number never loses one of its own.
fn degrees(value: f64) -> String {
    let text = format!("{value:.5}");
    text.trim_end_matches('0').trim_end_matches('.').to_owned()
}

/// One example, in the shape DALI §2.3 and TAP §2.6 ask for between them.
///
/// DALI's: an `id` so the example can be referenced by fragment, a `resource` pointing at
/// itself, `typeof="example"`, and exactly one `name`. TAP's: exactly one `query` holding
/// plain text, and any number of `table` elements naming what it is about. Every one of
/// those is an attribute a client matches on, so none of them is decoration.
fn push_example(out: &mut String, id: &str, table: &str, example: &TapExample) {
    let id = escape(id);
    let _ = write!(
        out,
        "<div id=\"{id}\" resource=\"#{id}\" typeof=\"example\">\n\
         <h2 property=\"name\">{}</h2>\n\
         <p class=\"table\">Table: <span property=\"table\">{}</span></p>\n\
         <pre property=\"query\">{}</pre>\n\
         </div>\n",
        escape(&example.name),
        escape(table),
        escape(&example.query),
    );
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
    fn an_angle_is_written_without_trailing_zeros() {
        assert_eq!(degrees(45.0), "45");
        assert_eq!(degrees(0.05), "0.05");
        assert_eq!(degrees(254.457_54), "254.45754");
        assert_eq!(degrees(-20.5), "-20.5");
        // The floor, which is what a deeply partitioned catalog lands on.
        assert_eq!(degrees(NARROWEST_RADIUS_DEG), "0.00028");
    }
}
