//! The three VOSI resources: is the service up, what can it do, and what tables has it.
//!
//! VOSI 1.1, with the TAP capability's own detail from TAPRegExt 1.0. A client reads all
//! three before it sends a query — it picks its interface, its language and its output
//! format out of the capabilities document — so anything wrong here is wrong before a
//! statement is ever written, and anything missing is something the client will not try.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::Response;
use serde::Deserialize;

use crate::adql::language;
use crate::app::routes::tap::answer::{answered, base_url, document};
use crate::app::routes::tap::format;
use crate::app::routes::tap::published::describe;
use crate::app::service::Service;
use crate::error::ApiError;
use crate::tap::metadata::TableMetadata;

/// Where the TAP resources sit under the API's own prefix.
const TAP_SEGMENT: &str = "tap";

/// The optional halves of ADQL 2.1 this service answers, each by the feature type that
/// declares it and the forms it covers.
///
/// **Naming the version is not a claim to the language.** ADQL 2.1 makes the geometry,
/// `CAST`, `COALESCE`, `ILIKE`, `WITH`, the set operators and `OFFSET` each optional, so
/// this is where a client learns what it may write — TOPCAT offers what is here and not
/// what is missing. A partial set is said by listing the forms rather than by leaving the
/// type out, which is what the geometry needs: five of the thirteen forms that type covers
/// are answered, and the rest are refused by name.
///
/// Every entry was measured against a running service. Reading it off the code would
/// declare what this crate registers rather than what a query gets, and most of these come
/// from DataFusion rather than from anything written here.
const LANGUAGE_FEATURES: &[(&str, &[&str])] = &[
    (
        "ivo://ivoa.net/std/tapregext#features-adqlgeo",
        &["POINT", "CIRCLE", "CONTAINS", "INTERSECTS", "DISTANCE"],
    ),
    (
        "ivo://ivoa.net/std/tapregext#features-adql-string",
        &["LOWER", "UPPER", "ILIKE"],
    ),
    (
        "ivo://ivoa.net/std/tapregext#features-adql-common-table",
        &["WITH"],
    ),
    (
        "ivo://ivoa.net/std/tapregext#features-adql-sets",
        &["UNION", "EXCEPT", "INTERSECT"],
    ),
    ("ivo://ivoa.net/std/tapregext#features-adql-type", &["CAST"]),
    (
        "ivo://ivoa.net/std/tapregext#features-adql-conditional",
        &["COALESCE"],
    ),
    (
        "ivo://ivoa.net/std/tapregext#features-adql-offset",
        &["OFFSET"],
    ),
];

/// Where a function this service has beyond the language is declared.
const UDF_FEATURE: &str = "ivo://ivoa.net/std/tapregext#features-udf";

/// Those functions, as a signature and what it does — the form TAPRegExt asks a UDF to
/// take, and what a client shows a user who is looking for it.
const UDFS: &[(&str, &str)] = &[(
    "MOC(serialization VARCHAR) -> REGION",
    "A Multi-Order Coverage map as a region, in IVOA's ASCII serialization: \
     MOC('4/30-33 38 52'). Compared with CONTAINS like any other shape, and tested \
     against the table's HEALPix column rather than its coordinates.",
)];

/// Is the service up.
///
/// It answers, so it is: nothing this service does can currently report otherwise, and a
/// document that said `false` would be one nothing could have produced. A deployment that
/// is really down does not reach this handler at all, which is what a client's timeout is
/// for.
pub(in crate::app) async fn availability() -> Response {
    document(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <vosi:availability xmlns:vosi=\"http://www.ivoa.net/xml/VOSIAvailability/v1.0\">\n\
         <vosi:available>true</vosi:available>\n\
         </vosi:availability>\n"
            .to_owned(),
    )
}

/// What this service can do, and where.
///
/// **Nothing optional is advertised that is not there.** A client picks what it may do out
/// of this document and has no way back, so a `uploadMethod` is declared only where an
/// upload works: told it may send a VOTable, a client fails at the point of sending rather
/// than at the point of choosing, and this service reads no VOTable.
///
/// **`/async` is not one of those, and no wording here can withhold it.** The TAP capability
/// declares one `<accessURL use="base">` and a client appends the resource names itself, so
/// a client is told about `/async` by the base url whatever else is written — which is
/// correct, TAP §2.2 making that resource a MUST, and is why there is no switch for it
/// either. While the resource is unimplemented what a client meets is a `404` at submission,
/// and that is a missing resource rather than a missing declaration.
pub(in crate::app) async fn capabilities(
    State(service): State<Service>,
    headers: HeaderMap,
) -> Response {
    let base = format!(
        "{}/{TAP_SEGMENT}",
        base_url(&headers, service.api_prefix.as_deref().unwrap_or("/"))
    );
    let mut out = String::new();
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    out.push_str(
        "<vosi:capabilities \
         xmlns:vosi=\"http://www.ivoa.net/xml/VOSICapabilities/v1.0\" \
         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \
         xmlns:vod=\"http://www.ivoa.net/xml/VODataService/v1.1\" \
         xmlns:tr=\"http://www.ivoa.net/xml/TAPRegExt/v1.0\">\n",
    );
    for (standard, resource) in [
        ("ivo://ivoa.net/std/VOSI#capabilities", "capabilities"),
        ("ivo://ivoa.net/std/VOSI#availability", "availability"),
        ("ivo://ivoa.net/std/VOSI#tables-1.1", "tables"),
    ] {
        let _ = write!(out, "<capability standardID=\"{standard}\">");
        interface(&mut out, &format!("{base}/{resource}"), "full", None);
        out.push_str("</capability>\n");
    }

    out.push_str(
        "<capability xsi:type=\"tr:TableAccess\" standardID=\"ivo://ivoa.net/std/TAP\">\n",
    );
    interface(&mut out, &base, "base", Some("1.1"));
    // Both versions, because a `LANG` of either is answered. 2.1 is what is implemented and
    // 2.0 is what it also reads — everything that standard wrote is valid 2.1, the
    // coordinate system argument of a geometry included.
    out.push_str("<language>\n<name>ADQL</name>\n");
    let _ = write!(
        out,
        "<version ivo-id=\"ivo://ivoa.net/std/ADQL#v{version}\">{version}</version>\n\
         <version ivo-id=\"ivo://ivoa.net/std/ADQL#v{earlier}\">{earlier}</version>\n",
        version = language::VERSION,
        earlier = language::EARLIER_VERSION,
    );
    out.push_str("<description>ADQL 2.1, reading 2.0 as well.</description>\n");
    for (feature, forms) in LANGUAGE_FEATURES {
        let _ = writeln!(out, "<languageFeatures type=\"{feature}\">");
        for form in *forms {
            let _ = write!(out, "<feature>\n<form>{form}</form>\n</feature>\n");
        }
        out.push_str("</languageFeatures>\n");
    }
    let _ = writeln!(out, "<languageFeatures type=\"{UDF_FEATURE}\">");
    for (signature, description) in UDFS {
        let _ = write!(
            out,
            "<feature>\n<form>{}</form>\n<description>{}</description>\n</feature>\n",
            escape(signature),
            escape(description)
        );
    }
    out.push_str("</languageFeatures>\n");
    out.push_str("</language>\n");
    // Derived from the one list the query resource reads, so the document cannot offer a
    // format a request would then be refused for asking about.
    for (alias, mime) in format::declared() {
        let _ = write!(
            out,
            "<outputFormat>\n<mime>{}</mime>\n<alias>{alias}</alias>\n</outputFormat>\n",
            escape(mime)
        );
    }
    // What a request may spend, as the two numbers a client reads MAXREC against. The
    // default is the hard limit: a request that names no MAXREC is bounded by the same
    // ceiling as one that names too large a number.
    let rows = service.adql_limits.max_rows;
    let _ = write!(
        out,
        "<outputLimit>\n<default unit=\"row\">{rows}</default>\n\
         <hard unit=\"row\">{rows}</hard>\n</outputLimit>\n"
    );
    out.push_str("</capability>\n</vosi:capabilities>\n");
    document(out)
}

/// One `<interface>` of a capability.
fn interface(out: &mut String, url: &str, use_: &str, version: Option<&str>) {
    let version = version.map_or_else(String::new, |version| format!(" version=\"{version}\""));
    let _ = write!(
        out,
        "\n<interface xsi:type=\"vod:ParamHTTP\" role=\"std\"{version}>\n\
         <accessURL use=\"{use_}\">{}</accessURL>\n</interface>\n",
        escape(url)
    );
}

/// How much of the table metadata to return.
#[derive(Debug, Deserialize)]
pub(in crate::app) struct Detail {
    /// `min` for the table names alone. Absent is every column.
    detail: Option<String>,
    /// Every other parameter, which is refused rather than ignored.
    #[serde(flatten)]
    unknown: BTreeMap<String, serde::de::IgnoredAny>,
}

/// What tables there are, and what columns they have.
///
/// This is the document TOPCAT fills its table browser from, so a client that cannot read
/// it has nothing to show a user.
pub(in crate::app) async fn tables(
    State(service): State<Service>,
    Query(detail): Query<Detail>,
) -> Response {
    answered(tableset(&service, &detail).await)
}

async fn tableset(service: &Service, detail: &Detail) -> Result<Response, ApiError> {
    let columns = wanted(detail)?;
    let described = describe(service).await?;
    Ok(document(render(&described, columns)))
}

/// One table in full, which is how a client asks about a table it already has the name of
/// rather than fetching every column of every table.
pub(in crate::app) async fn table(
    State(service): State<Service>,
    Path(name): Path<String>,
    Query(detail): Query<Detail>,
) -> Response {
    answered(one_table(&service, &name, &detail).await)
}

async fn one_table(service: &Service, name: &str, detail: &Detail) -> Result<Response, ApiError> {
    let columns = wanted(detail)?;
    let described = describe(service).await?;
    // The published spelling, and `TAP_SCHEMA`'s own names however they were written —
    // the same rule a query's `FROM` is matched by, since a client uses one name for both.
    let found = crate::tap::schema::resolve(name).unwrap_or_else(|| name.to_owned());
    let table = described
        .iter()
        .find(|table| table.qualified == found)
        .ok_or_else(|| {
            ApiError::not_found(format!("this service publishes no table named {name}"))
        })?;
    Ok(document(render(std::slice::from_ref(table), columns)))
}

/// Whether the caller asked for the columns, refusing a `detail` that means nothing.
///
/// VOSI 1.1 defines `min`, and nothing else. A value this service does not know would be
/// answered with the whole document — which a caller who asked for less cannot tell from
/// the service ignoring them.
fn wanted(detail: &Detail) -> Result<bool, ApiError> {
    if !detail.unknown.is_empty() {
        return Err(ApiError::bad_request(format!(
            "not accepted here: {}; this resource takes detail",
            detail
                .unknown
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    match detail.detail.as_deref() {
        None => Ok(true),
        Some("min") => Ok(false),
        Some(other) => Err(ApiError::bad_request(format!(
            "detail {other:?} is not something this resource does; detail=min returns the \
             table names without their columns"
        ))),
    }
}

/// The tableset document, grouped by schema the way VODataService has it.
fn render(described: &[TableMetadata], columns: bool) -> String {
    let mut out = String::new();
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    out.push_str(
        "<vosi:tableset \
         xmlns:vosi=\"http://www.ivoa.net/xml/VOSITables/v1.0\" \
         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \
         xmlns:vod=\"http://www.ivoa.net/xml/VODataService/v1.1\">\n",
    );
    // In the order they were described, which puts `TAP_SCHEMA` first and then the
    // operator's own list as written.
    let mut schemas: Vec<&str> = described
        .iter()
        .map(|table| table.schema.as_str())
        .collect();
    schemas.dedup();
    for schema in schemas {
        let _ = write!(out, "<schema>\n<name>{}</name>\n", escape(schema));
        for table in described.iter().filter(|table| table.schema == schema) {
            let _ = write!(
                out,
                "<table type=\"table\">\n<name>{}</name>\n",
                escape(&table.qualified)
            );
            if let Some(description) = &table.description {
                let _ = writeln!(out, "<description>{}</description>", escape(description));
            }
            if columns {
                for column in &table.columns {
                    push_column(&mut out, column);
                }
                // After the columns, which is where VODataService puts them — and out of
                // the same list `TAP_SCHEMA.keys` is built from, the two being a pair a
                // validator reads against each other.
                for key in &table.keys {
                    push_key(&mut out, key);
                }
            }
            out.push_str("</table>\n");
        }
        out.push_str("</schema>\n");
    }
    out.push_str("</vosi:tableset>\n");
    out
}

/// One `<column>`, in VODataService's own order: name, description, unit, ucd, then the
/// type and the flags.
fn push_column(out: &mut String, column: &crate::tap::metadata::ColumnMetadata) {
    let _ = write!(out, "<column>\n<name>{}</name>\n", escape(&column.name));
    if let Some(unit) = column.unit {
        let _ = writeln!(out, "<unit>{}</unit>", escape(unit));
    }
    if let Some(ucd) = column.ucd {
        let _ = writeln!(out, "<ucd>{}</ucd>", escape(ucd));
    }
    let arraysize = column
        .arraysize
        .map_or_else(String::new, |size| format!(" arraysize=\"{size}\""));
    // **VODataService has no `xtype` attribute, in any version.** What carries a DALI
    // xtype here is `extendedType`, whose 1.2 wording is that its value "will usually be a
    // VOTable xtype as defined by DALI" — with `extendedSchema` left off, which is what
    // says the value is read that way rather than against a scheme of somebody's own. So
    // the same fact is `xtype` in `TAP_SCHEMA` and `extendedType` in this document, which
    // reads like a mistake and is the spelling each standard asks for.
    let extended = column
        .xtype
        .map_or_else(String::new, |xtype| format!(" extendedType=\"{xtype}\""));
    let _ = writeln!(
        out,
        "<dataType xsi:type=\"vod:VOTableType\"{arraysize}{extended}>{}</dataType>",
        column.datatype
    );
    // VODataService's own words for the two: `indexed` says a constraint on this column
    // makes the query read less, `primary` is its spelling of TAP's `principal`.
    if column.indexed {
        out.push_str("<flag>indexed</flag>\n");
    }
    if column.principal {
        out.push_str("<flag>primary</flag>\n");
    }
    if column.std {
        out.push_str("<flag>std</flag>\n");
    }
    out.push_str("</column>\n");
}

/// One `<foreignKey>`, which VODataService nests inside the table it is *from* — so there
/// is nowhere to put the key's own id, and none is needed.
fn push_key(out: &mut String, key: &crate::tap::metadata::ForeignKey) {
    let _ = write!(
        out,
        "<foreignKey>\n<targetTable>{}</targetTable>\n",
        escape(&key.target_table)
    );
    for (from, target) in &key.columns {
        let _ = write!(
            out,
            "<fkColumn>\n<fromColumn>{}</fromColumn>\n<targetColumn>{}</targetColumn>\n\
             </fkColumn>\n",
            escape(from),
            escape(target)
        );
    }
    out.push_str("</foreignKey>\n");
}

/// A name out of somebody's parquet file is markup until it is escaped.
fn escape(value: &str) -> String {
    quick_xml::escape::escape(value).into_owned()
}

#[cfg(test)]
mod tests {
    use crate::tap::metadata::ColumnMetadata;

    use super::*;

    fn described() -> Vec<TableMetadata> {
        vec![TableMetadata {
            schema: "sky".to_owned(),
            qualified: "sky.objects".to_owned(),
            description: Some("Objects & their positions".to_owned()),
            columns: vec![
                ColumnMetadata {
                    name: "ra".to_owned(),
                    datatype: "double",
                    arraysize: None,
                    xtype: None,
                    unit: Some("deg"),
                    ucd: Some("pos.eq.ra;meta.main"),
                    indexed: true,
                    principal: true,
                    std: false,
                },
                ColumnMetadata {
                    name: "observed".to_owned(),
                    datatype: "char",
                    arraysize: Some("*"),
                    xtype: Some(crate::output::votable::TIMESTAMP),
                    unit: None,
                    ucd: None,
                    indexed: false,
                    principal: true,
                    std: false,
                },
            ],
            keys: vec![crate::tap::metadata::ForeignKey {
                id: "k".to_owned(),
                target_table: "sky.fields".to_owned(),
                columns: vec![("field_id".to_owned(), "id".to_owned())],
            }],
        }]
    }

    #[test]
    fn a_tableset_carries_the_columns_and_what_is_known_about_them() {
        let out = render(&described(), true);
        assert!(out.contains("<name>sky</name>"), "{out}");
        assert!(out.contains("<name>sky.objects</name>"), "{out}");
        assert!(out.contains("<ucd>pos.eq.ra;meta.main</ucd>"), "{out}");
        assert!(out.contains("<unit>deg</unit>"), "{out}");
        assert!(
            out.contains("<dataType xsi:type=\"vod:VOTableType\">double</dataType>"),
            "{out}"
        );
        assert!(out.contains("<flag>indexed</flag>"), "{out}");
        // VODataService has no `xtype` attribute in any version; what carries a DALI xtype
        // is `extendedType`, with no `extendedSchema` beside it to send a reader elsewhere.
        assert!(
            out.contains(
                "<dataType xsi:type=\"vod:VOTableType\" arraysize=\"*\" \
                 extendedType=\"timestamp\">char</dataType>"
            ),
            "{out}"
        );
        assert!(!out.contains("extendedSchema"), "{out}");
        // A description out of a catalog is markup until it is escaped.
        assert!(out.contains("Objects &amp; their positions"), "{out}");
        // The keys, which `TAP_SCHEMA.keys` reads out of the same list — a validator
        // compares the two documents against each other.
        assert!(
            out.contains("<targetTable>sky.fields</targetTable>"),
            "{out}"
        );
        assert!(out.contains("<fromColumn>field_id</fromColumn>"), "{out}");
    }

    /// `detail=min` is the names alone, which is what a client asks for when it wants a
    /// list rather than every column of a 150-column catalog.
    #[test]
    fn detail_min_leaves_the_columns_out() {
        let out = render(&described(), false);
        assert!(out.contains("<name>sky.objects</name>"), "{out}");
        assert!(!out.contains("<column>"), "{out}");
    }

    /// Answered with the whole document, a `detail` this service does not know is a caller
    /// who asked for less and cannot tell that from being ignored.
    #[test]
    fn a_detail_this_resource_does_not_know_is_refused() {
        let asked = |value: Option<&str>| Detail {
            detail: value.map(str::to_owned),
            unknown: BTreeMap::new(),
        };
        assert!(wanted(&asked(None)).unwrap());
        assert!(!wanted(&asked(Some("min"))).unwrap());
        assert!(wanted(&asked(Some("max"))).is_err());
        assert!(wanted(&asked(Some(""))).is_err());
    }
}
