//! What this service says about a table it publishes.
//!
//! One description, read by both of the resources that publish metadata: `TAP_SCHEMA`'s
//! tables, which a client queries, and VOSI's `/tables`, which a client fetches. The two
//! are the same facts in two documents, and a client that read one and queried against the
//! other would find the names disagreeing — so neither builds its own.
//!
//! Nothing here reads anything. It is handed an arrow schema and what the catalog says
//! about its own columns, and turns the pair into rows.

use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};

use crate::adql::names;
use crate::output::votable;

/// One published table.
#[derive(Debug, Clone)]
pub struct TableMetadata {
    /// The schema half of the published name.
    pub schema: String,
    /// The whole name, `schema.table`, which is what a query writes.
    pub qualified: String,
    /// What a client sees in a table browser, or nothing.
    pub description: Option<String>,
    pub columns: Vec<ColumnMetadata>,
    /// What this table's columns refer to in another. Only `TAP_SCHEMA`'s own tables have
    /// any: a key between two catalogs is a claim about their contents that nobody made.
    pub keys: Vec<ForeignKey>,
}

/// One foreign key, as both documents that publish it need it.
///
/// `TAP_SCHEMA.keys` and VOSI's `<foreignKey>` are the same fact in two places, and a
/// validator compares them — so there is one list and both are rendered from it.
#[derive(Debug, Clone)]
pub struct ForeignKey {
    /// What `TAP_SCHEMA.keys` and `TAP_SCHEMA.key_columns` join on. VOSI has no room for
    /// it and does not need one, a key being nested inside its table there.
    pub id: String,
    /// The table the columns refer to, by its published name.
    pub target_table: String,
    /// Each column of this table beside the one it refers to.
    pub columns: Vec<(String, String)>,
}

/// One column of one table, in the terms both documents use.
#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    /// The name that selects this value, delimited where ADQL needs it delimited — which
    /// is what TAP §4.3 asks the published name to be. A leaf of a nested column carries
    /// the dotted path that reaches it.
    pub name: String,
    /// The VOTable datatype, as [`votable::spelling`] has it.
    pub datatype: &'static str,
    /// `"*"` for a value of no fixed length — a string, or one row's whole array.
    pub arraysize: Option<&'static str>,
    /// What the characters of a value mean where DALI names it, which here is an instant.
    /// Both documents carry it, under the two names their own standards give it.
    pub xtype: Option<&'static str>,
    pub unit: Option<&'static str>,
    pub ucd: Option<&'static str>,
    /// Whether a constraint on this column makes a query read less. Here that is the two
    /// coordinate columns and the spatial index: a region over the pair chooses which
    /// partitions are opened, and the index prunes row groups inside them.
    pub indexed: bool,
    /// Whether the column is part of the content rather than the machinery. Every column
    /// of a catalog is, except the HEALPix index — which is a number the importer wrote to
    /// make the tiling work and which nobody asked a catalog for.
    pub principal: bool,
    /// Whether some standard defines the column. Nothing in somebody's catalog does;
    /// `TAP_SCHEMA`'s own columns do.
    pub std: bool,
}

/// What the catalog says about its own columns, which is the only thing that can say it.
#[derive(Debug, Clone, Copy, Default)]
pub struct Marks<'a> {
    pub ra: Option<&'a str>,
    pub dec: Option<&'a str>,
    /// The spatial index, where the catalog names one and the files have it.
    pub healpix: Option<&'a str>,
}

/// Degrees, which is what HATS stores a position in.
const DEGREES: &str = "deg";

/// UCD1+ for the two coordinates. `meta.main` says this is *the* position of the row
/// rather than one of several, which is how a client told nothing else finds it.
const RA_UCD: &str = "pos.eq.ra;meta.main";
const DEC_UCD: &str = "pos.eq.dec;meta.main";

/// Describe one published table.
pub fn describe(
    qualified: &str,
    schema_name: &str,
    fields: &SchemaRef,
    marks: Marks<'_>,
    description: Option<String>,
) -> TableMetadata {
    let mut columns = Vec::new();
    for field in fields.fields() {
        columns.extend(declare(field, marks));
    }
    TableMetadata {
        schema: schema_name.to_owned(),
        qualified: qualified.to_owned(),
        description,
        columns,
        keys: Vec::new(),
    }
}

/// One arrow field, as the zero or more columns a client may name.
///
/// **A nested column is declared by its leaves, dotted, and the struct itself is not a
/// row.** A leaf has a type and the struct has none — a row for the struct could carry
/// only an invented one — and the dotted name is what selects the value, which is what
/// TAP §4.3 asks the published name to be. Depth stops there: a struct inside a struct has
/// no leaf with a spelling either, so it is absent for the same reason.
///
/// **The dot is structure and must not be quoted.** TAP §4.3 has a name that needs quoting
/// published *with* its quotes; this is the case it does not anticipate, a name that must
/// not be. `"lightcurve.mag"` is one delimited identifier naming no field.
///
/// A column whose type has no VOTable spelling at all is absent rather than invented.
/// Declaring it would offer a name whose answer nothing can describe.
fn declare(field: &Field, marks: Marks<'_>) -> Vec<ColumnMetadata> {
    if let DataType::Struct(inner) = field.data_type() {
        return inner
            .iter()
            .filter_map(|leaf| {
                let spelled = element(leaf)?;
                Some(ColumnMetadata {
                    name: names::as_written(&format!("{}.{}", field.name(), leaf.name())),
                    datatype: spelled.datatype,
                    arraysize: spelled.arraysize,
                    xtype: spelled.xtype,
                    unit: None,
                    ucd: None,
                    indexed: false,
                    // A light curve's magnitudes are the content if anything is.
                    principal: true,
                    std: false,
                })
            })
            .collect();
    }
    let Some(spelled) = element(field) else {
        return Vec::new();
    };
    let name = field.name();
    let is = |marked: Option<&str>| marked == Some(name.as_str());
    let (unit, ucd) = match (is(marks.ra), is(marks.dec)) {
        (true, _) => (Some(DEGREES), Some(RA_UCD)),
        (_, true) => (Some(DEGREES), Some(DEC_UCD)),
        _ => (None, None),
    };
    vec![ColumnMetadata {
        name: names::as_written(name),
        datatype: spelled.datatype,
        arraysize: spelled.arraysize,
        xtype: spelled.xtype,
        unit,
        ucd,
        indexed: is(marks.ra) || is(marks.dec) || is(marks.healpix),
        principal: !is(marks.healpix),
        std: false,
    }]
}

/// How a field is declared, reading through a list to what it holds.
///
/// A list of scalars is one value of no fixed length, which VOTable spells as the
/// element's own datatype with `arraysize="*"` — the same spelling a string gets, a string
/// being an array of characters. A list of anything else has no spelling and is `None`.
///
/// **A list of instants is one of those.** An `xtype` says what the characters of one value
/// are, and a run of them is not that value: a client reading `xtype="timestamp"` would
/// parse the whole array as a single date and fail. The collision a string and a list of
/// strings already have is survivable because neither claims to be anything but characters.
fn element(field: &Field) -> Option<votable::Spelling> {
    match field.data_type() {
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            let spelled = votable::spelling(item).ok()?;
            spelled.xtype.is_none().then_some(votable::Spelling {
                arraysize: Some("*"),
                ..spelled
            })
        }
        _ => votable::spelling(field).ok(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{Fields, Schema, TimeUnit};

    use super::*;

    fn schema(fields: Vec<Field>) -> SchemaRef {
        Arc::new(Schema::new(fields))
    }

    fn described(fields: Vec<Field>, marks: Marks<'_>) -> Vec<ColumnMetadata> {
        describe("sky.objects", "sky", &schema(fields), marks, None).columns
    }

    /// The two coordinates carry the UCD and the unit a client finds a position by, and
    /// they are what a constraint prunes on.
    #[test]
    fn the_catalogs_own_columns_are_marked() {
        let columns = described(
            vec![
                Field::new("ra", DataType::Float64, true),
                Field::new("dec", DataType::Float64, true),
                Field::new("_healpix_29", DataType::Int64, false),
                Field::new("phot_g_mean_mag", DataType::Float32, true),
            ],
            Marks {
                ra: Some("ra"),
                dec: Some("dec"),
                healpix: Some("_healpix_29"),
            },
        );
        let by_name = |wanted: &str| {
            columns
                .iter()
                .find(|column| column.name == wanted)
                .unwrap()
                .clone()
        };
        assert_eq!(by_name("ra").ucd, Some(RA_UCD));
        assert_eq!(by_name("ra").unit, Some(DEGREES));
        assert_eq!(by_name("dec").ucd, Some(DEC_UCD));
        assert!(by_name("ra").indexed && by_name("dec").indexed);
        // The index is what a query prunes on and is not what a catalog is about — and
        // ADQL cannot write a leading underscore bare, so the published name has quotes.
        assert!(by_name("\"_healpix_29\"").indexed);
        assert!(!by_name("\"_healpix_29\"").principal);
        // `dec` is a reserved word and is published bare all the same.
        assert_eq!(by_name("dec").ucd, Some(DEC_UCD));
        // An ordinary column: content, no unit this service could know, nothing to prune.
        let ordinary = by_name("phot_g_mean_mag");
        assert!(ordinary.principal && !ordinary.indexed && !ordinary.std);
        assert_eq!(ordinary.ucd, None);
        assert_eq!(ordinary.datatype, "float");
    }

    /// A light curve is declared by the names that select its parts, and the struct
    /// itself is not one of them — it has no type a document could carry.
    #[test]
    fn a_nested_column_is_declared_by_its_leaves() {
        let inner = Fields::from(vec![
            Field::new(
                "mag",
                DataType::List(Arc::new(Field::new("element", DataType::Float32, true))),
                true,
            ),
            Field::new(
                "mjd",
                DataType::List(Arc::new(Field::new("element", DataType::Float64, true))),
                true,
            ),
        ]);
        let columns = described(
            vec![Field::new("lightcurve", DataType::Struct(inner), true)],
            Marks::default(),
        );
        assert_eq!(
            columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["lightcurve.mag", "lightcurve.mjd"]
        );
        // One row's whole array, which VOTable spells as the element's type of no fixed
        // length.
        assert_eq!(columns[0].datatype, "float");
        assert_eq!(columns[0].arraysize, Some("*"));
        assert_eq!(columns[1].datatype, "double");
    }

    /// A type nothing can describe is absent rather than invented: the published name
    /// would otherwise be one whose answer no document can say anything about.
    #[test]
    fn a_column_with_no_spelling_is_left_out() {
        let columns = described(
            vec![
                Field::new("id", DataType::Int64, false),
                // Nothing VOTable has a type for.
                Field::new("counts", DataType::UInt64, true),
                Field::new(
                    "map",
                    DataType::Map(
                        Arc::new(Field::new(
                            "entries",
                            DataType::Struct(Fields::from(vec![
                                Field::new("keys", DataType::Utf8, false),
                                Field::new("values", DataType::Int32, true),
                            ])),
                            false,
                        )),
                        false,
                    ),
                    true,
                ),
            ],
            Marks::default(),
        );
        assert_eq!(
            columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["id"]
        );
    }

    /// A stamped row is published as an instant, so a client knows to parse the characters
    /// as one — and an array of them is not published at all, an `xtype` being a claim
    /// about one value.
    #[test]
    fn an_instant_carries_its_xtype_and_an_array_of_them_has_no_spelling() {
        let columns = described(
            vec![
                Field::new(
                    "observed",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
                Field::new("day", DataType::Date32, true),
                Field::new(
                    "epochs",
                    DataType::List(Arc::new(Field::new(
                        "element",
                        DataType::Timestamp(TimeUnit::Second, None),
                        true,
                    ))),
                    true,
                ),
            ],
            Marks::default(),
        );
        assert_eq!(
            columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["observed", "day"]
        );
        assert_eq!(columns[0].datatype, "char");
        assert_eq!(columns[0].arraysize, Some("*"));
        assert_eq!(columns[0].xtype, Some(votable::TIMESTAMP));
        // A date cannot carry a time, and its width says as much.
        assert_eq!(columns[1].arraysize, Some("10"));
        assert_eq!(columns[1].xtype, Some(votable::TIMESTAMP));
    }

    /// A string is an array of characters, which is the same spelling a list gets.
    #[test]
    fn a_string_is_a_variable_length_value() {
        let columns = described(
            vec![Field::new("band", DataType::Utf8, true)],
            Marks::default(),
        );
        assert_eq!(columns[0].datatype, "unicodeChar");
        assert_eq!(columns[0].arraysize, Some("*"));
    }
}
