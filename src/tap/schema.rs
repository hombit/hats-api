//! `TAP_SCHEMA`: the service's own metadata, as five tables a client can query.
//!
//! TAP §4's tables, held in memory and built per request out of what
//! [`crate::tap::metadata`] describes. It is the same content VOSI's `/tables` publishes —
//! a client asks whichever way suits it — and it describes itself as well, since querying
//! `TAP_SCHEMA.columns` is the first thing a client does and it has to find the answer's
//! own columns there.
//!
//! **Nothing here is held between requests.** The five tables are rebuilt each time one is
//! named, out of a fresh read of the catalogs. What that costs is a few small `GET`s on a
//! metadata query; what it buys is that a catalog an operator republished is described as
//! it is now rather than as it was when the process started.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;

use crate::error::ApiError;
use crate::tap::metadata::{ColumnMetadata, ForeignKey, TableMetadata};

/// The schema every one of these tables is in, and the one name a client hardcodes.
pub const TAP_SCHEMA: &str = "TAP_SCHEMA";

/// The five tables of TAP §4, in the order they describe each other.
pub const TABLES: [&str; 5] = ["schemas", "tables", "columns", "keys", "key_columns"];

/// Which of the five a name refers to, or nothing.
///
/// **The one place a name is matched case-insensitively.** A client cannot have read this
/// name off an answer it has not received yet, so the fixed names of TAP §4 are what it
/// hardcodes — and which case it hardcodes them in is the client's business. Every name
/// this service *publishes* is matched exactly, the client having read that one first.
pub fn resolve(spelling: &str) -> Option<String> {
    let (schema, table) = spelling.split_once('.')?;
    if !schema.eq_ignore_ascii_case(TAP_SCHEMA) {
        return None;
    }
    TABLES
        .iter()
        .find(|known| known.eq_ignore_ascii_case(table))
        .map(|known| format!("{TAP_SCHEMA}.{known}"))
}

/// What each of the five holds, which is also what `TAP_SCHEMA` publishes about itself.
///
/// The columns are TAP §4's, spelled as it spells them — `"size"` included, which that
/// section writes with its quotes because `SIZE` is a reserved word in ADQL. These are the
/// published spellings as they stand, so nothing decides them a second time: a catalog's
/// own column goes through `adql::names::as_written`, and that applies the shape of a name
/// and not the reserved list.
fn definition(table: &str) -> &'static [(&'static str, Column)] {
    match table {
        "schemas" => &[
            ("schema_name", Column::Text),
            ("utype", Column::Text),
            ("description", Column::Text),
            ("schema_index", Column::Number),
        ],
        "tables" => &[
            ("schema_name", Column::Text),
            ("table_name", Column::Text),
            ("table_type", Column::Text),
            ("utype", Column::Text),
            ("description", Column::Text),
            ("table_index", Column::Number),
        ],
        "columns" => &[
            ("table_name", Column::Text),
            ("column_name", Column::Text),
            ("datatype", Column::Text),
            ("arraysize", Column::Text),
            ("xtype", Column::Text),
            ("\"size\"", Column::Number),
            ("description", Column::Text),
            ("utype", Column::Text),
            ("unit", Column::Text),
            ("ucd", Column::Text),
            ("indexed", Column::Number),
            ("principal", Column::Number),
            ("std", Column::Number),
            ("column_index", Column::Number),
        ],
        "keys" => &[
            ("key_id", Column::Text),
            ("from_table", Column::Text),
            ("target_table", Column::Text),
            ("description", Column::Text),
            ("utype", Column::Text),
        ],
        "key_columns" => &[
            ("key_id", Column::Text),
            ("from_column", Column::Text),
            ("target_column", Column::Text),
        ],
        _ => &[],
    }
}

/// The two kinds of value TAP §4's own tables hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Column {
    Text,
    /// An integer, which is also how §4.3 spells a boolean: `indexed`, `principal` and
    /// `std` are 0 or 1 and no other value is allowed.
    Number,
}

impl Column {
    const fn arrow(self) -> DataType {
        match self {
            Self::Text => DataType::Utf8,
            Self::Number => DataType::Int32,
        }
    }

    const fn votable(self) -> (&'static str, Option<&'static str>) {
        match self {
            Self::Text => ("unicodeChar", Some("*")),
            Self::Number => ("int", None),
        }
    }
}

/// `TAP_SCHEMA` as it describes itself, which goes into the documents beside the published
/// tables.
///
/// Every column here is `std`: these are defined by TAP rather than by whoever wrote a
/// catalog, which is the distinction §4.3 draws.
pub fn self_description() -> Vec<TableMetadata> {
    TABLES
        .iter()
        .map(|table| TableMetadata {
            schema: TAP_SCHEMA.to_owned(),
            qualified: format!("{TAP_SCHEMA}.{table}"),
            description: Some(format!("The {table} of TAP_SCHEMA.")),
            columns: definition(table)
                .iter()
                .map(|(name, kind)| {
                    let (datatype, arraysize) = kind.votable();
                    ColumnMetadata {
                        // As TAP writes it, quotes and all: the definition above *is* the
                        // published spelling, so nothing here decides it a second time.
                        name: (*name).to_owned(),
                        datatype,
                        arraysize,
                        // Text and whole numbers, both of which mean what they say.
                        xtype: None,
                        unit: None,
                        ucd: None,
                        indexed: false,
                        principal: true,
                        std: true,
                    }
                })
                .collect(),
            keys: KEYS
                .iter()
                .filter(|(_, from, ..)| from == table)
                .map(|(id, _, from, target, target_column)| ForeignKey {
                    id: (*id).to_owned(),
                    target_table: format!("{TAP_SCHEMA}.{target}"),
                    columns: vec![((*from).to_owned(), (*target_column).to_owned())],
                })
                .collect(),
        })
        .collect()
}

/// The foreign keys `TAP_SCHEMA` has, which are its own: `(key, from table, from column,
/// target table, target column)`.
///
/// These are in TAP §4's definition of these tables rather than anything this service
/// decided, so publishing them is describing the schema accurately — a client can join
/// `columns` to `tables` and get what the standard says it gets. Nothing else here declares
/// one: a key between two catalogs is a claim about their contents that nobody made, and an
/// invented one would be a join whose answer is wrong rather than empty.
const KEYS: [(&str, &str, &str, &str, &str); 5] = [
    (
        "k_tables_schemas",
        "tables",
        "schema_name",
        "schemas",
        "schema_name",
    ),
    (
        "k_columns_tables",
        "columns",
        "table_name",
        "tables",
        "table_name",
    ),
    ("k_keys_from", "keys", "from_table", "tables", "table_name"),
    (
        "k_keys_target",
        "keys",
        "target_table",
        "tables",
        "table_name",
    ),
    (
        "k_key_columns_keys",
        "key_columns",
        "key_id",
        "keys",
        "key_id",
    ),
];

/// One of the five, holding what these tables describe.
pub fn provider(
    table: &str,
    described: &[TableMetadata],
) -> Result<Arc<dyn TableProvider>, ApiError> {
    let batch = rows_of(table, described)?;
    let schema = batch.schema();
    let provider = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|error| ApiError::internal(format!("{TAP_SCHEMA}.{table}: {error}")))?;
    Ok(Arc::new(provider))
}

/// One of the five as a batch, which is what [`provider`] holds and what a test reads.
fn rows_of(table: &str, described: &[TableMetadata]) -> Result<RecordBatch, ApiError> {
    let definition = definition(table);
    let schema = Arc::new(Schema::new(
        definition
            .iter()
            .map(|(name, kind)| Field::new(unquoted(name), kind.arrow(), true))
            .collect::<Vec<_>>(),
    ));
    let rows = match table {
        "schemas" => schemas(described),
        "tables" => tables(described),
        "columns" => columns(described),
        "keys" => keys(described),
        "key_columns" => key_columns(described),
        _ => Vec::new(),
    };
    batch(&schema, definition, &rows)
}

/// A published name carries its quotes; the arrow field behind it is the name itself.
fn unquoted(name: &str) -> &str {
    name.trim_matches('"')
}

/// One row, as a value per column of the definition. `None` is a null.
type Row = Vec<Option<String>>;

fn schemas(described: &[TableMetadata]) -> Vec<Row> {
    let mut names: Vec<&str> = described
        .iter()
        .map(|table| table.schema.as_str())
        .collect();
    names.dedup();
    names
        .into_iter()
        .enumerate()
        .map(|(index, name)| vec![Some(name.to_owned()), None, None, Some(index.to_string())])
        .collect()
}

fn tables(described: &[TableMetadata]) -> Vec<Row> {
    described
        .iter()
        .enumerate()
        .map(|(index, table)| {
            vec![
                Some(table.schema.clone()),
                Some(table.qualified.clone()),
                // The only other value §4.2 allows is `view`, and nothing here is one.
                Some("table".to_owned()),
                None,
                table.description.clone(),
                Some(index.to_string()),
            ]
        })
        .collect()
}

fn columns(described: &[TableMetadata]) -> Vec<Row> {
    let flag = |set: bool| Some(i32::from(set).to_string());
    described
        .iter()
        .flat_map(|table| {
            table
                .columns
                .iter()
                .enumerate()
                .map(move |(index, column)| {
                    vec![
                        Some(table.qualified.clone()),
                        Some(column.name.clone()),
                        Some(column.datatype.to_owned()),
                        column.arraysize.map(str::to_owned),
                        column.xtype.map(str::to_owned),
                        None,
                        None,
                        None,
                        column.unit.map(str::to_owned),
                        column.ucd.map(str::to_owned),
                        flag(column.indexed),
                        flag(column.principal),
                        flag(column.std),
                        Some(index.to_string()),
                    ]
                })
        })
        .collect()
}

/// Out of what the tables describe rather than out of [`KEYS`], so that these rows and
/// VOSI's `<foreignKey>` cannot come to say different things — which is what a validator
/// compares them for.
fn keys(described: &[TableMetadata]) -> Vec<Row> {
    described
        .iter()
        .flat_map(|table| {
            table.keys.iter().map(move |key| {
                vec![
                    Some(key.id.clone()),
                    Some(table.qualified.clone()),
                    Some(key.target_table.clone()),
                    None,
                    None,
                ]
            })
        })
        .collect()
}

fn key_columns(described: &[TableMetadata]) -> Vec<Row> {
    described
        .iter()
        .flat_map(|table| &table.keys)
        .flat_map(|key| {
            key.columns.iter().map(move |(from, target)| {
                vec![
                    Some(key.id.clone()),
                    Some(from.clone()),
                    Some(target.clone()),
                ]
            })
        })
        .collect()
}

/// The rows as arrow, one array per column.
fn batch(
    schema: &SchemaRef,
    definition: &[(&str, Column)],
    rows: &[Row],
) -> Result<RecordBatch, ApiError> {
    let columns = definition
        .iter()
        .enumerate()
        .map(|(index, (_, kind))| {
            let values = rows.iter().map(|row| row.get(index).cloned().flatten());
            match kind {
                Column::Text => Arc::new(values.collect::<StringArray>()) as ArrayRef,
                Column::Number => Arc::new(
                    values
                        .map(|value| value.and_then(|text| text.parse::<i32>().ok()))
                        .collect::<Int32Array>(),
                ) as ArrayRef,
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|error| ApiError::internal(format!("{TAP_SCHEMA} could not be built: {error}")))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::AsArray;

    use crate::tap::metadata::{ColumnMetadata, TableMetadata};

    use super::*;

    fn published() -> Vec<TableMetadata> {
        let mut described = self_description();
        described.push(TableMetadata {
            schema: "sky".to_owned(),
            qualified: "sky.objects".to_owned(),
            description: None,
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
                    name: "band".to_owned(),
                    datatype: "unicodeChar",
                    arraysize: Some("*"),
                    xtype: None,
                    unit: None,
                    ucd: None,
                    indexed: false,
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
            keys: Vec::new(),
        });
        described
    }

    /// Whatever case a client hardcoded, and nothing else — a name this service publishes
    /// is matched exactly.
    #[test]
    fn the_bootstrap_names_resolve_however_they_are_spelled() {
        for spelling in [
            "TAP_SCHEMA.tables",
            "tap_schema.tables",
            "Tap_Schema.Tables",
        ] {
            assert_eq!(
                resolve(spelling).as_deref(),
                Some("TAP_SCHEMA.tables"),
                "{spelling}"
            );
        }
        for spelling in ["TAP_SCHEMA.nope", "gaia.tables", "tables", "TAP_UPLOAD.t"] {
            assert_eq!(resolve(spelling), None, "{spelling}");
        }
    }

    /// The first thing a client queries is `TAP_SCHEMA.columns`, so the answer's own
    /// columns have to be in it.
    #[test]
    fn tap_schema_describes_itself_and_the_published_tables() {
        let described = published();
        let names = |table: &str, column: &str| text(&rows_of(table, &described).unwrap(), column);

        let listed = names("tables", "table_name");
        assert!(
            listed.contains(&"TAP_SCHEMA.columns".to_owned()),
            "{listed:?}"
        );
        assert!(listed.contains(&"sky.objects".to_owned()), "{listed:?}");

        // One schema row apiece, and no repeats.
        let schemas = names("schemas", "schema_name");
        assert_eq!(schemas, ["TAP_SCHEMA", "sky"]);

        let described_columns = names("columns", "column_name");
        assert!(described_columns.contains(&"ra".to_owned()));
        // Published with its quotes, SIZE being reserved in ADQL.
        assert!(described_columns.contains(&"\"size\"".to_owned()));
    }

    /// 0 or 1, no other value allowed, and each says something different about a column.
    #[test]
    fn the_flags_are_integers() {
        let batch = rows_of("columns", &published()).unwrap();
        let numbers = |name: &str| {
            let index = batch.schema().index_of(name).unwrap();
            batch
                .column(index)
                .as_primitive::<datafusion::arrow::datatypes::Int32Type>()
                .iter()
                .map(|value| value.unwrap_or(-1))
                .collect::<Vec<_>>()
        };
        for flag in ["indexed", "principal", "std"] {
            assert!(
                numbers(flag).iter().all(|value| (0..=1).contains(value)),
                "{flag}: {:?}",
                numbers(flag)
            );
        }
        // TAP's own columns are defined by a standard and a catalog's are not.
        for (name, std) in text(&batch, "table_name").iter().zip(numbers("std")) {
            assert_eq!(std == 1, name.starts_with(TAP_SCHEMA), "{name}");
        }
    }

    /// `TAP_SCHEMA`'s own keys are in TAP §4's definition of it, so describing itself
    /// means declaring them. Nothing else here declares one: a key between two catalogs is
    /// a claim about their contents that nobody made.
    #[test]
    fn tap_schemas_own_foreign_keys_are_declared() {
        let keys = rows_of("keys", &published()).unwrap();
        assert_eq!(keys.num_rows(), KEYS.len());
        assert!(text(&keys, "from_table").contains(&"TAP_SCHEMA.columns".to_owned()));
        assert_eq!(
            text(&keys, "target_table")
                .iter()
                .filter(|target| *target == "TAP_SCHEMA.tables")
                .count(),
            3
        );

        // One column pair per key, joined to it by the same id.
        let columns = rows_of("key_columns", &published()).unwrap();
        assert_eq!(text(&columns, "key_id"), text(&keys, "key_id"));
        assert!(text(&columns, "from_column").contains(&"table_name".to_owned()));

        for table in ["keys", "key_columns"] {
            assert!(provider(table, &published()).is_ok(), "{table}");
        }
    }

    /// What DALI calls an instant is `xtype` here and `extendedType` in VOSI's document,
    /// and a client reads one of the two before it decides how to parse a column.
    #[test]
    fn an_instant_column_publishes_its_xtype() {
        let batch = rows_of("columns", &published()).unwrap();
        let by_name = text(&batch, "column_name")
            .into_iter()
            .zip(text(&batch, "xtype"))
            .collect::<Vec<_>>();
        assert!(
            by_name.contains(&(
                "observed".to_owned(),
                crate::output::votable::TIMESTAMP.to_owned()
            )),
            "{by_name:?}"
        );
        // And nothing else claims to be one: a null is what an ordinary column carries.
        assert!(
            by_name.contains(&("ra".to_owned(), String::new())),
            "{by_name:?}"
        );
    }

    /// A name ADQL cannot write bare carries its quotes, which is what a client copies.
    #[test]
    fn a_published_name_is_one_a_query_can_write() {
        let columns = text(&rows_of("columns", &published()).unwrap(), "column_name");
        assert!(columns.contains(&"\"size\"".to_owned()), "{columns:?}");
        assert!(columns.contains(&"table_name".to_owned()), "{columns:?}");
    }

    /// One text column's values, with a null read as empty.
    fn text(batch: &RecordBatch, column: &str) -> Vec<String> {
        let index = batch.schema().index_of(column).unwrap();
        batch
            .column(index)
            .as_string::<i32>()
            .iter()
            .map(|value| value.unwrap_or_default().to_owned())
            .collect()
    }
}
