//! Writes the corpus tables with the CDS `votable` crate, as a third independent writer.
//!
//! Usage: `cds-writer <spec.json> <out-dir>`. The spec is written by `generate.py`: a list of
//! tables, each with its fields and its rows in the corpus's JSON cell encoding. Every table
//! is written once per serialization named in it, as `cds-<serialization>-<table>.vot`.
//!
//! The crate serializes whatever `VOTableValue` it is handed, so the conversion below does
//! what its writer does not: a fixed-width string is padded to its width with NULs for the
//! binary serializations (the crate writes the bytes it is given, whatever their number), and
//! the strings of a 2-D char column are padded with spaces for TABLEDATA, where they are
//! concatenated with nothing between them.

use std::{env, fs, path::Path, str::FromStr};

use serde_json::Value;
use votable::{
    Data, Description, Field, Link, Resource, Table, Values,
    datatype::Datatype,
    field::{ArraySize, Precision},
    impls::{VOTableValue, mem::InMemTableDataRows},
    resource::ResourceSubElem,
    values::{Max, Min, Opt},
    votable::{VOTable, Version},
};

#[derive(Clone, Copy, PartialEq)]
enum Serialization {
    TableData,
    Binary,
    Binary2,
}

impl Serialization {
    fn parse(s: &str) -> Self {
        match s {
            "tabledata" => Self::TableData,
            "binary" => Self::Binary,
            "binary2" => Self::Binary2,
            other => panic!("unknown serialization {other}"),
        }
    }
}

fn float(v: &Value) -> f64 {
    match v {
        Value::Number(n) => n.as_f64().unwrap(),
        Value::String(s) if s == "NaN" => f64::NAN,
        Value::String(s) if s == "Infinity" => f64::INFINITY,
        Value::String(s) if s == "-Infinity" => f64::NEG_INFINITY,
        other => panic!("not a float: {other}"),
    }
}

fn int(v: &Value) -> i64 {
    v.as_i64().unwrap_or_else(|| panic!("not an integer: {v}"))
}

/// An integer element of an array; `null` inside an array is the column's magic value.
fn int_elem(v: &Value, null: Option<i64>) -> i64 {
    match v {
        Value::Null => null.expect("null array element without a VALUES null"),
        other => int(other),
    }
}

fn pad(s: &str, width: usize, with: char, unicode: bool) -> String {
    let len = if unicode {
        s.encode_utf16().count()
    } else {
        s.len()
    };
    let mut out = s.to_string();
    for _ in len..width {
        out.push(with);
    }
    out
}

fn fixed_width(arraysize: &Option<String>) -> Option<usize> {
    arraysize
        .as_deref()
        .filter(|a| !a.contains('*') && !a.contains('x'))
        .map(|a| a.parse().unwrap())
}

fn cell_to_value(field: &Value, cell: &Value, ser: Serialization) -> VOTableValue {
    if cell.is_null() {
        return VOTableValue::Null;
    }
    let datatype = field["datatype"].as_str().unwrap();
    let arraysize = field["arraysize"].as_str().map(str::to_string);
    let null: Option<i64> = field["null"].as_str().map(|s| s.parse().unwrap());
    let binary = ser != Serialization::TableData;
    let list = || cell.as_array().unwrap();
    let ints = || list().iter().map(|e| int_elem(e, null));
    let floats = || list().iter().map(float);
    match (datatype, arraysize.is_some()) {
        ("boolean", false) => VOTableValue::Bool(cell.as_bool().unwrap()),
        ("unsignedByte", false) => VOTableValue::Byte(int(cell) as u8),
        ("short", false) => VOTableValue::Short(int(cell) as i16),
        ("int", false) => VOTableValue::Int(int(cell) as i32),
        ("long", false) => VOTableValue::Long(int(cell)),
        ("float", false) => VOTableValue::Float(float(cell) as f32),
        ("double", false) => VOTableValue::Double(float(cell)),
        ("floatComplex", false) => {
            let l = list();
            VOTableValue::ComplexFloat((float(&l[0]) as f32, float(&l[1]) as f32))
        }
        ("doubleComplex", false) => {
            let l = list();
            VOTableValue::ComplexDouble((float(&l[0]), float(&l[1])))
        }
        ("char", false) => VOTableValue::CharASCII(cell.as_str().unwrap().chars().next().unwrap()),
        ("unicodeChar", false) => {
            VOTableValue::CharUnicode(cell.as_str().unwrap().chars().next().unwrap())
        }
        ("char" | "unicodeChar", true) => {
            let unicode = datatype == "unicodeChar";
            let a = arraysize.as_deref().unwrap();
            if let Some((width, _)) = a.split_once('x') {
                // A 2-D char column: a list of fixed-width strings.
                let width: usize = width.parse().unwrap();
                let fill = if binary { '\0' } else { ' ' };
                VOTableValue::StringArray(
                    list()
                        .iter()
                        .map(|s| pad(s.as_str().unwrap(), width, fill, unicode))
                        .collect(),
                )
            } else {
                let s = cell.as_str().unwrap();
                match fixed_width(&arraysize) {
                    Some(width) if binary => VOTableValue::String(pad(s, width, '\0', unicode)),
                    _ => VOTableValue::String(s.to_string()),
                }
            }
        }
        ("boolean", true) => {
            VOTableValue::BooleanArray(list().iter().map(|b| b.as_bool()).collect())
        }
        ("unsignedByte", true) => VOTableValue::ByteArray(ints().map(|v| v as u8).collect()),
        ("short", true) => VOTableValue::ShortArray(ints().map(|v| v as i16).collect()),
        ("int", true) => VOTableValue::IntArray(ints().map(|v| v as i32).collect()),
        ("long", true) => VOTableValue::LongArray(ints().collect()),
        ("float", true) => VOTableValue::FloatArray(floats().map(|v| v as f32).collect()),
        ("double", true) => VOTableValue::DoubleArray(floats().collect()),
        ("floatComplex", true) => {
            let v: Vec<f32> = floats().map(|v| v as f32).collect();
            VOTableValue::ComplexFloatArray(v.chunks(2).map(|c| (c[0], c[1])).collect())
        }
        ("doubleComplex", true) => {
            let v: Vec<f64> = floats().collect();
            VOTableValue::ComplexDoubleArray(v.chunks(2).map(|c| (c[0], c[1])).collect())
        }
        (other, _) => panic!("datatype {other} is not written by this program"),
    }
}

fn build_field(spec: &Value) -> Field {
    let s = |k: &str| spec[k].as_str();
    let mut field = Field::new(s("name").unwrap(), Datatype::from_str(s("datatype").unwrap()).unwrap());
    if let Some(v) = s("ID") {
        field = field.set_id(v);
    }
    if let Some(v) = s("arraysize") {
        field = field.set_arraysize(ArraySize::from_str(v).unwrap());
    }
    if let Some(v) = s("unit") {
        field = field.set_unit(v);
    }
    if let Some(v) = s("ucd") {
        field = field.set_ucd(v);
    }
    if let Some(v) = s("utype") {
        field = field.set_utype(v);
    }
    if let Some(v) = s("xtype") {
        field = field.set_xtype(v);
    }
    if let Some(v) = spec["width"].as_u64() {
        field = field.set_width(v as u16);
    }
    if let Some(v) = s("precision") {
        field = field.set_precision(Precision::from_str(v).unwrap());
    }
    if let Some(v) = s("description") {
        field = field.set_description(Description::new(v));
    }
    if let Some(v) = s("link") {
        field = field.push_link(Link::new().set_href(v));
    }
    let mut values: Option<Values> = None;
    if let Some(v) = s("null") {
        values = Some(values.unwrap_or_default().set_null(v));
    }
    if let Some(v) = s("min") {
        values = Some(values.unwrap_or_default().set_min(Min::new(v)));
    }
    if let Some(v) = s("max") {
        values = Some(values.unwrap_or_default().set_max(Max::new(v)));
    }
    if let Some(options) = spec["options"].as_array() {
        let mut vals = values.unwrap_or_default();
        for opt in options {
            vals = vals.push_opt(
                Opt::new(opt["value"].as_str().unwrap()).set_name(opt["name"].as_str().unwrap()),
            );
        }
        values = Some(vals);
    }
    if let Some(v) = values {
        field = field.set_values(v);
    }
    field
}

fn write_table(spec: &Value, ser_name: &str, out_dir: &Path) {
    let ser = Serialization::parse(ser_name);
    let fields: &Vec<Value> = spec["fields"].as_array().unwrap();
    let rows: Vec<Vec<VOTableValue>> = spec["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            fields
                .iter()
                .zip(row.as_array().unwrap())
                .map(|(f, c)| cell_to_value(f, c, ser))
                .collect()
        })
        .collect();
    let mut table = Table::new().set_name(spec["name"].as_str().unwrap());
    for f in fields {
        table = table.push_field(build_field(f));
    }
    let table = table.set_data(Data::new_empty().set_tabledata(InMemTableDataRows::new(rows)));
    let resource = Resource::default().push_sub_elem(ResourceSubElem::from_table(table));
    let version = Version::from_str(spec["version"].as_str().unwrap_or("1.4")).unwrap();
    let mut votable = VOTable::new(version, resource).wrap();
    match ser {
        Serialization::TableData => {}
        Serialization::Binary => votable.to_binary().unwrap(),
        Serialization::Binary2 => votable.to_binary2().unwrap(),
    }
    let name = spec["name"].as_str().unwrap();
    let path = out_dir.join(format!("cds-{ser_name}-{name}.vot"));
    votable.to_ivoa_xml_file(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let spec: Value = serde_json::from_str(&fs::read_to_string(&args[1]).unwrap()).unwrap();
    let out_dir = Path::new(&args[2]);
    for table in spec["tables"].as_array().unwrap() {
        for ser in table["serializations"].as_array().unwrap() {
            write_table(table, ser.as_str().unwrap(), out_dir);
        }
    }
}
