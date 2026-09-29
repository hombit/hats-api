//! A VOTable document, walked once from the top, into the table it carries.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig};
use base64::engine::{DecodePaddingMode, general_purpose};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use quick_xml::events::{BytesStart, Event};
use quick_xml::{Reader, XmlVersion};

use crate::votable::binary::{self, Cursor, Dialect};
use crate::votable::column::Builder;
use crate::votable::field::Declared;
use crate::votable::tabledata;

/// The table a document carries, as Arrow.
#[derive(Debug)]
pub struct Table {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

/// How many rows go into one batch. DataFusion's own default, so a scan over the table
/// hands its operators the size they are tuned for.
const BATCH_ROWS: usize = 8192;

/// How many items the document may expand into, per byte of it, before it is refused.
///
/// Every serialization spends bytes on every item but one: an empty `TD` is a null of any
/// size, so `<TD/>` under a `FIELD` of `arraysize="100000000"` is five bytes asking for a
/// hundred million values. A cell's items are really held — a fixed-size list keeps them
/// whether the cell is there or not — so this bounds what the upload's byte cap is meant to.
const ITEMS_PER_BYTE: usize = 16;

/// How many items any document may hold regardless of its size, so a short one with a few
/// wide columns is not refused for being short.
const ITEMS_FLOOR: usize = 1 << 20;

/// The `base64` of a `STREAM`, with or without its padding: writers differ, and a stream
/// that decodes the same either way means the same either way.
const BASE64: GeneralPurpose = GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

/// Whether a document starts the way a VOTable does: its first element is `VOTABLE`, in
/// whichever namespace and under whichever prefix.
///
/// Asked of the head of a file, so a document cut off after its first element still
/// answers; anything that is not XML, or whose root is something else — an HTML listing, an
/// error page — is not one.
pub fn is_votable(head: &[u8]) -> bool {
    let text = match text_of(head) {
        Ok(text) => text,
        // A head cut inside a multi-byte character is still worth reading up to that point.
        Err(_) => Cow::Owned(String::from_utf8_lossy(head).into_owned()),
    };
    let mut reader = Reader::from_str(&text);
    loop {
        match reader.read_event() {
            Ok(Event::Start(element) | Event::Empty(element)) => {
                return local(&element) == "VOTABLE";
            }
            Ok(Event::Eof) | Err(_) => return false,
            Ok(Event::Text(text)) if !text.trim_ascii().is_empty() => {
                return false;
            }
            Ok(_) => {}
        }
    }
}

/// Read the document's table.
///
/// **Which table**: the first `TABLE` that has a `DATA`, or where none has, the first
/// `TABLE`. A document carrying one table is the ordinary case and this is it; a `TABLE`
/// with no `DATA` ahead of it is the template §3.8 has a later `TABLE ref` point at, and is
/// not what the document is carrying.
///
/// **What is refused**: a `STREAM` whose data is anywhere but in the document, which is a
/// document naming a place for this service to read; a `FITS` serialization; and anything
/// the document says that is not what its `FIELD`s declare.
pub fn read(bytes: &[u8]) -> Result<Table, String> {
    let text = text_of(bytes)?;
    let budget = bytes.len().saturating_mul(ITEMS_PER_BYTE).max(ITEMS_FLOOR);
    Walker {
        reader: Reader::from_str(&text),
        stack: Vec::new(),
        tables: HashMap::new(),
        values: HashMap::new(),
        first: None,
        open: None,
        error: None,
        budget,
        items: 0,
        astropy: false,
    }
    .walk()
}

/// A `TABLE` whose `FIELD`s are being read.
#[derive(Debug, Default)]
struct Open {
    id: Option<String>,
    reference: Option<String>,
    /// The row count the `TABLE` declares, which is optional and is asked only to tell two
    /// readings of a stream apart.
    nrows: Option<usize>,
    fields: Vec<Declared>,
    /// The `FIELD` whose children are being read.
    field: Option<Declared>,
}

/// A decoded `BINARY` or `BINARY2` stream, and what its `TABLE` says about it.
struct Stream<'a> {
    bytes: &'a [u8],
    /// Whether each row starts with `BINARY2`'s null flags.
    flagged: bool,
    nrows: Option<usize>,
}

struct Walker<'a> {
    reader: Reader<&'a [u8]>,
    /// The elements open around the reader, by local name.
    stack: Vec<String>,
    /// Every `TABLE` with an `ID` so far, for a later `ref`.
    tables: HashMap<String, Vec<Declared>>,
    /// Every `VALUES` with an `ID` so far, by its `null`.
    values: HashMap<String, Option<String>>,
    /// The first `TABLE`'s fields, for a document in which no table has data.
    first: Option<Vec<Declared>>,
    open: Option<Open>,
    /// What an `INFO name="QUERY_STATUS" value="ERROR"` said, for a document that is a
    /// service's refusal rather than a table.
    error: Option<String>,
    budget: usize,
    /// Items read so far, against `budget`.
    items: usize,
    /// Whether the document says astropy wrote it, which it does in a comment ahead of the
    /// root: astropy lays two kinds of cell out differently from STIL.
    astropy: bool,
}

impl Walker<'_> {
    fn walk(mut self) -> Result<Table, String> {
        let mut rooted = false;
        // The table, once read. The document is still walked to its end: DALI §4.4 lets a
        // service say its query failed after the rows it had, and a table a failed query left
        // behind is not one to answer from as though it were whole.
        let mut found: Option<Table> = None;
        loop {
            let event = self
                .reader
                .read_event()
                .map_err(|error| xml_error(&error))?;
            let started = matches!(event, Event::Start(_));
            match event {
                Event::Start(element) | Event::Empty(element) if !rooted => {
                    if local(&element) != "VOTABLE" {
                        return Err(format!(
                            "this is XML whose root element is <{}>, not a VOTable",
                            element.name().as_ref()
                        ));
                    }
                    rooted = true;
                    if started {
                        self.stack.push("VOTABLE".to_owned());
                    }
                }
                Event::Start(element) => {
                    let name = local(&element).to_owned();
                    if let Some(table) = self.opened(&name, &element, false, found.is_some())? {
                        found = Some(table);
                    }
                }
                Event::Empty(element) => {
                    let name = local(&element).to_owned();
                    if let Some(table) = self.opened(&name, &element, true, found.is_some())? {
                        found = Some(table);
                    }
                }
                Event::End(_) => {
                    let name = self.stack.pop().unwrap_or_default();
                    self.closed(&name)?;
                }
                Event::Eof => break,
                Event::Comment(comment) if comment.contains("astropy") => self.astropy = true,
                Event::Text(text) if !rooted && !text.trim_ascii().is_empty() => {
                    return Err("this is not a VOTable: it is not XML".to_owned());
                }
                _ => {}
            }
        }
        if !rooted {
            return Err("this is not a VOTable: it holds no element at all".to_owned());
        }
        if let Some(said) = self.error.take() {
            return Err(match said.trim() {
                "" => "this VOTable says the query that produced it failed".to_owned(),
                said => format!("this VOTable says the query that produced it failed: {said}"),
            });
        }
        match (found, self.first.take()) {
            (Some(table), _) => Ok(table),
            (None, Some(fields)) => finish(&builders(fields)?, Vec::new()),
            (None, None) => Err("this VOTable holds no TABLE".to_owned()),
        }
    }

    fn parent(&self) -> &str {
        self.stack.last().map(String::as_str).unwrap_or_default()
    }

    /// An element opened. `Some` is the table, read whole. `read` is whether the table has
    /// been already, after which only a failed `QUERY_STATUS` is looked for.
    fn opened(
        &mut self,
        name: &str,
        element: &BytesStart<'_>,
        empty: bool,
        read: bool,
    ) -> Result<Option<Table>, String> {
        if read && name != "INFO" {
            if !empty {
                self.stack.push(name.to_owned());
            }
            return Ok(None);
        }
        let attributes = attributes(element)?;
        let get = |key: &str| {
            attributes
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value.clone())
        };
        let parent = self.parent().to_owned();
        match name {
            "TABLE" => {
                self.open = Some(Open {
                    id: get("ID"),
                    reference: get("ref"),
                    nrows: get("nrows").and_then(|rows| rows.trim().parse().ok()),
                    ..Open::default()
                });
                if empty {
                    self.table_closed()?;
                    return Ok(None);
                }
            }
            "FIELD" if parent == "TABLE" => {
                let declared = Declared::from_attributes(&attributes);
                if let Some(open) = &mut self.open {
                    match empty {
                        true => open.fields.push(declared),
                        false => open.field = Some(declared),
                    }
                }
                if empty {
                    return Ok(None);
                }
            }
            "DESCRIPTION" if parent == "FIELD" && !empty => {
                let text = self.text_until_end("DESCRIPTION")?;
                if let Some(field) = self.open.as_mut().and_then(|open| open.field.as_mut()) {
                    field.description = Some(text.trim().to_owned());
                }
                return Ok(None);
            }
            "VALUES" => {
                let null = match get("ref") {
                    Some(reference) => self.values.get(&reference).cloned().ok_or_else(|| {
                        format!("VALUES ref={reference:?} names no VALUES before it")
                    })?,
                    None => get("null"),
                };
                if let Some(id) = get("ID") {
                    self.values.insert(id, null.clone());
                }
                if parent == "FIELD"
                    && let Some(field) = self.open.as_mut().and_then(|open| open.field.as_mut())
                {
                    field.null = null;
                }
            }
            "INFO" => {
                if self.failed(&attributes, empty)? {
                    return Ok(None);
                }
            }
            "DATA" if parent == "TABLE" => {
                let Some(open) = self.open.take() else {
                    return Ok(None);
                };
                let nrows = open.nrows;
                let fields = self.fields_of(open)?;
                let mut builders = builders(fields)?;
                let batches = match empty {
                    true => Vec::new(),
                    false => self.data(&mut builders, nrows)?,
                };
                return finish(&builders, batches).map(Some);
            }
            _ => {}
        }
        if !empty {
            self.stack.push(name.to_owned());
        }
        Ok(None)
    }

    /// Whether an `INFO` says the query that produced the document failed. Where it does,
    /// what it says is kept, read through its end.
    fn failed(&mut self, attributes: &[(String, String)], empty: bool) -> Result<bool, String> {
        let get = |key: &str| {
            attributes
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, value)| value.as_str())
        };
        let failed = get("name") == Some("QUERY_STATUS")
            && get("value").is_some_and(|value| value.eq_ignore_ascii_case("ERROR"));
        if failed {
            let said = match empty {
                true => String::new(),
                false => self.text_until_end("INFO")?,
            };
            self.error = Some(said);
        }
        Ok(failed)
    }

    fn closed(&mut self, name: &str) -> Result<(), String> {
        match name {
            "FIELD" => {
                if let Some(open) = &mut self.open
                    && let Some(field) = open.field.take()
                {
                    open.fields.push(field);
                }
            }
            "TABLE" => self.table_closed()?,
            _ => {}
        }
        Ok(())
    }

    /// A `TABLE` with no `DATA` ended: remembered for a later `ref`, and as the answer if no
    /// table in the document has data.
    fn table_closed(&mut self) -> Result<(), String> {
        let Some(open) = self.open.take() else {
            return Ok(());
        };
        let id = open.id.clone();
        let fields = self.fields_of(open)?;
        if let Some(id) = id {
            self.tables.insert(id, fields.clone());
        }
        if self.first.is_none() {
            self.first = Some(fields);
        }
        Ok(())
    }

    /// A table's `FIELD`s, or where it has none of its own, those of the table its `ref`
    /// names (§3.8).
    fn fields_of(&self, open: Open) -> Result<Vec<Declared>, String> {
        match (open.fields.is_empty(), open.reference) {
            (true, Some(reference)) => self
                .tables
                .get(&reference)
                .cloned()
                .ok_or_else(|| format!("TABLE ref={reference:?} names no TABLE before it")),
            _ => Ok(open.fields),
        }
    }

    /// The inside of a `DATA`, through its end.
    fn data(
        &mut self,
        builders: &mut Vec<Builder>,
        nrows: Option<usize>,
    ) -> Result<Vec<RecordBatch>, String> {
        let mut batches = Vec::new();
        loop {
            match self
                .reader
                .read_event()
                .map_err(|error| xml_error(&error))?
            {
                Event::Start(element) => {
                    let name = local(&element).to_owned();
                    match name.as_str() {
                        "TABLEDATA" => self.tabledata(builders, &mut batches)?,
                        "BINARY" | "BINARY2" => {
                            let stream = self.stream(&name)?;
                            let decoded = BASE64
                                .decode(stream.as_bytes())
                                .or_else(|_| general_purpose::STANDARD.decode(stream.as_bytes()))
                                .map_err(|error| {
                                    format!("the {name} STREAM is not base64: {error}")
                                })?;
                            let stream = Stream {
                                bytes: &decoded,
                                flagged: name == "BINARY2",
                                nrows,
                            };
                            self.binary(builders, &stream, &mut batches)?;
                        }
                        "FITS" => {
                            return Err("this VOTable carries its rows as FITS, which this \
                                        service does not read; send TABLEDATA, BINARY or \
                                        BINARY2"
                                .to_owned());
                        }
                        // An `INFO` may follow the serialization, and may be where a service
                        // says the query failed after the rows it had.
                        "INFO" => {
                            if !self.failed(&attributes(&element)?, false)? {
                                self.skip("INFO")?;
                            }
                        }
                        other => self.skip(other)?,
                    }
                }
                Event::Empty(element) => match local(&element) {
                    "FITS" => {
                        return Err("this VOTable carries its rows as FITS, which this service \
                                    does not read; send TABLEDATA, BINARY or BINARY2"
                            .to_owned());
                    }
                    "INFO" => {
                        self.failed(&attributes(&element)?, true)?;
                    }
                    _ => {}
                },
                Event::End(_) => return Ok(batches),
                Event::Eof => return Err("the document ends inside its DATA".to_owned()),
                _ => {}
            }
        }
    }

    /// Everything up to the end of an element this reader has nothing to do with.
    fn skip(&mut self, name: &str) -> Result<(), String> {
        let mut depth = 1usize;
        loop {
            match self
                .reader
                .read_event()
                .map_err(|error| xml_error(&error))?
            {
                Event::Start(_) => depth += 1,
                Event::End(_) => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(());
                    }
                }
                Event::Eof => return Err(format!("the document ends inside a {name}")),
                _ => {}
            }
        }
    }

    fn tabledata(
        &mut self,
        builders: &mut [Builder],
        batches: &mut Vec<RecordBatch>,
    ) -> Result<(), String> {
        let mut rows = 0usize;
        let mut pending = 0usize;
        loop {
            match self
                .reader
                .read_event()
                .map_err(|error| xml_error(&error))?
            {
                Event::Start(element) if local(&element) == "TR" => {
                    rows += 1;
                    self.row(builders, rows)?;
                    pending += 1;
                }
                // A table always has a column, `builders` refusing one with none.
                Event::Empty(element) if local(&element) == "TR" => {
                    return Err(format!(
                        "row {} has no cells, and the table has {} columns",
                        rows + 1,
                        builders.len()
                    ));
                }
                Event::Start(element) | Event::Empty(element) => {
                    return Err(format!(
                        "TABLEDATA holds a <{}>, where only TR belongs",
                        element.name().as_ref()
                    ));
                }
                Event::End(_) => {
                    if pending > 0 {
                        batches.push(batch(builders)?);
                    }
                    return Ok(());
                }
                Event::Eof => return Err("the document ends inside its TABLEDATA".to_owned()),
                _ => {}
            }
            if pending == BATCH_ROWS {
                batches.push(batch(builders)?);
                pending = 0;
            }
        }
    }

    /// One `TR`, through its end.
    fn row(&mut self, builders: &mut [Builder], row: usize) -> Result<(), String> {
        let mut at = 0usize;
        loop {
            let cell = match self
                .reader
                .read_event()
                .map_err(|error| xml_error(&error))?
            {
                Event::Start(element) if local(&element) == "TD" => {
                    refuse_encoded(&element)?;
                    Some(self.text_until_end("TD")?)
                }
                Event::Empty(element) if local(&element) == "TD" => {
                    refuse_encoded(&element)?;
                    None
                }
                Event::Start(element) | Event::Empty(element) => {
                    return Err(format!(
                        "row {row} holds a <{}>, where only TD belongs",
                        element.name().as_ref()
                    ));
                }
                Event::End(_) => {
                    return match at == builders.len() {
                        true => Ok(()),
                        false => Err(format!(
                            "row {row} has {at} cells, and the table has {} columns",
                            builders.len()
                        )),
                    };
                }
                Event::Eof => return Err(format!("the document ends inside row {row}")),
                _ => continue,
            };
            let count = builders.len();
            let builder = builders.get_mut(at).ok_or_else(|| {
                format!("row {row} has more cells than the table's {count} columns")
            })?;
            // An empty cell is paid for before it is allocated: it is the one cell whose
            // items the document did not spend bytes on.
            if cell.as_deref().is_none_or(str::is_empty) {
                self.affordable(builder.null_cost())?;
            }
            let before = builder.appended();
            tabledata::push(builder, cell.as_deref())
                .map_err(|error| format!("row {row}, column {}: {error}", builder.column.name))?;
            let spent = builder.appended() - before;
            self.spend(spent)?;
            at += 1;
        }
    }

    /// The text of a `BINARY`'s or `BINARY2`'s `STREAM`, through the end of the
    /// serialization element.
    fn stream(&mut self, serialization: &str) -> Result<String, String> {
        let mut text = String::new();
        loop {
            let event = self
                .reader
                .read_event()
                .map_err(|error| xml_error(&error))?;
            let empty = matches!(event, Event::Empty(_));
            match event {
                Event::Start(element) | Event::Empty(element) if local(&element) == "STREAM" => {
                    let attributes = attributes(&element)?;
                    let get = |key: &str| {
                        attributes
                            .iter()
                            .find(|(name, _)| name == key)
                            .map(|(_, value)| value.as_str())
                    };
                    if get("href").is_some_and(|href| !href.trim().is_empty()) {
                        return Err(format!(
                            "this VOTable's {serialization} STREAM points elsewhere for its \
                             rows, and this service reads only what the document carries; \
                             send the rows inline"
                        ));
                    }
                    match get("encoding").map(str::trim) {
                        Some(encoding) if encoding.eq_ignore_ascii_case("base64") => {}
                        other => {
                            return Err(format!(
                                "this VOTable's {serialization} STREAM has encoding {:?}; data \
                                 carried inside the document is base64",
                                other.unwrap_or("")
                            ));
                        }
                    }
                    if !empty {
                        text = self.text_until_end("STREAM")?;
                    }
                }
                Event::Start(element) | Event::Empty(element) => {
                    return Err(format!(
                        "{serialization} holds a <{}>, where only STREAM belongs",
                        element.name().as_ref()
                    ));
                }
                Event::End(_) => {
                    text.retain(|c| !c.is_ascii_whitespace());
                    return Ok(text);
                }
                Event::Eof => return Err(format!("the document ends inside its {serialization}")),
                _ => {}
            }
        }
    }

    /// A `BINARY` or `BINARY2` stream, in whichever [`Dialect`] it parses in.
    ///
    /// The second is tried only where a column is one the two read differently, and only
    /// where the first did not parse; the document's own `nrows`, where it has one, has to
    /// agree as well. astropy's first, where the document says astropy wrote it, and
    /// otherwise STIL's.
    fn binary(
        &mut self,
        builders: &mut Vec<Builder>,
        stream: &Stream<'_>,
        batches: &mut Vec<RecordBatch>,
    ) -> Result<(), String> {
        let ambiguous = builders.iter().any(binary::is_ambiguous);
        let nrows = stream.nrows.filter(|_| ambiguous);
        let first = match self.astropy {
            true => Dialect::Astropy,
            false => Dialect::Stil,
        };
        let spent = self.items;
        let error = match self.rows_of(builders, stream, first, nrows) {
            Ok(read) => {
                batches.extend(read);
                return Ok(());
            }
            Err(error) if !ambiguous => return Err(error),
            Err(error) => error,
        };
        self.items = spent;
        let mut fresh = builders
            .iter()
            .map(|builder| Builder::new(builder.column.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        match self.rows_of(&mut fresh, stream, first.other(), nrows) {
            Ok(read) => {
                *builders = fresh;
                batches.extend(read);
                Ok(())
            }
            Err(_) => Err(error),
        }
    }

    fn rows_of(
        &mut self,
        builders: &mut [Builder],
        stream: &Stream<'_>,
        dialect: Dialect,
        nrows: Option<usize>,
    ) -> Result<Vec<RecordBatch>, String> {
        let (bytes, flagged) = (stream.bytes, stream.flagged);
        let mut batches = Vec::new();
        let flag_bytes = match flagged {
            true => builders.len().div_ceil(8),
            false => 0,
        };
        let mut cursor = Cursor::new(bytes);
        let mut rows = 0usize;
        let mut pending = 0usize;
        while !cursor.is_empty() {
            rows += 1;
            let started = cursor.position();
            let flags = cursor
                .take(flag_bytes)
                .map_err(|error| format!("row {rows}: {error}"))?;
            for (at, builder) in builders.iter_mut().enumerate() {
                let flag = flags.get(at / 8).copied().unwrap_or_default();
                let null = flagged && flag & (0x80 >> (at % 8)) != 0;
                let before = builder.appended();
                binary::push(builder, &mut cursor, null, dialect).map_err(|error| {
                    format!("row {rows}, column {}: {error}", builder.column.name)
                })?;
                let spent = builder.appended() - before;
                self.spend(spent)?;
            }
            // A row of nothing but empty fixed arrays reads no bytes, and a stream of them
            // would never end.
            if cursor.position() == started {
                return Err(format!(
                    "row {rows} takes no bytes of the stream, so the stream says nothing about \
                     how many rows it holds"
                ));
            }
            pending += 1;
            if pending == BATCH_ROWS {
                batches.push(batch(builders)?);
                pending = 0;
            }
        }
        if let Some(declared) = nrows
            && declared != rows
        {
            return Err(format!(
                "the stream holds {rows} rows, and its TABLE says nrows=\"{declared}\""
            ));
        }
        if pending > 0 {
            batches.push(batch(builders)?);
        }
        Ok(batches)
    }

    /// Whether so many more items fit.
    fn affordable(&self, items: usize) -> Result<(), String> {
        match self.items.saturating_add(items) <= self.budget {
            true => Ok(()),
            false => Err(format!(
                "this VOTable expands to more than {} values, which is more than a document \
                 of its size is allowed to hold",
                self.budget
            )),
        }
    }

    fn spend(&mut self, items: usize) -> Result<(), String> {
        self.affordable(items)?;
        self.items += items;
        Ok(())
    }

    /// The text inside an element that holds nothing else, through its end.
    fn text_until_end(&mut self, name: &str) -> Result<String, String> {
        let mut text = String::new();
        loop {
            match self
                .reader
                .read_event()
                .map_err(|error| xml_error(&error))?
            {
                Event::Text(chunk) => text.push_str(&chunk.xml10_content()),
                Event::CData(chunk) => text.push_str(&chunk.xml10_content()),
                Event::GeneralRef(reference) => match reference.resolve_char_ref() {
                    Ok(Some(character)) => text.push(character),
                    Ok(None) => text.push_str(predefined(&reference).ok_or_else(|| {
                        format!(
                            "&{}; is not an entity XML defines, and this reader expands no \
                             other",
                            &*reference
                        )
                    })?),
                    Err(error) => return Err(xml_error(&error)),
                },
                Event::End(_) => return Ok(text),
                Event::Start(element) | Event::Empty(element) => {
                    return Err(format!(
                        "a {name} holds a <{}>, where only text belongs",
                        element.name().as_ref()
                    ));
                }
                Event::Eof => return Err(format!("the document ends inside a {name}")),
                _ => {}
            }
        }
    }
}

/// The five entities XML predefines, which are the only ones this reader expands: a
/// document declaring its own in a `DOCTYPE` is asking for an expansion nothing here does.
fn predefined(name: &str) -> Option<&'static str> {
    Some(match name {
        "lt" => "<",
        "gt" => ">",
        "amp" => "&",
        "apos" => "'",
        "quot" => "\"",
        _ => return None,
    })
}

/// VOTable 1.1 let a `TD` carry `encoding`, which 1.2 removed. What it would have meant is
/// base64 of a cell, which is not text this reader can take as the value.
fn refuse_encoded(element: &BytesStart<'_>) -> Result<(), String> {
    match attributes(element)?
        .iter()
        .find(|(name, _)| name == "encoding")
    {
        Some((_, encoding)) if !encoding.trim().is_empty() => Err(format!(
            "a TD has encoding {encoding:?}, which VOTable has not allowed since 1.2"
        )),
        _ => Ok(()),
    }
}

fn builders(fields: Vec<Declared>) -> Result<Vec<Builder>, String> {
    if fields.is_empty() {
        return Err("this VOTable's TABLE declares no FIELD".to_owned());
    }
    let mut builders: Vec<Builder> = Vec::with_capacity(fields.len());
    for (position, declared) in fields.into_iter().enumerate() {
        let column = declared.column(position)?;
        // Two columns of one name are two answers to one reference, and a statement naming
        // it could mean either.
        if builders
            .iter()
            .any(|builder| builder.column.name == column.name)
        {
            return Err(format!(
                "this VOTable has two FIELDs named {:?}; give each column its own name",
                column.name
            ));
        }
        builders.push(Builder::new(column)?);
    }
    Ok(builders)
}

fn schema(builders: &[Builder]) -> SchemaRef {
    Arc::new(Schema::new(
        builders
            .iter()
            .map(|builder| builder.column.arrow_field())
            .collect::<Vec<_>>(),
    ))
}

fn batch(builders: &mut [Builder]) -> Result<RecordBatch, String> {
    let schema = schema(builders);
    let columns = builders
        .iter_mut()
        .map(Builder::finish)
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(schema, columns).map_err(|error| error.to_string())
}

fn finish(builders: &[Builder], batches: Vec<RecordBatch>) -> Result<Table, String> {
    Ok(Table {
        schema: schema(builders),
        batches,
    })
}

/// An element's attributes, by local name, their references expanded.
fn attributes(element: &BytesStart<'_>) -> Result<Vec<(String, String)>, String> {
    element
        .attributes()
        .map(|attribute| {
            let attribute =
                attribute.map_err(|error| format!("an attribute is malformed: {error}"))?;
            let value = attribute
                .normalized_value(XmlVersion::Implicit1_0)
                .map_err(|error| xml_error(&error))?;
            Ok((
                attribute.key.local_name().as_ref().to_owned(),
                value.into_owned(),
            ))
        })
        .collect()
}

fn local<'a>(element: &'a BytesStart<'_>) -> &'a str {
    let name = element.name().0;
    name.rsplit(':').next().unwrap_or(name)
}

fn xml_error(error: &quick_xml::Error) -> String {
    format!("the document is not well-formed XML: {error}")
}

/// The document as text, in the encoding it says it is in.
///
/// A byte-order mark decides first, then the XML declaration's `encoding`, then UTF-8,
/// which is XML's own default. `encoding_rs` does the decoding and refuses bytes that are not
/// what the label says, rather than replacing them with a character the document never had.
fn text_of(bytes: &[u8]) -> Result<Cow<'_, str>, String> {
    if let Some((encoding, bom)) = encoding_rs::Encoding::for_bom(bytes) {
        return encoding
            .decode_without_bom_handling_and_without_replacement(
                bytes.get(bom..).unwrap_or_default(),
            )
            .ok_or_else(|| format!("the document is not valid {}", encoding.name()));
    }
    let encoding = match declared_encoding(bytes) {
        None => encoding_rs::UTF_8,
        Some(label) => encoding_rs::Encoding::for_label(label.as_bytes())
            .ok_or_else(|| format!("the document is in {label:?}, which is not an encoding"))?,
    };
    encoding
        .decode_without_bom_handling_and_without_replacement(bytes)
        .ok_or_else(|| format!("the document is not valid {}", encoding.name()))
}

/// The `encoding` of an XML declaration at the very start of the document.
fn declared_encoding(bytes: &[u8]) -> Option<String> {
    let head = bytes.get(..bytes.len().min(256))?;
    // Cut inside a character is still fine: the declaration is ASCII and well before the cut.
    let head = match std::str::from_utf8(head) {
        Ok(head) => head,
        Err(error) => std::str::from_utf8(head.get(..error.valid_up_to())?).ok()?,
    };
    let declaration = head.strip_prefix("<?xml")?.split("?>").next()?;
    let (_, rest) = declaration.split_once("encoding")?;
    let rest = rest.trim_start().strip_prefix('=')?.trim_start();
    let quote = rest.chars().next().filter(|c| *c == '"' || *c == '\'')?;
    let (label, _) = rest.strip_prefix(quote)?.split_once(quote)?;
    Some(label.to_owned())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::concat_batches;
    use datafusion::arrow::datatypes::{DataType, Float32Type, Int16Type, Int32Type};

    use super::*;
    use crate::votable::field;

    /// §5.1's table, with a `VALUES null` on the short so BINARY can say its null too.
    const HEAD: &str = r#"<?xml version="1.0"?>
<VOTABLE version="1.5" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">
<RESOURCE><TABLE>
  <FIELD name="aString" datatype="char" arraysize="10"/>
  <FIELD name="aShort" datatype="short"><VALUES null="99"/></FIELD>
  <FIELD name="varInts" datatype="int" arraysize="*"/>
  <FIELD name="Floats" datatype="float" arraysize="3" unit="mag"/>
  <DATA>"#;
    const TAIL: &str = "</DATA></TABLE></RESOURCE></VOTABLE>";

    fn one(text: &str) -> RecordBatch {
        let table = read(text.as_bytes()).unwrap();
        concat_batches(&table.schema, &table.batches).unwrap()
    }

    /// The same rows three ways, each of which has to read back as the same arrays.
    fn check(batch: &RecordBatch) {
        assert_eq!(batch.num_rows(), 2);
        let strings = batch.column(0).as_string::<i32>();
        assert_eq!((strings.value(0), strings.value(1)), ("Apple", "Orange"));
        let shorts = batch.column(1).as_primitive::<Int16Type>();
        assert!(shorts.is_null(0));
        assert_eq!(shorts.value(1), 15);
        let ints = batch.column(2).as_list::<i32>();
        let first = ints.value(0);
        assert_eq!(
            first.as_primitive::<Int32Type>().values().to_vec(),
            [1, 2, 4, 8, 16]
        );
        assert_eq!(ints.value(1).len(), 3);
        let floats = batch.column(3).as_fixed_size_list();
        assert_eq!(
            floats
                .value(1)
                .as_primitive::<Float32Type>()
                .values()
                .to_vec(),
            [2.33f32, 4.66, 9.53]
        );
        let described = batch.schema();
        let described = described.field(3);
        assert_eq!(
            described.metadata().get(field::UNIT).map(String::as_str),
            Some("mag")
        );
        assert!(matches!(
            described.data_type(),
            DataType::FixedSizeList(_, 3)
        ));
    }

    #[test]
    fn the_examples_table_reads_the_same_from_every_serialization() {
        let tabledata = format!(
            "{HEAD}<TABLEDATA>
    <TR> <TD>Apple</TD>  <TD/>       <TD>1 2 4 8 16</TD> <TD>1.62 4.56 3.44</TD> </TR>
    <TR> <TD>Orange</TD> <TD>15</TD> <TD>23 -11 9</TD>   <TD>2.33 4.66 9.53</TD> </TR>
  </TABLEDATA>{TAIL}"
        );
        check(&one(&tabledata));

        // The rows as §5.3's figure lays them out: fixed chars, the short's magic value,
        // a count and its ints, three floats.
        let mut rows = Vec::new();
        let row = |out: &mut Vec<u8>, name: &str, short: i16, ints: &[i32], floats: [f32; 3]| {
            let mut padded = name.as_bytes().to_vec();
            padded.resize(10, b' ');
            out.extend(padded);
            out.extend(short.to_be_bytes());
            out.extend(u32::try_from(ints.len()).unwrap().to_be_bytes());
            for int in ints {
                out.extend(int.to_be_bytes());
            }
            for float in floats {
                out.extend(float.to_be_bytes());
            }
        };
        row(
            &mut rows,
            "Apple",
            99,
            &[1, 2, 4, 8, 16],
            [1.62, 4.56, 3.44],
        );
        row(&mut rows, "Orange", 15, &[23, -11, 9], [2.33, 4.66, 9.53]);
        let binary = format!(
            "{HEAD}<BINARY><STREAM encoding='base64'>{}</STREAM></BINARY>{TAIL}",
            general_purpose::STANDARD.encode(&rows)
        );
        check(&one(&binary));

        // BINARY2 flags the null instead: 0b0100_0000 is the second of four columns.
        let mut flagged = Vec::new();
        let mut first = Vec::new();
        row(
            &mut first,
            "Apple",
            0,
            &[1, 2, 4, 8, 16],
            [1.62, 4.56, 3.44],
        );
        flagged.push(0b0100_0000);
        flagged.extend(first);
        let mut second = Vec::new();
        row(&mut second, "Orange", 15, &[23, -11, 9], [2.33, 4.66, 9.53]);
        flagged.push(0);
        flagged.extend(second);
        let encoded = general_purpose::STANDARD.encode(&flagged);
        // Wrapped the way writers wrap it, which is whitespace the decoder must not see.
        let wrapped: Vec<String> = encoded
            .as_bytes()
            .chunks(20)
            .map(|line| String::from_utf8_lossy(line).into_owned())
            .collect();
        let binary2 = format!(
            "{}<BINARY2><STREAM encoding=\"base64\">\n{}\n</STREAM></BINARY2>{TAIL}",
            HEAD.replace("<VALUES null=\"99\"/>", ""),
            wrapped.join("\n")
        );
        check(&one(&binary2));
    }

    /// A variable multi-dimensional array written with its count as primitives, the way STIL
    /// writes it, and as slices, the way astropy does, reads back as the same cells.
    #[test]
    fn both_readings_of_a_variable_array_count_are_read() {
        let head = r#"<VOTABLE><RESOURCE><TABLE nrows="2">
<FIELD name="pairs" datatype="short" arraysize="2x*"/>
<FIELD name="names" datatype="char" arraysize="3x*"/>
<DATA><BINARY><STREAM encoding="base64">"#;
        let tail = "</STREAM></BINARY></DATA></TABLE></RESOURCE></VOTABLE>";
        let rows = |slices: bool| {
            let mut out = Vec::new();
            for (pairs, names) in [(&[1i16, 2, 3, 4][..], "abcdef"), (&[5, 6][..], "ghi")] {
                let (short_count, char_count) = match slices {
                    true => (pairs.len() / 2, names.len() / 3),
                    false => (pairs.len(), names.len()),
                };
                out.extend(u32::try_from(short_count).unwrap().to_be_bytes());
                for value in pairs {
                    out.extend(value.to_be_bytes());
                }
                out.extend(u32::try_from(char_count).unwrap().to_be_bytes());
                out.extend(names.as_bytes());
            }
            general_purpose::STANDARD.encode(out)
        };
        for (slices, astropy) in [(false, false), (true, false), (true, true), (false, true)] {
            let comment = match astropy {
                true => "<!-- Produced with astropy.io.votable -->",
                false => "",
            };
            let document = format!("{comment}{head}{}{tail}", rows(slices));
            let batch = one(&document);
            let pairs = batch.column(0).as_list::<i32>();
            assert_eq!(
                pairs.value(0).as_primitive::<Int16Type>().values().to_vec(),
                [1, 2, 3, 4],
                "slices {slices}, astropy {astropy}"
            );
            let names = batch.column(1).as_list::<i32>();
            let first = names.value(0);
            let first = first.as_string::<i32>();
            assert_eq!((first.value(0), first.value(1)), ("abc", "def"));
            assert_eq!(names.value(1).len(), 1);
        }
    }

    /// A count as large as four bytes can say is refused at the end of the stream, not
    /// allocated: the bytes it asks for are taken from the stream before anything is built.
    #[test]
    fn a_count_larger_than_the_stream_is_refused_before_it_is_allocated() {
        for (datatype, arraysize) in [
            ("double", "*"),
            ("char", "*"),
            ("bit", "*"),
            ("short", "2x*"),
        ] {
            let mut stream = u32::MAX.to_be_bytes().to_vec();
            stream.extend([0u8; 16]);
            let document = format!(
                "<VOTABLE><RESOURCE><TABLE><FIELD name=\"a\" datatype=\"{datatype}\" \
                 arraysize=\"{arraysize}\"/><DATA><BINARY><STREAM encoding=\"base64\">{}\
                 </STREAM></BINARY></DATA></TABLE></RESOURCE></VOTABLE>",
                general_purpose::STANDARD.encode(stream)
            );
            // Whichever check says so first — the stream running out, or the count not being
            // a whole number of slices — it says so about this cell.
            let refused = read(document.as_bytes()).unwrap_err();
            assert!(
                refused.contains("row 1, column a"),
                "{datatype} {arraysize}: {refused}"
            );
        }
    }

    /// The walk is a loop over a stack of names, so a document nested as deep as its size
    /// allows is read without recursing into it.
    #[test]
    fn elements_nested_a_hundred_thousand_deep_are_walked_not_recursed() {
        let depth = 100_000;
        let document = format!(
            "<VOTABLE><RESOURCE>{}{}<TABLE><FIELD name=\"a\" datatype=\"int\"/><DATA>\
             <TABLEDATA><TR><TD>1</TD></TR></TABLEDATA></DATA></TABLE></RESOURCE></VOTABLE>",
            "<GROUP>".repeat(depth),
            "</GROUP>".repeat(depth),
        );
        assert_eq!(one(&document).num_rows(), 1);
    }

    /// Bytes that are not what the document says it is written in are refused rather than
    /// read with replacement characters standing in for them.
    #[test]
    fn a_document_that_is_not_the_encoding_it_declares_is_refused() {
        let mut document = b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><VOTABLE><RESOURCE>\
            <TABLE><FIELD name=\"s\" datatype=\"char\" arraysize=\"*\"/><DATA><TABLEDATA><TR><TD>"
            .to_vec();
        document.extend([0xC3, 0x28]);
        document.extend(b"</TD></TR></TABLEDATA></DATA></TABLE></RESOURCE></VOTABLE>");
        let refused = read(&document).unwrap_err();
        assert!(refused.contains("UTF-8"), "{refused}");
    }

    #[test]
    fn a_stream_elsewhere_is_refused_rather_than_fetched() {
        let refused = read(
            format!("{HEAD}<BINARY2><STREAM href=\"file:///etc/passwd\"/></BINARY2>{TAIL}")
                .as_bytes(),
        )
        .unwrap_err();
        assert!(refused.contains("elsewhere"), "{refused}");
    }

    #[test]
    fn an_entity_the_document_declares_is_not_expanded() {
        let refused = read(
            br#"<?xml version="1.0"?>
<!DOCTYPE VOTABLE [<!ENTITY lol "lol"><!ENTITY lol2 "&lol;&lol;">]>
<VOTABLE><RESOURCE><TABLE><FIELD name="a" datatype="char" arraysize="*"/>
<DATA><TABLEDATA><TR><TD>&lol2;</TD></TR></TABLEDATA></DATA></TABLE></RESOURCE></VOTABLE>"#,
        )
        .unwrap_err();
        assert!(refused.contains("lol2"), "{refused}");
    }

    #[test]
    fn an_error_document_says_what_the_service_said() {
        let refused = read(
            br#"<VOTABLE><RESOURCE type="results">
<INFO name="QUERY_STATUS" value="ERROR">no such table</INFO></RESOURCE></VOTABLE>"#,
        )
        .unwrap_err();
        assert!(refused.contains("no such table"), "{refused}");
    }

    #[test]
    fn an_empty_cell_of_a_huge_fixed_array_is_refused_before_it_is_allocated() {
        let mut document = String::from(
            "<VOTABLE><RESOURCE><TABLE><FIELD name=\"a\" datatype=\"double\" \
             arraysize=\"100000000\"/><DATA><TABLEDATA>",
        );
        for _ in 0..4 {
            document.push_str("<TR><TD/></TR>");
        }
        document.push_str("</TABLEDATA></DATA></TABLE></RESOURCE></VOTABLE>");
        // Each row asks for a hundred million values; the check between cells stops the
        // second before its allocation, the first being within the floor.
        let refused = read(document.as_bytes());
        assert!(refused.is_err(), "{:?}", refused.map(|table| table.schema));
    }

    /// A document around a single column of `field`, with `data` inside its `DATA`.
    fn single(field: &str, data: &str) -> String {
        format!("<VOTABLE><RESOURCE><TABLE>{field}<DATA>{data}</DATA></TABLE></RESOURCE></VOTABLE>")
    }

    fn refused(document: &str) -> String {
        match read(document.as_bytes()) {
            Ok(table) => panic!("{document} was read: {table:?}"),
            Err(said) => said,
        }
    }

    /// More rows than one batch holds come back as several batches, in order, from every
    /// serialization.
    #[test]
    fn a_table_longer_than_a_batch_is_read_in_batches() {
        let count = BATCH_ROWS + 3;
        let field = "<FIELD name=\"n\" datatype=\"int\"/>";
        let rows: String = (0..count)
            .map(|n| format!("<TR><TD>{n}</TD></TR>"))
            .collect();
        let stream = (0..count)
            .flat_map(|n| i32::try_from(n).unwrap().to_be_bytes())
            .collect::<Vec<_>>();
        let binary = general_purpose::STANDARD.encode(&stream);
        let flagged = (0..count)
            .flat_map(|n| {
                let mut row = vec![0u8];
                row.extend(i32::try_from(n).unwrap().to_be_bytes());
                row
            })
            .collect::<Vec<_>>();
        let binary2 = general_purpose::STANDARD.encode(&flagged);
        for data in [
            format!("<TABLEDATA>{rows}</TABLEDATA>"),
            format!("<BINARY><STREAM encoding=\"base64\">{binary}</STREAM></BINARY>"),
            format!("<BINARY2><STREAM encoding=\"base64\">{binary2}</STREAM></BINARY2>"),
        ] {
            let table = read(single(field, &data).as_bytes()).unwrap();
            let sizes: Vec<usize> = table.batches.iter().map(RecordBatch::num_rows).collect();
            assert_eq!(sizes, [BATCH_ROWS, 3]);
            let batch = concat_batches(&table.schema, &table.batches).unwrap();
            let values = batch.column(0).as_primitive::<Int32Type>();
            assert!(
                values
                    .values()
                    .iter()
                    .zip(0..)
                    .all(|(value, n)| *value == n)
            );
        }
    }

    /// A `DATA` with nothing in it, and a `STREAM` with nothing in it, are a table of no
    /// rows rather than a refusal.
    #[test]
    fn an_empty_data_or_stream_is_a_table_of_no_rows() {
        let field = "<FIELD name=\"n\" datatype=\"int\"/>";
        for document in [
            format!("<VOTABLE><RESOURCE><TABLE>{field}<DATA/></TABLE></RESOURCE></VOTABLE>"),
            single(field, "<BINARY><STREAM encoding=\"base64\"/></BINARY>"),
            single(
                field,
                "<BINARY2><STREAM encoding=\"base64\"></STREAM></BINARY2>",
            ),
            single(field, "<TABLEDATA/>"),
        ] {
            let table = read(document.as_bytes()).unwrap();
            assert_eq!(table.schema.fields().len(), 1, "{document}");
            assert_eq!(
                table
                    .batches
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
                0,
                "{document}"
            );
        }
    }

    /// A `TABLE` with no `DATA` is still the table where no other has one, and a `TABLE`
    /// with neither fields nor data has no columns to answer with.
    #[test]
    fn a_table_with_no_data_answers_with_its_columns_or_is_refused() {
        let table = read(
            b"<VOTABLE><RESOURCE><TABLE ID=\"t\"><FIELD name=\"a\" datatype=\"int\"/></TABLE>\
              <TABLE ref=\"t\"/></RESOURCE></VOTABLE>",
        )
        .unwrap();
        assert_eq!(table.schema.field(0).name(), "a");
        assert!(table.batches.is_empty());

        let said = refused("<VOTABLE><RESOURCE><TABLE/></RESOURCE></VOTABLE>");
        assert!(said.contains("declares no FIELD"), "{said}");
        let said = refused("<VOTABLE><RESOURCE><TABLE ref=\"t\"/></RESOURCE></VOTABLE>");
        assert!(said.contains("names no TABLE"), "{said}");
        let said = refused("<VOTABLE><RESOURCE/></VOTABLE>");
        assert!(said.contains("holds no TABLE"), "{said}");
    }

    /// Each way a document can be other than what its `FIELD`s declare is refused, and the
    /// refusal says which.
    #[test]
    fn a_document_that_contradicts_itself_is_refused_saying_how() {
        let int = "<FIELD name=\"n\" datatype=\"int\"/>";
        let pairs = "<FIELD name=\"p\" datatype=\"short\" arraysize=\"2x*\"/>";
        let stream = |bytes: &[u8]| {
            format!(
                "<BINARY><STREAM encoding=\"base64\">{}</STREAM></BINARY>",
                general_purpose::STANDARD.encode(bytes)
            )
        };
        let mut two_pairs = Vec::new();
        for _ in 0..2 {
            two_pairs.extend(2u32.to_be_bytes());
            two_pairs.extend([0, 1, 0, 2]);
        }
        let cases = [
            (single(int, "<TABLEDATA><TR/></TABLEDATA>"), "has no cells"),
            (
                single(int, "<TABLEDATA><TR><TD>1</TD><TD>2</TD></TR></TABLEDATA>"),
                "more cells",
            ),
            (
                single(int, "<TABLEDATA><TR></TR></TABLEDATA>"),
                "has 0 cells",
            ),
            (
                single(
                    int,
                    "<TABLEDATA><TR><TD encoding=\"base64\">AAAAAQ==</TD></TR></TABLEDATA>",
                ),
                "encoding",
            ),
            (
                single(int, "<TABLEDATA><TR><TD><B>1</B></TD></TR></TABLEDATA>"),
                "only text",
            ),
            (single(int, "<TABLEDATA><TD>1</TD></TABLEDATA>"), "only TR"),
            (
                single(int, "<TABLEDATA><TR><B/></TR></TABLEDATA>"),
                "only TD",
            ),
            (
                single(int, "<TABLEDATA><TR><TD>x</TD></TR></TABLEDATA>"),
                "row 1, column n",
            ),
            (single(int, "<FITS/>"), "FITS"),
            (
                single(int, "<FITS><STREAM href=\"x.fits\"/></FITS>"),
                "FITS",
            ),
            (single(int, "<BINARY><TD/></BINARY>"), "only STREAM"),
            (
                single(int, "<BINARY><STREAM>AAAAAQ==</STREAM></BINARY>"),
                "base64",
            ),
            (
                single(
                    int,
                    "<BINARY2><STREAM encoding=\"gzip\">AAAAAQ==</STREAM></BINARY2>",
                ),
                "gzip",
            ),
            (
                single(
                    int,
                    "<BINARY><STREAM encoding=\"base64\">%%%%</STREAM></BINARY>",
                ),
                "not base64",
            ),
            (single(int, &stream(&[0, 0, 1])), "row 1, column n"),
            (
                single(
                    "<FIELD name=\"none\" datatype=\"int\" arraysize=\"0\"/>",
                    &stream(&[0]),
                ),
                "takes no bytes",
            ),
            (
                format!(
                    "<VOTABLE><RESOURCE><TABLE nrows=\"3\">{pairs}<DATA>{}</DATA></TABLE>\
                     </RESOURCE></VOTABLE>",
                    stream(&two_pairs)
                ),
                "nrows=\"3\"",
            ),
            (
                single(
                    &format!("{int}<FIELD name=\"n\" datatype=\"long\"/>"),
                    "<TABLEDATA/>",
                ),
                "two FIELDs",
            ),
            (
                single(
                    "<FIELD name=\"n\" datatype=\"int\"><VALUES ref=\"v\"/></FIELD>",
                    "<TABLEDATA/>",
                ),
                "names no VALUES",
            ),
            (
                single(int, "<TABLEDATA><TR><TD>&#xFFFFFF;</TD></TR></TABLEDATA>"),
                "well-formed",
            ),
            (
                "<VOTABLE><RESOURCE><INFO name=\"QUERY_STATUS\" value=\"error\"/></RESOURCE>\
                 </VOTABLE>"
                    .to_owned(),
                "failed",
            ),
            (
                single(
                    int,
                    "<TABLEDATA><TR><TD>1</TD></TR></TABLEDATA>\
                     <INFO name=\"QUERY_STATUS\" value=\"ERROR\">cut short</INFO>",
                ),
                "cut short",
            ),
            (
                single(
                    int,
                    "<TABLEDATA><TR><TD>1</TD></TR></TABLEDATA>\
                     <INFO name=\"QUERY_STATUS\" value=\"ERROR\"/>",
                ),
                "failed",
            ),
            ("<table/>".to_owned(), "root element is <table>"),
            ("not xml at all".to_owned(), "not XML"),
            ("<!-- nothing -->".to_owned(), "no element"),
        ];
        for (document, says) in cases {
            let said = refused(&document);
            assert!(said.contains(says), "{document}: {said}");
        }
    }

    /// A document cut short anywhere inside its table is refused rather than read as the
    /// rows before the cut.
    #[test]
    fn a_document_that_ends_inside_its_table_is_refused() {
        let field = "<FIELD name=\"n\" datatype=\"int\"/>";
        for cut in [
            format!("<VOTABLE><RESOURCE><TABLE>{field}<DATA>"),
            format!("<VOTABLE><RESOURCE><TABLE>{field}<DATA><INFO name=\"x\">"),
            format!("<VOTABLE><RESOURCE><TABLE>{field}<DATA><TABLEDATA>"),
            format!("<VOTABLE><RESOURCE><TABLE>{field}<DATA><TABLEDATA><TR>"),
            format!("<VOTABLE><RESOURCE><TABLE>{field}<DATA><TABLEDATA><TR><TD>1"),
            format!("<VOTABLE><RESOURCE><TABLE>{field}<DATA><BINARY>"),
        ] {
            let said = refused(&cut);
            assert!(said.contains("ends inside"), "{cut}: {said}");
        }
    }

    /// What XML says is not content — a comment, a processing instruction — is not part of
    /// a cell, and a `CDATA` section and a character reference are.
    #[test]
    fn a_cell_is_its_text_and_nothing_else_xml_carries() {
        let batch = one(&single(
            "<FIELD name=\"s\" datatype=\"char\" arraysize=\"*\"/>",
            "<TABLEDATA><TR><TD>a<!-- not this --><?pi not this?>b<![CDATA[<c>]]>&#x64;&amp;\
             </TD></TR></TABLEDATA>",
        ));
        assert_eq!(batch.column(0).as_string::<i32>().value(0), "ab<c>d&");
    }

    #[test]
    fn a_declared_encoding_is_read_off_the_declaration() {
        assert_eq!(
            declared_encoding(b"<?xml version='1.0' encoding='ISO-8859-1'?><VOTABLE/>"),
            Some("ISO-8859-1".to_owned())
        );
        assert_eq!(
            declared_encoding(b"<?xml version=\"1.0\"?><VOTABLE/>"),
            None
        );
        assert_eq!(declared_encoding(b"<VOTABLE/>"), None);
    }

    #[test]
    fn only_a_votable_root_is_a_votable() {
        assert!(is_votable(
            b"<?xml version=\"1.0\"?>\n<!-- x --><VOTABLE version=\"1.4\">"
        ));
        assert!(is_votable(b"\xEF\xBB\xBF<vot:VOTABLE xmlns:vot=\"x\">"));
        assert!(!is_votable(b"<!DOCTYPE html><html><body>"));
        assert!(!is_votable(b"PAR1\x15\x04"));
        assert!(!is_votable(b"a,b\n1,2\n"));
        assert!(!is_votable(b""));
        assert!(!is_votable(
            b"<?xml version=\"1.0\"?>\n<!-- only a comment -->"
        ));
        assert!(!is_votable(b"<<<"));
    }
}
