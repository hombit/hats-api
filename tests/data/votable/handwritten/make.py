# /// script
# requires-python = ">=3.11"
# ///
"""Writes the hand-written VOTable edge cases in this directory, and each one's ground truth.

    uv run make.py

Each case is one document exercising one thing the VOTable 1.5 text allows or forbids, with
the rows a reader must return, or the reason it must refuse. They are written out by this
script rather than by an editor so that the bytes that matter — a byte-order mark, a Latin-1
character, a base64 stream — are exactly what the case says.
"""

import base64
import json
import struct
from pathlib import Path

HERE = Path(__file__).parent
NS = 'xmlns="http://www.ivoa.net/xml/VOTable/v1.3"'


def column(name, datatype, arraysize=None, **more):
    """One column of a ground truth, every key present."""
    out = {
        "name": name,
        "datatype": datatype,
        "arraysize": arraysize,
        "xtype": None,
        "unit": None,
        "ucd": None,
        "utype": None,
        "description": None,
        "null": None,
    }
    out.update(more)
    return out


def table(fields, data, *, head=f'<VOTABLE version="1.5" {NS}>', tail="</VOTABLE>"):
    """A document of one TABLE in one RESOURCE."""
    return (
        f'<?xml version="1.0" encoding="UTF-8"?>\n{head}<RESOURCE><TABLE>{fields}'
        f"<DATA>{data}</DATA></TABLE></RESOURCE>{tail}"
    )


def binary(payload: bytes, element="BINARY"):
    encoded = base64.b64encode(payload).decode()
    return f'<{element}><STREAM encoding="base64">{encoded}</STREAM></{element}>'


# name: (document as text or bytes, columns, rows, refuse, notes)
CASES = {}


def accept(name, document, columns, rows, notes, serialization="TABLEDATA"):
    CASES[name] = (document, columns, rows, None, notes, serialization)


def refuse(name, document, why):
    CASES[name] = (document, [], [], why, None, None)


# --- documents a reader must read ---------------------------------------------------------

accept(
    "prefixed-namespace",
    '<?xml version="1.0"?>\n<vot:VOTABLE version="1.4" '
    'xmlns:vot="http://www.ivoa.net/xml/VOTable/v1.3"><vot:RESOURCE><vot:TABLE>'
    '<vot:FIELD name="a" datatype="int"/><vot:DATA><vot:TABLEDATA>'
    "<vot:TR><vot:TD>1</vot:TD></vot:TR><vot:TR><vot:TD>2</vot:TD></vot:TR>"
    "</vot:TABLEDATA></vot:DATA></vot:TABLE></vot:RESOURCE></vot:VOTABLE>",
    [column("a", "int")],
    [[1], [2]],
    "Every element under a namespace prefix: elements are matched by local name.",
)
accept(
    "no-namespace-v1.1",
    '<VOTABLE version="1.1"><RESOURCE><TABLE><FIELD name="a" datatype="double"/>'
    "<DATA><TABLEDATA><TR><TD>1.5</TD></TR></TABLEDATA></DATA></TABLE></RESOURCE></VOTABLE>",
    [column("a", "double")],
    [[1.5]],
    "VOTable 1.1 with no namespace and no XML declaration.",
)
accept(
    "bom-utf8",
    b"\xef\xbb\xbf"
    + table('<FIELD name="s" datatype="unicodeChar" arraysize="*"/>',
            "<TABLEDATA><TR><TD>Ярослав</TD></TR></TABLEDATA>").encode(),
    [column("s", "unicodeChar", "*")],
    [["Ярослав"]],
    "A UTF-8 byte-order mark ahead of the XML declaration.",
)
accept(
    "latin1-declared",
    '<?xml version="1.0" encoding="ISO-8859-1"?>\n'.encode()
    + f'<VOTABLE version="1.3" {NS}><RESOURCE><TABLE>'
      '<FIELD name="s" datatype="char" arraysize="*"/><DATA><TABLEDATA>'
      "<TR><TD>caf\xe9</TD></TR></TABLEDATA></DATA></TABLE></RESOURCE></VOTABLE>".encode("latin-1"),
    [column("s", "char", "*")],
    [["café"]],
    "Declared ISO-8859-1, with é as the single byte 0xE9.",
)
accept(
    "cdata-entities-comments",
    table(
        '<FIELD name="s" datatype="char" arraysize="*"/><FIELD name="n" datatype="int"/>',
        "<TABLEDATA><!-- a comment --><TR><TD><![CDATA[<b>&]]></TD><TD>1</TD></TR>"
        "<?pi ignored?><TR><TD>a &amp; b &lt;c&gt; &#x042F;&#65;</TD><TD>2</TD></TR>"
        "</TABLEDATA>",
    ),
    [column("s", "char", "*"), column("n", "int")],
    [["<b>&", 1], ["a & b <c> ЯA", 2]],
    "CDATA, the predefined entities and character references in a TD, and a comment and a "
    "processing instruction between rows.",
)
accept(
    "numbers",
    table(
        '<FIELD name="h" datatype="short"/><FIELD name="b" datatype="unsignedByte"/>'
        '<FIELD name="d" datatype="double"/><FIELD name="v" datatype="int" arraysize="*"/>',
        "<TABLEDATA>"
        "<TR><TD>0x1F</TD><TD>0xff</TD><TD> +1.5e3 </TD><TD>1\n  2\n 3</TD></TR>"
        "<TR><TD>0xFFFF</TD><TD>7</TD><TD>+Inf</TD><TD> 4 </TD></TR>"
        "<TR><TD>+12</TD><TD>0</TD><TD>-Inf</TD><TD/></TR>"
        "<TR><TD>-3</TD><TD>255</TD><TD>NaN</TD><TD>5 6</TD></TR>"
        "</TABLEDATA>",
    ),
    [column("h", "short"), column("b", "unsignedByte"), column("d", "double"),
     column("v", "int", "*")],
    [[31, 255, 1500.0, [1, 2, 3]], [-1, 7, "Infinity", [4]], [12, 0, "-Infinity", None],
     [-3, 255, "NaN", [5, 6]]],
    "Hexadecimal integers (0xFFFF is -1 as a short), signs, whitespace around a number, "
    "and an array broken across lines.",
)
accept(
    "booleans",
    table(
        '<FIELD name="f" datatype="boolean"/>',
        "<TABLEDATA>" + "".join(
            f"<TR><TD>{v}</TD></TR>" if v is not None else "<TR><TD/></TR>"
            for v in ["T", "f", "1", "0", "true", "FalsE", "?", None]
        ) + "</TABLEDATA>",
    ),
    [column("f", "boolean")],
    [[True], [False], [True], [False], [True], [False], [None], [None]],
    "Every spelling of a boolean §6 allows, and both of its nulls.",
)
accept(
    "nested-and-several-tables",
    f'<?xml version="1.0"?>\n<VOTABLE version="1.4" {NS}><RESOURCE>'
    '<INFO name="QUERY_STATUS" value="OK"/><RESOURCE><TABLE><FIELD name="first" datatype="int"/>'
    "<DATA><TABLEDATA><TR><TD>1</TD></TR></TABLEDATA></DATA></TABLE></RESOURCE>"
    '<TABLE><FIELD name="second" datatype="int"/><DATA><TABLEDATA><TR><TD>2</TD></TR>'
    "</TABLEDATA></DATA></TABLE></RESOURCE></VOTABLE>",
    [column("first", "int")],
    [[1]],
    "The table is in a nested RESOURCE, and a second TABLE follows it: the first with DATA "
    "is the one read.",
)
accept(
    "table-and-values-ref",
    f'<?xml version="1.0"?>\n<VOTABLE version="1.4" {NS}><RESOURCE>'
    '<TABLE ID="template"><FIELD name="n" datatype="short">'
    '<VALUES ID="nulls" null="-99"/></FIELD><FIELD name="m" datatype="short">'
    '<VALUES ref="nulls"/></FIELD></TABLE>'
    '<TABLE ref="template"><DATA><TABLEDATA><TR><TD>-99</TD><TD>4</TD></TR>'
    "<TR><TD>5</TD><TD>-99</TD></TR></TABLEDATA></DATA></TABLE></RESOURCE></VOTABLE>",
    [column("n", "short", null="-99"), column("m", "short", null="-99")],
    [[None, 4], [5, None]],
    "A template TABLE with no DATA, a later TABLE taking its FIELDs by ref, and a VALUES "
    "taking its null by ref.",
)
accept(
    "field-names",
    table(
        '<FIELD ID="only_id" datatype="int"/><FIELD datatype="int"/>'
        '<FIELD name="my ra" datatype="int"/><FIELD name="2mass.J" datatype="int"/>'
        '<FIELD name="Mixed" datatype="int"/>',
        "<TABLEDATA><TR><TD>1</TD><TD>2</TD><TD>3</TD><TD>4</TD><TD>5</TD></TR></TABLEDATA>",
    ),
    [column("only_id", "int"), column("col2", "int"), column("my ra", "int"),
     column("2mass.J", "int"), column("Mixed", "int")],
    [[1, 2, 3, 4, 5]],
    "A FIELD with only an ID is called by it, one with neither by its position; names ADQL "
    "cannot write unquoted are kept as written.",
)
accept(
    "no-data",
    f'<?xml version="1.0"?>\n<VOTABLE version="1.4" {NS}><RESOURCE><TABLE>'
    '<FIELD name="a" datatype="int"/></TABLE></RESOURCE></VOTABLE>',
    [column("a", "int")],
    [],
    "A TABLE with no DATA is its columns and no rows.",
    serialization=None,
)
accept(
    "empty-tabledata",
    table('<FIELD name="a" datatype="int"/>', "<TABLEDATA/>"),
    [column("a", "int")],
    [],
    "A DATA whose TABLEDATA has no rows.",
)
accept(
    "overflow-after-table",
    f'<?xml version="1.0"?>\n<VOTABLE version="1.4" {NS}><RESOURCE type="results">'
    '<INFO name="QUERY_STATUS" value="OK"/><TABLE><FIELD name="a" datatype="int"/>'
    "<DATA><TABLEDATA><TR><TD>1</TD></TR></TABLEDATA></DATA></TABLE>"
    '<INFO name="QUERY_STATUS" value="OVERFLOW"/></RESOURCE></VOTABLE>',
    [column("a", "int")],
    [[1]],
    "OVERFLOW after the table is a whole answer cut at a bound, not a failure.",
)
accept(
    "binary-variable-2d",
    table(
        '<FIELD name="pairs" datatype="short" arraysize="2x*"/>',
        binary(struct.pack(">i4h", 4, 1, 2, 3, 4) + struct.pack(">i2h", 2, 5, 6)),
    ),
    [column("pairs", "short", "2x*")],
    [[[1, 2, 3, 4]], [[5, 6]]],
    "A variable 2-D array whose count is its primitives, as STIL writes it.",
    serialization="BINARY",
)
accept(
    "binary-variable-2d-slices",
    "<!-- Produced with astropy.io.votable -->\n" + table(
        '<FIELD name="pairs" datatype="short" arraysize="2x*"/>',
        binary(struct.pack(">i4h", 2, 1, 2, 3, 4) + struct.pack(">i2h", 1, 5, 6)),
    ),
    [column("pairs", "short", "2x*")],
    [[[1, 2, 3, 4]], [[5, 6]]],
    "The same cells with the count as slices of the last dimension, as astropy writes it.",
    serialization="BINARY",
)

# --- documents a reader must refuse -------------------------------------------------------

FIELD = '<FIELD name="a" datatype="int"/>'
refuse("stream-href-file", table(FIELD, '<BINARY><STREAM href="file:///etc/passwd"/></BINARY>'),
       "a STREAM whose data is elsewhere names a place for the reader to read")
refuse("stream-href-http",
       table(FIELD, '<BINARY2><STREAM href="http://169.254.169.254/latest"/></BINARY2>'),
       "a STREAM whose data is elsewhere names a place for the reader to read")
refuse("fits", table(FIELD, '<FITS extnum="1"><STREAM encoding="base64">AAAA</STREAM></FITS>'),
       "FITS inside a VOTable is not read")
refuse(
    "doctype-entities",
    '<?xml version="1.0"?>\n<!DOCTYPE VOTABLE [<!ENTITY a "aaaaaaaaaa">'
    '<!ENTITY b "&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;"><!ENTITY c "&b;&b;&b;&b;&b;&b;&b;&b;&b;&b;">]>'
    f'<VOTABLE {NS}><RESOURCE><TABLE><FIELD name="s" datatype="char" arraysize="*"/>'
    "<DATA><TABLEDATA><TR><TD>&c;</TD></TR></TABLEDATA></DATA></TABLE></RESOURCE></VOTABLE>",
    "an entity the document declares is not expanded",
)
refuse(
    "external-entity",
    '<?xml version="1.0"?>\n<!DOCTYPE VOTABLE [<!ENTITY x SYSTEM "file:///etc/passwd">]>'
    f'<VOTABLE {NS}><RESOURCE><TABLE><FIELD name="s" datatype="char" arraysize="*"/>'
    "<DATA><TABLEDATA><TR><TD>&x;</TD></TR></TABLEDATA></DATA></TABLE></RESOURCE></VOTABLE>",
    "an external entity is a file for the reader to read",
)
refuse("bad-base64", table(FIELD, '<BINARY><STREAM encoding="base64">!!not base64!!</STREAM>'
                                  "</BINARY>"),
       "the stream is not base64")
refuse("truncated-binary", table(FIELD, binary(b"\x00\x00\x00\x01\x00\x00\x00")),
       "the stream ends inside a row")
refuse("tr-too-few", table(FIELD + FIELD.replace('"a"', '"b"'),
                           "<TABLEDATA><TR><TD>1</TD></TR></TABLEDATA>"),
       "a row has fewer cells than the table has columns")
refuse("tr-too-many", table(FIELD, "<TABLEDATA><TR><TD>1</TD><TD>2</TD></TR></TABLEDATA>"),
       "a row has more cells than the table has columns")
refuse("bad-number", table(FIELD, "<TABLEDATA><TR><TD>twelve</TD></TR></TABLEDATA>"),
       "a cell is not a number of the column's type")
refuse("unknown-datatype", table('<FIELD name="a" datatype="string"/>',
                                 "<TABLEDATA><TR><TD>x</TD></TR></TABLEDATA>"),
       "string is not a VOTable datatype")
refuse("missing-datatype", table('<FIELD name="a"/>', "<TABLEDATA><TR><TD>x</TD></TR></TABLEDATA>"),
       "a FIELD with no datatype")
refuse("no-table", f'<?xml version="1.0"?>\n<VOTABLE {NS}><RESOURCE>'
                   '<INFO name="QUERY_STATUS" value="OK"/></RESOURCE></VOTABLE>',
       "the document holds no TABLE")
refuse("not-votable", "<!DOCTYPE html>\n<html><body><table><tr><td>1</td></tr></table></body></html>",
       "the root element is not VOTABLE")
refuse("duplicate-names", table(FIELD + FIELD, "<TABLEDATA><TR><TD>1</TD><TD>2</TD></TR></TABLEDATA>"),
       "two columns of one name")
refuse(
    "error-after-table",
    f'<?xml version="1.0"?>\n<VOTABLE version="1.4" {NS}><RESOURCE type="results">'
    '<INFO name="QUERY_STATUS" value="OK"/><TABLE><FIELD name="a" datatype="int"/>'
    "<DATA><TABLEDATA><TR><TD>1</TD></TR></TABLEDATA></DATA></TABLE>"
    '<INFO name="QUERY_STATUS" value="ERROR">the read failed part-way</INFO></RESOURCE>'
    "</VOTABLE>",
    "the document says the query that produced it failed, after the rows (DALI §4.4)",
)


def main():
    for old in HERE.glob("*.vot*"):
        old.unlink()
    for name, (document, columns, rows, why, notes, serialization) in CASES.items():
        path = HERE / f"{name}.vot"
        data = document if isinstance(document, bytes) else document.encode()
        path.write_bytes(data)
        truth = {
            "producer": "hand-written (make.py)",
            "serialization": serialization,
            "columns": columns,
            "rows": rows,
            "refuse": why,
            "notes": notes,
        }
        (HERE / f"{name}.vot.json").write_text(
            json.dumps(truth, ensure_ascii=False, indent=1) + "\n", encoding="utf-8"
        )


if __name__ == "__main__":
    main()
