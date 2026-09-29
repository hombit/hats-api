# /// script
# requires-python = ">=3.11"
# dependencies = ["astropy==8.0.1", "numpy"]
# ///
"""Writes the synthetic VOTable corpus in this directory and the ground truth beside each file.

    uv run generate.py [--scratch DIR]

The tables are defined once, below, in the corpus's JSON cell encoding. Three independent
writers then serialize them: astropy (its object model, and `Table.write`, which is what pyvo
uploads), STILTS (reading a TABLEDATA document this script writes), and the CDS `votable`
crate (the Rust program in `cds-writer/`, built with cargo). Every file is written as each of
TABLEDATA, BINARY and BINARY2 where the writer can.

The ground truth of a file is what the file itself says, decoded by `decode_file` below, which
is written from the VOTable 1.5 text and nothing else. Three things are checked against it and
written into the file's `notes`: the source data it was written from (a difference there is
something the writer or the serialization lost), astropy reading the file, and STILTS reading
the file. A difference is recorded; it never changes the ground truth.

`--scratch` is where the STILTS input documents, the cargo build and the read-backs go; by
default a temporary directory.
"""

import argparse
import base64
import io
import json
import math
import re
import shutil
import struct
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from collections import OrderedDict, defaultdict
from pathlib import Path
from xml.sax.saxutils import escape, quoteattr

import astropy
import numpy as np
from astropy.io.votable import parse as astropy_parse
from astropy.io.votable import tree
from astropy.table import MaskedColumn, Table

HERE = Path(__file__).resolve().parent

NAN = "NaN"
INF = "Infinity"
NINF = "-Infinity"


def f32(x):
    """The exact value a float32 column holds for `x`."""
    return float(np.float32(x))


# ---------------------------------------------------------------------------------------------
# The tables. Cells are in the corpus encoding: None is null, floats may be "NaN", "Infinity",
# "-Infinity", a complex is [re, im], an array is a flat list in file order, and `None` inside an
# integer array is that column's VALUES null. A field is a dict of FIELD attributes, plus
# "description", "link", "null", "min", "max" and "options" for its children.
# ---------------------------------------------------------------------------------------------


def fld(name, datatype, arraysize=None, **kw):
    return {"name": name, "datatype": datatype, "arraysize": arraysize, **kw}


SCALARS = {
    "name": "scalars",
    "fields": [
        fld("b", "boolean"),
        fld("bit", "bit"),
        fld("ub", "unsignedByte"),
        fld("s", "short"),
        fld("i", "int"),
        fld("l", "long"),
        fld("c", "char"),
        fld("u", "unicodeChar"),
        fld("f", "float"),
        fld("d", "double"),
        fld("fc", "floatComplex"),
        fld("dc", "doubleComplex"),
        fld("str", "char", "*"),
        fld("ustr", "unicodeChar", "*"),
    ],
    "rows": [
        [True, True, 0, -32768, -2147483648, -9223372036854775808, "a", "Я", 1.5, 1.5,
         [1.5, -1.5], [1.5, -1.5], "hello", "Привет"],
        [False, False, 255, 32767, 2147483647, 9223372036854775807, "Z", "λ",
         f32(-3.4028234663852886e38), 1.7976931348623157e308, [f32(0.1), f32(-0.2)], [0.1, -0.2],
         "", ""],
        [None, True, 1, 0, 0, 0, "<", "中", f32(1.1754943508222875e-38), 5e-324, [NAN, NAN],
         [NAN, 0.0], "x y", "日本語"],
        [True, False, 128, -1, -1, -1, "&", "é", NAN, NAN, [0.0, 0.0], [0.0, 0.0], "<&>\"'",
         "<&>\"'"],
        [None] * 14,
        [False, True, 42, 12345, 123456789, 1234567890123456789, "0", "Ω", INF, INF, [INF, 1.0],
         [NINF, 1.0], "  lead and trail  ", "  ведущие "],
        [True, False, 7, 1, 2, 3, "~", "a", NINF, NINF, [3.0, 4.0], [3.0, 4.0], "multi word",
         "Ελληνικά"],
    ],
}

ARRAYS = {
    "name": "arrays",
    "fields": [
        fld("i3", "int", "3"),
        fld("d3", "double", "3"),
        fld("iv", "int", "*"),
        fld("fv", "float", "*"),
        fld("s10", "short", "10*"),
        fld("lv", "long", "*"),
        fld("ubv", "unsignedByte", "*"),
        fld("i2x3", "int", "2x3"),
        fld("d2xs", "double", "2x*"),
        fld("c8xs", "char", "8x*"),
        fld("c8x3", "char", "8x3"),
        fld("bool4", "boolean", "4"),
        fld("bit5", "bit", "5"),
        fld("bit12", "bit", "12"),
        fld("bitv", "bit", "*"),
        fld("fc2", "floatComplex", "2"),
        fld("dcv", "doubleComplex", "*"),
    ],
    "rows": [
        [[1, 2, 3], [1.5, -2.5, 1e300], [1, 2, 4, 8, 16], [1.5, NAN, -2.0], [1, 2],
         [-9223372036854775808, 9223372036854775807], [0, 255, 16], [0, 1, 2, 3, 4, 5],
         [1.0, 2.0, 3.0, 4.0, 5.0, 6.0], ["abc", "defgh"], ["a", "bb", "ccc"],
         [True, False, None, True], [True, False, True, True, False],
         [True, False, False, False, True, True, True, True, False, False, True, False],
         [True, False, True], [1.0, 2.0, 3.0, 4.0], [1.0, 2.0, 3.0, 4.0]],
        [[-2147483648, 0, 2147483647], [NAN, INF, NINF], [], [], [], [], [],
         [-1, -2, -3, -4, -5, -6], [], [], ["", "x", ""], [False, False, False, False],
         [False] * 5, [False] * 12, [], [0.5, -0.5, NAN, NAN], []],
        [None] * 17,
        [[7, 8, 9], [0.0, 1.0, 2.0], [-7], [INF], [-32768, 32767, 0, 1, 2, 3, 4, 5, 6, 7], [0],
         [1], [10, 20, 30, 40, 50, 60], [0.5, -0.5], ["12345678"], ["12345678", " lead", "z"],
         [None, None, True, True], [True] * 5, [True] * 12,
         [True, False, True, False, True, False, True, False, True], [0.0, 0.0, 0.0, 0.0],
         [0.5, -0.5]],
    ],
}

STRINGS = {
    "name": "strings",
    "fields": [
        fld("cfix", "char", "10"),
        fld("cvar", "char", "*"),
        fld("cbnd", "char", "5*"),
        fld("ufix", "unicodeChar", "6"),
        fld("uvar", "unicodeChar", "*"),
    ],
    "rows": [
        ["Apple", "Apple", "abc", "Яблоко", "Привет мир"],
        ["  lead", "  lead", " b ", "λόγος", "Ελληνικά"],
        ["trail   ", "trail   ", "abcde", "中文", "日本語テキスト"],
        ["", "", "", "", ""],
        [None, None, None, None, None],
        ["<&>\"'", "<&>\"'", "<>", " ab  ", "смешанный ASCII"],
        ["exactly10!", "multi word text", "x", "ÅÄÖ", "trail  "],
        ["tab\tin", "tab\tin", "  ", "€", "  lead"],
    ],
}

NULLS = {
    "name": "nulls",
    "fields": [
        fld("ub_n", "unsignedByte", null="255"),
        fld("s_n", "short", null="-32768"),
        fld("i_n", "int", null="-999"),
        fld("l_n", "long", null="-9223372036854775808"),
        fld("iarr_n", "int", "3", null="-1"),
        fld("svar_n", "short", "*", null="0"),
        fld("b_n", "boolean"),
        fld("f_n", "float"),
        fld("d_n", "double"),
        fld("str_n", "char", "*"),
    ],
    "rows": [
        [0, None, 1, None, [1, None, 3], [1, None, 2], True, NAN, None, "a"],
        [None, 32767, None, 9223372036854775807, [None, None, None], [], None, 1.0, NAN, None],
        [254, -32767, -998, 0, [4, 5, 6], None, False, None, 1.0, ""],
        [None, 0, None, None, None, [None], None, 2.0, None, "b"],
        [1, None, 2147483647, 1, [0, 0, None], [5], True, NAN, 3.0, None],
    ],
}


def wide(n):
    """`n` columns of rotating types, null on the diagonal, so each column's flag is exercised."""
    types = [("short", None), ("int", None), ("double", None), ("char", "*"), ("boolean", None),
             ("float", None), ("long", None), ("unsignedByte", None), ("char", "3")]
    fields, samples = [], []
    for k in range(n):
        dt, asz = types[k % len(types)]
        fields.append(fld(f"c{k:02d}", dt, asz))
        samples.append({
            "short": k - 5, "int": 1000 * k, "double": k + 0.25, "char": f"v{k}",
            "boolean": k % 2 == 0, "float": f32(k / 4), "long": -(10 ** 12) * k,
            "unsignedByte": k,
        }[dt])
    rows = []
    for r in range(n + 1):
        rows.append([None if r == k else samples[k] for k in range(n)])
    rows.append([None] * n)
    return {"name": f"wide{n}", "fields": fields, "rows": rows}


XTYPES = {
    "name": "xtypes",
    "fields": [
        fld("ts", "char", "*", xtype="timestamp"),
        fld("tsfix", "char", "10", xtype="timestamp"),
        fld("pt", "double", "2", xtype="point", unit="deg", ucd="pos.eq"),
        fld("fpt", "float", "2", xtype="point", unit="deg"),
        fld("circ", "double", "3", xtype="circle", unit="deg"),
        fld("poly", "double", "*", xtype="polygon", unit="deg"),
        fld("intv", "double", "2", xtype="interval"),
        fld("lintv", "long", "2", xtype="interval"),
    ],
    "rows": [
        ["2000-01-02T15:20:30.456", "2002-03-04", [12.3, 45.6], [f32(12.3), f32(45.6)],
         [12.3, 45.6, 0.5], [10.0, 10.0, 10.2, 10.0, 10.2, 10.2, 10.0, 10.2], [0.5, 1.0], [0, 2]],
        ["2001-02-03T04:05:06", "1999-12-31", [0.0, -90.0], [0.0, -90.0], [0.0, 90.0, 180.0],
         [0.0, 0.0, 1.0, 0.0, 1.0, 1.0], [NINF, 0.0], [-5, 5]],
        ["2002-03-04", "2020-02-29", [359.9, 90.0], [f32(359.9), 90.0], [359.9, -89.5, 1e-06],
         [350.0, -10.0, 10.0, -10.0, 10.0, 10.0, 350.0, 10.0], [0.0, INF], [0, 0]],
        ["2000-01-02T15:20:30Z", "2000-01-01", [180.0, 0.0], [180.0, 0.0], [1.0, 2.0, 3.0],
         [], [NINF, INF], [-9223372036854775808, 9223372036854775807]],
        [None] * 8,
    ],
}

METADATA = {
    "name": "metadata",
    "fields": [
        fld("ra", "double", ID="col_ra", unit="deg", ucd="pos.eq.ra;meta.main",
            utype="stc:AstroCoords.Position2D.Value2.C1", width=10, precision="6", ref="icrs",
            description="Right ascension (ICRS)"),
        fld("dec", "double", ID="col_dec", unit="deg", ucd="pos.eq.dec;meta.main",
            utype="stc:AstroCoords.Position2D.Value2.C2", width=10, precision="6", ref="icrs",
            description="Declination (ICRS)"),
        fld("mag", "float", ID="col_mag", unit="mag", ucd="phot.mag;em.opt.V", width=6,
            precision="3", min="-5", max="30", description="V magnitude"),
        fld("flag", "short", ID="col_flag", ucd="meta.code", null="-1",
            options=[{"name": "good", "value": "0"}, {"name": "bad", "value": "1"}],
            description="Quality flag; -1 when unknown"),
        fld("obs_mjd", "double", ID="col_mjd", unit="d", ucd="time.epoch", xtype="mjd",
            ref="tt", width=12, precision="5", description="Epoch of observation"),
        fld("url", "char", "*", ID="col_url", ucd="meta.ref.url",
            description="Where the <preview> & \"notes\" live",
            link="https://example.org/preview?id=${col_ra}&amp=1"),
    ],
    "rows": [
        [10.684708, 41.26875, f32(3.44), 0, 58000.5, "https://example.org/a"],
        [83.822083, -5.391111, f32(4.0), 1, 58001.25, None],
        [None, None, None, None, None, "https://example.org/c?x=1&y=2"],
    ],
}

EMPTY = {
    "name": "empty",
    "fields": [f for f in SCALARS["fields"]] + [ARRAYS["fields"][k] for k in (0, 2, 7, 8)],
    "rows": [],
}


def large():
    rng = np.random.default_rng(42)
    n = 300
    rows = []
    for k in range(n):
        ra = round(float(rng.uniform(0, 360)), 6)
        dec = round(float(rng.uniform(-90, 90)), 6)
        mag = f32(round(float(rng.uniform(10, 25)), 3))
        if k % 97 == 0:
            mag = NAN
        flag = int(rng.integers(0, 100))
        rows.append([k, ra, dec, None if k % 31 == 0 else mag, None if k % 13 == 0 else flag])
    return {
        "name": "large",
        "fields": [fld("id", "long"), fld("ra", "double", unit="deg"),
                   fld("dec", "double", unit="deg"), fld("mag", "float", unit="mag"),
                   fld("flag", "short")],
        "rows": rows,
    }


BITVAR = {
    "name": "bitvar",
    "fields": [fld("bitv", "bit", "*"), fld("after", "int")],
    "rows": [[[True, False, True], 1], [[], 2], [[True] * 9, 3]],
}

TABLES = [SCALARS, ARRAYS, STRINGS, NULLS, wide(9), wide(17), XTYPES, METADATA, EMPTY, large()]
BY_NAME = {t["name"]: t for t in TABLES}

SERIALIZATIONS = ["tabledata", "binary", "binary2"]

# Columns a writer cannot write at all, found by trying; the rest of the table is written.
EXCLUDE = {
    # astropy 8.0.1: `E01: Invalid size specifier '8x' for a char field`. A variable-length bit
    # array it writes as a count of bits followed by one byte per bit, so it has a table of its
    # own (BITVAR) rather than making every astropy BINARY arrays file unreadable.
    "astropy": {"arrays": ["c8xs", "c8x3", "bitv"]},
    # votable 0.7.0: a bit scalar panics on write (`VOTableValue::Bool` asserts a boolean
    # schema) and a bit array cannot be constructed (`BitVec`'s field is private).
    "cds": {"scalars": ["bit"], "arrays": ["bit5", "bit12", "bitv"], "empty": ["bit"],
            "wide9": [], "wide17": []},
}

# ---------------------------------------------------------------------------------------------
# Datatype facts shared by the writers and the decoder.
# ---------------------------------------------------------------------------------------------

INT_TYPES = {"unsignedByte": "B", "short": ">h", "int": ">i", "long": ">q"}
FLOAT_TYPES = {"float": ">f", "double": ">d"}
COMPLEX_TYPES = {"floatComplex": ">f", "doubleComplex": ">d"}
SIZES = {"boolean": 1, "unsignedByte": 1, "short": 2, "int": 4, "long": 8, "char": 1,
         "unicodeChar": 2, "float": 4, "double": 8, "floatComplex": 8, "doubleComplex": 16}
INT_RANGES = {"unsignedByte": (0, 255), "short": (-(2 ** 15), 2 ** 15 - 1),
              "int": (-(2 ** 31), 2 ** 31 - 1), "long": (-(2 ** 63), 2 ** 63 - 1)}


class Shape:
    """What an arraysize says: the fixed dimensions, and whether (and how far) the last varies."""

    def __init__(self, arraysize):
        self.text = arraysize
        self.dims = []  # every dimension; the last may be None when variable
        self.variable = False
        self.bound = None
        if arraysize is None:
            return
        parts = arraysize.split("x")
        for k, p in enumerate(parts):
            p = p.strip()
            if p.endswith("*"):
                if k != len(parts) - 1:
                    raise DecodeError(f"'*' before the last dimension in arraysize {arraysize!r}")
                self.variable = True
                self.bound = int(p[:-1]) if p[:-1] else None
                self.dims.append(None)
            else:
                if not p.isdigit():
                    raise DecodeError(f"bad arraysize {arraysize!r}")
                self.dims.append(int(p))

    @property
    def is_array(self):
        return self.text is not None

    @property
    def inner(self):
        """Primitives in one item of the last dimension (1 for a 1-D array)."""
        return math.prod(self.dims[:-1]) if self.dims else 1

    @property
    def fixed_count(self):
        return None if self.variable else math.prod(self.dims)


def is_string(field):
    return field["datatype"] in ("char", "unicodeChar") and field["arraysize"] is not None and \
        "x" not in field["arraysize"]


def is_char2d(field):
    return field["datatype"] in ("char", "unicodeChar") and field["arraysize"] is not None and \
        "x" in field["arraysize"]


def json_float(x):
    if isinstance(x, str):
        return x
    if math.isnan(x):
        return NAN
    if math.isinf(x):
        return INF if x > 0 else NINF
    return float(x)


def py_float(x):
    return {NAN: float("nan"), INF: float("inf"), NINF: float("-inf")}.get(x, x) \
        if isinstance(x, str) else float(x)


# ---------------------------------------------------------------------------------------------
# The reference decoder: VOTable 1.5 as written, into the corpus encoding.
# ---------------------------------------------------------------------------------------------


class DecodeError(Exception):
    pass


def local(tag):
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else tag


def field_meta(elem, ids=None):
    """A FIELD element's attributes and children, in the ground truth's `columns` shape. A
    `VALUES ref` takes the null of the VALUES it names."""
    desc = None
    null = None
    for child in elem:
        if local(child.tag) == "DESCRIPTION":
            desc = "".join(child.itertext()).strip()
        elif local(child.tag) == "VALUES":
            values = child
            if child.get("ref") is not None:
                values = (ids or {}).get(child.get("ref"))
                if values is None or local(values.tag) != "VALUES":
                    raise DecodeError(f"VALUES ref={child.get('ref')!r} names no VALUES")
            null = values.get("null")
    if elem.get("datatype") is None:
        raise DecodeError(f"FIELD {elem.get('name')!r} has no datatype")
    if elem.get("datatype") not in SIZES and elem.get("datatype") != "bit":
        raise DecodeError(f"unknown datatype {elem.get('datatype')!r}")
    Shape(elem.get("arraysize"))
    return {
        "name": elem.get("name") if elem.get("name") is not None else elem.get("ID"),
        "datatype": elem.get("datatype"),
        "arraysize": elem.get("arraysize"),
        "xtype": elem.get("xtype"),
        "unit": elem.get("unit"),
        "ucd": elem.get("ucd"),
        "utype": elem.get("utype"),
        "description": desc,
        "null": null,
    }


def parse_int(text, datatype):
    t = text.strip()
    m = re.fullmatch(r"0x([0-9a-fA-F]+)", t)
    if m:
        digits = m.group(1)
        bits = 8 * SIZES[datatype]
        if len(digits) > bits // 4:
            raise DecodeError(f"too many hexadigits for a {datatype}: {text!r}")
        v = int(digits, 16)
        # "0x followed by 1 to 4 hexadigits" for a short makes 0x8000-0xFFFF legal spellings,
        # which only a two's-complement reading puts in range.
        if datatype != "unsignedByte" and v >= 1 << (bits - 1):
            v -= 1 << bits
    elif re.fullmatch(r"[+-]?[0-9]+", t):
        v = int(t)
    else:
        raise DecodeError(f"not a {datatype}: {text!r}")
    lo, hi = INT_RANGES[datatype]
    if not lo <= v <= hi:
        raise DecodeError(f"{datatype} out of range: {text!r}")
    return v


FLOAT_RE = re.compile(r"[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?")


def parse_float(text, datatype):
    t = text.strip()
    if t == "NaN":
        v = float("nan")
    elif t in ("+Inf", "Inf"):
        v = float("inf")
    elif t == "-Inf":
        v = float("-inf")
    elif FLOAT_RE.fullmatch(t):
        v = float(t)
    else:
        # Not a TABLEDATA literal. Accept the spellings other languages print for the specials,
        # so the ground truth can say what the writer meant, and let the caller record it.
        lowered = t.lower().lstrip("+")
        if lowered in ("inf", "infinity"):
            v = float("inf")
        elif lowered in ("-inf", "-infinity"):
            v = float("-inf")
        elif lowered == "nan":
            v = float("nan")
        else:
            raise DecodeError(f"not a {datatype}: {text!r}")
        LENIENT.append(f"{datatype} written as {text!r}, which is not a TABLEDATA literal")
    if datatype == "float":
        v = float(np.float32(v)) if math.isfinite(v) else v
    return v


LENIENT = []  # non-literal spellings met while decoding the current file


def parse_bool_token(tok):
    if tok in ("T", "t", "1") or tok.lower() == "true":
        return True
    if tok in ("F", "f", "0") or tok.lower() == "false":
        return False
    if tok in ("?", " ", "\0", ""):
        return None
    raise DecodeError(f"not a boolean: {tok!r}")


def null_value(field):
    """The VALUES null as a value of the column's datatype, or None."""
    text = field.get("null")
    if text is None:
        return None
    dt = field["datatype"]
    if dt in INT_TYPES:
        return parse_int(text, dt)
    if dt in FLOAT_TYPES:
        return parse_float(text, dt)
    return text


def is_magic(v, magic):
    if magic is None or v is None:
        return False
    if isinstance(v, float) and isinstance(magic, float) and math.isnan(v) and math.isnan(magic):
        return True
    return v == magic


def finish_string(s, fixed):
    s = s.split("\0", 1)[0]
    return s.rstrip(" ") if fixed else s


def chunk_strings(text, width, fixed_count, unicode):
    units = text.encode("utf-16-be") if unicode else None
    if unicode:
        n = len(units) // 2
        chars = [units[2 * k: 2 * k + 2].decode("utf-16-be") for k in range(n)]
    else:
        chars = list(text)
    if fixed_count is not None:
        chars += [" "] * (width * fixed_count - len(chars))
    out = []
    for k in range(0, len(chars), width):
        out.append(finish_string("".join(chars[k:k + width]), True))
    return out


def td_cell(field, text):
    dt = field["datatype"]
    shape = Shape(field["arraysize"])
    magic = null_value(field)
    if text == "":
        return None
    if dt in ("char", "unicodeChar"):
        if not shape.is_array:
            v = finish_string(text, False)
            return None if is_magic(v, magic) else v
        if is_string(field):
            v = finish_string(text, not shape.variable)
            return None if is_magic(v, magic) else v
        width = shape.dims[0]
        rest = shape.dims[1:]
        count = None if shape.variable else math.prod(rest)
        return chunk_strings(text, width, count, dt == "unicodeChar")
    if dt == "boolean" and not shape.is_array:
        return parse_bool_token(text.strip()) if text.strip() else None
    if dt == "bit":
        bits = re.sub(r"\s+", "", text)
        if bits == "":
            return None
        vals = []
        for ch in bits:
            if ch not in "01":
                raise DecodeError(f"not a bit: {ch!r}")
            vals.append(ch == "1")
        if not shape.is_array:
            if len(vals) != 1:
                raise DecodeError(f"{len(vals)} bits in a bit scalar")
            return vals[0]
        check_count(field, shape, len(vals))
        return vals
    toks = text.split()
    if not toks:
        return None
    if dt == "boolean":
        vals = [parse_bool_token(t) for t in toks]
        check_count(field, shape, len(vals))
        return vals
    if dt in INT_TYPES:
        vals = [parse_int(t, dt) for t in toks]
        vals = [None if is_magic(v, magic) else v for v in vals]
    elif dt in FLOAT_TYPES:
        vals = [parse_float(t, dt) for t in toks]
        vals = [None if is_magic(v, magic) else json_float(v) for v in vals]
    elif dt in COMPLEX_TYPES:
        base = "float" if dt == "floatComplex" else "double"
        vals = [json_float(parse_float(t, base)) for t in toks]
        if len(vals) % 2:
            raise DecodeError(f"odd number of values in a {dt}")
    else:
        raise DecodeError(f"unknown datatype {dt!r}")
    per = 2 if dt in COMPLEX_TYPES else 1
    if not shape.is_array:
        if len(vals) != per:
            raise DecodeError(f"{len(vals)} values in a {dt} scalar")
        return vals if per == 2 else vals[0]
    check_count(field, shape, len(vals) // per)
    return vals


def check_count(field, shape, n):
    if not shape.variable:
        if n != shape.fixed_count:
            raise DecodeError(f"{n} elements in {field['name']!r}, arraysize {shape.text}")
    else:
        if n % shape.inner:
            raise DecodeError(f"{n} elements in {field['name']!r} is not a whole number of "
                              f"items of arraysize {shape.text}")
        if shape.bound is not None and n // shape.inner > shape.bound:
            raise DecodeError(f"{n} elements in {field['name']!r}, over the bound {shape.text}")


class Reader:
    def __init__(self, data):
        self.data = data
        self.pos = 0

    def take(self, n):
        if self.pos + n > len(self.data):
            raise DecodeError(f"stream ends inside a row (wanted {n} bytes at {self.pos}, "
                              f"{len(self.data) - self.pos} left)")
        out = self.data[self.pos:self.pos + n]
        self.pos += n
        return out

    def done(self):
        return self.pos >= len(self.data)


def bin_primitive(dt, raw):
    if dt in INT_TYPES:
        return struct.unpack(INT_TYPES[dt], raw)[0]
    if dt in FLOAT_TYPES:
        return struct.unpack(FLOAT_TYPES[dt], raw)[0]
    raise AssertionError(dt)


def bin_cell(field, r, var_count):
    """One cell of a BINARY row. `var_count` says what a variable array's count counts:
    "items" (last-dimension slices) or "primitives"."""
    dt = field["datatype"]
    shape = Shape(field["arraysize"])
    magic = null_value(field)
    if shape.variable:
        count = struct.unpack(">i", r.take(4))[0]
        if count < 0:
            raise DecodeError(f"negative array count {count}")
        if dt == "bit":
            n = count if var_count == "primitives" or shape.inner == 1 else count * shape.inner
        elif var_count == "items":
            n = count * shape.inner
        else:
            n = count
            if n % shape.inner:
                raise DecodeError(f"count {count} is not a whole number of items")
        if shape.bound is not None and n // shape.inner > shape.bound:
            raise DecodeError(f"count {count} over the bound {shape.text}")
    else:
        n = shape.fixed_count if shape.is_array else 1
    if dt == "bit" and shape.variable and var_count == "items":
        # astropy's layout, which the "items" reading is: the count of bits, then one byte
        # per bit, set where the byte is not zero.
        return [byte != 0 for byte in r.take(n)]
    if dt == "bit":
        raw = r.take((n + 7) // 8)
        if not shape.is_array:
            # One bit in one byte, whose padding §6 has be zero: any bit set is the value,
            # which reads STIL's 0x80 and astropy's 0x08 alike.
            return raw[0] != 0
        bits = [bool(raw[k // 8] & (0x80 >> (k % 8))) for k in range(n)]
        return bits
    raw = r.take(n * SIZES[dt])
    if dt in ("char", "unicodeChar"):
        text = raw.decode("utf-16-be") if dt == "unicodeChar" else raw.decode("latin-1")
        if dt == "char" and any(ord(ch) > 127 for ch in text):
            LENIENT.append("a char column carries bytes above 127")
        if not shape.is_array:
            v = finish_string(text, False)
            return None if is_magic(v, magic) else v
        if is_string(field):
            v = finish_string(text, not shape.variable)
            return None if is_magic(v, magic) else v
        width = shape.dims[0]
        return chunk_strings(text, width, None, dt == "unicodeChar")
    if dt == "boolean":
        vals = [parse_bool_token(chr(b)) for b in raw]
        return vals if shape.is_array else vals[0]
    if dt in COMPLEX_TYPES:
        fmt = COMPLEX_TYPES[dt]
        k = struct.calcsize(fmt)
        vals = [json_float(struct.unpack(fmt, raw[j:j + k])[0]) for j in range(0, len(raw), k)]
        return vals
    k = SIZES[dt]
    vals = [bin_primitive(dt, raw[j:j + k]) for j in range(0, len(raw), k)]
    if dt in FLOAT_TYPES:
        vals = [None if is_magic(v, magic) else json_float(v) for v in vals]
    else:
        vals = [None if is_magic(v, magic) else v for v in vals]
    return vals if shape.is_array else vals[0]


def decode_stream(fields, data, flagged, var_count):
    r = Reader(data)
    rows = []
    nflag = (len(fields) + 7) // 8
    while not r.done():
        flags = r.take(nflag) if flagged else b"\0" * nflag
        row = []
        for k, f in enumerate(fields):
            v = bin_cell(f, r, var_count)
            if flags[k // 8] & (0x80 >> (k % 8)):
                v = None
            row.append(v)
        if flagged:
            spare = flags[-1] & ((1 << (8 * nflag - len(fields))) - 1) if len(fields) % 8 else 0
            if spare:
                LENIENT.append("unused null-flag bits are set")
        rows.append(row)
    return rows


def first_table(root):
    for elem in root.iter():
        if local(elem.tag) == "TABLE":
            return elem
    raise DecodeError("no TABLE")


MAX_ATTRIBUTES = 256


def decode_file(path, var_count="items"):
    """The file's first TABLE: its columns and its rows, in the corpus encoding."""
    del LENIENT[:]
    raw = Path(path).read_bytes()
    if re.search(rb"<!DOCTYPE[^>]*\[", raw) or b"<!ENTITY" in raw:
        raise DecodeError("the document declares entities in a DTD")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        raise DecodeError(f"not well-formed XML: {e}") from e
    if local(root.tag) != "VOTABLE":
        raise DecodeError(f"the root element is {local(root.tag)!r}, not VOTABLE")
    ids = {}
    for elem in root.iter():
        if len(elem.attrib) > MAX_ATTRIBUTES:
            raise DecodeError(f"{local(elem.tag)} has {len(elem.attrib)} attributes")
        if elem.get("ID") is not None:
            ids[elem.get("ID")] = elem
    table = first_table(root)
    definer = table
    if table.get("ref") is not None and not any(local(e.tag) == "FIELD" for e in table):
        definer = ids.get(table.get("ref"))
        if definer is None or local(definer.tag) != "TABLE":
            raise DecodeError(f"TABLE ref={table.get('ref')!r} names no TABLE")
    fields = [field_meta(e, ids) for e in definer if local(e.tag) == "FIELD"]
    names = [f["name"] for f in fields]
    if len(set(names)) != len(names):
        dup = next(n for n in names if names.count(n) > 1)
        raise DecodeError(f"two columns are named {dup!r}")
    data = next((e for e in table if local(e.tag) == "DATA"), None)
    serialization = None
    rows = []
    if data is not None:
        ser = next(iter(data))
        serialization = local(ser.tag)
        if serialization == "TABLEDATA":
            for tr in ser:
                if local(tr.tag) != "TR":
                    continue
                tds = [td for td in tr if local(td.tag) == "TD"]
                if len(tds) != len(fields):
                    raise DecodeError(f"{len(tds)} TDs for {len(fields)} FIELDs")
                rows.append([td_cell(f, "".join(td.itertext())) for f, td in zip(fields, tds)])
        elif serialization in ("BINARY", "BINARY2"):
            stream = next(iter(ser))
            if stream.get("href"):
                raise DecodeError(f"the STREAM is remote (href={stream.get('href')!r})")
            if stream.get("encoding") != "base64":
                raise DecodeError(f"STREAM encoding {stream.get('encoding')!r} is not base64")
            raw = base64.b64decode("".join((stream.text or "").split()), validate=True)
            rows = decode_stream(fields, raw, serialization == "BINARY2", var_count)
        else:
            raise DecodeError(f"the {serialization} serialization is not read")
    return {"columns": fields, "rows": rows, "serialization": serialization,
            "lenient": sorted(set(LENIENT))}


def decode_any(path):
    """Decodes with either reading of a variable array's count, saying which one fitted."""
    results = {}
    for vc in ("items", "primitives"):
        try:
            results[vc] = decode_file(path, vc)
        except DecodeError as e:
            results[vc] = e
    ok = {k: v for k, v in results.items() if not isinstance(v, Exception)}
    if not ok:
        raise results["items"]
    return ok


# ---------------------------------------------------------------------------------------------
# The source data as the contract would read it back from a given serialization, to find what
# a writer lost.
# ---------------------------------------------------------------------------------------------


def contract_cell(field, v, serialization):
    if v is None:
        return None
    dt = field["datatype"]
    shape = Shape(field["arraysize"])
    if is_string(field):
        v = finish_string(v, not shape.variable)
        return None if serialization == "TABLEDATA" and v == "" else v
    if is_char2d(field):
        v = [finish_string(s, True) for s in v]
        return None if serialization == "TABLEDATA" and not v else v
    if shape.variable and serialization == "TABLEDATA" and not v:
        return None
    if dt == "float":
        return [json_float(f32(py_float(x))) if x is not None else None for x in v] \
            if isinstance(v, list) else json_float(f32(py_float(v)))
    if dt == "floatComplex":
        return [json_float(f32(py_float(x))) for x in v]
    return v


def through_tabledata(table):
    """The table as STILTS is handed it: a TABLEDATA document, where an empty string and an
    empty variable-length array are both an empty TD, which is null."""
    return {**table, "rows": [[contract_cell(f, v, "TABLEDATA") for f, v in zip(table["fields"],
                                                                                row)]
                              for row in table["rows"]]}


def same(a, b, dt=None):
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(same(x, y, dt) for x, y in zip(a, b))
    if isinstance(a, float) and isinstance(b, float):
        if dt in ("float", "floatComplex"):
            return np.float32(a) == np.float32(b)
        return a == b
    if isinstance(a, bool) or isinstance(b, bool):
        return a is b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return a == b
    return a == b


def all_null_list(v):
    return isinstance(v, list) and v and all(x is None for x in v)


def diff_rows(columns, expected, got, what, equivalent=None):
    """Differences between two sets of rows, grouped by column."""
    notes = []
    if len(expected) != len(got):
        return [f"{what}: {len(got)} rows where the file has {len(expected)}"]
    by_col = defaultdict(list)
    for r, (erow, grow) in enumerate(zip(expected, got)):
        for c, (e, g) in enumerate(zip(erow, grow)):
            if same(e, g, columns[c]["datatype"]):
                continue
            if equivalent and equivalent(columns[c], e, g):
                continue
            by_col[c].append((r, e, g))
    for c, diffs in by_col.items():
        r, e, g = diffs[0]
        more = f" (and {len(diffs) - 1} more rows)" if len(diffs) > 1 else ""
        notes.append(f"{what}: column {columns[c]['name']!r} row {r} is "
                     f"{json.dumps(g, ensure_ascii=False)} where the file says "
                     f"{json.dumps(e, ensure_ascii=False)}{more}")
    return notes


def source_notes(table, written_fields, decoded, serialization):
    """What the file says differently from the data it was written from."""
    names = [f["name"] for f in written_fields]
    idx = [next(k for k, f in enumerate(table["fields"]) if f["name"] == n) for n in names]
    source = [[contract_cell(table["fields"][k], row[k], serialization) for k in idx]
              for row in table["rows"]]
    out = []
    if len(source) != len(decoded):
        return [f"the source had {len(source)} rows; the file has {len(decoded)}"]
    by_col = defaultdict(list)
    for r, (srow, drow) in enumerate(zip(source, decoded)):
        for c, (s, d) in enumerate(zip(srow, drow)):
            if not same(s, d, written_fields[c]["datatype"]):
                by_col[c].append((r, s, d))
    for c, diffs in by_col.items():
        r, s, d = diffs[0]
        more = f" (and {len(diffs) - 1} more rows)" if len(diffs) > 1 else ""
        out.append(f"differs from the source data: column {names[c]!r} row {r} was "
                   f"{json.dumps(s, ensure_ascii=False)}, the file says "
                   f"{json.dumps(d, ensure_ascii=False)}{more}")
    return out


# ---------------------------------------------------------------------------------------------
# Writer 1: astropy's object model, and `Table.write`.
# ---------------------------------------------------------------------------------------------


NP_DTYPES = {"boolean": "?", "bit": "?", "unsignedByte": "u1", "short": "i2", "int": "i4",
             "long": "i8", "float": "f4", "double": "f8", "floatComplex": "c8",
             "doubleComplex": "c16"}


def astropy_value(field, v):
    """(value, mask) for one cell of astropy's masked record array.

    An array has to be of the column's own numpy dtype: astropy 8.0.1 writes each element of a
    variable-length array with `tobytes()`, so an int64 array in a `short` column goes into the
    BINARY stream as eight bytes an element under a count of elements, a stream no reader can
    parse."""
    dt = field["datatype"]
    shape = Shape(field["arraysize"])
    magic = null_value(field)
    np_dt = NP_DTYPES.get(dt)

    def num(x):
        return py_float(x) if dt in FLOAT_TYPES or dt in COMPLEX_TYPES else x

    if v is None:
        if shape.variable:
            if is_string(field):
                return "", True
            return np.ma.array(np.zeros(0, np_dt), mask=np.zeros(0, bool)), True
        return None, True
    if is_string(field) or dt in ("char", "unicodeChar"):
        return v, False
    if dt in COMPLEX_TYPES:
        vals = [complex(py_float(v[k]), py_float(v[k + 1])) for k in range(0, len(v), 2)]
        if not shape.is_array:
            return vals[0], False
        arr = np.array(vals, dtype=np_dt)
    elif dt == "boolean" and shape.is_array:
        arr = np.ma.array(np.array([bool(x) if x is not None else False for x in v], "?"),
                          mask=[x is None for x in v])
        return arr, False
    elif shape.is_array:
        mask = [x is None for x in v]
        fill = magic if magic is not None else 0
        arr = np.ma.array(np.array([num(x) if x is not None else fill for x in v], np_dt),
                          mask=mask)
    else:
        return num(v), False
    if shape.is_array and len(shape.dims) > 1:
        inner = shape.dims[:-1]
        arr = arr.reshape([-1] + inner[::-1]) if shape.variable else \
            arr.reshape(shape.dims[::-1])
    if shape.variable and not np.ma.isMaskedArray(arr):
        arr = np.ma.array(arr, mask=np.zeros(arr.shape, bool))
    return arr, False


def astropy_field(vot, f):
    kw = {k: f[k] for k in ("ID", "unit", "ucd", "utype", "xtype", "width", "precision", "ref")
          if f.get(k) is not None}
    field = tree.Field(vot, name=f["name"], datatype=f["datatype"], arraysize=f["arraysize"],
                       **kw)
    if f.get("description"):
        field.description = f["description"]
    if f.get("null") is not None:
        field.values.null = null_value(f)
    if f.get("min") is not None:
        field.values.min = float(f["min"])
    if f.get("max") is not None:
        field.values.max = float(f["max"])
    for opt in f.get("options", []):
        field.values.options.append((opt["name"], opt["value"]))
    if f.get("link"):
        field.links.append(tree.Link(href=f["link"]))
    return field


def binary_magic(table, fields, arrays_only):
    """astropy refuses to write a null integer into BINARY without a VALUES null (W31), and a
    null integer array into BINARY2 too, flag or no flag. So this declares one where the source
    has none: the lowest value the column does not hold."""
    out, notes = [], []
    for f in fields:
        k = table["fields"].index(f)
        cells = [row[k] for row in table["rows"]]
        if arrays_only and f["arraysize"] is None:
            out.append(f)
            continue
        if f["datatype"] in INT_TYPES and f.get("null") is None and None in cells:
            held = set()
            for c in cells:
                held.update(c if isinstance(c, list) else [c])
            lo, hi = INT_RANGES[f["datatype"]]
            magic = next(v for v in (range(hi, lo - 1, -1) if f["datatype"] == "unsignedByte"
                                     else range(lo, hi)) if v not in held)
            f = {**f, "null": str(magic)}
            notes.append(f"{f['name']}: VALUES null={magic}")
        out.append(f)
    if notes:
        what = "integer array" if arrays_only else "integer"
        notes = [f"astropy will not write a null {what} into this serialization without a "
                 f"VALUES null (W31), so the generator declared one where the source had none: "
                 + "; ".join(notes) + "."]
    return out, notes


def write_astropy(table, fields, serialization, path, version="1.5"):
    notes = []
    if serialization in ("binary", "binary2"):
        fields, notes = binary_magic(table, fields, serialization == "binary2")
        table = {**table, "fields": [next((g for g in fields if g["name"] == f["name"]), f)
                                     for f in table["fields"]]}
    vot = tree.VOTableFile(version=version)
    res = tree.Resource()
    vot.resources.append(res)
    t = tree.TableElement(vot, name=table["name"])
    res.tables.append(t)
    if table["name"] == "metadata":
        vot.infos.append(tree.Info(name="producer_note", value="corpus metadata table"))
        res.coordinate_systems.append(tree.CooSys(ID="icrs", system="ICRS", epoch="J2000"))
        res.time_systems.append(tree.TimeSys(ID="tt", timeorigin="MJD-origin", timescale="TT",
                                             refposition="TOPOCENTER"))
        res.infos.append(tree.Info(name="QUERY_STATUS", value="OK"))
        t.description = "A table whose metadata is the point"
        t.links.append(tree.Link(href="https://example.org/table", title="about"))
        t.params.append(tree.Param(vot, name="survey", datatype="char", arraysize="*",
                                   value="corpus", ucd="meta.id"))
        t.params.append(tree.Param(vot, ID="epoch", name="epoch", datatype="double",
                                   value="2000.0", unit="yr"))
    t.fields.extend(astropy_field(vot, f) for f in fields)
    if table["name"] == "metadata":
        g = tree.Group(t, name="position", ucd="pos.eq")
        g.description = "Where the object is"
        g.entries.append(tree.FieldRef(t, ref="col_ra"))
        g.entries.append(tree.FieldRef(t, ref="col_dec"))
        g.entries.append(tree.ParamRef(t, ref="epoch"))
        t.groups.append(g)
    t.create_arrays(len(table["rows"]))
    idx = {f["name"]: table["fields"].index(f) for f in fields}
    for r, row in enumerate(table["rows"]):
        for f in fields:
            name = t.fields[fields.index(f)].ID or f["name"]
            value, mask = astropy_value(f, row[idx[f["name"]]])
            variable = Shape(f["arraysize"]).variable
            if value is not None:
                if variable or not isinstance(value, np.ndarray):
                    t.array.data[name][r] = value
                else:
                    t.array.data[name][r] = np.ma.getdata(value)
            if mask:
                t.array.mask[name][r] = True
            elif isinstance(value, np.ma.MaskedArray) and not variable:
                t.array.mask[name][r] = np.ma.getmaskarray(value)
    t.format = serialization
    vot.to_xml(str(path))
    return notes


def write_astropy_table_write(path, serialization):
    """An astropy Table written by `Table.write(format="votable")`, which is the path pyvo takes
    to upload one: the VOTable metadata is derived from numpy dtypes and masks."""
    t = Table()
    t["id"] = MaskedColumn([1, 2, 3, 4], dtype=np.int64, mask=[False, True, False, False])
    t["ra"] = np.array([10.5, 20.25, np.nan, 359.999999])
    t["ra"].unit = "deg"
    t["ra"].description = "Right ascension"
    t["mag"] = MaskedColumn(np.array([15.5, 16.0, 17.25, 0.0], dtype=np.float32),
                            mask=[False, False, False, True])
    t["name"] = ["alpha", "", "gamma  ", "δέλτα"]
    t["flag"] = MaskedColumn([True, False, True, False], mask=[False, False, True, False])
    t["vec"] = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9], [-1, -2, -3]], dtype=np.int16)
    t["small"] = np.array([0, 255, 7, 1], dtype=np.uint8)
    kw = {} if serialization is None else {"tabledata_format": serialization}
    t.write(str(path), format="votable", overwrite=True, **kw)
    return {"id": [1, None, 3, 4], "ra": [10.5, 20.25, NAN, 359.999999],
            "mag": [15.5, 16.0, 17.25, None], "name": ["alpha", "", "gamma  ", "δέλτα"],
            "flag": [True, False, None, False],
            "vec": [[1, 2, 3], [4, 5, 6], [7, 8, 9], [-1, -2, -3]], "small": [0, 255, 7, 1]}


# ---------------------------------------------------------------------------------------------
# Writer 2: STILTS, reading a TABLEDATA document written here.
# ---------------------------------------------------------------------------------------------


def td_literal(field, v):
    dt = field["datatype"]
    shape = Shape(field["arraysize"])
    if v is None:
        return ""
    if dt in ("char", "unicodeChar"):
        if is_char2d(field):
            width = shape.dims[0]
            return "".join(s + " " * (width - len(s)) for s in v)
        return v

    def one(x):
        if x is None:
            return field.get("null")
        if isinstance(x, bool):
            return "T" if x else "F"
        if isinstance(x, str):
            return {NAN: "NaN", INF: "+Inf", NINF: "-Inf"}[x]
        if isinstance(x, float):
            return repr(x)
        return str(x)

    if dt == "bit":
        # Space-separated: STIL 4.3-6 reads "10110" as one token, which it takes for a single
        # true, and fills the rest of a fixed array with false.
        return " ".join("1" if b else "0" for b in (v if isinstance(v, list) else [v]))
    if dt == "boolean":
        if isinstance(v, list):
            return " ".join("?" if b is None else one(b) for b in v)
        return one(v)
    if isinstance(v, list):
        return " ".join(one(x) for x in v)
    return one(v)


def field_xml(f):
    attrs = [("name", f["name"])]
    for k in ("ID", "datatype", "arraysize", "xtype", "unit", "ucd", "utype", "width",
              "precision", "ref"):
        if f.get(k) is not None:
            attrs.append((k, str(f[k])))
    body = []
    if f.get("description"):
        body.append(f"<DESCRIPTION>{escape(f['description'])}</DESCRIPTION>")
    vals = []
    if f.get("min") is not None:
        vals.append(f'<MIN value={quoteattr(f["min"])}/>')
    if f.get("max") is not None:
        vals.append(f'<MAX value={quoteattr(f["max"])}/>')
    for o in f.get("options", []):
        vals.append(f'<OPTION name={quoteattr(o["name"])} value={quoteattr(o["value"])}/>')
    if vals or f.get("null") is not None:
        null = f' null={quoteattr(f["null"])}' if f.get("null") is not None else ""
        body.append(f"<VALUES{null}>{''.join(vals)}</VALUES>")
    if f.get("link"):
        body.append(f"<LINK href={quoteattr(f['link'])}/>")
    a = " ".join(f"{k}={quoteattr(v)}" for k, v in attrs)
    return f"<FIELD {a}>{''.join(body)}</FIELD>" if body else f"<FIELD {a}/>"


def write_stilts_input(table, fields, path):
    idx = [table["fields"].index(f) for f in fields]
    out = ['<?xml version="1.0" encoding="UTF-8"?>',
           '<VOTABLE version="1.5" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">', "<RESOURCE>"]
    if table["name"] == "metadata":
        out += ['<COOSYS ID="icrs" system="ICRS" epoch="J2000"/>',
                '<TIMESYS ID="tt" timeorigin="MJD-origin" timescale="TT" '
                'refposition="TOPOCENTER"/>',
                '<INFO name="QUERY_STATUS" value="OK"/>']
    out.append(f"<TABLE name={quoteattr(table['name'])}>")
    if table["name"] == "metadata":
        out += ["<DESCRIPTION>A table whose metadata is the point</DESCRIPTION>",
                '<PARAM name="survey" datatype="char" arraysize="*" value="corpus" '
                'ucd="meta.id"/>',
                '<PARAM ID="epoch" name="epoch" datatype="double" value="2000.0" unit="yr"/>']
    out += [field_xml(f) for f in fields]
    if table["name"] == "metadata":
        out.append('<GROUP name="position" ucd="pos.eq"><DESCRIPTION>Where the object is'
                   '</DESCRIPTION><FIELDref ref="col_ra"/><FIELDref ref="col_dec"/>'
                   '<PARAMref ref="epoch"/></GROUP>')
    out.append("<DATA><TABLEDATA>")
    for row in table["rows"]:
        cells = "".join(f"<TD>{escape(td_literal(f, row[k]))}</TD>" for f, k in zip(fields, idx))
        out.append(f"<TR>{cells}</TR>")
    out += ["</TABLEDATA></DATA>", "</TABLE>", "</RESOURCE>", "</VOTABLE>"]
    path.write_text("\n".join(out) + "\n", encoding="utf-8")


def stilts(*args):
    proc = subprocess.run(["stilts", *args], capture_output=True, text=True)
    if proc.returncode:
        raise RuntimeError(proc.stderr.strip() or f"exit status {proc.returncode}")
    return proc


STILTS_OFMT = {"tabledata": "TABLEDATA", "binary": "BINARY", "binary2": "BINARY2"}


STILTS_NOTE = ("Written by STILTS from a TABLEDATA document, so an empty string or an empty "
               "variable-length array in the source was already null on the way in.")


def write_stilts(src, path, serialization, version=None):
    opts = [f"format={STILTS_OFMT[serialization]}"]
    if version:
        opts.append(f"version=V{version.replace('.', '')}")
    stilts("tcopy", f"in={src}", "ifmt=votable", f"out={path}",
           f"ofmt=votable({','.join(opts)})")


# ---------------------------------------------------------------------------------------------
# Writer 3: the CDS `votable` crate.
# ---------------------------------------------------------------------------------------------


def run_cds(jobs, scratch):
    spec = {"tables": []}
    for table, fields, sers in jobs:
        idx = [table["fields"].index(f) for f in fields]
        spec["tables"].append({
            "name": table["name"], "version": "1.4", "serializations": sers,
            "fields": fields, "rows": [[row[k] for k in idx] for row in table["rows"]]})
    spec_path = scratch / "cds-spec.json"
    spec_path.write_text(json.dumps(spec, ensure_ascii=False))
    subprocess.run(["cargo", "run", "--quiet", "--release", "--manifest-path",
                    str(HERE / "cds-writer" / "Cargo.toml"), "--target-dir",
                    str(scratch / "cds-target"), "--", str(spec_path), str(HERE)], check=True)


# ---------------------------------------------------------------------------------------------
# Reading the files back with astropy and STILTS.
# ---------------------------------------------------------------------------------------------


def norm_astropy_cell(field, value, mask):
    dt = field["datatype"]
    shape = Shape(field["arraysize"])
    if isinstance(value, np.ma.MaskedArray) and value.ndim == 0:
        mask = bool(np.ma.getmaskarray(value)) or bool(mask)
        value = value.item() if not mask else None
    if isinstance(mask, np.ndarray) and mask.ndim == 0:
        mask = bool(mask)
    if mask is True or (isinstance(mask, (bool, np.bool_)) and bool(mask)):
        return None
    if value is None or value is np.ma.masked:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8", "replace")
    if is_string(field) or (dt in ("char", "unicodeChar") and not shape.is_array):
        return finish_string(str(value), shape.is_array and not shape.variable)
    if isinstance(value, np.ndarray):
        m = np.ma.getmaskarray(value) if np.ma.isMaskedArray(value) else \
            (np.asarray(mask) if isinstance(mask, np.ndarray) else np.zeros(value.shape, bool))
        data = np.ma.getdata(value)
        flat, mflat = data.reshape(-1), np.broadcast_to(m, data.shape).reshape(-1)
        out = []
        for x, mk in zip(flat, mflat):
            if dt in COMPLEX_TYPES:
                out += [None, None] if mk else [json_float(float(x.real)),
                                                json_float(float(x.imag))]
            elif mk:
                out.append(None)
            elif dt in FLOAT_TYPES:
                out.append(json_float(float(x)))
            elif dt in ("boolean", "bit"):
                out.append(bool(x))
            else:
                out.append(int(x))
        return out
    if dt in COMPLEX_TYPES:
        return [json_float(float(value.real)), json_float(float(value.imag))]
    if dt in FLOAT_TYPES:
        return json_float(float(value))
    if dt in ("boolean", "bit"):
        return bool(value)
    return int(value)


def read_astropy(path, columns):
    vot = astropy_parse(str(path), verify="ignore")
    t = vot.get_first_table()
    arr = t.array
    names = arr.dtype.names
    raw_fixed = {}
    rows = []
    for r in range(len(arr)):
        row = []
        for c, f in enumerate(columns):
            v = arr.data[names[c]][r]
            m = arr.mask[names[c]][r]
            if is_string(f) and not Shape(f["arraysize"]).variable and not np.all(m):
                raw_fixed.setdefault(f["name"], []).append(
                    v.decode("latin-1") if isinstance(v, bytes) else str(v))
            row.append(norm_astropy_cell(f, v, m))
        rows.append(row)
    return rows, raw_fixed


def read_stilts(path, scratch, columns):
    """What STILTS reads, as STILTS writes it back out in BINARY2: the one serialization that
    tells a null from an empty string or array, and keeps a string's trailing spaces."""
    out = scratch / "stilts-readback.vot"
    stilts("tcopy", f"in={path}", "ifmt=votable", f"out={out}",
           "ofmt=votable(format=BINARY2,version=V15)")
    # STIL counts the primitives of a variable multi-dimensional array.
    back = decode_file(out, "primitives")
    return back["rows"], back["columns"], {}


def astropy_equivalent(field, expected, got):
    # astropy has one mask per element of a fixed array, so a null cell reads as every element
    # null and cannot be told from that; and a zero-length variable array reads as null.
    if expected is None and all_null_list(got):
        return True
    if got is None and all_null_list(expected):
        return True
    return False


# ---------------------------------------------------------------------------------------------
# Putting it together.
# ---------------------------------------------------------------------------------------------

SUMMARY = defaultdict(list)
RAW_FIXED = defaultdict(dict)


def ground_truth(path, producer, table, fields, serialization_hint, scratch, extra_notes=()):
    decoded_by = decode_any(path)
    notes = list(extra_notes)
    if len(decoded_by) == 2 and not same(decoded_by["items"]["rows"],
                                         decoded_by["primitives"]["rows"]):
        # Both readings parsed: pick the one that matches the source data.
        best = None
        for vc, d in decoded_by.items():
            if table is not None and not source_notes(table, fields, d["rows"],
                                                      d["serialization"]):
                best = vc
        vc = best or "items"
    else:
        vc = "items" if "items" in decoded_by else "primitives"
    decoded = decoded_by[vc]
    has_var_nd = any(Shape(c["arraysize"]).variable and Shape(c["arraysize"]).inner > 1
                     and any(row[k] for row in decoded["rows"])
                     for k, c in enumerate(decoded["columns"]))
    if has_var_nd and decoded["serialization"] in ("BINARY", "BINARY2"):
        other = "primitives" if vc == "items" else "items"
        notes.append(f"a variable multi-dimensional array's 4-byte count is written as the "
                     f"number of {'last-dimension items' if vc == 'items' else 'primitives'}; "
                     f"read as {other}, the stream "
                     + ("also parses, to other values" if other in decoded_by else
                        "does not parse"))
        SUMMARY["var-count"].append(f"{path.name}: {vc}")
    if decoded["lenient"]:
        notes.append("not conformant, and the ground truth takes the evident meaning: "
                     + "; ".join(decoded["lenient"]))
        SUMMARY["lenient"] += [f"{path.name}: {msg}" for msg in decoded["lenient"]]
    columns, rows = decoded["columns"], decoded["rows"]
    if table is not None:
        lost = source_notes(table, fields, rows, decoded["serialization"])
        notes += lost
        SUMMARY["lost"] += [f"{path.name}: {n}" for n in lost]
    try:
        arows, araw = read_astropy(path, columns)
        d = diff_rows(columns, rows, arows, "astropy 8.0.1 reads", astropy_equivalent)
        for name, vals in araw.items():
            RAW_FIXED[path.name][f"astropy:{name}"] = vals
    except Exception as e:  # noqa: BLE001
        msg = str(e).replace(str(path), path.name)[:200]
        d = [f"astropy 8.0.1 fails to read it: {type(e).__name__}: {msg}"]
    notes += d
    SUMMARY["astropy"] += [f"{path.name}: {n}" for n in d]
    try:
        srows, scols, sraw = read_stilts(path, scratch, columns)
        d = diff_rows(columns, rows, srows, "STILTS reads (as its BINARY2 output says)")
        for name, vals in sraw.items():
            RAW_FIXED[path.name][f"stilts:{name}"] = vals
    except Exception as e:  # noqa: BLE001
        d = [f"STILTS fails to read it: {str(e).splitlines()[0][:200]}"]
    notes += d
    SUMMARY["stilts"] += [f"{path.name}: {n}" for n in d]
    truth = OrderedDict([
        ("producer", producer),
        ("serialization", decoded["serialization"]),
        ("columns", columns),
        ("rows", rows),
        ("refuse", None),
        ("notes", "\n".join(notes) if notes else None),
    ])
    write_truth(path, truth)


def write_truth(path, truth):
    """One row per line, so a diff of the ground truth reads as a diff of rows."""
    head = {k: v for k, v in truth.items() if k != "rows"}
    lines = ["{"]
    for k, v in head.items():
        if k == "columns":
            cols = ",\n    ".join(json.dumps(c, ensure_ascii=False) for c in v)
            lines.append(f'  "columns": [\n    {cols}\n  ],' if v else '  "columns": [],')
            rows = ",\n    ".join(json.dumps(r, ensure_ascii=False, allow_nan=False)
                                  for r in truth["rows"])
            lines.append(f'  "rows": [\n    {rows}\n  ],' if truth["rows"] else '  "rows": [],')
        else:
            lines.append(f"  {json.dumps(k)}: {json.dumps(v, ensure_ascii=False)},")
    lines[-1] = lines[-1].rstrip(",")
    lines.append("}")
    Path(str(path) + ".json").write_text("\n".join(lines) + "\n", encoding="utf-8")


def stilts_version():
    out = stilts("-version").stdout
    stilts_v = re.search(r"STILTS version (\S+)", out).group(1)
    stil_v = re.search(r"STIL version (\S+)", out).group(1)
    return f"STILTS {stilts_v} (STIL {stil_v})"


def columns_for(writer, table):
    skip = EXCLUDE.get(writer, {}).get(table["name"], [])
    return [f for f in table["fields"] if f["name"] not in skip]


def omitted_note(writer, table):
    skip = EXCLUDE.get(writer, {}).get(table["name"], [])
    return [f"columns {', '.join(skip)} of the source table are omitted: this writer cannot "
            f"write them"] if skip else []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scratch", type=Path, default=None)
    args = ap.parse_args()
    scratch = args.scratch or Path(tempfile.mkdtemp(prefix="votable-corpus-"))
    scratch.mkdir(parents=True, exist_ok=True)

    for old in HERE.glob("*.vot"):
        old.unlink()
    for old in HERE.glob("*.vot.json"):
        old.unlink()

    astropy_producer = f"astropy {astropy.__version__}"
    stilts_producer = stilts_version()
    cds_producer = "votable crate 0.7.0 (CDS, Rust)"
    jobs = []  # (path, producer, table, fields, serialization, notes)

    # astropy
    for table in TABLES:
        fields = columns_for("astropy", table)
        sers = ["binary2"] if table["name"] == "large" else SERIALIZATIONS
        for ser in sers:
            path = HERE / f"astropy-{ser}-{table['name']}.vot"
            notes = write_astropy(table, fields, ser, path)
            jobs.append((path, astropy_producer, table, fields,
                         omitted_note("astropy", table) + notes))
    for ser in ("binary", "binary2"):
        path = HERE / f"astropy-{ser}-bitvar.vot"
        write_astropy(BITVAR, BITVAR["fields"], ser, path)
        jobs.append((path, astropy_producer, BITVAR, BITVAR["fields"], [
            "astropy 8.0.1 writes a variable-length bit array as the count of bits followed by one "
            "byte per bit, 0x08 for a set bit (and a bit scalar as 0x08 too, where the most "
            "significant bit is the one that counts). VOTable 1.5 packs eight bits to a byte; "
            "the ground truth reads astropy's layout, which is the 'items' reading's, since "
            "uploads written by astropy have to read."]))
    for version, sers in (("1.1", ["tabledata", "binary"]), ("1.2", ["tabledata", "binary"]),
                          ("1.3", ["binary2"])):
        for ser in sers:
            path = HERE / f"astropy-v{version}-{ser}-nulls.vot"
            notes = write_astropy(NULLS, NULLS["fields"], ser, path, version=version)
            jobs.append((path, astropy_producer, NULLS, NULLS["fields"],
                         [f"VOTable version {version}."] + notes))
    for ser in (None, "binary", "binary2"):
        path = HERE / f"astropy-tablewrite-{ser or 'default'}.vot"
        try:
            write_astropy_table_write(path, ser)
        except Exception as e:  # noqa: BLE001
            SUMMARY["unwritable"].append(f"{path.name}: {type(e).__name__}: {e}")
            path.unlink(missing_ok=True)
            continue
        jobs.append((path, astropy_producer, None, None,
                     ["An astropy Table written by Table.write(format='votable'"
                      + (f", tabledata_format='{ser}')" if ser else ")")
                     + ", the path pyvo takes to upload a table; the source data had "
                       "id=[1,null,3,4], ra=[10.5,20.25,NaN,359.999999], "
                       "mag=[15.5,16,17.25,null], name=['alpha','','gamma  ','δέλτα'], "
                       "flag=[true,false,null,false], small uint8=[0,255,7,1]."]))

    # STILTS
    for table in TABLES:
        fields = columns_for("stilts", table)
        src = scratch / f"stilts-input-{table['name']}.vot"
        write_stilts_input(table, fields, src)
        sers = ["binary2"] if table["name"] == "large" else SERIALIZATIONS
        for ser in sers:
            path = HERE / f"stilts-{ser}-{table['name']}.vot"
            write_stilts(src, path, ser)
            jobs.append((path, stilts_producer, through_tabledata(table), fields, [STILTS_NOTE]))
    src = scratch / "stilts-input-nulls.vot"
    for version, sers in (("1.1", ["tabledata", "binary"]), ("1.2", ["tabledata", "binary"]),
                          ("1.3", ["tabledata", "binary2"]), ("1.5", SERIALIZATIONS)):
        for ser in sers:
            path = HERE / f"stilts-v{version}-{ser}-nulls.vot"
            write_stilts(src, path, ser, version)
            jobs.append((path, stilts_producer, through_tabledata(NULLS), NULLS["fields"],
                         [f"VOTable version {version}.", STILTS_NOTE]))
    src = scratch / "stilts-input-arrays.vot"
    path = HERE / "stilts-v1.1-binary-arrays.vot"
    write_stilts(src, path, "binary", "1.1")
    jobs.append((path, stilts_producer, through_tabledata(ARRAYS), ARRAYS["fields"],
                 ["VOTable version 1.1.", STILTS_NOTE]))

    # CDS
    cds_jobs = []
    for table in [SCALARS, ARRAYS, STRINGS, NULLS, BY_NAME["wide17"], XTYPES, METADATA, EMPTY]:
        fields = columns_for("cds", table)
        cds_jobs.append((table, fields, SERIALIZATIONS))
    run_cds(cds_jobs, scratch)
    for table, fields, sers in cds_jobs:
        for ser in sers:
            path = HERE / f"cds-{ser}-{table['name']}.vot"
            jobs.append((path, cds_producer, table, fields, omitted_note("cds", table)))

    for path, producer, table, fields, notes in jobs:
        print(f"checking {path.name}", file=sys.stderr)
        try:
            ground_truth(path, producer, table, fields, None, scratch, notes)
        except DecodeError as e:
            others = []
            try:
                astropy_parse(str(path), verify="ignore").get_first_table()
                others.append("astropy 8.0.1 reads it without an error")
            except Exception as ae:  # noqa: BLE001
                others.append(f"astropy 8.0.1 refuses it too ({type(ae).__name__})")
            try:
                stilts("tcopy", f"in={path}", "ifmt=votable", f"out={scratch / 'x.vot'}",
                       "ofmt=votable")
                others.append("STILTS reads it without an error")
            except RuntimeError:
                others.append("STILTS refuses it too")
            truth = OrderedDict([("producer", producer), ("serialization", None),
                                 ("columns", []), ("rows", []), ("refuse", str(e)),
                                 ("notes", "\n".join(notes + [
                                     "The writer produced a document that is not valid "
                                     "VOTable, so a reader must refuse it; "
                                     + "; ".join(others) + "."]))])
            write_truth(path, truth)
            SUMMARY["invalid"].append(f"{path.name}: {e}")

    report = {k: v for k, v in SUMMARY.items()}
    report["raw_fixed_char"] = RAW_FIXED
    (scratch / "summary.json").write_text(json.dumps(report, ensure_ascii=False, indent=1))
    print(f"summary in {scratch / 'summary.json'}", file=sys.stderr)


if __name__ == "__main__":
    main()
