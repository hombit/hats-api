"""Table upload — TAP 1.1 section 2.5.

A client sends a table with its query and joins against it as TAP_UPLOAD. It is an
optional feature: a service declares in its capabilities whether it has one, and one
that declares none is conforming without it.

So there are two questions here, and only the second is a conformance question: does
upload work, and does the service's answer about upload match what it declared. The
first is reported as it comes — a service that has no upload fails it, which is a fact
about what a client can do here rather than a verdict on anyone.
"""

from __future__ import annotations

import math
import tempfile
from io import BytesIO
from pathlib import Path

import numpy as np
import pytest
from astropy.io.votable import from_table
from astropy.table import MaskedColumn, Table

from tap_conformance import tapquery
from tap_conformance.taplint import assert_clean

#: The round trip's statement. The index orders the rows, since TAP promises no order
#: of its own and the uploaded rows are deliberately not in index order.
ROUND_TRIP = "SELECT * FROM TAP_UPLOAD.mytable ORDER BY my_table_index"


def mytable() -> Table:
    """A table carrying every kind of value a crossmatch list or a small catalog does.

    Integers at the edges of their widths, floats with `NaN` and both infinities, a
    `float` next to a `double`, booleans, text past ASCII, a fixed-size array, and a
    missing value in every column that has a way to say one. The rows are shuffled, so a
    service that answered in upload order without sorting would be caught by the
    `ORDER BY`.
    """
    order = [3, 0, 4, 1, 2]
    index = np.array(order, dtype=np.int64)

    def masked(values, mask, dtype=None):
        column = MaskedColumn(values, mask=mask, dtype=dtype)
        return column[order]

    table = Table()
    table["my_table_index"] = index
    # Each type's extremes, but one above its minimum: STILTS declares the minimum as the
    # VALUES null of every integer column it writes in TABLEDATA and BINARY, so a minimum
    # it uploads reads back — as VOTable 1.5 §4.7 says it must — as missing.
    table["edges"] = np.array([-32767, 32767, 0, 1, -1], dtype="i2")[order]
    table["edges_long"] = np.array(
        [-9223372036854775807, 9223372036854775807, 0, 1, -1], dtype="i8"
    )[order]
    table["small"] = masked([-32767, 0, 1, 32767, 7], [False, False, True, False, False], "i2")
    table["medium"] = masked(
        [-2147483647, 2147483647, 5, 0, -1], [False, False, False, True, False], "i4"
    )
    table["large"] = masked(
        [-9223372036854775807, 9223372036854775807, 1383212200036217, 0, 42],
        [False, False, False, False, True],
        "i8",
    )
    table["mag"] = np.array([18.25, np.nan, 21.5, -0.125, 3.0e38], dtype=np.float32)[order]
    table["flux"] = np.array([1.1, np.inf, -np.inf, np.nan, 5e-300], dtype=np.float64)[order]
    # No missing value: astropy writes a masked boolean column as `bit`, which has no null
    # (W39, "Bit values can not be masked"), so one could not survive the upload.
    table["flag"] = np.array([True, False, True, False, True])[order]
    table["name"] = masked(
        ["M 31", "Ярослав", "γ Cas", "NGC 224", "pad"], [False, False, False, False, True], str
    )
    table["pos"] = np.array(
        [[10.68, 41.27], [0.0, -90.0], [359.99, 89.5], [180.0, 0.0], [45.5, -12.25]],
        dtype=np.float64,
    )[order]
    return table


def cells(table: Table, name: str) -> list:
    """One column's values in a form two clients' tables can be compared in.

    A missing value is `None`, whatever the client read it as. Two pairs are one value,
    VOTable 1.5 §5.5 saying so of each: `NaN` and a null in a float column ("VOTable
    implementations are not required to distinguish these cases", and astropy masks every
    `NaN` it reads), and an empty string and a null in a text column ("in TABLEDATA ...
    empty and null strings are not distinguished"). A `float` column is compared at
    `float` width, the clients differing in whether they widen it.
    """
    column = table[name]
    out = []
    for at in range(len(table)):
        value = column[at]
        if np.ma.is_masked(value):
            out.append(None)
            continue
        if isinstance(value, np.ndarray):
            out.append([normal(item, column.dtype) for item in value.tolist()])
            continue
        out.append(normal(value, column.dtype))
    return out


def normal(value, dtype):
    if isinstance(value, bytes):
        value = value.decode()
    if isinstance(value, (np.floating, float)):
        if math.isnan(value):
            return None
        if dtype == np.float32:
            return float(np.float32(value))
        return float(value)
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    if isinstance(value, (np.integer, int)):
        return int(value)
    if isinstance(value, str):
        return str(value) or None
    return value


#: The VALUES null a masked integer is written with where a serialization needs one:
#: astropy refuses to write one into BINARY without it, and none of `mytable`'s values is it.
MAGIC = 12345


def upload_of(serialization: str) -> BytesIO:
    """`mytable` as astropy writes it in one serialization, which is what pyvo sends."""
    document = BytesIO()
    table = mytable()
    for column in table.itercols():
        if isinstance(column, MaskedColumn) and column.dtype.kind == "i":
            column.fill_value = MAGIC
    votable = from_table(table)
    for field in votable.get_first_table().fields:
        if field.datatype in ("short", "int", "long"):
            field.values.null = MAGIC
    votable.to_xml(document, tabledata_format=serialization.lower())
    document.seek(0)
    return document


def sent() -> Table:
    """What was uploaded, in the order the query asks for it back."""
    table = mytable()
    table.sort("my_table_index")
    return table


def disagreements(got: Table, want: Table) -> list[str]:
    """Every way `got` is not `want`: columns, then cells."""
    wrong = []
    if got.colnames != want.colnames:
        wrong.append(f"columns {got.colnames}, expected {want.colnames}")
        return wrong
    if len(got) != len(want):
        wrong.append(f"{len(got)} rows, expected {len(want)}")
        return wrong
    for name in want.colnames:
        mine, theirs = cells(got, name), cells(want, name)
        if mine != theirs:
            wrong.append(f"{name}: {mine}, expected {theirs}")
    return wrong


SERIALIZATIONS = ["TABLEDATA", "BINARY", "BINARY2"]


def through_pyvo(tap, serialization: str) -> Table:
    return tap.run_sync(ROUND_TRIP, uploads={"mytable": upload_of(serialization)}).to_table()


def through_stilts(tap, stilts_command, serialization: str) -> Table:
    """The round trip through `stilts tapquery`, which is what TOPCAT uploads with.

    STILTS reads the table and writes a VOTable of its own to send, so what reaches the
    service is a second writer's serialization of the same rows.

    The file STILTS reads is astropy's BINARY2, which carries every value as its own bits,
    with two things set right between the two clients that would otherwise be blamed on the
    service: astropy writes an infinity in TABLEDATA as `inf`, which VOTable spells `+Inf`
    and STIL reads as `NaN`; and it declares a boolean column as a scalar `bit`, which STIL
    fails on in BINARY2, so that column is declared `boolean`.
    """
    if stilts_command is None:
        pytest.skip("STILTS is not installed")
    with tempfile.TemporaryDirectory() as scratch:
        source = Path(scratch) / "mytable.vot"
        votable = from_table(mytable())
        for field in votable.get_first_table().fields:
            if field.datatype == "bit":
                field.datatype = "boolean"
        votable.to_xml(str(source), tabledata_format="binary2")
        try:
            return tapquery.query(
                stilts_command,
                tap.baseurl,
                ROUND_TRIP,
                uploads={"mytable": source},
                upload_format=serialization,
            )
        except tapquery.Unavailable as missing:
            pytest.skip(str(missing))


@pytest.mark.parametrize("serialization", SERIALIZATIONS)
def test_round_trip_through_pyvo(tap, serialization, record_property):
    """TAP §2.7.6: "clients must be able to upload and then query a valid table and round
    trip all values". Uploaded by `pyvo` as astropy writes it, selected back whole, and
    compared cell by cell.
    """
    back = through_pyvo(tap, serialization)
    wrong = disagreements(back, sent())
    record_property("detail", "; ".join(wrong) or f"{len(back)} rows came back unchanged")
    assert not wrong, "; ".join(wrong)


@pytest.mark.parametrize("serialization", SERIALIZATIONS)
def test_round_trip_through_stilts(tap, stilts_command, serialization, record_property):
    """The same round trip, uploaded by `stilts tapquery`."""
    back = through_stilts(tap, stilts_command, serialization)
    wrong = disagreements(back, sent())
    record_property("detail", "; ".join(wrong) or f"{len(back)} rows came back unchanged")
    assert not wrong, "; ".join(wrong)


@pytest.mark.parametrize("serialization", SERIALIZATIONS)
def test_both_clients_get_the_same_rows_back(
    tap, stilts_command, serialization, record_property
):
    """Whatever each client makes of the answer, they make the same of it.

    The round trips above hold each client to what was sent; this holds them to each
    other, which is what a user switching between TOPCAT and a notebook depends on.
    """
    wrong = disagreements(
        through_stilts(tap, stilts_command, serialization), through_pyvo(tap, serialization)
    )
    record_property("detail", "; ".join(wrong) or "both clients read the same rows")
    assert not wrong, "; ".join(wrong)


def declared(tap) -> list[str]:
    return [str(method) for method in (tap.upload_methods or [])]


def test_inline_upload(tap, record_property):
    """A table uploaded with the query is queryable as TAP_UPLOAD."""
    uploaded = Table({"id": np.arange(3), "x": np.arange(3) * 1.5})
    found = tap.run_sync(
        "SELECT * FROM TAP_UPLOAD.t1", uploads={"t1": uploaded}
    ).to_table()
    record_property("detail", f"{len(found)} rows came back from an upload")
    assert len(found) == 3


def test_capabilities_agree_with_behaviour(tap, record_property):
    """What the service says about upload is what it does.

    Declaring a method it has not got sends a client down a path that fails at the
    point of sending data; having one it does not declare means no client will try.
    """
    methods = declared(tap)
    record_property("detail", f"declared: {', '.join(methods) or 'none'}")
    uploaded = Table({"id": np.arange(2)})
    try:
        tap.run_sync("SELECT * FROM TAP_UPLOAD.t1", uploads={"t1": uploaded})
        works = True
    except Exception:  # noqa: BLE001 — whether it worked is the whole question
        works = False
    assert works == bool(methods), (
        f"upload {'works' if works else 'does not work'} "
        f"while the capabilities declare {methods or 'no method'}"
    )


@pytest.mark.taplint("UPL")
def test_queries_with_uploads(stage, record_property):
    """Queries carrying an uploaded table are answered."""
    record_property("detail", stage.summarize())
    assert_clean(stage)
