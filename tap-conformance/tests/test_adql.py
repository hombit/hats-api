"""The query language — ADQL 2.1.

What a service has to understand for a query written against any other TAP service to
run here. Only the mandatory parts: the optional geometry beyond a circle, region
functions and user-defined functions are each a service's own decision, and a service
that refuses them is not less conforming for it.
"""

from __future__ import annotations

import pytest


def test_contains_a_circle(tap, queryable, coordinates, center, record_property):
    """A cone written as CONTAINS(POINT, CIRCLE) is answered.

    The one shape every client writes, and the one every catalog is asked for.
    """
    ra, dec = coordinates
    center_ra, center_dec = center
    found = tap.run_sync(
        f"SELECT COUNT(*) AS n FROM {queryable} WHERE 1=CONTAINS("
        f"POINT('ICRS', {ra}, {dec}), "
        f"CIRCLE('ICRS', {center_ra}, {center_dec}, 0.2))"
    ).to_table()
    count = int(found["n"][0])
    record_property("detail", f"{count} rows in the cone")
    assert count > 0, "the cone matched no rows, where the fetched data says it should"


def test_distance(tap, queryable, coordinates, center, record_property):
    """DISTANCE between two points is a value a query can select and order by."""
    ra, dec = coordinates
    center_ra, center_dec = center
    found = tap.run_sync(
        f"SELECT TOP 5 DISTANCE(POINT('ICRS', {ra}, {dec}), "
        f"POINT('ICRS', {center_ra}, {center_dec})) AS sep "
        f"FROM {queryable} ORDER BY sep"
    ).to_table()
    values = [float(value) for value in found["sep"]]
    record_property("detail", f"nearest {values[:3]}")
    assert values == sorted(values), "ORDER BY on a computed distance did not order"


def test_arithmetic_and_alias(tap, queryable, coordinates, record_property):
    """An expression over columns is a column, and AS names it."""
    ra, _ = coordinates
    found = tap.run_sync(
        f"SELECT TOP 3 {ra} * 2.0 AS doubled FROM {queryable}"
    ).to_table()
    record_property("detail", f"columns {found.colnames}")
    assert found.colnames == ["doubled"]


def test_where_and_order_by(tap, queryable, coordinates, record_property):
    """A predicate and an ordering, which is most of what a client writes."""
    ra, _ = coordinates
    found = tap.run_sync(
        f"SELECT TOP 10 {ra} FROM {queryable} WHERE {ra} > 0 ORDER BY {ra} DESC"
    ).to_table()
    values = [float(value) for value in found[found.colnames[0]]]
    record_property("detail", f"{len(values)} rows, descending")
    assert values == sorted(values, reverse=True), "ORDER BY DESC did not order"


def test_group_by(tap, queryable, coordinates, center, record_property):
    """An aggregate with a grouping, which ADQL makes mandatory.

    Bounded by a cone so that it groups a few thousand rows rather than a catalog.
    """
    ra, dec = coordinates
    center_ra, center_dec = center
    found = tap.run_sync(
        f"SELECT {ra} AS position, COUNT(*) AS n FROM {queryable} "
        f"WHERE 1=CONTAINS(POINT('ICRS', {ra}, {dec}), "
        f"CIRCLE('ICRS', {center_ra}, {center_dec}, 0.1)) "
        f"GROUP BY {ra}"
    ).to_table()
    record_property("detail", f"{len(found)} groups")
    assert len(found) >= 1


def test_string_comparison(tap, queryable, record_property):
    """LIKE over string literals, which ADQL makes mandatory.

    Over literals rather than over a column: which columns hold a string is a
    catalog's business, and what is being asked here is whether the language works.
    """
    found = tap.run_sync(
        f"SELECT TOP 1 'ab' AS matched FROM {queryable} WHERE 'ab' LIKE 'a%'"
    ).to_table()
    record_property("detail", f"{len(found)} rows matched a LIKE over literals")
    assert len(found) == 1, "LIKE over two literals that match returned nothing"


def test_delimited_identifier(tap, queryable, coordinates, record_property):
    """A name in double quotes is a delimited identifier, matched as written.

    It is how a client asks for a column whose name is mixed case or a reserved word,
    and what a client writes when it copies a name out of TAP_SCHEMA.
    """
    ra, _ = coordinates
    bare = ra.strip('"')
    found = tap.run_sync(f'SELECT TOP 2 "{bare}" FROM {queryable}').to_table()
    record_property("detail", f"columns {found.colnames}")
    assert len(found.colnames) == 1


@pytest.mark.parametrize("function", ["ABS(-1.5)", "CEILING(1.2)", "FLOOR(1.8)", "SQRT(4.0)"])
def test_mandatory_functions(tap, queryable, function, record_property):
    """The numeric functions ADQL 2.1 requires of every service."""
    found = tap.run_sync(f"SELECT TOP 1 {function} AS value FROM {queryable}").to_table()
    record_property("detail", f"{function} = {found['value'][0]}")
    assert len(found) == 1
