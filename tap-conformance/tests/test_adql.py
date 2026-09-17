"""The query language — ADQL 2.1.

What a service has to understand for a query written against any other TAP service to
run here. The mandatory parts are asked of every service. The optional ones — the
geometry, the string and set operators, `CAST`, `OFFSET` and the rest — are each a
service's own decision, so they are asked only of a service that declares them: a
service that leaves one out is not less conforming for it, and one that declares one it
does not answer has told a client something untrue.
"""

from __future__ import annotations

import re

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
        f"SELECT {ra} AS grouped, COUNT(*) AS n FROM {queryable} "
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


#: ADQL 2.1's optional features, by the TAPRegExt type that declares each.
FEATURE_TYPES = {
    "geometry": "ivo://ivoa.net/std/tapregext#features-adqlgeo",
    "string": "ivo://ivoa.net/std/tapregext#features-adql-string",
    "common-table": "ivo://ivoa.net/std/tapregext#features-adql-common-table",
    "sets": "ivo://ivoa.net/std/tapregext#features-adql-sets",
    "type": "ivo://ivoa.net/std/tapregext#features-adql-type",
    "conditional": "ivo://ivoa.net/std/tapregext#features-adql-conditional",
    "unit": "ivo://ivoa.net/std/tapregext#features-adql-unit",
    "offset": "ivo://ivoa.net/std/tapregext#features-adql-offset",
}


def feature_queries(table: str, ra: str, dec: str, center: tuple[float, float]) -> dict:
    """The smallest query that needs each form ADQL 2.1 defines, and nothing optional else.

    Every geometry writes its coordinate system: the argument is optional in 2.1 and
    required in 2.0, so writing it is what a query valid under either looks like. The
    cone keeps each one cheap against a catalog of any size.
    """
    x, y = center
    cone = f"CIRCLE('ICRS', {x}, {y}, 0.1)"
    point = f"POINT('ICRS', {ra}, {dec})"
    inside = f"SELECT TOP 1 {ra} FROM {table} WHERE 1=CONTAINS({point}, {{shape}})"
    computed = f"SELECT TOP 1 {{value}} AS computed FROM {table}"
    return {
        "POINT": inside.format(shape=cone),
        "CIRCLE": inside.format(shape=cone),
        "CONTAINS": inside.format(shape=cone),
        "INTERSECTS": f"SELECT TOP 1 {ra} FROM {table} WHERE 1=INTERSECTS({point}, {cone})",
        "DISTANCE": computed.format(value=f"DISTANCE({point}, POINT('ICRS', {x}, {y}))"),
        "BOX": inside.format(shape=f"BOX('ICRS', {x}, {y}, 0.2, 0.2)"),
        "POLYGON": inside.format(
            shape=f"POLYGON('ICRS', {x - 0.1}, {y - 0.1}, {x + 0.1}, {y - 0.1}, {x}, {y + 0.1})"
        ),
        "REGION": inside.format(shape=f"REGION('CIRCLE ICRS {x} {y} 0.1')"),
        "AREA": computed.format(value=f"AREA({cone})"),
        "CENTROID": computed.format(value=f"CENTROID({cone})"),
        "COORD1": computed.format(value=f"COORD1(POINT('ICRS', {x}, {y}))"),
        "COORD2": computed.format(value=f"COORD2(POINT('ICRS', {x}, {y}))"),
        "COORDSYS": computed.format(value=f"COORDSYS(POINT('ICRS', {x}, {y}))"),
        "LOWER": computed.format(value="LOWER('Ab')"),
        "UPPER": computed.format(value="UPPER('Ab')"),
        "ILIKE": f"SELECT TOP 1 {ra} FROM {table} WHERE 'Ab' ILIKE 'a%'",
        "WITH": f"WITH sampled AS (SELECT TOP 1 {ra} FROM {table}) SELECT {ra} FROM sampled",
        "UNION": f"SELECT {ra} FROM {table} WHERE 1=0 UNION SELECT {ra} FROM {table} WHERE 1=0",
        "EXCEPT": f"SELECT {ra} FROM {table} WHERE 1=0 EXCEPT SELECT {ra} FROM {table} WHERE 1=0",
        "INTERSECT": (
            f"SELECT {ra} FROM {table} WHERE 1=0 INTERSECT SELECT {ra} FROM {table} WHERE 1=0"
        ),
        "CAST": computed.format(value=f"CAST({ra} AS INTEGER)"),
        "COALESCE": computed.format(value=f"COALESCE({ra}, 0.0)"),
        "IN_UNIT": computed.format(value=f"IN_UNIT({ra}, 'rad')"),
        # Ordered, an offset without one skipping arbitrary rows; and over TAP_SCHEMA, which
        # every service has and which is small enough that the ordering costs nothing.
        "OFFSET": "SELECT TOP 1 table_name FROM TAP_SCHEMA.tables ORDER BY table_name OFFSET 1",
    }


def declared_forms(tap, feature_type: str) -> list[str]:
    """The forms the service declares under one feature type, as `pyvo` reads them.

    Read in the check rather than in a fixture: a capabilities document that will not load
    is a finding about the service, and a fixture that raised would report it as the suite
    being broken instead.
    """
    forms = []
    for language in tap.get_tap_capability().languages:
        if str(language.name).upper() != "ADQL":
            continue
        for feature_list in language.languagefeaturelists:
            if str(feature_list.type).lower() == feature_type.lower():
                forms += [str(feature.form) for feature in feature_list.features]
    return forms


@pytest.mark.parametrize("feature", sorted(FEATURE_TYPES))
def test_declared_features_answer(
    tap, queryable, coordinates, center, feature, record_property
):
    """Every form of an optional feature the service declares answers a query.

    TAPRegExt §2.3: a language feature listed in the capabilities is one the service
    supports, and a client offers what it finds there. A declared form that is refused
    is a client told it may write something it may not. A feature left undeclared is
    skipped rather than failed, being optional.
    """
    forms = declared_forms(tap, FEATURE_TYPES[feature])
    if not forms:
        pytest.skip(f"the service declares no {feature} feature")
    ra, dec = coordinates
    queries = feature_queries(queryable, ra, dec, center)
    answered, refused, unknown = [], [], []
    for form in forms:
        # A form may be written as a signature — `BOX(...)` — and the name is what leads it.
        name = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)", form)
        query = queries.get(name.group(1).upper()) if name else None
        if query is None:
            unknown.append(form)
            continue
        try:
            tap.run_sync(query).to_table()
            answered.append(name.group(1).upper())
        except Exception as error:  # noqa: BLE001 — a refusal of any kind is the finding
            refused.append(f"{name.group(1).upper()}: {str(error).splitlines()[0][:120]}")
    detail = f"declared {len(forms)}, answered {', '.join(answered) or 'none'}"
    if unknown:
        detail += f"; no query here for {', '.join(unknown)}"
    record_property("detail", detail)
    assert not refused, f"declared and refused: {'; '.join(refused)}"
    assert answered, f"none of the declared forms is one this check knows: {forms}"


@pytest.mark.parametrize("function", ["ABS(-1.5)", "CEILING(1.2)", "FLOOR(1.8)", "SQRT(4.0)"])
def test_mandatory_functions(tap, queryable, function, record_property):
    """The numeric functions ADQL 2.1 requires of every service.

    The alias is not `value`: that is a reserved word, and three of the four services
    this suite is calibrated against refuse the query over it while a fourth accepts it.
    An alias is this check's own spelling rather than anything it is asking about, so it
    uses one no parser can object to.
    """
    found = tap.run_sync(f"SELECT TOP 1 {function} AS computed FROM {queryable}").to_table()
    record_property("detail", f"{function} = {found['computed'][0]}")
    assert len(found) == 1
