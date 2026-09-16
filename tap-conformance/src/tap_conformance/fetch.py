#!/usr/bin/env python3
"""Download what the suite compares against, once.

The tables the service under test publishes are real HATS catalogs on S3 — this
downloads none of them. What it downloads is the *answers*: the same ADQL, put to
a TAP service that has been answering it correctly for years, saved as the
standard against which this service's answer is read.

One small catalog is built rather than downloaded, out of the same rows: a cone of
Gaia DR3, imported by `hats-import`. It is there because a protocol validator asks
for whole rows — `SELECT TOP 1 *` over a catalog 153 columns wide and a partition
of 300 MB is a slow way to find out whether a VOTable is well formed — and because
a suite that cannot run when S3 is having a bad morning is worth having. The
importer is the real one for the same reason the catalogs are: a layout written
here by hand would be this repository agreeing with itself.

The dependencies and their lock are `tap-conformance/python/pyproject.toml` and
`uv.lock` beside it, which is what makes them something Dependabot can raise a
pull request against. Building the sample needs the `fixtures` group, which is not
installed to run the checks:

    uv run --project tap-conformance/python --group fixtures \\
        tap-conformance/python/fetch.py --out tap-conformance/data
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
import sys
import warnings
from pathlib import Path

import numpy as np
import pyvo
from astropy.table import Table

# Bumped whenever the shape of what this writes changes, so a stale cached copy is
# refetched rather than read as the current one.
MANIFEST_VERSION = 4

# The reference service: the golden standard every answer here is read against.
# Chosen for answering this catalog correctly for a decade, not for holding it.
REFERENCE_SERVICE = "https://gea.esac.esa.int/tap-server/tap"
REFERENCE_SERVICE_NAME = "ESA Gaia Archive"
REFERENCE_TABLE = "gaiadr3.gaia_source"
ATTRIBUTION = (
    "Gaia DR3, ESA/Gaia/DPAC — https://www.cosmos.esa.int/web/gaia/dr3-acknowledgements"
)

# The sky the suite works over. A field at the equator, off the galactic plane, so a
# cone of a few thousand rows is a couple of seconds to download.
CENTER_RA = 45.0
CENTER_DEC = 0.0
# Degrees. What the sample catalog is built from.
SAMPLE_RADIUS = 0.5
# Every reference query stays inside the sample, which is what lets one query be put
# to the sample and to the whole catalog and be expected to give one answer.
QUERY_RADIUS = 0.3

# The sample's columns. Written out rather than `*`: the datatypes this suite is meant
# to exercise — a long integer key, doubles, a string, a boolean, and a column most
# rows have no value for — have to be chosen on purpose rather than hoped for.
SAMPLE_COLUMNS = [
    "source_id",
    "designation",
    "ra",
    "dec",
    "parallax",
    "pmra",
    "pmdec",
    "phot_g_mean_mag",
    "phot_bp_mean_mag",
    "phot_rp_mean_mag",
    "radial_velocity",
    "in_qso_candidates",
]

# A sync query is answered with the service's own default row limit unless asked
# otherwise, and that default is smaller than this download.
MAXREC = 100_000

# The tables the service under test is asked to publish.
#
# The first two are the same catalog at two sizes, which is what makes a disagreement
# readable: a query that agrees with the reference service on the sample and not on the
# whole catalog is a statement about the catalog, and one that fails on both is a
# statement about this service.
TABLES = [
    {
        "key": "gaia_sample",
        "name": "sample.gaia_dr3",
        "kind": "sample",
        "directory": "hats/gaia_sample",
        "ra_column": "ra",
        "dec_column": "dec",
        "id_column": "source_id",
        "compare": True,
        "note": "a cone of Gaia DR3, imported here from the reference service's answer",
    },
    {
        "key": "gaia_s3",
        "name": "gaia_dr3.gaia_source",
        "kind": "real",
        "url": "s3://stpubdata/gaia/gaia_dr3/public/hats/gaia",
        "ra_column": "ra",
        "dec_column": "dec",
        "id_column": "source_id",
        "compare": True,
        "note": "the whole of Gaia DR3 as HATS, public on AWS",
    },
    {
        "key": "ztf_dr24",
        "name": "ztf.dr24_object",
        "kind": "real",
        "url": "s3://irsa-fornax-testdata/ZTF/dr24/object",
        "ra_column": "ra",
        "dec_column": "dec",
        "id_column": "objectid",
        # No reference service answers this one: its light curves are a nested column,
        # which is a shape TAP has no reference implementation of. What it is published
        # for is what a client makes of the metadata of such a table.
        "compare": False,
        "nested": True,
        "note": "ZTF DR24 objects, whose light curves are a nested column",
    },
]


def cone(ra_column: str, dec_column: str, radius: float) -> str:
    """The ADQL both the reference service and the service under test are asked."""
    return (
        f"1=CONTAINS(POINT('ICRS', {ra_column}, {dec_column}), "
        f"CIRCLE('ICRS', {CENTER_RA}, {CENTER_DEC}, {radius}))"
    )


def reference_queries() -> list[dict]:
    """What is asked of the reference service, and later of the service here.

    One text, two table names: `{table}` is the only thing that differs between the
    two runs. Anything else differing would make a disagreement unreadable — it
    could be the query rather than the answer.

    `rtol` is what a value may differ by. Zero for anything a service copies out of
    its own storage; loose for an aggregate, where the order of summation is the
    implementation's business and not something either service promises.
    """
    inside = cone("ra", "dec", QUERY_RADIUS)
    return [
        {
            "id": "cone",
            "description": "a cone, a few columns, ordered by the key",
            "adql": (
                "SELECT source_id, ra, dec, phot_g_mean_mag FROM {table} "
                f"WHERE {inside} ORDER BY source_id"
            ),
            "rtol": 0.0,
        },
        {
            "id": "filter",
            "description": "a cone narrowed by a value, which prunes on a column",
            "adql": (
                "SELECT source_id, phot_g_mean_mag FROM {table} "
                f"WHERE {inside} AND phot_g_mean_mag < 18 ORDER BY source_id"
            ),
            "rtol": 0.0,
        },
        {
            "id": "count",
            "description": "COUNT(*) over a cone",
            "adql": f"SELECT COUNT(*) AS n FROM {{table}} WHERE {inside}",
            "rtol": 0.0,
        },
        {
            "id": "aggregate",
            "description": "aggregates over a cone",
            "adql": (
                "SELECT COUNT(*) AS n, MIN(phot_g_mean_mag) AS brightest, "
                "MAX(phot_g_mean_mag) AS faintest "
                f"FROM {{table}} WHERE {inside}"
            ),
            "rtol": 1e-9,
        },
        {
            "id": "expression",
            "description": "a computed column under an alias, which names a FIELD",
            "adql": (
                "SELECT TOP 20 source_id, phot_bp_mean_mag - phot_rp_mean_mag AS bp_rp "
                f"FROM {{table}} WHERE {inside} ORDER BY source_id"
            ),
            "rtol": 1e-12,
        },
        {
            "id": "distance",
            "description": "ADQL's DISTANCE, which every service computes itself",
            "adql": (
                "SELECT TOP 20 source_id, "
                "DISTANCE(POINT('ICRS', ra, dec), "
                f"POINT('ICRS', {CENTER_RA}, {CENTER_DEC})) AS sep "
                f"FROM {{table}} WHERE {inside} ORDER BY source_id"
            ),
            "rtol": 1e-9,
        },
        {
            "id": "nulls",
            "description": "a column most rows have no value for",
            "adql": (
                "SELECT TOP 30 source_id, radial_velocity FROM {table} "
                f"WHERE {inside} ORDER BY source_id"
            ),
            "rtol": 0.0,
        },
        {
            "id": "strings",
            "description": "a character column, and a boolean beside it",
            "adql": (
                "SELECT TOP 20 source_id, designation, in_qso_candidates FROM {table} "
                f"WHERE {inside} ORDER BY source_id"
            ),
            "rtol": 0.0,
        },
    ]


def run_query(service, adql: str, what: str) -> Table:
    """One sync query, with the row limit raised and truncation refused.

    A truncated download would become a sample with a hole in it, and a truncated
    reference answer would become a standard this service is then asked to fail.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("error", pyvo.dal.DALOverflowWarning)
        try:
            result = service.run_sync(adql, maxrec=MAXREC)
        except pyvo.dal.DALOverflowWarning as overflow:
            raise SystemExit(
                f"{what}: the reference service truncated its answer ({overflow}). "
                "Lower SAMPLE_RADIUS and run again."
            ) from overflow
    return result.to_table()


def arrow_table(table: Table):
    """The rows as arrow, keeping a null a null.

    Going through pandas would not: a masked integer widens to a float and a masked
    float arrives as NaN, which is a value this data holds elsewhere. Telling those
    two apart is half of what the suite then checks the service gets right.
    """
    import pyarrow as pa

    arrays = []
    for name in table.colnames:
        column = table[name]
        values = np.asarray(column)
        mask = None
        if getattr(column, "mask", None) is not None:
            mask = np.asarray(column.mask, dtype=bool)
            if not mask.any():
                mask = None
        if values.dtype.kind in "US":
            arrays.append(
                pa.array([str(value) for value in values.tolist()], type=pa.string(), mask=mask)
            )
        elif values.dtype.kind == "O":
            arrays.append(pa.array(values.tolist(), mask=mask))
        else:
            arrays.append(pa.array(values, mask=mask))
    return pa.Table.from_arrays(arrays, names=list(table.colnames))


def build_sample(out: Path, rows: Table) -> dict:
    """The downloaded rows as a HATS catalog, imported by `hats-import`.

    The importer is the one the catalogs this service reads are written with, so what
    comes out is a catalog rather than this repository's idea of one — which is the
    only version of it worth testing against.
    """
    import pyarrow.parquet as pq
    from hats_import.catalog.arguments import ImportArguments
    from hats_import.pipeline import pipeline

    root = out / "hats"
    target = root / "gaia_sample"
    if target.exists():
        shutil.rmtree(target)
    staging = out / "tmp"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    source = staging / "gaia_sample.parquet"
    pq.write_table(arrow_table(rows), source)

    pipeline(
        ImportArguments(
            output_path=root,
            output_artifact_name="gaia_sample",
            input_file_list=[source],
            file_reader="parquet",
            ra_column="ra",
            dec_column="dec",
            # Small enough that a few thousand rows land in several partitions, which
            # is what gives a query something to prune.
            pixel_threshold=500,
            highest_healpix_order=10,
            tmp_dir=staging,
            progress_bar=False,
            dask_n_workers=1,
            dask_threads_per_worker=1,
        )
    )
    shutil.rmtree(staging, ignore_errors=True)

    partitions = sorted(target.glob("dataset/**/Npix=*.parquet"))
    return {"rows": len(rows), "partitions": len(partitions)}


def fetch(out: Path) -> dict:
    service = pyvo.dal.TAPService(REFERENCE_SERVICE)
    reference = out / "reference"
    reference.mkdir(parents=True, exist_ok=True)

    print(f"downloading a sample from {REFERENCE_SERVICE_NAME}", flush=True)
    rows = run_query(
        service,
        f"SELECT {', '.join(SAMPLE_COLUMNS)} FROM {REFERENCE_TABLE} "
        f"WHERE {cone('ra', 'dec', SAMPLE_RADIUS)} ORDER BY source_id",
        "the sample download",
    )
    print(f"  {len(rows)} rows", flush=True)
    if len(rows) == 0:
        raise SystemExit("the reference service returned no rows")
    built = build_sample(out, rows)
    print(f"  imported {built['partitions']} partitions", flush=True)

    compared = [table["name"] for table in TABLES if table["compare"]]
    queries = []
    for query in reference_queries():
        print(f"  {query['id']}", flush=True)
        answer = run_query(
            service, query["adql"].format(table=REFERENCE_TABLE), query["id"]
        )
        path = reference / f"{query['id']}.vot"
        answer.write(path, format="votable", overwrite=True)
        queries.append(
            {
                **query,
                "tables": compared,
                "rows": len(answer),
                "reference": f"reference/{path.name}",
            }
        )

    (out / "queries.json").write_text(json.dumps(queries, indent=2) + "\n")
    manifest = {
        "version": MANIFEST_VERSION,
        "fetched_utc": dt.datetime.now(dt.UTC).isoformat(timespec="seconds"),
        "center": {"ra": CENTER_RA, "dec": CENTER_DEC},
        "sample_radius_deg": SAMPLE_RADIUS,
        "query_radius_deg": QUERY_RADIUS,
        "reference_service": REFERENCE_SERVICE,
        "reference_service_name": REFERENCE_SERVICE_NAME,
        "reference_table": REFERENCE_TABLE,
        "attribution": ATTRIBUTION,
        "tables": [{**table, **(built if table["key"] == "gaia_sample" else {})} for table in TABLES],
    }
    (out / "MANIFEST.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True, help="where the data goes")
    parser.add_argument(
        "--force",
        action="store_true",
        help="download again even where a current copy is already there",
    )
    args = parser.parse_args()

    manifest = args.out / "MANIFEST.json"
    if manifest.exists() and not args.force:
        current = json.loads(manifest.read_text())
        if current.get("version") == MANIFEST_VERSION:
            print(f"{manifest} is current; nothing to download")
            return 0
        print(f"{manifest} was written for an older layout; downloading again")

    args.out.mkdir(parents=True, exist_ok=True)
    written = fetch(args.out)
    print(f"wrote {len(written['tables'])} table descriptions to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
