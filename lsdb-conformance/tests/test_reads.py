"""What LSDB gets out of a catalog, served by this service and read off S3 directly.

Every question below is LSDB's own work — the cone search, the crossmatch and the
predicate are all computed in the client. The only thing that differs between the two
runs is where the parquet bytes came from, so what these compare is the file server.
"""

from __future__ import annotations

import lsdb
import pytest

from lsdb_conformance.compare import assert_same

#: Five of Gaia DR3's 152 columns, from the front, the middle and the end.
GAIA_COLUMNS = ["source_id", "ra", "dec", "phot_bp_mean_mag", "libname_gspphot"]
#: Five of Euclid Q1's seven.
EUCLID_COLUMNS = ["object_id", "ra", "dec", "mer_flux_h_templfit", "class_phz_classification"]

#: The default clock is set for a read of a few columns; these read whole partitions.
slow = pytest.mark.timeout(1800)


def test_head(both):
    def read(open_catalog):
        return open_catalog("sdss_dr7_spectra", columns=["OBJID", "RA", "DEC"]).head(5)

    assert_same(*both(read))


@slow
def test_first_partition_of_gaia(both):
    """A whole partition with no projection: 726k rows across every column."""

    def read(open_catalog):
        return open_catalog("gaia_dr3").partitions[0].compute()

    assert_same(*both(read))


@slow
def test_cone_search_on_gaia(both):
    """Ten arcminutes of sky, which LSDB reads as two partitions."""

    def read(open_catalog):
        return open_catalog("gaia_dr3", search_filter=lsdb.ConeSearch(10, 40, 600)).compute()

    assert_same(*both(read))


@slow
def test_last_partition_of_euclid(both):
    """The other end of the partition list, on a catalog of seven columns."""

    def read(open_catalog):
        return open_catalog("euclid_q1").partitions[-1].compute()

    assert_same(*both(read))


@slow
def test_crossmatch_gaia_with_euclid(both):
    """LSDB's crossmatch, over two catalogs read through the same route.

    One partition of the result: what varies is where the bytes of both catalogs came
    from, and Gaia is 2016 partitions deep.
    """

    def read(open_catalog):
        gaia = open_catalog("gaia_dr3", columns=GAIA_COLUMNS)
        euclid = open_catalog("euclid_q1", columns=EUCLID_COLUMNS)
        return gaia.crossmatch(euclid).partitions[0].compute()

    assert_same(*both(read))


@slow
def test_filtered_partition_of_gaia(both):
    """A projection and a row filter, which LSDB pushes into its own parquet reader."""

    def read(open_catalog):
        gaia = open_catalog("gaia_dr3", columns=["pm"], filters=[("pm", ">", 100)])
        return gaia.partitions[123].compute()

    assert_same(*both(read))
