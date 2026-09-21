"""The catalogs this benchmark asks, the cone it asks for, and the columns it reads.

One cone, one arcsecond across, over four catalogs of deliberately different shape: a
wide flat detection table, two with a nested column read field by field, and one read
whole. What a query costs against these is the columns it projects far more than the rows
it returns, so the column lists are the benchmark as much as the cone is — widening one
measures something else.

Every source here is public and read anonymously.
"""

from __future__ import annotations

from dataclasses import dataclass

#: Where the `s3://` buckets below are. Applied to any `s3://` source, including one
#: given on the command line, so a bucket elsewhere needs its own region written in.
REGION = "us-east-1"

#: The centre of the cone, in degrees, and its radius in arcseconds. A position every one
#: of these catalogs holds something at, which is what makes the four answers comparable.
RA = 254.45755
DEC = 35.34236
RADIUS_ARCSEC = 1.0


@dataclass(frozen=True)
class Catalog:
    """Where a catalog is, and what to read out of it."""

    source: str
    columns: tuple[str, ...]


CATALOGS: dict[str, Catalog] = {
    "tess": Catalog(
        source="https://data.lsdb.io/hats/tess/tess_lightcurve",
        columns=(
            "_healpix_29",
            "ra_obj",
            "dec_obj",
            "ticid",
            # Three fields of the light curve rather than the whole of it: the answer is
            # one `lightcurve` column carrying those, which is the projection into a
            # struct that costs least and is read most.
            "lightcurve.time",
            "lightcurve.sap_flux",
            "lightcurve.sap_flux_err",
        ),
    ),
    "gaia": Catalog(
        source="https://data.lsdb.io/hats/gaia_dr3_epoch_phot",
        columns=(
            "_healpix_29",
            "source_id",
            "epoch_photometry.g_transit_time",
            "epoch_photometry.g_transit_flux",
            "epoch_photometry.g_transit_flux_over_error",
            "epoch_photometry.bp_obs_time",
            "epoch_photometry.bp_flux",
            "epoch_photometry.bp_flux_over_error",
            "epoch_photometry.rp_obs_time",
            "epoch_photometry.rp_flux",
            "epoch_photometry.rp_flux_over_error",
            "epoch_photometry.variability_flag_g_reject",
            "epoch_photometry.variability_flag_bp_reject",
            "epoch_photometry.variability_flag_rp_reject",
        ),
    ),
    "ztf": Catalog(
        source="s3://ipac-irsa-ztf/ztf/enhanced/dr24/lc/hats",
        # Its partitions are directories, so this is also the one catalog whose read
        # needs a listing before it opens a file.
        columns=("_healpix_29", "objectid", "objra", "objdec", "lightcurve"),
    ),
    "ps1": Catalog(
        source="s3://stpubdata/panstarrs/ps1/public/hats/detection",
        columns=(
            "_healpix_29",
            "objID",
            "ra",
            "dec",
            "obsTime",
            "psfFlux",
            "psfFluxErr",
            "filterID",
        ),
    ),
}
