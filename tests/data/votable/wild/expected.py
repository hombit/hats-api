# /// script
# requires-python = ">=3.11"
# dependencies = ["astropy", "numpy"]
# ///
"""Regenerate the ground truth beside every VOTable in this directory.

    uv run expected.py            # rewrite every <file>.vot.json
    uv run expected.py NAME.vot   # just the named ones

Each cell is decoded here from the document itself — the TD text, or the BINARY and
BINARY2 stream — under the encoding contract, and then compared cell by cell with what
astropy and STILTS (`stilts` on PATH) read from the same file. The contract wins; every
disagreement is written into the file's `notes`.

The contract, as this script applies it: null is an empty TD of any type, a BINARY2 null
flag, a scalar (string or number) equal to the VALUES null value, and a boolean of `?`,
space or NUL. Integers are exact; floats are numbers, with NaN and the infinities as the
strings "NaN", "Infinity" and "-Infinity", and a `float` column holds the value as a
32-bit float would. A string stops at its first NUL, and one of fixed width loses its
trailing spaces; a variable-width string keeps them. Anything with an arraysize other
than a 1-D string is a flat list in file order, a complex number is `[re, im]`, and a
2-D char array is a list of fixed-width strings. In BINARY, which has no null flag, an
empty variable-width string is `""` and a NaN is "NaN".

What was fetched from where, and what a reader must refuse, is in `SOURCES`, since
neither can be recovered from a file.
"""

import base64
import gzip
import json
import math
import struct
import subprocess
import sys
import warnings
import xml.etree.ElementTree as ET
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
FETCHED = "2026-09-29"

GAVO = "https://dc.g-vo.org/tap/sync?REQUEST=doQuery&LANG=ADQL&"
SIMBAD = "https://simbad.cds.unistra.fr/simbad/sim-tap/sync?REQUEST=doQuery&LANG=ADQL&"
ARI = "https://gaia.ari.uni-heidelberg.de/tap/sync?REQUEST=doQuery&LANG=ADQL&"
ESA = "https://gea.esac.esa.int/tap-server/tap/sync?REQUEST=doQuery&LANG=ADQL&"
NED = "https://ned.ipac.caltech.edu/tap/sync?REQUEST=doQuery&LANG=ADQL&"
IRSA = "https://irsa.ipac.caltech.edu/TAP/sync?REQUEST=doQuery&LANG=ADQL&"
TAPVIZIER = "http://tapvizier.cds.unistra.fr/TAPVizieR/tap/sync?REQUEST=doQuery&LANG=ADQL&"
ASTROPY = (
    "https://raw.githubusercontent.com/astropy/astropy/"
    "006f4960ab7311b83c36ca66d5df70274419e144/astropy/io/votable/tests/data/"
)
PYVO = (
    "https://raw.githubusercontent.com/astropy/pyvo/"
    "4909bc046f144258eb45b70fa9d4a23418bc33e6/pyvo/dal/tests/data/"
)

Q_SIMBAD = (
    "QUERY=SELECT+TOP+12+oid%2c+main_id%2c+ra%2c+dec%2c+coo_err_maj%2c+coo_qual%2c+plx_value"
    "%2c+rvz_redshift%2c+rvz_type%2c+otype%2c+nbref%2c+update_date%2c+coo_bibcode+FROM+basic"
    "+WHERE+ra+BETWEEN+10+AND+10.2+AND+dec+BETWEEN+41+AND+41.5+ORDER+BY+oid"
)
Q_SDSS = (
    "QUERY=SELECT+TOP+10+obj_id%2c+ra%2c+dec%2c+u%2c+err_u%2c+mode%2c+class%2c+photo_flags"
    "%2c+psfmags%2c+flags%2c+types%2c+field_mjds%2c+spec_z%2c+spec_class%2c+sdss_id+FROM"
    "+sdssdr16.main+WHERE+1%3dCONTAINS%28POINT%28ra%2c+dec%29%2c+CIRCLE%28180%2c+0.5%2c+0.03%29%29"
)
Q_RAVE = (
    "QUERY=SELECT+TOP+12+raveid%2c+raj2000%2c+dej2000%2c+rv%2c+plate_number%2c+al_n%2c+id_ppmxl"
    "%2c+id_tycho2%2c+rep_flag%2c+cluster_flag%2c+teff_k+FROM+rave.main"
)
Q_CARMENES = (
    "QUERY=SELECT+TOP+5+ssa_pubdid%2c+ssa_cdate%2c+ssa_targname%2c+ssa_targetpos%2c+ssa_region"
    "%2c+ssa_creator%2c+ssa_redshift%2c+ssa_length%2c+ssa_dateobs%2c+accsize+FROM+carmenes.data"
)
Q_CALIFA = (
    "QUERY=SELECT+TOP+6+ssa_pubdid%2c+ssa_cdate%2c+ssa_pdate%2c+ssa_targname%2c+ssa_targetpos"
    "%2c+ssa_location%2c+ssa_snr%2c+ssa_length+FROM+califadr3.spectra"
)
Q_ROLES = (
    "QUERY=SELECT+TOP+8+ivoid%2c+role_name%2c+base_role%2c+role_ivoid+FROM+rr.res_role+WHERE"
    "+role_name+LIKE+%27%25%c3%b6%25%27+OR+role_name+LIKE+%27%25%c3%a9%25%27"
)
Q_GAIA_GAVO = (
    "QUERY=SELECT+TOP+5+source_id%2c+ra%2c+dec%2c+pmra%2c+pmdec%2c+parallax%2c+phot_g_mean_mag"
    "%2c+radial_velocity+FROM+gaia.dr3lite+WHERE+1%3dCONTAINS%28POINT%28ra%2c+dec%29%2c+CIRCLE"
    "%2856.75%2c+24.12%2c+0.05%29%29"
)
Q_ESA = (
    "QUERY=SELECT+TOP+10+source_id%2c+designation%2c+ra%2c+dec%2c+parallax%2c+pmra%2c+ruwe%2c"
    "+phot_g_mean_mag%2c+radial_velocity%2c+phot_variable_flag%2c+has_xp_continuous%2c"
    "+in_qso_candidates%2c+teff_gspphot%2c+libname_gspphot%2c+ref_epoch+FROM+gaiadr3.gaia_source"
    "+WHERE+1%3dCONTAINS%28POINT%28ra%2c+dec%29%2c+CIRCLE%2856.75%2c+24.12%2c+0.05%29%29"
)
Q_ARI = (
    "QUERY=SELECT+TOP+8+source_id%2c+designation%2c+ra%2c+dec%2c+parallax%2c+ruwe%2c"
    "+phot_g_mean_mag%2c+radial_velocity%2c+phot_variable_flag%2c+has_xp_continuous%2c"
    "+in_qso_candidates%2c+libname_gspphot+FROM+gaiadr3.gaia_source+WHERE+1%3dCONTAINS%28POINT"
    "%28%27ICRS%27%2c+ra%2c+dec%29%2c+CIRCLE%28%27ICRS%27%2c+56.75%2c+24.12%2c+0.05%29%29"
)
Q_NED = (
    "QUERY=SELECT+TOP+10+objid%2c+prefname%2c+prefphytype%2c+ra%2c+dec%2c+uncmaja%2c+zflag%2c+z"
    "%2c+zunc%2c+n_crosref%2c+n_notes+FROM+NEDTAP.objdir+WHERE+CONTAINS%28POINT%28%27J2000%27%2c"
    "+ra%2c+dec%29%2c+CIRCLE%28%27J2000%27%2c+10.68%2c+41.27%2c+0.05%29%29%3d1"
)
Q_IRSA = (
    "QUERY=SELECT+TOP+10+cntr%2c+designation%2c+ra%2c+dec%2c+w1mpro%2c+w1sigmpro%2c+w4mpro%2c"
    "+w4sigmpro%2c+cc_flags%2c+ext_flg%2c+nb%2c+na%2c+ph_qual%2c+w1mjdmean%2c+pmra+FROM"
    "+allwise_p3as_psd+WHERE+CONTAINS%28POINT%28%27ICRS%27%2c+ra%2c+dec%29%2c+CIRCLE%28%27ICRS"
    "%27%2c+10.68%2c+41.27%2c+0.02%29%29%3d1"
)
Q_HIPPARCOS = (
    "QUERY=SELECT+TOP+10+HIP%2c+RAhms%2c+DEdms%2c+Vmag%2c+VarFlag%2c+RAICRS%2c+DEICRS%2c+Plx"
    "%2c+pmRA%2c+SpType+FROM+%22I%2f239%2fhip_main%22+ORDER+BY+HIP"
)
VIZIER_GAIA = (
    "?-source=I/355/gaiadr3&-out.max=10&-out=Source,RA_ICRS,DE_ICRS,Plx,pmRA,RUWE,Gmag,RV,"
    "VarFlag,NSS,PQSO,Teff,SolID&-c=56.75+24.12&-c.rm=3"
)

TAPLIB_FORMAT = (
    "TAPLibrary ignores RESPONSEFORMAT and answers BINARY whatever it names; "
    "this one was fetched with the TAP 1.0 FORMAT parameter."
)
TAPLIB = "TAPLibrary (VOLLT, Java)"
DACHS = "GAVO DaCHS 2.12.2 (Python), per INFO server_software"
QUERY_ERROR = "QUERY_STATUS is ERROR"
VALUES_NULL_STRING = (
    "Char columns here declare VALUES null='' and VOTable 1.5 §5.5 has the null value "
    "respected for every type, so their empty strings are null; astropy ignores VALUES null "
    "on strings."
)

SOURCES = {
    # --- CDS SIMBAD: TAPLibrary -------------------------------------------------------
    "simbad-binary-basic.vot": dict(
        producer=f"SIMBAD TAP, {TAPLIB}; INFO PROVIDER 'CDS'",
        source_url=SIMBAD + "RESPONSEFORMAT=votable&" + Q_SIMBAD,
        notes="Default votable format is BINARY.",
    ),
    "simbad-tabledata-basic.vot": dict(
        producer=f"SIMBAD TAP, {TAPLIB}; INFO PROVIDER 'CDS'",
        source_url=SIMBAD + "FORMAT=votable%2ftd&" + Q_SIMBAD,
        notes=TAPLIB_FORMAT,
    ),
    "simbad-binary2-basic.vot": dict(
        producer=f"SIMBAD TAP, {TAPLIB}; INFO PROVIDER 'CDS'",
        source_url=SIMBAD + "FORMAT=votable%2fb2&" + Q_SIMBAD,
        notes=TAPLIB_FORMAT,
    ),
    "simbad-fits-basic.vot": dict(
        producer=f"SIMBAD TAP, {TAPLIB}; INFO PROVIDER 'CDS'",
        source_url=SIMBAD
        + "FORMAT=votable%2ffits&QUERY=SELECT+TOP+5+oid%2c+main_id%2c+ra%2c+dec+FROM+basic"
        "+WHERE+ra+BETWEEN+10+AND+10.2+AND+dec+BETWEEN+41+AND+41.5+ORDER+BY+oid",
        refuse="FITS serialization: DATA holds a base64 FITS file in STREAM, not a VOTable encoding",
        notes=TAPLIB_FORMAT,
    ),
    "simbad-error-unknown-column.vot": dict(
        producer=f"SIMBAD TAP, {TAPLIB}",
        source_url=SIMBAD
        + "FORMAT=votable&QUERY=SELECT+TOP+3+no_such_column+FROM+basic",
        refuse=QUERY_ERROR,
        notes="HTTP 400. The INFO USER value was the requester's address and is replaced by "
        "192.0.2.1 (TEST-NET-1).",
    ),
    # --- CDS TAPVizieR: TAPLibrary ------------------------------------------------------
    "tapvizier-tabledata-hipparcos.vot": dict(
        producer="TAPVizieR, per INFO server_software 'TAPVizieR-Vollt/1.1.3'",
        source_url=TAPVIZIER + "FORMAT=votable%2ftd&" + Q_HIPPARCOS,
        notes=TAPLIB_FORMAT,
    ),
    "tapvizier-binary-hipparcos.vot": dict(
        producer="TAPVizieR, per INFO server_software 'TAPVizieR-Vollt/1.1.3'",
        source_url=TAPVIZIER + "FORMAT=votable%2fb&" + Q_HIPPARCOS,
        notes=TAPLIB_FORMAT,
    ),
    "tapvizier-binary2-hipparcos.vot": dict(
        producer="TAPVizieR, per INFO server_software 'TAPVizieR-Vollt/1.1.3'",
        source_url=TAPVIZIER + "FORMAT=votable%2fb2&" + Q_HIPPARCOS,
        notes=TAPLIB_FORMAT,
    ),
    "tapvizier-error-column.vot": dict(
        producer="TAPVizieR, per INFO PROVIDER 'CDS'",
        source_url=TAPVIZIER
        + "FORMAT=votable&QUERY=SELECT+TOP+10+no_such_column+FROM+%22I%2f239%2fhip_main%22",
        refuse=QUERY_ERROR,
        notes="HTTP 400. The INFO USER value was the requester's address and is replaced by "
        "192.0.2.1 (TEST-NET-1).",
    ),
    # --- GAVO Data Center: DaCHS ------------------------------------------------------
    "gavo-tabledata-obscore.vot": dict(
        producer=DACHS,
        source_url=GAVO
        + "RESPONSEFORMAT=votable%2ftd&QUERY=SELECT+TOP+10+obs_publisher_did%2c+obs_collection"
        "%2c+dataproduct_type%2c+calib_level%2c+t_min%2c+t_max%2c+s_ra%2c+s_dec%2c+s_fov%2c"
        "+s_region%2c+em_min%2c+access_estsize%2c+t_xel%2c+pol_states+FROM+ivoa.obscore+WHERE"
        "+obs_collection%3d%27HDAP%27+ORDER+BY+obs_publisher_did",
    ),
    "gavo-binary-sdss-arrays.vot": dict(
        producer=DACHS, source_url=GAVO + "RESPONSEFORMAT=votable&" + Q_SDSS
    ),
    "gavo-tabledata-sdss-arrays.vot": dict(
        producer=DACHS, source_url=GAVO + "RESPONSEFORMAT=votable%2ftd&" + Q_SDSS
    ),
    "gavo-binary2-sdss-arrays.vot": dict(
        producer=DACHS, source_url=GAVO + "RESPONSEFORMAT=votable%2fb2&" + Q_SDSS
    ),
    "gavo-tabledata-rave-boolean.vot": dict(
        producer=DACHS,
        source_url=GAVO + "RESPONSEFORMAT=votable%2ftd&" + Q_RAVE,
        notes="Booleans are written 0 and 1 in TABLEDATA.",
    ),
    "gavo-binary2-rave-boolean.vot": dict(
        producer=DACHS, source_url=GAVO + "RESPONSEFORMAT=votable%2fb2&" + Q_RAVE
    ),
    "gavo-tabledata-carmenes-geometry.vot": dict(
        producer=DACHS,
        source_url=GAVO + "RESPONSEFORMAT=votable%2ftd&" + Q_CARMENES,
        notes=VALUES_NULL_STRING,
    ),
    "gavo-binary-carmenes-geometry.vot": dict(
        producer=DACHS,
        source_url=GAVO + "RESPONSEFORMAT=votable&" + Q_CARMENES,
        notes=VALUES_NULL_STRING
        + " The null point is two NaNs, which BINARY cannot mark.",
    ),
    "gavo-tabledata-califa-point-timestamp.vot": dict(
        producer=DACHS,
        source_url=GAVO + "RESPONSEFORMAT=votable%2ftd&" + Q_CALIFA,
        notes=VALUES_NULL_STRING,
    ),
    "gavo-binary2-califa-point-timestamp.vot": dict(
        producer=DACHS,
        source_url=GAVO + "RESPONSEFORMAT=votable%2fb2&" + Q_CALIFA,
        notes=VALUES_NULL_STRING,
    ),
    "gavo-tabledata-unicode-names.vot": dict(
        producer=DACHS, source_url=GAVO + "RESPONSEFORMAT=votable%2ftd&" + Q_ROLES
    ),
    "gavo-binary-unicode-names.vot": dict(
        producer=DACHS, source_url=GAVO + "RESPONSEFORMAT=votable&" + Q_ROLES
    ),
    "gavo-tabledata-mivot-gaia.vot": dict(
        producer=DACHS,
        source_url=GAVO + "RESPONSEFORMAT=vodml&" + Q_GAIA_GAVO,
        notes="RESPONSEFORMAT=vodml: VOTable 1.6 media type with a MIVOT (mivot:VODML) "
        "annotation block in a RESOURCE type=meta.",
    ),
    "gavo-error-timeout.vot": dict(
        producer=DACHS,
        source_url=GAVO
        + "RESPONSEFORMAT=votable&QUERY=SELECT+TOP+10+obj_id%2c+ra%2c+dec%2c+u%2c+err_u%2c+mode"
        "%2c+class%2c+photo_flags%2c+psfmags%2c+flags%2c+types%2c+field_mjds%2c+spec_z%2c"
        "+spec_class%2c+sdss_id+FROM+sdssdr16.main+WHERE+ra+BETWEEN+180+AND+180.01+ORDER+BY+obj_id",
        refuse=QUERY_ERROR,
        notes="HTTP 500 with a query timeout. Starts with an xml-stylesheet processing "
        "instruction and has no XML declaration.",
    ),
    "gavo-binary-scs-arihip.vot": dict(
        producer=DACHS + ", Simple Cone Search",
        source_url="https://dc.g-vo.org/arihip/q/cone/scs.xml?RA=10.68&DEC=41.27&SR=1.5&VERB=1",
    ),
    "gavo-binary-sia1-bgds.vot": dict(
        producer=DACHS + ", SIAP 1",
        source_url="https://dc.g-vo.org/bgds/q/sia/siap.xml?POS=10.68,41.27&SIZE=1&MAXREC=4",
    ),
    "gavo-binary-siav2-sitewide.vot": dict(
        producer=DACHS + ", SIAP 2",
        source_url="https://dc.g-vo.org/__system__/siap2/sitewide/siap2.xml"
        "?POS=CIRCLE%2010.68%2041.27%200.5&MAXREC=4",
        notes="QUERY_STATUS is OVERFLOW (MAXREC reached): a truncated answer, not an error.",
    ),
    "gavo-tabledata-ssa-califa.vot": dict(
        producer=DACHS + ", SSAP",
        source_url="https://dc.g-vo.org/califa/q3/s/ssap.xml?REQUEST=queryData"
        "&POS=352.636,0.088&SIZE=0.1&MAXREC=3",
        notes="QUERY_STATUS is OVERFLOW (MAXREC reached). An SSA answer is TABLEDATA where "
        "DaCHS's TAP default is BINARY. " + VALUES_NULL_STRING,
    ),
    "gavo-tabledata-datalink-califa.vot": dict(
        producer=DACHS + ", DataLink links response",
        source_url="https://dc.g-vo.org/califa/q3/sdl/dlmeta"
        "?ID=ivo%3A%2F%2Forg.gavo.dc%2F~%3Fcalifa%2Fdatadr3%2FCOMB%2FNGC7684.COMB-21-01",
        notes="Served as application/x-votable+xml;content=datalink.",
    ),
    # --- ARI Gaia: TAPLibrary ---------------------------------------------------------
    "arigaia-binary-gaiadr3.vot": dict(
        producer=f"ARI Gaia TAP, {TAPLIB}; INFO PROVIDER 'ARI'",
        source_url=ARI + "RESPONSEFORMAT=votable&" + Q_ARI,
        notes="Gaia booleans are published as short.",
    ),
    "arigaia-tabledata-gaiadr3.vot": dict(
        producer=f"ARI Gaia TAP, {TAPLIB}; INFO PROVIDER 'ARI'",
        source_url=ARI + "FORMAT=votable%2ftd&" + Q_ARI,
        notes=TAPLIB_FORMAT,
    ),
    "arigaia-binary2-gaiadr3.vot": dict(
        producer=f"ARI Gaia TAP, {TAPLIB}; INFO PROVIDER 'ARI'",
        source_url=ARI + "FORMAT=votable%2fb2&" + Q_ARI,
        notes=TAPLIB_FORMAT,
    ),
    "arigaia-error-adql-syntax.vot": dict(
        producer=f"ARI Gaia TAP, {TAPLIB}",
        source_url=ARI
        + "RESPONSEFORMAT=votable&"
        + Q_ARI.replace("%27ICRS%27%2c+", ""),
        refuse=QUERY_ERROR,
        notes="HTTP 400: the ADQL 2.0 parser wants a coordinate system as POINT's first argument.",
    ),
    # --- ESA Gaia archive -------------------------------------------------------------
    "esagaia-binary2-gaiadr3.vot": dict(
        producer="ESA Gaia Archive TAP+ (ESAC, Java)",
        source_url=ESA + "RESPONSEFORMAT=votable&" + Q_ESA,
        notes="The format called votable is BINARY2.",
    ),
    "esagaia-tabledata-gaiadr3.vot": dict(
        producer="ESA Gaia Archive TAP+ (ESAC, Java)",
        source_url=ESA + "RESPONSEFORMAT=votable_plain&" + Q_ESA,
    ),
    "esagaia-gzip-gaiadr3.vot": dict(
        producer="ESA Gaia Archive TAP+ (ESAC, Java)",
        source_url=ESA
        + "RESPONSEFORMAT=votable_gzip&QUERY=SELECT+TOP+3+source_id%2c+ra+FROM+gaiadr3.gaia_source"
        "+WHERE+1%3dCONTAINS%28POINT%28ra%2c+dec%29%2c+CIRCLE%2856.75%2c+24.12%2c+0.02%29%29",
        refuse="not XML: a gzip file served as application/x-votable+xml with no Content-Encoding",
        notes="Rows are those of the document inside the gzip (BINARY2), for a reader that "
        "decompresses rather than refuses.",
    ),
    "esagaia-error-unknown-table.vot": dict(
        producer="ESA Gaia Archive TAP+ (ESAC, Java)",
        source_url=ESA
        + "RESPONSEFORMAT=votable&QUERY=SELECT+TOP+3+%2a+FROM+gaiadr3.no_such_table",
        refuse=QUERY_ERROR,
        notes="HTTP 400. VOTable 1.2 namespace and no XML declaration.",
    ),
    # --- NED: TAPLibrary --------------------------------------------------------------
    "ned-binary-objdir.vot": dict(
        producer=f"NED TAP, {TAPLIB}; INFO PROVIDER 'NASA/IPAC Extragalatic Database (NED)'",
        source_url=NED + "FORMAT=votable&" + Q_NED,
    ),
    "ned-tabledata-objdir.vot": dict(
        producer=f"NED TAP, {TAPLIB}; INFO PROVIDER 'NASA/IPAC Extragalatic Database (NED)'",
        source_url=NED + "FORMAT=votable%2ftd&" + Q_NED,
        notes=TAPLIB_FORMAT,
    ),
    "ned-binary2-objdir.vot": dict(
        producer=f"NED TAP, {TAPLIB}; INFO PROVIDER 'NASA/IPAC Extragalatic Database (NED)'",
        source_url=NED + "FORMAT=votable%2fb2&" + Q_NED,
        notes=TAPLIB_FORMAT,
    ),
    # --- IRSA -------------------------------------------------------------------------
    "irsa-tabledata-allwise.vot": dict(
        producer="IRSA TAP (IPAC, Java)",
        source_url=IRSA + "RESPONSEFORMAT=votable&" + Q_IRSA,
        notes="FIELDs carry a non-standard irsa_format attribute.",
    ),
    "irsa-binary2-allwise.vot": dict(
        producer="IRSA TAP (IPAC, Java)",
        source_url=IRSA + "RESPONSEFORMAT=votable%2fb2&" + Q_IRSA,
        notes="FIELDs carry a non-standard irsa_format attribute.",
    ),
    # --- HEASARC, MAST, Data Lab ------------------------------------------------------
    "heasarc-binary-xmmmaster.vot": dict(
        producer="HEASARC Xamin TAP (Java)",
        source_url="https://heasarc.gsfc.nasa.gov/xamin/vo/tap/sync?REQUEST=doQuery&LANG=ADQL"
        "&RESPONSEFORMAT=votable&QUERY=SELECT+TOP+10+obsid%2c+name%2c+ra%2c+dec%2c+time%2c"
        "+end_time%2c+duration%2c+pi_lname%2c+status%2c+data_in_heasarc%2c+public_date+FROM"
        "+xmmmaster+WHERE+CONTAINS%28POINT%28%27ICRS%27%2c+ra%2c+dec%29%2c+CIRCLE%28%27ICRS%27"
        "%2c+83.63%2c+22.01%2c+0.5%29%29%3d1",
        notes="The capabilities declare one output format, output-votable-td as text/xml; "
        "what comes back under RESPONSEFORMAT=votable is BINARY.",
    ),
    "mast-tabledata-obscore.vot": dict(
        producer="MAST vo-tap, per its comment 'MAST VOTable encoder version 1.0'",
        source_url="https://mast.stsci.edu/vo-tap/api/v0.1/caom/sync?REQUEST=doQuery&LANG=ADQL"
        "&RESPONSEFORMAT=votable&QUERY=SELECT+TOP+10+obs_id%2c+obs_collection%2c"
        "+dataproduct_type%2c+calib_level%2c+t_min%2c+t_exptime%2c+s_ra%2c+s_dec%2c+s_region%2c"
        "+em_min%2c+target_name%2c+instrument_name%2c+access_estsize+FROM+ivoa.obscore+WHERE"
        "+CONTAINS%28POINT%28%27ICRS%27%2c+s_ra%2c+s_dec%29%2c+CIRCLE%28%27ICRS%27%2c+83.63%2c"
        "+22.01%2c+0.02%29%29%3d1",
        notes="unit='n.n' on dimensionless columns; a comment precedes the root element.",
    ),
    "datalab-tabledata-desdr2.vot": dict(
        producer="NOIRLab Astro Data Lab, 'DALServer TAP Query' (Java)",
        source_url="https://datalab.noirlab.edu/tap/sync?REQUEST=doQuery&LANG=ADQL"
        "&RESPONSEFORMAT=votable&QUERY=SELECT+TOP+10+coadd_object_id%2c+ra%2c+dec%2c+mag_auto_g"
        "%2c+magerr_auto_g%2c+flags_g%2c+extended_class_coadd%2c+tilename+FROM+des_dr2.main"
        "+WHERE+ra+BETWEEN+10.0+AND+10.02+AND+dec+BETWEEN+-40.02+AND+-40.0",
        notes="VOTable 1.2 in no namespace at all, with xsi:noNamespaceSchemaLocation set to "
        "'xmlns:http://...'. BINARY2 is not offered: asking for it returns TABLEDATA.",
    ),
    "datalab-error-point.vot": dict(
        producer="NOIRLab Astro Data Lab, 'DALServer TAP Query' (Java)",
        source_url="https://datalab.noirlab.edu/tap/sync?REQUEST=doQuery&LANG=ADQL"
        "&RESPONSEFORMAT=votable&QUERY=SELECT+TOP+10+coadd_object_id%2c+ra%2c+dec%2c+mag_auto_g"
        "%2c+magerr_auto_g%2c+flags_g%2c+extended_class_coadd%2c+tilename+FROM+des_dr2.main"
        "+WHERE+CONTAINS%28POINT%28%27ICRS%27%2c+ra%2c+dec%29%2c+CIRCLE%28%27ICRS%27%2c+10.0%2c"
        "+-40.0%2c+0.02%29%29%3d1",
        refuse=QUERY_ERROR,
        notes="HTTP 200 for an error. CRLF line endings, VOTable 1.2 namespace; the message is "
        "a PostgreSQL exception.",
    ),
    # --- VizieR, classic ASU interface --------------------------------------------------
    "vizier-tabledata-hipparcos.vot": dict(
        producer="VizieR 7.6, per INFO server_software",
        source_url="https://vizier.cds.unistra.fr/viz-bin/votable?-source=I/239/hip_main"
        "&-out.max=10&-out=HIP,RAICRS,DEICRS,Vmag,Plx,e_Plx,B-V,SpType,CCDM,Nsys"
        "&-c=10.68+41.27&-c.rd=5",
        notes="QUERY_STATUS OVERFLOW after the TABLE; numbers zero-padded and signed ("
        "'004.18103104', '+41.84943398'); VALUES null='NaN' on floats; a FIELD with "
        "type='hidden'; arraysize='12*'.",
    ),
    "vizier-tabledata-gaiadr3.vot": dict(
        producer="VizieR 7.6, per INFO server_software",
        source_url="https://vizier.cds.unistra.fr/viz-bin/votable/-b" + VIZIER_GAIA,
        notes="Asked for as votable/-b, which answers TABLEDATA.",
    ),
    "vizier-binary-gaiadr3.vot": dict(
        producer="VizieR 7.6, per INFO server_software",
        source_url="https://vizier.cds.unistra.fr/viz-bin/votable"
        + VIZIER_GAIA
        + "&-out.form=bin64",
        notes="VizieR offers no BINARY2 here. Its double null is NaN with every bit set "
        "(0xFFFFFFFFFFFFFFFF), under VALUES null='NaN'.",
    ),
    "vizier-malformed-gaiadr3.vot": dict(
        producer="VizieR 7.6, per INFO server_software",
        source_url="https://vizier.cds.unistra.fr/viz-bin/votable/-b2" + VIZIER_GAIA,
        refuse="not well-formed XML: the DATA is '<BINARY><STREAM</STREAM></BINARY>'",
        notes="What VizieR answered for votable/-b2, HTTP 200. The same query as "
        "vizier-binary-gaiadr3.vot, which has ten rows.",
    ),
    "vizier-tabledata-2mass.vot": dict(
        producer="VizieR 7.6, per INFO server_software",
        source_url="https://vizier.cds.unistra.fr/viz-bin/votable?-source=II/246/out"
        "&-out.max=8&-out=2MASS,RAJ2000,DEJ2000,Jmag,e_Jmag,Kmag,Qflg,Rflg,Cflg,Xflg,Aflg,JD"
        "&-c=83.63+22.01&-c.rm=2&-out.form=DTD",
        notes="-out.form=DTD made no difference to the document.",
    ),
    # --- static files from other projects' test data ----------------------------------
    "astropytests-tabledata-gemini.vot": dict(
        producer="Gemini Observatory Archive (as captured in astropy's test data)",
        source_url=ASTROPY + "gemini.xml",
    ),
    "astropytests-tabledata-irsa-nph-m31.vot": dict(
        producer="IRSA nph cone search, VOTable 'v1.0' (as captured in astropy's test data)",
        source_url=ASTROPY + "irsa-nph-m31.xml",
        td_nulls=("null", "-"),
        refuse="numeric TDs hold the words 'null' and '-', which are not numbers",
        notes="version='v1.0', no namespace, a DOCTYPE naming the us-vo.org DTD. Rows read "
        "'null' and '-' in a numeric TD as null, for a reader that is lenient rather than "
        "refusing.",
    ),
    "astropytests-error-irsa-nph.vot": dict(
        producer="IRSA nph cone search (as captured in astropy's test data)",
        source_url=ASTROPY + "irsa-nph-error.xml",
        refuse="no RESOURCE and no TABLE; an INFO named ERROR, not a QUERY_STATUS",
    ),
    "astropytests-binary-vizier.vot": dict(
        producer="VizieR (as captured in astropy's test data as vizier_b2_votable.xml)",
        source_url=ASTROPY + "vizier_b2_votable.xml",
        notes="Named for BINARY2 upstream, but the DATA is BINARY.",
    ),
    "astropytests-binary2-masked-strings.vot": dict(
        producer="astropy test data",
        source_url=ASTROPY + "binary2_masked_strings.xml",
    ),
    "astropytests-tabledata-mivot.vot": dict(
        producer="astropy test data, a MIVOT-annotated table",
        source_url=ASTROPY + "mivot_annotated_table.xml",
    ),
    "pyvotests-tabledata-datalink.vot": dict(
        producer="pyvo test data, a DataLink links response",
        source_url=PYVO + "datalink/datalink.xml",
    ),
    "pyvotests-tabledata-proc.vot": dict(
        producer="pyvo test data, a DataLink response with a processing service descriptor",
        source_url=PYVO + "datalink/proc.xml",
    ),
    "pyvotests-tabledata-obscore-image.vot": dict(
        producer="pyvo test data, an ObsCore answer",
        source_url=PYVO + "tap/obscore-image.xml",
    ),
    "pyvotests-error-status-with-table.vot": dict(
        producer="pyvo test data, written by astropy.io.votable 5.0.1",
        source_url=PYVO + "query/errorstatus.xml",
        refuse=QUERY_ERROR,
        notes="The error document carries a TABLE with rows; its QUERY_STATUS still says ERROR.",
    ),
}

# ------------------------------------------------------------------------------------
# The document


def local(tag):
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else ""


def child(elem, name):
    return next((c for c in elem if local(c.tag) == name), None)


def document_bytes(path):
    data = path.read_bytes()
    if data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    return data


def query_status(root):
    for e in root.iter():
        if local(e.tag) == "INFO" and e.get("name") == "QUERY_STATUS":
            return e.get("value")
    return None


def first_table(root):
    return next((e for e in root.iter() if local(e.tag) == "TABLE"), None)


class Field:
    def __init__(self, elem):
        self.name = elem.get("name")
        self.datatype = elem.get("datatype")
        self.arraysize = elem.get("arraysize")
        values = child(elem, "VALUES")
        self.null = values.get("null") if values is not None else None
        desc = child(elem, "DESCRIPTION")
        self.meta = {
            "name": self.name,
            "datatype": self.datatype,
            "arraysize": self.arraysize,
            "xtype": elem.get("xtype"),
            "unit": elem.get("unit"),
            "ucd": elem.get("ucd"),
            "utype": elem.get("utype"),
            "description": "".join(desc.itertext()).strip()
            if desc is not None
            else None,
            "null": self.null,
        }
        dims = [] if self.arraysize is None else self.arraysize.split("x")
        self.variable = bool(dims) and dims[-1].endswith("*")
        self.fixed_dims = [int(d.rstrip("*")) for d in dims if d.rstrip("*")]
        if self.variable and len(dims) > 1:
            raise NotImplementedError(
                f"{self.name}: multidimensional variable arraysize"
            )

    @property
    def is_string(self):
        return self.datatype in ("char", "unicodeChar")

    @property
    def is_array(self):
        # A 1-D string is one value; a 2-D char array is a list of strings.
        if self.arraysize is None:
            return False
        return not (self.is_string and len(self.arraysize.split("x")) == 1)

    @property
    def is_float(self):
        return self.datatype in ("float", "double", "floatComplex", "doubleComplex")

    def magic(self):
        if self.null is None:
            return None
        if self.is_float:
            return float(self.null)
        if self.datatype in ("unsignedByte", "short", "int", "long"):
            return parse_int(self.null)
        return None


def parse_int(text):
    t = text.strip()
    body = t.lstrip("+-")
    if body[:2].lower() == "0x":
        return int(t[: len(t) - len(body)] + body[2:], 16)
    return int(t)


def is_magic(field, v):
    m = field.magic()
    if m is None or v is None or isinstance(v, str):
        return False
    if isinstance(m, float) and math.isnan(m):
        return isinstance(v, float) and math.isnan(v)
    return v == m


def f32(x):
    return float(np.float32(x))


def number(field, text):
    if field.datatype in ("float", "floatComplex"):
        return f32(float(text))
    if field.datatype in ("double", "doubleComplex"):
        return float(text)
    return parse_int(text)


# Words a file writes in a numeric TD to mean null, which no VOTable version allows.
# Set per file from SOURCES' `td_nulls`; a file that needs them is one to refuse.
LENIENT_NULLS = frozenset()

TRUE = {"t", "1", "true"}
FALSE = {"f", "0", "false"}


def boolean(text):
    t = text.strip().lower()
    if t in TRUE:
        return True
    if t in FALSE:
        return False
    if t in ("?", "", "\0"):
        return None
    raise ValueError(f"not a boolean: {text!r}")


def string(field, text):
    """A 1-D char or unicodeChar value, and null where it is the VALUES null — which
    VOTable 1.5 §5.5 has respected for every type, strings included."""
    text = text.split("\0", 1)[0]
    if field.arraysize is not None and not field.variable:
        # A TD may hold more than the declared width; the width is what the column is.
        text = text[: field.fixed_dims[0]].rstrip(" ")
    if field.null is not None and text == field.null:
        return None
    return text


def td_cell(field, text):
    """One TD under the contract."""
    if text == "":
        return None
    dt = field.datatype
    if field.is_string and not field.is_array:
        return string(field, text)
    if field.is_string:  # 2-D char: fixed-width strings
        width = field.fixed_dims[0]
        return [
            text[i : i + width].split("\0", 1)[0].rstrip(" ")
            for i in range(0, len(text), width)
        ]
    if dt == "bit":
        bits = [c == "1" for c in "".join(text.split())]
        return bits if field.is_array else bits[0]
    if dt == "boolean":
        tokens = text.split()
        if (
            len(tokens) == 1
            and len(tokens[0]) > 1
            and tokens[0].lower() not in TRUE | FALSE
        ):
            tokens = list(tokens[0])
        values = [boolean(t) for t in tokens]
        return values if field.is_array else values[0]
    if text.strip() == "" or text.strip() in LENIENT_NULLS:
        return None
    values = [number(field, t) for t in text.split()]
    values = [None if is_magic(field, v) else v for v in values]
    if dt in ("floatComplex", "doubleComplex") and not field.is_array:
        return values
    if not field.is_array:
        return values[0]
    return values


STRUCT = {
    "unsignedByte": ("B", 1),
    "short": ("h", 2),
    "int": ("i", 4),
    "long": ("q", 8),
    "float": ("f", 4),
    "double": ("d", 8),
    "floatComplex": ("f", 4),
    "doubleComplex": ("d", 8),
}


class Stream:
    def __init__(self, data):
        self.data = data
        self.pos = 0

    def take(self, n):
        if self.pos + n > len(self.data):
            raise EOFError
        chunk = self.data[self.pos : self.pos + n]
        self.pos += n
        return chunk

    def done(self):
        return self.pos >= len(self.data)


def binary_cell(field, s):
    """One cell of a BINARY or BINARY2 row: the contract's value, before null flags."""
    dt = field.datatype
    fixed = math.prod(field.fixed_dims) if field.fixed_dims else 1
    count = struct.unpack(">I", s.take(4))[0] if field.variable else fixed
    if dt == "char":
        raw = s.take(count)
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.decode("latin-1")
        return decoded_string(field, text)
    if dt == "unicodeChar":
        return decoded_string(field, s.take(2 * count).decode("utf-16-be"))
    if dt == "bit":
        raw = s.take((count + 7) // 8)
        bits = [bool(raw[i // 8] >> (7 - i % 8) & 1) for i in range(count)]
        return bits if field.is_array else bits[0]
    if dt == "boolean":
        values = [boolean(chr(b)) for b in s.take(count)]
        return values if field.is_array else values[0]
    code, size = STRUCT[dt]
    n = count * (2 if "Complex" in dt else 1)
    values = list(struct.unpack(f">{n}{code}", s.take(n * size)))
    if dt in ("float", "floatComplex"):
        values = [f32(v) for v in values]
    values = [None if is_magic(field, v) else v for v in values]
    if dt in ("floatComplex", "doubleComplex") and not field.is_array:
        return values
    return values if field.is_array else values[0]


def decoded_string(field, text):
    if field.is_array:  # 2-D char
        width = field.fixed_dims[0]
        return [
            text[i : i + width].split("\0", 1)[0].rstrip(" ")
            for i in range(0, len(text), width)
        ]
    return string(field, text)


def binary_rows(fields, stream_bytes, flavour):
    s = Stream(stream_bytes)
    rows = []
    nbytes = (len(fields) + 7) // 8
    while not s.done():
        nulls = [False] * len(fields)
        if flavour == "BINARY2":
            mask = s.take(nbytes)
            nulls = [bool(mask[i // 8] >> (7 - i % 8) & 1) for i in range(len(fields))]
        row = []
        for field, null in zip(fields, nulls):
            value = binary_cell(field, s)
            row.append(None if null else value)
        rows.append(row)
    return rows


def tabledata(fields, body):
    rows = []
    for tr in body:
        if local(tr.tag) != "TR":
            continue
        tds = [td for td in tr if local(td.tag) == "TD"]
        if len(tds) != len(fields):
            raise ValueError(f"a TR with {len(tds)} TDs for {len(fields)} FIELDs")
        rows.append([td_cell(f, "".join(td.itertext())) for f, td in zip(fields, tds)])
    return rows


def read(path):
    """(columns, fields, rows, serialization, status) straight from the document."""
    root = ET.fromstring(document_bytes(path))
    status = query_status(root)
    table = first_table(root)
    if table is None:
        return [], [], None, status
    fields = [Field(e) for e in table if local(e.tag) == "FIELD"]
    data = child(table, "DATA")
    if data is None:
        return fields, [], None, status
    body = next(
        c for c in data if local(c.tag) in ("TABLEDATA", "BINARY", "BINARY2", "FITS")
    )
    flavour = local(body.tag)
    if flavour == "TABLEDATA":
        rows = tabledata(fields, body)
    elif flavour in ("BINARY", "BINARY2"):
        stream = child(body, "STREAM")
        if stream.get("href"):
            raise NotImplementedError("STREAM href")
        rows = binary_rows(
            fields, base64.b64decode("".join(stream.itertext())), flavour
        )
    else:
        rows = fits_rows(fields, body)
    return fields, rows, flavour, status


def fits_rows(fields, body):
    """The embedded FITS table, read with astropy.io.fits: astropy's VOTable reader only
    follows an href and cannot read a FITS STREAM given inline."""
    import io

    from astropy.io import fits

    stream = child(body, "STREAM")
    hdul = fits.open(io.BytesIO(base64.b64decode("".join(stream.itertext()))))
    data = hdul[int(body.get("extnum", 1))].data
    return [
        [astropy_cell(f, record[j], np.ma.nomask) for j, f in enumerate(fields)]
        for record in data
    ]


# ------------------------------------------------------------------------------------
# JSON form


def encoded(v):
    if isinstance(v, float):
        if math.isnan(v):
            return "NaN"
        if math.isinf(v):
            return "Infinity" if v > 0 else "-Infinity"
    if isinstance(v, list):
        return [encoded(x) for x in v]
    return v


# ------------------------------------------------------------------------------------
# astropy


def astropy_rows(path, fields):
    from astropy.io.votable import parse

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        vot = parse(str(path), verify="ignore")
        table = vot.get_first_table()
    arr = table.array
    names = arr.dtype.names
    rows = []
    for i in range(len(arr)):
        row = []
        for j, field in enumerate(fields):
            value = arr.data[names[j]][i]
            mask = arr.mask[names[j]][i]
            row.append(astropy_cell(field, value, mask))
        rows.append(row)
    return rows


def astropy_cell(field, value, mask):
    if isinstance(value, np.ma.MaskedArray):
        mask = np.ma.getmaskarray(value) | np.asarray(mask)
        value = value.data
    if field.is_array:
        if np.ndim(mask) == 0 and bool(mask):
            return None
        flat = np.asarray(value).ravel(order="C").tolist()
        m = np.broadcast_to(np.asarray(mask), np.shape(value)).ravel(order="C").tolist()
        out = []
        for x, mx in zip(flat, m):
            x = plain(field, x)
            if isinstance(x, list):
                out.extend([None, None] if mx else x)
            else:
                out.append(None if mx else x)
        return out
    if np.any(mask):
        return None
    return plain(field, value)


def plain(field, x):
    if isinstance(x, np.generic):
        x = x.item()
    if isinstance(x, bytes):
        x = x.decode("utf-8", "replace")
    if isinstance(x, complex):
        return [plain(field, x.real), plain(field, x.imag)]
    if isinstance(x, float) and field.datatype in ("float", "floatComplex"):
        return f32(x)
    return x


# ------------------------------------------------------------------------------------
# STILTS


def stilts_rows(path):
    done = subprocess.run(
        [
            "stilts",
            "tpipe",
            f"in={path}",
            "ifmt=votable",
            "ofmt=votable-tabledata",
            "out=-",
        ],
        capture_output=True,
        check=True,
    )
    root = ET.fromstring(done.stdout)
    table = first_table(root)
    fields = [Field(e) for e in table if local(e.tag) == "FIELD"]
    return fields, tabledata(fields, child(child(table, "DATA"), "TABLEDATA"))


# The ways another reader is seen to differ often enough to be named, keyed by
# (the contract's value, the other reader's), each after flattening one level of list.
KINDS = {
    ("NaN", None): "has null where the contract has NaN",
    (None, "NaN"): "has NaN where the contract has null",
    ("", None): "has null where the contract has an empty string",
    (None, ""): "has an empty string where the contract has null",
    ("", "¿"): "has '¿' where the contract has a NUL-truncated empty string",
}


def kind(o, t):
    if isinstance(o, list) and isinstance(t, list) and len(o) == len(t):
        pairs = {json.dumps([a, b]) for a, b in zip(o, t) if a != b}
        return tuple(json.loads(next(iter(pairs)))) if len(pairs) == 1 else "mixed"
    return (o, t) if not isinstance(o, list) and not isinstance(t, list) else "mixed"


def compare(fields, ours, theirs, who):
    """Where another reader disagrees with the contract: one line per kind of
    difference naming its columns, and one line per column for anything else."""
    if len(ours) != len(theirs):
        return [f"{who} reads {len(theirs)} rows, not {len(ours)}"]
    grouped, lines = {}, []
    for j, field in enumerate(fields):
        diffs = [
            (encoded(o[j]), encoded(t[j]))
            for o, t in zip(ours, theirs)
            if encoded(o[j]) != encoded(t[j])
        ]
        if not diffs:
            continue
        kinds = {kind(o, t) for o, t in diffs}
        if len(kinds) == 1 and next(iter(kinds)) in KINDS:
            grouped.setdefault(KINDS[next(iter(kinds))], []).append(
                f"{field.name} ({len(diffs)}/{len(ours)})"
            )
            continue
        o, t = diffs[0]
        lines.append(
            f"{who}: column {field.name!r} differs in {len(diffs)} of {len(ours)} rows, "
            f"e.g. {json.dumps(t, ensure_ascii=False)} where the contract gives "
            f"{json.dumps(o, ensure_ascii=False)}"
        )
    return [
        f"{who} {what}: {', '.join(cols)}" for what, cols in grouped.items()
    ] + lines


# ------------------------------------------------------------------------------------


def truth(path):
    global LENIENT_NULLS
    source = SOURCES[path.name]
    LENIENT_NULLS = frozenset(source.get("td_nulls", ()))
    try:
        fields, rows, flavour, status = read(path)
    except ET.ParseError:
        if not source.get("refuse"):
            raise
        fields, rows, flavour, status = [], [], None, None
    LENIENT_NULLS = frozenset()
    notes = [source["notes"]] if source.get("notes") else []
    if flavour == "FITS":
        notes.append("Rows are astropy.io.fits's reading of the embedded FITS table.")
    elif fields:
        try:
            notes += compare(fields, rows, astropy_rows(path, fields), "astropy")
        except Exception as exc:  # noqa: BLE001 - a reader failing is a finding
            notes.append(f"astropy cannot read it: {type(exc).__name__}: {exc}")
    if fields:
        try:
            stilts_fields, stilts = stilts_rows(path)
            notes += compare(fields, rows, stilts, "STILTS")
        except subprocess.CalledProcessError as exc:
            err = exc.stderr.decode(errors="replace").strip().splitlines()
            notes.append(f"STILTS cannot read it: {err[-1] if err else exc}")
    refuse = source.get("refuse")
    if refuse is None and status == "ERROR":
        raise ValueError(f"{path.name}: QUERY_STATUS ERROR with no refuse reason")
    return {
        "producer": source["producer"],
        "source_url": source["source_url"],
        "fetched": source.get("fetched", FETCHED),
        "serialization": flavour,
        "columns": [f.meta for f in fields],
        "rows": encoded(rows),
        "refuse": refuse,
        "notes": "\n".join(notes) or None,
    }


def main():
    names = sys.argv[1:] or sorted(p.name for p in HERE.glob("*.vot"))
    missing = sorted(set(p.name for p in HERE.glob("*.vot")) - set(SOURCES))
    if missing:
        raise SystemExit(f"no SOURCES entry for {missing}")
    for name in names:
        path = HERE / name
        out = path.with_name(path.name + ".json")
        out.write_text(json.dumps(truth(path), indent=1, ensure_ascii=False) + "\n")
        print(out.name)


if __name__ == "__main__":
    main()
