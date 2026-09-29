# VOTable corpus

Documents for `tests/votable_corpus.rs`, which reads every `<file>.vot` that has a
`<file>.vot.json` beside it and holds the reader to it: the columns as the FIELDs declared
them, every cell under the contract that file's header describes, or `refuse` where the
document must be refused. The documents are kept byte for byte as written, so the text
fixers in `.pre-commit-config.yaml` leave this directory alone.

## generated/

The same tables written by three writers in every serialization each has: astropy 8.0.1,
STILTS, and the CDS Rust `votable` crate (`cds-writer/`). `uv run generate.py` rewrites every
file and its JSON; it needs `stilts` on the path and cargo. The ground truth is what each file
says, decoded from the VOTable text by the script itself, except where a writer's layout is
one the reader supports on its own terms — astropy's count of a variable multi-dimensional
array and its one-byte-per-bit variable `bit` array. Each file's `notes` says what the writer
lost and where astropy and STILTS read it differently.

## handwritten/

One edge case per document, written by `uv run make.py`: namespaces, encodings, CDATA and
entities, number and boolean spellings, nested and referenced tables, both counts of a variable
2-D array — and the documents a reader must refuse, from a `STREAM href` to an entity the
document declares.

## wild/

VOTables as real services wrote them: small answers from a dozen writers, in every
serialization each offers, plus a few error documents a reader must refuse. Each
`<file>.vot` has a `<file>.vot.json` beside it holding the columns (from the first
TABLE's FIELDs, DESCRIPTION text trimmed), the rows, the url fetched, and `refuse` where
a reader must refuse the document. `uv run expected.py` in that directory regenerates
every JSON; `SOURCES` in it is where the urls and refusals live.

The rows are decoded from the document itself — TD text, or the BINARY and BINARY2
stream — under the encoding contract, and compared cell by cell with astropy and with
STILTS. The contract wins, and every disagreement is in the file's `notes`. STILTS is
compared through its own TABLEDATA re-serialization (`stilts tpipe
ofmt=votable-tabledata`), so a null float comes back from it as `NaN` and a NUL
character as `¿`; those lines say what STILTS writes, not only what it reads.

Fetched 2026-09-29 (UTC):

- `simbad-*` — CDS SIMBAD TAP, TAPLibrary (Java): `basic` in BINARY, TABLEDATA, BINARY2 and FITS-in-VOTable, and an error.
- `tapvizier-*` — CDS TAPVizieR, TAPLibrary (Java): Hipparcos in TABLEDATA, BINARY and BINARY2, and an error.
- `gavo-*` — GAVO Data Center, DaCHS 2.12.2 (Python): ObsCore, SDSS DR16 arrays, RAVE booleans, CARMENES and CALIFA points/polygons/timestamps, unicodeChar names, a MIVOT-annotated answer, SCS, SIA 1, SIA 2, SSA, a DataLink links response, and a timeout error.
- `arigaia-*` — ARI Gaia TAP, TAPLibrary (Java): Gaia DR3 in BINARY, TABLEDATA and BINARY2, and an ADQL error.
- `esagaia-*` — ESA Gaia Archive TAP+ (Java): Gaia DR3 in BINARY2 and TABLEDATA, a gzip body served as a VOTable, and an error.
- `ned-*` — NED TAP, TAPLibrary (Java): `objdir` in BINARY, TABLEDATA and BINARY2.
- `irsa-*` — IRSA TAP (Java): AllWISE in TABLEDATA and BINARY2.
- `heasarc-*` — HEASARC Xamin TAP (Java): `xmmmaster`, BINARY under a capabilities document naming TABLEDATA.
- `mast-*` — MAST vo-tap, "MAST VOTable encoder 1.0": CAOM ObsCore in TABLEDATA.
- `datalab-*` — NOIRLab Astro Data Lab, DALServer (Java): DES DR2 in TABLEDATA with no namespace, and an error.
- `vizier-*` — VizieR 7.6 classic ASU interface: Hipparcos, 2MASS and Gaia DR3 in TABLEDATA and BINARY, and a malformed answer to `votable/-b2`.
- `astropytests-*` — astropy's test data at a pinned commit: captured Gemini, IRSA nph (VOTable "v1.0") and VizieR answers, BINARY2 masked strings, a MIVOT example, and an IRSA error.
- `pyvotests-*` — pyvo's test data at a pinned commit: DataLink, a service descriptor, ObsCore, and an error document carrying a table.
