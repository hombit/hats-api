# TAP conformance

What today's TAP clients make of this service, reported rather than enforced.

Every question here is asked by a client astronomers already have — `pyvo`, and STILTS
`taplint`, which is the validator TAP services are actually judged by. Nothing in this
directory implements any part of TAP itself. That is the point: a suite written against
this service's own idea of the protocol would agree with it by construction, which is
worth nothing. The suite was written before the implementation for the same reason.

The one exception is a few lines that read a returned VOTable for *where* a marker was
written rather than what it says, because no client exposes that and DALI is specific
about it.

## Three questions

A standard marks every clause MUST or SHOULD and stops there, which leaves the things
worth knowing unanswered. Every check is labelled with which of these it speaks to, and
the report counts them separately.

1. **Does this service follow the standard?** — the checks that read what came back.
   A document either carries what the specification asks for or it does not.
2. **Do the clients work against it?** — the checks whose answer came through `pyvo`'s
   parser or a STILTS stage. Not the same question: a service can carry every INFO the
   standard asks for and still hand astropy a byte it refuses to decode, and then the
   feature does not exist as far as a user is concerned.
3. **Are the answers right?** — the checks that put one query to this service and to a
   service that has been answering it for years, and compare. No amount of well-formed
   XML establishes this, and it is the question a person running a query actually has.

Which question a check speaks to is derived from how it asks, so it stays true as
checks are added; `@pytest.mark.asks(...)` overrides that where the reading is wrong.

`tap-conformance-survey` adds a fourth thing, which is not a question about this
service: the same checks against services that have been answering TAP for years, to
decide **what matters**. A feature every established server implements is one clients
depend on whatever the standard calls it; a feature none of them implements is one to
think twice about — and as likely a check here reading the standard more strictly than
anyone reads it in practice. The reference runs are done by hand and committed under
`references/`; CI never puts a question to anyone else's service.

## Running it

```sh
cd tap-conformance
uv run --group fixtures tap-conformance-fetch --out data   # once
cargo build --bin hats-api --manifest-path ../Cargo.toml
uv run pytest -c pyproject.toml
```

And, to read this service beside the ones that have been answering TAP for years:

```sh
uv run tap-conformance-survey                      # from the committed snapshots
uv run tap-conformance-survey --refresh-references # ask them again, by hand
```

The report lands in `report/`: `report.md` to read, `report.json` for anything that
compares one run against another, `taplint.json` as the validator wrote it, and
`matrix.md` where the survey has been run.

`-c pyproject.toml` is not decoration. pytest works out where its configuration is from
the paths on the command line, and an option pointing outside this directory — a binary
in `../target`, a report directory elsewhere — sends it looking in the repository root,
where there is none.

Useful options:

| | |
|---|---|
| `--base-url URL` | put the questions to a service that is already running, and start nothing. Pointing it at a mature service is how the suite itself is checked |
| `--server-binary PATH` | which service to start; defaults to `../target/debug/hats-api` |
| `--data DIR` | what `tap-conformance-fetch` downloaded |
| `--report-dir DIR` | where the report goes |
| `--stilts CMD` / `--stilts-jar PATH` | STILTS, if it is not on the path as `stilts` |
| `--skip-taplint` | the `pyvo` half alone, which is seconds rather than minutes |
| `--strict-conformance` | exit non-zero when something fails. Off by default: a conformance run is a report |

## What the outcomes mean

pytest's own vocabulary, which is already the right one:

- **pass** — the standard asks for it and the service does it.
- **fail** — the standard asks for it and the service does not.
- **skip** — the question could not be put. A table that is not published, a validator
  that is not installed, reference answers that were never downloaded.

There is deliberately no "expected failure". Whether a service has decided not to
implement something is a fact about that service's plans, and a suite that knew about
those decisions would be one written against an implementation. A MUST that goes
unanswered is a failure here whoever is asked — `/async` included — and what to do
about it is a decision made somewhere a measurement cannot reach.

A run exits zero however many checks fail, because that is the output. It exits
non-zero for the two things that are not results: the service under test falling over,
and the suite failing to run. Both leave a report that reads like a service missing
every feature when what happened is that nobody asked it anything, so the report says so
above the numbers and the run goes red.

## What is asked, and where

One file per part of the standards, and each holds both halves — what `pyvo` makes of
it and what `taplint` says about it — so the report reads as one list of features
rather than as two lists from two tools.

| | |
|---|---|
| `test_availability.py` | VOSI 1.1 §3 — is the service up |
| `test_capabilities.py` | TAP §2.4, TAPRegExt — what it says it can do, and that it advertises nothing it has not got |
| `test_tables_metadata.py` | VOSI 1.1 §2 — the table metadata resource |
| `test_tap_schema.py` | TAP §4 — the same metadata as tables a client can query, and that the two agree |
| `test_sync_query.py` | TAP §2.1–2.7 — `/sync`, its parameters, and that each is honoured or refused |
| `test_output_formats.py` | TAP §2.7.3, DALI §3.4 — VOTable, CSV, TSV, and what an unknown format does |
| `test_maxrec.py` | TAP §2.7.4, DALI §4.4 — `MAXREC`, and whether a truncated answer says it is truncated |
| `test_errors.py` | DALI §4.4 — a failed query is a VOTable with a status, not an HTML page |
| `test_adql.py` | ADQL 2.1 — the mandatory language, nothing optional |
| `test_examples.py` | DALI §2.3 — the examples a client offers in a menu, and whether they run |
| `test_async.py` | TAP §2.2, UWS — expected failures, plus that the absence is legible |
| `test_upload.py` | TAP §2.5 — expected failures, plus that capabilities and behaviour agree |
| `test_reference_data.py` | not a standard: the same query, asked here through both clients and read against what a service that has been answering it for years said |

## The data

Three tables are published, and only one of them is downloaded.

| table | |
|---|---|
| `gaia_dr3.gaia_source` | the whole of Gaia DR3 as HATS, public on AWS. What the comparisons are really about |
| `sample.gaia_dr3` | a 0.5° cone of the same catalog, imported by `hats-import` from the reference service's own answer. It is what the validator works over — asking for whole rows of a 153-column catalog whose partitions are hundreds of megabytes is a slow way to find out whether a VOTable is well formed — and it keeps the suite runnable when S3 is having a bad morning |
| `ztf.dr24_lc` | ZTF DR24 light curves, which are a nested column. Published for what a client makes of the metadata of such a table; no reference service answers anything like it. The light-curve catalog rather than the object one — the objects carry a position and some counts, and the nesting is the point |

`tap-conformance-fetch` downloads the reference answers — the same ADQL, put to the ESA
Gaia Archive — and builds the sample out of the rows it fetched. Every reference query
stays inside the sample's cone, which is what lets one query be put to the sample and to
the whole catalog and be expected to give one answer.

It runs **once**. A suite that fetched its reference answers per run would be comparing
this service against a moving target, and slowly. CI keeps the download in a cache keyed
by the fetch script, so changing what is asked for downloads it again and nothing else
does.

Gaia DR3 is ESA/Gaia/DPAC's, under their
[acknowledgement terms](https://www.cosmos.esa.int/web/gaia/dr3-acknowledgements).
Nothing downloaded here is committed.

## Client quirks worth knowing

`CLIENTS.md` beside this file is what running the suite against correct services turned
up about pyvo and STILTS themselves — two of them worked around here, the rest worth
knowing before publishing a TAP surface.

One choice of this suite's own belongs with them: **`taplint` is asked for its stages by
name**, not the default set. A STILTS release that adds a stage would otherwise add a
row to the report that nothing here has decided the meaning of. ObsCore, ObsLocTAP and
EPN-TAP are left out — they validate data models a service chooses to publish, and one
that publishes none of them is not less conforming for it.

## Where a check and a working service disagree

Running the suite against mature services turns up places where this suite's reading of
a clause and theirs part company. Each one is a question about the check: a reading no
established service shares is more likely to be too strict than to have found several
independent bugs, and one they all share is a reading to follow here.
`REFERENCE_SERVICES.md` beside this file is the list, unsettled.
