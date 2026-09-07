# Development plan

## Progress tracker

Updated in the same commit as the code. A step is `done` only when its tests pass,
`cargo clippy --all-targets` and `cargo fmt --check` are clean, and §0 still holds.

Status values: `todo`, `in progress`, `done`, `deferred` (with what it waits for) and
`dropped` (with the reason). See `CLAUDE.md` for what goes in this file and what does
not.

The two deferred backends wait on §3 and §4 rather than on each other: the file-server
interface is what the service is for, and every backend added before it is one more
thing to keep working while that is built.

| § | step | status | notes |
|---|---|---|---|
| 2.1 | OpenDAL backend layer, s3 migrated first | done | |
| 2.2 | GCS and Azure | done | |
| 8.3 | network policy | done | |
| 2.3 | HTTP/HTTPS, range probe, materialization | done | |
| 2.4 | WebDAV | done | |
| 2.5 | Hugging Face | deferred | until after §4, and droppable |
| 3.1 | two-mode configuration | done | |
| 3.2 | routing | done | |
| 3.3 | API request shape (`select`/`where`, `region`) | done | |
| 3.4 | file-server request shape | done | |
| 4 | file-server interface | done | |
| 4.1 | write the README | done | |
| 4.2 | say the ordering guarantees in the user documentation | todo | the guarantees hold and `CLAUDE.md` has the rule; the README does not mention order at all |
| 4.3 | what the engine costs | todo | measurement, not code. Its findings decide the rest of `query::session_config` |
| 4.4 | a directory page worth looking at | todo | presentation only, and constrained: the markup is scraped by `fsspec` |
| 5.1 | HATS catalog metadata | todo | |
| 5.2 | spatial predicate | part | `circle` and `box` refine per row against a parquet target. What is left is the HATS target — partition pruning, the `_healpix_29` prefilter, `polygon`/`moc` — and `POST /api/v1/hats`. Order policy and range budget to be settled by measurement first |
| 5.3 | sync / plan / auto | todo | |
| 7.3 | serve the API description | todo | after §5: it describes the API, and §5 is still adding to it |
| 6.8 | request cost benchmark | todo | prerequisite for the rest of §6 — it ranks the layers |
| 6.1–6.7 | caching | todo | build in the order §6.8 ranks |
| 7 | operational surface | todo | |

§1 is the target shape, §2–§7 the phases in order, §8 the conditions every phase must
keep, §9 what is deferred.

Rules a finished phase leaves behind live in `CLAUDE.md`, not here.

## 0. Invariants

Three invariants for every phase below:

1. **The service only ever reads.** No endpoint writes, no mount is writable. Mounts
   therefore carry no `readonly` flag. Adding writes is a new document, not a new flag.
2. **Stateless per request.** No session cache, no catalog registry across requests.
   §6's caches must be evictable, bounded, and correct when empty.
3. **Nothing reaches an object store without passing its mode's policy.** Today:
   `AccessPolicy::authorize` in `storage::open`, the only route to a store. §3 adds a
   second mode with its own table, leaving two tables, one resolver, and no route to
   bytes that consults neither.

## 1. The shape we are aiming at

One binary, two interfaces over shared storage and query layers.

```
  POST /api/v1/…      ┌───────────────────────────────────────┐
                      │ API mode: url-addressed               │
                      │  the caller names the location        │
                      │  any scheme, incl. local files        │
                      │  governed by [api.access]  ◄──┐       │
                      └───────────────┬───────────────┼───────┘
                                      │               │ each mount
  GET  /  /hats  …    ┌───────────────┴───────────────┼───────┐
                      │ File-server mode: path-addr.  │ grants│
                      │  the operator named it        │ its   │
                      │  local directories            │ prefix│
                      │  governed by [[mount]] ───────┘       │
                      └───────────────┬───────────────────────┘
                                      │
                      ┌───────────────┴───────────────────────┐
                      │  shared: storage → query → output     │
                      └───────────────────────────────────────┘
```

The modes differ in who names the location:

- **API mode** takes the location from the caller as a `url`, which may carry storage
  options and credentials. `[api.access]` governs where a caller may point the service.
  Every backend is reachable, local files included; local access is denied by default,
  not absent.
- **File-server mode** maps an operator-configured prefix onto a URL path (`/` →
  `/srv/data`, `/hats` → `/data/hats`). The caller never names a store and never supplies
  a credential. Outside a mount is a 404, not a policy refusal. A mount's source is a
  local directory.

The query engine, parquet writer, object stores and path-resolution primitives are one
implementation under both. The request shapes differ (§3.3, §3.4).

Both modes may be enabled at once, and either may be off.

## 2. Phase 1 — more storage backends

OpenDAL is the backend layer for everything but `file://`; `object_store_opendal` adapts
an `Operator` into the `ObjectStore` trait DataFusion consumes. `s3`, `gs` and `az` set
the pattern the remaining backends follow — see `CLAUDE.md` for what one has to satisfy.

### 2.4 WebDAV

`opendal`'s `services-webdav`, as `webdav://host/path`. WebDAV is HTTP plus `PROPFIND`,
which is the listing operation the http backend lacks, so a WebDAV-hosted catalog gets all
three of §5.1's discovery tiers and can be served through §4's directory pages.

| option | meaning |
|---|---|
| `transport` | `https` (the default) or `http`, since the scheme names the protocol |
| `username`, `password` | credentials, given together, and subject to §8.1 |

Policy: `[api.access.webdav]`, three-state `endpoints` as elsewhere. Separate from
`[api.access.http]` — a host that may be read as flat objects is not thereby a host whose
directory tree may be enumerated.

Two things are already built and need only to be pointed at it. The range probe and
materialization are backend-agnostic — WebDAV is HTTP underneath, and
`MaterializingStore` wraps any store — and `[access.network]` governs the host without
knowing which backend asked.

The credentials cannot simply go on the builder, though. `MaterializingStore` probes with
a request of its own, made outside the store, so a server that authenticates would answer
the probe `401`; the probe reads a non-success as "let the store try", and a server that
then ignored `Range` would feed the reader the head of the file where it asked for the
tail. WebDAV's `username` and `password` are HTTP basic auth and nothing more, so lowering
them into the one `HeaderMap` that both the probe and the store's transport already carry
is what makes the two provably agree.

`opendal`'s `WebdavConfig` keeps `password` and `token` out of its own `Debug`, so §8.5's
sixth obligation has nothing to add to `CREDENTIAL_UNSAFE_TARGETS` — but its `parse_error`
puts the origin's response body into the error message, which §8.3 forbids echoing. Check
whether the http service does the same before assuming this one is new.

### 2.5 Hugging Face

`opendal`'s `services-hf`, as `hf://namespace/name/path`. Lists through the repo tree API,
so §4's directory pages and §5.1's tier 3 work.

Options: `revision` (default `main`), and `token` for gated datasets — a credential,
handled as §8.1 requires.

**Drop this backend** if it will not fit the shape the others set: it has the least
astronomy data behind it and its absence costs nothing structural.

**Deliverable.** `SUPPORTED_SCHEMES = ["s3", "gs", "az", "http", "https", "webdav", "hf", "file"]`,
one policy section per backend, a matrix test that every scheme is allowed by the
narrowest config that should allow it and refused by the config that turns it off, and
`storage::tests` passing unchanged across the migration.

The matrix cannot say "refused by the default config": the default for a remote backend
is any endpoint, not none. Making remote backends default-deny is §3.1's to decide,
along with the rest of the access table.

## 3. Phase 2 — two modes of operation

Separate configuration (§3.1), routing (§3.2) and request shapes (§3.3, §3.4), but one
internal query representation and one execution path. `columns`/`filters` and
`select`/`where` parse into the same thing; a semantic divergence between them is a bug.

### 3.3 The API request shape

Settled, `region` included.

```json
{
  "url": "s3://bucket/hats/ztf_dr24",
  "region": [{"type": "circle", "ra": 320.65747, "dec": -12.35315, "radius_arcsec": 36}],
  "ra_column": "objra",
  "dec_column": "objdec",
  "select": "objectid, lightcurve.mag AS mag, objra, objdec",
  "where": "filterid = 2 AND mag < 20 AND objectid IN (1383212200036217, 1383212200036218)",
  "format": "parquet",
  "limit": 1000
}
```

`region` is a structured field rather than part of the `where` expression (§3.5), so that
it can drive partition pruning in HATS mode (§5.2).

**It is always an array; each element is an object with a `type`.** One region is an array
of one, and the array is a union. `ra_column` and `dec_column` are required alongside it:
a lone parquet file says nothing about which of its columns are a position. A catalog's
`properties` (§5.1) does, which is what will make them optional for a HATS target.

The two shapes still to add:

| `type` | fields |
|---|---|
| `polygon` | `vertices: [[ra, dec], …]` |
| `moc` | `ascii`, or `url` — an IVOA MOC given directly |

- **`moc: {url: …}` is a caller-named fetch** and goes through §8.3 like any other.
- **Every shape has to lower to a MOC** for §5.2's partition pruning. `circle` and `box`
  today lower only to a per-row predicate, which is all a single-file target needs.
- **Intersection and difference** are cheap to add as explicit combinators over the array.

### 3.4 The file-server request shape

`GET`, a query string on the file's own path, using vizcat's parameter names. Browsers,
`wget`, `lsdb` clients and §5.3 plan entries speak this one.

**We adopt vizcat's query syntax — not more, not less.** Parameter names and the grammar
inside them; not their URL structure, not their limits.

| parameter | ours |
|---|---|
| `columns` | comma-separated column names, projection. No count limit, and a name SQL will not take bare is quoted: `"E(BP-RP)"` |
| `filters` | one boolean SQL expression, the same language `where` takes, with `&&` accepted for `AND` |

**We take their parameter names, not their limits and not their behaviour.** The point is
that one client can read both services; nothing about matching them is a reason to serve a
worse answer. So the eight-column cap is not adopted, and neither is the silence: a
`filters` that does not parse, or that names a column the file has not got, is a 400 here.
A predicate that goes missing returns every row, and a caller cannot tell that from a
predicate that matched them all.

`AND` is not a convenience alias. `&` ends a query parameter, so `&&` has to reach the
server percent-encoded, and a caller writing the documented spelling by hand gets a
request that parses as `filters=Gmag>8.0` and a second clause that never arrives.
Accepting `AND` gives that caller something they can type.

`||` is not accepted for `OR`. It is SQL's string concatenation, so honouring it would
leave one spelling with two meanings and no way to write the other.

Never reuse one of their parameter names for different semantics. Extensions with no
vizcat equivalent — `format`, `limit` — take names of our own.

**No spatial parameter.** Spatial selection here is by path, addressing `Norder=k/Npix=p`
directly. A caller who wants the catalog to choose partitions uses the API (§5.2) against
the same data, which the mount's derived grant permits.

**What has a query surface at all is one configured list**, `[data] filenames`, of globs
matched against a file's own name — `["*.parq", "*.parquet", "*.pq", "_metadata",
"_common_metadata"]` by default, which is what a HATS catalog contains. The two modes
differ only in what they do with a name that is not on it: the file server has bytes to
send, so it sends them and ignores the parameters; the API has nothing else to do with an
object, so it answers 404. Being on the list is a claim about the name and not about the
contents — a `part0.parquet` that is not one is refused by the parquet reader, as the
caller's mistake rather than a fault here.

What the live service turned out to do, which is why the table above is short: **`filters`
is not implemented there.** Every value — a valid predicate, a nonsense string, a name no
column has — returns byte-identical output, including one filter matching every row and one
matching none. `columns` does work, is matched case-sensitively against the file's own
spelling, is not percent-decoded, and has no `${X}` escape: `E(BP-RP)` is written literally.
So there is no deployed grammar to copy, and `filters` is ours to define. It is defined as
the language `where` already speaks, which is the one that needs no second parser.

#### The same two languages in the API body

`columns`/`filters` is a second way of saying what `select`/`where` say, so a caller who
knows one should not have to learn the other to move between the modes. The API body
accepts either pair — `{select, where}` or `{columns, filters}` — and refuses a request
carrying fields from both, which is a caller who thinks they mean different things.

This is one endpoint with two vocabularies, not two endpoints: the transport, the
policy, the target and the output are identical, and only the wording of the projection
and the predicate differ. It is also not a query string. The `GET` shape is a separate
question and stays where it is (§9.2), for the reason §3.3 is a `POST` at all.

- **The narrower language stays narrower.** `select` takes expressions and aliases;
  `columns` takes names. Accepting `columns` in the API body does not widen it to
  expressions — a caller who wants one writes `select`.
- **One parser, one allowlist.** `filters` lowers to the expression `sql.rs` already
  checks, rather than executing down a path of its own. The one difference between the
  two is `&&`, and it is rewritten on the token stream rather than on the text: `&&`
  inside a string literal is not an operator, and a substitution over characters that
  cannot tell the difference is a parser written by accident, which is what §3.5 says not
  to do.

### 3.5 How much SQL

**SQL expressions are accepted; SQL statements are not. The spatial constraint is a
structured field, not an expression, until ADQL (§9).**

- **Expressions in SQL.** A predicate language needs comparisons, `AND`/`OR`/`NOT`, `IN`,
  `BETWEEN`, `IS NULL` and arithmetic. DataFusion parses and plans these already, and
  astronomers know the syntax from ADQL.
- **No statements.** `FROM` admits joins — a join across two catalogs is a crossmatch,
  which is `lsdb`'s job — and `GROUP BY`, which breaks §5.3's plan mode and the
  pre-execution cost estimate §5.3 and §8.4 depend on. Keeping the target in the path and
  its identity in `url` leaves no `FROM` to abuse, so the restriction needs no enforcement
  machinery.
- **Spatial stays structured.** Partition pruning requires *recognising* the constraint. A
  named `region` field is recognised by construction. As `WHERE cone_contains(…)` it would
  be a UDF call to pattern-match inside a logical plan — implementable via a
  `TableProvider` with `supports_filters_pushdown`, but best-effort: an unexpected
  phrasing degrades silently to scanning every partition.

**With ADQL** (§9), the spatial functions are standardised — `CONTAINS`, `POINT`,
`CIRCLE`, `DISTANCE` — so the matcher targets specified spellings and can refuse
unrecognised spatial predicates rather than scanning everything. That is when the
constraint can move into the expression.

## 4. Phase 3 — the file-server interface

Modelled on <https://vizcat.cds.unistra.fr/hats/> and
<https://github.com/astronomy-commons/lsdb-server>, with the query surface from §3.4.

The static-serving path must not regress: an `lsdb` client pointed at a mount should work
with no knowledge of anything else this service does. Nothing here has been tried against
a real one yet, which is the one check this phase cannot do by reading.

### 4.2 Say the ordering guarantees in the user documentation

The guarantees themselves hold and `CLAUDE.md` carries the rule; what is left is telling
a caller. The README says nothing about order, and a difference in ordering between two
interfaces over the same data is exactly what someone will not expect to have to ask
about:

- The file-server mode returns rows in the file's order, so reading one partition twice
  gives the same rows in the same places.
- The API mode promises no order.
- Both answer a `limit` reproducibly, and under the file-server's order a `limit` is the
  *front* of the file rather than an arbitrary selection.

Worth stating plainly rather than burying, since the second point is the surprising one
and the third is what stops a caller building a pager on the first.

Two loose ends, neither blocking:

- **What turning work stealing off costs.** A partition that finishes early no longer
  helps a slow sibling, so a file whose row groups decode unevenly is slower to read.
  Measured with the rest of the engine's costs below.
- **Streaming a parquet answer.** Partition 0 is complete and correct while later
  partitions are still being read, so a parquet response could be written a partition at
  a time instead of after the whole answer is in memory. Wants §6's shape first, since
  what it saves is peak memory on a large answer.

### 4.3 What the engine costs

`query::session_config` sets seven options from what each is
documented to do; none has been measured here. What that costs and what it buys, against
a real file and per option:

- `pushdown_filters` and `reorder_filters` — the two that are off by default upstream,
  and the two claimed to be worth the most.
- `enable_page_index`, `bloom_filter_on_read`, `pruning` — each pays a metadata read
  before it can save a data read, which is a different trade over a slow origin than over
  a local mount. The question for the first two is what they cost against a file that
  carries neither structure, since that decides whether leaving them on is free
  insurance. It is not "do today's files have them": which importer wrote a file is not
  something this service gets to assume, and page statistics are coming.
- the batch size and the target partition count, which are not set at all today. §4.2 has
  since made `target_partitions` a correctness question for one of the two modes, so what
  is left here is its cost.

Measure over the shapes this service is for: a point lookup by id, a range over a sorted
column, a predicate on a column with no statistics, and a wide projection versus a narrow
one. The result wanted is a heuristic — which of these should depend on the request or the
file rather than being fixed at startup — not a table of numbers.

This is not §6.8. That one ranks the caching layers by where a request's time goes; this
one is about the engine's own knobs, and it comes first because a cache measured against
a badly configured engine ranks the wrong layer.

### 4.4 A directory page worth looking at

The generated listing is a bare table and reads worse than what `apache` and `nginx`
serve, which is the comparison a person browsing a catalog actually makes. Presentation
only: what is listed and in what order is settled, and none of it changes here.

Four constraints, because each rules out the obvious way to do this:

- **The page is scraped.** `fsspec`'s HTTP filesystem and the `lsdb` clients above it
  find entries by reading `href` attributes out of the markup. So every entry keeps a
  plain `<a href="…">` that a regex can find, and a redesign that moves the link into a
  script or builds the url out of a data attribute breaks a client that never asked for
  a page at all. `a_browser_gets_a_page_and_a_client_does_not` is what notices.
- **Nothing is fetched from anywhere.** No CDN, no webfont, no framework — the same
  reason §7.3 bundles its renderer rather than linking one. A page that is blank on the
  network this service is built for is worse than a plain one. Inline CSS, and any script
  small enough to inline and optional enough that the page is complete without it.
- **No parameters.** `CLAUDE.md` settles that a listing takes none, and §3.4 has since
  given query parameters a meaning on a file's own url. Sorting and filtering, if they
  arrive, are client-side over markup that is already correct without them.
- **Built per request.** There is no listing cache until §6.1 and a HATS `Dir=` level is
  ten thousand entries, so this stays string formatting rather than a template engine.

What to do, roughly in the order a person notices it:

- **A breadcrumb heading.** `Index of /hats/ztf_dr24/dataset/Norder=5` is a dead string;
  each component a link makes climbing two levels one click rather than two.
- **Alignment.** A monospace column for names and sizes is most of why the `apache` page
  is readable at a glance, against a proportional font and ragged columns here.
- **A summary line** — how many directories, how many files, the total size. A `Dir=`
  level is where the page is longest and where this matters most.
- **Legibility**: row striping, a hover row, and `prefers-color-scheme`, so it is not a
  white rectangle at night.
- **Times a person can read.** `2026-01-27T22:26:43Z` is right for a machine; rendering
  it in the reader's own timezone is a few lines of optional script over a `datetime`
  attribute that stays the machine-readable one.
- **Say that a data file can be queried.** A caller who browsed to a partition has no way
  to discover §3.4's parameters, and this page is the only thing that could tell them.
  One line naming `columns` and `filters` beside an entry matching `[data] filenames` is
  the whole feature's discoverability.

## 5. Phase 4 — the HATS interface

Removes the requirement that a caller know which partition file holds their object.

### 5.1 Catalog metadata

A HATS catalog is a directory with `properties`, `partition_info.csv`,
`_metadata`/`_common_metadata`, and `dataset/Norder=k/Dir=d/Npix=p/*.parquet`. Add
`src/hats/`:

- `properties.rs` — the `properties` file: catalog name, type, ra/dec column names,
  `hats_order`, row count.
- `partitions.rs` — the partition list, by the tiers below.
- `pixels.rs` — HEALPix nested-scheme arithmetic: region → covering pixel set, pixel →
  `_healpix_29` range, ancestor/descendant tests. Use `cdshealpix` rather than writing it.

**Partition discovery, in priority order.** Each tier is used only when the one above is
absent:

| | source | requests | gives |
|---|---|---|---|
| 1 | `partition_info.csv` | one small `GET` | `(Norder, Npix)` only |
| 2 | `_metadata` | one `GET`, footer-first | the pixel list, plus row counts, byte sizes and column statistics per partition |
| 3 | listing `dataset/` | one `LIST` per level, paginated | the pixel list, parsed from `Norder=k/Npix=p` path names |

`_metadata` is a parquet file whose footer carries the `FileMetaData` of every partition,
each row group tagged with its `file_path`, so one ranged footer read reconstructs the
partition list. It also supplies the per-partition sizes §5.3's estimates need, so it is
worth reading **even when tier 1 succeeded** — lazily, when a query needs sizing, not on
every catalog open.

- `_common_metadata` cannot substitute: schema only, no row groups, no `file_path`
  entries. It is useful for validating a projection before reading data.
- Reading `_metadata` here is what makes it answerable at all. It is on `[data] filenames`
  by default, so a caller can already put a query on its url — and gets a 400, because the
  rows its footer describes are in the files beside it and the ranged read for them runs
  off the end of `_metadata` itself. Once this tier exists, that request has a real answer
  available: the partition list, or the statistics per partition. Decide then whether it
  gets one, since the answer is metadata rather than the rows the caller asked for.
- `_metadata` can reach hundreds of MB for a wide schema over many partitions. Cap it,
  and fall through to tier 3 rather than blocking on a large download. Against a
  non-ranging server it has already been copied whole by the time it is read, so the cap
  that matters there is `limits.max_materialize_bytes`.
- Listing may be unavailable entirely: an `http(s)://` catalog has no listing operation,
  leaving tiers 1 and 2. The same catalog served over WebDAV (§2.4) has all three.

If tiers 1 and 2 disagree, prefer `partition_info.csv` and log at `warn` — they disagree
when a catalog is malformed or being rewritten.

The parsed partition list goes in §6.1's HATS metadata cache; the `_metadata` footer goes
in the parquet metadata cache.

### 5.2 The spatial predicate

`region` (§3.3) is one field of an ordinary request, beside `where` and `select`. The
`mode` field — `sync`, `plan`, `auto` (default) — controls delivery (§5.3).

**Execution, HATS target:**

1. `region` → MOC → intersect with the partition list. This is free, needs no column in
   the data, and already separates two cases:
   - **partitions fully inside the region** — every row qualifies, no spatial test at all;
   - **partitions the region only partly covers** — rows need a per-row test.
2. Per surviving partition, push down the `where` clauses that map onto its columns, fused
   with whatever spatial predicate step 3 produces, so a partition whose statistics rule out
   `filterid = 2` is never read.
3. Row-level refinement, **for boundary partitions only**. Where the catalog has a spatial
   index column (`_healpix_29`, usual but not guaranteed), prefilter on it — see below;
   otherwise go straight to the geometric test against `ra`/`dec`, read into the scan and
   dropped unless `select` asked for them.
4. Union, project to `select`, return.

**Prefiltering on `_healpix_29` is required where the column exists**, but not as one exact
covering — a covering fine enough to be exact is enormous (at order 29 a one-degree circle's
boundary is of order 10⁷ cells), and testing membership against thousands of ranges costs
more per row than the trigonometry it replaces. Two things keep it cheap:

- **Two coarse range sets, not one.** An **inner** set of cells wholly inside the region:
  rows there are accepted with no geometric test. An **outer** set covering the region:
  rows outside it are rejected with no geometric test. Only rows between the two — the
  boundary shell — reach the trigonometry. Neither set has to be tight, so both stay small.
- **Computed per boundary partition, not for the whole region.** The covering order is
  chosen relative to that partition's own cell rather than to the region's size, so each
  boundary partition contributes a handful of ranges however large the region is, and for a
  large region most partitions are interior (step 1) and contribute none.

Size the range sets to what the page index can prune: the aim is skipping row groups and
pages on a column the partition is already sorted by, not per-row membership. The order
policy and the range budget are to be settled by measurement before implementing §5.2.

This reduces bytes read per row, not rows per query. A region over a dense catalog can
still select terabytes, which is what §5.3's `max_scanned_bytes` and plan mode are for.

**Parquet target:** steps 1 and 3's prefilter do not apply — there is one file and no
`_healpix_29` to lean on — leaving the geometric test against the two columns the request
names. That part is built: `circle` compares haversines and `box` is a range in each
coordinate, each carrying whatever coordinate bounds can be pruned on.

What the HATS target adds over it: the covering step every shape needs, the partition
intersection, the interior/boundary split, and the `_healpix_29` range sets. `polygon` and
`moc` arrive with the covering step, since that is the half they are missing.

Nearest-object lookup is a `circle` plus ordering and `limit: 1`, not a predicate, and
waits for ordering. `crossmatch` is out of scope — `lsdb`'s job.

### 5.3 Small queries and large queries

A region over a dense catalog can touch hundreds of partitions of hundreds of MB.

- **`mode=sync`** — run it and return rows, bounded by `max_partitions`,
  `max_scanned_bytes` and `timeout`. Sizes come from `_metadata` (§5.1); without it, fall
  back to a `HEAD` per candidate partition or to counting partitions. Exceeding a limit is
  a 413 whose body is the plan below.
- **`mode=plan`** — resolve the catalog and return a work list without reading data:

  ```json
  {
    "catalog": "s3://bucket/hats/ztf_dr24",
    "num_partitions": 3,
    "estimated_bytes": 1140850688,
    "requires_credentials": true,
    "requests": [
      {
        "order": 5, "pixel": 12240,
        "method": "POST",
        "path": "/api/v1/parquet",
        "body": {"url": "…", "where": "_healpix_29 = …", "select": "…"},
        "estimated_bytes": 380375000
      }
    ]
  }
  ```

  Each entry is a request against this service. The client fans out with its own
  concurrency limit and concatenates; retries are per partition. `method` and `path` are
  separate fields so the file-server case, where entries are `GET`s under a mount, uses the
  same shape.

  **Credentials are never echoed into a plan** (§8.1). When the original request carried
  them, the entries carry the stripped url and `requires_credentials` is `true`; the client
  re-attaches the credentials it already holds before sending. Returning them would enable
  nothing the client cannot already do, while copying the secret into a response that gets
  logged, cached and pasted. This is not configurable: an option to echo them would be a
  footgun with no capability behind it. The flag exists so the client re-attaches
  deliberately rather than discovering the need through a 403.
- **`mode=auto`** — sync under the limits, plan over them. The response states which.

No job queue, job ids or polling: the plan is a list of stateless requests. See §7.2.

## 6. Phase 5 — caching

### 6.0 Invariants

1. **A cache is never a source of truth.** Empty it at any moment and every request still
   returns the same answer, slower.
2. **The access policy is consulted before the cache.** A lookup that short-circuits
   `AccessPolicy::authorize` would keep serving an object admitted under an older config,
   or reachable through a different mount.
3. **The key includes everything affecting the bytes, credentials included.** Two callers
   with different credentials for one `s3://` url may be entitled to different objects, so
   entries are keyed by credential identity — `HMAC(per-process key, credential set)`,
   never a bare hash of a possibly low-entropy secret, since a cache that outlives or lives
   outside the process holds that key at rest.
4. **Credentialed entries are always revalidated**, never served on TTL alone. The
   conditional request carries the caller's own credentials, so a revoked or narrowed
   credential fails at the origin and the entry is evicted. Keying by credential gives
   isolation between callers; only revalidation catches a credential that has stopped being
   valid, which is the one thing the key cannot express.
5. **Bound every layer twice**, by entries and by bytes.
6. **Every layer is emptiable at runtime** (§6.6).

### 6.1 The layers

| layer | holds | keyed by | bound | default |
|---|---|---|---|---|
| **HATS catalog metadata** | parsed `properties`, partition list, derived MOC | catalog url + validator | entries | on |
| **Parquet file metadata** | `FileMetaData` footer, page index, bloom filter headers | object url + validator | bytes | on |
| **Range-support verdict** | does this object's server honour `Range`? | object url | entries, short TTL | on |
| **Object bytes** | whole objects or ranges | object url + validator (+ range) | bytes, disk if enabled | off |
| **Directory listings** (§4) | one page of a listing | prefix + validator | entries | on, short TTL |
| **Negative results** | 404 for a missing object | url | entries | on, very short TTL |
| Query results | — | — | — | not cached, §6.5 |

The first two are the ones worth building. A `format=parquet` request reads the source
footer **three times**, and two of those are this crate's own: DataFusion fetches it once
while inferring the schema and its own `FileMetadataCache` serves the scan from that,
while `parquet_out::read_layout` goes straight to the store and pays two requests — the
parquet reader's default prefetch is 8 bytes, enough for the footer tail and never enough
for the footer, so the second fetch is unconditional. Counted, not read off the code:
every shape measured came to `+2` requests for the layout.

**Nothing here should be reading metadata itself.** The read belongs to DataFusion, which
has already done it, so the layout should come out of what the session already holds
rather than off the wire again. The obstacle is reach, not design: the entry in
DataFusion's cache is an `Arc<dyn FileMetadata>` whose only accessor is `as_any`, and the
concrete `CachedParquetMetaData` lives in `datafusion-datasource-parquet`, which the
`datafusion` facade does not re-export — so getting at it means taking that crate as a
direct dependency, version-locked to DataFusion the way `object_store` already is. Decide
that here, where the metadata cache is being built anyway, rather than paying it for one
call site.

Two smaller things fall out of the same measurement, neither of them a cache: DataFusion's
own `metadata_size_hint` defaults to 512 KiB and ours defaults to 8 bytes, and the layout
read is sequenced after the query when it does not depend on it.

Negative caching applies to absence only. A 403 is recomputed every time, since the policy
behind it can be reloaded.

The range-support verdict is keyed by object rather than by host, which is how
`materialize` already decides it. A host is not one answer: the same server can hand back
static files that range and generated responses that do not. Caching it saves the probe
request on a second read of the same object, and nothing more — so it is worth little
until there is a second read, which is what makes it the last of these three to build.
The probe is also the transfer for a non-ranging object, so caching the verdict alone
does not avoid re-fetching; that is the object-bytes layer's job.

### 6.2 Expiration and validation

| backend | validator | revalidation |
|---|---|---|
| s3 / gcs / azure | `ETag` | conditional `GET`/`HEAD` with `If-None-Match` → `304` |
| http(s) | `ETag`, else `Last-Modified` | `If-None-Match` / `If-Modified-Since` |
| local file | `(device, inode, mtime_nsec, size)` | `stat` |

Freshness, in order of preference:

1. **Declared immutable → never revalidate.** A mount or access rule sets
   `immutable = true`.
2. **Otherwise revalidate after `ttl`.** Within the TTL, serve the hit. After it,
   revalidate: a `304` or unchanged `stat` refreshes in place and keeps the parsed value;
   a changed validator evicts and refetches.
3. **No validator → TTL alone**, kept short, and reported in the metrics.

`ttl` is the staleness contract: an update is visible within `ttl`, or immediately when a
validator changes. Anything that can miss an update — including §6.7's watcher — is an
optimization on top of validation, so a missed event costs `ttl`-bounded staleness, never
a permanently wrong answer.

**Callers cannot set the TTL**; it is operator-only, in `[cache.*]` and per-mount. A
caller-set TTL would be shared state, abusable in both directions — short as a
cache-busting amplifier, long as a way to degrade another caller's freshness. Request-side
`Cache-Control` is honoured instead, being per-request by construction:

| request header | meaning |
|---|---|
| `no-cache` | revalidate before serving this caller; the `304` refreshes the shared entry |
| `max-age=N` | do not serve this caller an entry older than N; falls back to revalidation, never eviction |
| `no-store` | do not cache what this request produced |

Two guards:

- **Coalesce and floor revalidations.** Concurrent `no-cache` requests for one key collapse
  into a single conditional request, with a per-key minimum revalidation interval.
- **`immutable` wins over `no-cache`**, and the response says so in a header. The
  operator's escape hatch for a wrong `immutable` is `SIGHUP`/purge (§6.6).

### 6.3 Implementation

One `src/cache.rs` with a single generic bounded cache used by every layer. `moka` (async,
TTL + TTI, weight-based eviction) is the expected dependency; `quick_cache` if something
smaller is wanted. Each layer is an instance with its own weigher and limits.

Entries hold parsed values (`Arc<FileMetaData>`, a parsed partition list), shared by `Arc`
so a hit costs a clone.

Concurrent misses on one key must collapse into a single fetch (`moka`'s `try_get_with`).

### 6.4 Configuration

```toml
[cache]
enabled = true            # master switch; false makes every layer a no-op

[cache.hats]
max_entries = 256
ttl = "5m"
revalidate = true

[cache.parquet]
max_bytes = "512MiB"
ttl = "5m"
revalidate = true

[cache.object]
enabled = false           # §6.5
max_bytes = "1GiB"
# dir = "/var/cache/hats-api"   # absent: memory only

[cache.listing]
ttl = "30s"
```

Mounts and access rules carry `immutable` and `ttl` overrides, since freshness is a
property of the data.

### 6.5 What stays outside the process

**Object byte ranges belong in a reverse proxy.** nginx/Varnish `proxy_cache` with `slice`
solves it, and immutable objects make the key trivial. Ship a `docker-compose.yml` with
nginx in front, `proxy_cache_valid` tuned for immutable objects and `slice 1m`, plus
`docs/caching.md`. `[cache.object]` exists for deployments that cannot front the service
with a proxy, and stays off by default.

The exception is the scratch copy `materialize` makes for a non-ranging server, where
there is no ranged request for a proxy to cache. It is deleted with the request that made
it today, so a second read of the same object copies it again. **Enable `[cache.object]`
with `dir` set for that case**, under the same `max_bytes` and eviction rules as every
other layer, and have the copy outlive its request — which means the eviction budget and
`limits.max_materialize_total_bytes` become one accounting rather than two.

**Query results are not cached.** The key space is the cross product of url, predicate,
projection and format, and the hit rate on point lookups is near zero.

### 6.6 Observability and control

- Per layer: hits, misses, evictions, entries, bytes, and revalidations split into `304`
  versus changed, through §7.1's `/api/v1/metrics`.
- `X-Cache: hit | miss | revalidated` on file-server responses.
- `POST /api/v1/cache/purge`, optionally scoped to a url prefix or one layer, behind
  whatever admin gate §7 settles on; absent that, bind it to loopback.
- **`SIGHUP` empties every cache**, and later reloads the config.

### 6.7 Later: invalidation from the filesystem

For local mounts, two levels:

1. **Signal-driven.** `SIGHUP` as above: a publishing pipeline ends with `kill -HUP` and
   the next request sees the new data. No watching, no per-path bookkeeping, no platform
   differences.
2. **Watch-driven.** `notify` over inotify and FSEvents, watching each mount and evicting
   only keys under a changed path. Constraints to handle:
   - inotify watches are per-directory and capped by `fs.inotify.max_user_watches`; a HATS
     tree is tens of thousands of directories, so a recursive watch can exhaust the limit
     and stop reporting silently.
   - events coalesce, arrive out of order, and are lost on `IN_Q_OVERFLOW` — treat an
     overflow as "empty this mount's entries".
   - a write in progress fires events before the file is complete: evict, and let the next
     request refetch, rather than reading on the event.
   - network filesystems report nothing for changes made on another host. Log what is
     actually being watched at startup.
   - remote stores are out of scope; there is no push channel short of bucket
     notifications.

   Validation and TTL stay on underneath, per §6.2.

### 6.8 Prerequisite

Add a benchmark measuring the cost breakdown of a request — footer read, metadata parse,
data read, and the second footer read for `format=parquet` — against a real file. Build the
layers it justifies, in the order it ranks them. If the numbers are transfer-bound, as the
README's "Known costs" suggests, the duplicate footer read in §6.1 is the cheaper fix.

## 7. Phase 6 — operational surface

### 7.1 The basics

- `/api/v1/metrics` — Prometheus text format: request counts and latencies by endpoint and
  status, bytes fetched from stores, cache hit rates, partitions scanned.
- Request limits: a global concurrency cap and a per-request timeout, configured, returning
  429/504.
- `docker-compose.yml` for the realistic deployment: `hats-api` + nginx cache + MinIO.

### 7.2 Long requests

**No async job interface in this plan.** TAP's `/async` (§9) is what forces one, and UWS
specifies its shape, so it is built once rather than invented and then reconciled.

| slow case | decomposable? | answer |
|---|---|---|
| large region over many partitions | yes, by partition | §5.3 plan mode |
| all-columns read of one big partition | partly, by column | request fewer columns |
| materializing 2 GiB from a non-ranging host | no | prefetch, below |
| a slow or distant origin | no | timeout, reported clearly |

A job system would break §0's statelessness invariant and add job ids as an authorization
surface for §8. Raising timeouts is not an alternative: nginx's `proxy_read_timeout`
defaults to 60 seconds. Two things to build instead:

1. **Stream the response** — parquet in row-group chunks, JSON element by element. Keeps
   time-to-first-byte short, keeps bytes flowing so intermediaries do not drop the
   connection, and caps memory on large results.
2. **A prefetch primitive.** `POST /api/v1/prefetch` with a url returns `202` and warms
   `[cache.object]` in the background; `GET` on it reports residency. This is a cache
   operation, not a job: no result to store, no per-user state, nothing to expire beyond
   what the cache expires, idempotent, and a no-op if never called.

Document the limitation: against a non-ranging origin holding a huge object, the first
request after a cold start times out, and prefetch is the way around it.

### 7.3 Serve the API description

The service describes itself: `GET {api.prefix}/openapi.json` for the document, and a
browsable rendering of it at `{api.prefix}/docs`. A deployment is then self-documenting
for whoever finds it, and a client generator has something to read.

**Generated from the types, never written beside them.** `QueryRequest` already is the
schema — its fields, its `deny_unknown_fields`, the storage options, the region shapes.
A description maintained separately is one that disagrees with the service the first
time a field is added, and a confidently wrong API document is worse than none. This is
the same reason the endpoint rules are derived from `Backend` rather than listed next
to it.

`utoipa` (5, MIT/Apache-2.0, ~14M recent downloads) is the one to use: derive macros
over the same `serde` types, with `utoipa-axum` binding the routes so a route added
without a description is visible rather than quietly absent. `aide` does the same job
from the router side but is an order of magnitude less used and still pre-1.0.

- **Bundle the renderer; do not fetch it from a CDN.** `utoipa-swagger-ui` embeds its
  assets, while the `scalar` and `redoc` variants pull a script from the internet at page
  load. This service is built for networks where the browser cannot do that, and a
  documentation page that is blank in exactly the deployment it was written for is not
  documentation. It costs binary size, which is the trade being made.
- **It describes API mode only, and must say so.** The file-server mode has no route set
  to enumerate: every url under a mount is a data path. The listing response and the
  query parameters are describable, "any path below this prefix" is not, so the README
  stays the document for that half rather than OpenAPI pretending to cover it.
- **Not before §5.** §5.2 still adds `polygon` and `moc` to `region` and brings
  `POST /api/v1/hats`, and §5.3 adds the sync / plan / auto modes. Describing the API
  before those land describes a shape that then changes — the reason §4.1 waits, applied to the document that is harder to correct
  because clients will have generated code from it.
- IVOA's VOSI asks the same question in the astronomy vocabulary — `/capabilities` and
  `/availability`, arriving with TAP in §9.5. Nothing here should make serving both
  awkward: they are two renderings of one description, not two descriptions.

## 8. Security requirements

Conditions every phase must keep.

**Threat model.** The operator is trusted — they wrote the config and run the process. The
caller is not: they supply a url, storage options, credentials, a projection and a
predicate, and the service makes network and filesystem requests on their behalf. The
policy is the only thing between a caller and the machine's own network.

### 8.1 No credential leakage

Held today by `errors_never_carry_the_credentials` and the tests around it; every new
backend in §2 is a chance to break it.

- Credentials are **stripped at the boundary**: everything downstream of `storage::open` —
  logs, spans, errors, DataFusion, metrics — sees only `scheme://host/key`. New backends
  add option names to that stripping.
- **No credential in a log line**, including at `debug` and `trace`. This is not only
  about the code here. A dependency may derive `Debug` on a struct holding a secret and
  log it, and the SigV4 signer under OpenDAL does exactly that at `DEBUG`, so
  `RUST_LOG=debug` would otherwise write every caller's secret to the log. The filter is
  therefore **not purely the operator's to choose**: `logging::CREDENTIAL_UNSAFE_TARGETS`
  is silenced after `RUST_LOG` is applied, where a more specific directive wins, so
  raising the log level cannot re-enable it. Silencing a target loses its diagnostics,
  which is why it is a list with a reason per entry rather than a wildcard.
- **No credential in an error message**, including errors raised inside `object_store` or
  DataFusion, which must only ever be handed the stripped url.
- **No credential in a response**, including §5.3's plan bodies.
- **No credential in a metric label.**
- **The request body is the only source of credentials.** The service reads them from
  nowhere else: not from environment variables, not from files on disk, not from ambient
  discovery by any backend SDK (§2.2), not from an instance profile or metadata server. A
  request with no credentials is unsigned, never the process's own identity. Supporting an
  operator-configured credential source is §9, and it must be explicit in the config when
  it arrives.
- A caller must not reach another's credentials through the cache — §6.0's keying rule.

### 8.2 No local filesystem until the config says so

- Default deny, and **an empty list means none**, not "unset, so allow".
- Every path is **canonicalized before matching**, so `..` cannot climb out and a symlink
  inside an allowed directory cannot lead out of one. Without `follow_symlinks`, a path
  traversing a symlink at all is refused.
- **Refusal must not be a filesystem oracle.** Outside every allowed directory is 403
  whether or not the path exists; only inside one does a missing file become 404. Preserve
  this when adding the file-server mode.
- Derived API grants (§3.1) are never wider than the mount they come from.
- Run as an unprivileged user, and document `ReadOnlyPaths=`/`ProtectSystem=` (systemd)
  and read-only bind mounts (Docker) in `docs/deployment.md`.

### 8.3 No local network until the config says so

A single **`[access.network]`** section governs every backend, since this is a property of
the destination address rather than the protocol. Everything in it is off by default.

```toml
[access.network]
allow_loopback = false      # 127.0.0.0/8, ::1, localhost
allow_private = false       # RFC1918, fc00::/7, link-local incl. 169.254.0.0/16
allow_local_names = false   # single-label, and anything not on a delegated TLD
# allow_cidrs = ["10.1.2.0/24"]
# allow_hosts = ["minio.internal"]
```

Rules to preserve:

- **Names and addresses are two layers and both are needed.** An internal host on public
  address space defeats the address rules; writing the address down defeats the name
  rules. A name is judged before resolution, every address it resolves to after.
- **Decide on the resolved address**, which also settles literal-form tricks — IPv4-mapped
  IPv6, NAT64's embedded v4. **Every** resolved address must pass, not only the first: an
  answer mixing a public address with `127.0.0.1` is an answer built to be retried.
- **The check runs inside the HTTP client's own resolver**, whose return value *is* the
  set of addresses the connection is attempted against. That is what closes the
  resolve-then-connect gap; a check anywhere earlier is a check against an answer that
  can be replaced.
- **Never echo a remote response body into an error** — report the status code and the
  stripped url.
- Naming an endpoint in `[access.s3].endpoints` remains permission enough for it, at both
  layers. The network rules govern what a caller may reach, not what the operator
  configured.

§3.1 moves this under `[api.access.network]` along with the rest of the access table.

### 8.4 Bounded work per request

- Per request: a timeout, a cap on bytes fetched from the store, §5.3's
  `max_partitions` and `max_scanned_bytes`. `limits.max_materialize_bytes` is done.
- Per process: a concurrency limit (§7.1). `limits.max_materialize_total_bytes` and
  `limits.max_concurrent_materializations` are done, and `[limits]` is where the rest of
  these belong.
- **A timeout is still missing on every one of these paths**, materialization included.
  At multi-GiB sizes it is what a slow origin hits first, well before any byte cap, and
  §7.2 is where it lands.
- **A response cap is missing, and every field of a request is now optional.** A `POST`
  naming only a url reads a whole partition, and both writers buffer the result whole
  before sending it, so a wide file is answered out of memory. `limit` is the caller's to
  set and is no bound at all when they do not. What is needed is a row or byte ceiling on
  the result the operator sets, and §7.2's streaming, which removes the buffering that
  makes the size a memory question rather than a bandwidth one.

  **The ceiling is per format, not one number.** A row costs far more as JSON than as
  parquet — keys repeated per row, numbers as text, no encoding or compression — so a
  count that is generous for one is wrong for the other, in both directions: sized for
  JSON it refuses parquet results that would have been cheap, and sized for parquet it
  lets a JSON answer grow to something no client wants. Bytes written is the measure the
  two have in common, and a row count is the one a caller can predict, so it likely wants
  both.
- `POST` body size limit. The depth and node caps on the expressions are done, at parse
  time, and are `limits.max_expression_depth` and `limits.max_expression_nodes`. They
  bound the caller's text only; neither says anything about how many rows come back. The
  projection needs no cap of its own: it is bounded by the schema, and the byte and time
  limits govern the data it moves.
- Reject pathological parquet early — a footer claiming implausible row-group or column
  counts is a 400, not an allocation.

**What the query engine already offers**, checked against DataFusion 55 rather than
assumed, so that the next person does not go looking for it twice:

| bound | how |
|---|---|
| memory ceiling per query | `RuntimeEnvBuilder::with_memory_limit` installs a `GreedyMemoryPool`, so an over-large query fails with `ResourcesExhausted` instead of taking the process down. DataFusion's own documentation says the limit is not respected on every path, so it is a guard rather than a proof, and a byte cap is still wanted beside it |
| spilling to disk | on by default, which quietly turns a memory limit into a disk one. `DiskManagerMode::Disabled` refuses it and `with_max_temp_directory_size` caps it; either way it is a decision to make rather than inherit |
| cost before execution | `ExecutionPlan::partition_statistics` estimates rows and bytes from the footer without reading data — the same numbers §5.3's plan mode needs, from the same place |
| per-request timeout | not the engine's. `tokio::time::timeout` around the collect |
| result size | not the engine's either. Nothing in it bounds what a `collect` returns |

The last two are one piece of work with the response cap above, not three: `collect`
builds the whole answer in memory before either writer starts, so the row ceiling and
§7.2's streaming are the same change, and the timeout is what catches the case where the
row count alone never gets large enough to trip it.

### 8.5 Obligations on every new backend

1. Its option names are on the stripping list, with a test that an error mentioning a bad
   option does not carry the credential.
2. Its destination goes through §8.3's network check, after resolution.
3. It is default-deny in the config, with the same three-state shape as `endpoints`.
4. Its refusals do not distinguish "does not exist" from "not allowed" outside an allowed
   scope.
5. There is a test that the default config refuses it, and one that the narrowest config
   that should allow it does.
6. **Its SDK is checked for logging credentials**, at `trace`, with a request that
   carries them — a new backend brings a new signer and a new chance at §8.1's
   dependency problem. Anything found goes in `CREDENTIAL_UNSAFE_TARGETS` with its
   reason, which the canary test then holds to being the complete list.

Run `cargo deny` (advisories + licences) in CI.

## 9. Future development

1. **Remote mount sources.** A `[[mount]]` fronting `s3://bucket/hats/` or an HTTP tree.
   Listings become paginated `LIST` calls, static serving becomes a proxied range read with
   the origin's `ETag` passed through, and §8.3's network policy starts applying to the
   file-server mode. `[api.access]` needs a prefix rule kind first: `source =
   "s3://bucket/hats/"` is not an endpoint entry, which would allow every bucket at that
   endpoint, and a mount's derived grant must stay no wider than the mount. This is
   also the first mount that could need credentials, so it is where §8.1's rule is
   revisited: any operator-configured credential source — a config value, an environment
   variable, a file — is named explicitly in the config for that mount, never discovered
   from the process environment. It also invalidates the reasoning behind the one
   advisory `deny.toml` ignores, which turns on every key being the caller's own.
2. **A plain-url API for public data.** One `GET` whose only parameter is the location —
   `s3://bucket/hats/part0.parquet`, `https://data.example.com/x.parquet` — with the
   scheme naming the backend the way `Backend::from_scheme` already does. It is the
   one-liner a browser, a `curl` or a notebook cell can write, and it costs nothing new
   to execute: it lowers to the same request the `POST` shape carries.

   **Anonymous only, and that is what makes it a `GET`.** §3.3 is `POST` because the
   request carries credentials and a query string is written to every proxy's access log
   on the way. So this shape must refuse a credential rather than ignore one — no
   `storage` object, no headers, no url with a query string on it — and the moment
   anything here could carry a secret it goes back to being a `POST`. Refusing is the
   whole design: accepting a credential "just this once" is how one ends up in a log.

   `[api.access]` still decides what may be named; this changes how a request is written,
   not what it may reach. It takes §3.4's `columns`/`filters`, which by then every other
   shape speaks. One thing left to settle when it is built: where it sits in the url
   space, given a url nested in a url needs encoding either way.
3. **SQL, then ADQL, as front ends.** Both parse into the structured query the service
   already executes (§3.5), rather than opening a second execution path. ADQL's `CONTAINS`,
   `POINT`, `CIRCLE`, `DISTANCE` map onto §5.2's spatial predicates. Plain SQL first: it
   settles the lowering and the rejection messages before the IVOA grammar.
4. **TAP protocol.** IVOA TAP over the ADQL layer: `/sync`, `/async`, VOSI endpoints,
   `VOTable` output, the UWS job model. `/async` is a real job system with state, and is
   where §5.3's and §7.2's no-job-queue decision is revisited.
5. **Filesystem-driven cache invalidation** (§6.7): `SIGHUP` first, then a `notify` watcher
   over local mounts.
6. **Aggregating inside a nested column.** A ZTF row holds a whole light curve in
   `lightcurve.mag`, and the mean magnitude of one object is not expressible today.

   The obstacle is not the expression rules: an operation over one row's list is a scalar
   function, which §3.3's checks already allow. It is that the build registers no such
   function — `datafusion`'s `nested_expressions` feature is off, so `array_avg`,
   `array_sum`, `array_product`, `array_first`, `cardinality`, `length`, `distance`,
   `cosine_distance` and `inner_product` do not exist. Turning it on makes all of them
   callable at once, which is the decision to weigh rather than the code to write.

   **`avg(lightcurve.mag)` is not this** and must keep being refused: `avg` summarizes
   rows, so it averages the column down the file rather than along one light curve. The
   two read almost the same and mean entirely different things, so whatever is added has
   to be named so that a caller cannot reach for one and get the other.

   `array_filter`, `array_transform` and `array_any_match` take lambdas, which §3.3
   refuses as expression kinds. Either they stay refused — leaving a feature registered
   but unreachable, which needs saying in the error rather than a bare "not supported" —
   or the lambda arms are reconsidered, which is a wider decision than this item.
7. **Separate crates, separate repos.** Once ADQL and TAP exist, split into `hats`, `adql`
   and `tap` so each is usable without the others.

   `hats` is the catalog itself, not this service's use of it: the properties file, the
   partitioning, `Norder`/`Npix`/`Dir` addressing, the MOC, `_metadata` and
   `partition_info.csv` — what the Python `hats` library covers, in Rust, for anyone
   reading a HATS catalog with no service in front of it. §5.1 and §5.2 are where that
   code gets written, so the split is a matter of where it lives rather than of writing
   it twice.

Each phase leaves the service useful, and each is a prerequisite for the next rather than a
parallel track.
