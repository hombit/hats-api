# Development plan

## Progress tracker

Updated in the same commit as the code. A step is `done` only when its tests pass,
`cargo clippy --all-targets` and `cargo fmt --check` are clean, and §0 still holds.

Status values: `todo`, `in progress`, `done`, `deferred` (with what it waits for) and
`dropped` (with the reason). See `CLAUDE.md` for what goes in this file and what does
not.

**A step that is `done` keeps its row and loses its section**, so the numbering below
skips. What is written out is what is still to be decided; the rules a finished step left
behind are in `CLAUDE.md` and what it built is in the README.

| § | step | status | notes |
|---|---|---|---|
| 2.1 | OpenDAL backend layer, s3 migrated first | done | |
| 2.2 | GCS and Azure | done | |
| 8.3 | network policy | done | |
| 2.3 | HTTP/HTTPS, range probe, materialization | done | |
| 2.4 | WebDAV | done | |
| 2.5 | Hugging Face | done | the redirect hop is `storage/redirect.rs` and is one backend's; OpenDAL's `services-hf` is not used |
| 3.1 | two-mode configuration | done | |
| 3.2 | routing | done | |
| 3.3 | API request shape (`columns`/`filters`, `region`) | done | what `region` may still gain is §5.2 |
| 3.4 | file-server request shape | done | |
| 4 | file-server interface | done | never tried against a real `lsdb` client, which is the one check it cannot do by reading |
| 4.1 | write the README | done | |
| 4.2 | say the ordering guarantees in the user documentation | done | |
| 4.3 | what the engine costs | done | `target_partitions` under a `limit` is the one knob a request would want to set for itself |
| 4.4 | a directory page worth looking at | done | |
| 5.1 | HATS catalog metadata | done | |
| 5.2 | spatial predicate | done | `polygon` is §9 |
| 5.3 | two endpoints, rows and plan | done | |
| 5.4 | a catalog under a mount | done | |
| 3.5 | a mount over a store | done | `source` takes any url `storage::open_dir` reads, with the mount's own `storage` beside it; serving one is a ranged read and a delimited listing against the origin. `[[tap.table]]` names a path under a mount rather than a url, so a published catalog can need a credential |
| 7.3 | serve the API description | done | |
| 7.4 | compress JSON responses, never parquet | done | |
| 8.4 | a clock on every request | done | `[limits] max_request_seconds`; the rest of §8.4 is not done |
| 7.5 | VOTable output | in progress | flat columns answer; a nested one is refused by name until §7.5's four decisions are made |
| 6.5 | request cost benchmark | todo | prerequisite for the rest of §6 — it ranks the layers |
| 6 | caching | todo | build in the order §6.5 ranks |
| 7 | operational surface | todo | |
| 10.2 | `box` renamed `zone` | done | |
| 10.1 | the ADQL request shape | todo | |
| 10.3 | the statement planned, over parquet tables | done | every function ADQL makes mandatory is answered; the geometry it leaves optional is §10.7's |
| 10.4 | HATS catalogs as tables | done | partitions pruned by `PruningPredicate` over each cell's span, not by recognising a region |
| 10.5 | one large table and small ones | todo | |
| 10.6 | two large catalogs | todo | a crossmatch is answered as a nested-loop join; this is making it an equijoin once the left row is expanded to cells, with three things to measure first |
| 11.0 | the conformance suite | done | `pyvo` and STILTS `taplint` against a built service, in CI as a report rather than a gate. Written before any of §11, so none of it is tuned to what was built |
| 11.1 | the tables the service publishes | done | temporary until §9.3, and §0.2 holds only while the list is config |
| 11.2 | `/sync` and the parameters | done | an unrecognised parameter is ignored, which the validator checks for and the plan had wrong |
| 11.3 | VOTable, `MAXREC`, `OVERFLOW`, errors | done | `MAXREC` truncates *after* the query's own `TOP` rather than overriding it, and `MAXREC=0` carries the marker — both the other way round in the plan |
| 11.4 | `TAP_SCHEMA` | done | a name is matched the way ADQL says rather than exactly, which is what §10.8.1 now diverges from only on the `simple` routes |
| 11.5 | VOSI capabilities, availability, tables | done | |
| 11.6 | `csv` and `tsv` | done | |
| 11.13 | a region over `Float32` coordinates | done | |
| 11.7 | Simple Cone Search, 1.03 and 2.0 | todo | after §11.11. Days, TAP having paid for all of it. 1.03 inherits none of DALI — its own error shape, UCD1, no `MAXREC`; the 2.0 draft inherits nearly all of it and adds `TABLE` |
| 11.8 | `/async` and UWS | todo | tier 1, and the only thing in it. Every reference service has one. The shape is settled — a job is a record, a store trait over it, the runner process-local — and §0.2 is what it reopens. Was §9.4 |
| 11.9 | `/examples` | deferred | waits for §6.1's catalog metadata cache. A menu TOPCAT offers, not something a client needs to work, and every example in it is generated from a published catalog |
| 11.10 | what a caller gets told | todo | later. Its own page; TAP takes form parameters and `/docs` describes JSON bodies |
| 11.11 | table upload | in progress | a url as `UPLOAD`, queried as `TAP_UPLOAD.name`, with this service's own `UPLOAD_STORAGE_OPTION` and `UPLOAD_TYPE`, is built. Inline VOTable upload stays later — the one capability the four reference services do not share |
| 11.12 | `parquet` and `json` over TAP, and a nested column in `TAP_SCHEMA` | todo | later. No reference service can be asked about either; the nested half waits on §7.5 |
| 11.14 | a DALI parameter value, read once and typed | done | `tap::dali`, a `serde` data format; the upload parameters are read through it, and §11.7's shapes are written as types over the same reader |

§2–§7, §10 and §11 are the phases in order, §8 the conditions every phase must keep, §9
what is deferred.

## 0. Invariants

1. **The service only ever reads.** No endpoint writes, no mount is writable. Mounts
   therefore carry no `readonly` flag. Adding writes is a new document, not a new flag.
2. **Stateless per request.** No session cache, no catalog registry across requests.
   §6's caches must be evictable, bounded, and correct when empty.
3. **Nothing reaches an object store without passing its mode's policy.**
   `AccessPolicy::authorize` in `storage::open` is the only route to a store, and a
   `file://` url reaches a mount's `path` and nothing else.

## 5. Phase 4 — the HATS interface

### 5.1 Catalog metadata

**A query on `_metadata`'s own url has no answer.** It is on `[data] filenames`, so a
caller reaches one today and gets a 400 — the rows its footer describes are in the files
beside it. The partition list and the per-partition statistics are both available to answer
with; decide whether it gets one, since what it would return is metadata rather than rows.

**A projection could be checked before any partition is read.**
`dataset/_common_metadata` is the schema and nothing else, so one small `GET` would say
whether `columns` names a column the catalog has. Worth having once there is a reason to
pay for the request: today the first partition's footer answers on the way to reading it.

Nothing is cached, so every request against a catalog pays two `GET`s before a row.

### 5.2 The spatial predicate

**`region` may still gain two things**, both additions rather than corrections:

- **`moc: {url: …}`, a MOC fetched rather than sent.** A caller-named fetch, so it needs
  §8.3's rules deciding it, a bound on what may be pulled down, and an answer to whether a
  plan echoes the url — cheap, and each entry re-fetches — or the cells it resolved to,
  which is self-contained and large.
- **Intersection and difference**, as explicit combinators over the array, which is a
  union today.

**A `moc` against a file with no HEALPix column is refused.** The refusal is honest and
every HATS partition has the column, so the two ways to make it answer are optimizations
waiting for a reason. A `ScalarUDF` computing a cell per row gives the right rows and
prunes nothing — written once and taken out again as too much machinery for that;
`ScalarUDFImpl::preimage` is what would make it prune and is the thing to look at first.
Coordinate bounds derived from the MOC's own cells *would* prune, and need the cell walk
bounded by degrading the MOC to a coarse depth first, plus a pad of the cell's own
diameter, a cell's extreme latitude not being at a vertex.

**A partition that is a directory cannot be read over `http(s)://`,** and says so as
whatever the listing failed with rather than as a refusal naming the reason. The names
inside such a partition are in none of the catalog's metadata, so an origin with no listing
operation has no second chance.

Nearest-object lookup is a `circle` plus ordering and `limit: 1`, and waits for ordering.
`crossmatch` is `lsdb`'s job.

### 5.3 Small queries and large queries

**A file-server plan has no route.** `method` and `path` are separate fields so entries
could be `GET`s under a mount, and nothing emits them. The catalog page's Plan button posts
to the API's plan route, so a mount with the API off offers no plan at all — which is what
a route of `GET` entries would fix, if anyone wants one.

A plan is a list of stateless requests, and stays one whatever §11.8 builds: it answers a
request too large for one response with the requests that would do it, which is a different
answer from one that runs the whole thing somewhere else and hands over an id. See §7.2.

### 5.4 A catalog under a mount

Three things a url deliberately does not do, each waiting for someone to want it: a `box`
(four numbers would fit), an offset (`engine::query::Order` promises nothing within a partition, so
it would have to say what it is an offset into), and a `catalog` field in the JSON listing
so a client need not recognise a catalog from the names the way this service does. A
collection has no `dataset/_common_metadata`, so the page falls back to the schema on the
first answer rather than following it to its primary table.

## 6. Phase 5 — caching

**§6.5 comes first.** Which layers are worth building is a measurement, and the design
below is written only as far as the decisions that are already made.

### 6.0 Invariants

1. **A cache is never a source of truth.** Empty it at any moment and every request still
   returns the same answer, slower.
2. **The access policy is consulted before the cache**, or an entry outlives the config
   that admitted it.
3. **The key includes everything affecting the bytes, credentials included** — as
   `HMAC(per-process key, credential set)`, never a bare hash of a low-entropy secret.
4. **Credentialed entries are always revalidated**, never served on TTL alone: the
   conditional request carries the caller's own credentials, so a revoked one fails at the
   origin. Keying gives isolation between callers; only revalidation catches a credential
   that has stopped being valid.
5. **Bound every layer twice**, by entries and by bytes, and make every layer emptiable at
   runtime.
6. **The TTL is the operator's, never the caller's.** A caller-set TTL is shared state,
   abusable short as a cache-busting amplifier and long as a way to degrade someone else's
   freshness. Request-side `Cache-Control` — `no-cache`, `max-age`, `no-store` — is
   per-request by construction and is honoured instead, with concurrent revalidations
   coalesced and floored, and `immutable` winning over `no-cache`.

### 6.1 The layers

| layer | holds | keyed by | bound |
|---|---|---|---|
| **HATS catalog metadata** | parsed `properties`, partition list, derived MOC | catalog url + validator | entries |
| **Parquet file metadata** | `FileMetaData` footer, page index, bloom filter headers | object url + validator | bytes |
| **Range-support verdict** | does this object's server honour `Range`? | object url | entries, short TTL |
| **Object bytes** | whole objects or ranges | object url + validator (+ range) | bytes; off by default, §6.4 |
| **Directory listings** | one page of a listing | prefix + validator | entries, short TTL |
| **Negative results** | 404 for a missing object only — a 403 is recomputed, the policy behind it being reloadable | url | entries, very short TTL |

Validators: `ETag` for the object stores, `ETag` else `Last-Modified` over http(s), and
`(device, inode, mtime_nsec, size)` for a local file. Declared `immutable` never
revalidates; otherwise a TTL bounds staleness and a changed validator evicts. Anything that
can miss an update — §6.6's watcher — is an optimization on top of that, so a missed event
costs `ttl`-bounded staleness rather than a permanently wrong answer.

**The catalog metadata layer has a reader that answers no query.** `/tables`, `TAP_SCHEMA`
and §11.9's examples page are each built out of the properties, the partition list and
`dataset/_common_metadata` of every published table, and a client fetches all three before
it has asked for a row. Those reads are the whole cost of those resources, so the layer is
what makes them cheap rather than what makes them faster.

**The first two are the ones worth building, and the reason is measured.** A
`format=parquet` request reads the source footer twice: DataFusion fetches it while
inferring the schema and serves the scan from its own `FileMetadataCache`, while
`output::parquet::read_layout` goes to the store and reads it again through a fetcher of
this crate's own, which knows nothing about that cache.

**Nothing here should be reading metadata itself**; the read belongs to DataFusion, which
has already done it. The obstacle is reach: the cached entry is an `Arc<dyn FileMetadata>`
whose only accessor is `as_any`, and the concrete type lives in
`datafusion-datasource-parquet`, which the facade does not re-export — so it means taking
that crate as a direct dependency, version-locked the way `object_store` already is. Decide
that here rather than paying it for one call site.

The range-support verdict is keyed by object, not by host — one server hands back static
files that range and generated responses that do not. It saves only the probe on a second
read, and the probe is also the transfer for a non-ranging object, so it is the last of
these to build.

### 6.2 Implementation

One `src/cache.rs`, a single generic bounded cache, one instance per layer with its own
weigher and limits. `moka` (async, TTL + TTI, weight-based eviction) is the expected
dependency, `quick_cache` if something smaller is wanted. Entries hold parsed values shared
by `Arc`, so a hit costs a clone, and concurrent misses on one key must collapse into a
single fetch.

Configuration is `[cache]` with a master switch and a section per layer; mounts and access
rules carry `immutable` and `ttl` overrides, freshness being a property of the data.

### 6.3 Observability and control

Per layer: hits, misses, evictions, entries, bytes, and revalidations split into `304`
versus changed, through §7.1's metrics. `X-Cache: hit | miss | revalidated` on file-server
responses. A purge endpoint behind whatever admin gate §7 settles on, or bound to loopback.
**`SIGHUP` empties every cache**, and later reloads the config.

### 6.4 What stays outside the process

**Object byte ranges belong in a reverse proxy** — nginx or Varnish with `slice`, which
immutable objects make trivial to key. Ship a `docker-compose.yml` and `docs/caching.md`
for it. The in-process layer stays off by default and exists for deployments that cannot
front the service with a proxy.

The exception is the scratch copy `materialize` makes for a non-ranging server: there is no
ranged request for a proxy to cache, and the copy dies with its request, so a second read
copies it again. Turning the object layer on with a `dir` is that case's answer, and it
merges the eviction budget with `limits.max_materialize_total_bytes` rather than leaving two
accountings.

**Query results are not cached.** The key space is url × predicate × projection × format,
and the hit rate on point lookups is near zero.

### 6.5 Prerequisite

Measure the cost breakdown of a request — footer read, metadata parse, data read, and the
second footer read for `format=parquet` — against a real file, and build the layers it
justifies in the order it ranks them. `tests/engine.rs` is the harness and already counts
requests, which is what settled the duplicate footer above; what it does not do is
attribute *time* to each stage, and its numbers are against a local file, so they are the
floor. The ranking needs an origin with latency in it.

### 6.6 Later: invalidation from the filesystem

`SIGHUP` first — a publishing pipeline ends with `kill -HUP`, and there is nothing to watch
and no platform differences. A `notify` watcher over local mounts is the second level, and
the traps are the reason it is second: inotify watches are per-directory and a HATS tree
can exhaust `fs.inotify.max_user_watches` and then stop reporting silently; events coalesce
and are lost on overflow, which has to mean "empty this mount"; a write in progress fires
before the file is complete, so evict rather than read on the event; and a network
filesystem reports nothing at all for another host's changes. Remote stores are out of
scope — there is no push channel short of bucket notifications. Validation and TTL stay on
underneath either way.

## 7. Phase 6 — operational surface

### 7.1 The basics

- `/api/v1/metrics` — Prometheus text: request counts and latencies by endpoint and status,
  bytes fetched, cache hit rates, partitions scanned.
- A global concurrency cap, configured, returning 429. The per-request clock is done.
- `docker-compose.yml` for the realistic deployment: `hats-api` + nginx cache + MinIO.

### 7.2 Long requests

**The job interface is TAP's, and it is §11.8.** UWS specifies its shape, so it gets built
once there rather than invented here and then reconciled — and the answers below are what a
request outside TAP gets, none of them being a job. Everything in this section stands
whether or not §11.8 has landed: a job is a second answer to a slow request and not a
replacement for making one fast.

| slow case | decomposable? | answer |
|---|---|---|
| large region over many partitions | yes, by partition | §5.3 plan mode |
| all-columns read of one big partition | partly, by column | request fewer columns |
| materializing 2 GiB from a non-ranging host | no | prefetch, below |
| a slow or distant origin | no | the clock, and a message that says which bound |

Raising the clock is not an alternative: nginx's `proxy_read_timeout` defaults to 60
seconds, shorter than this service's own default. Two things to build instead:

1. **Stream the response** — parquet in row-group chunks, JSON element by element — which
   keeps time-to-first-byte short, keeps bytes flowing so intermediaries do not drop the
   connection, and caps memory on a large result. Under the file-server's order it is
   available a partition at a time. It moves two trades already shipped: compression
   buffers before it emits, so fewer bytes cost time-to-first-byte; and the clock bounds
   the response future rather than the body, so a request that has begun streaming is one
   the clock stops measuring.
2. **A prefetch primitive.** `POST /api/v1/prefetch` returns `202` and warms the object
   cache; `GET` reports residency. A cache operation and not a job: no result to store, no
   per-user state, idempotent, a no-op if never called. Document the limitation it exists
   for — against a non-ranging origin holding a huge object, the first request after a cold
   start runs out the clock.

### 7.3 The API description, as it changes

`region` may still gain `moc: {url}` and the combinators (§5.2), and §7.2's streaming would
change how a large answer arrives. Both change the document, and clients will have
generated code from it by then, so each needs its shape decided before it lands. IVOA's
VOSI asks the same question in the astronomy vocabulary and answers it already, under
`{api.prefix}/tap/tables` — two renderings of one description, not two descriptions.

### 7.5 A nested column in a VOTable

`format=votable` answers a flat table and refuses a struct or a list column by name. The
nested half is a set of decisions rather than a piece of code: the shape it lowers to is
not in the standard, and every alternative puts a value in an answer that a reader cannot
tell from a different value.

The shape to follow is
[this notebook](https://github.com/lincc-frameworks/notebooks_lf/blob/main/lsdb/busy_week_2025/VOTable-example-for-hats.ipynb):
a `GROUP` carrying the column's name and a `FIELDref` per subfield, beside flat `FIELD`s
named `diaSource.band`, each holding one row's whole array. Four things it does not settle.

- **An array of strings has no spelling.** VOTable's only form is `arraysize="8x*"` — a
  variable number of *fixed-width* strings, space-padded — so the width has to be measured
  over the answer first, and a value with real trailing spaces comes back trimmed. The
  notebook hits this and leaves it. A band is the ordinary case, so this one blocks the rest.
- **A null inside an array.** A float has `NaN` and a boolean has `?`. An integer has only
  `VALUES`'s `null`, a magic value a real measurement can equal, so it needs a pass to find
  one nothing uses and a refusal when there is none. A string has nothing at all.
- **Both arrow shapes are one VOTable.** `List<Struct<…>>` and `Struct<List<…>, …>` — which
  is what nested-pandas writes — produce the same `GROUP`. Writing them as two cases is how
  they come to disagree about a null at the struct level, which belongs to neither field.
- **Depth is refused, not flattened.** A struct in a struct, or a list of lists, has no
  `GROUP` to become.

Beside it, once that is settled: the same notebook's actual subject is **VOTable-in-Parquet**,
a whole VOTable header in the file's key/value metadata under
`IVOA.VOTable-Parquet.content`. A catalog carrying one has already said what its columns'
`unit`, `ucd` and `DESCRIPTION` are — metadata no answer here can otherwise have, and what
would make this output worth reading in an IVOA client. It needs a VOTable *parser*, which
nothing here has; `votable` on crates.io is the CDS implementation, weighed once for the
writing side and turned down, so the reading is where it earns its dependencies.

## 10. Phase 7 — ADQL

IVOA's query language over the catalogs this service already reads, at one endpoint:
`POST {api.prefix}/v1/adql`. What it buys is that a TAP client and an `lsdb` user ask the
same data the same question, in the spelling every other astronomy service takes.

**DataFusion plans the statement; this service translates it.** ADQL is SQL with a handful
of spelling differences, so the route rewrites the parsed statement where the two disagree,
refuses what neither would answer correctly, and hands DataFusion a syntax tree over tables
registered by the caller's names. Grouping, ordering, joins, subqueries and set operations
are the planner's, and nothing here re-decides what they mean. It is a different execution
path from the `simple` routes on purpose: those fan a selection out over
partitions and make promises about order and work lists that a planned statement does not.
Where the two share code it is because the code is the same thing — the region covering, the
storage layer, the answer writers — not to keep one path.

The region test is DataFusion's too. `point`, `circle`, `moc` and `contains` are scalar
functions, and `contains` replaces its own call during the optimizer's simplify pass with the
expression `sky::region::predicate` already builds, so a region said in a statement prunes row
groups exactly as the `region` field does. `simplify`, not `preimage`: the latter answers
with one contiguous interval and a covering is many.

Three stages after the first, each opening something the one before could not express.
**The work lands as one stack of pull requests that merges together**; a piece that nothing
calls — the functions with no route registering them — is not merged on its own. **Stopping
after any stage leaves a coherent service**, and no stage advertises the next one's
capability.

References: [ADQL 2.1](https://www.ivoa.net/documents/ADQL/20231215/REC-ADQL-2.1.html),
[TAP 1.1](https://www.ivoa.net/documents/TAP/20190927/REC-TAP-1.1.html), the
[UDF catalogue](https://www.ivoa.net/documents/udf-catalogue/20240807/PEN-udf-catalogue-1.2-20240807.html).
MOC is in none of them: it is slated for ADQL 2.3, and the only shipping precedent is
DaCHS, whose spelling §10.7 follows.

### 10.1 The request

```json
{
  "query": "SELECT TOP 100 source_id, ra, dec FROM gaia WHERE 1 = CONTAINS(POINT(ra, dec), CIRCLE(45.0, -20.0, 0.1))",
  "tables": {
    "gaia": {"type": "hats", "url": "s3://bucket/gaia_dr3", "storage": {}, "region": {}}
  }
}
```

- **The caller names the tables, and the request is the only place they come from.** A table
  entry is what the `parquet` and `hats` bodies already take — `url`, `storage`, `region` —
  under a `type` that says which of the two it is, so the serde types and their
  descriptions are reused rather than restated. A url may be `file://` resolving through a
  mount, or remote, exactly as it may today. That keeps §0.2: nothing is registered between
  requests.
- **A table name is an ADQL regular identifier**, and it answers to its own spelling and to
  its lowercase, the way a column does on this route (§10.8). `TAP_UPLOAD` and `TAP_SCHEMA`
  are refused as names, both being TAP's own.
- **A table's `region` is a view definition** — "this table is that catalog restricted to
  this shape" — and it is the only `region` in the request. There is no request-level one:
  a caller writing ADQL says where they are looking in the `WHERE` clause, and a second
  spelling beside it would be two ways to say one thing. A table region and a predicate
  region compose as the intersection, which needs no rule because it is what `AND` means.
- **`RESPONSEFORMAT` is not taken; the existing `format` is.** VOTable, JSON and parquet are
  already answered per §7.5 and are what a TAP layer will need anyway.

Whether the `simple/hats` routes later move onto `hats_table`'s provider is deferred, not
assumed: they promise an order and answer with a work list, and it does neither.

### 10.5 Stage three — one large table and small ones

Several tables where one is a catalog and the rest fit in memory. **This needs no new
operator**: DataFusion's hash join with the small side as the build side is a broadcast join
already, and the two cases originally imagined — small tables only, and one large among
small ones — are one piece of work, the first being the second with nothing large in it.

What is new is deciding *which* side is small, and refusing rather than discovering. A
declared size is a fan-out hint and not a cost (§5.3), so the build side is either bounded by
the memory pool and allowed to fail, or bounded up front by the entry's own kind — a
`parquet` table is small, a `hats` one is not. Settle that before building it.

### 10.6 Stage four — two large catalogs

**A crossmatch is answered already, and what is left is making it scale.** ADQL's own
spelling — a circle centred on the other side's row — plans as a `NestedLoopJoinExec` over
whatever each side's own region left, which is the right answer wherever both sides are
narrow and the wrong shape as soon as one is not. So this stage is a plan for the same
query, not a new surface, and nothing in it changes what a caller writes.

The shape: an ordinary equijoin. Each left row is expanded into the order-*k* cells its
match disk touches, the sides are joined on that cell, and the separation is the residual
filter:

```
left row  →  cells covering the disk of radius r around it   -- one column of lists, unnested
join on   left.cell = right.cell
filter    DISTANCE(left.ra, left.dec, right.ra, right.dec) < r
```

**Nothing spatial is asked of the planner.** DataFusion has no spatial join and no range
join: a `BETWEEN` across two tables plans as a `NestedLoopJoinExec`, which is the whole
build side in memory and a scan of it per probe row, and `PiecewiseMergeJoinExec` — the
nearest thing — takes a single inequality and is experimental, which `CLAUDE.md` already
records for the filter case. What the expansion does is remove the need for one. HEALPix is
nested, so a
cell at order *k* is a prefix of every cell inside it and **a range at order *k* is an
equality at order *k***; both sides reduced to that column is a hash join, partitioned, with
no whole build side and DataFusion's own memory accounting under it. The right side's key is
one shift of an existing column, `_healpix_29 >> (2 * (29 - k))`.

**Never enumerate below the join order.** One order-5 cell holds 4²⁴ order-29 values, so the
expansion is of a *cell's neighbourhood at order k* and never of a range at the column's own
order. That is the one way to write this that does not work, and it looks reasonable.

**Expanding the left is what makes the margin disappear.** A pair either side of a cell edge
has different cells at every order, which no prefix trick reaches — it is the margin
problem. Covering the left row's whole disk answers it without touching the right side:

- **No duplicates, structurally.** A right row is in exactly one cell, so a pair can meet in
  exactly one cell. Nothing to deduplicate, and it is the right side staying unreplicated
  that guarantees it rather than an argument about join order.
- **The fan-out is adaptive.** `cone_coverage_approx` at order *k* returns one cell for an
  interior row and two to four near an edge — paid where the geometry needs it, not as a
  blanket neighbour cost.
- **The large catalog is read once and unmodified.** A margin catalog, where one exists, is
  this query reading less: an optimisation over it, not a different design.

Choose *k* so a cell is larger than *r*; a large radius forces a coarse *k* and therefore
large partitions, which is the trade-off to state rather than tune.

Three things to measure before building it, none of which affects correctness:

1. **Whether the declared partitioning is used**, or `EnforceDistribution` re-hashes both
   sides anyway. A shuffle we did not need, if so.
2. **What a per-row covering costs.** Fine over millions of left rows, not over billions.
   The fallback is a distance-to-edge test: one cell for an interior row, the covering only
   near a boundary.
3. **Whether building the list column needs `nested_expressions`.** It is off, and
   `make_array` is behind it; a UDF returning a `ListArray` should sidestep that, `Unnest`
   being a plan node rather than an array function. Worth confirming, because turning the
   feature on makes every array function callable at once — which is §9.5's decision and
   not this one's.

It is a recognised query shape rather than general `JOIN` support. Anything outside it is
refused, naming what it would have cost.

### 10.7 What ADQL asks for, and what is refused

**The geometry functions are an optional feature** — `ivo://ivoa.net/std/tapregext#features-adqlgeo`
— and TAPRegExt declares them one form at a time, so the service advertises `POINT`,
`CIRCLE`, `CONTAINS`, `INTERSECTS` and `DISTANCE` without owing the rest. Optional too, for
whoever goes looking: `LOWER`/`UPPER`/`ILIKE`, common table expressions, set operations,
`CAST`, `COALESCE`, `OFFSET`, `IN_UNIT`, and every UDF.

Refused, each with a message that says it is refused rather than unsupported: `POLYGON`
(§9.2), `BOX` (a centre with great-circle edges, which the `zone` region is not), `REGION`
(an STC-S parser, deprecated in ADQL 2.1), `COORDSYS` and `ivo_geom_transform` (frame
transforms), `IN_UNIT` (a units library), `AREA` and `CENTROID` (they want geometries as
values, and no file here has a geometry column).

Added because they are cheap here and expensive elsewhere:

- **`MOC('4/30-33 38 52 7/324-934')`**, DaCHS's spelling, as a region inside `CONTAINS`. It
  is `Shape::Moc` once `contains` rewrites itself, the one shape matched exactly and the
  cheapest predicate the service has. DaCHS cannot compare a point to a MOC directly; against
  a HEALPix-indexed catalog it is what we are fastest at.
- **`ivo_healpix_index(order, ra, dec)` and `ivo_healpix_center(order, index)`**, a few lines
  of `cdshealpix` from the UDF catalogue.

**Four function names do not map by spelling**: `CEILING`, `TRUNCATE` and `LOG` are `ceil`,
`trunc` and `ln`, and `MOD(x, y)` is `x % y`. The mapping is written out and tested, never a
passthrough by name. Every other ADQL name needs nothing, DataFusion lowercasing an unquoted
function name itself. `LOG` is the one that matters: ADQL's is natural and DataFusion's `log`
base ten, and since `log` is refused as ambiguous, a passthrough written by accident is an
error rather than a number 2.3 times off.

**Still to add, each small and none blocking a stage:**

- **`INTERSECTS` between two shapes.** Against a point it is `CONTAINS`; between a circle and
  a MOC it is a covering intersection nothing builds yet (§5.2).
- **`LOWER`, `UPPER`, `ILIKE`.** `string_expressions` is off, and turning it on is `CLAUDE.md`'s
  deliberate decision about everything in that feature, not only these three.

### 10.8 Three divergences to write down

Each is a place this service deliberately answers differently from the specification, and
each belongs in `CLAUDE.md` and in the route's own description once built — found where
someone will look, not here, since this file is deleted when the work in it is done.

1. **An identifier answers to its own spelling and to its lowercase.** ADQL folds unquoted
   identifiers to uppercase. Astronomy column names are mixed-case as a matter of course and
   a caller reads them off the file, so the existing rule wins and applies to table names too.
2. **A bound reached returns no rows at all.** TAP truncates at `MAXREC` and marks the result
   `QUERY_STATUS=OVERFLOW`; rows cut off are a value a caller cannot tell from the whole
   answer, so this service refuses instead. The TAP route is where that was revisited: there
   `OVERFLOW` is an in-band statement that the answer is partial, and the row bound truncates.
3. **`RAND` and an unordered `TOP` are the first answers here that are not reproducible.**
   ADQL says nothing about which rows `TOP n` returns without an `ORDER BY`, so an arbitrary
   set conforms — but every other route promises more than that, and a reader will carry the
   stronger assumption across unless it is written down.

## 11. Phase 8 — the IVOA interfaces

IVOA's Table Access Protocol over §10's ADQL layer, so that TOPCAT, `pyvo` and `astroquery`
reach these catalogs as they reach any archive. The resources are siblings
under one base url, `{api.prefix}/tap`, which is what the specification requires of every
resource but `/availability` and what a client builds its urls from by appending fixed names.

Simple Cone Search (§11.7) rides along on the same plumbing, which is the only reason it is
in this phase rather than a phase of its own: it is a different protocol from a different
decade and shares no document with TAP but the VOTable.

References: [TAP 1.1](https://www.ivoa.net/documents/TAP/20190927/REC-TAP-1.1.html),
[DALI 1.1](https://www.ivoa.net/documents/DALI/20170517/REC-DALI-1.1.html),
[VOSI 1.1](https://www.ivoa.net/documents/VOSI/20170524/REC-VOSI-1.1.html),
[TAPRegExt 1.0](https://www.ivoa.net/documents/TAPRegExt/20120827/REC-TAPRegExt-1.0.html).

**These are not four things to build; TAP is written on top of the others and defers to
them constantly.** Most of what is built here is DALI, and reading TAP alone leaves
the actual requirement unread. `RESPONSEFORMAT` is "fully described in DALI" (TAP §2.7.3),
and it is DALI §3.4.3 that says a service *should fail* where the format asked for is one
it does not support. The error document is TAP §3.3 saying "see DALI for details", which
is DALI §4.2 — and §4.4 is where `QUERY_STATUS` lives and where the `OVERFLOW` marker is
put after the table. `MAXREC`, `RUNID`, case-insensitive parameter names and repeated
parameters are all DALI §3. What `/sync` is, as a resource, is DALI's DALI-sync pattern.

The one part of DALI that is async's alone is the DALI-async pattern, which is what hands
off to UWS — so it arrives with §11.7 and nothing before it.

**Follow the reference before writing the check or the code.** Four established services
ignore DALI §3.4.3 and answer an unsupported `RESPONSEFORMAT` with a VOTable, which read
as "the check is too strict" until the sentence was actually looked up; and two checks
here demanded a 4xx where TAP §3.3 explicitly permits a 200 carrying an error document.

**Each step below is measured rather than argued about.** `tap-conformance/` puts `pyvo`
and STILTS `taplint` to a built service and reports which parts of the standards answer.
It exists already and every step here moves its numbers; a step is not finished because
the code reads right. Two things about it constrain what follows:

- **What the suite calls a failure is not always this service's.** A check no established
  TAP service passes is a check to re-read before treating it as a requirement, which is
  what the survey against reference services is for. `tap-conformance/REFERENCE_SERVICES.md`
  is the list as it stands, and it is unreviewed.
- **Conforming and being usable are two results, not one.** The report counts them apart,
  because a document can carry everything the standard asks for and still be one a client
  cannot parse.

### What is left, and why in this order

The suite was run against four separate TAP implementations — ESA Gaia, ARI-Gaia, IRSA and
MAST — before any of this was written, and what they have in common is what set the order.
A specification marks everything MUST or SHOULD and cannot say which of it a client
actually needs; four independent services agreeing does say so.

**Tier 1 is `/async`, alone.** All four implement it, so by that rule it belongs with
everything already built and is held back by one thing only: it is a job model, which is
state, against a service whose every answer today is collected inside one request future.
It is the one piece of this phase that is a design question rather than a mapping.

**Everything else is later**, with one piece taken out of it and moved ahead of tier 1:
naming a catalog by url as `UPLOAD` (§11.11), which is the only way a TAP client can ask
about a catalog this service does not publish, and which `/adql` already answers in its own
body. Inline upload stays later, IRSA offering none and MAST half, so the ecosystem has not
settled it and the four agree only on *declaring* what they have. `/examples` is a menu TOPCAT offers rather than something a client needs to work. The
formats this service has of its own — `parquet`, `json` — and how a nested column is
declared are questions no reference service can be asked, because none of them has such a
column.

**Until Tier 1 lands this is deliberately not a conforming TAP service, in exactly one
place.** `/async` is a MUST (TAP §2.2), and what that costs is in the README rather than
left to be discovered from a validator: a query too slow for `max_request_seconds` has
nowhere to go, because the resource a client would be sent to does not exist, and the
answer is to make the query smaller.


### 11.7 Simple Cone Search, both versions

[Simple Cone Search 1.03](https://www.ivoa.net/documents/REC/DAL/ConeSearch-20080222.html):
`RA`, `DEC` and `SR` in decimal degrees, ICRS, and a VOTable of the rows inside that cone.
That is the whole protocol.

[SCS 2.0](https://github.com/ivoa-std/SCS2) is built on DALI, so it inherits nearly all of
what TAP already answers where 1.03 inherits none of it — `MAXREC`, `RESPONSEFORMAT`, DALI
error documents, VOSI `/capabilities` and `/tables` beside the query endpoint, and UCD1+. Its
one genuinely new idea is `TABLE`, which breaks 1.03's identity of one service with one
table and makes the url space look like TAP's rather than like 1.03's.

It is a Working Draft, which is a fact about maintenance rather than a reason to wait: the
checks for it each name the clause they came from, so when the draft moves, what has to
move with it is findable.

So the two are not one piece of work done twice. **1.03 is the odd one**, and the list below
is what *it* does not inherit; SCS2 costs a parameter and a second set of capabilities.

**It is cheap because TAP paid for it.** A cone predicate, HATS partitions pruned by it, a
VOTable writer and a list of published tables with known coordinate columns are every part
of it, and all four are built. On its own it would have been a thin slice of the same
plumbing; now it is a route that parses three numbers. It also answers for more clients
than TAP does, being what everything speaks.

**What it cannot do is why it went second.** One cone, one table, no predicate, no
projection, no join. `phot_g_mean_mag < 18` is not expressible. The reason this service
exists is ADQL over HATS and TAP is what exposes that; this is the smaller door.

Four things it does *not* inherit, because it predates DALI by a decade:

- **The error document is not DALI's.** A cone search reports failure as a stubbed VOTable
  carrying an `INFO` (or `PARAM`) with `name="Error"` — not `QUERY_STATUS="ERROR"`. So
  `output::votable::error` does not carry over; this needs its own, which is small and must
  not be unified with that one on the grounds that both are errors in VOTables.
- **The UCDs are UCD1, not UCD1+.** The three required columns are marked `ID_MAIN`,
  `POS_EQ_RA_MAIN` and `POS_EQ_DEC_MAIN`, where the rest of this phase writes
  `meta.id;meta.main` and `pos.eq.ra;meta.main`. Whether to publish both spellings is the
  one decision here worth making deliberately: clients of this protocol are old.
- **An ID column is mandatory**, and a HATS catalog does not promise one. Which column it
  is has to come from somewhere — the catalog's own metadata or the published-table entry —
  and a catalog with no such column cannot be published over this protocol at all.
- **There is no `MAXREC`.** `MaxRecords` is a registry property describing the service, not
  a request parameter, and the protocol says nothing about truncating. What this service's
  row bound does here therefore needs deciding rather than inheriting: silently truncating
  is the failure this repository keeps refusing.

**One endpoint per table** in 1.03, which is the other shape difference — a cone search
service *is* a table, where TAP and SCS2 publish many under one base url. So the url space
needs a decision that TAP did not need.

**The file-server mode's circle should end up spelled the same way**, and that is the part
of this step with a cost. A url there carries `ra`, `dec` and one of `radius_deg` or
`radius_arcsec`; cone search carries `RA`, `DEC` and `SR` in degrees. One service answering
a cone two ways in two url spaces is two things for a reader to learn and two places for the
bound to be applied.

It is not a rename, because the existing spelling is a rule with a reason behind it:
positions are unsuffixed and an extent names its unit, precisely so that a bare radius
cannot be read as degrees by one caller and arcseconds by another — and `SR` is a bare
radius. Three things have to be settled together:

- whether `RA`/`DEC`/`SR` are accepted as aliases beside the existing names, or replace them
- what a request naming both spellings means, which is the case that has to be refused
  rather than resolved
- whether `max_query_radius_arcsec` bounds `SR` as well, and what a cone search does when it
  is exceeded — 1.03 has no answer shape for a refusal beyond its `Error` INFO

Aliases are the likely answer, since the file server's names are published and a cone search
client cannot be asked to learn new ones. What must not happen is the two drifting: whatever
is decided, one piece of code parses a circle from a url.

**The conformance suite covers both already**, ten checks over the two, skipped until
`--scs-url` and `--scs2-url` name an endpoint — they are given rather than guessed, so that
nothing here encodes a url space this step has not decided. The 1.03 half goes through
`pyvo.dal.SCSService` and is calibrated against VizieR's cone search; the 2.0 half asks over
HTTP because no client implements a draft yet, and each of its checks names the clause it
came from so that what has to move when the draft moves is findable.

### 11.8 `/async` and UWS — tier 1

The one MUST this service does not answer, and the whole of tier 1. Every reference service
implements it, so there is no reading of the evidence in which it is optional; what holds it
back is that it is the only part of this phase that is a design question rather than a
mapping.

A job model is state — creation, phases, polling, results that outlive the request that
asked for them, destruction times and their collection — against a service whose every
answer today is collected inside one request future. It is where §5.3's and §7.2's
no-job-queue decision is revisited, where §0.2 is reopened, and where a job id becomes an
authorization surface. UWS specifies the shape, so it gets built once rather than invented
and then reconciled.

#### One execution path, two resources

`/sync` today reads the parameters, resolves the format, translates, opens the tables, runs
and encodes, and none of that knows it is inside a request future. It comes out as one
callable taking a `Service`, the parameters and a `Limits`, and producing the encoded answer
with its content type and its overflow flag. `/sync` awaits it and puts that in the response;
`/async` spawns it and puts it in a file. The uploads, `MAXREC`, the `TAP_SCHEMA` tables and
the format table then answer the same on both resources by construction, rather than by a
test that the two have not drifted.

What the two do *not* share is when the parameters are checked. TAP §2.7: the requirements
on them "must be satisfied (and errors returned if not) only when the query is run (in the
sense of UWS job execution)". So a `POST /async` carrying no `QUERY` creates a job and
redirects; the refusal is the job's, at `RUN`, as `ERROR` with the document at `/error`. A
job therefore holds the pairs as the caller sent them and `Parameters::read` runs inside the
runner — with one exception, and it is the only thing read at submission:
`UPLOAD_STORAGE_OPTION` is lifted out of the pairs as they arrive and put in the runner's
table, because a credential is the one thing that must not be written down. Checking it is
still the runner's; keeping it is not the store's.

The same sentence is why the result is encoded at completion into the format
`RESPONSEFORMAT` named — DALI §3.4.3 makes it "the content-type of the result resource(s)
the client can retrieve", which is a decision taken at run time and not at submission.

#### A job is data; running one is not

Two things, with two lifetimes, and the split is the whole of what makes the store
replaceable.

- **The record.** Id, phase, the three timestamps, execution duration, destruction time, the
  parameters as sent, the error summary, and where the result is — its content type, its
  length, and the name of the file holding it. Every field a value: no handles, no futures,
  nothing that cannot be written to a row.
- **The runner.** A process-local table keyed by job id, holding the abort handle and the
  job's credentials, plus a semaphore for how many may execute at once. Never stored — a
  restart has no running jobs by definition, so there is nothing for a persistent store to
  reconstruct.

The store is a trait over `create`, `get`, `list`, `apply`, `delete` and `sweep`, with
`apply` taking a closed enum of transitions — run, started, completed, failed, aborted,
destruction, execution duration, parameter — and returning the resulting record. A
transition rather than a read-modify-write is what lets one `Mutex<HashMap>` and one
`UPDATE … WHERE phase = ?` both be atomic without a version column and without an
optimistic-retry loop at every call site, and it keeps the state machine in one place that
both implementations call. What it costs is that this is not a general key-value store,
which it was never going to be.

#### The result is a file, and it is served as one

**A result goes to disk and never into the store.** What the store holds is a name, a
content type and a length; the bytes are a file under `[limits] scratch_dir`, beside the
copies `MaterializingStore` already puts there. One root, two kinds of thing under it, and
two budgets on one disk: `max_materialize_total_bytes` bounds the ephemeral copies and the
job quota bounds the results, neither knows about the other, and an operator sizing the
volume adds them. A results volume separate from the scratch one is the key to add if
anybody wants it, and not before. Keeping them in the record instead would mean a process holding every live job's answer
at once — the retention, not the peak, being what would exhaust it — and would put a blob in
the thing designed to become a row.

`GET /async/{id}/results/result` serves that file statically. Two things come with that and
neither is available to a body built in memory: a `Content-Length`, and a ranged read, so a
client resuming a large download is doing what it already does against a mount. The
compression predicate is unchanged and already right — it excludes by content type, so a
parquet result served this way keeps its length and its ranges for the same reason a mounted
parquet file does.

What the files need, each rule a failure that has a name elsewhere in this document:

- **A file's presence means a complete result.** Written under a temporary name and renamed
  on completion, so a crash mid-write leaves nothing a later request can read as an answer —
  a truncated document being one a client cannot tell from a whole one.
- **Each run writes into a directory of its own**, and the sweep deletes the *other* runs'
  rather than emptying the parent. Emptying it is wrong twice over: two processes sharing the
  volume — a second replica, or a restart overlapping a draining old one — would each destroy
  the other's live results, and a sweep running beside new jobs would delete files the current
  run is still writing. A per-run directory makes both impossible by construction rather than
  by timing.
- **The sweep does not block startup.** Nothing about correctness waits on it: an id is 128
  random bits, so a new job cannot collide with a stale file and no old file can be served as
  a new result. It is disk housekeeping, so it is a spawned task, and a failure is logged
  rather than fatal — garbage on disk is not a reason to refuse to serve.
- **That the directory is writable is checked at startup, and that one does block.** A
  service that cannot write a result cannot answer `/async`, which is not optional, so it is
  an operator's mistake to hear at startup the way a `[[tap.table]]` whose url will not open
  already is. One probe file, never a walk.
- **No mount may publish it.** A results directory served by the file server hands every
  result to anyone who can list a directory, which is the whole of the id's protection gone.
  It is the same shape as the mount-inside-the-API-prefix check and belongs beside it, at
  startup.
- **Destroying a job deletes its file**, and the byte budget is therefore a disk quota rather
  than a memory one.

**What this does not fix is the peak.** `output::votable::encode` and its siblings take a
collected `QueryResult` and build the whole document in memory, so a job still holds its
rows and its rendered answer while it is being written. Disk removes the copy held for the
job's *lifetime*, which is minutes to a day and multiplied by every live job; the seconds-long
peak while one result is encoded is unchanged, and making it small is the streaming writer in
§7.2 rather than anything here. Say which of the two a bound is about before setting it.

#### The id is the only thing protecting a job

UWS §2.2.1 asks only that the identifier "should be a legal URI path element". Everything
else here follows from §3, whose only access control is authentication and a `403`: with
neither, the id *is* the capability. So it is 128 bits from the OS CSPRNG, written
base64url without padding — 22 characters. Never a counter, a timestamp, a UUIDv7 or a
ULID, each of which is guessable to within a window.

Three consequences, and each is a decision rather than a fallout.

- **The job list is empty.** §2.2.2.1 asks for "a list (which may be empty) of all the jobs
  … that the client can see in the current security context", and §3 leaves the policy to
  the service, noting that a user "might only obtain a restricted list of jobs within the
  joblist". The policy here is that a job is visible to whoever holds its id, and an
  anonymous caller's context holds nothing — so `GET /async` is a well-formed `uws:jobs`
  describing none. `PHASE`, `AFTER` and `LAST` are read and answered rather than refused.
  This is the standard's own allowance and not a divergence.
- **`404`, never `403`.** §3 asks for a `403` where a caller may not see a job, which would
  confirm that the id exists — and the id is the whole of the protection. §2.2 already
  answers an absent job with `404`, so both cases answer alike and neither is
  distinguishable from the other. That one *is* the divergence, and it is the only one.
- **No credential is ever stored or echoed.** `UPLOAD_STORAGE_OPTION` carries a secret by
  design, and a job outlives the request that sent it, so the value goes in the runner's
  process-local table and never into the record — a row-backed store then inherits no
  secret and can never write one to disk. The parameters list omits the parameter entirely:
  §2.1.11 calls it "an enumeration of the Job parameters" and requires no completeness, so
  withholding needs no masking syntax invented for it. `UPLOAD` and `UPLOAD_TYPE` are echoed,
  `refuse_userinfo` being what makes a caller's url safe to print — but the rendering goes
  through the same "as far as it is safe to print" path, since a job is readable before it
  has been run and so before that refusal has happened.

#### The phases, and what a bound does

`PENDING`, `QUEUED`, `EXECUTING`, `COMPLETED`, `ERROR`, `ABORTED`. `HELD` and `SUSPENDED`
describe a scheduler this has not got; `UNKNOWN` describes a service that has lost track of
a job, which an in-process store cannot do. **`ARCHIVED` is not used**: §2.1.3 describes it
as "an alternative that the server may choose" at destruction time, so nothing requires it,
and a phase kept for one eviction path is a state every client and every test has to know
about. A job over the byte budget is destroyed, which is what §2.1.7 describes in full —
execution aborted, results destroyed, "the service forgets that the job existed".

The clock is `executionduration`, which UWS defines "in real clock seconds" and whose being
exceeded "should automatically abort the job, which has the same effect as when a manual
'Abort' is requested" — so `ABORTED`, not `ERROR`. It replaces `max_request_seconds` for the
work; the router's clock still bounds every HTTP request against the resource, which is why
`WAIT` is capped below it. **CPU time is not a bound that can be offered**: it is not
observable per task, and what actually bounds CPU here is the concurrent-job count against
DataFusion's `target_partitions`. Say that rather than add a knob nothing enforces.

`WAIT` is UWS 1.1's and belongs to the job resource alone, not to `/phase`; it blocks only
in `PENDING`, `QUEUED` and `EXECUTING`, `-1` means indefinitely, and a service "may impose a
maximum blocking time" — so the cap is the standard's own allowance rather than a
shortfall.

What a job may spend is `[limits]`'s, except where `[tap.async.limits]` overrides a field,
each defaulting to its sync value — one list of bound names, and an operator writes only the
difference. `max_partitions` is the field this exists for: a job is what a request too wide
for one response future turns into, so the partition count is what async buys. The counters
— `max_bytes_fetched`, `max_rows`, `max_query_memory_bytes` — are unchanged and per job.

**There is no switch turning it on.** TAP §2.2 makes `/async` a MUST alongside §2.1's
`/sync`, so a TAP surface has both or is not one — an operator who publishes a
`[[tap.table]]` is publishing a job resource, and the only question left is what it may
spend. What already decides whether there is any TAP at all is the table list: no
`[[tap.table]]`, no resources, which is the existing rule and is unchanged. An operator with
no room for jobs sets the bounds low; an operator who cannot host them at all cannot publish
TAP, and that is the standard's answer rather than this service's.

The job system's own knobs go in `[tap.async]`: how many records are kept, how many run at
once, the disk quota over all of them and the ceiling on one, the default and maximum
execution duration, the default and maximum destruction time, and the cap on `WAIT`. **Not
where the results go** — that is `[limits] scratch_dir`, which already exists and already
means "where this service puts bytes on local disk". A second path key would be a second
answer to one question, and an operator pointing one at a volume and forgetting the other is
the failure it would buy. Two pressures with two answers, and they must not be
collapsed: the disk quota destroys the oldest completed job, while the record count refuses a
new job with `503` — a caller can retry a refusal, and a burst of submissions must not be
able to take away results that have already been promised.

**The quota is disk and the engine's bounds are memory, and neither stands in for the
other.** `max_query_memory_bytes` is DataFusion's working set while a job runs;
`max_rows` is how large an answer may be; the quota is how much finished answer may be
lying around. A single job can be within all three and a hundred of them still fill a disk,
which is what the quota alone catches.

#### When something fails

**Every failure has to land on a phase.** Nobody is waiting on an HTTP request, so there is
no status to return and no caller to tell — a job that fails and does not say so is a job
that polls as `EXECUTING` until its execution duration runs out, which a client cannot tell
from a slow query. That is the shape to check each of these against.

- **The query fails** — bad ADQL, a table that is not there, a store that refuses. `ERROR`,
  with the `ApiError` the sync route would have returned as the `errorSummary` and as the
  DALI document at `/error`. A failed job has no result, so `/results/result` is a `404` and
  `/error` is where the answer is.
- **A bound is reached.** `ERROR` naming the bound, except the clock, which is `ABORTED`.
  `max_rows` is the one that does not fail at all: under TAP it truncates and marks
  `OVERFLOW`, the same as on `/sync`.
- **The result cannot be written** — no space, or over the per-result ceiling. `ERROR` naming
  it, discovered while writing rather than predicted, since the size is not known until the
  document is made.
- **The job task panics.** The runner keeps the `JoinHandle` and records the outcome,
  `JoinError::is_panic` included, as `ERROR` with a message of this crate's own. A spawned
  task nobody joins is the case that strands a job in `EXECUTING`, and it is the only failure
  here that leaves no other trace.
- **A transition cannot be recorded.** The work is done and the store will not take the
  result — which an in-process one cannot do, and a row-backed one can. There is nothing to
  hand back: log it loudly, leave the phase, and let the clock abort it. Worth knowing before
  choosing the second implementation rather than after.
- **The process dies.** Everything goes — the record with it, the store being in-process — so
  a client polling gets `404`, which is the destroyed-job case the standard already describes.
  The per-run directory is what stops the file outliving it.
- **`DELETE` while running.** Abort the handle, drop the record, delete the file. The
  DataFusion stream unwinds on drop, which is the same mechanism a `limit` already stops a
  catalog read with.

#### What stops being true

- **§0.1 is rewritten rather than deleted.** No endpoint writes to a store and no mount is
  writable, still. What changes is that the service writes a result to a scratch directory of
  its own, at a path it chooses — which is the same kind of write `MaterializingStore`
  already makes, and the reason the invariant has to say *where* rather than *whether*.
- **§0.2 is rewritten rather than deleted.** Nothing is cached across requests still holds.
  What changes is that a caller's own job outlives the request that made it.
- **One process.** A job created on one replica is a `404` on another, so a multi-replica
  deployment needs sticky routing until the store is shared. That is the concrete reason the
  trait is worth its cost before there is a second implementation, and it belongs in the
  README beside the deployment notes rather than being discovered.
- **A restart loses every job**, which is the destroyed-job case the standard already
  describes and answers `404`.
- **`{api.prefix}/tap/async` stops answering `404`**, and the README's sentence about a
  query too slow for `max_request_seconds` having nowhere to go stops being true.

**`/capabilities` needs nothing, and that is a consequence of both resources being
mandatory.** The TAP capability declares one interface with `use="base"` and a client derives
`/sync` and `/async` from it, so with both always present there is nothing conditional to
advertise and nothing that could come to disagree with what answers. What is left is
cosmetic: TAPRegExt also allows a `use="full"` interface per resource, the way the three VOSI
capabilities here already declare one, and whether the reference services bother is worth a
look when the document is next touched — it changes what a validator says and nothing a
client does.

The corollary is the state today, and it is already written down at the head of §11: a client
reading `use="base"` will try `/async` and get a `404`. That is not a missing declaration to
be added, because there is no declaration that would withhold it — it is the missing resource,
and this section is what closes it.

`tap-conformance/tests/test_async.py` is the measurement, and it is written and failing.
Thirteen checks against a service with no `/async`, none of them passing on a blanket 404 —
every one submits a real job first — and each naming the clause it came from. What they ask
is what `taplint`'s `JobStage` does not: it reads `/phase`, `/executionduration`,
`/destruction`, `/quote` and the parameters list, POSTs a `runId` and `RUN` and `ABORT`, and
deletes, and it never fetches the job list, never fetches `/error` or `/results`, never
*writes* a destruction time or an execution duration, and never asks what becomes of a job
submitted with no `QUERY`. The job-list checks deliberately do not require a submitted job to
appear, §2.2.2.1's "may be empty" and "the current security context" putting that with the
service's policy — so the visibility decision above is measured for conformance and not
against one reading of it.

### 11.9 `/examples`

A DALI-examples page of queries that run: TOPCAT reads it and offers them in a menu. All
four reference services publish one, but a client works without it, and TAP §2.6 asks for it
as a SHOULD; DALI §2.3 makes an absent one a 404, which is what the url answers while there
is no page.

**It is a resource of its own, and nothing it costs is shared.** A client fetches
`/examples` before it has asked anything, so whatever the page reads is read to draw a menu,
and read again for the next client's menu. That makes the cost the question this step turns
on rather than a detail of it.

**Every example is generated, because `[[tap.table]]` is a name and a url.** Which columns a
table has, which two hold a position, and where on the sky it holds rows are the catalog's
to answer, so the page reads what `/tables` reads: the properties, the partition list and
`dataset/_common_metadata`, per published table, per fetch. **So it waits for §6.1's catalog
metadata cache**, where those are in memory already and the page is assembled out of them.

**A good example wants more than metadata, and that is the part to bound.** A cone needs a
position the catalog holds rows at, which one of its own partition cells gives without
reading data. A predicate that matches anything needs to know what the values are like,
which nothing short of a partition's statistics says, and a page that opens a partition per
table per fetch is one nobody can afford. What a query says has to come from what the
catalog already says about itself.

Three things the generated queries have to avoid, none of them visible from a table name:

- **`SELECT *` is not an example.** These catalogs are 150 to 370 columns wide, which is ten
  to seventy seconds against about one for four named columns, the same rule
  `app/openapi/` already follows.
- **A nested column is refused by the format the menu is read in.** `votable` has no form for
  `lightcurve.mag` (§7.5), so a projection chosen blindly is a 400 in the first query a new
  user runs.
- **A `TOP n` beside the cone is what keeps the read to the partitions the cone names.**

The document is well-formed XML and so is authored as XHTML (DALI §2.3), carries one `vocab`
attribute for the whole page, and gives each example an `id`, a `resource` pointing at
itself, `typeof="example"`, a plain-text `name` and exactly one plain-text `query`, with the
fully qualified table names as `table` (TAP §2.6). It is declared in `/capabilities` as
`ivo://ivoa.net/std/DALI#examples`. It is a page this service serves, so: no CDN, complete
without JavaScript.

Left to decide: whether an operator may write examples of their own beside the generated
ones. It is what makes a real archive's menu worth reading, and it puts caller-facing ADQL
in a config file with nothing checking that it still runs.

### 11.10 What a caller gets told

`/docs` describes JSON bodies and TAP takes form parameters, so the TAP surface is described
in its own page rather than bent into the OpenAPI document. What it has to say, once, and in
the README as well: the base url to paste into TOPCAT, the table names, whether `/async` is
there yet and what to do instead while it is not, and which formats carry a nested column.

### 11.11 Table upload

Two halves, and only the first is planned. **A caller names a catalog by url and queries it
as `TAP_UPLOAD.name`**, which is what `POST /adql` already does with its `tables` and what a
TAP client has no other way to ask for. **Inline upload** — a VOTable sent in the request —
is the second half and stays later: it is the one capability the four reference services do
not share (IRSA offers none, MAST half), so the ecosystem has not settled it, and it is
caller-supplied bytes rather than a url, which reopens what a request may spend.

`UPLOAD=name,uri`, the `TAP_UPLOAD` schema and a uri that is an `http(s)` url rather than
`param:` are all TAP §2.7.6's own, uploads accumulating over repeated parameters the way DALI
§3.2 has anything repeat. What is this service's is what the url may point at — a HATS catalog
or a parquet file, neither of them the VOTable the standard means — and the storage options
such a url needs. So a client can write the parameter and nothing else about it is borrowed.

**Neither TAP nor DALI keys a value by anything but `UPLOAD`'s one comma**, so the two
parameters below take that shape and no other: `<upload>,…`, repeated for more. A sub-parameter
syntax would be invented twice over, and DALI's own structured values are fixed tuples of
numbers rather than anything keyed. TAP's answer to an upload url needing authentication is
credential delegation, a service holding the caller's certificate; these are this service's
answer instead and are not that.

- **`UPLOAD_STORAGE_OPTION=<upload>,<option>,<value>`**, one option to a value, holding what
  `/adql`'s `storage` holds. One spelling of storage options in the service, or the two drift.
  **The value runs to the end**, so a secret carrying a comma, a space or an `=` arrives
  whole; a separator inside a value truncates a credential, which is a request that reads as
  anonymous. The option's own name says how many fields follow it, which is how DALI reads a
  shape — `CIRCLE` three numbers, `RANGE` four — so `header`, the one option that is a map,
  takes a name before its value. It is accepted on `GET` as on `POST`: TAP gives the two carriers one
  syntax, and a credential in a url is already spent by the time this service could refuse it.
  What the service can do is not make it worse — the log records a path and never a query
  string, and that has to stay true.
- **`UPLOAD_TYPE`**, optional, `name,hats`. Absent, the type is worked out: a
  name matching the data-file globs is a parquet file, and a directory holding
  `hats.properties`, `properties` or `collection.properties` is a catalog. Guessing costs a
  request or two and must fail as a refusal naming what was looked for, never as a broken
  catalog.
- **Nothing is declared in `/capabilities`.** A `uploadMethod` tells a client it may send a
  VOTable, which this half refuses, so it waits for the inline half. Which means the feature
  is found by reading the README rather than by a client discovering it, and that is the
  price of not advertising what is not there.

Every url goes through the access policy exactly as `/adql`'s tables do, and a catalog reached
this way is bounded by the same three bounds; `[[tap.table]]` stays credential-free, an
operator's secret having no place in a published surface. The two reserved schemas are already
reserved. The suite's upload checks are inline VOTable and stay red.

### 11.12 `parquet` and `json` over TAP, and a nested column in `TAP_SCHEMA`

The formats this service has of its own, advertised in `/capabilities` as what they are, and
the only way a nested column can be answered at all — `votable`, `csv` and `tsv` each refuse
one.

Both halves are here because no reference service can be asked about either. None of them
publishes a nested column, so there is no practice to follow and no check that can be
calibrated against anybody: what `TAP_SCHEMA.columns` should say about `lightcurve.mag` is a
decision to make alone, and it waits on §7.5 deciding what a nested column is in a VOTable
first.

### 11.14 A DALI parameter value, read once and typed

`tap::dali` is the `serde` data format, and `UPLOAD`, `UPLOAD_TYPE` and
`UPLOAD_STORAGE_OPTION` are read through it. What is left is the parameters that do not exist
yet: §11.7's `POS`, `CIRCLE`, `RANGE`, `POLYGON` and the `BAND`/`TIME` intervals are the same
grammar with numbers in it, and are written as types rather than as a fourth reader.

`sky::region`'s own parsing stays where it is: a `Region` is a structured field in a JSON
body, which `serde` already reads. This is about parameters, which arrive as text.

## 8. Security requirements

Conditions every phase must keep. What holds them today is in `CLAUDE.md`; what is here is
what a later phase can still break.

**Threat model.** The operator is trusted — they wrote the config and run the process. The
caller is not: they supply a url, storage options, credentials, a projection and a
predicate, and the service makes network and filesystem requests on their behalf.

### 8.1 No credential leakage

- **The request body is the only source.** Not an environment variable, not a file on disk,
  not ambient discovery by an SDK, not an instance profile. A request with no credentials is
  unsigned, never the process's own identity. A `[[mount]]`'s own `storage` is the one
  operator-configured credential, and it is named explicitly in the config for that mount
  rather than discovered from anywhere.
- Credentials are stripped at the boundary, and a new backend adds its option names to that
  stripping and brings a new signer to check (§8.5).
- A caller must not reach another's through the cache — §6.0's keying rule.

### 8.2 No local filesystem until the config says so

- **Refusal must not be a filesystem oracle.** Outside a mount, 403 whether or not anything
  is there; only inside one does a missing file become 404. A mount over a store keeps the
  first half and cannot have the second: a flat namespace has no missing directory, so a
  prefix with nothing under it lists as empty.
- Run as an unprivileged user, and document `ReadOnlyPaths=`/`ProtectSystem=` (systemd) and
  read-only bind mounts (Docker) in `docs/deployment.md`. **Not written yet.**

### 8.3 No local network until the config says so

- **Names and addresses are two layers and both are needed**, and every resolved address
  must pass, not only the first.
- **The check runs inside the HTTP client's own resolver**, whose return value *is* the set
  of addresses the connection is attempted against. Anywhere earlier is a check against an
  answer that can be replaced.
- The one hop that is followed is judged by the resolver like any other, the follow-up
  request being made through the same client. What a later backend must not do is reach a
  destination by any route that is not that client.

### 8.4 Bounded work per request

The clock, the materialization caps and the catalog routes' three bounds are done. What is
left:

- **A per-process concurrency limit** (§7.1).
- **A response cap.** Every field of a request is optional, so a `POST` naming only a url
  reads a whole partition, and both writers buffer the result whole — a wide file is
  answered out of memory. `limit` is the caller's and is no bound when they do not set one.
  **The ceiling is per format, not one number**: a row costs far more as JSON than as
  parquet, so a count generous for one is wrong for the other in both directions. Bytes
  written is what the two have in common and a row count is what a caller can predict, so it
  likely wants both. This and §7.2's streaming are one piece of work, `collect` building the
  whole answer before either writer starts.
- **A `POST` body size limit.** The expression depth and node caps bound the caller's text
  only. The projection needs no cap of its own, being bounded by the schema.
- **Reject pathological parquet early** — a footer claiming implausible row-group or column
  counts is a 400, not an allocation.

**What the query engine already offers**, checked against DataFusion 55 rather than
assumed, so nobody goes looking twice: `RuntimeEnvBuilder::with_memory_limit` installs a
`GreedyMemoryPool`, so an over-large query fails with `ResourcesExhausted` rather than
taking the process down — a guard and not a proof, its own documentation saying the limit
is not respected on every path. Spilling to disk is on by default, which quietly turns a
memory limit into a disk one; `DiskManagerMode::Disabled` refuses it and
`with_max_temp_directory_size` caps it. `ExecutionPlan::partition_statistics` estimates
rows and bytes from the footer without reading data — the numbers §5.3's plan mode uses.
Nothing in it bounds what a `collect` returns.

### 8.5 Obligations on every new backend

1. Its option names are on the stripping list, with a test that an error mentioning a bad
   option does not carry the credential.
2. Its destination goes through §8.3's network check, after resolution.
3. It is default-deny in the config, with the same three-state `endpoints` shape.
4. Its refusals do not distinguish "does not exist" from "not allowed" outside an allowed
   scope.
5. There is a test that the default config refuses it, and one that the narrowest config
   that should allow it does.
6. **Its SDK is checked for logging credentials**, at `trace`, with a request that carries
   them. Anything found goes in `CREDENTIAL_UNSAFE_TARGETS` with its reason, which the
   canary test then holds to being the complete list.

## 9. Future development

1. **A plain-url API for public data.** One `GET` whose only parameter is the location, the
   scheme naming the backend as `Backend::from_scheme` already does: the one-liner a
   browser, a `curl` or a notebook cell can write, lowering to the request the `POST` shape
   already carries.

   **Anonymous only, and that is what makes it a `GET`.** The `POST` shape is a `POST`
   because a query string is written to every proxy's access log. So this one must *refuse*
   a credential rather than ignore one — no `storage` object, no headers, no url with a
   query string — and the moment anything here could carry a secret it goes back to being a
   `POST`. Left to settle: where it sits in the url space, a url nested in a url needing
   encoding either way.
2. **A `polygon` region.** `vertices: [[ra, dec], …]`, alongside `circle` and `zone`. Every
   other shape is a formula — one `Expr` that prunes on the coordinate columns — and this
   one is not. Four things to settle before it:

   - **A closed loop on a sphere bounds two regions and the vertex list does not say
     which**, there being no "outside" on a sphere. The reading has to be stated — winding
     order, or the smaller of the two — and a caller who writes the vertices the other way
     round gets the complement of what they meant, a wrong answer rather than an error.
     Refusing the ambiguous case is not available: both readings are legal polygons.
   - **What an edge is has to be stated too.** Two vertices at one declination are joined by
     a great circle or by a parallel, and the two differ by degrees at high declination. A
     caller writing a "rectangle" means the second; `cdshealpix` means the first.
   - **The per-row test is a loop, not an expression** — a crossing count over `N` edges
     with `N` the caller's to choose, the one shape whose cost per row the request sets.
     That wants a UDF, and a UDF is a thing `engine/sql.rs`'s volatility rule and §5.2's pruning
     both have to be taught about.
   - **Self-intersecting and degenerate input**, each of which the covering and the row test
     can disagree about.

   `cdshealpix` supplies the covering; the exact test, the conventions and the refusals are
   the work. §10 does not wait on it — ADQL's `POLYGON` is part of an optional feature, and
   the second and third points above are also what `BOX` turns on, which is why §10.7
   refuses that one rather than mapping it onto a shape with different edges.
3. **Tables discovered rather than declared.** `[[tap.table]]` is written out per table and
   is temporary; the HATS registry is where it comes from instead. It reopens two things at
   once. §0.2, because a set of tables fetched from elsewhere is a registry across requests,
   with a refresh, a staleness window and two requests that may disagree about what exists.
   And where a registry-named catalog is addressed from, since `[[tap.table]]` names a path
   under a `[[mount]]` and a registry hands back urls — so either the registry's urls are
   resolved against the mounts, or a table gains an address the rest of the service has no
   way to name.
4. **Filesystem-driven cache invalidation** (§6.6).
5. **Aggregating inside a nested column.** A ZTF row holds a whole light curve in
   `lightcurve.mag`, and the mean magnitude of one object is not expressible today. The
   obstacle is not the expression rules — an operation over one row's list is a scalar
   function, which the allowlist admits — it is that `datafusion`'s `nested_expressions`
   feature is off, so `array_avg`, `cardinality`, `distance` and the rest do not exist.
   Turning it on makes all of them callable at once, which is the decision to weigh.

   **`avg(lightcurve.mag)` is not this** and must keep being refused: `avg` summarizes rows,
   so it averages the column down the file rather than along one light curve. The two read
   almost alike and mean entirely different things, so whatever is added has to be named so
   a caller cannot reach for one and get the other. `array_filter`, `array_transform` and
   `array_any_match` take lambdas, which the allowlist refuses as expression kinds — either
   they stay refused, which needs saying in the error rather than a bare "not supported", or
   the lambda arms are reconsidered, which is wider than this item.
6. **Separate crates, separate repos.** Once ADQL and TAP exist, split into `hats`, `adql`
   and `tap`. `hats` is the catalog itself rather than this service's use of it — the
   properties file, the partitioning, `Norder`/`Npix`/`Dir`, the MOC, `_metadata` and
   `partition_info.csv`, what the Python `hats` library covers, for anyone reading a catalog
   with no service in front of it. That code is written, so the split is a matter of where
   it lives.

Each phase leaves the service useful, and each is a prerequisite for the next rather than a
parallel track.
