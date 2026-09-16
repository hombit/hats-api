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
| 2.5 | Hugging Face | deferred | until the redirect hop is decided. Still droppable |
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
| 11.1 | the tables the service publishes | todo | tier 0. A name and a url, no storage options; temporary until §9.4, and §0.2 holds only while the list is config |
| 11.2 | `/sync` and the parameters | todo | tier 0. Form-encoded is a carrier no route takes today |
| 11.3 | VOTable, `MAXREC`, `OVERFLOW`, errors | todo | tier 0. `output::votable::encode` needs a trailer; the overflow marker goes after the table |
| 11.4 | `TAP_SCHEMA` | todo | tier 0. Names are strict here, the published spelling being what a client copies |
| 11.5 | VOSI capabilities, availability, tables | todo | tier 0 |
| 11.6 | `csv` and `tsv` | todo | tier 0 by cost rather than by demand — two writers over the `QueryResult` that exists. Two of the four reference services offer neither |
| 11.7 | Simple Cone Search, 1.03 and 2.0 | todo | tier 0.5. Days on top of tier 0, which pays for all of it. 1.03 inherits none of DALI — its own error shape, UCD1, no `MAXREC`; the 2.0 draft inherits nearly all of it and adds `TABLE` |
| 11.8 | `/async` and UWS | todo | tier 1, and the only thing in it. Every reference service has one; it is held back for being state rather than a mapping. Was §9.4 |
| 11.9 | `/examples` | todo | later. A menu TOPCAT offers, not something a client needs to work |
| 11.10 | what a caller gets told | todo | later. Its own page; TAP takes form parameters and `/docs` describes JSON bodies |
| 11.11 | table upload | todo | later. The one capability the four reference services do not share — IRSA has none, MAST half |
| 11.12 | `parquet` and `json` over TAP, and a nested column in `TAP_SCHEMA` | todo | later. No reference service can be asked about either; the nested half waits on §7.5 |

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

## 2. Phase 1 — more storage backends

### 2.5 Hugging Face

`opendal`'s `services-hf`, as `hf://datasets/<owner>/<repo>[@revision]/<path>` — the
spelling DuckDB and `fsspec` use, so the repo type is written rather than defaulted to
`model`. Listing is the repo tree API, so the directory pages and §5.1's discovery work
whatever else is decided.

**One thing decides it: the redirect.** Checked against `services-hf` 0.58.2 and `hf-xet`
1.6.0. Reads go one of two ways, per store:

- **`xet`, the default.** The object comes over `hf-xet`'s own session — its own `reqwest`
  client, thread pool and disk cache — so the resolver would not see the addresses the
  bytes come from and `storage::materialize::Transfers` would not see the bytes. The session is
  built unconditionally, so choosing the other mode avoids using it but not building it.
  Reads are said to be several times faster this way: revisit if the session ever takes an
  `HttpTransport`. The `xet-*` crates also read `HF_TOKEN` and `HF_ENDPOINT` themselves,
  below opendal, which reopens the ambient-credential question by a different door.
- **`http`.** Everything is on the operator's transport, except that
  `GET …/resolve/<rev>/<path>` answers `302` towards a CDN for anything LFS-backed — every
  parquet file in a dataset — and `redirect::Policy::none()` is deliberate.

So `http` mode needs a redirect hop that does not exist: re-authorize the target through
the same resolver, and drop the caller's credentials before following, as
`huggingface_hub` does. **That hop is its own piece of work** — it is equally what a
`https://huggingface.co/datasets/…` url needs through the http backend, and what any
CDN-fronted origin needs.

**The anonymous request is settled, and not by an environment variable.** A configured
*empty* token short-circuits the builder's whole ambient chain (`HF_TOKEN`,
`$HF_TOKEN_PATH`, `$HF_HOME/token`, `~/.cache/huggingface/token`): the header builder
errors on an empty token and the call site drops the header rather than failing. Verified
on the wire. Three costs: it has to be set through serde, because `HfBuilder::token("")` is
ignored and `HfConfig`'s fields use unexported types — and that config is `#[serde(default)]`
with no `deny_unknown_fields`, so an upstream rename would silently put the ambient token
back on the wire, which makes the `tests/ambient_credentials.rs` case the guarantee itself.
It is safe only in `http` mode, `xet`'s token refresh having no empty check. And it makes
`Capability::write` read `true`, harmless under §0 but no longer descriptive.

**Drop this backend** if the redirect hop is not wanted: it has the least astronomy data
behind it and its absence costs nothing structural.

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

No job queue, job ids or polling: a plan is a list of stateless requests. See §7.2.

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

**The first two are the ones worth building, and the reason is measured.** A
`format=parquet` request reads the source footer three times, two of them this crate's own:
DataFusion fetches it while inferring the schema and serves the scan from its own
`FileMetadataCache`, while `output::parquet::read_layout` goes to the store and pays two
requests — the reader's default prefetch is 8 bytes, enough for the footer tail and never
for the footer. Every shape measured came to `+2` requests for the layout.

**Nothing here should be reading metadata itself**; the read belongs to DataFusion, which
has already done it. The obstacle is reach: the cached entry is an `Arc<dyn FileMetadata>`
whose only accessor is `as_any`, and the concrete type lives in
`datafusion-datasource-parquet`, which the facade does not re-export — so it means taking
that crate as a direct dependency, version-locked the way `object_store` already is. Decide
that here rather than paying it for one call site. Two smaller things from the same
measurement, neither a cache: DataFusion's `metadata_size_hint` defaults to 512 KiB against
our 8 bytes, and the layout read is sequenced after the query when it does not depend on it.

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

**No async job interface in this plan.** TAP's `/async` (§11.8) is what forces one and UWS
specifies its shape, so it gets built once rather than invented and then reconciled. A job
system would also break §0's statelessness and add job ids as an authorization surface.

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
VOSI asks the same question in the astronomy vocabulary and arrives with TAP in §11.5 — two
renderings of one description, not two descriptions.

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
  its lowercase, the way a column does — not to ADQL's uppercase folding (§10.8).
  `TAP_UPLOAD` and `TAP_SCHEMA` are refused as names now, before §11.4 needs them.
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
   feature on makes every array function callable at once — which is §9.6's decision and
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
(§9.3), `BOX` (a centre with great-circle edges, which the `zone` region is not), `REGION`
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
   answer, so this service refuses instead. Revisited at §11.3, where `OVERFLOW` is at least
   an in-band statement that the answer is partial.
3. **`RAND` and an unordered `TOP` are the first answers here that are not reproducible.**
   ADQL says nothing about which rows `TOP n` returns without an `ORDER BY`, so an arbitrary
   set conforms — but every other route promises more than that, and a reader will carry the
   stronger assumption across unless it is written down.

## 11. Phase 8 — the IVOA interfaces

IVOA's Table Access Protocol over §10's ADQL layer, so that TOPCAT, `pyvo` and `astroquery`
reach these catalogs with no client written for this service. The resources are siblings
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
them constantly.** Most of what tier 0 implements is DALI, and reading TAP alone leaves
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
the code reads right. Three things about it constrain what follows:

- **A published table is a real catalog on S3 and a small sample beside it.** The sample
  is what the validator works over, a validator asking for whole rows of a 153-column
  catalog being slow rather than informative. So §11.1's list has to answer both a
  `file://` url under a mount and an `s3://` one, which it does.
- **What the suite calls a failure is not always this service's.** A check no established
  TAP service passes is a check to re-read before treating it as a requirement, which is
  what the survey against reference services is for. `tap-conformance/REFERENCE_SERVICES.md`
  is the list as it stands, and it is unreviewed.
- **Conforming and being usable are two results, not one.** The report counts them apart,
  because a document can carry everything the standard asks for and still be one a client
  cannot parse.

### The order, and what decided it

The suite was run against four separate TAP implementations — ESA Gaia, ARI-Gaia, IRSA and
MAST — before any of this was written, and what they have in common is what sets the order
here. A specification marks everything MUST or SHOULD and cannot say which of it a client
actually needs; four independent services agreeing does say so.

**Tier 0 is what every one of those four implements, and what is cheap here.** Availability,
capabilities, table metadata, `TAP_SCHEMA`, synchronous queries, ADQL, VOTable, `MAXREC`
with its overflow marker, and error documents: all four have all of it, so there is no part
of it a client can be expected to work around. Nothing in Tier 0 is optional in practice
whatever the standard calls it. `csv` and `tsv` join it for the opposite reason — the four
disagree about them, so they are not required, but they are two writers over a `QueryResult`
that already exists and cost about a day between them.

**Tier 1 is `/async`, alone.** All four implement it, so it belongs in Tier 0 by the rule
above and is held back by one thing only: it is a job model, which is state, against a
service whose every answer today is collected inside one request future. It is the one
piece of this phase that is a design question rather than a mapping, and mixing it into
Tier 0 would stall everything that is a mapping.

**Everything else is later, and named as such below.** Table upload is the clearest case:
IRSA offers none and MAST half, so the ecosystem has not settled it, and the four agree
only on *declaring* what they have. `/examples` is a menu TOPCAT offers rather than
something a client needs to work. The formats this service has of its own — `parquet`,
`json` — and how a nested column is declared are questions no reference service can be
asked, because none of them has such a column.

**Until Tier 1 lands this is deliberately not a conforming TAP service, in exactly one
place.** `/async` is a MUST (TAP §2.2). What that costs is a real thing and belongs in the
README rather than being discovered from a validator: a query too slow for
`max_request_seconds` has nowhere to go, because the resource a client would be sent to does
not exist. `taplint` will say so, and the answer is that the query has to be made smaller
until Tier 1 lands.

Two things follow from being sync-only in the meantime, and both are refusals rather than
silence: `/capabilities` advertises no async interface, and `{api.prefix}/tap/async` answers
`404`. Advertising one and failing the job submission is worse than not offering it — a
client chooses the interface off the capabilities document and has no way back.

### 11.1 The tables the service publishes

A TAP query names a table the service already knows: `TAP_SCHEMA` is service-side and has
nowhere to put a name that arrived with the query, so the per-request `tables` of §10.1 has
no equivalent here. **The list is config, and that is the temporary half of this phase** —
§9.4 is where the tables come from somewhere else, and nothing built here may assume the
list is written by hand.

A table is a name and a url, local or remote, and nothing else:

```toml
[[tap.table]]
name = "gaia_dr3.gaia_source"
url = "file:///hats/gaia_dr3"

[[tap.table]]
name = "ztf.dr24_object"
url = "s3://irsa-fornax-testdata/ZTF/dr24/object"
```

**§0.2 survives this and must keep surviving it.** A list read from the config at startup is
config, not a registry accumulated across requests; nothing here caches a catalog between
two requests. The moment tables are discovered rather than declared — §9.4 — that stops
being true and §0.2 is what has to be reopened.

Four things this shape settles:

- **A name is the operator's, not the mount's.** Deriving `schema.table` from a mount's
  `path` would make a mount rename break every query a client has saved, and would leave a
  remote catalog — which has no mount — unnameable. The cost is that a catalog is not
  published until someone writes it down, which is the right default for a surface that
  serves whatever it lists to anyone who can reach it.
- **No storage options, so a published table is one that reads anonymously.** A url is the
  whole of what a table is: `[[tap.table]]` has no `storage`, and a catalog needing a
  credential is not publishable over TAP until §9.4 decides where one would come from. What
  that buys is that the question §8.1 would otherwise have to answer here — an operator's
  secret in a config file, on a surface whose answers are public — does not arise. Adding the
  field later is adding that question, not a convenience.
- **The columns come from `dataset/_common_metadata`**, which `hats/table.rs` already reads
  first as one small `GET` — every partition's columns and no rows. So a table's metadata
  costs one request rather than a partition read, which is what makes §11.4 and §11.5
  answerable at all.
- **A url is checked against the access policy like any other.** A table's url goes through
  `storage::open_dir`, so an operator naming an endpoint the policy refuses finds out at
  startup rather than on a caller's query.

### 11.2 `/sync`, and the parameters

`GET` and `POST` both, the latter `application/x-www-form-urlencoded` — a new carrier, every
route today taking JSON or a typed query struct. TAP §2.1 notes a `GET` may be answered from
a cache and that a client needing current data must `POST`; nothing here caches, so the two
differ only in where the parameters are read from.

Parameter names are case-insensitive (DALI §3.1) and values are not, except where a
parameter's own definition says so. What is taken:

| parameter | |
|---|---|
| `QUERY` | the ADQL statement, to `adql::translate` unchanged |
| `LANG` | `ADQL` only; anything else is refused naming what is taken |
| `RESPONSEFORMAT` / `FORMAT` | §11.3. TAP §2.7.3 requires `FORMAT` be accepted as the equivalent |
| `MAXREC` | §11.3 |
| `RUNID` | at most 64 characters, written to the log and nowhere else (DALI §3.4.6) |
| `REQUEST` | `doQuery` accepted, any other value refused |

**`REQUEST` is accepted although TAP 1.1 removed it** (Appendix A.3). A 1.0-era client sends
it, the value carries no meaning this service acts on, and refusing the request over it would
400 a query that would otherwise run. Accepting the one value rather than ignoring the
parameter is what keeps `REQUEST=doSomethingElse` from being read as a `doQuery`.

**Everything else is refused, and DALI does not say otherwise** — §3.1 settles the case of
parameter names and is silent on unrecognised ones, so the house rule stands: a parameter
this service acts on is honoured or refused, never dropped. A repeated single-valued
parameter is refused too (DALI §3.2).

### 11.3 What comes back

**One format here, and it is `votable`** — mandatory, the default, and the only one all
four reference services agree on. `csv` and `tsv` are §11.6; `parquet` and `json` are
§11.11. What this step owns is that each name and its media type come from one list, the
way `Format` already holds the three it has, so the later two steps add entries rather
than a second way of deciding.

**A media type is part of the answer, not decoration.** One reference service labels its
VOTable `text/xml`, and a client that picks its parser by content type picks wrong — which
is the whole failure this list exists to avoid.

**`MAXREC` is not `limit`.** Three of its rules are its own:

- It **overrides `TOP`** (TAP §2.7.4), so `SELECT TOP 100 … ` with `MAXREC=10` returns ten.
- **`MAXREC=0` returns the columns and no rows**, needs no overflow marker, and the service
  may skip execution entirely. TOPCAT uses it to inspect a table.
- **Truncation is marked, not silent.** `<INFO name="QUERY_STATUS" value="OVERFLOW"/>` goes
  *after* the `TABLE` (DALI §4.4), where the `OK` this encoder already writes goes before it.
  So `output::votable::encode` grows a trailer; it cannot be said in the prologue, which is written
  before the row count is known.

**Reading `MAXREC` rows cannot tell a full answer from a truncated one**, so the read asks
for `MAXREC + 1` and reports the overflow if it arrives. Exactly `MAXREC` rows is otherwise
two different answers with one spelling, which is the failure this service keeps finding.

**`OVERFLOW` is where §10.8.2 is revisited, and only for the row bound.** `max_rows` becomes
a truncation that says it is a truncation, because the marker is precisely the in-band
statement whose absence made refusing the right answer everywhere else. `max_partitions` and
`max_bytes_fetched` have no such marker and no plan to answer with, so they stay refusals.
Two bounds with two answer shapes on one route is the deliberate part.

**An error is a VOTable**, `<INFO name="QUERY_STATUS" value="ERROR">message</INFO>` before
the table (DALI §4.4), with a 4xx or 5xx status. `ApiError` renders JSON, so these routes
need a rendering of their own; what may be named in the message is unchanged — the caller's
own url and the names inside a catalog they asked for, never a local path.

**The answer's columns are the `SELECT` clause's, in number, order and name** — TAP §3.2, and
a `FIELD` takes the alias where one was written. `engine::sql::packed` answers `lightcurve.mag,
lightcurve.mjd` as one `lightcurve` column holding both subfields, which would disagree with
that — and does not arise, because **a nested column in a VOTable is refused**. §3.2 is met
by there being no such answer. That refusal is this phase's behaviour and not a gap waiting
on something: whatever form a nested column eventually takes here has to satisfy §3.2 as one
of its conditions, which is a constraint on that decision rather than a reason to make it
now.

**A `FIELD`'s `name` and its `ID` say the same thing.** The two clients disagree about which
they read — one answer came back with `source_id` through `pyvo` and `SOURCE_ID` through
STILTS, for one query against one service — so a query written in TOPCAT and pasted into a
notebook raises `KeyError`. Nothing in the standards forces the two apart; making them agree
costs nothing and removes the failure.

### 11.4 `TAP_SCHEMA`

The four tables of TAP §4 — `schemas`, `tables`, `columns`, and the empty `keys` and
`key_columns` — registered as in-memory tables so a client learns the column list by querying
them, which is how `pyvo` and TOPCAT ask. `TAP_SCHEMA` describes itself as well, since that
is the first thing a client queries.

`output::votable::spelling` already returns the `(datatype, arraysize)` pair `columns` needs, so the
mapping is not written twice. `indexed`, `principal` and `std` are not-null and are this
service's to answer: the spatial index column and the two coordinate columns are the
`indexed` ones, being what a region prunes on.

**A name answers to its own spelling and to nothing else, and `TAP_SCHEMA` is what says what
that spelling is.** This interface is stricter than every other route here, and the standard
is what affords it: TAP §4.2 and §4.3 say the published `table_name` and `column_name` are
"the string that is recommended for use in querying", and that a name needing quotes is
published *with the quotes*. So a mixed-case column goes into `TAP_SCHEMA.columns` as
`"Gmag"`, a client that builds its query from `TAP_SCHEMA` writes a delimited identifier, and
ADQL matches a delimited identifier case-sensitively. Strictness costs a caller nothing
because the thing they copy is already correct — which is not true on any other route, where
there is no `TAP_SCHEMA` to copy from and §10.8.1's lowercase fallback is what stands in for
one.

So: no folding, no lowercase fallback, and a name that does not match is refused naming the
column. ADQL says an unquoted identifier is case-insensitive but does not say whether it
folds up or down, and places no requirement on the service's matching at all — it names the
resulting interoperability problem and leaves it open. Being strict is therefore a choice the
language permits rather than a divergence from it, which is the opposite of §10.8.1's
standing.

**`TAP_SCHEMA`'s own name is the one exception, and it has to be.** A client queries
`TAP_SCHEMA.columns` to find out what this service calls things, so it cannot have learned
that name from the answer it has not received yet — it hardcodes a spelling, and which one is
the client's business. The five fixed names of TAP §4 therefore resolve case-insensitively;
every name this service publishes does not. The exception is exactly the bootstrap and does
not extend to a table an operator declared, whose spelling a client reads before it writes.

**A nested column is declared by its leaves, dotted, and the struct itself is not a row.**
`TAP_SCHEMA.columns` gets `lightcurve.mag` and `lightcurve.mjd`, each with its leaf's own
datatype and `arraysize="*"`, and no row named `lightcurve`. Three things behind that:

- **A leaf has a type and the struct has none.** A leaf holds one row's whole array, which
  VOTable spells as the element's datatype with `arraysize="*"`. A row for the struct could
  carry only an invented type, which is what this section's strictness is against.
- **It is what a caller writes.** `engine/sql.rs` reads `lightcurve.mag` as a path into a struct
  where the head is one of the file's own fields, so the published name is the name that
  selects the value — which is what TAP §4.3 asks the published name to be.
- **Depth is not declared.** A struct in a struct has no leaf with a spelling either, so it
  is absent for the same reason the struct is, rather than by a second rule.

**Declaring a column is not promising every format can return it, and three of the five
cannot.** `json` and `parquet` answer these; `votable`, `csv` and `tsv` refuse. That split is
this phase's behaviour rather than a temporary state — `output/votable.rs` refuses a nested column
by name today and keeps doing so until there is a right way to write one, which is a decision
this phase does not make and must not anticipate. For `csv` and `tsv` there is no decision to
make at all: CSV has no notion of structure, DALI §3.4.3 names the media type and says
nothing about nesting, and `arrow-csv`'s writer rejects any `is_nested()` type outright — so
the refusal is the writer's and nothing here implements it.

What the declaration buys is that a caller can see the column exists and reach it in a format
that carries it. Omitting it would make a catalog look narrower than it is, and would make
`SELECT *` fail over columns the client was never told about.

**The dot is structure and must not be quoted.** TAP §4.3 says to publish a name *with*
quotes where it must be quoted; this is the case it does not anticipate, a name that must
*not* be. `"lightcurve.mag"` is one delimited identifier naming no field, so a client that
quotes defensively breaks on it. Worth stating on §11.7's page, there being nowhere in
`TAP_SCHEMA` to say it.

### 11.5 VOSI

- **`/availability`** — `<vosi:availability><vosi:available>true</vosi:available></vosi:availability>`,
  and nothing this service can currently say is false.
- **`/capabilities`** — the TAP capability at `ivo://ivoa.net/std/TAP`, the two VOSI ones at
  `ivo://ivoa.net/std/VOSI#capabilities` and `#availability`, and `#tables-1.1` for the third
  below. The TAPRegExt detail — `language`, `outputFormat`, `outputLimit` — is a SHOULD that
  clients do read, and every value in it is already a config key or the format list, so it is
  derived rather than written out beside them.
- **`/tables`** — the same metadata as §11.4 in VOSI's own XML, with `?detail=min` returning
  table names without columns and `/tables/{name}` returning one table in full.

### 11.6 `csv` and `tsv`

Two writers over the `QueryResult` that `votable` already answers from, and the last of
tier 0. TAP §2.7.3 makes them a SHOULD and the reference services split two against two on
them, so this is not here because anyone requires it — it is here because it is a day's
work beside a week's, and a day's work that makes a spreadsheet and a `curl` into clients.

`arrow-csv`'s writer rejects any `is_nested()` type outright, so a nested column is refused
by the writer rather than by anything written here. Both names and media types go in §11.3's
one list.

### 11.7 Simple Cone Search, both versions — tier 0.5

[Simple Cone Search 1.03](https://www.ivoa.net/documents/REC/DAL/ConeSearch-20080222.html):
`RA`, `DEC` and `SR` in decimal degrees, ICRS, and a VOTable of the rows inside that cone.
That is the whole protocol.

[SCS 2.0](https://github.com/ivoa-std/SCS2) is built on DALI, so it inherits nearly all of
tier 0 where 1.03 inherits none of it — `MAXREC`, `RESPONSEFORMAT`, DALI error documents,
VOSI `/capabilities` and `/tables` beside the query endpoint, and UCD1+ on the columns. Its
one genuinely new idea is `TABLE`, which breaks 1.03's identity of one service with one
table and makes the url space look like TAP's rather than like 1.03's.

It is a Working Draft, which is a fact about maintenance rather than a reason to wait: the
checks for it each name the clause they came from, so when the draft moves, what has to
move with it is findable.

So the two are not one piece of work done twice. **1.03 is the odd one**, and the list below
is what *it* does not inherit; SCS2 costs a parameter and a second set of capabilities.

**It is here because tier 0 pays for it.** A cone predicate, HATS partitions pruned by it,
a VOTable writer and a list of published tables with known coordinate columns are every
part of it, and tier 0 builds all four for other reasons. On its own it would be a thin
slice of the same plumbing; after tier 0 it is a route that parses three numbers. It also
answers for more clients than TAP does, being what everything speaks.

**What it cannot do is why it is not tier 0.** One cone, one table, no predicate, no
projection, no join. `phot_g_mean_mag < 18` is not expressible. The reason this service
exists is ADQL over HATS and TAP is what exposes that; this is the smaller door.

Four things it does *not* inherit from tier 0, because it predates DALI by a decade:

- **The error document is not DALI's.** A cone search reports failure as a stubbed VOTable
  carrying an `INFO` (or `PARAM`) with `name="Error"` — not `QUERY_STATUS="ERROR"`. So
  §11.3's renderer does not carry over; this needs its own, which is small and must not be
  unified with the other one on the grounds that both are errors in VOTables.
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

The one MUST tier 0 does not answer, and the whole of tier 1. Every reference service
implements it, so there is no reading of the evidence in which it is optional; what holds it
back is that it is the only part of this phase that is a design question rather than a
mapping.

A job model is state — creation, phases, polling, results that outlive the request that
asked for them, destruction times and their collection — against a service whose every
answer today is collected inside one request future. It is where §5.3's and §7.2's
no-job-queue decision is revisited, where §0.2 is reopened, and where a job id becomes an
authorization surface. UWS specifies the shape, so it gets built once rather than invented
and then reconciled.

Two things stop being true when it lands, and both are written down elsewhere as temporary:
`/capabilities` starts advertising an async interface, and `{api.prefix}/tap/async` stops
answering `404`.

### 11.9 `/examples`

A DALI-examples page of queries that run: TOPCAT reads it and offers them in a menu. Later
rather than tier 0 — all four reference services publish one, but a client works without it,
which is the difference between a capability and a convenience.

The rules `app/openapi/` already follows apply unchanged — an example is a query that runs and
is judged on what it costs, so each names a few columns and a catalog example carries a
circle. It is a page this service serves, so: no CDN, complete without JavaScript.

### 11.10 What a caller gets told

`/docs` describes JSON bodies and TAP takes form parameters, so the TAP surface is described
in its own page rather than bent into the OpenAPI document. What it has to say, once, and in
the README as well: the base url to paste into TOPCAT, the table names, whether `/async` is
there yet and what to do instead while it is not, and which formats carry a nested column.

### 11.11 Table upload

`TAP_UPLOAD`, and the one capability the four reference services do not share — IRSA offers
none, MAST half. So the ecosystem has not settled it, which is what puts it here rather than
in tier 0, and it is the case that shows what they *do* agree on: each of them declares in
`/capabilities` exactly what it has. That is the shape to copy for anything not implemented.

It also reopens what a request may spend, an uploaded table being caller-supplied bytes that
a query then joins against.

### 11.12 `parquet` and `json` over TAP, and a nested column in `TAP_SCHEMA`

The formats this service has of its own, advertised in `/capabilities` as what they are, and
the only way a nested column can be answered at all — `votable`, `csv` and `tsv` each refuse
one.

Both halves are here because no reference service can be asked about either. None of them
publishes a nested column, so there is no practice to follow and no check that can be
calibrated against anybody: what `TAP_SCHEMA.columns` should say about `lightcurve.mag` is a
decision to make alone, and it waits on §7.5 deciding what a nested column is in a VOTable
first.

## 8. Security requirements

Conditions every phase must keep. What holds them today is in `CLAUDE.md`; what is here is
what a later phase can still break.

**Threat model.** The operator is trusted — they wrote the config and run the process. The
caller is not: they supply a url, storage options, credentials, a projection and a
predicate, and the service makes network and filesystem requests on their behalf.

### 8.1 No credential leakage

- **The request body is the only source.** Not an environment variable, not a file on disk,
  not ambient discovery by an SDK, not an instance profile. A request with no credentials is
  unsigned, never the process's own identity. An operator-configured source is §9.1 and must
  be explicit in the config when it arrives.
- Credentials are stripped at the boundary, and a new backend adds its option names to that
  stripping and brings a new signer to check (§8.5).
- A caller must not reach another's through the cache — §6.0's keying rule.

### 8.2 No local filesystem until the config says so

- **Refusal must not be a filesystem oracle.** Outside a mount, 403 whether or not anything
  is there; only inside one does a missing file become 404. §9.1's remote mount sources have
  to keep that where the answer comes from a store.
- Run as an unprivileged user, and document `ReadOnlyPaths=`/`ProtectSystem=` (systemd) and
  read-only bind mounts (Docker) in `docs/deployment.md`. **Not written yet.**

### 8.3 No local network until the config says so

- **Names and addresses are two layers and both are needed**, and every resolved address
  must pass, not only the first.
- **The check runs inside the HTTP client's own resolver**, whose return value *is* the set
  of addresses the connection is attempted against. Anywhere earlier is a check against an
  answer that can be replaced.
- A redirect hop (§2.5) is the one thing that would add a destination the resolver has not
  judged, which is why it is a piece of work rather than a flag.

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

1. **Remote mount sources.** A `[[mount]]` fronting `s3://bucket/hats/` or an HTTP tree.
   Listings become paginated `LIST` calls, static serving a proxied range read with the
   origin's `ETag` passed through, and §8.3's network policy starts applying to the
   file-server mode. `[api.access]` needs a prefix rule kind first: a source prefix is not
   an endpoint entry, which would allow every bucket at that endpoint, and a mount's
   derived grant must stay no wider than the mount. It is also the first mount that could
   need credentials, so it is where §8.1 is revisited — any operator-configured source is
   named explicitly in the config for that mount, never discovered from the environment —
   and it invalidates the reasoning behind the one advisory `deny.toml` ignores, which
   turns on every key being the caller's own.
2. **A plain-url API for public data.** One `GET` whose only parameter is the location, the
   scheme naming the backend as `Backend::from_scheme` already does: the one-liner a
   browser, a `curl` or a notebook cell can write, lowering to the request the `POST` shape
   already carries.

   **Anonymous only, and that is what makes it a `GET`.** The `POST` shape is a `POST`
   because a query string is written to every proxy's access log. So this one must *refuse*
   a credential rather than ignore one — no `storage` object, no headers, no url with a
   query string — and the moment anything here could carry a secret it goes back to being a
   `POST`. Left to settle: where it sits in the url space, a url nested in a url needing
   encoding either way.
3. **A `polygon` region.** `vertices: [[ra, dec], …]`, alongside `circle` and `zone`. Every
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
4. **Tables discovered rather than declared.** §11.1's list is written out per table and is
   temporary; the HATS registry is where it comes from instead. It reopens two things at
   once. §0.2, because a set of tables fetched from elsewhere is a registry across requests,
   with a refresh, a staleness window and two requests that may disagree about what exists.
   And §11.1's url-only table, because a catalog the registry names may need a credential to
   read — which is §8.1's question on a surface whose answers are public, and the reason
   §11.1 declines to answer it early.
5. **Filesystem-driven cache invalidation** (§6.6).
6. **Aggregating inside a nested column.** A ZTF row holds a whole light curve in
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
7. **Separate crates, separate repos.** Once ADQL and TAP exist, split into `hats`, `adql`
   and `tap`. `hats` is the catalog itself rather than this service's use of it — the
   properties file, the partitioning, `Norder`/`Npix`/`Dir`, the MOC, `_metadata` and
   `partition_info.csv`, what the Python `hats` library covers, for anyone reading a catalog
   with no service in front of it. That code is written, so the split is a matter of where
   it lives.

Each phase leaves the service useful, and each is a prerequisite for the next rather than a
parallel track.
