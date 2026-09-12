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
| 3.3 | API request shape (`select`/`where`, `region`) | done | what `region` may still gain is §5.2 |
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

§2–§7 are the phases in order, §8 the conditions every phase must keep, §9 what is
deferred.

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
  bytes come from and `materialize::Transfers` would not see the bytes. The session is
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
whether a `select` names a column the catalog has. Worth having once there is a reason to
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
(four numbers would fit), an offset (`query::Order` promises nothing within a partition, so
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
`FileMetadataCache`, while `parquet_out::read_layout` goes to the store and pays two
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

**No async job interface in this plan.** TAP's `/async` (§9.5) is what forces one and UWS
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
VOSI asks the same question in the astronomy vocabulary and arrives with TAP in §9.5 — two
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
3. **A `polygon` region.** `vertices: [[ra, dec], …]`, alongside `circle` and `box`. Every
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
     That wants a UDF, and a UDF is a thing `sql.rs`'s volatility rule and §5.2's pruning
     both have to be taught about.
   - **Self-intersecting and degenerate input**, each of which the covering and the row test
     can disagree about.

   `cdshealpix` supplies the covering; the exact test, the conventions and the refusals are
   the work.
4. **SQL, then ADQL, as front ends.** Both parse into the structured query the service
   already executes rather than opening a second execution path. ADQL's `CONTAINS`,
   `POINT`, `CIRCLE` and `DISTANCE` map onto §5.2's predicates — and because those
   spellings are specified, that is also when a spatial constraint can move out of the
   structured `region` field into the expression, with unrecognised spatial predicates
   refused rather than silently scanning everything. Plain SQL first: it settles the
   lowering and the rejection messages before the IVOA grammar.
5. **TAP protocol.** IVOA TAP over the ADQL layer: `/sync`, `/async`, VOSI, §7.5's VOTable
   output, the UWS job model. `/async` is a real job system with state, and is where §5.3's
   and §7.2's no-job-queue decision is revisited.
6. **Filesystem-driven cache invalidation** (§6.6).
7. **Aggregating inside a nested column.** A ZTF row holds a whole light curve in
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
8. **Separate crates, separate repos.** Once ADQL and TAP exist, split into `hats`, `adql`
   and `tap`. `hats` is the catalog itself rather than this service's use of it — the
   properties file, the partitioning, `Norder`/`Npix`/`Dir`, the MOC, `_metadata` and
   `partition_info.csv`, what the Python `hats` library covers, for anyone reading a catalog
   with no service in front of it. That code is written, so the split is a matter of where
   it lives.

Each phase leaves the service useful, and each is a prerequisite for the next rather than a
parallel track.
