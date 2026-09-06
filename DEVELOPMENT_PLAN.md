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
| 2.4 | WebDAV | deferred | until after §4. Blocked on the scheme question below, which decides whether a test server is reachable at all |
| 2.5 | Hugging Face | deferred | until after §4, and droppable |
| 3.1 | two-mode configuration | done | |
| 3.2 | routing | done | |
| 3.3 | API request shape (`select`/`where`) | done | `region` is specified below and built in §5.2, which is where it can first be executed |
| 3.4 | file-server request shape | todo | needs `docs/vizcat-compat.md` written from the live service first |
| 4 | file-server interface | todo | |
| 4.1 | write the README | todo | after §4: both interfaces are then settled, and one document can describe them together. It is a stub until then |
| 5.1 | HATS catalog metadata | todo | |
| 5.2 | spatial predicate | todo | brings `region` (§3.3) and `POST /api/v1/hats` with it. Order policy and range budget to be settled by measurement first |
| 5.3 | sync / plan / auto | todo | |
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

**Settle the scheme before writing any of it.** `webdav://` names a protocol and a server
but not the transport underneath, and everything else here depends on what fills that gap:

- `http`/`https` are one backend reached two ways, so the caller's url states the
  transport and `allow_plain_http` is only the operator's half of a decision the caller
  also makes with `allow_http`. A single `webdav://` scheme has no caller half, so
  `[access.webdav]` cannot carry an `allow_plain_http` that means the same thing.
- If `webdav://` is always TLS, no server in this repository's tests can be reached
  through it — they are all plain http on loopback, and the resolver will not reach a
  cert-less host. The range probe, the credential on the wire and materialization would
  then be verified for this backend only by reading them, which §8.5 and `CLAUDE.md`
  both refuse.

So the choice is between a second scheme for the cleartext case, an operator-listed
endpoint whose own scheme decides the transport for that host, and accepting an untested
backend. Whichever is picked, the endpoint list must not be able to hold an entry that
nothing could ever match.

| option | meaning |
|---|---|
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

The transport, the target paths and the two expression fields are settled. What is left
of this step is `region`, specified here and built in §5.2 — a region needs the catalog's
own `ra`/`dec` column names to refine on, and those come from §5.1's properties file, so
there is nothing it could execute against before then. `POST /api/v1/hats` arrives with
it, for the same reason.

```json
{
  "url": "s3://bucket/hats/ztf_dr24",
  "region": [{"type": "circle", "ra": 320.65747, "dec": -12.35315, "radius": 0.01}],
  "select": "objectid, lightcurve.mag AS mag, objra, objdec",
  "where": "filterid = 2 AND mag < 20 AND objectid IN (1383212200036217, 1383212200036218)",
  "format": "parquet",
  "limit": 1000
}
```

`region` is a structured field rather than part of the `where` expression (§3.5), and
drives partition pruning in HATS mode (§5.2).

**It is always an array; each element is an object with a `type`.** One region is an
array of one. Every shape lowers to a MOC, which is what the planner needs anyway.

```json
"region": [{"type": "circle", "ra": 320.65747, "dec": -12.35315, "radius": 0.01}]
"region": [{"type": "circle", …}, {"type": "moc", …}]
```

| `type` | fields |
|---|---|
| `circle` | `ra`, `dec`, `radius` — the cone search, under ADQL's name for it |
| `box` | `ra`, `dec`, `width`, `height` |
| `polygon` | `vertices: [[ra, dec], …]` |
| `moc` | `ascii`, or `url` — an IVOA MOC given directly |

Degrees throughout; a `frame` field defaults to `icrs`. Unknown fields are rejected.

- **The array is a union.** State it in the docs and in error text, since a reader coming
  from `where` may expect `AND`. Intersection and difference are cheap to add later as
  explicit combinators.
- **`moc: {url: …}` is a caller-named fetch** and goes through §8.3 like any other.

### 3.4 The file-server request shape

`GET`, a query string on the file's own path, using vizcat's parameter names. Browsers,
`wget`, `lsdb` clients and §5.3 plan entries speak this one.

**We adopt vizcat's query syntax — not more, not less.** Parameter names and the grammar
inside them; not their URL structure, not their limits.

| vizcat parameter | meaning | ours |
|---|---|---|
| `columns` | comma-separated column names, projection (`${X}` escapes awkward names) | same name, same meaning, no count limit |
| `filters` | a SQL-`WHERE`-like row constraint, e.g. `Gmag>8.0 && o_Gmag>100` | same name, same meaning; accept `AND` as well as `&&` |

Never reuse one of their parameter names for different semantics. Extensions with no
vizcat equivalent — `format`, `limit` — take names of our own.

**No spatial parameter.** Spatial selection here is by path, addressing `Norder=k/Npix=p`
directly. A caller who wants the catalog to choose partitions uses the API (§5.2) against
the same data, which the mount's derived grant permits.

Before implementing, complete `docs/vizcat-compat.md` from the live service: the operator
set `filters` accepts, whether `AND`/`OR` work alongside `&&`, the `${X}` escaping rules,
and the response shapes.

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

Serving a file's bytes is done: §3.2 resolves the path and `tower_http`'s `ServeFile`
answers with the ranges, the validators and the conditional requests. What is left, in
order of precedence:

1. **A directory path** → a listing. HTML for a browser (`Accept: text/html`), JSON
   otherwise — a `readdir` over the mount. Cap entries and paginate: a HATS `Dir=` level
   holds ten thousand entries. A directory is a 404 until this exists.
2. **A parquet file with query parameters** → a query through `query.rs` +
   `parquet_out.rs`, with the file taken from the mount. The parameters are §3.4's, which
   waits on `docs/vizcat-compat.md`. A file with no query parameters keeps going out
   verbatim, whatever its extension.

The static-serving path must not regress: an `lsdb` client pointed at a mount should work
with no knowledge of anything else this service does. Nothing here has been tried against
a real one yet, which is the one check this phase cannot do by reading.

### 4.1 Write the README

Both interfaces exist by this point and neither is still moving, so one document can
describe them together: what the service is for, the two request shapes, the storage
options, the configuration file, and how to run it. Written earlier it would document a
shape that then changed, which is worse than the stub that is there now.

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

**Parquet target:** the same, with step 1 skipped. Today's `column == value` behaviour is
`where` with no `region`.

`box` and `polygon` are the same machinery with a different covering step. Nearest-object
lookup is a `circle` plus ordering and `limit: 1`, not a predicate, and waits for ordering.
`crossmatch` is out of scope — `lsdb`'s job.

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

The first two are the ones worth building. Parquet footers are read **twice per
`format=parquet` request** — once by `query.rs` to plan, once by `parquet_out.rs` to copy
the source layout; fix that directly as well as caching it.

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
- OpenAPI description of the API mode, generated rather than hand-written.
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
- `POST` body size limit. The depth and node caps on the expressions are done, at parse
  time. The projection needs no cap: it is bounded by the schema, and the byte and time
  limits govern the data it moves.
- Reject pathological parquet early — a footer claiming implausible row-group or column
  counts is a 400, not an allocation.

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
   not what it may reach. Two things to settle when it is built: where it sits in the url
   space, given a url nested in a url needs encoding either way, and whether it answers
   only the whole object or takes §3.4's `columns`/`filters` as well — those are the same
   parameters the file-server mode already speaks, and having two spellings of them would
   be the divergence §3 exists to avoid.
3. **WebDAV over cleartext.** §2.4 serves TLS only. A WebDAV server on an internal
   network without a certificate is an ordinary deployment, so there needs to be a way to
   say so — a second scheme the caller writes, an operator-listed `http://` endpoint that
   decides the transport for that host, or both. It is deferred rather than dropped
   because the shape has to match `[access.http]`'s two halves: the operator agreeing
   cleartext is acceptable on this network, and the caller agreeing to put *their*
   `username` and `password` on it. Getting one half and calling it done is how a
   credential ends up in the open.
4. **SQL, then ADQL, as front ends.** Both parse into the structured query the service
   already executes (§3.5), rather than opening a second execution path. ADQL's `CONTAINS`,
   `POINT`, `CIRCLE`, `DISTANCE` map onto §5.2's spatial predicates. Plain SQL first: it
   settles the lowering and the rejection messages before the IVOA grammar.
5. **TAP protocol.** IVOA TAP over the ADQL layer: `/sync`, `/async`, VOSI endpoints,
   `VOTable` output, the UWS job model. `/async` is a real job system with state, and is
   where §5.3's and §7.2's no-job-queue decision is revisited.
6. **Filesystem-driven cache invalidation** (§6.7): `SIGHUP` first, then a `notify` watcher
   over local mounts.
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
