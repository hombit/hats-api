# Conventions

## Where things go

Four places, four jobs. Writing something in the wrong one is what makes all four rot.

- **Commit messages** are the only place for history. What changed, why it changed, what
  it replaced, what was tried and rejected, what a decision turned on — all of it, and
  only there. Nothing else in the repository records what happened.
- **Code comments** explain the tricky part in front of them: a constraint, a failure
  mode, a reason a plausible alternative is wrong. Not what changed, not when.
- **`DEVELOPMENT_PLAN.md`** is for planning — what is still to be done and what
  constrains it. Never what was built. Finishing a step means moving its row to `done`
  and changing whatever a later step now has to do differently; if nothing later
  changed, the row is the whole update. A note that could begin "we added" is a commit
  message. Something the plan assumed and got wrong is corrected in place, not annotated.
  Work that was considered and not done is simply absent — the commit says why.
- **`CLAUDE.md`** is for instructions to whoever works here next. A rule a finished piece
  of work leaves behind belongs here, phrased as a rule rather than as a story about
  where it came from.

Nothing outside `DEVELOPMENT_PLAN.md` may refer to it — no section number, no filename,
not in code, comments, test names, config or workflows. It is scaffolding and gets
deleted when the work in it is done, and a reference to `§8.1` becomes a dangling
pointer the moment that happens. When code needs a reason, the comment states the
reason; if that makes the comment longer, it was leaning on the plan to finish its
sentence. The `no-plan-references` pre-commit hook greps for both spellings; this file
is exempt because it is where the rule is written down.

## Where the code lives

Modules are grouped by what they are about, and a directory's name is the subject rather
than the layer:

```
access/   what a request may reach: the endpoint rules (policy), the addresses behind them
          (network), the readable directories (mount), a path under one (local), and which
          files are data (data)
          `local` is a path on this machine; a mount whose source is a store has no such
          path, and the two forks apart in `Mounts::resolve`'s callers rather than inside it
storage/  a url opened into a store: the url itself (store), each backend's options
          (options), each builder (backends), and a server that will not serve ranges
          (materialize)
engine/   the caller's SQL (sql) and running a selection against one parquet file (query)
sky/      a shape on the sky: what it means as a predicate (region), which cells cover it
          (healpix), and saying one inside a query (geometry)
hats/     a catalog: its own files (catalog, partitions, properties), recognising one
          somebody is browsing (browse), a request fanned out over it (query), it as a
          table a statement names (table), and the partitions that table reads as rows are
          pulled (scan)
adql/     the statement rewrite (translate), running one (query), the functions the
          language requires (functions), and how a name is written (names)
tap/      what this service publishes over TAP: the operator's tables (tables), what is
          said about each (metadata), and TAP_SCHEMA's own five (schema)
output/   an answer written out: json, dsv, votable, parquet
app/      the HTTP surface: the service and router (service), the file-server mode (files),
          what every body shares (request), what every answer carries (answer), one module
          per route under routes/ — with routes/tap/ for the IVOA resources — the directory
          page (listing) and the API description (openapi/)
```

`config`, `error` and `logging` stay at the top, being everyone's.

**A `mod.rs` holds declarations and nothing else** — the module doc, its `mod` lines and
the `pub use` the rest of the crate reads it through. The code goes in a file named for
what it is, beside it. A `mod.rs` that grows a type is one where the directory's contents
can only be read by scrolling past it.

Moving a type out of a `mod.rs` changes what its siblings can see: a field the parent kept
private is no longer visible to the other children, so it needs `pub(super)`, and a test
helper two siblings share needs `pub(in crate::<module>)`.

## Dependencies

Look for a public crate before writing one. Especially for anything security- or
performance-critical: a widely used crate has had far more eyes on its edge cases than
anything written here in an afternoon. `cargo search`, then `cargo info <name>` for the
licence and features.

Say what was picked and what it costs — added crates, licence, what it does not cover —
rather than adding it silently. Not every crate is worth it: a one-liner with a heavy
dependency tree is not, and neither is one that solves a different problem than the one
at hand.

**Say it in the commit message, not in `Cargo.toml`.** A comment there explaining which
feature a crate was added for is a second place to keep in step with the code, and it is
the one nobody updates when the feature moves or grows: the dependency is still listed
and the reason beside it has quietly stopped being true. What a line in that file is for
is a constraint on the *version* — why `object_store` tracks DataFusion's, why the
properties parser is the one it is — which stays true as long as the line does.

Keep `object_store` matched to DataFusion's, and `reqwest` and
`opendal-http-transport-reqwest` matched to what `opendal` resolves to. Two copies of a
crate are two distinct types, so a mismatch is a type error rather than a version
warning — and one that surfaces several crates from the line that caused it.
`deny.toml` denies multiple versions of those three by name, so `cargo deny check bans`
says which crate went double before the compiler gets a chance to be unhelpful about it.

## Credentials

- **A credential leaves this service in exactly one place, and a caller has to ask.**
  `StorageOptions::echo` writes a request's own options back into a plan's entries, and only
  where that request set `return_storage`. Nothing else reads a secret out — not a log, not
  an error, not a metric, not a plan by default. It discloses nothing, being the caller's
  own secret in a response to their own request; what it costs is a plan that is now a
  document with a credential in it, so a second caller for such an echo needs the same
  argument made again rather than a reference to this one.
- **A credential is never printed, and neither is anything a caller wrote beside one.**
  `storage::Headers` prints its count and nothing else: both the name and the value of a
  header come from the caller, and a token typed into a name is still a token in this
  service's log. The same reasoning is why anything holding one — `MaterializingStore`,
  `WithHeaders` — has a hand-written `Debug`. A `HeaderMap`'s own `Debug` redacts values
  marked sensitive but prints every name in the clear, so a derive is not enough. Mark
  values `set_sensitive(true)` as well; it is the backstop, not the guarantee.
- A credential is never a `String`. Use `secrecy::SecretString`, and
  `storage::SourceUrl` for a caller-supplied url. Neither prints its value, so
  `#[derive(Debug)]` around them is safe. `StorageOptions::named` enforces this: a
  credential is registered with `Named::credential`, which takes a `SecretString` and
  nothing else, and a `SecretString` cannot be registered with `Named::plain`. Both
  mistakes are compile errors, and so is adding an option field and not registering it
  at all.
- `url::Url`'s own `Debug` prints its `password` field, so a struct holding one needs a
  hand-written `Debug` rather than a derive.
- A credential option is a value, never a path or a filename. Reading a credential off
  local disk at a caller's direction is both "the request is the only source" and the
  local-path rules, broken at once.
- A caller's string that ends up inside a hostname — a region, an account — goes through
  `storage::require_label` first, and what gets passed on is the `HostLabel` it returns,
  not the `&str` that went in. A `/` in it moves the host to whatever came before, which
  is a way past the endpoint policy rather than a cosmetic problem.
- Nothing downstream of `storage::open` sees a url with options on it.
- A dependency that logs credentials goes in `logging::CREDENTIAL_UNSAFE_TARGETS`, with
  a reason. `tests/credential_logging.rs` is what catches the next one.

## Adding a backend

Add the `Backend` variant and follow the compile errors. Every match on a `Backend` is
exhaustive, and the served schemes, the accepted options and the endpoint rules are all
derived from it rather than written out beside it, so there is no list to forget.

`Backend::provider` is the fork in that road. A backend that has a provider is addressed
by bucket: the url's host is a bucket name, the server is the `endpoint` option, and the
provider's own service is what a request naming no endpoint means. A backend that has
none is addressed by origin: the url is the server, so it takes no `endpoint` and has no
default to fall back to. That is all `provider` decides — it says nothing about
credentials, and an origin-addressed backend can still take `headers`. Ask `has_provider`
rather than matching on the variant, so a later backend of either shape lands on the
right side without this being rewritten.

A backend may serve more than one scheme — `Backend::schemes` returns a slice, and
`http`/`https` are one backend reached two ways.

**A backend's options are its own type, and that type is the only list of them.** Add an
`XOptions` struct, register its fields in the `Group` impl, add the `Credentials` variant, and
`accepted_options` picks the names up from `names_of::<XOptions>()`. What this prevents is the
list and the backend function disagreeing: `gcs_builder` is handed a `&GcsOptions` and cannot
read `sas_token`, so "the options gcs takes" is one fact rather than a `&[&str]` beside a
function that reaches wherever it likes. The failure that shape allowed was silent in the worst
direction — an option accepted at the boundary and never used.

Three things hold it together, and each is a compile error rather than a check:

- **Every option is registered by destructuring.** `Group::named` and `Group::echo_into` both
  take the struct apart, so a field added and not registered does not build. `StorageOptions`
  destructures its groups in turn, so a whole group — or a bare field beside `endpoint` — added
  and not placed does not build either.
- **`Named::credential` takes a `SecretString` and nothing else**, so a secret cannot be
  registered as plain, which is what would make `allow_cleartext` wave a request through.
- **`StorageOptions::resolve` is where the refusal and the narrowing happen together.** It
  refuses another backend's options and hands back the one `Credentials` variant, so a builder
  is given a value it could not have obtained without the check having run. Reading the groups
  directly, beside a separate call to the check, is what that closes.

`endpoint` and `allow_http` belong to no backend and stay bare fields, named by the `ENDPOINT`
and `ALLOW_HTTP` consts. `endpoint` joins the accepted list only where `has_provider`; a
single-field group for either would be a type bought for nothing.

**The wire form is flat and stays flat.** The groups are `#[serde(flatten)]`, so a caller sends
one flat object — which is also why `StorageOptions` cannot use `deny_unknown_fields`: serde
ignores it on a struct with a `flatten` and drops unmatched keys in silence. A misspelled
`secret_acces_key` dropped that way is an anonymous request the caller reads as an
authenticated one, so the leftovers land in `unknown` and `for_scheme` refuses them. Anything
added here keeps both halves: flat outside, and nothing dropped.

Before committing to a service, check it offers both of these; one that does not cannot
be served here at all.

- **`skip_signature`, or whatever the service calls it.** It is what makes an anonymous
  request anonymous. Without it OpenDAL walks its ambient chain and answers with the
  deployment's identity, and nothing on this side can prevent that.
- **A switch for every ambient credential source**, disabled per store rather than
  globally — `disable_config_load`, `disable_ec2_metadata`, `disable_vm_metadata`.

**The ambient chain to check is the whole dependency tree's, not the builder's.** A service
crate's own dependencies read the environment below OpenDAL, where no builder call reaches
them: `opendal-service-hf` depends on `hf-xet`, which reads `HF_TOKEN` and `HF_ENDPOINT`
itself and is built unconditionally whichever download mode is chosen. A backend whose
credential can be picked up somewhere this crate cannot switch off is one that cannot be
served here at all, whatever its builder offers — so read what the service crate pulls in
before deciding it satisfies the two switches above. `hf://` is written against the Hub's
HTTP API for this reason and not for a shortage of a crate.

A service with no ambient chain at all satisfies both by construction — OpenDAL's http
service sends an `Authorization` header only when the builder was handed one — but say so
in the backend function rather than leaving the absence of the calls to be read as an
oversight.

Then follow the shape the existing ones set:

- Addressing is per backend: path-style against a named S3-compatible endpoint,
  virtual-host against the provider itself.
- `allow_http` stays ours. OpenDAL follows the endpoint's own scheme without asking, so
  the cleartext decision has no backend half to defer to. It has two halves of its own,
  and both must pass:
  - **the operator's**, `access.<backend>.allow_plain_http`, for a backend whose url is
    its own address. What cleartext costs there is the assurance that the bytes came from
    the host the url named, and only the operator knows the network.
  - **the caller's**, the `allow_http` option, wherever the request can carry a
    credential. That is every backend: an origin-addressed one has no `endpoint` option
    but may still be given `headers`. Do not reason from "this backend has no endpoint"
    to "this backend has no secret to protect".
- `object_store` is trait-only here — the `ObjectStore` trait DataFusion consumes, plus
  `LocalFileSystem` for `file://`. Put a new backend on OpenDAL's side; never re-enable
  an `object_store` backend feature.
- **A credential that the probe also needs goes out as a header, not into the builder.**
  `MaterializingStore`'s probe is this crate's own request rather than OpenDAL's, so a
  credential configured on the builder reaches the reads and not the probe. The probe
  then gets a `401`, reads it as "not a `206`", concludes the server ranges, and hands
  the reader the head of the file at every offset — a wrong answer rather than an error.
  `webdav_builder` is the worked example, and `tests/webdav.rs` is what catches it.
- **Ask whether its servers honour `Range`.** A provider's does. A server the caller
  named may not, and OpenDAL accepts a `200` to a ranged read without complaint, so the
  reader gets the head of the file where it asked for the tail — a wrong answer, not an
  error. Any backend whose host comes from the request goes behind
  `storage::materialize::MaterializingStore`, which decides per object and copies to scratch when
  it has to. Reading the code cannot tell the two servers apart; `tests/http_ranges.rs`
  serves both and checks the rows.

## One store per authority

**DataFusion keys a registered object store by `scheme://host[:port]` and drops the path.**
`register_object_store` is called with a `RemoteFile::base`, so what a store is registered
under is the url's authority and nothing below it — which is a fact about every backend and
is what a statement naming two tables runs into.

Two consequences, and a backend has to be built for both:

- **A store must answer for every object at its authority, not just the one the url named.**
  An `s3://bucket` store serves that whole bucket, so a second table in the same bucket
  reaches the right object through it. `hf://` is where this is easy to get wrong: the
  authority is `datasets`, not the repository, so a store built for one repository would be
  registered over by the next and a query naming two Hugging Face catalogs would read both
  through whichever landed last. `HfStore` is therefore the Hub, and the repository is read
  off each key — the first two segments, with everything after them a path inside it, since a
  repository is a directory tree and a catalog is wherever in it somebody put one.
- **Two tables at one authority get one store, so they get one set of credentials.** The
  second registration wins. Two catalogs in one bucket with different keys, or two on one
  Hugging Face Hub with different tokens, is the case that cannot be expressed — and it is
  the registry's grain rather than any backend's. Do not work around it by keying a store on
  something DataFusion does not read; what would fix it is a registry of this crate's own,
  which is a larger decision than any one backend.

  `storage::Authorities` is what refuses that case, and **it compares what built the store
  rather than what the request wrote.** The two are the same thing only where the caller's
  own url is what got opened: a `file://` url resolves through the mounts, so one landing
  in a store-backed mount opens with the *operator's* options under the origin's authority
  while the request carried none. Comparing the written options there would let a second
  table naming that origin outright compare as "no options" on both sides, share the store,
  and read a url the caller chose with the mount's credentials — the mount grants its
  prefix and the store grants the authority.

  **The credentials decide and where they came from does not.** Two stores built from one
  set of options are one store, so which registration survives changes nothing: two mounts
  an operator wrote the same key into share, and so do a mount and a caller who sent the
  key the mount holds, who had it already. Refusing by provenance instead would refuse the
  ordinary crossmatch — two catalogs in one bucket, mounted separately — which is a query
  this service exists to answer.

  **The handle carries what built it, and `storage::Opened` can only be made from one.**
  `RemoteFile::opened` and `RemoteDir::opened` are the only ways to get one, so the
  authority and the credentials come off the same store and a call site has nothing to
  pair wrongly. Do not reintroduce a helper that works the provenance out from the url
  again beside the check: which mount a url lands in is settled once, by the policy, on
  the way to building the store, and a second derivation is the same fact in two places
  with nothing holding them together. `Mount::open` is what stamps a store-backed mount,
  because `storage::open_configured_dir` is handed a url and options and has no mount in
  front of it.

  **A `SessionContext` is what a store is registered into, so one per context is one per
  authority.** `app::routes::tap::published::describe` builds a context per published table
  for exactly this: two tables under two mounts in one bucket hold two sets of credentials,
  and a shared context would have the second registration decide both. Anything that opens
  several tables into one context owes the `Authorities` check instead.

## The network

- A remote store is built through `storage::remote_store`, which is the one thing that
  turns a configured builder into something that can make a request — and the one place
  that puts the access policy's HTTP transport on it. A backend function returns its
  builder and never holds an `Operator`. `clippy.toml` disallows `Operator::new`,
  `reqwest::Client::new` and `reqwest::Client::builder` outside their single permitted
  call sites, each of which carries an `#[expect]` saying so; a new one needs a reason
  written down next to it. An `#[expect]` clippy reports as *unfulfilled* means the rule
  is not covering the call it was written for — check what the call actually resolves to
  rather than deleting the attribute.
- A request this crate makes itself, rather than through a store, goes through
  `NetworkPolicy::client`. It is the same built client the transport wraps, so it carries
  the same resolver and the same refusal to follow redirects. Building a second client
  would resolve names again with nothing checking the answer.
- The address check belongs in the resolver and nowhere else. Checking a host and then
  letting a client resolve it again is DNS rebinding: the answer that passed is not the
  answer that gets connected to.
- Do not follow redirects. A 3xx is the origin choosing the next destination, which
  would carry the caller's credentials to a host no endpoint rule named. The client's own
  policy is `Policy::none()` and stays that way; a store says what it does with a 3xx by
  the `Redirects` it is built with, and `Refused` is the answer unless the origin hands a
  file over *by* redirecting.

  **`Redirects::Followed` is one backend's, and what makes it acceptable is four
  conditions rather than the absence of the rule.** They are written out in
  `storage/redirect.rs`: the origin that named the target was already authorized, the hop
  may not go from `https` to `http`, a credential does not cross an origin, and every
  address is still judged by the resolver. Two things follow for anyone adding a backend.
  The credential half is written for an `Authorization` header, so a backend whose
  credential is a signature over the request — S3's, Azure's — must not be given the layer
  without deciding what a hop does to the signature, which is a different question. And a
  backend that merely *has* redirects is not this case: the reason is that there is no
  second route to the bytes.
- A host named in an endpoint list is permission at both layers — the endpoint rules and
  the network rules. An operator should not have to say it twice.
- `access` decides which endpoint may be named; `network` decides which address may be
  reached. A new rule belongs in whichever of those it is actually about.

## The caller's SQL

`engine/sql.rs` is the only way a caller's expression becomes something this service runs,
and everything about what SQL means here is decided there.

- **Expressions, never statements.** Each field is parsed on its own with `sqlparser`,
  and the parser must reach the end of the string. That is what makes it an expression
  rather than the head of something longer: without the end-of-input check, `1 UNION
  SELECT …` parses as `1` and the rest is dropped in silence. Never assemble a caller's
  text into a SQL statement and check the plan afterwards — the check would be the only
  thing standing between a column list and a join.
- **`allowed` is an allowlist over an exhaustive match**, not a list of what is refused.
  A DataFusion upgrade that adds an expression kind is then a compile error, and someone
  decides whether it belongs in a per-row expression rather than a caller discovering
  that it already did. Add the variant to the arm it belongs in; do not add a wildcard.
- **A function is judged by volatility, never by name.** Only `Immutable` passes: `now()`
  is `Stable` and `random()` is `Volatile`, and both make one request's answer differ
  from the next's for the same query, which is wrong to cache and wrong to reproduce from
  a plan. The rule holds for functions this crate has never compiled in, which is why it
  is not a list of names.

  `engine::sql::AMBIGUOUS` is the one exception, and it is a named one rather than a widening.
  Volatility asks whether an answer matches the next one; it cannot ask whether the answer
  is the one the caller read their own expression as asking for. A name belongs there only
  when both readings are plausible *and* the wrong one comes back as a number rather than
  as an error — `log`, base ten here and the natural logarithm in MySQL, `numpy` and ADQL.
  A function that is merely unwanted is left out of the build instead, where it is already
  an error naming it.

  **`rand` is the one name let *through* the rule, and only inside a statement.** ADQL makes
  it mandatory, so the route that answers ADQL has to have it; every other route refuses it,
  which is what scoping the exception to `engine::sql::Shape::Statement` means. Keep it out of
  `AMBIGUOUS`: that is a list of names refused by a rule they pass, this is a name passed by
  one it fails, and two lists with opposite senses under one name is how the wrong one gets
  extended. It is the only answer this service gives that differs between two identical
  requests, which is a fact worth checking against before adding a second.

  Refusing a name is only allowed where what it meant has another spelling already —
  `log10`, `ln` and `log2` for `log` — and the refusal names them. **Do not add the missing
  spelling by registering one.** The registry a request plans against is DataFusion's, whole
  and unedited; a name of this crate's own would be one no other reader of the same SQL has,
  so an expression that works here would fail everywhere the caller takes it. Every request
  builds its context in `engine::query::session_context`, which is what keeps that one list rather
  than one per call site.

  **What a *language* requires is the other case, and it is not an exception to that.**
  `sky::geometry::register` and `adql::functions::register` put `contains`, `point`, `circle`,
  `moc` and `rand` on the ADQL route's context, and the test is the same one: a name every
  other reader of that language also has. `CONTAINS` and `RAND` are ADQL's, written down in
  a standard, so a statement that works here works against any ADQL service — which is what
  `lg` could not say and why `lg` was the mistake. The two registers are per route and never
  on `session_context`, so the `simple` routes keep exactly DataFusion's own list.
- **A column answers to its own name and to its name in lowercase, and to nothing else.**
  Astronomy column names are mixed-case as a matter of course — `Gmag`, `Norder`,
  `objectId` — and a caller reads them off the file, so the file's spelling has to work;
  lowercase has to work too, because that is what SQL says an unquoted name means. Every
  other casing is refused rather than resolved, so which names a column answers to never
  depends on what else is in the file. Two columns whose lowercase forms collide are each
  reachable by writing them out, and the form they share names neither.

  **This is the `simple` routes' rule and the file server's. ADQL follows ADQL.** On that
  route an unquoted name is case-insensitive and a delimited one is exact (ADQL 2.1
  §2.1.3), for columns and tables alike, which is what `adql::query::resolve_identifiers`
  applies. The difference is what a caller has to go on: a TAP client reads the spelling out
  of `TAP_SCHEMA` before it writes anything, and on the other routes there is nothing to
  read, so there the strict rule trades a guess for a refusal. Do not unify the two.

  This is `resolve_identifiers`, and it needs `enable_ident_normalization` to stay off in
  `engine::query::session_config` — with it on, DataFusion lowercases what the pass did not
  rewrite, and `OBJECTID` starts finding `objectid` again. Neither half works alone: a
  session config that turns normalization back on quietly widens the rule.
- **The schema is what types a literal.** Plan against the file's `DFSchema` so that
  `objectid = 1383212200036217` becomes an `Int64` literal, which row-group statistics,
  the page index and a bloom filter can all prune on. Compared as a string it reads the
  whole file and returns nothing — a slow wrong answer rather than an error.

- **A row's nested column is one value, and a projection into it returns that value.**
  `columns: ["lightcurve.mag", "lightcurve.mjd"]` comes back as one `lightcurve` holding those
  two fields, the way `pyarrow` reads a subset of a struct — never as two columns beside each
  other and never flattened. A client that asked for less of a light curve still has a light
  curve; handing back `mag` and `mjd` separately makes the reader above — `nested_pandas`,
  `astropy` — put the row together again, against a schema that no longer matches the file's.

  `engine::sql::regrouped` and `engine::sql::packed` are where that happens, which is why it
  is in `engine/sql.rs`
  with the rest of what a projection means rather than in the route that took the parameter:
  a body's list and a query string's comma-separated text both reach it, and a path means the
  same in either. Two rules go with it, and each is a case someone will otherwise write the
  other way:

  - **A name that reaches a whole column takes it whole.** `lightcurve` and `lightcurve.mag`
    together are `lightcurve`, every field: the deeper name asks for part of what the
    shallower one already returns, so the union is the column and neither is dropped.
  - **The head of a path has to be one of the file's own fields.** A compound identifier
    whose head is not is a qualified column reference and stays the planner's; treating it as
    a path packs the column into a struct named after the table.

- **`columns` is names and `filters` is an expression.** A caller who wants a computed
  column or an alias writes ADQL. The predicate is the full expression language: a smaller
  grammar would be a second parser and a second allowlist, and it could not express what
  `lsdb` pushes down, which is a disjunction.

  Do not widen `columns` into expressions. The ADQL route already answers a computed column,
  over the same tables and with a planner that sees the whole statement, so expressions in a
  field would be a second way to say it with nothing to choose between the two.

  **The separators belong to the query string alone.** A body writes a list of names and an
  `AND`; a url has one `columns=` and one `filters=`, so the comma between names and the
  `&&`, `,` and `;` between conditions are spellings it needs and nothing else does.
  `engine::sql::columns` takes the list and `engine::sql::column_text` the comma-separated string, both
  through the same per-name code; `engine::sql::filters` and `engine::sql::filter_text` are the same
  expression with and without the separator rewrite. Accepting a separator in a body would
  leave one meaning with two spellings in the carrier that needs neither.

  **The three query routes answer on every target together.** `app::service::with_queries`
  registers them as one set and `app::openapi::description::describe_queries` describes the
  same set, since a request that answered on only some targets would be exceptions a caller has
  to remember.

  **Each endpoint takes its own request type**, in the route module that answers it:
  `routes::parquet::ParquetQuery` carries the column names a file cannot supply for itself,
  `routes::hats::CatalogQuery` carries none of them, `routes::hats::CatalogPlanQuery` adds the
  one field that only means something where a plan is the answer. A field an endpoint has no
  use for is not a field of its request, so there is nothing there to drop in silence, nothing
  to honour by a later change, and the description shows each route what that route takes.
  Both catalog types lower to one `Lowered`, which is what everything below those routes works
  in — so a further catalog endpoint is a wire type and a `lowered()`.

  **A request type declares its fields in the order a body is written** — url, storage,
  region, then the projection and predicate, then how the answer comes back — because that
  declaration is the order `/docs` renders. `ParquetQuery::fields` and its siblings repeat the
  list for the sentence a refusal ends with, and `each_route_describes_its_own_body` holds the
  two to one order. A `#[serde(flatten)]` field would break this: it is an `allOf` and its part
  always lands first.

  **The body stays flat on the wire.** `{url, columns, filters, region}`, never a nested query
  object. Keys a route has no field for are collected into a flattened `unknown` and refused by
  `app::request::refuse_unknown`, which names what the endpoint does take — which `deny_unknown_fields`
  would not, and which the `flatten` on `unknown` rules out anyway. Anything added to one of
  those structs must keep both halves: flat outside, nothing dropped.
- **A parameter this service acts on is honoured or refused, never dropped.** A `filters`
  that does not parse, or that names a column the file has not got, is a 400. Ignoring it
  returns every row, which the caller cannot tell from a predicate that matched every row
  — a wrong answer rather than an error, and the same failure shape as a server that
  ignores `Range`. The service whose parameter names these are does exactly this, which is
  why the names were taken and the behaviour was not.

  A parameter on a path that has no query surface is a different thing and is ignored, the
  way any HTTP server ignores what it has no use for. `access::data::DataFiles` is what draws that
  line — a list of filename globs — so whether a request is a query at all is decided
  before any parameter is read, rather than by each parameter deciding for itself.

  **Which list is the file's, never the mode's.** `[data] filenames` is the default and a
  `[[mount]]` may name its own in place of it, so a file under a mount is judged by that
  mount's list whichever route reached it — the file server asks `Mount::data_files`, and
  the API asks the mount a `file://` url resolved to. A route that reaches for
  `Service::data_files` where a mount governs the file makes the two modes disagree about
  what one file is.

  **For a directory the line is which directory this is.** A catalog has a query surface and
  no other directory has one, so a catalog answers or refuses every parameter it reads and
  anything else is the listing it has always been, parameters and all. The circle is not part
  of that line: it narrows an answer rather than making one possible, the way `columns` and
  `limit` do against a file.

Adding a scalar function feature to the `datafusion` dependency adds everything it
registers to what a caller may call. That is the decision being made; make it
deliberately.

`math_expressions` is the one that is on, because arithmetic over a column is what a
catalog query is for and every function in it is `Immutable` but one — `random()`, which
the volatility rule already refuses and which `engine::sql::tests` holds to that. The rest stay
off, and a request naming one of their functions is an error naming it. Turning another on
means reading its list: `string_expressions` and `regex_expressions` in particular carry
functions whose cost is in the data rather than in the expression, which is a different
question from whether they are immutable.

## A circle in a url

`ra`, `dec` and one radius are the one shape a query string carries: a `zone` is two ordered
pairs and a `moc` is a document, and neither is a parameter. It is built into a `Region` and
checked by `Region::shape`, so a circle means the same thing in a url as in a body rather
than being validated twice.

- **The cap is the file-server mode's alone.** `[limits] max_query_radius_arcsec` acts on
  the request's own numbers, before anything is opened. The other three bounds act on what
  reading turns out to cost and answer with a plan; a url has no plan to answer with, so the
  bound that can refuse early is the one that has to.
- **The circle is optional, and a `limit` is the other narrowing.** Either bounds the request;
  neither is what makes a catalog's url answerable, and a url with neither is the whole
  catalog and refused. So the page offers the limit first — a catalog answers something the
  moment it is opened, and the circle is what a reader adds — and nothing may go back to
  treating the circle as the thing that makes a query a query.
- **A catalog under a mount refuses `ra_column` and `dec_column`**, and a lone file requires
  them. That is the same split the API's `parquet` and `hats` targets make, for the same
  reason, and it is why a url and a body lower to one `Selection` rather than each deciding.
- **`hats::local` is a hint and never the answer.** It recognises a catalog from a filename
  and one small read, because a page has to know before anyone asks; everything it says yes
  to is opened by `Catalog::open` a moment later, and a directory that lied gets the refusal
  any other would. Its walk upwards climbs only a catalog's own layers — `dataset`,
  `Norder=`, `Dir=`, `Npix=` — so a directory beside a catalog is not offered the catalog's
  query, and it stops at the mount, a listing going no higher than one.

## The order of the rows

**The file-server mode returns rows in the source file's order. The API mode promises
nothing about order. Both answer a `limit` reproducibly.** `engine::query::Order` is how a request
says which it is, and `engine::query::reproducible` turns that plus the presence of a `limit` into
the one decision the rest of the code reads.

A `limit` is the part that is easy to get wrong. Under an unstable order it stops being
"how many rows" and becomes "which rows", so the same request returns a different subset
each time — and a caller cannot tell that from the data having changed. That is why
reproducibility does not follow the mode.

Three mechanisms, and all three are load-bearing:

- **`enable_file_stream_work_stealing` must be off.** A file scan hands byte-range morsels
  to whichever partition goes idle, so the partition holding the start of the file is not
  reliably partition 0. Each partition's own rows stay contiguous and ascending either
  way, which is what makes this dangerous: with stealing on the answer comes out *nearly*
  ordered, and often exactly ordered, so it survives a spot check.
- **`collect_partitioned`, not `collect`.** `collect` puts a `CoalescePartitionsExec` on
  top, which takes whichever batch is ready. `collect_partitioned` sorts by partition
  index, and with stealing off that index is the file's order. It keeps every partition
  working, so this is not a serial read — do not reach for `target_partitions = 1`.
- **A `limit` is applied while reading, never by the plan.** `DataFrame::limit` also
  inserts a `CoalescePartitionsExec`, so a plan-level limit chooses arbitrary rows by
  construction. `engine::query::first_rows_in_order` walks the partitions in index order and
  stops, which is both correct and cheap: a stream that is never polled never reads its
  byte range.

`engine::query::tests` crosses these against row counts, row-group sizes, and files written with
page statistics, chunk statistics and none — because the guarantee cannot depend on how a
file was written, and which importer wrote a caller's file is not something this service
gets to assume. Two things such a test needs to stay honest:

- **Assert the scan was actually split.** DataFusion does not split a file below
  `repartition_file_min_size`, which is 1 MiB, so a small fixture reads in one partition
  and every ordering assertion passes while checking nothing. `engine::query::execute` returns the
  partition count for exactly this.
- **Defeat compression.** An ascending column ZSTDs down to nothing, so a fixture needs a
  scattered one to reach that 1 MiB at all.

## The region on the sky

`sky/region.rs` is the only place a shape becomes a predicate. It is a structured field rather
than an expression because the constraint has to be *recognised* to be planned on, and it
lowers to a `datafusion` `Expr` directly rather than to SQL text — there is nothing to
quote and no second parser.

- **Positions are degrees and unsuffixed; an extent names its unit.** `radius_deg` and
  `radius_arcsec`, exactly one of them. A bare `radius` is a plausible cone under either
  reading, the two differ by 3600, and no validation can tell them apart — so the field
  name is the only thing that can.
- **Compare haversines, never angles.** Haversine is monotone in the separation over the
  whole range one can take, so an `asin` on the way out is one more function per row and
  one more rounding for the same rows. It also makes the right ascension need no wrapping:
  the difference enters as `sin²(Δ/2)`, which a whole turn leaves alone, so a file writing
  `[0, 360)` and one writing `[-180, 180)` both work with no arithmetic on the column.
- **A bound around the exact test may only ever be too wide.** The coordinate ranges beside
  the haversine exist to let statistics prune row groups; they are `AND`ed onto the answer,
  so one that is too tight does not fail — it drops rows near the edge and returns fewer
  than the formula asked for. Hence the pad, and hence giving the right-ascension bound up
  entirely where `asin` gets too steep to trust: near a pole its own rounding exceeds the
  pad. `the_ra_reach_of_a_disk_contains_its_boundary` is what holds this, and it has to
  evaluate the extreme at the position angle it is known to be at — `cos θ = tan dec₀ tan
  r`. A scan over position angle walks past the peak near a pole and then passes with the
  guard removed.
- **Which columns hold a position comes from the request, never from the column names.**
  `ra_column` and `dec_column` are required alongside `region`. A file says nothing about
  which of its columns are coordinates, and a guess from conventional names would answer a
  different question than the one asked without saying so. A catalog's `properties` is the
  one thing allowed to supply them.

  **Where a catalog supplies them they are also the only pair allowed**, and a region over
  any other of its columns is refused. A catalog's partitions are chosen by a HEALPix index,
  and that index says where `hats_col_ra` and `hats_col_dec` put a row and nothing about any
  other column — so a region over a different pair is pruned by statistics that do not
  describe it, and the partitions dropped can be exactly the ones holding the positions asked
  for. Fewer rows than the shape contains, with nothing in the answer to say why. A file
  declares nothing and so constrains nothing: naming the two columns is the caller's only
  claim there, and there is nothing for it to contradict.

  How the claim reaches the check is a mark on the schema — `sky::geometry::COORDINATE` on the two
  fields, written by `hats::table::marked` and read by `sky::geometry::declared_position`. It rides
  on the field, so an alias, a join or a subquery between the table and the region test
  changes nothing; a registry keyed by table name would have to resolve all three.
- **A shape that reads two ways is refused, not resolved.** A `zone`'s `ra` runs eastward
  from the first value to the second, so `[350, 10]` and `[10, 350]` are different zones
  and both are legal; the two values naming the *same* point is refused, because it reads
  equally as an empty zone and as the whole sky. `hats` reads that case as the whole sky —
  a deliberate divergence, since nothing in the answer would say which reading was used.
- **A `moc` is cells, and that changes three rules rather than adding a shape.** It is used
  at the caller's own depth — `Shape::covering` ignores `Detail` for it, since re-covering
  the shape *is* moving it — so its inner and outer sets are the same set and it is the one
  exact shape. It has no coordinate test, so it needs no `ra_column`; and it has nothing
  behind its covering, so the covering may not be dropped. `sky::healpix::Cells` carries that
  last one: `Required` turns off `ROW_RANGE_BUDGET`, which for every other shape trades a
  long covering for the geometry and here would trade it for nothing. A dropped covering
  there returns no rows, which reads exactly like a MOC that holds none — and a file with no
  usable HEALPix column is refused for the same reason rather than answered.
- **A caller's MOC is not validated beyond being non-empty.** The JSON parser is lenient: an
  order that does not exist or a value that is not a list comes back as no cells rather than
  as an error. The emptiness check is what refuses those, and it is the right place — a
  region selecting nothing is the failure worth catching, whatever produced it.
- **A coordinate is an `f64` inside the geometry and its own type everywhere else.**
  Every literal in these expressions is an `f64`, and nothing may be trusted to coerce a
  `Float32` column to meet them: a region in a statement is built during the simplify pass,
  after type coercion has run. So `sql::coordinate_column` hands a narrower column over as a
  cast, and `region::separation` casts whatever it is given — a crossmatch and a `DISTANCE`
  arrive there with the caller's own columns. The column in a projection, an answer or any
  other filter is untouched. `sky::region::tests` crosses both column types against both
  right-ascension conventions, and `a_catalog_with_narrow_coordinates_answers_a_region` is the
  statement case.
- A caller's shape is validated once, into a `sky::region::Shape`, and everything downstream
  reads that. Two readings of one field — the predicate's and the covering's — is how the
  two come to disagree about what `dec: [10, 10]` meant.

## The HEALPix covering

`sky/healpix.rs` turns a shape into cells: which partitions of a catalog it can touch, and
which rows of one it cannot. `cdshealpix` computes the coverings and `moc` holds them.

- **Two sets, and each may only be wrong one way.** The outer set contains the shape, so a
  row outside it really is outside; the inner set is contained by it, so a row inside it
  really is inside. Every widening is allowed on the outer set and every loss on the inner
  one; the reverse of either is a wrong row. `a_covering_brackets_its_shape` asks both
  questions of a lattice over the whole sky, which is what catches a covering that drifted.
- **Only the outer set is about being right.** It contains the region, so nothing it
  rejects was wanted, and `outer AND exact` alone would be a complete and correct filter.
  The inner set never removes a row — it says which rows need no geometry. Keep that
  distinction when changing either: a bug in the outer set loses rows, a bug in the inner
  set returns rows that are not in the region, and only the first of those is a question
  about correctness of the *filter*.
- The row predicate is `inner OR (outer AND exact)` and the partition test uses both. What
  the inner half is worth per row is **not measured**: it costs two comparisons per inner
  range on every row and saves the geometry only where DataFusion's `OR` short-circuits,
  which needs the left side true for four rows in five. A batch well inside the region, in a
  file sorted by the column, is that lopsided — which is the case it is kept for. If it is
  ever dropped or defended further, do it with a benchmark.
- **Never a covering at the order the column is written at.** A covering fine enough to be
  exact along a boundary is of order 10⁷ cells for a one-degree circle at order 29, and a
  row tested against thousands of ranges costs more than the trigonometry that was being
  saved. Every depth here is capped well short of 29.
- **Choosing partitions and testing rows are two coverings, not one at two scales.**
  `Detail` is which. Choosing partitions is answered once per partition, so the range count
  is free and the depth follows the *catalog's* order — at the catalog's own order a
  partition the region merely touches is one cell and can only come back as a boundary, so
  it is taken a couple of orders finer. Testing rows puts every range into an expression
  every row meets, so the count is budgeted and the depth follows the *shape* — with a
  floor at the partition's own order, since `Norder` and `Npix` give a partition's exact
  span of cells without reading anything. Without the floor a region far larger than a
  partition is covered at a scale that cannot tell one part of that partition from another,
  and what survives `within` is a single range no row can fail. A change that makes one of
  these better at the other's expense has made something worse.
- **Choosing partitions is driven from the covering, never from the partition list.**
  `Coverage::reaches` walks the covering's ranges and searches into the list, resuming each
  search where the last one landed. Asking `cover` about every partition instead is a pass
  over the whole catalog to find the four partitions a region touches, and a walk that does
  not resume returns a partition coarser than the covering once per range it spans. `cover`
  classifies a candidate once found; it is the loop around it that must not be the catalog.
- **A range set is not a cheap membership test.** It lowers to `h BETWEEN … OR h BETWEEN …`,
  which DataFusion evaluates as two comparison kernels and an `OR` per range over every
  batch — linear in the ranges, no tree and no search. Sixty of them are more arithmetic per
  row than the haversine they were meant to spare it. So the row-level ranges exist to
  **skip row groups**, and are sized to that: a partition holds tens of groups and no set of
  ranges can skip more than exist. Measured, the bytes read were identical at budgets of 8,
  64 and 256 — only the expression's length differed.
- **The covering goes on the left of the `AND`.** DataFusion's `AND` inspects the left side
  first: all false skips the right entirely, and under a fifth true switches to evaluating
  the right on the selected rows alone. That is the mechanism by which the covering spares
  the trigonometry — not the covering's mere presence. Reversed, every row pays the sines.
  The optimizer would put it back, classing `BETWEEN` as cheap and `sin` as expensive, but
  an expression that depends on being corrected is one nobody can read.
- What the alternatives to a range set cost, checked against DataFusion 55 rather than
  assumed, since the answer is the library's and not ours:
  - **A hash set exists and is blind to statistics.** `h >> shift IN (cells…)` becomes an
    `Int64StaticFilter` — constant time per row. But `PruningPredicate` needs a column, and
    a shifted column is not one, so it prunes nothing. It could only ever be an addition
    beside a small range set, and it needs measuring first.
  - **Nothing searches, and sortedness is used only for I/O.** There is no tree or binary
    search in the expression layer. A sorted column pays off through the page index, which
    the ranges already drive. A merge against a table of ranges would be the asymptotically
    right shape, but `PiecewiseMergeJoinExec` takes a single inequality, is experimental,
    and would mean giving up the one-scan-with-a-filter plan.
  - **`ScalarUDFImpl::preimage` says one interval and no more.** A UDF that declares the
    interval `f(x) = v` inverts to has `f(col) = v` rewritten into `col >= lo AND col <
    hi`, which prunes. One contiguous interval, for a comparison: a covering is many.
  - **`ScalarUDFImpl::simplify` says a whole covering**, and is how `sky::geometry::Contains`
    does it: the call replaces itself during the optimizer's simplify pass with the
    expression `sky::region::predicate` builds, and that pass runs before the scan's pruning
    predicate is made. So a region said as a function reads exactly the bytes the `region`
    field reads, which `naming_the_healpix_column_reads_less_of_the_file` asserts.

- **A shape built out of columns is a crossmatch, and it carries no covering.**
  `contains(point(b.ra, b.dec), circle(a.ra, a.dec, r))` is ADQL's own spelling of one, and
  the circle is a different circle for every row of `a` — so there is no one covering, no
  pruning, and what it becomes is `sky::region::within`, the separation and the bound. That it
  prunes nothing is not a gap to be closed by recognising the pattern harder: a scan is
  pruned by one predicate, and this is a predicate over two rows. What bounds such a query
  is each side's *own* region, which does prune, and `max_partitions` refusing a side that
  has none.

  `sky::region::separation` is the one haversine, and both forms go through it: a cone, a
  crossmatch, and `distance(...)` as a value. A second copy of that formula is how two ways
  of asking the same question come to disagree about which pairs are a degree apart.

- **A region test in a statement has to say which table it is about.** `sky::region::Spatial`
  carries a `relation` for that, and `sky::geometry::Contains` fills it in from the caller's own
  `point(...)`. With two catalogs joined, a bare `ra` is a column of each and the predicate
  will not plan — and `_healpix_29` is a column of each too, which is worse: `SpatialIndex`
  finds two, reports that neither names an index, and the covering is silently dropped. Both
  halves are needed, and `engine::sql::fields_of` is what narrows the search to one table's fields.
  `the_adql_route_crossmatches_two_catalogs` runs with one partition allowed, so a lost
  covering is a refusal rather than a slow pass.

- **A region function goes only on a context whose planner also chooses what to scan.**
  The covering prunes row groups inside a file; a catalog's partitions are chosen before any
  file is opened, from a `region` field. On a catalog route `contains` would prune within
  every partition and still open all of them. `sky::geometry::register` is called where a query
  can say a region as a function, never on `engine::query::session_context`.
- **Budget the boundary, not the area.** The interior of a shape merges into few ranges
  whatever the depth — the whole sky is one — so the range count follows the boundary's
  length. Sizing by area instead is the same thing up to a constant for a round shape and
  wrong for a thin one, and a strip of declination is an ordinary request.
- **No cone wider than a quarter turn.** `cone_coverage_approx` stops being a superset as
  the radius approaches a half turn: at 179 degrees it comes back missing a tenth of the
  cells. A disk larger than a hemisphere is the complement of the disk opposite it, and a
  declination band is cut at the equator so each half is measured from its nearer pole.
- **`zone_coverage` is the inner covering's, never the outer's.** Its walk along an edge
  drops wedges of the cell beyond it when the edge lies on a seam between base cells and
  the zone reaches into a polar cap — and a zone's edges are exactly where a caller writes a
  round number. Dropping cells is what an inner covering is allowed to do. For the outer
  one a zone is the intersection of two supersets built from cones: its declination band,
  and the cones enclosing the pieces of its arc.
- **`_healpix_29` is discovered; every other index column has to be named.** It is the one
  name that carries its own order, so it is the only one a file can be recognised as having
  — `SpatialIndex::discover` takes it where the schema holds exactly one column of that
  name, of a type wide enough for an order-29 cell. Two of them names neither, the way
  `engine::sql::resolve_identifiers` has it for any shared name. Everything it rejects is `None`
  rather than an error: nobody claimed the column was there, so its absence is a file with
  no index rather than a fault. That is what makes a HATS partition queried directly as fast
  as the same partition reached through its catalog.
- **A HEALPix column is a name *and* an order.** HATS recommends `_healpix_29` and
  recommends nothing else about it, so the column may be called anything and be written at
  any order, in any integer type wide enough for it — an order-13 catalog fits `Int32`. The
  order is what says which cell a value is, so it is never inferred from the name: read at
  the wrong order every bound is one no row satisfies, which returns nothing rather than
  failing. `SpatialIndex::resolve` refuses a type too narrow for the order it was given.
- **The column is an accelerator, and that is a claim about every answer.** Naming it
  changes what a query costs and never which rows come back, which is why
  `sky::region::tests` runs every case both ways over one fixture and asserts they agree —
  and why one test measures `data_bytes_read` to show the prefilter ran at all. A file
  sorted by the column skips row groups; one that is not gets the same rows, having only
  saved the trigonometry. Nothing checks for the sorting, because nothing depends on it.

## What a catalog says about itself

`hats/` reads a catalog's own files.

**This service is not a validator, of a catalog or of a library.** It reads what it needs
in order to answer the request in front of it, and it fails on a fault it *meets on the
way* — a number that will not parse, a cell number its order does not have, a file that is
not there. It does not go looking: no pass over the partitions checking they tile the sky,
no summing row counts to see whether they match `hats_nrows`, no re-deriving what a
dependency already computed. A broken catalog gets a broken answer, the same as it would
from any other reader, and that is the catalog's problem and its writer's to fix.

Two things follow, and both have been got wrong here before:

- **A check is not free because it is cheap.** The argument "it is only one pass, and the
  list is already sorted" is how a reader becomes a validator one pass at a time. Each one
  is also a claim this service then has to be right about, on catalogs nobody here has
  seen.
- **A library's invariants are the library's.** `moc`'s ranges are disjoint and normalized
  because that is what a `RangeMOC` is; `cdshealpix`'s coverings are supersets because that
  is what the function returns. Nothing here re-checks any of it. Where `sky::healpix::tests`
  cross a covering against its shape, that is testing *this crate's* use of the library —
  the depth it chose, the complement it took — not auditing the library.

Everything below is about reading a catalog, not about judging one.

- **A catalog is a directory, and `storage::open_dir` is how one is opened.** `open`
  requires an object key because it is written for a caller naming a file; that refusal is
  the only check a directory drops. A name from one of the catalog's own files — a
  `file_path` out of `_metadata`, an entry out of a listing — is joined onto the prefix as
  a *path*. Parsed as a url it could carry a scheme or an authority and address a different
  server, which is a catalog choosing where this service connects. That one is not
  validation: it is this service deciding where it will connect, which is never a caller's
  file's decision to make.
- **Where the properties file and the partition list both answer, the partitions do.** The
  deepest order comes from the partitions because that is the list a query is read from.
  `hats_order` is simply not consulted for it — not consulted and compared, just not
  consulted.
- **All three discovery sources describe the same catalog, so all three must produce the
  same partitions.** They differ only in what they know *beside* the cells: `_metadata`
  carries per-partition rows and bytes and the others carry nothing. That is why a
  partition's path is derived from its cell and `hats_npix_suffix` rather than remembered
  from whichever source found it — `partition_info.csv` carries no path at all, so deriving
  is the only thing the three can agree on. `every_source_finds_the_same_partitions` is
  what holds this.

  The order they are tried in follows their cost. `partition_info.csv` is one small `GET`
  and, with the path derived, it is everything a query needs to start — so it goes first
  and answers for every catalog an importer writes. `_metadata` is the fallback and can be
  hundreds of MB, since it holds no rows and its footer is therefore the whole file; over
  `limits.max_catalog_metadata_bytes` it is not fetched at all and the listing answers
  instead. Nothing may reorder these so that the expensive source is on the ordinary path.
- **`hats_npix_suffix` of `/` means the partition is a directory of parquet files**, and it
  is not an exotic shape — ZTF DR24's object catalog is written that way. So nothing may
  assume a partition is one object: `Catalog::partition` returns a `Partitioned`, which is
  one file or a directory, and reading the directory needs a listing. A catalog written
  this way and served over `http(s)://` cannot be read at all, because the names inside a
  partition appear nowhere in the catalog's own metadata.
- **Listing a directory-partitioned catalog scales with the catalog, so it is done in one of
  two ways and the cheaper is counted, not guessed.** `hats_npix_suffix=/` is the only shape
  that needs a listing at all — every other catalog derives a partition's path from the cell
  and the suffix and asks the store nothing. For one that does, the names inside a partition
  are what an entry is built from and appear in none of the catalog's metadata. Asking each
  chosen partition is one request apiece; walking the dataset once is one request per thousand
  files, a listing being paginated at about that. So `Search::partition_files` compares the two
  and a plan over a whole catalog walks: for ZTF DR24 that is thirteen requests against 12,485,
  which was six minutes. Neither walk is more correct than the other and a test holds them to
  the same answer; what the comparison decides is only the cost.

  **None of this is about sizes.** A directory's bytes are not one file's, so these partitions
  carry no `estimated_bytes` either way, and `_metadata` — which is where a size comes from
  when there is one — is not consulted for a catalog whose partition list came from
  `partition_info.csv`. A listing that looks like it is costing a size check is costing a name.

- **The properties file is Java properties, read by `java-properties` — but as UTF-8.**
  The format specifies ISO-8859-1 and the crate defaults to it; a HATS file is written by
  Python and is UTF-8. The two agree on ASCII and part company after it, so the default
  would turn an accented name or a degree sign into different characters — data, not an
  error. Do not "correct" that call back to the format's default. Nor hand-roll the parser
  again: `\uXXXX`, a trailing `\` continuing a line, `:` or bare whitespace as a separator
  and `!` as a comment are all in the format and none is in importer output today, which is
  precisely the shape of a bug that appears years later on one catalog. What is ours is the
  typed accessors — which keys exist and what they mean — not the reading.
- **`hats.properties`, then `properties`, then `collection.properties`.** The first is the
  preferred spelling and the second is deprecated, so that order costs an ordinary catalog
  one request; a collection is asked for last and pays for the two misses ahead of it.
  Nothing may probe for a collection first to save a request there — it would spend one on
  every catalog instead.
- **A collection is followed one hop, downwards, and only ever downwards.**
  `hats_primary_table_url` is a url-shaped key in a file this service reads, so following
  an absolute path or a url with a scheme would let that file choose where the service
  connects next — past the endpoint rules, on a request whose caller named the collection
  and nothing else. A relative path inside the collection is the whole of what is
  followed; anything else is a 400 telling the caller to name the catalog directly.
- **`hats_col_healpix` and `hats_col_healpix_order`, and `_healpix_29` at 29 when a catalog
  says neither.** The order is never read off the column's *name*: `healpix13` is a real
  catalog's column at order 13, and at the wrong order every bound is one no row satisfies —
  no rows rather than an error. What makes the fallback safe is that it is a candidate and
  not a claim: `SpatialIndex::resolve` asks the file's schema, and a file with no such
  column is queried on the geometry alone. A request naming its own pair overrides it.
- **The partition list is sorted by each cell's `sky::healpix::span` start and searched into.**
  That is what makes `Partitions::overlapping` two binary searches rather than a pass over
  the catalog, and it is the order partitions, rows and plan entries come back in. Sorting
  by name instead puts `Npix=1000` before `Npix=2`, which is neither spatial nor numeric.
- **A cell that is not a cell fails where it is read, which is the shape every fault here
  should have.** A pixel outside its order is refused as it is parsed, because converting
  it is what this code was doing anyway and the conversion has no answer. Contrast a pass
  over the finished list asking whether the cells tile the sky: that is a check gone
  looking, and it does not belong here. The partitions are taken for a tiling, `Partitions`
  is sorted and deduplicated on that basis, and a catalog whose cells nest gets whatever
  falls out.

## A request against a catalog

`hats/query.rs` is where a request meets a catalog: it opens one, settles which columns
hold a position, chooses the partitions, and reads them. The rest of `hats/` reads the
catalog's files
and decides nothing; `sky/healpix.rs` answers questions about cells and knows nothing about a
catalog's contents. Keep it that way — the decisions belong in the one module that has a
request in front of it.

- **The catalog names its own columns, and a catalog request has no field for them.**
  `ra_column`, `dec_column`, `healpix_column` and `healpix_order` are `hats_col_*`'s to
  answer, so `CatalogQuery` does not carry them and a body that does is refused as a name the
  route has no field for. Not ignored: a dropped one returns rows tested against columns the
  caller did not write, which they cannot tell from the ones they asked for. They *are*
  written into a plan's entries, since the single-file route those entries go to has no
  catalog to ask.
- **`hats_col_healpix` is passed on only where the catalog names it.** Where it does not,
  nothing fills in the `_healpix_29` default here — the file's own schema is asked instead,
  by `SpatialIndex::discover`. What that keeps is the difference between a claim and a
  guess: a column the catalog named and the file lacks is a broken catalog and says so,
  while the recommended name simply being absent is a file with no index.
- **The coordinate columns are the region's requirement, not the catalog's.** They are
  resolved only where a request carries a region. A catalog that names neither still
  answers a query that asks no spatial question, and refusing one is refusing a request
  that was never going to read a coordinate.
- **A partition the region contains gets no spatial test at all.** That is what the inner
  covering is for, and it is why `Coverage` is computed from both sides. `Selection.spatial`
  is `None` for such a partition — not an empty region, which means something else.
- **Partitions are read in the catalog's own order**, which is HEALPix order, and that is a
  promise the catalog routes make and the single-file route does not. A cell's number is
  where it is on the sky, so the order costs nothing — the partitions are enumerated anyway
  — and it makes a `limit` a coherent piece of sky rather than an arbitrary sample. Order
  *within* a partition is `engine::query::Order`'s and is unchanged.
- **Partitions are read several at a time, and the answer is still the catalog's order.**
  `buffered` yields by position, so the parallelism costs nothing in reproducibility — which
  is the whole of what a `limit` here depends on. Each partition is read with the whole limit
  as its own, since none of them knows what the ones before it matched, and the total is
  trimmed at the end.
- **A `limit` stops the read, at partition granularity.** Once the partitions already yielded
  hold enough rows the rest are dropped unpolled, and a stream that is never polled reads
  nothing. So the front of a catalog costs the front of it. Where it stops is the catalog's
  order and not whichever partition finished first, so the same request stops in the same
  place every time; the overshoot is the reads already in flight, which is what
  `max_concurrent_partitions` bounds.
- **Three bounds, and which of them acts before work happens depends on the `limit`.**
  `max_bytes_fetched` and `max_rows` are always counters watched between partitions. Do not
  make them exact — a total shared across concurrent scans and read often enough to stop one
  mid-file would serialize the thing it is bounding. Overshoot is the price, and something
  else has to bound it.

  `max_partitions` is that something, and it is checked against the chosen list before a byte
  is read — but only for a request with no `limit`, where the chosen list really is what will
  be read. With a limit the read stops itself, so the limit is the bound that acts first and
  the partition count joins the counters. Do not collapse these two cases: refusing a
  `?limit=10` for naming a thousand partitions refuses a request that would have read one,
  and dropping the up-front check for a request with no limit leaves nothing acting early.

  **A partition's declared size is not the pre-check the counters are missing.** It is the
  whole file's compressed size and a query fetches a pruned projection, so it runs one to
  two orders of magnitude high — refusing on it would refuse requests that go on to read a
  percent of it. What it bounds is how large one request could be, which is a fan-out hint
  and not a cost. Do not reach for it to make `max_bytes_fetched` act earlier.
- **A statement reads a catalog the same way: lazily, in HEALPix order, and counted.**
  `hats::scan::CatalogScanExec` is the ADQL route's scan. Pruning chooses among partitions
  without reading anything, and nothing past that — no listing, no footer — happens to a
  partition until the stream reaches it. So stopping is whatever above it stops pulling: a
  `LIMIT`, or a filter that has its rows. `max_partitions` counts partitions opened, and a
  statement that pulls past it ends in a refusal, never in the rows so far.

  Two things keep that true. **Do not enumerate a catalog's partitions in `scan`**: a
  directory-partitioned one is a request per partition, which for ZTF is minutes spent
  planning a query that reads one file. And **round-robin repartition stays off on the ADQL
  context**: a `RepartitionExec` above the scan drains it in a task of its own, so
  `TOP 10 … WHERE mag < 10` reads to the bound. `a_limit_is_answered_from_the_partitions_it_needs`
  holds both.

  **`ORDER BY` the index column is the same walk, from either end.** A partition's index values
  are a range fixed by its cell, and no two overlap, so sorting each partition's rows and walking
  the list forwards or backwards sorts them all. `hats::OrderByIndex` swaps the ordered scan in
  under a `SortExec` and removes the sort only where DataFusion's equivalence analysis says the
  new input satisfies it — never on a match of its own over names or aliases. A `TOP` sort whose
  first key is the index and whose later keys are not keeps its `SortExec`, rebuilt over the
  ordered scan: DataFusion's TopK stops pulling once a batch's last row is past its heap on the
  shared prefix. Without an explicit
  `ORDER BY` nothing is sorted within a partition: the catalog order is a property of the walk,
  not a promise about rows. Two traps: `try_pushdown_sort` is the built-in hook and a
  `FilterExec` does not pass it down; and the fetch the sort carried has to be pushed down again
  after the swap, since a filter without one gathers a whole batch and reads to the bound.
  `an_order_by_the_index_reads_from_that_end_of_the_catalog` holds all of it.
- **A bound reached returns the plan, never a partial answer.** Rows cut off at a limit are
  a value the caller cannot tell from the whole answer. `Outcome::TooMuchWork` carries which
  bound and its two numbers, and the route renders the work list with `reason` set.
- **A plan entry is built from the url the caller wrote, never from a store's.** For a
  mounted catalog a store's url is the operator's absolute path on disk, so an entry built
  from one would publish it — and would hand back a url that names nothing, a local file
  being addressed by its mount. `Search::entries` therefore returns a *path* below the
  catalog and the route joins it onto the caller's url; keep that split.
- **The plan route is not bounded by the limits.** Answering a request too large to run is
  what it is for.
- **The clock is the one bound that hands back nothing.** `max_request_seconds` is a layer
  over the whole router rather than a counter in this loop, so it is reached with the work
  already done and no work list to answer with — a `504` and a sentence. It is also the
  bound that acts first for anything slow, the byte ceiling being larger than a slow link
  covers in the time. Do not give it a plan: building one at that point would run a second
  round of catalog reads for a request that has already been given up on, and the caller
  who wants a plan has a route that answers without doing any of the work.

  It bounds the response future and not the body, which is what makes a large mounted file
  stream freely: every query is collected before it answers, so the handler's own future is
  the work. A bound over the body would cut a download this service is happy to serve and
  bound nothing a query does.

## What a caller's file is like

**Nothing here may rely on how a HATS catalog happens to be written today.** Not the row
group size, not one row group per file, not which of column statistics, the page index and
a bloom filter it carries, not the compression, not the column order, not the writer.
`hats-import` is not the only importer, importers change, and a caller's file may predate
or postdate anything measured here.

That applies to code and to conclusions equally. A setting kept because "today's files
have no page index" is a setting that breaks quietly the week an importer starts writing
one — and one dropped for the same reason is worse, because nothing in the answer would
say the query got slower. A measurement over one file shape is a measurement of that
shape: `engine::query::tests` and `tests/engine.rs` both cross their cases over how the file was
written for this reason, and a finding that holds on one shape and not another is a
finding about the shape.

What a file is observed to carry is worth writing down — it says what is worth asking an
importer for. It is never worth depending on.

## What an answer can say

A value the caller cannot tell apart from a different value is the failure this service
keeps finding, in a new place each time. It reads like data rather than like a fault, so
nothing downstream reports it.

- **JSON has no number for `NaN` or either infinity, and arrow's writer spells all three
  `null`.** Three values a file holds, reported as a fourth it does not — and in a
  photometric column all three are ordinary. `output::json::to_json` installs an `EncoderFactory`
  that writes them as the strings `"NaN"`, `"Infinity"` and `"-Infinity"`, which `float()`
  and `Number()` both read back. It takes over only a column that actually holds one, so
  ordinary data keeps the writer's own faster formatting; the cost of the check is a pass
  over the values, and of the formatting about 3% on a column that needs it.
- **A null is written rather than omitted.** `explicit_nulls` is off by default, which
  makes a row's keys depend on that row's own values and a null indistinguishable from a
  column the projection never asked for.
- **A float is formatted at its own width.** Widening an `f32` to an `f64` first prints
  `1.1` as `1.100000023841858` — the same value, and not the same answer.
- **The page sets these apart from the numbers**, italic and muted, rather than leaving a
  blank cell that reads as nothing much. `null` is marked in any column; the three strings
  count only in a float column, since elsewhere a string is just a string.

A body is compressed on the way out where the client asked for it, which is one layer over
the whole router and two rules to keep:

- **A body that is already compressed is excluded by its content type**, not by its route.
  Parquet is the one today — `app::service::compression` names `PARQUET_CONTENT_TYPE` beside what
  `DefaultPredicate` excludes — and that one line covers a file served off a mount and a
  query encoded into one. A new response type carrying its own compression is another name
  in that predicate; a route-shaped rule would already have missed one of parquet's two
  ways out.
- **A compressed body has no `Content-Length`.** Anything a client is told to size a buffer
  from, or to seek in, has to be a body the predicate declines — which is what makes the
  rule above about the ranged reads an `lsdb` client does, rather than about CPU.

**A parquet answer is seekable, and only parquet is.** `app::answer::seekable` slices the
body this request generated: `206` with a `Content-Range`, `416` for a range past the end,
and `Accept-Ranges: bytes` either way. Parquet is read footer-first or not at all, so a
query answer that refused ranges was one no reader could open — `fsspec` reports such a url
as `partial: False`, hands `pyarrow` a streaming file, and the read ends at `Cannot seek
streaming HTTP file`. Saying `Accept-Ranges: none` was honest and still left the query
surface unusable from the client it was built for.

Three things that rule turns on, and each is a reason not to widen it:

- **The slice is of this request's own body**, regenerated per request, so a reader that
  takes a footer and then three column chunks runs the query four times. What keeps the
  slices coherent is that the same query over the same file answers the same bytes — the
  file's own order, one layout, one writer. There is no cache holding them together, and a
  source file that changes under a reader is the one case nothing here can catch.
- **Only parquet**, because it is the one format read by seeking and the one excluded from
  the compression layer, so it is the one whose `Content-Length` a client can trust. A
  ranged JSON body would be a slice of something the layer above may then re-encode.
- **Only where a `Range` can mean anything.** The API mode answers a `POST` carrying a body,
  so it passes no request parts and keeps `Accept-Ranges: none`; advertising ranges on a
  route that cannot honour them is the same mislabelling one step earlier.

## Directory listings

A directory is served the way `apache` and `nginx` serve one: its own `index.html` if it
has one, otherwise every entry, ordered by name. No paging, no cap, no sort parameters.

- **`text/html` gets a page; everything else gets JSON.** `*/*` is what every client
  library sends, and it is not a request for markup. This is also why none of the
  content-negotiation crates is used: they resolve a wildcard *to* `text/html`, which is
  the right default for a website and the wrong one for a data service.
- **A name is the filesystem's, and it is encoded twice.** Into a url — where `/`, `%`
  and the delimiters must not survive literally, and `=` must, because HATS directories
  are called `Norder=5` — and into HTML, where a name is markup until it is escaped.
  Both encodings live in `app/listing.rs`; nothing outside it builds a url out of a name.
- **The page is scraped, so every link on it is a claim about the directory.** `fsspec`'s
  HTTP filesystem — and the `lsdb` clients above it — reads a directory by pulling every
  `href` out of the markup and keeping the ones below the url it asked for. So each entry
  stays a plain `<a href>` an expression can find, rather than a link a script assembles;
  and nothing else on the page may point below the directory. The breadcrumb and the
  parent row point upwards and are dropped, but a link offering a query on an entry would
  arrive at a client as a file that does not exist. Say such a thing in prose. The catalog's
  own url is the case that tests the rule from the other side: it is this directory or one
  above, so a link to it would survive the scrape — and would put an ancestor somewhere other
  than the breadcrumb, which is where a reader looks for one.
- **The page carries everything it needs.** No CDN, no webfont, no framework: a page that
  is blank on the network this service is built for is worse than a plain one. Inline the
  CSS, and let any script be small enough to inline and optional enough that the markup is
  complete without it.
- **A listing describes only what the same mount would serve.** A mount that does not
  follow symlinks does not list them either, since listing one would only advertise a
  404. `DirEntry::metadata` is an `lstat` and answers "a symlink" a second time; a mount
  that does follow them needs `fs::metadata` on the resolved path to learn what is
  behind one.

## What a mount tells a caller

A mount publishes a directory, not the machine it is on. What is on disk — the source
path, the layout above it, whether a name exists outside what the mount serves — is the
operator's business, and none of it may appear in an answer.

**A `[[mount]]` is the only directory this service reads, and its `path` is the address in
both modes.** The file server publishes it there when `serve` says so, and a `file://` url
in an API request names that same path — never the `source`. So there is no second list of
directories to keep in step with the mounts, and no spelling of a path that reaches a
directory no mount named. `Mounts::resolve` is every mount, which is what the API asks;
`Mounts::published` is the served ones, which is what the file server asks. Reaching for
`resolve` in the file server publishes what an operator did not.

**A source is a directory on this machine or a prefix in a store, and the fork is the
scheme.** `MountSource` carries which, and everything that needs a filesystem asks
`Mount::local_source` rather than being handed something that stands in for one — a store
has no symlinks, no `stat` per name and no server of ours underneath it. What both kinds
share is `Mount::open`, which is a `RemoteDir` either way, so everything that only reads a
directory is written once.

- **Naming a source is the permission, and it is the operator's own url.**
  `[api.access]` decides where a *caller* may point this service; there has never been a
  section of it for a local directory, and a store-backed source is the same thing in
  another scheme. `storage::NamedBy` is what carries that through `build`, and what it
  skips is the endpoint rules and nothing else — the options are still checked, the
  cleartext rule still applies to the operator's own credential, and every address still
  goes through the resolver.

  **A configured source is read through its own HTTP client, and the address rules are
  not on it.** `NetworkPolicy` builds two: a caller's url goes through the one whose
  resolver is `[api.access.network]`, and a `[[mount]]`'s source through
  `configured_transport`, which has no resolver of ours. That is not a hole and not a
  loosening — what the address check protects against is a *caller* pointing this service
  somewhere, and a source's url and options are the config's, fixed at startup, with
  nothing in either a request can reach.

  **Do not put a source's hosts into the rules instead.** It is the shorter change and it
  is wrong: those rules are what a caller's url is judged by, so a host in them is a
  server a caller may name too. Two clients is what keeps the grant exactly as wide as the
  mount, and `a_source_is_reachable_without_becoming_one_a_caller_may_name` holds both
  halves — the mount opens, and the same server named in a request is still refused.

- **A mount's `storage` is the operator's credential and the only one in the config.**
  `StorageOptions::configured` is how it is copied out, spelled out rather than derived,
  and it is the one place a struct holding credentials is copied at all. A local source
  carrying one is a startup error rather than an option nobody reads.

- **A store has no directories, and a listing says so.** `RemoteDir::level` is
  `list_with_delimiter` — one request for one directory, where `RemoteDir::list` walks the
  whole tree — and a prefix with nothing under it comes back empty rather than missing,
  there being no key to be absent. An origin with no listing operation serves its files
  and cannot be browsed.

- **Serving a store-backed mount means answering `Range` here.** `tower_http::ServeFile`
  does it for a local file and there is no equivalent over a store, so `app::files`
  answers the range, the validators and `HEAD` itself. A ranged request answered `200` is
  the mislabelling this service refuses everywhere else: `fsspec` reads a parquet footer
  by range and does not check for a `206`.

Two consequences that are easy to get backwards:

- **A mount's `path` is checked for overlap whether or not it is served.** It is an
  address either way, so two mounts sharing one is two answers to one question.
- **`serve` is the file server's alone.** It says nothing about what the API may read, and
  a rule about local access that reads `serve` is a rule in the wrong place.

- **A local path never reaches a caller.** The log is a different question and may say
  anything — it is the operator's. A response may not, and an error message is where one
  gets out. A store names the path it was reading, so the message a store or a reader
  raises about a local file is not repeatable as-is. `ApiError::from_mount` is where that
  is turned into a message of this crate's own; the original goes to the log. **Both
  modes need it**: a caller who named `file:///hats/x.parquet` wrote a mount's `path`,
  and the store's message names its `source`. Only a url the caller wrote may keep the
  store's own message, the path in that one being theirs.

  **A store-backed mount hides its source the same way, and `ApiError::from_mounted_store`
  is that rule.** The reason is identical — the bucket, the endpoint and the operator's
  prefix are no part of what the caller wrote — and everything the local rule turns on is
  different: there is an origin behind this one, so `502` blames something that exists and
  `404` about an object that is not there is the truth. So the status is kept and only the
  message is replaced, and only where the message names the source at all. `ApiError::Hidden`
  is what carries a status with a message of ours, and nothing else makes one.
- **`RemoteFile::url` is that path**, spelled `file:///…`, for anything local. So it goes
  in the log and never in a response — and neither does an `object_store::path::Error`,
  whose own `Display` prints the path it was handed. `from_mount` is no help with either:
  it passes a `BadRequest` through untouched, on the ground that this crate wrote it, and
  a message this crate wrote out of `file.url` is exactly the case that defeats. What
  a refusal may name is what the *caller* wrote — the url they sent, or the name inside a
  catalog they asked for.
- **No status may describe a store the caller never named.** A mount has no origin behind
  it, so `502` blames a gateway that does not exist. Neither is it a `500`: the bytes of
  that same file are served without complaint when the url carries no query, so a failure
  to read it *as parquet* is a statement about the file. It is a `400`.
- **A refusal says the same thing whether or not the file exists.** A directory that
  cannot be listed and one that is not published answer alike, since the difference is
  itself something about the disk.

Proving any of this needs a case that reaches the arm in question, and the arms are not
obvious: a file whose footer will not parse, one that is empty, and one whose footer
describes rows that are not in it are three different errors with three different
statuses. `a_data_file_that_is_not_parquet_is_the_callers_mistake` carries all three, and
the last one has to be built from a file with more data than footer — strip a ten-row
fixture and the offsets still land inside what is left, so nothing reads past the end and
the test passes without the guarantee.

## Names

**A type and its collection never differ by one character.** `Noun` and `Nouns` read alike
at a use site and mistaking one for the other compiles until it does not, so the collection
is spelled out: `HatsPartition` and `HatsPartitionList`.

That pair carries its prefix for a second reason. DataFusion already owns "partition" here
— `collect_partitioned`, `target_partitions`, `partition_count` are the scan's parallel
partitions and have nothing to do with a catalog's cells. Where a word is already taken,
say which one is meant.

## Comments

Focused and informative. Say what the code does and what a reader could not work out
from reading it — a constraint, a failure mode, a reason a plausible alternative is
wrong.

Not a changelog: "now", "no longer", "used to", "moved here" say nothing to someone
seeing the file for the first time. That story goes in the commit message.

Not thinking-out-loud: no reasoning towards the decision, no defending it to a reviewer,
no restating what the line below already says.

## The API description

`app/openapi/` builds the document and renders the page at `{api.prefix}/docs`: `description`
is what this service's own routes say, `document` builds the document around them, and `page`
renders it. The schemas come from the same `serde` types the routes deserialize, so a field
added to a request appears in both by compiling. Everything below is about keeping that true,
and readable.

- **A `///` on a request or response type is the caller's text.** It says what the thing is and
  how to write it; the reasoning for why it is that way goes in a `//` above, which `utoipa`
  does not read. There is no second channel — `description` is not a field-level `#[schema]`
  attribute — so a doc comment written for the next maintainer is published verbatim to
  whoever is trying to send a request.

  Describe, do not justify, and do not define by negation. "No `WHERE` keyword, just the
  condition" becomes "the condition that would follow a `WHERE` keyword"; "not what the object
  is — that is the url" becomes "the url says which object to read; these say how to reach
  it". A reader wants the thing itself, and a caller cannot act on what something is not.

- **Describe what a caller writes, not what the service does with it.** `columns` says how to
  spell a column and quote it; it does not say which spellings `resolve_identifiers` matches,
  because that is the service's business and unactionable. Say the constraint that changes what
  they type — both halves of a pair, exactly one of two radii, which backends take an option.

- **What a route takes is derived, never restated.** `storage::option_schemes` gives each
  storage option the schemes it applies to, out of the same list that refuses a wrong one, so
  the document cannot claim an option is for a backend that would reject it. Anything else the
  page says about applicability should come the same way.

- **The page renders this document, not arbitrary OpenAPI.** It handles the vocabulary
  `app::openapi::description` emits and no more. Three shapes it has to see through, each of which reads as
  a fault when it does not: a nullable field is `oneOf[null, T]` and must be reported as `T`; a
  tagged variant's tag is the heading and is not repeated among its fields; a component that is
  an `allOf` of others is flattened, since a `#[serde(flatten)]` group is a Rust arrangement and
  the body has no nesting in it.

- **No CDN, and no bundled renderer either.** The page is rendered here, complete without
  JavaScript, for the reason every page this service serves is. Weighed and rejected: Swagger
  UI, RapiDoc, Redoc, Scalar — each loads from a CDN as it ships, so each would have to be
  vendored, and the smallest sends the browser 863 KB against this page's ~40 KB. Scalar's
  defaults also route a try-it request through `proxy.scalar.com`, which on this API would send
  a caller's storage credentials to a third party from a page this service served.

- **An example is a body that runs, and it is judged on what it costs.** Every operation's
  example is sent as-is by the page's runner, so one that 400s or takes a minute is a page that
  reads as broken. What a request against a real catalog costs is **the columns it projects**
  and not the rows it returns — these catalogs are 150 to 370 columns wide, and asking for all
  of them is ten to seventy seconds where four named ones are about one. So every example names
  a few columns, and a catalog example carries a circle, without which the query reads every
  partition. `description::example` takes the columns and the condition as arguments, since
  they belong to the target.

  **The targets differ on purpose, and one of them is a single file.** The catalog examples
  name Gaia DR3, which is all-sky and evenly partitioned, so a reader who moves the circle gets
  the same answer in the same time — an example tuned to one lucky spot is worse than a slow
  one. The single-file example names ZTF DR24's smallest partition because it is the only data
  here with a nested column, and a dotted name reaching into a struct is a headline feature a
  reader can see demonstrated nowhere else on the page. Naming a partition outright is what
  makes picking a small one free: at 180 KB against nearly 4 GB for ZTF's largest, it costs the
  example nothing and saves it a second.

- **`utoipa`'s generics need naming by hand.** `ToSchema::schemas` composes a type argument
  into the name — `PlanBody_T` — while `ToSchema::name` drops it and answers `PlanBody` for
  every instantiation. `description::named` registers under `ToSchema::name`, so two instantiations of
  one generic registered through it land at one key, where the second silently replaces the
  first. A generic request or response type needs its name passed in. A recursive type also
  needs `#[schema(no_recursion)]`, or building the document overflows the stack.

## Tests

`cargo test` must pass with no network, no Docker and no credentials. Anything needing a
real server is a separate test binary that skips when its env vars are absent.

Do not run MinIO or any other container locally; the MinIO tests are CI's.

Check a signer by running it, not by reading it. `tests/credential_logging_canary.rs`
asserts each backend actually logged something, because a backend whose signer never ran
reads exactly like one that leaked nothing. The same goes for the network rules: prove a
guarantee has teeth by removing it and watching the test fail.

Known blind spot to work around, not to trust: `disable_config_load`'s second job —
stopping `AWS_ENDPOINT_URL` from redirecting a request — is not observable through a url
with no `endpoint` option, because virtual-host addressing turns a redirected endpoint
into `bucket.<host>`, which does not resolve. Every backend's equivalent guard shares
it.

## The TAP surface

`tap/` is what this service publishes over IVOA's protocols and `app/routes/tap/` is the
HTTP it is published through. TAP is written on top of DALI and defers to it constantly, so
reading TAP alone leaves the requirement unread: `RESPONSEFORMAT` is "fully described in
DALI", the error document is DALI §4.2, `QUERY_STATUS` and the `OVERFLOW` marker are §4.4,
and the parameter rules are §3.

- **A published table is `[[tap.table]]` and nothing else.** A name and a `path` — a path
  under a `[[mount]]`, which is the address the file server publishes that catalog at and
  an API request names it by. No storage options and no url: what it takes to reach the
  catalog was written once, on the mount, so an operator's secret stays out of the section
  describing a surface whose answers are public *and* a catalog behind a credential is
  publishable. Adding either field back is reopening that, not adding a convenience. Every
  path is turned into its `file://` url and goes through `storage::open_dir` at startup, so
  the refusal reaches an operator rather than a caller — and the mount's own options reach
  the TAP resources without being repeated, because a `file://` url resolves through the
  mounts like any other.
- **One list of facts feeds both documents that publish them.** `TAP_SCHEMA` and VOSI
  `/tables` are the same metadata twice, and a validator reads them against each other, so
  both are rendered from `tap::metadata` — the columns, the flags and the foreign keys
  alike. A second list is how the two come to disagree about a name.
- **A published name is one a query can write.** `adql::names::as_written` delimits a name
  ADQL's grammar does not admit — `_healpix_29` is in every HATS catalog — and TAP §4.3 asks
  the published name to carry the quotes. Two things it deliberately does not do: it does
  not apply the reserved-word list, `DEC` being on it and bare in every catalog anyone
  publishes, and it does not quote a dotted path as a whole, the dot being structure. A
  validator complains about the second; `"lightcurve.mag"` names no field, which settles it.
- **A row bound truncates here and refuses everywhere else.** `OVERFLOW` after the table is
  the in-band statement whose absence makes a cut answer indistinguishable from a whole one,
  so `adql::query::Rows` carries which a request wants. `csv` and `tsv` have nowhere to put
  it and carry `x-hats-overflow` instead, which is this service's own and better than
  nothing being said.
- **`MAXREC` truncates after the query's own `TOP`, never over it.** TAP §2.7.4: the
  truncation "occurs after any limitations imposed by the query", so `TOP 2` with `MAXREC=10`
  is two rows and no overflow. `MAXREC=0` is the columns, no rows, and the marker whether or
  not anything matched — and the query need not be run at all.
- **Nothing is advertised that is not there.** A client picks its interface out of the
  capabilities document and has no way back, so there is no async interface in it while
  `/tap/async` answers 404, and no `uploadMethod` while `UPLOAD` is refused. The output
  formats come from the same list the query resource reads.
- **A name nobody defines is ignored; a standard one this service has not got is refused.**
  `taplint` adds a parameter of its own to every query and reports a service that refuses as
  breaking it, which is also what every HTTP server does with a query string it has no use
  for. The house rule is about a parameter this service *acts on*.
- **A parameter's value is read by `tap::dali`, into a type.** It is a `serde` data format
  over the one grammar DALI writes every value in: fields separated by a delimiter, where a
  leading keyword says how many follow it. So a parameter is a type — an `enum` whose
  variants are the keywords, a tuple whose arity is the count — rather than a chain of
  `split_once` and a conditional per call site. Three things in it are the format's and not
  a type's: `Tail` takes the rest of a value verbatim, so a credential carrying the delimiter
  arrives whole; the delimiter is the parameter's, TAP writing a comma and DALI a space; and
  a keyword is matched whatever its case, the type's own spelling being what it is read as.
  A new parameter is a type over that reader, never a fourth parser.
- **A parameter of this service's own is written `<name>,…` and repeats.** `UPLOAD` is the
  only parameter in TAP or DALI whose value is keyed by a name, and its key is one comma;
  DALI's structured values are fixed tuples of numbers, and several values of anything are
  said by repeating the parameter (§3.2). So `UPLOAD_STORAGE_OPTION` and `UPLOAD_TYPE` take
  that shape and nothing more inventive — and in the first, **the value runs to the end**,
  since a separator inside a value cuts a credential short and an anonymous request is not
  one a caller can tell from an authenticated one. How many fields precede that value is the
  option's own name to say, the way DALI's `POS` reads three numbers after `CIRCLE` and four
  after `RANGE`: `header` names a header before its value, every other option does not. A new
  parameter here follows the same rule rather than growing a syntax of its own.
- **A url as `UPLOAD` is not the upload `/capabilities` would be advertising.** The standard's
  referenced upload fetches a VOTable; this fetches a HATS catalog or a parquet file, so no
  `uploadMethod` is declared while that is all it does, and the feature is found by reading
  the README. TAP's own answer for a url needing credentials is delegation, which this is
  not.
- **Every answer is a document a TAP client can read, refusals included.** `ApiError` renders
  JSON, which a client looking for `QUERY_STATUS` has nothing to say about — so the status is
  kept and the body is replaced, by `app::routes::tap::answer::answered`.

## Measuring the TAP surface

`tap-conformance/` drives `pyvo` and STILTS against a built service and reports which
parts of TAP, DALI, VOSI and ADQL answer. It is a uv project with its own lock, run with
`uv run pytest -c pyproject.toml` from that directory.

- **It is written against the standards and the clients, never against this service.** A
  check is argued from a specification or from what other TAP services do, and a check
  that fails is a finding until shown otherwise — never a reason to loosen the check so
  the number goes up. That is why it was written before the implementation, and it is the
  one property the whole thing is worth nothing without.
- **Nothing may pass against a service that implements none of it.** Six checks once did:
  a missing resource answers 404, which reads as a refusal to anything that only looks at
  the status, and a validator stage that finds no document to validate reports a warning
  rather than an error. So a refusal has to *be* one — a VOTable carrying
  `QUERY_STATUS="ERROR"`, which is what DALI §4.4 says — and a stage that looked at
  nothing fails. Run the suite against a service with the feature ripped out; if the
  number does not fall, the check is measuring nothing.
- **`taplint` lints, `tapquery` is the client, and `votlint` reads one document.** The
  validator composes its own queries from the metadata and is nobody's way of getting data;
  `stilts tapquery` is what TOPCAT runs underneath; `stilts votlint` takes the bytes of one
  answer and reads them as a VOTable reader would, schema and all. All three are used, for
  different questions. The third is the only one that looks at the document a query answered
  with: `taplint` validates the three VOSI documents and no others.

  **`votlint` exits 0 whatever it finds**, so its findings are its output and nothing may
  read its status. It takes the body on standard input, which is what keeps the bytes
  checked the ones that came off the wire.
- **A check that a `taplint` stage already covers does not get a hand-written twin.** The
  validator is stronger wherever they overlap — it checks documents against schemas, every
  UCD against the vocabulary, `/tables` against `TAP_SCHEMA` column by column. What belongs
  beside it is what it structurally cannot do: whether an answer is *right*, whether the
  other client can read it, and how a deliberate absence behaves.
- **Three questions, counted apart** — does it follow the standard, do the clients work
  against it, are the answers right. A document can carry everything the standard asks for
  and still hand a client a byte it refuses to decode. Which question a check speaks to is
  derived from how it asks, so it stays true as checks are added.
- **There is no "expected failure" and nothing may be marked one.** Whether this service
  has decided not to implement something is a fact about its plans, and a suite that knew
  about those decisions would be one written against an implementation. A MUST that goes
  unanswered is a failure whoever is asked.
- **Green for a finding, red for a bug.** Failing checks are the output — nobody expects
  the whole of TAP, and a mark that is always red is one everybody scrolls past. The run
  exits non-zero for a crash of the service under test or of the suite, an exception in a
  check included; both leave a report that reads exactly like a service implementing
  nothing.
- **Every check has a clock**, and it is `pytest-timeout` rather than a budget written into
  each check. One slow service held a whole run four times before that was learned.
- **CI never puts a question to another service.** The suite can be pointed anywhere with
  `--base-url`, and `tap-conformance-survey` does exactly that across several reference
  services — but by hand, with the results committed under `references/`. Putting somebody
  else's service through a few dozen queries on every push is not ours to do.
- **Reference snapshots are regenerated all together.** They are only comparable if every
  column was produced by the same checks; a changed check makes a row mean different things
  in different columns, and the disagreements are the entire value of having them.

## Before committing

`cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`, `cargo test
--all-targets`. `pre-commit run --all-files` runs all three.

Also `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --document-private-items`, which
pre-commit does not run. It is where a doc link to a private item turns up, and clippy
does not see those.

**Every change goes on a branch and through a pull request.** Nothing is committed to
`main` directly. What that buys is a green run of the whole matrix — the live MinIO and
WebDAV jobs among them, which nothing local can stand in for — before the branch anyone
else builds from has the change on it.

## Releasing

Six steps, in this order. The first three are one commit, and the tag is what turns it
into a release.

1. **`version` in `Cargo.toml`.** This is the number the release is; everything below
   reads it rather than restating it.
2. **`cargo update`.** A release is the moment to take the dependency updates that need no
   code change, so the version that gets a tag is the one built against current crates.
   `Cargo.lock` is committed, so this is a real change and belongs in the release commit.
   Anything that will not build is a pull request of its own, not a release problem.
3. **`CHANGELOG.md`.** A new `## [x.y.z] - YYYY-MM-DD` section, dated in UTC, holding what
   `[Unreleased]` had accumulated. **One line per entry**, naming the thing that changed —
   a route, a body field, a config key, an environment variable, an image tag, a status —
   and nothing about how it works or why. A reader scanning for what breaks their client
   cannot scan a paragraph, and the whole of the reasoning is in the commit anyway. A
   dependency bump that changes none of those is not an entry at all. Add the comparison
   link at the foot beside the others.

   **An entry that breaks a caller starts with `**Breaking**`**, before anything else on
   the line — a renamed or removed field, a route that moves, a default that changes an
   answer, a status a client matched on. The reader deciding whether to upgrade is
   scanning for exactly these, and a section holding one of them among several ordinary
   entries gives them nothing to scan for. The heading does not say it: a break can land
   under `Changed` or `Removed` alike, and `Added` is the one heading it never lands under.

   **An entry ends with the pull requests that carried it**, last on the line and after
   the full stop, each written out as a link — `[#123](https://github.com/hombit/hats-api/pull/123)`,
   and `[#123](…), [#256](…)` where it took more than one. An issue is the same with
   `/issues/`. The line says what changed and nothing about how, so the number is the
   whole of the way from the changelog to the reasoning, and the file is read outside
   GitHub — in an editor, in a package's docs — where a bare `#123` links to nothing. A
   number is not always known when the line is written, so a release fills in the ones
   that went in without one; an entry no pull request produced carries nothing rather
   than a guess.

   `[Unreleased]` keeps all six headings with `--` under them, which is the menu whoever
   adds an entry picks from. A release takes the headings that have entries, leaves the
   placeholders where they are, and the new section carries only the headings it filled.
4. **Commit it as `vx.y.z`**, the version alone as the subject. That is what makes the
   release commit findable among the ones that describe changes.
5. **Tag `vx.y.z`** on that commit and push it. The tag is the trigger: pushing it builds
   and publishes the release image, and the Docker workflow checks the tag against
   `Cargo.toml`'s `version`, so a tag that disagrees with step 1 fails rather than
   publishing a mislabelled image.
6. **The GitHub release**, titled `Release vx.y.z`, with GitHub's own generated notes as
   its body:

   ```sh
   gh release create vx.y.z --title "Release vx.y.z" --generate-notes
   ```

   That is every merged pull request with its number and author, the new contributors and
   the compare link — the whole development history of the release, written by nobody.

   Something may be added above it by hand, and only something critical: a step an
   operator has to take before upgrading, a change that will break a running deployment.
   Not a summary of the release, which the list below it already is.

   The changelog is a separate document and neither is copied into the other. It is
   written by hand, carries only what a caller or an operator does differently, and is
   read by someone deciding whether to upgrade. The release notes are read by someone
   asking what went into this tag and who wrote it. Merging them gives each reader the
   other one's document.
