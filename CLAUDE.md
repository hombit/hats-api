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

## Dependencies

Look for a public crate before writing one. Especially for anything security- or
performance-critical: a widely used crate has had far more eyes on its edge cases than
anything written here in an afternoon. `cargo search`, then `cargo info <name>` for the
licence and features.

Say what was picked and what it costs — added crates, licence, what it does not cover —
rather than adding it silently. Not every crate is worth it: a one-liner with a heavy
dependency tree is not, and neither is one that solves a different problem than the one
at hand.

Keep `object_store` matched to DataFusion's, and `reqwest` and
`opendal-http-transport-reqwest` matched to what `opendal` resolves to. Two copies of a
crate are two distinct types, so a mismatch is a type error rather than a version
warning — and one that surfaces several crates from the line that caused it.
`deny.toml` denies multiple versions of those three by name, so `cargo deny check bans`
says which crate went double before the compiler gets a chance to be unhelpful about it.

## Credentials

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

Before committing to a service, check it offers both of these; one that does not cannot
be served here at all.

- **`skip_signature`, or whatever the service calls it.** It is what makes an anonymous
  request anonymous. Without it OpenDAL walks its ambient chain and answers with the
  deployment's identity, and nothing on this side can prevent that.
- **A switch for every ambient credential source**, disabled per store rather than
  globally — `disable_config_load`, `disable_ec2_metadata`, `disable_vm_metadata`.

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
- **Ask whether its servers honour `Range`.** A provider's does. A server the caller
  named may not, and OpenDAL accepts a `200` to a ranged read without complaint, so the
  reader gets the head of the file where it asked for the tail — a wrong answer, not an
  error. Any backend whose host comes from the request goes behind
  `materialize::MaterializingStore`, which decides per object and copies to scratch when
  it has to. Reading the code cannot tell the two servers apart; `tests/http_ranges.rs`
  serves both and checks the rows.

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
  would carry the caller's credentials to a host no endpoint rule named.
- A host named in an endpoint list is permission at both layers — the endpoint rules and
  the network rules. An operator should not have to say it twice.
- `access` decides which endpoint may be named; `network` decides which address may be
  reached. A new rule belongs in whichever of those it is actually about.

## The caller's SQL

`sql.rs` is the only way a caller's expression becomes something this service runs, and
everything about what SQL means here is decided there.

- **Expressions, never statements.** Each field is parsed on its own with `sqlparser`,
  and the parser must reach the end of the string. That is what makes it an expression
  rather than the head of something longer: without the end-of-input check, `1 UNION
  SELECT …` parses as `1` and the rest is dropped in silence. Never assemble a caller's
  text into a SQL statement and check the plan afterwards — the check would be the only
  thing standing between a select list and a join.
- **`allowed` is an allowlist over an exhaustive match**, not a list of what is refused.
  A DataFusion upgrade that adds an expression kind is then a compile error, and someone
  decides whether it belongs in a per-row expression rather than a caller discovering
  that it already did. Add the variant to the arm it belongs in; do not add a wildcard.
- **A function is judged by volatility, never by name.** Only `Immutable` passes: `now()`
  is `Stable` and `random()` is `Volatile`, and both make one request's answer differ
  from the next's for the same query, which is wrong to cache and wrong to reproduce from
  a plan. The rule holds for functions this crate has never compiled in, which is why it
  is not a list of names.
- **Identifier normalization is off**, in `query::session_config`. Astronomy column names
  are mixed-case as a matter of course — `Gmag`, `Norder`, `objectId` — and SQL's usual
  lowercasing would report every one of them as missing. Unquoted identifiers therefore
  mean exactly what the file calls them. A new session config must keep this, or the same
  query answers differently depending on which one built it.
- **The schema is what types a literal.** Plan against the file's `DFSchema` so that
  `objectid = 1383212200036217` becomes an `Int64` literal, which row-group statistics,
  the page index and a bloom filter can all prune on. Compared as a string it reads the
  whole file and returns nothing — a slow wrong answer rather than an error.

Adding a scalar function feature to the `datafusion` dependency adds everything it
registers to what a caller may call. That is the decision being made; make it
deliberately.

## Comments

Focused and informative. Say what the code does and what a reader could not work out
from reading it — a constraint, a failure mode, a reason a plausible alternative is
wrong.

Not a changelog: "now", "no longer", "used to", "moved here" say nothing to someone
seeing the file for the first time. That story goes in the commit message.

Not thinking-out-loud: no reasoning towards the decision, no defending it to a reviewer,
no restating what the line below already says.

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

## Before committing

`cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`, `cargo test
--all-targets`. `pre-commit run --all-files` runs all three.

Also `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --document-private-items`, which
pre-commit does not run. It is where a doc link to a private item turns up, and clippy
does not see those.
