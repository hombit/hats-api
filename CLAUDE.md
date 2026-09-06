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

Before committing to a service, check it offers both of these; one that does not cannot
be served here at all.

- **`skip_signature`, or whatever the service calls it.** It is what makes an anonymous
  request anonymous. Without it OpenDAL walks its ambient chain and answers with the
  deployment's identity, and nothing on this side can prevent that.
- **A switch for every ambient credential source**, disabled per store rather than
  globally — `disable_config_load`, `disable_ec2_metadata`, `disable_vm_metadata`.

Then follow the shape the existing ones set:

- Addressing is per backend: path-style against a named S3-compatible endpoint,
  virtual-host against the provider itself.
- `allow_http` stays ours. OpenDAL follows the endpoint's own scheme without asking, so
  the cleartext decision has no backend half to defer to.
- `object_store` is trait-only here — the `ObjectStore` trait DataFusion consumes, plus
  `LocalFileSystem` for `file://`. Put a new backend on OpenDAL's side; never re-enable
  an `object_store` backend feature.

## The network

- A remote store is built through `storage::remote_store`, which is the one thing that
  turns a configured builder into something that can make a request — and the one place
  that puts the access policy's HTTP transport on it. A backend function returns its
  builder and never holds an `Operator`. `clippy.toml` disallows `Operator::new` and
  `reqwest::Client::new` outside their single permitted call sites, each of which carries
  an `#[expect]` saying so; a new one needs a reason written down next to it.
- The address check belongs in the resolver and nowhere else. Checking a host and then
  letting a client resolve it again is DNS rebinding: the answer that passed is not the
  answer that gets connected to.
- Do not follow redirects. A 3xx is the origin choosing the next destination, which
  would carry the caller's credentials to a host no endpoint rule named.
- A host named in an endpoint list is permission at both layers — the endpoint rules and
  the network rules. An operator should not have to say it twice.
- `access` decides which endpoint may be named; `network` decides which address may be
  reached. A new rule belongs in whichever of those it is actually about.

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
