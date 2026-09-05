# Conventions

## Dependencies

Look for a public crate before writing one. Especially for anything security- or
performance-critical: a widely used crate has had far more eyes on its edge cases than
anything written here in an afternoon. `cargo search`, then `cargo info <name>` for the
licence and features.

Say what was picked and what it costs — added crates, licence, what it does not cover —
rather than adding it silently. Not every crate is worth it: a one-liner with a heavy
dependency tree is not, and neither is one that solves a different problem than the one
at hand.

## Credentials

- A credential is never a `String`. Use `secrecy::SecretString`, and
  `storage::SourceUrl` for a caller-supplied url. Neither prints its value, so
  `#[derive(Debug)]` around them is safe. New backends' option structs follow the same
  rule.
- `url::Url`'s own `Debug` prints its `password` field, so a struct holding one needs a
  hand-written `Debug` rather than a derive.
- Nothing downstream of `storage::open` sees a url with options on it.
- A dependency that logs credentials goes in `logging::CREDENTIAL_UNSAFE_TARGETS`, with
  a reason. `tests/credential_logging.rs` is what catches the next one.

## Comments

Focused and informative. Say what the code does and what a reader could not work out
from reading it — a constraint, a failure mode, a reason a plausible alternative is
wrong.

Not a changelog: "now", "no longer", "used to", "moved here" say nothing to someone
seeing the file for the first time. That story goes in the commit message.

Not thinking-out-loud: no reasoning towards the decision, no defending it to a reviewer,
no restating what the line below already says.

Never refer to `DEVELOPMENT_PLAN.md` — no section numbers, no filename, not in code,
tests, config or workflows. It gets deleted when the work in it is done.

`DEVELOPMENT_PLAN.md` is a plan, not a log: update it only when what remains to be done
changes.

## Tests

`cargo test` must pass with no network, no Docker and no credentials. Anything needing a
real server is a separate test binary that skips when its env vars are absent.

Do not run MinIO or any other container locally; the MinIO tests are CI's.

## Before committing

`cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`, `cargo test
--all-targets`. `pre-commit run --all-files` runs all three.

Also `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --document-private-items`, which
pre-commit does not run. It is where a doc link to a private item turns up, and clippy
does not see those.
