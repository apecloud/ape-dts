# Agent Guide for ApeDTS

This file provides repository-wide guidance for coding agents. Read any closer
`AGENTS.md` before changing a subtree. Use existing code and the linked documents
for implementation details; keep this guide focused on stable boundaries.

## Project Overview

ApeDTS is a Rust data migration and CDC engine supporting snapshot migration,
continuous synchronization, data validation/correction, and in-flight
transformation. Correctness, recoverability, and throughput matter together.

Start with [README.md](README.md) or [README_ZH.md](README_ZH.md). Configuration
reference: [docs/en/config.md](docs/en/config.md). Contribution workflow:
[CONTRIBUTING.md](CONTRIBUTING.md).

## Project Structure

The repository is a Cargo workspace. Keep changes in the crate that owns the
behavior, and follow the nearest existing implementation before adding helpers.

| Path | Responsibility |
| --- | --- |
| `dt-main/` | Engine binary entry point and startup |
| `dt-cli/` | Local CLI for generating configs and managing tasks |
| `dt-precheck/` | Preflight configuration and database prerequisite checks |
| `dt-connector/` | Source extractors, destination sinkers, and data checkers |
| `dt-pipeline/` | Data processing and coordination between extraction and sinking |
| `dt-parallelizer/` | Partitioning, scheduling, and parallel execution strategies |
| `dt-task/` | Configuration-driven component assembly and task lifecycle |
| `dt-common/` | Shared data models, configuration, metadata, errors, and utilities |
| `dt-tests/` | Integration test framework, cases, and service setup |
| `docs/` | User guides, configuration, task templates, and benchmarks |

The core event types are `DtItem` and `DtData` in
`dt-common/src/meta/dt_data.rs`. Trace their ownership through the affected
extractor, parallelizer, pipeline, and sinker when changing data flow.

## Core Data Path and Performance

- Prefer moving owned data between stages and borrowing for inspection. Avoid
  deep clones of `DtItem`, `DtData`, `RowData`, buffers, or batches unless separate
  ownership is necessary; copy only what is needed. Reconsider ownership before
  cloning to satisfy the borrow checker. Cheap handle clones such as `Arc::clone`
  are different from payload copies; do not add shared ownership just to avoid
  fixing unnecessary payload copies.
- Keep per-row and per-event processing lightweight. Avoid unnecessary JSON
  serialization/deserialization, typed-data-to-JSON round trips, repeated SQL or
  regex parsing, and repeated metadata computation. Reuse parsed or prepared
  state at the appropriate task/table/batch scope where semantics permit.
- Serialize at actual format or protocol boundaries. Inspect `Display`/`Debug`
  costs before logging payloads: formatting `DtData` or `RowData` can serialize
  the entire value. Prefer small identifying fields and existing metrics.
- Preserve bounded buffering and backpressure. Do not introduce unbounded queues,
  unbounded task spawning, or whole-stream materialization to simplify processing.
- Preserve transaction boundaries, required event ordering, routing/filtering
  semantics, and checkpoint recovery. Do not advance durable progress beyond
  successfully applied data. Performance changes must preserve these contracts.

## Rust and Async Practices

- Propagate recoverable failures with `Result` and `?`, using the existing
  `dt-common` error types/context helpers and surrounding error conventions.
  Keep useful operation and object context without exposing credentials.
- Avoid `unwrap`, `expect`, `panic!`, and `assert!`/`assert_eq!` for production
  error handling. Database responses, configuration, SQL, and event payloads are
  fallible input. Return an error rather than terminating a worker or process.
  Reserve panics/assertions for proven internal invariants when necessary and
  document why they hold; test assertions and fixture `unwrap`s are fine.
- Do not turn failures into success by silently skipping events, discarding a
  `Result`, or substituting empty/default data. Follow the task's existing error,
  retry, and cancellation policy.
- In async functions, scrutinize synchronous filesystem/network I/O, blocking
  database clients, `std::thread::sleep`, and contended `std::sync` locks. Prefer
  the runtime's async I/O, timers, and synchronization when waiting is involved.
  Reading `std::time::Instant` or using a short uncontended synchronous lock is
  not inherently wrong; judge whether the operation can block an executor thread.
- Never hold a synchronous lock guard across `.await`. Keep all critical
  sections short, including those protected by async locks, and release guards
  before I/O where possible.
- Put unavoidable blocking work or substantial CPU work on an appropriate
  blocking worker/dedicated thread with bounded concurrency. Do not spawn a
  blocking task per row or use offloading to justify avoidable serialization.
- Reuse the existing runtime; do not create nested runtimes or call blocking
  runtime entry points from async code. Follow existing worker shutdown and
  cancellation patterns and propagate worker failures.
- Prefer existing dependencies and abstractions. Avoid speculative frameworks,
  broad refactors, new `unsafe`, and dependency/toolchain changes unless needed
  for the task. Respect `rust-toolchain.toml` and workspace dependency versions.

## Tests and Comments

- Keep tests proportional to behavioral risk. Complex logic such as SQL parsing,
  rewriting, type conversion, ordering, and checkpoint handling needs focused
  unit coverage, including relevant edge cases and failure paths. Use compact
  table-driven cases when they make coverage clearer.
- Simple forwarding, accessors, or straightforward glue do not require new unit
  tests solely for coverage. Before relying on existing integration coverage,
  identify the case covering the changed behavior; add a targeted regression case
  when coverage is missing.
- Full workflow testing belongs in `dt-tests/`. Follow
  [dt-tests/README.md](dt-tests/README.md) and the closest existing suite. Reuse
  its runner, helpers, fixture layout, SQL loading strategy, and isolation rules;
  do not build a parallel integration harness or ad hoc database setup.
- Keep comments concise and in English, matching nearby code. Prefer a short
  function-level comment for purpose, non-obvious contracts, or tradeoffs. Add
  inline comments only where the logic needs local explanation; do not narrate
  obvious statements or require boilerplate comments for every function.

## Build and Validation

The Rust toolchain is pinned in `rust-toolchain.toml`; formatting uses the
nightly rustfmt version configured in `Makefile`. Use the current Makefile and
[CI workflow](.github/workflows/ci.yml) as the command sources of truth.

Start with the affected Cargo package (its name in `Cargo.toml` may differ from
the directory name; `dt-cli/` is package `dts-cli`). Run from the repository root:

```bash
# Build/check the affected crate first
cargo check -p <package>
cargo test -p <package> <test_filter>

# Formatting (uses the repository's pinned nightly rustfmt)
make format-check

# Broader production-crate checks, matching CI
cargo clippy --workspace --exclude dt-tests
cargo check --workspace --exclude dt-tests --all-targets --all-features
cargo nextest run --workspace --exclude dt-tests --lib --bins --no-fail-fast
cargo test --workspace --doc --exclude dt-tests
```

For integration tests, choose the affected suite using
[dt-tests/README.md](dt-tests/README.md) and
[the existing runner](dt-tests/scripts/run-integration-tests.sh). Its default
cleanup removes Compose services and volumes; when reusing an existing test
environment, use `--test --keep-docker`. Do not substitute an unfiltered workspace
test run for selecting suites and preparing their dependencies.

Start with the narrowest relevant checks and broaden for changes across crates
or shared data semantics. Documentation-only changes need link/content review,
not a Rust rebuild. Report what ran, failures, and any integration or performance
validation that remains unverified; do not claim correctness or speedups from
compilation alone.

## Change Hygiene

- Inspect `git status` before editing and preserve unrelated local changes.
- Keep diffs scoped to the request. Avoid unrelated cleanup and formatting churn.
- Keep existing task configurations valid and preserve their behavior after
  upgrades, including defaults. If a configuration breaking change is necessary,
  explain the impact and obtain explicit confirmation from the developer before
  implementing it.
- Preserve checkpoint and serialized-format compatibility unless a change is
  intentional; document migration/recovery implications.
- For user-visible changes, update relevant docs/examples and the changelog
  according to `CONTRIBUTING.md`.

## Pull Request Titles and Writing

Use a Conventional Commits-style PR title: `<type>: <short, specific summary>`.
Choose the type by the resulting change, not the implementation technique:

| Type | Use for |
| --- | --- |
| `feat` | New functionality or a new supported capability |
| `fix` | Correcting incorrect behavior, including regressions |
| `chore` | Repository/tooling maintenance without changing engine or CLI behavior |
| `refactor` | Restructuring production code without changing behavior |
| `perf` | Improving performance while preserving functional behavior |
| `docs` | Documentation-only changes |
| `test` | Adding or updating tests without production behavior changes |
| `build` / `ci` | Build/dependency configuration or CI workflow changes |
| `style` | Formatting-only changes without semantic changes |

For example, use `fix: preserve quoted identifiers in SQL parsing`, not
`chore: update parser` for a parsing bug. Do not hide a fix or feature under
`chore`; the current integration workflow skips PRs whose titles start with
`chore:`. Mark confirmed breaking changes with `!` before the colon and explain
the compatibility impact. The title marker does not replace the required
developer confirmation for configuration breaking changes.

Keep PR descriptions and PR/review comments concise and technical. Use
[the PR template](.github/PULL_REQUEST_TEMPLATE.md) for the description structure;
choose validation commands as described above and in current CI. Explain:

1. **Problem:** What concrete behavior is wrong or missing, and who is affected?
2. **Cause:** Why did the previous logic allow the problem? Distinguish confirmed
   causes from hypotheses.
3. **Example:** Show a small input, configuration, SQL statement, or event sequence
   with the previous result and the expected/new result.
4. **Solution:** Explain the changed decision or data flow and why it resolves the
   problem; mention material limitations or compatibility effects.

Add a short validation note with the relevant checks and outcomes. Scale detail
to the change: maintenance/docs PRs need only the applicable points, and a focused
review comment need not repeat the full PR explanation. Avoid filler, generic
claims such as "improves robustness", file-by-file diff narration, repeated
summaries, and investigation history that does not explain the final solution.
