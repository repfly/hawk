# Release process

Hawk releases from **annotated git tags** of the form `vX.Y.Z`. Tags drive both
the crates.io publish (`.github/workflows/publish.yml`) and the PyPI publish
(`.github/workflows/publish-python.yml`). Both workflows re-run the full
validation gate before publishing, so **a release cannot publish unless CI is
green**.

## What a tag triggers

Pushing a `v*` tag runs, in order:

1. **Validate** — `cargo fmt --check`, `cargo clippy -D warnings`,
   `cargo test --workspace --all-features`, and `cargo package -p hawk-engine`
   (plus `maturin sdist` for the Python pipeline).
2. **Build** — the Python pipeline builds wheels for Linux, macOS (arm64), and
   Windows, and a source distribution. The version is taken from the tag.
3. **Publish** — `hawk-engine` to crates.io, and the wheels + sdist to PyPI.

Both pipelines set the package version from the tag (`${GITHUB_REF#refs/tags/v}`),
so the published artifacts carry the tag's version regardless of the version
currently committed in `Cargo.toml`.

## Pre-release checklist

Run from a clean checkout of `main`:

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo test --workspace --all-features`
- [ ] `cargo test --workspace --doc --all-features`
- [ ] Storage-compatibility / reopen tests pass (part of the workspace tests).
- [ ] `cargo bench -p hawk-engine` if the release makes performance claims (see
      [benchmarks.md](benchmarks.md)).
- [ ] Python smoke test: `maturin develop -m crates/hawk-python/Cargo.toml` then
      `python -c "import hawk_engine"` and `python examples/python/basic_usage.py`.
- [ ] MCP smoke test: `cargo run -p hawk-mcp -- --db <db> --readonly` starts and
      responds to `help`/`schema`.
- [ ] **CHANGELOG updated:** move `Unreleased` entries under the new version,
      with Added/Changed/Fixed/Security/Breaking sections and a storage-format
      note. Update the compare links at the bottom.
- [ ] Decide whether the on-disk **format version** changed; if so, the
      CHANGELOG and [compatibility.md](compatibility.md) say so explicitly.

## Cutting the release

1. Update the version in `Cargo.toml` `[workspace.package]` (keeps the committed
   tree honest — the workflows also override it from the tag).
2. Update `CHANGELOG.md` as above and commit.
3. Tag and push:

   ```bash
   git tag -a v0.1.4 -m "v0.1.4"
   git push origin v0.1.4
   ```

4. Watch the `Publish to crates.io` and `Publish to PyPI` workflows. They will
   fail closed if `fmt`/`clippy`/tests fail.
5. Generate GitHub release notes from the CHANGELOG section for the tag, and
   link the benchmark snapshot if the release makes performance claims.

## Versioning

- All crates share one version via `[workspace.package]` `version`; the Python
  package derives from the same field. Keep them aligned — do not version crates
  independently.
- Pre-1.0: minor bumps may break APIs or the storage format; patch bumps should
  not. See [compatibility.md](compatibility.md).
- Runtime version is exposed by the server at `GET /version`; the CLI reports it
  via `--version`.

## Rollback

crates.io and PyPI releases are **immutable** — you cannot overwrite a published
version. To recover from a bad release:

1. **Yank** the bad version so new installs don't pick it up, without breaking
   existing pins:
   - crates.io: `cargo yank --version X.Y.Z hawk-engine` (un-yank with `--undo`).
   - PyPI: yank the release from the project page (or `pip`-side via the web UI).
2. Fix forward: land the fix on `main` and cut the next patch tag (`vX.Y.(Z+1)`).
3. Note the yanked version and the reason in the CHANGELOG.
