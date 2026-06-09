# Compatibility policy

Hawk is **pre-1.0**. Until 1.0, **minor** version bumps (0.x → 0.y) may contain
breaking changes; **patch** bumps (0.x.a → 0.x.b) are intended to be
non-breaking. Every breaking change is recorded in the
[CHANGELOG](../CHANGELOG.md).

There are several independently versioned surfaces. This document states the
guarantee for each.

## Storage format

The on-disk format has its own version number, independent of the package
version. The current format version is **3** (`HAWK` magic + zstd-compressed
bincode; see [file-format.md](file-format.md)).

- **Backward read:** a newer Hawk build reads databases written at the same or
  an **older** format version.
- **Forward read:** an older build **refuses** to open a database written at a
  **newer** format version, with a clear error naming the version it supports.
- **Breaking format changes** bump the format version (`FORMAT_VERSION` in
  `crates/hawk-engine/src/storage/file_format.rs`) and are noted in the
  CHANGELOG under the release that introduces them.
- Prefer **additive, serde-compatible** changes that do not require a version
  bump. Any format change must add a reopen test and document migration or
  rebuild behavior in the PR.

If a format change is not migratable in place, the upgrade path is **re-ingest
the source data** into a fresh database. There is no row store to migrate from,
so keep source data if you may need to rebuild.

## Query language (the DSL)

- New statements and clauses may be **added** in any release.
- Existing statement **semantics** are kept stable within a `0.x` line where
  practical; any change to the meaning or output shape of an existing statement
  is called out in the CHANGELOG.
- Output **column order and labels** may change between minor versions; do not
  parse the pretty-printed table. For stable machine parsing use
  `EXPORT ... AS JSON`/`AS CSV`, the Python `QueryResult.to_json()`, or the MCP
  `query` tool — these share one structured representation.

## Rust API (`hawk-engine`)

- The published crate follows SemVer **for the items it documents as public**.
- Pre-1.0, minor bumps may break the API; breaking changes are listed in the
  CHANGELOG.
- Internal modules and types not part of the documented surface may change at
  any time.

## Python API (`hawk_engine`)

- The classes and methods documented in [python.md](python.md) —
  `HawkDB`, `QueryResult`, `DatabaseStats`, `IngestReport`, `HawkError` — form
  the supported surface.
- The Python package version tracks the workspace version, so a given
  `hawk_engine` wheel and the matching `hawk-engine` crate behave identically.

## MCP tools (`hawk-mcp`)

- The tool **names** (`query`, `schema`, `stats`, `list_dimensions`,
  `open_database`, `create_database`, `ingest_file`, `help`) and their JSON
  argument shapes are the supported surface.
- Tool **descriptions** (the text shown to models) may be tuned at any time
  without a compatibility note.

## Version alignment

All crates share one workspace version (`Cargo.toml` `[workspace.package]`), and
the Python package derives its version from the same field. A release tag `vX.Y.Z`
publishes the crate and the wheels at that version together. See
[release-process.md](release-process.md).
