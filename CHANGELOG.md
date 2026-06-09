# Changelog

All notable changes to Hawk are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
follows [Semantic Versioning](https://semver.org). Hawk is **pre-1.0**: minor
versions may include breaking changes (see
[docs/compatibility.md](docs/compatibility.md)).

Storage format compatibility is called out explicitly. The on-disk format
version is independent of the package version; the current format version is
**3**.

## [Unreleased]

### Added
- Product positioning guide (`docs/positioning.md`) with when-to-use guidance and
  a tool comparison.
- Python developer guide (`docs/python.md`) and MCP developer guide
  (`docs/mcp.md`).
- Release process (`docs/release-process.md`), compatibility policy
  (`docs/compatibility.md`), and this changelog.

### Changed
- README: added when-to-use, project-maturity, Python, and MCP sections.

_Storage format: unchanged (v3)._

## [0.1.3]

### Added
- Practical runnable examples (`drift_analysis`, `privacy_safe_sharing`,
  `association_discovery`) and reproducible benchmark harness.
- Maintainer/contributor docs and CI quality gates (fmt, clippy, tests, audit).
- HTTP ingest hardening: body/batch size limits, optional auth token, localhost
  default bind.
- Schema validation and percent-encoded dimension keys.
- Raw-log and file-format hardening (segment rotation, total-size cap).

### Fixed
- Categorical accounting invariants.

_Storage format: v3._

## [0.1.2]

### Added
- Python bindings (`hawk_engine` via PyO3/Maturin) and MCP server (`hawk-mcp`).

_Storage format: v3._

## [0.1.1]

### Changed
- Workspace and release-workflow refinements.

_Storage format: v3._

## [0.1.0]

### Added
- Initial release: distribution-native storage, the SQL-like DSL (COMPARE,
  EXPLAIN, TRACK, MI, CORRELATIONS, and more), information-theoretic metrics,
  CSV/JSON/Parquet ingestion, and the Axum web UI.

_Storage format: v3._

[Unreleased]: https://github.com/repfly/hawk/compare/v0.1.3...HEAD
[0.1.3]: https://github.com/repfly/hawk/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/repfly/hawk/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/repfly/hawk/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/repfly/hawk/releases/tag/v0.1.0
