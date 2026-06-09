# Contributing

Thanks for helping improve Hawk. This guide covers local setup, expected checks, and pull request hygiene.

## Requirements

- Rust stable, with `rustfmt` and `clippy`
- Python 3.9+ for Python binding work
- Maturin for local Python extension builds

```bash
rustup toolchain install stable
rustup component add rustfmt clippy
python -m pip install --upgrade pip maturin
```

## Common Commands

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test --workspace --all-features
cargo bench -p hawk-engine --bench bench_jsd -- --test
cargo bench -p hawk-engine --bench bench_end_to_end -- --test
```

Python binding smoke check:

```bash
maturin build -m crates/hawk-python/Cargo.toml --out dist
python -m pip install dist/*.whl
python -c "import hawk_engine; print('ok')"
```

## Testing Expectations

- Add focused tests for correctness changes.
- Add integration tests when behavior crosses storage, ingest, query, or SQL boundaries.
- Add benchmark smoke coverage for performance-sensitive paths.
- Run formatting, clippy, and workspace tests before opening a PR.

## Pull Requests

Keep PRs scoped to one behavior or documentation phase. Include what changed, why it changed, how it was verified, and any compatibility or file-format impact.

Recommended commit style:

```text
type(scope): short imperative summary
```

Examples:

```text
fix(engine): reject invalid categorical indexes
perf(storage): cache dimension value catalogs
docs: add reproducible benchmark methodology
```
