# File Format

Hawk stores databases as directories of compressed binary files. The current format uses a short header followed by zstd-compressed `bincode` payloads.

```text
[4 bytes] "HAWK" magic
[4 bytes] format version, little-endian u32
[rest]    zstd-compressed bincode payload
```

## Files

| File | Payload type | Purpose |
|---|---|---|
| `meta.edb` | `MetaFile` | Schema, counters, raw-log configuration |
| `distributions.edb` | `DistributionFile` | Marginal distributions and joint distributions |
| `dist_index.edb` | `DistIndex` | Persistent lookup index |
| `snapshots.edb` | `SnapshotStore` | Historical distribution snapshots |

## Compatibility Rules

- Prefer additive serde-compatible changes.
- Keep in-memory indexes out of persisted files unless persistence is required.
- Bump the format version for incompatible payload changes.
- Add reopen tests for any file-format change.
- Document migration or rebuild behavior in the PR.

## Dimension Key Encoding

Dimension keys are ordered maps from dimension name to dimension value. The canonical string form joins encoded pairs with `/` and joins each name/value with `:`.

Reserved bytes in names and values are percent-encoded:

| Byte | Encoded |
|---|---|
| `%` | `%25` |
| `:` | `%3A` |
| `/` | `%2F` |

Keys without reserved bytes keep the historical representation, for example `time:2024/topic:news`. This keeps ordinary existing indexes stable while making separator-containing names and values unambiguous.

## Raw Logs

Raw logs live under `raw/` when enabled. They may contain original records and should be treated as sensitive data.

Current defaults:

| Setting | Default |
|---|---:|
| Segment name | `log_000001.raw`, `log_000002.raw`, ... |
| Max segment size | 16 MiB |
| Max total raw-log size | 128 MiB |

When appending a record would exceed the active segment limit, Hawk creates a new segment. After appending, Hawk deletes oldest segments until the total raw-log footprint is under the configured limit, while keeping at least one segment.

Raw-log retention is not privacy-preserving. Use it only when original-row retention is intended, and avoid enabling it for sensitive data unless storage access, retention, and deletion policies are explicit.
