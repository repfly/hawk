#!/usr/bin/env python3
"""Convert Criterion estimate JSON files into a compact Markdown table."""

import argparse
import json
from pathlib import Path


def fmt_ns(ns):
    if ns >= 1_000_000:
        return f"{ns / 1_000_000:.3f} ms"
    if ns >= 1_000:
        return f"{ns / 1_000:.3f} us"
    return f"{ns:.3f} ns"


def load_estimate(path):
    with path.open() as f:
        data = json.load(f)
    mean = data["mean"]["point_estimate"]
    lower = data["mean"]["confidence_interval"]["lower_bound"]
    upper = data["mean"]["confidence_interval"]["upper_bound"]
    return mean, lower, upper


def main():
    parser = argparse.ArgumentParser(
        description="Render target/criterion estimate JSON files as Markdown."
    )
    parser.add_argument(
        "criterion_dir",
        nargs="?",
        default="target/criterion",
        help="Criterion output directory. Default: target/criterion",
    )
    args = parser.parse_args()

    root = Path(args.criterion_dir)
    estimate_files = sorted(root.glob("*/new/estimates.json"))
    if not estimate_files:
        raise SystemExit(f"no Criterion estimates found under {root}")

    print("| Benchmark | Mean | 95% CI |")
    print("|---|---:|---:|")
    for path in estimate_files:
        name = path.parent.parent.name
        mean, lower, upper = load_estimate(path)
        print(f"| `{name}` | {fmt_ns(mean)} | {fmt_ns(lower)} - {fmt_ns(upper)} |")


if __name__ == "__main__":
    main()
