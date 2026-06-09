"""
Generate a realistic synthetic Community Notes–style dataset for Hawk testing.

Columns:
  - sentiment_score  (float, -1.0 to 1.0)   → continuous variable
  - political_leaning (str)                   → categorical variable
  - topic_label      (str)                    → dimension
  - created_at       (date)                   → time dimension

Run:
  python tests/generate_test_data.py --rows 10000 --seed 42 --output tests/fixtures/community_notes_10k.csv
"""

import argparse
import csv
from pathlib import Path
import random
from datetime import date, timedelta

TOPICS = {
    "russia-ukraine":  {"sentiment_mean": 0.15, "sentiment_std": 0.35, "leaning_weights": [0.25, 0.35, 0.40]},
    "climate-change":  {"sentiment_mean": -0.10, "sentiment_std": 0.40, "leaning_weights": [0.50, 0.30, 0.20]},
    "us-elections":    {"sentiment_mean": 0.05, "sentiment_std": 0.45, "leaning_weights": [0.35, 0.20, 0.45]},
    "ai-regulation":   {"sentiment_mean": 0.30, "sentiment_std": 0.30, "leaning_weights": [0.30, 0.45, 0.25]},
    "immigration":     {"sentiment_mean": -0.05, "sentiment_std": 0.50, "leaning_weights": [0.40, 0.20, 0.40]},
}

LEANINGS = ["left", "center", "right"]
START_DATE = date(2023, 1, 1)
END_DATE = date(2025, 6, 30)

def random_date(rng):
    delta = (END_DATE - START_DATE).days
    return START_DATE + timedelta(days=rng.randint(0, delta))

def clamp(val, lo, hi):
    return max(lo, min(hi, val))

def build_rows(num_rows, seed):
    rng = random.Random(seed)
    rows = []
    topics = list(TOPICS.keys())
    for _ in range(num_rows):
        topic = rng.choice(topics)
        cfg = TOPICS[topic]
        sentiment = clamp(rng.gauss(cfg["sentiment_mean"], cfg["sentiment_std"]), -1.0, 1.0)
        leaning = rng.choices(LEANINGS, weights=cfg["leaning_weights"], k=1)[0]
        created = random_date(rng)
        rows.append({
            "sentiment_score": round(sentiment, 4),
            "political_leaning": leaning,
            "topic_label": topic,
            "created_at": created.isoformat(),
        })
    return rows

def main():
    parser = argparse.ArgumentParser(description="Generate deterministic Hawk benchmark/test CSV data.")
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", default="tests/fixtures/community_notes_10k.csv")
    args = parser.parse_args()

    rows = build_rows(args.rows, args.seed)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["sentiment_score", "political_leaning", "topic_label", "created_at"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {out_path}")
    print(f"Seed: {args.seed}")
    print(f"Topics: {list(TOPICS.keys())}")
    print(f"Date range: {START_DATE} to {END_DATE}")

if __name__ == "__main__":
    main()
