"""
Generate a realistic synthetic Community Notes–style dataset for Hawk testing.

Columns:
  - sentiment_score  (float, -1.0 to 1.0)   → continuous variable
  - political_leaning (str)                   → categorical variable
  - topic_label      (str)                    → dimension
  - created_at       (date)                   → time dimension

Run:
  python tests/generate_test_data.py
  # produces tests/fixtures/community_notes_10k.csv
"""

import csv
import random
from datetime import date, timedelta

random.seed(42)

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
NUM_ROWS = 10_000

def random_date():
    delta = (END_DATE - START_DATE).days
    return START_DATE + timedelta(days=random.randint(0, delta))

def clamp(val, lo, hi):
    return max(lo, min(hi, val))

rows = []
for _ in range(NUM_ROWS):
    topic = random.choice(list(TOPICS.keys()))
    cfg = TOPICS[topic]
    sentiment = clamp(random.gauss(cfg["sentiment_mean"], cfg["sentiment_std"]), -1.0, 1.0)
    leaning = random.choices(LEANINGS, weights=cfg["leaning_weights"], k=1)[0]
    created = random_date()
    rows.append({
        "sentiment_score": round(sentiment, 4),
        "political_leaning": leaning,
        "topic_label": topic,
        "created_at": created.isoformat(),
    })

out_path = "tests/fixtures/community_notes_10k.csv"
with open(out_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["sentiment_score", "political_leaning", "topic_label", "created_at"])
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote {len(rows)} rows to {out_path}")
print(f"Topics: {list(TOPICS.keys())}")
print(f"Date range: {START_DATE} — {END_DATE}")
