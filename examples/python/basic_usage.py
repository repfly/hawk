"""Basic Hawk Python usage.

Install locally first:

    maturin develop -m crates/hawk-python/Cargo.toml

Then run:

    python examples/python/basic_usage.py
"""

from pathlib import Path
import shutil
import tempfile

import hawk_engine


def main():
    root = Path(tempfile.gettempdir()) / "hawk-python-basic"
    if root.exists():
        shutil.rmtree(root)

    csv_path = root.with_suffix(".csv")
    csv_path.write_text(
        "\n".join(
            [
                "score,category,time",
                "0.1,billing,2024",
                "0.2,billing,2024",
                "-0.4,login,2025",
                "-0.5,login,2025",
                "0.3,search,2025",
            ]
        )
        + "\n"
    )

    db = hawk_engine.HawkDB.create(str(root))
    report = db.ingest(str(csv_path))
    print(report)
    print(db.query("COMPARE category BETWEEN time:2024 AND time:2025"))
    print(db.query("TRACK category FROM time:2024"))
    db.close()

    shutil.rmtree(root, ignore_errors=True)
    csv_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
