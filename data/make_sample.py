"""
Copies a small sample of every landing table into data/sample/ so the dataset can be
seen in Git without uploading hundreds of MB.

Each sample has the first 20 rows plus up to 3 intentionally dirty rows, marked in the
last column of the console output (not in the file).

Usage (from the data/ folder):
  python make_sample.py --ingest-date 2026-11-07
"""
import argparse
import csv
import gzip
import re
from datetime import date
from pathlib import Path

FIRST_ROWS = 20
MAX_DIRTY = 3
EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# table -> function that returns True when the row is dirty
DIRTY_RULES = {
    "customers": lambda r: not EMAIL_REGEX.match(r["email"].strip().lower()),
    "orders": lambda r: date.fromisoformat(r["order_date"]) > date.today(),
    "order_items": lambda r: int(r["quantity"]) <= 0,
}
TABLES = ["customers", "products", "stores", "orders", "order_items"]


def read_csv_rows(folder):
    for path in sorted(folder.glob("*.csv.gz")):
        with gzip.open(path, "rt", encoding="utf-8", newline="") as file:
            yield from csv.DictReader(file)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-date", required=True)
    parser.add_argument("--out", default="out")
    args = parser.parse_args()

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    for table in TABLES:
        folder = Path(args.out) / "landing" / "oltp" / table / f"ingest_date={args.ingest_date}"
        rows, dirty = [], []
        for row in read_csv_rows(folder):
            if len(rows) < FIRST_ROWS:
                rows.append(row)
            elif table in DIRTY_RULES and len(dirty) < MAX_DIRTY and DIRTY_RULES[table](row):
                dirty.append(row)
            if len(rows) >= FIRST_ROWS and (table not in DIRTY_RULES or len(dirty) >= MAX_DIRTY):
                break
        with open(sample_dir / f"{table}.csv", "w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows + dirty)
        print(f"{table:12s} sample: {len(rows)} rows + {len(dirty)} dirty rows")

    events = Path(args.out) / "landing" / "logs" / "web_events" / f"ingest_date={args.ingest_date}"
    with gzip.open(next(events.glob("*.json.gz")), "rt", encoding="utf-8") as source, \
            open(sample_dir / "web_events.jsonl", "w", encoding="utf-8") as target:
        for _ in range(FIRST_ROWS):
            target.write(source.readline())
    print("web_events   sample: 20 lines")


if __name__ == "__main__":
    main()
