"""
Synthetic data generator for the lakehouse project.

Writes the landing zone files:
  <out>/landing/oltp/<table>/ingest_date=YYYY-MM-DD/part-0001.csv.gz
  <out>/landing/logs/web_events/ingest_date=YYYY-MM-DD/part-0001.json.gz

The data is deterministic: the same order id always produces the same order,
so batch 2 can update orders created in batch 1 without reading any file.
It also adds intentional dirty data (duplicates, bad emails, negative quantities,
future dates, messy text) so that the silver layer has something to clean.
"""
import argparse
import csv
import gzip
import json
import random
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path

PROFILES = {
    "mini": {"customers": 5_000, "products": 500, "stores": 30, "orders": 50_000, "events": 20_000},
    "full": {"customers": 300_000, "products": 5_000, "stores": 150, "orders": 4_300_000,
             "events": 1_000_000},
}
ROWS_PER_FILE = 250_000            # one .gz cannot be split, so we write several files
FIRST_ORDER_DATE = date(2025, 1, 1)
NEW_ORDERS_RATE = 0.08             # batch 2: new orders (8 % of batch 1)
UPDATED_ORDERS_RATE = 0.02         # batch 2: batch 1 orders changed to 'returned' (2 %)

FIRST_NAMES = ["juan", "maria", "carlos", "ana", "luis", "laura", "pedro", "sofia", "diego", "camila"]
LAST_NAMES = ["perez", "gomez", "rodriguez", "martinez", "lopez", "garcia", "torres", "ramirez"]
CATEGORIES = ["electronics", "home", "clothing", "sports", "toys", "beauty", "books", "groceries"]
LOCATIONS = [("Santander", "Bucaramanga"), ("Santander", "Floridablanca"), ("Antioquia", "Medellin"),
             ("Cundinamarca", "Bogota"), ("Valle del Cauca", "Cali"), ("Atlantico", "Barranquilla"),
             ("Bolivar", "Cartagena"), ("Norte de Santander", "Cucuta")]
STATUSES = ["completed", "shipped", "pending", "cancelled", "returned"]
STATUS_WEIGHTS = [70, 10, 8, 6, 6]
PAYMENTS = ["card", "cash", "transfer", "wallet"]
EVENT_TYPES = ["view", "search", "add_to_cart", "checkout"]


def rng_for(kind, number):
    return random.Random(f"{kind}-{number}")


# ---------- one function per entity: always the same output for the same id ----------

def make_customer(customer_id, ingest_date):
    r = rng_for("customer", customer_id)
    first, last = r.choice(FIRST_NAMES), r.choice(LAST_NAMES)
    birth_date = date(1945, 1, 1) + timedelta(days=r.randint(0, (date(2008, 12, 31) - date(1945, 1, 1)).days))
    email = f"{first}.{last}{customer_id}@example.com"
    if customer_id % 97 == 0:          # dirty: invalid email (~1 %)
        email = f"{first}.{last}{customer_id}-at-example"
    if customer_id % 13 == 0:          # dirty: messy text (silver must trim and fix case)
        first, last, email = f"  {first.upper()} ", last.upper(), email.upper()
    return [customer_id, first, last, email, birth_date.isoformat(), f"{ingest_date} 00:00:00"]


def product_price(product_id):
    return float(5_000 + (product_id * 7919 % 4_950) * 100)


def make_product(product_id, ingest_date):
    category = CATEGORIES[product_id % len(CATEGORIES)]
    return [product_id, f"{category} product {product_id}", category, f"{ingest_date} 00:00:00"]


def make_store(store_id, ingest_date):
    department, city = LOCATIONS[store_id % len(LOCATIONS)]
    return [store_id, f"Store {store_id}", city, department, f"{ingest_date} 00:00:00"]


def make_order(order_id, sizes, first_day, last_day):
    r = rng_for("order", order_id)
    order_date = first_day + timedelta(days=r.randint(0, (last_day - first_day).days))
    return {
        "order_id": order_id,
        "customer_id": r.randint(1, sizes["customers"]),
        "store_id": r.randint(1, sizes["stores"]),
        "order_date": order_date,
        "status": r.choices(STATUSES, STATUS_WEIGHTS)[0],
        "payment_method": r.choice(PAYMENTS),
    }


def order_row(order, updated_at):
    return [order["order_id"], order["customer_id"], order["store_id"], order["order_date"].isoformat(),
            order["status"], order["payment_method"], updated_at]


def make_items(order_id, sizes):
    r = rng_for("items", order_id)
    count = r.choices([1, 2, 3, 4, 5], [28, 28, 22, 13, 9])[0]      # mean = 2.47 items per order
    for k in range(1, count + 1):
        product_id = r.randint(1, sizes["products"])
        item_id = order_id * 10 + k
        quantity = r.randint(1, 5)
        if item_id % 211 == 0:         # dirty: negative quantity (~0.5 %)
            quantity = -quantity
        yield [item_id, order_id, product_id, quantity, product_price(product_id),
               r.choice([0, 0, 0, 5, 10, 15, 20])]


# ---------- writing files ----------

class TableWriter:
    """Writes rows into part-0001.csv.gz, part-0002.csv.gz... (or .json.gz)."""

    def __init__(self, folder, header=None):
        shutil.rmtree(folder, ignore_errors=True)      # re-running a batch replaces it
        folder.mkdir(parents=True)
        self.folder, self.header = folder, header
        self.extension = "csv.gz" if header else "json.gz"
        self.part, self.rows, self.total, self.file, self.csv = 0, 0, 0, None, None
        self.duplicates = random.Random(f"dup-{folder}")   # decides which rows are written twice

    def _open_next(self):
        self.close()
        self.part += 1
        self.rows = 0
        self.file = gzip.open(self.folder / f"part-{self.part:04d}.{self.extension}", "wt",
                              encoding="utf-8", newline="")
        if self.header:
            self.csv = csv.writer(self.file)
            self.csv.writerow(self.header)

    def write(self, row, duplicate_rate=0.0):
        times = 2 if self.duplicates.random() < duplicate_rate else 1     # dirty: duplicates
        for _ in range(times):
            if self.file is None or self.rows >= ROWS_PER_FILE:
                self._open_next()
            if self.header:
                self.csv.writerow(row)
            else:
                self.file.write(json.dumps(row) + "\n")
            self.rows += 1
            self.total += 1

    def close(self):
        if self.file:
            self.file.close()
            self.file = None


def open_writers(out, ingest_date):
    landing = Path(out) / "landing"
    headers = {
        "customers": ["customer_id", "first_name", "last_name", "email", "birth_date", "updated_at"],
        "products": ["product_id", "product_name", "category", "updated_at"],
        "stores": ["store_id", "store_name", "city", "department", "updated_at"],
        "orders": ["order_id", "customer_id", "store_id", "order_date", "status", "payment_method",
                   "updated_at"],
        "order_items": ["order_item_id", "order_id", "product_id", "quantity", "unit_price",
                        "discount_pct"],
    }
    writers = {name: TableWriter(landing / "oltp" / name / f"ingest_date={ingest_date}", header)
               for name, header in headers.items()}
    writers["web_events"] = TableWriter(landing / "logs" / "web_events" / f"ingest_date={ingest_date}")
    return writers


def write_dimensions(writers, sizes, ingest_date):
    """Customers, products and stores are sent in full in every batch (daily snapshot)."""
    for i in range(1, sizes["customers"] + 1):
        writers["customers"].write(make_customer(i, ingest_date), duplicate_rate=0.01)
    for i in range(1, sizes["products"] + 1):
        writers["products"].write(make_product(i, ingest_date))
    for i in range(1, sizes["stores"] + 1):
        writers["stores"].write(make_store(i, ingest_date))


def write_events(writers, sizes, ingest_date, count):
    r = random.Random(f"events-{ingest_date}")
    for event_id in range(1, count + 1):
        writers["web_events"].write({
            "event_id": f"{ingest_date}-{event_id}",
            "customer_id": r.randint(1, sizes["customers"]),
            "event_type": r.choice(EVENT_TYPES),
            "page": f"/product/{r.randint(1, sizes['products'])}",
            "event_ts": f"{ingest_date} {r.randint(0, 23):02d}:{r.randint(0, 59):02d}:{r.randint(0, 59):02d}",
        })


def write_orders_and_items(writers, sizes, order_ids, first_day, last_day, dirty_dates=False,
                           allow_returned=True):
    for order_id in order_ids:
        order = make_order(order_id, sizes, first_day, last_day)
        if not allow_returned and order["status"] == "returned":
            order["status"] = "completed"       # so batch 2 adds exactly the updated returns
        if dirty_dates and order_id % 333 == 0:       # dirty: future date (~0.3 %)
            order["order_date"] = last_day + timedelta(days=30)
        writers["orders"].write(order_row(order, f"{order['order_date']} 12:00:00"), duplicate_rate=0.01)
        for item in make_items(order_id, sizes):
            writers["order_items"].write(item, duplicate_rate=0.01)


# ---------- batches ----------

def run_batch_1(writers, sizes, ingest_date):
    last_day = ingest_date - timedelta(days=1)
    write_dimensions(writers, sizes, ingest_date)
    write_orders_and_items(writers, sizes, range(1, sizes["orders"] + 1),
                           FIRST_ORDER_DATE, last_day, dirty_dates=True)
    write_events(writers, sizes, ingest_date, sizes["events"])


def run_batch_2(writers, sizes, ingest_date, batch1_date):
    new_count = int(sizes["orders"] * NEW_ORDERS_RATE)
    updated_count = int(sizes["orders"] * UPDATED_ORDERS_RATE)
    write_dimensions(writers, sizes, ingest_date)

    # new orders, dated in the two days before this batch
    new_ids = range(sizes["orders"] + 1, sizes["orders"] + new_count + 1)
    write_orders_and_items(writers, sizes, new_ids,
                           ingest_date - timedelta(days=2), ingest_date - timedelta(days=1),
                           allow_returned=False)

    # existing orders of batch 1 that change to 'returned' (same data, new status and updated_at)
    first_day, batch1_last_day = FIRST_ORDER_DATE, batch1_date - timedelta(days=1)
    r, seen, updated = random.Random("updated-orders"), set(), 0
    while updated < updated_count:
        order_id = r.randint(1, sizes["orders"])
        if order_id in seen or order_id % 333 == 0:      # skip repeated and future-dated orders
            continue
        seen.add(order_id)
        order = make_order(order_id, sizes, first_day, batch1_last_day)
        if order["status"] in ("returned", "cancelled"):
            continue
        order["status"] = "returned"
        writers["orders"].write(order_row(order, f"{ingest_date} 09:00:00"))
        updated += 1

    write_events(writers, sizes, ingest_date, sizes["events"] // 10)
    return new_count, updated_count


def main():
    parser = argparse.ArgumentParser(description="Generate landing files for the lakehouse project")
    parser.add_argument("--profile", choices=PROFILES, required=True)
    parser.add_argument("--batch", type=int, choices=[1, 2], required=True)
    parser.add_argument("--ingest-date", required=True, help="YYYY-MM-DD of this batch")
    parser.add_argument("--batch1-date", help="YYYY-MM-DD of batch 1 (required for batch 2)")
    parser.add_argument("--out", default="out")
    args = parser.parse_args()

    sizes = PROFILES[args.profile]
    ingest_date = date.fromisoformat(args.ingest_date)
    writers = open_writers(args.out, args.ingest_date)

    if args.batch == 1:
        run_batch_1(writers, sizes, ingest_date)
    else:
        if not args.batch1_date:
            parser.error("--batch1-date is required for batch 2")
        new_count, updated_count = run_batch_2(writers, sizes, ingest_date,
                                               date.fromisoformat(args.batch1_date))
        print(f"batch 2: {new_count} new orders, {updated_count} orders updated to 'returned'")

    for name, writer in writers.items():
        writer.close()
        print(f"{name:12s} rows written: {writer.total:>10,}  files: {writer.part}")


if __name__ == "__main__":
    main()
