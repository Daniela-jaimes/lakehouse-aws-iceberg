
"""Configuración general del generador de datos."""

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "out"

PROFILES = {
    "mini": {
        "customers": 5_000,
        "products": 500,
        "stores": 30,
        "orders": 50_000,
        "web_events": 20_000,
    },
    "full": {
        "customers": 300_000,
        "products": 5_000,
        "stores": 150,
        "orders": 4_300_000,
        "order_items": 10_600_000,
        "web_events": 1_000_000,
    },
}

BATCH2_NEW_ORDERS = 4_000
BATCH2_UPDATED_ORDERS = 1_000

CHUNK_SIZE = 250_000
RANDOM_SEED = 42
FAKER_LOCALE = "es_CO"
