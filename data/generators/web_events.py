"""Generación de eventos web en formato JSON."""

import numpy as np

from config.settings import PROFILES


EVENT_TYPES = [
    "page_view",
    "product_view",
    "add_to_cart",
    "purchase",
    "search",
    "checkout",
]

OPERATING_SYSTEMS = [
    "Windows",
    "macOS",
    "Linux",
    "Android",
    "iOS",
]

BROWSERS = [
    "Chrome",
    "Firefox",
    "Safari",
    "Edge",
]


def generar_web_events(
    profile: str,
    batch: int,
    customer_count: int,
    product_count: int,
    ingest_date: str,
    rng=None,
):
    """
    Genera eventos web en formato JSON anidado.

    Los eventos se generan en ambos lotes y conservan
    identificadores numéricos para clientes y productos.
    """

    if profile not in PROFILES:
        raise ValueError(f"Perfil no válido: {profile}")

    if batch not in (1, 2):
        raise ValueError("El lote debe ser 1 o 2.")

    if customer_count <= 0 or product_count <= 0:
        raise ValueError(
            "Las cantidades de clientes y productos "
            "deben ser mayores que cero."
        )

    if rng is None:
        rng = np.random.default_rng(1000 + batch)

    total_events = PROFILES[profile]["web_events"]

    for i in range(1, total_events + 1):

        yield {
            "event_id": f"{ingest_date}-{i}",
            "ts": (
                f"{ingest_date}T"
                f"{i % 24:02d}:"
                f"{i % 60:02d}:00Z"
            ),
            "customer_id": int(
                rng.integers(1, customer_count + 1)
            ),
            "event_type": rng.choice(EVENT_TYPES),
            "product_id": int(
                rng.integers(1, product_count + 1)
            ),
            "device": {
                "os": rng.choice(OPERATING_SYSTEMS),
                "browser": rng.choice(BROWSERS),
            },
        }