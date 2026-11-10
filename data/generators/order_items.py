"""Generación de detalles de pedidos."""

import numpy as np

from config.settings import (
    PROFILES,
    BATCH2_NEW_ORDERS,
)


def generar_order_items(
    profile: str,
    batch: int,
    product_prices: list[int],
    rng=None,
):
    """
    Genera líneas de pedido con precios del catálogo.

    En el lote 1 introduce cantidades negativas intencionalmente.
    En el lote 2 genera detalles únicamente para pedidos nuevos.
    """

    if profile not in PROFILES:
        raise ValueError(f"Perfil no válido: {profile}")

    if batch not in (1, 2):
        raise ValueError("El lote debe ser 1 o 2.")

    if not product_prices:
        raise ValueError("El catálogo de precios está vacío.")

    if rng is None:
        rng = np.random.default_rng(1000 + batch)

    total_orders = PROFILES[profile]["orders"]

    # Determinar los IDs de los pedidos que tendrán detalles.
    if batch == 1:
        order_ids = np.arange(1, total_orders + 1)

    else:
        order_ids = np.arange(
            total_orders + 1,
            total_orders + BATCH2_NEW_ORDERS + 1,
        )

    # Cantidad de líneas por pedido según la guía.
    cantidades_lineas = rng.choice(
        [1, 2, 3, 4, 5],
        size=len(order_ids),
        p=[0.25, 0.30, 0.25, 0.12, 0.08],
    )

    total_items = int(cantidades_lineas.sum())

    # Cantidad de líneas con valores negativos en el lote 1.
    indices_invalidos = set()

    if batch == 1:
        cantidad_invalidos = max(2, total_items // 300)

        indices_invalidos = set(
            rng.choice(
                total_items,
                size=cantidad_invalidos,
                replace=False,
            ).tolist()
        )

    # Generar las líneas.
    indice_linea_global = 0

    for order_id, cantidad_lineas in zip(
        order_ids,
        cantidades_lineas,
    ):

        for posicion in range(1, int(cantidad_lineas) + 1):

            product_id = int(
                rng.integers(1, len(product_prices) + 1)
            )

            quantity = int(rng.integers(1, 5))

            # Introducir cantidades negativas intencionales.
            if indice_linea_global in indices_invalidos:
                quantity = -1

            discount_pct = float(
                rng.choice(
                    [0, 0, 0, 0.05, 0.10, 0.20]
                )
            )

            yield {
                "order_item_id": int(order_id * 10 + posicion),
                "order_id": int(order_id),
                "product_id": product_id,
                "quantity": quantity,
                "discount_pct": discount_pct,
                "unit_price": int(product_prices[product_id - 1]),
            }

            indice_linea_global += 1