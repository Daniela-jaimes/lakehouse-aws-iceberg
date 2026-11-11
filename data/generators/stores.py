"""Generación de datos sintéticos de tiendas."""

import numpy as np

from generators.customers import DEPARTAMENTOS


def generar_stores(cantidad: int, rng=None):
    """Genera tiendas con ubicaciones geográficas coherentes."""

    if cantidad <= 0:
        raise ValueError("La cantidad de tiendas debe ser mayor que cero.")

    if rng is None:
        rng = np.random.default_rng(42)

    departamentos = list(DEPARTAMENTOS.keys())

    pesos = [
        DEPARTAMENTOS[departamento][0]
        for departamento in departamentos
    ]

    for i in range(1, cantidad + 1):

        departamento = rng.choice(
            departamentos,
            p=pesos,
        )

        ciudades = DEPARTAMENTOS[departamento][1]
        ciudad = rng.choice(ciudades)

        yield {
            "store_id": i,
            "store_name": f"Tienda {i}",
            "city": ciudad,
            "department": departamento,
        }