"""Generación de datos sintéticos de productos."""

import numpy as np


CATEGORIAS = {
    "Electrónica": (80_000, 2_500_000),
    "Hogar": (20_000, 900_000),
    "Moda": (30_000, 400_000),
    "Deportes": (25_000, 700_000),
    "Supermercado": (3_000, 150_000),
    "Juguetes": (15_000, 300_000),
}

MARCAS = [
    "Aurora",
    "Nimbus",
    "Kora",
    "Zeta",
    "Andes",
]


def generar_products(cantidad: int, rng=None):
    """Genera productos con categorías, marcas y precios."""

    if cantidad <= 0:
        raise ValueError("La cantidad de productos debe ser mayor que cero.")

    if rng is None:
        rng = np.random.default_rng(42)

    categorias = list(CATEGORIAS.keys())

    for i in range(1, cantidad + 1):

        categoria = rng.choice(categorias)

        precio_minimo, precio_maximo = CATEGORIAS[categoria]

        precio = (
            int(rng.integers(precio_minimo, precio_maximo + 1)) // 100
        ) * 100

        yield {
            "product_id": i,
            "product_name": f"{categoria} Producto {i}",
            "category": categoria,
            "brand": rng.choice(MARCAS),
            "list_price": precio,
        }