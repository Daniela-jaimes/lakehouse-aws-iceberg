
"""Coordinador principal para generar los datos del Lakehouse."""

import argparse
from datetime import datetime
from pathlib import Path

import numpy as np
from faker import Faker

from config.settings import (
    PROFILES,
    RANDOM_SEED,
    FAKER_LOCALE,
)

from generators.customers import generar_customers
from generators.products import generar_products
from generators.stores import generar_stores
from generators.orders import generar_orders
from generators.order_items import generar_order_items
from generators.web_events import generar_web_events

from utils.file_writer import (
    escribir_csv_gz,
    escribir_json_gz,
)


COLUMNAS = {
    "customers": [
        "customer_id",
        "full_name",
        "email",
        "gender",
        "birth_date",
        "city",
        "department",
        "created_at",
    ],
    "products": [
        "product_id",
        "product_name",
        "category",
        "brand",
        "list_price",
    ],
    "stores": [
        "store_id",
        "store_name",
        "city",
        "department",
    ],
    "orders": [
        "order_id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "payment_method",
        "updated_at",
    ],
    "order_items": [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "discount_pct",
        "unit_price",
    ],
}


def validar_fecha(valor: str) -> str:
    """Valida que la fecha tenga formato YYYY-MM-DD."""

    try:
        datetime.strptime(valor, "%Y-%m-%d")
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "La fecha debe tener formato YYYY-MM-DD."
        ) from error

    return valor


def configurar_generadores():
    """Prepara generadores reproducibles para cada ejecución."""

    Faker.seed(RANDOM_SEED)

    fake = Faker(FAKER_LOCALE)

    rng_master = np.random.default_rng(RANDOM_SEED)

    return fake, rng_master


def generar_catalogos(
    profile: str,
    ingest_date: str,
    output_root: Path,
    fake: Faker,
    rng_master,
):
    """Genera los catálogos maestros en cada lote."""

    cantidades = PROFILES[profile]

    print("\n--- GENERANDO CATÁLOGOS ---")

    escribir_csv_gz(
        table="customers",
        ingest_date=ingest_date,
        columns=COLUMNAS["customers"],
        rows=generar_customers(
            cantidades["customers"],
            rng=rng_master,
            fake=fake,
        ),
        output_root=output_root,
    )

    # Los productos se conservan en memoria porque sus precios
    # se necesitan para generar las líneas de pedido.
    productos = list(
        generar_products(
            cantidades["products"],
            rng=rng_master,
        )
    )

    escribir_csv_gz(
        table="products",
        ingest_date=ingest_date,
        columns=COLUMNAS["products"],
        rows=productos,
        output_root=output_root,
    )

    escribir_csv_gz(
        table="stores",
        ingest_date=ingest_date,
        columns=COLUMNAS["stores"],
        rows=generar_stores(
            cantidades["stores"],
            rng=rng_master,
        ),
        output_root=output_root,
    )

    precios_productos = [
        producto["list_price"]
        for producto in productos
    ]

    return precios_productos


def generar_pedidos(
    profile: str,
    batch: int,
    ingest_date: str,
    batch1_date: str | None,
    output_root: Path,
    product_prices: list[int],
):
    """Genera pedidos y sus respectivas líneas."""

    cantidades = PROFILES[profile]

    # Se utiliza el mismo generador aleatorio para pedidos
    # y detalles, conservando la secuencia de generación.
    rng_transaccional = np.random.default_rng(1000 + batch)

    print("\n--- GENERANDO PEDIDOS ---")

    escribir_csv_gz(
        table="orders",
        ingest_date=ingest_date,
        columns=COLUMNAS["orders"],
        rows=generar_orders(
            profile=profile,
            batch=batch,
            customer_count=cantidades["customers"],
            store_count=cantidades["stores"],
            ingest_date=ingest_date,
            rng=rng_transaccional,
            batch1_date=batch1_date,
            output_root=output_root,
        ),
        output_root=output_root,
    )

    print("\n--- GENERANDO LÍNEAS DE PEDIDO ---")

    escribir_csv_gz(
        table="order_items",
        ingest_date=ingest_date,
        columns=COLUMNAS["order_items"],
        rows=generar_order_items(
            profile=profile,
            batch=batch,
            product_prices=product_prices,
            rng=rng_transaccional,
        ),
        output_root=output_root,
    )


def generar_eventos_web(
    profile: str,
    batch: int,
    ingest_date: str,
    output_root: Path,
):
    """Genera eventos web en formato JSON Lines."""

    cantidades = PROFILES[profile]

    print("\n--- GENERANDO EVENTOS WEB ---")

    escribir_json_gz(
        table="web_events",
        ingest_date=ingest_date,
        rows=generar_web_events(
            profile=profile,
            batch=batch,
            customer_count=cantidades["customers"],
            product_count=cantidades["products"],
            ingest_date=ingest_date,
            rng=np.random.default_rng(2000 + batch),
        ),
        output_root=output_root,
    )


def ejecutar_generador(
    profile: str,
    batch: int,
    ingest_date: str,
    batch1_date: str | None = None,
    output_root: Path | None = None,
):
    """Coordina la generación completa de un lote."""

    if output_root is None:
        output_root = Path("out")

    output_root = Path(output_root)

    fake, rng_master = configurar_generadores()

    print("=" * 55)
    print("GENERADOR DE DATOS - LAKEHOUSE AWS")
    print("=" * 55)
    print(f"Perfil: {profile}")
    print(f"Lote: {batch}")
    print(f"Fecha de ingesta: {ingest_date}")
    print(f"Directorio de salida: {output_root}")

    precios_productos = generar_catalogos(
        profile=profile,
        ingest_date=ingest_date,
        output_root=output_root,
        fake=fake,
        rng_master=rng_master,
    )

    generar_pedidos(
        profile=profile,
        batch=batch,
        ingest_date=ingest_date,
        batch1_date=batch1_date,
        output_root=output_root,
        product_prices=precios_productos,
    )

    generar_eventos_web(
        profile=profile,
        batch=batch,
        ingest_date=ingest_date,
        output_root=output_root,
    )

    print("\n" + "=" * 55)
    print("GENERACIÓN FINALIZADA")
    print("=" * 55)


def main():
    """Configura y ejecuta los argumentos de consola."""

    parser = argparse.ArgumentParser(
        description="Generador de datos sintéticos para el Lakehouse."
    )

    parser.add_argument(
        "--profile",
        required=True,
        choices=PROFILES.keys(),
        help="Perfil de generación: mini o full.",
    )

    parser.add_argument(
        "--batch",
        type=int,
        required=True,
        choices=[1, 2],
        help="Número del lote que se desea generar.",
    )

    parser.add_argument(
        "--ingest-date",
        required=True,
        type=validar_fecha,
        help="Fecha de ingesta con formato YYYY-MM-DD.",
    )

    parser.add_argument(
        "--batch1-date",
        type=validar_fecha,
        help="Fecha de ingesta del lote 1, necesaria para el lote 2.",
    )

    parser.add_argument(
        "--out",
        type=Path,
        default=Path("out"),
        help="Directorio raíz de salida. Por defecto: out.",
    )

    args = parser.parse_args()

    if args.batch == 2 and not args.batch1_date:
        parser.error(
            "--batch1-date es obligatorio cuando batch es 2."
        )

    if args.batch == 1 and args.batch1_date:
        parser.error(
            "--batch1-date solo se utiliza cuando batch es 2."
        )

    ejecutar_generador(
        profile=args.profile,
        batch=args.batch,
        ingest_date=args.ingest_date,
        batch1_date=args.batch1_date,
        output_root=args.out,
    )


if __name__ == "__main__":
    main()
