
"""Auditoría de calidad de los datos del Lakehouse."""

import argparse
import csv
import gzip
import re

from datetime import date, datetime
from pathlib import Path


EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def leer_csv(tabla: str, ingest_date: str, output_root: Path):
    """Lee todos los archivos CSV GZIP de una tabla."""

    ruta = (
        output_root
        / "landing"
        / "oltp"
        / tabla
        / f"ingest_date={ingest_date}"
    )

    archivos = sorted(ruta.glob(f"{tabla}-*.csv.gz"))

    if not archivos:
        raise FileNotFoundError(
            f"No se encontraron archivos para {tabla}: {ruta}"
        )

    registros = []

    for archivo in archivos:
        with gzip.open(
            archivo, "rt", encoding="utf-8", newline=""
        ) as contenido:
            registros.extend(csv.DictReader(contenido))

    return registros


def contar_duplicados(registros, campo):
    """Cuenta registros duplicados según un campo."""

    valores = [registro[campo] for registro in registros]
    return len(valores) - len(set(valores))


def es_fecha_futura(valor: str) -> bool:
    """Comprueba si una fecha ISO es posterior al día actual."""

    try:
        return datetime.strptime(
            valor, "%Y-%m-%d"
        ).date() > date.today()
    except ValueError:
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Auditoría de calidad del Lakehouse."
    )

    parser.add_argument(
        "--ingest-date",
        required=True,
        help="Fecha de ingesta con formato YYYY-MM-DD.",
    )

    parser.add_argument(
        "--out",
        type=Path,
        default=Path("out"),
        help="Directorio raíz de salida.",
    )

    args = parser.parse_args()

    customers = leer_csv(
        "customers", args.ingest_date, args.out
    )

    products = leer_csv(
        "products", args.ingest_date, args.out
    )

    orders = leer_csv(
        "orders", args.ingest_date, args.out
    )

    items = leer_csv(
        "order_items", args.ingest_date, args.out
    )

    correos_nulos = sum(
        not registro["email"]
        for registro in customers
    )

    correos_invalidos = sum(
        bool(registro["email"])
        and not EMAIL_PATTERN.match(registro["email"])
        for registro in customers
    )

    fechas_nacimiento_futuras = sum(
        es_fecha_futura(registro["birth_date"])
        for registro in customers
    )

    cantidades_invalidas = sum(
        int(registro["quantity"]) <= 0
        for registro in items
    )

    precios_nulos = sum(
        not registro["list_price"]
        for registro in products
    )

    nombres_con_espacios = sum(
        registro["full_name"] != registro["full_name"].strip()
        for registro in customers
    )

    print("=" * 60)
    print("AUDITORÍA DE CALIDAD - LAKEHOUSE")
    print("=" * 60)

    print(f"Clientes analizados: {len(customers):,}")
    print(f"Productos analizados: {len(products):,}")
    print(f"Pedidos analizados: {len(orders):,}")
    print(f"Líneas analizadas: {len(items):,}")

    print("\n--- CALIDAD DE CLIENTES ---")
    print(f"Correos nulos: {correos_nulos:,}")
    print(f"Correos inválidos: {correos_invalidos:,}")
    print(f"Fechas de nacimiento futuras: {fechas_nacimiento_futuras:,}")
    print(f"Nombres con espacios externos: {nombres_con_espacios:,}")

    print("\n--- CALIDAD DE PRODUCTOS ---")
    print(f"Precios nulos: {precios_nulos:,}")

    print("\n--- CALIDAD DE PEDIDOS ---")
    print(f"Fechas de pedidos futuras: {sum(es_fecha_futura(r['order_date']) for r in orders):,}")

    print("\n--- CALIDAD DE DETALLES ---")
    print(f"Cantidades cero o negativas: {cantidades_invalidas:,}")

    print("\n--- DUPLICADOS ---")
    print(
        "Clientes duplicados:",
        f"{contar_duplicados(customers, 'customer_id'):,}"
    )

    print(
        "Pedidos duplicados:",
        f"{contar_duplicados(orders, 'order_id'):,}"
    )

    print(
        "Líneas de pedido duplicadas:",
        f"{contar_duplicados(items, 'order_item_id'):,}"
    )

    print("=" * 60)


if __name__ == "__main__":
    main()
