
"""Valida los archivos GZIP generados por el Lakehouse."""

import argparse
import csv
import gzip
import json

from pathlib import Path

from config.settings import (
    PROFILES,
    BATCH2_NEW_ORDERS,
    BATCH2_UPDATED_ORDERS,
)


COLUMNAS = {
    "customers": [
        "customer_id", "full_name", "email", "gender",
        "birth_date", "city", "department", "created_at",
    ],
    "products": [
        "product_id", "product_name", "category",
        "brand", "list_price",
    ],
    "stores": [
        "store_id", "store_name", "city", "department",
    ],
    "orders": [
        "order_id", "customer_id", "store_id", "order_date",
        "status", "payment_method", "updated_at",
    ],
    "order_items": [
        "order_item_id", "order_id", "product_id",
        "quantity", "discount_pct", "unit_price",
    ],
}


def validar_fecha(valor: str) -> str:
    """Valida que la fecha tenga formato YYYY-MM-DD."""

    from datetime import datetime

    try:
        datetime.strptime(valor, "%Y-%m-%d")
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "La fecha debe tener formato YYYY-MM-DD."
        ) from error

    return valor


def contar_csv(ruta: Path, columnas_esperadas: list[str]) -> int:
    """Valida encabezados y cuenta registros CSV."""

    with gzip.open(
        ruta, "rt", encoding="utf-8", newline=""
    ) as archivo:
        lector = csv.DictReader(archivo)

        if lector.fieldnames != columnas_esperadas:
            raise ValueError(
                f"Columnas incorrectas: {lector.fieldnames}"
            )

        return sum(1 for _ in lector)


def contar_json(ruta: Path) -> int:
    """Valida registros JSON Lines y su estructura."""

    total = 0

    eventos_permitidos = {
        "page_view",
        "product_view",
        "add_to_cart",
        "purchase",
        "search",
        "checkout",
    }

    sistemas_operativos = {
        "Windows",
        "macOS",
        "Linux",
        "Android",
        "iOS",
    }

    navegadores = {
        "Chrome",
        "Firefox",
        "Safari",
        "Edge",
    }

    with gzip.open(ruta, "rt", encoding="utf-8") as archivo:
        for numero, linea in enumerate(archivo, start=1):

            try:
                registro = json.loads(linea)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"JSON inválido en línea {numero}: {error}"
                ) from error

            if not isinstance(registro, dict):
                raise ValueError(
                    f"La línea {numero} no contiene un objeto JSON."
                )

            campos = {
                "event_id",
                "ts",
                "customer_id",
                "event_type",
                "product_id",
                "device",
            }

            if not campos.issubset(registro):
                raise ValueError(
                    f"Faltan campos en el evento de la línea {numero}."
                )

            if not isinstance(registro["event_id"], str):
                raise ValueError(
                    f"event_id inválido en línea {numero}."
                )

            if not isinstance(registro["ts"], str):
                raise ValueError(
                    f"ts inválido en línea {numero}."
                )

            if type(registro["customer_id"]) is not int:
                raise ValueError(
                    f"customer_id inválido en línea {numero}."
                )

            if type(registro["product_id"]) is not int:
                raise ValueError(
                    f"product_id inválido en línea {numero}."
                )

            if registro["event_type"] not in eventos_permitidos:
                raise ValueError(
                    f"event_type inválido en línea {numero}."
                )

            device = registro["device"]

            if not isinstance(device, dict):
                raise ValueError(
                    f"device no es un objeto en línea {numero}."
                )

            if device.get("os") not in sistemas_operativos:
                raise ValueError(
                    f"Sistema operativo inválido en línea {numero}."
                )

            if device.get("browser") not in navegadores:
                raise ValueError(
                    f"Navegador inválido en línea {numero}."
                )

            total += 1

    return total


def validar_archivos(
    tabla: str,
    directorio: Path,
    extension: str,
) -> int:
    """Valida todos los archivos de una tabla."""

    archivos = sorted(directorio.glob(extension))

    if not archivos:
        raise FileNotFoundError(
            f"No se encontraron archivos en {directorio}"
        )

    total = 0

    for ruta in archivos:
        if tabla == "web_events":
            total += contar_json(ruta)
        else:
            total += contar_csv(ruta, COLUMNAS[tabla])

    return total


def main():
    parser = argparse.ArgumentParser(
        description="Validador de archivos del Lakehouse."
    )

    parser.add_argument(
        "--profile",
        required=True,
        choices=PROFILES.keys(),
    )

    parser.add_argument(
        "--batch",
        required=True,
        type=int,
        choices=[1, 2],
    )

    parser.add_argument(
        "--ingest-date",
        required=True,
        type=validar_fecha,
        help="Fecha de ingesta con formato YYYY-MM-DD.",
    )

    parser.add_argument(
        "--out",
        type=Path,
        default=Path("out"),
    )

    args = parser.parse_args()

    cantidades = PROFILES[args.profile]
    raiz = args.out / "landing"

    print("=" * 60)
    print("VALIDACIÓN DE ARCHIVOS DEL LAKEHOUSE")
    print("=" * 60)
    print(f"Perfil: {args.profile}")
    print(f"Lote: {args.batch}")
    print(f"Fecha: {args.ingest_date}")
    print(f"Directorio: {raiz}")

    if args.batch == 1:
        pedidos_esperados = (
            cantidades["orders"]
            + round(cantidades["orders"] * 0.01)
        )
        items_minimos = cantidades["orders"]
        items_maximos = cantidades["orders"] * 5

    else:
        pedidos_esperados = (
            BATCH2_NEW_ORDERS + BATCH2_UPDATED_ORDERS
        )
        items_minimos = BATCH2_NEW_ORDERS
        items_maximos = BATCH2_NEW_ORDERS * 5

    esperados = {
        "customers": cantidades["customers"],
        "products": cantidades["products"],
        "stores": cantidades["stores"],
        "orders": pedidos_esperados,
        "web_events": cantidades["web_events"],
    }

    directorios = {
        tabla: raiz / "oltp" / tabla
        / f"ingest_date={args.ingest_date}"
        for tabla in COLUMNAS
    }

    directorios["web_events"] = (
        raiz / "logs" / "web_events"
        / f"ingest_date={args.ingest_date}"
    )

    errores = []

    for tabla, esperado in esperados.items():
        try:
            directorio = directorios[tabla]

            extension = (
                "events-*.json.gz"
                if tabla == "web_events"
                else f"{tabla}-*.csv.gz"
            )

            total = validar_archivos(
                tabla,
                directorio,
                extension,
            )

            correcto = total == esperado

            print(
                f"[{'OK' if correcto else 'ERROR'}] "
                f"{tabla}: {total:,} registros "
                f"(esperados: {esperado:,})"
            )

            if not correcto:
                errores.append(
                    f"{tabla}: se esperaban {esperado:,}, "
                    f"se encontraron {total:,}."
                )

        except (OSError, EOFError, ValueError) as error:
            errores.append(f"{tabla}: {error}")
            print(f"[ERROR] {tabla}: {error}")

    try:
        total_items = validar_archivos(
            "order_items",
            directorios["order_items"],
            "order_items-*.csv.gz",
        )

        correcto = items_minimos <= total_items <= items_maximos

        print(
            f"[{'OK' if correcto else 'ERROR'}] "
            f"order_items: {total_items:,} registros "
            f"(rango: {items_minimos:,}-{items_maximos:,})"
        )

        if not correcto:
            errores.append(
                f"order_items: {total_items:,} registros "
                f"fuera del rango permitido."
            )

    except (OSError, EOFError, ValueError) as error:
        errores.append(f"order_items: {error}")
        print(f"[ERROR] order_items: {error}")

    print("\n" + "=" * 60)

    if errores:
        print(f"VALIDACIÓN FALLIDA: {len(errores)} problema(s).")
        for error in errores:
            print(f"- {error}")
    else:
        print("VALIDACIÓN COMPLETADA: todas las tablas son correctas.")

    print("=" * 60)

    if errores:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
