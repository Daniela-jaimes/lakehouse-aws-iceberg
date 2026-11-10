"""Generación de datos sintéticos de pedidos."""

from copy import deepcopy
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from config.settings import (
    PROFILES,
    BATCH2_NEW_ORDERS,
    BATCH2_UPDATED_ORDERS,
    OUTPUT_DIR,
)


ESTADOS = [
    "completed",
    "shipped",
    "pending",
    "cancelled",
    "returned",
]

PROBABILIDADES_ESTADOS = [
    0.80,
    0.08,
    0.05,
    0.05,
    0.02,
]

METODOS_PAGO = [
    "card",
    "cash",
    "pse",
    "nequi",
]


def _crear_pedido(
    order_id: int,
    customer_count: int,
    store_count: int,
    fecha_inicio: date,
    dias_disponibles: int,
    rng,
):
    """Construye un pedido individual."""

    desplazamiento = int(rng.integers(0, dias_disponibles))
    fecha_pedido = fecha_inicio + timedelta(days=desplazamiento)

    return {
        "order_id": order_id,
        "customer_id": int(
            rng.integers(1, customer_count + 1)
        ),
        "store_id": int(
            rng.integers(1, store_count + 1)
        ),
        "order_date": fecha_pedido.isoformat(),
        "status": rng.choice(
            ESTADOS,
            p=PROBABILIDADES_ESTADOS,
        ),
        "payment_method": rng.choice(METODOS_PAGO),
        "updated_at": (
            datetime.combine(
                fecha_pedido,
                datetime.min.time(),
            ) + timedelta(hours=12)
        ).strftime("%Y-%m-%d %H:%M:%S"),
    }


def _obtener_actualizaciones(
    profile: str,
    batch1_date: str,
    output_root: Path,
):
    """Selecciona pedidos existentes para actualizar en el lote 2."""

    ruta_pedidos = (
        output_root
        / "landing"
        / "oltp"
        / "orders"
        / f"ingest_date={batch1_date}"
    )

    archivos = sorted(ruta_pedidos.glob("orders-*.csv.gz"))

    if not archivos:
        raise FileNotFoundError(
            f"No se encontraron pedidos del lote 1 en: {ruta_pedidos}"
        )

    pedidos = pd.concat(
        [
            pd.read_csv(archivo, compression="gzip")
            for archivo in archivos
        ],
        ignore_index=True,
    )

    candidatos = (
        pedidos[
            pedidos["status"].isin(["completed", "shipped"])
        ]
        .drop_duplicates(subset=["order_id"])
    )

    if len(candidatos) < BATCH2_UPDATED_ORDERS:
        raise ValueError(
            "No hay suficientes pedidos completados o enviados "
            "para realizar las actualizaciones."
        )

    actualizaciones = candidatos.sample(
        n=BATCH2_UPDATED_ORDERS,
        random_state=7,
    ).copy()

    actualizaciones["status"] = "returned"

    return actualizaciones


def generar_orders(
    profile: str,
    batch: int,
    customer_count: int,
    store_count: int,
    ingest_date: str,
    rng=None,
    batch1_date: str | None = None,
    output_root: Path | None = None,
):
    """
    Genera pedidos para los lotes 1 y 2.

    Lote 1:
        Genera pedidos originales y copias duplicadas.

    Lote 2:
        Genera pedidos nuevos y actualiza pedidos existentes.
    """

    if profile not in PROFILES:
        raise ValueError(f"Perfil no válido: {profile}")

    if batch not in (1, 2):
        raise ValueError("El lote debe ser 1 o 2.")

    if rng is None:
        rng = np.random.default_rng(1000 + batch)

    if output_root is None:
        output_root = OUTPUT_DIR

    output_root = Path(output_root)

    total_orders = PROFILES[profile]["orders"]
    fecha_ingesta = date.fromisoformat(ingest_date)

    if batch == 1:

        fecha_inicio = date(2025, 1, 1)

        dias_disponibles = (
            fecha_ingesta - fecha_inicio
        ).days - 1

        if dias_disponibles <= 0:
            raise ValueError(
                "La fecha de ingesta debe permitir generar "
                "pedidos desde 2025-01-01."
            )

        # Seleccionar el 1 % de los pedidos que se duplicarán.
        indices_duplicados = set(
            pd.Series(range(total_orders)).sample(
                frac=0.01,
                random_state=1,
            ).tolist()
        )

        # Guardar el estado inicial para reproducir los pedidos.
        estado_rng = deepcopy(rng.bit_generator.state)

        # Primera pasada: generar todos los pedidos originales.
        for indice in range(total_orders):
            yield _crear_pedido(
                order_id=indice + 1,
                customer_count=customer_count,
                store_count=store_count,
                fecha_inicio=fecha_inicio,
                dias_disponibles=dias_disponibles,
                rng=rng,
            )

        # Segunda pasada: reconstruir solamente los duplicados.
        rng_duplicados = np.random.default_rng()
        rng_duplicados.bit_generator.state = estado_rng

        for indice in range(total_orders):

            pedido = _crear_pedido(
                order_id=indice + 1,
                customer_count=customer_count,
                store_count=store_count,
                fecha_inicio=fecha_inicio,
                dias_disponibles=dias_disponibles,
                rng=rng_duplicados,
            )

            if indice in indices_duplicados:
                yield pedido

    else:

        if not batch1_date:
            raise ValueError(
                "El lote 2 requiere indicar --batch1-date."
            )

        fecha_lote_1 = date.fromisoformat(batch1_date)

        if fecha_lote_1 >= fecha_ingesta:
            raise ValueError(
                "La fecha del lote 1 debe ser anterior "
                "a la fecha del lote 2."
            )

        # Comprobar y seleccionar las actualizaciones antes
        # de empezar a escribir los pedidos nuevos.
        actualizaciones = _obtener_actualizaciones(
            profile=profile,
            batch1_date=batch1_date,
            output_root=output_root,
        )

        # Generar los 4.000 pedidos nuevos.
        fecha_pedidos_nuevos = fecha_ingesta - timedelta(days=1)

        for order_id in range(
            total_orders + 1,
            total_orders + BATCH2_NEW_ORDERS + 1,
        ):

            yield _crear_pedido(
                order_id=order_id,
                customer_count=customer_count,
                store_count=store_count,
                fecha_inicio=fecha_pedidos_nuevos,
                dias_disponibles=1,
                rng=rng,
            )

        # Las actualizaciones conservan los demás campos originales.
        for registro in actualizaciones.to_dict(orient="records"):
            registro["updated_at"] = (
                f"{ingest_date} 09:00:00"
            )
            yield registro