"""Funciones auxiliares compartidas por los generadores."""

import random

from datetime import date, timedelta
from pathlib import Path


def crear_directorio(ruta: Path) -> None:
    """Crea un directorio y sus carpetas superiores."""
    ruta.mkdir(parents=True, exist_ok=True)


def generar_id(prefijo: str, numero: int) -> str:
    """Genera un identificador estable y legible."""
    return f"{prefijo}-{numero:08d}"


def generar_fecha_aleatoria(
    fecha_inicio: date,
    fecha_fin: date,
) -> date:
    """Genera una fecha aleatoria dentro de un intervalo."""

    dias = (fecha_fin - fecha_inicio).days

    return fecha_inicio + timedelta(
        days=random.randint(0, dias)
    )


def formatear_fecha_sucia(fecha: date) -> str:
    """Genera fechas en tres formatos diferentes."""

    formatos = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%b-%Y",
    ]

    return fecha.strftime(random.choice(formatos))