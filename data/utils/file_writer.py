
"""Funciones para escribir archivos CSV y JSON comprimidos en GZIP."""

import csv
import gzip
import json
import shutil

from pathlib import Path
from typing import Iterable

from config.settings import OUTPUT_DIR, CHUNK_SIZE


def obtener_ruta_salida(
    table: str,
    ingest_date: str,
    output_root: Path | None = None,
) -> Path:
    """Construye la ruta de salida según el tipo de tabla."""

    root = Path(output_root) if output_root is not None else OUTPUT_DIR

    if table == "web_events":
        return (
            root
            / "landing"
            / "logs"
            / "web_events"
            / f"ingest_date={ingest_date}"
        )

    return (
        root
        / "landing"
        / "oltp"
        / table
        / f"ingest_date={ingest_date}"
    )


def _preparar_directorio(output_path: Path) -> None:
    """Prepara el directorio y elimina archivos de ejecuciones anteriores."""

    if output_path.exists():
        shutil.rmtree(output_path)

    output_path.mkdir(parents=True, exist_ok=True)


def escribir_csv_gz(
    table: str,
    ingest_date: str,
    columns: list[str],
    rows: Iterable[dict],
    output_root: Path | None = None,
    chunk_size: int = CHUNK_SIZE,
) -> int:
    """Escribe registros CSV comprimidos en bloques GZIP."""

    if chunk_size <= 0:
        raise ValueError("chunk_size debe ser mayor que cero.")

    output_path = obtener_ruta_salida(
        table,
        ingest_date,
        output_root,
    )
    _preparar_directorio(output_path)

    total_rows = 0
    file_number = 1
    file = None

    try:
        for row in rows:

            if total_rows % chunk_size == 0:
                if file is not None:
                    file.close()

                file_path = (
                    output_path
                    / f"{table}-{file_number:04d}.csv.gz"
                )

                file = gzip.open(
                    file_path,
                    mode="wt",
                    encoding="utf-8",
                    newline="",
                )

                writer = csv.DictWriter(
                    file,
                    fieldnames=columns,
                    extrasaction="ignore",
                )
                writer.writeheader()

                file_number += 1

            writer.writerow(row)
            total_rows += 1

    finally:
        if file is not None:
            file.close()

    print(
        f"[OK] {table}: {total_rows:,} registros "
        f"escritos en {file_number - 1} archivo(s)."
    )

    return total_rows


def escribir_json_gz(
    table: str,
    ingest_date: str,
    rows: Iterable[dict],
    output_root: Path | None = None,
    chunk_size: int = CHUNK_SIZE,
) -> int:
    """Escribe registros JSON Lines comprimidos en bloques GZIP."""

    if chunk_size <= 0:
        raise ValueError("chunk_size debe ser mayor que cero.")

    output_path = obtener_ruta_salida(
        table,
        ingest_date,
        output_root,
    )
    _preparar_directorio(output_path)

    total_rows = 0
    file_number = 1
    file = None

    try:
        for row in rows:

            if total_rows % chunk_size == 0:
                if file is not None:
                    file.close()

                file_path = (
                    output_path
                    / f"events-{file_number:04d}.json.gz"
                )

                file = gzip.open(
                    file_path,
                    mode="wt",
                    encoding="utf-8",
                )

                file_number += 1

            file.write(
                json.dumps(
                    row,
                    ensure_ascii=False,
                    default=str,
                )
            )
            file.write("\n")

            total_rows += 1

    finally:
        if file is not None:
            file.close()

    print(
        f"[OK] {table}: {total_rows:,} registros "
        f"escritos en {file_number - 1} archivo(s)."
    )

    return total_rows
