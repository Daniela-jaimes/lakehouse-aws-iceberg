"""Generación de datos sintéticos de clientes."""

from datetime import date, timedelta

import numpy as np
from faker import Faker


DEPARTAMENTOS = {
    "Bogotá D.C.": (0.20, ["Bogotá"]),
    "Antioquia": (0.15, ["Medellín", "Envigado", "Bello"]),
    "Valle del Cauca": (0.10, ["Cali", "Palmira"]),
    "Atlántico": (0.07, ["Barranquilla", "Soledad"]),
    "Santander": (0.08, ["Bucaramanga", "Floridablanca", "Piedecuesta"]),
    "Cundinamarca": (0.06, ["Soacha", "Chía"]),
    "Bolívar": (0.05, ["Cartagena"]),
    "Norte de Santander": (0.04, ["Cúcuta"]),
    "Risaralda": (0.04, ["Pereira"]),
    "Caldas": (0.03, ["Manizales"]),
    "Tolima": (0.03, ["Ibagué"]),
    "Nariño": (0.03, ["Pasto"]),
    "Boyacá": (0.03, ["Tunja"]),
    "Meta": (0.03, ["Villavicencio"]),
    "Huila": (0.03, ["Neiva"]),
    "Córdoba": (0.03, ["Montería"]),
}


def generar_customers(cantidad: int, rng=None, fake=None):
    """Genera clientes con suciedad intencional para Silver."""

    if cantidad <= 0:
        raise ValueError("La cantidad de clientes debe ser mayor que cero.")

    if rng is None:
        rng = np.random.default_rng(42)

    if fake is None:
        Faker.seed(42)
        fake = Faker("es_CO")

    departamentos = list(DEPARTAMENTOS.keys())
    pesos = [DEPARTAMENTOS[d][0] for d in departamentos]

    # Seleccionar aleatoriamente el 1 % de clientes con correos sucios.
    k = max(2, cantidad // 100)
    indices_sucios = rng.choice(
        np.arange(1, cantidad + 1),
        size=min(k, cantidad),
        replace=False,
    )

    mitad = k // 2
    correos_invalidos = set(indices_sucios[:mitad])
    correos_mayusculas = set(indices_sucios[mitad:])

    # Seleccionar aleatoriamente las fechas de nacimiento futuras.
    cantidad_fechas_futuras = max(1, cantidad // 1000)
    indices_futuros = set(
        rng.choice(
            np.arange(1, cantidad + 1),
            size=min(cantidad_fechas_futuras, cantidad),
            replace=False,
        )
    )

    for i in range(1, cantidad + 1):

        departamento = rng.choice(
            departamentos,
            p=pesos,
        )

        ciudades = DEPARTAMENTOS[departamento][1]
        ciudad = rng.choice(ciudades)

        nombre_completo = (
            f"{fake.first_name()} {fake.last_name()}"
        )

        if rng.random() < 0.02:
            nombre_completo = f" {nombre_completo.lower()} "

        email = f"user{i}@example.com"

        if i in correos_invalidos:
            email = "no-es-un-correo"
        elif i in correos_mayusculas:
            email = email.upper()

        dias_nacimiento = int(rng.integers(0, 57 * 365))

        nacimiento = date(1950, 1, 1) + timedelta(
            days=dias_nacimiento
        )

        if i in indices_futuros:
            nacimiento = date(2031, 1, 1)

        fecha_creacion = date(2023, 1, 1) + timedelta(
            days=int(rng.integers(0, 1000))
        )

        yield {
            "customer_id": i,
            "full_name": nombre_completo,
            "email": email,
            "gender": rng.choice(
                ["F", "M", "O"],
                p=[0.48, 0.48, 0.04],
            ),
            "birth_date": nacimiento.isoformat(),
            "city": ciudad,
            "department": departamento,
            "created_at": fecha_creacion.isoformat(),
        }