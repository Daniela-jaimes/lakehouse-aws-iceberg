from pathlib import Path

from pyspark.sql import SparkSession, functions as F


BASE = Path(__file__).resolve().parents[1]

ARCHIVO = (
    BASE
    / "data"
    / "out"
    / "landing"
    / "oltp"
    / "customers"
    / "ingest_date=2026-11-07"
    / "customers-0001.csv.gz"
)

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("LakehouseCustomersLocal")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(str(ARCHIVO))
)

print("\n--- DATOS ORIGINALES ---")
print("Total de clientes:", df.count())

c = df.select(
    F.col("customer_id").cast("long"),
    F.initcap(F.trim("full_name")).alias("full_name"),
    F.when(
        F.lower(F.trim("email")).rlike(
            r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
        ),
        F.lower(F.trim("email"))
    ).alias("email"),
    F.col("gender"),
    F.to_date("birth_date").alias("birth_date"),
    F.trim("city").alias("city"),
    F.trim("department").alias("department"),
    F.to_date("created_at").alias("created_at")
)

c = c.withColumn(
    "birth_date",
    F.when(
        (F.col("birth_date") > F.current_date()) |
        (F.col("birth_date") < F.lit("1900-01-01").cast("date")),
        None
    ).otherwise(F.col("birth_date"))
)

print("\n--- RESULTADOS DE LA TRANSFORMACION ---")
print("Total transformado:", c.count())
print("Clientes sin ID:", c.filter(F.col("customer_id").isNull()).count())
print("Correos NULL:", c.filter(F.col("email").isNull()).count())
print("Fechas de nacimiento NULL:", c.filter(F.col("birth_date").isNull()).count())

print("\n--- MUESTRA DE CLIENTES ---")
c.show(10, truncate=False)

spark.stop()
