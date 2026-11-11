from pathlib import Path

from pyspark.sql import SparkSession, functions as F


BASE = Path(__file__).resolve().parents[1]

ARCHIVO = (
    BASE
    / "data"
    / "out"
    / "landing"
    / "oltp"
    / "stores"
    / "ingest_date=2026-11-07"
    / "stores-0001.csv.gz"
)

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("LakehouseStoresLocal")
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
print("Total de tiendas:", df.count())

s = df.select(
    F.col("store_id").cast("long"),
    F.trim("store_name").alias("store_name"),
    F.trim("city").alias("city"),
    F.trim("department").alias("department")
)

print("\n--- RESULTADOS DE LA TRANSFORMACION ---")
print("Total transformado:", s.count())
print("Tiendas sin ID:", s.filter(F.col("store_id").isNull()).count())
print("Nombres NULL:", s.filter(F.col("store_name").isNull()).count())
print("Ciudades NULL:", s.filter(F.col("city").isNull()).count())
print("Departamentos NULL:", s.filter(F.col("department").isNull()).count())
print("IDs duplicados:", s.count() - s.select("store_id").distinct().count())

print("\n--- TIPOS DE DATOS ---")
s.printSchema()

print("\n--- MUESTRA DE TIENDAS ---")
s.show(10, truncate=False)

spark.stop()
