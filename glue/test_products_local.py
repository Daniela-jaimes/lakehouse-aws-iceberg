from pathlib import Path

from pyspark.sql import SparkSession, functions as F


BASE = Path(__file__).resolve().parents[1]

ARCHIVO = (
    BASE
    / "data"
    / "out"
    / "landing"
    / "oltp"
    / "products"
    / "ingest_date=2026-11-07"
    / "products-0001.csv.gz"
)

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("LakehouseProductsLocal")
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
print("Total de productos:", df.count())

p = df.select(
    F.col("product_id").cast("long"),
    F.trim("product_name").alias("product_name"),
    F.initcap(F.trim("category")).alias("category"),
    F.trim("brand").alias("brand"),
    F.col("list_price").cast("decimal(14,2)")
)

print("\n--- RESULTADOS DE LA TRANSFORMACION ---")
print("Total transformado:", p.count())
print("Productos sin ID:", p.filter(F.col("product_id").isNull()).count())
print("Precios NULL:", p.filter(F.col("list_price").isNull()).count())
print("Precios no positivos:", p.filter(F.col("list_price") <= 0).count())
print("Categorías NULL:", p.filter(F.col("category").isNull()).count())
print("Marcas NULL:", p.filter(F.col("brand").isNull()).count())

print("\n--- TIPOS DE DATOS ---")
p.printSchema()

print("\n--- MUESTRA DE PRODUCTOS ---")
p.show(10, truncate=False)

spark.stop()
