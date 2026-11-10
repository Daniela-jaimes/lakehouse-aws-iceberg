from pyspark.sql import SparkSession, Window, functions as F

spark = (SparkSession.builder.master("local[2]").appName("test_items_local")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

BASE = "data/out/landing/oltp"

def leer(tabla, fecha):
    return (spark.read.option("header", "true")
            .csv(f"{BASE}/{tabla}/ingest_date={fecha}/{tabla}-0001.csv.gz")
            .withColumn("ingest_date", F.lit(fecha)))

def transformar(df):
    return df.select(
        F.col("order_item_id").cast("long").alias("order_item_id"),
        F.col("order_id").cast("long").alias("order_id"),
        F.col("product_id").cast("long").alias("product_id"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("decimal(14,2)").alias("unit_price"),
        F.col("discount_pct").cast("double").alias("discount_pct"),
        "ingest_date")

def dedupe(df, key, order_col):
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

def calidad(df):
    cond = (F.col("order_item_id").isNotNull() & (F.col("quantity") > 0)
            & (F.col("unit_price") > 0) & F.col("discount_pct").between(0, 1))
    df = df.withColumn("_ok", F.coalesce(cond, F.lit(False)))
    return df.filter("_ok").drop("_ok"), df.filter(~F.col("_ok")).count()

def check(nombre, obtenido, esperado):
    print(f"[{'OK' if obtenido == esperado else 'ERROR'}] {nombre}: obtenido={obtenido} esperado={esperado}")

raw1, raw2 = leer("order_items", "2026-11-07"), leer("order_items", "2026-11-09")
print("\n--- ESQUEMA ---")
print("columnas lote 1:", raw1.columns)
print("columnas lote 2:", raw2.columns)

t1, t2 = transformar(raw1), transformar(raw2)
print("\n--- DEDUPLICACION ---")
check("lote 1 filas originales", t1.count(), 123988)
d1 = dedupe(t1, "order_item_id", "ingest_date")
check("lote 1 items unicos", d1.count(), 123988)
check("lote 2 filas originales", t2.count(), 9818)
d2 = dedupe(t2, "order_item_id", "ingest_date")
check("lote 2 items unicos", d2.count(), 9818)

print("\n--- CALIDAD ---")
s1, rej1 = calidad(d1)
s2, rej2 = calidad(d2)
check("rechazados lote 1 (quantity <= 0)", rej1, 413)
check("rechazados lote 2", rej2, 0)
print(f"[INFO] porcentaje rechazado lote 1: {100 * rej1 / 123988:.2f}% (umbral 5%)")
s1.cache(); s2.cache()

print("\n--- MERGE ---")
check("solape de order_item_id entre lotes", s1.join(s2.select("order_item_id"), "order_item_id", "left_semi").count(), 0)
silver = s1.unionByName(s2)
check("total silver.order_items despues", silver.count(), 133393)
check("ids duplicados en silver", silver.count() - silver.select("order_item_id").distinct().count(), 0)

print("\n--- INTEGRIDAD CON ORDERS ---")
ped1 = spark.read.option("header", "true").csv(f"{BASE}/orders/ingest_date=2026-11-07/orders-0001.csv.gz").select(F.col("order_id").cast("long").alias("order_id"))
ped2 = spark.read.option("header", "true").csv(f"{BASE}/orders/ingest_date=2026-11-09/orders-0001.csv.gz").select(F.col("order_id").cast("long").alias("order_id"))
todos = ped1.union(ped2).distinct()
check("items huerfanos (sin pedido)", silver.join(todos, "order_id", "left_anti").count(), 0)
check("pedidos con al menos un item", silver.select("order_id").distinct().count(), 53963)

print("\n--- TIPOS ---")
silver.printSchema()
