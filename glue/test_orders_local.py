from pyspark.sql import SparkSession, Window, functions as F

spark = (SparkSession.builder.master("local[2]").appName("test_orders_local")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

BASE = "data/out/landing/oltp/orders"
ESTADOS = ["completed", "shipped", "pending", "cancelled", "returned"]

def leer(fecha):
    return (spark.read.option("header", "true")
            .csv(f"{BASE}/ingest_date={fecha}/orders-0001.csv.gz")
            .withColumn("ingest_date", F.lit(fecha)))

def transformar(df):
    return df.select(
        F.col("order_id").cast("long").alias("order_id"),
        F.col("customer_id").cast("long").alias("customer_id"),
        F.col("store_id").cast("long").alias("store_id"),
        F.to_date("order_date").alias("order_date"),
        F.lower(F.trim("status")).alias("status"),
        F.lower(F.trim("payment_method")).alias("payment_method"),
        F.to_timestamp("updated_at").alias("updated_at"),
        "ingest_date")

def dedupe(df, key, order_col):
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

def calidad(df):
    cond = (F.col("order_id").isNotNull() & F.col("customer_id").isNotNull()
            & (F.col("order_date") <= F.current_date())
            & F.col("status").isin(*ESTADOS))
    df = df.withColumn("_ok", F.coalesce(cond, F.lit(False)))
    return df.filter("_ok").drop("_ok"), df.filter(~F.col("_ok")).count()

def check(nombre, obtenido, esperado):
    print(f"[{'OK' if obtenido == esperado else 'ERROR'}] {nombre}: obtenido={obtenido} esperado={esperado}")

raw1, raw2 = leer("2026-11-07"), leer("2026-11-09")
print("\n--- ESQUEMA ---")
print("columnas lote 1:", raw1.columns)
print("columnas lote 2:", raw2.columns)

t1, t2 = transformar(raw1), transformar(raw2)
print("\n--- DEDUPLICACION ---")
check("lote 1 filas originales", t1.count(), 50500)
d1 = dedupe(t1, "order_id", "updated_at")
check("lote 1 pedidos unicos", d1.count(), 50000)
check("lote 2 filas originales", t2.count(), 5000)
d2 = dedupe(t2, "order_id", "updated_at")
check("lote 2 pedidos unicos", d2.count(), 5000)

print("\n--- CALIDAD ---")
s1, rej1 = calidad(d1)
s2, rej2 = calidad(d2)
check("rechazados lote 1", rej1, 0)
check("rechazados lote 2", rej2, 0)
s1.cache(); s2.cache()
check("returned antes del lote 2", s1.filter("status = 'returned'").count(), 990)

print("\n--- SIMULACION DEL MERGE ---")
sin_tocar = s1.join(s2.select("order_id"), "order_id", "left_anti")
silver = sin_tocar.unionByName(s2)
check("pedidos insertados (nuevos)", s2.join(s1.select("order_id"), "order_id", "left_anti").count(), 4000)
check("pedidos actualizados", s2.join(s1.select("order_id"), "order_id", "left_semi").count(), 1000)
check("total silver.orders despues", silver.count(), 54000)
check("returned despues del lote 2", silver.filter("status = 'returned'").count(), 2072)
cambiados = (silver.alias("n").join(s1.alias("o"), "order_id")
             .filter(F.col("n.updated_at") != F.col("o.updated_at")))
check("actualizados con updated_at nuevo y status returned",
      cambiados.filter("n.status = 'returned'").count(), 1000)
check("ids duplicados en silver", silver.count() - silver.select("order_id").distinct().count(), 0)

print("\n--- TIPOS ---")
silver.printSchema()
