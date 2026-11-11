from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F

BASE = Path(__file__).resolve().parents[1]
LANDING = BASE / "data" / "out" / "landing" / "oltp"
B1, B2 = "2026-11-07", "2026-11-09"
ESTADOS = ["completed", "shipped", "pending", "cancelled", "returned"]

spark = (SparkSession.builder.master("local[2]").appName("LakehouseFactLocal")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def leer(tabla, fecha):
    ruta = LANDING / tabla / f"ingest_date={fecha}" / f"{tabla}-0001.csv.gz"
    return (spark.read.option("header", True).option("inferSchema", True)
            .csv(str(ruta)).withColumn("ingest_date", F.lit(fecha)))


def check(nombre, obtenido, esperado):
    print(f"[{'OK' if obtenido == esperado else 'ERROR'}] {nombre}: obtenido={obtenido} esperado={esperado}")


def dedupe(df, key, order_col):
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def limpio(df, cond):
    return df.filter(F.coalesce(cond, F.lit(False)))


# ---------- silver: maestros (lote 1)
cust = leer("customers", B1).select(
    F.col("customer_id").cast("long"), F.trim("city").alias("city"), F.trim("department").alias("department"))
prod = leer("products", B1).select(F.col("product_id").cast("long"))
store = leer("stores", B1).select(F.col("store_id").cast("long"))


# ---------- silver: pedidos e items
def pedidos(f):
    o = leer("orders", f).select(
        F.col("order_id").cast("long").alias("order_id"), F.col("customer_id").cast("long").alias("customer_id"),
        F.col("store_id").cast("long").alias("store_id"), F.to_date("order_date").alias("order_date"),
        F.lower(F.trim("status")).alias("status"), F.lower(F.trim("payment_method")).alias("payment_method"),
        F.to_timestamp("updated_at").alias("updated_at"), "ingest_date")
    o = dedupe(o, "order_id", "updated_at")
    return limpio(o, F.col("order_id").isNotNull() & F.col("customer_id").isNotNull()
                  & (F.col("order_date") <= F.current_date()) & F.col("status").isin(*ESTADOS))


def items(f):
    i = leer("order_items", f).select(
        F.col("order_item_id").cast("long").alias("order_item_id"), F.col("order_id").cast("long").alias("order_id"),
        F.col("product_id").cast("long").alias("product_id"), F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("decimal(14,2)").alias("unit_price"),
        F.col("discount_pct").cast("double").alias("discount_pct"), "ingest_date")
    i = dedupe(i, "order_item_id", "ingest_date")
    return limpio(i, F.col("order_item_id").isNotNull() & (F.col("quantity") > 0)
                  & (F.col("unit_price") > 0) & F.col("discount_pct").between(0, 1))


o1, o2, i1, i2 = pedidos(B1), pedidos(B2), items(B1), items(B2)
orders_after2 = o1.join(o2.select("order_id"), "order_id", "left_anti").unionByName(o2)   # MERGE de silver.orders
items_after2 = i1.unionByName(i2)

# ---------- gold: construccion del hecho (mismo codigo que silver_to_gold.py)
sk = lambda *cols: F.abs(F.xxhash64(*cols))


def build_fact(items_df, orders_df, D):
    touched = orders_df.filter(F.col("ingest_date") == D).select(
        "order_id", "customer_id", "store_id", "order_date", "status", "payment_method")
    c_ = F.broadcast(cust.select("customer_id", "department", "city", F.lit(1).alias("_c")))
    p_ = F.broadcast(prod.select("product_id", F.lit(1).alias("_p")))
    s_ = F.broadcast(store.select("store_id", F.lit(1).alias("_s")))
    gross = F.col("quantity") * F.col("unit_price")
    return (items_df.drop("ingest_date").join(touched, "order_id")
            .join(c_, "customer_id", "left").join(p_, "product_id", "left").join(s_, "store_id", "left")
            .select("order_item_id", "order_id",
                    F.date_format("order_date", "yyyyMMdd").cast("int").alias("date_key"),
                    F.when(F.col("_c").isNotNull(), sk(F.col("customer_id"))).otherwise(F.lit(-1)).alias("customer_key"),
                    F.when(F.col("_p").isNotNull(), sk(F.col("product_id"))).otherwise(F.lit(-1)).alias("product_key"),
                    F.when(F.col("_s").isNotNull(), sk(F.col("store_id"))).otherwise(F.lit(-1)).alias("store_key"),
                    F.when(F.col("_c").isNotNull(), sk(F.col("department"), F.col("city"))).otherwise(F.lit(-1)).alias("geography_key"),
                    "order_date", "quantity", "unit_price", "discount_pct",
                    gross.cast("decimal(14,2)").alias("gross_amount"),
                    (gross * F.col("discount_pct")).cast("decimal(14,2)").alias("discount_amount"),
                    (gross * (1 - F.col("discount_pct"))).cast("decimal(14,2)").alias("net_amount"),
                    F.col("status").alias("order_status"), "payment_method"))


fact1 = build_fact(i1, o1, B1).cache()
fact2 = build_fact(items_after2, orders_after2, B2).cache()
fact = fact1.join(fact2.select("order_item_id"), "order_item_id", "left_anti").unionByName(fact2).cache()   # MERGE

print("\n--- LOTE 1 ---")
check("filas de fact_sales", fact1.count(), 123575)
check("pedidos distintos", fact1.select("order_id").distinct().count(), 49963)
check("net_amount total", str(fact1.agg(F.sum("net_amount")).first()[0]), "125042546835.00")

print("\n--- LOTE 2 (solo pedidos tocados) ---")
check("filas reconstruidas", fact2.count(), 12305)
check("pedidos distintos", fact2.select("order_id").distinct().count(), 4998)
check("filas que ya existian (se actualizan)", fact2.join(fact1.select("order_item_id"), "order_item_id", "left_semi").count(), 2487)
check("filas nuevas (se insertan)", fact2.join(fact1.select("order_item_id"), "order_item_id", "left_anti").count(), 9818)

print("\n--- DESPUES DEL MERGE ---")
check("filas de fact_sales", fact.count(), 133393)
check("order_item_id duplicados", fact.count() - fact.select("order_item_id").distinct().count(), 0)
check("pedidos distintos", fact.select("order_id").distinct().count(), 53963)
check("filas returned (antes 2468)", fact.filter("order_status = 'returned'").count(), 5147)
check("filas cancelled/returned (fuera del mart)", fact.filter("order_status IN ('cancelled','returned')").count(), 11727)
check("net_amount total", str(fact.agg(F.sum("net_amount")).first()[0]), "134989751255.00")
check("net_amount sin cancelled/returned", str(fact.filter("order_status NOT IN ('cancelled','returned')").agg(F.sum("net_amount")).first()[0]), "123176979780.00")
check("filas con descuento", fact.filter("discount_pct > 0").count(), 66516)
check("net != gross - discount (+-0.01)", fact.filter(F.abs(F.col("net_amount") - (F.col("gross_amount") - F.col("discount_amount"))) > 0.01).count(), 0)

print("\n--- INTEGRIDAD REFERENCIAL ---")
dk = dates = spark.sql("SELECT explode(sequence(to_date('2025-01-01'), to_date('2027-12-31'), interval 1 day)) AS d") \
    .select(F.date_format("d", "yyyyMMdd").cast("int").alias("k"))
unk = lambda: spark.range(1).select(F.lit(-1).cast("long").alias("k"))
cust_k = cust.select(sk(F.col("customer_id")).alias("k")).unionByName(unk())
prod_k = prod.select(sk(F.col("product_id")).alias("k")).unionByName(unk())
store_k = store.select(sk(F.col("store_id")).alias("k")).unionByName(unk())
geo_k = (cust.select("department", "city").union(
    spark.read.option("header", True).csv(str(LANDING / "stores" / f"ingest_date={B1}" / "stores-0001.csv.gz"))
    .select(F.trim("department").alias("department"), F.trim("city").alias("city"))).distinct()
    .select(sk(F.col("department"), F.col("city")).alias("k")).unionByName(unk()))
for col, dim in [("customer_key", cust_k), ("product_key", prod_k), ("store_key", store_k),
                 ("geography_key", geo_k), ("date_key", dk)]:
    check(f"{col} huerfanas", fact.join(dim, fact[col] == dim["k"], "left_anti").count(), 0)
    check(f"{col} = -1 (Desconocido)", fact.filter(F.col(col) == -1).count(), 0)

print("\n--- MUESTRA ---")
fact.orderBy("order_item_id").show(5, truncate=False)
fact.printSchema()
spark.stop()
