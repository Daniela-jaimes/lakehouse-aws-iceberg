from pathlib import Path

from pyspark.sql import SparkSession, functions as F

BASE = Path(__file__).resolve().parents[1]
LANDING = BASE / "data" / "out" / "landing" / "oltp"

spark = (SparkSession.builder.master("local[2]").appName("LakehouseDimsLocal")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def leer(tabla):
    ruta = LANDING / tabla / "ingest_date=2026-11-07" / f"{tabla}-0001.csv.gz"
    return spark.read.option("header", True).option("inferSchema", True).csv(str(ruta))


def check(nombre, obtenido, esperado):
    print(f"[{'OK' if obtenido == esperado else 'ERROR'}] {nombre}: obtenido={obtenido} esperado={esperado}")


# ---------- silver (mismas transformaciones que los tests anteriores)
cust = leer("customers").select(
    F.col("customer_id").cast("long"),
    F.initcap(F.trim("full_name")).alias("full_name"),
    F.when(F.lower(F.trim("email")).rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"), F.lower(F.trim("email"))).alias("email"),
    F.col("gender"),
    F.to_date("birth_date").alias("birth_date"),
    F.trim("city").alias("city"),
    F.trim("department").alias("department"))
cust = cust.withColumn("birth_date", F.when(
    (F.col("birth_date") > F.current_date()) | (F.col("birth_date") < F.lit("1900-01-01").cast("date")), None
).otherwise(F.col("birth_date")))

prod = leer("products").select(
    F.col("product_id").cast("long"), F.trim("product_name").alias("product_name"),
    F.initcap(F.trim("category")).alias("category"), F.trim("brand").alias("brand"),
    F.col("list_price").cast("decimal(14,2)"))

store = leer("stores").select(
    F.col("store_id").cast("long"), F.trim("store_name").alias("store_name"),
    F.trim("city").alias("city"), F.trim("department").alias("department"))

# ---------- gold (mismo codigo que silver_to_gold.py)
sk = lambda *cols: F.abs(F.xxhash64(*cols))


def with_unknown(df, **vals):
    unk = spark.range(1).select(*[F.lit(vals.get(f.name)).cast(f.dataType).alias(f.name) for f in df.schema.fields])
    return df.unionByName(unk)


REGION = {
    "Bogot\u00e1 D.C.": "Andina", "Antioquia": "Andina", "Valle del Cauca": "Pac\u00edfica",
    "Atl\u00e1ntico": "Caribe", "Santander": "Andina", "Cundinamarca": "Andina",
    "Bol\u00edvar": "Caribe", "Norte de Santander": "Andina", "Risaralda": "Andina",
    "Caldas": "Andina", "Tolima": "Andina", "Nari\u00f1o": "Pac\u00edfica",
    "Boyac\u00e1": "Andina", "Meta": "Orinoqu\u00eda", "Huila": "Andina", "C\u00f3rdoba": "Caribe"}
region_map = F.create_map([F.lit(x) for kv in REGION.items() for x in kv])

age = F.floor(F.months_between(F.current_date(), F.col("birth_date")) / 12).cast("int")
age_group = (F.when(age.isNull(), "Desconocido").when(age < 25, "18-24").when(age < 35, "25-34")
             .when(age < 45, "35-44").when(age < 55, "45-54").when(age < 65, "55-64").otherwise("65+"))

dim_customer = with_unknown(
    cust.select(sk(F.col("customer_id")).alias("customer_key"), "customer_id", "gender",
                F.year("birth_date").alias("birth_year"), age.alias("age"), age_group.alias("age_group")),
    customer_key=-1, customer_id=-1, gender="N/D", age_group="Desconocido")

dim_product = with_unknown(
    prod.select(sk(F.col("product_id")).alias("product_key"), "product_id", "product_name", "category", "brand",
                "list_price",
                F.when(F.col("list_price") < 50000, "Econ\u00f3mico").when(F.col("list_price") < 300000, "Medio")
                .otherwise("Premium").alias("price_tier")),
    product_key=-1, product_id=-1, product_name="Desconocido", category="Desconocido",
    brand="Desconocido", price_tier="Desconocido")

dim_store = with_unknown(
    store.select(sk(F.col("store_id")).alias("store_key"), "store_id", "store_name", "city", "department"),
    store_key=-1, store_id=-1, store_name="Desconocida", city="Desconocida", department="Desconocido")

geo = cust.select("department", "city").union(store.select("department", "city")).distinct()
dim_geography = with_unknown(
    geo.select(sk(F.col("department"), F.col("city")).alias("geography_key"), "department", "city",
               F.coalesce(region_map[F.col("department")], F.lit("Otra")).alias("region")),
    geography_key=-1, department="Desconocido", city="Desconocida", region="Desconocida")

dates = spark.sql("SELECT explode(sequence(to_date('2025-01-01'), to_date('2027-12-31'), interval 1 day)) AS full_date")
dim_date = with_unknown(
    dates.select(F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"), "full_date",
                 F.year("full_date").alias("year"), F.quarter("full_date").alias("quarter"),
                 F.month("full_date").alias("month"), F.date_format("full_date", "MMMM").alias("month_name"),
                 F.weekofyear("full_date").alias("week_of_year"), F.dayofweek("full_date").alias("day_of_week"),
                 F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
                 F.concat_ws("-Q", F.year("full_date"), F.quarter("full_date")).alias("year_quarter")),
    date_key=-1, month_name="Desconocido", year_quarter="Desconocido")

for d in (dim_customer, dim_product, dim_store, dim_geography, dim_date):
    d.cache()

# ---------- verificaciones
print("\n--- DIM_CUSTOMER ---")
check("filas (5000 + fila -1)", dim_customer.count(), 5001)
check("claves distintas", dim_customer.select("customer_key").distinct().count(), 5001)
check("filas con customer_key = -1", dim_customer.filter("customer_key = -1").count(), 1)
check("sin nombre ni correo (PII)", [c for c in dim_customer.columns if c in ("full_name", "email")], [])
check("age_group Desconocido (5 fechas futuras + fila -1)", dim_customer.filter("age_group = 'Desconocido'").count(), 6)
check("age_group nulo", dim_customer.filter(F.col("age_group").isNull()).count(), 0)
dim_customer.groupBy("age_group").count().orderBy("age_group").show()

print("--- DIM_PRODUCT ---")
check("filas (500 + fila -1)", dim_product.count(), 501)
check("claves distintas", dim_product.select("product_key").distinct().count(), 501)
check("filas con product_key = -1", dim_product.filter("product_key = -1").count(), 1)
check("categorias distintas (6 + Desconocido)", dim_product.select("category").distinct().count(), 7)
check("price_tier nulo", dim_product.filter(F.col("price_tier").isNull()).count(), 0)
dim_product.groupBy("price_tier").count().orderBy("price_tier").show()

print("--- DIM_STORE ---")
check("filas (30 + fila -1)", dim_store.count(), 31)
check("claves distintas", dim_store.select("store_key").distinct().count(), 31)
check("filas con store_key = -1", dim_store.filter("store_key = -1").count(), 1)

print("--- DIM_GEOGRAPHY ---")
check("filas (23 pares + fila -1)", dim_geography.count(), 24)
check("claves distintas", dim_geography.select("geography_key").distinct().count(), 24)
check("pares depto-ciudad duplicados", dim_geography.count() - dim_geography.select("department", "city").distinct().count(), 0)
check("region 'Otra' (depto sin mapear)", dim_geography.filter("region = 'Otra'").count(), 0)
check("filas con geography_key = -1", dim_geography.filter("geography_key = -1").count(), 1)
dim_geography.orderBy("department", "city").show(30, truncate=False)

print("--- DIM_DATE ---")
check("filas (1095 dias + fila -1)", dim_date.count(), 1096)
check("date_key distintos", dim_date.select("date_key").distinct().count(), 1096)
check("fecha minima", str(dim_date.filter("date_key <> -1").agg(F.min("full_date")).first()[0]), "2025-01-01")
check("fecha maxima", str(dim_date.filter("date_key <> -1").agg(F.max("full_date")).first()[0]), "2027-12-31")
check("20261107 (sabado) es fin de semana", dim_date.filter("date_key = 20261107").first()["is_weekend"], True)
check("20261109 (lunes) NO es fin de semana", dim_date.filter("date_key = 20261109").first()["is_weekend"], False)
check("20261109 trimestre", dim_date.filter("date_key = 20261109").first()["year_quarter"], "2026-Q4")

print("\n--- TIPOS ---")
dim_customer.printSchema()
spark.stop()
