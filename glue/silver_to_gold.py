import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket", "silver_db", "gold_db", "ingest_date"])
B, D = args["bucket"], args["ingest_date"]

spark = (SparkSession.builder
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
         .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
         .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{B}/warehouse/")
         .config("spark.sql.shuffle.partitions", "64")
         .getOrCreate())
glue = GlueContext(spark.sparkContext)
job = Job(glue)
job.init(args["JOB_NAME"], args)

S = lambda t: spark.table(f"glue_catalog.{args['silver_db']}.{t}")
G = lambda t: spark.table(f"glue_catalog.{args['gold_db']}.{t}")
sk = lambda *cols: F.abs(F.xxhash64(*cols))  # surrogate key determinista


def upsert(df, table, key, partition=None):
    full = f"glue_catalog.{args['gold_db']}.{table}"
    df = df.withColumn("_loaded_at", F.current_timestamp())
    if not spark.catalog.tableExists(full):
        w = (df.writeTo(full).using("iceberg")
             .tableProperty("format-version", "2")
             .tableProperty("write.parquet.compression-codec", "zstd")
             .option("fanout-enabled", "true"))
        if partition is not None:
            w = w.partitionedBy(partition)
        w.create()
        print(f"[ICEBERG] creada {full}")
    else:
        df.createOrReplaceTempView("src")
        spark.sql(f"""MERGE INTO {full} t USING src s ON t.{key} = s.{key}
                      WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
        print(f"[ICEBERG] MERGE en {full}")
    spark.catalog.clearCache()


def with_unknown(df, **vals):
    """Agrega la fila 'Desconocido' (clave -1) para que ningun hecho quede huerfano."""
    unk = spark.range(1).select(*[F.lit(vals.get(f.name)).cast(f.dataType).alias(f.name) for f in df.schema.fields])
    return df.unionByName(unk)


cust, prod, store, orders, items = S("customers"), S("products"), S("stores"), S("orders"), S("order_items")

REGION = {
    "Bogot\u00e1 D.C.": "Andina", "Antioquia": "Andina", "Valle del Cauca": "Pac\u00edfica",
    "Atl\u00e1ntico": "Caribe", "Santander": "Andina", "Cundinamarca": "Andina",
    "Bol\u00edvar": "Caribe", "Norte de Santander": "Andina", "Risaralda": "Andina",
    "Caldas": "Andina", "Tolima": "Andina", "Nari\u00f1o": "Pac\u00edfica",
    "Boyac\u00e1": "Andina", "Meta": "Orinoqu\u00eda", "Huila": "Andina", "C\u00f3rdoba": "Caribe"}
region_map = F.create_map([F.lit(x) for kv in REGION.items() for x in kv])

# ---------------- dimensiones (gold NO contiene nombre ni correo)
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

upsert(dim_customer, "dim_customer", "customer_key")
upsert(dim_product, "dim_product", "product_key")
upsert(dim_store, "dim_store", "store_key")
upsert(dim_geography, "dim_geography", "geography_key")
upsert(dim_date, "dim_date", "date_key")

# ---------------- hechos: solo pedidos tocados en este lote (nuevos o actualizados)
touched = orders.filter(F.col("ingest_date") == D).select(
    "order_id", "customer_id", "store_id", "order_date", "status", "payment_method")
c_ = F.broadcast(cust.select("customer_id", "department", "city", F.lit(1).alias("_c")))
p_ = F.broadcast(prod.select("product_id", F.lit(1).alias("_p")))
s_ = F.broadcast(store.select("store_id", F.lit(1).alias("_s")))
gross = F.col("quantity") * F.col("unit_price")

fact = (items.drop("ingest_date", "_loaded_at").join(touched, "order_id")
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

upsert(fact, "fact_sales", "order_item_id", F.months("order_date"))

# ---------------- data mart: la pregunta del gerente precalculada
mart = (G("fact_sales").filter(~F.col("order_status").isin("cancelled", "returned"))
        .join(G("dim_date").select("date_key", "year", "quarter"), "date_key")
        .join(G("dim_geography").select("geography_key", "department"), "geography_key")
        .join(G("dim_customer").select("customer_key", "age_group"), "customer_key")
        .groupBy("year", "quarter", "department", "age_group")
        .agg(F.countDistinct("order_id").alias("orders"), F.sum("quantity").alias("units"),
             F.sum("net_amount").alias("net_sales"))
        .withColumn("_loaded_at", F.current_timestamp()))

(mart.writeTo(f"glue_catalog.{args['gold_db']}.agg_sales_dept_age_quarter").using("iceberg")
 .tableProperty("write.parquet.compression-codec", "zstd").createOrReplace())
print("[ICEBERG] mart agg_sales_dept_age_quarter reconstruido")

job.commit()
