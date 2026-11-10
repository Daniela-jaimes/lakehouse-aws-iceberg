import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window, functions as F


# ============================================================
# 1. CONFIGURACION DEL JOB
# ============================================================

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "bucket", "bronze_db", "silver_db", "ingest_date"]
)

B = args["bucket"]
D = args["ingest_date"]


# ============================================================
# 2. CONFIGURACION DE SPARK E ICEBERG
# ============================================================

spark = (
    SparkSession.builder
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.glue_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog"
    )
    .config(
        "spark.sql.catalog.glue_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )
    .config(
        "spark.sql.catalog.glue_catalog.warehouse",
        f"s3://{B}/warehouse/"
    )
    .config("spark.sql.shuffle.partitions", "64")
    .getOrCreate()
)

glue = GlueContext(spark.sparkContext)

job = Job(glue)
job.init(args["JOB_NAME"], args)


# ============================================================
# 3. LECTURA DE TABLAS BRONZE
# ============================================================

def bronze(table):
    dyf = glue.create_dynamic_frame.from_catalog(
        database=args["bronze_db"],
        table_name=f"raw_{table}",
        push_down_predicate=f"ingest_date='{D}'"
    )

    return (
        dyf.toDF()
        .withColumn("ingest_date", F.col("ingest_date").cast("string"))
    )


# ============================================================
# 4. DEDUPLICACION
# ============================================================

def dedupe(df, key, order_col):
    w = (
        Window
        .partitionBy(key)
        .orderBy(F.col(order_col).desc())
    )

    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
    )


# ============================================================
# 5. CONTROL DE CALIDAD
# ============================================================

def quality_gate(df, cond, name, max_reject=0.05):
    """Separa validos de rechazados y falla si superan el umbral."""

    df = (
        df.withColumn("_ok", F.coalesce(cond, F.lit(False)))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    total = df.count()
    bad = df.filter(~F.col("_ok"))
    n_bad = bad.count()

    print(f"[QUALITY] {name}: total={total} rechazados={n_bad}")

    if n_bad:
        (
            bad.drop("_ok")
            .write
            .mode("append")
            .parquet(
                f"s3://{B}/silver/_rejects/{name}/ingest_date={D}/"
            )
        )

    if total and n_bad / total > max_reject:
        raise Exception(
            f"Calidad: {name} supera {max_reject:.0%} de rechazos"
        )

    return df.filter("_ok").drop("_ok")


# ============================================================
# 6. INSERCION Y ACTUALIZACION EN ICEBERG
# ============================================================

def upsert(df, table, key, partition=None):
    full = f"glue_catalog.{args['silver_db']}.{table}"

    df = df.withColumn("_loaded_at", F.current_timestamp())

    if not spark.catalog.tableExists(full):

        w = (
            df.writeTo(full)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .tableProperty("write.parquet.compression-codec", "zstd")
            .option("fanout-enabled", "true")
        )

        if partition is not None:
            w = w.partitionedBy(partition)

        w.create()

        print(f"[ICEBERG] creada {full}")

    else:

        df.createOrReplaceTempView("src")

        spark.sql(f"""
            MERGE INTO {full} t
            USING src s
            ON t.{key} = s.{key}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        print(f"[ICEBERG] MERGE en {full}")

    spark.catalog.clearCache()

# ============================================================
# 7. TRANSFORMACION DE CUSTOMERS
# ============================================================

c = bronze("customers").select(
    F.col("customer_id").cast("long"),
    F.initcap(F.trim("full_name")).alias("full_name"),
    F.when(
        F.lower(F.trim("email")).rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
        F.lower(F.trim("email"))
    ).alias("email"),
    F.col("gender"),
    F.to_date("birth_date").alias("birth_date"),
    F.trim("city").alias("city"),
    F.trim("department").alias("department"),
    F.to_date("created_at").alias("created_at"),
    "ingest_date"
)

# Las fechas de nacimiento futuras o anteriores a 1900 se convierten en NULL.
c = c.withColumn(
    "birth_date",
    F.when(
        (F.col("birth_date") > F.current_date()) |
        (F.col("birth_date") < F.lit("1900-01-01").cast("date")),
        None
    ).otherwise(F.col("birth_date"))
)

# Validacion, deduplicacion y escritura en Silver.
c = quality_gate(
    dedupe(c, "customer_id", "ingest_date"),
    F.col("customer_id").isNotNull(),
    "customers"
)

upsert(c, "customers", "customer_id")

# ============================================================
# 8. TRANSFORMACION DE PRODUCTS
# ============================================================

p = bronze("products").select(
    F.col("product_id").cast("long"),
    F.trim("product_name").alias("product_name"),
    F.initcap(F.trim("category")).alias("category"),
    F.trim("brand").alias("brand"),
    F.col("list_price").cast("decimal(14,2)"),
    "ingest_date"
)

p = quality_gate(
    dedupe(p, "product_id", "ingest_date"),
    F.col("product_id").isNotNull() &
    (F.col("list_price") > 0),
    "products"
)

upsert(p, "products", "product_id")

# ============================================================
# 9. TRANSFORMACION DE STORES
# ============================================================

s = bronze("stores").select(
    F.col("store_id").cast("long"),
    F.trim("store_name").alias("store_name"),
    F.trim("city").alias("city"),
    F.trim("department").alias("department"),
    "ingest_date"
)

s = quality_gate(
    dedupe(s, "store_id", "ingest_date"),
    F.col("store_id").isNotNull(),
    "stores"
)

upsert(s, "stores", "store_id")

# ============================================================
# 10. TRANSFORMACION DE ORDERS
# ============================================================

o = bronze("orders").select(
    F.col("order_id").cast("long"),
    F.col("customer_id").cast("long"),
    F.col("store_id").cast("long"),
    F.to_date("order_date").alias("order_date"),
    F.lower(F.trim("status")).alias("status"),
    F.lower(F.trim("payment_method")).alias("payment_method"),
    F.to_timestamp("updated_at").alias("updated_at"),
    "ingest_date"
)

o = quality_gate(
    dedupe(o, "order_id", "updated_at"),
    F.col("order_id").isNotNull() &
    F.col("customer_id").isNotNull() &
    (F.col("order_date") <= F.current_date()) &
    F.col("status").isin(
        "completed",
        "shipped",
        "pending",
        "cancelled",
        "returned"
    ),
    "orders"
)

upsert(
    o,
    "orders",
    "order_id",
    F.months("order_date")
)

# ============================================================
# 11. TRANSFORMACION DE ORDER_ITEMS
# ============================================================

i = bronze("order_items").select(
    F.col("order_item_id").cast("long"),
    F.col("order_id").cast("long"),
    F.col("product_id").cast("long"),
    F.col("quantity").cast("int"),
    F.col("unit_price").cast("decimal(14,2)"),
    F.col("discount_pct").cast("double"),
    "ingest_date"
)

i = quality_gate(
    dedupe(i, "order_item_id", "ingest_date"),
    F.col("order_item_id").isNotNull() &
    (F.col("quantity") > 0) &
    (F.col("unit_price") > 0) &
    F.col("discount_pct").between(0, 1),
    "order_items"
)

upsert(i, "order_items", "order_item_id")


# ============================================================
# 12. FINALIZACION DEL JOB
# ============================================================

job.commit()
print("[OK] Proceso Bronze a Silver finalizado correctamente.")
