"""Job 1: bronze (raw_* tables) -> silver (Iceberg). Only the Spark DataFrame API is used."""
import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

from helpers.cleaning import deduplicate, is_valid_email, lower_text, title_text, clean_text
from helpers.config import BRONZE_DB, SILVER_DB, get_bucket, table_name
from helpers.iceberg_io import read_bronze, upsert
from helpers.quality import quality_gate
from helpers.spark_session import create_session


# ---------- one function per entity: type + normalize, and the quality rules ----------

def prepare_customers(raw):
    df = raw.select(
        F.col("customer_id").cast("long").alias("customer_id"),
        title_text(F.col("first_name")).alias("first_name"),
        title_text(F.col("last_name")).alias("last_name"),
        lower_text(F.col("email")).alias("email"),
        F.to_date(F.col("birth_date")).alias("birth_date"),
        F.to_timestamp(F.col("updated_at")).alias("updated_at"),
    )
    rules = [
        ("missing_customer_id", F.col("customer_id").isNull()),
        ("invalid_email", ~is_valid_email(F.col("email"))),
    ]
    return df, rules, "updated_at"


def prepare_products(raw):
    df = raw.select(
        F.col("product_id").cast("long").alias("product_id"),
        clean_text(F.col("product_name")).alias("product_name"),
        title_text(F.col("category")).alias("category"),
        F.to_timestamp(F.col("updated_at")).alias("updated_at"),
    )
    rules = [
        ("missing_product_id", F.col("product_id").isNull()),
        ("missing_category", F.col("category").isNull()),
    ]
    return df, rules, "updated_at"


def prepare_stores(raw):
    df = raw.select(
        F.col("store_id").cast("long").alias("store_id"),
        clean_text(F.col("store_name")).alias("store_name"),
        title_text(F.col("city")).alias("city"),
        title_text(F.col("department")).alias("department"),
        F.to_timestamp(F.col("updated_at")).alias("updated_at"),
    )
    rules = [
        ("missing_store_id", F.col("store_id").isNull()),
        ("missing_location", F.col("city").isNull() | F.col("department").isNull()),
    ]
    return df, rules, "updated_at"


def prepare_orders(raw):
    df = raw.select(
        F.col("order_id").cast("long").alias("order_id"),
        F.col("customer_id").cast("long").alias("customer_id"),
        F.col("store_id").cast("long").alias("store_id"),
        F.to_date(F.col("order_date")).alias("order_date"),
        lower_text(F.col("status")).alias("status"),
        lower_text(F.col("payment_method")).alias("payment_method"),
        F.to_timestamp(F.col("updated_at")).alias("updated_at"),
    )
    rules = [
        ("missing_order_id", F.col("order_id").isNull()),
        ("missing_customer_id", F.col("customer_id").isNull()),
        ("missing_store_id", F.col("store_id").isNull()),
        ("invalid_order_date", F.col("order_date").isNull()),
        ("future_order_date", F.col("order_date") > F.current_date()),
        ("missing_status", F.col("status").isNull()),
    ]
    return df, rules, "updated_at"


def prepare_order_items(raw):
    df = raw.select(
        F.col("order_item_id").cast("long").alias("order_item_id"),
        F.col("order_id").cast("long").alias("order_id"),
        F.col("product_id").cast("long").alias("product_id"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("decimal(12,2)").alias("unit_price"),
        F.coalesce(F.col("discount_pct").cast("double"), F.lit(0.0)).alias("discount_pct"),
    )
    rules = [
        ("missing_order_item_id", F.col("order_item_id").isNull()),
        ("missing_order_id", F.col("order_id").isNull()),
        ("missing_product_id", F.col("product_id").isNull()),
        ("invalid_quantity", F.col("quantity").isNull() | (F.col("quantity") <= 0)),
        ("invalid_unit_price", F.col("unit_price").isNull() | (F.col("unit_price") < 0)),
        ("invalid_discount", (F.col("discount_pct") < 0) | (F.col("discount_pct") > 100)),
    ]
    return df, rules, None   # order items have no updated_at


# entity -> (bronze table, function, natural key, partition column)
ENTITIES = {
    "customers": ("raw_customers", prepare_customers, "customer_id", None),
    "products": ("raw_products", prepare_products, "product_id", None),
    "stores": ("raw_stores", prepare_stores, "store_id", None),
    "orders": ("raw_orders", prepare_orders, "order_id", "order_date"),
    "order_items": ("raw_order_items", prepare_order_items, "order_item_id", None),
}


def main():
    ingest_date = getResolvedOptions(sys.argv, ["ingest_date"])["ingest_date"]
    bucket = get_bucket()
    spark, glue_context = create_session(bucket)

    for entity, (bronze_table, prepare, key, partition_column) in ENTITIES.items():
        raw = read_bronze(glue_context, BRONZE_DB, bronze_table, ingest_date)
        typed, rules, order_column = prepare(raw)

        # validate first, so rows without key do not collapse into one in the dedupe
        rejects_path = f"s3://{bucket}/silver/_rejects/{entity}/ingest_date={ingest_date}/"
        valid = quality_gate(typed, entity, rules, rejects_path)

        unique = deduplicate(valid, key, order_column) if order_column \
            else valid.dropDuplicates([key])

        final = (unique
                 .withColumn("_ingest_date", F.lit(ingest_date).cast("date"))
                 .withColumn("_loaded_at", F.current_timestamp()))

        upsert(spark, final, table_name(SILVER_DB, entity), key,
               location=f"s3://{bucket}/silver/{entity}",
               partition_column=partition_column)
        print(f"[SILVER] entity={entity} written for ingest_date={ingest_date}")


main()
