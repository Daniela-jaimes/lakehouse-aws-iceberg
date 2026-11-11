"""Job 2: silver -> gold (star schema in Iceberg). Only the Spark DataFrame API is used."""
import sys
from datetime import date

from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

from helpers.config import DATE_END, DATE_START, GOLD_DB, SILVER_DB, get_bucket, table_name
from helpers.iceberg_io import create_or_replace_table, upsert
from helpers.keys import add_unknown_row, surrogate_key
from helpers.spark_session import create_session


# ---------- dimensions (no PII: names and emails stay in silver) ----------

def build_dim_customer(customers):
    age = F.floor(F.months_between(F.current_date(), F.col("birth_date")) / 12)
    age_group = (F.when(age.isNull(), "Unknown")
                  .when(age < 18, "Under 18")
                  .when(age <= 25, "18-25")
                  .when(age <= 35, "26-35")
                  .when(age <= 45, "36-45")
                  .when(age <= 60, "46-60")
                  .otherwise("61+"))
    df = customers.select(
        surrogate_key(F.col("customer_id")).alias("customer_key"),
        F.col("customer_id"),
        age_group.alias("age_group"),
    )
    return add_unknown_row(df, "customer_key", {"age_group": "Unknown"})


def build_dim_product(products):
    df = products.select(
        surrogate_key(F.col("product_id")).alias("product_key"),
        F.col("product_id"), F.col("product_name"), F.col("category"),
    )
    return add_unknown_row(df, "product_key",
                           {"product_name": "Unknown", "category": "Unknown"})


def build_dim_store(stores):
    df = stores.select(
        surrogate_key(F.col("store_id")).alias("store_key"),
        F.col("store_id"), F.col("store_name"),
    )
    return add_unknown_row(df, "store_key", {"store_name": "Unknown"})


def build_dim_geography(stores):
    df = (stores.select("department", "city").distinct()
                .select(surrogate_key(F.col("department"), F.col("city")).alias("geography_key"),
                        F.col("department"), F.col("city")))
    return add_unknown_row(df, "geography_key", {"department": "Unknown", "city": "Unknown"})


def build_dim_date(spark):
    days = (date.fromisoformat(DATE_END) - date.fromisoformat(DATE_START)).days + 1
    df = (spark.range(days)
               .select(F.date_add(F.lit(DATE_START).cast("date"),
                                  F.col("id").cast("int")).alias("full_date")))
    df = df.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("full_date"),
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").alias("day_of_month"),
        F.dayofweek("full_date").alias("day_of_week"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
    )
    return add_unknown_row(df, "date_key", {"month_name": "Unknown"})


# ---------- fact table ----------

def build_fact_sales(orders, items, stores, dims, ingest_date):
    # only the orders touched by this batch (new or updated)
    touched = (orders.filter(F.col("_ingest_date") == F.lit(ingest_date).cast("date"))
                     .select("order_id", "customer_id", "store_id", "order_date",
                             F.col("status").alias("order_status"), "payment_method"))

    store_geography = stores.select(
        "store_id",
        surrogate_key(F.col("department"), F.col("city")).alias("geography_key"))

    lines = (items.join(touched, "order_id", "inner")
                  .join(F.broadcast(dims["customer"].select("customer_id", "customer_key")),
                        "customer_id", "left")
                  .join(F.broadcast(dims["product"].select("product_id", "product_key")),
                        "product_id", "left")
                  .join(F.broadcast(dims["store"].select("store_id", "store_key")),
                        "store_id", "left")
                  .join(F.broadcast(store_geography), "store_id", "left")
                  .na.fill(-1, ["customer_key", "product_key", "store_key", "geography_key"]))

    gross = (F.col("quantity") * F.col("unit_price")).cast("decimal(14,2)")
    discount = F.round(F.col("gross_amount") * F.col("discount_pct") / 100, 2)

    return (lines
            .withColumn("gross_amount", gross)
            .withColumn("discount_amount", discount.cast("decimal(14,2)"))
            .withColumn("net_amount",
                        (F.col("gross_amount") - F.col("discount_amount")).cast("decimal(14,2)"))
            .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
            .withColumn("_loaded_at", F.current_timestamp())
            .select("order_item_id", "order_id", "date_key",
                    "customer_key", "product_key", "store_key", "geography_key",
                    "order_date", "quantity", "discount_pct", "unit_price",
                    "gross_amount", "discount_amount", "net_amount",
                    "order_status", "payment_method", "_loaded_at"))


# ---------- data mart ----------

def build_mart(fact, dims):
    return (fact.filter(~F.col("order_status").isin("cancelled", "returned"))
                .join(F.broadcast(dims["date"].select("date_key", "year", "quarter")), "date_key")
                .join(F.broadcast(dims["customer"].select("customer_key", "age_group")),
                      "customer_key")
                .join(F.broadcast(dims["geography"].select("geography_key", "department")),
                      "geography_key")
                .groupBy("year", "quarter", "age_group", "department")
                .agg(F.countDistinct("order_id").alias("orders"),
                     F.sum("quantity").alias("units"),
                     F.sum("net_amount").alias("net_sales")))


def main():
    ingest_date = getResolvedOptions(sys.argv, ["ingest_date"])["ingest_date"]
    bucket = get_bucket()
    spark, _ = create_session(bucket)

    silver = lambda name: spark.table(table_name(SILVER_DB, name))
    gold_location = lambda name: f"s3://{bucket}/gold/{name}"

    customers, products, stores = silver("customers"), silver("products"), silver("stores")
    orders, items = silver("orders"), silver("order_items")

    # dimensions: rebuilt from silver on every run (SCD type 1)
    dims = {
        "customer": build_dim_customer(customers),
        "product": build_dim_product(products),
        "store": build_dim_store(stores),
        "geography": build_dim_geography(stores),
        "date": build_dim_date(spark),
    }
    for name, df in dims.items():
        create_or_replace_table(df, table_name(GOLD_DB, f"dim_{name}"), gold_location(f"dim_{name}"))
        print(f"[GOLD] dim_{name} written")

    # fact: upsert of the touched orders, partitioned by month
    fact_new = build_fact_sales(orders, items, stores, dims, ingest_date)
    upsert(spark, fact_new, table_name(GOLD_DB, "fact_sales"), "order_item_id",
           location=gold_location("fact_sales"), partition_column="order_date")
    print("[GOLD] fact_sales upserted")

    # mart: recomputed from the whole fact table
    fact_all = spark.table(table_name(GOLD_DB, "fact_sales"))
    create_or_replace_table(build_mart(fact_all, dims),
                            table_name(GOLD_DB, "agg_sales_dept_age_quarter"),
                            gold_location("agg_sales_dept_age_quarter"))
    print("[GOLD] agg_sales_dept_age_quarter written")


main()
