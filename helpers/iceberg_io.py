"""Read bronze and write Iceberg tables using only the DataFrame / DataFrameWriterV2 API."""
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException


def read_bronze(glue_context, database, table, ingest_date):
    """Read one ingest_date partition of a crawler table."""
    try:
        frame = glue_context.create_dynamic_frame.from_catalog(
            database=database, table_name=table,
            push_down_predicate=f"ingest_date == '{ingest_date}'")
        return frame.toDF()
    except Exception:
        # ingest_date was typed as date: no predicate, filter after reading
        frame = glue_context.create_dynamic_frame.from_catalog(
            database=database, table_name=table)
        return frame.toDF().filter(F.col("ingest_date").cast("string") == ingest_date)


def table_exists(spark, table):
    try:
        spark.table(table)
        return True
    except AnalysisException:
        return False


def create_or_replace_table(df, table, location, partition_column=None):
    """Create the Iceberg table (or replace its contents) with the DataFrame schema."""
    writer = (df.writeTo(table).using("iceberg")
                .tableProperty("format-version", "2")
                .tableProperty("write.parquet.compression-codec", "zstd")
                .tableProperty("location", location))
    if partition_column:
        writer = writer.partitionedBy(F.months(partition_column))
    writer.createOrReplace()


def upsert(spark, new_df, table, key, location, partition_column=None):
    """
    Insert new keys and replace existing ones (what MERGE INTO does) without SQL:
      1. take the existing rows (only the touched months when the table is partitioned)
      2. drop the ones whose key arrives again (left_anti join)
      3. add the new rows and overwrite
    Running it twice with the same input leaves the same table (idempotent).
    """
    if not table_exists(spark, table):
        create_or_replace_table(new_df, table, location, partition_column)
        return

    existing = spark.table(table)
    new_keys = new_df.select(key).distinct()

    if partition_column:
        touched = new_df.select(F.date_trunc("month", partition_column).alias("_month")).distinct()
        existing = (existing.withColumn("_month", F.date_trunc("month", partition_column))
                            .join(touched, "_month", "left_semi")
                            .drop("_month"))

    result = existing.join(new_keys, key, "left_anti").unionByName(new_df)

    if partition_column:
        result.writeTo(table).overwritePartitions()   # replaces only the months in result
    else:
        result.writeTo(table).overwrite(F.lit(True))  # replaces the whole (small) table
