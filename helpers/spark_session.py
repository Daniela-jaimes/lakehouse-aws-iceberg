"""Creates the Spark session with the Iceberg catalog on top of the Glue Data Catalog."""
from awsglue.context import GlueContext
from pyspark import SparkConf, SparkContext

from helpers.config import CATALOG


def create_session(bucket):
    conf = SparkConf()
    conf.set("spark.sql.extensions",
             "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    conf.set(f"spark.sql.catalog.{CATALOG}.catalog-impl",
             "org.apache.iceberg.aws.glue.GlueCatalog")
    conf.set(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set(f"spark.sql.catalog.{CATALOG}.warehouse", f"s3://{bucket}/warehouse/")

    glue_context = GlueContext(SparkContext(conf=conf))
    return glue_context.spark_session, glue_context
