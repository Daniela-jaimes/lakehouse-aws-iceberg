"""Shared constants. Change names here, never inside the jobs."""
import boto3

REGION = "us-east-1"
CATALOG = "glue_catalog"          # Iceberg catalog name inside Spark

BRONZE_DB = "lakehouse_bronze"
SILVER_DB = "lakehouse_silver"
GOLD_DB = "lakehouse_gold"

MAX_REJECT_RATE = 0.05            # the job fails above 5 % rejected rows

DATE_START = "2020-01-01"         # range of dim_date
DATE_END = "2030-12-31"


def get_bucket():
    """Bucket name built by Terraform: lakehouse-<account>-us-east-1."""
    account = boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]
    return f"lakehouse-{account}-{REGION}"


def table_name(database, table):
    """Full Iceberg name, e.g. glue_catalog.lakehouse_silver.orders."""
    return f"{CATALOG}.{database}.{table}"
