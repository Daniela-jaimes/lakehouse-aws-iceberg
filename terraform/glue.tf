resource "aws_glue_catalog_database" "bronze" { name = "${var.project}_bronze" }
resource "aws_glue_catalog_database" "silver" {
  name         = "${var.project}_silver"
  location_uri = "s3://${aws_s3_bucket.lake.bucket}/silver/"
}
resource "aws_glue_catalog_database" "gold" {
  name         = "${var.project}_gold"
  location_uri = "s3://${aws_s3_bucket.lake.bucket}/gold/"
}
resource "aws_glue_classifier" "csv" {
  name = "${var.project}-csv-con-header"
  csv_classifier {
    contains_header = "PRESENT"
    delimiter       = ","
    quote_symbol    = "\""
  }
}
locals {
  oltp_tables = ["customers", "products", "stores", "orders", "order_items"]
}
resource "aws_glue_crawler" "oltp" {
  name          = "${var.project}-bronze-oltp"
  database_name = aws_glue_catalog_database.bronze.name
  role          = aws_iam_role.crawler.arn
  classifiers   = [aws_glue_classifier.csv.name]
  table_prefix  = "raw_"
  dynamic "s3_target" {
    for_each = local.oltp_tables
    content { path = "s3://${aws_s3_bucket.lake.bucket}/landing/oltp/${s3_target.value}/" }
  }
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}
resource "aws_glue_crawler" "logs" {
  name          = "${var.project}-bronze-logs"
  database_name = aws_glue_catalog_database.bronze.name
  role          = aws_iam_role.crawler.arn
  table_prefix  = "raw_"
  s3_target { path = "s3://${aws_s3_bucket.lake.bucket}/landing/logs/web_events/" }
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}
resource "aws_s3_object" "job_silver" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/bronze_to_silver.py"
  source = "${path.module}/../jobs/bronze_to_silver.py"
  etag   = filemd5("${path.module}/../jobs/bronze_to_silver.py")
}
resource "aws_s3_object" "job_gold" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/silver_to_gold.py"
  source = "${path.module}/../jobs/silver_to_gold.py"
  etag   = filemd5("${path.module}/../jobs/silver_to_gold.py")
}
locals {
  glue_args = {
    "--datalake-formats"        = "iceberg"
    "--enable-glue-datacatalog" = "true"
    "--enable-auto-scaling"     = "true"
    "--TempDir"                 = "s3://${aws_s3_bucket.lake.bucket}/glue-temp/"
    "--bucket"                  = aws_s3_bucket.lake.bucket
    "--bronze_db"               = aws_glue_catalog_database.bronze.name
    "--silver_db"               = aws_glue_catalog_database.silver.name
    "--gold_db"                 = aws_glue_catalog_database.gold.name
    "--ingest_date"             = "2026-11-07" # Step Functions losobreescribe
  }
}
resource "aws_glue_job" "bronze_to_silver" {
  name              = "${var.project}-bronze-to-silver"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = var.glue_workers
  timeout           = var.glue_timeout_min
  max_retries       = 0
  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lake.bucket}/${aws_s3_object.job_silver.key}"
  }
  default_arguments = local.glue_args
}
resource "aws_glue_job" "silver_to_gold" {
  name              = "${var.project}-silver-to-gold"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = var.glue_version
  worker_type       = "G.1X"
  number_of_workers = var.glue_workers
  timeout           = var.glue_timeout_min
  max_retries       = 0
  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.lake.bucket}/${aws_s3_object.job_gold.key}"
  }
  default_arguments = local.glue_args
}
