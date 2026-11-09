output "bucket_name" { value = aws_s3_bucket.lake.bucket }
output "athena_workgroup" { value = aws_athena_workgroup.wg.name }
output "state_machine_arn" { value = aws_sfn_state_machine.pipeline.arn }
output "bronze_db" { value = aws_glue_catalog_database.bronze.name }
output "silver_db" { value = aws_glue_catalog_database.silver.name }
output "gold_db" { value = aws_glue_catalog_database.gold.name }
