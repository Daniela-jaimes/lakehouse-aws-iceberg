resource "aws_athena_workgroup" "wg" {
  name          = "${var.project}-wg"
  force_destroy = true
  configuration {
    enforce_workgroup_configuration = true
    bytes_scanned_cutoff_per_query  = 5368709120 # 5 GB por consulta (~0,025 USD)
    engine_version { selected_engine_version = "Athena engine version 3" }
    result_configuration { output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/" }
  }
}
