data "aws_caller_identity" "me" {}
locals {
  bucket = "${var.project}-${data.aws_caller_identity.me.account_id}-${var.region}"
  acct   = data.aws_caller_identity.me.account_id
}
resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket
  force_destroy = true
}
resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "athena-results-7d"
    status = "Enabled"
    filter { prefix = "athena-results/" }
    expiration { days = 7 }
  }
  rule {
    id     = "glue-temp-3d"
    status = "Enabled"
    filter { prefix = "glue-temp/" }
    expiration { days = 3 }
  }
}
resource "aws_s3_object" "prefixes" {
  for_each = toset(["landing/", "silver/", "gold/", "scripts/", "athena-results/", "glue-temp/", "warehouse/"])
  bucket   = aws_s3_bucket.lake.id
  key      = each.key
  content  = ""
}
