#Los tres roles de servicio, con mínimo privilegio
# ---------- Rol de los crawlers
data "aws_iam_policy_document" "glue_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "crawler" {
  name               = "${var.project}-crawler-role"
  assume_role_policy = data.aws_iam_policy_document.glue_trust.json
}
resource "aws_iam_role_policy_attachment" "crawler_managed" {
  role       = aws_iam_role.crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}
resource "aws_iam_role_policy" "crawler_s3" {
  name = "leer-landing"
  role = aws_iam_role.crawler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = aws_s3_bucket.lake.arn },
      { Effect = "Allow", Action = ["s3:GetObject"], Resource = "${aws_s3_bucket.lake.arn}/landing/*" }
    ]
  })
}
# ---------- Rol de los jobs de Spark
resource "aws_iam_role" "glue" {
  name               = "${var.project}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_trust.json
}
data "aws_iam_policy_document" "glue_perms" {
  statement {
    actions = ["glue:*"]
    resources = [
      "arn:aws:glue:${var.region}:${local.acct}:catalog",
      "arn:aws:glue:${var.region}:${local.acct}:database/${var.project}_*",
      "arn:aws:glue:${var.region}:${local.acct}:table/${var.project}_*/*",
      "arn:aws:glue:${var.region}:${local.acct}:userDefinedFunction/${var.project}_*/*",
    ]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }
  statement {
    actions = ["logs:CreateLogGroup", "logs:CreateLogStream",
    "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${var.region}:${local.acct}:log-group:/aws-glue/*"]
  }
}
resource "aws_iam_role_policy" "glue" {
  name   = "glue-minimo"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_perms.json
}
# ---------- Rol de Step Functions
data "aws_iam_policy_document" "sfn_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "sfn" {
  name               = "${var.project}-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_trust.json
}
resource "aws_iam_role_policy" "sfn" {
  name = "sfn-glue-jobs"
  role = aws_iam_role.sfn.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns",
      "glue:BatchStopJobRun"]
      Resource = [aws_glue_job.bronze_to_silver.arn,
      aws_glue_job.silver_to_gold.arn]
    }]
  })
}
