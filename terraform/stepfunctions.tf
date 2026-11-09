resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.project}-pipeline"
  role_arn = aws_iam_role.sfn.arn

  definition = jsonencode({
    Comment = "Pipeline lakehouse: bronze -> silver -> gold"
    StartAt = "BronzeToSilver"
    States = {
      BronzeToSilver = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.bronze_to_silver.name
          Arguments = {
            "--ingest_date.$" = "$.ingest_date"
          }
        }
        ResultPath = "$.silver"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "PipelineFailed"
        }]
        Next = "SilverToGold"
      }
      SilverToGold = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.silver_to_gold.name
          Arguments = {
            "--ingest_date.$" = "$.ingest_date"
          }
        }
        ResultPath = "$.gold"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "PipelineFailed"
        }]
        Next = "PipelineSucceeded"
      }
      PipelineFailed = {
        Type  = "Fail"
        Error = "PipelineFailed"
        Cause = "Un job de Glue fallo. Revise CloudWatch Logs."
      }
      PipelineSucceeded = {
        Type = "Succeed"
      }
    }
  })
}
