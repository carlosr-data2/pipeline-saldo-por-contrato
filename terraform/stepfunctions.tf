resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/states/${var.prefixo}-fechamento"
  retention_in_days = 90
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.prefixo}-fechamento"
  role_arn = aws_iam_role.step_functions.arn

  definition = templatefile("${path.module}/templates/pipeline.asl.json", {
    job_bronze = aws_glue_job.pipeline["bronze_ingest"].name
    job_silver = aws_glue_job.pipeline["silver_quality"].name
    job_gold   = aws_glue_job.pipeline["gold_saldo"].name
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }
}

resource "aws_iam_role_policy" "step_functions_logs" {
  name = "logs"
  role = aws_iam_role.step_functions.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery",
        "logs:DeleteLogDelivery", "logs:ListLogDeliveries", "logs:PutResourcePolicy",
        "logs:DescribeResourcePolicies", "logs:DescribeLogGroups"
      ]
      Resource = "*"
    }]
  })
}
