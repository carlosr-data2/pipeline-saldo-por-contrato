# --- Role dos jobs Glue: menor privilégio (o bucket do projeto + Catalog + logs) ---
resource "aws_iam_role" "glue_job" {
  name = "${var.prefixo}-glue-job"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "glue_job" {
  name = "acesso-pipeline"
  role = aws_iam_role.glue_job.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "DadosDoProjeto"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.dados.arn, "${aws_s3_bucket.dados.arn}/*"]
      },
      {
        Sid    = "CatalogoIceberg"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase",
          "glue:GetTable", "glue:GetTables", "glue:CreateTable",
          "glue:UpdateTable", "glue:DeleteTable"
        ]
        Resource = [
          "arn:aws:glue:${var.regiao}:${data.aws_caller_identity.atual.account_id}:catalog",
          "arn:aws:glue:${var.regiao}:${data.aws_caller_identity.atual.account_id}:database/*",
          "arn:aws:glue:${var.regiao}:${data.aws_caller_identity.atual.account_id}:table/*/*"
        ]
      },
      {
        Sid      = "LogsEMetricas"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "cloudwatch:PutMetricData"]
        Resource = "*"
      }
    ]
  })
}

# --- Role da Step Function: iniciar/acompanhar os jobs Glue (integração .sync) ---
resource "aws_iam_role" "step_functions" {
  name = "${var.prefixo}-step-functions"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "step_functions" {
  name = "orquestrar-glue"
  role = aws_iam_role.step_functions.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
      Resource = [for job in aws_glue_job.pipeline : job.arn]
    }]
  })
}

# --- Role do agendador (EventBridge Scheduler): iniciar a Step Function + DLQ ---
resource "aws_iam_role" "agendador" {
  name = "${var.prefixo}-agendador"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "agendador" {
  name = "disparar-pipeline"
  role = aws_iam_role.agendador.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "states:StartExecution"
        Resource = aws_sfn_state_machine.pipeline.arn
      },
      {
        Effect   = "Allow"
        Action   = "sqs:SendMessage"
        Resource = aws_sqs_queue.dlq_disparo.arn
      }
    ]
  })
}
