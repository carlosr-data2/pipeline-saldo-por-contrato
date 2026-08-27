# Gatilho: tempo, não chegada de arquivo (ADR-001). 22:05 no fuso de São Paulo,
# logo após a janela D+0 22h do contrato.
resource "aws_scheduler_schedule" "fechamento_diario" {
  name = "${var.prefixo}-fechamento-diario"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "cron(5 22 * * ? *)"
  schedule_expression_timezone = "America/Sao_Paulo"

  target {
    arn      = aws_sfn_state_machine.pipeline.arn
    role_arn = aws_iam_role.agendador.arn
    input    = jsonencode({}) # dt resolvido dentro da própria Step Function

    retry_policy {
      maximum_retry_attempts       = 3
      maximum_event_age_in_seconds = 1800 # depois disso o alarme-sentinela é quem cobra
    }

    # Perda silenciosa do disparo é o único papel que sobrou para uma fila (ADR-003)
    dead_letter_config {
      arn = aws_sqs_queue.dlq_disparo.arn
    }
  }
}

resource "aws_sqs_queue" "dlq_disparo" {
  name                      = "${var.prefixo}-dlq-disparo"
  message_retention_seconds = 1209600 # 14 dias
}

# Execução FAILED/TIMED_OUT/ABORTED -> e-mail. O registro durável da falha é o
# histórico da própria Step Function (retomada via redrive — P4.3).
resource "aws_cloudwatch_event_rule" "execucao_falhou" {
  name = "${var.prefixo}-execucao-falhou"
  event_pattern = jsonencode({
    source      = ["aws.states"]
    detail-type = ["Step Functions Execution Status Change"]
    detail = {
      status          = ["FAILED", "TIMED_OUT", "ABORTED"]
      stateMachineArn = [aws_sfn_state_machine.pipeline.arn]
    }
  })
}

resource "aws_cloudwatch_event_target" "falha_para_sns" {
  rule = aws_cloudwatch_event_rule.execucao_falhou.name
  arn  = aws_sns_topic.alertas.arn
}
