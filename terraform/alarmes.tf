# Alarmes-sentinela: AUSÊNCIA de sinal vira alerta (cobre inclusive trigger perdido).
# Nota honesta (defesa P4.4): alarme de métrica cobre a janela por período diário;
# a checagem fina "22:15 sem execução" em produção seria um schedule + verificação
# de 5 linhas — trade-off registrado em docs/arquitetura.md.

resource "aws_cloudwatch_metric_alarm" "sem_execucao_diaria" {
  alarm_name          = "${var.prefixo}-sem-execucao-24h"
  alarm_description   = "Nenhuma execução do fechamento iniciada nas últimas 24h (agendador quebrado ou desligado)"
  namespace           = "AWS/States"
  metric_name         = "ExecutionsStarted"
  dimensions          = { StateMachineArn = aws_sfn_state_machine.pipeline.arn }
  statistic           = "Sum"
  period              = 86400
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching" # ausência total de métrica também é falha
  alarm_actions       = [aws_sns_topic.alertas.arn]
}

resource "aws_cloudwatch_metric_alarm" "sem_sucesso_diario" {
  alarm_name          = "${var.prefixo}-sem-sucesso-24h"
  alarm_description   = "Nenhum fechamento concluído com sucesso nas últimas 24h (SLA das 06:00 em risco)"
  namespace           = "AWS/States"
  metric_name         = "ExecutionsSucceeded"
  dimensions          = { StateMachineArn = aws_sfn_state_machine.pipeline.arn }
  statistic           = "Sum"
  period              = 86400
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alertas.arn]
}

resource "aws_cloudwatch_metric_alarm" "disparo_perdido" {
  alarm_name          = "${var.prefixo}-disparo-na-dlq"
  alarm_description   = "O agendador não conseguiu iniciar a Step Function; o evento está na DLQ"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.dlq_disparo.name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alertas.arn]
}
