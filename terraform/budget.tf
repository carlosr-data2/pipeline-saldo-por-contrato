# Guarda-corpo de custo da prova de conceito: alerta em 80% e em 100% do teto.
resource "aws_budgets_budget" "teto" {
  name         = "${var.prefixo}-teto-mensal"
  budget_type  = "COST"
  limit_amount = tostring(var.orcamento_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.email_alertas]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.email_alertas]
  }
}
