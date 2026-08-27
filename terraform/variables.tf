variable "regiao" {
  description = "Região AWS (us-east-1: menor custo de Glue/S3 para a prova de conceito)"
  type        = string
  default     = "us-east-1"
}

variable "prefixo" {
  description = "Prefixo dos nomes de recursos"
  type        = string
  default     = "saldo-contrato"
}

variable "email_alertas" {
  description = "E-mail que recebe falhas do pipeline (SNS) e alertas de orçamento"
  type        = string
}

variable "orcamento_usd" {
  description = "Teto mensal de gasto (US$) do budget alarm"
  type        = number
  default     = 10
}

# --- Dimensionamento dos jobs Glue (volume da prova de conceito; ver
# --- docs/arquitetura.md para o dimensionamento de producao de 300M/dia) ---
variable "glue_worker_type" {
  type    = string
  default = "G.1X" # 4 vCPU / 16 GB por worker
}

variable "glue_numero_workers" {
  type    = number
  default = 2 # mínimo do Glue; sobra para 200 mil linhas
}

variable "glue_timeout_minutos" {
  description = "Timeout por job. O SLA é <1h por execução do pipeline inteiro; nenhum estágio pode passar disso"
  type        = number
  default     = 15
}

variable "iceberg_versao" {
  description = "Versão do Iceberg embarcada via --extra-jars (Glue 5.0 nativo é 1.7.x, sem V3)"
  type        = string
  default     = "1.10.2"
}
