data "aws_caller_identity" "atual" {}

locals {
  bucket = "${var.prefixo}-${data.aws_caller_identity.atual.account_id}"
  # prefixos: raw/ (imutável, forense), warehouse/ (Iceberg), scripts/ (artefatos dos jobs)
  s3_warehouse = "s3://${local.bucket}/warehouse"
  s3_scripts   = "s3://${local.bucket}/scripts"
  s3_raw       = "s3://${local.bucket}/raw"
}

resource "aws_s3_bucket" "dados" {
  bucket        = local.bucket
  force_destroy = true # conta pessoal de prova de conceito: destruição limpa com terraform destroy
}

resource "aws_s3_bucket_public_access_block" "dados" {
  bucket                  = aws_s3_bucket.dados.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "dados" {
  bucket = aws_s3_bucket.dados.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Retenção 5y hot + 10y cold:
#  - raw/ (arquivos imutáveis, nunca referenciados por metadado vivo): lifecycle
#    por IDADE do objeto é seguro — Glacier aos 5 anos, expira aos 15.
#  - warehouse/ (Iceberg): lifecycle por idade NÃO serve — um data file antigo
#    continua referenciado pelo metadado atual da tabela; transicioná-lo quebra a
#    leitura (InvalidObjectState) e expirá-lo corrompe a tabela. A retenção do
#    warehouse é do próprio Iceberg (expire_snapshots + remoção de partições
#    antigas na manutenção agendada — docs/arquitetura.md).
resource "aws_s3_bucket_lifecycle_configuration" "retencao" {
  bucket = aws_s3_bucket.dados.id

  rule {
    id     = "raw-5y-hot-10y-cold"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    transition {
      days          = 1825 # 5 anos: hot -> cold
      storage_class = "GLACIER"
    }
    expiration {
      days = 5475 # 15 anos: fim da retenção (5 hot + 10 cold)
    }
  }
}
