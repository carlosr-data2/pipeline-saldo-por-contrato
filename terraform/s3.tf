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

# Retenção regulatória: 5 anos hot (S3 Standard/IA) + 10 anos cold (Glacier), depois expira.
resource "aws_s3_bucket_lifecycle_configuration" "retencao" {
  bucket = aws_s3_bucket.dados.id

  rule {
    id     = "retencao-5y-hot-10y-cold"
    status = "Enabled"
    filter {
      prefix = "warehouse/"
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
