# Glue Data Catalog: um database por camada. Os jobs criam e evoluem as TABELAS
# (schema é dono do código — ADR-010); a infra é dona de databases, jobs e permissões.
resource "aws_glue_catalog_database" "camadas" {
  for_each = toset(["bronze", "silver", "gold", "ref"])
  name     = each.key
}

# Artefatos de código enviados pelo próprio Terraform (sempre em dia com o repo).
resource "aws_s3_object" "scripts_jobs" {
  for_each    = toset(["bronze_ingest.py", "silver_quality.py", "gold_saldo.py"])
  bucket      = aws_s3_bucket.dados.id
  key         = "scripts/jobs/${each.key}"
  source      = "${path.module}/../src/jobs/${each.key}"
  source_hash = filemd5("${path.module}/../src/jobs/${each.key}")
}

data "archive_file" "src_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/.artefatos/src.zip"
}

resource "aws_s3_object" "src_zip" {
  bucket      = aws_s3_bucket.dados.id
  key         = "scripts/src.zip"
  source      = data.archive_file.src_zip.output_path
  source_hash = data.archive_file.src_zip.output_md5
}

locals {
  # Glue 5.0 embarca Iceberg 1.7.x, que não escreve V3 — por isso o runtime 1.10.x
  # entra via --extra-jars com --user-jars-first (ADR-004). Os jars são publicados
  # pelo runbook (make aws-publicar-artefatos) por serem binários externos.
  jars_iceberg = join(",", [
    "${local.s3_scripts}/jars/iceberg-spark-runtime-3.5_2.12-${var.iceberg_versao}.jar",
    "${local.s3_scripts}/jars/iceberg-aws-bundle-${var.iceberg_versao}.jar",
  ])

  args_comuns = {
    "--extra-py-files"                   = "s3://${aws_s3_bucket.dados.id}/${aws_s3_object.src_zip.key}"
    "--extra-jars"                       = local.jars_iceberg
    "--user-jars-first"                  = "true"
    "--datalake-formats"                 = "" # NÃO carregar o Iceberg nativo do Glue: conflito de versões
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--SALDO_CATALOGO"                   = "glue_catalog"
    "--SALDO_CATALOGO_IMPL"              = "glue"
    "--SALDO_WAREHOUSE"                  = local.s3_warehouse
    "--SALDO_SHUFFLE_PARTITIONS"         = "16"
  }

  jobs = {
    bronze_ingest = {
      args = {
        "--input" = "${local.s3_raw}/fin_contabilidade_saldo_contrato.csv"
        "--cosif" = "${local.s3_raw}/cosif_dominio.csv"
      }
    }
    silver_quality = { args = {} }
    gold_saldo     = { args = {} }
  }
}

resource "aws_glue_job" "pipeline" {
  for_each = local.jobs

  name              = "${var.prefixo}-${each.key}"
  role_arn          = aws_iam_role.glue_job.arn
  glue_version      = "5.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_numero_workers
  timeout           = var.glue_timeout_minutos

  # Retry pertence SÓ à Step Function (ADR-002): retry em dois níveis multiplica
  # execuções e queima a janela 22h→02h. Concurrency 1 = cinto de segurança da
  # idempotência contra disparo duplo.
  max_retries = 0
  execution_property {
    max_concurrent_runs = 1
  }

  command {
    script_location = "s3://${aws_s3_bucket.dados.id}/scripts/jobs/${each.key}.py"
    python_version  = "3"
  }

  default_arguments = merge(local.args_comuns, each.value.args)
}
