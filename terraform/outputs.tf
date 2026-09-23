output "bucket" {
  value = aws_s3_bucket.dados.id
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.pipeline.arn
}

output "jobs_glue" {
  value = [for job in aws_glue_job.pipeline : job.name]
}

output "origem_bronze" {
  value = "${local.origem_bronze} (formato ${var.origem_formato}, --dt vindo da Step Function)"
}

output "proximos_passos" {
  value = <<-EOT
    1. make aws-publicar-artefatos BUCKET=${aws_s3_bucket.dados.id}   # dados (CSV + Parquet particionado) + jars Iceberg
    2. aws stepfunctions start-execution --state-machine-arn ${aws_sfn_state_machine.pipeline.arn} \
         --input '{"dt": "2026-08-20"}'   # depois 21 e 22, em ordem
    3. Evidências: console Glue (histórico dos jobs), Athena/Glue Catalog (tabelas),
       Cost Explorer (custo real). Detalhes: docs/runbook_aws.md
  EOT
}
