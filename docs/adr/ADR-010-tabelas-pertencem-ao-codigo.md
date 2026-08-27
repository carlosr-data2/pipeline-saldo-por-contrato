# ADR-010 — Terraform provisiona a plataforma; as tabelas pertencem ao código

## Contexto
O Terraform cobre 100% da arquitetura AWS (S3+lifecycle, databases do Catalog,
jobs Glue, Step Functions, EventBridge+DLQ, SNS, alarmes, IAM, budget). Faltava
decidir quem é dono das TABELAS Iceberg.

## Decisão
As tabelas são criadas e evoluídas **pelo pipeline** (`CREATE TABLE IF NOT EXISTS`
em `lib/session.garantir_tabela`), não pelo Terraform. A infra é dona de
databases, jobs, permissões e políticas de ciclo de vida; o código é dono de
schema, particionamento e propriedades de tabela.

## Alternativas rejeitadas
- **Tabelas como recursos Terraform (`aws_glue_catalog_table`)**: todo schema
  evolution viraria um `terraform apply` acoplado ao deploy de infra; o estado do
  Terraform derivaria do metadado do Iceberg (que muda a cada commit de dados) —
  drift permanente. E o recurso do provider não fala Iceberg de verdade (metadado
  do Iceberg fica em arquivos, não no shape do recurso).

## Consequências
- O mesmo código cria as tabelas no HadoopCatalog local e no Glue Catalog — é o
  que faz a demo local ser fiel à AWS (P4.5).
- Governança de schema fica onde há revisão de código e testes — o schema do
  contrato está em `lib/schema.py`, versionado e testado.
