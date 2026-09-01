# ADR-010 — Terraform provisiona a plataforma; as tabelas pertencem ao código

## Contexto: o limite do "tudo como código"

O Terraform deste projeto cobre 100% da arquitetura AWS: S3 com lifecycle,
databases do Glue Data Catalog, os 3 jobs, Step Functions, EventBridge, SNS,
alarmes, IAM, budget. Diante disso, seria natural esperar as tabelas Iceberg
lá também — "infraestrutura como código" não deveria incluir tudo?

A resposta exige separar duas coisas que parecem iguais mas têm **ciclos de vida
diferentes**: a *plataforma* (buckets, jobs, permissões — muda quando a
arquitetura muda) e o *schema dos dados* (colunas, tipos, partições — muda
quando o contrato de dados evolui). Misturar os dois amarra dois ritmos de
mudança independentes.

## Decisão

- **Terraform é dono da plataforma**: databases do Catalog, jobs, orquestração,
  permissões, políticas de ciclo de vida do storage.
- **O código do pipeline é dono das tabelas**: cada job garante o que precisa
  com `CREATE TABLE IF NOT EXISTS` (função `garantir_tabela` em
  `lib/session.py`), com schema, particionamento e propriedades (`format-version=3`)
  declarados **onde o schema do contrato já vive** — em `lib/schema.py`,
  versionado, revisado em PR e coberto por teste.

## Alternativa rejeitada

**Tabelas como recursos Terraform (`aws_glue_catalog_table`).** Três problemas
práticos:

1. **Todo schema evolution viraria um `terraform apply`**: adicionar uma coluna
   ao contrato exigiria um deploy de *infraestrutura*, acoplando o ciclo de dados
   ao ciclo de infra;
2. **Drift permanente**: o metadado de uma tabela Iceberg muda a cada commit de
   dados (snapshots, manifestos, estatísticas). O estado do Terraform ficaria
   eternamente divergente da realidade, e cada `plan` acusaria mudanças que
   ninguém fez;
3. **O recurso do provider não fala Iceberg de verdade**: o metadado real do
   Iceberg vive em arquivos no S3, não no shape do recurso do Catalog; o
   Terraform enxergaria só uma casca.

## Consequências

- É essa separação que faz a **trilha local ser fiel à AWS**: o mesmo
  `garantir_tabela` cria as tabelas no HadoopCatalog do filesystem e no Glue
  Data Catalog. Se as tabelas fossem Terraform, a demo local precisaria de um
  caminho de criação paralelo, e a paridade se perderia.
- A governança de schema fica onde há revisão de código e teste, que é onde
  decisões de schema deveriam ser discutidas.
- O custo assumido: o primeiro run de cada job carrega a responsabilidade de
  criar suas tabelas (por isso o `IF NOT EXISTS` idempotente), e a documentação
  de "quais tabelas existem" vive no código e no `docs/arquitetura.md`, não no
  plan do Terraform.
