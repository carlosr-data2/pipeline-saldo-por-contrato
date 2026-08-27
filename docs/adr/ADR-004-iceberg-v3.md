# ADR-004 — Apache Iceberg V3, com runtime próprio embarcado no Glue

## Contexto
O desafio exige todo dado escrito em **Iceberg V3**. Além do requisito, o caso é
regulatório: auditoria precisa de snapshot/time travel, e a tabela de 5 anos
precisa de evolução de schema e de partição sem reescrita.

## Decisão
Todas as 9 tabelas em **Iceberg, `format-version=3`**, escritas com
`iceberg-spark-runtime-3.5` **1.10.2** + `iceberg-aws-bundle` **1.10.2**,
pinados nas três trilhas (venv local, Docker, Glue). No Glue 5.0, os jars entram
por `--extra-jars` com `--user-jars-first=true` e `--datalake-formats` vazio —
o Iceberg NATIVO do Glue 5.0 é 1.7.x e **não escreve V3**; carregar os dois
causaria conflito de classpath.

## Por que não Parquet puro em S3 (P1.2)
Parquet é o formato de ARQUIVO — o Iceberg é a camada de TABELA sobre ele:
- **Commit atômico**: INSERT OVERWRITE de partição vira troca de snapshot — leitor
  nunca vê estado parcial. Em Parquet puro, overwrite = janela de inconsistência.
- **Time travel/snapshots**: auditoria regulatória ("o que o fechamento viu às
  02h?") de graça; em Parquet puro, seria cópia manual.
- **Evolução de schema e de partição** por metadado, sem reescrever 5 anos.
- **Poda por metadados/estatísticas** (min/max por arquivo) além da poda de partição.
- V3 especificamente: deletion vectors e row lineage — relevantes se a evolução
  for para MERGE (ADR-005).
Custo aceito: manutenção do metadado (compactação, expiração de snapshots) —
`rewrite_data_files`/`expire_snapshots` agendados na evolução de produção.

## Risco V3 e plano B (P1.3)
Risco mapeado: ecossistema de LEITURA de V3 ainda é desigual (ex.: Athena).
Verificação objetiva feita: `format-version: 3` confirmado no `metadata.json` das
9 tabelas, nas trilhas local e Docker (teste automatizado + `make demo`).
Se algum consumidor V3 travar em produção: rebaixar para V2 é decisão de UMA
propriedade de tabela na criação — o pipeline não muda.
