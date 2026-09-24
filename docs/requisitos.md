# Requisitos ⇄ repositório

Este documento mapeia cada requisito do projeto para o lugar onde ele é tratado e a forma de verificar, e, no sentido inverso, cada artefato do repositório para o requisito que o justifica.

## Requisitos → repositório

### Volumetria, SLA e retenção

| Requisito | Onde é tratado | Como verificar |
|---|---|---|
| Escala de produção: ~80 milhões de contas, 3 a 5 contratos por conta, ~300 milhões de transações por dia | raciocínio de escala e dimensionamento em `docs/arquitetura.md` (seção "Escala de produção"); saldo incremental O(dia) que faz o volume caber no SLA (ADR-005) | contas de sizing: 20×G.2X → 15–25 min por estágio crítico |
| Processamento em menos de 1 hora, na janela de 22h (D+0) a 02h (D+1) | leitura do SLA em camadas (execução <1h; janela = orçamento de retentativas) em `docs/arquitetura.md`; timeout < SLA e retries centralizados (`terraform/glue.tf`, `pipeline.asl.json`, ADR-002) | seção "Leitura do SLA"; `terraform plan` mostra timeout e `MaxRetries=0` |
| Fechamento contábil até 06:00 de D+1 | contingência 02h–06h no playbook de falha (redrive + guarda de continuidade `SnapshotDescontinuo`) | `docs/arquitetura.md`; `test_gold_recusa_pular_dia_publicado_sem_snapshot` |
| Retenção de 5 anos hot e 10 anos cold | lifecycle S3 por idade no `raw/`; no warehouse Iceberg, retenção por partição via manutenção (lifecycle por idade quebraria tabelas vivas), em `terraform/s3.tf` | comentários em `s3.tf`; `docs/arquitetura.md` (seção "Retenção") |

### Dados e formato

| Requisito | Onde é tratado | Como verificar |
|---|---|---|
| Schema do contrato na ingestão: 12 campos tipados, todos obrigatórios | `src/lib/schema.py` (`CAMPOS_CONTRATO`, `tipar_contrato`): o contrato como código | `tests/test_schema.py`; tipos conferidos no Bronze |
| Origem em Parquet, particionada por data de processamento | `bronze_ingest.py` lê a origem nesse layout (`--formato parquet`, diretório particionado por `dt_processamento`, gerado por `scripts/csv_para_parquet_particionado.py`) ou o CSV único de exemplo; com `--dt`, ingere e sobrescreve só a partição do dia (ADR-014) | `tests/test_bronze.py`; log `bronze_commit` com `changed-partition-count = 1`; log `bronze_concluido`: 3 partições de 66.666 |
| Ingerir um CSV único e não particionado, particionar por `dt_processamento` e gravar em formato de tabela | `src/jobs/bronze_ingest.py`: CSV → tipagem → Iceberg V3 particionado | log `bronze_concluido`: 199.998 linhas, 3 partições |
| Campos chegam como texto; tipagem e validação ficam no pipeline | `tipar_contrato` (trim, vazio→NULL, cast sem descarte) + regras de validação no Silver | `tests/test_schema.py` |
| Referencial COSIF para validar `cod_cosif` e classificar na camada Gold | ingerido como `ref.cosif_dominio`; usado na regra R4 e na `classificacao_cosif` | tabela `ref` com 8 códigos; joins broadcast |
| Todo o dado gravado em Apache Iceberg V3 | `garantir_tabela` com `format-version=3`; runtime 1.10.2 pinado nas 3 trilhas (ADR-004) | rodapé do `make demo` valida o `metadata.json` das 9 tabelas; teste em `test_e2e.py` |

### Qualidade

Regras implementadas em `src/lib/dq.py` (motivos nomeados, quarentena sem descarte).

| Requisito | Onde é tratado | Como verificar |
|---|---|---|
| `id_transacao` único | regra R5 + `src/lib/dedup.py`: unicidade determinística sobre o publicado, com lookback; a inviabilidade da unicidade global literal a 300M/dia está documentada e devolvida ao owner (ADR-006) | 5 testes de dedup em `tests/test_dq.py`; motivos `ID_TRANSACAO_DUPLICADO_NO_LOTE`/`JA_PROCESSADO`; 3.276 duplicatas 100% explicadas pelo oráculo |
| `valor_lancamento` positivo; estorno indicado por `flag_estorno` | regra R2; semântica do estorno (inverte sinal) em `src/lib/saldo.py` (ADR-007) | `test_valor_nao_positivo`, `test_estorno_nao_e_violacao`; 3.989 na quarentena |
| `dt_lancamento` não posterior a `dt_processamento` | regra R3 | `test_data_lancamento_posterior_ao_processamento`; 3.257 na quarentena |
| `cod_cosif` presente no domínio COSIF | regra R4, join broadcast do referencial (`ref.cosif_dominio` ingerido no Bronze) | `test_cosif_fora_do_dominio`; 3.309 na quarentena |
| Campos obrigatórios sem nulos ou vazios | regra R1, motivo nomeado por campo | `test_campo_obrigatorio_nulo`; 2.411 `id_conta` na quarentena |
| Tratar os registros inválidos presentes no dataset de exemplo | quarentena com `motivos[]`, relatório `dq_relatorio` e gate (ADR-009); invariante bronze = silver + quarentena | relatório da demo: ~8% de quarentena decomposta por motivo, batendo com oráculo independente |

### Saídas (Gold)

Produzidas por `src/jobs/gold_saldo.py` + `src/lib/saldo.py`.

| Requisito | Onde é tratado | Como verificar |
|---|---|---|
| Saldo consolidado por contrato, com data de referência | `gold.saldo_contrato_diario`: snapshot diário incremental (ADR-005) | oráculo compara os 95.200 saldos ao centavo (`test_oraculo_dados_reais.py`) |
| Saldo consolidado por conta, somando os contratos | `gold.saldo_conta_diario` (`saldo_por_conta`) | oráculo confere conta a conta (46.287 contas) |
| Classificação contábil COSIF por tipo de contrato | `gold.classificacao_cosif` (`classificacao_cosif` + `flag_coerente`, ADR-008) | tabela no relatório da demo |
| Reconciliação de débitos e créditos por agência | `gold.reconciliacao_agencia` (`reconciliacao_por_agencia`) + controle cruzado que bloqueia publicação divergente | evento `reconciliacao_cruzada` com `divergencia: 0.0`; 46 agências conferidas pelo oráculo |

### Processamento

| Requisito | Onde é tratado | Como verificar |
|---|---|---|
| Spark como engine, executado no AWS Glue | 3 jobs PySpark (sem GlueContext, portáveis) provisionados como Glue Jobs 5.0 em `terraform/glue.tf` | `terraform plan`; scripts publicados no S3 pelo Terraform |
| Escolha da linguagem justificada | PySpark, API DataFrame sem UDFs | `docs/adr/ADR-011-pyspark.md` (com a alternativa Scala rejeitada e o critério) |
| Workers, timeout e retries configurados | `terraform/glue.tf` + `variables.tf`: G.1X×2, timeout 15 min, `MaxRetries=0` + `MaxConcurrentRuns=1`; retries centralizados na Step Function (ADR-002); dimensionamento de produção em `docs/arquitetura.md` | `terraform plan`; comentários justificando cada valor |
| Otimizações Spark: particionamento, cache e broadcast | particionamento: todas as tabelas por `dt_processamento`/`dt_referencia` + poda em toda leitura; cache: `persist()` justificado em `silver_quality.py` e `gold_saldo.py`; broadcast: `F.broadcast` explícito do referencial em `dq.py` e `saldo.py` | grep pelos três no código; discussão de limites (quando broadcast atrapalha, skew/AQE) em `docs/arquitetura.md` |
| Tratamento de erros e logging estruturado | `src/lib/log.py` (JSON por evento, duração monotônica); exceções tipadas (`GateReprovado`, `ReconciliacaoDivergente`, `SnapshotDescontinuo`); validação de schema na ingestão | logs da demo parseáveis com `jq`; eventos `etapa_erro`/`job_falhou` |
| Metadados no Glue Data Catalog | catálogo por configuração em `src/lib/session.py`: GlueCatalog na AWS, HadoopCatalog como equivalente local (ADR-012); databases via Terraform, tabelas via código (ADR-010) | mesma demo nas duas trilhas; databases no console AWS |
| Reprocessamento idempotente | INSERT OVERWRITE dinâmico de partição; `escrever_particao` (caso do DataFrame vazio); dedup determinística; guarda de continuidade | `test_reprocessamento_e_idempotente`; reexecução de um dia com resultado idêntico |

### Arquitetura e operação

| Requisito | Onde é tratado | Como verificar |
|---|---|---|
| Desenho de arquitetura documentado | diagrama mermaid no `README.md` + `docs/arquitetura.md` completo | leitura dos dois documentos |
| Execução do pipeline local (Docker) ou em conta AWS | as duas trilhas: `make demo` (Docker, 1 comando, sem AWS) e `terraform/` + `docs/runbook_aws.md` (com estimativa de custo prévia); execução real na conta AWS documentada em `docs/evidencias.md` | executar `make demo`; capturas em `docs/evidencias/` |
| Decisões de arquitetura com trade-offs e alternativas | 14 ADRs, todos com decisão, alternativa rejeitada e critério | `docs/adr/` |

### Escala

| Requisito | Onde é tratado | Como verificar |
|---|---|---|
| Descrever como a solução escala para o volume de produção | `docs/arquitetura.md` (seção "Escala de produção: ~300M transações/dia"); vazão medida no Glue com dataset sintético em `docs/labs_aws.md` | contas de sizing, custo estimado e mitigação de skew; tabelas de vazão em `docs/labs_aws.md` |

## Repositório → requisitos (por que cada artefato existe)

| Artefato | Existe para atender |
|---|---|
| `src/jobs/bronze_ingest.py` | ingestão da origem (Parquet particionado ou CSV), tipagem, partição do dia e schema do contrato |
| `src/jobs/silver_quality.py` | as 5 regras de qualidade e o tratamento das violações; gate de fechamento |
| `src/jobs/gold_saldo.py` | as 4 saídas Gold e o reprocessamento idempotente |
| `src/lib/schema.py` | schema do contrato e tipagem dos campos que chegam como texto |
| `src/lib/dq.py` | regras de qualidade, com motivos e broadcast do referencial |
| `src/lib/dedup.py` | unicidade de `id_transacao` |
| `src/lib/saldo.py` | saídas Gold: sinal/estorno, cálculo incremental, agregações |
| `src/lib/session.py` | Iceberg V3, Glue Data Catalog e otimizações de sessão (AQE/skew) |
| `src/lib/config.py` | mesmos jobs nas 3 trilhas (env local, argumentos do Glue) |
| `src/lib/log.py` | tratamento de erros e logging estruturado |
| `tests/` (33 testes) | prova executável das regras, da idempotência, do gate, do V3, do Bronze por dia e do gerador sintético |
| `tests/oraculo.py` | correção das saídas Gold por dupla implementação independente |
| `terraform/` (35 recursos) | Spark no Glue com workers/timeout/retries e Data Catalog; retenção; operação (orquestração, alarmes, IAM, budget) |
| `terraform/templates/pipeline.asl.json` | SLA e janela: retries centralizados e retomada (redrive) |
| `docker/` + `docker-compose.yml` + `Makefile` | execução local com Docker em 1 comando, com paridade com o Glue 5.0 |
| `scripts/demo.sh` + `scripts/relatorio_demo.py` | execução local de ponta a ponta + evidência do V3 tabela a tabela |
| `scripts/csv_para_parquet_particionado.py` | origem no layout do contrato: Parquet particionado por `dt_processamento` (ADR-014) |
| `scripts/gerar_dataset_sintetico.py` + `scripts/medir_vazao_glue.py` | escala para o volume de produção, com vazão medida em vez de estimada (`docs/labs_aws.md`) |
| `docs/adr/` (14 ADRs) | decisões explicáveis, com trade-offs e alternativas |
| `docs/arquitetura.md` | desenho de arquitetura e raciocínio de escala |
| `docs/runbook_aws.md` | execução em conta AWS, com custo estimado antes de cada execução |
| `docs/labs_aws.md` | idempotência, reprocessamento e escala exercitados na conta AWS real |
| `dados/` | dataset de exemplo (simulado) e referencial COSIF, versionados para clone-and-run (ADR-013) |
| `.github/workflows/ci.yml` | qualidade contínua das provas acima (lint + suíte + validação do Terraform) |
