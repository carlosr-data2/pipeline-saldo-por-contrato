# Matriz de rastreabilidade — desafio ⇄ repositório

Mapeamento bidirecional entre o enunciado do desafio e este repositório, **na
ordem e com o texto dos itens tal como aparecem no enunciado** (seções §2.2,
§2.3, §3, §5 e §6). A primeira parte prova **cobertura** — todo requisito tem
solução e forma de verificação; a segunda prova **pertinência** — todo artefato
existe por causa de um requisito.

## Desafio → Repositório

### §2.2 — Volumetria e Requisitos de Performance

| Item do enunciado | Onde é tratado | Como verificar |
|---|---|---|
| "Contas ativas ~80 milhões" / "Contratos por conta (média) 3 a 5" / "Transações diárias ~300 milhões" | raciocínio de escala e dimensionamento em `docs/arquitetura.md` (seção "Escala de produção"); saldo incremental O(dia) que faz o volume caber no SLA (ADR-005) | contas de sizing: 20×G.2X → 15–25 min por estágio crítico |
| "SLA de processamento < 1 horas (D+0 22h → D+1 02h)" | leitura do SLA em camadas (execução <1h; janela = orçamento de retentativas) em `docs/arquitetura.md`; timeout < SLA e retries centralizados (`terraform/glue.tf`, `pipeline.asl.json`, ADR-002) | seção "Leitura do SLA"; `terraform plan` mostra timeout e `MaxRetries=0` |
| "Janela de fechamento contábil até 06:00 D+1" | contingência 02h–06h no playbook de falha (redrive + guarda de continuidade `SnapshotDescontinuo`) | `docs/arquitetura.md`; `test_gold_recusa_pular_dia_publicado_sem_snapshot` |
| "Retenção histórica 5 anos (hot) + 10 anos (cold)" | lifecycle S3 por idade no `raw/`; no warehouse Iceberg, retenção por partição via manutenção (lifecycle por idade quebraria tabelas vivas) — `terraform/s3.tf` | comentários em `s3.tf`; `docs/arquitetura.md` (seção "Retenção") |

### §2.3 — Contrato de Dados

| Item do enunciado | Onde é tratado | Como verificar |
|---|---|---|
| Schema do contrato (Bronze — Ingestão): 12 campos tipados, todos NOT NULL | `src/lib/schema.py` (`CAMPOS_CONTRATO`, `tipar_contrato`) — o contrato como código | `tests/test_schema.py`; tipos conferidos no Bronze |
| "Formato de origem: Parquet (particionado por data de processamento)" | leitor de origem em `bronze_ingest.py` (CSV conforme entregue — ver §6); partição alvo `dt_processamento` implementada | log `bronze_concluido`: 3 partições de 66.666 |

**Regras de Qualidade (Data Quality)** — implementadas em `src/lib/dq.py` (motivos nomeados, quarentena sem descarte):

| Item do enunciado (texto exato) | Onde é tratado | Como verificar |
|---|---|---|
| "id_transacao deve ser único globalmente" | regra R5 + `src/lib/dedup.py` — unicidade determinística sobre o publicado, com lookback; inviabilidade do "global literal" a 300M/dia documentada e devolvida ao owner (ADR-006) | 5 testes de dedup em `tests/test_dq.py`; motivos `ID_TRANSACAO_DUPLICADO_NO_LOTE`/`JA_PROCESSADO`; 3.276 duplicatas 100% explicadas pelo oráculo |
| "valor_lancamento > 0 (estornos usam flag_estorno = true)" | regra R2; semântica do estorno (inverte sinal) em `src/lib/saldo.py` (ADR-007) | `test_valor_nao_positivo`, `test_estorno_nao_e_violacao`; 3.989 na quarentena |
| "dt_lancamento <= dt_processamento" | regra R3 | `test_data_lancamento_posterior_ao_processamento`; 3.257 na quarentena |
| "cod_cosif deve existir na tabela de domínio COSIF (referencial)" | regra R4, join broadcast do referencial (`ref.cosif_dominio` ingerido no Bronze) | `test_cosif_fora_do_dominio`; 3.309 na quarentena |
| "Completude: campos NOT NULL não podem conter valores vazios ou nulos" | regra R1, motivo nomeado por campo | `test_campo_obrigatorio_nulo`; 2.411 `id_conta` na quarentena |

**Output Esperado (Gold)** — produzido por `src/jobs/gold_saldo.py` + `src/lib/saldo.py`:

| Item do enunciado (texto exato) | Onde é tratado | Como verificar |
|---|---|---|
| "Saldo consolidado por contrato (id_contrato) com data de referência" | `gold.saldo_contrato_diario` — snapshot diário incremental (ADR-005) | oráculo compara os 95.200 saldos ao centavo (`test_oraculo_dados_reais.py`) |
| "Saldo consolidado por conta (id_conta) agregando todos os contratos" | `gold.saldo_conta_diario` (`saldo_por_conta`) | oráculo confere conta a conta (46.287 contas) |
| "Classificação contábil COSIF por tipo de contrato" | `gold.classificacao_cosif` (`classificacao_cosif` + `flag_coerente`, ADR-008) | tabela no relatório da demo |
| "Métricas de reconciliação: total de débitos vs. créditos por agência" | `gold.reconciliacao_agencia` (`reconciliacao_por_agencia`) + controle cruzado que bloqueia publicação divergente | evento `reconciliacao_cruzada` com `divergencia: 0.0`; 46 agências conferidas pelo oráculo |
| "Todo o dado deve ser escrito com Apache Iceberg V3" | `garantir_tabela` com `format-version=3`; runtime 1.10.2 pinado nas 3 trilhas (ADR-004) | rodapé do `make demo` valida o `metadata.json` das 9 tabelas; teste em `test_e2e.py` |

### §3 — Requisitos Técnicos do Desafio

| Item do enunciado (texto exato) | Onde é tratado | Como verificar |
|---|---|---|
| "pipeline de processamento de dados utilizando Apache Spark como engine, executado no serviço AWS Glue" | 3 jobs PySpark (sem GlueContext — portáveis) provisionados como Glue Jobs 5.0 em `terraform/glue.tf` | `terraform plan`; scripts publicados no S3 pelo Terraform |
| "Utilizar PySpark ou Scala (justificar a escolha da linguagem)" | PySpark, API DataFrame sem UDFs | `docs/adr/ADR-011-pyspark.md` (com a alternativa Scala rejeitada e o critério) |
| "Implementar jobs Glue com configuração adequada de workers, timeout e retries" | `terraform/glue.tf` + `variables.tf`: G.1X×2, timeout 15 min, `MaxRetries=0` + `MaxConcurrentRuns=1`; retries centralizados na Step Function (ADR-002); dimensionamento de produção em `docs/arquitetura.md` | `terraform plan`; comentários justificando cada valor |
| "Demonstrar conhecimento de otimizações Spark (partitioning, caching, broadcast joins)" | *partitioning*: todas as tabelas por `dt_processamento`/`dt_referencia` + poda em toda leitura; *caching*: `persist()` justificado em `silver_quality.py` e `gold_saldo.py`; *broadcast*: `F.broadcast` explícito do referencial em `dq.py` e `saldo.py` | grep pelos três no código; discussão de limites (quando broadcast atrapalha, skew/AQE) em `docs/arquitetura.md` |
| "Implementar tratamento de erros e logging estruturado" | `src/lib/log.py` (JSON por evento, duração monotônica); exceções tipadas (`GateReprovado`, `ReconciliacaoDivergente`, `SnapshotDescontinuo`); validação de schema na ingestão | logs da demo parseáveis com `jq`; eventos `etapa_erro`/`job_falhou` |
| "Utilizar Glue Data Catalog para gerenciamento de metadados" | catálogo por configuração em `src/lib/session.py`: GlueCatalog na AWS, HadoopCatalog como equivalente local (ADR-012); databases via Terraform, tabelas via código (ADR-010) | mesma demo nas duas trilhas; databases no console AWS |
| "Considerar estratégias de re-processamento (idempotência)" | INSERT OVERWRITE dinâmico de partição; `escrever_particao` (caso do DataFrame vazio); dedup determinística; guarda de continuidade | `test_reprocessamento_e_idempotente`; reexecução ao vivo na demo com resultado idêntico |
| "Apresentar desenho de arquitetura de solução" | diagrama mermaid no `README.md` + `docs/arquitetura.md` completo | — |

### §5 — Prazo e Entregáveis

| Item do enunciado | Onde é tratado | Como verificar |
|---|---|---|
| "Formato de entrega: arquivo ZIP" | `scripts/package_zip.sh` (git archive; confere limite de anexo de e-mail) | `./scripts/package_zip.sh` |
| "Demonstrar execução do pipeline (local com docker ou em conta AWS)" | as duas trilhas: `make demo` (Docker, 1 comando, sem AWS) e `terraform/` + `docs/runbook_aws.md` (com estimativa de custo prévia); execução real na conta AWS documentada em `docs/evidencias.md` | executar `make demo`; capturas em `docs/evidencias/` |
| "Saber explicar em detalhes cada decisão arquitetural" / "Justificar os trade-offs" / "propor alternativas" | 13 ADRs, todos com decisão, alternativa rejeitada e critério | `docs/adr/` |

### §6 — Conjuntos de Dados Disponibilizados

| Item do enunciado | Onde é tratado | Como verificar |
|---|---|---|
| "entregue como um arquivo CSV único e não particionado (...) ingerir os dados, particioná-los por dt_processamento e escrevê-los no formato de tabela adequado faz parte do trabalho do candidato" | `src/jobs/bronze_ingest.py`: CSV → tipagem → Iceberg V3 particionado | log `bronze_concluido`: 199.998 linhas, 3 partições |
| "Os campos são entregues como texto (a tipagem e a validação são responsabilidade do candidato)" | `tipar_contrato` (trim, vazio→NULL, cast sem descarte) + regras de validação no Silver | `tests/test_schema.py` |
| "O dataset contém propositalmente registros que violam as regras de qualidade (...) a fim de exercitar o tratamento de qualidade de dados" | quarentena com `motivos[]`, relatório `dq_relatorio` e gate (ADR-009); invariante bronze = silver + quarentena | relatório da demo: ~8% de quarentena decomposta por motivo, batendo com oráculo independente |
| "cosif_dominio.csv — tabela de domínio (...) utilizada para validar o campo cod_cosif e para a classificação contábil na camada Gold" | ingerido como `ref.cosif_dominio`; usado na regra R4 e na `classificacao_cosif` | tabela `ref` com 8 códigos; joins broadcast |
| "O candidato deve raciocinar e descrever como a solução escala para a volumetria de produção" | `docs/arquitetura.md` (seção "Escala de produção: ~300M transações/dia") | contas de sizing, custo estimado e mitigação de skew |

## Repositório → Desafio (por que cada artefato existe)

| Artefato | Existe para atender |
|---|---|
| `src/jobs/bronze_ingest.py` | §6 (CSV → tipagem → partição) e §2.3 schema do contrato |
| `src/jobs/silver_quality.py` | §2.3 Regras de Qualidade (as 5) + §6 tratamento das violações; gate de fechamento |
| `src/jobs/gold_saldo.py` | §2.3 Output Esperado (as 4 saídas) + §3 idempotência |
| `src/lib/schema.py` | §2.3 schema + §6 tipagem como responsabilidade do candidato |
| `src/lib/dq.py` | §2.3 Regras de Qualidade, com motivos e broadcast do referencial |
| `src/lib/dedup.py` | §2.3 "id_transacao deve ser único globalmente" |
| `src/lib/saldo.py` | §2.3 Output Esperado: sinal/estorno, incremental, agregações |
| `src/lib/session.py` | §2.3 Iceberg V3 + §3 Data Catalog + otimizações de sessão (AQE/skew) |
| `src/lib/config.py` | mesmos jobs nas 3 trilhas (env local, argumentos do Glue) — §3 e §5 |
| `src/lib/log.py` | §3 "tratamento de erros e logging estruturado" |
| `tests/` (28 testes) | prova executável de §2.3 e §3 (regras, idempotência, gate, V3) |
| `tests/oraculo.py` | correção dos outputs de §2.3 por dupla implementação independente |
| `terraform/` (35 recursos) | §3 Spark no Glue + workers/timeout/retries + Catalog; §2.2 retenção; operação (orquestração, alarmes, IAM, budget) |
| `terraform/templates/pipeline.asl.json` | §2.2 SLA/janela: retries centralizados e retomada (redrive) |
| `docker/` + `docker-compose.yml` + `Makefile` | §5 "demonstrar execução (...) local com docker" — 1 comando, paridade com Glue 5.0 |
| `scripts/demo.sh` + `scripts/relatorio_demo.py` | §5 demonstração + evidência do V3 tabela a tabela (§2.3) |
| `scripts/package_zip.sh` | §5 "formato de entrega: arquivo ZIP" |
| `docs/adr/` (13 ADRs) | §4/§5: decisões explicáveis, trade-offs e alternativas |
| `docs/arquitetura.md` | §3 desenho de arquitetura + §2.2/§6 raciocínio de escala |
| `docs/runbook_aws.md` | §5 demonstração "em conta AWS", com custo estimado antes de cada execução |
| `dados/` | §6 conjuntos disponibilizados, versionados para clone-and-run (ADR-013) |
| `.github/workflows/ci.yml` | qualidade contínua das provas acima (lint + suíte + validação do Terraform) |
