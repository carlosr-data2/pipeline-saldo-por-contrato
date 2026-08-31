# Matriz de rastreabilidade — desafio ⇄ repositório

Mapeamento bidirecional entre os requisitos do desafio e os artefatos deste
repositório: a primeira tabela prova **cobertura** (todo requisito tem solução e
verificação); a segunda prova **pertinência** (todo artefato existe por um
requisito — nada é enfeite).

## Desafio → Repositório (onde cada requisito é resolvido)

### Saídas de negócio (Gold)

| Requisito | Onde é resolvido | Como verificar |
|---|---|---|
| Saldo consolidado diário por contrato, com data de referência | `src/jobs/gold_saldo.py` + `src/lib/saldo.py` (`snapshot_saldo`) — snapshot diário incremental, ADR-005 | `make demo` → tabela `gold.saldo_contrato_diario`; `tests/test_oraculo_dados_reais.py` compara os 95.200 saldos ao centavo |
| Saldo consolidado por conta, agregando contratos | `src/lib/saldo.py` (`saldo_por_conta`) | tabela `gold.saldo_conta_diario`; oráculo confere conta a conta |
| Classificação contábil COSIF por tipo de contrato | `src/lib/saldo.py` (`classificacao_cosif`) + `flag_coerente` (ADR-008) | tabela `gold.classificacao_cosif` no relatório da demo |
| Reconciliação: débitos vs. créditos por agência | `src/lib/saldo.py` (`reconciliacao_por_agencia`) + controle cruzado no gold | tabela `gold.reconciliacao_agencia`; evento `reconciliacao_cruzada` com `divergencia: 0.0` |
| Todo dado escrito em Apache Iceberg **V3** | `src/lib/session.py` (`garantir_tabela`, `format-version=3`) + jars 1.10.2 pinados (ADR-004) | rodapé do `make demo` valida o `metadata.json` das 9 tabelas; teste em `tests/test_e2e.py` |

### Ingestão e qualidade

| Requisito | Onde é resolvido | Como verificar |
|---|---|---|
| Bronze a partir do contrato: CSV único → tipagem → partição por `dt_processamento` | `src/jobs/bronze_ingest.py` + `src/lib/schema.py` (`tipar_contrato`) | log `bronze_concluido`: 199.998 linhas, 3 partições de 66.666; `tests/test_schema.py` |
| DQ: `id_transacao` único globalmente | `src/lib/dedup.py` + regra R5 em `src/lib/dq.py` — política "unicidade sobre o publicado" com lookback (ADR-006) | `tests/test_dq.py` (5 casos de dedup); motivos `DUPLICADO_NO_LOTE`/`JA_PROCESSADO` no relatório |
| DQ: `valor_lancamento > 0` (estorno é flag) | regra R2 em `src/lib/dq.py`; semântica do estorno em `src/lib/saldo.py` (ADR-007) | `test_valor_nao_positivo`, `test_estorno_nao_e_violacao` |
| DQ: `dt_lancamento <= dt_processamento` | regra R3 em `src/lib/dq.py` | `test_data_lancamento_posterior_ao_processamento` |
| DQ: `cod_cosif` existe no domínio | regra R4 em `src/lib/dq.py` (broadcast do referencial) | `test_cosif_fora_do_dominio`; 3.309 na quarentena |
| DQ: completude dos campos NOT NULL | regra R1 em `src/lib/dq.py` (por campo, com motivo nomeado) | `test_campo_obrigatorio_nulo`; 2.411 `id_conta` nulos na quarentena |
| Tratamento dos registros que violam regras | quarentena com `motivos[]` (nunca descarte) + relatório `dq_relatorio` + **gate** em `src/jobs/silver_quality.py` (ADR-009) | invariante bronze = silver + quarentena; `test_gate_bloqueia_fechamento_com_qualidade_ruim` |

### Requisitos técnicos

| Requisito | Onde é resolvido | Como verificar |
|---|---|---|
| Pipeline Spark executado no AWS Glue | `terraform/glue.tf` (3 jobs Glue 5.0) + jobs PySpark puros sem GlueContext (portáveis) | `terraform plan`; scripts publicados no S3 pelo próprio Terraform |
| PySpark ou Scala, com justificativa | PySpark, API DataFrame sem UDFs — ADR-011 | `docs/adr/ADR-011-pyspark.md` |
| Configuração de workers, timeout e retries | `terraform/glue.tf` + `variables.tf` (G.1X×2, timeout 15, `MaxRetries=0`, `MaxConcurrentRuns=1`); produção em `docs/arquitetura.md` (P3.5) | `terraform plan`; ADR-002 (retry só na Step Function) |
| Otimização: particionamento | partição `dt_processamento`/`dt_referencia` em todas as tabelas + poda de partição em toda leitura | DDLs em `garantir_tabela`; filtros nos jobs |
| Otimização: broadcast join | `F.broadcast` explícito do domínio COSIF em `src/lib/dq.py` e `src/lib/saldo.py` | grep `F.broadcast`; discussão de quando atrapalha em `docs/arquitetura.md` |
| Otimização: cache | `persist()`/`unpersist()` em `silver_quality.py` (regras → 3 escritas) e `gold_saldo.py` (silver → 4 saídas) | comentários no código explicam o porquê de cada um |
| Tratamento de erros e logging estruturado | `src/lib/log.py` (JSON por evento, duração monotônica); exceções tipadas (`GateReprovado`, `ReconciliacaoDivergente`, `SnapshotDescontinuo`); validação de schema na ingestão | logs da demo parseáveis com `jq`; eventos `etapa_erro`/`job_falhou` |
| Glue Data Catalog (e equivalente local) | catálogo por configuração em `src/lib/session.py`: GlueCatalog na AWS, HadoopCatalog local (ADR-012); databases no Terraform, tabelas no código (ADR-010) | mesma demo nas duas trilhas; databases visíveis no console |
| Reprocessamento idempotente | INSERT OVERWRITE dinâmico de partição; `escrever_particao` (caso do DataFrame vazio); dedup determinística; guarda de continuidade no gold | `test_reprocessamento_e_idempotente`, `test_gold_recusa_pular_dia_publicado_sem_snapshot`; reexecução ao vivo na demo |
| Desenho de arquitetura | diagrama no `README.md` + `docs/arquitetura.md` (fluxo, camadas, SLA, observabilidade) | — |
| Escala para ~300M transações/dia e ~80M contas no SLA | saldo incremental O(dia) (ADR-005) + dimensionamento e contas em `docs/arquitetura.md` | seção "Escala de produção" |
| Retenção 5 anos hot + 10 anos cold | lifecycle S3 no `raw/` + retenção por partição via manutenção Iceberg no warehouse (`terraform/s3.tf`) | comentários em `s3.tf`; `docs/arquitetura.md` |
| Demonstração: local com Docker **ou** conta AWS | as duas: `make demo` (Docker, 1 comando, sem AWS) e `terraform/` + `docs/runbook_aws.md` | executar `make demo`; runbook com custo estimado |
| Entrega em ZIP | `scripts/package_zip.sh` (git archive, confere limite de e-mail) | `./scripts/package_zip.sh` |
| Decisões arquiteturais defensáveis, com trade-offs e alternativas | 13 ADRs, todos com alternativa rejeitada e critério | `docs/adr/` |

## Repositório → Desafio (por que cada artefato existe)

| Artefato | Existe para atender |
|---|---|
| `src/jobs/bronze_ingest.py` | Bronze do contrato (tipagem + partição) e ingestão do referencial COSIF |
| `src/jobs/silver_quality.py` | as 5 regras de DQ, quarentena com motivos, relatório e gate de fechamento |
| `src/jobs/gold_saldo.py` | as 4 saídas Gold + idempotência + controle de consistência + guarda de continuidade |
| `src/lib/schema.py` | o schema do contrato como código (tipagem sem descarte) |
| `src/lib/dq.py` | as 5 regras com motivos; broadcast do referencial |
| `src/lib/dedup.py` | a regra de unicidade — determinística e auditável |
| `src/lib/saldo.py` | semântica de negócio: sinal/estorno, incremental, agregações |
| `src/lib/session.py` | Iceberg V3 + Data Catalog por configuração (Glue/local); AQE e skew join |
| `src/lib/config.py` | mesmos jobs nas 3 trilhas (env local, argv do Glue) |
| `src/lib/log.py` | logging estruturado exigido no enunciado |
| `tests/` (28 testes) | prova executável de cada regra, idempotência, gate e V3 |
| `tests/oraculo.py` | correção do saldo por dupla implementação (independente de Spark) |
| `terraform/` (35 recursos) | Spark no Glue, catálogo, orquestração, alarmes, IAM, budget — IaC de ponta a ponta |
| `terraform/templates/pipeline.asl.json` | orquestração com retries centralizados e retomada (redrive) |
| `docker/` + `docker-compose.yml` + `Makefile` | demonstração local em 1 comando, com paridade de motor com o Glue 5.0 |
| `scripts/demo.sh` + `scripts/relatorio_demo.py` | execução de ponta a ponta e evidência do V3 tabela a tabela |
| `scripts/package_zip.sh` | o formato de entrega |
| `docs/adr/` (13 ADRs) | decisão + alternativa rejeitada + critério — o coração da avaliação |
| `docs/arquitetura.md` | desenho, leitura do SLA e raciocínio de escala para produção |
| `docs/runbook_aws.md` | trilha AWS com estimativa de custo antes de cada execução |
| `dados/` | dataset do caso versionado para clone-and-run (ADR-013) |
| `.github/workflows/ci.yml` | qualidade contínua: lint, suíte completa e validação do Terraform a cada push |
