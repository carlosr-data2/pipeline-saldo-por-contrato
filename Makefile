# Trilha local (Docker): pipeline de ponta a ponta sem AWS.
#   make build   constrói a imagem (Spark 3.5.4 + Java 17 + Iceberg 1.10.2)
#   make demo    executa o pipeline completo (3 dias) e imprime o relatório
#   make test    roda a suíte de testes dentro do container
#   make lint    roda o ruff dentro do container
#   make shell   abre um shell no container
#   make limpar  remove o warehouse local (dados derivados)

COMPOSE := docker compose

.PHONY: build demo relatorio test lint shell limpar relogio

# Origem da demo: csv (CSV único, um Bronze para os três dias, como no dataset de exemplo)
# ou parquet (converte para Parquet particionado e roda o Bronze por dia, ADR-014).
ORIGEM ?= csv

build:
	$(COMPOSE) build

demo: build
	$(COMPOSE) run --rm -e ORIGEM=$(ORIGEM) pipeline bash scripts/demo.sh

# Mostra o estado final do warehouse já processado (~30s), sem reprocessar nada.
# Com o warehouse já processado pelo `make demo`, mostra o resultado sem rodar o
# pipeline de novo; para provar idempotência, reexecute um único dia.
relatorio:
	$(COMPOSE) run --rm pipeline python scripts/relatorio_demo.py

test: build
	$(COMPOSE) run --rm pipeline python -m pytest -q

lint: build
	$(COMPOSE) run --rm pipeline ruff check src tests

shell: build
	$(COMPOSE) run --rm pipeline bash

# roda dentro do container: os arquivos do warehouse são criados pelo root do
# container via bind mount; apagar no host falharia com Permission denied
limpar:
	$(COMPOSE) run --rm pipeline rm -rf warehouse logs

# WSL2: após hibernação do Windows, o relógio da VM pode divergir do host.
# O Iceberg valida timestamps monotônicos nos commits de metadado e recusa
# escrever com relógio inconsistente ("Invalid update timestamp ..."). Rodar
# este alvo (ou `wsl --shutdown` no PowerShell) antes da demo resolve.
relogio:
	sudo hwclock -s
	date

# Trilha AWS, depois de `terraform apply` (ver docs/runbook_aws.md):
#   make aws-publicar-artefatos BUCKET=<saida `bucket` do terraform>
# Sobe os dados de origem (CSV + Parquet particionado por dt_processamento, o formato
# do contrato, ADR-014) e os jars do Iceberg; scripts e src.zip o Terraform já sobe.

ICEBERG_VERSAO := 1.10.2
MAVEN := https://repo1.maven.org/maven2/org/apache/iceberg

.PHONY: baixar-jars aws-publicar-artefatos

# --fail: página de erro HTTP não vira "jar"; o teste de tamanho barra download parcial
baixar-jars:
	mkdir -p jars
	test -s jars/iceberg-spark-runtime-3.5_2.12-$(ICEBERG_VERSAO).jar || \
	  curl -sSL --fail --retry 5 --retry-delay 15 -o jars/iceberg-spark-runtime-3.5_2.12-$(ICEBERG_VERSAO).jar \
	    "$(MAVEN)/iceberg-spark-runtime-3.5_2.12/$(ICEBERG_VERSAO)/iceberg-spark-runtime-3.5_2.12-$(ICEBERG_VERSAO).jar"
	test "$$(stat -c%s jars/iceberg-spark-runtime-3.5_2.12-$(ICEBERG_VERSAO).jar)" -gt 1000000
	test -s jars/iceberg-aws-bundle-$(ICEBERG_VERSAO).jar || \
	  curl -sSL --fail --retry 5 --retry-delay 15 -o jars/iceberg-aws-bundle-$(ICEBERG_VERSAO).jar \
	    "$(MAVEN)/iceberg-aws-bundle/$(ICEBERG_VERSAO)/iceberg-aws-bundle-$(ICEBERG_VERSAO).jar"
	test "$$(stat -c%s jars/iceberg-aws-bundle-$(ICEBERG_VERSAO).jar)" -gt 1000000

aws-publicar-artefatos: baixar-jars aws-publicar-origem
	aws s3 cp dados/cosif_dominio.csv s3://$(BUCKET)/raw/
	aws s3 cp jars/ s3://$(BUCKET)/scripts/jars/ --recursive

# --- Origem no formato do contrato (ADR-014) e laboratórios na conta (docs/labs_aws.md) ---
# PY: interpretador com PySpark. Padrão = container do projeto; com venv, `make ... PY=python`.
CSV    ?= dados/fin_contabilidade_saldo_contrato.csv
NOME   ?= fin_contabilidade_saldo_contrato
FATOR  ?= 10
ULTIMOS ?= 3
ifeq ($(PY),)
PY     := $(COMPOSE) run --rm pipeline python
PRE_PY := build
endif

.PHONY: converter-parquet gerar-sintetico aws-publicar-origem medir-vazao

# CSV -> raw/<NOME>/dt_processamento=YYYY-MM-DD/*.parquet (os campos continuam texto)
converter-parquet: $(PRE_PY)
	$(PY) scripts/csv_para_parquet_particionado.py --input $(CSV) --destino raw/$(NOME)

# dataset sintético FATOR× maior, mesmo perfil de violações (stdlib, sem Spark); opção de skew:
#   make gerar-sintetico FATOR=10 EXTRA="--contas-quentes 0.2"
gerar-sintetico:
	python3 scripts/gerar_dataset_sintetico.py --fator $(FATOR) --saida raw/sintetico_x$(FATOR).csv $(EXTRA)

# publica um dataset em raw/ (CSV + Parquet particionado). Para o sintético:
#   make aws-publicar-origem BUCKET=<bucket> CSV=raw/sintetico_x10.csv NOME=sintetico_x10
aws-publicar-origem: converter-parquet
	test -n "$(BUCKET)" || (echo "uso: make aws-publicar-origem BUCKET=<bucket> [CSV=... NOME=...]" && exit 1)
	aws s3 cp $(CSV) s3://$(BUCKET)/raw/$(NOME).csv
	aws s3 cp raw/$(NOME) s3://$(BUCKET)/raw/$(NOME) --recursive

# vazão, DPU-h e custo dos últimos runs (AWS CLI); LINHAS = linhas por dia do dataset medido
#   make medir-vazao LINHAS=666660
medir-vazao:
	python3 scripts/medir_vazao_glue.py --ultimos $(ULTIMOS) $(if $(LINHAS),--linhas-por-dia $(LINHAS),)
