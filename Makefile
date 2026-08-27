# Trilha LOCAL (Docker) — demonstração de ponta a ponta sem AWS.
#   make build   constrói a imagem (Spark 3.5.4 + Java 17 + Iceberg 1.10.2)
#   make demo    executa o pipeline completo (3 dias) e imprime o relatório
#   make test    roda a suíte de testes dentro do container
#   make lint    roda o ruff dentro do container
#   make shell   abre um shell no container
#   make limpar  remove o warehouse local (dados derivados)

COMPOSE := docker compose

.PHONY: build demo test lint shell limpar

build:
	$(COMPOSE) build

demo: build
	$(COMPOSE) run --rm pipeline bash scripts/demo.sh

test: build
	$(COMPOSE) run --rm pipeline python -m pytest -q

lint: build
	$(COMPOSE) run --rm pipeline ruff check src tests

shell: build
	$(COMPOSE) run --rm pipeline bash

# roda dentro do container: os arquivos do warehouse são criados pelo root do
# container via bind mount — apagar no host falharia com Permission denied
limpar:
	$(COMPOSE) run --rm pipeline rm -rf warehouse logs

# Trilha AWS — depois de `terraform apply` (ver docs/runbook_aws.md):
#   make aws-publicar-artefatos BUCKET=<saida `bucket` do terraform>
# Sobe os dados de origem e os jars do Iceberg; scripts e src.zip o Terraform já sobe.

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

aws-publicar-artefatos: baixar-jars
	test -n "$(BUCKET)" || (echo "uso: make aws-publicar-artefatos BUCKET=<bucket>" && exit 1)
	aws s3 cp dados/fin_contabilidade_saldo_contrato.csv s3://$(BUCKET)/raw/
	aws s3 cp dados/cosif_dominio.csv s3://$(BUCKET)/raw/
	aws s3 cp jars/ s3://$(BUCKET)/scripts/jars/ --recursive
