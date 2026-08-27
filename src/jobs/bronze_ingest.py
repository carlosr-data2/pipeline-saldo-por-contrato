"""Job 1 — Bronze: ingestão do CSV com tipagem do contrato e partição por dt_processamento.

Origem: CSV único com todos os campos como texto (conforme entregue).
Destino: tabela Iceberg V3 particionada por dt_processamento, escrita com
INSERT OVERWRITE dinâmico de partição — reprocessar o mesmo arquivo é idempotente.
Também materializa o referencial COSIF (ref.cosif_dominio).
"""
import argparse
import sys

from pyspark.sql import functions as F

from lib.config import Config
from lib.log import JobLogger
from lib.schema import NOMES_CAMPOS, ddl_contrato, tipar_contrato
from lib.session import criar_spark, garantir_tabela

COLUNAS_COSIF = ["cod_cosif", "descricao", "natureza", "tipo_contrato_associado"]


def executar(spark, cfg: Config, log: JobLogger, caminho_csv: str, caminho_cosif: str) -> None:
    with log.etapa("leitura_csv", arquivo=caminho_csv):
        bruto = spark.read.csv(caminho_csv, header=True, inferSchema=False)
        faltantes = set(NOMES_CAMPOS) - set(bruto.columns)
        if faltantes:
            raise ValueError(f"schema do arquivo diverge do contrato; colunas ausentes: {sorted(faltantes)}")

    with log.etapa("tipagem_e_escrita_bronze"):
        for ns in ("bronze", "ref"):
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cfg.catalogo}.{ns}")
        garantir_tabela(
            spark,
            cfg.tb_bronze,
            ddl_contrato() + ", _arquivo_origem string, _ts_ingestao timestamp",
            particao="dt_processamento",
        )
        tipado = (
            tipar_contrato(bruto)
            .withColumn("_arquivo_origem", F.input_file_name())
            .withColumn("_ts_ingestao", F.current_timestamp())
        )
        tipado.writeTo(cfg.tb_bronze).overwritePartitions()

    with log.etapa("escrita_referencial_cosif", arquivo=caminho_cosif):
        cosif = spark.read.csv(caminho_cosif, header=True, inferSchema=False).select(*COLUNAS_COSIF)
        garantir_tabela(spark, cfg.tb_ref_cosif, ", ".join(f"{c} string" for c in COLUNAS_COSIF))
        cosif.writeTo(cfg.tb_ref_cosif).overwritePartitions()

    particoes = (
        spark.table(cfg.tb_bronze).groupBy("dt_processamento").count().orderBy("dt_processamento").collect()
    )
    log.evento(
        "bronze_concluido",
        particoes={str(r["dt_processamento"]): r["count"] for r in particoes},
        total=sum(r["count"] for r in particoes),
        referencial_cosif=spark.table(cfg.tb_ref_cosif).count(),
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Ingestão Bronze")
    parser.add_argument("--input", required=True, help="CSV transacional de origem")
    parser.add_argument("--cosif", required=True, help="CSV do domínio COSIF")
    args, _ = parser.parse_known_args(argv)  # Glue injeta args próprios (--JOB_NAME etc.)

    cfg = Config.do_ambiente()
    log = JobLogger("bronze_ingest")
    spark = criar_spark("bronze_ingest", cfg)
    try:
        executar(spark, cfg, log, args.input, args.cosif)
        return 0
    except Exception as exc:
        log.evento("job_falhou", erro=type(exc).__name__, mensagem=str(exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
