"""Job 1 — Bronze: ingestão da origem com tipagem do contrato e partição por dt_processamento.

Origem (--formato):
  parquet — diretório particionado por dt_processamento, o formato de origem que o
            contrato define (raw/<nome>/dt_processamento=YYYY-MM-DD/);
  csv     — arquivo único com todos os campos como texto, como chega da origem.
Modo (--dt):
  com --dt — ingere SÓ a partição do dia e sobrescreve só ela: é o fechamento diário
             (ADR-014). Na origem Parquet o filtro vira poda de partição — só o
             diretório do dia é aberto;
  sem --dt — ingere todas as partições presentes na origem (carga inicial; o CSV do
             exemplo traz três dias de uma vez).
Destino: tabela Iceberg V3 particionada por dt_processamento, escrita com INSERT
OVERWRITE dinâmico de partição — reprocessar um dia é idempotente e não toca os demais.
Também materializa o referencial COSIF (ref.cosif_dominio).
"""
import argparse
from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lib.config import Config
from lib.log import JobLogger
from lib.schema import NOMES_CAMPOS, ddl_contrato, tipar_contrato
from lib.session import criar_spark, garantir_tabela

COLUNAS_COSIF = ["cod_cosif", "descricao", "natureza", "tipo_contrato_associado"]
FORMATOS = ("parquet", "csv")
CHAVES_COMMIT = ("changed-partition-count", "added-records", "added-data-files", "deleted-records")


def ler_origem(spark, caminho: str, formato: str) -> DataFrame:
    """Lê a origem e devolve os campos do contrato como TEXTO, em qualquer formato.

    No Parquet particionado, dt_processamento não está dentro dos arquivos: vem do
    caminho (dt_processamento=YYYY-MM-DD) e o Spark infere o tipo. O cast para string
    devolve o campo à forma de entrega do contrato (texto) sem perder a poda: um
    predicado sobre a coluna de partição continua sendo avaliado nos metadados do
    diretório, antes de abrir qualquer arquivo.
    """
    if formato == "csv":
        bruto = spark.read.csv(caminho, header=True, inferSchema=False)
    elif formato == "parquet":
        bruto = spark.read.parquet(caminho)
    else:
        raise ValueError(f"formato de origem desconhecido: {formato!r} (esperado: {' | '.join(FORMATOS)})")
    faltantes = set(NOMES_CAMPOS) - set(bruto.columns)
    if faltantes:
        raise ValueError(f"schema da origem diverge do contrato; colunas ausentes: {sorted(faltantes)}")
    return bruto.select(*[F.col(c).cast("string").alias(c) for c in NOMES_CAMPOS])


def resumo_ultimo_commit(spark, tabela: str) -> dict:
    """Resumo do snapshot mais recente (metadados do próprio Iceberg): quantas partições
    o commit tocou e quantos registros/arquivos entraram. É a prova, no log, de que o
    modo por dia reescreveu uma única partição."""
    linhas = spark.sql(
        f"SELECT operation, summary FROM {tabela}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    if not linhas:
        return {}
    resumo = dict(linhas[0]["summary"])
    return {"operacao": linhas[0]["operation"], **{k: resumo.get(k) for k in CHAVES_COMMIT}}


def executar(
    spark,
    cfg: Config,
    log: JobLogger,
    caminho_origem: str,
    caminho_cosif: str,
    formato: str = "csv",
    dt: str | None = None,
) -> None:
    dt_ref = date.fromisoformat(dt) if dt else None
    modo = "particao_do_dia" if dt_ref else "todas_as_particoes"

    with log.etapa("leitura_origem", origem=caminho_origem, formato=formato, modo=modo, dt=dt):
        bruto = ler_origem(spark, caminho_origem, formato)

    with log.etapa("tipagem_e_escrita_bronze", dt=dt):
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
        if dt_ref is not None:
            # Filtro sobre a coluna de partição tipada: na origem Parquet o otimizador o
            # empurra até a leitura (poda) e só o diretório do dia é aberto. A contagem
            # antes de escrever custa uma leitura extra da partição, mas evita o silêncio:
            # com DataFrame vazio, overwritePartitions é no-op e o "dia ingerido" não
            # existiria — o Silver só acusaria depois, com um job a mais na conta.
            tipado = tipado.where(F.col("dt_processamento") == F.lit(dt_ref))
            if tipado.count() == 0:
                raise ValueError(f"partição {dt} vazia na origem — lote não chegou ou data errada")
        tipado.writeTo(cfg.tb_bronze).overwritePartitions()
        log.evento("bronze_commit", dt=dt, modo=modo, commit=resumo_ultimo_commit(spark, cfg.tb_bronze))

    with log.etapa("escrita_referencial_cosif", arquivo=caminho_cosif):
        cosif = spark.read.csv(caminho_cosif, header=True, inferSchema=False).select(*COLUNAS_COSIF)
        garantir_tabela(spark, cfg.tb_ref_cosif, ", ".join(f"{c} string" for c in COLUNAS_COSIF))
        cosif.writeTo(cfg.tb_ref_cosif).overwritePartitions()

    bronze = spark.table(cfg.tb_bronze)
    if dt_ref is not None:
        # só a partição publicada: leitura podada, nunca varredura do histórico
        particoes = {dt: bronze.where(F.col("dt_processamento") == F.lit(dt_ref)).count()}
    else:
        linhas = bronze.groupBy("dt_processamento").count().orderBy("dt_processamento").collect()
        particoes = {str(r["dt_processamento"]): r["count"] for r in linhas}
    log.evento(
        "bronze_concluido",
        modo=modo,
        particoes=particoes,
        total=sum(particoes.values()),
        referencial_cosif=spark.table(cfg.tb_ref_cosif).count(),
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Ingestão Bronze")
    parser.add_argument(
        "--input", required=True, help="origem: diretório Parquet particionado por dt_processamento, ou CSV"
    )
    parser.add_argument("--cosif", required=True, help="CSV do domínio COSIF")
    parser.add_argument("--formato", default="csv", choices=FORMATOS, help="formato da origem (padrão: csv)")
    parser.add_argument(
        "--dt", default=None, help="partição dt_processamento (YYYY-MM-DD); sem ela, todas as da origem"
    )
    args, _ = parser.parse_known_args(argv)  # Glue injeta args próprios (--JOB_NAME etc.)

    cfg = Config.do_ambiente()
    log = JobLogger("bronze_ingest")
    spark = criar_spark("bronze_ingest", cfg)
    try:
        executar(spark, cfg, log, args.input, args.cosif, formato=args.formato, dt=args.dt)
        return 0
    except Exception as exc:
        log.evento("job_falhou", erro=type(exc).__name__, mensagem=str(exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    # Sem sys.exit: o Glue executa o script dentro do driver e trata SystemExit
    # (mesmo com código 0) como job FAILED. Exceção real já encerra com código != 0.
    main()
