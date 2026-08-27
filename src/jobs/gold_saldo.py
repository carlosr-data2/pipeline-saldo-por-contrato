"""Job 3 — Gold: saldo incremental e agregações contábeis do dia de referência.

Saídas (todas Iceberg V3, particionadas por dt_referencia, INSERT OVERWRITE dinâmico):
  gold.saldo_contrato_diario  — snapshot completo: saldo(D) = snapshot(D-1) + movimento(D)
  gold.saldo_conta_diario     — agregação do snapshot por conta
  gold.classificacao_cosif    — tipo_contrato × cod_cosif com o referencial COSIF
  gold.reconciliacao_agencia  — débitos vs. créditos por agência

Controle de consistência (antes de publicar QUALQUER saída): o líquido somado das
agências deve bater com o movimento somado dos contratos — duas rotas de agregação
independentes sobre o mesmo Silver. Divergência acima da tolerância aborta o job
sem publicar nada.
"""
import argparse
import sys
from datetime import date

from pyspark.sql import functions as F

from lib.config import Config
from lib.log import JobLogger
from lib.saldo import (
    classificacao_cosif,
    movimento_por_contrato,
    reconciliacao_por_agencia,
    saldo_por_conta,
    snapshot_saldo,
)
from lib.session import criar_spark, garantir_tabela


class ReconciliacaoDivergente(RuntimeError):
    """Agregações independentes não bateram — nada foi publicado."""


def executar(spark, cfg: Config, log: JobLogger, dt: str) -> None:
    dt_ref = date.fromisoformat(dt)

    with log.etapa("preparacao_tabelas"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cfg.catalogo}.gold")
        garantir_tabela(
            spark,
            cfg.tb_saldo_contrato,
            "id_contrato string, id_conta string, saldo decimal(28,2), movimento_dia decimal(28,2),"
            " qtd_lancamentos_dia bigint, dt_referencia date",
            particao="dt_referencia",
        )
        garantir_tabela(
            spark,
            cfg.tb_saldo_conta,
            "dt_referencia date, id_conta string, qtd_contratos bigint, saldo decimal(28,2),"
            " movimento_dia decimal(28,2)",
            particao="dt_referencia",
        )
        garantir_tabela(
            spark,
            cfg.tb_classificacao_cosif,
            "tipo_contrato string, cod_cosif string, descricao string, natureza string,"
            " flag_coerente boolean, qtd_lancamentos bigint, valor_bruto decimal(28,2),"
            " valor_assinado decimal(28,2), dt_referencia date",
            particao="dt_referencia",
        )
        garantir_tabela(
            spark,
            cfg.tb_reconciliacao,
            "cod_agencia string, total_creditos decimal(28,2), total_debitos decimal(28,2),"
            " liquido decimal(28,2), qtd_lancamentos bigint, dt_referencia date",
            particao="dt_referencia",
        )

    with log.etapa("leitura_silver", dt=dt):
        silver_dia = spark.table(cfg.tb_silver).where(F.col("dt_processamento") == F.lit(dt_ref))
        # persist: o Silver do dia alimenta 4 agregações + o controle de consistência;
        # sem persist, cada saída reexecutaria a leitura (P3.4 — custo: memória/spill).
        silver_dia.persist()
        total_silver = silver_dia.count()
        if total_silver == 0:
            raise ValueError(f"partição {dt} vazia no Silver — gate reprovado ou data errada")

    with log.etapa("snapshot_incremental", dt=dt):
        movimento = movimento_por_contrato(silver_dia)
        anterior = spark.table(cfg.tb_saldo_contrato).where(F.col("dt_referencia") < F.lit(dt_ref))
        dt_anterior = anterior.agg(F.max("dt_referencia")).collect()[0][0]
        snapshot_ant = (
            anterior.where(F.col("dt_referencia") == F.lit(dt_anterior))
            if dt_anterior is not None
            else spark.createDataFrame([], "id_contrato string, id_conta string, saldo decimal(28,2)")
        )
        snapshot = snapshot_saldo(snapshot_ant, movimento, dt_ref)
        snapshot.persist()
        log.evento("snapshot_base", dt=dt, snapshot_anterior=str(dt_anterior))

    with log.etapa("agregacoes", dt=dt):
        contas = saldo_por_conta(snapshot)
        dominio = spark.table(cfg.tb_ref_cosif)
        classificacao = classificacao_cosif(silver_dia, dominio, dt_ref)
        reconciliacao = reconciliacao_por_agencia(silver_dia, dt_ref)

    with log.etapa("controle_consistencia", dt=dt):
        liquido_agencias = reconciliacao.agg(F.sum("liquido")).collect()[0][0] or 0
        movimento_contratos = snapshot.agg(F.sum("movimento_dia")).collect()[0][0] or 0
        divergencia = abs(float(liquido_agencias) - float(movimento_contratos))
        log.evento(
            "reconciliacao_cruzada",
            dt=dt,
            liquido_agencias=str(liquido_agencias),
            movimento_contratos=str(movimento_contratos),
            divergencia=divergencia,
        )
        if divergencia > cfg.tolerancia_reconciliacao:
            raise ReconciliacaoDivergente(
                f"divergência de {divergencia:.2f} BRL entre agências e contratos na partição {dt}"
            )

    with log.etapa("publicacao_gold", dt=dt):
        snapshot.writeTo(cfg.tb_saldo_contrato).overwritePartitions()
        contas.writeTo(cfg.tb_saldo_conta).overwritePartitions()
        classificacao.writeTo(cfg.tb_classificacao_cosif).overwritePartitions()
        reconciliacao.writeTo(cfg.tb_reconciliacao).overwritePartitions()

    log.evento(
        "gold_concluido",
        dt=dt,
        lancamentos_dia=total_silver,
        contratos_no_snapshot=snapshot.count(),
        saldo_total=str(snapshot.agg(F.sum("saldo")).collect()[0][0]),
    )
    snapshot.unpersist()
    silver_dia.unpersist()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Gold — saldo por contrato")
    parser.add_argument("--dt", required=True, help="data de referência (YYYY-MM-DD)")
    args, _ = parser.parse_known_args(argv)

    cfg = Config.do_ambiente()
    log = JobLogger("gold_saldo")
    spark = criar_spark("gold_saldo", cfg)
    try:
        executar(spark, cfg, log, args.dt)
        return 0
    except Exception as exc:
        log.evento("job_falhou", erro=type(exc).__name__, mensagem=str(exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
