"""Job 2 — Silver: regras de qualidade, deduplicação, quarentena e gate de fechamento.

Para a partição do dia:
  1. aplica as 5 regras do contrato (lib.dq) + dedup determinística (lib.dedup);
  2. publica Silver (linhas limpas, com valor_assinado) e Quarentena (com motivos);
  3. publica o relatório de qualidade da partição (dq_relatorio);
  4. GATE: se a taxa de quarentena passar do limiar, o job falha DEPOIS de publicar
     silver/quarentena/relatório — o dado de diagnóstico existe, mas o Gold não roda
     e o fechamento não acontece com dado ruim (Step Functions para no erro).
"""
import argparse
import sys
from datetime import date, timedelta

from pyspark.sql import functions as F

from lib.config import Config
from lib.dq import COL_MOTIVOS, aplicar_regras, contagem_por_motivo
from lib.log import JobLogger
from lib.saldo import com_valor_assinado
from lib.schema import NOMES_CAMPOS, ddl_contrato
from lib.session import criar_spark, garantir_tabela


class GateReprovado(RuntimeError):
    """Qualidade abaixo do mínimo regulatório — fechamento bloqueado."""


def escrever_particao(spark, df, tabela: str, dt_ref) -> int:
    """INSERT OVERWRITE dinâmico da partição; com DataFrame vazio, overwritePartitions
    é no-op e deixaria linhas velhas num reprocessamento — nesse caso, limpa a partição."""
    qtd = df.count()
    if qtd > 0:
        df.writeTo(tabela).overwritePartitions()
    else:
        spark.sql(f"DELETE FROM {tabela} WHERE dt_processamento = date'{dt_ref}'")
    return qtd


def executar(spark, cfg: Config, log: JobLogger, dt: str) -> None:
    dt_ref = date.fromisoformat(dt)

    with log.etapa("preparacao_tabelas"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cfg.catalogo}.silver")
        garantir_tabela(
            spark,
            cfg.tb_silver,
            ddl_contrato() + ", valor_assinado decimal(18,2), _ts_validacao timestamp",
            particao="dt_processamento",
        )
        garantir_tabela(
            spark,
            cfg.tb_quarentena,
            ddl_contrato() + ", motivos array<string>, _ts_validacao timestamp",
            particao="dt_processamento",
        )
        garantir_tabela(
            spark,
            cfg.tb_dq_relatorio,
            "dt_processamento date, metrica string, qtd bigint, _ts_validacao timestamp",
            particao="dt_processamento",
        )

    with log.etapa("leitura_bronze", dt=dt):
        bronze_dia = (
            spark.table(cfg.tb_bronze)
            .where(F.col("dt_processamento") == F.lit(dt_ref))
            .select(*NOMES_CAMPOS)
        )
        total_bronze = bronze_dia.count()
        if total_bronze == 0:
            raise ValueError(f"partição {dt} vazia no Bronze — falha upstream ou data errada")

    with log.etapa("aplicacao_regras", dt=dt):
        dominio = spark.table(cfg.tb_ref_cosif)
        inicio_lookback = dt_ref - timedelta(days=cfg.dedup_lookback_dias)
        ids_historico = (
            spark.table(cfg.tb_silver)
            .where(
                (F.col("dt_processamento") >= F.lit(inicio_lookback))
                & (F.col("dt_processamento") < F.lit(dt_ref))
            )
            .select("id_transacao")
        )
        avaliado = aplicar_regras(bronze_dia, dominio, ids_historico)
        # persist: o resultado das regras alimenta Silver, Quarentena e o relatório —
        # sem persist, o plano (janela + 2 joins) executaria três vezes.
        avaliado.persist()

    with log.etapa("escrita_silver_quarentena", dt=dt):
        limpo = avaliado.where(F.size(COL_MOTIVOS) == 0)
        silver = (
            com_valor_assinado(limpo)
            .withColumn("_ts_validacao", F.current_timestamp())
            .select(*NOMES_CAMPOS, "valor_assinado", "_ts_validacao")
        )
        quarentena = (
            avaliado.where(F.size(COL_MOTIVOS) > 0)
            .withColumn("_ts_validacao", F.current_timestamp())
            .select(*NOMES_CAMPOS, COL_MOTIVOS, "_ts_validacao")
        )
        total_silver = escrever_particao(spark, silver, cfg.tb_silver, dt_ref)
        total_quarentena = escrever_particao(spark, quarentena, cfg.tb_quarentena, dt_ref)

    with log.etapa("relatorio_qualidade", dt=dt):
        por_motivo = {r["motivo"]: r["qtd"] for r in contagem_por_motivo(avaliado).collect()}

        # Observabilidade (não é regra do contrato, não bloqueia): coerência
        # entre o cod_cosif lançado e o associado ao tipo de contrato no domínio.
        incoerentes = (
            limpo.join(
                F.broadcast(dominio.select("cod_cosif", "tipo_contrato_associado")), "cod_cosif", "left"
            )
            .where(F.col("tipo_contrato") != F.col("tipo_contrato_associado"))
            .count()
        )

        metricas = {
            "total_bronze": total_bronze,
            "total_silver": total_silver,
            "total_quarentena": total_quarentena,
            "obs_cosif_incoerente_com_tipo_contrato": incoerentes,
            **{f"motivo:{m}": q for m, q in sorted(por_motivo.items())},
        }
        relatorio = spark.createDataFrame(
            [(dt_ref, metrica, int(qtd)) for metrica, qtd in metricas.items()],
            "dt_processamento date, metrica string, qtd bigint",
        ).withColumn("_ts_validacao", F.current_timestamp())
        relatorio.writeTo(cfg.tb_dq_relatorio).overwritePartitions()

    avaliado.unpersist()

    taxa_quarentena = 100.0 * total_quarentena / total_bronze
    log.evento(
        "qualidade_particao",
        dt=dt,
        taxa_quarentena_pct=round(taxa_quarentena, 2),
        limiar_pct=cfg.max_quarentena_pct,
        **metricas,
    )
    if taxa_quarentena > cfg.max_quarentena_pct:
        log.evento("gate_reprovado", dt=dt, taxa_quarentena_pct=round(taxa_quarentena, 2))
        raise GateReprovado(
            f"taxa de quarentena {taxa_quarentena:.2f}% > limiar {cfg.max_quarentena_pct}% na partição {dt}"
        )
    log.evento("gate_aprovado", dt=dt)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Qualidade Silver")
    parser.add_argument("--dt", required=True, help="partição dt_processamento (YYYY-MM-DD)")
    args, _ = parser.parse_known_args(argv)

    cfg = Config.do_ambiente()
    log = JobLogger("silver_quality")
    spark = criar_spark("silver_quality", cfg)
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
