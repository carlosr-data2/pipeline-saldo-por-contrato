"""Prova de correção por dupla implementação, sobre o dataset REAL do desafio:
o pipeline Spark e o oráculo em Python puro (tests/oraculo.py) devem produzir
exatamente as mesmas contagens de qualidade e os mesmos saldos."""
import os
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from jobs.bronze_ingest import executar as bronze
from jobs.gold_saldo import executar as gold
from jobs.silver_quality import executar as silver
from lib.log import JobLogger
from oraculo import processar

RAIZ = os.path.join(os.path.dirname(__file__), "..")
CSV = os.path.join(RAIZ, "dados", "fin_contabilidade_saldo_contrato.csv")
COSIF = os.path.join(RAIZ, "dados", "cosif_dominio.csv")

DIAS = ["2026-08-20", "2026-08-21", "2026-08-22"]


@pytest.mark.slow
def test_pipeline_bate_com_oraculo_no_dataset_real(spark, cfg):
    log = JobLogger("teste_oraculo")
    bronze(spark, cfg, log, CSV, COSIF)
    for dt in DIAS:
        silver(spark, cfg, log, dt)
        gold(spark, cfg, log, dt)

    esperado = processar(CSV, COSIF, lookback_dias=cfg.dedup_lookback_dias)

    for dia, exp in esperado.items():
        dt = str(dia)
        silver_qtd = spark.table(cfg.tb_silver).where(F.col("dt_processamento") == dt).count()
        quarentena_qtd = spark.table(cfg.tb_quarentena).where(F.col("dt_processamento") == dt).count()
        assert (silver_qtd, quarentena_qtd) == (exp["silver"], exp["quarentena"]), dt
        assert silver_qtd + quarentena_qtd == exp["total"], dt  # nada descartado

        relatorio = {
            r["metrica"]: r["qtd"]
            for r in spark.table(cfg.tb_dq_relatorio).where(F.col("dt_processamento") == dt).collect()
        }
        for motivo, qtd in exp["motivos"].items():
            assert relatorio[f"motivo:{motivo}"] == qtd, (dt, motivo)

    # saldos do último dia (acumulado dos 3): contrato a contrato
    ultimo = max(esperado)
    obtido = {
        r["id_contrato"]: r["saldo"]
        for r in spark.table(cfg.tb_saldo_contrato).where(F.col("dt_referencia") == str(ultimo)).collect()
    }
    saldo_esperado = esperado[ultimo]["saldo_contrato"]
    assert set(obtido) == set(saldo_esperado)
    divergentes = {c for c, v in saldo_esperado.items() if Decimal(str(obtido[c])) != v}
    assert not divergentes, f"{len(divergentes)} contratos divergem do oráculo"
