"""Ponta a ponta com dados sintéticos controlados: bronze → silver (gate) → gold,
num warehouse Iceberg temporário — mesmo caminho de código da produção."""
import csv
import dataclasses
import glob
import json
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from jobs.bronze_ingest import executar as bronze
from jobs.gold_saldo import executar as gold
from jobs.silver_quality import GateReprovado
from jobs.silver_quality import executar as silver
from lib.log import JobLogger
from lib.schema import NOMES_CAMPOS

LOG = JobLogger("teste_e2e")


def linha(id_tx, dia, **kw):
    base = {
        "id_transacao": id_tx,
        "id_contrato": kw.get("id_conta", "A") + "-CTR0",
        "id_conta": "A",
        "cod_agencia": "0001",
        "tipo_contrato": "CC",
        "tipo_lancamento": "CREDITO",
        "valor_lancamento": "100.00",
        "dt_lancamento": f"{dia} 10:00:00",
        "dt_processamento": dia,
        "cod_cosif": "1.1.1.00.0",
        "flag_estorno": "false",
        "id_lote": f"LOTE-{dia}",
    }
    base.update(kw)
    return base


LINHAS = [
    # dia 20
    linha("t1", "2026-08-20", tipo_lancamento="CREDITO", valor_lancamento="100.00"),
    linha("t2", "2026-08-20", tipo_lancamento="DEBITO", valor_lancamento="30.00"),
    linha("t3", "2026-08-20", valor_lancamento="-5.00"),                      # quarentena: valor
    linha("t4", "2026-08-20", cod_cosif="9.9.9.99.9"),                        # quarentena: cosif
    linha("t1", "2026-08-20", valor_lancamento="999.00",
          dt_lancamento="2026-08-20 11:00:00"),                               # quarentena: dup no lote
    # dia 21
    linha("t5", "2026-08-21", id_conta="B", tipo_lancamento="JUROS", valor_lancamento="10.00"),
    linha("t1", "2026-08-21", valor_lancamento="50.00"),                      # quarentena: já publicado
    linha("t3", "2026-08-21", valor_lancamento="5.00"),                       # reenvio corrigido: aceito
]


@pytest.fixture
def csvs(tmp_path):
    transacional = tmp_path / "transacoes.csv"
    with open(transacional, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=NOMES_CAMPOS)
        w.writeheader()
        w.writerows(LINHAS)
    cosif = tmp_path / "cosif.csv"
    cosif.write_text(
        "cod_cosif,descricao,natureza,tipo_contrato_associado\n"
        "1.1.1.00.0,Disponibilidades,ATIVO,CC\n"
    )
    return str(transacional), str(cosif)


def test_pipeline_completo(spark, cfg, csvs):
    # o dataset sintético tem quarentena alta de propósito; o gate em si é
    # exercitado em test_gate_bloqueia_fechamento_com_qualidade_ruim
    cfg = dataclasses.replace(cfg, max_quarentena_pct=80.0)
    transacional, cosif = csvs
    bronze(spark, cfg, LOG, transacional, cosif)

    # Bronze: tudo que chegou, particionado, em Iceberg V3
    assert spark.table(cfg.tb_bronze).count() == len(LINHAS)
    meta_arquivos = sorted(
        glob.glob(f"{cfg.warehouse}/bronze/fin_contabilidade_saldo_contrato/metadata/*.metadata.json")
    )
    with open(meta_arquivos[-1]) as f:
        assert json.load(f)["format-version"] == 3

    silver(spark, cfg, LOG, "2026-08-20")
    silver(spark, cfg, LOG, "2026-08-21")

    # Silver: invariante bronze = silver + quarentena, e motivos esperados
    assert spark.table(cfg.tb_silver).count() + spark.table(cfg.tb_quarentena).count() == len(LINHAS)
    motivos = {
        (r["id_transacao"], str(r["dt_processamento"])): sorted(r["motivos"])
        for r in spark.table(cfg.tb_quarentena).collect()
    }
    assert motivos[("t3", "2026-08-20")] == ["VALOR_NAO_POSITIVO"]
    assert motivos[("t4", "2026-08-20")] == ["COSIF_FORA_DO_DOMINIO"]
    assert motivos[("t1", "2026-08-20")] == ["ID_TRANSACAO_DUPLICADO_NO_LOTE"]
    assert motivos[("t1", "2026-08-21")] == ["ID_TRANSACAO_JA_PROCESSADO"]
    dia21_publicados = {
        r["id_transacao"]
        for r in spark.table(cfg.tb_silver).where(F.col("dt_processamento") == "2026-08-21").collect()
    }
    assert dia21_publicados == {"t5", "t3"}  # reenvio corrigido de t3 aceito

    gold(spark, cfg, LOG, "2026-08-20")
    gold(spark, cfg, LOG, "2026-08-21")

    # Gold dia 21: carry-forward + movimento do dia
    snap = {
        r["id_contrato"]: r
        for r in spark.table(cfg.tb_saldo_contrato).where(F.col("dt_referencia") == "2026-08-21").collect()
    }
    assert snap["A-CTR0"]["saldo"] == Decimal("75.00")   # (100 − 30) do dia 20 + 5 do reenvio
    assert snap["A-CTR0"]["movimento_dia"] == Decimal("5.00")
    assert snap["B-CTR0"]["saldo"] == Decimal("10.00")
    recon = {
        r["cod_agencia"]: r
        for r in spark.table(cfg.tb_reconciliacao).where(F.col("dt_referencia") == "2026-08-20").collect()
    }
    assert recon["0001"]["total_creditos"] == Decimal("100.00")
    assert recon["0001"]["total_debitos"] == Decimal("30.00")


def test_reprocessamento_e_idempotente(spark, cfg, csvs):
    cfg = dataclasses.replace(cfg, max_quarentena_pct=80.0)
    transacional, cosif = csvs
    bronze(spark, cfg, LOG, transacional, cosif)
    for dt in ("2026-08-20", "2026-08-21"):
        silver(spark, cfg, LOG, dt)
        gold(spark, cfg, LOG, dt)

    def estado(tabela, coluna_dt, dt):
        return sorted(
            str(r) for r in spark.table(tabela).where(F.col(coluna_dt) == dt).drop("_ts_validacao").collect()
        )

    antes = {
        "silver": estado(cfg.tb_silver, "dt_processamento", "2026-08-21"),
        "quarentena": estado(cfg.tb_quarentena, "dt_processamento", "2026-08-21"),
        "saldo": estado(cfg.tb_saldo_contrato, "dt_referencia", "2026-08-21"),
    }
    # reexecução do MESMO dia (P4.1): overwrite dinâmico da partição, sem duplicar
    silver(spark, cfg, LOG, "2026-08-21")
    gold(spark, cfg, LOG, "2026-08-21")
    depois = {
        "silver": estado(cfg.tb_silver, "dt_processamento", "2026-08-21"),
        "quarentena": estado(cfg.tb_quarentena, "dt_processamento", "2026-08-21"),
        "saldo": estado(cfg.tb_saldo_contrato, "dt_referencia", "2026-08-21"),
    }
    assert antes == depois


def test_gold_recusa_pular_dia_publicado_sem_snapshot(spark, cfg, csvs):
    """Guarda de continuidade: se o Gold de um dia não rodou (ex.: gate reprovado)
    mas o Silver foi publicado, o dia seguinte NÃO pode somar por cima — o
    movimento do dia pulado sumiria do saldo em silêncio, para sempre."""
    from jobs.gold_saldo import SnapshotDescontinuo

    cfg = dataclasses.replace(cfg, max_quarentena_pct=80.0)
    transacional, cosif = csvs
    bronze(spark, cfg, LOG, transacional, cosif)
    silver(spark, cfg, LOG, "2026-08-20")
    silver(spark, cfg, LOG, "2026-08-21")
    # dia 20 publicado no Silver, mas Gold do dia 20 nunca rodou:
    with pytest.raises(SnapshotDescontinuo):
        gold(spark, cfg, LOG, "2026-08-21")
    # processado em ordem, o mesmo dia passa
    gold(spark, cfg, LOG, "2026-08-20")
    gold(spark, cfg, LOG, "2026-08-21")


def test_gate_bloqueia_fechamento_com_qualidade_ruim(spark, cfg, csvs):
    transacional, cosif = csvs
    bronze(spark, cfg, LOG, transacional, cosif)
    rigido = dataclasses.replace(cfg, max_quarentena_pct=0.001)
    with pytest.raises(GateReprovado):
        silver(spark, rigido, LOG, "2026-08-20")
    # o diagnóstico foi publicado mesmo com o gate reprovado…
    assert spark.table(cfg.tb_quarentena).count() > 0
    relatorio = {r["metrica"]: r["qtd"] for r in spark.table(cfg.tb_dq_relatorio).collect()}
    assert relatorio["total_quarentena"] == 3
    # …mas o Gold nunca roda (a Step Function para no erro do estágio Silver).
