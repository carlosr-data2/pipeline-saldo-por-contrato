"""Relatório da demonstração: estado final das tabelas das três camadas.

Também prova, lendo o metadata.json de cada tabela, que TODO o dado foi escrito
em Apache Iceberg V3 (requisito do desafio).
"""
import glob
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyspark.sql import functions as F  # noqa: E402

from lib.config import Config  # noqa: E402
from lib.session import criar_spark  # noqa: E402


def main() -> None:
    cfg = Config.do_ambiente()
    spark = criar_spark("relatorio_demo", cfg)
    spark.sparkContext.setLogLevel("ERROR")

    print("\n=== Camadas e contagens ===")
    tabelas = [
        cfg.tb_bronze, cfg.tb_ref_cosif, cfg.tb_silver, cfg.tb_quarentena,
        cfg.tb_dq_relatorio, cfg.tb_saldo_contrato, cfg.tb_saldo_conta,
        cfg.tb_classificacao_cosif, cfg.tb_reconciliacao,
    ]
    for t in tabelas:
        print(f"{t}: {spark.table(t).count()} linhas")

    print("\n=== Relatório de qualidade por partição ===")
    spark.table(cfg.tb_dq_relatorio).orderBy("dt_processamento", "metrica").show(60, truncate=False)

    print("=== Amostra da quarentena (nunca descarte silencioso: sempre com motivos) ===")
    spark.table(cfg.tb_quarentena).select("dt_processamento", "id_transacao", "motivos").show(5, truncate=False)

    print("=== Saldo por contrato (últimos snapshots, amostra) ===")
    spark.table(cfg.tb_saldo_contrato).orderBy(F.desc("dt_referencia"), F.desc("saldo")).show(5)

    print("=== Reconciliação débito × crédito por agência (amostra) ===")
    spark.table(cfg.tb_reconciliacao).orderBy(F.desc("dt_referencia"), "cod_agencia").show(5)

    print("=== Formato de tabela: Apache Iceberg, format-version por tabela ===")
    for t in tabelas:
        caminho = os.path.join(cfg.warehouse, *t.split(".")[1:], "metadata", "*.metadata.json")
        with open(sorted(glob.glob(caminho))[-1]) as f:
            versao = json.load(f)["format-version"]
        print(f"{t}: format-version={versao}")
        assert versao == 3, f"{t} não está em Iceberg V3"
    print("\nDemo concluída: todas as tabelas em Iceberg V3, gate aprovado, saldos publicados.")

    spark.stop()


if __name__ == "__main__":
    main()
