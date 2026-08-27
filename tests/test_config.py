import sys

from lib.config import Config


def test_config_le_variaveis_de_ambiente(monkeypatch):
    monkeypatch.setenv("SALDO_CATALOGO", "meucat")
    monkeypatch.setenv("SALDO_GATE_MAX_QUARENTENA_PCT", "3.5")
    cfg = Config.do_ambiente()
    assert cfg.catalogo == "meucat"
    assert cfg.max_quarentena_pct == 3.5


def test_config_le_argumentos_no_formato_do_glue(monkeypatch):
    """O Glue entrega default_arguments via argv (--NOME valor), nunca via env —
    a config PRECISA enxergá-los, senão os jobs na AWS rodam com defaults locais."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "job.py",
            "--SALDO_CATALOGO", "glue_catalog",
            "--SALDO_CATALOGO_IMPL", "glue",
            "--SALDO_WAREHOUSE", "s3://bucket/warehouse",
            "--SALDO_SHUFFLE_PARTITIONS", "16",
        ],
    )
    cfg = Config.do_ambiente()
    assert cfg.catalogo == "glue_catalog"
    assert cfg.impl == "glue"
    assert cfg.warehouse == "s3://bucket/warehouse"
    assert cfg.shuffle_partitions == 16


def test_env_tem_precedencia_sobre_argv(monkeypatch):
    monkeypatch.setenv("SALDO_CATALOGO", "do_env")
    monkeypatch.setattr(sys, "argv", ["job.py", "--SALDO_CATALOGO", "do_argv"])
    assert Config.do_ambiente().catalogo == "do_env"
