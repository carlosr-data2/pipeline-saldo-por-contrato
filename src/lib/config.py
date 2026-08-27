"""Configuração do pipeline, resolvida por variáveis de ambiente.

O mesmo código roda local (catálogo Iceberg "hadoop" em filesystem) e no AWS Glue
(catálogo Iceberg "glue" = Glue Data Catalog). A troca é 100% configuração — nenhum
job importa GlueContext, o que mantém o motor portável.
"""
import os
import sys
from dataclasses import dataclass


def _param(nome: str, padrao: str) -> str:
    """Resolve um parâmetro: variável de ambiente (trilha local) ou argumento
    de job no formato do Glue (--NOME valor), que entrega parâmetros via argv."""
    if nome in os.environ:
        return os.environ[nome]
    argv = sys.argv
    chave = f"--{nome}"
    if chave in argv and argv.index(chave) + 1 < len(argv):
        return argv[argv.index(chave) + 1]
    return padrao


@dataclass(frozen=True)
class Config:
    catalogo: str          # nome lógico do catálogo Spark (ex.: "local", "glue_catalog")
    impl: str              # "hadoop" (filesystem) | "glue" (Glue Data Catalog)
    warehouse: str         # path do warehouse (dir local ou s3://...)
    max_quarentena_pct: float   # gate: % máxima de quarentena antes de bloquear o fechamento
    tolerancia_reconciliacao: float  # gate: divergência máxima (BRL) entre agregações independentes
    dedup_lookback_dias: int    # janela de verificação de id_transacao contra o histórico
    shuffle_partitions: int     # dimensionado p/ volume local; em produção via spark-submit/Glue

    @classmethod
    def do_ambiente(cls) -> "Config":
        return cls(
            catalogo=_param("SALDO_CATALOGO", "local"),
            impl=_param("SALDO_CATALOGO_IMPL", "hadoop"),
            warehouse=_param("SALDO_WAREHOUSE", os.path.abspath("warehouse")),
            max_quarentena_pct=float(_param("SALDO_GATE_MAX_QUARENTENA_PCT", "10.0")),
            tolerancia_reconciliacao=float(_param("SALDO_GATE_TOLERANCIA_BRL", "0.01")),
            dedup_lookback_dias=int(_param("SALDO_DEDUP_LOOKBACK_DIAS", "7")),
            shuffle_partitions=int(_param("SALDO_SHUFFLE_PARTITIONS", "8")),
        )

    # ---- nomes totalmente qualificados das tabelas (catalogo.namespace.tabela) ----
    @property
    def tb_bronze(self) -> str:
        return f"{self.catalogo}.bronze.fin_contabilidade_saldo_contrato"

    @property
    def tb_ref_cosif(self) -> str:
        return f"{self.catalogo}.ref.cosif_dominio"

    @property
    def tb_silver(self) -> str:
        return f"{self.catalogo}.silver.fin_contabilidade_saldo_contrato"

    @property
    def tb_quarentena(self) -> str:
        return f"{self.catalogo}.silver.quarentena"

    @property
    def tb_dq_relatorio(self) -> str:
        return f"{self.catalogo}.silver.dq_relatorio"

    @property
    def tb_saldo_contrato(self) -> str:
        return f"{self.catalogo}.gold.saldo_contrato_diario"

    @property
    def tb_saldo_conta(self) -> str:
        return f"{self.catalogo}.gold.saldo_conta_diario"

    @property
    def tb_classificacao_cosif(self) -> str:
        return f"{self.catalogo}.gold.classificacao_cosif"

    @property
    def tb_reconciliacao(self) -> str:
        return f"{self.catalogo}.gold.reconciliacao_agencia"
