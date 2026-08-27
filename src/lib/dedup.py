"""Deduplicação determinística de id_transacao.

O dataset comprova que duplicatas NÃO são reentregas idênticas: todo id duplicado
tem payloads divergentes. Regra adotada (ADR-006): unicidade é sobre o que ENTRA NO
RAZÃO, não sobre o que chega —
  - dentro do lote: o vencedor é escolhido só entre as linhas válidas nas demais
    regras (linha inválida já vai à quarentena pelo próprio motivo e não pode
    "vencer" nem condenar uma linha válida); entre válidas, vence a de dt_lancamento
    mais antigo, com empate decidido por hash da linha inteira — determinístico;
  - contra o histórico: id já PUBLICADO no Silver dentro do lookback vence sempre
    (não se retrata dado usado em fechamento anterior). Id apenas quarentenado no
    passado não conta: o reenvio corrigido de um registro rejeitado é aceito.
Perdedores nunca são descartados: vão à quarentena com motivo.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .schema import NOMES_CAMPOS

COL_HASH = "_linha_hash"
COL_ORDEM_DUP = "_ordem_dup"


def com_hash_linha(df: DataFrame) -> DataFrame:
    """Hash SHA-256 da linha inteira do contrato (desempate estável e auditável)."""
    return df.withColumn(
        COL_HASH,
        F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in NOMES_CAMPOS]), 256),
    )


def marcar_ordem_duplicata(df: DataFrame, col_valida: str) -> DataFrame:
    """Numera as linhas de cada id_transacao, priorizando as válidas nas demais regras.

    Linhas inválidas ordenam depois de todas as válidas, portanto uma linha válida
    com _ordem_dup > 1 sempre tem outra VÁLIDA à sua frente — só essas são
    duplicatas perdedoras. Requer COL_HASH já presente (com_hash_linha).
    """
    janela = Window.partitionBy("id_transacao").orderBy(
        F.col(col_valida).desc(), F.col("dt_lancamento").asc_nulls_last(), F.col(COL_HASH).asc()
    )
    return df.withColumn(COL_ORDEM_DUP, F.row_number().over(janela))
