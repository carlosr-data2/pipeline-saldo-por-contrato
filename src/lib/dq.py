"""As 5 regras de qualidade do contrato de dados.

Toda violação gera um motivo textual; a linha inteira vai para a quarentena com o
array de motivos (nunca descarte silencioso). O que não viola segue para o Silver.

Regras do contrato:
  R1 completude   — campos NOT NULL sem valor
  R2 valor        — valor_lancamento > 0 (estorno é flag, não sinal)
  R3 datas        — dt_lancamento <= dt_processamento
  R4 cosif        — cod_cosif existe no domínio COSIF
  R5 unicidade    — id_transacao único (no lote e contra o histórico recente)
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .dedup import COL_ORDEM_DUP, com_hash_linha, marcar_ordem_duplicata
from .schema import NOMES_CAMPOS

COL_MOTIVOS = "motivos"

MOTIVO_NULO = "CAMPO_OBRIGATORIO_NULO"
MOTIVO_VALOR = "VALOR_NAO_POSITIVO"
MOTIVO_DATA = "DATA_LANCAMENTO_POSTERIOR_AO_PROCESSAMENTO"
MOTIVO_COSIF = "COSIF_FORA_DO_DOMINIO"
MOTIVO_DUP_LOTE = "ID_TRANSACAO_DUPLICADO_NO_LOTE"
MOTIVO_DUP_HIST = "ID_TRANSACAO_JA_PROCESSADO"


def aplicar_regras(
    df_tipado: DataFrame,
    dominio_cosif: DataFrame,
    ids_historico: DataFrame | None = None,
) -> DataFrame:
    """Anexa a coluna `motivos` (array). Linha limpa = array vazio.

    dominio_cosif: referencial com a coluna cod_cosif. É pequeno por natureza
    (dezenas de linhas) — join broadcast explícito, sem shuffle do lado grande.
    ids_historico: ids já publicados no Silver na janela de lookback (ou None na
    primeira carga). Ao contrário do domínio, em produção tem centenas de milhões
    de linhas — join com shuffle normal; quem limita o tamanho é a janela (ADR-006).
    """
    df = com_hash_linha(df_tipado)

    # R4 — existência no domínio (join de existência; broadcast explícito do referencial)
    dominio = F.broadcast(dominio_cosif.select(F.col("cod_cosif").alias("_cosif_dominio")).distinct())
    df = df.join(dominio, df["cod_cosif"] == dominio["_cosif_dominio"], "left")

    # R5 (histórico) — id já PUBLICADO em fechamento anterior dentro do lookback.
    # Id apenas quarentenado não conta: reenvio corrigido é aceito (ADR-006).
    if ids_historico is not None:
        historico = ids_historico.select(F.col("id_transacao").alias("_id_historico")).distinct()
        df = df.join(historico, df["id_transacao"] == historico["_id_historico"], "left")
    else:
        df = df.withColumn("_id_historico", F.lit(None).cast("string"))

    # R1–R4 + R5-histórico primeiro: a unicidade intra-lote (abaixo) só disputa
    # entre linhas válidas nessas regras — unicidade é sobre o que entra no razão.
    motivos_base = [
        F.when(F.col(campo).isNull(), F.lit(f"{MOTIVO_NULO}:{campo}")) for campo in NOMES_CAMPOS
    ] + [
        F.when(F.col("valor_lancamento") <= 0, F.lit(MOTIVO_VALOR)),
        F.when(F.to_date(F.col("dt_lancamento")) > F.col("dt_processamento"), F.lit(MOTIVO_DATA)),
        F.when(F.col("cod_cosif").isNotNull() & F.col("_cosif_dominio").isNull(), F.lit(MOTIVO_COSIF)),
        F.when(F.col("_id_historico").isNotNull(), F.lit(MOTIVO_DUP_HIST)),
    ]
    df = df.withColumn("_motivos_base", F.array_compact(F.array(*motivos_base)))
    df = df.withColumn("_valida_base", F.size("_motivos_base") == 0)

    # R5 (lote) — entre as válidas de um mesmo id, só a primeira entra no razão
    df = marcar_ordem_duplicata(df, "_valida_base")
    motivo_dup = F.when(F.col("_valida_base") & (F.col(COL_ORDEM_DUP) > 1), F.lit(MOTIVO_DUP_LOTE))

    return df.withColumn(
        COL_MOTIVOS, F.array_compact(F.concat("_motivos_base", F.array(motivo_dup)))
    ).drop("_cosif_dominio", "_id_historico", "_motivos_base", "_valida_base")


def contagem_por_motivo(df_com_motivos: DataFrame) -> DataFrame:
    """Explode os motivos para o relatório de qualidade (uma linha por regra violada)."""
    return (
        df_com_motivos.select(F.explode(COL_MOTIVOS).alias("motivo"))
        .groupBy("motivo")
        .agg(F.count("*").alias("qtd"))
    )
