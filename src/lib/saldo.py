"""Regras de negócio do saldo: sinal, estorno, movimento diário e snapshot incremental.

Convenção de sinal (ADR-007 — o contrato não define; premissa registrada):
  CREDITO, JUROS            -> aumentam o saldo (+)
  DEBITO, TARIFA, IOF       -> reduzem o saldo (−)
  flag_estorno = true       -> inverte o sinal do lançamento original

Saldo incremental (ADR-005): saldo(D) = snapshot(D-1) ⟗ movimento(D).
Nunca full scan do histórico — é isso que faz 300M/dia caber no SLA de 1h.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

TIPOS_CREDITO = ["CREDITO", "JUROS"]


def com_valor_assinado(df: DataFrame) -> DataFrame:
    sinal_tipo = F.when(F.col("tipo_lancamento").isin(TIPOS_CREDITO), F.lit(1)).otherwise(F.lit(-1))
    sinal_estorno = F.when(F.col("flag_estorno"), F.lit(-1)).otherwise(F.lit(1))
    return df.withColumn(
        "valor_assinado",
        (F.col("valor_lancamento") * sinal_tipo * sinal_estorno).cast("decimal(18,2)"),
    )


def movimento_por_contrato(silver_dia: DataFrame) -> DataFrame:
    """Movimento líquido do dia por contrato (id_conta é 1:1 com o contrato)."""
    return silver_dia.groupBy("id_contrato").agg(
        F.max("id_conta").alias("id_conta"),
        F.sum("valor_assinado").cast("decimal(28,2)").alias("movimento_dia"),
        F.count("*").alias("qtd_lancamentos_dia"),
    )


def snapshot_saldo(snapshot_anterior: DataFrame, movimento: DataFrame, dt_referencia) -> DataFrame:
    """Carry-forward do snapshot anterior + movimento do dia (full outer join).

    Contratos sem movimento mantêm o saldo; contratos novos entram com saldo = movimento.
    """
    anterior = snapshot_anterior.select(
        F.col("id_contrato").alias("_id_contrato_ant"),
        F.col("id_conta").alias("_id_conta_ant"),
        F.col("saldo").alias("_saldo_ant"),
    )
    juncao = anterior.join(
        movimento, anterior["_id_contrato_ant"] == movimento["id_contrato"], "full_outer"
    )
    return juncao.select(
        F.coalesce(F.col("id_contrato"), F.col("_id_contrato_ant")).alias("id_contrato"),
        F.coalesce(F.col("id_conta"), F.col("_id_conta_ant")).alias("id_conta"),
        (F.coalesce(F.col("_saldo_ant"), F.lit(0)) + F.coalesce(F.col("movimento_dia"), F.lit(0)))
        .cast("decimal(28,2)")
        .alias("saldo"),
        F.coalesce(F.col("movimento_dia"), F.lit(0)).cast("decimal(28,2)").alias("movimento_dia"),
        F.coalesce(F.col("qtd_lancamentos_dia"), F.lit(0)).alias("qtd_lancamentos_dia"),
        F.lit(dt_referencia).cast("date").alias("dt_referencia"),
    )


def saldo_por_conta(snapshot_contratos: DataFrame) -> DataFrame:
    """Agrega o snapshot de contratos por conta (todos os contratos da conta)."""
    return snapshot_contratos.groupBy("dt_referencia", "id_conta").agg(
        F.count("*").alias("qtd_contratos"),
        F.sum("saldo").cast("decimal(28,2)").alias("saldo"),
        F.sum("movimento_dia").cast("decimal(28,2)").alias("movimento_dia"),
    )


def classificacao_cosif(silver_dia: DataFrame, dominio_cosif: DataFrame, dt_referencia) -> DataFrame:
    """Distribuição contábil observada: tipo_contrato × cod_cosif, com o referencial.

    `flag_coerente` marca se o cod_cosif do lançamento é o associado ao tipo de
    contrato no domínio — a incoerência não é regra do contrato (não bloqueia),
    mas é reportada como métrica de observabilidade (ADR-008).
    """
    dominio = F.broadcast(
        dominio_cosif.select("cod_cosif", "descricao", "natureza", "tipo_contrato_associado")
    )
    return (
        silver_dia.join(dominio, "cod_cosif", "left")
        .withColumn("flag_coerente", F.col("tipo_contrato") == F.col("tipo_contrato_associado"))
        .groupBy("tipo_contrato", "cod_cosif", "descricao", "natureza", "flag_coerente")
        .agg(
            F.count("*").alias("qtd_lancamentos"),
            F.sum("valor_lancamento").cast("decimal(28,2)").alias("valor_bruto"),
            F.sum("valor_assinado").cast("decimal(28,2)").alias("valor_assinado"),
        )
        .withColumn("dt_referencia", F.lit(dt_referencia).cast("date"))
    )


def reconciliacao_por_agencia(silver_dia: DataFrame, dt_referencia) -> DataFrame:
    """Débitos vs. créditos por agência — controle contábil do fechamento."""
    return (
        silver_dia.groupBy("cod_agencia")
        .agg(
            F.sum(F.when(F.col("valor_assinado") > 0, F.col("valor_assinado")).otherwise(0))
            .cast("decimal(28,2)")
            .alias("total_creditos"),
            (-F.sum(F.when(F.col("valor_assinado") < 0, F.col("valor_assinado")).otherwise(0)))
            .cast("decimal(28,2)")
            .alias("total_debitos"),
            F.sum("valor_assinado").cast("decimal(28,2)").alias("liquido"),
            F.count("*").alias("qtd_lancamentos"),
        )
        .withColumn("dt_referencia", F.lit(dt_referencia).cast("date"))
    )
