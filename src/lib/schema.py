"""Schema do contrato de dados e tipagem do Bronze.

O CSV chega com todos os campos como texto; a tipagem aplica o schema do contrato.
Valor não conversível vira NULL — e NULL em campo obrigatório é capturado pela regra
de completude no Silver, com motivo. Nada é descartado na tipagem.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# (campo, tipo do contrato) — todos NOT NULL no contrato
CAMPOS_CONTRATO = [
    ("id_transacao", "string"),
    ("id_contrato", "string"),
    ("id_conta", "string"),
    ("cod_agencia", "string"),
    ("tipo_contrato", "string"),
    ("tipo_lancamento", "string"),
    ("valor_lancamento", "decimal(18,2)"),
    ("dt_lancamento", "timestamp"),
    ("dt_processamento", "date"),
    ("cod_cosif", "string"),
    ("flag_estorno", "boolean"),
    ("id_lote", "string"),
]

NOMES_CAMPOS = [c for c, _ in CAMPOS_CONTRATO]

DOMINIO_TIPO_CONTRATO = ["CC", "POUP", "CDB", "LCI", "CONSORCIO", "SEGURO"]
DOMINIO_TIPO_LANCAMENTO = ["DEBITO", "CREDITO", "TARIFA", "JUROS", "IOF"]


def tipar_contrato(df_texto: DataFrame) -> DataFrame:
    """Aplica o schema do contrato sobre colunas texto (trim + vazio→NULL + cast)."""
    colunas = []
    for campo, tipo in CAMPOS_CONTRATO:
        limpa = F.nullif(F.trim(F.col(campo)), F.lit(""))
        if tipo == "boolean":
            # só "true"/"false" são válidos; qualquer outra coisa vira NULL (→ regra de completude)
            tipada = (
                F.when(F.lower(limpa) == "true", F.lit(True))
                .when(F.lower(limpa) == "false", F.lit(False))
                .otherwise(F.lit(None).cast("boolean"))
            )
        else:
            tipada = limpa.cast(tipo)
        colunas.append(tipada.alias(campo))
    return df_texto.select(*colunas)


def ddl_contrato() -> str:
    """Colunas do contrato em DDL, para criação das tabelas Iceberg."""
    return ", ".join(f"{campo} {tipo}" for campo, tipo in CAMPOS_CONTRATO)
