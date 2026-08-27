from lib.schema import NOMES_CAMPOS, tipar_contrato

LINHA_VALIDA = {
    "id_transacao": "tx-1",
    "id_contrato": "CTA1-CTR0",
    "id_conta": "CTA1",
    "cod_agencia": "0001",
    "tipo_contrato": "CC",
    "tipo_lancamento": "CREDITO",
    "valor_lancamento": "100.50",
    "dt_lancamento": "2026-08-20 10:00:00",
    "dt_processamento": "2026-08-20",
    "cod_cosif": "1.1.1.00.0",
    "flag_estorno": "false",
    "id_lote": "LOTE-1",
}


def df_texto(spark, *linhas):
    return spark.createDataFrame(
        [tuple(linha.get(c) for c in NOMES_CAMPOS) for linha in linhas],
        ", ".join(f"{c} string" for c in NOMES_CAMPOS),
    )


def test_tipagem_aplica_o_schema_do_contrato(spark):
    tipado = tipar_contrato(df_texto(spark, LINHA_VALIDA))
    tipos = dict(tipado.dtypes)
    assert tipos["valor_lancamento"] == "decimal(18,2)"
    assert tipos["dt_lancamento"] == "timestamp"
    assert tipos["dt_processamento"] == "date"
    assert tipos["flag_estorno"] == "boolean"
    linha = tipado.collect()[0]
    assert str(linha["valor_lancamento"]) == "100.50"
    assert linha["flag_estorno"] is False


def test_vazio_e_espacos_viram_nulo(spark):
    linha = dict(LINHA_VALIDA, id_conta="   ", cod_cosif="")
    tipado = tipar_contrato(df_texto(spark, linha)).collect()[0]
    assert tipado["id_conta"] is None
    assert tipado["cod_cosif"] is None


def test_valor_e_boolean_invalidos_viram_nulo_sem_descartar_linha(spark):
    linha = dict(LINHA_VALIDA, valor_lancamento="abc", flag_estorno="talvez")
    resultado = tipar_contrato(df_texto(spark, linha))
    assert resultado.count() == 1  # tipagem nunca descarta
    tipado = resultado.collect()[0]
    assert tipado["valor_lancamento"] is None
    assert tipado["flag_estorno"] is None
