
from lib.dq import COL_MOTIVOS, aplicar_regras
from lib.schema import tipar_contrato
from test_schema import LINHA_VALIDA, df_texto


def dominio(spark):
    return spark.createDataFrame([("1.1.1.00.0",), ("4.1.1.00.0",)], "cod_cosif string")


def avaliar(spark, *linhas, historico=None):
    df = tipar_contrato(df_texto(spark, *linhas))
    hist = (
        spark.createDataFrame([(i,) for i in historico], "id_transacao string")
        if historico is not None
        else None
    )
    saida = aplicar_regras(df, dominio(spark), hist)
    return {
        (r["id_transacao"], str(r["valor_lancamento"])): list(r[COL_MOTIVOS])
        for r in saida.collect()
    }


def test_linha_valida_sem_motivos(spark):
    motivos = avaliar(spark, LINHA_VALIDA)
    assert motivos == {("tx-1", "100.50"): []}


def test_valor_nao_positivo(spark):
    resultado = avaliar(spark, dict(LINHA_VALIDA, valor_lancamento="-5.00"))
    assert resultado[("tx-1", "-5.00")] == ["VALOR_NAO_POSITIVO"]
    resultado = avaliar(spark, dict(LINHA_VALIDA, valor_lancamento="0.00"))
    assert resultado[("tx-1", "0.00")] == ["VALOR_NAO_POSITIVO"]


def test_estorno_nao_e_violacao(spark):
    resultado = avaliar(spark, dict(LINHA_VALIDA, flag_estorno="true"))
    assert resultado[("tx-1", "100.50")] == []


def test_data_lancamento_posterior_ao_processamento(spark):
    linha = dict(LINHA_VALIDA, dt_lancamento="2026-08-21 00:10:00")
    resultado = avaliar(spark, linha)
    assert resultado[("tx-1", "100.50")] == ["DATA_LANCAMENTO_POSTERIOR_AO_PROCESSAMENTO"]


def test_cosif_fora_do_dominio(spark):
    resultado = avaliar(spark, dict(LINHA_VALIDA, cod_cosif="9.9.9.99.9"))
    assert resultado[("tx-1", "100.50")] == ["COSIF_FORA_DO_DOMINIO"]


def test_campo_obrigatorio_nulo(spark):
    resultado = avaliar(spark, dict(LINHA_VALIDA, id_conta=""))
    assert resultado[("tx-1", "100.50")] == ["CAMPO_OBRIGATORIO_NULO:id_conta"]


def test_multiplas_violacoes_acumulam_motivos(spark):
    linha = dict(LINHA_VALIDA, valor_lancamento="-1.00", cod_cosif="9.9.9.99.9")
    resultado = avaliar(spark, linha)
    assert resultado[("tx-1", "-1.00")] == ["VALOR_NAO_POSITIVO", "COSIF_FORA_DO_DOMINIO"]


def test_dedup_entre_validas_vence_a_mais_antiga(spark):
    cedo = dict(LINHA_VALIDA, dt_lancamento="2026-08-20 08:00:00", valor_lancamento="1.00")
    tarde = dict(LINHA_VALIDA, dt_lancamento="2026-08-20 09:00:00", valor_lancamento="2.00")
    resultado = avaliar(spark, tarde, cedo)
    assert resultado[("tx-1", "1.00")] == []
    assert resultado[("tx-1", "2.00")] == ["ID_TRANSACAO_DUPLICADO_NO_LOTE"]


def test_linha_invalida_nao_vence_nem_condena_a_valida(spark):
    # A inválida chegou antes, mas unicidade é sobre o que entra no razão:
    # a válida publica; a inválida cai pela própria violação, não por duplicidade.
    invalida_cedo = dict(LINHA_VALIDA, dt_lancamento="2026-08-20 08:00:00", valor_lancamento="-9.00")
    valida_tarde = dict(LINHA_VALIDA, dt_lancamento="2026-08-20 09:00:00", valor_lancamento="7.00")
    resultado = avaliar(spark, invalida_cedo, valida_tarde)
    assert resultado[("tx-1", "7.00")] == []
    assert resultado[("tx-1", "-9.00")] == ["VALOR_NAO_POSITIVO"]


def test_id_ja_publicado_no_historico_e_rejeitado(spark):
    resultado = avaliar(spark, LINHA_VALIDA, historico=["tx-1"])
    assert resultado[("tx-1", "100.50")] == ["ID_TRANSACAO_JA_PROCESSADO"]


def test_reenvio_corrigido_apos_quarentena_e_aceito(spark):
    # id "tx-1" nunca foi PUBLICADO (não está no histórico do Silver) — o reenvio
    # corrigido de um registro quarentenado entra normalmente.
    resultado = avaliar(spark, LINHA_VALIDA, historico=["outro-id"])
    assert resultado[("tx-1", "100.50")] == []


def test_dedup_e_deterministica_em_empate_de_timestamp(spark):
    a = dict(LINHA_VALIDA, valor_lancamento="1.00")
    b = dict(LINHA_VALIDA, valor_lancamento="2.00")
    r1 = avaliar(spark, a, b)
    r2 = avaliar(spark, b, a)  # ordem de chegada invertida
    assert r1 == r2  # vencedor decidido por hash da linha, não pela ordem do arquivo
