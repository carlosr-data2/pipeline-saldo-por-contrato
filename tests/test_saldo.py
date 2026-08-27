from datetime import date
from decimal import Decimal

from lib.saldo import (
    com_valor_assinado,
    movimento_por_contrato,
    reconciliacao_por_agencia,
    saldo_por_conta,
    snapshot_saldo,
)
from lib.schema import tipar_contrato
from test_schema import LINHA_VALIDA, df_texto


def silver(spark, *linhas):
    return com_valor_assinado(tipar_contrato(df_texto(spark, *linhas)))


def test_convencao_de_sinal(spark):
    casos = [
        ("CREDITO", "false", Decimal("100.50")),
        ("JUROS", "false", Decimal("100.50")),
        ("DEBITO", "false", Decimal("-100.50")),
        ("TARIFA", "false", Decimal("-100.50")),
        ("IOF", "false", Decimal("-100.50")),
        ("CREDITO", "true", Decimal("-100.50")),  # estorno inverte o sinal
        ("DEBITO", "true", Decimal("100.50")),
    ]
    linhas = [dict(LINHA_VALIDA, tipo_lancamento=t, flag_estorno=f) for t, f, _ in casos]
    resultado = silver(spark, *linhas).select("tipo_lancamento", "flag_estorno", "valor_assinado").collect()
    obtido = {(r["tipo_lancamento"], r["flag_estorno"]): r["valor_assinado"] for r in resultado}
    for tipo, flag, esperado in casos:
        assert obtido[(tipo, flag == "true")] == esperado, (tipo, flag)


def test_movimento_liquido_por_contrato(spark):
    df = silver(
        spark,
        dict(LINHA_VALIDA, tipo_lancamento="CREDITO", valor_lancamento="100.00"),
        dict(LINHA_VALIDA, tipo_lancamento="TARIFA", valor_lancamento="30.00"),
    )
    mov = movimento_por_contrato(df).collect()[0]
    assert mov["movimento_dia"] == Decimal("70.00")
    assert mov["qtd_lancamentos_dia"] == 2


def test_snapshot_carrega_saldo_anterior_e_soma_movimento(spark):
    anterior = spark.createDataFrame(
        [("c1", "conta1", Decimal("50.00")), ("c2", "conta2", Decimal("10.00"))],
        "id_contrato string, id_conta string, saldo decimal(28,2)",
    )
    movimento = movimento_por_contrato(
        silver(
            spark,
            dict(LINHA_VALIDA, id_contrato="c1", id_conta="conta1", valor_lancamento="25.00"),
            dict(LINHA_VALIDA, id_contrato="c3", id_conta="conta3", valor_lancamento="5.00"),
        )
    )
    snap = {r["id_contrato"]: r for r in snapshot_saldo(anterior, movimento, date(2026, 8, 21)).collect()}
    assert snap["c1"]["saldo"] == Decimal("75.00")   # anterior + movimento
    assert snap["c2"]["saldo"] == Decimal("10.00")   # carry-forward sem movimento
    assert snap["c2"]["movimento_dia"] == Decimal("0.00")
    assert snap["c3"]["saldo"] == Decimal("5.00")    # contrato novo
    assert all(r["dt_referencia"] == date(2026, 8, 21) for r in snap.values())


def test_saldo_por_conta_agrega_contratos(spark):
    anterior = spark.createDataFrame([], "id_contrato string, id_conta string, saldo decimal(28,2)")
    movimento = movimento_por_contrato(
        silver(
            spark,
            dict(LINHA_VALIDA, id_contrato="c1", id_conta="A", valor_lancamento="10.00"),
            dict(LINHA_VALIDA, id_contrato="c2", id_conta="A", valor_lancamento="20.00"),
            dict(LINHA_VALIDA, id_contrato="c3", id_conta="B", valor_lancamento="5.00"),
        )
    )
    snap = snapshot_saldo(anterior, movimento, date(2026, 8, 20))
    contas = {r["id_conta"]: r for r in saldo_por_conta(snap).collect()}
    assert contas["A"]["saldo"] == Decimal("30.00")
    assert contas["A"]["qtd_contratos"] == 2
    assert contas["B"]["saldo"] == Decimal("5.00")


def test_reconciliacao_separa_debitos_e_creditos(spark):
    df = silver(
        spark,
        dict(LINHA_VALIDA, cod_agencia="0001", tipo_lancamento="CREDITO", valor_lancamento="100.00"),
        dict(LINHA_VALIDA, cod_agencia="0001", tipo_lancamento="DEBITO", valor_lancamento="40.00"),
        dict(LINHA_VALIDA, cod_agencia="0002", tipo_lancamento="IOF", valor_lancamento="7.00"),
    )
    recon = {r["cod_agencia"]: r for r in reconciliacao_por_agencia(df, date(2026, 8, 20)).collect()}
    assert recon["0001"]["total_creditos"] == Decimal("100.00")
    assert recon["0001"]["total_debitos"] == Decimal("40.00")
    assert recon["0001"]["liquido"] == Decimal("60.00")
    assert recon["0002"]["total_creditos"] == Decimal("0.00")
    assert recon["0002"]["total_debitos"] == Decimal("7.00")
    assert recon["0002"]["liquido"] == Decimal("-7.00")
