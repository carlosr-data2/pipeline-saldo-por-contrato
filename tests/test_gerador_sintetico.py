"""O gerador sintético reproduz o perfil do dataset de exemplo (schema, partições diárias,
taxas de violação abaixo do gate) e é determinístico por seed."""
import csv
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import gerar_dataset_sintetico as gerador  # noqa: E402

from lib.schema import NOMES_CAMPOS  # noqa: E402


def test_gerador_reproduz_o_perfil_do_dataset(tmp_path):
    saida = tmp_path / "sintetico.csv"
    resumo = gerador.gerar(str(saida), fator=0.03, dias=3, seed=7)  # ~2.000 linhas/dia

    with open(saida, newline="") as f:
        leitor = csv.DictReader(f)
        assert leitor.fieldnames == NOMES_CAMPOS
        linhas = list(leitor)
    n = len(linhas)
    assert n == resumo["linhas"] == 3 * resumo["linhas_por_dia"]

    por_dia = Counter(r["dt_processamento"] for r in linhas)
    assert set(por_dia) == {"2026-08-20", "2026-08-21", "2026-08-22"}
    assert len(set(por_dia.values())) == 1  # partições do mesmo tamanho, como no original

    assert 0.008 <= sum(1 for r in linhas if r["id_conta"] == "") / n <= 0.016
    assert 0.012 <= sum(1 for r in linhas if r["cod_cosif"] == "9.9.9.99.9") / n <= 0.021
    assert 0.012 <= sum(1 for r in linhas if r["dt_lancamento"][:10] > r["dt_processamento"]) / n <= 0.021
    assert 0.016 <= sum(1 for r in linhas if float(r["valor_lancamento"]) <= 0) / n <= 0.026
    assert all(r["tipo_contrato"] in gerador.TIPOS_CONTRATO for r in linhas)
    assert all(r["tipo_lancamento"] in gerador.TIPOS_LANCAMENTO for r in linhas)

    ids = Counter(r["id_transacao"] for r in linhas)
    duplicados = {i for i, q in ids.items() if q > 1}
    dias_por_id = {i: {r["dt_processamento"] for r in linhas if r["id_transacao"] == i} for i in duplicados}
    assert duplicados and any(len(d) > 1 for d in dias_por_id.values())  # há colisão cruzando dias

    # o que o pipeline quarentenaria (violações + linhas excedentes de id repetido) fica
    # abaixo do gate de 10%, como no dataset original (~8%)
    violacoes = sum(
        1
        for r in linhas
        if r["id_conta"] == ""
        or r["cod_cosif"] == "9.9.9.99.9"
        or r["dt_lancamento"][:10] > r["dt_processamento"]
        or float(r["valor_lancamento"]) <= 0
    )
    excedentes = sum(q - 1 for q in ids.values() if q > 1)
    assert 0.06 <= (violacoes + excedentes) / n <= 0.10


def test_gerador_e_deterministico_por_seed(tmp_path):
    a, b = tmp_path / "a.csv", tmp_path / "b.csv"
    gerador.gerar(str(a), fator=0.01, seed=3)
    gerador.gerar(str(b), fator=0.01, seed=3)
    assert a.read_bytes() == b.read_bytes()
    gerador.gerar(str(b), fator=0.01, seed=4)
    assert a.read_bytes() != b.read_bytes()
