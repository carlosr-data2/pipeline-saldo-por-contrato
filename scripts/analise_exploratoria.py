"""Análise exploratória do dataset — recomputa os números de docs/analise_exploratoria.md.

Propositalmente em Python puro (stdlib, sem Spark): uma contagem independente do
pipeline, no mesmo espírito do oráculo dos testes. Qualquer pessoa confere os
achados que fundamentaram as decisões de arquitetura com:

    python3 scripts/analise_exploratoria.py
"""
import csv
from collections import Counter, defaultdict
from pathlib import Path

DADOS = Path(__file__).resolve().parent.parent / "dados"


def fmt(n: int) -> str:
    return f"{n:,}".replace(",", ".")


def vazio(v: str | None) -> bool:
    return v is None or v.strip() == ""


def main() -> None:
    with open(DADOS / "cosif_dominio.csv", newline="", encoding="utf-8") as f:
        referencial = list(csv.DictReader(f))
    dominio = {r["cod_cosif"] for r in referencial}
    tipo_por_cosif = {r["cod_cosif"]: r["tipo_contrato_associado"] for r in referencial}

    with open(DADOS / "fin_contabilidade_saldo_contrato.csv", newline="", encoding="utf-8") as f:
        linhas = list(csv.DictReader(f))

    print("=== Base ===")
    print(f"registros: {fmt(len(linhas))}")
    for dt, qtd in sorted(Counter(r["dt_processamento"] for r in linhas).items()):
        print(f"  {dt}: {fmt(qtd)}")
    print(f"referencial COSIF: {len(dominio)} códigos")

    print("\n=== Violações das regras do contrato ===")
    nulos = Counter(
        campo for r in linhas for campo, v in r.items() if vazio(v)
    )
    for campo, qtd in nulos.items():
        print(f"campo obrigatório nulo ({campo}): {fmt(qtd)}")

    neg = sum(1 for r in linhas if not vazio(r["valor_lancamento"]) and float(r["valor_lancamento"]) < 0)
    zero = sum(1 for r in linhas if not vazio(r["valor_lancamento"]) and float(r["valor_lancamento"]) == 0)
    print(f"valor_lancamento <= 0: {fmt(neg + zero)} ({fmt(neg)} negativos + {fmt(zero)} zeros)")

    data_ruim = sum(
        1 for r in linhas
        if not vazio(r["dt_lancamento"]) and r["dt_lancamento"][:10] > r["dt_processamento"]
    )
    print(f"dt_lancamento posterior ao processamento: {fmt(data_ruim)}")

    fora = Counter(
        r["cod_cosif"].strip() for r in linhas
        if not vazio(r["cod_cosif"]) and r["cod_cosif"].strip() not in dominio
    )
    print(f"cod_cosif fora do domínio: {fmt(sum(fora.values()))} (valores: {dict(fora)})")

    print("\n=== Duplicatas de id_transacao ===")
    grupos = defaultdict(list)
    for r in linhas:
        grupos[r["id_transacao"]].append(r)
    dups = {k: v for k, v in grupos.items() if len(v) > 1}
    excedentes = sum(len(v) - 1 for v in dups.values())
    cruzam = sum(1 for v in dups.values() if len({x["dt_processamento"] for x in v}) > 1)
    identicas = sum(
        1 for v in dups.values()
        if all(tuple(x.values()) == tuple(v[0].values()) for x in v[1:])
    )
    print(f"grupos com id repetido: {fmt(len(dups))} ({fmt(excedentes)} linhas excedentes)")
    print(f"grupos com conteúdo 100% idêntico: {fmt(identicas)}"
          " -> são colisões com payload divergente, não reentregas")
    print(f"grupos cruzando dias de processamento: {fmt(cruzam)}"
          f" ({100 * cruzam / len(dups):.0f}% — dedup só-no-lote não bastaria)")

    print("\n=== Incoerência COSIF x tipo de contrato (não é regra do contrato) ===")
    no_dominio = [
        r for r in linhas if not vazio(r["cod_cosif"]) and r["cod_cosif"].strip() in dominio
    ]
    incoerentes = sum(
        1 for r in no_dominio
        if r["tipo_contrato"].strip() != tipo_por_cosif[r["cod_cosif"].strip()]
    )
    pct = 100 * incoerentes / len(no_dominio)
    print(f"registros com código válido porém de OUTRO tipo de contrato: "
          f"{fmt(incoerentes)} de {fmt(len(no_dominio))} ({pct:.1f}%)")
    print("-> medido e reportado como observação; a decisão é do data owner (ADR-008)")


if __name__ == "__main__":
    main()
