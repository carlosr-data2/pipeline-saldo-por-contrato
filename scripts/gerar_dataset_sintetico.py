"""Gera um dataset sintético FATOR vezes maior que o de exemplo, com o mesmo perfil:
schema do contrato com campos em texto, 3 partições diárias, cardinalidades escaladas
(contas, 3 a 5 contratos por conta, 46 agências) e as violações de qualidade nas
taxas medidas na análise exploratória: id_conta nulo, COSIF inexistente, data
futura (+2 dias), valor negativo/zero, duplicatas no lote e duplicatas cruzando dias,
sempre com payload divergente. COSIF sorteado independente do tipo (5/6 incoerente).

Propositalmente em Python puro (stdlib, sem Spark), determinístico por --seed.
É o insumo do laboratório de vazão (docs/labs_aws.md): medir o pipeline num volume
maior que o de exemplo e extrapolar para os ~300 M/dia de produção.

Uso:
    python3 scripts/gerar_dataset_sintetico.py --fator 10 --saida raw/sintetico_x10.csv
    python3 scripts/gerar_dataset_sintetico.py --fator 10 --contas-quentes 0.2   # skew: 20% em 20 contas
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import os
import random
import time
import uuid
from datetime import date, datetime, timedelta

CAMPOS = [
    "id_transacao", "id_contrato", "id_conta", "cod_agencia", "tipo_contrato",
    "tipo_lancamento", "valor_lancamento", "dt_lancamento", "dt_processamento",
    "cod_cosif", "flag_estorno", "id_lote",
]
LINHAS_BASE_POR_DIA = 66_666   # o dataset de exemplo: 3 × 66.666
CONTAS_BASE = 50_000           # ids CTA000000000..CTA000049999
TIPOS_CONTRATO = ["CC", "POUP", "CDB", "LCI", "CONSORCIO", "SEGURO"]
TIPOS_LANCAMENTO = ["DEBITO", "CREDITO", "TARIFA", "JUROS", "IOF"]
COSIF_VALIDOS = ["1.1.1.00.0", "1.2.1.00.0", "1.3.1.00.0", "1.3.2.00.0",
                 "4.1.1.00.0", "4.2.1.00.0", "8.1.1.00.0", "7.1.1.00.0"]
COSIF_INEXISTENTE = "9.9.9.99.9"
AGENCIAS = sorted(f"{a:04d}" for a in random.Random(46).sample(range(1, 500), 46))
TAXA_ESTORNO = 0.049

# Taxas por dia medidas no dataset de exemplo (docs/analise_exploratoria.md):
# somam ~8,1% de linhas em quarentena, abaixo do gate de 10%, como no original.
TAXAS = {
    "id_conta_nulo": 0.0122,
    "cosif_inexistente": 0.0163,
    "data_futura": 0.0164,
    "valor_negativo": 0.0100,
    "valor_zero": 0.0105,
    "dup_no_lote": 0.0153,          # dia 1; nos demais dias a duplicata vem do dia anterior
    "dup_no_lote_outros_dias": 0.0001,
    "dup_do_dia_anterior": 0.0154,
}


def _mix(x: int) -> int:
    """Hash inteiro barato e determinístico (atributos fixos por conta/contrato)."""
    x = (x * 0x9E3779B1 + 0x7F4A7C15) & 0xFFFFFFFF
    x ^= x >> 15
    x = (x * 0x2C1B3C6D) & 0xFFFFFFFF
    return x ^ (x >> 12)


def _contratos_da_conta(conta: int) -> int:
    """3 a 5 contratos por conta, na proporção observada (P(≥4)=0,61; P(5)=0,28)."""
    p = _mix(conta) % 100
    return 3 + (p < 61) + (p < 28)


def _id_transacao(seed: int, dia: int, i: int) -> str:
    """Id determinístico por (seed, dia, linha): permite referenciar uma linha anterior
    (duplicata) sem guardar milhões de ids em memória."""
    digest = hashlib.blake2b(f"{seed}:{dia}:{i}".encode(), digest_size=16).digest()
    return str(uuid.UUID(bytes=digest))


def gerar(
    saida: str,
    fator: float = 10.0,
    dias: int = 3,
    inicio: date | None = None,
    seed: int = 42,
    contas_quentes: float = 0.0,
    qtd_contas_quentes: int = 20,
) -> dict:
    inicio = inicio or date(2026, 8, 20)
    rng = random.Random(seed)
    n_por_dia = int(round(LINHAS_BASE_POR_DIA * fator))
    n_contas = max(100, int(round(CONTAS_BASE * fator)))
    quentes = rng.sample(range(n_contas), min(qtd_contas_quentes, n_contas)) if contas_quentes > 0 else []
    injetadas = dict.fromkeys(TAXAS, 0)
    inicio_exec = time.monotonic()

    os.makedirs(os.path.dirname(os.path.abspath(saida)), exist_ok=True)
    with open(saida, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(CAMPOS)
        for d in range(dias):
            dia = inicio + timedelta(days=d)
            base_dia = datetime(dia.year, dia.month, dia.day)
            taxas = dict(TAXAS)
            if d == 0:
                taxas["dup_do_dia_anterior"] = 0.0
            else:
                taxas["dup_no_lote"] = taxas["dup_no_lote_outros_dias"]
            taxas.pop("dup_no_lote_outros_dias")
            marcas = {
                nome: set(rng.sample(range(n_por_dia), int(round(taxa * n_por_dia))))
                for nome, taxa in taxas.items()
            }
            for nome, idx in marcas.items():
                injetadas[nome] += len(idx)

            for i in range(n_por_dia):
                if quentes and rng.random() < contas_quentes:
                    conta = quentes[rng.randrange(len(quentes))]
                else:
                    conta = rng.randrange(n_contas)
                ctr = rng.randrange(_contratos_da_conta(conta))
                id_conta = f"CTA{conta:09d}"
                id_contrato = f"{id_conta}-CTR{ctr}"
                tipo_contrato = TIPOS_CONTRATO[_mix(conta * 8 + ctr) % len(TIPOS_CONTRATO)]
                valor = rng.randint(100, 500_000) / 100
                dt_lancamento = base_dia + timedelta(minutes=rng.randrange(24 * 60))
                cod_cosif = COSIF_VALIDOS[rng.randrange(len(COSIF_VALIDOS))]
                id_tx = _id_transacao(seed, d, i)

                if i in marcas["id_conta_nulo"]:
                    id_conta = ""
                if i in marcas["cosif_inexistente"]:
                    cod_cosif = COSIF_INEXISTENTE
                if i in marcas["data_futura"]:
                    dt_lancamento += timedelta(days=2)
                if i in marcas["valor_negativo"]:
                    valor = -valor
                elif i in marcas["valor_zero"]:
                    valor = 0.0
                if i > 0 and i in marcas["dup_no_lote"]:
                    id_tx = _id_transacao(seed, d, rng.randrange(i))       # colide com linha anterior do dia
                elif d > 0 and i in marcas["dup_do_dia_anterior"]:
                    id_tx = _id_transacao(seed, d - 1, rng.randrange(n_por_dia))  # colide com o dia anterior

                w.writerow([
                    id_tx,
                    id_contrato,
                    id_conta,
                    AGENCIAS[_mix(conta + 7) % len(AGENCIAS)],
                    tipo_contrato,
                    TIPOS_LANCAMENTO[rng.randrange(len(TIPOS_LANCAMENTO))],
                    f"{valor:.2f}",
                    dt_lancamento.strftime("%Y-%m-%d %H:%M:00"),
                    dia.isoformat(),
                    cod_cosif,
                    "true" if rng.random() < TAXA_ESTORNO else "false",
                    f"LOTE-{dia.isoformat()}",
                ])

    return {
        "arquivo": saida,
        "bytes": os.path.getsize(saida),
        "fator": fator,
        "dias": [(inicio + timedelta(days=d)).isoformat() for d in range(dias)],
        "linhas_por_dia": n_por_dia,
        "linhas": n_por_dia * dias,
        "contas": n_contas,
        "contas_quentes": len(quentes),
        "injetadas": injetadas,
        "segundos": round(time.monotonic() - inicio_exec, 1),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Dataset sintético no perfil do dataset de exemplo")
    parser.add_argument(
        "--fator", type=float, default=10.0, help="múltiplo do volume do dataset de exemplo (10 = ~2 M linhas)"
    )
    parser.add_argument("--dias", type=int, default=3)
    parser.add_argument("--inicio", type=date.fromisoformat, default=date(2026, 8, 20))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--saida", default=None, help="padrão: raw/sintetico_x<fator>.csv")
    parser.add_argument(
        "--contas-quentes", type=float, default=0.0,
        help="fração das linhas concentrada em poucas contas (skew); 0 = uniforme",
    )
    parser.add_argument("--qtd-contas-quentes", type=int, default=20)
    args = parser.parse_args()

    fator_txt = f"{args.fator:g}"
    saida = args.saida or os.path.join("raw", f"sintetico_x{fator_txt}.csv")
    resumo = gerar(
        saida, args.fator, args.dias, args.inicio, args.seed, args.contas_quentes, args.qtd_contas_quentes
    )
    fmt = lambda n: f"{n:,}".replace(",", ".")  # noqa: E731
    print(f"gerado: {resumo['arquivo']} ({resumo['bytes'] / 1e6:.0f} MB) em {resumo['segundos']} s")
    print(f"  {fmt(resumo['linhas'])} linhas = {len(resumo['dias'])} dias × {fmt(resumo['linhas_por_dia'])}"
          f" · {fmt(resumo['contas'])} contas · {resumo['contas_quentes']} contas quentes")
    print("  violações injetadas (total nos dias):")
    for nome, qtd in resumo["injetadas"].items():
        print(f"    {nome:<24} {fmt(qtd):>10}")


if __name__ == "__main__":
    main()
