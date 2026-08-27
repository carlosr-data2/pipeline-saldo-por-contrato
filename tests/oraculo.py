"""Oráculo de verificação: reimplementação INDEPENDENTE da política do pipeline,
em Python puro (csv/decimal/hashlib), sem Spark.

Serve para provar a correção do pipeline por dupla implementação: os números do
Silver (quarentena por motivo) e do Gold (saldos) devem bater exatamente com os
produzidos pelos jobs Spark. Qualquer divergência é bug em um dos dois lados.
"""
import csv
import hashlib
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal

CAMPOS = [
    "id_transacao", "id_contrato", "id_conta", "cod_agencia", "tipo_contrato",
    "tipo_lancamento", "valor_lancamento", "dt_lancamento", "dt_processamento",
    "cod_cosif", "flag_estorno", "id_lote",
]
TIPOS_CREDITO = {"CREDITO", "JUROS"}


def _tipar(linha: dict) -> dict:
    """Espelha lib/schema.tipar_contrato: trim, vazio->None, cast (falha->None)."""
    t = {}
    for campo in CAMPOS:
        v = (linha.get(campo) or "").strip() or None
        t[campo] = v
    try:
        t["valor_lancamento"] = (
            Decimal(t["valor_lancamento"]).quantize(Decimal("0.01")) if t["valor_lancamento"] else None
        )
    except ArithmeticError:
        t["valor_lancamento"] = None
    try:
        t["dt_lancamento"] = datetime.fromisoformat(t["dt_lancamento"]) if t["dt_lancamento"] else None
    except ValueError:
        t["dt_lancamento"] = None
    try:
        t["dt_processamento"] = date.fromisoformat(t["dt_processamento"]) if t["dt_processamento"] else None
    except ValueError:
        t["dt_processamento"] = None
    if t["flag_estorno"] is not None:
        low = t["flag_estorno"].lower()
        t["flag_estorno"] = True if low == "true" else False if low == "false" else None
    return t


def _hash_linha(t: dict) -> str:
    """Espelha lib/dedup.com_hash_linha: sha2(concat_ws('||', colunas), 256).
    concat_ws do Spark IGNORA nulos (não emite separador para eles)."""
    partes = []
    for campo in CAMPOS:
        v = t[campo]
        if v is None:
            continue
        if isinstance(v, bool):
            partes.append("true" if v else "false")
        elif isinstance(v, datetime):
            partes.append(v.strftime("%Y-%m-%d %H:%M:%S"))
        else:
            partes.append(str(v))
    return hashlib.sha256("||".join(partes).encode()).hexdigest()


def _motivos_base(t: dict, dominio: set, publicados: set) -> list:
    motivos = [f"CAMPO_OBRIGATORIO_NULO:{c}" for c in CAMPOS if t[c] is None]
    if t["valor_lancamento"] is not None and t["valor_lancamento"] <= 0:
        motivos.append("VALOR_NAO_POSITIVO")
    if t["dt_lancamento"] and t["dt_processamento"] and t["dt_lancamento"].date() > t["dt_processamento"]:
        motivos.append("DATA_LANCAMENTO_POSTERIOR_AO_PROCESSAMENTO")
    if t["cod_cosif"] is not None and t["cod_cosif"] not in dominio:
        motivos.append("COSIF_FORA_DO_DOMINIO")
    if t["id_transacao"] in publicados:
        motivos.append("ID_TRANSACAO_JA_PROCESSADO")
    return motivos


def valor_assinado(t: dict) -> Decimal:
    sinal = Decimal(1) if t["tipo_lancamento"] in TIPOS_CREDITO else Decimal(-1)
    if t["flag_estorno"]:
        sinal = -sinal
    return t["valor_lancamento"] * sinal


def processar(caminho_csv: str, caminho_cosif: str, lookback_dias: int = 7):
    """Processa cada partição em ordem cronológica, como o pipeline faz.

    Retorna por partição: contagens (silver, quarentena, por motivo) e, por
    contrato/conta/agência, o saldo acumulado esperado até aquela data.
    """
    with open(caminho_cosif, newline="") as f:
        dominio = {r["cod_cosif"].strip() for r in csv.DictReader(f)}

    por_dia = defaultdict(list)
    with open(caminho_csv, newline="") as f:
        for linha in csv.DictReader(f):
            t = _tipar(linha)
            chave_dia = t["dt_processamento"]
            por_dia[chave_dia].append(t)

    publicados_por_dia = {}   # dt -> set de ids publicados naquele dia
    resultado = {}
    saldo_contrato = defaultdict(Decimal)
    conta_do_contrato = {}

    for dia in sorted(d for d in por_dia if d is not None):
        linhas = por_dia[dia]
        inicio_lookback = dia - timedelta(days=lookback_dias)
        publicados_lookback = set()
        for d_ant, ids in publicados_por_dia.items():
            if inicio_lookback <= d_ant < dia:
                publicados_lookback |= ids

        avaliadas = []
        for t in linhas:
            motivos = _motivos_base(t, dominio, publicados_lookback)
            avaliadas.append((t, motivos))

        # dedup intra-lote: só entre linhas válidas nas demais regras
        validas_por_id = defaultdict(list)
        for t, motivos in avaliadas:
            if not motivos:
                validas_por_id[t["id_transacao"]].append(t)
        perdedoras = set()
        for grupo in validas_por_id.values():
            grupo.sort(
                key=lambda t: (t["dt_lancamento"] is None, t["dt_lancamento"] or datetime.min, _hash_linha(t))
            )
            for t in grupo[1:]:
                perdedoras.add(id(t))

        silver, quarentena = [], []
        contagem_motivos = defaultdict(int)
        for t, motivos in avaliadas:
            if not motivos and id(t) in perdedoras:
                motivos = ["ID_TRANSACAO_DUPLICADO_NO_LOTE"]
            for m in motivos:
                contagem_motivos[m] += 1
            (quarentena if motivos else silver).append(t)

        publicados_por_dia[dia] = {t["id_transacao"] for t in silver}

        for t in silver:
            saldo_contrato[t["id_contrato"]] += valor_assinado(t)
            conta_do_contrato[t["id_contrato"]] = t["id_conta"]

        recon_agencia = defaultdict(lambda: [Decimal(0), Decimal(0)])  # creditos, debitos
        for t in silver:
            va = valor_assinado(t)
            if va > 0:
                recon_agencia[t["cod_agencia"]][0] += va
            else:
                recon_agencia[t["cod_agencia"]][1] += -va

        saldo_conta = defaultdict(Decimal)
        for contrato, saldo in saldo_contrato.items():
            saldo_conta[conta_do_contrato[contrato]] += saldo

        resultado[dia] = {
            "total": len(linhas),
            "silver": len(silver),
            "quarentena": len(quarentena),
            "motivos": dict(sorted(contagem_motivos.items())),
            "saldo_contrato": dict(saldo_contrato),
            "saldo_conta": dict(saldo_conta),
            "reconciliacao_agencia": {a: tuple(v) for a, v in recon_agencia.items()},
        }
    return resultado
