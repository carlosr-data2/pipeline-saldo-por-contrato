#!/usr/bin/env bash
# Demo de ponta a ponta na trilha local: bronze → silver (gate) → gold, 3 dias.
# Nenhuma chamada de API AWS. Reexecutar é idempotente (INSERT OVERWRITE de partição).
#
#   ORIGEM=csv     (padrão) CSV único, um Bronze para os três dias — como chega da origem
#   ORIGEM=parquet converte o CSV para Parquet particionado por dt_processamento (o formato
#                  de origem do contrato) e roda o Bronze POR DIA, só a partição do dia (ADR-014)
set -euo pipefail
cd "$(dirname "$0")/.."

ORIGEM="${ORIGEM:-csv}"
RAW="${SALDO_RAW:-raw}"
CSV=dados/fin_contabilidade_saldo_contrato.csv
COSIF=dados/cosif_dominio.csv
PARQUET="$RAW/fin_contabilidade_saldo_contrato"
DIAS=(2026-08-20 2026-08-21 2026-08-22)

if [ "$ORIGEM" = "parquet" ]; then
  echo "==> [0/3] Origem no formato do contrato: CSV -> Parquet particionado por dt_processamento"
  python scripts/csv_para_parquet_particionado.py --input "$CSV" --destino "$PARQUET"
else
  echo "==> [1/3] Bronze: tipagem do contrato + partição por dt_processamento (Iceberg V3), CSV único"
  python src/jobs/bronze_ingest.py --input "$CSV" --cosif "$COSIF"
fi

for dia in "${DIAS[@]}"; do
  if [ "$ORIGEM" = "parquet" ]; then
    echo "==> [1/3] Bronze ${dia}: só a partição do dia (poda na origem, overwrite de uma partição)"
    python src/jobs/bronze_ingest.py --input "$PARQUET" --formato parquet --cosif "$COSIF" --dt "${dia}"
  fi
  echo "==> [2/3] Silver ${dia}: 5 regras do contrato + dedup + quarentena + gate"
  python src/jobs/silver_quality.py --dt "${dia}"
  echo "==> [3/3] Gold ${dia}: saldo incremental + classificação COSIF + reconciliação"
  python src/jobs/gold_saldo.py --dt "${dia}"
done

echo "==> Relatório final da demonstração"
python scripts/relatorio_demo.py
