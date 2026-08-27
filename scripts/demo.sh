#!/usr/bin/env bash
# Demo de ponta a ponta na trilha local: bronze → silver (gate) → gold, 3 dias.
# Nenhuma chamada de API AWS. Reexecutar é idempotente (INSERT OVERWRITE de partição).
set -euo pipefail
cd "$(dirname "$0")/.."

DIAS=(2026-08-20 2026-08-21 2026-08-22)

echo "==> [1/3] Bronze: tipagem do contrato + partição por dt_processamento (Iceberg V3)"
python src/jobs/bronze_ingest.py \
  --input dados/fin_contabilidade_saldo_contrato.csv \
  --cosif dados/cosif_dominio.csv

for dia in "${DIAS[@]}"; do
  echo "==> [2/3] Silver ${dia}: 5 regras do contrato + dedup + quarentena + gate"
  python src/jobs/silver_quality.py --dt "${dia}"
  echo "==> [3/3] Gold ${dia}: saldo incremental + classificação COSIF + reconciliação"
  python src/jobs/gold_saldo.py --dt "${dia}"
done

echo "==> Relatório final da demonstração"
python scripts/relatorio_demo.py
