"""Converte o CSV único de exemplo para o formato de origem do contrato: Parquet
particionado por dt_processamento (raw/<nome>/dt_processamento=YYYY-MM-DD/*.parquet).

Todos os campos continuam como TEXTO: a tipagem e a validação são responsabilidade
do Bronze (especificação). Aqui só se reproduz o layout de entrega — nada é validado,
convertido ou descartado. Serve à trilha local (ORIGEM=parquet make demo) e à
publicação em raw/ na AWS (make aws-publicar-origem). ADR-014.

Uso:
    python scripts/csv_para_parquet_particionado.py --input dados/fin_contabilidade_saldo_contrato.csv \
        --destino raw/fin_contabilidade_saldo_contrato [--arquivos-por-particao 4]
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pyspark.sql import SparkSession  # noqa: E402

from lib.schema import NOMES_CAMPOS  # noqa: E402


def converter(spark, caminho_csv: str, destino: str, arquivos_por_particao: int | None = None) -> dict:
    """Escreve o layout particionado e devolve {partição: linhas} lido de volta do destino."""
    bruto = spark.read.csv(caminho_csv, header=True, inferSchema=False).select(*NOMES_CAMPOS)
    if arquivos_por_particao:
        # round-robin: cada partição de dt_processamento recebe N arquivos (útil para
        # simular a entrega em vários arquivos e o paralelismo de leitura)
        bruto = bruto.repartition(arquivos_por_particao)
    bruto.write.mode("overwrite").partitionBy("dt_processamento").parquet(destino)
    layout = (
        spark.read.parquet(destino).groupBy("dt_processamento").count().orderBy("dt_processamento").collect()
    )
    return {str(r["dt_processamento"]): r["count"] for r in layout}


def main() -> None:
    parser = argparse.ArgumentParser(description="CSV -> Parquet particionado por dt_processamento")
    parser.add_argument("--input", required=True, help="CSV de origem (campos como texto)")
    parser.add_argument("--destino", required=True, help="diretório de saída (raw/<nome>)")
    parser.add_argument("--arquivos-por-particao", type=int, default=None)
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("csv_para_parquet_particionado")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    layout = converter(spark, args.input, args.destino, args.arquivos_por_particao)
    spark.stop()

    print(f"origem no formato do contrato em {args.destino}/ (campos continuam texto):")
    for dt, qtd in layout.items():
        pasta = os.path.join(args.destino, f"dt_processamento={dt}")
        arquivos = [f for f in os.listdir(pasta) if f.endswith(".parquet")] if os.path.isdir(pasta) else []
        print(f"  dt_processamento={dt}: {qtd:,} linhas em {len(arquivos)} arquivo(s)".replace(",", "."))


if __name__ == "__main__":
    main()
