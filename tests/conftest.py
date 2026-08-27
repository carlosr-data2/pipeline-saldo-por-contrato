import os
import sys
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lib.config import Config  # noqa: E402

JAR_ICEBERG = os.environ.get(
    "ICEBERG_JAR", "/opt/spark-jars/iceberg-spark-runtime-3.5_2.12-1.10.2.jar"
)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    sessao = (
        SparkSession.builder.master("local[2]")
        .appName("testes-pipeline-saldo")
        .config("spark.jars", JAR_ICEBERG)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    sessao.sparkContext.setLogLevel("ERROR")
    yield sessao
    sessao.stop()


@pytest.fixture
def cfg(spark, tmp_path) -> Config:
    """Config isolada por teste: catálogo Iceberg próprio num warehouse temporário.

    Catálogos Iceberg podem ser registrados em runtime via spark.conf — cada teste
    enxerga só as suas tabelas.
    """
    catalogo = f"t{uuid.uuid4().hex[:8]}"
    spark.conf.set(f"spark.sql.catalog.{catalogo}", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"spark.sql.catalog.{catalogo}.type", "hadoop")
    spark.conf.set(f"spark.sql.catalog.{catalogo}.warehouse", str(tmp_path / "warehouse"))
    return Config(
        catalogo=catalogo,
        impl="hadoop",
        warehouse=str(tmp_path / "warehouse"),
        max_quarentena_pct=10.0,
        tolerancia_reconciliacao=0.01,
        dedup_lookback_dias=7,
        shuffle_partitions=4,
    )
