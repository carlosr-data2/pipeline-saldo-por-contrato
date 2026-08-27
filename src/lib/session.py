"""Criação da SparkSession com catálogo Iceberg configurável.

Local  -> catálogo "hadoop": metadados no filesystem (equivalente local do Data Catalog).
AWS    -> catálogo "glue": Glue Data Catalog como catálogo Iceberg + warehouse em S3.

O corpo dos jobs é idêntico nas duas trilhas; só esta função muda de comportamento.
"""
from pyspark.sql import SparkSession

from .config import Config


def criar_spark(app_name: str, cfg: Config) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{cfg.catalogo}", "org.apache.iceberg.spark.SparkCatalog")
        # Determinismo e otimização:
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.sql.adaptive.enabled", "true")
        # mitigação de skew (contas concentradas)
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
    )
    if cfg.impl == "glue":
        builder = (
            builder.config(
                f"spark.sql.catalog.{cfg.catalogo}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"
            )
            .config(f"spark.sql.catalog.{cfg.catalogo}.warehouse", cfg.warehouse)
            .config(f"spark.sql.catalog.{cfg.catalogo}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        )
    else:
        builder = (
            builder.config(f"spark.sql.catalog.{cfg.catalogo}.type", "hadoop")
            .config(f"spark.sql.catalog.{cfg.catalogo}.warehouse", cfg.warehouse)
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def garantir_tabela(spark: SparkSession, nome: str, ddl_colunas: str, particao: str | None = None) -> None:
    """Cria a tabela Iceberg V3 se não existir (schema é dono do código, não da infra)."""
    particionamento = f"PARTITIONED BY ({particao})" if particao else ""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {nome} ({ddl_colunas})
        USING iceberg
        {particionamento}
        TBLPROPERTIES (
          'format-version'='3',
          'write.parquet.compression-codec'='zstd'
        )
        """
    )
