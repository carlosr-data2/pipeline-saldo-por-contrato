# Paridade de motor com o AWS Glue 5.0: Spark 3.5.4 + Java 17 + Python 3.11,
# com o MESMO runtime Iceberg (1.10.2) que o Terraform envia ao Glue via --extra-jars.
# bookworm fixado: o Spark 3.5 exige Java <= 17, e o Debian trixie só traz o 21
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY requirements-dev.txt requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements-dev.txt

# O jar entra no diretório de jars do PySpark: nenhuma configuração extra de
# submit é necessária — espelho do --extra-jars do Glue.
ARG ICEBERG_VERSAO=1.10.2
RUN curl -sSL --retry 5 --retry-delay 15 \
      -o /usr/local/lib/python3.11/site-packages/pyspark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSAO}.jar \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSAO}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSAO}.jar" \
    && test "$(stat -c%s /usr/local/lib/python3.11/site-packages/pyspark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSAO}.jar)" -gt 1000000

WORKDIR /app
ENV PYTHONPATH=/app/src \
    SALDO_WAREHOUSE=/app/warehouse \
    ICEBERG_JAR=/usr/local/lib/python3.11/site-packages/pyspark/jars/iceberg-spark-runtime-3.5_2.12-1.10.2.jar
