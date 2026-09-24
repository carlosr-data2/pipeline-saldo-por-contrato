"""Bronze por partição do dia (ADR-014): --dt sobrescreve só a partição do dia, partição
vazia na origem falha alto, e a origem Parquet particionada é lida com poda."""
import csv
import json
from datetime import date

import pytest
from pyspark.sql import functions as F

from jobs.bronze_ingest import executar as bronze
from jobs.bronze_ingest import ler_origem
from lib.log import JobLogger
from lib.schema import NOMES_CAMPOS, tipar_contrato
from test_schema import LINHA_VALIDA, df_texto

LOG = JobLogger("teste_bronze")
COSIF = "cod_cosif,descricao,natureza,tipo_contrato_associado\n1.1.1.00.0,Disponibilidades,ATIVO,CC\n"


def linha(id_tx: str, dia: str, hora: str = "10:00:00") -> dict:
    return dict(LINHA_VALIDA, id_transacao=id_tx, dt_processamento=dia, dt_lancamento=f"{dia} {hora}")


def linhas_base():
    return [linha("a20", "2026-08-20"), linha("b20", "2026-08-20", "11:00:00"), linha("a21", "2026-08-21")]


def escrever_csv(caminho, linhas) -> str:
    with open(caminho, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=NOMES_CAMPOS)
        w.writeheader()
        w.writerows(linhas)
    return str(caminho)


@pytest.fixture
def cosif(tmp_path) -> str:
    caminho = tmp_path / "cosif.csv"
    caminho.write_text(COSIF)
    return str(caminho)


def particao(spark, cfg, dt: str) -> dict:
    return {
        r["id_transacao"]: r
        for r in spark.table(cfg.tb_bronze).where(F.col("dt_processamento") == dt).collect()
    }


def test_por_dia_sobrescreve_so_a_particao_do_dia(spark, cfg, tmp_path, cosif):
    bronze(spark, cfg, LOG, escrever_csv(tmp_path / "v1.csv", linhas_base()), cosif)  # carga inicial
    dia20_antes = particao(spark, cfg, "2026-08-20")
    assert set(dia20_antes) == {"a20", "b20"}

    # versão 2 da origem: o dia 21 ganha uma linha e o dia 20 muda, mas só o 21 é pedido
    v2 = linhas_base()
    v2[0] = dict(v2[0], valor_lancamento="999.99")
    v2.append(linha("b21", "2026-08-21", "12:00:00"))
    bronze(spark, cfg, LOG, escrever_csv(tmp_path / "v2.csv", v2), cosif, formato="csv", dt="2026-08-21")

    assert set(particao(spark, cfg, "2026-08-21")) == {"a21", "b21"}
    dia20_depois = particao(spark, cfg, "2026-08-20")
    assert dia20_depois["a20"]["valor_lancamento"] == dia20_antes["a20"]["valor_lancamento"]  # não leu a v2
    assert dia20_depois["a20"]["_ts_ingestao"] == dia20_antes["a20"]["_ts_ingestao"]  # partição intocada


def test_por_dia_recusa_particao_vazia_na_origem(spark, cfg, tmp_path, cosif):
    origem = escrever_csv(tmp_path / "v1.csv", linhas_base())
    with pytest.raises(ValueError, match="vazia"):
        bronze(spark, cfg, LOG, origem, cosif, dt="2026-08-25")


def test_origem_parquet_particionada_e_lida_com_poda(spark, cfg, tmp_path, cosif, capsys):
    origem = str(tmp_path / "raw" / "fin_contabilidade_saldo_contrato")
    df_texto(spark, *linhas_base()).write.partitionBy("dt_processamento").parquet(origem)

    bruto = ler_origem(spark, origem, "parquet")
    assert dict(bruto.dtypes) == {c: "string" for c in NOMES_CAMPOS}  # texto, inclusive a coluna de partição
    assert bruto.count() == 3

    # o filtro do dia sobre a coluna tipada chega à leitura como filtro de partição
    dia = tipar_contrato(bruto).where(F.col("dt_processamento") == F.lit(date(2026, 8, 21)))
    plano = spark._jvm.PythonSQLUtils.explainString(dia._jdf.queryExecution(), "formatted")
    filtros = plano.split("PartitionFilters: [", 1)[1].split("]", 1)[0]
    assert "dt_processamento" in filtros

    bronze(spark, cfg, LOG, origem, cosif, formato="parquet", dt="2026-08-21")
    linhas = spark.table(cfg.tb_bronze).collect()
    assert {r["id_transacao"] for r in linhas} == {"a21"}
    assert "dt_processamento=2026-08-21" in linhas[0]["_arquivo_origem"]

    eventos = [json.loads(linha) for linha in capsys.readouterr().out.splitlines() if linha.startswith("{")]
    commit = next(e for e in eventos if e["evento"] == "bronze_commit")["commit"]
    assert commit["changed-partition-count"] == "1"
    assert commit["added-records"] == "1"
