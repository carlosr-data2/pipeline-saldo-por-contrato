# ADR-004 — Apache Iceberg V3, com runtime próprio embarcado no Glue

## Contexto: por que "só Parquet" não basta neste caso

O desafio exige todo dado em **Iceberg V3** — mas vale entender por que a
exigência faz sentido aqui, além de ser requisito. Parquet é um formato de
**arquivo**: excelente em compressão e leitura colunar, mas sem noção de
"tabela" — um diretório de Parquets não sabe o que é uma transação, um schema
oficial ou uma versão. O Iceberg é a camada de **tabela** por cima dos arquivos
(que continuam sendo Parquet), e é essa camada que um razão contábil de 5 anos
exige.

## Decisão

As 9 tabelas do pipeline em **Iceberg, `format-version=3`**, escritas com
`iceberg-spark-runtime-3.5` **1.10.2** + `iceberg-aws-bundle` 1.10.2 — a mesma
versão **pinada nas três trilhas** (venv de desenvolvimento, imagem Docker,
Glue), garantindo que o motor que valida localmente é o motor que roda na nuvem.

No Glue há um detalhe decisivo: o **Glue 5.0 embarca Iceberg 1.7.x, que não
escreve V3**. A solução: os jars 1.10.2 entram por `--extra-jars`, com
`--user-jars-first = true` (os nossos vencem no classpath) e
`--datalake-formats` vazio — sem isso, o Iceberg nativo subiria junto e o
conflito de versões geraria erros difíceis de diagnosticar.

## O que o Iceberg compra, concretamente (a resposta à pergunta P1.2)

1. **Commit atômico** — o INSERT OVERWRITE de partição do pipeline é uma troca
   de snapshot: um leitor vê o estado anterior ou o novo, nunca o meio. Em
   Parquet puro, overwrite é "apaga e regrava" com uma janela de inconsistência
   no meio do fechamento. (Visto na prática durante o desenvolvimento: um clock
   skew de ambiente fez o Iceberg *recusar* um commit inconsistente em vez de
   corromper a tabela — o comportamento exato que se quer de um razão.)
2. **Time travel** — "o que o fechamento viu às 02h?" é uma consulta de
   snapshot; requisito de auditoria que em Parquet puro exigiria cópias manuais.
3. **Evolução de schema e de partição por metadado** — sem reescrever 5 anos de
   dados quando o contrato mudar.
4. **Poda por estatísticas de arquivo** (min/max por coluna) — além da poda de
   partição, o motor pula arquivos inteiros que não contêm o que a consulta
   busca.

Custo aceito: o metadado precisa de manutenção — compactação
(`rewrite_data_files`) e expiração de snapshots (`expire_snapshots`), agendadas
fora da janela na evolução de produção.

## O risco do V3, e o plano B (a resposta à P1.3)

O V3 é recente, e o ecossistema de **leitura** ainda é desigual — o Athena, por
exemplo, pode recusar tabelas V3. Mitigações em camadas: o consumidor de
referência desta solução é Spark (que lê V3 com o runtime embarcado); a
conformidade é **verificada, não presumida** — o relatório da demo lê o
`metadata.json` de cada tabela e valida `format-version: 3`, com teste
automatizado; e se um consumidor V3 travar em produção, rebaixar para V2 é
**uma propriedade** na criação da tabela — o pipeline não muda uma linha.

## Alternativa rejeitada

**Parquet puro em S3 (+ partições Hive).** Bastaria para um dump analítico
descartável. Para um razão regulatório com overwrite diário, auditoria por data
e 5 anos de evolução, faltariam exatamente as quatro capacidades acima — e cada
uma teria que ser reinventada à mão, pior.
