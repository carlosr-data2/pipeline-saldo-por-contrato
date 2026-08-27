# ADR-011 — PySpark, não Scala

## Contexto
O desafio pede PySpark ou Scala, com justificativa.

## Decisão
**PySpark**, com uma restrição de estilo que sustenta a escolha: só API
DataFrame/SQL, **zero UDFs Python**.

## Justificativa
- Com API DataFrame, PySpark e Scala geram o MESMO plano Catalyst — a diferença
  de performance entre as linguagens só aparece quando dados atravessam a
  fronteira JVM↔Python (UDFs), que este pipeline não usa (sinal, dedup e regras
  são expressões nativas; o hash é `sha2` do Spark).
- Glue 5.0 tem Python como caminho first-class (scripts direto no S3, sem etapa
  de build); Scala exigiria pipeline de build/empacotamento de JAR.
- Custo de manutenção: Python é a língua franca de times de dados — revisão,
  on-call e evolução ficam acessíveis ao time inteiro.

## Alternativas rejeitadas
- **Scala**: ganharia se houvesse UDF pesada inevitável, tipagem de domínio
  complexa em Datasets, ou um time JVM. Nenhum dos três é o caso.

## Consequências
A regra "sem UDF" vira critério de revisão: qualquer lógica nova precisa caber em
expressão Catalyst; se um dia não couber, a UDF entra como exceção consciente
(preferindo pandas_udf vetorizada) — não como hábito.
