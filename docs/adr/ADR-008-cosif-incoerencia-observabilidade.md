# ADR-008 — Incoerência COSIF × tipo de contrato é observabilidade, não bloqueio

## Contexto
A regra do contrato para `cod_cosif` é uma só: **existir no domínio COSIF**.
O profiling revelou outra coisa: **83,3% dos registros válidos** têm `cod_cosif`
que existe no domínio mas está associado a OUTRO tipo de contrato (ex.: lançamento
CC com COSIF de SEGURO).

## Decisão
- `cod_cosif` fora do domínio (`9.9.9.99.9`, 3.309 registros) → **quarentena**
  (regra do contrato).
- Incoerência cosif×tipo_contrato → **não bloqueia e não quarentena**: vira
  métrica no relatório DQ (`obs_cosif_incoerente_com_tipo_contrato`) e coluna
  `flag_coerente` na tabela Gold de classificação, que reporta a distribuição
  observada `tipo_contrato × cod_cosif × natureza`.

## Alternativas rejeitadas
- **Promover a coerência a regra de bloqueio**: quarentenaria 83% do dataset — o
  fechamento nunca aconteceria. Inventar regra que o contrato não pediu, com esse
  efeito, é exatamente o que um gate regulatório NÃO deve fazer.
- **Reclassificar silenciosamente** (sobrescrever o cod_cosif pelo associado ao
  tipo de contrato): pipeline "corrigindo" lançamento contábil por conta própria
  é adulteração de dado regulatório.

## Consequências
O número (83%) sugere dado mockado com associação aleatória — mas a resposta de
arquitetura é a mesma no mundo real: medir, expor no relatório e devolver a
pergunta ao data owner com evidência (P5.2), mantendo o pipeline fiel ao contrato.
