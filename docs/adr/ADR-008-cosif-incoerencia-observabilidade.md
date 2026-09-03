# ADR-008 — Incoerência COSIF × tipo de contrato é observabilidade, não bloqueio

## Contexto: quando a análise exploratória encontra um problema que o contrato não previu

A regra do contrato para `cod_cosif` é uma só: **existir no domínio COSIF**. O
pipeline a implementa e ela funciona como esperado: 3.309 registros com o
código `9.9.9.99.9` (inexistente no referencial) vão para a quarentena.

A análise exploratória, porém, revelou algo que a regra não cobre: o referencial associa
cada código COSIF a um tipo de contrato (`tipo_contrato_associado`), e **83,3%
dos registros válidos** têm um código que *existe*, mas pertence a **outro**
tipo: lançamentos de conta corrente com COSIF de seguros, CDBs com COSIF de
poupança. O código passa na regra; a combinação não faz sentido contábil.

A questão que esta ADR resolve: o que o pipeline deve fazer com um problema
real que o contrato não pediu para tratar?

## Decisão

Três respostas, uma por camada de responsabilidade:

1. **A regra do contrato continua exata**: existência no domínio → quarentena
   para quem falha. Nem mais, nem menos do que o contrato define.
2. **A incoerência vira métrica, não bloqueio**: o relatório de qualidade
   publica `obs_cosif_incoerente_com_tipo_contrato` (~51 mil registros/dia — o
   prefixo `obs_` marca que é observação, não violação), e a tabela Gold de
   classificação carrega a coluna `flag_coerente`, expondo a distribuição
   observada de `tipo_contrato × cod_cosif`.
3. **A pergunta volta ao data owner com evidência**: "83% dos lançamentos têm
   COSIF incoerente com o tipo de contrato — isso é esperado? A coerência deve
   virar regra?" — registrada como uma das dúvidas formais de contrato.

## Alternativas rejeitadas

**1. Promover a coerência a regra de bloqueio.** O efeito prático: 83% do
dataset na quarentena, e o fechamento nunca aconteceria. Além do dano imediato,
há o princípio: seria o pipeline **inventando uma regra** que o contrato não
pediu. Regra de qualidade nasce de acordo com o owner, não de iniciativa
unilateral do engenheiro.

**2. Reclassificar silenciosamente** (sobrescrever o `cod_cosif` pelo código
associado ao tipo de contrato). A mais perigosa das opções: um pipeline
"corrigindo" classificação contábil por conta própria é **adulteração de dado
regulatório**: o auditor encontraria um código no razão diferente do que o
sistema de origem enviou, sem trilha de quem decidiu a troca.

## Consequências

O número (83%) sugere fortemente dado sintético com associação aleatória, mas a
resposta de arquitetura seria idêntica com dado real: medir, expor e devolver
a decisão a quem é dono dela, mantendo o pipeline estritamente fiel ao
contrato. Se o owner promover a coerência a regra, a implementação é pequena
(um motivo novo em `dq.py`, um teste) e o limiar do gate (ADR-009) protege o
fechamento durante a transição.
