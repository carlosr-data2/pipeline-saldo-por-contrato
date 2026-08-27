# ADR-009 — Gate de fechamento: o que bloqueia vs. o que segue com quarentena

## Contexto
Qualidade em contexto regulatório tem dois erros possíveis: fechar com dado ruim
(multa/auditoria) e não fechar por preciosismo (perder as 06:00). O gate torna o
critério EXPLÍCITO em vez de deixá-lo implícito no código.

## Decisão
Nunca descarte silencioso: toda violação vai à quarentena **com motivos** (array).
O fechamento é BLOQUEADO (job falha → Step Functions para → SNS) quando:
1. **Falha estrutural**: arquivo ilegível, schema divergente do contrato, partição
   vazia — erro imediato, nada publicado.
2. **Taxa de quarentena da partição > limiar** (default 10%, configurável): acima
   disso o lote está doente demais para fechar; o Silver, a quarentena e o
   relatório DQ são publicados MESMO com gate reprovado (diagnóstico primeiro,
   bloqueio depois — quem opera precisa ver o porquê), mas o Gold nunca roda.
3. **Reconciliação cruzada divergente** (no Gold): o líquido somado por agência
   deve bater com o movimento somado por contrato — duas rotas de agregação
   independentes sobre o mesmo Silver. Divergindo acima da tolerância (US$ 0,01),
   nada é publicado.
O que viola regra mas fica abaixo do limiar: segue para quarentena + relatório,
e o fechamento acontece — com o número exposto, não escondido.

## Alternativas rejeitadas
- **Bloquear em qualquer violação**: com 8% de quarentena típica neste dataset, o
  fechamento nunca sairia; qualidade vira DoS contra o próprio negócio.
- **Nunca bloquear (só relatar)**: taxa de 60% passaria batida às 06:00 — o
  balanço fecharia com metade do movimento. Inaceitável.
- **Limiar fixo em código**: o número certo é decisão do negócio, que muda; por
  isso é parâmetro (`SALDO_GATE_MAX_QUARENTENA_PCT`) com default conservador.

## Consequências
O dataset do desafio tem ~8% de quarentena → o gate aprova com o default e o
mecanismo de bloqueio é provado por teste automatizado (limiar artificialmente
baixo → `GateReprovado`, Gold não roda).
