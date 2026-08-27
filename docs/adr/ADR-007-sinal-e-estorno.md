# ADR-007 — Convenção de sinal e efeito do estorno no saldo

## Contexto
O contrato define 5 tipos de lançamento (DEBITO, CREDITO, TARIFA, JUROS, IOF) e
`valor_lancamento > 0` sempre — mas **não define o sinal de cada tipo** no saldo.
Sem isso não existe "saldo". Premissa assumida e registrada (é também resposta à
P5.2: dúvida real de contrato levantada ao owner).

## Decisão
`valor_assinado = valor_lancamento × sinal(tipo) × sinal(estorno)`:
- CREDITO, JUROS → **+** (entram recursos na conta)
- DEBITO, TARIFA, IOF → **−** (saem recursos: saque/cobrança/tributo)
- `flag_estorno = true` → **inverte** o sinal do lançamento original (P2.5): o
  estorno de um débito devolve dinheiro; o estorno de um crédito retira. O
  estorno NÃO é violação de qualidade — valor continua > 0, a semântica está na
  flag (como o contrato manda).

## Alternativas rejeitadas
- **Tratar estorno como par de ajuste separado (contra-lançamento próprio)**: o
  dataset não traz vínculo com a transação original (não há id_transacao_origem);
  inventar o vínculo seria ficção. Registrada como pergunta ao owner.
- **Quarentenar tipos "ambíguos" (JUROS pode ser recebido ou pago)**: bloquearia
  40% do dataset por dúvida semântica; a convenção documentada + reconciliação
  débito×crédito dá visibilidade do efeito agregado enquanto a dúvida não volta
  do owner.

## Consequências
A correção do saldo (P2.4) não depende de ninguém confiar na convenção: ela é
verificada por (a) oráculo independente em Python puro que recalcula os saldos
contrato a contrato, e (b) reconciliação cruzada no Gold — o líquido somado por
agência tem que bater com o movimento somado por contrato antes de publicar.
