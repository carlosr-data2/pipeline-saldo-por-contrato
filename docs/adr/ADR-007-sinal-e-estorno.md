# ADR-007 — Convenção de sinal e efeito do estorno no saldo

## Contexto: sem sinal, não existe "saldo"

O contrato de dados define cinco tipos de lançamento (DEBITO, CREDITO, TARIFA,
JUROS, IOF) e exige `valor_lancamento > 0` sempre; os estornos são marcados
pela flag, não pelo sinal. O que o contrato **não** define é o essencial para o
produto final: **qual tipo soma e qual subtrai** do saldo. Sem essa definição, a
palavra "saldo" não tem significado computável.

Diante de uma lacuna assim, há três posturas possíveis: travar e esperar
resposta do owner (inviável no prazo), assumir em silêncio (perigoso, porque a
suposição fica invisível), ou **assumir explicitamente, documentar e devolver a
pergunta**. Esta ADR é a terceira postura em prática, e a lacuna em si está
registrada entre as dúvidas formais devolvidas ao dono do contrato.

## Decisão

```
valor_assinado = valor_lancamento × sinal(tipo) × sinal(estorno)

sinal(tipo):     CREDITO, JUROS        → +1   (recursos entram na conta)
                 DEBITO, TARIFA, IOF   → −1   (recursos saem: saque, cobrança, tributo)
sinal(estorno):  flag_estorno = true   → ×(−1)  (inverte o lançamento original)
```

O efeito do estorno é a parte que mais gera discussão: estornar um
débito **devolve** dinheiro (+); estornar um crédito **retira** (−). E um ponto
que confunde à primeira leitura: estorno **não é violação de qualidade**. Os
2.006 valores negativos e 1.983 zeros do dataset violam a regra de *valor*;
estorno legítimo tem valor positivo e a semântica inteira na flag, exatamente
como o contrato manda.

As sete combinações relevantes de tipo × flag têm teste unitário
(`test_convencao_de_sinal`).

## Alternativas rejeitadas

**1. Tratar estorno como par de ajuste vinculado à transação original.** Seria
o modelo contábil mais rico, mas o dataset não traz o vínculo (não existe
`id_transacao_origem`), e inventá-lo por inferência (casar por valor e conta)
não é aceitável em dado regulatório. Fica registrado como pergunta ao
owner; sem o vínculo, também não é possível validar "estorno órfão" (estorno de
transação que nunca existiu) — limitação declarada.

**2. Quarentenar os tipos semanticamente ambíguos.** JUROS, por exemplo, pode
ser recebido (+) ou pago (−) dependendo do produto. Bloquear por dúvida
semântica quarentenaria ~40% do dataset e pararia o fechamento.
A convenção documentada mantém o fechamento vivo enquanto a dúvida tramita com
o owner.

## Consequências

A correção do saldo **não depende de ninguém confiar na convenção**: ela é
verificada por dois mecanismos que a cercam. O oráculo independente em Python
puro recalcula todos os saldos sob a mesma convenção declarada e exige igualdade
exata com o Spark; e a reconciliação cruzada do Gold (líquido por agência ×
movimento por contrato) precisa bater ao centavo antes de qualquer publicação.
Se o owner um dia responder com uma convenção diferente, a mudança é localizada
(uma função, `com_valor_assinado`, seus testes e o oráculo) e o reprocessamento
é o replay padrão.
