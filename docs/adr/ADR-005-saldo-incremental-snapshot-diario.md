# ADR-005 — Saldo incremental com snapshot diário completo

## Contexto: o problema que esta decisão resolve

O produto final do pipeline é o saldo consolidado diário por contrato — em
produção, ~300 milhões de contratos, com 5 anos de histórico regulatório.

A forma ingênua de calcular saldo é somar todas as transações do contrato desde
o início. Com 5 anos de retenção, isso significa varrer ~550 **bilhões** de
linhas *todos os dias* — e o custo cresce sem teto conforme o histórico cresce.
Nenhum SLA de 1 hora sobrevive a isso. Antes de discutir cluster, worker ou
otimização, é o **algoritmo** que precisa mudar: o trabalho diário tem que ser
proporcional ao dia, não ao histórico.

## Decisão

Calcular o saldo de forma **incremental**, como o saldo anterior de uma fatura:
ninguém resoma a conta desde a abertura — parte-se do saldo de ontem e aplica-se
o movimento de hoje.

```
saldo(D) = snapshot(D−1)  ⟗  movimento(D)
           (full outer join por id_contrato)
```

Na prática, o job Gold do dia D faz quatro coisas:

1. **Agrega o movimento do dia**: soma o `valor_assinado` dos lançamentos
   válidos (Silver) por contrato — 300M de linhas viram ~1 linha por contrato
   com movimento;
2. **Busca a base**: o snapshot mais recente anterior a D
   (`max(dt_referencia) < D` — tolera feriados e lacunas de calendário);
3. **Junta os dois** com *full outer join*, cobrindo os três casos possíveis:
   contrato **só no snapshot** (sem movimento hoje) → carrega o saldo de ontem
   ("carry-forward"); contrato **só no movimento** (novo) → saldo = movimento;
   contrato **nos dois** → soma;
4. **Publica** o resultado como a partição `dt_referencia = D` da tabela
   `gold.saldo_contrato_diario`, via **INSERT OVERWRITE dinâmico de partição**.

Esse último detalhe carrega a idempotência de graça: reprocessar o dia D é
simplesmente reescrever a partição D — uma operação atômica no Iceberg. Rodar o
job duas vezes produz exatamente o mesmo estado (provado por teste e por
checksum — P4.1).

O resultado é uma tabela de **"fotos diárias"**: cada partição contém o saldo de
*todos* os contratos naquela data — o padrão contábil clássico, que responde
diretamente à pergunta de auditoria "qual era o saldo do contrato X em
qualquer data D?" com a leitura de uma única partição.

## Alternativas rejeitadas

**1. Full scan do histórico a cada dia.** O ponto de partida da discussão:
O(histórico) por dia, custo crescente sem teto, SLA inviável. Rejeitada pela
própria aritmética (~550 bilhões de linhas/dia aos 5 anos de histórico).

**2. Tabela *current-state* com MERGE INTO.** Manter uma única tabela com o
saldo atual de cada contrato e aplicar upserts diários (`MERGE INTO`). Prós
reais: muito menos storage (uma versão por contrato em vez de uma por dia).
Contras decisivos para *este* caso: perde o histórico diário pronto para
consulta — que auditoria e balancete exigem — e o reprocessamento de um dia
passado deixa de ser "reescrever uma partição" para virar uma compensação de
upserts, mais difícil de raciocinar e de auditar. O MERGE INTO é a ferramenta
certa em outro cenário: upsert **por chave sem partição de negócio** — uma
dimensão de cadastro, um CDC de clientes (a comparação completa entre os dois
padrões é a resposta da pergunta P4.2 da defesa). Nota de evolução: os *deletion
vectors* do Iceberg V3 barateiam exatamente esse padrão — se o futuro pedir CDC,
o V3 já adotado é o pré-requisito.

**3. Snapshot apenas dos contratos com movimento.** Reduziria o volume diário,
mas quebraria a leitura mais importante — "saldo de *todos* os contratos em D" —
obrigando o consumidor a varrer partições para trás até encontrar a última
aparição de cada contrato. Trocaria custo de escrita (barato, previsível) por
custo de leitura (caro, no caminho do fechamento).

## Consequências (e os números delas)

- **Storage do snapshot completo**: ~300M linhas/dia; em Parquet com compressão
  zstd (~30–40 bytes/linha), isso dá **9–12 GB/dia ≈ 20 TB em 5 anos** de
  camada hot — na faixa de US$ 470/mês em S3 Standard (menos com
  Intelligent-Tiering). É o preço da "foto diária", pago conscientemente: barato
  perto do valor de auditoria, e mitigável (sort por `id_contrato` na escrita
  melhora compressão e poda; lifecycle para cold conforme ADR/`s3.tf`).
- **Reprocessamento em cadeia**: como o snapshot de D+1 depende do de D,
  corrigir um dia passado exige reprocessar em ordem dali até hoje. A operação
  está coberta pelo replay do runbook (execuções com `{"dt": ...}` em
  sequência), e o desenho protege contra o esquecimento: o job Gold tem uma
  **guarda de continuidade** — se existe dia publicado no Silver sem snapshot
  Gold antes de D, ele se recusa a rodar (`SnapshotDescontinuo`), porque somar
  por cima de um buraco faria o movimento daquele dia sumir do saldo em
  silêncio, para sempre.
- **Dependência da base**: o primeiro dia processado parte de snapshot vazio
  (saldo inicial = movimento do dia), e dias sem lote não bloqueiam — a base é
  "o snapshot mais recente antes de D", não "D−1 do calendário".
