# ADR-005 — Saldo incremental com snapshot diário completo

## Contexto
Saldo consolidado diário por contrato (300M contratos em produção) com histórico
regulatório de 5 anos. Recalcular saldo somando 5 anos de transações (full scan)
não cabe em SLA nenhum.

## Decisão
`saldo(D) = snapshot(D-1) ⟗ movimento(D)` — **incremental**:
- `gold.saldo_contrato_diario` é um **snapshot completo por dia** (carry-forward
  dos contratos sem movimento), particionado por `dt_referencia`;
- escrita por **INSERT OVERWRITE dinâmico da partição**: reprocessar o dia D é
  reescrever a partição D — idempotente por construção (P4.1);
- o job acha a base como `max(dt_referencia) < D` — tolera feriados/lacunas.

## Alternativas rejeitadas
- **Full scan do histórico**: O(histórico) por dia; inviabiliza o SLA e o custo.
- **Tabela current-state com MERGE INTO** (P4.2): menos storage, mas perde o
  histórico diário pronto para consulta (exigido para auditoria/balancete em
  qualquer data), e reprocessar um dia passado vira compensação de upsert em vez
  de reescrita de partição. MERGE INTO é a ferramenta certa quando há upsert por
  chave SEM noção de partição de negócio (ex.: dimensão de cadastro) — não aqui.
- **Snapshot só de contratos com movimento**: quebra a leitura "saldo de todos os
  contratos em D" (consumidor teria que varrer partições para trás).

## Consequências
- Custo de storage: ~300M linhas/dia; em Parquet+zstd (~30–40 B/linha) ≈ 9–12
  GB/dia ≈ **~20 TB em 5 anos hot** — aceitável em S3 (~US$ 470/mês em Standard,
  menos com Intelligent-Tiering), e é o padrão contábil de "foto diária".
- Reprocessar D exige recascatear D+1..hoje (o snapshot de D+1 depende de D) —
  operação coberta pelo replay do runbook (ADR-002).
