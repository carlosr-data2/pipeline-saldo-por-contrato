# ADR-006 — Unicidade sobre o publicado (dedup determinística + lookback)

## Contexto
O contrato exige `id_transacao` único globalmente. O profiling do dataset mudou o
problema: **100% dos ids duplicados têm payloads divergentes** (2.736 grupos,
zero cópias idênticas) e **71% cruzam partições**. Ou seja: não são reentregas a
colapsar com `dropDuplicates` — são colisões que exigem uma política.

## Decisão
Unicidade é sobre **o que entra no razão** (Silver), não sobre o que chega:
1. **Intra-lote**: o vencedor é escolhido só entre as linhas VÁLIDAS nas demais
   regras — janela por `id_transacao` ordenando por (`dt_lancamento` ASC, hash
   SHA-256 da linha como desempate). Determinístico: mesma entrada, mesmo
   vencedor, independentemente da ordem do arquivo. Linha inválida não vence nem
   condena uma válida.
2. **Entre lotes**: anti-join contra os ids JÁ PUBLICADOS no Silver numa janela de
   lookback configurável (default 7 dias). Id publicado vence sempre — não se
   retrata dado usado em fechamento anterior.
3. Consequência deliberada: **o reenvio corrigido de um registro quarentenado é
   aceito** (o id nunca entrou no razão) — quarentena existe para correção e
   reenvio, não para condenar o id para sempre.
4. Perdedores vão à quarentena com motivo (`DUPLICADO_NO_LOTE` / `JA_PROCESSADO`).

## Alternativas rejeitadas
- **Unicidade global literal (5 anos)**: anti-join diário de 300M contra ~550
  bilhões de chaves. Mitigável (bloom filter por partição, bucketing por chave),
  mas o custo não paga o risco marginal — colisão de UUID legítima após 7 dias é
  patologia de upstream, não fluxo. A renegociação do contrato ("único global" →
  "único em janela de X dias") é pergunta registrada ao data owner (P5.2).
- **Quarentenar o grupo inteiro em colisão**: perde o registro válido junto com o
  inválido; pior para o fechamento e para o cliente.
- **`dropDuplicates`**: não determinístico e descarta silenciosamente — proibido
  em contexto regulatório.

## Consequências
Verificado por oráculo independente: 3.276 linhas extras de duplicata do dataset
terminam 100% explicadas (1.027 no lote + 2.053 contra histórico + 196 absorvidas
por outras violações do próprio registro), com `bronze = silver + quarentena`.
