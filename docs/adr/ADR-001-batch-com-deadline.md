# ADR-001 — Batch crítico com deadline, não event-driven

## Contexto
O caso é um fechamento contábil regulatório: lote diário (`id_lote` por dia,
partição `dt_processamento`), consumidor único (fechamento D+1 06:00) e requisito
de latência expresso como **deadline**, não como frescor contínuo.

## Decisão
Pipeline batch disparado por **tempo** (EventBridge Scheduler, 22:05
America/Sao_Paulo), não pela chegada de arquivo nem por stream de eventos.

## Alternativas rejeitadas
- **Streaming (Kinesis/Firehose + Spark Structured Streaming)**: não existe fonte
  de eventos no caso; adicionaria custo fixo e complexidade operacional sem reduzir
  o único risco que importa (perder as 06:00). Estado de agregação contínua por
  80M+ contas é caro e desnecessário quando o consumidor é diário.
- **Event-driven por S3 (`s3:ObjectCreated` → pipeline)**: acopla o disparo ao
  layout de entrega do upstream (n arquivos = n eventos, precisa de marcador de
  lote completo). Vira a escolha certa se o contrato de entrega mudar para
  "arquivo-marcador de lote fechado"; registrado como evolução, não como default.

## Consequências
O gatilho por tempo exige detecção de **ausência** (lote não chegou / execução não
iniciou) — coberta pelos alarmes-sentinela e pela DLQ do disparo (ADR-003).
