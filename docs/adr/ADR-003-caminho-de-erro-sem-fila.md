# ADR-003 — Caminho de erro sem fila intermediária

## Contexto
Pipeline batch sequencial: a "fila" natural entre estágios é o próprio
orquestrador. Falhas precisam de: notificação, registro durável e retomada.

## Decisão
- Notificação: regra EventBridge `Step Functions FAILED/TIMED_OUT/ABORTED → SNS (e-mail)`.
- Registro durável: o **histórico de execução da própria Step Function** (input,
  estado que falhou, causa) — e a retomada é o **redrive**.
- Ausência de sinal vira alerta: alarmes-sentinela no CloudWatch
  (`ExecutionsStarted < 1/dia`, `ExecutionsSucceeded < 1/dia`, ambos com
  `treat_missing_data = breaching`).
- **Única fila do desenho**: DLQ (SQS) no alvo do EventBridge Scheduler — cobre a
  perda silenciosa do disparo (o evento fica retido 14 dias, com alarme). Custa
  uma linha de Terraform e zero por mês.

## Alternativas rejeitadas
- **SQS entre estágios**: entre jobs batch sequenciais não há produtor/consumidor
  desacoplado nem picos a amortecer; a fila só adicionaria um lugar a mais para
  dado "sumir" e uma semântica de retry paralela à da SFN.
- **Fila de "registros com erro"**: registro ruim não é evento a reprocessar em
  fila — é dado regulatório a auditar; o lugar dele é a tabela de quarentena
  versionada no lakehouse (com motivo), não uma fila com TTL.

## Consequências
A precisão fina dos sentinelas ("nenhuma execução até 22:15") fica na granularidade
diária do alarme de métrica; a checagem no minuto exato em produção seria um
schedule + verificação trivial — trade-off documentado em docs/arquitetura.md.
