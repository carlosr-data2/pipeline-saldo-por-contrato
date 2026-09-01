# ADR-003 — Caminho de erro sem fila intermediária

## Contexto: qual papel uma fila teria neste desenho

Filas (SQS) resolvem um problema específico: desacoplar produtor de consumidor
que trabalham em ritmos diferentes, amortecendo picos. Este pipeline não tem
esse problema. São três estágios **estritamente sequenciais**, onde o segundo
só existe depois que o primeiro termina, e a "fila" natural entre eles é o
próprio orquestrador. Antes de adicionar SQS por hábito, valia perguntar se
sobra algum papel real para uma fila aqui. Sobra um só, e pequeno.

O que o caminho de erro precisa de verdade são três capacidades: **notificar**
uma pessoa quando algo falha, **registrar** a falha de forma durável para
diagnóstico, e **retomar** do ponto certo depois da correção.

## Decisão

Cada capacidade com a ferramenta que já a oferece nativamente:

- **Notificação**: regra EventBridge sobre mudança de status da Step Function:
  `FAILED / TIMED_OUT / ABORTED` → tópico SNS → e-mail. Simples e imediato.
- **Registro durável**: o **histórico de execução da própria Step Function**
  (input, estado que falhou, causa, timestamps). Não é preciso duplicar isso em
  lugar nenhum; e a retomada é o **redrive**, que parte exatamente desse
  registro.
- **Ausência de sinal vira alerta**: dois alarmes-sentinela no CloudWatch,
  "nenhuma execução iniciada em 24h" e "nenhum sucesso em 24h", ambos com
  `treat_missing_data = breaching`: a *falta* da métrica também dispara. Isso
  cobre o cenário que notificação de falha não cobre: quando nada rodou (o
  ponto cego do gatilho por cron, assumido no ADR-001).
- **A única fila do desenho**: uma DLQ (SQS) no alvo do EventBridge Scheduler.
  Papel dela: se o Scheduler não conseguir *iniciar* a Step Function, o evento
  de disparo fica retido por 14 dias (com alarme) em vez de evaporar. Custa
  uma linha de Terraform e zero por mês, e cobre a perda silenciosa do disparo.

## Alternativas rejeitadas

**1. SQS entre os estágios do pipeline.** Entre jobs batch sequenciais não há
produtor e consumidor desacoplados, não há pico a amortecer. A fila só criaria
um lugar a mais onde dado pode ficar preso ou perder-se, e uma semântica de
retry paralela à da Step Function (exatamente o que o ADR-002 eliminou).

**2. Fila de "registros com erro".** Tentação comum: jogar as linhas reprovadas
numa fila para reprocessamento. Mas registro ruim em contexto regulatório não é
evento a reprocessar, é **dado a auditar**: precisa de motivo, histórico e
consulta. O lugar dele é a tabela de quarentena no lakehouse (versionada,
consultável, com `motivos[]`), não uma fila com TTL de 14 dias.

## Consequências

A precisão dos sentinelas é diária: eles detectam "não rodou nas últimas 24h",
não "não rodou até as 22:15 em ponto". Para o minuto exato, a solução de
produção é um schedule adicional com uma verificação trivial (5 linhas) — custo
mínimo, mas fora do escopo da prova de conceito. Trade-off registrado em
`docs/arquitetura.md`.
