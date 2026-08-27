# ADR-002 — Step Functions como dona única dos retries

## Contexto
Três jobs Glue estritamente sequenciais (bronze → silver com gate → gold), janela
22h→02h, e dois mecanismos de retry disponíveis (Glue `MaxRetries` e `Retry` da
Step Function). Retry em dois níveis multiplica execuções (2×3=6 tentativas por
estágio) e queima a janela.

## Decisão
**EventBridge Scheduler → Step Functions → jobs Glue via `startJobRun.sync`.**
Retries SÓ na Step Function (backoff exponencial + jitter, 2 retentativas);
nos jobs Glue, `MaxRetries=0` e `MaxConcurrentRuns=1` (cinto de segurança da
idempotência contra disparo duplo). Retomada de falha via **redrive** — reexecuta
do estado que falhou, sem repetir o que já passou.

## Alternativas rejeitadas
- **Glue Workflows**: sem redrive, controle de erro e passagem de parâmetros
  pobres; preso ao Glue (a orquestração não veria um passo fora dele).
- **MWAA (Airflow)**: custo fixo (~US$ 300+/mês) e operação de ambiente para UM
  pipeline. É a escolha certa quando a organização padroniza dezenas de DAGs e
  precisa de backfill declarativo; aqui seria peso morto.
- **Retry no Glue + na SFN**: multiplicação de tentativas, tempo de janela
  imprevisível, e o mesmo erro tratado em dois lugares.

## Consequências
O reprocessamento em cadeia (dia D corrigido → refazer D+1..hoje) é um loop de
`start-execution` com `{"dt": ...}` (runbook §3) — deliberadamente fora da state
machine para não transformar a máquina de fechamento diário num orquestrador de
backfill (simplicidade > generalidade aqui).
