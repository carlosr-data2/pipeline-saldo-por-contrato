# ADR-002 — Step Functions como dona única dos retries

## Contexto: retry em dois níveis

O pipeline tem três jobs Glue estritamente sequenciais (bronze → silver com gate
→ gold) e uma janela apertada (22h→02h). Tanto o Glue (`MaxRetries`) quanto o
orquestrador sabem retentar, e o risco está justamente aí: com retry nos dois
níveis, uma falha persistente no primeiro estágio geraria 2×3 = 6 execuções
antes de alguém ser avisado, queimando a janela inteira em tentativas cegas.
Num sistema com deadline, retry descontrolado só consome o orçamento de tempo
sem supervisão.

## Decisão

**EventBridge Scheduler → Step Functions → jobs Glue via `glue:startJobRun.sync`**,
com uma regra simples: a Step Function é a **dona única dos retries**.

- Nos jobs Glue: `MaxRetries = 0` (o Glue nunca retenta por conta própria) e
  `MaxConcurrentRuns = 1`. Este último é o cinto de segurança da idempotência:
  mesmo que dois disparos aconteçam por engano, nunca rodam ao mesmo tempo.
- Na Step Function: retry com backoff exponencial + jitter, **assimétrico por
  estágio**: 2 retentativas no Bronze (falhas dominadas por I/O transitório) e
  1 no Silver e no Gold, porque as falhas dominantes deles (gate reprovado,
  reconciliação divergente) são determinísticas: repetir não conserta dado
  ruim, só gasta janela.
- Retomada de falha via **redrive**: reexecuta *do estágio que falhou*. Se
  bronze e silver passaram e o gold caiu, só o gold reroda. É a razão de serem
  três jobs, não um.

## Alternativas rejeitadas

**1. Glue Workflows.** Orquestraria os mesmos jobs, mas sem redrive, com
controle de erro e passagem de parâmetros pobres, e preso ao Glue: se um passo
futuro sair do Glue (uma validação em Lambda, por exemplo), o workflow não o
enxerga.

**2. MWAA (Airflow gerenciado).** A escolha certa quando a organização
padroniza *dezenas* de DAGs e precisa de backfill declarativo e um time de
plataforma por trás. Para um único pipeline, significa ~US$ 300+/mês de custo
fixo e a operação de um ambiente inteiro; não se justifica aqui.

**3. Retry nos dois níveis (Glue + SFN).** O cenário dos 2×3: multiplicação de
tentativas, tempo de janela imprevisível e o mesmo erro tratado em dois lugares
diferentes: ninguém sabe dizer quantas vezes algo vai rodar.

## Consequências

O reprocessamento **em cadeia** (corrigir o dia D e refazer D+1 até hoje) ficou
deliberadamente *fora* da state machine: é um loop de `start-execution` com
`{"dt": ...}` em sequência (runbook §3). Transformar a máquina de fechamento
diário num orquestrador de backfill a complicaria para todos os dias em nome de
um caso raro. Preferiu-se simplicidade no caminho quente e procedimento
documentado para o caso excepcional.
