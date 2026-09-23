# Laboratórios na conta AWS — operar para provocar e observar

A primeira versão da trilha AWS provou que a arquitetura funciona
([`docs/evidencias.md`](evidencias.md)). Estes laboratórios reoperam a mesma
conta com outra intenção: **provocar cenários e observar o que a plataforma faz**
— no S3, no Data Catalog, nos logs e no histórico da Step Function. Cada lab diz o
que rodar, o que olhar e o que anotar.

- [Laboratórios na conta AWS — operar para provocar e observar](#laboratórios-na-conta-aws--operar-para-provocar-e-observar)
  - [Antes de começar](#antes-de-começar)
  - [Lab 1 — Bronze por partição do dia (ADR-014)](#lab-1--bronze-por-partição-do-dia-adr-014)
  - [Lab 2 — Vazão medida e extrapolação para 300 M/dia](#lab-2--vazão-medida-e-extrapolação-para-300-mdia)
  - [Outros experimentos, não roteirizados](#outros-experimentos-não-roteirizados)

## Antes de começar

- Infraestrutura provisionada (runbook §1) e inscrição do SNS confirmada.
- AWS CLI autenticada; Docker para os alvos que usam Spark (ou um venv com
  PySpark, passando `PY=python` ao `make`).
- **Cron desligado durante os labs**: `terraform apply -var agendamento_ativo=false`.
  A execução manual com `{"dt": …}` continua disponível; sem isso, o disparo das
  22:05 roda em cima dos experimentos e os alarmes-sentinela reclamam de qualquer
  dia sem execução.
- Estimativa de custo (fórmula do runbook §2): Lab 1 ≈ 6 execuções × US$ 0,14 <
  **US$ 1**; Lab 2 ≈ 2 configurações × 3 dias com 10× o volume ≈ **US$ 2 a 3**.
  O budget de US$ 10 continua sendo o teto.
- Ao terminar: `terraform apply` sem `-var` devolve os defaults;
  `terraform -chdir=terraform destroy` encerra; Cost Explorer no dia seguinte
  fecha a disciplina de estimar antes e medir depois.

## Lab 1 — Bronze por partição do dia (ADR-014)

**Objetivo:** ver o Bronze ler só a partição do dia na origem Parquet e reescrever
só ela; ver o que acontece quando o lote não chegou; comparar com a origem CSV.

**Passos**

1. Publicar a origem no formato do contrato:
   ```bash
   make aws-publicar-artefatos BUCKET=$(terraform -chdir=terraform output -raw bucket)
   ```
   O alvo converte o CSV para `raw/fin_contabilidade_saldo_contrato/dt_processamento=…/`
   e sobe os dois (o CSV fica em `raw/` como cópia fiel). Confira com
   `aws s3 ls s3://<bucket>/raw/fin_contabilidade_saldo_contrato/`.
2. `terraform apply` (o default já é `origem_formato = parquet`). No plan, observe
   o `--input` sem `.csv`, o `--formato parquet` e, na definição da state machine,
   o `'--dt'` no estágio Bronze. O output `origem_bronze` resume a configuração.
3. Execute os três dias em ordem (runbook §3). No CloudWatch Logs Insights, no log
   group do job Bronze:
   ```
   filter evento = "bronze_commit" | fields dt, commit.operacao, commit.`changed-partition-count`, commit.`added-records`
   ```
   Cada execução deve mostrar `changed-partition-count = 1` e `added-records = 66666`.
4. **Reexecute o dia 21** e vá ao S3:
   ```bash
   aws s3 ls s3://<bucket>/warehouse/bronze/fin_contabilidade_saldo_contrato/metadata/   # novo vN.metadata.json
   aws s3 ls s3://<bucket>/warehouse/bronze/fin_contabilidade_saldo_contrato/data/dt_processamento=2026-08-20/
   aws s3 ls s3://<bucket>/warehouse/bronze/fin_contabilidade_saldo_contrato/data/dt_processamento=2026-08-21/
   aws glue get-table --database-name bronze --name fin_contabilidade_saldo_contrato \
     --query 'Table.Parameters.[metadata_location, previous_metadata_location]'
   ```
   Só o diretório do dia 21 ganha arquivos novos; o do dia 20 mantém a data de
   modificação original. Baixe o `metadata.json` corrente e o anterior e compare a
   lista `snapshots`: o snapshot novo referencia os mesmos arquivos de dados dos
   dias 20 e 22 e arquivos novos só do 21.
5. **Lote que não chegou:** `start-execution` com input `{}`. O `ResolverData`
   resolve a data de hoje e o Bronze falha com "partição vazia na origem". Observe
   três runs do Bronze no histórico do Glue (o retry de 2 tentativas da Step
   Function retentando uma falha determinística — consequência registrada no
   ADR-014), a execução FAILED, o e-mail do SNS e nada publicado no Bronze.
6. **Compare com o CSV:** `terraform apply -var origem_formato=csv` e rode o dia
   20. No run do Glue, compare bytes lidos e duração com o run do passo 3: com o
   CSV o Bronze lê o arquivo inteiro e filtra; com o Parquet, abre um diretório.
   Volte com `terraform apply`.

**O que anotar**

| Observação | Parquet + `--dt` | CSV + `--dt` |
|---|---|---|
| bytes lidos pelo Bronze (métricas do run) | | |
| duração do Bronze (ExecutionTime) | | |
| `changed-partition-count` no `bronze_commit` | | |
| diretórios com arquivos novos no S3 após reexecutar o dia 21 | | |
| estágio em que a execução com input `{}` falhou | | |

## Lab 2 — Vazão medida e extrapolação para 300 M/dia

**Objetivo:** substituir a premissa de `docs/arquitetura.md` (20 a 50 mil
linhas/s por core) por um número medido: **linhas/s por vCPU** em duas
configurações de worker, e extrapolar ao volume de produção.

**Passos**

1. Gerar o dataset sintético 10× (~2 M linhas, ~315 MB, menos de um minuto,
   mesmo perfil de violações do dataset de exemplo — a suíte de testes confere):
   ```bash
   make gerar-sintetico FATOR=10                          # raw/sintetico_x10.csv
   make gerar-sintetico FATOR=10 EXTRA="--contas-quentes 0.2"   # opcional: 20% das linhas em 20 contas (skew)
   ```
2. Publicar como origem particionada:
   ```bash
   make aws-publicar-origem BUCKET=<bucket> CSV=raw/sintetico_x10.csv NOME=sintetico_x10
   ```
3. Apontar o pipeline ao sintético, com folga de timeout para a medição:
   ```bash
   terraform -chdir=terraform apply -var origem_nome=sintetico_x10 -var glue_timeout_minutos=30 -var agendamento_ativo=false
   ```
4. Rodar os três dias em ordem e medir:
   ```bash
   make medir-vazao LINHAS=666660
   ```
   O script lê os runs pela AWS CLI e imprime, por job e por dia, minutos,
   DPU-hora e US$; por configuração, linhas/s, linhas/s por vCPU e a
   extrapolação.
5. Repetir com mais memória por worker:
   ```bash
   terraform -chdir=terraform apply -var origem_nome=sintetico_x10 -var glue_timeout_minutos=30 \
     -var agendamento_ativo=false -var glue_worker_type=G.2X
   make medir-vazao LINHAS=666660 ULTIMOS=6       # as duas configurações lado a lado
   ```
6. Opcional: `-var glue_numero_workers=4` com G.1X separa "mais cores" de "mais
   memória". Para ver o plano físico (broadcast do COSIF, o Exchange da janela por
   `id_transacao`, as partições que o AQE juntou), adicione aos `args_comuns` de
   `glue.tf` `"--enable-spark-ui" = "true"` e
   `"--spark-event-logs-path" = "s3://<bucket>/spark-logs/"` e abra o Spark UI
   pelo console do Glue.
7. Voltar aos defaults: `terraform -chdir=terraform apply`.

**As contas**

```
vazão (linhas/s)        = linhas do dia ÷ ExecutionTime somado dos três jobs
vazão por vCPU          = vazão ÷ (workers × vCPU por worker)      G.1X = 4 · G.2X = 8
tempo a 300 M (h)       = 300 000 000 ÷ vazão ÷ 3600               na mesma configuração
tempo a 300 M (min)     = 300 000 000 ÷ (vazão por vCPU × vCPU da configuração alvo) ÷ 60
```

A convenção de vCPU conta todos os workers (inclusive o que hospeda o driver);
mantenha-a igual nas duas configurações para a comparação valer.

**Tabela para preencher**

| Configuração | Bronze | Silver | Gold | Pipeline | DPU-h | US$ | linhas/s | linhas/s/vCPU | 300 M na config alvo (20 × G.2X) |
|---|---|---|---|---|---|---|---|---|---|
| 2 × G.1X (8 vCPU) | | | | | | | | | |
| 2 × G.2X (16 vCPU) | | | | | | | | | |
| 4 × G.1X (16 vCPU), opcional | | | | | | | | | |

**Como ler o resultado**

- O `ExecutionTime` é o tempo cobrado; exclui provisionamento, mas inclui a
  inicialização do Spark (30 a 60 s por job) e a cobrança mínima é de 1 minuto.
  Em runs curtos isso domina — por isso o mínimo de 10×; com 50× a medida fica
  mais limpa (custo ≈ 5× maior).
- A extrapolação assume escala linear. Ela vale para a parte proporcional ao dia
  (tipagem, regras, agregações), não para os dois shuffles que crescem mais que
  linearmente com o volume: a janela por `id_transacao` no Silver e o
  sort-merge do snapshot no Gold. Trate o número como **limite inferior otimista**
  e compare-o com a tabela de dimensionamento de `docs/arquitetura.md`; se a
  premissa de 20 a 50 mil linhas/s por core não se confirmar, é a tabela que
  muda, não a medida.
- O Silver é o job a observar: se ele domina o tempo, o gargalo é o shuffle da
  dedup (e o lookback), não a leitura. Com `--contas-quentes`, o Gold passa a
  mostrar o skew na agregação por conta — o cenário do salting descrito na
  arquitetura.

## Outros experimentos, não roteirizados

- **Gate reprovado + retry + redrive:** baixe `--SALDO_GATE_MAX_QUARENTENA_PCT`
  para 5 no console do job Silver, rode o dia 20 (Silver falha, um retry inútil,
  `dq_relatorio` publicado mesmo assim, e-mail), tente o dia 21 (Gold recusa com
  `SnapshotDescontinuo`), restaure e faça o redrive. O `terraform apply` seguinte
  acusa o drift do parâmetro editado à mão.
- **Snapshot obsoleto:** altere linhas do dia 21 na origem, reexecute só o 21 e
  consulte o snapshot do 22 — ele continua construído sobre o 21 antigo até o 22
  ser reexecutado.
- **Athena × Iceberg V3:** uma consulta simples numa tabela Gold responde, com
  dado, o risco de leitura descrito no ADR-004.
