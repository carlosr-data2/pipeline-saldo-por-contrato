# Runbook — Trilha AWS (execução e evidências)

A trilha AWS prova a arquitetura na conta real: jobs no Glue, tabelas no Data Catalog,
orquestração na Step Function e **custo medido**. A demonstração local (`make demo`)
não depende de nada disto; esta trilha é a prova na plataforma real.

## 0. Pré-requisitos

- AWS CLI autenticada na conta pessoal (`aws sts get-caller-identity`)
- Terraform >= 1.6
- Docker não é necessário nesta trilha

## 1. Provisionar (uma vez)

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # editar email_alertas
terraform init
terraform plan    # conferir: 35 recursos, nada fora do prefixo saldo-contrato-*
terraform apply
```

Depois do apply: **confirmar a inscrição do SNS** no e-mail recebido (sem isso, alertas não chegam).

Publicar dados e jars (os scripts e o src.zip o próprio Terraform sobe):

```bash
cd ..
make aws-publicar-artefatos BUCKET=$(terraform -chdir=terraform output -raw bucket)
```

## 2. Estimar o custo ANTES de executar (disciplina de FinOps)

Fórmula do Glue: `custo = workers × DPU/worker × horas × US$0,44 (us-east-1)`.

| Item | Conta | Estimativa |
|---|---|---|
| 1 job Glue (G.1X = 1 DPU, 2 workers, ~3 min) | 2 × 0,05h × 0,44 | ~US$ 0,05 |
| 1 execução do pipeline (3 jobs) | 3 × 0,05 | **~US$ 0,14** |
| 3 execuções (dias 20, 21, 22) | 3 × 0,14 | ~US$ 0,42 |
| Step Functions, S3, logs, SNS | — | centavos |
| **Total esperado do exercício** | | **< US$ 1** |

Guarda-corpos: budget de US$ 10 com alerta em 80%, `timeout` de 15 min por job,
`MaxConcurrentRuns=1`. Se algo travar, o teto do estrago é conhecido.

## 3. Executar o fechamento (3 dias, em ordem — o saldo é incremental)

```bash
ARN=$(terraform -chdir=terraform output -raw state_machine_arn)
for dt in 2026-08-20 2026-08-21 2026-08-22; do
  aws stepfunctions start-execution --state-machine-arn "$ARN" --input "{\"dt\": \"$dt\"}"
  # aguardar SUCCEEDED antes do próximo (console ou describe-execution)
done
```

O agendamento real (22:05 America/Sao_Paulo) fica provisionado e resolve o `dt`
sozinho — a execução manual acima é o replay parametrizado (mesmo mecanismo do
reprocessamento).

## 4. Checklist de evidências

1. **Step Functions**: grafo da execução verde (Bronze → Silver → Gold) + duração.
2. **Glue console → Jobs → Runs**: histórico com worker type, DPU-hours e logs.
3. **Data Catalog**: databases `bronze/silver/gold/ref` e as 9 tabelas Iceberg.
4. **CloudWatch Logs**: uma linha de log JSON estruturado (evento `qualidade_particao`).
5. **Quarentena**: contagem por motivo (job de consulta rápida ou Athena*).
6. **Custo real**: Cost Explorer (D+1), filtro serviço Glue + tag `projeto` —
   anotar o número medido para comparar com a estimativa da seção 2.
7. **Alarme/SNS**: e-mail de teste (parar o schedule 1 dia ou `aws sns publish`).

*Nota Athena: o suporte de leitura a Iceberg **V3** no Athena é recente/parcial;
se a query reclamar de `format-version`, a evidência do catálogo é o console do
Glue Data Catalog + um job Spark de consulta (o cenário de compatibilidade de
leitura previsto no ADR-004).

## 5. Demonstrações de robustez (opcionais)

- **Idempotência**: reexecutar o dia 2026-08-21 e mostrar que as contagens
  do Gold não mudam (INSERT OVERWRITE dinâmico da partição).
- **Falha + redrive**: renomear temporariamente o CSV no S3 → execução
  falha no Bronze → e-mail do SNS chega → restaurar o arquivo → **Redrive** no
  console retoma do estado que falhou, sem reprocessar o que já passou.

## 6. Encerrar

```bash
terraform -chdir=terraform destroy   # bucket com force_destroy: limpeza completa
```
