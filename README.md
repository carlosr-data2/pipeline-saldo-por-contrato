# Pipeline de Saldo por Contrato

Pipeline batch de contabilidade regulatória: cálculo diário de saldo consolidado por contrato e por conta,
com classificação COSIF e reconciliação débito × crédito, construído sobre Apache Spark + Apache Iceberg,
executável em duas trilhas:

- **Local (Docker)** — execução de ponta a ponta em um comando, sem nenhuma dependência de AWS.
- **AWS (Terraform)** — AWS Glue + Step Functions + EventBridge + Data Catalog, provisionados por IaC.

> Em construção — este README será completado com o diagrama de arquitetura e as instruções das duas trilhas.
