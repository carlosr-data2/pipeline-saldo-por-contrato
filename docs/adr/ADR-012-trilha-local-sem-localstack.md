# ADR-012 — Trilha local: Spark puro em Docker; catálogo por configuração; sem LocalStack

## Contexto
A demo ao vivo não pode depender de rede/conta AWS. E o desafio pergunta
explicitamente pelo equivalente local do Glue Data Catalog (P4.5).

## Decisão
- Trilha local = os MESMOS três jobs PySpark, em Docker (Spark 3.5.4 + Java 17 +
  Iceberg 1.10.2 — paridade pinada com o Glue 5.0), `make demo` = 1 comando.
- Catálogo por configuração (`lib/session.criar_spark`): local usa **HadoopCatalog**
  (metadados no filesystem); AWS usa **GlueCatalog** (Glue Data Catalog) com
  warehouse em S3. Nenhuma linha de job muda entre as trilhas.
- **Sem LocalStack**: zero API AWS no caminho crítico da demo. A infra se valida
  com `terraform validate` + CI e se PROVA na conta real por centavos (runbook).

## Alternativas rejeitadas
- **LocalStack**: emular Glue/SFN localmente testa a emulação, não a arquitetura —
  Glue job real, IAM real e catálogo real só existem na AWS. Custo de manter o
  emulador > valor da evidência, dado que a conta real custa < US$ 1 (runbook §2).
- **HadoopCatalog em produção**: sem catálogo central (governança, permissões
  finas, integração Athena/LakeFormation) e com commit por rename — certo para
  demo em filesystem, errado para S3 multi-writer. Cada catálogo no seu habitat.

## Consequências
A demo é determinística e offline (plano A da defesa); a AWS vira evidência
(histórico de jobs, tabelas no Catalog, custo medido) em vez de dependência.
