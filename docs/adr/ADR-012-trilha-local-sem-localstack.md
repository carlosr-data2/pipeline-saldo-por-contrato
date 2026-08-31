# ADR-012 — Trilha local: Spark puro em Docker; catálogo por configuração; sem LocalStack

## Contexto: o que uma demo local precisa provar — e o que ela não consegue provar

A demonstração ao vivo não pode depender de rede, credencial ou disponibilidade
de nuvem — a regra de ouro de qualquer demo. Ao mesmo tempo, o desafio pergunta
explicitamente pelo equivalente local do Glue Data Catalog (P4.5). Isso coloca a
questão central desta ADR: **qual é a fronteira honesta entre o que se valida
localmente e o que só a nuvem real valida?**

A resposta adotada: localmente valida-se **o pipeline** (lógica, dados,
formato de tabela, catálogo) com paridade de motor; a **plataforma AWS** (Glue
de verdade, IAM de verdade, catálogo gerenciado) se valida onde ela existe — na
conta real, por centavos.

## Decisão

- **Trilha local = os MESMOS três jobs**, executados em Docker com as versões
  pinadas que vão para a nuvem: Spark 3.5.4 + Java 17 + Iceberg 1.10.2 (a
  tríade do Glue 5.0, com o runtime Iceberg que o próprio Terraform envia via
  `--extra-jars`). `make demo` roda tudo em um comando, **sem nenhuma chamada
  de API AWS** no caminho crítico.
- **Catálogo por configuração**: uma única função (`criar_spark` em
  `lib/session.py`) decide a implementação do catálogo Iceberg. Local:
  **HadoopCatalog** — os metadados das tabelas vivem no filesystem, cumprindo
  localmente o papel do catálogo. AWS: **GlueCatalog** — o Glue Data Catalog,
  com warehouse em S3. Nenhuma linha de job muda entre as trilhas; é a resposta
  concreta à P4.5.
- **Sem LocalStack**: a infraestrutura se *valida* com `terraform validate` +
  CI, e se *prova* na conta real (runbook, < US$ 1 o exercício completo).

## Alternativas rejeitadas

**1. LocalStack (emular Glue/SFN/S3 localmente).** O argumento decisivo: emular
o Glue **testa a emulação, não a arquitetura** — job Glue real, IAM real e
catálogo gerenciado real só existem na AWS; um verde no emulador não prova que
o verde acontece na nuvem, e um vermelho pode ser bug do emulador. Somado a
isso, o custo de manter o emulador funcionando supera o valor da evidência
quando a evidência real custa centavos. LocalStack tem seu lugar (testar SDKs e
integrações finas offline); orquestração de Glue não é esse lugar.

**2. HadoopCatalog em produção** (a simetria inversa: levar o catálogo local
para a nuvem). Rejeitada com a mesma lógica de "cada coisa no seu habitat": o
HadoopCatalog não tem governança central (permissões finas, integração com
Athena/Lake Formation) e seu commit é baseado em rename — atômico em
filesystem, **frágil em S3** com múltiplos escritores. Perfeito para demo em
disco local; errado para um lakehouse corporativo.

## Consequências

- A demo é **determinística e offline** — o plano A da defesa não tem
  dependência externa nenhuma.
- A AWS muda de papel: deixa de ser dependência e vira **evidência** (histórico
  de jobs no console, tabelas no Catalog, custo medido no Cost Explorer).
- O preço da fronteira: diferenças exclusivas da plataforma (limites do Glue,
  IAM, comportamento do catálogo gerenciado) só aparecem na trilha AWS — por
  isso ela existe no runbook e não foi descartada.
