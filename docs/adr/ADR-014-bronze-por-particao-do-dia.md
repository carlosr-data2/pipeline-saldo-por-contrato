# ADR-014: Bronze por partição do dia, com a origem no formato do contrato

## Contexto: o que a revisão encontrou

Na primeira versão, o Bronze lia o CSV único do dataset de exemplo inteiro a cada
execução e sobrescrevia todas as partições presentes nele. Funciona para os três
dias do exemplo, mas não sustenta o fechamento diário: a Step Function resolvia o
`dt` e o passava ao Silver e ao Gold, e o Bronze não recebia nada. Ele não sabia
qual arquivo era "o de hoje", reingeria dias já fechados (carimbando um
`_ts_ingestao` novo em partições que não mudaram) e, a 300 M de linhas/dia, faria
trabalho proporcional ao arquivo inteiro, não ao dia. O próprio contrato define a
origem como Parquet particionado por `dt_processamento`; o CSV único é o formato
do dataset de exemplo, não o da produção.

## Decisão

1. O Bronze recebe `--dt` como os outros estágios. Com ele, ingere só a
   partição do dia e sobrescreve só ela (INSERT OVERWRITE dinâmico de uma
   partição). Sem `--dt`, mantém o comportamento de carga inicial: todas as
   partições presentes na origem.
2. A origem é lida no formato do contrato (`--formato parquet`: diretório
   `raw/<nome>/dt_processamento=YYYY-MM-DD/`), com o CSV único mantido como
   formato de compatibilidade (`--formato csv`) para a demo e os testes. Em
   qualquer formato, os campos chegam ao Bronze como texto; a tipagem continua
   sendo responsabilidade do pipeline, como a especificação pede.
3. O filtro do dia é sobre a coluna de partição tipada. No Parquet
   particionado, o otimizador o empurra até a leitura como filtro de partição:
   só o diretório do dia é aberto (há teste que confere isso no plano físico).
   Trabalho proporcional ao dia, como nos outros estágios.
4. Partição vazia na origem falha no Bronze. Antes, um dia sem lote só
   era acusado pelo Silver ("partição vazia no Bronze"), um job depois. Agora o
   Bronze conta a partição antes de escrever e falha com "partição vazia na
   origem: lote não chegou ou data errada".
5. O commit vai para o log. O evento `bronze_commit` publica o resumo do snapshot
   Iceberg recém-criado (`changed-partition-count`, `added-records`,
   `added-data-files`), lido dos metadados da própria tabela. É a prova de que uma
   execução por dia toca uma única partição.

A conversão CSV → Parquet particionado é um script à parte
(`scripts/csv_para_parquet_particionado.py`), usado pela demo
(`ORIGEM=parquet make demo`) e pela publicação em `raw/` na AWS
(`make aws-publicar-origem`).

## Alternativas rejeitadas

**1. Manter a reingestão total.** Simples, mas O(arquivo) por dia e reescrita de
partições já fechadas. Em contabilidade, tocar um dia fechado sem motivo é ruído
de auditoria, mesmo que o conteúdo seja idêntico.

**2. Job bookmarks ou crawler do Glue para descobrir "o que é novo".** Acoplaria a
semântica do lote a um mecanismo de estado do Glue, opaco à trilha local e ao
redrive: reprocessar um dia exigiria manipular o bookmark. O dia como parâmetro
explícito é reproduzível, testável e idêntico nas duas trilhas.

**3. Disparo por chegada de arquivo em `raw/`.** Já rejeitado no ADR-001 (exige
marcador de lote completo). O parâmetro `dt` é compatível com essa evolução: o
evento só teria de passá-lo.

## Consequências

- A Step Function passa o `dt` aos três estágios; uma execução com input vazio
  resolve a data de hoje e, se o lote não chegou, falha no Bronze, mais cedo e
  com um job a menos na conta.
- O retry do Bronze na Step Function (2 tentativas, pensado para I/O
  transitório) agora também retenta a falha determinística "lote não chegou".
  É o mesmo trade-off registrado para o Silver e o Gold no ADR-002. Se isso
  incomodar, a saída é a mesma proposta lá: decidir na state machine em vez de
  dentro do job.
- A contagem antes da escrita custa uma leitura extra da partição do dia (não do
  histórico). Aceito, porque evita que `overwritePartitions` vire no-op em
  silêncio com um DataFrame vazio.
- O Terraform ganha `origem_formato` (padrão `parquet`), `origem_nome` (apontar
  o pipeline a outro dataset em `raw/`, como o sintético dos laboratórios) e
  `agendamento_ativo` (desligar o cron durante experimentos). Os laboratórios
  que exercitam tudo isso na conta real estão em `docs/labs_aws.md`.
