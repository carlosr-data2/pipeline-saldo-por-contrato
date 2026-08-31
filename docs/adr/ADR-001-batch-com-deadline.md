# ADR-001 — Batch crítico com deadline, não event-driven

## Contexto: classificar o problema antes de desenhar a solução

A primeira decisão de arquitetura não é de tecnologia — é de **classificação do
problema**. Três características do caso apontam a mesma direção: o dado chega
em **lote diário** (há um `id_lote` por dia e uma partição `dt_processamento`);
o consumidor é **um só e tem hora marcada** (o fechamento contábil de D+1 às
06:00); e o requisito de latência é um **deadline**, não frescor contínuo —
ninguém precisa do saldo atualizado às 14h37, precisa dele correto antes das
06:00.

Isso é a definição de um *batch crítico com deadline*. Errar essa classificação
custaria caro: tratar como streaming adicionaria complexidade permanente para
resolver um problema que não existe; tratar como batch "relaxado" ignoraria que
perder as 06:00 tem consequência regulatória.

## Decisão

Pipeline batch disparado por **tempo**: EventBridge Scheduler às 22:05
(America/Sao_Paulo), logo após a janela em que o dado fica pronto. O gatilho é o
relógio + a verificação de prontidão — não a chegada de um arquivo, não um
evento.

## Alternativas rejeitadas

**1. Streaming (Kinesis/Firehose + Spark Structured Streaming).** Não existe
fonte de eventos no caso — o upstream entrega lote. Adotar streaming exigiria
inventar a fonte, pagar custo fixo de infraestrutura contínua e manter estado de
agregação para 80M+ de contas — tudo isso sem reduzir o único risco que importa
(perder as 06:00). Streaming seria a escolha certa se o consumidor precisasse do
saldo intradiário; este consumidor não precisa.

**2. Event-driven por chegada de arquivo (`s3:ObjectCreated` → pipeline).**
Tem apelo — "processa assim que chegar" — mas acopla o disparo ao layout de
entrega do upstream: se o lote vier em N arquivos, são N eventos, e passa a ser
necessário um marcador de "lote completo" para não processar pela metade. Vira
a escolha certa **se** o contrato de entrega evoluir para incluir um
arquivo-marcador de lote fechado — registrado aqui como evolução natural, não
como default.

## Consequências

Gatilho por tempo tem um ponto cego: ele não percebe sozinho que o lote **não
chegou** ou que o disparo **se perdeu**. Por isso esta decisão obriga as duas
seguintes: detecção de ausência via alarmes-sentinela e DLQ no disparo
(ADR-003). Quem escolhe cron assume o dever de vigiar o silêncio.
