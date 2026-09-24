# ADR-009: Gate de fechamento, o que bloqueia e o que segue com quarentena

## Contexto: os dois erros possíveis de um controle de qualidade

Qualidade de dados em contexto regulatório é um trade-off entre dois erros de
naturezas opostas:

- **Fechar com dado ruim**: o balanço consolida lixo; o custo aparece depois,
  em auditoria, multa e retrabalho de republicação;
- **Não fechar por preciosismo**: qualquer violação trava o pipeline; com ~8%
  de quarentena típica (o normal deste dataset), o fechamento simplesmente nunca
  aconteceria, e a área de qualidade viraria um bloqueio permanente do próprio
  negócio.

A maioria dos pipelines resolve esse trade-off implicitamente, espalhado em
ifs pelo código. Aqui ele fica explícito, configurável e testável: um gate
com critérios nomeados, que qualquer pessoa (engenheiro, contador, auditor)
consegue ler e discutir.

## Decisão

Ponto de partida: nenhuma violação é descartada em silêncio.
Toda linha reprovada vai para `silver.quarentena` com um *array* de motivos
(uma linha pode violar mais de uma regra), e as contagens por motivo são
publicadas como tabela (`silver.dq_relatorio`), e não só em log.

Sobre essa base, o gate define três condições de bloqueio, em ordem de
severidade:

1. **Falha estrutural: erro imediato, nada publicado.** Arquivo ilegível,
   schema divergente do contrato, partição vazia. Sem estrutura não há lote,
   então não há como seguir com ressalva.
2. **Taxa de quarentena da partição acima do limiar: publica o diagnóstico e
   bloqueia o fechamento.** O limiar é parâmetro
   (`SALDO_GATE_MAX_QUARENTENA_PCT`, default 10%). A ordem das operações é
   deliberada: o Silver, a quarentena e o relatório são gravados
   antes do gate decidir; só então, se a taxa estourar, o job falha e o
   Gold nunca roda. Quem for acionado às 3h da manhã precisa encontrar
   o diagnóstico pronto (quantas violações, de que tipo, quais registros), e
   não um pipeline que falhou sem deixar rastro.
3. **Reconciliação cruzada divergente (no Gold): nada é publicado.** Antes de
   gravar qualquer saída, o job compara duas rotas de agregação independentes
   sobre o mesmo Silver: o líquido somado por agência deve bater com o movimento
   somado por contrato. Divergência acima da tolerância (US$ 0,01) aborta o job
   antes de qualquer escrita. Aqui, ao contrário do Silver, não existe
   "publicar o diagnóstico", porque um saldo errado publicado é exatamente o
   dano que se quer evitar.

O que viola regra mas fica abaixo do limiar segue: quarentena, relatório
e fechamento normal, com o número exposto. A decisão de
conviver com 8% de quarentena (ou de apertar o limiar) é do negócio, e o desenho
dá a ela o instrumento: um parâmetro e um relatório de tendência.

## Alternativas rejeitadas

**1. Bloquear em qualquer violação (tolerância zero).** Com a taxa típica deste
dataset (~8%), o fechamento nunca sairia. Tolerância zero em dado que chega
sujo paralisa o negócio sem protegê-lo.

**2. Nunca bloquear (só relatar).** O oposto: um lote com 60% de
quarentena (cenário coberto por teste) passaria batido e o balanço fecharia com
quase metade do movimento faltando.

**3. Limiar fixo em código (hard-coded).** O número certo (5%? 10%? 15%?) é
uma decisão do negócio que muda com o tempo e com a maturidade do upstream. Por
isso é parâmetro com default conservador, não constante.

## Consequências

- **Com o dataset de exemplo, o gate aprova**: taxas de 8,07% / 8,08% / 7,93%
  nos três dias, contra o limiar de 10%; o log registra `gate_aprovado` por
  partição, com a decomposição por motivo.
- **O mecanismo de bloqueio é provado por teste**: um teste roda o
  Silver com limiar artificialmente baixo e exige `GateReprovado`, verificando
  também que o diagnóstico (quarentena + relatório) foi publicado antes do
  bloqueio.
- **O caminho da falha está integrado à operação**: gate reprovado, job Glue
  falha, Step Function para (o Gold nunca inicia) e sai e-mail via SNS. A retomada
  após correção do upstream é o redrive; e se alguém tentar pular o dia
  reprovado, a guarda de continuidade do Gold (ADR-005) recusa.
- **O limiar configurável força a conversa com o negócio**: "estamos fechando
  com 8% de quarentena todo dia" deixa de ficar escondido no pipeline e vira
  uma linha de relatório que alguém precisa assinar.
