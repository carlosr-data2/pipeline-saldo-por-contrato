# ADR-006 — Unicidade sobre o publicado (dedup determinística + lookback)

## Contexto: o que os dados revelaram antes de qualquer regra

O contrato exige que `id_transacao` seja "único globalmente". A primeira reação
seria um `dropDuplicates` e pronto, mas a análise exploratória do dataset mudou o problema
por completo. Dois achados:

1. **Nenhuma duplicata é uma cópia idêntica.** Dos 2.736 ids duplicados, 100%
   têm conteúdo divergente entre as ocorrências: valores, contas e datas
   diferentes sob o mesmo id. Ou seja, não são reentregas do mesmo registro (que
   poderiam ser colapsadas sem perda), são **colisões**: duas transações
   diferentes disputando o mesmo identificador.
2. **71% das duplicatas cruzam partições** (1.940 de 2.736): o mesmo id aparece
   em dias de processamento diferentes. Uma deduplicação que só olha dentro do
   lote do dia deixaria a maioria passar.

Isso descarta o `dropDuplicates` por duas razões: ele escolhe o sobrevivente de
forma **não determinística** (depende da ordem física de leitura; duas execuções
podem publicar registros diferentes, o que destrói a idempotência), e **descarta
em silêncio**, inaceitável em dado regulatório, onde toda exclusão precisa de
motivo e rastro. Escolher qual registro sobrevive virou, portanto, uma decisão de
arquitetura.

## Decisão

O princípio que resolve todos os casos: **unicidade é sobre o que entra no razão
(Silver), não sobre o que chega.** Desdobrado em quatro regras:

1. **Dentro do lote** — quando duas ou mais linhas *válidas nas demais regras*
   disputam o mesmo id, vence a de `dt_lancamento` mais antigo; empate é
   decidido pelo hash SHA-256 da linha inteira. O critério é 100% determinístico:
   a mesma entrada produz o mesmo vencedor, em qualquer ordem de arquivo e em
   qualquer reexecução (há teste que embaralha a ordem e exige resultado igual).
2. **Linha inválida não participa da disputa.** Um registro que já violou outra
   regra (valor, data, COSIF, nulo) vai para a quarentena *pelo próprio motivo*:
   ele não pode "vencer" a janela e, mais importante, não pode **condenar** uma
   linha válida com o mesmo id. Sem essa cláusula, um registro quebrado
   bloquearia a versão correta da mesma transação.
3. **Entre lotes** — cada dia é comparado (anti-join) contra os ids **já
   publicados no Silver** numa janela de lookback configurável (padrão: 7 dias).
   Id já publicado vence sempre: não se retrata um dado que já entrou em
   fechamento contábil anterior.
4. **A consequência deliberada**, e talvez a mais importante operacionalmente:
   como a comparação histórica olha só o *publicado*, o **reenvio corrigido de um
   registro quarentenado é aceito**. O fluxo natural de correção (upstream
   conserta o registro rejeitado e reenvia com o mesmo id) funciona; a quarentena
   existe para permitir correção, não para bloquear um id definitivamente.

E, como em todo o pipeline, **perdedores nunca são descartados**: vão para a
quarentena com motivo (`ID_TRANSACAO_DUPLICADO_NO_LOTE` ou
`ID_TRANSACAO_JA_PROCESSADO`).

## Alternativas rejeitadas

**1. Unicidade global literal (contra 5 anos de histórico).** Levar o "único
globalmente" ao pé da letra significaria, em produção, um anti-join diário de
300 milhões de ids contra ~550 **bilhões** de chaves acumuladas. Existem
mitigações conhecidas (bloom filter por partição, bucketing por chave), mas o
custo não paga o risco marginal: uma colisão legítima de UUID reaparecendo
*depois* de 7 dias é patologia de sistema upstream, não fluxo normal, e o lugar
de tratá-la é o próprio upstream. A postura adotada: implementar a janela e
**devolver a questão ao data owner** como proposta de renegociação do contrato
("único globalmente" → "único em janela de N dias"), registrada como pergunta
formal.

**2. Quarentenar o grupo inteiro em caso de colisão.** Mais "conservador" à
primeira vista, mas pior na prática: junto com o registro suspeito, joga fora o
registro **válido**. O cliente fica sem o lançamento correto no saldo, e o
fechamento perde dado bom por culpa de dado ruim.

**3. `dropDuplicates` / `row_number` sem critério declarado.** Não determinístico
(quebra idempotência e auditoria) e sem rastro do que foi removido. Em contexto
regulatório, é a diferença entre "removi X porque Y" e "sumiu".

## Consequências

- **Todos os números fecham, e são conferíveis.** As 3.276 linhas excedentes de
  duplicata do dataset terminam 100% explicadas: 1.027 perdedoras dentro do lote
  + 2.053 rejeitadas contra o histórico + 196 que caíram por violação própria
  (outra regra) antes de disputar a janela. A conferência é feita por um oráculo
  independente em Python puro, que reimplementa esta política e exige igualdade
  exata com o Spark.
- **Custo previsível em produção**: o anti-join histórico é limitado pela janela
  (7 dias ≈ ~2 bilhões de chaves, e não 550 bilhões), e a janela é um parâmetro;
  aumentá-la é decisão de negócio com preço conhecido.
- **Um detalhe fino de implementação** que a política exige: a ordenação da
  janela usa `valida DESC, dt_lancamento ASC, hash ASC` — o `valida DESC` é o que
  garante a regra 2 (inválidas ordenam depois de todas as válidas, então nunca
  ocupam a posição de vencedora).
