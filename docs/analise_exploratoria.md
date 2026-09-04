# Análise exploratória do dataset — feita antes do desenho

O dataset foi explorado antes de qualquer linha de pipeline, e os achados
mudaram decisões de arquitetura. Este é o resumo; cada consequência aponta
para o ADR onde a decisão está fundamentada.

- [Análise exploratória do dataset — feita antes do desenho](#análise-exploratória-do-dataset--feita-antes-do-desenho)
  - [Achado por achado — da análise à decisão](#achado-por-achado--da-análise-à-decisão)
  - [Achados e consequências](#achados-e-consequências)
  - [Os termos da tabela](#os-termos-da-tabela)
  - [A incoerência COSIF × tipo, em duas checagens](#a-incoerência-cosif--tipo-em-duas-checagens)
  - [Os números traçados](#os-números-traçados)
  - [Por que medir e não bloquear (a decisão, ADR-008)](#por-que-medir-e-não-bloquear-a-decisão-adr-008)
  - [Duas leituras que a tabela sustenta](#duas-leituras-que-a-tabela-sustenta)

**Base:** 199.998 registros, 3 partições diárias (2026-08-20/21/22) de
66.666 registros cada; referencial COSIF com 8 códigos válidos.

## Achado por achado — da análise à decisão

1. **Duplicatas não são re-entregas → a dedup virou ADR.**
Achado: 2.736 grupos de id_transacao duplicado e nenhum grupo com conteúdo idêntico — são payloads DIFERENTES disputando o mesmo id. Consequência: dropDuplicates() (a solução de uma linha) escolheria o vencedor por sorteio — inaceitável em contabilidade. Nasceu o ADR-006: vencedor determinístico (dt_lancamento + hash como desempate) e perdedores para a quarentena com motivo. Sem a EDA, dropDuplicates pareceria suficiente.

2. **71% das duplicatas cruzam partições → nasceu o lookback.**
Achado: 1.940 dos 2.736 grupos têm as cópias em DIAS diferentes. Consequência: dedup só dentro do lote de hoje deixa passar a maioria — o incremental diário precisa checar contra o que já foi publicado. Nasceu o anti-join contra a silver numa janela de 7 dias — e a renegociação do "único globalmente" (inviável a 300M/dia × 5 anos) para "unicidade em janela". Um número (71%) definiu um componente inteiro do silver.

3. **Os volumes de violação → calibraram o gate.**
Achados: valor≤0 (3.989), COSIF fora do domínio (3.309), data futura (3.257), conta nula (2.411) — total ~8,1% de linhas problemáticas. Consequência dupla: (a) a quarentena tinha que guardar TUDO com motivo, porque 8% de descarte silencioso é um buraco contábil; (b) o limiar do gate em 10% — o dataset real passa (8,1% < 10%), e o bloqueio se prova por teste com limiar artificial. O 10% não foi chutado; foi calibrado no dado.

4. **A incoerência de 83% → virou dado, não bloqueio.**
Achado: 83,3% dos registros válidos têm cod_cosif incoerente com o tipo_contrato. Consequência: se isso fosse regra de bloqueio, o pipeline rejeitaria o dataset quase inteiro — então claramente NÃO é violação do contrato (que só exige existir no domínio); é uma pergunta ao data owner. Nasceu o ADR-008: a flag_coerente na gold classificacao_cosif expõe a incoerência como dado consultável. A EDA impediu o erro clássico de inventar regra que o contrato não pediu — e que teria matado o pipeline.

5. **Sinal não definido + 5 tipos de lançamento → a convenção do ADR-007.**
Achado: tipo_lancamento tem 5 valores e o contrato não diz quem soma e quem subtrai — sem isso, não existe saldo. Consequência: convenção assumida e documentada (CREDITO/JUROS = +; DEBITO/TARIFA/IOF = −; estorno inverte), com oráculo independente validando. A EDA revelou que o requisito central era incompleto — antes de escrever uma linha do gold.

6. **Sem saldo de abertura → o snapshot parte do vazio (ADR-005).**
Achado: os 3 dias são só movimento; não há posição inicial. Consequência: primeiro dia parte de snapshot vazio, e é por isso que os saldos saem negativos no dataset sintético — visível em qualquer consulta à gold.

## Achados e consequências

| Achado | Número | Consequência no desenho |
|---|---|---|
| `id_transacao` duplicado | 2.736 grupos, 3.276 linhas excedentes | **100% com payload divergente** (valores, contas e datas diferentes sob o mesmo id): são colisões, não reentregas — `dropDuplicates` descartado; política determinística de dedup (ADR-006) |
| Duplicatas cruzando dias | 1.940 de 2.736 (**71%**) | dedup só-dentro-do-lote deixaria a maioria passar → anti-join contra o histórico publicado, com janela de lookback (ADR-006) |
| `valor_lancamento <= 0` | 3.989 (2.006 negativos + 1.983 zeros) | quarentena pela regra de valor positivo; estorno legítimo tem valor > 0 e semântica na flag (ADR-007) |
| `cod_cosif` fora do domínio | 3.309 (todos `9.9.9.99.9`) | quarentena pela regra de existência no domínio |
| `dt_lancamento` posterior ao processamento | 3.257 | quarentena pela regra de consistência de datas — e prova que a data de negócio chega fora de ordem, o que decide a partição por `dt_processamento` |
| `id_conta` nulo/vazio | 2.411 | quarentena pela regra de completude: sem chave não há agregação por conta |
| COSIF incoerente com `tipo_contrato` | **83,3%** dos registros válidos | o código existe no domínio, mas pertence a outro tipo de contrato. NÃO é regra do contrato → vira métrica de observabilidade + pergunta formal ao data owner, nunca bloqueio (ADR-008) |
| Taxa total de violação | ~8% por dia (8,07 / 8,08 / 7,93%) | dimensiona o limiar do gate: com default de 10%, este dataset fecha — e o mecanismo de bloqueio é provado por teste com limiar artificial (ADR-009) |

## Os termos da tabela

**Grupo e linhas excedentes.** Grupo é o conjunto de linhas que compartilham o
mesmo `id_transacao`; duplicado é o grupo com 2 ou mais. Linhas excedentes são
as que sobram além da primeira de cada grupo — um id com 3 ocorrências conta
como 1 grupo e 2 excedentes. É por isso que 3.276 > 2.736: alguns ids aparecem
três vezes ou mais. As excedentes medem exatamente quantas linhas a
deduplicação precisa tirar do caminho do Silver para a unicidade valer.

**Payload divergente.** O id é a etiqueta da linha; o payload é a carga — tudo
o mais: valor, conta, datas, agência, tipo. Payload divergente significa mesma
etiqueta com cargas diferentes: sob o mesmo id, outra transação.

**Reentrega × colisão.** Reentrega é a mesma transação chegando duas vezes
(payload idêntico): colapsar em uma não perde nada. Colisão é o sistema de
origem emitir o mesmo id para duas transações reais distintas (payload
divergente): escolher qual sobrevive decide qual valor entra no saldo, e o
pipeline não tem como saber qual é a "dona legítima" do id. Neste dataset,
100% dos grupos são colisões — nenhum tem cópia idêntica.

**Dedup determinística.** A mesma entrada produz sempre o mesmo vencedor, em
qualquer ordem de leitura e em qualquer cluster. O critério é total, sem
empate possível: linhas válidas primeiro, depois o `dt_lancamento` mais
antigo, e o hash SHA-256 da linha inteira como desempate final. Um
`dropDuplicates` não dá essa garantia — ele mantém a primeira linha que
aparecer, que é ordem física de leitura, não critério — e duas execuções
poderiam publicar saldos diferentes. Há teste que embaralha a ordem da
entrada e exige o mesmo resultado.

## A incoerência COSIF × tipo, em duas checagens

Pergunta 1 (regra do contrato, R4): "esse código EXISTE?"
O referencial cosif_dominio.csv tem 8 códigos válidos. Se o registro traz 9.9.9.99.9, que não está na lista → violação → quarentena. Foram 3.309.

Pergunta 2 (NÃO é regra do contrato): "esse código existe, mas é do tipo CERTO?"
Repare no referencial: cada código tem um tipo_contrato_associado — 1.3.1.00.0 é "Títulos - CDB", associado ao tipo CDB; 1.2.1.00.0 é "Poupança", associado ao tipo POUP. A incoerência é: um contrato de POUPANÇA carregando o código contábil de CDB. O código é real, existe no domínio, passa na regra 1 — mas a combinação é estranha.

A analogia: é um CEP válido de outra cidade. O envelope tem um CEP que existe de verdade nos Correios (passa na validação "CEP existe") — mas é CEP de São Paulo num endereço escrito Curitiba. Você não devolve a carta; você anota a esquisitice e pergunta a quem mantém o cadastro.

## Os números traçados

196.689 = os registros cujo código passou na pergunta 1 (199.998 totais − 3.309 com código inexistente).

163.938 = desses, quantos falham na pergunta 2: código válido, mas associado a OUTRO tipo de contrato.
83,3% = 163.938 ÷ 196.689.

E o detalhe: 83,3% é exatamente 5/6. Há 6 tipos de contrato; se o gerador do dataset sorteou o código independente do tipo, a chance de acertar o tipo por acaso é ~1/6 — e é precisamente o que se observa (16,7% coerente).

Ou seja: o próprio número denuncia que, nesse dado sintético, código e tipo foram sorteados separadamente. Isso reforça a decisão: não é "83% do banco está errado", é "a relação código×tipo desse dataset não carrega informação — quem decide o que fazer é o dono do dado".

## Por que medir e não bloquear (a decisão, ADR-008)

O contrato só pede a pergunta 1. Bloquear pela pergunta 2 seria inventar regra que ninguém pediu.
Se bloqueasse, quarentenaria 83% do banco — o fechamento não sairia. Uma "regra de qualidade" que reprova quase tudo não é regra, é sintoma de que a premissa está errada.
Então vira DADO: a gold classificacao_cosif calcula a flag_coerente (src/lib/saldo.py — literalmente tipo_contrato == tipo_contrato_associado) e reporta a distribuição. O analista consulta por SQL, o data owner decide.
Em síntese: código inexistente é violação e bloqueia; código de outro tipo é anomalia e vira flag — porque a primeira é regra do contrato e a segunda é pergunta ao dono do dado.

## Duas leituras que a tabela sustenta

- **Nada é descartado em silêncio.** As 3.276 linhas excedentes de duplicata
  terminam 100% explicadas: 1.027 perdedoras dentro do lote + 2.053
  rejeitadas contra o histórico + 196 que caíram por violação própria antes
  de disputar. A conferência é do oráculo independente dos testes.
- **Regra de qualidade nasce do contrato.** O achado mais
  grave (83% de incoerência COSIF) não virou bloqueio justamente porque o
  contrato não o pede: virou métrica publicada e pergunta a quem é dono da
  decisão.

Todos os números desta página são reproduzíveis em segundos, por uma contagem
independente do pipeline:

```bash
python3 scripts/analise_exploratoria.py
```

E as contagens por motivo também ficam publicadas na tabela
`silver.dq_relatorio` a cada execução do pipeline.
