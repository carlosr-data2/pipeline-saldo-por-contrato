# Análise exploratória do dataset — feita antes do desenho

O dataset foi explorado antes de qualquer linha de pipeline, e os achados
mudaram decisões de arquitetura. Este é o resumo; cada consequência aponta
para o ADR onde a decisão está fundamentada.

**Base:** 199.998 registros, 3 partições diárias (2026-08-20/21/22) de
66.666 registros cada; referencial COSIF com 8 códigos válidos.

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

## Os termos da tabela, em miúdos

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

## Duas leituras que a tabela sustenta

- **Nada é descartado em silêncio.** As 3.276 linhas excedentes de duplicata
  terminam 100% explicadas: 1.027 perdedoras dentro do lote + 2.053
  rejeitadas contra o histórico + 196 que caíram por violação própria antes
  de disputar. A conferência é do oráculo independente dos testes.
- **Regra de qualidade nasce do contrato, não do engenheiro.** O achado mais
  grave (83% de incoerência COSIF) não virou bloqueio justamente porque o
  contrato não o pede: virou métrica publicada e pergunta a quem é dono da
  decisão.

Todos os números desta página são reproduzíveis em segundos, por uma contagem
independente do pipeline (Python puro, sem Spark — no mesmo espírito do
oráculo dos testes):

```bash
python3 scripts/analise_exploratoria.py
```

E as contagens por motivo também ficam publicadas na tabela
`silver.dq_relatorio` a cada execução do pipeline.
