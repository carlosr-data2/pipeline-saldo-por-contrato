# ADR-011 — PySpark, não Scala

## Contexto: a pergunta certa não é "qual linguagem é mais rápida"

O desafio pede PySpark ou Scala, com justificativa. A discussão clássica
("Scala é mais rápida porque o Spark é JVM") esconde uma nuance que decide a
questão: com a **API DataFrame**, o código Python e o código Scala geram o
**mesmo plano de execução** — quem executa é o Catalyst, na JVM, nos dois
casos. A diferença de performance só aparece quando dados atravessam a
fronteira JVM↔Python, ou seja: **em UDFs Python**. Sem UDFs, a escolha da
linguagem é uma escolha de *ergonomia e manutenção*, não de velocidade.

## Decisão

**PySpark**, com uma restrição de estilo que sustenta a escolha: só API
DataFrame/SQL, **zero UDFs Python**. Toda a lógica do pipeline — sinal e
estorno, as cinco regras, o hash de desempate do dedup — está escrita em
expressões nativas do Spark (`when`, `sha2`, `concat_ws`, janelas), que rodam
inteiramente na JVM.

Com a performance neutralizada, os critérios que sobram apontam para Python:

- **Glue first-class**: no Glue 5.0, script Python vai direto para o S3 — sem
  etapa de build; Scala exigiria pipeline de compilação e empacotamento de JAR;
- **Manutenção**: Python é a língua franca dos times de dados — revisão de
  código, on-call e evolução ficam acessíveis ao time inteiro, não a um
  subconjunto;
- **Ferramental do projeto**: pytest, oráculo de verificação e profiling no
  mesmo ecossistema do pipeline.

## Alternativa rejeitada

**Scala.** Ganharia a disputa em três cenários, nenhum presente aqui: uma UDF
pesada e inevitável (a fronteira JVM↔Python passaria a custar caro); modelagem
de domínio complexa com Datasets tipados; ou um time já JVM. Se algum desses
surgir, a fronteira está limpa — os jobs não dependem de nada específico de
Python além da API pública do Spark.

## Consequências

A regra "sem UDF" virou **critério de revisão de código**: lógica nova precisa
caber em expressão Catalyst. Se um dia não couber, a exceção entra consciente e
documentada — preferindo `pandas_udf` vetorizada (que amortiza o custo da
fronteira) a UDF linha a linha — e não como hábito que degrada o pipeline aos
poucos.
