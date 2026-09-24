# ADR-013: Dados de exemplo versionados no repositório

## Contexto: onde os dados precisam estar

O dataset de exemplo (CSV de 31 MB + referencial COSIF) precisa estar disponível em
dois lugares: na máquina de quem clona o repositório (para o `make demo`
funcionar de primeira) e no CI (o teste-oráculo processa o dataset completo a
cada push).

Isso define a primeira experiência de quem clona o repositório: `git clone && make demo`
funcionando sem nenhum passo extra, ou uma lista de pré-requisitos antes da
primeira execução.

## Decisão

Versionar os CSVs diretamente em `dados/`, no git, sem LFS. Os 31 MB estão
confortavelmente abaixo dos limites do GitHub (100 MB por arquivo), o clone traz
tudo, e o CI usa os mesmos arquivos sem download adicional. O dado é imutável
(é o dataset de exemplo, não muda), então o custo clássico de versionar dados
(histórico inchando a cada atualização) não se aplica.

## Alternativas rejeitadas

**1. Git LFS.** Resolveria um problema que não existe neste tamanho: LFS vale a
pena quando arquivos são grandes ou mudam com frequência. Aqui, adicionaria uma
dependência de ferramenta no clone (quem não tem LFS instalado
recebe ponteiros em vez de dados, e uma execução quebrada) para economizar 31 MB
que o git puro carrega sem esforço.

**2. Download externo (S3, link no README).** Quebra o clone-and-run offline,
acrescenta um passo manual antes da primeira execução e cria um ponto de falha
externo, que só seria descoberto quando alguém tentasse rodar.

## Consequências

- **Clone-and-run real**: o pipeline roda no primeiro comando após o clone, em
  qualquer máquina com Docker.
- O repositório carrega ~31 MB a mais: custo aceito em troca da
  reprodutibilidade imediata.
