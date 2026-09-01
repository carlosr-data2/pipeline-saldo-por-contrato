# ADR-013 — Dados do desafio versionados no repositório

## Contexto: uma decisão pequena que define a primeira impressão

O dataset do caso (CSV de 31 MB + referencial COSIF) precisa estar disponível em
três lugares: na máquina de quem clona o repositório (para o `make demo`
funcionar de primeira), no CI (o teste-oráculo processa o dataset completo a
cada push), e no ZIP de entrega (que tem o limite prático de ~25 MB de anexo de
e-mail).

A decisão parece burocrática, mas define a **primeira experiência de quem
avalia**: `git clone && make demo` funcionando sem nenhum passo extra, ou
uma lista de pré-requisitos antes da primeira execução.

## Decisão

Versionar os CSVs diretamente em `dados/`, no git, **sem LFS**. Os 31 MB estão
confortavelmente abaixo dos limites do GitHub (100 MB por arquivo), o clone traz
tudo, e o CI usa os mesmos arquivos sem download adicional. O dado é imutável
(é o insumo do desafio, não muda), então o custo clássico de versionar dados
(histórico inchando a cada atualização) não se aplica.

## Alternativas rejeitadas

**1. Git LFS.** Resolveria um problema que não existe neste tamanho: LFS vale a
pena quando arquivos são grandes ou mudam com frequência. Aqui, adicionaria uma
dependência de ferramenta no clone (quem não tem LFS instalado
recebe ponteiros em vez de dados, e uma demo quebrada) para economizar 31 MB
que o git puro carrega sem esforço.

**2. Download externo (S3, link no README).** Quebra o clone-and-run offline,
acrescenta um passo manual antes da primeira execução e cria um ponto de falha
externo, que pode ser descoberto quebrado justamente na hora da demonstração.

## Consequências

- **Clone-and-run real**: a demo funciona no primeiro comando após o clone, em
  qualquer máquina com Docker.
- **ZIP dentro do limite**: no pacote de entrega o CSV entra comprimido pelo
  próprio ZIP (~9 MB; pacote total ~8,5 MB), com folga sob os 25 MB. O
  `scripts/package_zip.sh` confere o tamanho e **falha** se estourar, para o
  limite nunca ser descoberto pelo servidor de e-mail.
- O repositório carrega ~31 MB a mais: custo aceito conscientemente em troca
  da reprodutibilidade imediata.
