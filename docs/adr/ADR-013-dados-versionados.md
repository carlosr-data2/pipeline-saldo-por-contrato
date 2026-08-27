# ADR-013 — Dados de exemplo versionados no repositório

## Contexto
O dataset (CSV de 31 MB + referencial COSIF) precisa estar à mão para
clone-and-run, testes de CI e para o ZIP do projeto (limite prático de ~25 MB de
arquivo).

## Decisão
Versionar os CSVs em `dados/` no git, sem LFS: 31 MB < limites do GitHub
(100 MB/arquivo), e quem clona roda `make demo` direto após o clone — zero
passos de download. O CI usa os mesmos dados no teste-oráculo.

## Alternativas rejeitadas
- **Git LFS**: adiciona dependência de ferramenta no clone de quem usa o repositório para
  economizar 31 MB — atrito sem benefício neste tamanho.
- **Download externo (S3/link)**: quebra o clone-and-run offline e cria um ponto
  de falha na hora em que precisa rodar.

## Consequências
No ZIP do projeto, o CSV entra COMPRIMIDO pelo próprio ZIP (~9 MB) — cabe com
folga; `scripts/package_zip.sh` confere o tamanho final e falha se
passar de 25 MB.
