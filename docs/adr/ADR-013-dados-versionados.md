# ADR-013 — Dados do desafio versionados no repositório

## Contexto
O dataset (CSV de 31 MB + referencial COSIF) precisa estar à mão para
clone-and-run, testes de CI e para o ZIP de entrega (limite prático de ~25 MB de
anexo de e-mail).

## Decisão
Versionar os CSVs em `dados/` no git, sem LFS: 31 MB < limites do GitHub
(100 MB/arquivo), e o avaliador roda `make demo` direto após o clone — zero
passos de download. O CI usa os mesmos dados no teste-oráculo.

## Alternativas rejeitadas
- **Git LFS**: adiciona dependência de ferramenta no clone do avaliador para
  economizar 31 MB — atrito sem benefício neste tamanho.
- **Download externo (S3/link)**: quebra o clone-and-run offline e cria um ponto
  de falha na véspera da defesa.

## Consequências
No ZIP de entrega, o CSV entra COMPRIMIDO pelo próprio ZIP (~9 MB) — cabe com
folga no e-mail; `scripts/package_zip.sh` confere o tamanho final e falha se
passar de 25 MB.
