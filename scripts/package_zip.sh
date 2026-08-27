#!/usr/bin/env bash
# Gera o ZIP de entrega em dist/ a partir do que está VERSIONADO no git
# (git archive): nada de artefatos locais, estado de terraform, caches ou
# material que não pertence à entrega. Confere o limite de anexo de e-mail.
set -euo pipefail
cd "$(dirname "$0")/.."

NOME="${1:-pipeline-saldo-por-contrato}"
DESTINO="dist/${NOME}.zip"
LIMITE_MB=25

rm -rf dist && mkdir -p dist
git archive --format=zip -9 -o "${DESTINO}" HEAD

TAMANHO_MB=$(( $(stat -c%s "${DESTINO}") / 1024 / 1024 ))
echo "gerado: ${DESTINO} (${TAMANHO_MB} MB)"
echo
echo "conteúdo (nível 1 e 2):"
unzip -l "${DESTINO}" | awk '{print $4}' | grep -v '^$' | cut -d/ -f1-2 | sort -u

if [ "${TAMANHO_MB}" -ge "${LIMITE_MB}" ]; then
  echo "ERRO: ${TAMANHO_MB} MB >= limite de ${LIMITE_MB} MB para anexo de e-mail" >&2
  exit 1
fi
echo
echo "OK: abaixo do limite de ${LIMITE_MB} MB"
