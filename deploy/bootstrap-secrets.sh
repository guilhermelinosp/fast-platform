#!/usr/bin/env bash
# Cria o Secret `fast-database` (ns fast) copiando as credenciais do Postgres do cluster
# (ns tools, Secret postgres-credentials). Nenhum valor e impresso e nada vai para o git.
#   deploy/bootstrap-secrets.sh
set -euo pipefail

kubectl get namespace fast >/dev/null 2>&1 || kubectl create namespace fast >/dev/null

USER_B64="$(kubectl get secret postgres-credentials -n tools -o jsonpath='{.data.POSTGRES_USER}')"
PASS_B64="$(kubectl get secret postgres-credentials -n tools -o jsonpath='{.data.POSTGRES_PASSWORD}')"
[ -n "$USER_B64" ] && [ -n "$PASS_B64" ] || { echo "ERRO: postgres-credentials sem POSTGRES_USER/POSTGRES_PASSWORD"; exit 1; }

kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: fast-database
  namespace: fast
type: Opaque
data:
  DATABASE_USERNAME: ${USER_B64}
  DATABASE_PASSWORD: ${PASS_B64}
EOF
echo "Secret fast-database criado/atualizado no namespace fast."
