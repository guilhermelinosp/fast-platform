#!/usr/bin/env bash
# Teste de fumaca do fast no cluster: prova o fluxo completo
#   API -> Postgres (outbox) -> listeners -> Redpanda -> sockets
# e o cache (Redis) na leitura de pedidos.
#   deploy/smoke-test.sh
# Variaveis: API_HOST (padrao fast.hellnet.com.br, resolvido pelo DNS interno; de fora de casa alcance
# pelo Cloudflare WARP), GATEWAY_IP (opcional: forca o IP do Gateway em vez de resolver o nome),
# NS (padrao fast), WAIT (padrao 20, segundos de espera por evento).
set -uo pipefail

NS="${NS:-fast}"
API_HOST="${API_HOST:-fast.hellnet.com.br}"
GATEWAY_IP="${GATEWAY_IP:-}"
WAIT="${WAIT:-20}"
TOPIC_REQ="br.com.hellnet.fast.order.requested.v1"
TOPIC_ACC="br.com.hellnet.fast.order.accepted.v1"

FALHAS=0
ok()   { printf 'ok    %s\n' "$1"; }
falha(){ printf 'FALHA %s\n' "$1"; FALHAS=$((FALHAS+1)); }
check(){ # descricao esperado obtido
  if [ "$2" = "$3" ]; then ok "$1 -> $3"; else falha "$1 -> $3 (esperado $2)"; fi
}
http()  {
  if [ -n "$GATEWAY_IP" ]; then curl -sk -m 15 --resolve "${API_HOST}:443:${GATEWAY_IP}" "$@"; else curl -sk -m 15 "$@"; fi
}
code()  { http -o /dev/null -w '%{http_code}' "$@"; }
hwm()   { kubectl exec -n tools redpanda-0 -c redpanda -- rpk topic describe "$1" -p 2>/dev/null | awk 'NR==2{print $NF}'; }
espera_hwm() { # topico minimo
  local i; for i in $(seq 1 "$WAIT"); do
    [ "$(hwm "$1")" -ge "$2" ] 2>/dev/null && return 0; sleep 1
  done; return 1
}

echo "== 1. Deployments"
for d in fast-api fast-listeners fast-sockets; do
  if kubectl rollout status "deploy/$d" -n "$NS" --timeout=120s >/dev/null 2>&1; then ok "$d disponivel"; else falha "$d nao ficou disponivel"; fi
done

echo "== 2. Probes pelo gateway (https://${API_HOST})"
check "GET /live"  200 "$(code "https://${API_HOST}/live")"
check "GET /ready" 200 "$(code "https://${API_HOST}/ready")"

echo "== 3. Fluxo do pedido"
RIDER="$(python3 -c 'import uuid;print(uuid.uuid4())')"
DRIVER="$(python3 -c 'import uuid;print(uuid.uuid4())')"
ORDER="$(python3 -c 'import uuid;print(uuid.uuid4())')"
REQ0="$(hwm "$TOPIC_REQ")"; ACC0="$(hwm "$TOPIC_ACC")"
BODY="{\"id\":\"${ORDER}\",\"pickup_latitude\":-23.55,\"pickup_longitude\":-46.63,\"destination_latitude\":-23.56,\"destination_longitude\":-46.65}"

check "POST /orders" 201 "$(code -X POST -H 'Content-Type: application/json' -H "rider_id: ${RIDER}" -d "$BODY" "https://${API_HOST}/api/v1/orders")"
check "POST /orders (mesmo id) e conflito" 409 "$(code -X POST -H 'Content-Type: application/json' -H "rider_id: ${RIDER}" -d "$BODY" "https://${API_HOST}/api/v1/orders")"
STATUS="$(http -H "rider_id: ${RIDER}" "https://${API_HOST}/api/v1/orders/${ORDER}" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("status",""))' 2>/dev/null)"
check "GET /orders/:id (status, 1a leitura = miss)" requested "$STATUS"
check "GET /orders/:id (2a leitura = hit do cache)" 200 "$(code -H "rider_id: ${RIDER}" "https://${API_HOST}/api/v1/orders/${ORDER}")"
check "GET /orders/:id de OUTRO passageiro" 404 "$(code -H "rider_id: $(python3 -c 'import uuid;print(uuid.uuid4())')" "https://${API_HOST}/api/v1/orders/${ORDER}")"
if espera_hwm "$TOPIC_REQ" $((REQ0+1)); then ok "evento order.requested chegou ao Redpanda (listeners publicou)"; else falha "order.requested nao chegou ao Redpanda em ${WAIT}s"; fi

check "POST /accept" 201 "$(code -X POST -H "driver_id: ${DRIVER}" "https://${API_HOST}/api/v1/orders/${ORDER}/accept")"
check "POST /accept de novo e conflito" 409 "$(code -X POST -H "driver_id: ${DRIVER}" "https://${API_HOST}/api/v1/orders/${ORDER}/accept")"
if espera_hwm "$TOPIC_ACC" $((ACC0+1)); then ok "evento order.accepted chegou ao Redpanda"; else falha "order.accepted nao chegou ao Redpanda em ${WAIT}s"; fi

echo "== 4. Consumers (sockets)"
for t in "$TOPIC_REQ" "$TOPIC_ACC"; do
  LAG=""
  for i in $(seq 1 "$WAIT"); do
    LAG="$(kubectl exec -n tools redpanda-0 -c redpanda -- rpk group describe "$t" 2>/dev/null | awk '/TOTAL-LAG/{print $2}')"
    [ "$LAG" = "0" ] && break; sleep 1
  done
  check "lag do grupo ${t##*.fast.}" 0 "${LAG:-?}"
done

echo "== 5. Sem erros nos logs dos pods"
for d in fast-api fast-listeners fast-sockets; do
  N="$(kubectl logs -n "$NS" "deploy/$d" --tail=300 2>/dev/null | grep -ciE '"level":"(error|fatal)"|panic')"
  check "erros em $d" 0 "$N"
done

echo
if [ "$FALHAS" -eq 0 ]; then echo "TUDO OK: o fast esta no ar no cluster e o fluxo completo funciona."; else echo "$FALHAS verificacao(oes) falharam."; fi
exit "$FALHAS"
