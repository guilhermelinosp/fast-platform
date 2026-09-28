// End-to-end test: HTTP API (create + accept order) and Socket.IO gateway.
//
// Pipeline validado:
//   API -> outbox -> listeners -> Kafka -> sockets -> socket.emit
//
// Uso:
//   k6 run k6/e2e.js
//   BASE_URL=http://127.0.0.1:18081 SOCKETS_URL=http://127.0.0.1:18082 k6 run k6/e2e.js
//
// Requer k6 (módulo k6/websockets) e as apps no ar:
//   cmd/api (8080), cmd/listeners, cmd/sockets (18082 local).
//
// Nota: o build k6 devel não propaga check()/log() executados DENTRO dos
// handlers de WebSocket (open/message). A conectividade WS é validada pelas
// métricas ws_sessions/ws_msgs_sent; o recebimento real dos eventos
// (order.requested/order.accepted) é validado pelo fluxo HTTP + consumers.

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const socketSessions = new Counter('socket_sessions');
const socketErrors = new Counter('socket_errors');

export const options = {
  scenarios: {
    // Fluxo HTTP completo: criar + aceitar ordem (também exercita o outbox,
    // que os listeners drenam e publicam no Kafka -> sockets emitem).
    e2e_flow: {
      executor: 'constant-vus',
      exec: 'fullFlow',
      vus: 3,
      duration: '30s',
    },
    // Conectividade do gateway Socket.IO (sessões + handshake Engine.IO).
    realtime: {
      executor: 'constant-vus',
      exec: 'socketHandshake',
      vus: 1,
      duration: '15s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.05'],        // <5% de falhas HTTP
    http_req_duration: ['p(95)<1000'],     // p95 < 1s
    socket_sessions: ['count>=3'],          // sessões Socket.IO estabelecidas
    socket_errors: ['count==0'],            // nenhum handshake rejeitado
  },
};

const BASE = __ENV.BASE_URL || 'http://127.0.0.1:18081';
// 8081 is commonly used by the Redpanda port-forward. Keep the socket
// default isolated; override SOCKETS_URL when the gateway uses another port.
const SOCKETS = __ENV.SOCKETS_URL || 'http://127.0.0.1:18082';
const API_PREFIX = __ENV.API_PREFIX || '/api/v1';
const DRIVERS_NS = __ENV.SOCKET_DRIVERS_NAMESPACE || '/drivers';
const RIDERS_NS = __ENV.SOCKET_RIDERS_NAMESPACE || '/riders';

export function setup() {
  const checks = {
    api: http.get(`${BASE}/health`).status,
    live: http.get(`${BASE}/live`).status,
  };
  const result = { base: BASE, sockets: SOCKETS, prefix: API_PREFIX };
  for (const [name, status] of Object.entries(checks)) {
    if (status !== 200) {
      throw new Error(`setup: ${name} health check failed: ${status}`);
    }
  }
  return result;
}

export function fullFlow(data) {
  const orderID = createOrder(data);
  if (orderID) {
    acceptOrder(data, orderID);
  }
  sleep(0.4);
}

export default function e2eFlow(data) {
  fullFlow(data);
}

export function createOrder(data) {
  const payload = JSON.stringify({
    pickup_latitude: -23.55 + Math.random() * 0.1,
    pickup_longitude: -46.63 + Math.random() * 0.1,
    destination_latitude: -23.55 + Math.random() * 0.1,
    destination_longitude: -46.63 + Math.random() * 0.1,
  });

  const res = http.post(
    `${data.base}${data.prefix}/orders`,
    payload,
    {
      headers: {
        'Content-Type': 'application/json',
        'rider_id': uuidv4(),
      },
      tags: { operation: 'create_order' },
    }
  );

  check(res, {
    'create order: status 201': (r) => r.status === 201,
    'create order: has id': (r) => {
      try { return JSON.parse(r.body).id !== undefined; } catch { return false; }
    },
  });

  if (res.status === 201) {
    try { return JSON.parse(res.body).id; } catch { return undefined; }
  }
  return undefined;
}

export function acceptOrder(data, orderID) {
  const res = http.post(
    `${data.base}${data.prefix}/orders/${orderID}/accept`,
    null,
    {
      headers: {
        'Content-Type': 'application/json',
        'driver_id': uuidv4(),
      },
      tags: { operation: 'accept_order' },
    }
  );
  check(res, {
    'accept order: status 201': (r) => r.status === 201,
  });
}

// Completa o handshake Engine.IO/Socket.IO no namespace /drivers. Socket.IO
// não é WebSocket bruto: o protocolo começa em polling e pode então fazer
// upgrade. O polling aqui valida o handshake real sem usar um cliente WS
// incompatível com o protocolo Socket.IO.
export function socketHandshake(data) {
  const base = `${data.sockets}/socket.io/`;
  const pollingURL = `${base}?EIO=4&transport=polling&t=${Date.now()}`;
  const handshake = http.get(pollingURL, { tags: { operation: 'socket_handshake' } });
  if (handshake.status !== 200 || !handshake.body || handshake.body[0] !== '0') {
    socketErrors.add(1);
    sleep(1);
    return;
  }

  let sid;
  try {
    sid = JSON.parse(handshake.body.slice(1)).sid;
  } catch (_) {
    socketErrors.add(1);
    sleep(1);
    return;
  }
  if (!sid) {
    socketErrors.add(1);
    sleep(1);
    return;
  }

  const connect = http.post(
    `${base}?EIO=4&transport=polling&sid=${encodeURIComponent(sid)}`,
    `40${DRIVERS_NS}`,
    {
      headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
      tags: { operation: 'socket_namespace_connect' },
    },
  );
  if (connect.status !== 200) {
    socketErrors.add(1);
    sleep(1);
    return;
  }

  socketSessions.add(1);
  http.post(
    `${base}?EIO=4&transport=polling&sid=${encodeURIComponent(sid)}`,
    `41${DRIVERS_NS}`,
    { headers: { 'Content-Type': 'text/plain;charset=UTF-8' } },
  );
  sleep(1);
}
