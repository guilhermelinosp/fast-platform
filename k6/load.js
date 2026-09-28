import http from 'k6/http';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

// Volumetria do fluxo principal: criar ordem (order.requested) e aceitar
// (order.accepted). Alvo: medir throughput e latência da API HTTP.
//
// Uso:
//   k6 run k6/load.js
//   k6 run --vus 50 --duration 2m k6/load.js

export const options = {
  scenarios: {
    // Sob carga sustentada: fluxo completo criar + aceitar ordem
    ride_flow: {
      executor: 'ramping-vus',
      exec: 'createAndAccept',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 10 },   // ramp up
        { duration: '1m', target: 30 },    // pico
        { duration: '30s', target: 0 },    // ramp down
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],        // <1% de falhas
    http_req_duration: ['p(95)<500'],      // p95 < 500ms
  },
};

const BASE = __ENV.BASE_URL || 'http://127.0.0.1:18081';
const API_PREFIX = __ENV.API_PREFIX || '/api/v1';

// Riders pre-alocados para evitar UUID aleatório por iteração.
const riders = new SharedArray('riders', () => {
  const arr = [];
  for (let i = 0; i < 100; i++) {
    arr.push(uuidv4());
  }
  return arr;
});

function riderID() {
  return riders[Math.floor(Math.random() * riders.length)];
}

export function setup() {
  // Descobre se o backend está no ar.
  const res = http.get(`${BASE}/health`);
  if (res.status !== 200) {
    throw new Error(`health check failed: ${res.status} ${res.body}`);
  }
  return { base: BASE, prefix: API_PREFIX };
}

// Função default: usada quando o usuário roda com --vus/--duration (sem o
// bloco scenarios). Exercita o fluxo completo por iteração: criar a ordem e
// aceitá-la, medindo o caminho real de ponta a ponta.
export default function loadFlow(data) {
  createAndAccept(data);
}

// Cria uma ordem e aceita a mesma ordem na sequência — o fluxo real.
export function createAndAccept(data) {
  const orderID = createOrder(data);
  if (orderID) {
    acceptOrder(data, orderID);
  }
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
        'rider_id': riderID(),
      },
      tags: { operation: 'create_order' },
    }
  );

  check(res, {
    'create order: status 201': (r) => r.status === 201,
    'create order: has id': (r) => {
      try {
        return JSON.parse(r.body).id !== undefined;
      } catch {
        return false;
      }
    },
  });

  if (res.status === 201) {
    try {
      return JSON.parse(res.body).id;
    } catch {
      return undefined;
    }
  }
  return undefined;
}

// Aceita uma ordem previamente criada (fluxo real). O driver_id é lido do
// header pela API.
export function acceptOrder(data, orderID) {
  const driverID = uuidv4();
  const res = http.post(
    `${data.base}${data.prefix}/orders/${orderID}/accept`,
    null,
    {
      headers: {
        'Content-Type': 'application/json',
        'driver_id': driverID,
      },
      tags: { operation: 'accept_order' },
    }
  );

  check(res, {
    'accept order: status 201': (r) => r.status === 201,
  });

  sleep(0.2); // ~5 rps por VU
}
