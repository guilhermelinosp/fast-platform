# fast-platform-modular

Plataforma de corridas (ride-hailing) em Go, dividida em três binários que se
comunicam por **PostgreSQL (outbox)** e **Kafka**. Um passageiro pede uma
corrida, um motorista aceita, e cada passo chega em tempo real aos apps via
**Socket.IO**.

[![pipeline](https://github.com/guilhermelinosp/fast-platform-modular/actions/workflows/pipeline.yml/badge.svg)](https://github.com/guilhermelinosp/fast-platform-modular/actions/workflows/pipeline.yml)
[![pr-check](https://github.com/guilhermelinosp/fast-platform-modular/actions/workflows/pr-check.yml/badge.svg)](https://github.com/guilhermelinosp/fast-platform-modular/actions/workflows/pr-check.yml)
[![CodeQL](https://github.com/guilhermelinosp/fast-platform-modular/actions/workflows/codeql.yml/badge.svg)](https://github.com/guilhermelinosp/fast-platform-modular/actions/workflows/codeql.yml)

## Arquitetura

| Binário | Pasta | Papel |
|---|---|---|
| **fast-platform** | `cmd/api` | API HTTP (Gin): recebe o pedido e o aceite e grava tudo em uma escrita atômica |
| **fast-listeners** | `cmd/listeners` | Lê o outbox do PostgreSQL e publica no Kafka |
| **fast-sockets** | `cmd/sockets` | Consome o Kafka e entrega os eventos por Socket.IO |

```text
App do passageiro ──POST /api/v1/orders──► fast-platform
                                              │  1 INSERT atômico:
                                              │  orders + order_status_history + outbox_events
                                              ▼
                                         PostgreSQL ── NOTIFY outbox_events ──► fast-listeners
                                                                                    │ publica
                                                                                    ▼
                                                                                  Kafka
                                                                                    │ order.requested.v1
                                                                                    ▼
                                                                              fast-sockets ──► Socket.IO /drivers  (apps dos motoristas)

App do motorista ──POST /api/v1/orders/:orderId/accept──► (mesmo caminho, tópico order.accepted.v1)
                                                                              fast-sockets ──► Socket.IO /riders   (sala order:<orderId>)
```

Decisões que moldam o código:

- **Banco append-only.** As tabelas só recebem `INSERT` e `SELECT`; nunca
  `UPDATE` nem `DELETE`. Uma mudança de estado é uma nova linha (por exemplo, o
  status do pedido vira uma linha em `order_status_history`) e o estado atual é a
  última linha. A publicação do outbox também é uma linha, em
  `outbox_publications`.
- **Outbox transacional.** O pedido e o evento são gravados no mesmo statement
  (CTEs de `INSERT`), então nunca existe pedido sem evento nem evento sem pedido.
  O `fast-listeners` é acordado por `LISTEN/NOTIFY` e, como rede de segurança,
  reconcilia a cada 5 s os eventos da última hora (e faz uma varredura completa
  na partida e de hora em hora).
- **Tudo com `context`.** Os acessos a banco, cache e Kafka recebem o contexto da
  requisição, então o trace e o cancelamento atravessam as camadas.

## API HTTP (`fast-platform`)

| Método e rota | Descrição |
|---|---|
| `POST /api/v1/orders` | Cria o pedido. Corpo: `id` (opcional), `rider_id` (ou header `rider_id`; sem ele, gera um UUID), `pickup_latitude`, `pickup_longitude`, `destination_latitude`, `destination_longitude`. Responde `201`. |
| `POST /api/v1/orders/:orderId/accept` | O motorista aceita. Header `driver_id` (UUID). Responde `201`, ou `409` (`ORDER_NOT_ACCEPTABLE` se o pedido não está no estado "requested" ou não existe; `ORDER_ALREADY_ACCEPTED` se já foi aceito). |
| `GET /live`, `/ready`, `/health` | Probes de saúde da telemetria. Não geram trace nem log de request. |

Erros seguem um envelope único (`code` e `message`) definido em `internal/platform`.

## Socket.IO (`fast-sockets`)

- Dois namespaces, configurados por `SOCKET_DRIVERS_NAMESPACE` (motoristas) e
  `SOCKET_RIDERS_NAMESPACE` (passageiros).
- O servidor emite cada evento com o **nome do tópico** Kafka
  (`KAFKA_TOPIC_ORDER_REQUESTED` para os motoristas, `KAFKA_TOPIC_ORDER_ACCEPTED`
  para os passageiros).
- O passageiro entra na sala do próprio pedido com o evento `order.subscribe`
  (sala `order:<orderId>`) e recebe o aceite nela.

### Client de teste

`cmd/sockets/client.go` é um client de teste do próprio binário: conecta como
motorista e como passageiro, loga cada pacote (namespace, evento e ids) e inscreve
o passageiro nos pedidos que o motorista recebe. Lê o mesmo `cmd/sockets/.env` do
servidor (`SOCKET_URL`, padrão `ws://localhost:8080`, `SOCKET_DRIVERS_NAMESPACE`,
`SOCKET_RIDERS_NAMESPACE` e `KAFKA_TOPIC_ORDER_REQUESTED`):

```bash
cd cmd/sockets && go run . client
```

## Configuração

Cada binário lê o `.env` da própria pasta (`cmd/<binário>/.env`, ignorado pelo
git); copie o `cmd/<binário>/.env.example` e ajuste; variáveis já definidas no ambiente têm prioridade. Faltando uma variável
obrigatória, o processo falha com um erro claro.

| Variável | Usada por | Descrição |
|---|---|---|
| `HELLNET_SERVICE`, `HELLNET_ENVIRONMENT` | todos | Nome do serviço e ambiente (`Development` liga o modo debug do Gin) |
| `HELLNET_PORT` | api, sockets | Porta HTTP (padrão `8080`) |
| `HELLNET_TELEMETRY_ENDPOINT` | todos | Endpoint OTLP/HTTP (Alloy) |
| `DATABASE_HOST`, `DATABASE_PORT`, `DATABASE_NAME`, `DATABASE_USERNAME`, `DATABASE_PASSWORD`, `DATABASE_POOL_MAX_SIZE` | api, listeners | Conexão PostgreSQL |
| `KAFKA_BROKERS`, `KAFKA_SECURITY_PROTOCOL` | todos | Conexão Kafka |
| `KAFKA_TOPIC_ORDER_REQUESTED`, `KAFKA_TOPIC_ORDER_ACCEPTED` | todos | Tópicos dos eventos |
| `KAFKA_MATCHING_CONSUMER_GROUP` | listeners | Grupo do consumer de matching |
| `SOCKET_DRIVERS_NAMESPACE`, `SOCKET_RIDERS_NAMESPACE` | api, sockets | Namespaces Socket.IO |
| `HELLNET_CACHE_CONNECTION`, `HELLNET_CACHE_ENABLE_L2`, `HELLNET_CACHE_DEFAULT_TTL` | api, listeners | Cache L1 (memória) e L2 (Redis), por exemplo `localhost:6379` |
| `BODY_LIMIT`, `READ_TIMEOUT`, `WRITE_TIMEOUT`, `IDLE_TIMEOUT`, `READ_HEADER_TIMEOUT`, `SHUTDOWN_TIMEOUT`, `CORS_ALLOWED_ORIGINS`, `TRUSTED_PROXIES` | api | Limites e timeouts HTTP |

> O cache lê as variáveis com o prefixo `HELLNET_CACHE_`; nomes sem o prefixo
> (`CACHE_CONNECTION`) são ignorados e o L2 fica desligado.

## Observabilidade

Os três serviços usam [hellnet-lib-telemetry](https://github.com/guilhermelinosp/hellnet-lib-telemetry)
e exportam traces, métricas e logs por OTLP/HTTP.

- **Um trace por jornada.** O contexto do trace viaja da API até o socket: a API
  grava o contexto no payload do outbox, o listener o continua no span
  `outbox.publish` e o Kafka o leva nos headers (`traceparent`) até o consumer.
  Os headers também carregam `event_id`, `event_type` e `order_id`.
- **Spans.** `POST /api/v1/orders` → `orders.requested` → `db.execute` →
  `db.insert`; `outbox.publish` → `send <tópico>` → `process <tópico>` →
  `socket.emit.*`. Os spans de SQL têm nomes de baixa cardinalidade
  (`db.insert`, `db.select`, `db.transaction`).
- **Logs** em JSON, com `trace_id` e `span_id`.
- **Métricas.** `http_requests_total` e `http_request_duration_seconds` (rótulos
  `http_route`, `method`, `status`), `http_response_size_bytes`,
  `messaging_client_*` e `messaging_process_duration_seconds` (Kafka),
  `db_client_*` (banco) e `worker_jobs_total` para os casos de uso.
- Identificadores como `order_id` ficam nos spans e nos logs, **nunca** em rótulos
  de métrica (gerariam uma série por pedido).

## Matching (em standby)

O consumer `internal/matching` escolhe um motorista disponível para cada pedido
(lê a última linha de `driver_availability_history` com status `online`). Ele está
**comentado** em `cmd/listeners/main.go`, e **nada grava a disponibilidade dos
motoristas** ainda, então ele não teria um motorista para atribuir. Para
religá-lo, descomente o bloco e dê aos motoristas um jeito de ficarem online.

## Estrutura

```text
cmd/api         API HTTP
cmd/listeners   outbox -> Kafka (matching em standby)
cmd/sockets     Kafka -> Socket.IO
internal/orders       pedido (HTTP, serviço, repositório)
internal/drivers      aceite do motorista
internal/matching     escolha de motorista (standby)
internal/listeners    outbox, NOTIFY e reconciliação
internal/sockets      servidor Socket.IO e consumers
internal/platform     middleware, erros, bootstrap e propagação de trace
internal/env          leitura de variáveis de ambiente
```

## Desenvolvimento

```bash
# em cada pasta cmd/<binário>, com o .env ao lado:
go run -race main.go

go test -race ./...
golangci-lint run ./...
```

Os hooks do [Lefthook](.lefthook.yml) rodam `gofmt`, `vet`, testes (com e sem
`-race`), build, `go mod tidy`, lint, `govulncheck` e o scan de segredos antes de
cada commit. Commits seguem [Conventional Commits](https://www.conventionalcommits.org/).

## CI

Workflows em `.github/workflows`: `pipeline` (build, teste e release), `pr-check`
(lint, qualidade, segredos e Conventional Commits), `codeql`, `security` e
atualizações automáticas de dependências.

## Contribuindo e licença

Veja [CONTRIBUTING.md](CONTRIBUTING.md) e [SECURITY.md](SECURITY.md).
