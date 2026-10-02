# fast-platform

Plataforma de corridas (ride-hailing) em Go, dividida em três serviços, cada um em seu
repositório, que se comunicam por **PostgreSQL (outbox)** e **Kafka**. Um passageiro pede uma
corrida, um motorista aceita, e cada passo chega em tempo real aos apps via
**Socket.IO**.

[![pipeline](https://github.com/guilhermelinosp/fast-platform/actions/workflows/pipeline.yml/badge.svg)](https://github.com/guilhermelinosp/fast-platform/actions/workflows/pipeline.yml)
[![pr-check](https://github.com/guilhermelinosp/fast-platform/actions/workflows/pr-check.yml/badge.svg)](https://github.com/guilhermelinosp/fast-platform/actions/workflows/pr-check.yml)
[![CodeQL](https://github.com/guilhermelinosp/fast-platform/actions/workflows/codeql.yml/badge.svg)](https://github.com/guilhermelinosp/fast-platform/actions/workflows/codeql.yml)

## Início rápido

A API lê o `.env` da própria pasta (`cmd/api/.env`, ignorado pelo git): copie o `cmd/api/.env.example` e ajuste (veja [Configuração](#configuração)). São necessários PostgreSQL, Redis e Kafka.

```bash
cd cmd/api && go run -race main.go   # fast-platform (API HTTP)
```

O [fast-listeners](https://github.com/guilhermelinosp/fast-listeners) (outbox -> Kafka) e o [fast-sockets](https://github.com/guilhermelinosp/fast-sockets) (Kafka -> Socket.IO) têm cada um o seu repositório e o seu README.

## Configuração

Cada binário lê o `.env` da própria pasta (`cmd/<binário>/.env`, ignorado pelo
git); copie o `cmd/<binário>/.env.example` e ajuste; variáveis já definidas no ambiente têm prioridade. Faltando uma variável
obrigatória, o processo falha com um erro claro.

| Variável | Usada por | Descrição |
|---|---|---|
| `HELLNET_SERVICE`, `HELLNET_ENVIRONMENT` | api | Nome do serviço e ambiente (`Development` liga o modo debug do Gin) |
| `HELLNET_PORT` | api | Porta HTTP (padrão `8080`) |
| `HELLNET_TELEMETRY_ENDPOINT` | api | Endpoint OTLP/HTTP (Alloy) |
| `DATABASE_HOST`, `DATABASE_PORT`, `DATABASE_NAME`, `DATABASE_USERNAME`, `DATABASE_PASSWORD`, `DATABASE_POOL_MAX_SIZE` | api | Conexão PostgreSQL |
| `KAFKA_BROKERS`, `KAFKA_SECURITY_PROTOCOL` | api | Conexão Kafka |
| `KAFKA_TOPIC_ORDER_REQUESTED`, `KAFKA_TOPIC_ORDER_ACCEPTED` | api | Tópicos dos eventos |
| `SOCKET_DRIVERS_NAMESPACE`, `SOCKET_RIDERS_NAMESPACE` | api | Namespaces Socket.IO |
| `HELLNET_CACHE_CONNECTION`, `HELLNET_CACHE_ENABLE_L2`, `HELLNET_CACHE_DEFAULT_TTL` | api | Cache L1 (memória) e L2 (Redis), por exemplo `localhost:6379` |
| `BODY_LIMIT`, `READ_TIMEOUT`, `WRITE_TIMEOUT`, `IDLE_TIMEOUT`, `READ_HEADER_TIMEOUT`, `SHUTDOWN_TIMEOUT`, `CORS_ALLOWED_ORIGINS`, `TRUSTED_PROXIES` | api | Limites e timeouts HTTP |

> O cache lê as variáveis com o prefixo `HELLNET_CACHE_`; nomes sem o prefixo
> (`CACHE_CONNECTION`) são ignorados e o L2 fica desligado.

## Arquitetura

| Serviço | Repositório | Papel |
|---|---|---|
| **fast-platform** | este (`cmd/api`) | API HTTP (Gin): recebe o pedido e o aceite e grava tudo em uma escrita atômica |
| **fast-listeners** | [fast-listeners](https://github.com/guilhermelinosp/fast-listeners) | Lê o outbox do PostgreSQL e publica no Kafka (inclui o consumer de matching, em standby) |
| **fast-sockets** | [fast-sockets](https://github.com/guilhermelinosp/fast-sockets) | Consome o Kafka e entrega os eventos por Socket.IO |

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
| `POST /api/v1/orders/:orderId/accept` | O motorista aceita. Header `driver_id` (UUID). Antes de gravar, consulta o status do pedido pelo mesmo cache e recusa de imediato se ele já não está "requested" (o status só avança); o guard do SQL continua sendo a palavra final. Responde `201`, ou `409` (`ORDER_NOT_ACCEPTABLE` se o pedido não está no estado "requested" ou não existe; `ORDER_ALREADY_ACCEPTED` se já foi aceito). |
| `GET /api/v1/orders/:orderId` | Lê o pedido e o status atual (última linha de `order_status_history`). A leitura passa pelo cache (L1 memória e L2 Redis, TTL de 10 s, com proteção contra stampede); Exige o header `rider_id` e só mostra o pedido ao passageiro que o fez: pedido inexistente ou de outro passageiro dá `404 ORDER_NOT_FOUND` (o 404 não é guardado no cache); `400` se o id ou o `rider_id` não são UUID. |
| `GET /live`, `/ready`, `/health` | Probes de saúde da telemetria. Não geram trace nem log de request. |

Erros seguem um envelope único (`code` e `message`) definido em `internal/platform`.

## Socket.IO e listeners

O servidor Socket.IO (namespaces, salas, client de teste) está documentado no [fast-sockets](https://github.com/guilhermelinosp/fast-sockets); o publicador do outbox e o consumer de matching (em standby), no [fast-listeners](https://github.com/guilhermelinosp/fast-listeners).

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

## Estrutura

```text
cmd/api               API HTTP
internal/orders       pedido (HTTP, serviço, repositório)
internal/drivers      aceite do motorista
internal/platform     middleware, erros, bootstrap e propagação de trace
internal/env          leitura de variáveis de ambiente
```

## Desenvolvimento

```bash
go test -race ./...
go vet ./...
golangci-lint run ./...
```

Os hooks do [Lefthook](.lefthook.yml) rodam `gofmt`, `vet`, testes (com e sem `-race`), build, `go mod tidy`, lint, `govulncheck` e o scan de segredos; instale-os uma vez com `lefthook install`. Commits seguem [Conventional Commits](https://www.conventionalcommits.org/).

## CI/CD

| Workflow | Gatilho | O que faz |
|---|---|---|
| `pr-check` | pull request | shellcheck, estratégia de merge e Conventional Commits (`merge-check`), Gitleaks, labels e o gate de qualidade Go (integridade do módulo, vet, testes com race e cobertura, lint, build, dependency review). O `pr-gate` reúne tudo e é o check obrigatório |
| `pipeline` | push na `main` (ignora `.github/**`) ou manual | guarda de semver (bloqueia major automático), tag imutável + GitHub Release, imagem de container |
| `codeql` | diário ou manual | análise estática (CodeQL) |
| `security` | diário ou manual | scans de Gitleaks e Trivy |
| `auto-pr` | push em `feat/**` ou `fix/**` | abre o pull request automaticamente |
| `dependabot-actions-auto-merge` | pull requests do Dependabot | faz auto-merge das atualizações de GitHub Actions |

Os workflows chamam workflows reutilizáveis de [templates](https://github.com/guilhermelinosp/templates), fixados por SHA de commit. O release precisa do secret `HELLNET_ACTIONS_PRIVATE_KEY` e da variável `HELLNET_ACTIONS_CLIENT_ID`.

## Contribuindo e licença

Veja [CONTRIBUTING.md](CONTRIBUTING.md) e [SECURITY.md](SECURITY.md). Licença [Apache 2.0](LICENSE).
