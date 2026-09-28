# golang-api-template

> Opinionated, production-ready GitHub template for Go HTTP APIs.
> Boot with **~70–80% of infrastructure done** — you write business logic.

[![pipeline](https://github.com/guilhermelinosp/fast-platform-modular/actions/workflows/pipeline.yml/badge.svg)](https://github.com/guilhermelinosp/fast-platform-modular/actions/workflows/pipeline.yml)
[![pr-check](https://github.com/guilhermelinosp/fast-platform-modular/actions/workflows/pr-check.yml/badge.svg)](https://github.com/guilhermelinosp/fast-platform-modular/actions/workflows/pr-check.yml)
[![CodeQL](https://github.com/guilhermelinosp/fast-platform-modular/actions/workflows/codeql.yml/badge.svg)](https://github.com/guilhermelinosp/fast-platform-modular/actions/workflows/codeql.yml)

Three non-negotiable statements about this codebase:

```text
Gin is an implementation detail.
hellnet-lib-telemetry is the standard observability layer.
Business logic does not depend on Gin.
```

---

## What you get for free

| Capability | Where it comes from |
|---|---|
| HTTP routing, handlers, validation, error envelope | `internal/api` abstraction + Gin adapter |
| Structured logging (`slog`, JSON, trace-correlated) | [hellnet-lib-telemetry](https://github.com/guilhermelinosp/hellnet-lib-telemetry) |
| Metrics (`http_requests_total`, `http_request_duration_seconds`, inflight, sizes, errors, runtime) | hellnet-lib-telemetry |
| Distributed tracing (OTLP via otelhttp) | hellnet-lib-telemetry |
| `/live` `/ready` `/health` `/metrics` endpoints | hellnet-lib-telemetry |
| Sensitive-data redaction in logs | hellnet-lib-telemetry (`RedactSensitive`) |
| Graceful shutdown with correct telemetry flush order | template bootstrap + lib `Shutdown()` |
| Secure timeouts, request-id, security headers, CORS | template adapter middlewares |
| Tests via stdlib only (`testing` + `httptest`) | template suites |
| CI: test/lint/CodeQL/dependency-review/govulncheck | `.github/workflows` |
| Release: semver tag → GH release → goreleaser binaries → image | org reusable workflows + GoReleaser |
| Container (distroless, non-root, reproducible) | `Containerfile` |

OpenTelemetry SDK wiring, Prometheus registry, health-check registry and log
redaction are **not** re-implemented here by design.

---

## Quick start (template UX)

```bash
# 1. Use this template on GitHub, then:
git clone https://github.com/<you>/my-project && cd my-project

# 2. Configure (optional locally!):
cp .env.example .env          # APP_* and/or TELEMETRY_*

# 3. Verify everything works:
make test
make run                      # boots even without telemetry envs

curl -s localhost:8080/health
curl -s localhost:8080/metrics
curl -s 'localhost:8080/api/v1/hello?name=you'

# 4. Write business logic. That's your 20%.
```

No collector? Local structured logging and Prometheus `/metrics` remain active;
only remote OTLP export and profiling stay off. Add a
`TELEMETRY_ENDPOINT` to `.env` or the process environment to enable
remote logs, metrics, traces, and profiling without changing application code.

---

## Architecture

```text
                         HTTP
                          │
                ┌─────────▼──────────┐
                │  http.Server       │            ← internal/server
                │  (timeouts, drain) │               plain net/http lifecycle
                └─────────┬──────────┘
                          │
        telemetry.Middleware(hellnet-lib-telemetry)    ← logs+metrics+traces for ALL routes
                          │
                ┌─────────▼──────────┐
                │   Gin Adapter      │            ← internal/api/ginadapter
                │ req-id│sec-headers │               THE ONLY Gin-aware package
                │ cors │ recovery    │
                └─────────┬──────────┘
                          │ implements api.Router
                ┌─────────▼──────────┐
                │  API Abstraction   │            ← internal/api
                │ Handler/Requested/   │               transport-neutral contracts
                │ Response/errors    │
                └─────────┬──────────┘
                          ▼
                       Handler                       ← internal/<domain>/handler.go
                          ▼
                       Service
                  ┌──────┴──────┐
             RepositoryImp     External API           ← wire ops.HealthRegister /
                                                 ops.HTTPClient when added

Observability:  API ─► gin middleware ─► hellnet-lib-telemetry ─► Logs │ Metrics │ Traces
Lifecycle:      context ─► config ─► telemetry.New ─► deps ─► server
                        ⇄ signal ─► server.Shutdown ─► ops.Shutdown
```

### Why this layering pays off

1. **Swap-proof business code** — services/handlers never import `gin`; they
   see `internal/api.Handler`, `Request`, `Response`, `*Error` only.
2. **Direct telemetry composition** — `cmd/api` initializes and wires
   `hellnet-lib-telemetry`; domain packages receive only the resulting logger
   and remain independent from telemetry construction.
3. **Honest testing** — tests exercise handlers through the abstraction port
   AND through the real Gin adapter, using only `httptest`.

---

## Environment variables

Two strict namespaces, zero overlap:

### Application (`internal/config` reads these)

| Variable | Default | Purpose |
|---|---|---|
| `APP_NAME` | `golang-api-template` | Service identity exposed by the application |
| `APP_ENV` | `development` | `production` enables Gin release mode |
| `APP_PORT` | `8080` | Listen port |
| `APP_SHUTDOWN_TIMEOUT` | `10s` | Drain budget; keep < k8s `terminationGracePeriodSeconds` |
| `APP_READ_TIMEOUT` / `APP_WRITE_TIMEOUT` / `APP_IDLE_TIMEOUT` / `APP_READ_HEADER_TIMEOUT` | `15s` / `30s` / `120s` / `10s` | Explicit `http.Server` hardening |
| `APP_CORS_ALLOWED_ORIGINS` | *(disabled)* | Comma-separated exact origins or `*` |

Build metadata (`version`, `commit`, `date`) arrives via `-ldflags`
(Makefile/Containerfile/CI) and appears at `GET /`.

### Telemetry — owned by hellnet-lib-telemetry

Documented with the library's own conventions; **never mirrored into `APP_*`**:

| Variable | Required | Purpose |
|---|---|---|
| `TELEMETRY_SERVICE` | required | Service identifier reported everywhere |
| `TELEMETRY_ENDPOINT` | required for remote export | OTLP base URL incl. port (e.g. `http://alloy.monitoring:4318`) |
| `TELEMETRY_ENVIRONMENT` | optional | Deployment environment resource attribute |

The library also accepts the legacy `HELLNET_*` names as fallback. With no
endpoint, OTLP export is disabled while stdout logs and `/metrics` stay active.

Full variable reference: <https://github.com/guilhermelinosp/hellnet-lib-telemetry>

### Driver notifications / webhook

The backend consumes the same Kafka ride-requested message as the producer and
sends it through `Kafka -> NotificationSink -> webhook dispatcher` to the
external notification service. The service can then fan out the notification as
a push message to the Driver app. Flutter is not changed and does not consume
Kafka directly.

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_DRIVER_BFF_CONSUMER_GROUP` | `fast-driver-bff` | Dedicated group; never share the matching group |
| `FAST_NOTIFICATION_WEBHOOK_URL` | *(disabled)* | Absolute `http`/`https` endpoint; enables the notification consumer |
| `FAST_NOTIFICATION_WEBHOOK_SECRET` | *(required with URL)* | HMAC-SHA256 signing secret |
| `FAST_NOTIFICATION_WEBHOOK_TIMEOUT` | `5s` | Per-attempt HTTP timeout |
| `FAST_NOTIFICATION_WEBHOOK_MAX_RETRIES` | `3` | Retries after the initial request, maximum `10` |
| `FAST_NOTIFICATION_WEBHOOK_BACKOFF` | `250ms` | Exponential retry backoff, maximum `30s` |

`KAFKA_TOPIC_RIDE_REQUESTED` is unchanged. Its current default is
`fast-ride-requested.v1`; when set (for example to
`fast.ride.requested.v1` in `cmd/api/.env`), the consumer uses that exact topic.
`KAFKA_TOPIC_PREFIX`, when used, is resolved by the Kafka library in
the same way for the producer and consumer.

#### Webhook contract

Each consumed `ride.requested.v1` event produces one JSON `POST` body. The
`ride` object is the original Kafka event, preserving its existing camelCase
fields:

```json
{
  "event": "ride.requested.v1",
  "event_id": "event-id",
  "ride": {
    "eventId": "event-id",
    "eventVersion": 1,
    "occurredAt": 1720000000000,
    "rideId": "ride-id",
    "riderId": "rider-id",
    "pickupLatitude": -23.56,
    "pickupLongitude": -46.65,
    "destinationLatitude": -23.55,
    "destinationLongitude": -46.64
  }
}
```

`driver_id` is included only when a caller supplies one; the current
`rides.Requested` Kafka model contains no driver ID or driver token, so the
integrated consumer omits it. No driver targeting or persistence is invented.
The signature is `sha256=<lowercase hex HMAC-SHA256(body)>` in
`X-Notification-Signature`. `X-Notification-Event-ID` and `Idempotency-Key`
also carry `event_id` when present. HTTP 2xx succeeds; network errors, 408,
429, and 5xx are retried, while other statuses fail without retry.

---

## Observability

Everything below exists because the library does it natively:

* **Logging** — one structured logger: JSON to stdout *and* OTLP, correlated
  with `trace_id`. Inject `ops.Logger` into application dependencies.
  Do **not** add zap/zerolog/logrus.
* **Metrics** — HTTP instrumentation happens once around the whole router
  (`telemetry.Middleware(ops, handler)`): requests/duration/inflight/
  response+body size/error totals plus runtime (GC, memory, goroutines).
  `GET /metrics` serves Prometheus format from the same registry used for OTLP.
* **Tracing** — inbound spans extracted by the same middleware. For business
  operations that actually deserve a span:

```go
err := ops.WithSpan("riders.process", func(ctx context.Context) error {
    return s.repo.Requested(ctx, order)
})
```

  Skip spans for trivial calls — signal-to-noise is a feature.
* **Health/readiness** — `/live` `/ready` `/health` are library handlers; the
  template only mounts them. Dependency checks plug in through
  [`HealthRegister`](#adding-an-observable-dependency-postgresql-redis-kafka).

---

## Project layout

```text
cmd/api/main.go              # tiny bootstrap: wiring + shutdown order ONLY
internal/
├── api/                     # TRANSPORT-NEUTRAL contracts (no gin import)
│   ├── handler.go           #   Handler interface, Route, methods
│   ├── request.go           #   Requested port + strict JSON BindInto
│   ├── response.go          #   Response building blocks
│   ├── errors.go            #   error taxonomy + MapError (sanitized envelope)
│   ├── router.go            #   Router interface, Middleware type
│   └── api.go               #   RegisterPlatform: /, /live, /ready, /health,
│                            #   /metrics, /api/v1 group mounting
│   └── ginadapter/          # ← THE ONLY PACKAGE THAT IMPORTS GIN
│       ├── router.go        #   engine build, {name}→:name translation, groups
│       ├── handler.go       #   Requested port impl, JSON writes, error funnel
│       └── middleware.go    #   request-id, security headers, CORS, recovery
├── config/config.go         # APP_* parsing, timeouts, build metadata
├── server/server.go         # http.Server + graceful Run(ctx)
├── hello/                   # reference module (replace me!)
│   ├── Service.go           #   business rules behind Service interface
│   └── handler.go           #   route declarations + input binding
openapi/openapi.yaml         # contract of the REAL endpoints (kept honest)
Containerfile · Makefile · .goreleaser.yaml · .github/workflows/*
```

---

## Development

```bash
make run                 # zero-config local boot
make test                # race + shuffle, stdlib-only stacks
make cover-html          # coverage report
make lint                # golangci-lint
make build               # bin/golang-api-template with ldflags metadata
make image               # podman build (works with docker too)
./bin/golang-api-template --version-check via curl localhost:8080/
```

Tests run fast and deterministically without an OTLP collector: the library
keeps telemetry local when the endpoint is empty.

### Testing philosophy

* Abstraction behaviors (`errors` mapping, request/response ports) tested
  without any framework.
* Adapter integration behavior (routing, recovery, headers, CORS, 404/405
  envelopes) tested through `httptest` against the real Gin engine.
* `internal/server` proves listen/graceful-drain with a live socket.

---

## OpenAPI

Contract lives at [`openapi/openapi.yaml`](openapi/openapi.yaml) documenting
the real surface: root discovery, probes, metrics and `/api/v1/hello` flavors.
Hand-maintained **but enforced**: every endpoint change should touch it in the
same PR (keeping docs honest beats generating drift).

---

## CI / CD

| Workflow | Trigger | Contents |
|---|---|---|
| `pr-check` | PR | shellcheck · merge strategy/conventional commits · gitleaks · labeler · **go-quality** (tidy guard/vet/race tests/build/lint/govulncheck/dependency-review) |
| `codeql` | PR→main | CodeQL security+quality (Go, actions) |
| `pipeline` | push main | org release (semver tag + GH Release) → go-quality → goreleaser artifacts/checksums upload → container image |

Every job is an import from [ci-templates](https://github.com/guilhermelinosp/ci-templates) — this repository owns zero CI logic, only flow declarations. Reuse the same two files in any Go Service.

Releases trigger on tag push (created by the org `release` workflow with
semver derived from conventional commits). GoReleaser ships archives +
checksums for linux/amd64, linux/arm64, darwin/amd64, darwin/arm64,
windows/amd64 onto that same tag's release page.

---

## Container

`Containerfile`: multi-stage (golang-alpine → distroless static), cache-mounts
for modules/build cache, `-trimpath`, ldflags-injected metadata, non-root user,
single static binary. Dev tools are absent from the runtime image by design.

---

## Recipes

### Creating a new API from this template

1. `Use this template` on GitHub → clone.
2. Search-and-replace module name `guilhermelinosp/golang-api-template` in
   `go.mod` + imports.
3. Rename `TELEMETRY_SERVICE` value wherever you configure it
   (`.env.example`, your deployment platform of choice).
4. Delete `internal/hello/*`, its registration lines in `cmd/api/main.go`,
   and the `/api/v1` paths in `openapi/openapi.yaml`.
5. `make test && make run` → green baseline restored.
6. Build your first domain module following the recipe below.

### Adding an endpoint (the whole ceremony)

```go
// internal/riders/handler.go
package riders

import (
    "context"
    "net/http"

    "github.com/guilhermelinosp/fast-platform-modular/internal/api"
)

type RequestedInput struct{ SKU string `json:"sku"` }

func (h *Handler) request(ctx context.Context, req api.Requested) (api.Response, error) {
    var in RequestedInput
    if err := req.Bind(&in); err != nil {          // strict JSON → 400/413 handled
        return api.Response{}, err
    }
    order, err := h.Service.Requested(ctx, in.SKU)    // pure ctx flow: no gin anywhere
    if err != nil {
        return api.Response{}, err                 // *api.Error → predictable envelope
    }
    return api.JSON(http.StatusCreated, order), nil
}

// Route declarations (path wildcards use web syntax):
func (h *Handler) Routes() []api.Route {
    return []api.Route{
        {Method: api.MethodPost, Path: "/riders", Handler: api.HandlerFunc(h.request)},
        {Method: api.MethodGet, Path: "/riders/{id}", Handler: api.HandlerFunc(h.get)},
    }
}
```

Wire it — three lines in `cmd/api/main.go`:

```go
ordersHandler := riders.NewHandler(riders.NewService(repo))
depsRoutes := append(helloHandler.Routes(), ordersHandler.Routes()...)
api.RegisterPlatform(router, info, api.Deps{Routes: depsRoutes})
```

That single registration yields: routing under `/api/v1`, request-id/security
headers/CORS/recovery, request logs+metrics+traces, validation/error envelopes,
request-size enforcement. Update `openapi/openapi.yaml` alongside.

### Adding an observable dependency (PostgreSQL, Redis, Kafka)

There is **no template-level health framework** — register checks directly
with the library; readiness reflects them immediately:

```go
// cmd/api/main.go, after opening your dependency
ops.HealthRegister("postgres", func(ctx context.Context) error {
    return sqlDB.PingContext(ctx) // readiness flips 503 automatically if down
})

// Outbound HTTP gets tracing/metrics for free by borrowing the lib client:
client := ops.HTTPClient(&http.Client{Timeout: 5 * time.Second})
```

Notes worth knowing:

* checks receive the `ctx` from the library on each probe run;
* the lib exports `healthcheck_status` gauges → visible at `/metrics` and OTLP;
* Kafka/cache equivalents follow the identical pattern with a cheap ping call;
* need an in-code DB pool? the library ships `WatchDB(db *sql.DB, "postgres")`.

---

## Deliberately NOT included (KISS/YAGNI)

DI frameworks · config libraries like Viper · second logging/metrics/tracing
stacks · parallel health framework · CQRS/event-bus/command-bus · repository
frameworks · generated OpenAPI pipelines · separate admin port · per-signal
OTLP toggles (the library exposes one switch by design) · validator libraries
(strict decoding covers template needs until proven otherwise).

Every omission has an owner: add dependencies only after
stdlib → hellnet-lib-telemetry → gin all fail to solve the problem.

---

## Contributing & License

See [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md).
Licensed under [Apache 2.0](LICENSE).

<!-- release pipeline verification -->

<!-- verify round 2 -->

<!-- verify round 3 -->

<!-- verify round 4 -->
