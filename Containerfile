# syntax=docker/dockerfile:1
# Multi-stage build: reproducible, minimal, non-root runtime.

ARG GO_VERSION=1.27

# ── Build stage ──────────────────────────────────────────────────────────────
FROM docker.io/library/golang:${GO_VERSION}-alpine AS builder

# Build metadata (overridable by CI; defaults keep local builds honest).
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown

WORKDIR /src

# Cache-friendly dependency layer.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOFLAGS=-trimpath \
    go build -ldflags="-w -s -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" \
    -o /bin/platform ./cmd/platform

# ── Runtime stage ────────────────────────────────────────────────────────────
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /bin/platform /platform

# Platform endpoints: /live /ready /health on the same port.
EXPOSE 8080

# Numeric UID/GID (distroless "nonroot"): Kubernetes cannot verify runAsNonRoot for a named user.
USER 65532:65532

ENTRYPOINT ["/platform"]
