# syntax=docker/dockerfile:1.7

# --- build stage ---
FROM golang:1.27-alpine@sha256:8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414 AS build
WORKDIR /src

# Cache go.mod/go.sum download separately from source so editing source
# code doesn't bust the module-download layer.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# BuildKit cache mounts for the module and build caches speed up repeat
# builds in CI without baking the cache into the image layers.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/network-inventory-planning ./cmd/network-inventory-planning && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/mcp ./cmd/mcp

# --- runtime stage ---
FROM alpine:3.24@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6
# apk upgrade picks up any CVE fixes published to the 3.24 branch since the
# base image was last rebuilt (e.g. openssl point releases); ca-certificates
# is still pinned explicitly for a reproducible, auditable base layer.
RUN apk upgrade --no-cache && \
    apk add --no-cache ca-certificates=20260909-r0 && \
    addgroup -g 1000 -S app && adduser -u 1000 -S app -G app
WORKDIR /app
COPY --from=build --chown=app:app /out/network-inventory-planning ./network-inventory-planning
# The read-only MCP server (Streamable HTTP on :8090). The api image
# entrypoint is unchanged; the chart's mcp Deployment runs `/app/mcp`.
COPY --from=build --chown=app:app /out/mcp ./mcp
# The golang-migrate startup step reads these (MIGRATIONS_PATH); the default
# in cmd/network-inventory-planning/main.go points at the source tree, which
# does not exist inside the image, so point it at /app/migrations instead.
COPY --from=build --chown=app:app /src/internal/adapters/outbound/postgres/migrations ./migrations
ENV MIGRATIONS_PATH=migrations
USER 1000
EXPOSE 8080 8090
ENTRYPOINT ["./network-inventory-planning"]
