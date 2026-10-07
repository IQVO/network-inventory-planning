# syntax=docker/dockerfile:1.7

# --- build stage ---
FROM golang:1.27-alpine@sha256:cf6fca6641884b8433441b2b0652976f975e1d0fdd26d177eaaf8596087f3125 AS build
WORKDIR /src

# Cache go.mod/go.sum download separately from source so editing source
# code doesn't bust the module-download layer.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# Build EVERY cmd/* binary by looping over the cmd/ directories, so a new
# composition root (cmd/mcp, a projector, ...) can never be merged with a
# Dockerfile that silently forgets to build it (CI skips the image build on
# non-main PRs, so the first symptom would otherwise be a CrashLoopBackOff on
# `exec: "/app/<binary>": no such file or directory`). Each binary lands in
# /out/<dirname>.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eu; \
    mkdir -p /out; \
    for d in cmd/*/; do \
        name="$(basename "$d")"; \
        echo "building ${name}"; \
        CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o "/out/${name}" "./cmd/${name}"; \
    done

# --- runtime stage ---
FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b
RUN apk upgrade --no-cache && \
    apk add --no-cache ca-certificates tzdata && \
    addgroup -g 1000 -S app && adduser -u 1000 -S app -G app
WORKDIR /app
# Every binary built above -> /app/<name>.
COPY --from=build --chown=app:app /out/ ./
# golang-migrate files live under internal/; the chart sets
# MIGRATIONS_PATH=migrations (relative to /app), matching the siblings' layout.
COPY --from=build --chown=app:app /src/internal/adapters/outbound/postgres/migrations ./migrations
USER 1000
EXPOSE 8080
ENTRYPOINT ["./network-inventory-planning"]
