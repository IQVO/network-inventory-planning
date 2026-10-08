# network-inventory-planning (Helm chart)

Deploys the Network Inventory & Planning service — the network-tier bounded
context of the warehouse fleet: transfer option simulation, the transfer
approval saga (ADR 0002/0005) and the network fact read models.

## Prerequisites

- Kubernetes with an HTTP ingress/routing story (the Service is ClusterIP;
  reach it via port-forward or your cluster's gateway)
- PostgreSQL (migrations `0001`–`0003` apply automatically on pod start via
  `MIGRATIONS_PATH` baked into the image; the `mcp` component, when enabled, runs the same idempotent step)
- Kafka (optional — see below)

## Install

```sh
helm install network-inventory-planning ./charts/network-inventory-planning \
  --set database.existingSecret=nip-database \
  --set kafka.enabled=true \
  --set kafka.brokers=kafka:9092 \
  --set kafka.siteCapabilityConsumerGroup=nip-site-capability \
  --set kafka.siteSkuDemandConsumerGroup=nip-site-sku-demand \
  --set kafka.capacityPlanConsumerGroup=nip-capacity-plan \
  --set kafka.transferReplyConsumerGroup=nip-transfer-reply \
  --set kafka.transferFactConsumerGroup=nip-transfer-fact \
  --set config.transferPickPathId=pick-path \
  --set config.transferDispatchPathId=dispatch-path
```

Or render only:

```sh
helm template network-inventory-planning ./charts/network-inventory-planning
```

## Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `replicaCount` | `1` | Deployment replicas (ignored when `autoscaling.enabled=true`) |
| `image.repository` | `claudioed/network-inventory-planning` | Image |
| `image.tag` | `.Chart.AppVersion` | Image tag |
| `service.type` / `service.port` | `ClusterIP` / `80` | Service (targetPort 8080) |
| `resources` | 100m/128Mi req, 500m/256Mi lim | Container resources |
| `autoscaling.enabled` | `false` | HPA (max 3, 70% CPU) — off by default |
| `environment` | `local` | `ENVIRONMENT` label on exported telemetry |
| `config.httpAddr` | `:8080` | `HTTP_ADDR` |
| `config.maxStaleness` | `""` | `PLANNING_MAX_STALENESS` (Go duration; binary default 10m) |
| `config.transferPickPathId` | `""` | `TRANSFER_PICK_PATH_ID` — no default on purpose; unset makes `/v1/transfers:approve` answer 503 config-incomplete (ADR 0005, fail-closed) |
| `config.transferPickCptOffset` | `""` | `TRANSFER_PICK_CPT_OFFSET` (binary default 2h) |
| `config.transferDispatchPathId` | `""` | `TRANSFER_DISPATCH_PATH_ID` |
| `config.transferDispatchCptOffset` | `""` | `TRANSFER_DISPATCH_CPT_OFFSET` (binary default 3h) |
| `config.outboxRelayEnabled` | `"true"` | `OUTBOX_RELAY_ENABLED` — transactional-outbox relay drain |
| `database.url` | `""` | Inline `DATABASE_URL`; prefer `database.existingSecret` |
| `database.existingSecret` | `""` | Existing Secret name with `DATABASE_URL` key (`secretKeyRef`) |
| `database.existingSecretKey` | `DATABASE_URL` | Key inside that Secret |
| `kafka.enabled` | `false` | Gates `KAFKA_BROKERS` + all consumer-group env |
| `kafka.brokers` | `kafka:9092` | `KAFKA_BROKERS` (comma-separated) |
| `kafka.siteCapabilityConsumerGroup` | `""` | `SITE_CAPABILITY_CONSUMER_GROUP` — empty = consumer off |
| `kafka.siteSkuDemandConsumerGroup` | `""` | `SITE_SKU_DEMAND_CONSUMER_GROUP` — empty = consumer off |
| `kafka.capacityPlanConsumerGroup` | `""` | `CAPACITY_PLAN_CONSUMER_GROUP` — empty = consumer off |
| `kafka.transferReplyConsumerGroup` | `""` | `TRANSFER_REPLY_CONSUMER_GROUP` — empty = consumer off |
| `kafka.transferFactConsumerGroup` | `""` | `TRANSFER_FACT_CONSUMER_GROUP` — empty = consumer off |
| `extraEnv` | `[]` | Extra container env (rendered verbatim) |
| `mcp.enabled` | `false` | Renders the read-only MCP server (`cmd/mcp`): its own Deployment + Service, `component=mcp`, command `/app/mcp` |
| `mcp.httpAddr` | `:8090` | `MCP_ADDR` (Streamable HTTP at `/` and `/mcp`, `GET /healthz`) |
| `mcp.service.type` / `mcp.service.port` | `ClusterIP` / `8090` | MCP Service (targetPort 8090) |
| `mcp.migrationsPath` | `migrations` | `MIGRATIONS_PATH` inside the image |
| `mcp.logLevel` | `info` | `LOG_LEVEL` |
| `mcp.resources` / `mcp.replicaCount` / `mcp.extraEnv` | 100m/128Mi req, 500m/256Mi lim / `1` / `[]` | MCP container sizing and extra env |
| `frontend.enabled` | `false` | Renders the console-remote nginx pod (`nip_mfe`, ADR 0010): its own Deployment + ClusterIP Service, `component=frontend`. No Ingress/HTTPRoute |
| `frontend.image.repository` / `frontend.image.tag` | `warehouse/network-inventory-planning-frontend` / chart appVersion | Image built from `web/Dockerfile` (side-loaded in kind, never pulled) |
| `frontend.service.port` / `frontend.service.targetPort` | `80` / `8080` | Frontend Service (nginx-unprivileged listens on 8080) |
| `frontend.replicaCount` / `frontend.resources` | `1` / 100m/128Mi req, 500m/256Mi lim | Frontend sizing |
| Liveness / readiness / startup probes | `/healthz` | startup: 2s×30; readiness flips with the drain |

### MCP server (opt-in, read-only)

`mcp.enabled=true` adds a second Deployment + Service next to the api, both
selecting on `app.kubernetes.io/component` (`api` / `mcp`) so each Service
selects exactly one Deployment. The MCP binary reuses the api's database
Secret (`DATABASE_URL`, optional direct `MIGRATIONS_DATABASE_URL`) and
`config.maxStaleness`; it starts no outbox relay, no Kafka consumer and never
dials Kafka. Tools (all read-only): `get_transfer`, `list_transfers`,
`find_stuck_transfers`, `simulate_transfer_options`. There is no auth: the
ClusterIP boundary is the access control (fleet decision, 2026-09-11). See
`docs/docs/adr/0008-transfer-read-side-and-read-only-mcp.md`.

### Console remote (opt-in, static)

`frontend.enabled=true` adds an nginx pod serving the `nip_mfe` Module
Federation remote from `web/` (ADR 0010), `component=frontend`, with a
ClusterIP Service. The chart never routes it: warehouse-infra's Nginx web
gateway serves `/mfes/network-inventory-planning/` from this Service, and Kong
serves the API only. The remote calls this service's own REST API through
`${apiOrigin}/api/network-inventory-planning`.

### Consumer groups are per-consumer off switches

Each `kafka.*ConsumerGroup` env var has **no default**: an empty value means
that consumer does not run. This is deliberate (`cmd/network-inventory-planning/main.go`):
a default group id would let a locally run process join the live cluster's
group and steal its partitions. Set group ids only for the deployment that
owns them.

### Lint / test

```sh
ct lint --charts charts/network-inventory-planning --validate-maintainers=false --check-version-increment=false
helm template charts/network-inventory-planning >/dev/null
python3 charts/network-inventory-planning/tests/test_env_wiring.py
python3 charts/network-inventory-planning/tests/test_service_selectors.py
```
