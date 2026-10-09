# ADR-0012: OTLP trace and metric export for the API process

Status: Accepted

## Context

ADR 0007 (Phase-4 observability) installed only the W3C Trace Context
propagator and recorded that "a full tracer provider (OTLP export) is a later
slice". That slice never landed, so the API process emitted no OpenTelemetry
metrics at all. The consequence was visible once the per-context Grafana
dashboard was added (warehouse-infra `scripts/gen-context-dashboards.py`):
Prometheus held no `service_name` series for NIP, so every HTTP rate / error /
latency and Go-runtime panel was empty.

Every other HTTP context in the fleet already exports OTLP through the OTel
Collector with the same small adapter (`internal/adapters/outbound/telemetry`,
first written in warehouse-planning, copied into product-master).

## Decision

1. Add `internal/adapters/outbound/telemetry` (copied from the fleet
   convention). `Setup` installs a `TracerProvider` and a `MeterProvider`
   exporting over OTLP/gRPC, the W3C propagator ADR 0007 already required, and
   Go runtime metrics. It never blocks on the Collector: with no Collector the
   service starts and serves as before and telemetry is dropped.
2. `cmd/network-inventory-planning` calls `Setup` first, with
   `service.name = network-inventory-planning` (`httpadapter.DefaultServiceName`),
   and flushes it on exit. It must run before `Routes()` is built.
3. `Routes()` wraps the `ServeMux` in `otelhttp`, which records
   `http.server.request.duration` labelled with the ServeMux pattern as
   `http.route`. A test pins that, because the dashboards depend on it.
4. The Helm chart passes `OTEL_EXPORTER_OTLP_ENDPOINT` (values `otel.*`,
   default the in-cluster Collector) and `SERVICE_VERSION`.
5. Push only. There is no Prometheus `/metrics` scrape handler.

## Out of scope

The `mcp`, `nip-projector` and `nip-reports` binaries are not instrumented by
this ADR. The dashboard's `network-inventory-planning.*` service regex already
covers them when they are, using the same `Setup`.

No business-metric counters are added: there is no agreed metric to export
yet, and the dashboard declares none.

## Consequences

- The `Request rate / Error rate / p95` and `Go runtime` panels of the NIP
  dashboard fill in for the API process once the new image is deployed.
- Spans for HTTP requests and for the Kafka paths that already inject and
  extract trace context are now actually exported, so a transfer can be
  followed across contexts in Jaeger.
- Adds the OTel SDK, OTLP gRPC exporters and gRPC to `go.mod`.
