---
paths:
  - "internal/adapters/**/kafka/**"
  - "internal/adapters/outbound/events/**"
  - "apis/asyncapi*"
---

# Cross-service integration events (Kafka)

This service will both consume inventory/order/facility execution facts and publish planning/orchestration facts. Its integration topic is `warehouse.network-inventory-planning.events` and its local analytics topic is `warehouse.network-inventory-planning.analytics`.

## Events: CloudEvents 1.0 is MANDATORY

Every Kafka message this service produces or consumes is a CloudEvents 1.0 event in structured content mode. Build and validate them only through `internal/adapters/kafka/cloudevents`. No flat envelope, dual-read, dual-write, or envelope feature flag is permitted.

- Source: `/warehouse/network-inventory-planning`
- Published type prefix: `com.warehouse.wes.network-inventory-planning`
- Kafka message key: aggregate ID; use `kafkago.Hash{}`.
- Every producer adds `content-type: application/cloudevents+json; charset=UTF-8` and preserves W3C trace headers.
- Consumers dispatch on the exact full type, deduplicate CloudEvent `id`, log/DLQ invalid data, and commit past it. Consumer group IDs are environment-configured.

### Planned published types

| `type` | topic | `subject` | `dataschema` |
| --- | --- | --- | --- |
| `com.warehouse.wes.network-inventory-planning.transfer.TransferProposed` | `warehouse.network-inventory-planning.events` | proposal ID | `urn:warehouse:network-inventory-planning:events:TransferProposed:v1` |
| `com.warehouse.wes.network-inventory-planning.transfer.TransferPlanApproved` | `warehouse.network-inventory-planning.events` | plan ID | `urn:warehouse:network-inventory-planning:events:TransferPlanApproved:v1` |
| `com.warehouse.wes.network-inventory-planning.transfer.OriginReservationRequested` | `warehouse.network-inventory-planning.events` | plan ID | `urn:warehouse:network-inventory-planning:events:OriginReservationRequested:v1` |

Exact consumed-type contracts will be added atomically with producer changes in the owning services; this repository must not invent producer contracts.
