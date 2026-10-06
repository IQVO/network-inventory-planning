# Domain model: ubiquitous language, aggregates, events, use cases

## Ubiquitous Language

- **Network Position** — a time-stamped, site-and-SKU projection of usable inventory, customer reservations, confirmed inbound, and committed outbound. It is planning input, not an inventory ledger.
- **Replenishment Policy** — versioned guardrails for one SKU at one site: safety stock, target position, service priority, and maximum outbound quantity.
- **Transfer Lane** — an approved directed origin-to-destination route with lead time and unit handling cost.
- **Transfer Proposal** — a deterministic, explainable recommendation to move a SKU between two sites. It is not permission to pick stock.
- **Transfer Plan** — the approved, durable orchestration record. It coordinates reservations, WES work, dispatch, receipt, and reconciliation without owning any of them.

## Aggregates

- **Transfer Proposal** (`internal/domain/transfer`): protects planning invariants: only known positions participate; source protection remains intact; a route must be allowed; quantity is positive and bounded by source surplus and destination deficit; score must be positive.
- **Transfer Plan** (`internal/domain/transfer`): will protect lifecycle transitions and idempotency once persistence and external command delivery are introduced.

## Domain events

- `TransferProposed` — a positive-scored candidate was generated from a policy version and a coherent position snapshot.
- `TransferPlanApproved` — an operator or policy explicitly accepted a proposal.
- `OriginReservationRequested` — approval asks Inventory Storage to hold source stock before WES work may be created.
- `TransferDispatched` / `TransferReceived` / `TransferReconciled` — lifecycle facts received from owning contexts.

## Key use cases (`internal/application/usecases`)

- `GenerateTransferProposals` — validates a single coherent planning snapshot and invokes the deterministic transfer planner.
- `ApproveTransferPlan` — will persist an explicit approval and publish the first reservation command through the transactional outbox.
