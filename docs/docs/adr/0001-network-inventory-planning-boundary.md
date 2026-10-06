# ADR-0001: Network Inventory Planning Boundary

Status: Accepted

## Decision

Create Network Inventory Planning as a WES-core bounded context for SKU/site network position, replenishment policies, transfer lanes, deterministic transfer proposals, and transfer-plan orchestration.

Inventory Storage remains the authority for physical stock and reservations. Order Management remains the authority for order allocation. Facility Layout remains the authority for site topology. WES contexts remain the authority for work execution. This context does not synchronously invoke them; it integrates through versioned CloudEvents 1.0 facts and commands.

A transfer proposal is advisory until an approved transfer plan obtains an explicit origin reservation. No WES work or carrier action is issued before that reservation confirmation.

## Consequences

- Every recommendation has reproducible inputs, policy version, score breakdown, and reason codes.
- Eventual consistency is handled by snapshot freshness and lifecycle reconciliation rather than distributed transactions.
- Physical stock cannot be moved by the planner alone.
