# Derived from: apis/openapi.yaml (approveTransfer:
# POST /v1/transfers:approve) and .claude/rules/domain-model.md (Transfer Plan:
# the approved, durable orchestration record; TransferPlanApproved and
# OriginReservationRequested; approval coordinates, it does not own
# reservations or stock).
#
# Approval is validated against the CURRENT fail-closed read models and is
# idempotent per the required Idempotency-Key header.
@bdd
Feature: Approving a transfer proposal starts the allocation saga
  As an operator
  I want to approve an advisory proposal exactly once, safely
  So that origin stock is requested from inventory-storage without double transfers

  Background:
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"

  Scenario: A valid approval persists the saga and awaits inventory-storage
    When I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 200
    And the response content type is "application/json"
    And the JSON field "transferId" starts with "trf-"
    And the JSON field "state" is "ALLOCATING"
    And the JSON field "originSiteId" is "WH1"
    And the JSON field "destinationSiteId" is "WH2"
    And the JSON field "sku" is "SKU-1"
    And the JSON field "quantity" is 5
    And the JSON field "policyVersion" is "policy-v3"
    And the JSON field "replayed" is false
    And the JSON field "expiresAt" is "2026-10-07T15:00:00Z"
    And the JSON field "transferLineId" is "{T1}:1"
    And the JSON field "reservationId" is absent
    And the JSON field "rejectionReason" is absent

  Scenario: The saga walks PROPOSED, APPROVED and ALLOCATING in one step and keeps the operator's reason
    Given an approved transfer "T1"
    When I request GET "/v1/transfers/{T1}"
    Then the response status is 200
    And the audit trail states read "DRAFT, PROPOSED, APPROVED, ALLOCATING"
    And the audit trail events read "TransferDrafted, TransferProposed, TransferPlanApproved, TransferAllocationRequested"
    And the JSON field "audit.1.cause" is "operator approved rebalance"

  Scenario: Approval raises the integration events and the analytics occurrences
    When I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the domain event "TransferPlanApproved" was published 1 time
    And the domain event "TransferAllocationRequested" was published 1 time
    And the domain event "TransferStateAdvanced" was published 4 times
    And the domain event "WorkDemandReleased" was not published

  Scenario: Everything the approval publishes is a CloudEvents 1.0 event
    When I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then every published message is a valid CloudEvents 1.0 event
    And the topic "warehouse.network-inventory-planning.events" carries these CloudEvents:
      | type                        |
      | TransferPlanApproved        |
      | TransferAllocationRequested |
    And the topic "warehouse.network-inventory-planning.analytics" carries these CloudEvents:
      | type                  |
      | TransferStateAdvanced |
      | TransferStateAdvanced |
      | TransferStateAdvanced |
      | TransferStateAdvanced |
    And the "TransferPlanApproved" CloudEvent has subject "{T1}"
    And the "TransferAllocationRequested" CloudEvent has subject "{T1}:1"

  Scenario: Replaying a key with the same payload returns the original transfer
    Given an approved transfer "T1"
    When I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 200
    And the JSON field "replayed" is true
    And the JSON field "transferId" is "{T1}"
    And the domain event "TransferPlanApproved" was published 1 time
    And the domain event "TransferAllocationRequested" was published 1 time

  Scenario: A replay creates no second transfer
    Given an approved transfer "T1"
    And I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    When I request GET "/v1/transfers"
    Then the JSON field "total" is 1

  Scenario: A replay after the saga advanced returns the persisted state, not the first answer
    Given a transfer "T1" in state "ALLOCATED"
    When I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 200
    And the JSON field "replayed" is true
    And the JSON field "state" is "ALLOCATED"
    And the JSON field "reservationId" is "res-1"

  Scenario Outline: Reusing a key for a different request is a conflict
    Given an approved transfer "T1"
    When I approve a transfer of <quantity> units of SKU "SKU-1" from "WH1" to "WH2" under policy "<policy>" with the idempotency key "T1"
    Then the response status is 409
    And the problem detail type is "idempotency-conflict"
    And transfer "T1" is in state "ALLOCATING"

    Examples: what differs from the first request
      | quantity | policy     |
      | 6        | policy-v3  |
      | 5        | policy-v9  |

  Scenario: A fresh key approves the same proposal again as a separate transfer
    Given an approved transfer "T1"
    When I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T2"
    Then the response status is 200
    And the JSON field "replayed" is false
    When I request GET "/v1/transfers"
    Then the JSON field "total" is 2

  Scenario: The operator's reason is optional
    When I send POST "/v1/transfers:approve" with the idempotency key "T1" and this body:
      """
      {"originSiteId":"WH1","destinationSiteId":"WH2","sku":"SKU-1","quantity":5,"policyVersion":"policy-v3","proposalAsOf":"2026-10-06T14:55:00Z"}
      """
    Then the response status is 200
    And the JSON field "state" is "ALLOCATING"

  Scenario: The Idempotency-Key header is required
    When I send POST "/v1/transfers:approve" with this body:
      """
      {"originSiteId":"WH1","destinationSiteId":"WH2","sku":"SKU-1","quantity":5,"policyVersion":"policy-v3","proposalAsOf":"2026-10-06T14:55:00Z"}
      """
    Then the response status is 400
    And the problem detail type is "idempotency-key-required"
    And no transfer exists
    And no domain events were published

  Scenario Outline: A request that is not a valid approval document is a 400
    When I send POST "/v1/transfers:approve" with the idempotency key "bad" and this body:
      """
      <body>
      """
    Then the response status is 400
    And the problem detail type is "invalid-request"
    And no transfer exists

    Examples: malformed requests
      | body                                                                                                                                    | why                    |
      | {"originSiteId":"WH1",                                                                                                                  | truncated JSON         |
      | {"originSiteId":"WH1","destinationSiteId":"WH2","sku":"SKU-1","quantity":5,"policyVersion":"p","proposalAsOf":"2026-10-06T14:55:00Z","surprise":1} | unknown field          |
      | {"originSiteId":"WH1","destinationSiteId":"WH2","sku":"SKU-1","quantity":"five","policyVersion":"p","proposalAsOf":"2026-10-06T14:55:00Z"}      | quantity is not a number |
      | {"originSiteId":"WH1","destinationSiteId":"WH2","sku":"SKU-1","quantity":5,"policyVersion":"p","proposalAsOf":"yesterday"}              | proposalAsOf is not a time |

  Scenario Outline: A proposal the domain refuses on its face is a 422
    When I send POST "/v1/transfers:approve" with the idempotency key "bad" and this body:
      """
      {"originSiteId":"<origin>","destinationSiteId":"<destination>","sku":"SKU-1","quantity":<quantity>,"policyVersion":"<policy>","proposalAsOf":"2026-10-06T14:55:00Z"}
      """
    Then the response status is 422
    And the problem detail type is "invalid-approval"
    And no transfer exists

    Examples: invalid proposals
      | origin | destination | quantity | policy    | why                           |
      | WH1    | WH2         | 0        | policy-v3 | the quantity is zero          |
      | WH1    | WH2         | -3       | policy-v3 | the quantity is negative      |
      | WH1    | WH1         | 5        | policy-v3 | origin and destination match  |
      | WH1    | WH2         | 5        |           | the policy version is missing |

  Scenario: A proposal without its snapshot watermark is a 422
    When I send POST "/v1/transfers:approve" with the idempotency key "bad" and this body:
      """
      {"originSiteId":"WH1","destinationSiteId":"WH2","sku":"SKU-1","quantity":5,"policyVersion":"policy-v3"}
      """
    Then the response status is 422
    And the problem detail type is "invalid-approval"

  Scenario Outline: Approval fails closed when the current facts do not support the transfer
    When I approve a transfer of <quantity> units of SKU "<sku>" from "<origin>" to "<destination>" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 422
    And the problem detail type is "facts-incomplete"
    And the JSON field "detail" contains "<refusal>"
    And no transfer exists
    And no domain events were published

    Examples: refusals
      | origin | destination | sku   | quantity | refusal                                       |
      | WH9    | WH2         | SKU-1 | 5        | origin site WH9 has no complete facts         |
      | WH1    | WH9         | SKU-1 | 5        | destination site WH9 has no complete facts    |
      | WH1    | WH2         | SKU-9 | 5        | shows no in-window demand for SKU SKU-9       |
      | WH1    | WH2         | SKU-1 | 461      | cannot cover its in-window demand             |

  Scenario: The origin's capacity may be used up exactly
    When I approve a transfer of 460 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 200
    And the JSON field "state" is "ALLOCATING"

  Scenario: Without any facts nothing can be approved
    Given no planning facts are declared
    When I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 422
    And the problem detail type is "facts-incomplete"
    And no transfer exists

  Scenario: Stale facts refuse the approval
    When 11 minutes pass
    And I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 422
    And the problem detail type is "facts-incomplete"
    And the JSON field "detail" contains "stale"
    And no transfer exists

  Scenario Outline: A site disabled for its role in the transfer refuses the approval
    Given site "<site>" cannot <direction> transfers
    When I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 422
    And the problem detail type is "facts-incomplete"
    And no transfer exists

    Examples: disabled directions
      | site | direction |
      | WH1  | send      |
      | WH2  | receive   |

  Scenario: An origin without a published capacity plan refuses the approval
    Given site "WH1" has no published capacity plan
    When I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 422
    And the problem detail type is "facts-incomplete"

  Scenario: A deployment without a releasable pick leg approves nothing
    Given the pick work release is not configured
    When I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 503
    And the problem detail type is "config-incomplete"
    And no transfer exists
    And no domain events were published

  Scenario: An instance without a database approves nothing
    Given this instance runs without a database
    When I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 503
    And the problem detail type is "saga-unavailable"
