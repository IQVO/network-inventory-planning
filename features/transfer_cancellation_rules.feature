# Derived from: docs/docs/adr/0022-cancel-after-allocation-compensation.md,
# docs/docs/adr/0023-cancelled-transfer-holds-no-reservation.md, the
# aggregate canvas (Cancel / CancelAfterAllocation / MarkReleased rows) and the
# use-cases table (CancelTransfer: 200 / 202 / 404 / 409 cancel-refused /
# 422 invalid-cancel; RELEASING and CANCELLED are idempotent replays).
#
# The coverage-driven suite cancels through the use case. These scenarios call
# the REAL route, POST /v1/transfers/{id}/cancellation, and pin its status,
# its slugs and what each answer publishes.
@bdd
Feature: Operator cancellation of a transfer over REST
  As an operator
  I want to cancel a transfer that should not go ahead
  So that stock held at the origin is released and nothing moves physically after a cancel

  Background:
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"

  Scenario: Cancelling a transfer still waiting for inventory-storage closes the saga at once
    Given an approved transfer "T1"
    When I send POST "/v1/transfers/{T1}/cancellation" with the idempotency key "C1" and this body:
      """
      {"reason":"wrong lane selected"}
      """
    Then the response status is 200
    And the JSON field "state" is "CANCELLED"
    And the JSON field "transferId" is "{T1}"
    And the JSON field "transferLineId" is "{T1}:1"
    And the JSON field "reservationId" is absent
    And transfer "T1" is in state "CANCELLED"
    When I request GET "/v1/transfers/{T1}"
    Then the audit trail states read "DRAFT, PROPOSED, APPROVED, ALLOCATING, CANCELLED"
    And the JSON field "audit.4.cause" is "wrong lane selected"
    And the domain event "TransferStateAdvanced" was published 5 times
    And the domain event "TransferAllocationReleaseRequested" was not published

  Scenario: Cancelling an ALLOCATED transfer answers 202 and asks inventory-storage to release the hold
    Given a transfer "T1" in state "ALLOCATED"
    When I send POST "/v1/transfers/{T1}/cancellation" with the idempotency key "C1" and this body:
      """
      {"reason":"customer order withdrawn"}
      """
    Then the response status is 202
    And the JSON field "state" is "RELEASING"
    And the JSON field "reservationId" is "res-1"
    And the domain event "TransferAllocationReleaseRequested" was published 1 time
    And the release command for transfer "T1" names reservation "res-1" for 5 units of SKU "SKU-1" at site "WH1" because "customer order withdrawn"
    And the domain event "TransferStateAdvanced" was published 6 times

  Scenario: A retried cancel while the release is in flight asks inventory-storage only once
    Given a transfer "T1" in state "ALLOCATED"
    And I send POST "/v1/transfers/{T1}/cancellation" with the idempotency key "C1" and this body:
      """
      {"reason":"customer order withdrawn"}
      """
    When I send POST "/v1/transfers/{T1}/cancellation" with the idempotency key "C2" and this body:
      """
      {"reason":"customer order withdrawn"}
      """
    Then the response status is 202
    And the response header "Idempotency-Replayed" is absent
    And the JSON field "state" is "RELEASING"
    And the domain event "TransferAllocationReleaseRequested" was published 1 time
    And the domain event "TransferStateAdvanced" was published 6 times

  Scenario: A retried cancel of an already cancelled transfer answers 200 with the same state and publishes nothing
    Given an approved transfer "T1"
    And I send POST "/v1/transfers/{T1}/cancellation" with the idempotency key "C1" and this body:
      """
      {"reason":"wrong lane selected"}
      """
    When I send POST "/v1/transfers/{T1}/cancellation" with the idempotency key "C2" and this body:
      """
      {"reason":"wrong lane selected"}
      """
    Then the response status is 200
    And the JSON field "state" is "CANCELLED"
    And the response header "Idempotency-Replayed" is absent
    And the domain event "TransferStateAdvanced" was published 5 times

  Scenario: Replaying the same cancel key returns the first answer even after the saga moved on
    Given a transfer "T1" in state "ALLOCATED"
    And I send POST "/v1/transfers/{T1}/cancellation" with the idempotency key "C1" and this body:
      """
      {"reason":"customer order withdrawn"}
      """
    And I remember the response
    And a "TransferStockAllocationReleased" fact arrives for transfer "T1"
    When I send POST "/v1/transfers/{T1}/cancellation" with the idempotency key "C1" and this body:
      """
      {"reason":"customer order withdrawn"}
      """
    Then the response status is 202
    And the response header "Idempotency-Replayed" is true
    And the response is the one I remembered
    And the JSON field "state" is "RELEASING"
    And transfer "T1" is in state "CANCELLED"
    And the domain event "TransferAllocationReleaseRequested" was published 1 time

  Scenario Outline: A transfer whose stock is physically in motion or whose saga is closed cannot be cancelled
    Given a transfer "T1" in state "<state>"
    When I send POST "/v1/transfers/{T1}/cancellation" with the idempotency key "C1" and this body:
      """
      {"reason":"too late"}
      """
    Then the response status is 409
    And the problem detail type is "cancel-refused"
    And transfer "T1" is in state "<state>"
    And the domain event "TransferAllocationReleaseRequested" was not published

    Examples: states past the point of no return
      | state         | why                                  |
      | PICKED        | the pick is done                     |
      | IN_TRANSIT    | the freight is on the road           |
      | ARRIVED       | the stock is at the destination dock |
      | RECEIVED      | the saga is complete                 |
      | UNFULFILLABLE | inventory-storage already refused it |

  Scenario: Cancelling a transfer that does not exist is a 404
    When I send POST "/v1/transfers/trf-ghost/cancellation" with the idempotency key "C1" and this body:
      """
      {"reason":"housekeeping"}
      """
    Then the response status is 404
    And the problem detail type is "transfer-not-found"

  Scenario Outline: A cancel without a usable reason is refused and changes nothing
    Given an approved transfer "T1"
    When I send POST "/v1/transfers/{T1}/cancellation" with the idempotency key "C1" and this body:
      """
      <body>
      """
    Then the response status is 422
    And the problem detail type is "invalid-cancel"
    And transfer "T1" is in state "ALLOCATING"
    And the domain event "TransferStateAdvanced" was published 4 times

    Examples: missing reasons
      | body                | why                          |
      | {"reason":""}       | an empty reason              |
      | {"reason":"   "}    | a blank reason               |
      | {}                  | no reason at all             |

  Scenario Outline: A cancel request that is not a valid cancellation document is a 400
    Given an approved transfer "T1"
    When I send POST "/v1/transfers/{T1}/cancellation" with the idempotency key "C1" and this body:
      """
      <body>
      """
    Then the response status is 400
    And the problem detail type is "invalid-request"
    And transfer "T1" is in state "ALLOCATING"

    Examples: malformed requests
      | body                                      | why            |
      | {"reason":"changed my mind","force":true} | unknown field  |
      | {"reason":                                | truncated JSON |
      | not json at all                           | not JSON       |

  Scenario: The cancel route needs an Idempotency-Key like every other write
    Given an approved transfer "T1"
    When I send POST "/v1/transfers/{T1}/cancellation" without an idempotency key and this body:
      """
      {"reason":"wrong lane selected"}
      """
    Then the response status is 400
    And the problem detail type is "idempotency-key-required"
    And transfer "T1" is in state "ALLOCATING"

  Scenario: A key spent on an approval cannot be reused to cancel
    Given an approved transfer "T1"
    When I send POST "/v1/transfers/{T1}/cancellation" with the idempotency key "T1" and this body:
      """
      {"reason":"wrong lane selected"}
      """
    Then the response status is 409
    And the problem detail type is "idempotency-key-reused"
    And transfer "T1" is in state "ALLOCATING"

  Scenario: An allocation reply that loses the race with a cancel never releases pick work
    Given an approved transfer "T1"
    And I send POST "/v1/transfers/{T1}/cancellation" with the idempotency key "C1" and this body:
      """
      {"reason":"wrong lane selected"}
      """
    When a "TransferStockAllocated" fact arrives for transfer "T1"
    Then the fact is refused as an illegal transition
    And transfer "T1" is in state "CANCELLED"
    And the work demand "{T1}:pick" was not released
    When I request GET "/v1/transfers/{T1}"
    Then the JSON field "reservationId" is absent

  Scenario: A pick that races an in-flight release is refused and releases no dispatch work
    Given a transfer "T1" in state "RELEASING"
    When a "TransferPicked" fact arrives for transfer "T1"
    Then the fact is refused as an illegal transition
    And transfer "T1" is in state "RELEASING"
    And the work demand "{T1}:dispatch" was not released

  Scenario: A cancelled transfer is listed under the CANCELLED state
    Given an approved transfer "T1"
    And 1 minute passes
    And a transfer "T2" in state "RELEASING"
    And 1 minute passes
    And a transfer "T3" in state "CANCELLED"
    When I request GET "/v1/transfers?state=CANCELLED"
    Then the response status is 200
    And the JSON array "items" has 1 element
    And the JSON field "items.0.id" is "{T3}"
    And the JSON field "items.0.state" is "CANCELLED"
    And the JSON field "items.0.reservationId" is absent
