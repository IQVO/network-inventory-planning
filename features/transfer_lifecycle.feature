# Derived from: apis/openapi.yaml (TransferState, ApproveTransferResponse,
# TransferView, getTransfer: GET /v1/transfers/{id}) and
# .claude/rules/domain-model.md (Transfer Plan: lifecycle transitions;
# TransferDispatched / TransferReceived / TransferReconciled are facts received
# from the owning contexts, never commands this context issues).
#
# After approval the saga is driven ONLY by facts from other contexts —
# inventory-storage's allocation replies and receipt/stow facts, and
# fulfillment-execution's pick/dispatch/arrival facts. They arrive over Kafka,
# so these scenarios hand them to the same use cases the consumers call, and
# read every outcome back through the REST API.
#
#   DRAFT -> PROPOSED -> APPROVED -> ALLOCATING -> ALLOCATED -> PICKED
#         -> IN_TRANSIT -> ARRIVED -> RECEIVED          (terminal)
#                       \-> UNFULFILLABLE               (terminal)
@bdd
Feature: Transfer saga state machine
  As an operator
  I want a transfer to move only along its legal path and refuse everything else
  So that the saga never contradicts what the owning contexts have done

  Background:
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"

  Scenario: inventory-storage allocating the stock makes the transfer ALLOCATED and releases the pick work
    Given an approved transfer "T1"
    When a "TransferStockAllocated" fact arrives for transfer "T1"
    Then the fact is applied
    And transfer "T1" is in state "ALLOCATED"
    And the TRANSFER_PICK work demand "{T1}:pick" for 5 units was released

  Scenario: The allocation is visible on the transfer
    Given a transfer "T1" in state "ALLOCATED"
    When I request GET "/v1/transfers/{T1}"
    Then the JSON field "state" is "ALLOCATED"
    And the JSON field "reservationId" is "res-1"
    And the JSON field "rejectionReason" is absent
    And the JSON field "pickedQuantity" is absent

  Scenario Outline: inventory-storage refusing the allocation makes the transfer UNFULFILLABLE with a closed reason
    Given an approved transfer "T1"
    When a "TransferStockAllocationRejected" fact with reason "<reason>" arrives for transfer "T1"
    Then the fact is applied
    And transfer "T1" is in state "UNFULFILLABLE"
    And the work demand "{T1}:pick" was not released
    When I request GET "/v1/transfers/{T1}"
    Then the JSON field "rejectionReason" is "<reason>"
    And the JSON field "reservationId" is absent

    Examples: the closed rejection vocabulary
      | reason                |
      | ORIGIN_SITE_UNKNOWN   |
      | INSUFFICIENT_USABLE   |
      | IDEMPOTENCY_CONFLICT  |

  Scenario Outline: Each fact moves the transfer exactly one step along its path
    Given a transfer "T1" in state "<from>"
    When a "<fact>" fact arrives for transfer "T1"
    Then the fact is applied
    And transfer "T1" is in state "<to>"

    Examples: the legal transitions
      | from       | fact                            | to            |
      | ALLOCATING | TransferStockAllocated          | ALLOCATED     |
      | ALLOCATING | TransferStockAllocationRejected | UNFULFILLABLE |
      | ALLOCATED  | TransferPicked                  | PICKED        |
      | PICKED     | TransferDispatched              | IN_TRANSIT    |
      | IN_TRANSIT | TransferReceiptStaged           | ARRIVED       |
      | IN_TRANSIT | TransferArrived                 | ARRIVED       |
      | ARRIVED    | TransferStockStowed             | RECEIVED      |

  Scenario: A transfer's whole life is recorded in an immutable, ordered audit trail
    Given a transfer "T1" in state "RECEIVED"
    When I request GET "/v1/transfers/{T1}"
    Then the response status is 200
    And the JSON field "state" is "RECEIVED"
    And the audit trail states read "DRAFT, PROPOSED, APPROVED, ALLOCATING, ALLOCATED, PICKED, IN_TRANSIT, ARRIVED, RECEIVED"
    And the audit trail events read "TransferDrafted, TransferProposed, TransferPlanApproved, TransferAllocationRequested, TransferStockAllocated, TransferPicked, TransferDispatched, TransferReceiptStaged, TransferStockStowed"
    And the audit trail is numbered consecutively from 1
    And the JSON field "version" is 9
    And the JSON field "pickedQuantity" is 5

  Scenario Outline: A fact that does not fit the current state is refused and changes nothing
    Given a transfer "T1" in state "<state>"
    When a "<fact>" fact arrives for transfer "T1"
    Then the fact is refused as an illegal transition
    And transfer "T1" is in state "<state>"

    Examples: out-of-order, duplicate and post-terminal facts
      | state         | fact                            |
      | ALLOCATING    | TransferPicked                  |
      | ALLOCATING    | TransferDispatched              |
      | ALLOCATING    | TransferStockStowed             |
      | ALLOCATED     | TransferStockAllocated          |
      | ALLOCATED     | TransferStockAllocationRejected |
      | ALLOCATED     | TransferDispatched              |
      | PICKED        | TransferStockAllocated          |
      | PICKED        | TransferReceiptStaged           |
      | IN_TRANSIT    | TransferPicked                  |
      | IN_TRANSIT    | TransferStockStowed             |
      | ARRIVED       | TransferDispatched              |
      | ARRIVED       | TransferArrived                 |
      | UNFULFILLABLE | TransferStockAllocated          |
      | UNFULFILLABLE | TransferPicked                  |
      | RECEIVED      | TransferPicked                  |
      | RECEIVED      | TransferReceiptStaged           |
      | RECEIVED      | TransferStockStowed             |

  Scenario: A short pick is recorded, and only what was picked is dispatched
    Given a transfer "T1" in state "ALLOCATED"
    When a "TransferPicked" fact for 3 units arrives for transfer "T1"
    Then the fact is applied
    And transfer "T1" is in state "PICKED"
    And the TRANSFER_DISPATCH work demand "{T1}:dispatch" for 3 units was released
    When I request GET "/v1/transfers/{T1}"
    Then the JSON field "quantity" is 5
    And the JSON field "pickedQuantity" is 3

  Scenario: A complete pick releases the dispatch leg for the full quantity
    Given a transfer "T1" in state "ALLOCATED"
    When a "TransferPicked" fact arrives for transfer "T1"
    Then the fact is applied
    And the TRANSFER_DISPATCH work demand "{T1}:dispatch" for 5 units was released

  Scenario Outline: A pick the allocation cannot account for is refused
    Given a transfer "T1" in state "ALLOCATED"
    When a "TransferPicked" fact for <units> units arrives for transfer "T1"
    Then the fact is refused as inconsistent with the transfer
    And transfer "T1" is in state "ALLOCATED"

    Examples: impossible picks
      | units | why                                 |
      | 0     | nothing was picked                  |
      | 6     | more than the 5 units allocated     |

  Scenario Outline: A stow fact that does not complete this transfer's line is refused
    Given a transfer "T1" in state "ARRIVED"
    When a "TransferStockStowed" fact for <units> units at site "<site>" arrives for transfer "T1"
    Then the fact is refused as inconsistent with the transfer
    And transfer "T1" is in state "ARRIVED"

    Examples: inconsistent stow facts
      | units | site | why                                |
      | 5     | WH9  | stowed at a site that is not the destination |
      | 0     | WH2  | nothing was stowed                 |

  Scenario Outline: A fact about an unknown transfer is skipped, never retried
    When a "<fact>" fact arrives for transfer "ghost"
    Then the fact is skipped because the transfer is unknown

    Examples: facts for a transfer this context never approved
      | fact                            |
      | TransferStockAllocated          |
      | TransferStockAllocationRejected |
      | TransferPicked                  |
      | TransferDispatched              |
      | TransferReceiptStaged           |
      | TransferStockStowed             |

  Scenario: An unconfigured dispatch leg does not undo a completed pick
    Given the dispatch work release is not configured
    And a transfer "T1" in state "ALLOCATED"
    When a "TransferPicked" fact arrives for transfer "T1"
    Then the fact is applied but the dispatch work cannot be released
    And transfer "T1" is in state "PICKED"
    And the work demand "{T1}:dispatch" was not released

  Scenario: Facts for one transfer never touch another
    Given an approved transfer "T1"
    And an approved transfer "T2"
    When a "TransferStockAllocated" fact arrives for transfer "T1"
    Then transfer "T1" is in state "ALLOCATED"
    And transfer "T2" is in state "ALLOCATING"
