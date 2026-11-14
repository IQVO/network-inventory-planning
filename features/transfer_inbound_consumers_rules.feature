# Derived from: aggregate canvas section 5 (Corrective policies), ADR 0013
# (poison replies are dead-lettered), ADR 0022/0023 (release fact), the
# use-cases table (ApplyTransfer* invariants) and troubleshooting ("A fact is
# ignored" / "A transfer reply or fact lands on ...dlq").
#
# The coverage-driven suite hands facts to the use cases. These scenarios send
# real CloudEvents 1.0 messages through the PRODUCTION consumers
# (TransferReplyConsumer on warehouse.inventory.events, TransferFactConsumer on
# warehouse.fulfillment.events), so the policy between the broker and the
# aggregate is pinned too:
#
#   a replay or an out-of-order message  -> logged, committed past
#   a message that can never apply        -> dead-lettered, committed past
#   a duplicate CloudEvents id            -> a no-op
@bdd
Feature: Inbound transfer replies and facts: skip, dead-letter, dedupe
  As an operator
  I want a replayed or out-of-order message skipped and a poison message set aside
  So that no message wedges the partition and no sibling can corrupt a transfer

  Background:
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"

  Scenario: A redelivered allocation reply is applied once
    Given an approved transfer "T1"
    When inventory-storage publishes event "E1" TransferStockAllocated for transfer "T1"
    Then the message is acknowledged without a dead letter
    And transfer "T1" is in state "ALLOCATED"
    When inventory-storage publishes event "E1" TransferStockAllocated for transfer "T1"
    Then the message is acknowledged without a dead letter
    And transfer "T1" is in state "ALLOCATED"
    And the domain event "WorkDemandReleased" was published 1 time
    And the domain event "TransferStateAdvanced" was published 5 times

  Scenario: A second allocation reply under a new event id is an illegal transition, skipped and never dead-lettered
    Given an approved transfer "T1"
    And inventory-storage publishes event "E1" TransferStockAllocated for transfer "T1"
    When inventory-storage publishes event "E2" TransferStockAllocated for transfer "T1"
    Then the message is acknowledged without a dead letter
    And transfer "T1" is in state "ALLOCATED"
    And the domain event "WorkDemandReleased" was published 1 time

  Scenario: The pick work is due the pick offset after the allocation reply, not after the approval
    Given an approved transfer "T1"
    And 5 minutes pass
    When inventory-storage publishes event "E1" TransferStockAllocated for transfer "T1"
    Then the TRANSFER_PICK work demand "{T1}:pick" was released at site "WH1" on path "transfer-pick-path" with a critical pull time of "2026-10-06T17:05:00Z"

  Scenario: The dispatch work is due the dispatch offset after the pick fact and carries the picked units
    Given a transfer "T1" in state "ALLOCATED"
    And 10 minutes pass
    When fulfillment-execution publishes event "F1" TransferPicked for transfer "T1" with "quantity" set to 3
    Then the message is acknowledged without a dead letter
    And transfer "T1" is in state "PICKED"
    And the TRANSFER_DISPATCH work demand "{T1}:dispatch" for 3 units was released
    And the TRANSFER_DISPATCH work demand "{T1}:dispatch" was released at site "WH1" on path "transfer-dispatch-path" with a critical pull time of "2026-10-06T18:11:00Z"

  Scenario Outline: An allocation reply that contradicts the transfer is dead-lettered and the transfer keeps waiting
    Given an approved transfer "T1"
    When inventory-storage publishes event "E1" TransferStockAllocated for transfer "T1" with "<field>" set to <value>
    Then the message is dead-lettered by the transfer reply consumer
    And transfer "T1" is in state "ALLOCATING"
    And the work demand "{T1}:pick" was not released

    Examples: refused allocation replies
      | field            | value                                                            | why                                    |
      | transfer_line_id | "trf-other:1"                                                    | another transfer's line                |
      | origin_site_id   | "WH9"                                                            | not the transfer's origin              |
      | sku              | "SKU-9"                                                          | not the transfer's SKU                 |
      | reservation_id   | ""                                                               | no reservation id                      |
      | expires_at       | "yesterday"                                                      | the expiry is not a time               |
      | expires_at       | "0001-01-01T00:00:00Z"                                           | no expiry                              |
      | allocations      | []                                                               | nothing was allocated                  |
      | allocations      | [{"stock_unit_id":"su-1","bin_id":"bin-A1","quantity":4}]        | allocations total 4, not 5             |
      | allocations      | [{"stock_unit_id":"su-1","bin_id":"bin-A1","quantity":6}]        | allocations total 6, not 5             |
      | allocations      | [{"stock_unit_id":"","bin_id":"bin-A1","quantity":5}]            | an allocation without a stock unit     |
      | allocations      | [{"stock_unit_id":"su-1","bin_id":"","quantity":5}]              | an allocation without a bin            |
      | allocations      | [{"stock_unit_id":"su-1","bin_id":"bin-A1","quantity":0},{"stock_unit_id":"su-2","bin_id":"bin-A2","quantity":5}] | a zero-quantity allocation |

  Scenario: Allocations split over several bins are accepted when they add up to the transfer quantity
    Given an approved transfer "T1"
    When inventory-storage publishes event "E1" TransferStockAllocated for transfer "T1" with "allocations" set to [{"stock_unit_id":"su-1","bin_id":"bin-A1","quantity":2},{"stock_unit_id":"su-2","bin_id":"bin-A2","quantity":3}]
    Then the message is acknowledged without a dead letter
    And transfer "T1" is in state "ALLOCATED"

  Scenario Outline: A rejection that contradicts the transfer or the closed vocabulary is dead-lettered
    Given an approved transfer "T1"
    When inventory-storage publishes event "E1" TransferStockAllocationRejected for transfer "T1" with "<field>" set to <value>
    Then the message is dead-lettered by the transfer reply consumer
    And transfer "T1" is in state "ALLOCATING"

    Examples: refused rejections
      | field            | value          | why                          |
      | reason           | "OUT_OF_STOCK" | not in the closed vocabulary |
      | reason           | ""             | no reason                    |
      | transfer_line_id | "trf-other:1"  | another transfer's line      |

  Scenario Outline: A pick fact the allocation cannot account for is dead-lettered
    Given a transfer "T1" in state "ALLOCATED"
    When fulfillment-execution publishes event "F1" TransferPicked for transfer "T1" with "quantity" set to <units>
    Then the message is dead-lettered by the transfer fact consumer
    And transfer "T1" is in state "ALLOCATED"
    And the work demand "{T1}:dispatch" was not released

    Examples: impossible picks
      | units | why                              |
      | 0     | nothing was picked               |
      | -2    | a negative quantity              |
      | 6     | more than the 5 units allocated  |

  Scenario Outline: A destination fact that contradicts the transfer is dead-lettered
    Given a transfer "T1" in state "ARRIVED"
    When inventory-storage publishes event "S1" TransferStockStowed for transfer "T1" with "<field>" set to <value>
    Then the message is dead-lettered by the transfer reply consumer
    And transfer "T1" is in state "ARRIVED"

    Examples: refused stow facts
      | field               | value                                                    | why                           |
      | destination_site_id | "WH9"                                                    | not the destination           |
      | sku                 | "SKU-9"                                                  | not the transfer's SKU        |
      | transfer_line_id    | "trf-other:1"                                            | another transfer's line       |
      | stowed_quantity     | 0                                                        | nothing was stowed            |
      | allocations         | []                                                       | no stow allocation            |

  Scenario Outline: A release fact that does not match the transfer it claims to settle is dead-lettered
    Given a transfer "T1" in state "RELEASING"
    When inventory-storage publishes event "R1" TransferStockAllocationReleased for transfer "T1" with "<field>" set to <value>
    Then the message is dead-lettered by the transfer reply consumer
    And transfer "T1" is in state "RELEASING"
    When I request GET "/v1/transfers/{T1}"
    Then the JSON field "reservationId" is "res-1"

    Examples: refused release facts
      | field            | value         | why                                    |
      | outcome          | "LOST"        | outside RELEASED / ALREADY_RESOLVED    |
      | reservation_id   | "res-9"       | another reservation                    |
      | origin_site_id   | "WH9"         | not the transfer's origin              |
      | sku              | "SKU-9"       | not the transfer's SKU                 |
      | quantity         | 4             | not the transfer's quantity            |
      | transfer_line_id | "trf-other:1" | another transfer's line                |

  Scenario Outline: Either release outcome settles the cancellation and lets go of the reservation
    Given a transfer "T1" in state "RELEASING"
    When inventory-storage publishes event "R1" TransferStockAllocationReleased for transfer "T1" with "outcome" set to "<outcome>"
    Then the message is acknowledged without a dead letter
    And transfer "T1" is in state "CANCELLED"
    When I request GET "/v1/transfers/{T1}"
    Then the JSON field "reservationId" is absent

    Examples: the closed outcome set
      | outcome          | why                                  |
      | RELEASED         | inventory-storage released the hold  |
      | ALREADY_RESOLVED | the reservation had already resolved |

  Scenario Outline: A message for a transfer this context never approved is skipped, not dead-lettered
    When <publisher> publishes event "X1" <message> for transfer "trf-ghost"
    Then the message is acknowledged without a dead letter

    Examples: every consumed message kind
      | publisher             | message                         |
      | inventory-storage     | TransferStockAllocated          |
      | inventory-storage     | TransferStockAllocationRejected |
      | inventory-storage     | TransferStockAllocationReleased |
      | inventory-storage     | TransferReceiptStaged           |
      | inventory-storage     | TransferStockStowed             |
      | fulfillment-execution | TransferPicked                  |
      | fulfillment-execution | TransferDispatched              |
      | fulfillment-execution | TransferArrived                 |

  Scenario Outline: A message that arrives out of order is skipped, never dead-lettered, and changes nothing
    Given a transfer "T1" in state "<state>"
    When <publisher> publishes event "X1" <message> for transfer "T1"
    Then the message is acknowledged without a dead letter
    And transfer "T1" is in state "<state>"

    Examples: messages the saga is not waiting for
      | state      | publisher             | message               |
      | ALLOCATED  | inventory-storage     | TransferReceiptStaged |
      | ALLOCATED  | fulfillment-execution | TransferDispatched    |
      | PICKED     | fulfillment-execution | TransferArrived       |
      | IN_TRANSIT | inventory-storage     | TransferStockStowed   |
      | RECEIVED   | fulfillment-execution | TransferPicked        |

  Scenario: Messages this context does not consume are ignored
    When the inventory topic carries the CloudEvent type "com.warehouse.wms.inventory-storage.stock.StockUnitRegistered"
    Then the message is acknowledged without a dead letter
    When the fulfillment topic carries the CloudEvent type "com.warehouse.wes.fulfillment-execution.task.TaskCompleted"
    Then the message is acknowledged without a dead letter

  Scenario: A message that is not a CloudEvents 1.0 event is skipped, never retried or dead-lettered
    When the inventory topic carries this raw message:
      """
      {"event_type":"legacy","transfer_id":"trf-1"}
      """
    Then the message is acknowledged without a dead letter
    When the fulfillment topic carries this raw message:
      """
      not json at all
      """
    Then the message is acknowledged without a dead letter

  Scenario: A poison message does not hold up the messages behind it
    Given an approved transfer "T1"
    And an approved transfer "T2"
    And inventory-storage publishes event "E1" TransferStockAllocated for transfer "T1" with "reservation_id" set to ""
    And the message is dead-lettered by the transfer reply consumer
    When inventory-storage publishes event "E2" TransferStockAllocated for transfer "T2"
    Then the message is acknowledged without a dead letter
    And transfer "T2" is in state "ALLOCATED"
    And transfer "T1" is in state "ALLOCATING"

  Scenario: A redelivered pick fact releases the dispatch work once
    Given a transfer "T1" in state "ALLOCATED"
    And fulfillment-execution publishes event "F1" TransferPicked for transfer "T1" with "quantity" set to 4
    When fulfillment-execution publishes event "F1" TransferPicked for transfer "T1" with "quantity" set to 4
    Then the message is acknowledged without a dead letter
    And transfer "T1" is in state "PICKED"
    And the domain event "WorkDemandReleased" was published 2 times

  Scenario: A pick fact on a deployment without a dispatch path commits PICKED and is skipped, not dead-lettered
    Given the dispatch work release is not configured
    And a transfer "T1" in state "ALLOCATED"
    When fulfillment-execution publishes event "F1" TransferPicked for transfer "T1"
    Then the message is acknowledged without a dead letter
    And transfer "T1" is in state "PICKED"
    And the work demand "{T1}:dispatch" was not released

  Scenario Outline: The first arrival fact wins and the other one is skipped
    Given a transfer "T1" in state "IN_TRANSIT"
    And <first publisher> publishes event "A1" <first> for transfer "T1"
    When <second publisher> publishes event "A2" <second> for transfer "T1"
    Then the message is acknowledged without a dead letter
    And transfer "T1" is in state "ARRIVED"
    And transfer "T1" entered "ARRIVED" exactly once, caused by the event "<first>"

    Examples: both arrival paths, in both orders
      | first publisher       | first                 | second publisher      | second                |
      | fulfillment-execution | TransferArrived       | inventory-storage     | TransferReceiptStaged |
      | inventory-storage     | TransferReceiptStaged | fulfillment-execution | TransferArrived       |
