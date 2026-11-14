# Derived from: docs/docs/adr/0017-case-multiple-transfers.md (case-multiple
# transfers). A SKU with an ACTIVE CASE level moves in whole cases:
#   1. the requested quantity is rounded to the NEAREST multiple of the case
#      size, a tie going UP;
#   2. never fewer than one case while the demand is at least 1;
#   3. never more than the origin can supply (floor(available / case) cases);
#      an origin below one case moves nothing: the planner counts a rejection,
#      approval answers 422 `below-one-case`;
#   4. the demand is never silently dropped: the plan carries caseMultiple,
#      cases, requestedQtyEaches, roundedQtyEaches and a SIGNED remainderEaches
#      (requested minus rounded);
#   5. a SKU without an active CASE level is planned exactly as before;
#   6. an unreadable pack view is fail-closed: 503 `pack-levels-unavailable`
#      on proposal generation and on approval.
# The case level is "in force" from its effective date, inclusive, until its
# end date, exclusive.
@bdd
Feature: Case-multiple transfers
  As an inventory planner
  I want a SKU sold in cases to move in whole cases and the unserved demand to stay visible
  So that the origin never ships a loose partial case and no demand is lost silently

  Scenario Outline: A proposal for a case SKU is rounded to whole cases and reports the signed remainder
    Given a planning snapshot as of "2026-10-06T15:00:00Z"
    And SKU "SKU-1" is packed in cases of 12
    And site "WH1" has <origin available> available units of SKU "SKU-1"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-1" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" has 10 available units of SKU "SKU-1"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock <target> and unit priority 3
    And an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    When I generate transfer proposals
    Then the response status is 200
    And the JSON field "proposals.0.Quantity" is <quantity>
    And the JSON field "proposals.0.caseMultiple" is 12
    And the JSON field "proposals.0.cases" is <cases>
    And the JSON field "proposals.0.requestedQtyEaches" is <requested>
    And the JSON field "proposals.0.roundedQtyEaches" is <quantity>
    And the JSON field "proposals.0.remainderEaches" is <remainder>

    Examples: the rounding rule (the destination holds 10, so the demand is the target minus 10)
      | origin available | target | requested | quantity | cases | remainder | rule                                            |
      | 100              | 27     | 17        | 12       | 1     | 5         | below the half case rounds down                 |
      | 100              | 28     | 18        | 24       | 2     | -6        | exactly half a case rounds UP                   |
      | 100              | 15     | 5         | 12       | 1     | -7        | never fewer than one case                       |
      | 100              | 34     | 24        | 24       | 2     | 0         | an exact multiple has no remainder              |
      | 100              | 50     | 40        | 36       | 3     | 4         | 40 is 3 cases and 4 eaches                      |
      | 50               | 50     | 40        | 24       | 2     | 16        | the origin surplus of 30 allows only 2 cases    |

  Scenario Outline: An origin that cannot supply one whole case proposes nothing
    Given a planning snapshot as of "2026-10-06T15:00:00Z"
    And SKU "SKU-1" is packed in cases of 12
    And site "WH1" has <origin available> available units of SKU "SKU-1"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-1" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" has 10 available units of SKU "SKU-1"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock 50 and unit priority 3
    And an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    When I generate transfer proposals
    Then the response status is 200
    And the JSON array "proposals" has <proposals> elements

    Examples: the surplus above the safety stock of 20 against one case of 12
      | origin available | proposals | surplus                       |
      | 31               | 0         | 11 eaches: not one whole case |
      | 32               | 1         | 12 eaches: exactly one case   |

  Scenario: A SKU without a case level is planned in eaches and carries no case fields
    Given a planning snapshot as of "2026-10-06T15:00:00Z"
    And SKU "SKU-1" is packed in cases of 12
    And site "WH1" has 100 available units of SKU "SKU-2"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-2" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" has 10 available units of SKU "SKU-2"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-2" with safety stock 0, target stock 27 and unit priority 3
    And an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    When I generate transfer proposals
    Then the response status is 200
    And the JSON field "proposals.0.Quantity" is 17
    And the JSON field "proposals.0.caseMultiple" is absent
    And the JSON field "proposals.0.remainderEaches" is absent

  Scenario Outline: A pack level of fewer than 2 eaches is not a case, a level of 2 is
    Given a planning snapshot as of "2026-10-06T15:00:00Z"
    And SKU "SKU-1" is packed in cases of <per case>
    And site "WH1" has 100 available units of SKU "SKU-1"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-1" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" has 10 available units of SKU "SKU-1"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock 25 and unit priority 3
    And an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    When I generate transfer proposals
    Then the response status is 200
    And the JSON field "proposals.0.Quantity" is <quantity>
    And the JSON field "proposals.0.caseMultiple" is <multiple>

    Examples: a demand of 15 eaches
      | per case | quantity | multiple | rule                                              |
      | 1        | 15       | absent   | one each per pack is no case: planned in eaches   |
      | 2        | 16       | 2        | two eaches per pack is the smallest real case     |

  Scenario Outline: A case level counts from its effective date, inclusive, until its end date, exclusive
    Given a planning snapshot as of "2026-10-06T15:00:00Z"
    And SKU "SKU-1" is packed in cases of 12 <validity>
    And site "WH1" has 100 available units of SKU "SKU-1"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-1" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" has 10 available units of SKU "SKU-1"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock 27 and unit priority 3
    And an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    When I generate transfer proposals
    Then the response status is 200
    And the JSON field "proposals.0.Quantity" is <quantity>

    Examples: the snapshot is as of 2026-10-06T15:00:00Z and the demand is 17 eaches
      | validity                                                  | quantity | why                                      |
      | from "2026-10-06T15:00:00Z"                               | 12       | effective exactly at the as-of           |
      | from "2026-10-06T15:00:01Z"                               | 17       | not effective yet                        |
      | from "2026-10-01T00:00:00Z" until "2026-10-06T15:00:01Z"  | 12       | still in force one second before its end |
      | from "2026-10-01T00:00:00Z" until "2026-10-06T15:00:00Z"  | 17       | ended exactly at the as-of               |

  Scenario: An unreadable pack view refuses proposal generation instead of planning in eaches
    Given a planning snapshot as of "2026-10-06T15:00:00Z"
    And the pack-level view is unavailable
    And site "WH1" has 100 available units of SKU "SKU-1"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-1" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" has 10 available units of SKU "SKU-1"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock 27 and unit priority 3
    And an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    When I generate transfer proposals
    Then the response status is 503
    And the problem detail type is "pack-levels-unavailable"

  Scenario: Approving a case SKU moves whole cases and keeps the unserved demand visible
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And SKU "SKU-1" is packed in cases of 12
    When I approve a transfer of 17 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 200
    And the JSON field "quantity" is 12
    And the JSON field "caseMultiple" is 12
    And the JSON field "cases" is 1
    And the JSON field "requestedQtyEaches" is 17
    And the JSON field "roundedQtyEaches" is 12
    And the JSON field "remainderEaches" is 5
    And the TransferPlanApproved event carries 12 units as 1 case of 12 with 17 requested and a remainder of 5
    And the allocation request asks inventory-storage for 12 units of SKU "SKU-1" at site "WH1"
    When I request GET "/v1/transfers/{T1}"
    Then the JSON field "quantity" is 12
    And the JSON field "cases" is 1
    And the JSON field "remainderEaches" is 5

  Scenario Outline: Approval rounds the request the same way the planner does
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And SKU "SKU-1" is packed in cases of 12
    When I approve a transfer of <requested> units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 200
    And the JSON field "quantity" is <quantity>
    And the JSON field "cases" is <cases>
    And the JSON field "remainderEaches" is <remainder>

    Examples: nearest case, ties up, never below one case
      | requested | quantity | cases | remainder |
      | 18        | 24       | 2     | -6        |
      | 5         | 12       | 1     | -7        |
      | 24        | 24       | 2     | 0         |
      | 29        | 24       | 2     | 5         |

  Scenario: Approval never asks the origin for more than it can send, and reports what was left
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And SKU "SKU-1" is packed in cases of 12
    When I approve a transfer of 470 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 200
    And the JSON field "quantity" is 456
    And the JSON field "cases" is 38
    And the JSON field "remainderEaches" is 14

  Scenario Outline: An origin that cannot send one whole case is refused with below-one-case
    Given site "WH1" is declared with capacity <capacity> and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And SKU "SKU-1" is packed in cases of 12
    When I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is <status>
    And the JSON field "<field>" is "<value>"

    Examples: the origin headroom is the capacity minus its 40 units of demand
      | capacity | status | field | value                                              | headroom                      |
      | 51       | 422    | type  | https://warehouse.example/problems/below-one-case  | 11 eaches: not one whole case |
      | 52       | 200    | state | ALLOCATING                                         | 12 eaches: exactly one case   |

  Scenario: A refused below-one-case approval creates nothing
    Given site "WH1" is declared with capacity 51 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And SKU "SKU-1" is packed in cases of 12
    When I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the problem detail type is "below-one-case"
    And no transfer exists
    And no domain events were published

  Scenario: A SKU without a case level is approved exactly as requested
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And SKU "SKU-2" is packed in cases of 12
    When I approve a transfer of 17 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 200
    And the JSON field "quantity" is 17
    And the JSON field "caseMultiple" is absent
    And the JSON field "remainderEaches" is absent
    And the TransferPlanApproved event carries 17 units and no case rounding

  Scenario: The pick work of a case transfer is released for the rounded quantity
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And SKU "SKU-1" is packed in cases of 12
    And an approved transfer "T1" of 17 units of SKU "SKU-1" from "WH1" to "WH2"
    When a "TransferStockAllocated" fact arrives for transfer "T1"
    Then the fact is applied
    And the TRANSFER_PICK work demand "{T1}:pick" for 12 units was released

  Scenario: Retiring the case level changes later approvals but never an approved transfer
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And SKU "SKU-1" is packed in cases of 12
    And an approved transfer "T1" of 17 units of SKU "SKU-1" from "WH1" to "WH2"
    And SKU "SKU-1" has its case level retired
    When I approve a transfer of 17 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T2"
    Then the response status is 200
    And the JSON field "quantity" is 17
    And the JSON field "caseMultiple" is absent
    When I request GET "/v1/transfers/{T1}"
    Then the JSON field "quantity" is 12
    And the JSON field "caseMultiple" is 12

  Scenario: An unreadable pack view refuses approval instead of approving in eaches
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And the pack-level view is unavailable
    When I approve a transfer of 17 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 503
    And the problem detail type is "pack-levels-unavailable"
    And no transfer exists
    And no domain events were published

  Scenario: A scheduled pass plans a case SKU in whole cases
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And SKU "SKU-1" is packed in cases of 12
    And the policy catalogue holds, for site "WH1", policy "pv-wh1" for SKU "SKU-1" with safety stock 10, target stock 100 and unit priority 1
    And the policy catalogue holds, for site "WH2", policy "pv-wh2" for SKU "SKU-1" with safety stock 0, target stock 100 and unit priority 10
    And the lane catalogue holds an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    When the scheduled rebalance runs
    And I request GET "/v1/rebalance-runs"
    Then the JSON field "items.0.outcome" is "COMPLETED"
    And the JSON field "items.0.proposalCount" is 1
    And the JSON field "items.0.rejectedCount" is 0

  Scenario: A scheduled pass counts a donor below one case as a rejection, not as an exhausted budget
    Given site "WH1" is declared with capacity 50 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And SKU "SKU-1" is packed in cases of 12
    And the policy catalogue holds, for site "WH1", policy "pv-wh1" for SKU "SKU-1" with safety stock 0, target stock 100 and unit priority 1
    And the policy catalogue holds, for site "WH2", policy "pv-wh2" for SKU "SKU-1" with safety stock 0, target stock 100 and unit priority 10
    And the lane catalogue holds an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    When the scheduled rebalance runs
    And I request GET "/v1/rebalance-runs"
    Then the JSON field "items.0.outcome" is "COMPLETED"
    And the JSON field "items.0.proposalCount" is 0
    And the JSON field "items.0.rejectedCount" is 1

  Scenario: A scheduled pass with an unreadable pack view is a FAILED run, not a plan in eaches
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And the policy catalogue holds, for site "WH1", policy "pv-wh1" for SKU "SKU-1" with safety stock 10, target stock 100 and unit priority 1
    And the policy catalogue holds, for site "WH2", policy "pv-wh2" for SKU "SKU-1" with safety stock 0, target stock 100 and unit priority 10
    And the lane catalogue holds an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    And the pack-level view is unavailable
    When the scheduled rebalance runs
    And I request GET "/v1/rebalance-runs"
    Then the JSON field "items.0.outcome" is "FAILED"
    And the JSON field "items.0.failClosedReason" contains "read pack levels"
    And the domain event "RebalanceRunCompleted" was not published
