# Derived from: apis/openapi.yaml (generateTransferProposals:
# POST /v1/transfer-proposals:generate) and .claude/rules/domain-model.md
# (Transfer Proposal aggregate: only known positions participate; source
# protection remains intact; a route must be allowed; quantity is positive and
# bounded by source surplus and destination deficit; score must be positive).
@bdd
Feature: Advisory transfer proposals from a declared planning snapshot
  As an inventory planner
  I want deterministic, explainable transfer proposals over a coherent snapshot
  So that I can see what could move between sites without anything being reserved

  Background:
    Given a planning snapshot as of "2026-10-06T15:00:00Z"

  Scenario: A proposal moves the destination's deficit when the origin has surplus
    Given site "WH1" has 100 available units of SKU "SKU-1"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-1" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" has 10 available units of SKU "SKU-1"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock 50 and unit priority 3
    And an enabled lane from "WH1" to "WH2" with lead time 48 hours and unit handling cost 1
    When I generate transfer proposals
    Then the response status is 200
    And the response content type is "application/json"
    And the JSON array "proposals" has 1 element
    And the JSON field "proposals.0.Origin" is "WH1"
    And the JSON field "proposals.0.Destination" is "WH2"
    And the JSON field "proposals.0.SKU" is "SKU-1"
    And the JSON field "proposals.0.Quantity" is 40
    And the JSON field "proposals.0.PolicyVersion" is "policy-v7"
    And the JSON field "proposals.0.PositionAsOf" is "2026-10-06T15:00:00Z"

  Scenario: A proposal explains itself with reason codes and a score breakdown
    Given site "WH1" has 100 available units of SKU "SKU-1"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-1" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" has 10 available units of SKU "SKU-1"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock 50 and unit priority 3
    And an enabled lane from "WH1" to "WH2" with lead time 48 hours and unit handling cost 1
    When I generate transfer proposals
    Then the response status is 200
    And the JSON field "proposals.0.Reasons" is ["DESTINATION_BELOW_TARGET","ORIGIN_ABOVE_SAFETY_STOCK","APPROVED_LANE"]
    And the JSON field "proposals.0.ScoreBreakdown.PriorityBenefit" is 120
    And the JSON field "proposals.0.ScoreBreakdown.HandlingPenalty" is 40
    And the JSON field "proposals.0.ScoreBreakdown.LeadTimePenalty" is 2

  Scenario Outline: The quantity is bounded by source surplus and destination deficit
    Given site "WH1" has <origin available> available units of SKU "SKU-1"
    And site "WH1" has <origin reserved> customer reservations of SKU "SKU-1"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-1" with safety stock <safety stock>, target stock 0 and unit priority 0
    And site "WH2" has <destination available> available units of SKU "SKU-1"
    And site "WH2" has <destination inbound> confirmed inbound units of SKU "SKU-1"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock <target stock> and unit priority 3
    And an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    When I generate transfer proposals
    Then the response status is 200
    And the JSON field "proposals.0.Quantity" is <quantity>

    Examples: the binding constraint changes
      | origin available | origin reserved | safety stock | destination available | destination inbound | target stock | quantity | constraint                          |
      | 100              | 0               | 20           | 10                    | 0                   | 50           | 40       | the destination deficit             |
      | 40               | 0               | 20           | 10                    | 0                   | 50           | 20       | the origin surplus above safety     |
      | 100              | 70              | 20           | 10                    | 0                   | 50           | 10       | customer reservations at the origin |
      | 100              | 0               | 20           | 10                    | 30                  | 50           | 10       | confirmed inbound at the destination |

  Scenario Outline: No proposal is made when the origin or the destination does not call for one
    Given site "WH1" has <origin available> available units of SKU "SKU-1"
    And site "WH1" has <origin committed> committed outbound units of SKU "SKU-1"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-1" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" has <destination available> available units of SKU "SKU-1"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock 50 and unit priority 3
    And an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    When I generate transfer proposals
    Then the response status is 200
    And the JSON array "proposals" has 0 elements

    Examples: nothing to rebalance
      | origin available | origin committed | destination available | reason                                      |
      | 20               | 0                | 10                    | the origin sits exactly at its safety stock |
      | 100              | 80               | 10                    | committed outbound consumes the surplus     |
      | 100              | 0                | 50                    | the destination is exactly at target        |
      | 100              | 0                | 60                    | the destination is above target             |

  Scenario Outline: A proposal needs an approved lane in the right direction
    Given site "WH1" has 100 available units of SKU "SKU-1"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-1" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" has 10 available units of SKU "SKU-1"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock 50 and unit priority 3
    And an <lane state> lane from "<lane origin>" to "<lane destination>" with lead time 24 hours and unit handling cost 1
    When I generate transfer proposals
    Then the response status is 200
    And the JSON array "proposals" has <proposals> elements

    Examples: lane rules
      | lane state | lane origin | lane destination | proposals | why                          |
      | enabled    | WH1         | WH2              | 1         | the approved lane exists     |
      | disabled   | WH1         | WH2              | 0         | the lane is disabled         |
      | enabled    | WH2         | WH1              | 0         | the lane runs the other way  |

  Scenario: Without any lane there is nothing to propose
    Given site "WH1" has 100 available units of SKU "SKU-1"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-1" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" has 10 available units of SKU "SKU-1"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock 50 and unit priority 3
    When I generate transfer proposals
    Then the response status is 200
    And the JSON array "proposals" has 0 elements

  Scenario: Only known positions participate
    Given site "WH1" has 100 available units of SKU "SKU-1"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-1" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock 50 and unit priority 3
    And an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    When I generate transfer proposals
    Then the response status is 200
    And the JSON array "proposals" has 0 elements

  Scenario: Source protection needs the origin's own policy
    Given site "WH1" has 100 available units of SKU "SKU-1"
    And site "WH2" has 10 available units of SKU "SKU-1"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock 50 and unit priority 3
    And an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    When I generate transfer proposals
    Then the response status is 200
    And the JSON array "proposals" has 0 elements

  Scenario: A proposal whose handling cost outweighs its benefit is not made
    Given site "WH1" has 100 available units of SKU "SKU-1"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-1" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" has 10 available units of SKU "SKU-1"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock 50 and unit priority 1
    And an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 5
    When I generate transfer proposals
    Then the response status is 200
    And the JSON array "proposals" has 0 elements

  Scenario: Proposals are ordered by score, best first
    Given site "WH1" has 100 available units of SKU "SKU-1"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-1" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" has 10 available units of SKU "SKU-1"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock 50 and unit priority 3
    And site "WH1" has 100 available units of SKU "SKU-2"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-2" with safety stock 10, target stock 0 and unit priority 0
    And site "WH2" has 0 available units of SKU "SKU-2"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-2" with safety stock 0, target stock 20 and unit priority 9
    And an enabled lane from "WH1" to "WH2" with lead time 48 hours and unit handling cost 1
    When I generate transfer proposals
    Then the response status is 200
    And the JSON array "proposals" has 2 elements
    And the JSON field "proposals.0.SKU" is "SKU-2"
    And the JSON field "proposals.1.SKU" is "SKU-1"

  Scenario: The same snapshot always yields the same proposals
    Given site "WH1" has 100 available units of SKU "SKU-1"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-1" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" has 10 available units of SKU "SKU-1"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock 50 and unit priority 3
    And an enabled lane from "WH1" to "WH2" with lead time 48 hours and unit handling cost 1
    When I generate transfer proposals
    Then I generate the same transfer proposals again and get an identical answer

  Scenario: Generating proposals is advisory: nothing is reserved, persisted or published
    Given site "WH1" has 100 available units of SKU "SKU-1"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-1" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" has 10 available units of SKU "SKU-1"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock 50 and unit priority 3
    And an enabled lane from "WH1" to "WH2" with lead time 48 hours and unit handling cost 1
    When I generate transfer proposals
    Then the response status is 200
    And no transfer exists
    And no domain events were published

  Scenario: A snapshot that mixes points in time is refused
    Given site "WH1" has 100 available units of SKU "SKU-1"
    And site "WH2" has 10 available units of SKU "SKU-1"
    And site "WH2" has its position of SKU "SKU-1" as of "2026-10-06T14:00:00Z"
    When I generate transfer proposals
    Then the response status is 422
    And the problem detail type is "invalid-planning-snapshot"

  Scenario: Generation works on an instance without a database
    Given this instance runs without a database
    And site "WH1" has 100 available units of SKU "SKU-1"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-1" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" has 10 available units of SKU "SKU-1"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock 50 and unit priority 3
    And an enabled lane from "WH1" to "WH2" with lead time 48 hours and unit handling cost 1
    When I generate transfer proposals
    Then the response status is 200
    And the JSON field "proposals.0.Quantity" is 40

  Scenario Outline: A request that is not a valid snapshot document is a 400
    When I send POST "/v1/transfer-proposals:generate" with this body:
      """
      <body>
      """
    Then the response status is 400
    And the problem detail type is "invalid-request"

    Examples: malformed requests
      | body                                                                          | why                  |
      | {"asOf": "2026-10-06T15:00:00Z", "positions":                                 | truncated JSON       |
      | not json at all                                                               | not JSON             |
      | {"asOf": "2026-10-06T15:00:00Z", "positions": [], "policies": [], "lanes": [], "surprise": 1} | unknown field |
      | {"asOf": "yesterday", "positions": [], "policies": [], "lanes": []}           | asOf is not a time   |
