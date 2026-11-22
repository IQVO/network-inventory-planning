# Derived from: docs/docs/adr/0015-rebalance-policy-and-lane-catalogue.md, the
# use-cases table (RunScheduledRebalance: a lane only when enabled and both ends
# are capable; need <= target; donor keeps safety stock; a refusal or an
# unreadable catalogue is a FAILED run, not an error; run_id =
# rebal-<id>-<startedAt>; PutRebalancePolicy / PutTransferLane: Validate, If-Match,
# an identical PUT writes nothing, lists ordered by key) and the troubleshooting
# table ("Scheduled runs are COMPLETED but proposalCount is always 0": an origin
# has no policy for the SKU, the destination's in-window demand is 0, or the
# origin's headroom minus its safety stock is <= 0; a positive rejectedCount
# means candidates cost more than their priority benefit; PUT 412 version-mismatch
# when the entry does not exist yet).
#
# The scheduled pass is observe-only: what it decided is visible only as the
# proposalCount and rejectedCount of its run row.
@bdd
Feature: Scheduled rebalance planning rules and catalogue writes
  As a network planner
  I want the scheduled pass to plan from the catalogues by the documented rules
  So that a pass never promises the same need or headroom twice and a catalogue write is safe

  Scenario: Two destinations are both served while the origin's headroom lasts
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 40 units of demand for SKU "SKU-1"
    And site "WH3" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And the policy catalogue holds, for site "WH1", policy "pv-wh1" for SKU "SKU-1" with safety stock 10, target stock 100 and unit priority 1
    And the policy catalogue holds, for site "WH2", policy "pv-wh2" for SKU "SKU-1" with safety stock 0, target stock 100 and unit priority 10
    And the policy catalogue holds, for site "WH3", policy "pv-wh3" for SKU "SKU-1" with safety stock 0, target stock 100 and unit priority 5
    And the lane catalogue holds an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    And the lane catalogue holds an enabled lane from "WH1" to "WH3" with lead time 24 hours and unit handling cost 1
    When the scheduled rebalance runs
    And I request GET "/v1/rebalance-runs"
    Then the JSON field "items.0.proposalCount" is 2
    And the JSON field "items.0.rejectedCount" is 0

  Scenario: One pass never promises the same headroom twice, and an exhausted budget is not a rejection
    Given site "WH1" is declared with capacity 90 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 40 units of demand for SKU "SKU-1"
    And site "WH3" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And the policy catalogue holds, for site "WH1", policy "pv-wh1" for SKU "SKU-1" with safety stock 10, target stock 100 and unit priority 1
    And the policy catalogue holds, for site "WH2", policy "pv-wh2" for SKU "SKU-1" with safety stock 0, target stock 100 and unit priority 10
    And the policy catalogue holds, for site "WH3", policy "pv-wh3" for SKU "SKU-1" with safety stock 0, target stock 100 and unit priority 5
    And the lane catalogue holds an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    And the lane catalogue holds an enabled lane from "WH1" to "WH3" with lead time 24 hours and unit handling cost 1
    When the scheduled rebalance runs
    And I request GET "/v1/rebalance-runs"
    Then the JSON field "items.0.outcome" is "COMPLETED"
    And the JSON field "items.0.proposalCount" is 1
    And the JSON field "items.0.rejectedCount" is 0

  Scenario Outline: The donor keeps its safety stock of the headroom
    Given site "WH1" is declared with capacity 90 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And the policy catalogue holds, for site "WH1", policy "pv-wh1" for SKU "SKU-1" with safety stock <safety>, target stock 100 and unit priority 1
    And the policy catalogue holds, for site "WH2", policy "pv-wh2" for SKU "SKU-1" with safety stock 0, target stock 100 and unit priority 10
    And the lane catalogue holds an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    When the scheduled rebalance runs
    And I request GET "/v1/rebalance-runs"
    Then the JSON field "items.0.proposalCount" is <proposals>
    And the JSON field "items.0.rejectedCount" is 0

    Examples: the origin headroom is 90 minus its 40 units of demand, so 50
      | safety | proposals | why                                  |
      | 49     | 1         | one unit of headroom is left to give |
      | 50     | 0         | the whole headroom is protected      |

  Scenario Outline: The destination is never planned above its target stock
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And the policy catalogue holds, for site "WH1", policy "pv-wh1" for SKU "SKU-1" with safety stock 10, target stock 100 and unit priority 1
    And the policy catalogue holds, for site "WH2", policy "pv-wh2" for SKU "SKU-1" with safety stock 0, target stock <target> and unit priority 10
    And the lane catalogue holds an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    When the scheduled rebalance runs
    And I request GET "/v1/rebalance-runs"
    Then the JSON field "items.0.proposalCount" is <proposals>

    Examples: the destination shows 10 units of in-window demand
      | target | proposals | why                                  |
      | 0      | 0         | a target of 0 means do not stock here |
      | 1      | 1         | one unit may still be planned        |

  Scenario Outline: A candidate nobody asked for is not proposed
    Given site "WH1" is declared with capacity <origin capacity> and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And the policy catalogue holds, for site "WH2", policy "pv-wh2" for SKU "<destination sku>" with safety stock 0, target stock 100 and unit priority 10
    And the lane catalogue holds an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    And the policy catalogue holds, for site "WH1", policy "pv-wh1" for SKU "<origin sku>" with safety stock 0, target stock 100 and unit priority 1
    When the scheduled rebalance runs
    And I request GET "/v1/rebalance-runs"
    Then the JSON field "items.0.outcome" is "COMPLETED"
    And the JSON field "items.0.proposalCount" is 0
    And the JSON field "items.0.rejectedCount" is 0

    Examples: the origin has no policy for the SKU, the destination has no demand for it, the origin has no headroom
      | origin capacity | origin sku | destination sku | why                                                 |
      | 500             | SKU-9      | SKU-1           | the origin is unmanaged for SKU-1 and never donates |
      | 500             | SKU-9      | SKU-9           | the destination shows no in-window demand for SKU-9 |
      | 30              | SKU-1      | SKU-1           | the origin demand exceeds its capacity              |

  Scenario: Each completed pass announces itself with a run id built from its row id and start time
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    When the scheduled rebalance runs
    Then the RebalanceRunCompleted run id is "rebal-1-20261006T150000Z"

  Scenario: An unreadable catalogue is a FAILED run that says so, not a silent empty COMPLETED one
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And the policy catalogue is unreadable
    When the scheduled rebalance runs
    And I request GET "/v1/rebalance-runs"
    Then the JSON field "items.0.outcome" is "FAILED"
    And the JSON field "items.0.failClosedReason" contains "load rebalance catalogue"
    And the JSON field "items.0.snapshotAsOf" is "2026-10-06T14:59:00Z"
    And the domain event "RebalanceRunCompleted" was not published

  Scenario Outline: An update changes the entry and moves its revision and update time, an identical PUT writes nothing
    Given the policy catalogue holds, for site "WH2", policy "pv-1" for SKU "SKU-1" with safety stock 5, target stock 50 and unit priority 10
    And 5 minutes pass
    When I send PUT "/v1/rebalance-policies" with this body:
      """
      {"Version":"<version>","Site":"WH2","SKU":"SKU-1","SafetyStock":5,"TargetStock":<target>,"UnitPriority":10}
      """
    Then the response status is 200
    And the JSON field "outcome" is "<outcome>"
    And the JSON field "revision" is <revision>
    And the JSON field "updatedAt" is "<updatedAt>"
    And the response header "ETag" is "<revision>"

    Examples: the stored entry is revision 1, written at 15:00
      | version | target | outcome   | revision | updatedAt            | why                                      |
      | pv-2    | 80     | UPDATED   | 2        | 2026-10-06T15:05:00Z | a changed entry is revision 2, just now  |
      | pv-1    | 50     | UNCHANGED | 1        | 2026-10-06T15:00:00Z | an identical PUT touches nothing         |

  Scenario Outline: An If-Match for an entry that does not exist yet is a 412 and creates nothing
    When I send PUT "<path>" with If-Match "1" and this body:
      """
      <body>
      """
    Then the response status is 412
    And the problem detail type is "version-mismatch"
    When I request GET "<path>"
    Then the JSON array "items" has 0 elements

    Examples: both catalogues
      | path                   | body                                                                                         |
      | /v1/rebalance-policies | {"Version":"pv-1","Site":"WH2","SKU":"SKU-1","SafetyStock":0,"TargetStock":10,"UnitPriority":1} |
      | /v1/transfer-lanes     | {"Origin":"WH1","Destination":"WH2","LeadTime":86400000000000,"UnitHandlingCost":1,"Enabled":true} |

  Scenario Outline: An entry the catalogue rules refuse is a 422 and nothing is stored
    When I send PUT "<path>" with this body:
      """
      <body>
      """
    Then the response status is 422
    And the problem detail type is "invalid-catalogue-entry"
    When I request GET "<path>"
    Then the JSON array "items" has 0 elements

    Examples: each rule of Policy.Validate and Lane.Validate
      | path                   | body                                                                                                        | why                          |
      | /v1/rebalance-policies | {"Version":"pv-1","Site":"WH2","SKU":"SKU-1","SafetyStock":-1,"TargetStock":10,"UnitPriority":1}            | negative safety stock        |
      | /v1/rebalance-policies | {"Version":"pv-1","Site":"WH2","SKU":"SKU-1","SafetyStock":0,"TargetStock":10,"UnitPriority":-1}            | negative unit priority       |
      | /v1/rebalance-policies | {"Version":"pv-1","Site":"","SKU":"SKU-1","SafetyStock":0,"TargetStock":10,"UnitPriority":1}                | no site                      |
      | /v1/rebalance-policies | {"Version":"pv-1","Site":"WH2","SKU":"","SafetyStock":0,"TargetStock":10,"UnitPriority":1}                  | no SKU                       |
      | /v1/transfer-lanes     | {"Origin":"WH1","Destination":"WH2","LeadTime":-1,"UnitHandlingCost":1,"Enabled":true}                      | negative lead time           |
      | /v1/transfer-lanes     | {"Origin":"","Destination":"WH2","LeadTime":0,"UnitHandlingCost":1,"Enabled":true}                          | no origin                    |
      | /v1/transfer-lanes     | {"Origin":"WH1","Destination":"","LeadTime":0,"UnitHandlingCost":1,"Enabled":true}                          | no destination               |

  Scenario Outline: The limits themselves are valid entries
    When I send PUT "<path>" with this body:
      """
      <body>
      """
    Then the response status is 201
    And the JSON field "outcome" is "CREATED"

    Examples: the edges of the rules
      | path                   | body                                                                                      | why                                  |
      | /v1/rebalance-policies | {"Version":"pv-1","Site":"WH2","SKU":"SKU-1","SafetyStock":5,"TargetStock":5,"UnitPriority":0} | a target equal to the safety stock   |
      | /v1/rebalance-policies | {"Version":"pv-1","Site":"WH2","SKU":"SKU-1","SafetyStock":0,"TargetStock":0,"UnitPriority":0} | everything zero                      |
      | /v1/transfer-lanes     | {"Origin":"WH1","Destination":"WH2","LeadTime":0,"UnitHandlingCost":0,"Enabled":false}    | no lead time and no cost             |

  Scenario Outline: A catalogue write that is not a valid document is a 400
    When I send PUT "<path>" with this body:
      """
      <body>
      """
    Then the response status is 400
    And the problem detail type is "invalid-request"

    Examples: malformed requests
      | path                   | body                                                                                                       | why            |
      | /v1/rebalance-policies | {"Version":"pv-1","Site":"WH2","SKU":"SKU-1","SafetyStock":0,"TargetStock":10,"UnitPriority":1,"Extra":1}   | unknown field  |
      | /v1/rebalance-policies | {"Version":"pv-1","Site":                                                                                  | truncated JSON |
      | /v1/transfer-lanes     | {"Origin":"WH1","Destination":"WH2","LeadTime":0,"UnitHandlingCost":1,"Enabled":true,"Extra":1}            | unknown field  |
      | /v1/transfer-lanes     | not json at all                                                                                            | not JSON       |

  Scenario: The catalogues are listed ordered by key whatever the order they were written in
    Given the policy catalogue holds, for site "WH2", policy "pv-1" for SKU "SKU-2" with safety stock 0, target stock 10 and unit priority 1
    And the policy catalogue holds, for site "WH1", policy "pv-2" for SKU "SKU-9" with safety stock 0, target stock 10 and unit priority 1
    And the policy catalogue holds, for site "WH1", policy "pv-3" for SKU "SKU-1" with safety stock 0, target stock 10 and unit priority 1
    And the lane catalogue holds an enabled lane from "WH2" to "WH1" with lead time 24 hours and unit handling cost 1
    And the lane catalogue holds an enabled lane from "WH1" to "WH3" with lead time 24 hours and unit handling cost 1
    And the lane catalogue holds an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    When I request GET "/v1/rebalance-policies"
    Then the JSON array "items" has 3 elements
    And the JSON field "items.0.policy.Site" is "WH1"
    And the JSON field "items.0.policy.SKU" is "SKU-1"
    And the JSON field "items.1.policy.SKU" is "SKU-9"
    And the JSON field "items.2.policy.Site" is "WH2"
    When I request GET "/v1/transfer-lanes"
    Then the JSON array "items" has 3 elements
    And the JSON field "items.0.lane.Destination" is "WH2"
    And the JSON field "items.1.lane.Destination" is "WH3"
    And the JSON field "items.2.lane.Origin" is "WH2"
