# Derived from: apis/openapi.yaml (putRebalancePolicy / listRebalancePolicies:
# PUT|GET /v1/rebalance-policies; putTransferLane / listTransferLanes:
# PUT|GET /v1/transfer-lanes; listRebalanceRuns) and ADR 0015 (policy and
# lane catalogue).
#
# NIP owns the operator-managed Rebalance Policy (per site + SKU) and
# Transfer Lane (per directed origin → destination) catalogues. The bodies
# are the EXISTING Policy / Lane shapes. PUT is idempotent with optimistic
# concurrency (ETag / If-Match). The scheduled rebalance pass plans from the
# catalogues — still observe-only — and uses a lane only when site_capability
# lets its origin send and its destination receive.
@bdd
Feature: Rebalance policy and transfer lane catalogues
  As a network planner
  I want to manage rebalance policies and transfer lanes
  So that the scheduled rebalance pass proposes transfers instead of nothing

  Scenario: The scheduled pass proposes a transfer from the catalogues
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And the policy catalogue holds, for site "WH1", policy "pv-wh1" for SKU "SKU-1" with safety stock 10, target stock 100 and unit priority 1
    And the policy catalogue holds, for site "WH2", policy "pv-wh2" for SKU "SKU-1" with safety stock 0, target stock 100 and unit priority 10
    And the lane catalogue holds an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    When the scheduled rebalance runs
    And I request GET "/v1/rebalance-runs"
    Then the JSON field "runs.0.outcome" is "COMPLETED"
    And the JSON field "runs.0.proposalCount" is 1
    And the JSON field "runs.0.rejectedCount" is 0
    And the domain event "RebalanceRunCompleted" was published 1 time

  Scenario: A pass that proposes still approves, allocates and releases nothing
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And the policy catalogue holds, for site "WH1", policy "pv-wh1" for SKU "SKU-1" with safety stock 10, target stock 100 and unit priority 1
    And the policy catalogue holds, for site "WH2", policy "pv-wh2" for SKU "SKU-1" with safety stock 0, target stock 100 and unit priority 10
    And the lane catalogue holds an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    When the scheduled rebalance runs
    Then no transfer exists
    And the domain event "TransferPlanApproved" was not published
    And the domain event "TransferAllocationRequested" was not published
    And the domain event "WorkDemandReleased" was not published

  Scenario: A lane to a site without complete capability facts is never used
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And the policy catalogue holds, for site "WH1", policy "pv-wh1" for SKU "SKU-1" with safety stock 10, target stock 100 and unit priority 1
    And the policy catalogue holds, for site "WH3", policy "pv-wh3" for SKU "SKU-1" with safety stock 0, target stock 100 and unit priority 10
    And the lane catalogue holds an enabled lane from "WH1" to "WH3" with lead time 24 hours and unit handling cost 1
    When the scheduled rebalance runs
    And I request GET "/v1/rebalance-runs"
    Then the JSON field "runs.0.outcome" is "COMPLETED"
    And the JSON field "runs.0.proposalCount" is 0

  Scenario Outline: A site capability that forbids a direction fails the pass closed
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And site "<site>" cannot <direction> transfers
    And the policy catalogue holds, for site "WH1", policy "pv-wh1" for SKU "SKU-1" with safety stock 10, target stock 100 and unit priority 1
    And the policy catalogue holds, for site "WH2", policy "pv-wh2" for SKU "SKU-1" with safety stock 0, target stock 100 and unit priority 10
    And the lane catalogue holds an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    When the scheduled rebalance runs
    And I request GET "/v1/rebalance-runs"
    Then the JSON field "runs.0.outcome" is "FAILED"
    And the JSON field "runs.0.failClosedReason" contains "<reason>"
    And the JSON field "runs.0.proposalCount" is 0
    And the domain event "RebalanceRunCompleted" was not published

    Examples: capability gates
      | site | direction | reason                                  |
      | WH1  | send      | WH1 is disabled as a transfer origin      |
      | WH2  | receive   | WH2 is disabled as a transfer destination |

  Scenario: A disabled lane is never used
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And the policy catalogue holds, for site "WH1", policy "pv-wh1" for SKU "SKU-1" with safety stock 10, target stock 100 and unit priority 1
    And the policy catalogue holds, for site "WH2", policy "pv-wh2" for SKU "SKU-1" with safety stock 0, target stock 100 and unit priority 10
    And the lane catalogue holds a disabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    When the scheduled rebalance runs
    And I request GET "/v1/rebalance-runs"
    Then the JSON field "runs.0.proposalCount" is 0

  Scenario: A candidate whose cost outweighs its benefit is counted as rejected
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And the policy catalogue holds, for site "WH1", policy "pv-wh1" for SKU "SKU-1" with safety stock 10, target stock 100 and unit priority 1
    And the policy catalogue holds, for site "WH2", policy "pv-wh2" for SKU "SKU-1" with safety stock 0, target stock 100 and unit priority 1
    And the lane catalogue holds an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 5
    When the scheduled rebalance runs
    And I request GET "/v1/rebalance-runs"
    Then the JSON field "runs.0.outcome" is "COMPLETED"
    And the JSON field "runs.0.proposalCount" is 0
    And the JSON field "runs.0.rejectedCount" is 1

  Scenario: Creating a policy answers 201 with revision 1 as its ETag
    When I send PUT "/v1/rebalance-policies" with this body:
      """
      {"Version":"pv-1","Site":"WH2","SKU":"SKU-1","SafetyStock":5,"TargetStock":50,"UnitPriority":10}
      """
    Then the response status is 201
    And the response header "ETag" is "1"
    And the JSON field "outcome" is "CREATED"
    And the JSON field "revision" is 1
    And the JSON field "policy.TargetStock" is 50
    And the JSON field "updatedAt" is "2026-10-06T15:00:00Z"

  Scenario: Replaying the same PUT is an idempotent no-op
    Given the policy catalogue holds, for site "WH2", policy "pv-1" for SKU "SKU-1" with safety stock 5, target stock 50 and unit priority 10
    When I send PUT "/v1/rebalance-policies" with this body:
      """
      {"Version":"pv-1","Site":"WH2","SKU":"SKU-1","SafetyStock":5,"TargetStock":50,"UnitPriority":10}
      """
    Then the response status is 200
    And the JSON field "outcome" is "UNCHANGED"
    And the JSON field "revision" is 1

  Scenario: A stale If-Match is refused with 412
    Given the policy catalogue holds, for site "WH2", policy "pv-1" for SKU "SKU-1" with safety stock 5, target stock 50 and unit priority 10
    When I send PUT "/v1/rebalance-policies" with If-Match "1" and this body:
      """
      {"Version":"pv-2","Site":"WH2","SKU":"SKU-1","SafetyStock":5,"TargetStock":80,"UnitPriority":10}
      """
    Then the response status is 200
    And the response header "ETag" is "2"
    When I send PUT "/v1/rebalance-policies" with If-Match "1" and this body:
      """
      {"Version":"pv-3","Site":"WH2","SKU":"SKU-1","SafetyStock":5,"TargetStock":90,"UnitPriority":10}
      """
    Then the response status is 412
    And the problem detail type is "version-mismatch"
    When I request GET "/v1/rebalance-policies"
    Then the JSON array "policies" has 1 element
    And the JSON field "policies.0.policy.TargetStock" is 80
    And the JSON field "policies.0.revision" is 2

  Scenario: A lane update under If-Match is guarded by its revision
    When I send PUT "/v1/transfer-lanes" with this body:
      """
      {"Origin":"WH1","Destination":"WH2","LeadTime":86400000000000,"UnitHandlingCost":1,"Enabled":true}
      """
    Then the response status is 201
    And the response header "ETag" is "1"
    And the JSON field "outcome" is "CREATED"
    When I send PUT "/v1/transfer-lanes" with If-Match "1" and this body:
      """
      {"Origin":"WH1","Destination":"WH2","LeadTime":172800000000000,"UnitHandlingCost":2,"Enabled":true}
      """
    Then the response status is 200
    And the response header "ETag" is "2"
    And the JSON field "outcome" is "UPDATED"
    When I send PUT "/v1/transfer-lanes" with If-Match "1" and this body:
      """
      {"Origin":"WH1","Destination":"WH2","LeadTime":259200000000000,"UnitHandlingCost":3,"Enabled":true}
      """
    Then the response status is 412
    And the problem detail type is "version-mismatch"
    When I request GET "/v1/transfer-lanes"
    Then the JSON array "lanes" has 1 element
    And the JSON field "lanes.0.lane.LeadTime" is 172800000000000
    And the JSON field "lanes.0.lane.UnitHandlingCost" is 2
    And the JSON field "lanes.0.revision" is 2

  Scenario Outline: A malformed If-Match is refused with 400 and applied nothing
    When I send PUT "<path>" with If-Match <ifMatch> and this body:
      """
      <body>
      """
    Then the response status is 400
    And the problem detail type is "invalid-if-match"
    When I request GET "<path>"
    Then the JSON array "<list>" has 0 elements

    Examples: malformed If-Match values
      | path                   | ifMatch | list     | body                                                                                                       | why                    |
      | /v1/rebalance-policies | abc     | policies | {"Version":"pv-1","Site":"WH2","SKU":"SKU-1","SafetyStock":0,"TargetStock":10,"UnitPriority":1}              | not a revision         |
      | /v1/rebalance-policies | 0       | policies | {"Version":"pv-1","Site":"WH2","SKU":"SKU-1","SafetyStock":0,"TargetStock":10,"UnitPriority":1}              | revisions start at 1   |
      | /v1/transfer-lanes     | abc     | lanes    | {"Origin":"WH1","Destination":"WH2","LeadTime":86400000000000,"UnitHandlingCost":1,"Enabled":true}          | not a revision         |
      | /v1/transfer-lanes     | -1      | lanes    | {"Origin":"WH1","Destination":"WH2","LeadTime":86400000000000,"UnitHandlingCost":1,"Enabled":true}          | revisions are positive |

  Scenario Outline: An entry refused on its face is a 422
    When I send PUT "<path>" with this body:
      """
      <body>
      """
    Then the response status is 422
    And the problem detail type is "invalid-catalogue-entry"

    Examples: invalid entries
      | path                   | body                                                                                              | why                       |
      | /v1/rebalance-policies | {"Version":"pv-1","Site":"WH2","SKU":"SKU-1","SafetyStock":9,"TargetStock":1,"UnitPriority":1}    | target below safety stock |
      | /v1/rebalance-policies | {"Version":"","Site":"WH2","SKU":"SKU-1","SafetyStock":0,"TargetStock":1,"UnitPriority":1}        | no policy version         |
      | /v1/transfer-lanes     | {"Origin":"WH1","Destination":"WH1","LeadTime":0,"UnitHandlingCost":1,"Enabled":true}             | a lane to itself          |
      | /v1/transfer-lanes     | {"Origin":"WH1","Destination":"WH2","LeadTime":0,"UnitHandlingCost":-1,"Enabled":true}            | negative handling cost    |

  Scenario: Lanes are directed and listed with their revision
    Given the lane catalogue holds an enabled lane from "WH1" to "WH2" with lead time 24 hours and unit handling cost 1
    And the lane catalogue holds a disabled lane from "WH2" to "WH1" with lead time 48 hours and unit handling cost 2
    When I request GET "/v1/transfer-lanes"
    Then the response status is 200
    And the JSON array "lanes" has 2 elements
    And the JSON field "lanes.0.lane.Origin" is "WH1"
    And the JSON field "lanes.0.lane.Enabled" is true
    And the JSON field "lanes.1.lane.Origin" is "WH2"
    And the JSON field "lanes.1.lane.Enabled" is false
    And the JSON field "lanes.1.revision" is 1

  Scenario Outline: An instance without a database cannot serve the catalogues
    Given this instance runs without a database
    When I request GET "<path>"
    Then the response status is 503
    And the problem detail type is "catalogue-unavailable"

    Examples: catalogues
      | path                   |
      | /v1/rebalance-policies |
      | /v1/transfer-lanes     |
