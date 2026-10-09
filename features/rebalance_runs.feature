# Derived from: apis/openapi.yaml (listRebalanceRuns: GET /v1/rebalance-runs)
# and .claude/rules/domain-model.md (advisory planning: proposals are not
# permission to pick stock).
#
# A scheduled rebalance pass (NIP_REBALANCE_SCHEDULE, ADR 0007) is
# OBSERVE-ONLY: it builds the same fail-closed snapshot the simulation builds,
# records one run row and never approves anything. A fail-closed refusal is a
# FAILED run carrying its reason, never a hidden error.
@bdd
Feature: Scheduled rebalance run history
  As an operator
  I want to see what each scheduled rebalance pass saw and decided
  So that a chronically stale read model is visible instead of silently ignored

  Scenario: With no passes the history is an empty list
    When I request GET "/v1/rebalance-runs"
    Then the response status is 200
    And the response content type is "application/json"
    And the JSON array "runs" has 0 elements

  Scenario: A pass over complete, fresh facts is recorded as COMPLETED
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    When the scheduled rebalance runs
    And I request GET "/v1/rebalance-runs"
    Then the response status is 200
    And the JSON array "runs" has 1 element
    And the JSON field "runs.0.id" is 1
    And the JSON field "runs.0.outcome" is "COMPLETED"
    And the JSON field "runs.0.startedAt" is "2026-10-06T15:00:00Z"
    And the JSON field "runs.0.snapshotAsOf" is "2026-10-06T14:59:00Z"
    And the JSON field "runs.0.proposalCount" is 0
    And the JSON field "runs.0.rejectedCount" is 0
    And the JSON field "runs.0.failClosedReason" is absent

  Scenario: A pass is observe-only: it approves, allocates and releases nothing
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    When the scheduled rebalance runs
    Then no transfer exists
    And the domain event "RebalanceRunCompleted" was published 1 time
    And the domain event "TransferPlanApproved" was not published
    And the domain event "TransferAllocationRequested" was not published
    And the domain event "WorkDemandReleased" was not published

  Scenario: Stale facts make the pass a FAILED run that says why
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    When 11 minutes pass
    And the scheduled rebalance runs
    And I request GET "/v1/rebalance-runs"
    Then the JSON field "runs.0.outcome" is "FAILED"
    And the JSON field "runs.0.failClosedReason" contains "stale"
    And the JSON field "runs.0.snapshotAsOf" is "2026-10-06T14:59:00Z"
    And the JSON field "runs.0.proposalCount" is 0
    And the domain event "RebalanceRunCompleted" was not published

  Scenario: An empty read model is a FAILED run without a watermark
    Given no planning facts are declared
    When the scheduled rebalance runs
    And I request GET "/v1/rebalance-runs"
    Then the JSON field "runs.0.outcome" is "FAILED"
    And the JSON field "runs.0.failClosedReason" contains "no site capability facts"
    And the JSON field "runs.0.snapshotAsOf" is absent

  Scenario: Runs are listed newest first
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    When the scheduled rebalance runs
    And 11 minutes pass
    And the scheduled rebalance runs
    And I request GET "/v1/rebalance-runs"
    Then the JSON array "runs" has 2 elements
    And the JSON field "runs.0.id" is 2
    And the JSON field "runs.0.outcome" is "FAILED"
    And the JSON field "runs.1.id" is 1
    And the JSON field "runs.1.outcome" is "COMPLETED"

  Scenario: The page size bounds the history
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    When the scheduled rebalance runs
    And 1 minute passes
    And the scheduled rebalance runs
    And 1 minute passes
    And the scheduled rebalance runs
    And I request GET "/v1/rebalance-runs?limit=2"
    Then the JSON array "runs" has 2 elements
    And the JSON field "runs.0.id" is 3
    When I request GET "/v1/rebalance-runs"
    Then the JSON array "runs" has 3 elements

  Scenario Outline: The page size must be an integer from 1 to 200
    When I request GET "/v1/rebalance-runs?limit=<limit>"
    Then the response status is <status>

    Examples: page sizes
      | limit | status | why                  |
      | 1     | 200    | smallest page        |
      | 200   | 200    | largest page         |
      | 0     | 400    | below the minimum    |
      | 201   | 400    | above the maximum    |
      | -1    | 400    | negative             |
      | abc   | 400    | not an integer       |

  Scenario: A bad page size is a problem document
    When I request GET "/v1/rebalance-runs?limit=0"
    Then the response status is 400
    And the problem detail type is "invalid-limit"

  Scenario: An instance without a database cannot show the history
    Given this instance runs without a database
    When I request GET "/v1/rebalance-runs"
    Then the response status is 503
    And the problem detail type is "rebalance-runs-unavailable"
