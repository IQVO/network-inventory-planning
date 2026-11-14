# Derived from: use-cases table (CheckStuckTransfers: age in state > the
# state's threshold, observe-only, one TransferStuckDetected per stuck transfer
# per tick), ADR 0007 (per-state thresholds) and ADR 0022 (RELEASING is
# watched like ALLOCATING).
#
# Default thresholds: ALLOCATING 1h, RELEASING 1h, PICKED 24h, IN_TRANSIT 72h;
# every other non-terminal state 24h. "Over" is strictly over: a transfer
# exactly at its threshold is not stuck yet. The clock starts at the LAST
# transition, not at the creation.
@bdd
Feature: Saga health check: per-state stuck thresholds
  As an operator
  I want a transfer flagged exactly when it has waited longer than its state allows
  So that a stalled saga is visible without false alarms and without anything being cancelled for me

  Background:
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"

  Scenario Outline: A transfer is stuck once it has waited strictly longer than its state's threshold
    Given a transfer "T1" in state "<state>"
    When <seconds> seconds pass
    And the saga health check runs
    Then the saga health check has detected <detected> stuck transfers

    Examples: the threshold of every watched state, at the boundary
      | state      | seconds | detected | threshold                |
      | ALLOCATING | 3600    | 0        | 1h exactly is not over   |
      | ALLOCATING | 3601    | 1        | 1h and a second          |
      | RELEASING  | 3600    | 0        | 1h exactly is not over   |
      | RELEASING  | 3601    | 1        | 1h and a second          |
      | ALLOCATED  | 86400   | 0        | 24h default              |
      | ALLOCATED  | 86401   | 1        | 24h default              |
      | PICKED     | 86400   | 0        | 24h                      |
      | PICKED     | 86401   | 1        | 24h                      |
      | IN_TRANSIT | 259200  | 0        | 72h exactly is not over  |
      | IN_TRANSIT | 259201  | 1        | 72h and a second         |
      | ARRIVED    | 86400   | 0        | 24h default              |
      | ARRIVED    | 86401   | 1        | 24h default              |

  Scenario: A transfer is stuck at a different age in each state, so the verdict names the state it waits in
    Given a transfer "T1" in state "ALLOCATED"
    When 25 hours pass
    And the saga health check runs
    Then the stuck detection for transfer "T1" reports state "ALLOCATED" aged 90000 seconds against a threshold of 86400 seconds

  Scenario: The stuck clock restarts at every transition
    Given a transfer "T1" in state "ALLOCATED"
    And 50 minutes pass
    And I cancel transfer "T1" with reason "customer order withdrawn"
    When 30 minutes pass
    And the saga health check runs
    Then the saga health check has detected 0 stuck transfers
    When 31 minutes pass
    And the saga health check runs
    Then the saga health check has detected 1 stuck transfer
    And the stuck detection for transfer "T1" reports state "RELEASING" aged 3660 seconds against a threshold of 3600 seconds

  Scenario: One tick reports only the transfers that are over their threshold
    Given an approved transfer "T1"
    And 90 minutes pass
    And the planning facts are refreshed
    And an approved transfer "T2"
    When 45 minutes pass
    And the saga health check runs
    Then the saga health check has detected 1 stuck transfer
    And the domain event "TransferStuckDetected" was published 1 time
    And the stuck detection for transfer "T1" reports state "ALLOCATING" aged 8100 seconds against a threshold of 3600 seconds

  Scenario: A transfer that was cancelled is no longer watched
    Given an approved transfer "T1"
    And I cancel transfer "T1" with reason "wrong lane selected"
    When 100 hours pass
    And the saga health check runs
    Then the saga health check has detected 0 stuck transfers

  Scenario: The health check observes and never acts
    Given an approved transfer "T1"
    And 2 hours pass
    When the saga health check runs
    Then the saga health check has detected 1 stuck transfer
    And transfer "T1" is in state "ALLOCATING"
    And the domain event "TransferStateAdvanced" was published 4 times
    And the domain event "TransferAllocationReleaseRequested" was not published
    And the domain event "TransferPlanApproved" was published 1 time
    When I request GET "/v1/transfers/{T1}"
    Then the JSON field "version" is 4
    And the JSON array "audit" has 4 elements
