# Derived from: apis/openapi.yaml (getTransferFunnelReport, getStateDwellReport,
# getStuckTransfersReport, getRebalanceRunsReport, getReportsFreshness:
# GET /reports/*, served by the separate nip-reports binary on port 8092) and
# .claude/rules/domain-model.md / .claude/rules/analytics.md (reports are
# derived from the analytics stream only, never dual-written from the OLTP
# core).
#
# These scenarios run the whole path: a REST call or a ticker tick publishes
# CloudEvents 1.0 analytics occurrences, the production AnalyticsConsumer
# projects them, and the reports read the projection over HTTP.
@bdd
Feature: Saga-health analytics reports
  As an operations analyst
  I want read-only reports of how transfers progress, dwell and get stuck
  So that I can spot a degrading saga without touching the transactional core

  Background:
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"

  # ------------------------------------------------------- transfer funnel --

  Scenario: Before anything is projected the funnel is empty and echoes the default range
    When I request the report "/reports/transfer-funnel"
    Then the response status is 200
    And the response content type is "application/json"
    And the JSON array "days" has 0 elements
    And the JSON field "from" is "2026-09-06T15:00:00Z"
    And the JSON field "to" is "2026-10-06T15:00:00Z"

  Scenario: The funnel counts the distinct transfers that reached each state, per day
    Given an approved transfer "T1"
    And an approved transfer "T2"
    And 1 minute passes
    And the analytics projector has consumed the analytics topic
    When I request the report "/reports/transfer-funnel"
    Then the response status is 200
    And the JSON array "days" has 4 elements
    And the JSON field "days.0.day" is "2026-10-06"
    And the JSON field "days.0.state" is "ALLOCATING"
    And the JSON field "days.0.transfers" is 2
    And the JSON field "days.1.state" is "APPROVED"
    And the JSON field "days.2.state" is "DRAFT"
    And the JSON field "days.3.state" is "PROPOSED"
    And the JSON field "days.3.transfers" is 2

  Scenario: A transfer further along the saga shows up in the later states only
    Given a transfer "T1" in state "PICKED"
    And an approved transfer "T2"
    And 1 minute passes
    And the analytics projector has consumed the analytics topic
    When I request the report "/reports/transfer-funnel"
    Then the JSON array "days" has 6 elements
    And the JSON field "days.0.state" is "ALLOCATED"
    And the JSON field "days.0.transfers" is 1
    And the JSON field "days.1.state" is "ALLOCATING"
    And the JSON field "days.1.transfers" is 2
    And the JSON field "days.4.state" is "PICKED"
    And the JSON field "days.4.transfers" is 1

  Scenario: Redelivered analytics messages are counted once
    Given an approved transfer "T1"
    And 1 minute passes
    And the analytics projector has consumed the analytics topic
    And the analytics topic redelivers every message
    When I request the report "/reports/transfer-funnel"
    Then the JSON array "days" has 4 elements
    And the JSON field "days.0.transfers" is 1

  Scenario: A refused approval leaves no trace in the funnel
    Given no planning facts are declared
    And I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    And 1 minute passes
    And the analytics projector has consumed the analytics topic
    When I request the report "/reports/transfer-funnel"
    Then the JSON array "days" has 0 elements

  Scenario: Days are UTC calendar days and a range selects them
    Given an approved transfer "T1"
    And 1 day passes
    And the planning facts are refreshed
    And an approved transfer "T2"
    And the analytics projector has consumed the analytics topic
    When I request the report "/reports/transfer-funnel?from=2026-10-07T00:00:00Z&to=2026-10-08T00:00:00Z"
    Then the JSON array "days" has 4 elements
    And the JSON field "days.0.day" is "2026-10-07"
    And the JSON field "days.0.transfers" is 1
    And the JSON field "from" is "2026-10-07T00:00:00Z"
    And the JSON field "to" is "2026-10-08T00:00:00Z"

  Scenario Outline: The range is half-open: from is inclusive, to is exclusive
    Given an approved transfer "T1"
    And the analytics projector has consumed the analytics topic
    When I request the report "/reports/transfer-funnel?from=<from>&to=<to>"
    Then the response status is 200
    And the JSON array "days" has <rows> elements

    Examples: the transfer was approved at exactly 2026-10-06T15:00:00Z
      | from                 | to                   | rows | why                          |
      | 2026-10-06T15:00:00Z | 2026-10-06T15:00:01Z | 4    | a fact at from is in         |
      | 2026-10-05T00:00:00Z | 2026-10-06T15:00:00Z | 0    | a fact at to is out          |
      | 2026-10-06T15:00:01Z | 2026-10-07T00:00:00Z | 0    | a fact before from is out    |

  Scenario: With only a lower bound the range runs up to now
    When I request the report "/reports/transfer-funnel?from=2026-10-01T00:00:00Z"
    Then the JSON field "from" is "2026-10-01T00:00:00Z"
    And the JSON field "to" is "2026-10-06T15:00:00Z"

  Scenario: With only an upper bound the range is the 30 days before it
    When I request the report "/reports/transfer-funnel?to=2026-10-06T00:00:00Z"
    Then the JSON field "from" is "2026-09-06T00:00:00Z"
    And the JSON field "to" is "2026-10-06T00:00:00Z"

  Scenario Outline: A range that is not usable is a 400 problem, on every ranged report
    When I request the report "/reports/<report>?from=2026-10-07T00:00:00Z&to=2026-10-06T00:00:00Z"
    Then the response status is 400
    And the problem detail type is "invalid-report-range"

    Examples: the four ranged reports
      | report           |
      | transfer-funnel  |
      | state-dwell      |
      | stuck-transfers  |
      | rebalance-runs   |

  Scenario Outline: Range parameters are validated
    When I request the report "/reports/transfer-funnel?<query>"
    Then the response status is <status>

    Examples: boundaries
      | query                                           | status | why                          |
      | from=yesterday                                  | 400    | from is not RFC 3339         |
      | to=tomorrow                                     | 400    | to is not RFC 3339           |
      | from=2026-10-06T00:00:00Z&to=2026-10-06T00:00:00Z | 400  | the range is empty           |
      | from=2025-10-05T00:00:00Z&to=2026-10-07T00:00:00Z | 400  | 367 days exceed the maximum  |
      | from=2025-10-06T00:00:00Z&to=2026-10-07T00:00:00Z | 200  | 366 days are the maximum     |

  # ----------------------------------------------------------- state dwell --

  Scenario: State dwell reports saga-age percentiles at the moment a state was left
    Given an approved transfer "T1"
    And an approved transfer "T2"
    And 60 seconds pass
    And a "TransferStockAllocated" fact arrives for transfer "T1"
    And 40 seconds pass
    And a "TransferStockAllocated" fact arrives for transfer "T2"
    And 1 minute passes
    And the analytics projector has consumed the analytics topic
    When I request the report "/reports/state-dwell"
    Then the response status is 200
    And the JSON field "days.0.day" is "2026-10-06"
    And the JSON field "days.0.state" is "ALLOCATING"
    And the JSON field "days.0.transitions" is 2
    And the JSON field "days.0.p50_age_seconds" is 80
    And the JSON field "days.0.p95_age_seconds" is 98
    And the JSON field "days.1.state" is "APPROVED"
    And the JSON field "days.1.transitions" is 2

  Scenario: State dwell is empty before anything is projected
    When I request the report "/reports/state-dwell"
    Then the response status is 200
    And the JSON array "days" has 0 elements

  # --------------------------------------------------------- stuck transfers --

  Scenario: A transfer waiting past its threshold is reported as stuck
    Given an approved transfer "T1"
    And 2 hours pass
    When the saga health check runs
    Then the saga health check has detected 1 stuck transfer
    When 1 minute passes
    And the analytics projector has consumed the analytics topic
    And I request the report "/reports/stuck-transfers"
    Then the response status is 200
    And the JSON array "days" has 1 element
    And the JSON field "days.0.day" is "2026-10-06"
    And the JSON field "days.0.state" is "ALLOCATING"
    And the JSON field "days.0.detections" is 1
    And the JSON field "days.0.transfers" is 1
    And the JSON array "latest" has 1 element
    And the JSON field "latest.0.transfer_id" is "{T1}"
    And the JSON field "latest.0.state" is "ALLOCATING"
    And the JSON field "latest.0.age_seconds" is 7200
    And the JSON field "latest.0.threshold_seconds" is 3600
    And the JSON field "latest.0.detected_at" is "2026-10-06T17:00:00Z"

  Scenario: A transfer that stays stuck is re-detected on every pass, newest first
    Given an approved transfer "T1"
    And 2 hours pass
    And the saga health check runs
    And 1 minute passes
    And the saga health check runs
    And 1 minute passes
    And the analytics projector has consumed the analytics topic
    When I request the report "/reports/stuck-transfers"
    Then the JSON field "days.0.detections" is 2
    And the JSON field "days.0.transfers" is 1
    And the JSON array "latest" has 2 elements
    And the JSON field "latest.0.age_seconds" is 7260
    And the JSON field "latest.1.age_seconds" is 7200
    When I request the report "/reports/stuck-transfers?limit=1"
    Then the JSON array "latest" has 1 element
    And the JSON field "latest.0.age_seconds" is 7260

  Scenario: A transfer still inside its threshold is not stuck
    Given an approved transfer "T1"
    And 30 minutes pass
    When the saga health check runs
    Then the saga health check has detected 0 stuck transfers

  Scenario: A finished transfer is never stuck, however old
    Given a transfer "T1" in state "RECEIVED"
    And 100 hours pass
    When the saga health check runs
    Then the saga health check has detected 0 stuck transfers

  Scenario Outline: The latest-occurrences limit must be an integer from 1 to 100
    When I request the report "/reports/stuck-transfers?limit=<limit>"
    Then the response status is <status>

    Examples: limits
      | limit | status |
      | 1     | 200    |
      | 100   | 200    |
      | 0     | 400    |
      | 101   | 400    |
      | abc   | 400    |

  Scenario: A bad limit is a problem document
    When I request the report "/reports/stuck-transfers?limit=0"
    Then the problem detail type is "invalid-report-limit"

  # ------------------------------------------------------ rebalance-runs report --

  Scenario: Completed scheduled runs are summed per day
    When the scheduled rebalance runs
    And 1 minute passes
    And the scheduled rebalance runs
    And 1 minute passes
    And the analytics projector has consumed the analytics topic
    And I request the report "/reports/rebalance-runs"
    Then the response status is 200
    And the JSON array "days" has 1 element
    And the JSON field "days.0.day" is "2026-10-06"
    And the JSON field "days.0.runs" is 2
    And the JSON field "days.0.proposals" is 0
    And the JSON field "days.0.rejected" is 0
    And the JSON field "days.0.rejection_rate" is 0
    And the JSON field "days.0.stale_facts" is 0

  # ---------------------------------------------------------------- freshness --

  Scenario: Before any event is applied the freshness is null
    When I request the report "/reports/freshness"
    Then the response status is 200
    And the JSON field "as_of" is null
    And the JSON field "lag_seconds" is null

  Scenario: Freshness is how far the projection is behind the newest event applied
    Given an approved transfer "T1"
    And the analytics projector has consumed the analytics topic
    And 30 seconds pass
    When I request the report "/reports/freshness"
    Then the JSON field "as_of" is "2026-10-06T15:00:00Z"
    And the JSON field "lag_seconds" is 30
