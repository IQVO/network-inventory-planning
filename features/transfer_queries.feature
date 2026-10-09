# Derived from: apis/openapi.yaml (listTransfers: GET /v1/transfers and
# getTransfer: GET /v1/transfers/{id}) and .claude/rules/domain-model.md
# (Transfer Plan: the approved, durable orchestration record).
#
# The read side is read-only: it never mutates a transfer, and an instance
# without a database answers 503 rather than a fabricated empty answer.
@bdd
Feature: Reading transfers and their audit trail
  As an operator
  I want to list transfers and inspect one with its history
  So that I can see where every transfer is and how it got there

  Background:
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"

  Scenario: A transfer is shown with its facts and its audit trail
    Given an approved transfer "T1"
    When I request GET "/v1/transfers/{T1}"
    Then the response status is 200
    And the response content type is "application/json"
    And the JSON field "id" is "{T1}"
    And the JSON field "state" is "ALLOCATING"
    And the JSON field "sku" is "SKU-1"
    And the JSON field "quantity" is 5
    And the JSON field "originSiteId" is "WH1"
    And the JSON field "destinationSiteId" is "WH2"
    And the JSON field "policyVersion" is "policy-v3"
    And the JSON field "createdAt" is "2026-10-06T15:00:00Z"
    And the JSON field "updatedAt" is "2026-10-06T15:00:00Z"
    And the JSON field "expiresAt" is "2026-10-07T15:00:00Z"
    And the JSON field "version" is 4
    And the JSON array "audit" has 4 elements

  Scenario: The last transition time moves with the saga, the creation time does not
    Given an approved transfer "T1"
    And 5 minutes pass
    And a "TransferStockAllocated" fact arrives for transfer "T1"
    When I request GET "/v1/transfers/{T1}"
    Then the JSON field "createdAt" is "2026-10-06T15:00:00Z"
    And the JSON field "updatedAt" is "2026-10-06T15:05:00Z"
    And the JSON field "version" is 5
    And the JSON field "audit.4.occurredAt" is "2026-10-06T15:05:00Z"
    And the JSON field "audit.4.cause" is "reservation res-1"

  Scenario: An unknown transfer is a 404 problem
    When I request GET "/v1/transfers/trf-does-not-exist"
    Then the response status is 404
    And the problem detail type is "transfer-not-found"

  Scenario: Reading a transfer changes nothing
    Given an approved transfer "T1"
    When I request GET "/v1/transfers/{T1}"
    And I request GET "/v1/transfers/{T1}"
    Then the JSON field "version" is 4
    And the JSON array "audit" has 4 elements

  Scenario: With no transfers the list is an empty array, not null
    When I request GET "/v1/transfers"
    Then the response status is 200
    And the JSON array "items" has 0 elements
    And the JSON field "total" is 0
    And the JSON field "limit" is 50
    And the JSON field "offset" is 0

  Scenario: Transfers are listed newest first, without their audit trails
    Given an approved transfer "T1"
    And 1 minute passes
    And an approved transfer "T2"
    And 1 minute passes
    And an approved transfer "T3"
    When I request GET "/v1/transfers"
    Then the response status is 200
    And the JSON field "total" is 3
    And the JSON field "items.0.id" is "{T3}"
    And the JSON field "items.1.id" is "{T2}"
    And the JSON field "items.2.id" is "{T1}"
    And the JSON field "items.0.audit" is absent

  Scenario Outline: The state filter is case-insensitive
    Given an approved transfer "T1"
    And 1 minute passes
    And a transfer "T2" in state "ALLOCATED"
    When I request GET "/v1/transfers?state=<spelling>"
    Then the response status is 200
    And the JSON field "total" is 1
    And the JSON field "items.0.id" is "{T2}"

    Examples: spellings of ALLOCATED
      | spelling  |
      | ALLOCATED |
      | allocated |
      | Allocated |

  Scenario: Transfers can be filtered by origin and destination site
    Given an approved transfer "T1" of 5 units of SKU "SKU-1" from "WH1" to "WH2"
    And 1 minute passes
    And an approved transfer "T2" of 3 units of SKU "SKU-1" from "WH2" to "WH1"
    When I request GET "/v1/transfers?originSiteId=WH1"
    Then the JSON field "total" is 1
    And the JSON field "items.0.id" is "{T1}"
    When I request GET "/v1/transfers?destinationSiteId=WH1"
    Then the JSON field "total" is 1
    And the JSON field "items.0.id" is "{T2}"
    When I request GET "/v1/transfers?originSiteId=WH1&destinationSiteId=WH1"
    Then the JSON field "total" is 0
    And the JSON array "items" has 0 elements

  Scenario: Filters combine
    Given an approved transfer "T1" of 5 units of SKU "SKU-1" from "WH1" to "WH2"
    And 1 minute passes
    And a transfer "T2" of 3 units of SKU "SKU-1" from "WH1" to "WH2" in state "ALLOCATED"
    When I request GET "/v1/transfers?originSiteId=WH1&state=ALLOCATING"
    Then the JSON field "total" is 1
    And the JSON field "items.0.id" is "{T1}"

  Scenario: Paging reports the unpaged total
    Given an approved transfer "T1"
    And 1 minute passes
    And an approved transfer "T2"
    And 1 minute passes
    And an approved transfer "T3"
    When I request GET "/v1/transfers?limit=2"
    Then the JSON field "total" is 3
    And the JSON array "items" has 2 elements
    And the JSON field "limit" is 2
    And the JSON field "offset" is 0
    When I request GET "/v1/transfers?limit=2&offset=2"
    Then the JSON array "items" has 1 element
    And the JSON field "items.0.id" is "{T1}"
    And the JSON field "offset" is 2
    When I request GET "/v1/transfers?offset=3"
    Then the JSON field "total" is 3
    And the JSON array "items" has 0 elements

  Scenario Outline: A query the service cannot honour is a 400
    When I request GET "/v1/transfers?<query>"
    Then the response status is 400
    And the problem detail type is "invalid-query"

    Examples: invalid queries
      | query        | why                              |
      | state=SHIPPED | not a lifecycle state            |
      | limit=0      | below the minimum page size of 1 |
      | limit=-1     | negative page size               |
      | limit=201    | above the maximum page size      |
      | limit=abc    | not an integer                   |
      | offset=-1    | negative offset                  |
      | offset=abc   | not an integer                   |

  Scenario Outline: An instance without a database cannot read transfers
    Given this instance runs without a database
    When I request GET "<path>"
    Then the response status is 503
    And the problem detail type is "read-side-unavailable"

    Examples: read endpoints
      | path                |
      | /v1/transfers       |
      | /v1/transfers/trf-1 |
