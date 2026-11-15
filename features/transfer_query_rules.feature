# Derived from: the use-cases table (ListTransfers: state in the lifecycle
# vocabulary, case-insensitive; REST keyset paging, limit 1-500 default 100,
# opaque cursor from nextCursor) and the troubleshooting problem-type table
# (invalid-limit: limit outside 1-500).
#
# The vocabulary is the twelve saga states of apis/openapi.yaml TransferState,
# RELEASING included (ADR 0022: cancel after allocation runs ALLOCATED ->
# RELEASING -> CANCELLED), so an operator can list the transfers whose origin
# hold is being released.
@bdd
Feature: Transfer list filter vocabulary and paging edges
  As an operator
  I want the list filters and the page size judged at their documented edges
  So that no state is silently unfilterable and no page walk loses or repeats a transfer

  Background:
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"

  Scenario Outline: Every state of the lifecycle vocabulary is a valid filter, in any letter case
    Given an approved transfer "T1"
    When I request GET "/v1/transfers?state=<state>"
    Then the response status is 200
    And the JSON array "items" has <found> elements

    Examples: the transfer waits in ALLOCATING
      | state         | found |
      | DRAFT         | 0     |
      | PROPOSED      | 0     |
      | APPROVED      | 0     |
      | ALLOCATING    | 1     |
      | allocating    | 1     |
      | ALLOCATED     | 0     |
      | RELEASING     | 0     |
      | releasing     | 0     |
      | PICKED        | 0     |
      | IN_TRANSIT    | 0     |
      | in_transit    | 0     |
      | ARRIVED       | 0     |
      | RECEIVED      | 0     |
      | UNFULFILLABLE | 0     |
      | CANCELLED     | 0     |

  Scenario: A transfer whose origin hold is being released is listed under RELEASING
    Given a transfer "T1" in state "RELEASING"
    And a transfer "T2" in state "ALLOCATED"
    When I request GET "/v1/transfers?state=RELEASING"
    Then the response status is 200
    And the JSON array "items" has 1 element
    And the JSON field "items.0.id" is "{T1}"
    And the JSON field "items.0.state" is "RELEASING"

  Scenario: An unknown state is still refused
    When I request GET "/v1/transfers?state=FLYING"
    Then the response status is 400

  Scenario Outline: The page size accepts its smallest and largest value
    When I request GET "/v1/transfers?limit=<limit>"
    Then the response status is 200

    Examples: limit is 1 to 500
      | limit | why           |
      | 1     | smallest page |
      | 500   | largest page  |

  Scenario: A page that exactly fills the limit has no next page
    Given an approved transfer "T1"
    And 1 minute passes
    And an approved transfer "T2"
    When I request GET "/v1/transfers?limit=2"
    Then the JSON array "items" has 2 elements
    And the JSON field "nextCursor" is absent

  Scenario: Paging with a filter walks only the matching transfers
    Given an approved transfer "T1"
    And 1 minute passes
    And a transfer "T2" in state "ALLOCATED"
    And 1 minute passes
    And an approved transfer "T3"
    And 1 minute passes
    And an approved transfer "T4"
    When I request GET "/v1/transfers?state=ALLOCATING&limit=2"
    Then the JSON array "items" has 2 elements
    And the JSON field "items.0.id" is "{T4}"
    And the JSON field "items.1.id" is "{T3}"
    And the JSON field "nextCursor" is present
    When I request the next page of "/v1/transfers?state=ALLOCATING&limit=2"
    Then the JSON array "items" has 1 element
    And the JSON field "items.0.id" is "{T1}"
    And the JSON field "nextCursor" is absent
