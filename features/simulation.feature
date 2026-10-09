# Derived from: apis/openapi.yaml (simulateTransferOptions:
# GET /v1/transfer-simulations) and .claude/rules/domain-model.md (Network
# Inventory Planning projects LOCAL read models; it never mutates stock).
#
# The simulation is fail-closed: it answers from complete, fresh, direction-
# enabled facts or it answers 503. An empty read model is NOT a 200 with no
# sites — refusing to plan from nothing is the correct behaviour.
@bdd
Feature: Advisory network simulation from the local read models
  As an inventory planner
  I want a per-site capacity-versus-demand view built only from complete, fresh facts
  So that I never plan from partial state

  Scenario: With no facts declared the simulation fails closed
    Given no planning facts are declared
    When I request GET "/v1/transfer-simulations"
    Then the response status is 503
    And the problem detail type is "read-models-incomplete"
    And the JSON field "detail" contains "no site capability facts"

  Scenario: An instance without read models answers 503 rather than fabricating a simulation
    Given this instance runs without a database
    When I request GET "/v1/transfer-simulations"
    Then the response status is 503
    And the problem detail type is "read-models-unavailable"

  Scenario: Declared facts become a per-site headroom view
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    When I request GET "/v1/transfer-simulations"
    Then the response status is 200
    And the response content type is "application/json"
    And the JSON field "advisory" is true
    And the JSON field "asOf" is "2026-10-06T14:59:00Z"
    And the JSON array "sites" has 2 elements
    And the JSON field "sites.0.site" is "WH1"
    And the JSON field "sites.0.originEnabled" is true
    And the JSON field "sites.0.destinationEnabled" is true
    And the JSON field "sites.0.totalDemand" is 40
    And the JSON field "sites.0.capacityOverWindow" is 500
    And the JSON field "sites.0.capacityHeadroom" is 460
    And the JSON field "sites.0.windowStart" is "2026-10-06T16:00:00Z"
    And the JSON field "sites.0.windowEnd" is "2026-10-06T23:00:00Z"
    And the JSON field "sites.1.site" is "WH2"
    And the JSON field "sites.1.capacityHeadroom" is 290

  Scenario: A site whose demand exceeds its published capacity shows negative headroom
    Given site "WH1" is declared with capacity 30 and 40 units of demand for SKU "SKU-1"
    When I request GET "/v1/transfer-simulations"
    Then the response status is 200
    And the JSON field "sites.0.capacityHeadroom" is -10

  Scenario: Demand from several orders in the window adds up
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH1" also has 25 units of demand for SKU "SKU-2" inside its capacity window
    When I request GET "/v1/transfer-simulations"
    Then the response status is 200
    And the JSON field "sites.0.totalDemand" is 65
    And the JSON field "sites.0.capacityHeadroom" is 435

  Scenario: Demand due outside the capacity window does not count
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH1" also has 60 units of demand for SKU "SKU-1" due before its capacity window
    When I request GET "/v1/transfer-simulations"
    Then the response status is 200
    And the JSON field "sites.0.totalDemand" is 40

  Scenario: Removed demand no longer constrains planning
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH1" also has 70 units of removed demand for SKU "SKU-1"
    When I request GET "/v1/transfer-simulations"
    Then the response status is 200
    And the JSON field "sites.0.totalDemand" is 40

  Scenario: A site without a published capacity plan is left out, never zero-filled
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And site "WH2" has no published capacity plan
    When I request GET "/v1/transfer-simulations"
    Then the response status is 200
    And the JSON array "sites" has 1 element
    And the JSON field "sites.0.site" is "WH1"

  Scenario: Stale facts fail closed instead of being simulated
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    When 11 minutes pass
    And I request GET "/v1/transfer-simulations"
    Then the response status is 503
    And the problem detail type is "read-models-incomplete"
    And the JSON field "detail" contains "stale"

  Scenario Outline: A site disabled for a transfer direction fails the whole simulation closed
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"
    And site "WH2" cannot <direction> transfers
    When I request GET "/v1/transfer-simulations"
    Then the response status is 503
    And the JSON field "detail" contains "<refusal>"

    Examples: direction rules
      | direction | refusal                           |
      | send      | disabled as a transfer origin      |
      | receive   | disabled as a transfer destination |

  Scenario: Simulating is advisory: nothing is created, reserved or published
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    When I request GET "/v1/transfer-simulations"
    Then the response status is 200
    And no transfer exists
    And no domain events were published
