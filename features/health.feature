# Derived from: apis/openapi.yaml (getHealth: GET /healthz) and
# .claude/rules/domain-model.md (Network Inventory Planning is advisory and
# never mutates stock).
@bdd
Feature: Liveness probe
  As the platform
  I want a dependency-free liveness probe
  So that a healthy process is never restarted because a database is down

  Scenario: The process is up
    When I request GET "/healthz"
    Then the response status is 204

  Scenario: Liveness carries no dependency state, even without a database
    Given this instance runs without a database
    When I request GET "/healthz"
    Then the response status is 204
