# Derived from: docs/docs/operations/troubleshooting.md (problem types:
# idempotency-key-required / -invalid / -reused; "Approval -> 409
# idempotency-key-reused ... the fingerprint is method + path + SHA-256 of the
# whole body ... a replay with the byte-identical body answers the stored 200
# with Idempotency-Replayed: true"; "Approval -> 503 approval-unavailable:
# retry with the same key once the database is healthy"), the use-cases table
# (ApproveTransfer: idempotency key unique, same body -> replay, different body
# -> 409) and the fleet REST guideline section 2 (the key is 1-128 printable
# ASCII characters; 2xx and 4xx answers are stored, a 5xx is never stored).
@bdd
Feature: Idempotency-Key on keyed writes
  As an API client
  I want a retried write to be safe and a reused key to be refused
  So that a network retry never approves twice and a stale key never hides a different request

  Background:
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"

  Scenario Outline: The key is 1 to 128 characters
    When I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with an idempotency key of <length> characters
    Then the response status is <status>
    And the response content type is "<type>"

    Examples: the length limit
      | length | status | type                     | why                 |
      | 1      | 200    | application/json         | the shortest key    |
      | 128    | 200    | application/json         | exactly the limit   |
      | 129    | 400    | application/problem+json | one over the limit  |

  Scenario Outline: The key is printable ASCII, without spaces
    When I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "<key>"
    Then the response status is <status>
    And the response content type is "<type>"

    Examples: the character range 0x21 to 0x7e
      | key       | status | type                     | why                                    |
      | !         | 200    | application/json         | the first printable character          |
      | ~         | 200    | application/json         | the last printable character           |
      | a b       | 400    | application/problem+json | a space is below 0x21                  |
      | clé-1     | 400    | application/problem+json | a non-ASCII character                  |

  Scenario: A refused key is the idempotency-key-invalid problem and nothing was created
    When I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with an idempotency key of 129 characters
    Then the problem detail type is "idempotency-key-invalid"
    And no transfer exists
    And no domain events were published

  Scenario: Two Idempotency-Key headers are refused, there must be exactly one
    When I approve a transfer with two Idempotency-Key headers
    Then the response status is 400
    And the problem detail type is "idempotency-key-invalid"
    And no transfer exists

  Scenario Outline: A reused key with a request that differs in any way is a conflict, even when the values are the same
    Given an approved transfer "T1"
    When I send POST "/v1/transfer-approvals" with the idempotency key "T1" and this body:
      """
      {"originSiteId":"<origin>","destinationSiteId":"<destination>","sku":"<sku>","quantity":5,"policyVersion":"policy-v3","operatorReason":"<reason>","proposalAsOf":"<asOf>"}
      """
    Then the response status is 409
    And the problem detail type is "idempotency-key-reused"
    And transfer "T1" is in state "ALLOCATING"
    And the domain event "TransferPlanApproved" was published 1 time

    Examples: the fingerprint is method + path + SHA-256 of the whole body
      | origin | destination | sku   | reason                      | asOf                 | what differs                            |
      | WH2    | WH1         | SKU-1 | operator approved rebalance | 2026-10-06T14:55:00Z | the sites are swapped                   |
      | WH1    | WH2         | SKU-2 | operator approved rebalance | 2026-10-06T14:55:00Z | the SKU                                 |
      | WH1    | WH2         | SKU-1 | changed my mind             | 2026-10-06T14:55:00Z | the operator reason                     |
      | WH1    | WH2         | SKU-1 | operator approved rebalance | 2026-10-06T14:56:00Z | the proposal as-of                      |
      | WH1    | WH2         | SKU-1 | operator approved rebalance | 2026-10-06T14:55:00Z | the same values in a different key order |

  Scenario: A facts-incomplete refusal is not stored, so the retry under the same key executes once the facts recover
    Given 11 minutes pass
    And I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    And the problem detail type is "facts-incomplete"
    And the planning facts are refreshed
    When I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 200
    And the response header "Idempotency-Replayed" is absent
    And the JSON field "state" is "ALLOCATING"
    When I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 200
    And the response header "Idempotency-Replayed" is true

  Scenario: A server-side failure is never stored, so the retry with the same key succeeds once the cause is fixed
    Given the pick work release is not configured
    And I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    And the problem detail type is "config-incomplete"
    And the pick work release is configured again
    When I approve a transfer of 5 units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 200
    And the response header "Idempotency-Replayed" is absent
    And the JSON field "state" is "ALLOCATING"
    When I request GET "/v1/transfers"
    Then the JSON array "items" has 1 element

  Scenario: Proposal generation is a keyed write too: the same key and body replays the answer
    When I send POST "/v1/transfer-proposal-runs" with the idempotency key "G1" and this body:
      """
      {"asOf":"2026-10-06T15:00:00Z","positions":[],"policies":[],"lanes":[]}
      """
    Then the response status is 200
    And the response header "Idempotency-Replayed" is absent
    And I remember the response
    When I send POST "/v1/transfer-proposal-runs" with the idempotency key "G1" and this body:
      """
      {"asOf":"2026-10-06T15:00:00Z","positions":[],"policies":[],"lanes":[]}
      """
    Then the response status is 200
    And the response header "Idempotency-Replayed" is true
    And the response is the one I remembered

  Scenario: Proposal generation refuses a reused key with another snapshot
    Given I send POST "/v1/transfer-proposal-runs" with the idempotency key "G1" and this body:
      """
      {"asOf":"2026-10-06T15:00:00Z","positions":[],"policies":[],"lanes":[]}
      """
    When I send POST "/v1/transfer-proposal-runs" with the idempotency key "G1" and this body:
      """
      {"asOf":"2026-10-06T15:05:00Z","positions":[],"policies":[],"lanes":[]}
      """
    Then the response status is 409
    And the problem detail type is "idempotency-key-reused"

  Scenario: Proposal generation needs an Idempotency-Key
    When I send POST "/v1/transfer-proposal-runs" without an idempotency key and this body:
      """
      {"asOf":"2026-10-06T15:00:00Z","positions":[],"policies":[],"lanes":[]}
      """
    Then the response status is 400
    And the problem detail type is "idempotency-key-required"

  Scenario: A snapshot without its as-of is a 422 planning-snapshot problem
    When I send POST "/v1/transfer-proposal-runs" with this body:
      """
      {"positions":[],"policies":[],"lanes":[]}
      """
    Then the response status is 422
    And the problem detail type is "invalid-planning-snapshot"
