# Derived from: use-cases table (SimulateTransferOptions / BuildSnapshot: demand
# counts only when due_at is in the plan window [start, end); a stale or missing
# fact excludes that site; ApproveTransfer: ValidateApproval; GenerateTransferProposals:
# score = quantity x unitPriority - quantity x unitHandlingCost - leadTimeDays
# must be positive, output sorted by score, then SKU, origin, destination),
# ADR 0018 (per-site staleness: exactly MaxStaleness old is fresh, one tick older
# is stale; the NEWEST capability revision and the NEWEST capacity plan of a site
# are judged; a stale demand row, ACTIVE or REMOVED, excludes its site) and the
# read-model ingestion table (a strictly greater capability_revision supersedes;
# a newer published_at supersedes).
@bdd
Feature: Planning read models and approval at their documented boundaries
  As an inventory planner
  I want the planning facts judged exactly at their documented edges
  So that a one-second difference never silently changes what is planned or approved

  Background:
    Given site "WH1" is declared with capacity 500 and 40 units of demand for SKU "SKU-1"
    And site "WH2" is declared with capacity 300 and 10 units of demand for SKU "SKU-1"

  Scenario Outline: The capacity window is half-open: demand due at its start counts, demand due at its end does not
    Given site "WH1" also has 25 units of demand for SKU "SKU-1" due <when> of its capacity window
    When I request GET "/v1/transfer-simulations"
    Then the response status is 200
    And the JSON field "sites.0.site" is "WH1"
    And the JSON field "sites.0.totalDemand" is <total>

    Examples: the window is [start, end)
      | when                        | total | why                              |
      | at the start                | 65    | start is inclusive               |
      | one second before the start | 40    | still before the window          |
      | one second before the end   | 65    | still inside the window          |
      | at the end                  | 40    | end is exclusive                 |

  Scenario Outline: A fact exactly at the freshness budget is fresh and one second older is stale
    Given the facts of site "WH2" were last updated <seconds> seconds ago
    When I request GET "/v1/transfer-simulations"
    Then the response status is 200
    And the JSON array "sites" has <sites> elements

    Examples: PLANNING_MAX_STALENESS is 10 minutes
      | seconds | sites | why                                |
      | 600     | 2     | exactly the budget is still fresh  |
      | 601     | 1     | one second over excludes the site  |

  Scenario Outline: One stale demand row excludes its site even when its capability and capacity plan are fresh
    Given site "WH1" also has a 5-unit <state> demand for SKU "SKU-2" last updated 11 minutes ago
    When I request GET "/v1/transfer-simulations"
    Then the response status is 200
    And the JSON array "sites" has 1 element
    And the JSON field "sites.0.site" is "WH2"

    Examples: a REMOVED row is judged like an ACTIVE one
      | state   |
      | active  |
      | removed |

  Scenario Outline: The newest capacity plan of a site is the one planned with
    Given site "WH1" also published a capacity plan of <capacity> units <seconds> seconds ago
    When I request GET "/v1/transfer-simulations"
    Then the response status is 200
    And the JSON field "sites.0.site" is "WH1"
    And the JSON field "sites.0.capacityOverWindow" is <planned>
    And the JSON field "sites.0.capacityHeadroom" is <headroom>

    Examples: the declared plan was published 60 seconds ago with 500 units
      | capacity | seconds | planned | headroom | why                               |
      | 900      | 300     | 500     | 460      | an older plan does not supersede  |
      | 200      | 60      | 500     | 460      | the same publication time is not newer, the first stands |
      | 200      | 30      | 200     | 160      | a newer plan supersedes           |

  Scenario Outline: The newest capability revision of a site decides what it may do, whatever order they arrive in
    Given site "WH2" also published capability revision <first revision> that <first> receive transfers
    And site "WH2" also published capability revision <second revision> that <second> receive transfers
    When I request GET "/v1/transfer-simulations"
    Then the JSON field "sites.1.site" is "WH2"
    And the JSON field "sites.1.destinationEnabled" is <enabled>

    Examples: WH2 is declared at revision 1 and may receive; only a strictly greater revision supersedes
      | first revision | first  | second revision | second | enabled | why                                          |
      | 2              | cannot | 2               | cannot | false   | a newer revision replaces the declared one   |
      | 3              | cannot | 2               | can    | false   | a late older revision does not undo it       |
      | 1              | cannot | 1               | cannot | true    | the same revision is not newer, the first stands |

  Scenario Outline: The origin's capacity must cover ALL of its in-window demand plus the transfer
    Given site "WH1" also has 25 units of demand for SKU "SKU-2" inside its capacity window
    When I approve a transfer of <quantity> units of SKU "SKU-1" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is <status>

    Examples: capacity 500 minus 40 units of SKU-1 and 25 units of SKU-2 leaves 435
      | quantity | status | why                                            |
      | 435      | 200    | exactly what is left                           |
      | 436      | 422    | one unit more than the origin can cover        |

  Scenario Outline: The destination's demand for the SKU must fall inside the capacity window
    Given site "WH2" also has 30 units of demand for SKU "SKU-3" due <when> of its capacity window
    When I approve a transfer of 5 units of SKU "SKU-3" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is <status>

    Examples: the window is [start, end)
      | when                        | status | why                           |
      | at the start                | 200    | start is inclusive            |
      | one second before the end   | 200    | still inside the window       |
      | at the end                  | 422    | end is exclusive              |
      | one second before the start | 422    | still before the window       |

  Scenario: Demand that was removed does not justify a transfer
    Given site "WH2" also has 30 units of removed demand for SKU "SKU-3"
    When I approve a transfer of 5 units of SKU "SKU-3" from "WH1" to "WH2" under policy "policy-v3" with the idempotency key "T1"
    Then the response status is 422
    And the problem detail type is "facts-incomplete"
    And the JSON field "detail" contains "shows no in-window demand for SKU SKU-3"
    And no transfer exists

  Scenario Outline: A proposal is kept only while its score is strictly positive
    Given a planning snapshot as of "2026-10-06T15:00:00Z"
    And site "WH1" has 100 available units of SKU "SKU-1"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-1" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" has 10 available units of SKU "SKU-1"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock 50 and unit priority 4
    And an enabled lane from "WH1" to "WH2" with lead time <hours> hours and unit handling cost 3
    When I generate transfer proposals
    Then the response status is 200
    And the JSON array "proposals" has <proposals> elements

    Examples: 40 units x priority 4 = 160, minus 40 x handling 3 = 120, minus the lead time in days
      | hours | proposals | score                          |
      | 936   | 1         | 160 - 120 - 39 days = 1        |
      | 960   | 0         | 160 - 120 - 40 days = 0        |
      | 984   | 0         | 160 - 120 - 41 days = -1       |

  Scenario: Proposals with the same score are ordered by SKU
    Given a planning snapshot as of "2026-10-06T15:00:00Z"
    And site "WH1" has 100 available units of SKU "SKU-B"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-B" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" has 10 available units of SKU "SKU-B"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-B" with safety stock 0, target stock 50 and unit priority 3
    And site "WH1" has 100 available units of SKU "SKU-A"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-A" with safety stock 20, target stock 0 and unit priority 0
    And site "WH2" has 10 available units of SKU "SKU-A"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-A" with safety stock 0, target stock 50 and unit priority 3
    And an enabled lane from "WH1" to "WH2" with lead time 48 hours and unit handling cost 1
    When I generate transfer proposals
    Then the JSON array "proposals" has 2 elements
    And the JSON field "proposals.0.SKU" is "SKU-A"
    And the JSON field "proposals.1.SKU" is "SKU-B"

  Scenario: Proposals with the same score and SKU are ordered by origin
    Given a planning snapshot as of "2026-10-06T15:00:00Z"
    And site "WH2" has 100 available units of SKU "SKU-1"
    And site "WH2" holds policy "policy-v7" for SKU "SKU-1" with safety stock 20, target stock 0 and unit priority 0
    And site "WH1" has 100 available units of SKU "SKU-1"
    And site "WH1" holds policy "policy-v7" for SKU "SKU-1" with safety stock 20, target stock 0 and unit priority 0
    And site "WH3" has 10 available units of SKU "SKU-1"
    And site "WH3" holds policy "policy-v7" for SKU "SKU-1" with safety stock 0, target stock 50 and unit priority 3
    And an enabled lane from "WH2" to "WH3" with lead time 48 hours and unit handling cost 1
    And an enabled lane from "WH1" to "WH3" with lead time 48 hours and unit handling cost 1
    When I generate transfer proposals
    Then the JSON array "proposals" has 2 elements
    And the JSON field "proposals.0.Origin" is "WH1"
    And the JSON field "proposals.1.Origin" is "WH2"
