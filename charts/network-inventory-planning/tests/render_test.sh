#!/usr/bin/env bash
# Chart render assertions for charts/network-inventory-planning (run in CI by
# the chart-tests job, next to the python tests in this directory).
#
# Postgres is the only runtime: the binaries exit at boot without DATABASE_URL
# (no in-memory mode, no default DSN), so the chart refuses to render without
# a source for it. The env wiring per component is asserted by
# test_env_wiring.py and the Service selector isolation by
# test_service_selectors.py; this file owns the render-level invariants:
# mandatory-value refusal, both ci values files render, the api Deployment
# gets its database URL from a Secret, and the termination grace period never
# drops below the binary's shutdown bounds.
#
# Run: bash charts/network-inventory-planning/tests/render_test.sh
set -euo pipefail

chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
release=nip

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

render() { helm template "$release" "$chart_dir" "$@"; }

# --- the chart refuses to render without its mandatory inputs -----------
if render >/dev/null 2>&1; then
  fail "the chart must refuse to render without database.url / database.existingSecret"
fi
if ! render --set database.existingSecret=nip-database >/dev/null 2>&1; then
  fail "the chart must render with database.existingSecret alone"
fi
if ! render --set database.url=postgres://u@example.invalid:5432/db >/dev/null 2>&1; then
  fail "the chart must render with database.url alone"
fi

# --- both ci values files render -----------------------------------------
for values in ci/default-values.yaml ci/all-components-values.yaml; do
  render -f "$chart_dir/$values" >/dev/null || fail "$values must render"
done

# --- the api Deployment ---------------------------------------------------
api_deployment="$(render -f "$chart_dir/ci/default-values.yaml" --show-only templates/deployment.yaml)"
grep -A4 'name: DATABASE_URL$' <<<"$api_deployment" | grep -q 'secretKeyRef:' \
  || fail "DATABASE_URL must come from a Secret (secretKeyRef)"
grace="$(awk '/terminationGracePeriodSeconds:/ {print $2; exit}' <<<"$api_deployment")"
[[ "$grace" =~ ^[0-9]+$ ]] || fail "terminationGracePeriodSeconds must render as whole seconds, got '$grace'"
(( grace >= 30 )) || fail "terminationGracePeriodSeconds $grace is below the binary's shutdown bounds (30s)"
if grep -qi 'in-memory' <<<"$api_deployment"; then fail "the render mentions an in-memory mode"; fi

# --- every component renders together ------------------------------------
all="$(render -f "$chart_dir/ci/all-components-values.yaml")"
for component in mcp a2a projector reports analytics; do
  grep -q "name: ${release}-network-inventory-planning-${component}" <<<"$all" \
    || fail "the ${component} component was not rendered by ci/all-components-values.yaml"
done

# --- the A2A agent (cmd/a2a, ADR 0020) -----------------------------------
repo_dir="$(cd "$chart_dir/../.." && pwd)"
a2a_dep="$(render --set database.existingSecret=nip-database --set a2a.enabled=true --show-only templates/a2a-deployment.yaml)"
# env_value NAME: the plain value of one env var in a rendered Deployment.
env_value() { awk -v n="$2" '$0 ~ "- name: "n"$" {getline; sub(/^ *value: /, ""); gsub(/"/, ""); print; exit}' <<<"$1"; }
grep -A4 'name: DATABASE_URL$' <<<"$a2a_dep" | grep -q 'secretKeyRef:' || fail "the a2a DATABASE_URL must come from a Secret"
[[ "$(env_value "$a2a_dep" A2A_ADDR || true)" == ":8093" ]] || fail "the a2a agent must listen on :8093 (8090 is mcp, 8091 the projector admin, 8092 the reports API)"
grep -q 'command: \["/app/a2a"\]' <<<"$a2a_dep" || fail "the a2a Deployment must run /app/a2a"
# The agent is read-only: it dials no Kafka and publishes nothing.
for var in KAFKA_BROKERS EVENT_PUBLISHER OUTBOX_RELAY_ENABLED; do
  if grep -q "name: $var\$" <<<"$a2a_dep"; then fail "the a2a Deployment must not carry $var (read-only agent)"; fi
done
# The Agent Card must advertise a dialable absolute URL: it defaults to the
# release's in-cluster Service DNS (namespace-derived, so pattern-match) and
# stays overridable.
a2a_url="$(env_value "$a2a_dep" A2A_AGENT_URL || true)" # a missing var must reach the fail message below, not abort under set -e
[[ "$a2a_url" =~ ^http://[a-z0-9.-]+-a2a\.[a-z0-9-]+\.svc\.cluster\.local:8093/$ ]] || fail "A2A_AGENT_URL default is not the in-cluster Service DNS: $a2a_url"
a2a_override="$(render --set database.existingSecret=nip-database --set a2a.enabled=true --set a2a.agentUrl=http://gw.example/api/a2a/network-inventory-planning/ --show-only templates/a2a-deployment.yaml)"
[[ "$(env_value "$a2a_override" A2A_AGENT_URL || true)" == "http://gw.example/api/a2a/network-inventory-planning/" ]] || fail "a2a.agentUrl does not override A2A_AGENT_URL"
# Every variable cmd/a2a's boot config names as REQUIRED is rendered, derived
# from the Go source so a new mandatory variable fails until the chart has it.
a2a_vars="$(grep -ho '[A-Z][A-Z0-9_]\{2,\} is required' "$repo_dir"/cmd/a2a/main.go | sed -E 's/ is required//' | sort -u)"
[[ -n "$a2a_vars" ]] || fail "found no mandatory variables for cmd/a2a; the scan is looking in the wrong place"
for var in $a2a_vars; do
  grep -q "name: $var\$" <<<"$a2a_dep" || fail "the a2a Deployment does not render $var, which cmd/a2a requires"
done
# The route rewrites /api/a2a/<context> to / so the agent card stays reachable.
a2a_route="$(render --set database.existingSecret=nip-database --set a2a.enabled=true --set a2a.route.enabled=true --set "a2a.route.parentRefs[0].name=gw" --set "a2a.route.parentRefs[0].namespace=kong" --show-only templates/a2a-route.yaml)"
grep -q 'replacePrefixMatch: /$' <<<"$a2a_route" || fail "the a2a route must rewrite to /"
grep -q 'value: /api/a2a/network-inventory-planning$' <<<"$a2a_route" || fail "the a2a route path must be /api/a2a/network-inventory-planning"
# A2A is off by default: no a2a resource renders unless asked for.
if render --set database.existingSecret=nip-database --show-only templates/a2a-deployment.yaml 2>/dev/null | grep -q 'kind: Deployment'; then fail "the a2a Deployment rendered with default values"; fi

echo "PASS: network-inventory-planning chart render assertions"
