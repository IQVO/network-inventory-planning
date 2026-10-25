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
for component in mcp projector reports analytics; do
  grep -q "name: ${release}-network-inventory-planning-${component}" <<<"$all" \
    || fail "the ${component} component was not rendered by ci/all-components-values.yaml"
done

echo "PASS: network-inventory-planning chart render assertions"
