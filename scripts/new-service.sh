#!/usr/bin/env bash
# scripts/new-service.sh <service-name> <richest-domain-aggregate-pkg> [<event-subdomain> <event-context>]
#
# Instantiates warehouse-harness-template into the CURRENT working
# directory (run this FROM the new repo's checkout, with this template
# repo cloned somewhere else so you can reference/copy from it -- this
# script assumes it is itself being run from a COPY of this template's
# files already placed into the new repo, e.g. via
#   git clone https://github.com/claudioed/warehouse-harness-template <new-repo>
#   cd <new-repo> && rm -rf .git && git init
#   bash scripts/new-service.sh inventory stock
#
# Substitutes:
#   {{SERVICE}}            -> <service-name>          (Postgres user/db, image build tag stem)
#   {{SERVICE_REPO}}        -> current directory's basename (the actual repo/GitHub name)
#   {{RICHEST_AGGREGATE}}   -> <richest-domain-aggregate-pkg>
#   {{EVENT_SUBDOMAIN}}     -> <event-subdomain>  (wms | wes; optional)
#   {{EVENT_CONTEXT}}       -> <event-context>    (bounded-context segment of
#                              the CloudEvents `type`; optional)
#
# CloudEvents (fleet-mandatory, see .claude/rules/integration-events.md):
# when <event-subdomain> and <event-context> are given, the service is
# Kafka-capable and the script generates
# internal/adapters/kafka/cloudevents/{cloudevents.go,cloudevents_test.go}
# from templates/cloudevents/*.tmpl -- the ONLY envelope helper a Kafka
# publisher/consumer may use. Then run:
#   go get github.com/cloudevents/sdk-go/v2@latest github.com/segmentio/kafka-go
#   go test ./internal/adapters/kafka/cloudevents/...
# Omit both args for a service with no Kafka I/O (and delete
# .claude/rules/integration-events.md). There is NO envelope toggle to
# configure, ever.
#
# Does NOT and cannot fill in:
#   - .claude/rules/*.md content (domain model, REST routes, event
#     contracts) -- these are placeholders by design; an instantiated repo
#     with no real domain code yet has nothing true to fill them with.
#   - .gremlins.yaml's efficacy/mutant-coverage thresholds -- these MUST
#     be measured against real code (`make mutation-full`) before they
#     mean anything; the placeholders are left in place and `make
#     mutation` will fail loudly with an unhelpful gremlins config error
#     until you replace them, which is the point (better a loud failure
#     than a silently-accepted 0% threshold).
#   - go.mod's module path / real dependency versions -- run `go mod init
#     github.com/claudioed/<repo>` yourself, then `go get` the pinned
#     versions HARNESS.md references (arch-go, testcontainers, gremlins).
#
# After running this script:
#   1. grep -rn '{{' .   -- confirm zero remaining placeholders except the
#      two documented above ({{MEASURED_*}} in .gremlins.yaml).
#   2. Fill in .claude/rules/*.md for real once the domain exists.
#   3. Write real domain/application/adapters code, then:
#        go mod init github.com/claudioed/<repo>
#        go get github.com/arch-go/arch-go@v1.7.0
#        go test ./internal/architecture/... -v   # should PASS trivially on an empty tree
#   4. make mutation-full, read the measured efficacy/mutant-coverage,
#      set .gremlins.yaml's thresholds strictly BELOW those numbers.
#   5. Record "harness-template: v2" in this repo's AGENTS.md, and (Kafka
#      services) the "Events: CloudEvents 1.0 is MANDATORY" section from
#      .claude/rules/integration-events.md in its CLAUDE.md/AGENTS.md.
#   6. lefthook install
set -euo pipefail

if [[ $# -ne 2 && $# -ne 4 ]]; then
  echo "usage: $0 <service-name> <richest-domain-aggregate-pkg> [<event-subdomain> <event-context>]" >&2
  echo "  example: $0 inventory stock wms inventory-storage" >&2
  exit 1
fi

SERVICE="$1"
RICHEST_AGGREGATE="$2"
EVENT_SUBDOMAIN="${3:-}"
EVENT_CONTEXT="${4:-}"
SERVICE_REPO="$(basename "$(pwd)")"

if [[ -n "${EVENT_SUBDOMAIN}" && "${EVENT_SUBDOMAIN}" != "wms" && "${EVENT_SUBDOMAIN}" != "wes" ]]; then
  echo "error: <event-subdomain> must be wms or wes (fleet Event Standard), got '${EVENT_SUBDOMAIN}'" >&2
  exit 1
fi

echo "==> instantiating warehouse-harness-template"
echo "    SERVICE            = ${SERVICE}"
echo "    SERVICE_REPO       = ${SERVICE_REPO}  (from current directory name)"
echo "    RICHEST_AGGREGATE  = ${RICHEST_AGGREGATE}"
echo

# ci.yml ships as ci.yml.template (NOT .yml) in this template repo so
# GitHub Actions never tries to execute it here -- a template with no real
# service code would fail almost every job (no Dockerfile, no
# apis/openapi.yaml, no charts, no web/), which is a meaningless CI signal
# for a template rather than the actual thing worth verifying (that
# instantiation + the fitness tests work, which scripts/new-service.sh and
# `go test ./internal/architecture/... -v` already prove). Activate it now.
if [[ -f .github/workflows/ci.yml.template ]]; then
  mv .github/workflows/ci.yml.template .github/workflows/ci.yml
  echo "  activated .github/workflows/ci.yml (was ci.yml.template)"
fi

# CloudEvents 1.0 helper (fleet-mandatory envelope). Generated only for a
# Kafka-capable service; nothing here offers a flat/dual alternative.
if [[ -n "${EVENT_SUBDOMAIN}" ]]; then
  ce_dir="internal/adapters/kafka/cloudevents"
  mkdir -p "${ce_dir}"
  for f in cloudevents.go cloudevents_test.go; do
    sed \
      -e "s/{{SERVICE_REPO}}/${SERVICE_REPO}/g" \
      -e "s/{{EVENT_SUBDOMAIN}}/${EVENT_SUBDOMAIN}/g" \
      -e "s/{{EVENT_CONTEXT}}/${EVENT_CONTEXT}/g" \
      "templates/cloudevents/${f}.tmpl" > "${ce_dir}/${f}"
    echo "  generated ${ce_dir}/${f} (CloudEvents type prefix com.warehouse.${EVENT_SUBDOMAIN}.${EVENT_CONTEXT})"
  done
  gofmt -w "${ce_dir}" 2>/dev/null || true
else
  echo "  no <event-subdomain>/<event-context>: skipping CloudEvents helper (service has no Kafka I/O)"
fi

# .gremlins.yaml's {{MEASURED_*}} placeholders are deliberately NOT
# substituted here -- see the header comment above.
files_to_substitute=(
  Makefile
  .github/workflows/ci.yml
  internal/architecture/architecture_test.go
)

for f in "${files_to_substitute[@]}"; do
  if [[ ! -f "$f" ]]; then
    echo "::warning:: ${f} not found, skipping"
    continue
  fi
  sed -i.bak \
    -e "s/{{SERVICE_REPO}}/${SERVICE_REPO}/g" \
    -e "s/{{SERVICE}}/${SERVICE}/g" \
    -e "s/{{RICHEST_AGGREGATE}}/${RICHEST_AGGREGATE}/g" \
    "$f"
  rm -f "${f}.bak"
  echo "  substituted ${f}"
done

echo
echo "==> remaining placeholders (expected: only .gremlins.yaml's {{MEASURED_*}}, and any .claude/rules/*.md skeleton markers):"
grep -rn '{{' . --include='*.yaml' --include='*.yml' --include='Makefile' --include='*.go' --include='*.md' 2>/dev/null || echo "  (none found)"

echo
echo "==> next steps: see this script's own header comment, or HARNESS.md's"
echo "    'Instantiating this template' section."
