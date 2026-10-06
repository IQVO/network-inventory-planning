# SPEC.md — warehouse-harness-template

(Intended as CLAUDE.md, but that filename is protected in this
environment and requires the user's own manual creation/edit — see this
repo's own harness lesson about the protected-agent-filename write guard.
Please rename this file to CLAUDE.md, and symlink AGENTS.md -> CLAUDE.md
to match the fleet's convention, at your convenience.)

This repo is `harness-template: v2` for the warehouse-systems fleet's Go
bounded-context services. It is a TEMPLATE, not a runnable service: no
domain code, no `cmd/`, no `apis/openapi.yaml`.

## What's here

- `Makefile`, `.github/workflows/ci.yml`, `lefthook.yml`, `.golangci.yml`,
  `.gremlins.yaml` — the canonical local+CI quality gate, with
  `{{SERVICE}}` / `{{SERVICE_REPO}}` / `{{RICHEST_AGGREGATE}}` placeholders.
- `internal/architecture/architecture_test.go` + `fitness_test.go` — arch-go
  fitness tests. These COMPILE AND PASS as-is against this empty tree
  (there's nothing to violate yet) — `go test ./internal/architecture/...
  -v` is the fastest way to confirm you haven't broken the template.
- `.claude/rules/*.md` — skeleton guide files (domain model, REST API,
  integration events) with `<!-- fill in -->` placeholders, not fabricated
  content.
- `.claude/skills/how-to-*.md`, `.claude/commands/*.md` — reusable how-tos
  and review sensors, ported from `inventory-storage` (the fleet's richest
  reference harness) with a template note asking the instantiator to adapt
  every example to their own real code.
- `scripts/coverage-quality.py` — the drift job's "high line-coverage but
  a surviving mutant" detector, verbatim from inventory-storage (this
  script's logic is package-path-agnostic).
- `scripts/new-service.sh` — instantiation script. See its header comment.
- `HARNESS.md` — the manifest: every sensor's purpose/cost/lifecycle
  position, and the real incident behind every fitness test.
- `templates/cloudevents/*.go.tmpl` — the fleet reference CloudEvents 1.0
  helper + tests, generated into Kafka services by `new-service.sh`.

## Events: CloudEvents 1.0 is MANDATORY

Every Kafka message any service instantiated from this template produces or
consumes (integration `warehouse.<ctx>.events` AND analytics
`warehouse.<ctx>.analytics`) is a CloudEvents 1.0 event in structured
content mode. This is a hard fleet rule, not a preference, and the template
must only ever generate that:

- No flat envelope (`event_id`/`event_type`/`occurred_at`), no dual-write,
  no dual-read, no envelope toggle env var (`EVENT_ENVELOPE_MODE` is gone).
  Never add a template option, placeholder or skeleton that offers one;
  `TestNoEventEnvelopeToggleOrFlatEnvelope` fails CI if generated code does.
- Build/validate/(un)marshal with `github.com/cloudevents/sdk-go/v2/event`
  via `internal/adapters/kafka/cloudevents/` (generated from
  `templates/cloudevents/`); transport stays kafka-go.
- Kafka header `content-type: application/cloudevents+json; charset=UTF-8`.
- Required attributes: `specversion=1.0`, `id` (UUID, stable across outbox
  redelivery), `source=/warehouse/<repo>`, `type`, `subject` (aggregate
  id), `time` (occurred-at, UTC), `datacontenttype=application/json`,
  `dataschema=urn:warehouse:<repo>:<events|analytics>:<EventName>:v<N>`.
- `type` = `com.warehouse.<subdomain>.<bounded-context>.<entity>.<EventName>`;
  for a generated service the prefix is the `<event-subdomain>`/`<event-context>` passed to `new-service.sh`.
  Breaking payload change => new `.v2` type + new dataschema version, never mutate.
- Consumers dispatch on the FULL `type`, ignore unknown types, dedupe on
  `id`, and DLQ/skip (never crash, never parse a legacy shape) anything that
  fails CloudEvents validation.
- When editing `templates/cloudevents/`, re-prove it the way the PR that
  added it did: instantiate into a scratch copy with
  `new-service.sh demo agg wes demo-ctx`, `go get
  github.com/cloudevents/sdk-go/v2 github.com/segmentio/kafka-go`, `go test
  ./internal/...`.

Full standard and the fleet's cross-service type catalogue: warehouse-docs
`docs/strategic-design/event-standard-cloudevents.md`.

## Working in THIS repo (the template itself, not an instantiated copy)

- Before changing any sensor's config (`.golangci.yml`, `.gremlins.yaml`
  thresholds, CI job shape), check whether the change should also flow
  back into the 9 already-instantiated repos, or whether it's template-v2
  material. This repo has no `harness-audit` conformance checker yet
  (Task 6.2 of the harness-coverage-expansion plan) to catch that drift
  automatically — until it exists, changes here need a manual fan-out
  decision.
- `go build ./... && go vet ./... && gofmt -l .` should stay clean at all
  times — this repo has zero domain code, so there's no excuse for either
  to ever fail.
- `go test ./internal/architecture/... -v` should PASS on every commit —
  if it starts failing against this repo's own (nonexistent) domain code,
  something in the fitness tests' assumptions broke.

## When bumping the template version

Increment `harness-template: v<N>` (referenced in `HARNESS.md`'s
Versioning section and meant to be recorded in every instantiated repo's
own AGENTS.md) whenever a change here isn't purely additive/optional —
e.g. tightening a threshold, adding a new blocking CI job, changing a
placeholder's name. Purely-additive changes (a new advisory sensor, a new
`.claude/skills/` guide) don't need a version bump.
