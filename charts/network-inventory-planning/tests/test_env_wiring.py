#!/usr/bin/env python3
"""Assert the chart's env wiring matches the binary's contract.

Fails if the rendered Deployment, with everything enabled, misses any env
var the binary reads (cmd/network-inventory-planning/main.go) or wires any
consumer group while kafka.enabled=false (each group env var is its own off
switch and must NOT render by default).

Run: python3 charts/network-inventory-planning/tests/test_env_wiring.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

CHART_DIR = Path(__file__).resolve().parents[1]

ENABLE_EVERYTHING = [
    "--set", "database.existingSecret=nip-database",
    "--set", "kafka.enabled=true",
    "--set", "kafka.siteCapabilityConsumerGroup=nip-site-capability",
    "--set", "kafka.siteSkuDemandConsumerGroup=nip-site-sku-demand",
    "--set", "kafka.capacityPlanConsumerGroup=nip-capacity-plan",
    "--set", "kafka.transferReplyConsumerGroup=nip-transfer-reply",
    "--set", "kafka.transferFactConsumerGroup=nip-transfer-fact",
    "--set", "kafka.packLevelConsumerGroup=nip-pack-level",
    "--set", "config.transferPickPathId=pick-path",
    "--set", "config.transferPickCptOffset=90m",
    "--set", "config.transferDispatchPathId=dispatch-path",
    "--set", "config.transferDispatchCptOffset=4h",
    "--set", "config.maxStaleness=5m",
]

EXPECTED_ENV = {
    "HTTP_ADDR",
    "PLANNING_MAX_STALENESS",
    "TRANSFER_PICK_PATH_ID",
    "TRANSFER_PICK_CPT_OFFSET",
    "TRANSFER_DISPATCH_PATH_ID",
    "TRANSFER_DISPATCH_CPT_OFFSET",
    "OUTBOX_RELAY_ENABLED",
    "OUTBOX_MAX_ATTEMPTS",
    "DATABASE_URL",
    "MIGRATIONS_DATABASE_URL",
    "KAFKA_BROKERS",
    "SITE_CAPABILITY_CONSUMER_GROUP",
    "SITE_SKU_DEMAND_CONSUMER_GROUP",
    "CAPACITY_PLAN_CONSUMER_GROUP",
    "TRANSFER_REPLY_CONSUMER_GROUP",
    "TRANSFER_FACT_CONSUMER_GROUP",
    "PACK_LEVEL_CONSUMER_GROUP",
}


def render(extra_args: list[str]) -> list[dict]:
    out = subprocess.run(
        ["helm", "template", "nip", str(CHART_DIR), *extra_args],
        capture_output=True, text=True, check=True,
    ).stdout
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover - environment guard
        print("SKIP: PyYAML not available; cannot assert env wiring", file=sys.stderr)
        raise SystemExit(0)
    return [d for d in yaml.safe_load_all(out) if d]


def component_env(docs: list[dict], component: str) -> tuple[dict[str, str], set[str]]:
    """Return ({name: plain value}, {names wired via valueFrom}) of the first
    container of the Deployment carrying app.kubernetes.io/component=<component>."""
    for d in docs:
        if d.get("kind") != "Deployment":
            continue
        if d["spec"]["template"]["metadata"]["labels"].get("app.kubernetes.io/component") != component:
            continue
        containers = d["spec"]["template"]["spec"]["containers"]
        plain = {
            e["name"]: e.get("value", "")
            for e in containers[0]["env"]
            if "value" in e
        }
        from_ref = {
            e["name"]
            for e in containers[0]["env"]
            if "valueFrom" in e
        }
        return plain, from_ref
    return {}, set()


def deployment_env(docs: list[dict]) -> tuple[dict[str, str], set[str]]:
    """The api Deployment's env (the MCP Deployment, when enabled, is separate)."""
    return component_env(docs, "api")


# Env cmd/mcp reads (cmd/mcp/main.go) and that the chart must wire.
MCP_EXPECTED_ENV = {
    "MCP_ADDR",
    "MIGRATIONS_PATH",
    "LOG_LEVEL",
    "PLANNING_MAX_STALENESS",
    "DATABASE_URL",
    "MIGRATIONS_DATABASE_URL",
}

# Env the MCP binary must NEVER be given: it dials no Kafka, runs no relay and
# no consumer, and has no auth.
MCP_FORBIDDEN_ENV = {
    "KAFKA_BROKERS",
    "OUTBOX_RELAY_ENABLED",
    "OUTBOX_MAX_ATTEMPTS",
    "SITE_CAPABILITY_CONSUMER_GROUP",
    "SITE_SKU_DEMAND_CONSUMER_GROUP",
    "CAPACITY_PLAN_CONSUMER_GROUP",
    "TRANSFER_REPLY_CONSUMER_GROUP",
    "TRANSFER_FACT_CONSUMER_GROUP",
    "PACK_LEVEL_CONSUMER_GROUP",
    "HTTP_ADDR",
    "MCP_API_KEY",
    "API_KEY",
}

# Env cmd/a2a reads (cmd/a2a/main.go) and that the chart must wire. Both
# DATABASE_URL and A2A_AGENT_URL are mandatory at boot.
A2A_EXPECTED_ENV = {
    "A2A_ADDR",
    "A2A_AGENT_URL",
    "MIGRATIONS_PATH",
    "LOG_LEVEL",
    "DATABASE_URL",
    "MIGRATIONS_DATABASE_URL",
}

# Env the A2A binary must NEVER be given: the agent is read-only, dials no
# Kafka (so no brokers and no EVENT_PUBLISHER), runs no relay and no consumer,
# and has no auth.
A2A_FORBIDDEN_ENV = MCP_FORBIDDEN_ENV | {
    "EVENT_PUBLISHER",
    "MCP_ADDR",
    "PLANNING_MAX_STALENESS",
    "A2A_API_KEY",
}

# Env cmd/nip-projector reads (cmd/nip-projector/main.go) and that the chart
# must wire. It writes ONLY the analytical database: never the OLTP DSNs, the
# outbox relay or the OLTP consumer groups.
PROJECTOR_EXPECTED_ENV = {
    "ADMIN_ADDR",
    "KAFKA_BROKERS",
    "ANALYTICS_CONSUMER_GROUP",
    "ANALYTICS_MIGRATIONS_PATH",
    "LOG_LEVEL",
    "ANALYTICS_DATABASE_URL",
}
PROJECTOR_FORBIDDEN_ENV = {
    "DATABASE_URL",
    "MIGRATIONS_DATABASE_URL",
    "OUTBOX_RELAY_ENABLED",
    "OUTBOX_MAX_ATTEMPTS",
    "SITE_CAPABILITY_CONSUMER_GROUP",
    "SITE_SKU_DEMAND_CONSUMER_GROUP",
    "CAPACITY_PLAN_CONSUMER_GROUP",
    "TRANSFER_REPLY_CONSUMER_GROUP",
    "TRANSFER_FACT_CONSUMER_GROUP",
    "PACK_LEVEL_CONSUMER_GROUP",
    "ANALYTICS_READER_DATABASE_URL",
    "MCP_API_KEY",
    "API_KEY",
}

# Env cmd/nip-reports reads (cmd/nip-reports/main.go). Read-only over the
# analytical database: no Kafka, no OLTP database, no projector DSN.
REPORTS_EXPECTED_ENV = {
    "HTTP_ADDR",
    "LOG_LEVEL",
    "ANALYTICS_READER_DATABASE_URL",
}
REPORTS_FORBIDDEN_ENV = {
    "DATABASE_URL",
    "MIGRATIONS_DATABASE_URL",
    "ANALYTICS_DATABASE_URL",
    "KAFKA_BROKERS",
    "ANALYTICS_CONSUMER_GROUP",
    "OUTBOX_RELAY_ENABLED",
    "OUTBOX_MAX_ATTEMPTS",
    "MCP_API_KEY",
    "API_KEY",
}


def check_mcp(failures: list[str]) -> None:
    docs = render(ENABLE_EVERYTHING + ["--set", "mcp.enabled=true"])
    plain, from_ref = component_env(docs, "mcp")
    if not plain and not from_ref:
        failures.append("mcp.enabled=true rendered no MCP Deployment")
        return
    missing = MCP_EXPECTED_ENV - set(plain) - from_ref
    if missing:
        failures.append(f"MCP env vars missing with everything enabled: {sorted(missing)}")
    for dsn in ("DATABASE_URL", "MIGRATIONS_DATABASE_URL"):
        if dsn in plain or dsn not in from_ref:
            failures.append(f"MCP {dsn} must be wired via secretKeyRef, not a plain value")
    if plain.get("MCP_ADDR") != ":8090":
        failures.append(f"MCP_ADDR = {plain.get('MCP_ADDR')!r}, want ':8090'")
    forbidden = sorted((set(plain) | from_ref) & MCP_FORBIDDEN_ENV)
    if forbidden:
        failures.append(f"MCP Deployment must not carry {forbidden} (no Kafka, relay, consumers or auth)")
    # The api Deployment is untouched by enabling the MCP.
    api_plain, api_from_ref = deployment_env(docs)
    if "MCP_ADDR" in api_plain or "MCP_ADDR" in api_from_ref:
        failures.append("the api Deployment must not carry MCP_ADDR")

    # MCP is opt-in: the default render has no MCP Deployment/Service.
    default_kinds = {
        (d["kind"], d["metadata"]["name"]) for d in render(["--set", "database.existingSecret=x"])
    }
    if any(name.endswith("-mcp") for _, name in default_kinds):
        failures.append("MCP resources rendered with default values; mcp.enabled must default to false")

    # Since the requireDatabase gate, a render without a database source is
    # refused outright (asserted in the defaults block), so the MCP is always
    # rendered WITH a source: its DATABASE_URL must come from the Secret.
    mcp_plain, mcp_from_ref = component_env(render(["--set", "database.existingSecret=x", "--set", "mcp.enabled=true"]), "mcp")
    if "DATABASE_URL" in mcp_plain:
        failures.append("MCP rendered a literal DATABASE_URL instead of the Secret")
    if "DATABASE_URL" not in mcp_from_ref:
        failures.append("MCP did not render DATABASE_URL from the database Secret")


def check_a2a(failures: list[str]) -> None:
    import re

    docs = render(ENABLE_EVERYTHING + ["--set", "a2a.enabled=true"])
    plain, from_ref = component_env(docs, "a2a")
    if not plain and not from_ref:
        failures.append("a2a.enabled=true rendered no A2A Deployment")
        return
    missing = A2A_EXPECTED_ENV - set(plain) - from_ref
    if missing:
        failures.append(f"A2A env vars missing with everything enabled: {sorted(missing)}")
    for dsn in ("DATABASE_URL", "MIGRATIONS_DATABASE_URL"):
        if dsn in plain or dsn not in from_ref:
            failures.append(f"A2A {dsn} must be wired via secretKeyRef, not a plain value")
    if plain.get("A2A_ADDR") != ":8093":
        failures.append(f"A2A_ADDR = {plain.get('A2A_ADDR')!r}, want ':8093'")
    forbidden = sorted((set(plain) | from_ref) & A2A_FORBIDDEN_ENV)
    if forbidden:
        failures.append(f"A2A Deployment must not carry {forbidden} (read-only: no Kafka, relay, consumers or auth)")

    # The Agent Card advertises a dialable absolute URL: it defaults to the
    # release's in-cluster Service DNS (namespace-derived, so pattern-match) and
    # stays overridable.
    url = plain.get("A2A_AGENT_URL", "")
    if not re.fullmatch(r"http://nip-network-inventory-planning-a2a\.[a-z0-9-]+\.svc\.cluster\.local:8093/", url):
        failures.append(f"A2A_AGENT_URL default is not the in-cluster Service DNS: {url!r}")
    override = "http://gw.example/api/a2a/network-inventory-planning/"
    over_plain, _ = component_env(
        render(["--set", "database.existingSecret=x", "--set", "a2a.enabled=true", "--set", f"a2a.agentUrl={override}"]),
        "a2a",
    )
    if over_plain.get("A2A_AGENT_URL") != override:
        failures.append(f"a2a.agentUrl did not override A2A_AGENT_URL: {over_plain.get('A2A_AGENT_URL')!r}")

    # Every variable cmd/a2a's boot config names as REQUIRED is rendered, derived
    # from the Go source so a new mandatory variable fails until the chart has it.
    required = set()
    for source in sorted((CHART_DIR.parents[1] / "cmd" / "a2a").glob("*.go")):
        if source.name.endswith("_test.go"):
            continue
        required |= set(re.findall(r"([A-Z][A-Z0-9_]+) is required", source.read_text()))
    if not required:
        failures.append("found no mandatory variables in cmd/a2a; the scan is looking in the wrong place")
    unrendered = sorted(required - set(plain) - from_ref)
    if unrendered:
        failures.append(f"the A2A Deployment does not render {unrendered}, which cmd/a2a requires")

    # The api Deployment is untouched by enabling the A2A agent.
    api_plain, api_from_ref = deployment_env(docs)
    if "A2A_ADDR" in api_plain or "A2A_AGENT_URL" in api_plain or "A2A_ADDR" in api_from_ref:
        failures.append("the api Deployment must not carry A2A env")

    # A2A is opt-in: the default render has no A2A resource at all.
    default_kinds = {
        (d["kind"], d["metadata"]["name"]) for d in render(["--set", "database.existingSecret=x"])
    }
    if any(name.endswith("-a2a") for _, name in default_kinds):
        failures.append("A2A resources rendered with default values; a2a.enabled must default to false")

    # DATABASE_URL comes from the Secret even when only a Secret name is given.
    a2a_plain, a2a_from_ref = component_env(render(["--set", "database.existingSecret=x", "--set", "a2a.enabled=true"]), "a2a")
    if "DATABASE_URL" in a2a_plain or "DATABASE_URL" not in a2a_from_ref:
        failures.append("A2A did not render DATABASE_URL from the database Secret")


def check_analytics(failures: list[str]) -> None:
    """The analytics projector and reports wire exactly the env their binaries read."""
    args = ENABLE_EVERYTHING + [
        "--set", "analytics.enabled=true",
        "--set", "analytics.database.existingSecret=nip-analytics",
    ]
    docs = render(args)
    for component, expected, forbidden in (
        ("analytics-projector", PROJECTOR_EXPECTED_ENV, PROJECTOR_FORBIDDEN_ENV),
        ("analytics-reports", REPORTS_EXPECTED_ENV, REPORTS_FORBIDDEN_ENV),
    ):
        plain, from_ref = component_env(docs, component)
        if not plain and not from_ref:
            failures.append(f"analytics.enabled=true rendered no {component} Deployment")
            continue
        missing = expected - set(plain) - from_ref
        if missing:
            failures.append(f"{component} env vars missing: {sorted(missing)}")
        leaked = sorted((set(plain) | from_ref) & forbidden)
        if leaked:
            failures.append(f"{component} must not carry {leaked}")
        for dsn in {"ANALYTICS_DATABASE_URL", "ANALYTICS_READER_DATABASE_URL"} & expected:
            if dsn in plain or dsn not in from_ref:
                failures.append(f"{component} {dsn} must be wired via secretKeyRef, not a plain value")

    plain, _ = component_env(docs, "analytics-projector")
    if plain.get("ANALYTICS_CONSUMER_GROUP") != "network-inventory-planning-analytics":
        failures.append(f"ANALYTICS_CONSUMER_GROUP = {plain.get('ANALYTICS_CONSUMER_GROUP')!r}, want 'network-inventory-planning-analytics'")
    if plain.get("ANALYTICS_MIGRATIONS_PATH") != "analytics/migrations" or plain.get("ADMIN_ADDR") != ":8091":
        failures.append("projector ANALYTICS_MIGRATIONS_PATH / ADMIN_ADDR defaults are wrong")
    plain, _ = component_env(docs, "analytics-reports")
    if plain.get("HTTP_ADDR") != ":8092":
        failures.append(f"reports HTTP_ADDR = {plain.get('HTTP_ADDR')!r}, want ':8092'")

    # The reports reader is fed the READER DSN key, the projector the writer's.
    for d in docs:
        if d.get("kind") == "Deployment":
            comp = d["spec"]["template"]["metadata"]["labels"].get("app.kubernetes.io/component")
            keys = {
                e["name"]: e["valueFrom"]["secretKeyRef"]["key"]
                for e in d["spec"]["template"]["spec"]["containers"][0]["env"]
                if "valueFrom" in e
            }
            if comp == "analytics-reports" and keys.get("ANALYTICS_READER_DATABASE_URL") != "ANALYTICS_READER_DATABASE_URL":
                failures.append("the reports Deployment must read the reader DSN key")
            if comp == "analytics-projector" and keys.get("ANALYTICS_DATABASE_URL") != "ANALYTICS_DATABASE_URL":
                failures.append("the projector Deployment must read the writer DSN key")

    # The api Deployment is untouched by enabling analytics.
    api_plain, api_from_ref = deployment_env(docs)
    leaked = sorted((set(api_plain) | api_from_ref) & {"ANALYTICS_DATABASE_URL", "ANALYTICS_READER_DATABASE_URL", "ANALYTICS_CONSUMER_GROUP"})
    if leaked:
        failures.append(f"the api Deployment must not carry analytics env {leaked}")

    # Analytics is opt-in: the default render has no analytics Deployment/Service/Secret.
    default_names = {d["metadata"]["name"] for d in render(["--set", "database.existingSecret=x"]) if d.get("metadata")}
    if any(n.endswith(("-projector", "-reports", "-analytics")) for n in default_names):
        failures.append("analytics resources rendered with default values; analytics.enabled must default to false")


def main() -> int:
    failures: list[str] = []

    plain, from_ref = deployment_env(render(ENABLE_EVERYTHING))
    missing = EXPECTED_ENV - set(plain) - from_ref
    if missing:
        failures.append(f"env vars missing with everything enabled: {sorted(missing)}")

    # Both DSNs must come from a secretKeyRef, never a literal value.
    for dsn in ("DATABASE_URL", "MIGRATIONS_DATABASE_URL"):
        if dsn in plain or dsn not in from_ref:
            failures.append(f"{dsn} must be wired via secretKeyRef, not a plain value")

    # OUTBOX_MAX_ATTEMPTS (ADR 0016): the relay attempt cap defaults to the
    # fleet's 20 and 0 (unlimited) renders as is, never dropped as empty.
    if plain.get("OUTBOX_MAX_ATTEMPTS") != "20":
        failures.append(f"OUTBOX_MAX_ATTEMPTS = {plain.get('OUTBOX_MAX_ATTEMPTS')!r}, want '20' by default (ADR 0016)")
    tuned, _ = deployment_env(render(ENABLE_EVERYTHING + ["--set-string", "config.outboxMaxAttempts=0"]))
    if tuned.get("OUTBOX_MAX_ATTEMPTS") != "0":
        failures.append(f"config.outboxMaxAttempts=0 rendered {tuned.get('OUTBOX_MAX_ATTEMPTS')!r}, want '0' (unlimited)")

    # Default values: no kafka env at all (groups are off switches), no
    # transfer_* release config. DATABASE_URL no longer belongs in the
    # "leaked" set: since the requireDatabase gate, a render with NO database
    # source is REFUSED (asserted below) instead of silently rendering a pod
    # the binary would kill at boot.
    default_plain, default_from_ref = deployment_env(render(["--set", "database.existingSecret=x"]))
    leaked = sorted(
        (set(default_plain) | default_from_ref)
        & {
            "KAFKA_BROKERS",
            "SITE_CAPABILITY_CONSUMER_GROUP",
            "SITE_SKU_DEMAND_CONSUMER_GROUP",
            "CAPACITY_PLAN_CONSUMER_GROUP",
            "TRANSFER_REPLY_CONSUMER_GROUP",
            "TRANSFER_FACT_CONSUMER_GROUP",
            "PACK_LEVEL_CONSUMER_GROUP",
            "TRANSFER_PICK_PATH_ID",
            "TRANSFER_DISPATCH_PATH_ID",
        }
    )
    if leaked:
        failures.append(f"env vars leaked with default values: {leaked}")

    # The database-source gate: no url and no existingSecret must refuse the
    # render (same fleet standard as every sibling chart).
    bare = subprocess.run(
        ["helm", "template", "nip", str(CHART_DIR)],
        capture_output=True, text=True,
    )
    if bare.returncode == 0 or "database.url or database.existingSecret" not in bare.stderr:
        failures.append("a render with no database source must fail naming database.url / database.existingSecret")

    # ADR 0017: the pack-level consumer group is MANDATORY when kafka.enabled:
    # the chart must refuse to render without it, and wire it when set.
    if plain.get("PACK_LEVEL_CONSUMER_GROUP") != "nip-pack-level":
        failures.append(f"PACK_LEVEL_CONSUMER_GROUP = {plain.get('PACK_LEVEL_CONSUMER_GROUP')!r}, want 'nip-pack-level'")
    refused = subprocess.run(
        ["helm", "template", "nip", str(CHART_DIR),
         "--set", "database.existingSecret=nip-database", "--set", "kafka.enabled=true"],
        capture_output=True, text=True,
    )
    if refused.returncode == 0 or "packLevelConsumerGroup" not in refused.stderr:
        failures.append("kafka.enabled=true without kafka.packLevelConsumerGroup must fail the render naming packLevelConsumerGroup")

    # Routing is opt-in and renders the right kind.
    kinds = {d["kind"] for d in render(["--set", "database.existingSecret=x", "--set", "gatewayApi.enabled=true",
                                         "--set", "gatewayApi.parentRefs[0].name=gw", "--set", "ingress.enabled=true"])}
    if not {"HTTPRoute", "Ingress"} <= kinds:
        failures.append(f"routing templates did not render when enabled: {sorted(kinds)}")
    default_kinds = {d["kind"] for d in render(["--set", "database.existingSecret=x"])}
    if default_kinds & {"HTTPRoute", "Ingress"}:
        failures.append("routing resources rendered with default values; they must be opt-in")

    check_mcp(failures)
    check_a2a(failures)
    check_analytics(failures)

    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        return 1

    print(f"PASS: all {len(EXPECTED_ENV)} api env vars wire correctly; "
          "defaults leak no kafka/database/release config; "
          f"mcp wires {len(MCP_EXPECTED_ENV)} env vars, none of them kafka/relay/auth, and is off by default; "
          f"a2a wires {len(A2A_EXPECTED_ENV)} env vars (agent URL defaulted + overridable, no kafka/relay/auth), and is off by default; "
          f"analytics-projector wires {len(PROJECTOR_EXPECTED_ENV)} and analytics-reports {len(REPORTS_EXPECTED_ENV)} env vars "
          "(DSNs via secretKeyRef, no OLTP database), both off by default")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
