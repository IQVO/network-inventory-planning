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
    "DATABASE_URL",
    "MIGRATIONS_DATABASE_URL",
    "KAFKA_BROKERS",
    "SITE_CAPABILITY_CONSUMER_GROUP",
    "SITE_SKU_DEMAND_CONSUMER_GROUP",
    "CAPACITY_PLAN_CONSUMER_GROUP",
    "TRANSFER_REPLY_CONSUMER_GROUP",
    "TRANSFER_FACT_CONSUMER_GROUP",
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
    "SITE_CAPABILITY_CONSUMER_GROUP",
    "SITE_SKU_DEMAND_CONSUMER_GROUP",
    "CAPACITY_PLAN_CONSUMER_GROUP",
    "TRANSFER_REPLY_CONSUMER_GROUP",
    "TRANSFER_FACT_CONSUMER_GROUP",
    "HTTP_ADDR",
    "MCP_API_KEY",
    "API_KEY",
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
    "SITE_CAPABILITY_CONSUMER_GROUP",
    "SITE_SKU_DEMAND_CONSUMER_GROUP",
    "CAPACITY_PLAN_CONSUMER_GROUP",
    "TRANSFER_REPLY_CONSUMER_GROUP",
    "TRANSFER_FACT_CONSUMER_GROUP",
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

    # Without a database source the MCP still renders (tools answer
    # read-side-unavailable) but never wires DATABASE_URL.
    bare_plain, bare_from_ref = component_env(render(["--set", "mcp.enabled=true"]), "mcp")
    if "DATABASE_URL" in bare_plain or "DATABASE_URL" in bare_from_ref:
        failures.append("MCP rendered DATABASE_URL without a database source")


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

    # Default values: no kafka env at all (groups are off switches), no
    # DATABASE_URL (no secret set), no transfer_* release config.
    default_plain, default_from_ref = deployment_env(render([]))
    leaked = sorted(
        (set(default_plain) | default_from_ref)
        & {
            "KAFKA_BROKERS",
            "SITE_CAPABILITY_CONSUMER_GROUP",
            "SITE_SKU_DEMAND_CONSUMER_GROUP",
            "CAPACITY_PLAN_CONSUMER_GROUP",
            "TRANSFER_REPLY_CONSUMER_GROUP",
            "TRANSFER_FACT_CONSUMER_GROUP",
            "DATABASE_URL",
            "MIGRATIONS_DATABASE_URL",
            "TRANSFER_PICK_PATH_ID",
            "TRANSFER_DISPATCH_PATH_ID",
        }
    )
    if leaked:
        failures.append(f"env vars leaked with default values: {leaked}")

    # Routing is opt-in and renders the right kind.
    kinds = {d["kind"] for d in render(["--set", "database.existingSecret=x", "--set", "gatewayApi.enabled=true",
                                         "--set", "gatewayApi.parentRefs[0].name=gw", "--set", "ingress.enabled=true"])}
    if not {"HTTPRoute", "Ingress"} <= kinds:
        failures.append(f"routing templates did not render when enabled: {sorted(kinds)}")
    default_kinds = {d["kind"] for d in render(["--set", "database.existingSecret=x"])}
    if default_kinds & {"HTTPRoute", "Ingress"}:
        failures.append("routing resources rendered with default values; they must be opt-in")

    check_mcp(failures)
    check_analytics(failures)

    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        return 1

    print(f"PASS: all {len(EXPECTED_ENV)} api env vars wire correctly; "
          "defaults leak no kafka/database/release config; "
          f"mcp wires {len(MCP_EXPECTED_ENV)} env vars, none of them kafka/relay/auth, and is off by default; "
          f"analytics-projector wires {len(PROJECTOR_EXPECTED_ENV)} and analytics-reports {len(REPORTS_EXPECTED_ENV)} env vars "
          "(DSNs via secretKeyRef, no OLTP database), both off by default")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
