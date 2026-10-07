#!/usr/bin/env python3
"""Assert this chart's Service selector isolates the api component, and that the
chart refuses the two un-deployable configurations.

Why this exists (fleet-wide): `app.kubernetes.io/name` + `instance` are identical
on every pod a release creates. A Service that selects on those two alone selects
ALL of them (verified live in other fleet charts: a Kong request for /healthz
answered by the wrong pod). Every Deployment here carries
`app.kubernetes.io/component` in selector.matchLabels and its pod labels, and the
Service selects on it, so the Service selects EXACTLY ONE Deployment. Mirrors
warehouse-planning's charts/.../tests/test_service_selectors.py, minus the
components this chart does not have yet (mcp, frontend, analytics).

Run: python3 charts/network-inventory-planning/tests/test_service_selectors.py
Needs: helm, PyYAML.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

CHART_DIR = Path(__file__).resolve().parents[1]
RELEASE = "network-inventory-planning"

# Dummy DSN, no password: the chart refuses to render without a database source.
BASE = ["--set", "database.url=postgres://u@example.invalid:5432/db"]
ENABLE_EVERYTHING = BASE + [
    "--set", "kafka.enabled=true",
    "--set", "outbox.relayEnabled=true",
    "--set", "consumers.transferReplyGroup=g",
    "--set", "transfer.pickPathId=PICK",
    "--set", "gatewayApi.enabled=true",
    "--set", "ingress.enabled=true",
]


def helm(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["helm", "template", RELEASE, str(CHART_DIR), *args], capture_output=True, text=True
    )


def render(args: list[str]) -> list[dict]:
    out = helm(args)
    out.check_returncode()
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover - environment guard
        print("SKIP: PyYAML not available; cannot assert selectors", file=sys.stderr)
        raise SystemExit(0)
    return [d for d in yaml.safe_load_all(out.stdout) if d]


def matches(selector: dict, labels: dict) -> bool:
    return bool(selector) and all(labels.get(k) == v for k, v in selector.items())


def main() -> int:
    failures: list[str] = []

    docs = render(ENABLE_EVERYTHING)
    services = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Service"}
    deployments = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Deployment"}

    if RELEASE not in services:
        failures.append("the api Service was not rendered")
    elif services[RELEASE]["spec"]["selector"].get("app.kubernetes.io/component") != "api":
        failures.append("the api Service selector must pin component=api")

    for name, dep in deployments.items():
        match_labels = dep["spec"]["selector"].get("matchLabels") or {}
        pod_labels = dep["spec"]["template"]["metadata"].get("labels") or {}
        if "app.kubernetes.io/component" not in match_labels:
            failures.append(f"Deployment {name} selector.matchLabels lacks app.kubernetes.io/component")
        if not matches(match_labels, pod_labels):
            failures.append(f"Deployment {name} pod labels do not satisfy its own selector")

    for svc_name, svc in services.items():
        sel = svc["spec"].get("selector") or {}
        hit = [
            d for d, dep in deployments.items()
            if matches(sel, dep["spec"]["template"]["metadata"].get("labels") or {})
        ]
        if len(hit) != 1:
            failures.append(f"Service {svc_name} selects {len(hit)} Deployments {sorted(hit)}; expected exactly 1")

    # The consumers and the relay are env-gated: a default render must not turn any
    # of them on (an empty group id is how the binary keeps a consumer off).
    default_env = {
        e["name"]
        for d in render(BASE)
        if d.get("kind") == "Deployment"
        for e in d["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    for gated in (
        "OUTBOX_RELAY_ENABLED", "KAFKA_BROKERS", "TRANSFER_PICK_PATH_ID",
        "SITE_CAPABILITY_CONSUMER_GROUP", "SITE_SKU_DEMAND_CONSUMER_GROUP",
        "CAPACITY_PLAN_CONSUMER_GROUP", "TRANSFER_REPLY_CONSUMER_GROUP", "TRANSFER_FACT_CONSUMER_GROUP",
    ):
        if gated in default_env:
            failures.append(f"{gated} is rendered with default values; it must be opt-in")

    refused = helm([])
    if refused.returncode == 0 or "requires database.url or database.existingSecret" not in refused.stderr:
        failures.append("chart rendered (or failed for another reason) without database.url/existingSecret")

    refused = helm(BASE + ["--set", "outbox.relayEnabled=true"])
    if refused.returncode == 0 or "kafka.enabled is false" not in refused.stderr:
        failures.append("chart rendered (or failed for another reason) with the outbox relay but no kafka")

    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        return 1

    print(f"PASS: {len(services)} Service(s) each select exactly one Deployment; "
          "consumers, relay and pick path are opt-in; chart refuses to render without a "
          "database source and refuses the outbox relay without kafka")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
