#!/usr/bin/env python3
"""Assert this chart's Service selectors isolate each component.

Why this exists (fleet-wide): `app.kubernetes.io/name` + `instance` are identical
on every pod a release creates (the api, the MCP server, ...). A Service that
selects on those two alone selects ALL of them -- verified live in other fleet
charts, where a Kong request for an OLTP /healthz was answered by the wrong pod.
Every Deployment here carries `app.kubernetes.io/component` in selector.matchLabels
and its pod labels, and every Service selects on it, so each Service selects
EXACTLY ONE Deployment. This test fails if that ever stops being true.

It mirrors warehouse-planning's charts/.../tests/test_service_selectors.py,
trimmed to the components this chart has (api, mcp).

Run: python3 charts/network-inventory-planning/tests/test_service_selectors.py
Needs: helm, PyYAML.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

CHART_DIR = Path(__file__).resolve().parents[1]
RELEASE = "nip"
FULLNAME = "nip-network-inventory-planning"

BASE = ["--set", "database.existingSecret=nip-database"]
ENABLE_EVERYTHING = BASE + [
    "--set", "mcp.enabled=true",
    "--set", "autoscaling.enabled=true",
    "--set", "gatewayApi.enabled=true",
    "--set", "gatewayApi.parentRefs[0].name=gw",
    "--set", "ingress.enabled=true",
    "--set", "kafka.enabled=true",
]


def render(extra_args: list[str]) -> list[dict]:
    out = subprocess.run(
        ["helm", "template", RELEASE, str(CHART_DIR), *extra_args],
        capture_output=True, text=True, check=True,
    ).stdout
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover - environment guard
        print("SKIP: PyYAML not available; cannot assert selectors", file=sys.stderr)
        raise SystemExit(0)
    return [d for d in yaml.safe_load_all(out) if d]


def selector_of(doc: dict) -> dict:
    return doc.get("spec", {}).get("selector") or {}


def pod_labels_of(doc: dict) -> dict:
    return doc.get("spec", {}).get("template", {}).get("metadata", {}).get("labels") or {}


def matches(selector: dict, labels: dict) -> bool:
    return bool(selector) and all(labels.get(k) == v for k, v in selector.items())


def main() -> int:
    failures: list[str] = []

    docs = render(ENABLE_EVERYTHING)
    services = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Service"}
    deployments = {d["metadata"]["name"]: d for d in docs if d.get("kind") == "Deployment"}

    if FULLNAME not in services:
        failures.append(f"the api Service {FULLNAME} was not rendered")
    elif selector_of(services[FULLNAME]).get("app.kubernetes.io/component") != "api":
        failures.append("the api Service selector must pin component=api")

    mcp_name = f"{FULLNAME}-mcp"
    if mcp_name not in services:
        failures.append("the MCP Service was not rendered with mcp.enabled=true")
    elif selector_of(services[mcp_name]).get("app.kubernetes.io/component") != "mcp":
        failures.append("the MCP Service selector must pin component=mcp")
    if mcp_name not in deployments:
        failures.append("the MCP Deployment was not rendered with mcp.enabled=true")

    # The api Deployment pins component=api in BOTH selector.matchLabels and pod labels.
    api = deployments.get(FULLNAME)
    if api is None:
        failures.append(f"the api Deployment {FULLNAME} was not rendered")
    elif (api["spec"]["selector"].get("matchLabels") or {}).get("app.kubernetes.io/component") != "api" \
            or pod_labels_of(api).get("app.kubernetes.io/component") != "api":
        failures.append("the api Deployment must carry component=api in selector.matchLabels and its pod labels")

    mcp = deployments.get(mcp_name)
    if mcp is not None:
        if (mcp["spec"]["selector"].get("matchLabels") or {}).get("app.kubernetes.io/component") != "mcp" \
                or pod_labels_of(mcp).get("app.kubernetes.io/component") != "mcp":
            failures.append("the MCP Deployment must carry component=mcp in selector.matchLabels and its pod labels")
        if mcp["spec"]["template"]["spec"]["containers"][0].get("command") != ["/app/mcp"]:
            failures.append("the MCP container must run /app/mcp")

    # The HPA owns the api's replicas and scales the api Deployment only.
    for d in docs:
        if d.get("kind") == "HorizontalPodAutoscaler":
            if d["spec"]["scaleTargetRef"]["name"] != FULLNAME:
                failures.append("the HPA must scale the api Deployment")
            if "replicas" in (api or {}).get("spec", {}):
                failures.append("the api Deployment must omit replicas when its HPA owns them")

    # Routing (HTTPRoute / Ingress) points at the api Service, never the MCP one.
    for d in docs:
        if d.get("kind") == "Ingress":
            backends = [
                p["backend"]["service"]["name"]
                for rule in d["spec"].get("rules", [])
                for p in rule.get("http", {}).get("paths", [])
            ]
            if any(b != FULLNAME for b in backends):
                failures.append(f"Ingress backends {backends} must all be the api Service")
        if d.get("kind") == "HTTPRoute":
            backends = [
                ref["name"]
                for rule in d["spec"].get("rules", [])
                for ref in rule.get("backendRefs", [])
            ]
            if any(b != FULLNAME for b in backends):
                failures.append(f"HTTPRoute backends {backends} must all be the api Service")

    # Every Deployment's own selector must pin a component too, and be
    # satisfied by its pod labels.
    for name, dep in deployments.items():
        match_labels = dep["spec"]["selector"].get("matchLabels") or {}
        if "app.kubernetes.io/component" not in match_labels:
            failures.append(f"Deployment {name} selector.matchLabels lacks app.kubernetes.io/component")
        if not matches(match_labels, pod_labels_of(dep)):
            failures.append(f"Deployment {name} pod labels do not satisfy its own selector")

    # The real invariant: each Service selects exactly one Deployment.
    for svc_name, svc in services.items():
        sel = selector_of(svc)
        hit = [d for d, dep in deployments.items() if matches(sel, pod_labels_of(dep))]
        if len(hit) != 1:
            failures.append(
                f"Service {svc_name} selects {len(hit)} Deployments {sorted(hit)}; expected exactly 1"
            )

    # Default values must not deploy the MCP component at all, and the
    # lone api Service still selects exactly the api Deployment.
    default_docs = render(BASE)
    stray = [
        d["metadata"]["name"]
        for d in default_docs
        if d.get("metadata", {}).get("name", "").endswith("-mcp")
    ]
    if stray:
        failures.append(f"the MCP component rendered with default values: {stray}")
    default_deployments = {d["metadata"]["name"]: d for d in default_docs if d.get("kind") == "Deployment"}
    for d in default_docs:
        if d.get("kind") == "Service":
            hit = [n for n, dep in default_deployments.items() if matches(selector_of(d), pod_labels_of(dep))]
            if hit != [FULLNAME]:
                failures.append(f"default Service {d['metadata']['name']} selects {hit}; expected only {FULLNAME}")

    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        return 1

    print(f"PASS: {len(services)} Services each select exactly one Deployment; "
          "api pins component=api, mcp pins component=mcp and is off by default")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
