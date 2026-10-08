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
trimmed to the components this chart has (api, mcp, and the optional
frontend: the nginx pod that serves the nip_mfe console remote, ADR 0010 --
its own workload, component=frontend, a ClusterIP Service, never routed by
this chart).

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
    "--set", "frontend.enabled=true",
    "--set", "autoscaling.enabled=true",
    "--set", "gatewayApi.enabled=true",
    "--set", "gatewayApi.parentRefs[0].name=gw",
    "--set", "ingress.enabled=true",
    "--set", "kafka.enabled=true",
    "--set", "analytics.enabled=true",
    "--set", "analytics.database.existingSecret=nip-analytics",
    "--set", "analytics.projector.autoscaling.enabled=true",
    "--set", "analytics.reports.autoscaling.enabled=true",
]

PROJECTOR = f"{FULLNAME}-projector"
REPORTS = f"{FULLNAME}-reports"


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

    # The optional frontend: its own Deployment + ClusterIP Service pinned to
    # component=frontend, serving the static nip_mfe remote on 8080.
    frontend_name = f"{FULLNAME}-frontend"
    if frontend_name not in services:
        failures.append("the frontend Service was not rendered with frontend.enabled=true")
    else:
        fe_svc = services[frontend_name]
        if selector_of(fe_svc).get("app.kubernetes.io/component") != "frontend":
            failures.append("the frontend Service selector must pin component=frontend")
        if fe_svc["spec"].get("type") != "ClusterIP":
            failures.append("the frontend Service must be ClusterIP")
    fe = deployments.get(frontend_name)
    if fe is None:
        failures.append("the frontend Deployment was not rendered with frontend.enabled=true")
    else:
        if (fe["spec"]["selector"].get("matchLabels") or {}).get("app.kubernetes.io/component") != "frontend" \
                or pod_labels_of(fe).get("app.kubernetes.io/component") != "frontend":
            failures.append("the frontend Deployment must carry component=frontend in selector.matchLabels and its pod labels")
        fe_pod = fe["spec"]["template"]["spec"]
        if fe_pod["containers"][0].get("command"):
            failures.append("the frontend container must run the image's own nginx entrypoint, not an api/mcp command")
        if not fe_pod["securityContext"].get("runAsNonRoot"):
            failures.append("the frontend pod must run as non-root (nginx-unprivileged)")
    # The api and MCP Services never select the frontend pod (and vice versa).
    for svc_name, comp in ((FULLNAME, "api"), (mcp_name, "mcp")):
        if svc_name in services and fe is not None and matches(selector_of(services[svc_name]), pod_labels_of(fe)):
            failures.append(f"Service {svc_name} (component={comp}) must not select the frontend pod")

    # Frontend routing belongs to warehouse-infra's Nginx web gateway: this
    # chart renders no Ingress/HTTPRoute for it, and the api routes point at
    # the api Service only (checked below).
    for d in docs:
        if d.get("kind") in {"Ingress", "HTTPRoute"} and "frontend" in d["metadata"]["name"]:
            failures.append(f"{d['kind']} {d['metadata']['name']}: frontend routing must not live in this chart")

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

    # Each HPA owns the replicas of EXACTLY the Deployment it scales: the api's
    # HPA the api, the analytics ones the projector / reports.
    hpa_targets = {}
    for d in docs:
        if d.get("kind") == "HorizontalPodAutoscaler":
            hpa_targets[d["metadata"]["name"]] = d["spec"]["scaleTargetRef"]["name"]
            if d["spec"]["scaleTargetRef"]["name"] not in deployments:
                failures.append(f"HPA {d['metadata']['name']} scales a Deployment that does not exist")
    if hpa_targets.get(FULLNAME) != FULLNAME:
        failures.append("the api HPA must scale the api Deployment")
    for name in (FULLNAME, PROJECTOR, REPORTS):
        if name in hpa_targets and "replicas" in deployments.get(name, {}).get("spec", {}):
            failures.append(f"the {name} Deployment must omit replicas when its HPA owns them")
    if set(hpa_targets) != {FULLNAME, PROJECTOR, REPORTS} or any(hpa_targets[n] != n for n in hpa_targets):
        failures.append(f"HPAs {hpa_targets} must be exactly one per scalable Deployment, each scaling its own")

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

    # The analytics read side (ADR 0009): projector + reports Deployments, and a
    # reports Service that selects exactly the reports Deployment.
    check_analytics(failures, services, deployments)

    # The real invariant: each Service selects exactly one Deployment.
    for svc_name, svc in services.items():
        sel = selector_of(svc)
        hit = [d for d, dep in deployments.items() if matches(sel, pod_labels_of(dep))]
        if len(hit) != 1:
            failures.append(
                f"Service {svc_name} selects {len(hit)} Deployments {sorted(hit)}; expected exactly 1"
            )

    # Default values must not deploy the MCP, frontend or analytics components at all,
    # and the lone api Service still selects exactly the api Deployment.
    default_docs = render(BASE)
    stray = [
        d["metadata"]["name"]
        for d in default_docs
        if d.get("metadata", {}).get("name", "").endswith(("-mcp", "-frontend", "-projector", "-reports", "-analytics"))
    ]
    if stray:
        failures.append(f"optional components rendered with default values: {stray}")
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
          "api pins component=api, mcp and frontend pin their own component, are ClusterIP and are off by default; "
          "analytics-projector / analytics-reports pin their components, are off by default and fail closed")
    return 0


def check_analytics(failures: list[str], services: dict, deployments: dict) -> None:
    """The projector and reports Deployments, the reports Service, the guards."""
    for name, component, command in (
        (PROJECTOR, "analytics-projector", ["/app/nip-projector"]),
        (REPORTS, "analytics-reports", ["/app/nip-reports"]),
    ):
        dep = deployments.get(name)
        if dep is None:
            failures.append(f"the {component} Deployment {name} was not rendered with analytics.enabled=true")
            continue
        if (dep["spec"]["selector"].get("matchLabels") or {}).get("app.kubernetes.io/component") != component \
                or pod_labels_of(dep).get("app.kubernetes.io/component") != component:
            failures.append(f"the {component} Deployment must carry component={component} in selector.matchLabels and its pod labels")
        if dep["spec"]["template"]["spec"]["containers"][0].get("command") != command:
            failures.append(f"the {component} container must run {command}")

    # The reports Service exists and selects the reports Deployment; the projector has no Service.
    svc = services.get(REPORTS)
    if svc is None:
        failures.append("the reports Service was not rendered with analytics.enabled=true")
    elif selector_of(svc).get("app.kubernetes.io/component") != "analytics-reports":
        failures.append("the reports Service selector must pin component=analytics-reports")
    if PROJECTOR in services:
        failures.append("the projector serves only its admin port to probes and must have no Service")

    # Render-time guards: analytics without a DSN source, or without kafka, must not render.
    no_dsn = [*BASE, "--set", "kafka.enabled=true", "--set", "analytics.enabled=true"]
    no_kafka = [*BASE, "--set", "analytics.enabled=true", "--set", "analytics.database.existingSecret=x"]
    for label, args in (("a DSN source", no_dsn), ("kafka.enabled", no_kafka)):
        proc = subprocess.run(["helm", "template", RELEASE, str(CHART_DIR), *args], capture_output=True, text=True)
        if proc.returncode == 0:
            failures.append(f"analytics.enabled=true without {label} must fail at render time")
        elif "analytics.enabled is true" not in proc.stderr:
            failures.append(f"the missing-{label} failure must name analytics.enabled, got: {proc.stderr.strip()[:200]}")

    # The chart creates its own Secret from the DSN values, with both keys.
    docs = render([*BASE, "--set", "kafka.enabled=true", "--set", "analytics.enabled=true",
                   "--set", "analytics.database.projectorUrl=postgres://p@h:5432/db"])
    secrets = [d for d in docs if d.get("kind") == "Secret" and d["metadata"]["name"].endswith("-analytics")]
    if len(secrets) != 1 or set(secrets[0].get("stringData", {})) != {"ANALYTICS_DATABASE_URL", "ANALYTICS_READER_DATABASE_URL"}:
        failures.append("analytics.database.projectorUrl must render one analytics Secret with both DSN keys")
    # With an existingSecret the chart creates none.
    docs = render(ENABLE_EVERYTHING)
    if any(d.get("kind") == "Secret" and d["metadata"]["name"].endswith("-analytics") for d in docs):
        failures.append("analytics.database.existingSecret must suppress the chart-created Secret")


if __name__ == "__main__":
    raise SystemExit(main())
