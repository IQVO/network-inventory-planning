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


def deployment_env(docs: list[dict]) -> tuple[dict[str, str], set[str]]:
    """Return ({name: plain value}, {names wired via valueFrom})."""
    for d in docs:
        if d.get("kind") == "Deployment":
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


def main() -> int:
    failures: list[str] = []

    plain, from_ref = deployment_env(render(ENABLE_EVERYTHING))
    missing = EXPECTED_ENV - set(plain) - from_ref
    if missing:
        failures.append(f"env vars missing with everything enabled: {sorted(missing)}")

    # DATABASE_URL must come from a secretKeyRef, never a literal value.
    if "DATABASE_URL" in plain or "DATABASE_URL" not in from_ref:
        failures.append("DATABASE_URL must be wired via secretKeyRef, not a plain value")

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
            "TRANSFER_PICK_PATH_ID",
            "TRANSFER_DISPATCH_PATH_ID",
        }
    )
    if leaked:
        failures.append(f"env vars leaked with default values: {leaked}")

    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        return 1

    print(f"PASS: all {len(EXPECTED_ENV)} env vars wire correctly; "
          "defaults leak no kafka/database/release config")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
