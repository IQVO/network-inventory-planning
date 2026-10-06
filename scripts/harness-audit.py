#!/usr/bin/env python3
"""harness-audit -- Phase 6 Task 6.2 of the harness-coverage-expansion plan.

Reads each repo's origin/develop state (never a local working tree, which
can be dirty/on an unrelated branch -- see warehouse-systems-fleet-ops
skill's "Never trust a local checkout's working tree" lesson) and scores
harness coverage per repo, per regulation category (maintainability /
architecture fitness / behaviour), against the manifest below. Prints a
per-repo report and a fleet-wide summary table.

USAGE
    python3 harness-audit.py [--repos-root ~/warehouse-systems] [--json]

Exit code: 0 always (this is a REPORTING tool, not a gate -- it answers
"which repo's harness is behind", it does not block anything. Run it via
Hermes cron for a periodic drift report, per the plan's Task 6.2).

WHY A MANIFEST, NOT A HARDCODED CHECK LIST
Each sensor/guide is declared once below (name, category, how to detect
it) instead of being open-coded inline, so the manifest and this script's
logic stay separable -- extending coverage (a new sensor lands in the
template) means adding one manifest entry, not touching detection logic
scattered through the file.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_REPOS = [
    "inventory-storage",
    "order-management",
    "wes-work-planning",
    "fulfillment-execution",
    "workforce-management",
    "facility-layout",
    "labor-performance",
    "process-path-management",
    "warehouse-ops-agent",
    "network-fulfillment",
    "warehouse-planning",
]

CATEGORY_MAINTAINABILITY = "maintainability"
CATEGORY_ARCH_FITNESS = "architecture fitness"
CATEGORY_BEHAVIOUR = "behaviour"


@dataclass
class ManifestItem:
    name: str
    category: str
    # kind: "ci_job" checks .github/workflows/ci.yml for a top-level job
    # key; "file" checks a file exists (git ls-tree); "file_glob" checks
    # at least one file matches a prefix.
    kind: str
    target: str
    # Only meaningful for ci_job: some sensors are legitimately absent in
    # repos with no web/ or no chart -- caller passes has_web/has_chart to
    # skip those without penalizing the denominator.
    requires: str | None = None


MANIFEST: list[ManifestItem] = [
    # --- maintainability: computational sensors ---
    ManifestItem("lint", CATEGORY_MAINTAINABILITY, "ci_job", "lint"),
    ManifestItem("test+coverage", CATEGORY_MAINTAINABILITY, "ci_job", "test"),
    ManifestItem("mutation-fast", CATEGORY_MAINTAINABILITY, "ci_job", "mutation-fast"),
    ManifestItem("mutation (weekly)", CATEGORY_MAINTAINABILITY, "ci_job", "mutation"),
    ManifestItem("vuln", CATEGORY_MAINTAINABILITY, "ci_job", "vuln"),
    ManifestItem("api-lint", CATEGORY_MAINTAINABILITY, "ci_job", "api-lint"),
    ManifestItem("docs-api-drift", CATEGORY_MAINTAINABILITY, "ci_job", "docs-api-drift"),
    ManifestItem("drift (weekly)", CATEGORY_MAINTAINABILITY, "ci_job", "drift"),
    ManifestItem("web", CATEGORY_MAINTAINABILITY, "ci_job", "web", requires="has_web"),
    ManifestItem("helm-lint", CATEGORY_MAINTAINABILITY, "ci_job", "helm-lint", requires="has_chart"),
    ManifestItem("trivy-scan", CATEGORY_MAINTAINABILITY, "ci_job", "trivy-scan"),
    ManifestItem(".gremlins.yaml", CATEGORY_MAINTAINABILITY, "file", ".gremlins.yaml"),
    ManifestItem("lefthook.yml", CATEGORY_MAINTAINABILITY, "file", "lefthook.yml"),
    ManifestItem(".golangci.yml", CATEGORY_MAINTAINABILITY, "file", ".golangci.yml"),
    # --- maintainability: guides (feedforward) ---
    ManifestItem("AGENTS.md/CLAUDE.md", CATEGORY_MAINTAINABILITY, "file", "AGENTS.md"),
    ManifestItem(".claude/rules/", CATEGORY_MAINTAINABILITY, "file_glob", ".claude/rules/"),
    ManifestItem(".claude/skills/", CATEGORY_MAINTAINABILITY, "file_glob", ".claude/skills/"),
    # --- architecture fitness ---
    ManifestItem("arch-test CI job", CATEGORY_ARCH_FITNESS, "ci_job", "arch-test"),
    ManifestItem("architecture_test.go", CATEGORY_ARCH_FITNESS, "file", "internal/architecture/architecture_test.go"),
    ManifestItem("fitness_test.go (fleet invariants)", CATEGORY_ARCH_FITNESS, "file", "internal/architecture/fitness_test.go"),
    ManifestItem("architecture-review skill (inferential review)", CATEGORY_ARCH_FITNESS, "file",
                 ".claude/skills/architecture-review/SKILL.md"),
    ManifestItem("agent hooks (scripts/harness/hook.py)", CATEGORY_ARCH_FITNESS, "file", "scripts/harness/hook.py"),
    ManifestItem("guide-lint CI job", CATEGORY_MAINTAINABILITY, "ci_job", "guide-lint"),
    # --- behaviour ---
    ManifestItem("bdd CI job", CATEGORY_BEHAVIOUR, "ci_job", "bdd"),
    ManifestItem("integration CI job", CATEGORY_BEHAVIOUR, "ci_job", "integration"),
    ManifestItem("features/*.feature", CATEGORY_BEHAVIOUR, "file_glob", "features/"),
]


def run(cmd: list[str]) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    return result.stdout


def repo_file_list(repo: Path, ref: str = "origin/develop") -> list[str]:
    out = run(["git", "-C", str(repo), "ls-tree", "-r", "--name-only", ref])
    return out.splitlines() if out else []


def repo_ci_jobs(repo: Path, ref: str = "origin/develop") -> set[str]:
    """Top-level job keys under `jobs:` in .github/workflows/ci.yml."""
    content = run(["git", "-C", str(repo), "show", f"{ref}:.github/workflows/ci.yml"])
    if not content:
        return set()
    jobs: set[str] = set()
    in_jobs = False
    for line in content.splitlines():
        if line.startswith("jobs:"):
            in_jobs = True
            continue
        if not in_jobs:
            continue
        if line and not line.startswith(" ") and not line.startswith("#"):
            break  # left the jobs: block
        # a job key is exactly 2-space indented, ends with ':'
        if line.startswith("  ") and not line.startswith("    ") and line.rstrip().endswith(":"):
            key = line.strip().rstrip(":")
            if key:
                jobs.add(key)
    return jobs


def harness_template_version(repo: Path, ref: str = "origin/develop") -> str | None:
    content = run(["git", "-C", str(repo), "show", f"{ref}:AGENTS.md"])
    if not content:
        content = run(["git", "-C", str(repo), "show", f"{ref}:CLAUDE.md"])
    for line in content.splitlines():
        if "harness-template" in line:
            return line.strip()
    return None


@dataclass
class RepoAudit:
    name: str
    present: list[str] = field(default_factory=list)
    absent: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    category_scores: dict[str, tuple[int, int]] = field(default_factory=dict)
    harness_version: str | None = None


def audit_repo(repo: Path) -> RepoAudit:
    files = repo_file_list(repo)
    file_set = set(files)
    ci_jobs = repo_ci_jobs(repo)
    has_web = any(f.startswith("web/") for f in files)
    has_chart = any(f.startswith("charts/") and f.endswith("Chart.yaml") for f in files)

    audit = RepoAudit(name=repo.name)
    audit.harness_version = harness_template_version(repo)

    category_counts: dict[str, list[int]] = {}  # category -> [present, total]

    for item in MANIFEST:
        if item.requires == "has_web" and not has_web:
            audit.skipped.append(item.name)
            continue
        if item.requires == "has_chart" and not has_chart:
            audit.skipped.append(item.name)
            continue

        if item.kind == "ci_job":
            found = item.target in ci_jobs
        elif item.kind == "file":
            found = item.target in file_set
        elif item.kind == "file_glob":
            found = any(f.startswith(item.target) for f in files)
        else:
            found = False

        counts = category_counts.setdefault(item.category, [0, 0])
        counts[1] += 1
        if found:
            counts[0] += 1
            audit.present.append(item.name)
        else:
            audit.absent.append(item.name)

    audit.category_scores = {cat: (p, t) for cat, (p, t) in category_counts.items()}
    return audit


def format_pct(present: int, total: int) -> str:
    if total == 0:
        return "n/a"
    return f"{100 * present / total:.0f}%"


def print_report(audits: list[RepoAudit]) -> None:
    categories = [CATEGORY_MAINTAINABILITY, CATEGORY_ARCH_FITNESS, CATEGORY_BEHAVIOUR]

    print("=" * 100)
    print("harness-audit — fleet-wide harness coverage report")
    print("=" * 100)

    header = f"{'repo':<26}" + "".join(f"{c[:14]:<16}" for c in categories) + f"{'template':<12}"
    print(header)
    print("-" * len(header))
    for a in audits:
        row = f"{a.name:<26}"
        for cat in categories:
            p, t = a.category_scores.get(cat, (0, 0))
            row += f"{format_pct(p, t):<16}"
        row += f"{(a.harness_version or 'unversioned'):<12}"
        print(row)

    print()
    print("=" * 100)
    print("per-repo gaps (absent items)")
    print("=" * 100)
    for a in audits:
        if not a.absent:
            print(f"{a.name}: fully covered against the manifest")
            continue
        print(f"{a.name}:")
        for item in a.absent:
            print(f"  MISSING  {item}")
        if a.skipped:
            print(f"  (n/a, not applicable: {', '.join(a.skipped)})")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repos-root", default=str(Path.home() / "warehouse-systems"))
    parser.add_argument("--repos", nargs="*", default=DEFAULT_REPOS)
    parser.add_argument("--json", action="store_true", help="emit JSON instead of the text report")
    args = parser.parse_args()

    root = Path(args.repos_root)
    audits = []
    for repo_name in args.repos:
        repo_path = root / repo_name
        if not (repo_path / ".git").exists():
            print(f"::warning:: {repo_path} is not a git repo, skipping", file=sys.stderr)
            continue
        audits.append(audit_repo(repo_path))

    if args.json:
        out = [
            {
                "repo": a.name,
                "harness_version": a.harness_version,
                "category_scores": {
                    cat: {"present": p, "total": t} for cat, (p, t) in a.category_scores.items()
                },
                "present": a.present,
                "absent": a.absent,
                "skipped": a.skipped,
            }
            for a in audits
        ]
        print(json.dumps(out, indent=2))
    else:
        print_report(audits)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
