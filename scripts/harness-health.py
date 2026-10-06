#!/usr/bin/env python3
"""harness-health: does the harness WORK, not just exist? (harness-template v3, plan Phase 6.3)

harness-audit.py answers "is the file/job there" (presence). Presence hid real failures:
55 skill files that no runtime could load, scheduled sensors red for a week unnoticed.
harness-health adds the checks that catch those, per repo, from origin/develop + GitHub:

  guides   skills_ok / skills_flat   .claude/skills/<n>/SKILL.md with valid frontmatter vs flat files
  context  claude_lines, unscoped_rule_lines   always-loaded context size (lower is better)
  hooks    claude/codex/opencode adapters + scripts/harness/hook.py present
  ci       guide-lint job, repo_lint config sensors (cfg), ai-review workflow, make check-fast
  sync     managed files identical to this template (-N = N files drifted; the weekly sync opens the PR)
  runtime  last scheduled run per workflow (green/red), open harness:red issues
  version  harness-template: vN recorded in AGENTS.md

Exit code 0 always (reporting tool). `--json` for machines. `--fail-on-red` exits 1 if any
repo has a failing scheduled run or open harness:red issue (for cron alerting).

Usage: python3 harness-health.py [--repos-root ~/warehouse-systems] [--repos a b] [--org IQVO]
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_REPOS = ["inventory-storage", "facility-layout", "order-management", "fulfillment-execution",
                 "wes-work-planning", "workforce-management", "labor-performance", "process-path-management",
                 "warehouse-ops-agent", "network-fulfillment", "warehouse-planning"]


def run(cmd: list[str], cwd: str | None = None) -> str:
    return subprocess.run(cmd, capture_output=True, text=True, check=False, cwd=cwd).stdout


def show(repo: Path, path: str, ref: str = "origin/develop") -> str:
    return run(["git", "-C", str(repo), "show", f"{ref}:{path}"])


def frontmatter_ok(text: str, name: str) -> bool:
    if not text.startswith("---"):
        return False
    end = text.find("\n---", 3)
    if end < 0:
        return False
    fm = text[3:end]
    n = re.search(r"^name:\s*(\S+)", fm, re.M)
    d = re.search(r"^description:\s*(.+)", fm, re.M)
    return bool(n and n.group(1).strip("\"'") == name and d and len(d.group(1).strip()) > 10)


def load_audit():
    spec = importlib.util.spec_from_file_location("harness_audit", HERE / "harness-audit.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["harness_audit"] = mod  # dataclasses needs the module registered
    spec.loader.exec_module(mod)
    return mod


CORE_MANAGED = ["scripts/harness/hook.py", "scripts/harness/guide_lint.py", "scripts/harness/repo_lint.py",
                ".claude/settings.json"]


def managed_list() -> list[str]:
    """MANAGED file list from the template's own migrate_v3.py (single source of truth)."""
    txt = (HERE.parent / "tools" / "migrate_v3.py").read_text()
    block = txt[txt.index("MANAGED = ["):]
    block = block[:block.index("]")]
    return re.findall(r'"([^"]+)"', block)


def managed_drift(repo: Path, files: set) -> list[str]:
    """Managed files that differ from (or, for the core set, are missing vs) this template. Weekly sync closes them."""
    drift = []
    for rel in managed_list():
        src = HERE.parent / rel
        if not src.is_file():
            continue
        if rel not in files:
            if rel in CORE_MANAGED:
                drift.append(f"{rel} (missing)")
            continue
        if show(repo, rel) != src.read_text(errors="replace"):
            drift.append(rel)
    return drift


def health(repo: Path, org: str, audit_mod) -> dict:
    files = run(["git", "-C", str(repo), "ls-tree", "-r", "--name-only", "origin/develop"]).splitlines()
    fs = set(files)
    out: dict = {"repo": repo.name}
    out["drift"] = managed_drift(repo, fs)

    flat = [f for f in files if re.match(r"^\.claude/skills/[^/]+\.md$", f)]
    skill_dirs = sorted({f.split("/")[2] for f in files if re.match(r"^\.claude/skills/[^/]+/SKILL\.md$", f)})
    ok = [d for d in skill_dirs if frontmatter_ok(show(repo, f".claude/skills/{d}/SKILL.md"), d)]
    out["skills_ok"], out["skills_total"], out["skills_flat"] = len(ok), len(skill_dirs) + len(flat), len(flat)

    # CLAUDE.md or AGENTS.md may be the symlink (its blob is just the target name): take the real one
    out["claude_lines"] = max(len(show(repo, "CLAUDE.md").splitlines()), len(show(repo, "AGENTS.md").splitlines()))
    unscoped = scoped = 0
    for f in files:
        if re.match(r"^\.claude/rules/.+\.md$", f):
            t = show(repo, f)
            has_paths = t.startswith("---") and re.search(r"^paths:", t.split("\n---", 1)[0], re.M)
            n = len(t.splitlines())
            if has_paths:
                scoped += n
            else:
                unscoped += n
    out["unscoped_rule_lines"], out["scoped_rule_lines"] = unscoped, scoped

    out["hooks"] = {
        "claude": ".claude/settings.json" in fs and "scripts/harness/hook.py" in fs,
        "codex": ".codex/hooks.json" in fs,
        "opencode": ".opencode/plugins/harness.ts" in fs,
        "codex_skills_link": ".agents/skills" in fs,
    }
    ci = audit_mod.repo_ci_jobs(repo)
    out["ci"] = {"guide-lint": "guide-lint" in ci, "ai-review": ".github/workflows/ai-review.yml" in fs,
                 "check-fast": "check-fast:" in show(repo, "Makefile"),
                 # config sensors (repo_lint.py) shipped AND wired into the CI guide-lint job
                 "repo-lint": "scripts/harness/repo_lint.py" in fs
                 and "repo_lint.py" in show(repo, ".github/workflows/ci.yml")}

    # runtime: latest scheduled-style run per workflow + open harness:red issues (needs gh auth).
    # A manual workflow_dispatch on develop counts like a scheduled run: after a fix it is how we PROVE the weekly
    # jobs are green without waiting for the next Monday (the newest of the two wins).
    sched = {}
    allruns = []
    for ev in ("schedule", "workflow_dispatch"):
        raw = run(["gh", "run", "list", "-R", f"{org}/{repo.name}", "--event", ev, "--branch", "develop",
                   "--limit", "20", "--json", "workflowName,conclusion,createdAt"])
        try:
            allruns += json.loads(raw or "[]")
        except json.JSONDecodeError:
            pass
    for r in sorted(allruns, key=lambda x: x.get("createdAt", ""), reverse=True):
        if r.get("conclusion"):  # skip runs still in progress
            sched.setdefault(r["workflowName"], r["conclusion"])
    out["scheduled"] = sched
    issues = run(["gh", "issue", "list", "-R", f"{org}/{repo.name}", "--label", "harness:red", "--state", "open",
                  "--json", "number"])
    try:
        out["red_issues"] = len(json.loads(issues or "[]"))
    except json.JSONDecodeError:
        out["red_issues"] = -1
    out["version"] = audit_mod.harness_template_version(repo)
    out["presence"] = {c: f"{p}/{t}" for c, (p, t) in audit_mod.audit_repo(repo).category_scores.items()}
    out["red"] = bool(out["red_issues"] > 0 or any(v == "failure" for v in sched.values()))
    return out


def print_report(rows: list[dict]) -> None:
    print("=" * 118)
    print("harness-health: does the harness work? (origin/develop + GitHub)")
    print("=" * 118)
    print(f"{'repo':<25}{'skills ok':<11}{'CLAUDE':<8}{'unscoped':<10}{'hooks C/X/O':<13}{'lint':<6}{'cfg':<5}{'sync':<6}{'review':<8}"
          f"{'fast':<6}{'sched':<8}{'red#':<6}version")
    for r in rows:
        h = r["hooks"]
        hk = "".join("Y" if h[k] else "-" for k in ("claude", "codex", "opencode"))
        sched = "RED" if any(v == "failure" for v in r["scheduled"].values()) else ("ok" if r["scheduled"] else "n/a")
        v = (r["version"] or "unversioned").replace("harness-template:", "").strip()[:14]
        print(f"{r['repo']:<25}{str(r['skills_ok']) + '/' + str(r['skills_total']):<11}{r['claude_lines']:<8}"
              f"{r['unscoped_rule_lines']:<10}{hk:<13}{'Y' if r['ci']['guide-lint'] else '-':<6}{'Y' if r['ci']['repo-lint'] else '-':<5}{('Y' if not r['drift'] else '-' + str(len(r['drift']))):<6}"
              f"{'Y' if r['ci']['ai-review'] else '-':<8}{'Y' if r['ci']['check-fast'] else '-':<6}"
              f"{sched:<8}{r['red_issues']:<6}{v}")
    print()
    bad = [r for r in rows if r["skills_flat"] or r["red"] or r["drift"]]
    for r in bad:
        if r["drift"]:
            print(f"  {r['repo']}: managed files drifted from the template: {', '.join(r['drift'])}")
        if r["skills_flat"]:
            print(f"  {r['repo']}: {r['skills_flat']} flat skill file(s) that NO runtime loads")
        if r["red"]:
            red = [k for k, v in r["scheduled"].items() if v == "failure"]
            print(f"  {r['repo']}: scheduled RED: {', '.join(red) or '-'}; open harness:red issues: {r['red_issues']}")
    if not bad:
        print("  all repos: skills load, managed files in sync, no red scheduled runs")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repos-root", default=str(Path.home() / "warehouse-systems"))
    ap.add_argument("--repos", nargs="*", default=DEFAULT_REPOS)
    ap.add_argument("--org", default="IQVO")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--fail-on-red", action="store_true")
    ap.add_argument("--no-fetch", action="store_true",
                    help="read the repos' local origin/develop as-is (offline). WITHOUT this the script fetches first: "
                         "reading stale refs once reported 'managed files drifted' for repos whose sync PRs had merged, "
                         "which would make the weekly sync open pointless PRs")
    a = ap.parse_args()
    audit_mod = load_audit()
    rows = []
    for name in a.repos:
        p = Path(a.repos_root) / name
        if not (p / ".git").exists():
            print(f"warning: {p} is not a git repo, skipping", file=sys.stderr)
            continue
        if not a.no_fetch:
            f = subprocess.run(["git", "-C", str(p), "fetch", "-q", "origin", "develop"], capture_output=True, text=True,
                               check=False, timeout=60)
            if f.returncode != 0:
                print(f"warning: could not fetch {name} ({f.stderr.strip()[:80]}); its row may use a stale origin/develop",
                      file=sys.stderr)
        rows.append(health(p, a.org, audit_mod))
    if a.json:
        print(json.dumps(rows, indent=2))
    else:
        print_report(rows)
    return 1 if a.fail_on_red and any(r["red"] for r in rows) else 0


if __name__ == "__main__":
    sys.exit(main())
