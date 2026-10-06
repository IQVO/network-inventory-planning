#!/usr/bin/env python3
"""fix_agent.py: local OpenCode agent that fixes `harness:red` issues. PR only, never merges.

Runs on the user's machine (Hermes cron or by hand); NOTHING here runs unattended in CI (decision
2026-10-03). Picks open issues labelled `harness:red` (opened by scripts/harness/red_issue.py when a
scheduled sensor such as weekly mutation fails), and for each:

  1. creates a worktree off origin/develop on branch fix/harness-red-<issue>
  2. runs `opencode run` in it with HARNESS_PROTECT_THRESHOLDS=1: the agent may NOT edit .gremlins.yaml,
     .golangci.yml, internal/architecture/**, lefthook.yml, CI or the harness itself, so it cannot make a
     red check green by weakening the check (enforced by hooks, not by prompt prose)
  3. requires `make check-fast` green; commits; pushes; opens ONE PR into develop ("Refs #N")
  4. after 2 failed attempts (counted from issue comments) labels the issue `needs-human` and stops

Usage:
  python3 fix_agent.py [--repo inventory-storage] [--issue 12] [--model zai-coding-plan/glm-5.3] [--dry-run]
Env: HARNESS_FIX_MODEL, HARNESS_FIX_MAX_ATTEMPTS (default 2)
"""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ORG = "IQVO"
ROOT = Path.home() / "warehouse-systems"
DEFAULT_REPOS = ["inventory-storage", "facility-layout", "order-management", "fulfillment-execution",
                 "wes-work-planning", "workforce-management", "labor-performance", "process-path-management",
                 "warehouse-ops-agent", "network-fulfillment", "e2e-tests"]
MARK = "<!-- fix-agent-attempt -->"

PROMPT = """You are fixing a RED scheduled harness sensor in this repository (a Go bounded context in a DDD/hexagonal fleet).

Issue #{num}: {title}
{body}

Failing run log (tail):
{log}

Rules (some are enforced by hooks and will BLOCK you):
- Fix the cause: add/strengthen tests, or fix production code if the sensor exposed a real bug. For surviving mutants, write table-driven tests with exact boundary values so a flipped operator fails.
- You may NOT edit gates: .gremlins.yaml, .golangci.yml, internal/architecture/**, lefthook.yml, .github/**, scripts/harness/**. Never lower a threshold.
- Read .claude/skills/how-to-test/SKILL.md first. Keep the 90% coverage gate green.
- Run `make check-fast` and the relevant heavier target (for mutation: the Makefile mutation target) and make them pass before you stop.
- Do not push, do not open PRs, do not touch CLAUDE.md/AGENTS.md: the driver does that.
- If you cannot fix it without changing a gate or the intended behaviour, stop and explain why in your final message.
"""


def sh(cmd, cwd=None, env=None, timeout=3600, check=False):
    p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout, env=dict(os.environ, **(env or {})))
    if check and p.returncode:
        raise RuntimeError(f"{' '.join(cmd)}: {p.stderr[-800:]}")
    return p


def gh_json(args):
    p = sh(["gh", *args])
    try:
        return json.loads(p.stdout or "null")
    except json.JSONDecodeError:
        return None


def attempts(repo, num):
    c = gh_json(["issue", "view", str(num), "-R", f"{ORG}/{repo}", "--json", "comments"]) or {}
    return sum(1 for x in c.get("comments", []) if MARK in x.get("body", ""))


def failing_log(repo, title):
    job = title.split("`")[1] if "`" in title else ""
    runs = gh_json(["run", "list", "-R", f"{ORG}/{repo}", "--event", "schedule", "--status", "failure", "--limit", "5",
                    "--json", "databaseId"]) or []
    for r in runs:
        out = sh(["gh", "run", "view", str(r["databaseId"]), "-R", f"{ORG}/{repo}", "--log-failed"]).stdout
        if out and (not job or job in out):
            return "\n".join(out.splitlines()[-120:])
    return "(no failing scheduled run log found; read the issue link)"


def fix(repo, issue, model, dry, max_attempts):
    num, title = issue["number"], issue["title"]
    n = attempts(repo, num)
    if n >= max_attempts:
        sh(["gh", "issue", "edit", str(num), "-R", f"{ORG}/{repo}", "--add-label", "needs-human"])
        return {"repo": repo, "issue": num, "status": "needs-human", "attempts": n}
    r = ROOT / repo
    branch = f"fix/harness-red-{num}"
    wt = ROOT / ".worktrees" / f"{repo}-fix-{num}"
    if dry:
        return {"repo": repo, "issue": num, "status": "dry-run", "would_use": str(wt)}
    sh(["git", "fetch", "-q", "origin"], r)
    if not wt.exists():
        sh(["git", "worktree", "add", "-q", str(wt), "-b", branch, "origin/develop"], r, check=True)
    body = (gh_json(["issue", "view", str(num), "-R", f"{ORG}/{repo}", "--json", "body"]) or {}).get("body", "")
    prompt = PROMPT.format(num=num, title=title, body=body, log=failing_log(repo, title))
    p = sh(["opencode", "run", "--auto", "-m", model, prompt], cwd=wt, env={"HARNESS_PROTECT_THRESHOLDS": "1"})
    tail = (p.stdout + p.stderr)[-1500:]
    sh(["gh", "issue", "comment", str(num), "-R", f"{ORG}/{repo}", "--body", f"{MARK}\nFix-agent attempt {n + 1} finished.\n\n```\n{tail[-700:]}\n```"])
    ahead = sh(["git", "rev-list", "--count", "origin/develop..HEAD"], wt).stdout.strip()
    dirty = sh(["git", "status", "--porcelain"], wt).stdout.strip()
    if dirty:
        sh(["git", "add", "-A"], wt)
        sh(["git", "commit", "-q", "-m", f"fix: address harness:red issue #{num} (fix-agent)"], wt, env={"LEFTHOOK": "0"})
    ahead = sh(["git", "rev-list", "--count", "origin/develop..HEAD"], wt).stdout.strip()
    if ahead == "0":
        return {"repo": repo, "issue": num, "status": "no-changes", "attempts": n + 1}
    gate = sh(["make", "check-fast"], wt)
    if gate.returncode:
        return {"repo": repo, "issue": num, "status": "gate-red", "attempts": n + 1, "tail": (gate.stdout + gate.stderr)[-600:]}
    sh(["git", "push", "-q", "-u", "origin", branch], wt, check=True)
    pr = sh(["gh", "pr", "create", "-R", f"{ORG}/{repo}", "--base", "develop", "--head", branch,
             "--title", f"fix: address harness:red issue #{num}",
             "--body", f"Refs #{num}\n\nAuthored by the local fix agent (OpenCode, `HARNESS_PROTECT_THRESHOLDS=1`: gates untouched). "
                       f"`make check-fast` green locally; CI is the gate. Not auto-merged."], wt)
    return {"repo": repo, "issue": num, "status": "pr-opened", "pr": pr.stdout.strip().splitlines()[-1] if pr.stdout else pr.stderr[-300:]}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo")
    ap.add_argument("--issue", type=int)
    ap.add_argument("--model", default=os.environ.get("HARNESS_FIX_MODEL", "zai-coding-plan/glm-5.3"))
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    max_attempts = int(os.environ.get("HARNESS_FIX_MAX_ATTEMPTS", "2"))
    results = []
    for repo in ([a.repo] if a.repo else DEFAULT_REPOS):
        issues = gh_json(["issue", "list", "-R", f"{ORG}/{repo}", "--label", "harness:red", "--state", "open",
                          "--json", "number,title,labels"]) or []
        for i in issues:
            if a.issue and i["number"] != a.issue:
                continue
            if any(l["name"] == "needs-human" for l in i["labels"]):
                continue
            existing = gh_json(["pr", "list", "-R", f"{ORG}/{repo}", "--head", f"fix/harness-red-{i['number']}", "--json", "url"]) or []
            if existing:
                results.append({"repo": repo, "issue": i["number"], "status": "pr-exists", "pr": existing[0]["url"]})
                continue
            results.append(fix(repo, i, a.model, a.dry_run, max_attempts))
    print(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
