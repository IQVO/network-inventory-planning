#!/usr/bin/env python3
"""migrate_v3.py: bring ONE Go bounded-context repo to harness-template v3.

Idempotent. Run from anywhere:  python3 migrate_v3.py --repo <path> [--template <path>] [--dry-run]

What it does (mechanical, no per-repo judgement):
  1. skills    flat .claude/skills/*.md -> .claude/skills/<name>/SKILL.md + frontmatter
               (git mv, history kept). Without this NO runtime loads them.
  2. commands  .claude/commands/*.md -> .claude/skills/<name>/SKILL.md (same /name in
               Claude Code; discovered by OpenCode/Codex too).
  3. rules     adds `paths:` frontmatter to path-specific rules (domain-model etc. stay
               always-on). Claude Code then loads them lazily.
  4. managed   copies the template-managed files byte-for-byte (hooks, adapters, lint,
               red-issue helper, AI-review workflow) -- see MANAGED below.
  5. Makefile  appends check-fast / guide-lint / harness-test targets.
  6. ci.yml    adds an ADVISORY `guide-lint` job; adds harness:red issue open/close steps
               to the scheduled `mutation` and `drift` jobs.
  7. CLAUDE.md appends a generated "Scoped rules" pointer table between markers so
               runtimes WITHOUT path-scoped rules (OpenCode, Codex) still find them.
               (real file resolved through the AGENTS.md<->CLAUDE.md symlink)
  8. .gitignore  ignores .claude/settings.local.json.

What it deliberately does NOT do (per-repo judgement, done by a reviewer/agent):
  slimming CLAUDE.md to <=120 lines, fixing stale references guide-lint reports,
  new fitness rules.
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_TEMPLATE = os.path.abspath(os.path.join(HERE, ".."))

# Template-managed files: byte-identical copies, refreshed by the weekly sync (Phase 8).
MANAGED = [
    "scripts/harness/hook.py",
    "scripts/harness/test_hook.py",
    "scripts/harness/guide_lint.py",
    "scripts/harness/red_issue.py",
    "scripts/harness/fleet_drift.py",
    "scripts/harness/repo_lint.py",       # config sensors: workflow paths, needs, dependabot, owner drift, placeholders
    "scripts/harness/test_repo_lint.py",
    ".claude/settings.json",
    ".codex/hooks.json",
    ".opencode/plugins/harness.ts",
    ".github/workflows/ai-review.yml",
    "internal/architecture/violation_test.go",
    "internal/architecture/events_fitness_test.go",  # CloudEvents-only + replay CommitInterval  # archViolation: WHAT/WHY/FIX for arch-test failures
    "internal/architecture/catalogue_fitness_test.go",  # fleet CloudEvents ADR type catalogue == asyncapi contract
]
SYMLINKS = {".agents/skills": "../.claude/skills"}

SKILL_META = {
    "how-to-add-a-rest-endpoint": (
        "Add or change a REST endpoint in this service in the fleet's hexagonal order (domain invariant, use case, "
        "port, HTTP adapter, apis/openapi.yaml, generated docs, godog scenario). Use when touching "
        "internal/adapters/inbound/http, apis/openapi.yaml, or exposing a use case over HTTP."),
    "how-to-add-an-integration-event": (
        "Publish or consume a cross-service Kafka event: CloudEvents 1.0 type naming, AsyncAPI, transactional outbox, "
        "consumer-group rules. Use when touching internal/adapters kafka or outbox code, a publisher/consumer, or "
        "apis/asyncapi*.yaml."),
    "how-to-test": (
        "Write or review tests and diagnose a failing coverage, mutation, bdd or integration CI job: the four test "
        "layers, the 90% gate, gremlins threshold semantics, the testcontainers rule. Use when adding tests, killing "
        "a surviving mutant, or fixing a red check."),
    "how-to-write-an-adr": (
        "Write an Architecture Decision Record in this repo's numbering and format, including companion ADRs for "
        "cross-repo changes. Use when a design decision should be recorded or a change contradicts an existing ADR."),
    "how-to-add-a-frontend-remote": (
        "Add or change a micro-frontend remote under web/ (vite federation config in object form, /mfes/<context>/ "
        "base, remoteEntry, Docker/nginx packaging, console integration). Use when touching web/."),
    "how-to-add-an-mcp-tool-call": (
        "Add a read-only MCP tool call to this agent following the fleet zero-write rule. Use when touching the "
        "outbound MCP client adapters."),
    "code-review": (
        "Fast pre-commit semantic review of the uncommitted changes or a given git range: domain logic in the wrong "
        "layer, missing failure-path tests, adapter concerns in domain types, impure ports. Advisory counterpart to "
        "make check. Invoke explicitly: /code-review [range]."),
    "architecture-review": (
        "Bounded-context boundary and ADR-compliance review of a change (expensive, post-integration): hexagonal "
        "direction, cross-context coupling, contradicted ADRs. Invoke explicitly: /architecture-review [range]."),
    "domain-review": (
        "Ubiquitous-language drift review of a change against .claude/rules/domain-model.md: renamed or invented "
        "terms, aggregate-invariant leaks. Invoke explicitly: /domain-review [range]."),
}
COMMANDS = {"code-review", "architecture-review", "domain-review"}

WEB = ["web/**"]
REST = ["internal/adapters/inbound/http/**", "apis/openapi*.yaml", "apis/openapi/**"]
ANALYTICS = ["internal/adapters/outbound/analyticsstore/**", "internal/**/analytics*/**"]
EVENTS = ["internal/adapters/**/kafka/**", "internal/adapters/outbound/events/**", "apis/asyncapi*"]
DOCS = ["docs/**", "apis/**"]
CI = [".github/**", "Makefile", ".gremlins.yaml", ".golangci.yml", "lefthook.yml"]
TESTS = ["**/*_test.go", "features/**"]
RULE_PATHS = {
    "frontend-mfe.md": WEB, "frontend.md": WEB, "mfe-remotes.md": WEB,
    "rest-api-and-frontend.md": REST + WEB, "rest-api.md": REST, "api-contracts.md": REST,
    "api-and-integration.md": REST + EVENTS, "testing-and-api.md": REST + TESTS,
    "analytics-data-product.md": ANALYTICS, "analytics-and-observability.md": ANALYTICS,
    "integration-events.md": EVENTS, "integrations.md": EVENTS + ["internal/adapters/outbound/**"],
    "runtime-and-outbox.md": EVENTS + ["internal/adapters/outbound/postgres/**", "migrations/**"],
    "docs-and-api-drift.md": DOCS, "ci-quality-gates.md": CI, "testing-and-quality.md": CI + TESTS,
}


def sh(args, cwd, check=True):
    return subprocess.run(args, cwd=cwd, check=check, capture_output=True, text=True)


class Migrator:
    def __init__(self, repo, template, dry, keep_notes=False, profile="service", fast_deps="", sched=None):
        self.repo, self.template, self.dry = os.path.abspath(repo), os.path.abspath(template), dry
        self.keep_notes = keep_notes
        self.profile, self.fast_deps = profile, fast_deps
        self.sched = sched or []  # ["workflow.yml:job", ...] extra scheduled jobs that get red-issue steps
        self.log: list[str] = []

    def p(self, *a):
        return os.path.join(self.repo, *a)

    def say(self, msg):
        self.log.append(msg)
        print(("[dry] " if self.dry else "") + msg)

    def write(self, rel, text):
        if self.dry:
            return
        os.makedirs(os.path.dirname(self.p(rel)), exist_ok=True)
        with open(self.p(rel), "w", encoding="utf8") as fh:
            fh.write(text)

    def read(self, rel):
        with open(self.p(rel), encoding="utf8") as fh:
            return fh.read()

    # 1+2 -------------------------------------------------------------------
    def skills(self):
        sdir = self.p(".claude", "skills")
        if os.path.isdir(sdir):
            for f in sorted(os.listdir(sdir)):
                if f.endswith(".md") and os.path.isfile(os.path.join(sdir, f)):
                    self._to_skill(os.path.join(".claude/skills", f), f[:-3])
        cdir = self.p(".claude", "commands")
        if os.path.isdir(cdir):
            for f in sorted(os.listdir(cdir)):
                if f.endswith(".md"):
                    self._to_skill(os.path.join(".claude/commands", f), f[:-3], command=True)

    def _to_skill(self, src_rel, name, command=False):
        dest_rel = f".claude/skills/{name}/SKILL.md"
        desc = SKILL_META.get(name)
        if not desc:
            first = next((l[2:].strip() for l in self.read(src_rel).splitlines() if l.startswith("# ")), name)
            desc = f"{first}. Use when the task matches this guide."
        body = self.read(src_rel)
        if not self.keep_notes:
            body = re.sub(r"^<!-- TEMPLATE NOTE[^\n]*-->\n\n?", "", body)
        fm = f"---\nname: {name}\ndescription: {desc}\n"
        if command:
            fm += "disable-model-invocation: true\nargument-hint: \"[git range]\"\n"
        fm += "---\n\n"
        self.say(f"skill: {src_rel} -> {dest_rel}")
        if self.dry:
            return
        os.makedirs(os.path.dirname(self.p(dest_rel)), exist_ok=True)
        sh(["git", "mv", src_rel, dest_rel], self.repo)
        self.write(dest_rel, fm + body)
        sh(["git", "add", dest_rel], self.repo)

    # 3 ---------------------------------------------------------------------
    def rules(self):
        rdir = self.p(".claude", "rules")
        scoped = []
        if not os.path.isdir(rdir):
            return scoped
        for f in sorted(os.listdir(rdir)):
            if not f.endswith(".md"):
                continue
            rel = f".claude/rules/{f}"
            text = self.read(rel)
            if text.startswith("---"):
                m = re.search(r"^paths:\s*\n((?:\s+-\s.*\n)+)", text, re.M)
                if m:
                    scoped.append((f, re.findall(r'-\s*"?([^"\n]+)"?', m.group(1))))
                continue
            paths = RULE_PATHS.get(f)
            if not paths:
                continue
            fm = "---\npaths:\n" + "".join(f'  - "{g}"\n' for g in paths) + "---\n\n"
            self.say(f"rule scoped: {f} -> {paths[:2]}{'...' if len(paths) > 2 else ''}")
            self.write(rel, fm + text)
            scoped.append((f, paths))
        return scoped

    # 4 ---------------------------------------------------------------------
    def managed(self):
        rels = list(MANAGED)
        if self.profile == "service":
            # Go-service-only: fleet knowledge (canonical copy: warehouse-docs/agents/fleet/) + arch-test helper
            fleet = os.path.join(self.template, ".claude/rules/fleet")
            if os.path.isdir(fleet):
                rels += [".claude/rules/fleet/" + f for f in sorted(os.listdir(fleet)) if f.endswith(".md")]
        else:
            rels = [r for r in rels if not r.startswith("internal/")]
        for rel in rels:
            src = os.path.join(self.template, rel)
            if not os.path.isfile(src):
                self.say(f"WARN template lacks {rel}")
                continue
            if os.path.isfile(self.p(rel)) and open(src, "rb").read() == open(self.p(rel), "rb").read():
                continue
            self.say(f"managed: {rel}")
            if not self.dry:
                os.makedirs(os.path.dirname(self.p(rel)), exist_ok=True)
                shutil.copyfile(src, self.p(rel))
                if rel.endswith(".py"):
                    os.chmod(self.p(rel), 0o755)
        for link, target in SYMLINKS.items():
            if not os.path.islink(self.p(link)):
                self.say(f"symlink: {link} -> {target}")
                if not self.dry:
                    os.makedirs(os.path.dirname(self.p(link)), exist_ok=True)
                    os.symlink(target, self.p(link))

    # 5 ---------------------------------------------------------------------
    def makefile(self):
        if self.profile == "platform":
            return self.makefile_platform()
        if not os.path.isfile(self.p("Makefile")):
            self.say("no Makefile: skipping make targets")
            return
        text = self.read("Makefile")
        if "check-fast:" in text:
            return
        block = (
            "\n# --- agent harness (harness-template v3) -----------------------------------\n"
            ".PHONY: check-fast guide-lint harness-test\n"
            "# Fast local gate used by the agent Stop hook: format, vet, fitness tests, and the tests of\n"
            "# the packages changed vs HEAD. The full gate stays `make check` / `make check-all`.\n"
            "check-fast: fmt-check vet arch-test\n"
            "\t@pkgs=\"$$(python3 scripts/harness/hook.py changed-pkgs)\"; \\\n"
            "\tif [ -n \"$$pkgs\" ]; then go test $$pkgs; else echo \"check-fast: no changed Go packages\"; fi\n\n"
            "guide-lint: ## lint agent guides: skills load, references resolve, context budget\n"
            "\tpython3 scripts/harness/guide_lint.py\n\n"
            "harness-test: ## unit-test the agent hooks (pre/post/stop)\n"
            "\tpython3 scripts/harness/test_hook.py\n"
        )
        self.say("Makefile: + check-fast guide-lint harness-test")
        self.write("Makefile", text.rstrip("\n") + "\n" + block)

    def makefile_platform(self):
        """Non-Go platform repos (infra, e2e, console, ui-kit): check-fast = the repo's own fast checks."""
        text = self.read("Makefile") if os.path.isfile(self.p("Makefile")) else ""
        if "check-fast:" in text:
            return
        deps = self.fast_deps.strip()
        block = (
            "\n# --- agent harness (harness-template v3) -----------------------------------\n"
            ".PHONY: check-fast guide-lint harness-test\n"
            "# Fast local gate used by the agent Stop hook (this repo's own quick checks).\n"
            f"check-fast: {deps}\n\n"
            "guide-lint: ## lint agent guides: skills load, references resolve, context budget\n"
            "\tpython3 scripts/harness/guide_lint.py\n\n"
            "harness-test: ## unit-test the agent hooks (pre/post/stop)\n"
            "\tpython3 scripts/harness/test_hook.py\n"
        )
        self.say(f"Makefile: + check-fast ({deps or 'no deps'}) guide-lint harness-test")
        self.write("Makefile", (text.rstrip("\n") + "\n" if text else "") + block)

    # 6 ---------------------------------------------------------------------
    def ci(self):
        rel = ".github/workflows/ci.yml"
        if not os.path.isfile(self.p(rel)) and os.path.isfile(self.p(rel + ".template")):
            rel += ".template"  # the template repo itself ships its workflow inactive
        if not os.path.isfile(self.p(rel)):
            return
        lines = self.read(rel).splitlines()
        text = "\n".join(lines)
        m = re.search(r"uses: (actions/checkout@\S+(?: # \S+)?)", text)
        checkout = m.group(1) if m else "actions/checkout@v4"
        changed = False
        if not re.search(r"^  guide-lint:", text, re.M):
            job = [
                "  # Agent-guide sensor (harness-template v3): skills load, references resolve, context budget.",
                "  # ADVISORY (continue-on-error) until the guides are clean; then flip to blocking.",
                "  guide-lint:",
                "    runs-on: ubuntu-latest",
                "    timeout-minutes: 10",
                "    continue-on-error: true",
                "    steps:",
                f"      - uses: {checkout}",
                "      - run: python3 scripts/harness/guide_lint.py",
                "      - run: python3 scripts/harness/repo_lint.py",
                "      - run: python3 scripts/harness/test_hook.py",
                "      - run: python3 scripts/harness/test_repo_lint.py",
                "",
            ]
            idx = next((i for i, l in enumerate(lines) if re.match(r"^  complexity:", l)), None)
            if idx is None:
                idx = next((i for i, l in enumerate(lines) if re.match(r"^  test:", l)), None)
            if idx is None:  # platform repos: before the first job after `jobs:`
                j = next((i for i, l in enumerate(lines) if l.startswith("jobs:")), None)
                if j is not None:
                    idx = next((i for i in range(j + 1, len(lines)) if re.match(r"^  [A-Za-z0-9_-]+:\s*$", lines[i])), None)
            if idx is not None:
                while idx > 0 and (lines[idx - 1].startswith("  #") or lines[idx - 1].strip() == ""):
                    idx -= 1
                lines[idx:idx] = [""] + job
                changed = True
                self.say("ci.yml: + guide-lint job (advisory)")
        if self._add_repo_lint_steps(lines):
            changed = True
            self.say("ci.yml: + repo_lint steps in `guide-lint`")
        for jobname in ("mutation", "drift"):
            if self._add_red_steps(lines, jobname):
                changed = True
                self.say(f"ci.yml: + harness:red issue steps in `{jobname}`")
        if changed:
            self.write(rel, "\n".join(lines) + "\n")
        for spec in self.sched:
            wf, job = spec.split(":", 1)
            wrel = f".github/workflows/{wf}"
            if not os.path.isfile(self.p(wrel)):
                continue
            wl = self.read(wrel).splitlines()
            if self._add_red_steps(wl, job):
                self.say(f"{wf}: + harness:red issue steps in `{job}`")
                self.write(wrel, "\n".join(wl) + "\n")

    @staticmethod
    def _add_repo_lint_steps(lines):
        """Append repo_lint.py / test_repo_lint.py to an existing `guide-lint` job (idempotent)."""
        text = "\n".join(lines)
        if "repo_lint.py" in text:
            return False
        b = Migrator._job_bounds(lines, "guide-lint")
        if not b:
            return False
        start, end = b
        out = False
        for i in range(start, end):
            if "python3 scripts/harness/guide_lint.py" in lines[i]:
                indent = re.match(r"\s*", lines[i]).group(0)
                lines.insert(i + 1, f"{indent}- run: python3 scripts/harness/repo_lint.py")
                out = True
                break
        if out:
            b = Migrator._job_bounds(lines, "guide-lint")
            for i in range(b[0], b[1]):
                if "python3 scripts/harness/test_hook.py" in lines[i]:
                    indent = re.match(r"\s*", lines[i]).group(0)
                    lines.insert(i + 1, f"{indent}- run: python3 scripts/harness/test_repo_lint.py")
                    break
        return out

    @staticmethod
    def _job_bounds(lines, jobname):
        start = next((i for i, l in enumerate(lines) if re.match(rf"^  {re.escape(jobname)}:\s*$", l)), None)
        if start is None:
            return None
        end = len(lines)
        for i in range(start + 1, len(lines)):
            if re.match(r"^  [A-Za-z0-9_-]+:\s*$", lines[i]):
                end = i
                break
        while end > start and (lines[end - 1].strip() == "" or lines[end - 1].startswith("  #")):
            end -= 1
        return start, end

    def _add_red_steps(self, lines, jobname):
        b = self._job_bounds(lines, jobname)
        if not b:
            return False
        start, end = b
        block = "\n".join(lines[start:end])
        if "red_issue.py" in block:
            return False
        steps = [
            "      - name: Open or refresh the harness:red issue on a scheduled failure",
            "        if: failure() && github.event_name == 'schedule'",
            "        env:",
            "          GH_TOKEN: ${{ github.token }}",
            "        run: python3 scripts/harness/red_issue.py " + jobname +
            " \"${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}\"",
            "      - name: Close the harness:red issue when the scheduled run recovers",
            "        if: success() && github.event_name == 'schedule'",
            "        env:",
            "          GH_TOKEN: ${{ github.token }}",
            "        run: python3 scripts/harness/red_issue.py " + jobname + " --close",
        ]
        lines[end:end] = steps
        if not re.search(r"^    permissions:", block, re.M):
            ri = next((i for i in range(start, end) if re.match(r"^    runs-on:", lines[i])), None)
            if ri is not None:
                lines[ri + 1:ri + 1] = ["    permissions:", "      contents: read", "      issues: write"]
        return True

    # 7 ---------------------------------------------------------------------
    def claude_md(self, scoped):
        claude = self.p("CLAUDE.md")
        if not os.path.exists(claude):
            return
        real = os.path.realpath(claude)  # CLAUDE.md or AGENTS.md may be the symlink
        with open(real, encoding="utf8") as fh:
            text = fh.read()
        rows = "".join(f"| {', '.join('`'+g+'`' for g in paths[:3])}{' ...' if len(paths) > 3 else ''} | "
                       f"`.claude/rules/{f}` |\n" for f, paths in scoped)
        block = (
            "<!-- harness:scoped-rules:start (generated by tools/migrate_v3.py in warehouse-harness-template; do not hand-edit) -->\n"
            "## Scoped rules and harness\n\n"
            "Claude Code loads each rule below automatically when you touch the matching paths. OpenCode and "
            "Codex do NOT: read the rule BEFORE editing matching files.\n\n"
            + ("| When touching | Read |\n|---|---|\n" + rows + "\n" if rows else "")
            + "Hooks (`scripts/harness/hook.py`, wired for Claude Code, Codex and OpenCode) block pushes to "
            "develop/main, `--no-verify`, bare `rm -rf`, and edits to generated files, and feed gofmt/vet "
            "findings back after each edit. Before saying \"done\" run `make check-fast`; the full gate is "
            "`make check-all`. `HARNESS_OFF=1` disables the hooks when debugging the harness itself.\n"
            "<!-- harness:scoped-rules:end -->\n"
        )
        pat = re.compile(r"<!-- harness:scoped-rules:start.*?<!-- harness:scoped-rules:end -->\n", re.S)
        new = pat.sub(block, text) if pat.search(text) else text.rstrip("\n") + "\n\n" + block
        if new != text:
            self.say(f"CLAUDE.md: scoped-rules pointer block ({os.path.basename(real)} is the real file)")
            if not self.dry:
                with open(real, "w", encoding="utf8") as fh:
                    fh.write(new)

    # 7b --------------------------------------------------------------------
    def rewrite_refs(self):
        """After moving skills/commands, guides still cite the OLD flat paths: fix them."""
        files = []
        real_claude = os.path.realpath(self.p("CLAUDE.md"))
        if os.path.isfile(real_claude):
            files.append(real_claude)
        for sub in (".claude/rules", ".claude/skills"):
            for dp, _, fs in os.walk(self.p(sub)):
                files += [os.path.join(dp, f) for f in fs if f.endswith(".md")]
        pats = [(re.compile(r"\.claude/skills/([a-z0-9-]+)\.md"), r".claude/skills/\1/SKILL.md"),
                (re.compile(r"\.claude/commands/([a-z0-9-]+)\.md"), r".claude/skills/\1/SKILL.md")]
        rules = os.path.join(self.repo, ".claude", "rules")
        have = set(os.listdir(rules)) if os.path.isdir(rules) else set()
        # generic skills were copied from reference repos and cite rule files only some repos have
        subs = {}
        if "bounded-context-boundary.md" not in have and "domain-model.md" in have:
            subs[".claude/rules/bounded-context-boundary.md"] = ".claude/rules/domain-model.md"
        front = next((r for r in ("frontend-mfe.md", "frontend.md", "rest-api-and-frontend.md") if r in have), None)
        if "mfe-remotes.md" not in have and front:
            subs[".claude/rules/mfe-remotes.md"] = f".claude/rules/{front}"
        for f in sorted(set(files)):
            with open(f, encoding="utf8") as fh:
                text = fh.read()
            new = text
            for pat, rep in pats:
                new = pat.sub(rep, new)
            for old, rep in subs.items():
                new = new.replace(old, rep)
            if new != text:
                self.say(f"refs rewritten in {os.path.relpath(f, self.repo)}")
                if not self.dry:
                    with open(f, "w", encoding="utf8") as fh:
                        fh.write(new)

    # 8 ---------------------------------------------------------------------
    def gitignore(self):
        gi = self.p(".gitignore")
        text = open(gi, encoding="utf8").read() if os.path.exists(gi) else ""
        add = [e for e in (".claude/settings.local.json",) if e not in text]
        if add:
            self.say(".gitignore: + " + ", ".join(add))
            self.write(".gitignore", text.rstrip("\n") + "\n" + "\n".join(add) + "\n")

    def run(self):
        scoped = []
        if self.profile == "service":
            self.skills()
            scoped = self.rules()
        self.managed()
        self.makefile()
        self.ci()
        self.claude_md(scoped)
        self.rewrite_refs()
        self.gitignore()
        return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo", required=True)
    ap.add_argument("--template", default=DEFAULT_TEMPLATE)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--keep-notes", action="store_true", help="keep TEMPLATE NOTE comments (template repo itself)")
    ap.add_argument("--profile", choices=["service", "platform"], default="service",
                    help="platform = non-Go repo (infra/e2e/console/ui-kit): no skills/rules rewrite, custom check-fast")
    ap.add_argument("--fast-deps", default="", help="platform profile: make targets that make up check-fast")
    ap.add_argument("--sched", action="append", default=[], help="workflow.yml:job that gets harness:red steps")
    a = ap.parse_args()
    return Migrator(a.repo, a.template, a.dry_run, a.keep_notes, a.profile, a.fast_deps, a.sched).run()


if __name__ == "__main__":
    sys.exit(main())
