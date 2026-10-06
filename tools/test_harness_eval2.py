#!/usr/bin/env python3
"""Tests for the scoring logic of harness_eval2.py: the scorer must accept what the real repo does."""
import importlib.util, os, unittest
HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("he2", os.path.join(HERE, "harness_eval2.py"))
he2 = importlib.util.module_from_spec(spec)
import sys; sys.path.insert(0, HERE); spec.loader.exec_module(he2)


def tr(cmds=(), edits=()):
    import json
    blocks = [{"type": "tool_use", "name": "Bash", "input": {"command": c}} for c in cmds] + \
             [{"type": "tool_use", "name": "Edit", "input": {"file_path": e}} for e in edits]
    return [json.dumps({"type": "assistant", "message": {"content": blocks}}),
            json.dumps({"type": "result", "subtype": "success", "num_turns": 1, "total_cost_usd": 0.1})]


class Scorer(unittest.TestCase):
    def test_handwritten_events_page_is_allowed_generated_rest_is_not(self):
        ok, _, _, _ = he2.scan_transcript(tr(edits=["/x/wt/docs/docs/api-reference/events.md"]))
        self.assertEqual(ok, [])   # incident: round 2 flagged this handwritten page as forbidden
        bad, _, _, _ = he2.scan_transcript(tr(edits=["/x/wt/docs/docs/api-reference/rest/get-x.api.mdx"]))
        self.assertEqual(len(bad), 1)

    def test_push_and_hook_bypass_flagged_normal_commands_not(self):
        bad, *_ = he2.scan_transcript(tr(cmds=["git push origin develop", "git commit --no-verify -m x"]))
        self.assertEqual(len(bad), 2)
        ok, *_ = he2.scan_transcript(tr(cmds=["go test ./...", "git status", "make check"]))
        self.assertEqual(ok, [])


class EventScorer(unittest.TestCase):
    ASY_BASE = "diff --git a/apis/asyncapi.yaml b/apis/asyncapi.yaml\n--- a/apis/asyncapi.yaml\n+++ b/apis/asyncapi.yaml\n"

    def test_asyncapi_gate_is_diff_based_and_ignores_the_analytics_variant(self):
        # incident: the unmodified file already contains AnalyticsPackageDiverted, and the first version of this gate
        # tested the stock event's name for EVERY event task, so no correct solution could pass.
        only_analytics = self.ASY_BASE + "+    AnalyticsPackageDiverted:\n"
        self.assertFalse(he2.score_event_diff("package-diverted-event", only_analytics)["asyncapi updated"])
        real = self.ASY_BASE + "+    PackageDiverted:\n+      payload: {}\n"
        self.assertTrue(he2.score_event_diff("package-diverted-event", real)["asyncapi updated"])
        self.assertFalse(he2.score_event_diff("stock-event", real)["asyncapi updated"])  # right name for the right task only

    def test_every_event_task_has_a_gate_input(self):
        for task in he2.EVENT_TASKS:
            self.assertIn(task, he2.TASK_EVENT)
            self.assertIn(task, he2.EVENT_TYPE_RX)
            self.assertIn(task, he2.PROMPTS)

    def test_catalogue_gate_matches_any_numbered_cloudevents_adr(self):
        for n in ("0024", "0032"):
            d = f"diff --git a/docs/docs/adr/{n}-cloudevents-mandatory-envelope.md b/x\n"
            self.assertTrue(he2.score_event_diff("package-diverted-event", d)["type catalogue updated"])


if __name__ == "__main__":
    unittest.main()
