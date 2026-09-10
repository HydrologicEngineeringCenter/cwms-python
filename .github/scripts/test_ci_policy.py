"""Regression tests for CI selection and the required merge gate."""

import copy
import unittest

from ci_policy import gate, plan


class PolicyTests(unittest.TestCase):
    def pr(self, paths=None, draft=False):
        return plan("pull_request", {"pull_request": {"draft": draft}}, paths)

    def test_ready_pr_pairs_and_python_versions(self):
        rows = self.pr(["cwms/api.py"])["matrix"]["include"]
        self.assertEqual(len(rows), 6)
        self.assertEqual({r["python"] for r in rows}, {"3.9", "3.x"})
        self.assertTrue(all(r["cda"] == r["schema"] for r in rows))

    def test_schedule_and_manual_full_cross_product(self):
        for event, payload in [
            ("schedule", {}),
            ("workflow_dispatch", {}),
            ("workflow_dispatch", {"inputs": {"coverage": "full"}}),
        ]:
            with self.subTest(event=event, payload=payload):
                rows = plan(event, payload)["matrix"]["include"]
                self.assertEqual(len(rows), 18)
                self.assertEqual(
                    len({(r["python"], r["cda"], r["schema"]) for r in rows}), 18
                )

    def test_manual_representative(self):
        result = plan("workflow_dispatch", {"inputs": {"coverage": "representative"}})
        self.assertEqual(len(result["matrix"]["include"]), 6)

    def test_draft_to_ready(self):
        self.assertEqual(self.pr(["cwms/api.py"], draft=True)["mode"], "draft")
        self.assertEqual(self.pr(["cwms/api.py"])["mode"], "representative")

    def test_documentation_only(self):
        result = self.pr(
            ["README.md", "CONTRIBUTING.md", "docs/guide.rst", "docs/images/plot.png"]
        )
        self.assertEqual(result["mode"], "documentation")
        self.assertEqual(result["integration"], "false")

    def test_code_config_unknown_empty_and_uncertain_require_cda(self):
        for path in [
            "docs/conf.py",
            "pyproject.toml",
            "poetry.lock",
            "docker-compose.yml",
            ".github/workflows/testing.yml",
            "tests/resources/fixture.txt",
            "unknown",
        ]:
            with self.subTest(path=path):
                self.assertEqual(self.pr(["README.md", path])["mode"], "representative")
        for paths in (None, []):
            self.assertEqual(self.pr(paths)["mode"], "representative")

    def test_main_skips_database(self):
        result = plan("push", {})
        self.assertEqual(result["mode"], "main")
        self.assertEqual(result["integration"], "false")

    def test_invalid_inputs_fail_closed(self):
        with self.assertRaises(ValueError):
            plan("workflow_dispatch", {"inputs": {"coverage": "none"}})
        with self.assertRaises(ValueError):
            plan("unknown", {})

    def test_gate_matrix_failures_and_skips(self):
        needs = {
            name: {"result": "success"}
            for name in ("plan", "format", "unit", "integration")
        }
        needs["plan"]["outputs"] = {"mode": "representative"}
        gate(needs)
        for name in needs:
            for result in ("failure", "cancelled", "skipped"):
                with self.subTest(job=name, result=result):
                    changed = copy.deepcopy(needs)
                    changed[name]["result"] = result
                    with self.assertRaises(ValueError):
                        gate(changed)

    def test_gate_only_accepts_explicit_skip_modes(self):
        for mode in (
            "draft",
            "documentation",
            "main",
            "full",
            "representative",
            "",
            "unknown",
        ):
            needs = {name: {"result": "success"} for name in ("plan", "format", "unit")}
            needs["plan"]["outputs"] = {"mode": mode}
            needs["integration"] = {"result": "skipped"}
            if mode in {"draft", "documentation", "main"}:
                gate(needs)
            else:
                with self.assertRaises(ValueError):
                    gate(needs)


if __name__ == "__main__":
    unittest.main()
