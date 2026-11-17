#!/usr/bin/env python3
"""Test report preservation without touching the actual report or experiment runs.

Run: mise exec -- uv run --locked --no-sync python shutdown/test-render-shutdown-report.py
"""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

SUITE = Path(__file__).resolve().parent
ROOT = SUITE.parent
SUMMARY = SUITE / "pool-shutdown-runs/20261113T081413.844718Z/summary.json"


class ReportAppendixTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="shutdown-report-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.renderer = self.root / "shutdown/render-shutdown-report.py"
        self.renderer.parent.mkdir()
        self.appendix = self.root / "shutdown/shutdown-appendix.md"
        self.report = self.root / "shutdown.md"
        self.summary = self.root / "runs/summary.json"
        self.summary.parent.mkdir()
        shutil.copy2(SUITE / self.renderer.name, self.renderer)
        shutil.copy2(SUITE / self.appendix.name, self.appendix)
        shutil.copy2(SUMMARY, self.summary)

    def rendered_appendix(self):
        # Independent expectations for the maintained appendix's current local links.
        return (self.appendix.read_text()
                .replace("](../", "](")
                .replace("](solo-requeue-runs/", "](shutdown/solo-requeue-runs/"))

    def render(self, success=True):
        result = subprocess.run(
            [sys.executable, str(self.renderer), str(self.summary)],
            capture_output=True, text=True, check=False,
        )
        if success:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Refusing to overwrite report", result.stderr)
        return result

    def test_changed_experiment_preserves_appendix_source_and_rendered_content(self):
        source = self.appendix.read_bytes()
        self.render()
        original_report = self.report.read_bytes()
        self.assertTrue(original_report.endswith(self.rendered_appendix().encode()))
        data = json.loads(self.summary.read_text())
        data["outcomes"][0]["elapsed"] += 10
        self.summary.write_text(json.dumps(data))
        self.render()
        changed_report = self.report.read_bytes()
        self.assertNotEqual(original_report, changed_report)
        self.assertTrue(changed_report.endswith(self.rendered_appendix().encode()))
        self.assertEqual(self.appendix.read_bytes(), source)
        self.assertEqual(changed_report.count(b"## Appendix:"), 1)
        self.render()
        self.assertEqual(self.report.read_bytes(), changed_report)

    def test_explicit_appendix_edit_is_included_in_next_render(self):
        self.render()
        updated = self.appendix.read_text() + "\nManually maintained future clarification.\n"
        self.appendix.write_text(updated)
        self.render()
        self.assertTrue(self.report.read_text().endswith(self.rendered_appendix()))
        self.assertEqual(self.appendix.read_text(), updated)

    def test_relative_links_rebase_without_changing_source_or_external_urls(self):
        source = (
            "## Appendix: link destinations\n"
            "[report](../eta-prefetch.md?view=1#details)\n"
            "[artifact](solo-requeue-runs/example/summary.json)\n"
            "[web](https://example.com/a#b) [network](//example.com/a)\n"
            "[mail](mailto:test@example.com) [anchor](#details) [root](/a)\n"
        )
        expected = source.replace("](../eta-prefetch.md", "](eta-prefetch.md").replace(
            "](solo-requeue-runs/", "](shutdown/solo-requeue-runs/")
        self.appendix.write_text(source)
        self.render()
        self.assertTrue(self.report.read_text().endswith(expected))
        self.assertEqual(self.appendix.read_text(), source)
        self.assertIn("](shutdown/shutdown-appendix.md)", self.report.read_text())
        self.assertFalse((self.root / "shutdown-results.md").exists())
        self.assertFalse((self.renderer.parent / "shutdown.md").exists())

    def test_missing_appendix_leaves_existing_report_untouched(self):
        self.render()
        original = self.report.read_bytes()
        self.appendix.unlink()
        self.render(success=False)
        self.assertEqual(self.report.read_bytes(), original)

    def test_empty_appendix_leaves_existing_report_untouched(self):
        self.render()
        original = self.report.read_bytes()
        for empty in ("", " \n\t"):
            with self.subTest(content=repr(empty)):
                self.appendix.write_text(empty)
                self.render(success=False)
                self.assertEqual(self.report.read_bytes(), original)


if __name__ == "__main__":
    unittest.main(verbosity=2)
