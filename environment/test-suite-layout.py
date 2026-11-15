#!/usr/bin/env python3
"""Check the relocated suite layout without starting workers or installing tools."""
import ast
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from urllib.parse import unquote, urlsplit

DEV = Path(__file__).resolve().parents[1]
ROOT = DEV.parent
SUITES = ("shutdown", "eta-prefetch", "task-policy", "redis-connection", "redis-visibility")


class SuiteLayoutTests(unittest.TestCase):
    def test_mapped_files_exist_without_legacy_root_copies(self):
        mapping = json.loads((DEV / "environment/layout-migration.json").read_text())
        self.assertEqual(len(mapping.values()), len(set(mapping.values())))
        for old, new in mapping.items():
            with self.subTest(old=old):
                self.assertTrue((DEV / new).exists(), new)
                self.assertFalse((DEV / old).exists(), old)

    def test_scripts_parse_without_importing_probe_side_effects(self):
        for suite in (*SUITES, "environment"):
            for script in (DEV / suite).glob("*.py"):
                with self.subTest(script=script.name):
                    ast.parse(script.read_text(), filename=str(script))

    def test_maintained_markdown_links_and_explicit_dev_paths_resolve(self):
        reports = list(DEV.glob("*.md"))
        for directory in (*SUITES, "environment"):
            reports.extend((DEV / directory).glob("*.md"))
        for report in reports:
            content = report.read_text()
            for target in re.findall(r"\]\(([^)]+)\)", content):
                url = urlsplit(target)
                if url.scheme or not url.path:
                    continue
                with self.subTest(report=report.name, target=target):
                    self.assertTrue((report.parent / unquote(url.path)).exists(), target)
            for target in re.findall(r"(?<![\w/])\.dev/[\w./-]+", content):
                # Paths in prose/commands are checkout-relative, unlike Markdown links.
                with self.subTest(report=report.name, target=target):
                    self.assertTrue((ROOT / target.rstrip(".")).exists(), target)

    def test_runner_help_from_unrelated_working_directory(self):
        scripts = (
            "shutdown/verify-pool-shutdown.py",
            "shutdown/render-shutdown-report.py",
            "eta-prefetch/verify-eta-prefetch.py",
            "task-policy/verify-task-policies.py",
            "redis-connection/verify-redis-connections.py",
            "redis-visibility/verify-redis-visibility.py",
        )
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        env.pop("PYTHONPATH", None)
        with tempfile.TemporaryDirectory(prefix="celery-layout-cli-") as cwd:
            for script in scripts:
                with self.subTest(script=script):
                    result = subprocess.run(
                        [sys.executable, str(DEV / script), "--help"], cwd=cwd,
                        env=env, capture_output=True, text=True, timeout=30,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn("usage:", result.stdout)
        # verify-solo-requeue has no argument parser; its real two-case smoke run
        # verifies imports/paths instead. Never invoke it as a supposed help check.

    def test_setup_uses_checkout_and_shared_lock_from_unrelated_cwd(self):
        with tempfile.TemporaryDirectory(prefix="celery-layout-setup-") as tmp:
            tmp = Path(tmp)
            calls = tmp / "calls"
            for name in ("uv", "mise"):
                tool = tmp / name
                tool.write_text('#!/bin/sh\nprintf "%s\\n" "$PWD" "$*" >> "$LAYOUT_CALLS"\n')
                tool.chmod(0o700)
            env = dict(os.environ, PATH=f"{tmp}{os.pathsep}{os.environ['PATH']}",
                       LAYOUT_CALLS=str(calls), PYTHONDONTWRITEBYTECODE="1")
            env.pop("PYTHONPATH", None)
            result = subprocess.run(
                ["bash", str(DEV / "environment/setup-shutdown-env.sh")], cwd=tmp,
                env=env, capture_output=True, text=True, timeout=30,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(calls.read_text().splitlines(), [
                str(ROOT),
                "pip sync --python .dev/.venv/bin/python .dev/environment/shutdown-requirements.lock",
                str(ROOT), "install redis@8.2.3",
            ])
            self.assertIn(str(ROOT / "celery/__init__.py"), result.stdout)


if __name__ == "__main__":
    unittest.main(verbosity=2)
