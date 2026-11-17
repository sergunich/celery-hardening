#!/usr/bin/env python3
"""Portable layout and environment checks; no workers, servers or installations."""
import ast
import importlib
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from urllib.parse import unquote, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from environment.runtime import ROOT, SUITES, activate_sources, load_toml, provenance

DIRECTORIES = ("environment", *dict.fromkeys(Path(p).parts[0] for p in SUITES.values()))


def anchors(content):
    found, counts = set(), {}
    for heading in re.findall(r"^#{1,6} (.+)$", content, re.M):
        slug = re.sub(r"[^\w -]", "", heading.lower()).replace(" ", "-")
        number = counts.get(slug, 0)
        found.add(f"{slug}-{number}" if number else slug)
        counts[slug] = number + 1
    return found


class SuiteLayoutTests(unittest.TestCase):
    def test_python_and_tool_pins_agree(self):
        python = (ROOT / ".python-version").read_text().strip()
        config = load_toml("mise.toml")
        self.assertEqual(config["tools"]["python"], python)
        self.assertEqual(load_toml("pyproject.toml")["project"]["requires-python"], f"=={python}")
        self.assertEqual(load_toml("uv.lock")["requires-python"], f"=={python}")
        for tool, version in config["tools"].items():
            self.assertRegex(version, r"^\d+\.\d+\.\d+$")
            pin = load_toml("mise.lock")["tools"][tool][0]
            self.assertEqual(pin["version"], version)
            platforms = {key.removeprefix("platforms."): value for key, value in pin.items()
                         if key.startswith("platforms.")}
            self.assertEqual(set(platforms), {"linux-x64", "macos-arm64"})
            for platform in platforms.values():
                self.assertTrue(platform["url"].startswith("https://"))

    def test_uv_dependencies_are_locked_with_hashes(self):
        packages = load_toml("uv.lock")["package"]
        self.assertNotIn("celery", {p["name"] for p in packages})
        self.assertNotIn("kombu", {p["name"] for p in packages})
        for package in packages:
            if "virtual" in package["source"]:
                continue
            artifacts = package.get("wheels", []) + ([package["sdist"]] if "sdist" in package else [])
            self.assertTrue(artifacts, package["name"])
            self.assertTrue(all(a["hash"].startswith("sha256:") for a in artifacts))

    def test_source_pins_and_imports_are_local(self):
        activate_sources()
        for name, pin in load_toml("repos.lock.toml").items():
            self.assertRegex(pin["revision"], r"^[0-9a-f]{40}$")
            module = importlib.import_module(name)
            self.assertEqual(Path(module.__file__).resolve(), ROOT / "repos" / name / name / "__init__.py")
        self.assertIn("/repos/", (ROOT / ".gitignore").read_text().splitlines())

    def test_mise_tasks_share_the_environment(self):
        tasks = load_toml("mise.toml")["tasks"]
        for name, script in SUITES.items():
            self.assertEqual(tasks[name]["run"], f"uv run --locked --no-sync python {script}")
            self.assertIn("check", tasks[name]["depends"])
            self.assertTrue(tasks[name]["raw_args"])
            self.assertEqual(tasks[name]["dir"], "{{config_root}}")
        self.assertEqual(tasks["sync"]["run"], 'uv sync --locked --python "$(mise where python)/bin/python"')
        self.assertEqual(set(tasks["setup"]["depends"]), {"sources", "sync"})
        self.assertEqual(tasks["lint:md"]["run"], "rumdl check --config .rumdl.toml .")
        self.assertEqual(tasks["lint:md"]["dir"], "{{config_root}}")
        self.assertIn("rumdl", load_toml("mise.toml")["tools"])

    def test_scripts_parse_without_importing_probe_side_effects(self):
        for directory in DIRECTORIES:
            for script in (ROOT / directory).glob("*.py"):
                with self.subTest(script=str(script.relative_to(ROOT))):
                    ast.parse(script.read_text(), filename=str(script))

    def test_maintained_markdown_links_and_fragments_resolve(self):
        reports = list(ROOT.glob("*.md"))
        for directory in DIRECTORIES:
            reports.extend((ROOT / directory).glob("*.md"))
        for report in reports:
            for target in re.findall(r"\]\(([^)]+)\)", report.read_text()):
                url = urlsplit(target)
                if url.scheme or url.netloc:
                    continue
                destination = report.parent / unquote(url.path) if url.path else report
                with self.subTest(report=report.name, target=target):
                    self.assertTrue(destination.resolve().is_relative_to(ROOT), target)
                    self.assertTrue(destination.exists(), target)
                    if url.fragment and destination.suffix == ".md":
                        self.assertIn(unquote(url.fragment), anchors(destination.read_text()))

    def test_runner_help_from_unrelated_working_directory(self):
        scripts = (*SUITES.values(), "shutdown/render-shutdown-report.py")
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        env.pop("PYTHONPATH", None)
        with tempfile.TemporaryDirectory(prefix="experiment-cli-") as cwd:
            for script in scripts:
                with self.subTest(script=script):
                    result = subprocess.run([sys.executable, str(ROOT / script), "--help"], cwd=cwd,
                                            env=env, capture_output=True, text=True, timeout=30)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn("usage:", result.stdout)

    def test_provenance_uses_repository_relative_paths(self):
        activate_sources()
        recorded = provenance()
        self.assertEqual(set(recorded["source_revisions"]), {"celery", "kombu"})
        for path, digest in recorded["harness_sha256"].items():
            self.assertFalse(Path(path).is_absolute())
            self.assertTrue((ROOT / path).is_file())
            self.assertRegex(digest, r"^[0-9a-f]{64}$")


if __name__ == "__main__":
    unittest.main(verbosity=2)
