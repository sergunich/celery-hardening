#!/usr/bin/env python3
"""Check clone safety using temporary local Git repositories, never network access."""
from contextlib import ExitStack
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from environment import manage, runtime


class SourceSetupTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="source-setup-")
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        self.root = self.base / "experiments"
        self.root.mkdir()
        self.upstream = self.base / "upstream"
        self.upstream.mkdir()
        self.git(self.upstream, "init")
        (self.upstream / "source.txt").write_text("original\n")
        self.git(self.upstream, "add", "source.txt")
        self.git(self.upstream, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                 "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null", "commit", "-m", "Fixture")
        self.revision = self.git(self.upstream, "rev-parse", "HEAD")
        self.pins = {"celery": dict(url=self.upstream.as_uri(), revision=self.revision, version="fixture")}
        stack = self.enterContext(ExitStack())
        stack.enter_context(patch.object(manage, "ROOT", self.root))
        stack.enter_context(patch.object(runtime, "ROOT", self.root))
        stack.enter_context(patch.object(manage, "load_toml", return_value=self.pins))
        self.checkout = self.root / "repos/celery"

    def git(self, path, *args):
        return subprocess.check_output(["git", "-C", str(path), *args], text=True, stderr=subprocess.PIPE).strip()

    def test_clone_is_detached_at_pin_and_repeat_setup_is_safe(self):
        manage.sync_sources()
        self.assertEqual(self.git(self.checkout, "rev-parse", "HEAD"), self.revision)
        self.assertEqual(self.git(self.checkout, "rev-parse", "--abbrev-ref", "HEAD"), "HEAD")
        self.assertEqual(self.git(self.checkout, "status", "--porcelain"), "")
        manage.sync_sources()
        self.assertEqual((self.checkout / "source.txt").read_text(), "original\n")

    def test_dirty_checkout_is_not_reset(self):
        manage.sync_sources()
        (self.checkout / "source.txt").write_text("preserve this edit\n")
        with self.assertRaisesRegex(RuntimeError, "Dirty repos/celery"):
            manage.sync_sources()
        self.assertEqual((self.checkout / "source.txt").read_text(), "preserve this edit\n")

    def test_non_checkout_is_not_replaced(self):
        self.checkout.mkdir(parents=True)
        (self.checkout / "keep").write_text("valuable")
        with self.assertRaisesRegex(RuntimeError, "non-checkout"):
            manage.sync_sources()
        self.assertEqual((self.checkout / "keep").read_text(), "valuable")

    def test_wrong_origin_is_not_replaced(self):
        manage.sync_sources()
        self.git(self.checkout, "remote", "set-url", "origin", "https://example.invalid/other")
        with self.assertRaisesRegex(RuntimeError, "Unexpected origin"):
            manage.sync_sources()
        self.assertEqual(self.git(self.checkout, "rev-parse", "HEAD"), self.revision)

    def test_already_imported_external_source_is_rejected(self):
        with patch.dict(sys.modules, celery=SimpleNamespace(__file__=str(self.base / "celery/__init__.py"))), \
             patch.object(sys, "path", sys.path.copy()):
            with self.assertRaisesRegex(RuntimeError, "already imported from outside"):
                runtime.activate_sources()


if __name__ == "__main__":
    unittest.main(verbosity=2)
