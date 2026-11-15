#!/usr/bin/env python3
"""Check observation wrappers without importing/patching the worker application."""
import ast
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock


class ObservationTests(unittest.TestCase):
    def setUp(self):
        tree = ast.parse(Path(__file__).with_name("redis_visibility_probe.py").read_text())
        module = ast.Module(body=[node for node in tree.body if isinstance(node, ast.FunctionDef)
                                  and node.name in {"restore", "scan"}], type_ignores=[])
        self.events = []
        def emit(event, **fields):
            self.events.append(json.loads(json.dumps(dict(event=event, **fields))))
        self.restore_original = Mock(return_value="delegated")
        self.scan_original = Mock(return_value="scanned")
        self.ns = dict(emit=emit, _original_restore=self.restore_original, _original_scan=self.scan_original)
        exec(compile(module, "observer-functions", "exec"), self.ns)
        self.qos = SimpleNamespace(visibility_timeout=2, _vrestore_count=0)

    def test_bytes_tag_serializes_but_original_argument_is_preserved(self):
        result = self.ns["restore"](self.qos, b"tag", "client", leftmost=True)
        self.restore_original.assert_called_once_with(self.qos, b"tag", "client", leftmost=True)
        self.assertEqual(result, "delegated")
        self.assertEqual([e["tag"] for e in self.events], ["tag", "tag"])

    def test_string_tag_is_preserved(self):
        self.ns["restore"](self.qos, "tag")
        self.restore_original.assert_called_once_with(self.qos, "tag")
        self.assertEqual(self.events[0]["tag"], "tag")

    def test_restore_error_propagates_without_success_event(self):
        self.restore_original.side_effect = RuntimeError("failure")
        with self.assertRaisesRegex(RuntimeError, "failure"):
            self.ns["restore"](self.qos, b"tag")
        self.assertEqual([e["event"] for e in self.events], ["restore_attempt"])

    def test_scan_throttle_observation_does_not_skip_original(self):
        for count in (0, 1, 4):
            self.qos._vrestore_count = count
            result = self.ns["scan"](self.qos, interval=4)
            self.assertEqual(result, "scanned")
            self.assertFalse(self.qos._probe_scanning)
        self.assertEqual(self.scan_original.call_count, 3)
        self.assertEqual([e["actual"] for e in self.events], [True, False, True])

    def test_scan_error_restores_observation_context(self):
        self.scan_original.side_effect = RuntimeError("failure")
        with self.assertRaisesRegex(RuntimeError, "failure"):
            self.ns["scan"](self.qos)
        self.assertFalse(self.qos._probe_scanning)


if __name__ == "__main__":
    unittest.main()
