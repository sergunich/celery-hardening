#!/usr/bin/env python3
"""Source-path checks without live services; not broker/backend integration tests."""
import itertools
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from celery import Celery
from kombu.transport.redis import Channel


class ReadTrackingDict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reads = []

    def get(self, key, *args):
        self.reads.append(key)
        return super().get(key, *args)

    def __getitem__(self, key):
        self.reads.append(key)
        return super().__getitem__(key)

    def __contains__(self, key):
        self.reads.append(key)
        return super().__contains__(key)


class WiringTests(unittest.TestCase):
    def app(self, broker="redis://localhost/0", backend="redis://localhost/1"):
        app = Celery("visibility-audit", broker=broker, backend=backend, set_as_current=False)
        self.addCleanup(app.close)
        return app

    def test_all_presence_combinations_use_only_broker_options(self):
        for b, r, t in itertools.product((False, True), repeat=3):
            with self.subTest(B=b, R=r, T=t):
                app = self.app()
                app.conf.broker_transport_options = {"visibility_timeout": 17} if b else {}
                app.conf.result_backend_transport_options = {"visibility_timeout": 31} if r else {}
                if t:
                    app.conf.visibility_timeout = 47
                for factory in (app.connection_for_read, app.connection_for_write):
                    connection = factory()  # Constructs options; does not open a socket.
                    self.assertEqual(connection.transport_options, {"visibility_timeout": 17} if b else {})
                    self.assertEqual(connection.transport_options.get("visibility_timeout", Channel.visibility_timeout),
                                     17 if b else 3600)
                    connection.close()
                self.assertNotIn("visibility_timeout", app.backend.connparams)
                self.assertEqual(app.backend.expires, 86400)

    def test_explicit_per_connection_options_override_broker_config(self):
        app = self.app()
        app.conf.broker_transport_options = {"visibility_timeout": 17}
        connection = app.connection_for_read(transport_options={"visibility_timeout": 23})
        self.assertEqual(connection.transport_options["visibility_timeout"], 23)
        self.assertEqual(app.conf.broker_transport_options["visibility_timeout"], 17)
        connection.close()

    def test_broker_construction_is_not_backend_transport_dependent(self):
        # Mock the final constructor to inspect Celery's generic wiring without
        # requiring SQS/PGMQ/SLMQ drivers or connecting to any service.
        for url in ("redis://localhost/0", "sqs://", "pgmq://localhost/db", "slmq://localhost", "amqp://localhost//"):
            with self.subTest(url=url):
                app = self.app(broker=url)
                app.conf.update(broker_transport_options={"visibility_timeout": 17},
                                result_backend_transport_options={"visibility_timeout": 31}, visibility_timeout=47)
                with patch.object(app.amqp, "Connection") as factory:
                    app.connection_for_read()
                    self.assertEqual(factory.call_args.kwargs["transport_options"], {"visibility_timeout": 17})

    def test_redis_backend_uses_supported_options_not_visibility(self):
        app = self.app()
        opts = ReadTrackingDict(visibility_timeout=31, global_keyprefix="audit:",
                                retry_policy={"max_retries": 9}, result_chord_ordered=False)
        app.conf.result_backend_transport_options = opts
        app.conf.visibility_timeout = 47
        settings_type = type(app.conf)
        original = settings_type.__getitem__
        accesses = []
        def traced(settings, key):
            accesses.append(key)
            return original(settings, key)
        with patch.object(settings_type, "__getitem__", traced):
            backend = app.backend
            self.assertTrue(backend.get_key_for_task("id").startswith(b"audit:"))
            self.assertEqual(backend.retry_policy["max_retries"], 9)
            self.assertFalse(backend._chord_zset)
            self.assertNotIn("visibility_timeout", backend.connparams)
            app.connection_for_read().close()
        self.assertNotIn("visibility_timeout", accesses)
        self.assertNotIn("visibility_timeout", opts.reads)
        self.assertIn("global_keyprefix", opts.reads)
        self.assertIn("retry_policy", opts.reads)

    def test_sentinel_backend_uses_sentinel_options_not_visibility(self):
        app = self.app(backend="sentinel://localhost:26379/1")
        opts = ReadTrackingDict(visibility_timeout=31, master_name="audit-master",
                                min_other_sentinels=2, sentinel_kwargs={"socket_timeout": 4})
        app.conf.result_backend_transport_options = opts
        backend = app.backend
        fake_module = Mock()
        with patch.object(backend, "sentinel", fake_module):
            pool = backend._get_pool(**backend.connparams)
        constructor = fake_module.Sentinel.call_args
        self.assertEqual(constructor.kwargs["min_other_sentinels"], 2)
        self.assertEqual(constructor.kwargs["sentinel_kwargs"], {"socket_timeout": 4})
        self.assertNotIn("visibility_timeout", constructor.kwargs)
        master = fake_module.Sentinel.return_value.master_for
        self.assertEqual(master.call_args.kwargs["service_name"], "audit-master")
        self.assertNotIn("visibility_timeout", master.call_args.kwargs)
        self.assertIs(pool, master.return_value.connection_pool)
        self.assertNotIn("visibility_timeout", opts.reads)

    def test_other_key_value_backend_uses_result_key_prefix(self):
        app = self.app(broker="memory://", backend="cache+memory://")
        app.conf.result_backend_transport_options = {"global_keyprefix": "audit:", "visibility_timeout": 31}
        self.assertTrue(app.backend.get_key_for_task("id").startswith(b"audit:"))

    def test_rpc_consumer_uses_app_broker_connection(self):
        app = self.app(broker="memory://", backend="rpc://")
        app.conf.update(broker_transport_options={"visibility_timeout": 17},
                        result_backend_transport_options={"visibility_timeout": 31}, visibility_timeout=47)
        consumer = app.backend.result_consumer
        consumer.start("audit-task")
        try:
            self.assertEqual(consumer._connection.transport_options, {"visibility_timeout": 17})
        finally:
            consumer.stop()

    def test_aliased_dictionaries_can_indirectly_change_broker_value(self):
        app = self.app()
        shared = {"visibility_timeout": 17}
        app.conf.broker_transport_options = app.conf.result_backend_transport_options = shared
        app.conf.result_backend_transport_options["visibility_timeout"] = 23
        self.assertIs(app.conf.broker_transport_options, app.conf.result_backend_transport_options)
        connection = app.connection_for_read()
        self.assertEqual(connection.transport_options["visibility_timeout"], 23)
        connection.close()
        # With independent dictionaries, changing R does not change B.
        app.conf.result_backend_transport_options = {"visibility_timeout": 99}
        self.assertEqual(app.conf.broker_transport_options["visibility_timeout"], 23)

    def test_application_can_explicitly_forward_top_level_value(self):
        app = self.app()
        app.conf.visibility_timeout = 47
        app.conf.broker_transport_options = {"visibility_timeout": app.conf.visibility_timeout}
        self.assertEqual(app.connection_for_read().transport_options["visibility_timeout"], 47)
        app.conf.visibility_timeout = 99
        self.assertEqual(app.connection_for_read().transport_options["visibility_timeout"], 47)


if __name__ == "__main__":
    unittest.main(verbosity=2)
