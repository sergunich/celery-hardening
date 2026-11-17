#!/usr/bin/env python3
"""Connection-free checks for the application's opt-in hardening examples."""
from datetime import datetime, timezone
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from environment.runtime import activate_sources
activate_sources()
from celery import Celery, group
from celery.exceptions import Reject, WorkerShutdown, WorkerTerminate
from celery.worker import state

from solo_hardening import (
    ImmediateCelery, SchedulingForbidden, SOLO_KILL_JOB_ERROR,
    TaskProcessExit, solo_task_guard,
)


class GuardTests(unittest.TestCase):
    def raise_guarded(self, exc):
        @solo_task_guard
        def body():
            raise exc
        return body()

    def test_success_preserves_metadata_and_result(self):
        @solo_task_guard
        def body(value):
            """Example task."""
            return value
        self.assertEqual(body(7), 7)
        self.assertEqual(body.__name__, "body")
        self.assertEqual(body.__doc__, "Example task.")

    def test_exact_solo_cold_error_requests_requeue(self):
        for flag in (0, 1, True):
            with self.subTest(flag=flag), patch.object(state, "should_terminate", flag):
                with self.assertRaises(Reject) as caught:
                    self.raise_guarded(NotImplementedError(SOLO_KILL_JOB_ERROR))
                self.assertTrue(caught.exception.requeue)
                self.assertIsInstance(caught.exception.__cause__, NotImplementedError)

    def test_no_cold_request_does_not_convert_error(self):
        for flag in (None, False):
            with self.subTest(flag=flag), patch.object(state, "should_terminate", flag):
                error = NotImplementedError(SOLO_KILL_JOB_ERROR)
                with self.assertRaises(NotImplementedError) as caught:
                    self.raise_guarded(error)
                self.assertIs(caught.exception, error)

    def test_unrelated_errors_are_unchanged_even_during_cold_shutdown(self):
        with patch.object(state, "should_terminate", 0):
            for error in (NotImplementedError("application error"), ValueError("bad value")):
                with self.subTest(error=error), self.assertRaises(type(error)) as caught:
                    self.raise_guarded(error)
                self.assertIs(caught.exception, error)

    def test_plain_system_exit_is_ordinary_failure(self):
        with self.assertRaises(TaskProcessExit) as caught:
            self.raise_guarded(SystemExit(23))
        self.assertIsInstance(caught.exception.__cause__, SystemExit)

    def test_worker_exit_and_other_base_exceptions_escape(self):
        for error in (WorkerShutdown(0), WorkerTerminate(1), KeyboardInterrupt(), GeneratorExit()):
            with self.subTest(error=type(error).__name__), self.assertRaises(type(error)) as caught:
                self.raise_guarded(error)
            self.assertIs(caught.exception, error)

    def test_reject_exclusion_prevents_broad_autoretry_from_consuming_guard(self):
        app = ImmediateCelery('guard_test', broker='memory://', backend='cache+memory://',
                              set_as_current=False)
        self.addCleanup(app.close)
        @app.task(name='guard_test.task', shared=False, autoretry_for=(Exception,),
                  dont_autoretry_for=(Reject, TaskProcessExit, SchedulingForbidden))
        @solo_task_guard
        def body():
            raise NotImplementedError(SOLO_KILL_JOB_ERROR)
        with patch.object(state, "should_terminate", 0), patch.object(body, 'retry') as retry:
            with self.assertRaises(Reject):
                body.run()
            retry.assert_not_called()


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.app = ImmediateCelery('publication_test', broker='memory://',
                                   backend='cache+memory://', set_as_current=False)
        self.addCleanup(self.app.close)
        def work(value=None):
            return value
        self.task = self.app.task(name='publication_test.work', shared=False,
                                  default_retry_delay=0, max_retries=2)(work)
        self.publish = Mock()
        self.app.amqp.send_task_message = self.publish

    def test_immediate_send_task_delay_signature_and_group_publish(self):
        self.app.send_task(self.task.name, countdown=0)
        self.task.delay(1)
        self.task.s(2).apply_async()
        group([self.task.s(3)], app=self.app).apply_async()
        self.assertEqual(self.publish.call_count, 4)
        for call in self.publish.call_args_list:
            self.assertIsNone(call.args[2].headers['eta'])

    def test_scheduling_rejected_before_publication(self):
        options = [{'countdown': n} for n in (1, -1, float('nan'), '0')]
        options += [{'eta': datetime.now(timezone.utc)}, {'eta': ''}]
        for opts in options:
            with self.subTest(opts=opts), self.assertRaises(SchedulingForbidden):
                self.app.send_task(self.task.name, **opts)
        self.publish.assert_not_called()

    def test_task_signature_group_and_positional_arguments_cannot_schedule(self):
        attempts = (
            lambda: self.task.apply_async(countdown=1),
            lambda: self.task.s().set(countdown=1).apply_async(),
            lambda: group([self.task.s()], app=self.app).apply_async(countdown=1),
            lambda: self.app.send_task(self.task.name, (), {}, 1),
        )
        for attempt in attempts:
            with self.subTest(attempt=attempt), self.assertRaises(SchedulingForbidden):
                attempt()
        self.publish.assert_not_called()

    def push_worker_request(self):
        self.task.push_request(id='retry-test', called_directly=False, is_eager=False,
                               retries=0, args=(), kwargs={}, delivery_info={})
        self.addCleanup(self.task.pop_request)

    def test_explicit_immediate_retry_publishes_without_eta(self):
        self.push_worker_request()
        self.task.retry(countdown=0, throw=False)
        self.assertEqual(self.publish.call_count, 1)
        self.assertIsNone(self.publish.call_args.args[2].headers['eta'])
        self.assertEqual(self.publish.call_args.args[2].headers['retries'], 1)

    def test_delayed_retry_becomes_reject_without_requeue_in_stock_retry(self):
        self.push_worker_request()
        with self.assertRaises(Reject) as caught:
            self.task.retry(countdown=1)
        self.assertFalse(caught.exception.requeue)
        self.assertIsInstance(caught.exception.reason, SchedulingForbidden)
        self.publish.assert_not_called()

    def test_default_retry_delay_must_be_zero(self):
        self.push_worker_request()
        with patch.object(self.task, 'default_retry_delay', 180):
            with self.assertRaises(Reject) as caught:
                self.task.retry()
        self.assertFalse(caught.exception.requeue)
        self.publish.assert_not_called()

    def test_other_apps_can_bypass_policy(self):
        other = Celery('unprotected', broker='memory://', set_as_current=False)
        self.addCleanup(other.close)
        other.amqp.send_task_message = Mock()
        other.send_task(self.task.name, countdown=1)
        other.amqp.send_task_message.assert_called_once()


if __name__ == '__main__':
    unittest.main(verbosity=2)
