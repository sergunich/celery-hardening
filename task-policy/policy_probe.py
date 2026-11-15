"""Tasks and observation-only request hooks for time-limit/failure-policy tests.

Real CLI workers; original pool/request/transport implementations are unchanged.
The synthetic exceptions are explicitly separate from actual pool failures.
"""
import os
import time

from billiard.exceptions import WorkerLostError
from celery import signals
from celery.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded
from celery.worker.request import Request

from shutdown_probe import CONFIG, app, emit

app.conf.update(
    task_acks_late=CONFIG.get("acks_late", True),
    task_acks_on_failure=CONFIG.get("ack_failure", True),
    task_acks_on_timeout=CONFIG.get("ack_timeout", True),
    task_reject_on_worker_lost=CONFIG.get("reject_lost", False),
    task_retry_on_worker_lost=CONFIG.get("retry_lost", False),
    task_soft_time_limit=CONFIG.get("soft_limit"),
    task_time_limit=CONFIG.get("hard_limit"),
    worker_lost_wait=0.2,
    worker_prefetch_multiplier=CONFIG.get("prefetch", 1),
)


class ObservedRequest(Request):
    def on_accepted(self, pid, time_accepted):
        super().on_accepted(pid, time_accepted)
        emit("policy_accepted", task_id=self.id, executor_pid=pid,
             retries=self.request_dict.get("retries"), ack_requested=self.acknowledged)

    def on_timeout(self, soft, timeout):
        emit("policy_timeout", task_id=self.id, soft=soft, timeout=timeout)
        return super().on_timeout(soft, timeout)

    def on_failure(self, exc_info, *args, **kwargs):
        exc = exc_info.exception
        exc = getattr(exc, "exc", exc)
        emit("policy_failure", task_id=self.id, exception=type(exc).__name__,
             retries=self.request_dict.get("retries"))
        return super().on_failure(exc_info, *args, **kwargs)


@app.task(bind=True, name="policy_probe.work", Request=ObservedRequest,
          default_retry_delay=0.35, max_retries=CONFIG.get("max_retries", 1))
def work(self):
    # External attempt counter prevents uncontrolled raw-requeue loops without
    # altering the message's actual retries header. Dedicated private Redis only.
    attempt = self.backend.client.incr(f"policy-attempt:{self.request.id}")
    emit("policy_started", task_id=self.request.id, attempt=attempt,
         retries=self.request.retries, redelivered=self.request.delivery_info.get("redelivered", False))
    kind = CONFIG["kind"]
    try:
        if attempt > 1 and not CONFIG.get("always_fail"):
            time.sleep(0.05)
        elif kind == "fail":
            raise ValueError("intentional ordinary failure")
        elif kind in {"lost", "whole_exit"}:
            emit("policy_process_exit", task_id=self.request.id, attempt=attempt)
            os._exit(23)
        elif kind == "system_exit":
            raise SystemExit(23)
        elif kind == "synthetic_lost":
            raise WorkerLostError("synthetic task exception; no process died")
        elif kind == "synthetic_soft":
            raise SoftTimeLimitExceeded()
        elif kind == "synthetic_hard":
            raise TimeLimitExceeded(0.45)
        elif kind == "busy":
            end = time.monotonic() + 1.3
            while time.monotonic() < end:
                pass  # Deliberately never yields to gevent.
        else:
            try:
                end = time.monotonic() + 1.3
                while time.monotonic() < end:
                    time.sleep(0.03)
            except SoftTimeLimitExceeded as exc:
                emit("policy_soft_caught", task_id=self.request.id, action=CONFIG.get("soft_action"))
                if CONFIG.get("soft_action") == "retry":
                    raise self.retry(exc=exc)
                if CONFIG.get("soft_action") != "return":
                    raise
        emit("policy_completed", task_id=self.request.id, attempt=attempt)
        return "completed"
    except BaseException as exc:
        emit("policy_interrupted", task_id=self.request.id, exception=type(exc).__name__)
        raise
    finally:
        emit("policy_finally", task_id=self.request.id, attempt=attempt)


@app.task(name="policy_probe.ping", soft_time_limit=None, time_limit=None)
def ping():
    emit("policy_ping")
    return "alive"


@signals.task_received.connect(weak=False)
def received(request=None, **kwargs):
    emit("policy_received", task_id=request.id, task=request.name,
         retries=request.request_dict.get("retries"),
         redelivered=request.delivery_info.get("redelivered", False))


@signals.after_task_publish.connect(weak=False)
def published(headers=None, **kwargs):
    emit("policy_published", task_id=headers.get("id"), retries=headers.get("retries"), eta=headers.get("eta"))


@signals.task_postrun.connect(weak=False)
def after_return(task_id=None, task=None, state=None, **kwargs):
    if task.name == "policy_probe.work" and CONFIG["kind"] == "success_then_lost" and state == "SUCCESS":
        emit("policy_success_then_exit", task_id=task_id)
        os._exit(23)


@signals.worker_ready.connect(weak=False)
def resolved_settings(sender=None, **kwargs):
    if CONFIG["pool"] == "threads":
        # Observe failures stored in Future objects, which otherwise need not
        # appear in worker.log. Delegate the original submission unchanged.
        original = sender.pool.on_apply

        def observe_future(future):
            if not future.cancelled() and (exc := future.exception()) is not None:
                emit("policy_future_error", exception=type(exc).__name__, message=str(exc))

        def submit(*args, **options):
            result = original(*args, **options)
            result.f.add_done_callback(observe_future)
            return result

        sender.pool.on_apply = submit
    emit("policy_settings", **{k: getattr(work, k) for k in (
        "acks_late", "acks_on_failure", "acks_on_timeout", "reject_on_worker_lost",
        "retry_on_worker_lost", "soft_time_limit", "time_limit", "max_retries", "default_retry_delay")})
