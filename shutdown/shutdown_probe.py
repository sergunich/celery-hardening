"""Real tasks and observation-only hooks for the multi-pool CLI experiment.

Imported via `python -m celery -A shutdown_probe:app worker --pool ...`.
For gevent, Celery performs its normal early monkey-patching before importing us.
"""
import hashlib
import json
import os
import threading
import time

from billiard import get_start_method
from celery import Celery, signals
from celery.apps import worker as worker_module
from kombu.message import Message

CONFIG = json.loads(os.environ["SHUTDOWN_PROBE_CONFIG"])
FD = os.open(CONFIG["events"], os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)


def emit(event, **fields):
    # Each small JSON record is a single O_APPEND write (shared by pool children).
    record = dict(event=event, time=time.monotonic(), pid=os.getpid(),
                  thread=threading.get_ident(), **fields)
    os.write(FD, (json.dumps(record) + "\n").encode())


app = Celery("shutdown_probe", broker=CONFIG["broker"], backend=CONFIG["backend"])
app.conf.update(
    task_acks_late=CONFIG.get("acks_late", False),
    task_acks_on_failure_or_timeout=True,
    worker_soft_shutdown_timeout=CONFIG.get("soft_timeout", 0),
    worker_enable_soft_shutdown_on_idle=CONFIG.get("soft_on_idle", False),
    worker_prefetch_multiplier=1,
    worker_redirect_stdouts=False,
    worker_pool_start_method="fork",
    broker_transport_options={"visibility_timeout": 3600, "polling_interval": 0.1},
    result_backend_transport_options={"visibility_timeout": 3600},
    visibility_timeout=3600,
    task_default_queue=CONFIG["queue"],
)

# Observe completed transport operations, not just Request.acknowledged flags.
# The original operations run unchanged; no exceptions are swallowed.
_original_ack = Message.ack
_original_reject = Message.reject


def observed_ack(self, *args, **kwargs):
    result = _original_ack(self, *args, **kwargs)
    emit("message_ack", task_id=self.headers.get("id"), tag=self.delivery_tag)
    return result


def observed_reject(self, *args, **kwargs):
    result = _original_reject(self, *args, **kwargs)
    emit("message_reject", task_id=self.headers.get("id"),
         requeue=kwargs.get("requeue", args[0] if args else False))
    return result


Message.ack = observed_ack
Message.reject = observed_reject

# Record whether escalation actually reaches the hard handler. Some workers
# exit from an earlier cancellation error before the fourth signal can arrive.
_original_hard_shutdown = worker_module.on_hard_shutdown


def observed_hard_shutdown(worker):
    emit("hard_handler_entered")
    return _original_hard_shutdown(worker)


worker_module.on_hard_shutdown = observed_hard_shutdown


def native_work():
    emit("native_started", iterations=CONFIG["native_iterations"])
    hashlib.pbkdf2_hmac("sha256", b"probe", b"salt", CONFIG["native_iterations"])
    emit("native_returned")


@app.task(name="shutdown_probe.task")
def probe():
    emit("task_started", multiprocessing_start_method=get_start_method())
    steps = CONFIG.get("steps", 20)
    catch = CONFIG.get("catch", "none")
    completed = 0
    try:
        if CONFIG.get("native"):
            native_work()
        while steps is None or completed < steps:
            try:
                time.sleep(0.1)
                completed += 1
                emit("task_tick", step=completed)
            except BaseException as exc:
                if catch == "requeue":
                    from celery.exceptions import Reject
                    from celery.worker import state
                    # Experiment-only, version-specific recognition of the
                    # unsupported solo cancellation path; not a public API.
                    terminating = state.should_terminate is not None and state.should_terminate is not False
                    if isinstance(exc, NotImplementedError) and terminating and "does not implement kill_job" in str(exc):
                        emit("task_requeue_requested", exception=type(exc).__name__)
                        raise Reject("solo worker shutting down", requeue=True) from exc
                    raise
                if catch == "base" or (catch == "exception" and isinstance(exc, Exception)):
                    emit("task_caught", exception=type(exc).__name__, message=str(exc))
                else:
                    emit("task_interrupted", exception=type(exc).__name__, message=str(exc))
                    raise
        emit("task_completed")
        return "completed"
    finally:
        emit("task_finally")


@signals.worker_ready.connect(weak=False)
def ready(**kwargs):
    emit("worker_ready", pool=CONFIG["pool"], multiprocessing_start_method=get_start_method())


@signals.worker_shutting_down.connect(weak=False)
def shutting_down(sig=None, how=None, **kwargs):
    emit("worker_shutting_down", signal=sig, how=how)


@signals.worker_shutdown.connect(weak=False)
def shutdown(**kwargs):
    emit("worker_shutdown")


@signals.task_failure.connect(weak=False)
def failed(exception=None, **kwargs):
    emit("task_failure", exception=type(exception).__name__, message=str(exception))


@signals.task_success.connect(weak=False)
def succeeded(**kwargs):
    emit("task_success")


@signals.task_postrun.connect(weak=False)
def finished(state=None, **kwargs):
    emit("task_postrun", state=state)


@signals.task_retry.connect(weak=False)
def retried(reason=None, **kwargs):
    emit("task_retry", reason=str(reason))
