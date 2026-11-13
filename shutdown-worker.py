#!/usr/bin/env python3
"""A real solo worker, with tasks and observation hooks, not mocked shutdowns."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from celery import Celery, signals  # noqa: E402

parser = argparse.ArgumentParser()
parser.add_argument("--config", required=True)
parser.add_argument("--events", required=True)
args = parser.parse_args()
config = json.loads(args.config)
fd = os.open(args.events, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)


def emit(event, **fields):
    record = dict(event=event, time=time.monotonic(), pid=os.getpid(), **fields)
    os.write(fd, (json.dumps(record) + "\n").encode())


app = Celery("solo_shutdown_probe", broker="memory://", backend="cache+memory://")
app.conf.update(
    worker_soft_shutdown_timeout=config.get("soft_timeout", 0),
    worker_enable_soft_shutdown_on_idle=config.get("soft_on_idle", False),
    worker_redirect_stdouts=False,
    broker_transport_options={"polling_interval": 0.05},
    task_acks_late=config.get("acks_late", False),
    task_acks_on_failure_or_timeout=True,
    worker_prefetch_multiplier=1,
)


native_iterations = None
if config.get("native_first") or config.get("native_on_cancel"):
    # PBKDF2 runs in native code without returning to Python to run handlers.
    # Calibrate to ~2 seconds on this machine rather than assume CPU speed.
    before = time.monotonic()
    hashlib.pbkdf2_hmac("sha256", b"probe", b"salt", 200_000)
    native_iterations = max(200_000, int(200_000 * 2.0 / (time.monotonic() - before)))


def native_work():
    emit("native_started", iterations=native_iterations)
    hashlib.pbkdf2_hmac("sha256", b"probe", b"salt", native_iterations)
    emit("native_returned")


def remote_shutdown():
    time.sleep(0.3)
    emit("remote_shutdown_sent")
    app.control.broadcast("shutdown")


@app.task(name="solo_shutdown_probe.task")
def probe():
    emit("task_started", acks_late=app.conf.task_acks_late)
    if config.get("remote"):
        threading.Thread(target=remote_shutdown, daemon=True).start()
    catch = config.get("catch", "none")
    steps = config.get("steps", 20)
    completed = 0
    try:
        if config.get("native_first"):
            native_work()
        while steps is None or completed < steps:
            try:
                time.sleep(0.1)
                completed += 1
                emit("task_tick", step=completed)
            except BaseException as exc:
                # Deliberate application-level catch variants demonstrate why
                # cancellation/hard shutdown are not unconditional termination.
                if catch == "base" or (catch == "exception" and isinstance(exc, Exception)):
                    emit("task_caught", exception=type(exc).__name__, message=str(exc))
                    if config.get("native_on_cancel"):
                        native_work()
                else:
                    raise
        emit("task_completed")
        return "completed"
    finally:
        emit("task_finally")


@signals.task_failure.connect(weak=False)
def task_failed(exception=None, **kwargs):
    emit("task_failure", exception=type(exception).__name__, message=str(exception))


@signals.task_success.connect(weak=False)
def task_succeeded(**kwargs):
    emit("task_success")


@signals.task_postrun.connect(weak=False)
def task_finished(state=None, **kwargs):
    emit("task_postrun", state=state)


@signals.worker_shutting_down.connect(weak=False)
def shutting_down(sig=None, how=None, **kwargs):
    emit("worker_shutting_down", signal=sig, how=how)


@signals.worker_shutdown.connect(weak=False)
def shutdown(**kwargs):
    emit("worker_shutdown")


result = None
received_requests = []


@signals.task_received.connect(weak=False)
def received(request=None, **kwargs):
    # Retain the real Request for a final acknowledgment/cancellation snapshot.
    received_requests.append(request)


@signals.worker_ready.connect(weak=False)
def ready(**kwargs):
    global result
    emit("worker_ready")
    if not config.get("idle"):
        result = probe.delay()


worker = app.Worker(
    pool="solo", concurrency=1, loglevel="INFO", hostname="solo-probe@localhost",
    without_gossip=True, without_mingle=True, without_heartbeat=True,
)
emit("worker_constructed", pool=type(worker.pool).__module__)
worker.start()
emit("worker_returned", exitcode=worker.exitcode,
     result_state=result.state if result is not None else None,
     requests=[dict(acknowledged=r.acknowledged, cancelled=r._already_cancelled,
                    revoked=r._already_revoked) for r in received_requests])
sys.exit(worker.exitcode or 0)
