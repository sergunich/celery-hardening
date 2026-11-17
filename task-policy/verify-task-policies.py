#!/usr/bin/env python3
"""Focused real-worker comparison of time limits, ACK policy, and executor loss.

All resources/artifacts stay in this repository; only our private Redis is flushed.
--case may be repeated. PASS reproduces a behavior, not necessarily correct behavior.
"""
import argparse
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import time
import uuid

SUITE = Path(__file__).resolve().parent
sys.path.insert(0, str(SUITE.parent))
from environment.runtime import (
    ROOT, activate_sources, dependency_version, provenance, pythonpath, source_revision, private_redis,
)
activate_sources()
spec = importlib.util.spec_from_file_location("shutdown_helpers", ROOT / "shutdown/verify-pool-shutdown.py")
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)
Celery = helpers.Celery
POOLS = ("solo", "prefork", "threads", "gevent")
CASES = []


def add(name, pool, kind, *, starts=1, completed=0, acks=1, rejects=(), state="FAILURE",
        publications=0, timeouts=(), failure=None, **config):
    CASES.append(dict(name=name, pool=pool, kind=kind, **config,
        expected=dict(starts=starts, completed=completed, acks=acks, rejects=list(rejects),
                      state=state, publications=publications, timeouts=list(timeouts), failure=failure)))


for pool in POOLS:
    for ack in (True, False):
        add(f"failure-{pool}-{'ack' if ack else 'discard'}", pool, "fail", ack_failure=ack,
            ack_timeout=not ack, acks=int(ack), rejects=() if ack else (False,), failure="ValueError")
    add(f"soft-{pool}", pool, "sleep", soft_limit=0.35, ack_timeout=False,
        completed=int(pool != "prefork"), state="FAILURE" if pool == "prefork" else "SUCCESS",
        timeouts=(True,) if pool == "prefork" else (),
        failure="SoftTimeLimitExceeded" if pool == "prefork" else None)
    supported = pool in {"prefork", "gevent"}
    add(f"hard-{pool}", pool, "sleep", hard_limit=0.45, ack_failure=False,
        completed=int(not supported), state="FAILURE" if supported else "SUCCESS",
        timeouts=(False,) if supported else ())

add("failure-solo-early", "solo", "fail", acks_late=False, ack_failure=False,
    ack_timeout=False, reject_lost=True, retry_lost=True, failure="ValueError")
add("soft-prefork-discard", "prefork", "sleep", soft_limit=0.35, ack_failure=False,
    acks=0, rejects=(False,), timeouts=(True,), failure="SoftTimeLimitExceeded")
add("soft-prefork-caught", "prefork", "sleep", soft_limit=0.35, soft_action="return",
    completed=1, state="SUCCESS", timeouts=(True,))
add("soft-prefork-retry", "prefork", "sleep", soft_limit=0.35, soft_action="retry",
    starts=2, completed=1, acks=2, state="SUCCESS", publications=1, timeouts=(True,))
add("soft-solo-synthetic", "solo", "synthetic_soft", ack_failure=False,
    acks=0, rejects=(False,), failure="SoftTimeLimitExceeded")
for pool in ("prefork", "gevent"):
    add(f"hard-{pool}-requeue", pool, "sleep", hard_limit=0.45, ack_timeout=False,
        starts=2, completed=1, acks=1, rejects=(True,), state="SUCCESS", timeouts=(False,))
add("hard-gevent-busy", "gevent", "busy", hard_limit=0.45, completed=1, state="SUCCESS", timeouts=(False,))
add("hard-gevent-busy-requeue", "gevent", "busy", hard_limit=0.45, ack_timeout=False,
    starts=2, completed=2, acks=1, rejects=(True,), state="SUCCESS", timeouts=(False,))
add("hard-prefork-lost-flags", "prefork", "sleep", hard_limit=0.45,
    reject_lost=True, retry_lost=True, timeouts=(False,))
add("hard-solo-synthetic", "solo", "synthetic_hard", ack_timeout=False,
    starts=2, completed=1, acks=1, rejects=(True,), state="SUCCESS", failure="TimeLimitExceeded")
add("lost-prefork-ack", "prefork", "lost", failure="WorkerLostError")
add("lost-prefork-discard", "prefork", "lost", ack_failure=False, acks=0,
    rejects=(False,), failure="WorkerLostError")
add("lost-prefork-requeue", "prefork", "lost", reject_lost=True, max_retries=0,
    starts=2, completed=1, acks=1, rejects=(True,), state="SUCCESS", failure="WorkerLostError")
add("lost-prefork-retry", "prefork", "lost", reject_lost=True, retry_lost=True,
    starts=2, completed=1, acks=2, state="SUCCESS", publications=1, failure="WorkerLostError")
add("lost-prefork-early-retry", "prefork", "lost", acks_late=False, retry_lost=True,
    starts=2, completed=1, acks=2, state="SUCCESS", publications=1, failure="WorkerLostError")
add("lost-prefork-early-reject", "prefork", "lost", acks_late=False,
    reject_lost=True, failure="WorkerLostError")
for ack in (True, False):
    add(f"lost-prefork-exhausted-{'ack' if ack else 'discard'}", "prefork", "lost",
        always_fail=True, reject_lost=True, retry_lost=True, ack_failure=ack,
        starts=2, acks=2 if ack else 1, rejects=() if ack else (False,),
        publications=1, failure="WorkerLostError")
add("lost-prefork-after-success", "prefork", "success_then_lost",
    reject_lost=True, retry_lost=True, completed=1, state="SUCCESS", failure="WorkerLostError")
add("lost-solo-whole-process", "solo", "whole_exit", reject_lost=True, retry_lost=True,
    acks=0, state="PENDING")
for pool in POOLS:
    if pool == "prefork":
        add("system-exit-prefork", pool, "system_exit", retry_lost=True,
            starts=2, completed=1, acks=2, state="SUCCESS", publications=1)
    else:
        add(f"system-exit-{pool}", pool, "system_exit", reject_lost=True, retry_lost=True,
            acks=0, state="PENDING")
for pool in ("threads", "gevent"):
    add(f"system-exit-{pool}-credit", pool, "system_exit", prefetch=2,
        reject_lost=True, retry_lost=True, acks=0, state="PENDING")
add("lost-solo-synthetic-requeue", "solo", "synthetic_lost", reject_lost=True,
    starts=2, completed=1, acks=1, rejects=(True,), state="SUCCESS", failure="WorkerLostError")
add("lost-solo-synthetic-retry", "solo", "synthetic_lost", retry_lost=True,
    starts=2, completed=1, acks=2, state="SUCCESS", publications=1, failure="WorkerLostError")
for ack in (True, False):
    add(f"quit-solo-{'ack' if ack else 'discard'}", "solo", "quit", ack_failure=ack,
        ack_timeout=False, reject_lost=True, retry_lost=True, acks=int(ack),
        rejects=() if ack else (False,), failure="NotImplementedError")


def cleanup_group(process):
    """Kill only live members of our isolated worker group, never other users.

    macOS can report EPERM when a group has only dying/zombie processes. Recheck
    membership rather than blindly retrying or hiding errors for live processes.
    """
    def members():
        table = subprocess.check_output(["ps", "-axo", "pid=,pgid=,uid=,stat="], text=True)
        found = []
        for line in table.splitlines():
            pid, pgid, uid, status = line.split()
            if int(pgid) == process.pid and not status.startswith("Z"):
                found.append((int(pid), int(uid)))
        return found

    remaining = members()
    if not remaining:
        return
    if any(uid != os.getuid() for _, uid in remaining):
        raise RuntimeError("Refusing to signal a process group containing another user's process")
    try:
        helpers.kill_group(process.pid)
    except PermissionError:
        if members():
            raise


def run_case(case, run_dir, client, broker, backend):
    folder = run_dir / case["name"]
    folder.mkdir()
    client.flushall()
    event_path = folder / "events.jsonl"
    config = dict(case, broker=broker, backend=backend, events=str(event_path.relative_to(ROOT)), queue="policy-probe")
    (folder / "config.json").write_text(json.dumps(config, indent=2) + "\n")
    env = os.environ.copy()
    for key in ("REMAP_SIGTERM", "CELERY_RDBSIG"):
        env.pop(key, None)
    env.update(PYTHONPATH=pythonpath(SUITE, ROOT / "shutdown"), PYTHONDONTWRITEBYTECODE="1",
               PYTHONUNBUFFERED="1", SHUTDOWN_PROBE_CONFIG=json.dumps(config))
    command = [sys.executable, "-m", "celery", "-A", "policy_probe:app", "worker",
               "--pool", case["pool"], "--concurrency", "1", "--hostname", "policy-probe@localhost",
               "--loglevel", "INFO", "--without-gossip", "--without-mingle", "--without-heartbeat",
               "--queues", "policy-probe"]
    publisher = Celery("policy_publisher", broker=broker, backend=backend)
    task_id = str(uuid.uuid4())
    sent, states, snapshots = [], [], {}
    forced = False
    harness_error = None
    pinged = False
    started = time.monotonic()

    def events():
        return helpers.read_events(event_path)

    def own(ev, name):
        return [e for e in ev if e["event"] == name and e.get("task_id") == task_id]

    def checkpoint(name):
        snapshots[name] = helpers.broker_snapshot(client, task_id)

    def record_state():
        meta = publisher.backend.get_task_meta(task_id, cache=False)
        value = dict(state=meta["status"], detail=str(meta.get("result")))
        if not states or any(states[-1][k] != v for k, v in value.items()):
            states.append(dict(time=time.monotonic(), **value))
        return meta["status"]

    with (folder / "worker.log").open("w") as log:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        try:
            until = time.monotonic() + 25
            while not any(e["event"] == "policy_settings" for e in events()):
                if process.poll() is not None or time.monotonic() > until:
                    raise RuntimeError("worker did not become ready")
                time.sleep(0.02)
            publisher.send_task("policy_probe.work", task_id=task_id, queue="policy-probe")
            until = time.monotonic() + 18
            origin = None
            while True:
                ev = events()
                starts = own(ev, "policy_started")
                if starts and origin is None:
                    origin = starts[0]["time"]
                    checkpoint("first_start")
                record_state()
                if case["kind"] == "quit" and origin and not sent and time.monotonic() - origin >= 0.2:
                    sent.append(dict(signal="SIGQUIT", time=time.monotonic()))
                    os.kill(process.pid, signal.SIGQUIT)
                if process.poll() is not None:
                    break
                if case["kind"] == "system_exit" and case["pool"] != "prefork":
                    if origin and time.monotonic() - origin > 1.5:
                        break
                elif case["kind"] != "whole_exit":
                    count = len(own(ev, "message_ack")) + len(own(ev, "message_reject"))
                    expected = case["expected"]
                    if (len(starts) >= expected["starts"] and count >= expected["acks"] + len(expected["rejects"])
                            and len(own(ev, "policy_completed")) >= expected["completed"]
                            and (not expected["failure"] or own(ev, "policy_failure"))):
                        break
                if time.monotonic() > until:
                    raise RuntimeError("task did not reach expected terminal observation within 18 s")
                time.sleep(0.02)
            time.sleep(0.25)
            checkpoint("before_shutdown")
            record_state()
            if process.poll() is None and case["kind"] != "quit":
                publisher.send_task("policy_probe.ping", queue="policy-probe")
                # Prefork may need Billiard's delayed SIGKILL fallback before
                # a replacement child can accept this liveness probe.
                until = time.monotonic() + (10 if case["pool"] == "prefork" else 4)
                while process.poll() is None and time.monotonic() < until:
                    if any(e["event"] == "policy_ping" for e in events()):
                        pinged = True
                        break
                    time.sleep(0.02)
            if process.poll() is None:
                sent.append(dict(signal="SIGTERM", time=time.monotonic()))
                os.kill(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                forced = True
                helpers.kill_group(process.pid)
                process.wait(timeout=5)
        except Exception as exc:
            harness_error = f"{type(exc).__name__}: {exc}"
        finally:
            cleanup_group(process)
            process.wait(timeout=5)
    time.sleep(0.15)
    ev = events()
    final_state = record_state()
    checkpoint("after_exit")
    fetched = []
    if snapshots["after_exit"]["queued"]:
        with publisher.connection_for_read() as conn:
            queue = conn.SimpleQueue("policy-probe")
            try:
                while True:
                    try:
                        message = queue.get(block=False)
                    except queue.Empty:
                        break
                    fetched.append(dict(task_id=message.headers.get("id"), retries=message.headers.get("retries"),
                                        redelivered=message.delivery_info.get("redelivered")))
                    message.ack()
            finally:
                queue.close()
    o = dict(case=case, task_id=task_id, timeline=ev, states=states, final_state=final_state,
             snapshots=snapshots, fetched_after_exit=fetched, signals=sent, pinged=pinged,
             forced_kill=forced, returncode=process.returncode, elapsed=round(time.monotonic() - started, 3),
             harness_error=harness_error)
    exp = case["expected"]
    observations = dict(starts=len(own(ev, "policy_started")), completed=len(own(ev, "policy_completed")),
                        acks=len(own(ev, "message_ack")), rejects=[e["requeue"] for e in own(ev, "message_reject")],
                        state=final_state, publications=len(own(ev, "policy_published")),
                        timeouts=[e["soft"] for e in own(ev, "policy_timeout")])
    errors = [f"{k}: expected {v!r}, observed {observations[k]!r}" for k, v in exp.items()
              if k != "failure" and observations[k] != v]
    resolved = next((e for e in ev if e["event"] == "policy_settings"), {})
    for attr, key, default in (
        ("acks_late", "acks_late", True), ("acks_on_failure", "ack_failure", True),
        ("acks_on_timeout", "ack_timeout", True), ("reject_on_worker_lost", "reject_lost", False),
        ("retry_on_worker_lost", "retry_lost", False), ("soft_time_limit", "soft_limit", None),
        ("time_limit", "hard_limit", None), ("max_retries", "max_retries", 1),
    ):
        if attr not in resolved or resolved[attr] != case.get(key, default):
            errors.append(f"resolved {attr} does not match case configuration")
    if exp["failure"] and exp["failure"] not in [e["exception"] for e in own(ev, "policy_failure")]:
        errors.append(f"missing Request.on_failure({exp['failure']})")
    if harness_error:
        errors.append(harness_error)
    if forced:
        errors.append("safety-deadline kill")
    if case["kind"] not in {"quit", "whole_exit", "system_exit"} and not pinged:
        errors.append("worker did not execute a subsequent ping task")
    starts = own(ev, "policy_started")
    if exp["starts"] == 2 and len(starts) == 2:
        wanted_retries = [0, 1 if exp["publications"] else 0]
        if [e["retries"] for e in starts] != wanted_retries:
            errors.append("unexpected retry headers on successive executions")
        if True in exp["rejects"] and not starts[1]["redelivered"]:
            errors.append("requeued message must be marked redelivered on second execution")
    if case["kind"] == "system_exit" and case["pool"] != "prefork":
        if observations["publications"] or own(ev, "policy_failure"):
            errors.append("broken callback path unexpectedly reached lost-task policy")
        if case["pool"] == "threads":
            if not any(e["event"] == "policy_future_error" and e["exception"] == "TypeError" for e in ev):
                errors.append("expected callback TypeError in thread Future")
        elif "cannot unpack non-iterable ExceptionInfo object" not in (folder / "worker.log").read_text():
            errors.append("expected callback TypeError in worker log")
        if case["pool"] != "solo" and pinged != (case.get("prefetch", 1) > 1):
            errors.append("subsequent ping must reflect remaining delivery credit")
        if not snapshots["after_exit"]["queued"]:
            errors.append("failed delivery must be restored on worker cleanup")
    if case["kind"] == "busy":
        done = own(ev, "policy_completed")
        expired = own(ev, "policy_timeout")
        if not (done and expired and expired[0]["time"] > done[0]["time"]):
            errors.append("gevent deadline must fire only after non-yielding task body completed")
    if case["kind"] == "whole_exit":
        if not snapshots["after_exit"]["unacked"] or snapshots["after_exit"]["queued"]:
            errors.append("whole-process exit must leave Redis delivery unacked, not immediately queued")
    if case["kind"] not in {"whole_exit", "system_exit"} and any(snapshots["after_exit"].values()):
        errors.append("handled deliveries must not remain queued or unacked after exit")
    if snapshots["after_exit"]["queued"] and not any(e["task_id"] == task_id for e in fetched):
        errors.append("queued original message was not fetchable")
    o.update(observations=observations, errors=errors, passed=not errors)
    (folder / "result.json").write_text(json.dumps(o, indent=2) + "\n")
    publisher.close()
    print(f"{'PASS' if o['passed'] else 'FAIL'} {case['name']:36} {observations} "
          f"exit={o['returncode']} ping={pinged} {o['elapsed']}s", flush=True)
    for error in errors:
        print(f"  {error}", flush=True)
    return o


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=[c["name"] for c in CASES], action="append")
    args = parser.parse_args()
    run_dir = SUITE / "task-policy-runs" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    run_dir.mkdir(parents=True)
    summary = dict(provenance=provenance(), python=sys.version, platform=platform.platform(),
        revision=source_revision(),
        celery=__import__("celery").__version__,
        dependencies={n: dependency_version(n) for n in ("billiard", "kombu", "gevent", "greenlet", "redis")},
        outcomes=[])
    with helpers.private_redis(run_dir) as (client, broker, backend):
        summary["redis_server"] = client.info("server")["redis_version"]
        for case in CASES:
            if args.case and case["name"] not in args.case:
                continue
            summary["outcomes"].append(run_case(case, run_dir, client, broker, backend))
            (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Artifacts: {run_dir.relative_to(ROOT)}")
    return 0 if all(o["passed"] for o in summary["outcomes"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
