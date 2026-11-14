#!/usr/bin/env python3
"""Focused ETA/countdown, prefetch, and shutdown experiments against private Redis.

Run: .dev/.venv/bin/python .dev/verify-eta-prefetch.py [--case NAME]
Uses the existing shutdown suite's isolated Redis/process helpers and ACK observer.
"""
import argparse
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

DEV = Path(__file__).resolve().parent
ROOT = DEV.parent
spec = importlib.util.spec_from_file_location("shutdown_helpers", DEV / "verify-pool-shutdown.py")
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)
Celery = helpers.Celery

CASES = []
for pool in helpers.POOLS:
    CASES.append(dict(name=f"store-{pool}", pool=pool, kind="store"))
    for late in (False, True):
        CASES.append(dict(name=f"due-{pool}-{'late' if late else 'early'}", pool=pool, kind="due", acks_late=late))
CASES += [
    dict(name="cap-three", pool="prefork", kind="store", eta_limit=3),
    dict(name="cap-with-zero-prefetch", pool="prefork", kind="store", eta_limit=3, prefetch=0),
    dict(name="disable-prefetch", pool="prefork", kind="store", disable_prefetch=True),
    dict(name="cap-with-active", pool="prefork", kind="mixed_cap", eta_limit=3, prefetch=2, acks_late=True),
    dict(name="active-blocks-eta-credit", pool="prefork", kind="mixed_cap", eta_limit=3, acks_late=True),
    dict(name="cap-release", pool="prefork", kind="release", eta_limit=3, prefetch=2, acks_late=True),
    dict(name="initial-prefetch-above-cap", pool="prefork", kind="normal", eta_limit=3, prefetch=8, acks_late=True, count=6),
    dict(name="prefetch-early", pool="prefork", kind="normal", prefetch=2, count=3),
    dict(name="prefetch-late", pool="prefork", kind="normal", prefetch=2, count=3, acks_late=True),
    dict(name="cold-future-solo", pool="solo", kind="future_shutdown", shutdown="SIGQUIT"),
    dict(name="cold-future-prefork", pool="prefork", kind="future_shutdown", shutdown="SIGQUIT"),
    dict(name="soft-future-skipped", pool="prefork", kind="future_shutdown", shutdown="SIGQUIT", soft_timeout=1.0),
    dict(name="soft-future-enabled", pool="prefork", kind="future_shutdown", shutdown="SIGQUIT", soft_timeout=1.0, soft_on_idle=True),
    dict(name="soft-future-gevent", pool="gevent", kind="future_shutdown", shutdown="SIGQUIT", soft_timeout=1.0, soft_on_idle=True),
    dict(name="soft-idle-eta-becomes-due", pool="prefork", kind="future_shutdown", shutdown="SIGQUIT", soft_timeout=2.0,
         soft_on_idle=True, countdown=1.2, observe=0.3, future_count=1),
    dict(name="kill-future", pool="prefork", kind="future_shutdown", shutdown="SIGKILL"),
]
for pool in ("solo", "prefork", "gevent"):
    CASES.append(dict(name=f"cold-mixed-{pool}", pool=pool, kind="mixed_cold", shutdown="SIGQUIT", acks_late=True))


def run_case(case, run_dir, client, broker, backend):
    folder = run_dir / case["name"]
    folder.mkdir()
    client.flushall()  # Only the runner's private Redis server.
    event_path = folder / "events.jsonl"
    config = dict(case, events=str(event_path), broker=broker, backend=backend, queue="eta-probe")
    env = os.environ.copy()
    for name in ("REMAP_SIGTERM", "CELERY_RDBSIG"):
        env.pop(name, None)
    env.update(PYTHONPATH=os.pathsep.join([str(ROOT), str(DEV)]), PYTHONDONTWRITEBYTECODE="1",
               PYTHONUNBUFFERED="1", SHUTDOWN_PROBE_CONFIG=json.dumps(config))
    publisher = Celery("eta_probe_publisher", broker=broker, backend=backend)
    tasks, phases, sent = {}, {}, []
    forced = False
    started = time.monotonic()
    command = [sys.executable, "-m", "celery", "-A", "eta_probe:app", "worker",
               "--pool", case["pool"], "--concurrency", "1", "--hostname", "eta-probe@localhost",
               "--loglevel", "INFO", "--without-gossip", "--without-mingle", "--without-heartbeat",
               "--queues", "eta-probe"]
    (folder / "config.json").write_text(json.dumps(config, indent=2) + "\n")
    with (folder / "worker.log").open("w") as log:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)

        def events():
            return helpers.read_events(event_path)

        def wait_for(predicate, label, timeout=12):
            deadline = time.monotonic() + timeout
            while not predicate(events()):
                if process.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError(f"{case['name']}: did not reach {label}; see {folder / 'worker.log'}")
                time.sleep(0.01)

        def publish(role, duration=0.2, countdown=None, absolute=False):
            task_id = str(uuid.uuid4())
            options = {}
            if countdown is not None:
                options = {"eta": datetime.now(timezone.utc) + timedelta(seconds=countdown)} if absolute else {"countdown": countdown}
            published = time.monotonic()
            publisher.send_task("eta_probe.work", args=[role, duration], task_id=task_id, queue="eta-probe", **options)
            tasks[task_id] = dict(role=role, duration=duration, countdown=countdown, absolute=absolute, published=published)
            return task_id

        def checkpoint(name):
            observed = events()
            phases[name] = dict(
                time=time.monotonic(),
                scheduler=next((e for e in reversed(observed) if e["event"] == "scheduler_state"), None),
                received=[e["task_id"] for e in observed if e["event"] == "eta_received"],
                running=[e["task_id"] for e in observed if e["event"] == "eta_started"
                         and not any(x["event"] == "eta_completed" and x["task_id"] == e["task_id"] for x in observed)],
                completed=[e["task_id"] for e in observed if e["event"] == "eta_completed"],
                acked=[e["task_id"] for e in observed if e["event"] == "message_ack" and e.get("task_id") in tasks],
                broker={task_id: helpers.broker_snapshot(client, task_id) for task_id in tasks},
            )

        def has_event(name, task_id=None):
            return lambda ev: any(e["event"] == name and (task_id is None or e.get("task_id") == task_id) for e in ev)

        try:
            wait_for(has_event("worker_ready"), "worker ready", 25)
            wait_for(has_event("scheduler_state"), "scheduler observation")
            kind = case["kind"]
            if kind == "due":
                task_id = publish("due", duration=1.2, countdown=1.0)
                wait_for(has_event("eta_received", task_id), "ETA received")
                time.sleep(0.2)
                checkpoint("waiting")
                wait_for(has_event("eta_started", task_id), "ETA execution")
                time.sleep(0.25)
                checkpoint("running")
                wait_for(has_event("eta_completed", task_id), "ETA completion")
                time.sleep(0.25)
                checkpoint("completed")
            elif kind == "normal":
                for i in range(case["count"]):
                    publish(f"normal-{i}", duration=2.0)
                wait_for(has_event("eta_started"), "normal task running")
                time.sleep(0.5)
                checkpoint("loaded")
            else:
                if kind == "mixed_cap":
                    task_id = publish("active", duration=2.5)
                    wait_for(has_event("eta_started", task_id), "active task")
                count = 3 if kind in {"release", "mixed_cold", "future_shutdown"} else 5
                count = case.get("future_count", count)
                delay = case.get("countdown", 2.0 if kind == "release" else 60.0)
                for i in range(count):
                    publish(f"future-{i}", countdown=delay, absolute=(i % 2 == 1))
                time.sleep(case.get("observe", 0.6))
                checkpoint("loaded")
                if kind in {"store", "release"}:
                    immediate = publish("immediate")
                    time.sleep(0.6)
                    checkpoint("after_immediate")
                    if kind == "release":
                        wait_for(has_event("eta_completed", immediate), "immediate task after capacity released")
                        time.sleep(0.2)
                        checkpoint("released")
                if kind == "mixed_cold":
                    active = publish("active", duration=3.0)
                    wait_for(has_event("eta_started", active), "mixed active task")
                    time.sleep(0.2)
                    checkpoint("mixed_running")
            kind_signal = case.get("shutdown", "SIGTERM")
            sent.append(dict(signal=kind_signal, time=time.monotonic()))
            if kind_signal == "SIGKILL":
                helpers.kill_group(process.pid)
            else:
                os.kill(process.pid, getattr(signal, kind_signal))
            try:
                process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                forced = True
                helpers.kill_group(process.pid)
                process.wait(timeout=5)
            exited = time.monotonic()
        finally:
            helpers.kill_group(process.pid)
            process.wait(timeout=5)
    time.sleep(0.15)
    observed = events()
    per_task = {}
    for task_id, info in tasks.items():
        selected = [e for e in observed if e.get("task_id") == task_id]
        per_task[task_id] = dict(info,
            received=any(e["event"] == "eta_received" for e in selected),
            started=any(e["event"] == "eta_started" for e in selected),
            completed=any(e["event"] == "eta_completed" for e in selected),
            acked=any(e["event"] == "message_ack" for e in selected),
            request_ack_on_accept=next((e["ack_requested"] for e in selected if e["event"] == "request_accepted"), None),
            result=publisher.backend.get_task_meta(task_id)["status"],
            broker=helpers.broker_snapshot(client, task_id),
        )
    fetched_after_exit = []
    queued = sum(len(t["broker"]["queued"]) for t in per_task.values())
    if queued:
        with publisher.connection_for_read() as connection:
            queue = connection.SimpleQueue("eta-probe")
            try:
                for _ in range(queued):
                    message = queue.get(block=False)
                    fetched_after_exit.append(message.headers.get("id"))
                    message.ack()
            finally:
                queue.close()
    log_text = (folder / "worker.log").read_text()
    outcome = dict(case=case, tasks=per_task, phases=phases, signals=sent,
                   timeline=observed, returncode=process.returncode, forced_kill=forced,
                   shutdown_elapsed=round(exited - sent[-1]["time"], 3), elapsed=round(exited - started, 3),
                   fetched_after_exit=fetched_after_exit,
                   soft_logged="Initiating Soft Shutdown" in log_text,
                   errors_logged=[x for x in ("NotImplementedError", "BlockingSwitchOutError", "Pool not running") if x in log_text])
    outcome["errors"] = check(outcome)
    outcome["passed"] = not outcome["errors"]
    (folder / "result.json").write_text(json.dumps(outcome, indent=2) + "\n")
    publisher.close()
    futures = [t for t in per_task.values() if t["role"].startswith("future")]
    print(f"{'PASS' if outcome['passed'] else 'FAIL'} {case['name']:29} "
          f"future received/start/ACK={sum(t['received'] for t in futures)}/{sum(t['started'] for t in futures)}/{sum(t['acked'] for t in futures)} "
          f"exit={outcome['returncode']} errors={outcome['errors_logged']}", flush=True)
    for error in outcome["errors"]:
        print(f"  {error}", flush=True)
    return outcome


def check(o):
    c = o["case"]
    errors = []
    def require(ok, message):
        if not ok:
            errors.append(message)
    tasks, phases = o["tasks"], o["phases"]
    future_ids = {i for i, t in tasks.items() if t["role"].startswith("future")}
    require(not o["forced_kill"], "shutdown must finish before safety deadline")
    for task_id, t in tasks.items():
        if t["broker"]["queued"]:
            require(task_id in o["fetched_after_exit"], "queued message must be fetchable after worker exit")
    if c["kind"] == "due":
        task_id = next(iter(tasks))
        require(task_id in phases["waiting"]["received"] and not phases["waiting"]["running"], "ETA must be fetched but not started while waiting")
        require(not phases["waiting"]["acked"], "ETA must not ACK while waiting, even early-ack")
        require(bool(phases["waiting"]["broker"][task_id]["unacked"]), "waiting delivery must remain broker-unacked")
        expected_running_ack = not c.get("acks_late") and c["pool"] != "solo"
        require((task_id in phases["running"]["acked"]) == expected_running_ack, "running ETA ACK must follow pool and early/late policy")
        require(tasks[task_id]["completed"] and tasks[task_id]["acked"], "completed ETA must ACK")
        require(tasks[task_id]["request_ack_on_accept"] == (not c.get("acks_late", False)),
                "request acceptance must select early versus late ACK policy")
        start = next(e["time"] for e in o["timeline"] if e["event"] == "eta_started" and e["task_id"] == task_id)
        require(start >= tasks[task_id]["published"] + 0.95, "ETA must not execute before its due time")
    if c["kind"] in {"store", "mixed_cap", "release", "future_shutdown", "mixed_cold"}:
        loaded = phases["loaded"]
        require(not (future_ids & set(loaded["acked"])), "future ETA must not ACK on reservation")
        require(not (future_ids & set(loaded["running"])), "future ETA must not occupy active execution")
        require(loaded["scheduler"] is not None, "worker scheduler state must be observed")
        require(not (future_ids & set(loaded["scheduler"]["active"])), "future ETA must not appear active")
        if c["kind"] == "store":
            expected = 3 if c.get("eta_limit") and c.get("prefetch", 1) else 5
            require(len(future_ids & set(loaded["received"])) == expected, "scheduled fetch count must match expected effective cap")
            immediate = next(i for i, t in tasks.items() if t["role"] == "immediate")
            require((immediate in phases["after_immediate"]["completed"]) == (expected == 5), "ETA saturation should block delivery, not occupy execution slots")
        if c["kind"] == "mixed_cap":
            expected = 2 if c.get("prefetch", 1) == 2 else 0
            require(len(future_ids & set(loaded["received"])) == expected,
                    "ETA fetch must respect initial credit and share the total cap with active late-ack work")
        if c["kind"] == "release":
            require(len(loaded["received"]) == 3, "initial ETA cap must be reached")
            require(not phases["after_immediate"]["completed"], "immediate task should initially wait behind saturated ETA cap")
            require(all(t["completed"] and t["acked"] for t in tasks.values()), "due ETA executions must release capacity")
            require(phases["released"]["scheduler"]["qos_value"] == 1,
                    "reproduce saturated cap decreasing below configured initial prefetch of two")
        if c["kind"] in {"future_shutdown", "mixed_cold", "store", "mixed_cap"}:
            for task_id in future_ids:
                t = tasks[task_id]
                require(not t["acked"], "shutdown must not ACK waiting ETA")
                require(not t["started"], "waiting ETA must not execute during tested shutdown")
                if c.get("shutdown") == "SIGKILL":
                    require(bool(t["broker"]["unacked"]), "SIGKILL must leave fetched ETA unacked")
                else:
                    require(bool(t["broker"]["queued"]), "waiting ETA must be restored or remain queued")
    if c["kind"] == "normal":
        expected = 6 if c["name"] == "initial-prefetch-above-cap" else 2 if c.get("acks_late") else 3
        require(len(phases["loaded"]["received"]) == expected, "normal prefetch count must match early/late outstanding credit")
        require(len(phases["loaded"]["acked"]) == (0 if c.get("acks_late") else 1), "prefetched, unaccepted tasks must not be early-ACKed")
    if c["kind"] == "mixed_cold":
        active = next(t for t in tasks.values() if t["role"] == "active")
        require(not active["completed"], "cold shutdown must interrupt this running task")
        require(active["acked"] == (c["pool"] != "prefork"), "reproduce pool-specific late-ACK outcome for running work")
        require(active["result"] == ("FAILURE" if c["pool"] == "solo" else "PENDING"), "reproduce pool-specific running-task result")
    if c.get("soft_timeout"):
        require(o["soft_logged"] == c.get("soft_on_idle", False), "ETA-only worker counts as idle for soft-shutdown selection")
        if c.get("soft_on_idle") and c["pool"] == "prefork":
            require(o["shutdown_elapsed"] >= c["soft_timeout"] - 0.1, "enabled idle grace must elapse")
        if c.get("soft_on_idle") and c["pool"] == "gevent":
            require("BlockingSwitchOutError" in o["errors_logged"], "reproduce gevent idle soft-handler limitation")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", choices=[c["name"] for c in CASES])
    args = parser.parse_args()
    run_dir = DEV / "eta-prefetch-runs" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    run_dir.mkdir(parents=True)
    summary = dict(revision=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                   python=sys.version, platform=helpers.platform.platform(),
                   dependencies={n: helpers.importlib.metadata.version(n) for n in ("kombu", "billiard", "gevent", "redis")}, outcomes=[])
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
