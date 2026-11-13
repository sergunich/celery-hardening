#!/usr/bin/env python3
"""Compare real Celery CLI workers against a private Redis broker/backend.

.dev/.venv/bin/python .dev/verify-pool-shutdown.py [--pool prefork] [--case cold-late]
No shared Redis is used. Artifacts remain under .dev/pool-shutdown-runs/.
"""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import time
import uuid

DEV = Path(__file__).resolve().parent
ROOT = DEV.parent
sys.path.insert(0, str(ROOT))
from celery import Celery
import redis

POOLS = ["solo", "prefork", "threads", "gevent"]
ESCALATE = [(0.3, "SIGINT"), (0.6, "SIGINT"), (0.9, "SIGINT"), (1.2, "SIGINT")]
CASES = []
for name, description, options in [
    ("warm", "2 s task; TERM at 0.3 s; soft timeout 1 s (must not limit warm shutdown).",
     dict(signals=[(0.3, "SIGTERM")], soft_timeout=1.0)),
    ("cold", "2 s task; QUIT at 0.3 s; soft shutdown disabled.",
     dict(signals=[(0.3, "SIGQUIT")])),
    ("soft-short", "0.8 s task; QUIT at 0.3 s; 1.5 s grace period should allow completion.",
     dict(signals=[(0.3, "SIGQUIT")], steps=8, soft_timeout=1.5)),
    ("soft-long", "3 s task; QUIT at 0.3 s; 1 s grace period expires before completion.",
     dict(signals=[(0.3, "SIGQUIT")], steps=30, soft_timeout=1.0)),
    ("hard", "Endless task; four Ctrl+C signals at 0.3/0.6/0.9/1.2 s; soft timeout 2 s.",
     dict(signals=ESCALATE, steps=None, soft_timeout=2.0, deadline=4.0)),
    ("kill", "Endless task; SIGKILL to entire worker process group at 0.3 s.",
     dict(signals=[(0.3, "SIGKILL")], steps=None)),
]:
    for late in (False, True):
        CASES.append(dict(name=f"{name}-{'late' if late else 'early'}", group=name,
                          description=description, acks_late=late, **options))
CASES += [
    dict(name="warm-hung", group="edge", description="Endless late-ack task; TERM at 0.3 s; soft timeout 1 s; kill if alive at 3 s.",
         signals=[(0.3, "SIGTERM")], soft_timeout=1.0, acks_late=True, steps=None, deadline=3.0),
    dict(name="cold-catch-exception", group="edge", description="2 s late-ack task catches Exception around work; QUIT at 0.3 s.",
         signals=[(0.3, "SIGQUIT")], acks_late=True, catch="exception"),
    dict(name="cold-catch-base", group="edge", description="2 s late-ack task catches BaseException around work; QUIT at 0.3 s.",
         signals=[(0.3, "SIGQUIT")], acks_late=True, catch="base"),
    dict(name="hard-catch-exception", group="edge", description="Endless late-ack task catches Exception; four Ctrl+C signals; kill at 4 s if stuck.",
         signals=ESCALATE, steps=None, acks_late=True, catch="exception", soft_timeout=2.0, deadline=4.0),
    dict(name="hard-catch-base", group="edge", description="Endless late-ack task catches BaseException; four Ctrl+C signals; kill at 4 s if stuck.",
         signals=ESCALATE, steps=None, acks_late=True, catch="base", soft_timeout=2.0, deadline=4.0),
    dict(name="warm-native", group="native", description="About 2 s PBKDF2 in native code; late ack; TERM at 0.3 s.",
         signals=[(0.3, "SIGTERM")], native=True, steps=0, acks_late=True),
    dict(name="cold-native", group="native", description="About 2 s PBKDF2 in native code; late ack; QUIT at 0.3 s; no soft wait.",
         signals=[(0.3, "SIGQUIT")], native=True, steps=0, acks_late=True),
    dict(name="hard-native", group="native", description="About 2 s PBKDF2 in native code; late ack; four Ctrl+C signals at 0.3/0.6/0.9/1.2 s (handlers may be delayed/coalesced).",
         signals=ESCALATE, native=True, steps=0, acks_late=True, soft_timeout=2.0),
    dict(name="remote-warm", group="remote", description="2 s late-ack task; broker remote shutdown broadcast at 0.3 s (not an OS signal).",
         signals=[(0.3, "REMOTE")], acks_late=True),
    dict(name="idle-cold", group="idle", description="Idle worker; QUIT at 0.3 s; soft shutdown disabled.",
         signals=[(0.3, "SIGQUIT")], idle=True),
    dict(name="idle-soft-skipped", group="idle", description="Idle worker; QUIT at 0.3 s; soft timeout 1 s; idle grace disabled.",
         signals=[(0.3, "SIGQUIT")], idle=True, soft_timeout=1.0),
    dict(name="idle-soft-enabled", group="idle", description="Idle worker; QUIT at 0.3 s; soft timeout 1 s; idle grace explicitly enabled.",
         signals=[(0.3, "SIGQUIT")], idle=True, soft_timeout=1.0, soft_on_idle=True),
]


def read_events(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines(keepends=True) if line.endswith("\n")]


def kill_group(pid):
    try:
        os.killpg(pid, signal.SIGKILL)
        return True
    except ProcessLookupError:
        return False


@contextmanager
def private_redis(run_dir):
    install = subprocess.check_output(["mise", "where", "redis@8.2.3"], text=True).strip()
    executable = Path(install) / "bin/redis-server"
    socket = DEV / f"shutdown-{os.getpid()}.sock"
    if socket.exists():
        raise RuntimeError(f"Refusing to reuse existing socket {socket}")
    with (run_dir / "redis.log").open("w") as log:
        server = subprocess.Popen(
            [str(executable), "--port", "0", "--unixsocket", str(socket),
             "--unixsocketperm", "700", "--save", "", "--appendonly", "no", "--dir", str(run_dir)],
            stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )
        client = redis.Redis(unix_socket_path=str(socket))
        try:
            until = time.monotonic() + 10
            while True:
                try:
                    if client.ping():
                        break
                except redis.ConnectionError:
                    pass
                if server.poll() is not None or time.monotonic() > until:
                    raise RuntimeError(f"Private Redis did not start; see {run_dir / 'redis.log'}")
                time.sleep(0.05)
            yield client, f"redis+socket://{socket}?virtual_host=0", f"redis+socket://{socket}?virtual_host=1"
        finally:
            client.close()
            if server.poll() is None:
                server.terminate()
                try:
                    server.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait(timeout=5)
            socket.unlink(missing_ok=True)


def broker_snapshot(client, task_id):
    queued = []
    for key in client.scan_iter():
        if client.type(key) == b"list":
            for raw in client.lrange(key, 0, -1):
                message = json.loads(raw)
                if message.get("headers", {}).get("id") == task_id:
                    queued.append(dict(queue=key.decode(), redelivered=message.get("headers", {}).get("redelivered")))
    unacked = []
    for tag, raw in client.hgetall("unacked").items():
        message, exchange, routing_key = json.loads(raw)
        if message.get("headers", {}).get("id") == task_id:
            unacked.append(dict(tag=tag.decode(), exchange=exchange, routing_key=routing_key))
    return dict(queued=queued, unacked=unacked)


def checks(case, pool, events, outcome, log):
    """Reproduce observed semantics; PASS may mean a confirmed pool limitation."""
    errors = []
    def require(condition, message):
        if not condition:
            errors.append(message)
    def has(name):
        return any(e["event"] == name for e in events)
    require(has("worker_ready"), "worker must become ready")
    if not case.get("idle"):
        require(has("task_started"), "task must actually execute")
        require(outcome["published"], "task must be published")
    if outcome["ack_observed"]:
        require(not outcome["broker"]["unacked"], "ACKed delivery must not remain unacked at broker")
    if case["group"] == "warm" or case["name"] in {"warm-native", "remote-warm"}:
        require(outcome["completed"], "warm task must complete")
        require(outcome["result_state"] == "SUCCESS", "warm task must store SUCCESS")
        require(outcome["ack_observed"], "warm completed task must be ACKed")
        require(not outcome["forced_kill"], "finite warm worker must exit without supervisor kill")
    if case["name"] == "warm-hung":
        require(outcome["forced_kill"], "warm shutdown must not impose the soft timeout on an endless task")
    if case["group"] == "kill":
        require(outcome["returncode"] == -signal.SIGKILL, "process must die by SIGKILL")
        require(not outcome["completed"], "killed endless task must not complete")
        require(not outcome["task_finally"], "SIGKILL must skip task finally")
        # Solo blocks Redis's async hub, including the queued early ACK callback.
        expected_ack = not case["acks_late"] and pool != "solo"
        require(outcome["ack_observed"] == expected_ack, "SIGKILL must preserve observed pool-specific ACK behavior")
    if case.get("idle"):
        require(not has("task_started"), "idle case must have no task")
        require(not outcome["forced_kill"], "idle worker must exit")
        require(("Initiating Soft Shutdown" in log) == case.get("soft_on_idle", False), "idle soft wait must follow configuration")
    if case["group"] in {"soft-short", "soft-long"}:
        require("Initiating Soft Shutdown" in log, "soft wait must be initiated")
        sent_at = outcome["signals"][0]["time"]
        ticks_during_grace = [e for e in events if e["event"] == "task_tick"
                              and sent_at + 0.05 < e["time"] < sent_at + case["soft_timeout"] - 0.05]
        if pool == "solo":
            require(not ticks_during_grace, "solo must not make progress inside its soft handler sleep")
        elif pool in {"prefork", "threads"}:
            require(bool(ticks_during_grace), "task must make progress during controller's soft wait")
    if case["group"] in {"cold", "soft-short", "soft-long"} and pool == "solo":
        require(outcome["result_state"] == "FAILURE", "solo cancellation must become task failure")
        require("NotImplementedError" in log and outcome["ack_observed"], "solo unsupported cancellation must fail and ACK")
    if case["group"] in {"cold", "soft-long"} and pool == "prefork":
        require(not outcome["completed"], "prefork must stop work before completion")
        require(outcome["ack_observed"] == (not case["acks_late"]), "prefork cancellation must preserve unacked late delivery")
        if case["acks_late"]:
            require(bool(outcome["broker"]["queued"]), "prefork late-ack cancellation must restore message")
    if case["group"] in {"cold", "soft-long"} and pool == "threads":
        require(outcome["completed"] and outcome["result_state"] == "SUCCESS", "thread task must survive unsupported cancellation")
        require("NotImplementedError" in log, "thread cancellation must report unsupported kill_job")
    if case["group"] == "soft-short" and pool in {"prefork", "threads"}:
        require(outcome["completed"] and outcome["ack_observed"], "short task must finish within grace and ACK")
    if pool == "gevent" and case["group"] in {"cold", "soft-short", "soft-long"}:
        require("BlockingSwitchOutError" in log, "reproduce gevent's signal-context blocking error")
        require(outcome["ack_observed"], "gevent scenario must reproduce observed ACK")
        if case["group"] == "cold":
            require(not outcome["completed"] and outcome["result_state"] == "PENDING", "cold gevent must reproduce cancelled-but-ACKed task")
        else:
            require(outcome["completed"] and outcome["result_state"] == "SUCCESS", "failed gevent soft wait must fall back to letting task finish")
    if case["group"] == "hard":
        require(not outcome["completed"], "endless task must not complete")
        require(outcome["hard_entered"] == (pool in {"prefork", "threads"}), "record whether escalation actually reached hard")
        require(outcome["forced_kill"] == (pool == "threads" and case["acks_late"]), "reproduce threads late-ack shutdown hang")
    if case["name"] == "cold-catch-base":
        require(outcome["completed"] and outcome["ack_observed"], "finite task must survive caught cold exception")
    if case["name"] == "cold-catch-exception":
        require(outcome["completed"] == (pool in {"solo", "threads"}), "Exception must catch solo error but not SystemExit/GreenletExit")
    if case["name"] == "hard-catch-base":
        require(outcome["hard_entered"], "broad-catch test must reach hard handler")
        require(outcome["forced_kill"] == (pool in {"solo", "threads"}), "reproduce broad-catch hard limitations")
    if case["name"] == "hard-catch-exception":
        require(outcome["hard_entered"] == (pool != "gevent"), "gevent exits before hard unless GreenletExit is caught")
        require(outcome["forced_kill"] == (pool == "threads"), "reproduce threaded hard hang")
    if case["name"] == "cold-native":
        require(outcome["completed"] == (pool == "threads"), "native cancellation must not stop threaded task")
        require(outcome["ack_observed"] == (pool != "prefork"), "record native cold ACK differences")
    if case["name"] == "hard-native":
        require(outcome["hard_entered"] == (pool in {"prefork", "threads"}), "native solo/gevent SIGINTs must reproduce coalescing")
        require(outcome["completed"] == (pool != "prefork"), "record native task completion differences")
        require(outcome["ack_observed"] == (pool in {"solo", "gevent"}), "record native hard ACK differences")
        if pool == "threads":
            require(outcome["result_state"] == "SUCCESS" and bool(outcome["broker"]["queued"]),
                    "reproduce successful thread task with restored unacked message")
    return errors


def run_case(case, pool, run_dir, client, broker, backend, native_iterations):
    folder = run_dir / pool / case["name"]
    folder.mkdir(parents=True)
    # This is our own dedicated Redis process, never a user's Redis database.
    client.flushall()
    queue = "shutdown-probe"
    task_id = str(uuid.uuid4())
    config = dict(case, pool=pool, queue=queue, broker=broker, backend=backend,
                  events=str(folder / "events.jsonl"), native_iterations=native_iterations)
    (folder / "config.json").write_text(json.dumps(config, indent=2) + "\n")
    env = os.environ.copy()
    for key in ("REMAP_SIGTERM", "CELERY_RDBSIG"):
        env.pop(key, None)
    env.update(PYTHONPATH=os.pathsep.join([str(ROOT), str(DEV)]), PYTHONDONTWRITEBYTECODE="1",
               PYTHONUNBUFFERED="1", SHUTDOWN_PROBE_CONFIG=json.dumps(config))
    command = [sys.executable, "-m", "celery", "-A", "shutdown_probe:app", "worker",
               "--pool", pool, "--concurrency", "1", "--hostname", "shutdown-probe@localhost",
               "--loglevel", "INFO", "--without-gossip", "--without-mingle", "--without-heartbeat",
               "--queues", queue]
    publisher = Celery("shutdown_probe_publisher", broker=broker, backend=backend)
    origin = None
    sent = []
    forced = published = orphan_cleanup = False
    started = time.monotonic()
    snapshot_at_start = None
    with (folder / "worker.log").open("w") as log:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            pending = iter(case["signals"])
            next_signal = next(pending, None)
            while process.poll() is None:
                events = read_events(folder / "events.jsonl")
                now = time.monotonic()
                ready = next((e for e in events if e["event"] == "worker_ready"), None)
                if ready and not published and not case.get("idle"):
                    publisher.send_task("shutdown_probe.task", task_id=task_id, queue=queue)
                    published = True
                if origin is None:
                    marker = "worker_ready" if case.get("idle") else "task_started"
                    match = next((e for e in events if e["event"] == marker), None)
                    if match:
                        origin = match["time"]
                        snapshot_at_start = broker_snapshot(client, task_id)
                    elif now - started > 25:
                        raise RuntimeError(f"{pool}/{case['name']} did not start; see {folder / 'worker.log'}")
                if origin is not None:
                    if next_signal and now - origin >= next_signal[0]:
                        kind = next_signal[1]
                        sent.append(dict(signal=kind, time=time.monotonic()))
                        if kind == "REMOTE":
                            publisher.control.broadcast("shutdown", destination=["shutdown-probe@localhost"])
                        elif kind == "SIGKILL":
                            kill_group(process.pid)
                        else:
                            try:
                                os.kill(process.pid, getattr(signal, kind))
                            except ProcessLookupError:
                                break
                        next_signal = next(pending, None)
                    if now - origin > case.get("deadline", 10):
                        forced = True
                        kill_group(process.pid)
                        break
                time.sleep(0.01)
            process.wait(timeout=5)
            exited_at = time.monotonic()
        finally:
            # Also clean up surviving prefork children after parent exit (important on macOS).
            orphan_cleanup = kill_group(process.pid)
            process.wait(timeout=5)
    time.sleep(0.15)
    events = read_events(folder / "events.jsonl")
    start = origin if origin is not None else started
    metadata = publisher.backend.get_task_meta(task_id) if published else {}
    snapshot = broker_snapshot(client, task_id) if published else dict(queued=[], unacked=[])
    restored_delivery = None
    if snapshot["queued"]:
        # Confirm that a restored message really can be fetched; snapshot is BEFORE this drain.
        with publisher.connection_for_read() as connection:
            simple = connection.SimpleQueue(queue)
            try:
                delivery = simple.get(block=False)
                restored_delivery = dict(task_id=delivery.headers.get("id"),
                                         redelivered=delivery.delivery_info.get("redelivered"))
                delivery.ack()
            finally:
                simple.close()
    ack_events = [e for e in events if e["event"] == "message_ack" and e.get("task_id") == task_id]
    reject_events = [e for e in events if e["event"] == "message_reject" and e.get("task_id") == task_id]
    outcome = dict(
        pool=pool, case=case, published=published, task_id=task_id, returncode=process.returncode,
        forced_kill=forced, orphan_cleanup=orphan_cleanup,
        elapsed=round(exited_at - start, 3), result_state=metadata.get("status"),
        result_detail=str(metadata.get("result")),
        completed=any(e["event"] == "task_completed" for e in events),
        task_finally=any(e["event"] == "task_finally" for e in events),
        hard_entered=any(e["event"] == "hard_handler_entered" for e in events),
        shutdown_errors=[name for name in ("NotImplementedError", "BlockingSwitchOutError")
                         if name in (folder / "worker.log").read_text()],
        ack_observed=bool(ack_events), rejects=reject_events,
        broker_at_start=snapshot_at_start, broker=snapshot, restored_delivery=restored_delivery,
        signals=[dict(e, relative=round(e["time"] - start, 3)) for e in sent],
        timeline=[dict(e, relative=round(e["time"] - start, 3)) for e in events],
    )
    outcome["errors"] = checks(case, pool, events, outcome, (folder / "worker.log").read_text())
    if snapshot["queued"] and (restored_delivery or {}).get("task_id") != task_id:
        outcome["errors"].append("restored delivery must match original task ID")
    outcome["passed"] = not outcome["errors"]
    (folder / "result.json").write_text(json.dumps(outcome, indent=2) + "\n")
    publisher.close()
    loc = f"queue={len(snapshot['queued'])},unacked={len(snapshot['unacked'])}"
    print(f"{'PASS' if outcome['passed'] else 'FAIL'} {pool:7} {case['name']:22} "
          f"complete={outcome['completed']} state={outcome['result_state']} ACK={bool(ack_events)} "
          f"{loc} forced={forced} exit={process.returncode} {outcome['elapsed']}s", flush=True)
    for error in outcome["errors"]:
        print(f"  {error}", flush=True)
    return outcome


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pool", choices=POOLS, action="append")
    parser.add_argument("--case", choices=[c["name"] for c in CASES], action="append")
    args = parser.parse_args()
    if os.name != "posix":
        parser.error("POSIX process groups and signals are required")
    run_dir = DEV / "pool-shutdown-runs" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    run_dir.mkdir(parents=True)
    before = time.monotonic()
    hashlib.pbkdf2_hmac("sha256", b"probe", b"salt", 200_000)
    native_iterations = int(200_000 * 2.0 / (time.monotonic() - before))
    summary = dict(
        python=sys.version, platform=platform.platform(),
        revision=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        dependencies={name: importlib.metadata.version(name) for name in ("billiard", "kombu", "redis", "gevent", "greenlet")},
        outcomes=[],
    )
    with private_redis(run_dir) as (client, broker, backend):
        summary["redis_server"] = client.info("server")["redis_version"]
        for pool in args.pool or POOLS:
            for case in CASES:
                if args.case and case["name"] not in args.case:
                    continue
                summary["outcomes"].append(run_case(case, pool, run_dir, client, broker, backend, native_iterations))
                (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Artifacts: {run_dir.relative_to(ROOT)}")
    return 0 if all(o["passed"] for o in summary["outcomes"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
