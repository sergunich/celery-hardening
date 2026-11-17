#!/usr/bin/env python3
"""Redis-only connection/heartbeat experiments; real CLI workers, isolated faults.

Run with mise run redis-connection; --case may be repeated. No shared Redis,
Docker, firewall changes, or production-code patches are used.
"""
import argparse
import base64
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import threading
import time
from urllib.parse import unquote, urlparse
import uuid

SUITE = Path(__file__).resolve().parent
sys.path.insert(0, str(SUITE.parent))
from environment.runtime import (
    ROOT, CELERY, activate_sources, dependency_version, provenance, pythonpath, source_revision, private_redis,
)
activate_sources()
spec = importlib.util.spec_from_file_location("policy_helpers", ROOT / "task-policy/verify-task-policies.py")
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)
helpers = policy.helpers
Celery = helpers.Celery
from redis_fault_proxy import FaultProxy

POOLS = ("solo", "prefork", "threads", "gevent")
CASES = []


def add(name, pool, kind, **config):
    CASES.append(dict(name=name, pool=pool, kind=kind, **config))


for pool in POOLS:
    add(f"monitor-{pool}", pool, "monitor", monitor=True)
add("monitor-solo-disabled", "solo", "monitor", monitor=False)
add("monitor-gevent-busy", "gevent", "monitor", monitor=True, busy=True)
for pool in POOLS:
    add(f"idle-reconnect-{pool}", pool, "idle_cut", cycles=3 if pool in {"solo", "prefork"} else 1)
for pool in POOLS:
    for cancel in (False, True):
        add(f"active-{pool}-{'cancel' if cancel else 'keep'}", pool, "active_cut", cancel=cancel)
for pool in ("solo", "prefork"):
    add(f"active-{pool}-early", pool, "active_cut", cancel=True, acks_late=False)
    add(f"outage-{pool}", pool, "outage")
add("startup-retry-solo", "solo", "startup", retry=False, startup_retry=True)
add("startup-no-retry-solo", "solo", "startup", retry=True, startup_retry=False)
add("startup-max-retries-solo", "solo", "startup", max_retries=1, exhausted=True)
add("runtime-no-retry-solo", "solo", "no_retry", retry=False)
for pool in ("solo", "prefork", "gevent"):
    add(f"halfopen-brpop-{pool}", pool, "brpop", control=False)
add("halfopen-brpop-no-health", "solo", "brpop", control=False, health=0, socket_timeout=1)
add("delivery-reply-lost", "solo", "delivery", control=False, visibility=2)
for timeout in (None, 1):
    add(f"halfopen-monitor-{'unbounded' if timeout is None else 'bounded'}", "solo", "publish",
        monitor=True, socket_timeout=timeout)
for dedup in (False, True):
    add(f"ack-before-commit-{'dedup' if dedup else 'repeat'}", "solo", "ack", ack_fault="ack_before",
        visibility=2, dedup=dedup)
add("ack-reply-lost", "solo", "ack", ack_fault="ack_after", visibility=2)
add("visibility-crash-short", "solo", "crash", visibility=2)
add("visibility-crash-default", "solo", "crash", visibility=3600)
add("visibility-overlap-prefork", "prefork", "overlap", concurrency=2, visibility=2, dedup=True)
add("visibility-solo-other-worker", "solo", "overlap", other_worker=True, visibility=2, dedup=True)
for pool in ("solo", "gevent"):
    add(f"eta-reconnect-{pool}", pool, "eta")
add("warm-healthy-prefork", "prefork", "warm", monitor=True)
for pool in ("solo", "prefork"):
    add(f"warm-outage-{pool}", pool, "warm", outage=True, monitor=False)
for reduce in (True, False):
    add(f"prefetch-reconnect-{'reduce' if reduce else 'unchanged'}", "prefork", "active_cut",
        concurrency=2, reduce_prefetch=reduce)


class Monitor:
    """Observe actual Redis Pub/Sub monitoring events, not heartbeat_sent hooks."""
    def __init__(self, client, path):
        self.client, self.path = client, path
        self.stop = threading.Event()
        self.ready = threading.Event()
        self.error = None
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()
        if not self.ready.wait(3):
            raise RuntimeError(f"Monitor did not subscribe: {self.error}")

    def run(self):
        try:
            with self.path.open("w") as log, self.client.pubsub() as sub:
                sub.psubscribe("*")
                while not self.stop.is_set():
                    event = sub.get_message(timeout=0.1)
                    if not event:
                        continue
                    if event["type"] == "psubscribe":
                        self.ready.set()
                    if event["type"] != "pmessage":
                        continue
                    message = json.loads(event["data"])
                    # Redis Pub/Sub spans DB numbers; result-backend updates
                    # are not Kombu envelopes or worker-monitoring events.
                    if not isinstance(message, dict) or "body" not in message:
                        continue
                    body = message["body"]
                    if message.get("properties", {}).get("body_encoding") == "base64":
                        body = base64.b64decode(body)
                    body = json.loads(body)
                    if isinstance(body, dict) and "type" in body:
                        log.write(json.dumps(dict(time=time.monotonic(), type=body["type"],
                                                  hostname=body.get("hostname"))) + "\n")
                        log.flush()
        except BaseException as exc:
            self.error = repr(exc)

    def close(self):
        self.stop.set()
        self.thread.join(2)
        if self.thread.is_alive() or self.error:
            raise RuntimeError(f"Monitor failed: {self.error}")


def run_case(case, run_dir, client, broker, backend):
    folder = run_dir / case["name"]
    folder.mkdir()
    client.flushall()  # This runner's private Redis server only.
    events_path = folder / "events.jsonl"
    proxy = FaultProxy(unquote(urlparse(broker).path), folder / "proxy.jsonl")
    monitor = Monitor(client, folder / "monitor.jsonl")
    publisher = Celery("net_publisher", broker=broker, backend=backend)
    config = dict(case, events=str(events_path.relative_to(ROOT)), broker=proxy.url, backend=backend, queue="net-probe")
    (folder / "config.json").write_text(json.dumps(config, indent=2) + "\n")
    tasks, phases, actions, workers, logs, errors = {}, {}, [], [], [], []
    forced = []
    start = time.monotonic()

    def events():
        return helpers.read_events(events_path)

    def selected(name, task_id=None):
        return [e for e in events() if e["event"] == name and (task_id is None or e.get("task_id") == task_id)]

    def wait_for(predicate, label, timeout=15):
        until = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() > until:
                raise RuntimeError(f"Timeout waiting for {label}")
            if proxy.error:
                raise RuntimeError(proxy.error)
            time.sleep(0.03)

    def sleep(seconds):
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            if proxy.error:
                raise RuntimeError(proxy.error)
            time.sleep(0.03)

    def launch():
        index = len(workers)
        env = os.environ.copy()
        for key in ("REMAP_SIGTERM", "CELERY_RDBSIG"):
            env.pop(key, None)
        env.update(PYTHONPATH=pythonpath(SUITE, ROOT / "shutdown"), PYTHONDONTWRITEBYTECODE="1",
                   PYTHONUNBUFFERED="1", SHUTDOWN_PROBE_CONFIG=json.dumps(config))
        command = [sys.executable, "-m", "celery", "-A", "redis_connection_probe:app", "worker",
                   "--pool", case["pool"], "--concurrency", str(case.get("concurrency", 1)),
                   "--hostname", f"net-{index}@localhost", "--loglevel", "INFO",
                   "--without-gossip", "--without-mingle", "--queues", "net-probe", "--heartbeat-interval", "1"]
        if not case.get("monitor"):
            command.append("--without-heartbeat")
        log = (folder / ("worker.log" if index == 0 else f"worker-{index}.log")).open("w")
        logs.append(log)
        p = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                             start_new_session=True)
        workers.append(p)
        return p

    def ready(p):
        wait_for(lambda: any(e["pid"] == p.pid for e in selected("worker_ready")), "worker ready", 25)

    def action(name):
        record = proxy.action(name)
        actions.append(record)
        return record

    def send_signal(p, sig, reason):
        actions.append(dict(action=sig, time=time.monotonic(), pid=p.pid, reason=reason))
        if sig == "SIGKILL":
            helpers.kill_group(p.pid)
        elif p.poll() is None:
            os.kill(p.pid, getattr(signal, sig))

    def publish(role, duration=0.15, countdown=None, busy=False):
        task_id = str(uuid.uuid4())
        tasks[task_id] = dict(role=role, duration=duration, countdown=countdown, published=time.monotonic())
        publisher.send_task("redis_connection_probe.work", args=[role, duration, busy],
                            task_id=task_id, queue="net-probe", countdown=countdown)
        return task_id

    def checkpoint(name):
        ev = events()
        phases[name] = dict(time=time.monotonic(), alive=[p.poll() is None for p in workers],
            broker={task_id: helpers.broker_snapshot(client, task_id) for task_id in tasks},
            states={task_id: publisher.backend.get_task_meta(task_id, cache=False)["status"] for task_id in tasks},
            received={task_id: len(selected("net_received", task_id)) for task_id in tasks},
            starts={task_id: len(selected("net_started", task_id)) for task_id in tasks},
            completed={task_id: len(selected("net_completed", task_id)) for task_id in tasks},
            acked={task_id: len(selected("message_ack", task_id)) for task_id in tasks},
            scheduler=next((e for e in reversed(ev) if e["event"] == "net_state"), None))

    def ping(role="ping"):
        task_id = publish(role)
        wait_for(lambda: selected("net_completed", task_id), role)
        return task_id

    main_id = None
    try:
        kind = case["kind"]
        if kind == "startup":
            action("down")
        worker = launch()
        if kind == "startup":
            sleep(1)
            checkpoint("unavailable")
            if case.get("exhausted") or not case.get("startup_retry", True):
                wait_for(lambda: worker.poll() is not None, "startup refusal/exhaustion", 12)
                checkpoint("gave_up")
                action("up")
            else:
                action("up")
                ready(worker)
                main_id = ping()
        else:
            ready(worker)
            if kind == "monitor":
                sleep(1.3)
                main_id = publish("work", 2.8, busy=case.get("busy", False))
                wait_for(lambda: selected("net_started", main_id), "task start")
                sleep(1.2)
                checkpoint("running")
                wait_for(lambda: selected("net_completed", main_id), "task completed")
                sleep(1.3)
            elif kind == "idle_cut":
                for n in range(case["cycles"]):
                    before = len(selected("net_consumer_started"))
                    action("cut")
                    wait_for(lambda: len(selected("net_consumer_started")) > before, "consumer restart")
                    ping(f"ping-{n}")
                    checkpoint(f"reconnected-{n}")
            elif kind in {"active_cut", "outage", "warm"}:
                duration = 4 if kind != "outage" else 2
                main_id = publish("work", duration)
                wait_for(lambda: selected("net_started", main_id), "task start")
                sleep(0.25)
                checkpoint("before_fault")
                if kind == "warm":
                    send_signal(worker, "SIGTERM", "warm shutdown while task runs")
                    if case.get("outage"):
                        action("down")
                        sleep(5)
                        checkpoint("outage")
                        action("up")
                    wait_for(lambda: worker.poll() is not None, "warm worker exit", 18)
                else:
                    action("down" if kind == "outage" else "cut")
                    if kind == "outage":
                        sleep(4)
                        checkpoint("outage")
                        action("up")
                        sleep(5)
                    else:
                        sleep(5)
                    checkpoint("recovery")
                    if worker.poll() is None:
                        ping()
            elif kind == "no_retry":
                action("cut")
                wait_for(lambda: worker.poll() is not None, "runtime retry disabled exit")
            elif kind == "brpop":
                # Ensure the socket has actually entered the blocking pop.
                wait_for(lambda: any(e.get("command") == "BRPOP" for e in helpers.read_events(folder / "proxy.jsonl")), "BRPOP")
                action("blackhole_brpop")
                # Do not put a message into an already-forwarded BRPOP whose
                # reply will be dropped: isolate stale-connection detection.
                sleep(1.4)
                main_id = publish("ping")
                sleep(4)
                checkpoint("before_rescue")
                action("up")
                wait_for(lambda: selected("net_completed", main_id), "ping after recovery/rescue")
            elif kind == "delivery":
                wait_for(lambda: any(e.get("command") == "BRPOP" for e in helpers.read_events(folder / "proxy.jsonl")), "BRPOP")
                action("blackhole_brpop")
                # Unlike the health-check cases, publish while the already
                # forwarded BRPOP is outstanding, then drop its actual delivery.
                main_id = publish("lost-delivery")
                wait_for(lambda: any(e["event"] == "proxy_dropped_delivery" and e["task_id"] == main_id
                                     for e in helpers.read_events(folder / "proxy.jsonl")), "lost BRPOP delivery reply", 3)
                checkpoint("reply_lost")
                action("up")
                sleep(4)
                ping()
                checkpoint("after_visibility")
            elif kind == "publish":
                sleep(1.3)
                action("blackhole_publish")
                sleep(1.3)
                main_id = publish("ping")
                sleep(4)
                checkpoint("before_rescue")
                action("up")
                wait_for(lambda: selected("net_completed", main_id), "ping after publish recovery/rescue")
            elif kind == "ack":
                action(case["ack_fault"])
                main_id = publish("work", 1)
                wait_for(lambda: any(e["event"].startswith("ack_") for e in helpers.read_events(folder / "proxy.jsonl")), "ACK wire fault")
                checkpoint("ack_fault")
                sleep(2.5)
                action("up")
                sleep(7)
                checkpoint("recovery")
                ping()
            elif kind == "crash":
                main_id = publish("work", 5)
                wait_for(lambda: selected("net_started", main_id), "task start")
                send_signal(worker, "SIGKILL", "intentional crash, not safety deadline")
                worker.wait(timeout=5)
                checkpoint("crashed")
                replacement = launch()
                ready(replacement)
                sleep(4)
                checkpoint("replacement")
                ping()
            elif kind == "overlap":
                main_id = publish("work", 5)
                wait_for(lambda: selected("net_started", main_id), "task start")
                if case.get("other_worker"):
                    ready(launch())
                wait_for(lambda: len(selected("net_started", main_id)) >= 2, "visibility redelivery during execution")
                checkpoint("overlap")
                wait_for(lambda: len(selected("net_completed", main_id)) >= 2, "both executions complete")
            elif kind == "eta":
                main_id = publish("eta", countdown=4)
                wait_for(lambda: selected("net_received", main_id), "ETA receipt")
                sleep(0.2)
                checkpoint("scheduled")
                action("cut")
                sleep(6)
                checkpoint("after_eta")
                ping()
            else:
                raise ValueError(kind)
            sleep(0.25)
        checkpoint("before_exit")
    except Exception as exc:
        errors.append(f"Harness: {type(exc).__name__}: {exc}")
    finally:
        # Restore connectivity before final cleanup, without attributing this
        # rescue to automatic recovery (all earlier phase snapshots are retained).
        try:
            action("up")
        except Exception as exc:
            errors.append(f"Proxy rescue: {exc}")
        for p in workers:
            if p.poll() is None:
                send_signal(p, "SIGTERM", "harness cleanup")
                try:
                    p.wait(timeout=12)
                except subprocess.TimeoutExpired:
                    forced.append(p.pid)
                    send_signal(p, "SIGKILL", "safety deadline")
                    p.wait(timeout=5)
            policy.cleanup_group(p)
        for log in logs:
            log.close()
        try:
            proxy.close()
            monitor.close()
        except Exception as exc:
            errors.append(f"Resource cleanup: {exc}")
    time.sleep(0.15)
    checkpoint("after_exit")
    ev = events()
    per_task = {}
    for task_id, details in tasks.items():
        per_task[task_id] = dict(details,
            received=selected("net_received", task_id), starts=selected("net_started", task_id),
            completed=selected("net_completed", task_id), acks=selected("message_ack", task_id),
            rejects=selected("message_reject", task_id), ack_errors=selected("net_ack_error", task_id),
            result=publisher.backend.get_task_meta(task_id, cache=False)["status"],
            broker=helpers.broker_snapshot(client, task_id))
    fetched = []
    if any(t["broker"]["queued"] for t in per_task.values()):
        with publisher.connection_for_read() as conn:
            q = conn.SimpleQueue("net-probe")
            try:
                while True:
                    try:
                        message = q.get(block=False)
                    except q.Empty:
                        break
                    fetched.append(dict(task_id=message.headers.get("id"),
                                        redelivered=message.delivery_info.get("redelivered")))
                    message.ack()
            finally:
                q.close()
    outcome = dict(case=case, tasks=per_task, main_task=main_id, phases=phases, timeline=ev,
        proxy=helpers.read_events(folder / "proxy.jsonl"), monitoring=helpers.read_events(folder / "monitor.jsonl"),
        actions=actions, returncodes=[p.returncode for p in workers], forced_kills=forced,
        fetched_after_exit=fetched, errors=errors, elapsed=round(time.monotonic() - start, 3))
    outcome["errors"].extend(check(outcome))
    outcome["passed"] = not outcome["errors"]
    (folder / "result.json").write_text(json.dumps(outcome, indent=2) + "\n")
    publisher.close()
    t = per_task.get(main_id, {})
    print(f"{'PASS' if outcome['passed'] else 'FAIL'} {case['name']:36} "
          f"start/done/ACK={len(t.get('starts',[]))}/{len(t.get('completed',[]))}/{len(t.get('acks',[]))} "
          f"state={t.get('result')} broker={t.get('broker')} exit={outcome['returncodes']} "
          f"{outcome['elapsed']}s", flush=True)
    for error in outcome["errors"]:
        print(f"  {error}", flush=True)
    return outcome


def check(o):
    """Mechanism assertions; final measured dispositions remain separate fields."""
    errors = []
    def require(value, message):
        if not value:
            errors.append(message)
    case, ev = o["case"], o["timeline"]
    kind = case["kind"]
    main = o["tasks"].get(o["main_task"], {})
    require(not o["forced_kills"], "no safety-deadline kill")
    require(not any(e["event"] == "net_task_published" for e in ev),
            "connection/visibility recovery must not publish a Celery task retry")
    for e in ev:
        if e["event"] == "net_connected":
            require(not e["supports_heartbeats"], "Redis must not advertise AMQP heartbeats")
    for t in o["tasks"].values():
        require(all(e["retries"] == 0 for e in t["starts"]), "broker recovery must not increment task retries")
        if t["role"].startswith("ping"):
            require(t["result"] == "SUCCESS", "follow-up ping must complete")
    if kind == "startup":
        failed = case.get("exhausted") or not case.get("startup_retry", True)
        require(bool([e for e in ev if e["event"] == "worker_ready"]) != bool(failed), "startup retry policy")
        require(o["returncodes"] == ([1] if failed else [0]), "startup exit status")
    elif kind == "no_retry":
        require(o["returncodes"] == [1], "runtime retry disabled must exit")
    elif kind == "monitor":
        require(len(main.get("completed", [])) == 1, "monitor case must complete one task")
        heartbeats = [e for e in o["monitoring"] if e["type"] == "worker-heartbeat"]
        require(bool(heartbeats) == bool(case.get("monitor")), "actual monitoring event enable/disable")
        if main.get("starts") and main.get("completed"):
            a, b = main["starts"][0]["time"], main["completed"][0]["time"]
            during = [e for e in heartbeats if a < e["time"] < b]
            expected = case.get("monitor") and case["pool"] != "solo" and not case.get("busy")
            require(bool(during) == bool(expected), "monitor progress during task must follow pool scheduling")
    elif kind == "idle_cut":
        require(len([e for e in ev if e["event"] == "net_consumer_started"]) >= case["cycles"] + 1,
                "consumer must restart after each cut")
    elif kind in {"active_cut", "outage", "warm"}:
        require(bool(main.get("starts")), "active task must start before fault")
        if not case.get("cancel") or not case.get("acks_late", True) or case["pool"] in {"solo", "threads"}:
            require(bool(main.get("completed")), "uncancelled/unsupported-cancellation work should finish")
        if kind == "active_cut":
            late = case.get("acks_late", True)
            repeat = late and case["pool"] != "solo"
            cancelled = repeat and case.get("cancel") and case["pool"] in {"prefork", "gevent"}
            require(len(main.get("starts", [])) == (2 if repeat else 1), "active-cut execution count")
            require(len(main.get("completed", [])) == ((1 if cancelled else 2) if repeat else 1),
                    "active-cut completion count")
            require(main.get("result") == "SUCCESS" and not any(main.get("broker", {}).values()),
                    "active-cut deliveries must finish and be settled")
            if repeat and len(main.get("starts", [])) >= 2:
                require(main["starts"][1]["redelivered"], "second body execution must be a broker redelivery")
            if case.get("cancel") and late and case["pool"] != "solo":
                require(any(e["event"] == "net_cancel_attempt" for e in ev), "active late task must reach cancellation path")
            if case["pool"] == "solo" and main.get("completed"):
                detected = [e for e in ev if e["event"] == "net_connection_error"]
                require(bool(detected) and detected[0]["time"] > main["completed"][0]["time"],
                        "solo detects disconnect only after inline body returns")
                require(not any(e["event"] == "net_cancel_attempt" for e in ev), "solo has no active request left to cancel")
            if case["name"].startswith("prefetch-"):
                starts = [e for e in ev if e["event"] == "net_consumer_started"]
                require(all(e["prefetch"] == 4 and e["qos_global"] is False for e in starts),
                        "this checkout skips reduction for Redis's recorded QoS mode")
    elif kind == "brpop":
        phase = o["phases"].get("before_rescue", {})
        done = phase.get("completed", {}).get(o["main_task"], 0)
        require(bool(done) == bool(case.get("health", 1)), "BRPOP health checks determine automatic recovery in window")
    elif kind == "delivery":
        require(not main.get("received") and not main.get("starts") and not main.get("acks"),
                "lost pop reply must never reach the task consumer")
        require(main.get("result") == "PENDING" and not any(main.get("broker", {}).values()),
                "popped-but-unregistered message must be absent, not recoverable by visibility scan")
        require(any(e["event"] == "proxy_dropped_delivery" and e["task_id"] == o["main_task"] for e in o["proxy"]),
                "proxy must confirm loss of the original task's actual BRPOP reply")
    elif kind == "publish":
        phase = o["phases"].get("before_rescue", {})
        done = phase.get("completed", {}).get(o["main_task"], 0)
        require(bool(done) == bool(case.get("socket_timeout")), "sync publish timeout determines automatic recovery in window")
    elif kind == "ack":
        require(any(e["event"].startswith("ack_") for e in o["proxy"]), "fault must hit actual ACK transaction")
        require(bool(main.get("ack_errors")), "worker must observe ambiguous/failed ACK")
        expected = 1 if case.get("dedup") or case["ack_fault"] == "ack_after" else 2
        require(len(main.get("starts", [])) == expected, "ACK boundary/dedup task execution count")
        require(main.get("result") == "SUCCESS", "backend success must survive ACK fault")
        lost_record = o["phases"].get("ack_fault", {}).get("broker", {}).get(o["main_task"], {})
        require(bool(lost_record.get("unacked")) == (case["ack_fault"] == "ack_before"),
                "Redis state must independently confirm which side of ACK commit was lost")
        require(len(main.get("received", [])) == (1 if case["ack_fault"] == "ack_after" else 2),
                "dedup skips execution, not delivery of the restored message")
    elif kind == "crash":
        phase = o["phases"].get("crashed", {})
        require(bool(phase.get("broker", {}).get(o["main_task"], {}).get("unacked")), "crash must leave unacked delivery")
        expected = 2 if case["visibility"] == 2 else 1
        require(len(main.get("starts", [])) == expected, "visibility expiry controls replacement execution")
        require(o["returncodes"][0] == -9, "first worker intentionally SIGKILLed")
    elif kind == "overlap":
        require(len(main.get("starts", [])) == 2 and len(main.get("completed", [])) == 2, "two overlapping executions")
        if len(main.get("starts", [])) == 2 and main.get("completed"):
            require(main["starts"][1]["time"] < main["completed"][0]["time"], "second execution starts before first completion")
    elif kind == "eta":
        require(len(main.get("received", [])) == 2 and len(main.get("starts", [])) == 1
                and len(main.get("completed", [])) == 1 and main.get("result") == "SUCCESS",
                "ETA must be restored and execute once, not through both old and new timers")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=[c["name"] for c in CASES], action="append")
    args = parser.parse_args()
    run_dir = SUITE / "redis-connection-runs" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    run_dir.mkdir(parents=True)
    import kombu.transport.redis as transport
    sources = [CELERY / "celery/worker/consumer/consumer.py", CELERY / "celery/worker/loops.py", Path(transport.__file__),
               SUITE / "redis_connection_probe.py", SUITE / "redis_fault_proxy.py", Path(__file__),
               ROOT / "uv.lock"]
    summary = dict(provenance=provenance(), python=sys.version, platform=platform.platform(), celery=__import__("celery").__version__,
        revision=source_revision(),
        dependencies={n: dependency_version(n) for n in ("kombu", "redis", "billiard", "gevent", "greenlet")},
        source_sha256={str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}, outcomes=[])
    with helpers.private_redis(run_dir) as (client, broker, backend):
        summary["redis_server"] = client.info("server")["redis_version"]
        summary["redis_settings"] = {key: client.config_get(key)[key] for key in ("timeout", "tcp-keepalive", "appendonly", "save")}
        for case in CASES:
            if args.case and case["name"] not in args.case:
                continue
            summary["outcomes"].append(run_case(case, run_dir, client, broker, backend))
            (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Artifacts: {run_dir.relative_to(ROOT)}")
    return 0 if all(o["passed"] for o in summary["outcomes"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
