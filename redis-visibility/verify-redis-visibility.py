#!/usr/bin/env python3
"""Selected Redis visibility experiments. Maintained reports are never generated.

Uses real CLI workers and the existing mise Redis / uv environment. Only this
runner's private Redis is flushed. --case can be repeated.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import itertools
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
    ROOT, CELERY, activate_sources, dependency_version, provenance, pythonpath, source_revision, private_redis,
)
activate_sources()
spec = importlib.util.spec_from_file_location("policy_helpers", ROOT / "task-policy/verify-task-policies.py")
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)
helpers = policy.helpers
Celery = helpers.Celery
CASES = []


def add(name, kind, **config):
    CASES.append(dict(name=name, kind=kind, pool=config.pop("worker_pool", "solo"), **config))


for present in itertools.product((False, True), repeat=3):
    values = {key: 2 for key, enabled in zip("BRT", present) if enabled}
    add("config-" + ("".join(values) or "none"), "crash", **values)
add("config-B12-R2-T2", "crash", B=12, R=2, T=2)
add("config-B2-R12-T12", "crash", B=2, R=12, T=12)
add("publisher-short-only", "crash", B=12, publisher_visibility=2)
add("no-consumer-after-crash", "crash", B=2, no_consumer=True)
add("scan-defaults", "crash", B=2, default_scan=True)
add("scan-throttled", "crash", B=2, scan_interval=1, scan_throttle=8)
for late in (True, False):
    add(f"solo-alone-{'late' if late else 'early'}", "active", B=2, acks_late=late)
add("solo-short-other-queue", "active", B=12, observer=True, observer_queue="other")
add("solo-short-same-queue", "active", B=12, observer=True)
add("solo-early-short-same-queue", "active", B=12, observer=True, acks_late=False)
add("prefork-early-short-same-queue", "active", B=12, observer=True, acks_late=False,
    worker_pool="prefork")
add("solo-isolated-prefix", "active", B=12, observer=True, observer_prefix="isolated:")
add("solo-eta-short-late", "eta", B=2)
add("solo-eta-short-early-dedup", "eta", B=2, acks_late=False, dedup=True)
add("solo-eta-short-dedup", "eta", B=2, dedup=True)
add("solo-eta-long", "eta", B=12)
add("prefork-reserved-expiry", "reserved", B=2, worker_pool="prefork")
add("result-expiry-independent", "result", B=12, R=2, T=2, result_expires=2)


def broker_snapshot(client, task_id):
    queued, unacked = [], []
    for key in client.scan_iter():
        typ = client.type(key)
        if typ == b"list":
            for raw in client.lrange(key, 0, -1):
                message = json.loads(raw)
                if message.get("headers", {}).get("id") == task_id:
                    queued.append(dict(queue=key.decode(), redelivered=message.get("headers", {}).get("redelivered")))
        elif typ == b"hash" and key.endswith(b"unacked"):
            index = key[:-len(b"unacked")] + b"unacked_index"
            for tag, raw in client.hgetall(key).items():
                message, exchange, route = json.loads(raw)
                if message.get("headers", {}).get("id") == task_id:
                    score = client.zscore(index, tag)
                    unacked.append(dict(key=key.decode(), tag=tag.decode(), score=score,
                                        age=time.time() - score if score is not None else None,
                                        key_ttl=client.ttl(key), route=route))
    return dict(queued=queued, unacked=unacked)


def run_case(case, run_dir, client, broker, backend):
    folder = run_dir / case["name"]
    folder.mkdir()
    client.flushall()  # Isolated server owned by this run only.
    events_path = folder / "events.jsonl"
    publisher = Celery("visibility_publisher", broker=broker, backend=backend)
    publisher.conf.broker_transport_options = (
        {"visibility_timeout": case["publisher_visibility"]} if "publisher_visibility" in case else {})
    base = dict(case, broker=broker, backend=backend, events=str(events_path.relative_to(ROOT)), queue="visibility-probe")
    workers, logs, configs, tasks, phases, actions, errors, forced = [], [], [], {}, {}, [], [], []
    start = time.monotonic()
    main_id = second_id = None

    def events():
        return helpers.read_events(events_path)

    def selected(event, task_id=None):
        return [e for e in events() if e["event"] == event and (task_id is None or e.get("task_id") == task_id)]

    def wait(predicate, label, timeout=15):
        end = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() >= end:
                raise RuntimeError(f"Timeout: {label}")
            time.sleep(0.025)

    def launch(**overrides):
        config = dict(base, label=f"worker-{len(workers)}", **overrides)
        configs.append(config)
        (folder / f"{config['label']}-config.json").write_text(json.dumps(config, indent=2) + "\n")
        env = os.environ.copy()
        for key in ("REMAP_SIGTERM", "CELERY_RDBSIG"):
            env.pop(key, None)
        env.update(PYTHONPATH=pythonpath(SUITE, ROOT / "shutdown"), PYTHONDONTWRITEBYTECODE="1",
                   PYTHONUNBUFFERED="1", VISIBILITY_PROBE_CONFIG=json.dumps(config))
        pool = config["pool"]
        command = [sys.executable, "-m", "celery", "-A", "redis_visibility_probe:app", "worker",
                   "--pool", pool, "--concurrency", "1", "--hostname", config["label"] + "@localhost",
                   "--loglevel", "INFO", "--without-gossip", "--without-mingle", "--without-heartbeat",
                   "--queues", config["queue"]]
        log = (folder / f"{config['label']}.log").open("w")
        logs.append(log)
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        workers.append(process)
        wait(lambda: any(e["pid"] == process.pid for e in selected("ready")), "worker ready", 25)
        return process

    def publish(role, duration=0.12, countdown=None):
        task_id = str(uuid.uuid4())
        tasks[task_id] = dict(role=role, duration=duration, countdown=countdown, published=time.monotonic())
        publisher.send_task("redis_visibility_probe.work", args=[role, duration], task_id=task_id,
                            queue="visibility-probe", countdown=countdown)
        return task_id

    def checkpoint(label):
        phases[label] = dict(time=time.monotonic(), alive=[p.poll() is None for p in workers],
            tasks={task_id: dict(broker=broker_snapshot(client, task_id),
                state=publisher.backend.get_task_meta(task_id, cache=False)["status"],
                result_ttl=publisher.backend.client.ttl(publisher.backend.get_key_for_task(task_id)),
                received=len(selected("received", task_id)), started=len(selected("started", task_id)),
                completed=len(selected("completed", task_id)), ack=len(selected("ack", task_id)))
                for task_id in tasks})

    def terminate(p, sig, reason):
        actions.append(dict(time=time.monotonic(), pid=p.pid, signal=sig, reason=reason))
        if sig == "SIGKILL":
            helpers.kill_group(p.pid)
        elif p.poll() is None:
            os.kill(p.pid, getattr(signal, sig))

    try:
        worker = launch()
        kind = case["kind"]
        if kind == "crash":
            main_id = publish("abandoned", 8)
            wait(lambda: selected("started", main_id), "original task started")
            terminate(worker, "SIGKILL", "planned crash")
            worker.wait(timeout=5)
            checkpoint("crashed")
            if case.get("no_consumer"):
                time.sleep(3.2)
                checkpoint("no_consumer")
            launch()
            time.sleep(4)
            checkpoint("observed")
            if case.get("scan_throttle"):
                wait(lambda: selected("completed", main_id), "throttled scan eventually restores", 8)
                checkpoint("eventual")
        elif kind in {"active", "reserved"}:
            main_id = publish("long", 5)
            wait(lambda: selected("started", main_id), "original task started")
            began = selected("started", main_id)[0]["time"]
            if kind == "reserved":
                second_id = publish("reserved")
                wait(lambda: selected("received", second_id), "second task reserved")
            if case.get("observer"):
                launch(B=2, pool="solo", queue=case.get("observer_queue", "visibility-probe"),
                       prefix=case.get("observer_prefix", ""))
            time.sleep(max(0, began + 3.3 - time.monotonic()))
            checkpoint("expired_while_running")
            wait(lambda: any(e["attempt"] == 1 for e in selected("completed", main_id)), "first body finishes")
            time.sleep(1.5)
            checkpoint("after_completion")
        elif kind == "eta":
            main_id = publish("eta", countdown=6)
            wait(lambda: selected("received", main_id), "ETA receipt")
            time.sleep(3.2)
            checkpoint("waiting")
            time.sleep(5)
            checkpoint("after_eta")
        elif kind == "result":
            main_id = publish("result-ttl")
            wait(lambda: selected("ack", main_id), "result task ACK")
            checkpoint("stored")
            time.sleep(2.4)
            checkpoint("expired_result")
        else:
            raise ValueError(kind)
        checkpoint("before_exit")
    except Exception as exc:
        errors.append(f"Harness: {type(exc).__name__}: {exc}")
    finally:
        for process in workers:
            if process.poll() is None:
                terminate(process, "SIGTERM", "cleanup")
                try:
                    process.wait(timeout=12)
                except subprocess.TimeoutExpired:
                    forced.append(process.pid)
                    terminate(process, "SIGKILL", "safety deadline")
                    process.wait(timeout=5)
            policy.cleanup_group(process)
        for log in logs:
            log.close()
    checkpoint("after_exit")
    timeline = events()
    data = {task_id: dict(details,
            **{key: selected(key, task_id) for key in ("registered", "received", "accepted", "started", "completed", "ack", "reject", "ack_error")},
            state=publisher.backend.get_task_meta(task_id, cache=False)["status"],
            broker=broker_snapshot(client, task_id)) for task_id, details in tasks.items()}
    for path in folder.glob('worker-*.log'):
        errors.extend(f"{path.name}: {line}" for line in path.read_text().splitlines()
                      if 'ERROR/' in line or 'CRITICAL/' in line)
    outcome = dict(case=case, configs=configs, tasks=data, main_task=main_id, second_task=second_id,
                   phases=phases, timeline=timeline, actions=actions, returncodes=[p.returncode for p in workers],
                   forced_kills=forced, errors=errors, elapsed=round(time.monotonic() - start, 3))
    errors.extend(check(outcome))
    outcome["passed"] = not errors
    (folder / "result.json").write_text(json.dumps(outcome, indent=2) + "\n")
    publisher.close()
    task = data.get(main_id, {})
    print(f"{'PASS' if outcome['passed'] else 'FAIL'} {case['name']:35} "
          f"receipt/start/done/ACK=" + "/".join(str(len(task.get(k, []))) for k in ("received", "started", "completed", "ack"))
          + f" state={task.get('state')} exit={outcome['returncodes']} {outcome['elapsed']}s", flush=True)
    for error in errors:
        print(f"  {error}", flush=True)
    return outcome


def check(o):
    errors = []
    def require(value, message):
        if not value:
            errors.append(message)
    case, ev, task_id = o["case"], o["timeline"], o["main_task"]
    task = o["tasks"].get(task_id, {})
    kind = case["kind"]
    require(not o["forced_kills"], "no safety-deadline kills")
    require(not any(e["event"] in {"reject", "ack_error", "task_published"} for e in ev), "no rejects/ACK errors/task retries")
    require(all(e["retries"] == 0 for e in ev if e["event"] == "started"), "no task retry increments")
    for t in o["tasks"].values():
        require(all(e["redelivered"] for e in t["started"] if e["attempt"] > 1), "repeated bodies carry redelivery metadata")
        require(len({e["tag"] for e in t["registered"]}) == 1, "restoration preserves the delivery tag")
        if kind != "crash":
            require(len(t["ack"]) == len(t["received"]), "each live-worker receipt gets an ACK return, including skipped bodies")
            require(not any(t["broker"].values()), "all live-worker deliveries settled")
    if kind != "crash":
        require(all(code == 0 for code in o["returncodes"]), "live-worker cases exit cleanly")
    for config in o["configs"]:
        resolved = [e for e in ev if e["event"] == "resolved" and e["worker"] == config["label"]]
        require(bool(resolved), f"resolved configuration for {config['label']}")
        for r in resolved:
            require(r["channel_visibility"] == r["qos_visibility"] == config.get("B", 3600), "only B controls resolved timeout")
            require(r["top_present"] == ("T" in config) and r["top_value"] == config.get("T"), "top-level omission/value preserved")
            require(r["backend_options"] == ({"visibility_timeout": config["R"]} if "R" in config else {}), "backend option omission/value preserved")
            require(r["backend_connection_visibility"] is None, "backend does not forward visibility to redis-py")
    def phase(name, ident=task_id):
        return o["phases"].get(name, {}).get("tasks", {}).get(ident, {})
    starts, done = task.get("started", []), task.get("completed", [])
    if kind == "crash":
        require(o["returncodes"] == [-9, 0], "planned crash and clean replacement exit")
        require(bool(phase("crashed").get("broker", {}).get("unacked")), "crash leaves unacked record")
        fast = case.get("B", 3600) == 2 and not case.get("default_scan") and not case.get("scan_throttle")
        require(phase("observed").get("completed") == (1 if fast else 0), "configured timeout/scan controls recovery in window")
        recovered = fast or bool(case.get("scan_throttle"))
        require(len(starts) == (2 if recovered else 1) and len(done) == int(recovered), "crash/replacement execution count")
        require(task.get("state") == ("SUCCESS" if recovered else "PENDING"), "crash/replacement backend state")
        require(len(task.get("ack", [])) == int(recovered), "only completed replacement gets an ACK return")
        require(bool(task.get("broker", {}).get("unacked")) != recovered
                and not task.get("broker", {}).get("queued"), "crash final broker state matches recovery")
        if case.get("no_consumer"):
            p = phase("no_consumer").get("broker", {})
            require(bool(p.get("unacked")) and not p.get("queued"), "Redis does not restore without scanning consumer")
            require(all(x["age"] > 2 and x["key_ttl"] == -1 for x in p.get("unacked", [])), "expired visibility is not key expiration")
    elif kind == "active":
        duplicate = case.get("observer") and not case.get("observer_prefix") and case["pool"] != "prefork"
        require(len(starts) == len(done) == (2 if duplicate else 1), "active execution count")
        require(all(code == 0 for code in o["returncodes"]), "active worker exits")
        if case.get("observer_queue"):
            p = phase("expired_while_running")
            require(bool(p.get("broker", {}).get("queued")) and p.get("started") == 1,
                    "other queue worker restores but does not execute target delivery")
        elif duplicate:
            require(len(starts) == 2 and bool(done) and starts[1]["time"] < min(e["time"] for e in done), "overlapping executions")
        if not case.get("observer"):
            require(not any(e["event"] == "scan_call" and starts[0]["time"] < e["time"] < done[0]["time"] for e in ev),
                    "solo does not scan during inline execution")
            require(bool(phase("expired_while_running").get("broker", {}).get("unacked")), "solo record survives visibility age while loop blocked")
    elif kind == "eta":
        repeated = case["B"] == 2
        dedup = case.get("dedup") and case.get("acks_late", True)
        require(len(task.get("received", [])) >= (2 if repeated else 1), "ETA receipt/redelivery")
        require(phase("waiting").get("started") == 0, "ETA is still waiting during expiry")
        require((len(starts) == 1) if dedup or not repeated else len(starts) >= 2, "ETA body dedup depends on late ACK")
        require(len(done) == len(starts) and task.get("state") == "SUCCESS", "ETA bodies complete")
    elif kind == "reserved":
        second = o["tasks"].get(o["second_task"], {})
        p = phase("expired_while_running", o["second_task"])
        require(p.get("started") == 0 and bool(p.get("broker", {}).get("queued")), "reserved task expires before its first execution")
        require(len(starts) == len(done) == 2 and len(second.get("started", [])) == len(second.get("completed", [])) == 2,
                "active and reserved copies both execute twice")
    elif kind == "result":
        require(phase("stored").get("state") == "SUCCESS" and 0 < phase("stored").get("result_ttl", -1) <= 2, "result_expires sets result TTL")
        require(phase("expired_result").get("state") == "PENDING" and phase("expired_result").get("result_ttl") == -2, "result expires independently")
        require(len(starts) == len(done) == len(task.get("ack", [])) == 1, "expired result does not requeue settled delivery")
    if kind != "crash":
        require(not any(task.get("broker", {}).values()), "original message settled after live-worker cases")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=[c["name"] for c in CASES], action="append")
    args = parser.parse_args()
    run_dir = SUITE / "redis-visibility-runs" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    run_dir.mkdir(parents=True)
    import kombu.transport.redis as transport
    sources = [Path(__file__), SUITE / "redis_visibility_probe.py", ROOT / "task-policy/verify-task-policies.py",
               ROOT / "shutdown/verify-pool-shutdown.py", SUITE / "test-redis-visibility-probe.py",
               ROOT / "uv.lock", Path(transport.__file__),
               CELERY / "celery/app/base.py", CELERY / "celery/app/defaults.py", CELERY / "celery/backends/redis.py"]
    summary = dict(provenance=provenance(), python=sys.version, platform=platform.platform(), celery=__import__("celery").__version__,
        revision=source_revision(),
        dependencies={n: dependency_version(n) for n in ("kombu", "redis", "billiard", "gevent", "greenlet")},
        source_sha256={str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}, outcomes=[])
    with helpers.private_redis(run_dir) as (client, broker, backend):
        summary["redis_server"] = client.info("server")["redis_version"]
        summary["redis_settings"] = {key: client.config_get(key)[key] for key in ("timeout", "save", "appendonly")}
        for case in CASES:
            if args.case and case["name"] not in args.case:
                continue
            summary["outcomes"].append(run_case(case, run_dir, client, broker, backend))
            (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Artifacts: {run_dir.relative_to(ROOT)}", flush=True)
    return 0 if all(o["passed"] for o in summary["outcomes"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
