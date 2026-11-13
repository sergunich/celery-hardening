#!/usr/bin/env python3
"""Send OS signals to isolated real workers and assert the observed semantics.

Run: .dev/.venv/bin/python .dev/verify-solo-shutdown.py
Artifacts: .dev/shutdown-runs/<UTC timestamp>/{summary.json,<case>/*}
Use --case NAME (repeatable) to run a subset. POSIX only.
"""
import argparse
from datetime import datetime, timezone
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import time

DEV = Path(__file__).resolve().parent
ROOT = DEV.parent
# Times are relative to task_started (or worker_ready for idle workers).
CASES = [
    dict(name="warm-early", signals=[(0.3, "SIGTERM")], soft_timeout=1.0, expect="success"),
    dict(name="warm-late", signals=[(0.3, "SIGTERM")], soft_timeout=1.0, acks_late=True, expect="success"),
    dict(name="cold-early", signals=[(0.3, "SIGQUIT")], expect="failure"),
    dict(name="cold-late", signals=[(0.3, "SIGQUIT")], acks_late=True, expect="failure"),
    dict(name="soft-early", signals=[(0.3, "SIGQUIT")], soft_timeout=1.0, expect="soft_failure"),
    dict(name="soft-late", signals=[(0.3, "SIGQUIT")], soft_timeout=1.0, acks_late=True, expect="soft_failure"),
    dict(name="cold-caught", signals=[(0.3, "SIGQUIT")], catch="exception", expect="caught_success"),
    dict(name="soft-caught", signals=[(0.3, "SIGQUIT")], soft_timeout=1.0, catch="exception", expect="paused_success"),
    dict(name="warm-hung", signals=[(0.3, "SIGTERM")], soft_timeout=1.0, steps=None, deadline=2.0, expect="hung"),
    # Broad catch keeps the task alive through unsupported cold cancellation,
    # allowing the fourth Ctrl+C to reach Celery's hard-shutdown handler.
    dict(name="hard-escalation", signals=[(0.3, "SIGINT"), (0.6, "SIGINT"), (0.9, "SIGINT"), (1.2, "SIGINT")],
         soft_timeout=2.0, catch="exception", steps=None, expect="hard"),
    dict(name="hard-ordinary", signals=[(0.3, "SIGINT"), (0.6, "SIGINT"), (0.9, "SIGINT"), (1.2, "SIGINT")],
         soft_timeout=2.0, steps=None, expect="failure"),
    dict(name="hard-native", signals=[(0.3, "SIGINT"), (0.6, "SIGINT"), (0.9, "SIGINT"), (1.2, "SIGINT")],
         soft_timeout=2.0, catch="exception", steps=None, native_on_cancel=True, expect="hard"),
    dict(name="warm-native", signals=[(0.3, "SIGTERM")], native_first=True, steps=0, expect="success"),
    dict(name="hard-caught", signals=[(0.3, "SIGINT"), (0.6, "SIGINT"), (0.9, "SIGINT"), (1.2, "SIGINT")],
         soft_timeout=2.0, catch="base", steps=None, deadline=2.0, expect="hard_caught"),
    dict(name="sigkill", signals=[(0.3, "SIGKILL")], steps=None, expect="killed"),
    dict(name="idle-cold", signals=[(0.3, "SIGQUIT")], idle=True, expect="idle"),
    dict(name="idle-soft-skipped", signals=[(0.3, "SIGQUIT")], idle=True, soft_timeout=1.0, expect="idle"),
    dict(name="idle-soft-enabled", signals=[(0.3, "SIGQUIT")], idle=True, soft_timeout=1.0, soft_on_idle=True, expect="idle_soft"),
    dict(name="remote-warm", signals=[], remote=True, expect="remote"),
]


def read_events(path):
    if not path.exists():
        return []
    # The final write could be in progress; completed JSONL lines are sufficient.
    lines = path.read_text().splitlines(keepends=True)
    return [json.loads(line) for line in lines if line.endswith("\n")]


def check(case, events, sent, forced, returncode, log):
    errors = []
    def require(condition, message):
        if not condition:
            errors.append(message)
    def named(name):
        return [e for e in events if e["event"] == name]

    expected = case["expect"]
    failed = named("task_failure")
    caught = named("task_caught")
    success = named("task_success")
    ticks = named("task_tick")
    soft_log = "Initiating Soft Shutdown" in log
    if expected in {"success", "caught_success", "paused_success", "remote"}:
        require(bool(success), "task must succeed")
        require(not failed, "task must not fail")
        if sent:
            require(bool(success) and success[0]["time"] > sent[0]["time"], "success must follow signal")
    if expected in {"failure", "soft_failure"}:
        require(any(e["exception"] == "NotImplementedError" and "kill_job" in e["message"] for e in failed),
                "cold cancellation must fail task with NotImplementedError / kill_job")
        require(not success, "cancelled task must not succeed")
        returned = named("worker_returned")
        requests = returned[0].get("requests", []) if returned else []
        require(bool(requests) and requests[0]["acknowledged"], "failed task must be acknowledged with default ack-on-failure")
        require(bool(requests) and not requests[0]["cancelled"], "unsupported cancellation must not set cancelled flag")
    if expected in {"caught_success", "paused_success", "hard", "hard_caught"}:
        require(any(e["exception"] == "NotImplementedError" for e in caught), "task must catch unsupported cancellation")
    if expected == "soft_failure" and failed:
        require(failed[0]["time"] - sent[0]["time"] >= 0.9, "failure must wait for soft timeout")
        require(not [e for e in ticks if sent[0]["time"] + 0.1 < e["time"] < failed[0]["time"]],
                "task must not make progress during soft sleep")
    if expected == "paused_success":
        gaps = [b["time"] - a["time"] for a, b in zip(ticks, ticks[1:])]
        require(bool(gaps) and max(gaps) >= 0.9, "soft shutdown must pause task progress")
    if expected in {"soft_failure", "paused_success", "idle_soft", "hard", "hard_caught"}:
        require(soft_log, "soft shutdown log must be present")
    elif expected in {"success", "hung", "idle"}:
        require(not soft_log, "soft shutdown must not run on warm shutdown or skipped idle")
    if expected in {"hung", "hard_caught"}:
        require(forced, "worker must remain alive until supervisor deadline")
        require(not named("task_finally"), "task must not unwind before forced kill")
    else:
        require(not forced, "worker must exit before supervisor deadline")
    if expected == "hard":
        require(len(sent) == 4, "all four Ctrl+C signals must be sent")
        require(bool(named("task_finally")), "hard shutdown must unwind task")
        require(not success and not failed, "WorkerTerminate must not become ordinary task success/failure")
    if case.get("native_first"):
        handlers = named("worker_shutting_down")
        require(bool(handlers) and handlers[0]["time"] - sent[0]["time"] >= 0.7,
                "native task must delay execution of warm signal handler")
    if case.get("native_on_cancel"):
        unwound = named("task_finally")
        require(bool(unwound) and unwound[0]["time"] - sent[-1]["time"] >= 0.7,
                "native task must delay hard signal interruption")
        require(not named("native_returned"), "pending hard signal must interrupt on return from native code")
    if expected == "hard_caught":
        require(any(e["exception"] == "WorkerTerminate" for e in caught), "task must catch WorkerTerminate")
        require(any(e["time"] > sent[-1]["time"] + 0.2 for e in ticks), "task must continue after hard signal")
    if expected == "killed":
        require(returncode == -signal.SIGKILL, "process must die from SIGKILL")
        require(not named("task_finally") and not named("worker_returned"), "SIGKILL must skip cleanup")
    if expected in {"idle", "idle_soft"}:
        require(not named("task_started"), "idle worker must not run a task")
        require(bool(named("worker_returned")), "idle worker must return")
        if expected == "idle_soft" and named("worker_returned"):
            require(named("worker_returned")[0]["time"] - sent[0]["time"] >= 0.9, "idle soft wait must elapse")
    if expected == "remote":
        remote = named("remote_shutdown_sent")
        require(bool(remote) and bool(success) and remote[0]["time"] < success[0]["time"],
                "remote shutdown must be published while task is active")
        require("Got shutdown from remote" in log, "worker must process remote command")
    return errors


def run_case(case, run_dir):
    folder = run_dir / case["name"]
    folder.mkdir()
    event_path = folder / "events.jsonl"
    log_path = folder / "worker.log"
    env = os.environ.copy()
    # Make the default TERM mapping deterministic and don't write bytecode to the checkout.
    env.pop("REMAP_SIGTERM", None)
    env.pop("CELERY_RDBSIG", None)
    env.update(PYTHONPATH=str(ROOT), PYTHONDONTWRITEBYTECODE="1", PYTHONUNBUFFERED="1")
    sent, forced, origin = [], False, None
    started = time.monotonic()
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [sys.executable, str(DEV / "shutdown-worker.py"), "--config", json.dumps(case), "--events", str(event_path)],
            cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )
        try:
            schedule = iter(case["signals"])
            next_signal = next(schedule, None)
            while process.poll() is None:
                now = time.monotonic()
                if origin is None:
                    marker = "worker_ready" if case.get("idle") else "task_started"
                    matching = [e for e in read_events(event_path) if e["event"] == marker]
                    if matching:
                        origin = matching[0]["time"]
                    elif now - started > 20:
                        raise RuntimeError(f"{case['name']}: worker did not reach {marker}; see {log_path}")
                if origin is not None:
                    if next_signal and now - origin >= next_signal[0]:
                        record = dict(signal=next_signal[1], time=time.monotonic())
                        try:
                            os.kill(process.pid, getattr(signal, next_signal[1]))
                        except ProcessLookupError:
                            break
                        sent.append(record)
                        next_signal = next(schedule, None)
                    if now - origin >= case.get("deadline", 9.0):
                        forced = True
                        process.kill()
                        break
                time.sleep(0.01)
            process.wait(timeout=5)
        finally:
            # Kill only this experiment's isolated process group, never other workers.
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
    events = read_events(event_path)
    errors = check(case, events, sent, forced, process.returncode, log_path.read_text())
    returned = next((e for e in events if e["event"] == "worker_returned"), {})
    start = origin if origin is not None else started
    outcome = dict(
        case=case, passed=not errors, errors=errors, returncode=process.returncode,
        forced_kill=forced, signals=sent,
        elapsed=round(time.monotonic() - start, 3), result_state=returned.get("result_state"),
        timeline=[dict(e, relative=round(e["time"] - start, 3)) for e in events],
    )
    (folder / "result.json").write_text(json.dumps(outcome, indent=2) + "\n")
    print(f"{'PASS' if not errors else 'FAIL'} {case['name']}: exit={process.returncode}, "
          f"state={outcome['result_state']}, forced={forced}, elapsed={outcome['elapsed']}s", flush=True)
    for error in errors:
        print(f"  {error}")
    return outcome


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", choices=[c["name"] for c in CASES])
    args = parser.parse_args()
    if os.name != "posix":
        parser.error("POSIX signals are required")
    run_dir = DEV / "shutdown-runs" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    run_dir.mkdir(parents=True)
    cases = [c for c in CASES if not args.case or c["name"] in args.case]
    outcomes = [run_case(case, run_dir) for case in cases]
    summary = dict(
        python=sys.version, executable=sys.executable, platform=platform.platform(),
        revision=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        dependencies={name: importlib.metadata.version(name) for name in ("billiard", "kombu", "vine")},
        outcomes=outcomes,
    )
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Artifacts: {run_dir.relative_to(ROOT)}")
    return 0 if all(o["passed"] for o in outcomes) else 1


if __name__ == "__main__":
    raise SystemExit(main())
