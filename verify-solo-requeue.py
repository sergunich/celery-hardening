#!/usr/bin/env python3
"""Verify a version-specific task-level Reject workaround for solo cold shutdown.

Run: .dev/.venv/bin/python .dev/verify-solo-requeue.py
This does not guarantee interruption inside the task's catch block or broker recovery.
"""
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

DEV = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("shutdown_helpers", DEV / "verify-pool-shutdown.py")
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)


def main():
    run_dir = DEV / "solo-requeue-runs" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    run_dir.mkdir(parents=True)
    outcomes = []
    with helpers.private_redis(run_dir) as (client, broker, backend):
        for late in (True, False):
            case = dict(name=f"cold-requeue-{'late' if late else 'early'}", group="requeue",
                        signals=[(0.3, "SIGQUIT")], catch="requeue", acks_late=late,
                        description="Catch this checkout's solo kill_job error inside task work and raise Reject(requeue=True).")
            o = helpers.run_case(case, "solo", run_dir, client, broker, backend, native_iterations=0)
            expected = {
                "catch reached": any(e["event"] == "task_requeue_requested" for e in o["timeline"]),
                "body did not complete": not o["completed"],
                "task result remains PENDING": o["result_state"] == "PENDING",
                "ACK policy": o["ack_observed"] == (not late),
                "requeue policy": bool(o["broker"]["queued"]) == late,
                "reject operation": bool(o["rejects"]) == late,
                "no supervisor kill": not o["forced_kill"],
            }
            if late:
                expected["original message fetchable"] = o["restored_delivery"]["task_id"] == o["task_id"]
            o["errors"].extend(name for name, passed in expected.items() if not passed)
            o["passed"] = not o["errors"]
            (run_dir / "solo" / case["name"] / "result.json").write_text(json.dumps(o, indent=2) + "\n")
            outcomes.append(o)
            print(f"Requeue checks: {'PASS' if o['passed'] else 'FAIL'} {case['name']}: {o['errors']}", flush=True)
    (run_dir / "summary.json").write_text(json.dumps(dict(outcomes=outcomes), indent=2) + "\n")
    print(f"Artifacts: {run_dir}")
    return 0 if all(o["passed"] for o in outcomes) else 1


if __name__ == "__main__":
    raise SystemExit(main())
