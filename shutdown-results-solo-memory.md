# Solo pool shutdown: runtime verification and clarification

## Conclusion

**Warm shutdown works cooperatively. Soft/cold shutdown do not provide the
advertised running-task semantics for solo. Celery's hard shutdown can interrupt
Python execution, but is neither uncatchable nor guaranteed to run immediately.**

The runtime experiments confirm the earlier source analysis and sharpen two
important points:

1. Cold shutdown does not merely fail to terminate a child. Its unsupported
   cancellation call raises **inside the running task**, normally marking the
   task `FAILURE` with `NotImplementedError`. An application `except Exception`
   can catch this and continue executing.
2. `acks_late=True` alone does **not** protect that task from being acknowledged:
   the late-ack cold/soft cases finished with `Request.acknowledged=True` under
   the tested default acknowledge-on-failure behavior.

**19/19 behavior checks passed.** These tests assert the observed limitations;
this does not mean all four shutdown modes work correctly for solo.

## Environment and artifacts

- Checkout: Celery **5.7.0**, commit
  `6fcdbf904d410749ad7676d8ff6b1b25d6e19675`.
- OS: macOS 15.8.1, ARM64; CPython **3.13.9**.
- Dependencies: billiard **4.3.2**, kombu **5.7.0**, vine **5.1.0**.
- Installed with `uv` into `.dev/.venv`; all resolved runtime dependencies are
  pinned in `.dev/shutdown-requirements.lock`.
- Final complete run:
  [`shutdown-runs/20261112T160510.147837Z/summary.json`](shutdown-runs/20261112T160510.147837Z/summary.json).
- Each case directory contains `worker.log`, timestamped `events.jsonl`, and
  `result.json` with the signal schedule, assertions, exit status, and timeline.
- Earlier exploratory runs are also retained under `shutdown-runs/`.
- `.dev/` is locally ignored by `/.dev/` in `.git/info/exclude`;
  the repository's shared `.gitignore` was not changed. No production source
  files were changed and no commits were made.

## Reproduce

From the repository root:

```bash
bash .dev/setup-shutdown-env.sh
PYTHONDONTWRITEBYTECODE=1 .dev/.venv/bin/python .dev/verify-solo-shutdown.py
```

Run selected cases:

```bash
.dev/.venv/bin/python .dev/verify-solo-shutdown.py \
  --case soft-late --case hard-native
```

Scripts:

- `setup-shutdown-env.sh`: creates the Python environment if needed and syncs
  pinned dependencies. Uses this checkout via `PYTHONPATH`, not installed Celery.
- `shutdown-worker.py`: starts a real `app.Worker(pool="solo")` with Celery's
  normal signal handlers, task execution, result handling, and shutdown paths.
  Observation hooks record task and worker events; shutdown methods are not mocked.
- `verify-solo-shutdown.py`: runs each worker in its own subprocess/session,
  waits for task execution or worker readiness, sends actual POSIX signals,
  checks outcomes, and writes artifacts. It exits nonzero on assertion failure.
  Deadlines and cleanup kill only experiment processes, never unrelated workers.

The complete run takes roughly one minute. Two deliberately nonterminating
cases are killed by the harness at a two-second deadline. Other cases have a
nine-second safety deadline after task start/readiness. Native-code cases
calibrate PBKDF2 work to approximately two seconds on the current machine.

## Method and scope

- Broker: Kombu `memory://`; backend: `cache+memory://`.
- Tasks are published inside the worker process when it is ready, because the
  memory transport is process-local. Tasks run normally through the consumer
  and solo pool, not eagerly and not via direct function calls.
- Normal tasks perform 20 sequential 100 ms work steps. The work budget is a
  fixed number of steps, not a wall-clock deadline, so pausing the task cannot
  falsely appear to complete its work.
- Signals are scheduled relative to `task_started` (idle cases: `worker_ready`).
- Ordinary TERM/QUIT cases send their signal at about 0.3 seconds.
- Ctrl+C escalation sends SIGINT at 0.3, 0.6, 0.9, and 1.2 seconds.
- `REMAP_SIGTERM` is explicitly unset. Gossip, mingle, and worker heartbeat are
  disabled to isolate task/shutdown behavior. Ordinary OS-signal tests publish
  only one task.
- Early/late acknowledgment variants use
  `task_acks_on_failure_or_timeout=True` and otherwise default task failure policy.
- Request acknowledgment/cancellation flags are inspected after worker return.
- Remote shutdown is broadcast from a publisher thread sharing the memory
  transport; it is still consumed through the real worker control channel.
- Native-code cases use `hashlib.pbkdf2_hmac`, not a mock blocking call.

**Not tested:** Redis/RabbitMQ/SQS connection teardown, durable redelivery,
visibility timeouts, broker heartbeat survival, ETA tasks, task backlogs, prefork,
Linux/Windows, or older Celery releases. Memory transport cannot establish any
claim about real-broker message recovery. Timing and cleanup duration are
platform/transport dependent.

## Results

Elapsed times below are from task start/readiness to process exit, including
worker teardown. They are not all task execution times.

| Case | Observed result | Elapsed |
| --- | --- | ---: |
| `warm-early` | Task succeeds after TERM; worker exits | 4.143 s |
| `warm-late` | Same with late acknowledgment | 4.172 s |
| `cold-early` | Task fails with unsupported `kill_job`; acknowledged | 2.408 s |
| `cold-late` | Same failure; late-ack request is also acknowledged | 2.447 s |
| `soft-early` | Task pauses for 1 s, then fails like cold | 3.408 s |
| `soft-late` | Same pause/failure; late-ack request is acknowledged | 3.383 s |
| `cold-caught` | Task catches `NotImplementedError`, continues, succeeds | 4.376 s |
| `soft-caught` | Task pauses, catches cancellation error, resumes, succeeds | 5.274 s |
| `warm-hung` | Task keeps running despite TERM and 1 s soft setting; harness kills | 2.013 s |
| `hard-escalation` | Fourth Ctrl+C unwinds running task; backend remains `PENDING` | 1.263 s |
| `hard-ordinary` | Third Ctrl+C first causes task `FAILURE`; fourth interrupts teardown | 1.262 s |
| `hard-native` | Fourth Ctrl+C is delayed until native computation returns | 2.975 s |
| `warm-native` | TERM handler is delayed until native computation returns; task succeeds | 4.122 s |
| `hard-caught` | Task catches `WorkerTerminate` and continues; harness kills | 2.007 s |
| `sigkill` | OS kills worker; task `finally` and worker return do not run | 0.315 s |
| `idle-cold` | No task-cancellation error; worker exits | 4.091 s |
| `idle-soft-skipped` | Soft timeout configured but skipped without active tasks | 4.100 s |
| `idle-soft-enabled` | Explicit idle soft wait runs, then worker exits | 4.089 s |
| `remote-warm` | Remote shutdown waits behind running task; task succeeds first | 2.130 s |

### Warm: cooperative, not deadline-bounded

`warm-early` received TERM at **0.311 s**, completed its task at **2.060 s**, and
returned from the worker at **4.107 s**. Late acknowledgment behaved equivalently.

Both warm cases had `worker_soft_shutdown_timeout=1.0`. They did **not** enter
soft shutdown or stop the task at that deadline. `warm-hung` also kept running
until the harness killed it. **The soft setting is a pre-cold grace period,
not an automatic timeout on warm shutdown.**

Native work can delay the signal handler itself: `warm-native` received TERM at
**0.301 s**, but its warm handler ran only at **2.016 s**, when PBKDF2 returned.

### Soft: the main thread waits instead of advancing the task

In `soft-late`, QUIT was sent at **0.302 s**. The task made no progress during
the one-second soft sleep, then failed at **1.314 s**.

`soft-caught` makes the pause especially clear: the task caught the cancellation
error at **1.311 s**, resumed its remaining work, and completed at **3.182 s**
rather than roughly two seconds. The one-second wait was not a chance for the
solo task to finish; it delayed the task's execution.

Idle behavior is distinct: without `worker_enable_soft_shutdown_on_idle`, the
wait is skipped; with it, the handler waited approximately one second before
announcing cold shutdown. Overall process-exit times were similar because of
other worker teardown waits. Do not infer the soft wait from exit latency alone.

### Cold: failure injection, not supported pool cancellation

The actual traceback was:

```text
on_cold_shutdown()
  consumer.cancel_active_requests()
    request.cancel(pool)
      pool.terminate_job(...)
NotImplementedError: <class 'celery.concurrency.solo.TaskPool'> does not implement kill_job
```

For example, `cold-late` received QUIT at **0.319 s** and emitted task failure at
**0.371 s**. On worker return its result was `FAILURE` and its Request flags were:

```json
{"acknowledged": true, "cancelled": false, "revoked": false}
```

This happened for both early- and late-ack tasks. The task's `finally` ran because
this was Python exception unwinding, not process killing. With a broad
`except Exception` around the interrupted operation, the task instead continued
to `SUCCESS` (`cold-caught`). Thus cold shutdown is neither a clean supported
cancellation nor a reliable way to stop arbitrary solo application code.

### Hard: distinguish signal escalation from SIGKILL

To reach Celery's hard handler while the task was still running,
`hard-escalation` deliberately caught the preceding unsupported cancellation
error. The fourth SIGINT arrived at **1.202 s**; task `finally` ran at
**1.203 s**, and the worker returned at **1.204 s**. The backend remained
`PENDING`: no ordinary task success/failure was recorded by this interruption.
This was an **early-ack task**, so `PENDING` does not mean queued or recoverable.

Without that deliberate catch, `hard-ordinary` failed at **0.919 s** from the
third SIGINT's cancellation attempt. The fourth SIGINT then interrupted worker
teardown. Repeated Ctrl+C does not bypass the solo cancellation problem.

Hard shutdown's limitations were also reproduced:

- `hard-caught`: catching `BaseException` caught `WorkerTerminate(1)`; the task
  continued until the harness killed it. Ordinary `except Exception` does not
  catch `WorkerTerminate`.
- `hard-native`: the hard signal arrived at **1.208 s** while PBKDF2 was running;
  the task did not unwind until **2.955 s**. A pending Python signal handler
  cannot interrupt this native function at arbitrary times.
- `sigkill`: immediate OS termination skipped both task `finally` and normal
  worker return. This is fundamentally different from Celery's exception-based
  hard-shutdown handler.

All naturally exiting experiment processes reported status **0**, including
cold and hard cases. In those cases `worker.exitcode` was `None`; the harness
uses `sys.exit(worker.exitcode or 0)`. The production CLI similarly passes
`worker.exitcode` to Click, but CLI exit statuses were not separately tested.
**Do not use these process statuses as proof that a task succeeded.**

### Remote control is blocked by task execution

In `remote-warm`, a publisher thread sent shutdown at **0.305 s**. The task
succeeded at **2.072 s**; only then did the worker process the command and shut
down. OS signals bypass this broker-control-message scheduling bottleneck,
although Python handler execution can still be delayed by native work.

## Follow-up recommendation

For solo, prefer **TERM + a supervisor-enforced external deadline**. Let work
finish normally; if it cannot, SIGKILL is an unconditional last resort, with no
cleanup or message-recovery guarantee.

Do not rely on `worker_soft_shutdown_timeout` to give a running solo task a real
completion window, or on QUIT to cleanly cancel it. Do not assume `acks_late`
alone implies redelivery after the cancellation error demonstrated here.

If a real time-bounded grace period and independent task termination are
requirements, use prefork and verify the same scenarios with your production
broker, acknowledgment/retry policy, and supervisor. Tasks interrupted after
external side effects still need idempotency or transactional safeguards.

Relevant implementation: `celery/concurrency/solo.py`,
`celery/concurrency/base.py`, `celery/apps/worker.py`,
`celery/worker/worker.py`, `celery/worker/consumer/consumer.py`,
`celery/worker/request.py`, and `celery/worker/state.py`.
