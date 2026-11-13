# Celery shutdowns: solo, prefork, threads, and gevent

## Bottom line

These are **runtime observations for this checkout and environment**, not a
promise about every Celery release or platform. Tested: **Celery 5.7.0,
macOS ARM64, Python 3.13.9, Redis 8.2.3**. Prefork children used **spawn**.

| Pool | Warm: let running work finish | Soft: give work a completion window | Cold: stop running work | Hard: force exit via signal escalation |
| --- | --- | --- | --- | --- |
| **solo** | Works for finite tasks | **No useful window:** the task's own thread sleeps in the handler | Unsupported cancellation becomes a task exception | Can interrupt Python code; catchable/delayed; ordinary tasks may fail before hard is reached |
| **prefork** | Works | Worked for short tasks; expired long tasks were stopped | Worked for normal tasks; canceled late-ack messages were restored | Reached the hard handler and exited in tested cases; not equivalent to SIGKILL |
| **threads** | Works | Tasks keep running, but expiry cannot forcibly cancel them | **Unsupported:** controller errors while task keeps running | **Not reliable:** several endless-task cases still needed SIGKILL |
| **gevent** | Worked for cooperative tasks | **Broken in these signal tests:** `BlockingSwitchOutError`; work could exceed the timeout | Greenlet was stopped, but shutdown could error and **ACK incomplete late-ack work** | Ordinary cases exited before hard; catch-BaseException case reached it |

**Most important delivery findings:**

- `SUCCESS` does not imply ACK: a threaded native task finished successfully,
  but hard shutdown left its message restored in Redis (duplicate-execution risk).
- `PENDING` does not imply the task is still queued: gevent cold shutdown left
  incomplete tasks `PENDING` **and ACKed**, with no message left to redeliver.
- `RETRY` does not prove a retry was published: prefork early-ack cancellation
  wrote `RETRY`, but no message remained in the queue or unacked set.
- Early-ack **configuration** is not the same as an ACK having reached the broker.
  Solo's Redis event loop deferred its early ACK while the task occupied the thread.

**Validation:** 96/96 experiment checks passed. These checks reproduce
observed behavior, including bugs; they do **not** certify all shutdown modes as correct.

The preceding full run shared **92 cases** with this run; **0 differed** in task completion, backend state, observed ACK, message location, forced kill, exit code, or whether the hard handler ran. Elapsed times are not expected to match.

## How to read the results

Each test below states the task, signal schedule, and relevant settings before
its results table. All pools use concurrency **1**. The threaded pool's CLI name
is `threads`.

- **early / late** is `task_acks_late=False / True`, not an observed broker outcome.
- **Finished?** means the task body reached its final completion marker.
- **Backend** is the actual Redis result state read after the worker exits.
- **ACK sent?** means the original task delivery's `Message.ack()` returned in the
  worker. We also inspect Redis after exit; this is stronger than checking only
  Celery's `Request.acknowledged` flag. `No` is not inferred from `PENDING`.
- **Message after exit** is inspected before the harness drains any restored message:
  - **Absent:** neither queued nor in Redis's unacked hash.
  - **Restored:** queued again; the harness fetched it and verified the original task ID.
  - **Unacked:** still in Redis's unacked hash, not yet available in the queue.
    Visibility-timeout recovery was **not** waited out (configured to 3600 s).
  - **—:** no task was published.
- **Worker exit** gives the actual CLI process exit code and elapsed seconds
  since task start (idle tests: readiness). Code `0` does not prove task success.
  `KILL (deadline)` means the experiment's safety deadline was necessary;
  `KILL (requested)` is the intentional SIGKILL test.
- **Notes:** `kill_job error` = unsupported `terminate_job()` / `NotImplementedError`;
  `hub error` = gevent `BlockingSwitchOutError`. `Hard: yes/no` records actual
  entry into Celery's hard handler, not merely how many signals we planned to send.

For most cases, the task performs 100 ms work steps. “2 s task” means 20 steps,
not a wall-clock deadline; pausing the task cannot falsely finish its work.
All deadlines and native-code timings are approximate.

## 1. Warm shutdown — allow running work to finish

**Test (`warm-early`, `warm-late`):** a 2 s task receives `SIGTERM` at 0.3 s.
`worker_soft_shutdown_timeout=1` is deliberately enabled. It must **not** turn
warm shutdown into a one-second task deadline.

| Pool | Variant | Finished? | Backend | ACK sent? | Message after exit | Worker exit | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| solo | early | Yes | SUCCESS | Yes | Absent | 0 / 2.16 s | — |
| solo | late | Yes | SUCCESS | Yes | Absent | 0 / 2.15 s | — |
| prefork | early | Yes | SUCCESS | Yes | Absent | 0 / 4.00 s | — |
| prefork | late | Yes | SUCCESS | Yes | Absent | 0 / 4.37 s | — |
| threads | early | Yes | SUCCESS | Yes | Absent | 0 / 2.15 s | — |
| threads | late | Yes | SUCCESS | Yes | Absent | 0 / 2.17 s | — |
| gevent | early | Yes | SUCCESS | Yes | Absent | 0 / 5.09 s | — |
| gevent | late | Yes | SUCCESS | Yes | Absent | 0 / 4.08 s | — |

All four pools finished and ACKed finite work. The soft setting does not
automatically escalate a warm shutdown. See the endless-task case below.

## 2. Cold shutdown — cancel work without a grace period

**Test (`cold-early`, `cold-late`):** a 2 s task receives `SIGQUIT` at 0.3 s;
soft shutdown is disabled.

| Pool | Variant | Finished? | Backend | ACK sent? | Message after exit | Worker exit | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| solo | early | No | FAILURE | Yes | Absent | 0 / 0.40 s | kill_job error |
| solo | late | No | FAILURE | Yes | Absent | 0 / 0.39 s | kill_job error |
| prefork | early | No | RETRY | Yes | Absent | 0 / 1.92 s | — |
| prefork | late | No | PENDING | No | Restored | 0 / 1.92 s | — |
| threads | early | Yes | SUCCESS | Yes | Absent | 1 / 2.15 s | kill_job error |
| threads | late | Yes | SUCCESS | Yes | Absent | 1 / 2.14 s | kill_job error |
| gevent | early | No | PENDING | Yes | Absent | 1 / 1.07 s | hub error |
| gevent | late | No | PENDING | Yes | Absent | 1 / 1.06 s | hub error |

**Interpretation:** prefork restored its canceled late-ack message; solo instead
failed and ACKed the task. Threads did not stop the task. Gevent stopped the task
but ACKed it even with late acknowledgments; the backend stayed `PENDING`.
None of these interrupted-task results should be read as successful business work.

## 3. Soft shutdown — task fits inside the grace period

**Test (`soft-short-early`, `soft-short-late`):** a **0.8 s** task receives
`SIGQUIT` at 0.3 s, with a **1.5 s** soft timeout. Without a pool limitation,
the remaining work easily fits inside this grace period.

| Pool | Variant | Finished? | Backend | ACK sent? | Message after exit | Worker exit | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| solo | early | No | FAILURE | Yes | Absent | 0 / 1.89 s | kill_job error |
| solo | late | No | FAILURE | Yes | Absent | 0 / 1.91 s | kill_job error |
| prefork | early | Yes | SUCCESS | Yes | Absent | 0 / 3.65 s | — |
| prefork | late | Yes | SUCCESS | Yes | Absent | 0 / 3.43 s | — |
| threads | early | Yes | SUCCESS | Yes | Absent | 0 / 1.89 s | — |
| threads | late | Yes | SUCCESS | Yes | Absent | 0 / 1.87 s | — |
| gevent | early | Yes | SUCCESS | Yes | Absent | 1 / 1.05 s | hub error |
| gevent | late | Yes | SUCCESS | Yes | Absent | 1 / 1.07 s | hub error |

**Interpretation:** prefork and threads allowed the task to finish normally.
Solo slept in the task's own thread, then failed it. Gevent's task finished,
but **not through a successful soft-shutdown path**: the handler raised a hub
error and the worker fell back to error cleanup/waiting, exiting with code 1.

## 4. Soft shutdown — task outlasts the grace period

**Test (`soft-long-early`, `soft-long-late`):** a **3 s** task receives
`SIGQUIT` at 0.3 s, with a **1 s** soft timeout. Approximately 1.3 s after task
start, a functioning soft-to-cold transition should stop remaining work.

| Pool | Variant | Finished? | Backend | ACK sent? | Message after exit | Worker exit | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| solo | early | No | FAILURE | Yes | Absent | 0 / 1.41 s | kill_job error |
| solo | late | No | FAILURE | Yes | Absent | 0 / 1.41 s | kill_job error |
| prefork | early | No | RETRY | Yes | Absent | 0 / 2.91 s | — |
| prefork | late | No | PENDING | No | Restored | 0 / 2.92 s | — |
| threads | early | Yes | SUCCESS | Yes | Absent | 1 / 3.17 s | kill_job error |
| threads | late | Yes | SUCCESS | Yes | Absent | 1 / 3.17 s | kill_job error |
| gevent | early | Yes | SUCCESS | Yes | Absent | 1 / 3.18 s | hub error |
| gevent | late | Yes | SUCCESS | Yes | Absent | 1 / 3.19 s | hub error |

**Interpretation:** prefork stopped work and restored the late-ack message.
Solo made no task progress during the soft sleep and then failed the task.
Threads continued past the deadline because cancellation is unsupported.
Gevent's soft wait raised a hub error; its task also ran beyond the intended deadline.

## 5. Hard shutdown — repeated Ctrl+C escalation

**Test (`hard-early`, `hard-late`):** an **endless** task receives SIGINT at
0.3, 0.6, 0.9, and 1.2 s, with a 2 s soft timeout. The fourth signal is intended
to reach the hard handler. A worker still alive at **4 s** is killed by the harness.
If the worker exits earlier, later signals are not sent.

| Pool | Variant | Finished? | Backend | ACK sent? | Message after exit | Worker exit | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| solo | early | No | FAILURE | Yes | Absent | 0 / 0.98 s | kill_job error; Hard: no |
| solo | late | No | FAILURE | Yes | Absent | 0 / 1.01 s | kill_job error; Hard: no |
| prefork | early | No | RETRY | Yes | Absent | 1 / 2.49 s | Hard: yes |
| prefork | late | No | PENDING | No | Restored | 0 / 2.32 s | Hard: yes |
| threads | early | No | PENDING | Yes | Absent | 1 / 1.28 s | kill_job error; Hard: yes |
| threads | late | No | PENDING | No | Unacked | KILL (deadline) / 4.01 s | kill_job error; Hard: yes |
| gevent | early | No | PENDING | Yes | Absent | 1 / 1.07 s | hub error; Hard: no |
| gevent | late | No | PENDING | Yes | Absent | 1 / 1.06 s | hub error; Hard: no |

**Interpretation:** solo and gevent exited after earlier cancellation problems,
**before** hard was reached. This is why a process exit alone cannot validate the
hard handler. Catching cancellation in the additional tests below keeps those
tasks alive long enough to exercise further escalation.

Threads reached the hard handler in both variants, but the late-ack worker still
needed SIGKILL. The early-ack worker exited when `WorkerTerminate` interrupted
Python's interpreter/thread shutdown; that is not supported task cancellation
and should not be relied on as a general thread-termination mechanism.

## 6. OS SIGKILL — no Celery cleanup

**Test (`kill-early`, `kill-late`):** an endless task receives SIGKILL at 0.3 s.
The signal targets the **whole experiment process group**, including prefork
children, not just the worker parent. This is separate from Celery's hard handler.

| Pool | Variant | Finished? | Backend | ACK sent? | Message after exit | Worker exit | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| solo | early | No | PENDING | No | Unacked | KILL (requested) / 0.32 s | — |
| solo | late | No | PENDING | No | Unacked | KILL (requested) / 0.32 s | — |
| prefork | early | No | PENDING | Yes | Absent | KILL (requested) / 0.32 s | — |
| prefork | late | No | PENDING | No | Unacked | KILL (requested) / 0.32 s | — |
| threads | early | No | PENDING | Yes | Absent | KILL (requested) / 0.33 s | — |
| threads | late | No | PENDING | No | Unacked | KILL (requested) / 0.32 s | — |
| gevent | early | No | PENDING | Yes | Absent | KILL (requested) / 0.32 s | — |
| gevent | late | No | PENDING | No | Unacked | KILL (requested) / 0.31 s | — |

With solo + Redis's async consumer, the “early ACK” callback is queued on the same event loop blocked by
the task. In this case it had **not reached Redis before SIGKILL**. This was
reproduced; it is not a reason to treat early acknowledgment as safe in general.
The other pools sent their early ACK before the kill. All late-ack messages
remained unacked, not immediately queued for redelivery.

## 7. Warm shutdown of an endless task

**Test (`warm-hung`):** endless task, late acknowledgment, TERM at 0.3 s,
soft timeout 1 s. The harness kills any worker still alive at **3 s**.

| Pool | Variant | Finished? | Backend | ACK sent? | Message after exit | Worker exit | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| solo | late | No | PENDING | No | Unacked | KILL (deadline) / 3.02 s | — |
| prefork | late | No | PENDING | No | Unacked | KILL (deadline) / 3.01 s | — |
| threads | late | No | PENDING | No | Unacked | KILL (deadline) / 3.01 s | — |
| gevent | late | No | PENDING | No | Unacked | KILL (deadline) / 3.01 s | — |

All four workers stayed alive until the safety kill. Warm shutdown has no
automatic task deadline here, regardless of `worker_soft_shutdown_timeout`.

## 8. Can task code catch cancellation?

### 8.1 Catching exceptions during cold shutdown

**Tests:** 2 s late-ack task, QUIT at 0.3 s. Task code wraps each work step in:

- `cold-catch-exception`: `except Exception`.
- `cold-catch-base`: `except BaseException` (deliberately broad; not recommended).

| Pool | Variant | Finished? | Backend | ACK sent? | Message after exit | Worker exit | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| solo | Exception / late | Yes | SUCCESS | Yes | Absent | 0 / 2.25 s | — |
| solo | BaseException / late | Yes | SUCCESS | Yes | Absent | 0 / 2.24 s | — |
| prefork | Exception / late | No | PENDING | No | Restored | 0 / 1.91 s | — |
| prefork | BaseException / late | Yes | SUCCESS | Yes | Absent | 0 / 4.43 s | — |
| threads | Exception / late | Yes | SUCCESS | Yes | Absent | 1 / 2.15 s | kill_job error |
| threads | BaseException / late | Yes | SUCCESS | Yes | Absent | 1 / 2.14 s | kill_job error |
| gevent | Exception / late | No | PENDING | Yes | Absent | 1 / 1.11 s | hub error |
| gevent | BaseException / late | Yes | SUCCESS | Yes | Absent | 1 / 2.18 s | hub error |

Solo catches `NotImplementedError`. Prefork cancellation interrupts the child
with `SystemExit`, which needs `BaseException` to catch. Gevent injects
`GreenletExit`, also requiring `BaseException`. In these finite-task cases,
catching those base exceptions let the task finish. Threaded tasks caught
nothing: their cancellation error occurred in the **controller thread**, not
in the task thread, and the task continued either way.

### 8.2 Catching exceptions during hard escalation

**Tests:** endless late-ack task, the same four-SIGINT schedule as section 5,
and a **4 s** harness deadline:

- `hard-catch-exception`: catch `Exception` around each work step.
- `hard-catch-base`: catch `BaseException` around each work step.

| Pool | Variant | Finished? | Backend | ACK sent? | Message after exit | Worker exit | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| solo | Exception / late | No | PENDING | No | Restored | 0 / 1.29 s | Hard: yes |
| solo | BaseException / late | No | PENDING | No | Unacked | KILL (deadline) / 4.01 s | Hard: yes |
| prefork | Exception / late | No | PENDING | No | Restored | 0 / 2.31 s | Hard: yes |
| prefork | BaseException / late | No | PENDING | No | Restored | 0 / 2.31 s | Hard: yes |
| threads | Exception / late | No | PENDING | No | Unacked | KILL (deadline) / 4.01 s | kill_job error; Hard: yes |
| threads | BaseException / late | No | PENDING | No | Unacked | KILL (deadline) / 4.01 s | kill_job error; Hard: yes |
| gevent | Exception / late | No | PENDING | Yes | Absent | 1 / 1.09 s | hub error; Hard: no |
| gevent | BaseException / late | No | PENDING | No | Restored | 1 / 1.30 s | hub error; Hard: yes |

Solo's broad catch intercepted `WorkerTerminate` itself and kept running.
That exception is raised in the parent/controller for prefork and threads,
not injected into their task code. Prefork still exited even when its task
caught the earlier `SystemExit`. Gevent's broad catch kept the greenlet alive
past initial cancellation so the hard handler could finally be reached.

## 9. Native code delays and signal coalescing

These tasks call native `hashlib.pbkdf2_hmac` for approximately **2 s**, with
late acknowledgment. They do not execute Python work steps during that call.

### 9.1 Warm while native code runs

**Test (`warm-native`):** TERM at 0.3 s.

| Pool | Variant | Finished? | Backend | ACK sent? | Message after exit | Worker exit | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| solo | late | Yes | SUCCESS | Yes | Absent | 0 / 2.01 s | — |
| prefork | late | Yes | SUCCESS | Yes | Absent | 0 / 4.34 s | — |
| threads | late | Yes | SUCCESS | Yes | Absent | 0 / 2.07 s | — |
| gevent | late | Yes | SUCCESS | Yes | Absent | 0 / 4.12 s | — |

Solo and gevent handled TERM only after native work returned. Prefork's parent
and the threaded worker's controller could handle it while the task was in C.
All four tasks completed and were ACKed.

### 9.2 Cold while native code runs

**Test (`cold-native`):** QUIT at 0.3 s, no soft wait.

| Pool | Variant | Finished? | Backend | ACK sent? | Message after exit | Worker exit | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| solo | late | No | FAILURE | Yes | Absent | 0 / 2.05 s | kill_job error |
| prefork | late | No | PENDING | No | Restored | 0 / 3.42 s | — |
| threads | late | Yes | SUCCESS | Yes | Absent | 1 / 2.07 s | kill_job error |
| gevent | late | No | PENDING | Yes | Absent | 0 / 5.08 s | — |

The elapsed time includes native work and teardown. A cancellation request
is not guaranteed to interrupt an arbitrary C function immediately. In gevent,
the signal was handled in a different execution context from the cooperative
sleep tests, so this case did not produce the same hub error; incomplete work
was still ACKed. Threads again completed despite the cancellation request.

### 9.3 Four Ctrl+C signals while native code runs

**Test (`hard-native`):** SIGINT at 0.3/0.6/0.9/1.2 s, soft timeout 2 s.
The task returns normally if it survives the native call.

| Pool | Variant | Finished? | Backend | ACK sent? | Message after exit | Worker exit | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| solo | late | Yes | SUCCESS | Yes | Absent | 1 / 2.02 s | Hard: no |
| prefork | late | No | PENDING | No | Restored | 0 / 3.39 s | Hard: yes |
| threads | late | Yes | SUCCESS | No | Restored | 1 / 2.10 s | kill_job error; Hard: yes |
| gevent | late | Yes | SUCCESS | Yes | Absent | 1 / 4.13 s | Hard: no |

**Two hazards:**

- Solo/gevent's pending SIGINTs coalesced while Python was not handling them:
  only the warm handler ran after C returned. Sending four signals does not
  guarantee four stage transitions. The hard handler was not reached.
- Threads finished the task and stored `SUCCESS`, but the ACK was not sent and
  Redis restored the message. We fetched that original task ID again. This is
  a concrete **duplicate-execution risk**, not just a theoretical possibility.

## 10. Remote-control shutdown is not an OS signal

**Test (`remote-warm`):** 2 s late-ack task; the external publisher broadcasts
`shutdown` at 0.3 s through Redis. No OS shutdown signal is sent.

| Pool | Variant | Finished? | Backend | ACK sent? | Message after exit | Worker exit | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| solo | late | Yes | SUCCESS | Yes | Absent | 0 / 2.33 s | — |
| prefork | late | Yes | SUCCESS | Yes | Absent | 0 / 4.49 s | — |
| threads | late | Yes | SUCCESS | Yes | Absent | 0 / 2.15 s | — |
| gevent | late | Yes | SUCCESS | Yes | Absent | 0 / 2.15 s | — |

All tasks finished. Solo could not process the control message until the task
returned; the other pools could process control while task execution was active.
Remote shutdown remains cooperative: this test does not establish a way to
force-stop an endless task.

## 11. Idle workers

No task is published. QUIT is sent 0.3 s after worker readiness:

- `idle-cold`: soft shutdown disabled.
- `idle-soft-skipped`: soft timeout 1 s; idle soft wait disabled.
- `idle-soft-enabled`: soft timeout 1 s; `worker_enable_soft_shutdown_on_idle=True`.

| Pool | Variant | Finished? | Backend | ACK sent? | Message after exit | Worker exit | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| solo | No soft wait | — | — | — | — | 0 / 0.46 s | — |
| solo | Skip idle wait | — | — | — | — | 0 / 0.46 s | — |
| solo | Enable idle wait | — | — | — | — | 0 / 1.40 s | — |
| prefork | No soft wait | — | — | — | — | 0 / 1.88 s | — |
| prefork | Skip idle wait | — | — | — | — | 0 / 1.44 s | — |
| prefork | Enable idle wait | — | — | — | — | 0 / 2.91 s | — |
| threads | No soft wait | — | — | — | — | 0 / 0.56 s | — |
| threads | Skip idle wait | — | — | — | — | 0 / 0.37 s | — |
| threads | Enable idle wait | — | — | — | — | 0 / 1.40 s | — |
| gevent | No soft wait | — | — | — | — | 0 / 5.11 s | — |
| gevent | Skip idle wait | — | — | — | — | 0 / 5.10 s | — |
| gevent | Enable idle wait | — | — | — | — | 1 / 1.08 s | hub error |

The first two variants skipped the soft wait. The enabled variant waited for
solo, prefork, and threads. Gevent attempted the wait but raised the same hub
error. These tests have no reserved ETA tasks and say nothing about ETA recovery.

## Why the pools differ

- **Solo:** task execution, the consumer loop, and Python signal handlers share
  the main thread. `wait_for_soft_shutdown()` sleeps in that thread, suspending
  the task's Python progress. Cancellation calls an unimplemented pool method;
  its exception can propagate directly into the interrupted task. With Redis,
  the blocked loop can also delay ACK callbacks.
- **Prefork:** the parent waits while a child executes the task. It can ask the
  child to terminate separately. The initial cancellation uses an exception
  (`SystemExit`) in the child and therefore still has catchability/native-code
  limitations. Parent teardown can subsequently terminate the process.
- **Threads:** the main thread can sleep while the task thread continues.
  There is no supported `terminate_job()` implementation for forcibly canceling
  that thread. Raising `WorkerTerminate` in the main thread does not inject it
  into another thread; joins and interpreter shutdown affect whether exit occurs.
- **Gevent:** tasks are greenlets sharing a thread. Normal monkey-patched sleep
  is cooperative, but Celery's signal handler can run in the **hub callback
  context**, where calling a blocking gevent operation (`sleep` or pool `join`)
  raises `BlockingSwitchOutError`. This explains the observed soft/cold errors.
  Separately, `_make_killable_target()` catches `GreenletExit` and returns
  `(False, None, None)`. The request success callback can then send a late ACK
  even though the task body never finished and no backend success was stored.

Relevant source: `celery/apps/worker.py`, `celery/worker/worker.py`,
`celery/worker/consumer/consumer.py` (`call_soon_ack`),
`celery/worker/request.py`, and `celery/concurrency/{solo,prefork,thread,gevent}.py`.

## Recommendation

Prefer **warm shutdown** and configure a supervisor deadline for stuck workers.
Do not assume a soft timeout is a warm-shutdown deadline, that cold cancellation
works for every pool, or that repeated Ctrl+C is equivalent to SIGKILL.

Prefork gave the strongest tested soft/cold behavior, particularly for late-ack
redelivery. Still verify your production platform, broker, workload, and supervisor.
Avoid relying on broad base-exception catches to manage shutdown. Make externally
visible task effects idempotent: both loss and duplicate execution are possible,
and neither process exit codes nor result-backend states prove delivery safety.

## Reproduce and inspect

```bash
bash .dev/setup-shutdown-env.sh
PYTHONDONTWRITEBYTECODE=1 .dev/.venv/bin/python .dev/verify-pool-shutdown.py

# Selected pools/cases (flags are repeatable):
.dev/.venv/bin/python .dev/verify-pool-shutdown.py \
  --pool prefork --pool threads --pool gevent \
  --case soft-short-late --case soft-long-late --case cold-late
```

The full comparison takes about five minutes. The runner creates its own Redis
server on a private Unix socket, with TCP disabled and no persistence. It never
connects to an existing Redis instance. Each case uses a fresh worker process,
and the runner cleans up its workers, child processes, and Redis instance.
The temporary Redis databases are reset only inside that private server.

- `.dev/setup-shutdown-env.sh`: uv environment, pinned Python dependencies,
  Redis 8.2.3 installed through mise (no global/project activation).
- `.dev/verify-pool-shutdown.py`: sends signals, inspects Redis, checks behavior,
  and writes per-case artifacts; returns nonzero on check failure.
- `.dev/shutdown_probe.py`: task definitions and observation-only hooks. Actual
  workers run through `python -m celery ... --pool ...`; gevent receives Celery's
  normal early monkey-patching. Shutdown implementations are not replaced.
  ACK/reject methods and the hard handler are wrapped only to record calls,
  then delegate to their original implementations without swallowing exceptions.
- `.dev/render-shutdown-report.py`: regenerates this document from a complete
  run's `summary.json`, optionally comparing it with a previous run.

**Results used here:** [pool-shutdown-runs/20261113T081413.844718Z/summary.json](pool-shutdown-runs/20261113T081413.844718Z/summary.json).
Each `<pool>/<case>/` directory contains `config.json`, `events.jsonl`,
`worker.log`, and `result.json`. Signals and events use monotonic timestamps;
all planned and actually delivered signals are retained.

```bash
.dev/.venv/bin/python .dev/render-shutdown-report.py \
  .dev/pool-shutdown-runs/20261113T081413.844718Z/summary.json
```

## Environment and boundaries

- Celery checkout: **5.7.0**, `6fcdbf904d410749ad7676d8ff6b1b25d6e19675`.
- Platform: **macOS-15.8.1-arm64-arm-64bit-Mach-O**; Python **3.13.9**.
- Redis server **8.2.3**, used for both broker and backend in
  separate databases; a Unix socket transport, not TCP.
- Dependencies: billiard **4.3.2**, kombu **5.7.0**, redis **8.1.0**, gevent **26.9.0**, greenlet **3.5.6**.
- Pool concurrency 1. Prefork used Billiard's observed **spawn** start method on
  this macOS system. This validates the **prefork pool**, not Linux fork-specific behavior.
- Default acknowledge-on-failure enabled; `REMAP_SIGTERM` unset. Gossip, mingle,
  and worker heartbeat disabled to isolate task/shutdown behavior. No task retries
  are explicitly requested by task code. Each active-task case publishes one task.
- Not tested: Linux/Windows, RabbitMQ/SQS, Redis TCP/TLS, network failures,
  visibility-timeout recovery after an hour, multiple concurrent tasks, queued
  futures, ETA tasks, different acknowledgment policies, or production side effects.
- No universal shutdown-duration guarantee follows from these timings. Native
  code, signal coalescing, teardown, and application exception handling all matter.

All scripts, logs, and this report remain under locally ignored `.dev/`
(`/.dev/` in `.git/info/exclude`). No production code or shared ignore file was changed.
