# Task time limits, failure ACKs, and worker-loss policies across pools

> Maintained follow-up to [shutdown behavior](shutdown-results.md),
> [ETA/prefetch behavior](eta-prefetch-results.md), and shutdown appendix A8's
> solo `Reject(requeue=True)` workaround. Experiment runs write timestamped raw
> artifacts; **they do not regenerate or overwrite this report or its appendix**.

**Final verification: 47/47 selected cases passed.** PASS means the observed
behavior was reproduced, including identified bugs—not that every configuration
provides the intended protection. No Celery/Kombu/Billiard production code changed.

## Direct answer: these are three different mechanisms

1. **Enforcement:** can the pool interrupt an executing task when a deadline expires?
2. **Disposition:** if a task failure/timeout reaches the request handler, does it
   ACK, discard, requeue the original message, or publish a retry?
3. **Detection:** is there a surviving controller that can recognize a lost executor
   and run that handler?

A shared request-handler setting can work outside prefork without the pool being
able to generate the corresponding timeout or survive an actual process crash.

| Setting | Solo | Prefork | Threads | Gevent |
| --- | --- | --- | --- | --- |
| `task_soft_time_limit` | **Not enforced** | Enforced; catchable `SoftTimeLimitExceeded` | Not enforced | Not enforced |
| `task_acks_on_failure` | **Works**, with late ACK | Works, with late ACK | Works, with late ACK | Works, with late ACK |
| `task_acks_on_timeout` | Shared handler works for an explicitly raised `TimeLimitExceeded`; **no automatic pool timeout** | Controls hard-timeout disposition | No automatic pool timeout; shared handler is source-backed | Controls cooperative hard-timeout disposition; cannot interrupt non-yielding work on schedule |
| `task_reject_on_worker_lost` | **Does not protect against death of the solo process**; synthetic exception can reach shared policy | Requeues on detected child loss, with late ACK | No separate child-process supervisor; see `SystemExit` defect below | No separate child-process supervisor; see `SystemExit` defect below |
| `task_retry_on_worker_lost` | **Cannot publish a retry after the solo process dies**; synthetic exception can reach shared policy | Publishes a bounded/delayed retry on child loss, including with early ACK | No automatic recovery of whole-process death | No automatic recovery of whole-process death |

Here “timeout disposition” specifically means Celery/Billiard
**`TimeLimitExceeded` (hard timeout)**, not `SoftTimeLimitExceeded` or arbitrary
application/library timeout exceptions. An unhandled soft-time-limit exception
uses **`task_acks_on_failure`**, not `task_acks_on_timeout`.

### Version and defaults matter

The checkout identifies as Celery **5.7.0**, revision
`6fcdbf904d410749ad7676d8ff6b1b25d6e19675`. The separate `task_acks_on_failure`,
`task_acks_on_timeout`, and `task_retry_on_worker_lost` settings are documented as
**added in 5.7**. Do not assume they work in an older installed Celery release.

- `task_acks_late` defaults to **False**. Tests explicitly enable it except where
  early ACK is the subject of the case.
- `task_acks_on_failure` and `task_acks_on_timeout` default to `None`, inheriting
  `task_acks_on_failure_or_timeout`, whose default is **True**. The inheritance
  is source-checked, not a separate runtime case in this suite.
- Both worker-loss settings default to **False**.
- Soft and hard time limits default to unset.
- Worker-loss retry uses the task's **`max_retries`** and **`default_retry_delay`**
  attributes. Their normal defaults are 3 and 180 s; tests use 1 and 0.35 s.

## Why this reduced test matrix?

The previous suites already established actual ACK timing, early-ACK deferral in
solo, warm/cold/hard shutdown differences, and restoration of unaccepted messages.
Repeating every signal × pool × flag permutation would obscure the relevant
branches rather than exercise new ones.

| Selected area | Cases | Reason |
| --- | ---: | --- |
| Ordinary failure: all pools × ACK/discard, plus solo early-ACK control | 9 | Establish that failure policy is not prefork-specific and is gated by late ACK |
| Soft limits: all pools; prefork discard/catch/retry; solo synthetic exception | 8 | Separate deadline enforcement from exception handling and identify which ACK flag is consulted |
| Hard limits: all pools; supported-pool requeue; gevent busy work; lost flags; solo synthetic exception | 10 | Necessary to test `acks_on_timeout`, even though the question names only the soft-limit setting |
| Actual prefork child loss | 9 | Default ACK, discard, raw requeue, bounded retry, early ACK, exhausted retries, and loss after stored SUCCESS |
| Actual whole-process loss in solo | 1 | No surviving controller; both worker-loss flags enabled |
| `SystemExit`: all pools, plus spare-prefetch controls for threads/gevent | 6 | Catchable Python process-exit exceptions differ from `os._exit`; expose callback and credit behavior |
| Synthetic `WorkerLostError` in solo | 2 | Prove shared handlers can act without claiming real solo crash recovery |
| QUIT during solo work | 2 | Determine whether failure/timeout/lost flags change the previously measured `kill_job` error |
| **Total** | **47** | |

### Safe extrapolations, explicitly not extra runtime tests

- Ordinary exceptions handled by Celery's normal tracer use the same request
  failure branch. The ACK-versus-discard decision is not specific to the tested
  `ValueError`. Special classes such as `Reject`, `Retry`, `Ignore`,
  `TimeLimitExceeded`, `WorkerLostError`, and `MemoryError` must not be lumped
  into that category.
- The task's origin (immediate vs previously scheduled ETA) does not select a
  different failure-policy implementation once accepted for execution. Waiting,
  unaccepted ETA tasks remain a separate category, covered by the ETA report.
- A whole-process `os._exit`, SIGKILL, or fatal native crash leaves no Python
  controller to publish a worker-loss retry in solo/threads/gevent. Solo is tested
  here; previous process-group SIGKILL cases cover the no-cleanup endpoint across
  pools. Exact Redis recovery timing is not established by that equivalence.
- An unexpected prefork child exit with the parent alive reaches Billiard's lost
  worker reporting path. Here `os._exit(23)` and `SystemExit(23)` are tested;
  deliberately causing an OOM or native segmentation fault is unnecessary to
  establish the policy branch, but parent/container death is **not** equivalent.
- The common `Request.on_failure` classification also exists for threads/gevent;
  synthetic timeout/lost exceptions are only tested in solo. This does not prove
  that every executor failure successfully reaches the shared handler.
- Failed retry publication, `max_retries=None`, custom failure hooks, backend
  outages, rate-limited tasks, higher concurrency, and broker connection loss are
  source-based considerations only. RabbitMQ and eventlet were not runtime tested.

## Environment and measurement

macOS **15.8.1 ARM64**, CPython **3.13.9**, Redis server **8.2.3**, Redis client
**8.1.0**, Kombu **5.7.0**, Billiard **4.3.2**, gevent **26.9.0**, greenlet **3.5.6**.
Prefork used Billiard's **spawn** start method, not Linux fork.

Each case runs a real CLI worker, concurrency **1**, against the runner's private
Redis Unix socket. Broker and backend use separate databases. Prefetch multiplier
is 1 except the two explicit multiplier-2 controls. The Redis visibility timeout
is 3600 s; expiry/recovery at that timeout was **not** waited out.

Unless stated otherwise: late ACK, failure ACK **True**, timeout ACK **True**,
both lost-worker policies **False**, and no task time limits. The runner records
resolved task attributes to verify the configuration actually bound to the task.

- Timed workloads run for about **1.3 s** unless interrupted, normally using
  short sleeps. Soft limit is
  **0.35 s**; hard limit is **0.45 s**, in separate cases. These clocks begin at
  pool acceptance, not necessarily at the first task-body marker.
- For requeue/retry cases, a counter in the private backend makes the second
  execution succeed quickly. This prevents uncontrolled loops without changing
  Celery's `retries` header. Exhaustion cases intentionally crash both executions.
- **ACK/reject** columns refer to completed original transport operations, not
  `Request.acknowledged` alone. **Reject(false)** removes this Redis delivery;
  **Reject(true)** requeues it. Neither is an ACK.
- **Republish** means a new task message was published by the worker, with the
  same task ID and incremented retry header. It is not original-message requeue.
- Backend states are sampled with cache bypassed. Short-lived intermediate
  states may be missed; event timelines independently identify handler paths.
- A follow-up ping task checks continued execution where expected. Workers are
  then stopped with TERM. QUIT cases and naturally exiting cases differ explicitly.
- Observer hooks delegate unchanged to original request and transport methods.
  For threads, an observer reads completed Future exceptions; otherwise the
  `SystemExit` callback error can remain invisible in the worker log.

## 1. Ordinary task failure: policy works in every pool

Raise `ValueError` from the task. `acks_on_timeout` is deliberately set to the
opposite of `acks_on_failure`, confirming which setting controls this path.

| Cases | Pools | `acks_on_failure` | Task body completed? | Backend | ACK sent? | Reject | Retry/recovery | Worker exit |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `failure-<pool>-ack` (4) | All four | True | No | FAILURE | Yes | None | None; delivery absent | 0 |
| `failure-<pool>-discard` (4) | All four | False | No | FAILURE | No | `requeue=False` | **None; delivery absent** | 0 |
| `failure-solo-early` | solo, early ACK | False | No | FAILURE | **Yes** | None | None, even with both lost flags enabled | 0 |

**Disabling failure ACK is not automatic retry.** With late ACK and an ordinary
failure, Celery explicitly sends `reject(requeue=False)` rather than leaving the
delivery unsettled. The Redis transport removes its unacked records without
returning it to the queue: the delivery is discarded, and visibility-timeout
recovery has nothing left to restore. A broker with dead-letter routing may route
a non-requeued rejection elsewhere, but that was not tested here. See appendix A4.

**In every pool, failure ACK policy only applies with late acknowledgment.**
Early ACK (`task_acks_late=False`) is requested on pool acceptance; later failure
cannot undo an ACK. The actual transport operation need not finish before the
task's first instruction. Solo's queued early ACK was flushed only after inline
execution returned, but `Request.acknowledged` was already true. That delay does
not make failure ACK policy applicable to an early-ACK task. See appendix A5.

## 2. Soft time limit: enforced only by prefork among these pools

### Pool enforcement

All four cases configure soft limit 0.35 s and **`acks_on_timeout=False`**, while
keeping `acks_on_failure=True`.

| Case | Body behavior | Timeout callback? | Backend | ACK | Reject / republish | Worker exit |
| --- | --- | --- | --- | --- | --- | --- |
| `soft-solo` | Runs full 1.3 s; no soft exception | No | SUCCESS | Yes | None | 0 |
| `soft-prefork` | Interrupted; unhandled `SoftTimeLimitExceeded` | Soft | FAILURE | **Yes** | None | 0 |
| `soft-threads` | Runs full 1.3 s; no soft exception | No | SUCCESS | Yes | None | 0 |
| `soft-gevent` | Runs full 1.3 s; no soft exception | No | SUCCESS | Yes | None | 0 |

**This is expected behavior, not a defect:** the unhandled soft-limit exception
uses ordinary failure policy, so `acks_on_failure=True` ACKs it.
`acks_on_timeout=False` does not affect that path; it controls hard-timeout
disposition. The soft callback logs the deadline and the child receives a
catchable exception, whose outcome depends on the task's handling. See appendix A6.

### Catching, discarding, and explicitly retrying

| Case | Action / configuration | Task outcome | Backend over run | ACK / reject | Further execution | Worker exit |
| --- | --- | --- | --- | --- | --- | --- |
| `soft-prefork-discard` | Let soft exception escape; failure ACK False, timeout ACK True | First execution interrupted | FAILURE | No ACK; reject(false) | None | 0 |
| `soft-prefork-caught` | Catch soft exception and return normally | Returns early as success; full workload not performed | SUCCESS | ACK | None | 0 |
| `soft-prefork-retry` | Catch soft exception and call `self.retry()` | First interrupted; second succeeds | RETRY → SUCCESS | Both deliveries ACKed | One new message; retries 0 → 1 | 0 |
| `soft-solo-synthetic` | **Manually raise** `SoftTimeLimitExceeded`; failure ACK False | Ordinary traced failure | FAILURE | No ACK; reject(false) | None | 0 |

The synthetic solo row demonstrates exception handling, **not enforcement of a
soft deadline**. Soft exceptions can be caught; a soft limit is not a guaranteed
maximum task runtime. A separately supported hard limit is needed if the task
ignores the soft exception.

## 3. Hard time limit and `task_acks_on_timeout`

### Deadline enforcement and ACK

Each baseline case configures hard limit 0.45 s, failure ACK **False**, and timeout
ACK **True**. The body normally sleeps cooperatively for 1.3 s.

| Case | Body completed? | Hard-timeout callback? | Backend | ACK | Reject / further execution | Worker exit |
| --- | --- | --- | --- | --- | --- | --- |
| `hard-solo` | **Yes; limit ignored** | No | SUCCESS | Yes | None | 0 |
| `hard-prefork` | No | Yes | FAILURE | **Yes** | None | 0 |
| `hard-threads` | **Yes; limit ignored** | No | SUCCESS | Yes | None | 0 |
| `hard-gevent` | No | Yes | FAILURE | **Yes** | None | 0 |

As expected, the hard-timeout path uses `acks_on_timeout=True` and ACKs the
delivery; `acks_on_failure=False` does not control this path. Conversely,
disabling timeout ACK requeues, rather than discarding:

| Case | Special configuration | First body completed? | First delivery disposition | Next execution | Backend over run | Worker exit |
| --- | --- | --- | --- | --- | --- | --- |
| `hard-prefork-requeue` | Timeout ACK False | No | No ACK; reject(true) | Original reexecuted and ACKed; retries stays 0 | FAILURE → SUCCESS | 0 |
| `hard-gevent-requeue` | Timeout ACK False | No | No ACK; reject(true) | Original reexecuted and ACKed; retries stays 0 | FAILURE → SUCCESS | 0 |
| `hard-prefork-lost-flags` | Both lost flags True; timeout ACK True | No | ACK | **None: timeout is not classified as WorkerLostError** | FAILURE | 0 |
| `hard-solo-synthetic` | Manually raise `TimeLimitExceeded`; timeout ACK False | No | No ACK; reject(true) | Original reexecuted and ACKed; retries stays 0 | FAILURE → SUCCESS | 0 |

These requeues do **not** publish a bounded retry. A task that always exceeds its
hard deadline can repeatedly execute with `retries=0`; `max_retries` does not
bound raw requeue. The fixture deliberately succeeds on its second execution.

The synthetic solo row proves the timeout policy can run in solo **if that
exception reaches the handler**. Setting `task_time_limit` does not cause solo
to generate it. An application's generic `TimeoutError` is also not this class.

### Gevent: delayed timeout can duplicate completed work

The two busy cases never yield during the 1.3 s task body, despite a 0.45 s hard
limit. The body **finishes first**. When result processing yields to Redis I/O,
the overdue gevent timeout fires. In these runs Redis had already stored SUCCESS.

| Case | Timeout ACK | Body completion | Backend | ACK / reject | Further execution | Worker exit |
| --- | --- | --- | --- | --- | --- | --- |
| `hard-gevent-busy` | True | Body completed before timeout callback | SUCCESS | ACK | None, but timeout failure hook still fired | 0 |
| `hard-gevent-busy-requeue` | False | **Body completed before timeout callback; both executions completed** | SUCCESS | First reject(true); second ACK | **Original message executed twice**, retries stays 0 | 0 |

This was reproduced in the focused edge run and final full run. The fixture
makes the second invocation short; it proves reexecution after the first body
completed, not repetition of the entire 1.3 s busy loop on the second invocation.
Do not translate
“gevent cannot interrupt blocking work” into “the timeout is always silently
ignored.” It may arrive late, during result handling. Exactly which backend
operations complete before that interruption is timing-dependent; the measured
SUCCESS-plus-requeue outcome is not a universal result-backend guarantee.

Redis's backend implementation preserves an already stored SUCCESS when later
failure storage is attempted. This explains why the timeout failure hook/log and
backend SUCCESS can coexist. Backend SUCCESS alone is not proof that the broker
message was ACKed or that the task body ran only once.

## 4. Actual prefork child loss: reject vs retry

The task calls **`os._exit(23)` in the child**, leaving the parent worker alive.
Its task `finally` and normal trace failure handling cannot run. Billiard reports
`WorkerLostError` to the parent. Recovery cases let the next execution succeed.

| Case | Relevant configuration | First disposition | Further execution / retry header | Backend over run | Completed executions / started | Worker exit |
| --- | --- | --- | --- | --- | --- | --- |
| `lost-prefork-ack` | Defaults: lost flags False, failure ACK True | ACK | None | FAILURE | 0 / 1 | 0 |
| `lost-prefork-discard` | Failure ACK False; lost flags False | Reject(false), no ACK | None | FAILURE | 0 / 1 | 0 |
| `lost-prefork-requeue` | Reject-on-lost True; `max_retries=0` | Reject(true), no ACK | Original redelivered; retries **0 → 0**; next ACK | PENDING → SUCCESS | 1 / 2 | 0 |
| `lost-prefork-retry` | Both lost flags True | Publish new message, then ACK original | Retry wins over reject; retries **0 → 1**; next ACK | RETRY → SUCCESS | 1 / 2 | 0 |
| `lost-prefork-early-retry` | Early ACK; retry-on-lost True | Original already ACKed; publish retry | Retries **0 → 1**; next delivery also early-ACKed | RETRY → SUCCESS | 1 / 2 | 0 |
| `lost-prefork-early-reject` | Early ACK; reject-on-lost True | Original already ACKed | **No requeue** | FAILURE | 0 / 1 | 0 |
| `lost-prefork-exhausted-ack` | Both flags True; max retries 1; both executions crash; failure ACK True | Publish retry + ACK original | Second delivery ACKed; **no raw requeue after cap** | RETRY → FAILURE | 0 / 2 | 0 |
| `lost-prefork-exhausted-discard` | Same, failure ACK False | Publish retry + ACK original | Second delivery reject(false); **no raw requeue after cap** | RETRY → FAILURE | 0 / 2 | 0 |
| `lost-prefork-after-success` | Both flags True; exit from `task_postrun` **after SUCCESS stored** | Parent detects stored success and ACKs | No repeat execution or retry publication | SUCCESS | 1 / 1 | 0 |

All delivered recovery copies were actually executed. All messages for the tested
work were absent from the queue and unacked hash after these cases finished.
The parent remained usable and executed the follow-up ping.

### Important distinctions

- **Reject-on-lost requires late ACK. Retry-on-lost does not.** The latter can
  publish a new message even when the original delivery has already been ACKed.
- A retry has the **same task ID**, but is a newly published message with increased
  `retries` and an ETA derived from `default_retry_delay` (0.35 s here).
- Raw requeue preserves the retry header. The requeue case works even with
  `max_retries=0`, demonstrating that this is not a bounded retry mechanism.
- Exhausted worker-loss retries suppress fallback raw requeue even when
  `reject_on_worker_lost=True`; failure ACK then selects ACK vs non-requeued reject.
- The success-before-loss guard depends on being able to retrieve stored SUCCESS.
  Source explicitly skips that lookup for ignored results and the RPC backend;
  backend-unavailable behavior was not tested here.
- Source: if retry publication fails, handling falls through to ordinary lost
  policy. **That is not an unconditional requeue:** reject-on-lost and late ACK
  still matter. Publication failure was not injected in these experiments.
- Publishing a retry and acknowledging its predecessor are separate operations.
  This is not an exactly-once transaction or protection against all controller
  crashes between those steps.

## 5. Solo: a dead worker cannot handle its own loss

### Actual abrupt process exit

`lost-solo-whole-process` enables **both** lost flags and late ACK, then calls
`os._exit(23)` from the task.

| Body completed? | Backend | ACK | Reject / retry publication | Broker after exit | Worker exit |
| --- | --- | --- | --- | --- | --- |
| No | PENDING | None | None | **One unacked delivery; not queued** | 23 |

The task, pool, and controller share the process. There is nobody left to run
`Request.on_failure` or publish a retry. Recovery is a **broker/visibility-timeout
mechanism**, not either lost-worker setting. We did not wait for Redis's 3600 s
visibility timeout, and did not claim immediate restoration or bounded attempts.

### Synthetic exceptions are not process-loss recovery

These tasks explicitly raise `WorkerLostError` from normal Python task code; the
solo process remains alive. They deliberately test shared exception classification.

| Case | Flag | Task-body completions / starts | ACK / reject | Further execution | Backend over run | Worker exit |
| --- | --- | --- | --- | --- | --- | --- |
| `lost-solo-synthetic-requeue` | Reject-on-lost True | 1 / 2 | First reject(true); second ACK | Original redelivered; retries 0 → 0 | FAILURE → SUCCESS | 0 |
| `lost-solo-synthetic-retry` | Retry-on-lost True | 1 / 2 | Both deliveries ACKed | New message; retries 0 → 1 | RETRY → SUCCESS | 0 |

So “these flags are completely ignored outside prefork” is too strong. Their
shared handler can run in solo. **That does not make solo self-supervising.**
Do not manufacture `WorkerLostError` as an application retry API; use explicit
`self.retry()`, targeted autoretry, or `Reject(requeue=True)` for their intended
semantics.

### QUIT's `kill_job` error is not a timeout or lost-worker event

Both cases enable both lost flags and disable timeout ACK. QUIT arrives about
0.2 s into solo work; soft shutdown waiting is disabled.

| Case | Failure ACK | Body completed? | Backend | ACK | Reject | Retry / restored message | Worker exit |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `quit-solo-ack` | True | No | FAILURE | Yes | None | None | 0 |
| `quit-solo-discard` | False | No | FAILURE | No | **requeue=False** | **None** | 0 |

Both reach `Request.on_failure(NotImplementedError)`; neither reaches a hard
`on_timeout` callback or worker-loss retry path. Enabling lost flags, or disabling
timeout ACK, does **not** fix the previously observed solo QUIT recovery problem.
Disabling failure ACK changes ACK to discard, not to restoration.

The separately tested, version-specific task-level `Reject(requeue=True)`
workaround remains in [shutdown appendix A8](shutdown-results.md). Its guarded
exception handling, late-ACK requirement, and non-guarantees still apply.

## 6. `SystemExit` is a distinct failure path—and exposes a defect

`SystemExit(23)` is a Python exception, unlike immediate `os._exit(23)`. Test it
explicitly rather than assuming it behaves identically across pools.

| Case | Configuration | Backend | ACK / reject | Retry or recovery | Subsequent ping | Worker exit |
| --- | --- | --- | --- | --- | --- | --- |
| `system-exit-prefork` | Retry-on-lost True | RETRY → SUCCESS | Both deliveries ACKed | Parent publishes retry; second task completes | Executed | 0 |
| `system-exit-solo` | Both lost flags True | PENDING | None | **No retry**; cleanup restores original; fetched after exit | Worker already exited | **1** |
| `system-exit-threads` | Both flags True, prefetch 1 | PENDING | None | No retry; unacked until TERM cleanup restores it | **Not delivered: credit exhausted** | 0 after TERM |
| `system-exit-gevent` | Both flags True, prefetch 1 | PENDING | None | No retry; unacked until TERM cleanup restores it | **Not delivered: credit exhausted** | 0 after TERM |
| `system-exit-threads-credit` | Both flags True, prefetch **2** | PENDING | None | Same stranded delivery; restored on TERM | **Executed** | 0 after TERM |
| `system-exit-gevent-credit` | Both flags True, prefetch **2** | PENDING | None | Same stranded delivery; restored on TERM | **Executed** | 0 after TERM |

Outside prefork, `celery.concurrency.base.apply_target()` catches this
`BaseException`, constructs `WorkerLostError`, and calls the success callback
with a bare `ExceptionInfo`. The generated request `on_success()` instead expects
`(failed, retval, runtime)`. The resulting error is:

```text
TypeError: cannot unpack non-iterable ExceptionInfo object
```

- Solo logs the error and exits through cleanup, restoring the unacked message.
- Gevent logs the greenlet failure; the controller survives.
- Threads stores the callback error in the Future; the observer confirmed the
  same TypeError, which need not appear as an ordinary task failure in worker.log.
- **`Request.on_failure` was never reached in these non-prefork cases**, so neither
  lost flag ran. The prefetch-2 controls prove that threads/gevent can still
  execute another task; the prefetch-1 stall is outstanding delivery credit, not
  proof that every execution slot has died.

This is a defect in the tested checkout's callback path, not a supported recovery
strategy. Increasing prefetch does not repair the stranded delivery. Do not use
`sys.exit()` to request a safe task retry or to work around solo cancellation.

## Practical guidance, especially for solo

- **Failure disposition works in solo.** With late ACK, choose deliberately between
  ACK on failure and reject-without-requeue. Neither automatically retries an
  ordinary failed task.
- **Soft and hard task limits do not enforce runtime bounds in solo.** Use explicit
  I/O deadlines and cooperative checks where appropriate; use a separately
  supervised process/prefork if execution must be forcibly interrupted.
- **Lost-worker flags are not solo crash insurance.** Late ACK leaves a possible
  broker recovery path after abrupt process death, but timing, duplicates, and
  attempt limits require separate design. The worker must also be restarted by
  an external supervisor.
- For recoverable application errors, use `self.retry()` or narrowly scoped
  autoretry. Avoid broad retry rules that turn programming errors or unexpected
  shutdown exceptions into unbounded repeated side effects.
- Prefer TERM for normal solo shutdown. None of the five settings fixes the
  unsupported active-task QUIT cancellation path on its own.
- **Soft time limit is not soft shutdown.** `task_soft_time_limit` is a per-task
  execution timer; `worker_soft_shutdown_timeout` is a shutdown grace period.
  The earlier shutdown report's solo main-thread sleep findings still apply.
- Keep tasks idempotent. Raw requeue does not increment `retries`, backend SUCCESS
  is not a delivery receipt, and gevent's late-timeout case demonstrates duplicate
  body execution rather than merely a theoretical risk.

## Reproduce and inspect

```bash
# Existing uv environment and mise-managed Redis, no new dependencies:
bash .dev/setup-shutdown-env.sh
PYTHONDONTWRITEBYTECODE=1 .dev/.venv/bin/python .dev/verify-task-policies.py

# Solo-focused subset:
.dev/.venv/bin/python .dev/verify-task-policies.py \
  --case soft-solo --case hard-solo \
  --case failure-solo-discard --case lost-solo-whole-process \
  --case quit-solo-discard --case system-exit-solo
```

- Worker module: `.dev/policy_probe.py`.
- Runner: `.dev/verify-task-policies.py`.
- Final [47-case summary](task-policy-runs/20261114T094306.436639Z/summary.json).
- Focused [gevent/SystemExit confirmation](task-policy-runs/20261114T093326.106995Z/summary.json).
- Per-case `config.json`, `events.jsonl`, `worker.log`, and `result.json` contain
  resolved settings, ACK/reject events, retry publications, task attempts,
  backend history, broker snapshots, worker exit, and assertions.
- Console: `.dev/task-policy-validation-final.log`.

Exploratory/intermediate runs are retained. The initial expectation that gevent
busy work would produce no timeout callback was corrected after observing the
callback **after body completion**; the requeue variant was then added. A 4 s
prefork liveness deadline was too short when child replacement waited for
Billiard's delayed SIGKILL fallback; it is now 10 s. An intermediate run also
stopped on macOS process-group cleanup `EPERM` after worker exit; cleanup now
checks live, owned group members and still propagates errors for live processes.
The final run required no safety-deadline kills.

## Appendix: source-level classification and scope

### A1. Failure policy is not a scheduler or timeout implementation

The principal paths in this checkout are:

```text
Normal task exception
  → trace records failure
  → Request.on_failure(..., return_ok=True)
  → with late ACK: acks_on_failure ? ACK : reject(requeue=False)

Prefork soft deadline
  → soft timeout notification + signal to child
  → child raises SoftTimeLimitExceeded
  → catch/return, explicit retry, or ordinary task failure above

Supported hard deadline
  → pool timeout machinery / Request.on_timeout(soft=False)
  → TimeLimitExceeded failure handling
  → with late ACK: acks_on_timeout ? ACK : reject(requeue=True)

Detected WorkerLostError
  → if stored SUCCESS is found: preserve success, ACK late delivery, no retry
  → else retry_on_worker_lost and budget available:
       publish retry, record RETRY, ACK late original
  → otherwise late-ACK disposition:
       reject_on_worker_lost and retry budget not exhausted → requeue original
       else acks_on_failure ? ACK : reject(requeue=False)
```

Prefork's hard-timeout machinery can invoke both the error callback with
`TimeLimitExceeded` and the hard-timeout callback. Their disposal policy agrees;
`Request.acknowledged` prevents sending a second ACK/reject for that delivery.
This does not imply every associated failure hook fires exactly once.

Special task predicates and deliberate revocation/cancellation use other branches.
Do not extrapolate the default failure table to every subclass of `BaseException`.

### A2. Relevant source

- `celery/app/task.py`: setting binding/fallback, task retry defaults.
- `celery/app/trace.py`: distinction between ordinary exceptions, task predicates,
  and escaping `BaseException`; result storage precedes some postrun cleanup.
- `celery/worker/request.py`: `on_timeout`, `on_failure`, `_retry_worker_lost`,
  `_success_was_stored`, generated `on_success`, and ACK/reject guards.
- `celery/concurrency/solo.py` and `celery/concurrency/base.py`: inline execution,
  ignored timer options, and the observed BaseException callback mismatch.
- `celery/concurrency/thread.py`: executor submission; time-limit options unused.
- `celery/concurrency/gevent.py`: cooperative `Timeout` context; no soft timer.
- `celery/concurrency/asynpool.py` and installed Billiard `pool.py`: timeout timers,
  soft signal delivery, child termination, and lost-child reporting.
- `celery/backends/base.py`: SUCCESS-preserving result storage.
- `docs/userguide/configuration.rst`: setting versions, defaults, and intended semantics.

### A3. What these experiments do not guarantee

No Linux fork, RabbitMQ, eventlet, true OOM, native segfault, broker restart,
visibility-timeout expiry, multi-worker redistribution, or publish-failure
transaction was tested. An empty queue after discard does not mean work succeeded;
a restored message does not undo side effects; and a pass does not turn an
unsupported pool feature into a supported one.

The report and appendix are maintained text. The runner only writes new raw-run
directories; future executions cannot overwrite these explanations.

### A4. No ACK operation does not necessarily mean an unsettled delivery

ACK and rejection are different operations, but both can **settle** a delivery.
For the tested Redis transport, once the operation takes effect:

| Action | Removed from Redis's unacked records? | Returned to queue? | Visibility-timeout recovery of this delivery? |
| --- | --- | --- | --- |
| ACK | Yes | No | No |
| Reject, `requeue=False` | Yes | **No: discarded** | No |
| Reject, `requeue=True` | Yes | Yes | Not needed for this settled delivery; a subsequent delivery has its own lifecycle |
| Delivery left unsettled: neither ACK nor reject takes effect | No | Not yet | Eligible for recovery after the timeout |

Kombu's Redis `QoS.reject(requeue=False)` calls `_remove_from_indices()`, deleting
both the unacked hash entry and the delivery's timestamp-index entry. It does
not return the message to the queue. Consequently, disabling failure ACK for an
ordinary late-ACK task does **not** preserve that delivery for timeout recovery:
the explicit rejection removes the recovery records.

**No ACK operation observed ≠ delivery remains unacknowledged at the broker.**
This is why the results separately report ACK, reject, and broker state.

Visibility-timeout recovery applies when the delivery remains unsettled—for
example, after a worker dies before either operation takes effect. Kombu's Redis
transport recovery scan can then restore it after the timeout; this is not a
Redis-native scheduled task and need not happen precisely at the deadline.
Restoration also does not increment Celery's task retry counter. These experiments
inspected the unacked records but did not wait for their 3600 s timeout.

Relevant installed source: `kombu/transport/redis.py`, `QoS.reject`,
`_remove_from_indices`, and `restore_visible`.

### A5. Early-ACK irreversibility is common to all pools

The configuration is **`task_acks_late=False`**; there is no
`task_acks_early=True` setting. Early ACK is requested when the pool accepts a task
for execution, not merely when the consumer fetches or schedules its message.

Two distinct facts must be kept separate:

1. **Policy:** failure ACK settings apply only with late acknowledgment. An ACK
   that has taken effect cannot be undone by a subsequent task failure.
2. **Timing:** requesting an early ACK does not guarantee that the actual broker
   operation has completed before the first task-body instruction.

Solo makes the timing distinction particularly visible: its blocked event loop
can leave the transport ACK queued throughout inline execution. Nevertheless,
`Request.acknowledged` is already true after the request-level ACK is scheduled,
and `Request.reject()` refuses another disposition for that request. This is not
an exception to the common early-ACK policy.

If the process dies before the queued ACK takes effect, the broker delivery can
still remain unsettled and be recoverable. That is failure to complete the ACK,
not Celery reversing an ACK or applying `acks_on_failure=False` to early-ACK work.

### A6. Soft-limit failure versus hard-timeout disposition

For this checkout, the classification is:

```text
Unhandled SoftTimeLimitExceeded → task_acks_on_failure
Hard TimeLimitExceeded          → task_acks_on_timeout
```

Therefore the prefork soft-limit case with failure ACK enabled and timeout ACK
disabled is ACKed **as expected**. The two settings are not competing for control
of the same outcome. The original wording “despite timeout ACK being disabled”
was intended to distinguish the paths, not identify a defect.

A soft deadline raises a catchable exception in the prefork child. If the task:

- Lets it escape, ordinary failure policy applies.
- Catches it and returns normally, success handling applies.
- Catches it and calls `self.retry()`, explicit retry handling applies.

Hard-timeout handling uses `TimeLimitExceeded`. An application's generic
`TimeoutError` is not automatically classified as that exception. Conversely,
manually raising Celery/Billiard's `TimeLimitExceeded` can reach timeout-disposition
policy without proving that the pool enforces any deadline, as the solo synthetic
case demonstrates.

These notes clarify the existing runtime results and source paths; they do not
add new experiments or claim visibility-timeout recovery was runtime tested.
