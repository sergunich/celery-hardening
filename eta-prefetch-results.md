# ETA/countdown tasks: concurrency, prefetch, ACKs, and shutdown

> Maintained follow-up to [the shutdown comparison](shutdown-results.md).
> Experiment scripts write timestamped raw artifacts, **not this report**.
> Conclusions are scoped to the tested checkout and dependencies.

## Direct answers

1. **Waiting ETA/countdown tasks do not occupy execution-pool concurrency slots.**
   They occupy worker memory and timer entries and remain broker-unacknowledged.
   This was measured in **all four pools**: with concurrency 1, five waiting ETA
   tasks did not prevent an immediate task from executing.
2. **They are not ACKed merely because the worker fetched them**, even with
   `task_acks_late=False`. Early ACK is requested on pool acceptance for execution;
   late ACK normally follows execution. Solo's async Redis consumer can defer
   even the requested early ACK until the task returns.
3. **Ordinary prefetch is not an ETA-memory bound.** Each received ETA message
   increases the QoS allowance so the consumer can fetch more work despite
   holding that message unacked. `worker_prefetch_multiplier=1` did not stop the
   worker from fetching all five future tasks.
4. **`worker_eta_task_limit` is the intended bound** (introduced in Celery 5.6;
   default `None`, no configured ETA limit). It operates through QoS credit,
   rather than a separate execution-slot or timer-count gate. Saturating it can
   block immediate-task delivery even while the execution pool is idle.
5. **There are implementation caveats in this environment:** multiplier 0 bypassed
   the ETA cap; an initial prefetch above the cap was not clamped; and draining a
   saturated cap reduced effective prefetch below its configured starting value.
   Do not treat the setting as an unconditional hard memory limit.
6. **Waiting tasks and running tasks have different shutdown outcomes.** Waiting
   ETA messages were restored without ACK in the tested graceful/cold paths,
   even when a simultaneously running task failed or was incorrectly ACKed.
   SIGKILL left the fetched ETA messages in Redis's unacked hash instead.

**Final verification: 31/31 selected cases passed.** A pass means the recorded
behavior was reproduced, including identified limitations—not that every
combination works correctly.

## Why these tests, rather than every pool × signal × ACK combination?

The previous 96-case suite established the running-task cancellation differences.
This follow-up concentrates on the new boundary: **scheduled but not executing**.

| Area | Explicit cases | Reason |
| --- | ---: | --- |
| Store future tasks and run immediate work | 4: all pools | Verify that timer storage does not occupy execution slots; warm restoration across pools |
| ETA reaches due time, early vs late ACK | 8: all pools × 2 policies | Actual ACK timing differs for solo and controller/greenlet execution |
| QoS/ETA-limit interactions | 7: prefork | Shared consumer/Kombu code; isolate normal pool behavior from known solo/gevent shutdown defects |
| Ordinary prefetched tasks, early vs late ACK | 2: prefork | Establish fetched-versus-accepted-versus-ACKed behavior and pending-work restoration |
| ETA-only shutdown | 7 selected cases | Test no-active-task branch, idle soft wait, gevent handler error, and abrupt kill |
| Running task plus future tasks, cold shutdown | 3: solo, prefork, gevent | Different active-task outcomes must not be mistaken for waiting-message outcomes |

### What can be extrapolated, and what cannot?

- **Source-backed, not additional runtime coverage:** positive-QoS ETA accounting
  is implemented before pool execution in the shared strategy/consumer. The
  same bookkeeping should apply to the other pools with this Redis/Kombu setup.
  Exact admission timing can differ if the pool or event loop blocks.
- **Waiting early/late tasks:** neither policy ACKs an unaccepted scheduled task.
  The shared path and all-pool due-time tests support extrapolating the waiting
  phase without repeating every shutdown for both policies.
- **After pool acceptance:** normal pool behavior applies. The previous suite
  covers cancellation of active tasks; ETA origin does not add a distinct
  cancellation implementation. Timer/shutdown boundary races are not exhaustively
  covered by that observation.
- **Mixed thread-pool cold shutdown was not repeated:** the previous suite showed
  controller cancellation errors while task threads continue; the new threads
  storage test confirmed waiting-message warm restoration. The exact mixed-cold
  combination remains extrapolated, not a measured row.
- **Hard escalation with ETA-only workers was not multiplied across pools:**
  pending-message recovery depends on whether broker cleanup runs. The existing
  hard tests demonstrate that cleanup and handler reachability vary; the new
  SIGKILL test measures the no-cleanup endpoint. Do not infer guaranteed immediate
  redelivery after every hard-shutdown sequence.
- **Not safely extrapolated:** RabbitMQ native delayed delivery, AMQP QoS modes,
  broker acknowledgment timeouts, Redis visibility-timeout redelivery, connection
  loss, multi-worker redistribution, or the exact outcome of an ETA becoming due
  at a shutdown race. These need their own targeted tests if used in production.

## Test setup and column definitions

Same checkout as the earlier comparison: Celery **5.7.0**, commit
`6fcdbf904d410749ad7676d8ff6b1b25d6e19675`; CPython **3.13.9**, macOS ARM64,
Kombu **5.7.0**, billiard **4.3.2**, gevent **26.9.0**, Redis server **8.2.3**.
Prefork uses Billiard's **spawn** start method here. Concurrency is **1** throughout.

Each case starts a real Celery CLI worker against the runner's private Redis
Unix-socket server (separate broker/result databases). Defaults are prefetch
multiplier 1, no ETA cap, early ACK, and acknowledgment on failure enabled unless
a row says otherwise. Waiting-task batches alternate `countdown=60` and an
explicit timezone-aware `eta=now+60s`, exercising both publication forms.

- **Fetched:** a task-received event was observed.
- **Scheduled:** present in the worker's scheduled-request set/timer bookkeeping.
- **ACK sent:** the actual original `Message.ack()` completed, not merely a
  `Request.acknowledged=True` flag.
- **Restored:** fetched earlier and queued again after exit. Every queued original
  task ID was fetched by the harness to verify availability.
- **Still queued:** not fetched by the worker; it never needed restoration.
- **Unacked:** present in Redis's unacked hash after exit, not yet back in the queue.
  The configured visibility timeout is 3600 s; recovery after that interval was
  not waited out.

Scheduler snapshots are periodic and stop advancing while solo occupies its
main thread. Running-task markers and broker snapshots are independently
observed by the external runner; a stale solo snapshot is not proof of idleness.

## 1. Storage is not execution: all four pools

**Cases `store-<pool>`:** publish five tasks due in 60 s, observe them waiting,
then publish an immediate 0.2 s task. Finally send TERM while future tasks remain.

| Pool | Future fetched / scheduled | Future active | Future ACKs while waiting | Effective prefetch after fetch | Immediate task finished and ACKed? | Future messages after warm exit |
| --- | --- | ---: | ---: | ---: | --- | --- |
| solo | 5 / 5 | 0 | 0 | 6 | Yes | 5 restored; no ACK |
| prefork | 5 / 5 | 0 | 0 | 6 | Yes | 5 restored; no ACK |
| threads | 5 / 5 | 0 | 0 | 6 | Yes | 5 restored; no ACK |
| gevent | 5 / 5 | 0 | 0 | 6 | Yes | 5 restored; no ACK |

All future tasks remained `PENDING`. All workers exited with code 0.
An ETA is an **earliest execution time**, not a reserved execution slot or a
promise to start exactly on time. Once due, the request must still compete for
pool capacity and obey any task rate limit.

## 2. ACK timing when an ETA becomes executable

**Cases `due-<pool>-early/late`:** one task with countdown 1 s and task duration
1.2 s. Sample before ETA, about 0.25 s into execution, and after completion.
Then terminate the worker warmly.

| Pool | ACK policy | ACK while waiting? | ACK requested on acceptance? | Actual ACK during execution? | ACK after completion? | Final result / broker |
| --- | --- | --- | --- | --- | --- | --- |
| solo | Early | No | Yes | **No** | Yes | SUCCESS / absent |
| solo | Late | No | No | No | Yes | SUCCESS / absent |
| prefork | Early | No | Yes | Yes | Yes | SUCCESS / absent |
| prefork | Late | No | No | No | Yes | SUCCESS / absent |
| threads | Early | No | Yes | Yes | Yes | SUCCESS / absent |
| threads | Late | No | No | No | Yes | SUCCESS / absent |
| gevent | Early | No | Yes | Yes | Yes | SUCCESS / absent |
| gevent | Late | No | No | No | Yes | SUCCESS / absent |

“ACK after completion: Yes” means it had been sent by then, not that a second
ACK was sent. All waiting samples showed the message in Redis's unacked hash.
All tasks started no earlier than their countdown and completed successfully.

Solo's early ACK was queued on the same async hub blocked by inline task
execution. The `Request` flag was already true on acceptance, but the Redis
operation had not yet happened. This reproduces the earlier immediate-task
finding for ETA tasks too.

## 3. Ordinary prefetch: fetching is not acknowledging

**Cases `prefetch-early/late`:** prefork, concurrency 1, multiplier 2. Publish
three immediate 2 s tasks; sample while the first runs, then send TERM.

| Policy | Fetched at sample | Running | Fetched but not accepted | Broker-unacked | ACKs sent at sample | Outcome after warm shutdown |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Early | 3 | 1 | 2 | 2 | 1 | Running task SUCCESS; 2 pending tasks restored without ACK |
| Late | 2 | 1 | 1 | 2 | 0 | Running task SUCCESS and ACK; 1 pending task restored; 1 never fetched |

The broker prefetch allowance limits **outstanding unacknowledged deliveries**,
not all tasks the worker has ever fetched. Early-ACKing the accepted task frees
credit for another delivery. Prefetched tasks that have not been accepted by
the pool are not early-ACKed just because they entered the worker.

## 4. ETA limits and prefetch interact

These cases use prefork to isolate the shared consumer/QoS behavior. All scheduled
future messages shown below had **zero ACKs** at the observation point.

| Case | Initial prefetch | ETA limit | Workload at observation | Observed admission / credit | Can immediate work proceed before ETA? |
| --- | ---: | ---: | --- | --- | --- |
| `cap-three` | 1 | 3 | 5 future messages, then immediate task | 3 future fetched, 2 still queued; QoS 3 | **No:** immediate message also stays queued although pool is idle |
| `cap-with-zero-prefetch` | 0 (unlimited) | 3 | Same workload | All 5 future fetched; QoS remains 0 | Yes; configured ETA cap was bypassed |
| `disable-prefetch` | 1 | None | Same workload; `worker_disable_prefetch=True` | All 5 future fetched; QoS grows to 6 | Yes; disabling normal prefetch is not an ETA-memory cap |
| `cap-with-active` | 2 | 3 | 1 active late-ack task, then 5 future messages | Active delivery + 2 future deliveries = 3 unacked; remaining 3 future stay queued | Execution slot is occupied; total QoS cap is also full |
| `active-blocks-eta-credit` | 1 | 3 | Same workload | Active delivery exhausts initial credit; **0 future fetched** | No spare initial delivery credit; ETA compensation cannot run before receipt |
| `cap-release` | 2 | 3 | 3 tasks due in 2 s, then immediate task | 3 future fetched; immediate waits; all 4 eventually finish and ACK | Yes, after ETA work releases credit; final QoS is **1**, not configured 2 |
| `initial-prefetch-above-cap` | 8 | 3 | 6 immediate late-ack tasks, no ETA | All 6 fetched/unacked; QoS stays **8** | The claimed overall cap of 3 did not clamp the initial window |

For the holding cases, warm shutdown restored fetched future messages and left
unfetched ones queued. The running task in mixed-cap cases finished and ACKed.
In the initial-prefetch-above-cap case, the running task finished/ACKed and five
unaccepted tasks were restored without ACK.

### What the limit implementation actually does

`worker_eta_task_limit` is passed to Kombu `QoS(max_prefetch=...)`. In this
installed Kombu version:

1. Initial QoS is the ordinary concurrency × multiplier value; the constructor
   does not clamp it to `max_prefetch`.
2. Receiving an ETA calls `increment_eventually()`. If current QoS is nonzero,
   this increment is capped at `max_prefetch`.
3. A current QoS of zero means unlimited; the increment is skipped, so a positive
   ETA limit does not make multiplier-zero delivery bounded.
4. Due-time dispatch calls `decrement_eventually()`. Increment saturation does
   not remember the increments it discarded; subsequent decrements can therefore
   lower QoS below the original baseline (measured as 2 → 3 → 1).

These are **implementation/documentation discrepancies in this checkout and
Kombu version**, not recommended configuration tricks. The documentation's
claim of an unconditional overall unacked-message cap is too strong here.

With a positive starting prefetch no greater than the cap, the tested cap works
as an **overall outstanding-delivery ceiling**, shared by scheduled ETA tasks,
active late-ack work, and other unacked reservations. It is not an independent
quota of “N ETA tasks plus ordinary prefetch.”

## 5. Shutdown while all work is still scheduled

Default future count is 3, due in 60 s, with early ACK configured. These workers
have **no executing task** when the shutdown signal is sent.

| Case | Pool / signal | Soft setting | Future tasks started? | Future ACKs? | Future messages after exit | Worker outcome |
| --- | --- | --- | --- | --- | --- | --- |
| `cold-future-solo` | solo / QUIT | Disabled | No | 0 | 3 restored | Exit 0; no unsupported active-task cancellation |
| `cold-future-prefork` | prefork / QUIT | Disabled | No | 0 | 3 restored | Exit 0 |
| `soft-future-skipped` | prefork / QUIT | 1 s, idle wait off | No | 0 | 3 restored | Exit 0; **soft wait skipped** |
| `soft-future-enabled` | prefork / QUIT | 1 s, idle wait on | No | 0 | 3 restored | Exit 0; soft wait elapsed |
| `soft-future-gevent` | gevent / QUIT | 1 s, idle wait on | No | 0 | 3 restored | Exit 1; `BlockingSwitchOutError` |
| `soft-idle-eta-becomes-due` | prefork / QUIT | 2 s, idle wait on; **one ETA due in 1.2 s** | **No** | 0 | 1 restored | Exit 0; due time crossed during shutdown, but work did not execute |
| `kill-future` | prefork / process-group SIGKILL | Irrelevant | No | 0 | 3 still unacked; not queued | Exit -9; no cleanup |

The storage cases in section 1 additionally tested warm restoration in all four
pools. “ETA tasks held” does **not** mean “active tasks” for soft-shutdown selection:
without `worker_enable_soft_shutdown_on_idle`, the soft wait is skipped.

Enabling an idle grace period does not promise to execute scheduled work during
that window. In the tested prefork due-during-shutdown case the ETA arrived but
its task never started. The handler waits in the controller thread; timer dispatch
and subsequent shutdown cleanup are not a normal task-processing loop.

Restoration succeeded here even with no soft idle wait. This does not prove the
setting is unnecessary for every broker/failure scenario. Likewise, gevent's
successful restoration in this run does not make its handler error harmless.

## 6. Running work and waiting work must be distinguished

**Cases `cold-mixed-<pool>`:** first fetch three future messages, then start a
3 s immediate task. All tasks use late ACK. Send QUIT while that task is running;
soft shutdown is disabled.

| Pool | Message group | Finished? | Result | ACK sent? | Broker after exit | Worker error |
| --- | --- | --- | --- | --- | --- | --- |
| solo | 3 waiting ETA tasks | Never started | PENDING | **No** | All restored | — |
| solo | Running task | No | FAILURE | **Yes** | Absent | Unsupported `kill_job` |
| prefork | 3 waiting ETA tasks | Never started | PENDING | **No** | All restored | — |
| prefork | Running task | No | PENDING | **No** | Restored | — |
| gevent | 3 waiting ETA tasks | Never started | PENDING | **No** | All restored | — |
| gevent | Running task | No | PENDING | **Yes** | Absent | `BlockingSwitchOutError` |

The previous active-task findings reproduced. Crucially, the solo/gevent ACK
problem did **not** apply to the unaccepted waiting messages in these cases.
Do not describe every message on a shutting-down worker as “the canceled task.”

## Source-level lifecycle

For the tested worker-managed scheduling path:

```text
Publish countdown or ETA
  → broker delivery (subject to available delivery credit)
  → construct Request; task_received signal
  → ETA branch: increase QoS allowance, register timer, mark scheduled
       [no pool acceptance, no execution slot, no task ACK]
  → ETA arrives: mark reserved, dispatch to pool, reduce ETA QoS compensation
  → pool accepts task:
       early policy → request ACK
       late policy  → keep delivery unacked
  → execute task
  → normal success/failure/retry handling and applicable ACK/reject policy
```

Countdown and explicit ETA reach the same `Request.eta` scheduling branch.
Before any ETA becomes due and without a cap, a positive initial window B grows
approximately to **B + number of scheduled ETA messages**. This compensates for
messages that remain unacked without occupying execution slots. Actual updates
are applied by the consumer loop, not synchronously on every increment.

For a normal, non-rate-limited ETA task executed by solo, `on_task_request()`
runs the task synchronously. The due-time callback cannot continue to its QoS
decrement until that call returns, and queued ACK callbacks cannot run on the
same event loop while task execution occupies its thread. Updating the internal
QoS value and applying it to the consumer are separate steps; see the appendix
for the exact ordering and exceptions.

Relevant implementation:

- `celery/worker/strategy.py`: ETA branch, QoS increment, timer registration.
- `celery/worker/state.py`: `scheduled_requests` is distinct from active/reserved.
- `celery/worker/consumer/consumer.py`: `apply_eta_task`, `call_soon_ack`, and
  shutdown cancellation of scheduled timers/local bookkeeping.
- `celery/worker/consumer/tasks.py`: initial QoS, ETA limit wiring, Redis
  disable-prefetch gate based on reserved requests rather than scheduled requests.
- `celery/worker/request.py`: acceptance and early/late ACK policy.
- Kombu `common.py`: `QoS.increment_eventually`/`decrement_eventually` and cap behavior.

## Operational guidance and boundaries

- For a bounded worker-managed ETA workload, use a **positive finite ETA limit**
  and a **positive prefetch multiplier**, with initial concurrency × multiplier
  no greater than the limit. Test your dependency versions: the measured cap
  limitations mean this is not an unconditional memory-safety guarantee.
- A saturated ETA cap can starve immediate messages on the same consumer. Consider
  separate ETA and immediate-work queues **with separate worker capacity** when
  predictable immediate-task latency matters.
- Keep worker-held delays short. Use a persistent scheduler to publish near the
  due time for large or distant-future schedules instead of keeping them all in RAM.
- With Redis, the unacked interval includes the delay before execution. It can
  also include pool waiting and task runtime, especially with late ACK or solo's
  deferred early ACK. A too-short visibility timeout can cause redelivery while
  the original copy is still scheduled/running. Increasing it also delays crash
  recovery. We did not exercise that recovery loop here.
- RabbitMQ delivery-acknowledgment timeouts are a separate constraint on long-held
  unacked messages. Do not reuse Redis-specific timeout conclusions unchanged.
- **Broker-native delayed delivery is an important exception:** supported RabbitMQ
  quorum/native-delay routing can hold messages in broker-side delayed-delivery
  infrastructure rather than deliver them immediately to a worker timer. Celery's
  publication path selects this under supported queue/exchange configurations;
  direct exchanges have a documented limitation. We did not test native delivery.
- We did not measure large-backlog RSS, rate-limited ETA tasks, task expiry/revocation,
  multiple workers, higher concurrency, connection recovery, or Linux fork behavior.
  Five-message tests demonstrate the accounting mechanism, not a maximum safe size.

## Reproduce and inspect

```bash
# Uses the existing uv environment and mise-managed private Redis server:
bash .dev/setup-shutdown-env.sh
PYTHONDONTWRITEBYTECODE=1 .dev/.venv/bin/python .dev/verify-eta-prefetch.py

# Focused checks:
.dev/.venv/bin/python .dev/verify-eta-prefetch.py \
  --case cap-three --case cap-with-zero-prefetch \
  --case due-solo-early --case cold-mixed-gevent
```

- Worker module: `.dev/eta_probe.py`. Reuses the existing app and ACK observer
  from `shutdown_probe.py`; adds ETA-aware task markers, pool-acceptance markers,
  and periodic scheduler-state observations.
- Runner: `.dev/verify-eta-prefetch.py`. Reuses isolated Redis/process helpers
  from the shutdown suite. Broker snapshots come from outside the worker, so
  solo's blocked consumer does not block observation.
- Final [31-case summary](eta-prefetch-runs/20261113T152618.891666Z/summary.json).
- Each case directory contains `config.json`, `worker.log`, `events.jsonl`, and
  `result.json`, including phase snapshots and per-message ACK/result/location.
- Console summary: `.dev/eta-validation-console-final.log`.

Exploratory runs are retained. An initial mixed-cap assertion expected two ETA
messages behind an active late-ack task with prefetch 1, but correctly observed
zero: the active delivery had exhausted the only credit. The final suite tests
that behavior explicitly and uses prefetch 2 for the separate shared-cap case.
No Celery/Kombu production code was changed. No commits were made.

## Appendix: deterministic solo execution and event-loop ordering

This appendix is maintained documentation, like the rest of this report.
Experiment runs write raw artifacts and do not regenerate or overwrite it.
The normal execution path below describes the tested configuration; rate-limit,
revocation, and interruption distinctions are source-based, not additional
runtime cases.

### A1. Once `on_task_request()` is called, execution is synchronous

For an executable task using the standard solo pool, the normal,
non-rate-limited ETA path is:

```text
Event loop fires ETA timer
  → apply_eta_task()
    → on_task_request()
      → solo executes the task inline
      → task outcome is handled
    ← on_task_request() returns
    → qos.decrement_eventually()
  ← timer callback returns
Event loop regains control
```

There is **no normal scheduling decision** where solo sometimes executes inline
and sometimes queues the task for a later event-loop iteration. The earlier
wording “can execute” was unnecessarily vague about this deterministic behavior.

Despite the name `pool.apply_async()`, solo's implementation directly calls the
execution function. It does not submit work to another process, thread, or
greenlet. The consumer uses the direct task-processing path for solo rather
than prefork's semaphore-based dispatch path.

Calls such as `time.sleep()` or blocking I/O inside the task **do not return
control to Celery's event loop**. Releasing the GIL is not the same as yielding
to that loop: the thread still has not returned from task execution.

### A2. When can the event loop run before the task body executes?

Deferral occurs **before entering the normal solo execution call**, not because
`on_task_request()` nondeterministically chooses to defer it.

| Situation | Behavior |
| --- | --- |
| ETA has not arrived | Request remains on the timer; the event loop continues |
| ETA arrived, but a configured task rate limit has no available token | Rate-limit handling schedules another callback and returns; execution happens later |
| Request was revoked or expired | The task body is skipped, not deferred for later execution |
| Setup/execution is interrupted by an error or shutdown exception | The call may abort; this is not an alternative scheduling mechanism and does not guarantee a normal return to the loop |

The rate-limited ETA path uses `_limit_post_eta()` rather than
`apply_eta_task()`. It decrements the ETA QoS compensation **before** checking
and draining the rate-limit bucket. If a token is available, it can dispatch
immediately; otherwise it schedules another timer callback. Once it dispatches
an executable task to solo, that execution is still synchronous.

The experiments did not configure task rate limits, so their normal due-time
ordering should not be generalized to the rate-limited callback's QoS ordering.

### A3. Internal QoS decrement is distinct from applying it to the consumer

The ordinary due-time callback is:

```python
def apply_eta_task(self, task):
    task_reserved(task)
    self.on_task_request(task)       # Synchronous task execution with solo.
    self.qos.decrement_eventually()  # Reached after that call returns.
```

`decrement_eventually()` updates the **internal QoS value** inside this callback.
It does not itself apply the new prefetch count to the consumer/channel; the
consumer loop performs that update later.

Similarly, requesting an ACK can queue a callback on the hub. That callback
cannot execute on the same event loop while the task is occupying its thread.
Normal event-loop processing or shutdown cleanup must later process it.

The sequence above assumes the call returns normally, including ordinary task
failures handled by Celery's tracing machinery. An escaping shutdown exception
or other unhandled error may unwind the call stack instead, so the subsequent
QoS-decrement line is not guaranteed to execute in those paths.

Relevant implementation: `celery/concurrency/solo.py`,
`celery/concurrency/base.py` (`apply_target`), `celery/worker/components.py`,
`celery/worker/worker.py` (`_process_task`), and
`celery/worker/consumer/consumer.py` (`apply_eta_task`, `_limit_post_eta`,
`_schedule_bucket_request`).
