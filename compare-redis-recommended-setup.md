# Compare recommended Redis worker setups

Comparison of the maintained [solo](solo-redis-recommended-setup.md),
[prefork](prefork-redis-recommended-setup.md),
[threads](threads-redis-recommended-setup.md), and
[gevent](gevent-redis-recommended-setup.md) profiles.

These recommendations target our recorded **Celery 5.7.0 / Kombu 5.7.0** sources,
not every release. Runtime evidence used macOS ARM64, mostly concurrency 1;
prefork children used spawn. The profiles require workload-specific budgets and
deployment acceptance tests. This comparison adds no new runtime measurements.

## 1. Common recommendations for all pools

| Area | Shared recommendation |
| --- | --- |
| Delivery safety | Late ACK + retained SUCCESS + deduplication; business idempotency and durable reconciliation remain necessary. |
| Failure/retry policy | ACK handled failures/timeouts; retry selected failures explicitly with finite budgets. Raw requeue is not a bounded task retry. |
| Prefetch and ETA | Start with multiplier 1; use a positive, sized ETA guard; keep distant scheduling outside worker memory. |
| Visibility and retention | Size visibility for the full unacked interval; align all scanners; retain results through the expected recovery horizon. |
| Connectivity | Enable consumer reconnect, Redis health checks, and bounded broker/backend socket operations; bound backend/publication retry layers. |
| Normal shutdown | TERM, adequate external grace, and supervised restart. Exit status alone does not prove settlement. |
| Redis operations | Deliberate namespace isolation, noeviction/capacity alerts, suitable persistence, private authenticated access, and TLS where needed. |
| Validation/monitoring | Observe task effects, backend state, actual settlement/recovery, and worker progress separately; test the real deployment. |

### Delivery safety is an application design, not one ACK flag

All four profiles select:

- `task_acks_late=True`.
- `task_ignore_result=False` with a persistent Redis result backend.
- `worker_deduplicate_successful_tasks=True`.
- Effective failure/timeout ACK policy **True**. Solo uses the legacy fallback;
  the other templates explicitly set the 5.7 split flags too. The intended policy
  is the same; review per-task overrides.

This preserves recovery opportunities and can skip a completed body's restored
copy. It does **not** provide exactly-once execution or unconditional delivery:

- Concurrent copies can both start before SUCCESS exists. Independently republished
  messages are not automatically deduplicated merely because their task IDs match.
- SUCCESS can coexist with an unacked/restored delivery; an ACK error can occur
  after Redis already committed the ACK.
- PENDING can mean waiting/running work, an expired result, or a lost delivery.
- The measured BRPOP reply-loss window left neither a queued message nor an
  unacked record. Visibility recovery cannot recover a nonexistent record.

Use durable business operation IDs, atomic effects where possible, remote API
idempotency keys, and reconciliation of unfinished jobs. An outbox helps with
publication; it does not alone repair a delivery lost after publication.

Disabling ordinary failure ACK **discards through reject-without-requeue** in the
measured path. It does not request automatic retry. Conversely, disabling hard-timeout
ACK can raw-requeue, without incrementing task retries. Keep ordinary retries narrow,
finite, and deliberate; a task's `max_retries` does not cap visibility redelivery.

### Prefetch, ETA, and timing budgets

At multiplier 1, initial ordinary prefetch is approximately **C**, the worker's
execution concurrency—not necessarily one message. For solo, C is always 1.

Choose a positive `worker_eta_task_limit` at least as large as initial ordinary
prefetch. This is an admission guard, **not a strict memory guarantee**:

- Prefetch 1 and `worker_disable_prefetch=True` did not bound all future-message intake.
- Multiplier 0 is unlimited and bypassed the tested ETA cap.
- An initial prefetch above the cap was not clamped in this checkout.
- Cap saturation blocked immediate delivery despite idle execution capacity;
  draining could also reduce the QoS baseline. Revalidate after changing concurrency.

Keep distant schedules in durable scheduling storage and publish near execution
time. Short retry countdown/backoff delays also become ETA work.

Size visibility for **ETA/rate/pool waiting + execution + result handling + actual
ACK + margin**. It is a receipt-age threshold, not a task runtime limit. Longer
visibility delays abandoned-work recovery; eligibility still needs a live scanner
and consumer capacity. Retention should cover expected redelivery/outage recovery
if deduplication needs stored SUCCESS. Neither budget has one universal safe value.

In this stack, the normal Celery entry point is only
`broker_transport_options['visibility_timeout']`. The similarly named backend and
top-level entries have no independent effect. Do not alias the broker/backend
option dictionaries or confuse explicit application forwarding with core support.

Align visibility across **all processes actually scanning shared unacked records**,
including monitoring consumers. A worker on another queue can restore your delivery;
queue names alone do not isolate policies. Prefix/database isolation must be deliberate,
and changing a prefix does not migrate existing messages.

### Connection and backend policy

The four templates share these policy choices:

- `broker_connection_retry_on_startup=True`, `broker_connection_retry=True`, and
  `broker_connection_max_retries=None`: keep retrying after detected connection
  failures. This can wait indefinitely and needs operational alerting.
- `broker_channel_error_retry=False`: do not automatically restart on every
  channel-error category. Enable only for characterized errors; permanent errors
  can recur indefinitely.
- Redis health checks plus bounded connect/socket operations, configured separately
  for broker and result backend. TCP keepalive is supplementary; omit its options
  for Unix-socket deployments.
- A finite RedisBackend `retry_policy`, with `result_backend_always_retry=False`
  to avoid an unlimited generic outer retry layer. The Redis helper does not wrap
  every read. Enabling generic retries requires a finite budget and layered tests.
- Finite task-publication retries with handling for exhaustion and ambiguous publish.

The templates' **5 s socket/connect bounds, 25 s health interval, and three backend
retries are illustrative**, not production latency measurements. A socket timeout
is not a total operation/retry deadline and does not bound every async poller wait.
Consumer reconnect does not necessarily replay a failed ACK.

Monitoring heartbeats are separate from Redis health checks. Redis does not negotiate
AMQP protocol heartbeats. Keep monitoring if needed; `--without-heartbeat` is not
required for any profile and is not a connection-reliability setting.

### Common operational requirements

- Normal shutdown uses TERM with externally configured grace covering remaining
  work, result/ACK processing, retries, and teardown. Repeated TERM does not escalate.
  `worker_soft_shutdown_timeout` is a cold-path grace, not a timer started by TERM.
- A supervisor must deliver signals to the real worker and restart crashed processes.
  A final forced stop is a crash with possible partial effects and delayed recovery,
  not safe business cancellation.
- Bound application I/O, lock acquisition, total execution, and cleanup. Celery's
  Redis socket settings do not bound your HTTP/SQL/filesystem operations.
- Use noeviction with adequate capacity, alerts, and backpressure. Configure Redis
  persistence for the required recovery objective; it does not remove transport gaps.
  Broker/backend DB numbers on one Redis instance do not isolate its failure domain.
- Use unique worker names, deliberate routing, private authenticated endpoints,
  and verified TLS where appropriate. Budget all client connections and memory.

Before deployment, test longest work, TERM, process loss, explicit disconnects,
silent sockets, duplicate effects, exhausted retries, ETA saturation, and reconciliation.
Test backend-only and combined broker/backend outages: the earlier connection suite
kept the backend reachable. Persistence/failover and realistic load are additional
acceptance tests, not guarantees from those experiments.

## 2. Differences between the pool recommendations

| Decision | Solo (`solo`) | Prefork (`prefork`) | Threads (`threads`) | Gevent (`gevent`) |
| --- | --- | --- | --- | --- |
| Best fit | Simple serial, finite jobs | CPU work, process isolation, supported task limits | Bounded blocking I/O with thread-safe libraries | Audited cooperative I/O |
| Execution capacity | One inline task per worker process | C child processes | C threads sharing one process | C greenlets sharing one process |
| Deadline recommendation | Application deadlines; Celery limits not enforced | Application deadlines, then soft < hard limits | Application deadlines; Celery limits not enforced | Application deadlines; no soft limit; hard limit optional/cooperative |
| Cancel on broker loss | **False**; detection can arrive after completion | **True** for the restart/interruption-safe profile | **False**; running-thread cancellation unsupported | **False** baseline; True only after cancellation/recovery audit |
| Lost-executor retry | No solo-crash protection | **True**, bounded by task retry attributes, with parent alive | No prefork-style child-loss recovery | No prefork-style child-loss recovery |
| Control/monitoring during work | Pauses during inline body | Parent normally remains responsive | Normally progresses; GIL/native/shared-state caveats | Progress depends on cooperation; hub starvation affects everyone |
| Beyond normal TERM | Avoid QUIT cancellation workaround as standard policy | Optional tested soft-to-cold escalation | Cold/hard paths cannot reliably stop running threads | Avoid measured signal-context cold/soft defects |
| Resource lifecycle | Supervise/restart whole process | Profile child resources; optional child recycling | Audit shared state; replace whole worker for recycling | Patch early; audit libraries/shared state; replace whole worker |

These are **selected profile policies**, not identical defaults or guarantees of
capability. In particular, prefork cancellation assumes interruption-safe jobs;
gevent's False baseline does not mean its connection-error cancellation never works.

### Choose execution isolation before tuning concurrency

- **Solo:** `--concurrency` above 1 does not create additional execution slots.
  Scale separate supervised workers. A normal long task blocks the consumer loop,
  so a short remote-control ping/monitoring deadline is not a safe automatic restart
  trigger. Use durable progress and application deadlines instead.
- **Prefork:** size C against CPU quota, per-child RSS, and downstream limits.
  Initialize process-local clients safely and verify the actual start method.
  `worker_max_tasks_per_child` and `worker_max_memory_per_child` can help cumulative
  leaks, but the memory limit is in KiB and replacement occurs **after the task**;
  it is not a peak-allocation kill threshold.
- **Threads:** audit shared task attributes, clients, sessions, transactions, locks,
  and resource acquisition. Conventional CPython's GIL limits pure-Python CPU
  parallelism; native libraries can differ. The profile keeps
  `result_backend_thread_safe=False`; changing it selects a shared backend instance,
  not automatic locking or proof of thread safety. Measure connection costs.
- **Gevent:** use `--pool=gevent` on the CLI so patching occurs early. Validate DNS,
  TLS, database/native clients, locks, logging, and cleanup—not only a test sleep.
  A high greenlet count does not create Python CPU parallelism. Route incompatible
  work to another worker pool rather than trusting monkey-patching to fix it.

For enforceable independent interruption, choose prefork or another supervised
process boundary. Thread/greenlet concurrency does not provide process isolation.

### Deadline behavior changes the failure policy you can rely on

**Prefork** supports the measured soft exception and hard interruption paths. A
soft exception can be caught, and an unhandled one uses **failure ACK** policy.
A hard timeout uses **timeout ACK** policy; it is not a WorkerLostError, even if a
child is terminated. The recommended timeout ACK True settles that failed attempt;
intentional retry/reconciliation must decide what happens next.

**Solo and threads** did not enforce either task limit. Setting those numbers does
not compensate for unbounded application calls. A hung thread may require killing
the entire process and interrupting sibling jobs.

**Gevent** interrupted cooperative work with a hard limit, but a non-yielding body
finished before its overdue timeout fired during result I/O. SUCCESS could coexist
with timeout handling; timeout ACK False caused reexecution of completed work.
The baseline therefore leaves the hard limit unset, with mandatory application
bounds. Add a hard limit only after validating the entire cooperative trace.

### Cancellation and child-loss recovery are not interchangeable

The active connection-cut experiments found:

- **Solo:** loss was handled after the body/ACK path; cancel-on-loss found no active
  request left to stop. This is delayed detection, not duplicate protection.
- **Prefork:** cancellation off gave two starts/two completions; on gave two
  starts/one completion. True reduces continued original work for interruption-safe
  tasks, but does not roll back already committed effects.
- **Threads:** cancellation was attempted but raised `NotImplementedError`; both
  bodies still completed. Enabling it does not implement thread termination.
- **Gevent:** ordinary connection-handler cancellation worked, with two starts/one
  completion, but **two ACK returns**. The canceled wrapper could take a success-ACK
  callback; restoration had already supplied a copy in that experiment. True is
  an explicit audited alternative, not a universally safe switch or a fix for QUIT.

The prefork profile separately enables **5.7 `task_retry_on_worker_lost=True`** with
finite task `max_retries` and deliberate `default_retry_delay`, keeping raw
`task_reject_on_worker_lost=False`. This requires a surviving parent. Retry
publication can fail, and publishing a replacement plus ACKing the original is not
atomic. Budget exhaustion is a terminal failure to handle, not guaranteed redelivery.

No pool can run in-process recovery callbacks after its whole worker/controller
has died. External restart and remaining broker/application records are necessary.
Do not use synthetic WorkerLostError or SystemExit as an application recovery API;
the latter exposed a non-prefork callback defect, including stranded credit in
threads/gevent.

### Shutdown, ACK ordering, and visibility differ even with common settings

TERM is common, but escalation is not:

- **Prefork:** optional soft-to-cold shutdown let short work finish and restored
  canceled late-ACK work in the measured cases. Test the policy and supervise the
  entire process group; killing only a parent must not leave children executing.
- **Solo:** the soft wait paused the task's own thread; active cold cancellation
  raised unsupported `kill_job`. The guarded requeue workaround is not a standard API.
- **Threads:** running bodies continued past cold cancellation errors; some hard
  paths still needed SIGKILL. Executor shutdown can cancel pending futures but
  does not kill running threads.
- **Gevent:** signal-context paths raised `BlockingSwitchOutError` and could ACK
  incomplete work. This differs from the working ordinary connection-error handler.

Visibility still depends on who can scan. A lone solo worker did not scan during
its inline body, but another worker restored it; even its **early ACK** could be
queued until after completion. Prefork's early-ACK control settled before that
restoration window. Early ACK is nevertheless not the recovery profile recommended
for any of these pools.

While waiting on ETA, solo's loop is available: it accumulated and executed copies
in the visibility tests. Visibility restoration leaves local timers intact, unlike
the tested reconnect cleanup. Do not extrapolate exact solo duplicate counts to
all pools or assume the ETA guard fixes duplicate timers. Likewise, gevent's
scan cadence follows a different drain-events path, not the async timer cadence.

### What to validate especially for each pool

- **Solo:** delayed ACK/control progress, another scanner restoring active work,
  ETA copies, and supervisor grace without heartbeat-based false restarts.
- **Prefork:** actual child crash vs whole-worker crash, retry exhaustion/publication
  failure, soft/hard deadlines, process resource initialization, and the combination
  of deadlines with cancel-on-loss and loss retry.
- **Threads:** thread safety at intended C, connection/lock exhaustion, a truly stuck
  thread, concurrent duplicate effects, and whole-process shutdown impact.
- **Gevent:** early patching, every cooperative dependency, busy-body/slow-result
  timeout behavior, signal regressions, hub lag, and both cancellation policies.

The full profiles contain configuration templates and acceptance checklists.
Underlying evidence: [shutdown](shutdown-results.md),
[ETA/prefetch](eta-prefetch-results.md), [task policies](task-policy-results.md),
[Redis connections](redis-connection-results.md), and
[visibility/source-history audit](redis-visibility-results.md).
Maintain this comparison with those profiles; do not treat it as certification of
a new combined setup or a high-concurrency production benchmark.
