# Recommended threads + Redis setup

> **Recommended starting profile, not a newly validated production configuration.**
> Based on the recorded Celery **5.7.0** / Kombu **5.7.0** checkout, Redis **8.2.3**,
> CPython **3.13.9**, macOS ARM64. Most runtime cases used concurrency 1; realistic
> concurrent application safety and production sizing require separate tests.
>
> The CLI pool name is **`threads`**, not `threaded`.
> Other profiles: [solo](solo-redis-recommended-setup.md),
> [prefork](prefork-redis-recommended-setup.md), [gevent](gevent-redis-recommended-setup.md).

## 1. When this pool is appropriate

Use threads for **bounded blocking I/O using thread-safe libraries**, especially
when gevent compatibility/monkey-patching is undesirable. Task bodies run in
executor threads while the consumer/controller normally continues servicing work.
Our sleeping-task tests retained monitoring and connection-loss responsiveness.

Threads are not independently killable task processes. Use prefork instead when
you require enforced Celery execution limits, task-process isolation, independent
termination, or ordinary Python CPU parallelism. On the tested conventional
CPython build, the GIL limits pure-Python CPU parallelism; native libraries releasing
the GIL can behave differently. Long native calls holding the GIL can also harm
controller responsiveness. Free-threaded Python was not tested.

Recommended baseline:

- Idempotent jobs with explicit I/O and whole-operation deadlines.
- Late ACK, retained results, and successful-task deduplication.
- Prefetch multiplier 1, giving initial credit approximately equal to **C threads**.
- A positive ETA guard sized to permitted backlog, with initial credit <= guard.
- **No reliance on Celery time limits, thread cancellation, or child-loss recovery.**
- TERM for normal shutdown, plus an external deadline for a genuinely stuck process.

Do not use this pool for an operation that can hang forever if you cannot tolerate
terminating the entire worker and interrupting its other running tasks.

## 2. Budgets and shared-state design

| Item | Required decision |
| --- | --- |
| Concurrency C | Size to downstream throughput, memory, descriptors, and connection availability—not just CPU count or maximum thread count. |
| Application deadline | Bound socket operations, database calls, retries, lock/semaphore acquisition, and the total job. A Celery hard/soft limit does not provide the bound here. |
| Visibility | Cover ETA/retry delay, rate/executor waiting, execution, backend handling, and actual settlement plus margin. |
| Result retention | Cover expected successful-task redelivery/recovery, including outages. It is separate from visibility. |
| Shutdown grace | Cover remaining bounded tasks, result/ACK work, executor drain, and teardown; warm shutdown has no intrinsic deadline. |
| ETA guard | Positive and >= initial credit C; choose against memory/backlog. Saturation can starve immediate work. |

Task instances and application globals can be shared across concurrent requests.
Do not store per-job mutable data in shared task attributes or reuse an unsafe
session/pipeline/transaction across threads. Use supported task request context and
appropriately scoped clients; protect shared application state explicitly.

Budget client pools for actual concurrent operations, including tasks that need
multiple connections, backend instances, producers, and the controller. Bound
pool acquisition and avoid tasks holding one resource indefinitely while waiting
for another. Do not synchronously wait for tasks scheduled onto the same exhausted
worker pool; compose work asynchronously instead.

## 3. Configuration template

For **TCP Redis**, using a dedicated broker database or a deliberate shared-prefix
policy. Configure all participating publishers/workers/scanners consistently;
different queue names alone do not isolate unacked recovery.

Required environment values are deployment decisions. **5 s socket/connect
bounds, 25 s health interval, and three backend retries are illustrative**, not
load-tested production values. Configure before tasks/backends are initialized.

```python
import os

from celery import Celery


def positive_int(name):
    value = int(os.environ[name])
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


concurrency = positive_int("CELERY_WORKER_CONCURRENCY")
visibility_seconds = positive_int("CELERY_REDIS_VISIBILITY_SECONDS")
result_retention_seconds = positive_int("CELERY_RESULT_RETENTION_SECONDS")
eta_limit = positive_int("CELERY_ETA_TASK_LIMIT")
if eta_limit < concurrency:
    raise ValueError("ETA guard must be >= initial ordinary prefetch")

app = Celery(
    "jobs",
    broker=os.environ["CELERY_BROKER_URL"],
    backend=os.environ["CELERY_RESULT_BACKEND"],  # Persistent Redis backend.
)
app.conf.update(
    task_default_queue="jobs",
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    worker_concurrency=concurrency,
    worker_prefetch_multiplier=1,
    worker_eta_task_limit=eta_limit,

    task_acks_late=True,
    task_acks_on_failure_or_timeout=True,
    task_acks_on_failure=True,       # Explicit 5.7 split policy.
    task_acks_on_timeout=True,       # Policy, not a thread deadline mechanism.
    task_soft_time_limit=None,
    task_time_limit=None,
    task_retry_on_worker_lost=False,
    task_reject_on_worker_lost=False,
    task_ignore_result=False,
    worker_deduplicate_successful_tasks=True,
    result_expires=result_retention_seconds,
    worker_cancel_long_running_tasks_on_connection_loss=False,
    result_backend_thread_safe=False,

    broker_connection_retry_on_startup=True,
    broker_connection_retry=True,
    broker_connection_max_retries=None,
    broker_channel_error_retry=False,
    broker_transport_options={
        "visibility_timeout": visibility_seconds,
        "socket_connect_timeout": 5,
        "socket_timeout": 5,
        "health_check_interval": 25,
        "socket_keepalive": True,
    },
    redis_socket_connect_timeout=5,
    redis_socket_timeout=5,
    redis_backend_health_check_interval=25,
    redis_socket_keepalive=True,
    result_backend_transport_options={
        "retry_policy": {
            "max_retries": 3,
            "interval_start": 0.5,
            "interval_step": 0.5,
            "interval_max": 2,
        },
    },
    result_backend_always_retry=False,
)
```

- The explicit split failure/timeout settings refer to this **5.7** checkout;
  review support and task-level overrides before using another version.
- `result_backend_thread_safe=False` retains the default per-thread backend
  selection. It can increase backend objects/connection pools. Changing it to True
  selects a shared backend instance; it does not add locking to arbitrary backend
  code. Validate sharing before using it as a connection-count optimization.
- Only B (`broker_transport_options['visibility_timeout']`) feeds normal Redis
  recovery in this stack. R/top-level visibility entries are not additional timers.
  Keep broker and backend dictionaries independent, not aliased.
- Omit TCP keepalive fields for Unix-socket Redis. TLS, ACLs, prefixes, and serializer
  migrations require explicit deployment configuration; do not overwrite live routing.

Start with the same C used to validate initial credit and the ETA guard:

```bash
uv run celery -A myapp.celery_app:app worker \
  --pool=threads \
  --concurrency="${CELERY_WORKER_CONCURRENCY:?set concurrency}" \
  --prefetch-multiplier=1 \
  --queues=jobs \
  --hostname="${WORKER_ID:?set a unique worker id}@%h" \
  --loglevel=INFO
```

Keep monitoring events if used operationally. `--without-heartbeat` only disables
Celery monitoring heartbeats, not Redis health checks. Avoid mixing gevent monkey
patching into this thread-worker profile without a separate compatibility audit.

## 4. Failure and reconnect policy

| Event | What to expect / do |
| --- | --- |
| Ordinary exception | With late ACK and failure ACK True, the failed attempt is settled. Use narrowly scoped `self.retry()`/autoretry with finite attempts and bounded delay for recoverable errors. |
| Failure ACK disabled | Ordinary failure rejects without requeue. It does not make the attempt safely recoverable. |
| Soft or hard Celery time limit configured | Both failed to interrupt the tested thread task. Application deadlines remain necessary. |
| Broker loss while body runs | Controller can reconnect while the original thread continues. Measured late-ACK cases executed and completed two copies. |
| Cancel-on-loss enabled | The cancellation attempt raised `NotImplementedError`; both executions still completed. Keep False rather than implying a protection it cannot implement. |
| Entire process exits | All threads/controller are gone. External restart and remaining Redis/application records govern recovery. |
| `SystemExit` from task code | Measured non-prefork callback defect stranded delivery credit in a Future; this is not a supported task/thread recovery API. |

The `SystemExit` case produced `TypeError: cannot unpack non-iterable ExceptionInfo
object`. At prefetch 1 a subsequent ping task stalled; prefetch 2 allowed ping but
did not repair the stranded original. Do not increase prefetch to hide that defect.

Neither worker-loss flag creates a surviving controller for a whole-process
crash or turns thread cancellation into prefork child-loss handling. Shared handlers
can respond to synthetic exceptions, but do not manufacture those exceptions as
an application retry strategy.

### Safe duplicate handling

Connection recovery can restore a delivery before its original thread returns.
At higher C, another thread or worker can execute the copy concurrently. Our
connection thread cases used C=1; exact high-concurrency overlap is an extrapolation,
not a new measured threaded load test. The shared restoration mechanism and other
pool overlap tests are sufficient reason to require concurrency-safe idempotency.

Use durable business operation IDs, atomic database effects, and remote API
idempotency keys. A thread lock in one process does not protect against another
worker executing the same job. Successful-task deduplication is useful only after
success information exists and the message meets its redelivery conditions.

## 5. Shutdown and supervision

**TERM and wait for bounded tasks** is the normal policy. The thread pool's stop
path calls `ThreadPoolExecutor.shutdown(cancel_futures=True)`: pending futures may
be canceled, but running threads are waited for, not killed. Source behavior is
not a blanket runtime guarantee for every reserved-backlog race.

- `worker_soft_shutdown_timeout` does not bound TERM. Short tasks finished during
  the tested cold-path grace; expiry could not forcibly stop long-running threads.
- QUIT produced unsupported cancellation errors while finite task bodies continued.
  Do not configure `REMAP_SIGTERM=SIGQUIT` as a reliable thread shutdown strategy.
- Repeated Ctrl+C reaching a hard handler did not reliably exit a worker containing
  endless work; several cases still required SIGKILL.
- Keep an external final deadline. A forced process kill interrupts **all** active
  threads, can skip cleanup, and may leave unacked records until a later scan.
- A native threaded task stored SUCCESS while hard shutdown left its message
  restored. Backend success is not proof of settlement or single execution.
- Give every cleanup/finally path bounded I/O too. Never swallow `BaseException`
  or shutdown exceptions in a retry-forever loop.

Monitor controller responsiveness **and** task progress/age. A healthy ping does
not prove that every task thread is progressing. If one hung task cannot be allowed
to jeopardize sibling tasks during external restart, use process isolation instead.
Child recycling settings are not a thread replacement mechanism; ordinary memory
leaks accumulate in the shared process. Plan supervised whole-worker replacement
or select prefork when process recycling is needed.

## 6. Redis, scheduling, and infrastructure

- Avoid distant ETA/countdown on the worker; keep long-term schedules durably
  elsewhere. Short task-retry delays are ETA work and count toward unacked age.
- Prefetch multiplier 1 means initial credit C, not a universal backlog/memory
  bound. Multiplier 0 is unlimited and bypassed the tested ETA cap.
- The ETA guard can block immediate work when saturated; the tested QoS accounting
  can reduce the baseline after draining. `worker_disable_prefetch=True` did not
  cap all future-message intake. Test your permitted backlog and memory separately.
- A scanner on another queue can restore your messages. Align visibility across
  every process scanning the shared namespace, including monitoring consumers.
  Choose consistent prefixes/DB isolation deliberately; do not abandon old messages
  by changing a prefix without migration.
- A live controller can scan while its task threads run. Short visibility can
  therefore restore live work; it is not a thread execution deadline or lease renewal.
- Reconnect prefetch reduction was skipped for Redis's recorded `qos_global=False`
  in this checkout. Do not use it as the sole duplicate/capacity safeguard.
- Bound broker and backend socket operations separately. The broker settings do
  not bound application HTTP/SQL calls. A socket timeout does not bound every
  async poller wait; Redis health checks matter too.
- Consumer retries with maximum None permit indefinite reconnection after errors,
  not indefinite replay of failed ACKs. Keep outage alerts and supervisor policy.
  Channel-error retry is an opt-in recovery classification, not an ACK guarantee.
- Backend retry helper limits do not cover every read or bound an entire layered
  operation. Keep generic backend retries off unless you configure a finite outer
  budget and test it. Keep producer retries finite and handle ambiguous/exhausted publication.
- Configure appropriate Redis persistence, noeviction and memory alerts, private
  authenticated access, and verified TLS where required. Separate broker/backend
  databases on one Redis process do not isolate availability or memory pressure.
  These are recommendations, not measured persistence/failover guarantees.

## 7. Gotchas to avoid

| Mistake | Safer approach |
| --- | --- |
| “Threads are prefork with lower memory” | They share a process and mutable state, and lack independent task termination. |
| “Timeout/cancellation flags kill a blocked thread” | Bound application operations or choose a process boundary. |
| “Turning cancel-on-loss on prevents duplicates” | The tested implementation cannot cancel the running thread. |
| “Backend thread-safe True makes all client usage safe” | It selects sharing; audit actual backend, clients, pipelines, and context usage. |
| “Increasing C is free I/O throughput” | Enforce downstream/pool limits and bounded resource acquisition. |
| “A task's retry cap bounds broker redelivery” | Visibility/raw requeue do not increment task retries. |
| “PENDING proves no effects; SUCCESS proves ACK” | PENDING can mean missing/expired results or running work; SUCCESS can coexist with restored/unacked delivery. |
| “An ACK error means Redis did not apply it” | The commit can succeed while its reply is lost. |
| “Late ACK guarantees delivery” | The measured BRPOP reply-loss gap can leave no queued or unacked record. |
| “A process exit/worker shutdown log proves every thread stopped cleanly” | Observe actual exit and individual business completion; signal escalation was unreliable in some tests. |

Important jobs need durable application records and reconciliation. An outbox
helps publication but does not repair a delivery lost after publication by itself.
Do not blindly republish based only on PENDING, and do not treat repeated task IDs
as an automatic execution lock.

## 8. Acceptance checklist

- [ ] Test thread safety and load at intended C, including shared task objects,
  database/HTTP sessions, backend selection, resource exhaustion, and GIL-heavy paths.
- [ ] Verify resolved C, prefetch, ETA guard, actual channel visibility, task
  overrides, result retention, and broker/backend socket settings.
- [ ] Run maximum-duration work, then TERM; verify all body/result/ACK/exit outcomes
  within supervisor grace. Test a stuck thread in isolation from production.
- [ ] Exercise ordinary failure, bounded explicit retry, and exhausted/ambiguous
  publication; ensure business-level duplicate/attempt policy is effective.
- [ ] Cut and blackhole broker paths while multiple tasks run; verify duplicates
  cannot cause conflicting effects and the controller remains usable.
- [ ] Test result-backend failure and combined broker/backend outages. Earlier
  connection tests bypassed the broker fault for backend connections.
- [ ] Saturate short ETA/retry backlog and test restoration from another worker
  or queue. High-C ETA/cancellation interactions were not exhaustively measured.
- [ ] Exercise whole-process crash/restart and reconciliation of missing delivery
  records, uncertain ACKs, and completed jobs with expired results.
- [ ] Validate the chosen Redis persistence, failover, security, and memory policy
  under deployment load; no prior experiment certified those infrastructure choices.

## Evidence and maintenance

- [Shutdown report](shutdown-results.md): successful finite warm drain, unsupported
  cold cancellation, hard-shutdown hangs, and SUCCESS with restored threaded work.
- [Task-policy report](task-policy-results.md): no thread time-limit enforcement,
  ordinary failure disposition, and SystemExit/Future/credit defect.
- [Connection report](redis-connection-results.md): monitoring during work, reconnect
  duplicates, attempted-but-unsupported cancellation, ACK ambiguity, and delivery loss.
- [ETA/prefetch report](eta-prefetch-results.md): shared scheduled-work and QoS behavior.
- [Visibility report](redis-visibility-results.md): option wiring, shared scanning,
  age threshold, result retention, and limits of extrapolation to other pools.

Source checks include `celery/concurrency/thread.py`, `celery/app/base.py` backend
selection, and RedisBackend retry handling. Thread-safety/connection sizing and
illustrative production tuning are recommendations rather than new measurements.
Maintain this guide separately from raw reports; no new worker matrix or production
configuration changes were made to prepare it.
