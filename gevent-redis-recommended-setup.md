# Recommended gevent + Redis setup

> **Conditional profile for cooperative I/O, not a general replacement for prefork.**
> Based on our Celery **5.7.0**, Kombu **5.7.0**, gevent **26.9.0**, greenlet
> **3.5.6**, Redis **8.2.3**, CPython **3.13.9**, macOS ARM64 measurements.
> Signal-context cancellation and delayed-timeout defects were reproduced in this
> checkout. The combined configuration and production concurrency need validation.
>
> Other profiles: [solo](solo-redis-recommended-setup.md),
> [prefork](prefork-redis-recommended-setup.md), [threads](threads-redis-recommended-setup.md).

## 1. Workload suitability is the first configuration decision

Use gevent only when **all important execution paths cooperate with its scheduler**:
network clients, DNS/TLS, sleeps, synchronization, database drivers, logging,
result handling, and cleanup. One blocking path can starve unrelated greenlets,
monitoring, broker processing, and timers in the same process.

Prefer prefork for CPU-heavy work, uncooperative native libraries, enforced process
termination, or stronger fault isolation. Threads can suit bounded blocking I/O
with thread-safe libraries when gevent integration is unavailable, but do not add
independent task termination.

Recommended baseline:

- Finite cooperative jobs, explicit application deadlines, and concurrency-safe
  business idempotency.
- Late ACK, retained SUCCESS results, and completed-task deduplication.
- Prefetch multiplier 1 with deliberately sized C greenlets and a positive ETA
  guard no smaller than initial credit C.
- TERM for normal shutdown. **Do not depend on QUIT/soft-to-cold cancellation**
  in this measured checkout.
- Application deadlines as the primary policy. No Celery soft limit; an optional
  cooperative hard limit is a separately tested defense, not a process watchdog.
- Conservative **cancel-on-connection-loss False** unless you explicitly validate
  the interruption/recovery path for your jobs. This permits original work to
  continue and therefore requires protection against duplicate execution.

This baseline does not claim that avoiding cancellation prevents duplicates.
The choice trades interruption for continued execution; section 5 explains the
measured alternative and its ACK caveat.

## 2. Startup, budgets, and resource sizing

### Start through the CLI with the pool selected early

Use `--pool=gevent` on the actual Celery command line. Celery's
`maybe_patch_concurrency` / `_patch_gevent` performs early monkey-patching before
loading the worker application. Merely setting `worker_pool='gevent'` later in
application configuration is not a substitute for this startup ordering.

Do not preload socket/SSL/thread-sensitive application clients before the patch,
patch midway through a running process, or assume every C-extension client becomes
cooperative. Verify each library's supported gevent integration. Programmatic
embedding needs its own audited early-startup sequence; the CLI profile is safer
than an untested custom bootstrap.

### Size capacity against the actual bottleneck

| Budget | Recommendation |
| --- | --- |
| Concurrency C | Choose from downstream capacity, memory per in-flight job, descriptors, connection pools, and latency. High greenlet counts are not free throughput or Python CPU parallelism. |
| Application deadlines | Bound each operation and overall work, including retries and cleanup. Timers/checkpoints cannot run while the hub is starved. |
| Optional hard task limit | Only after verifying cooperative behavior through result handling. It can fire late rather than being silently ignored. |
| Visibility | Cover ETA/retry delay, waiting, execution, result processing, and actual ACK plus margin. Hub starvation can extend the interval. |
| Result retention | Cover successful-job redelivery/recovery, including outages and scan delays. |
| Shutdown grace | Allow remaining bounded work, result/ACK handling, pool join, and teardown after TERM. |
| ETA guard | Positive and >= C at multiplier 1; size from memory/backlog and test saturation. Revisit if concurrency/autoscaling changes. |

Count all backend/producer/application connections, not just greenlets. Backend
instance/context sharing depends on startup and library behavior; do not assume
one universal Redis connection pool. `result_backend_thread_safe=True` is not a
magic greenlet-safety or connection-count fix—validate sharing explicitly.

Task instances and globals may be shared. Do not keep per-job mutable state on a
shared task object or reuse unsafe transaction/pipeline objects concurrently.
Cooperative scheduling does not make read-modify-write sequences atomic across
I/O yields or across separate worker processes.

## 3. Configuration template

For **TCP Redis**, with private authenticated endpoints and verified TLS where
required. Use a dedicated broker database or consistent broker key prefixes for
all publishers, workers, and other recovery scanners. Plan old-message migration
before changing a prefix; backend prefixes are a separate concern.

The required workload budgets have no arbitrary defaults. The **5 s socket/connect
bounds, 25 s health interval, and three backend retries are illustrative** and must
be tuned/tested. Initialize configuration before tasks/backends; the CLI handles
gevent patching before importing the module below.

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
    task_acks_on_timeout=True,       # Avoid raw requeue if a hard limit is added.
    task_soft_time_limit=None,      # Not enforced by the tested gevent pool.
    task_time_limit=None,           # Application deadlines required; see section 4.
    task_retry_on_worker_lost=False,
    task_reject_on_worker_lost=False,
    task_ignore_result=False,
    worker_deduplicate_successful_tasks=True,
    result_expires=result_retention_seconds,
    worker_cancel_long_running_tasks_on_connection_loss=False,

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

The explicit split failure/timeout policies refer to this 5.7 checkout. Review
per-task overrides and existing retry/result/serialization settings. Omit TCP
keepalive fields for Unix-socket Redis. Keep broker/backend option dictionaries
independent; neither R visibility nor a standalone top-level visibility setting
adds a timer in this stack.

Start with the same C used to validate the ETA guard:

```bash
uv run celery -A myapp.celery_app:app worker \
  --pool=gevent \
  --concurrency="${CELERY_WORKER_CONCURRENCY:?set concurrency}" \
  --prefetch-multiplier=1 \
  --queues=jobs \
  --hostname="${WORKER_ID:?set a unique worker id}@%h" \
  --loglevel=INFO
```

Use separate worker processes/queues for incompatible CPU/blocking workloads.
Keep Celery monitoring if you need it; disabling monitoring is not a Redis
connection-health policy. Give workers unique names and route intended tasks to
`jobs`; changing defaults does not migrate existing queue contents.

## 4. Time limits, application errors, and cooperative progress

**Leaving `task_time_limit=None` does not authorize unbounded tasks.** The baseline
requires application deadlines and cooperative libraries. If you cannot bound a
path with those tools, select a process-based execution boundary.

| Behavior | Measured implication |
| --- | --- |
| Soft task limit | Did not interrupt the gevent task. There is no prefork-style soft exception guarantee. |
| Hard limit on cooperative work | Interrupted the tested task and invoked timeout policy. It is a cooperative timeout, not independently enforced process termination. |
| Hard limit on a non-yielding body | Body completed before the overdue timeout fired during result I/O. |
| Overdue hard timeout, timeout ACK True | Backend remained SUCCESS and ACK occurred, although timeout failure handling also ran. |
| Overdue hard timeout, timeout ACK False | The completed task was requeued and executed again; retries stayed 0. |

If you add `task_time_limit`, validate the **entire trace**, including slow result
writes and cleanup, not only an artificial cooperative sleep. This profile keeps
`task_acks_on_timeout=True` to avoid raw timeout-requeue loops. That settles a timed-out
attempt; it does not guarantee the business work completed. Use durable reconciliation
and intentional retries for work that must eventually finish.

Ordinary exceptions use failure ACK policy; disabling it discards rather than
requeues the failed attempt. Retry narrowly selected transient errors with finite
attempts/backoff/jitter. Short retry delays are still ETA work. `max_retries` does
not cap Redis restoration or raw requeue loops.

Do not swallow `GreenletExit`, `BaseException`, or shutdown exceptions in a
retry-forever loop. Do not use `SystemExit`, `WorkerLostError`, or
`TimeLimitExceeded` as custom task retry APIs. The measured non-prefork SystemExit
callback defect left a live controller with stranded delivery credit; larger
prefetch let other work through without fixing the original.

Yielding during long computation can improve hub responsiveness but does not
provide CPU parallelism or make the computation safely interruptible. Thread-offloading
an incompatible blocking call also does not make the underlying operation safely
cancellable; design the execution boundary for the actual failure requirement.

## 5. Connection-loss cancellation: an explicit tradeoff

The baseline leaves cancellation disabled to avoid assuming that arbitrary jobs
and this checkout's cancellation/ACK path are safe to interrupt.

| Measured cooperative active-cut case | Starts / body completions | ACK returns | Outcome |
| --- | --- | --- | --- |
| Cancellation False | 2 / 2 | 2 | Original continues; restored copy also executes |
| Cancellation True | 2 / 1 | **2** | First greenlet stops; restored copy completes |

**True did work in the ordinary connection-error handler.** It did not fail with
the OS-signal-context error discussed below. However, the canceled greenlet's
wrapper can return through a success callback and ACK without a completed body.
In this experiment restoration had already made another copy available, so work
recovered; the result is not a blanket guarantee for every cancellation/outage order.

For audited interruption-safe tasks, enabling
`worker_cancel_long_running_tasks_on_connection_loss=True` is a candidate to reduce
continued original execution after loss. Validate cleanup, partial effects, actual
settlement, and eventual recovery under your fault model before selecting it.
It does not affect already-ACKed requests, undo effects, fix visibility expiry with
healthy connections, or make signal-based cold shutdown safe.

With False, accept that duplicates can run concurrently. With True, accept that
cancellation itself introduces an interruption boundary. Both choices require
business idempotency and reconciliation; neither makes Redis delivery exactly once.

## 6. Shutdown: TERM, bounded cooperation, external supervision

**Use TERM for normal deployments.** Warm shutdown worked for finite cooperative
tasks; gevent pool shutdown joins its greenlets. It still has no shutdown-imposed
wall-clock deadline, and a stuck/non-yielding task can prevent timely progress.

Avoid `REMAP_SIGTERM=SIGQUIT`, routine QUIT, and Ctrl+C escalation as the normal
lifecycle in this checkout:

- Soft/cold signal paths raised `BlockingSwitchOutError`.
- Some work exceeded the intended soft-shutdown window.
- Cold shutdown could leave an **incomplete task PENDING but ACKed**, with no
  queued/unacked copy remaining for recovery.
- A successful process exit or an ACK callback does not prove body completion.

These are measured checkout-specific defects, not claims that every gevent
version always has the same signal behavior. Require a regression test against
any upgraded version before depending on a fix.

Give a supervisor enough grace for bounded work plus result/ACK/teardown time.
Keep a final external process deadline for genuine hangs, understanding it kills
all active greenlets and skips cleanup. A whole-process crash has no surviving
controller to run child-loss recovery flags. Process recycling/child memory-limit
settings are not per-greenlet isolation or memory reclamation mechanisms.

Observe hub responsiveness and durable job progress. Monitoring heartbeats ran
during cooperative bodies but stopped during a CPU-busy body. Redis did not close
those workers for missing Celery monitoring events. A missing event can indicate
hub starvation, not necessarily a disconnected broker; a recovered connection
or ping does not establish settlement of old work.

## 7. Redis and scheduling rules that remain necessary

- Size visibility for the full unacked interval, not the expected duration of a
  cooperative task alone. Other workers/scanners can restore records while this
  worker is starved, and expiry does not cancel existing local work.
- Align B visibility and broker namespaces across task workers and monitoring
  consumers. Another queue name is not isolation; prefixes/DB separation can be,
  but changing them does not move existing queued/unacked records.
- Keep distant ETA scheduling outside worker memory. Prefetch 1 gives initial
  credit C and does not bound all ETA intake; multiplier 0 bypassed the tested cap.
  Saturated ETA guards can block immediate delivery even with idle greenlets.
- Gevent ETA reconnect cleanup was measured to yield one body execution after
  two receipts. Do not extrapolate that to visibility scans: the latter do not
  clear old local timers. Duplicate-count/ETA-cap interactions need targeted tests.
- Do not copy the async-path approximate 100 s default scan period into gevent
  assumptions. This pool uses a different drain-events scheduling path; restoration
  still requires loop progress, and exact timing depends on the installed transport.
- Keep Redis health checks and bounded broker/backend socket operations. None
  can provide reliable scheduling while the hub is blocked. BRPOP health recovery
  was measured for cooperative gevent; it is not a watchdog outside the process.
- Consumer retries permit reconnect after detected failure, not failed-ACK replay.
  Channel-error retry is optional classification and can loop on permanent errors.
  Source/configuration wiring does not promise recovery of an old delivery.
- Keep backend and publication retries bounded. RedisBackend's retry helper does
  not cover every read, and socket timeouts do not cap whole retry sequences. If
  enabling generic backend retries, set a finite outer budget and test both layers.
- Use private authenticated Redis, verified TLS where needed, noeviction with
  memory alerts, and persistence appropriate to the recovery objective. Sharing
  one Redis process across broker/backend DBs still shares its failure domain.
  Persistence and failover were not certified by our experiments.

## 8. Gotchas to avoid

| Mistake | Safer interpretation |
| --- | --- |
| “Any Python I/O library becomes cooperative after patch_all” | Validate driver/C-extension/DNS/TLS/lock behavior; use supported integration or another pool. |
| “Large concurrency creates CPU parallelism” | Greenlets share execution capacity; size against I/O bottlenecks and memory. |
| “A hard limit always interrupts at its deadline” | It can be delayed until after body completion and fire during backend I/O. |
| “Timeout ACK False means a bounded retry” | It raw-requeues without incrementing retries; completed effects can repeat. |
| “Connection cancellation and QUIT are the same context” | Ordinary connection handling worked; OS-signal paths exposed separate failures. |
| “The greenlet ACKed, so it finished” | A canceled target can take a success-callback ACK path. |
| “Late ACK guarantees cold-shutdown recovery” | Incomplete gevent work was ACKed and absent after cold shutdown in this checkout. |
| “Deduplication prevents concurrent work” | It only suppresses eligible completed-task redelivery; early concurrent copies can both run. |
| “SUCCESS proves settlement; PENDING proves no effects” | Result storage/expiration, body effects, and broker records are different observations. |
| “An ACK error proves Redis retained the delivery” | Redis may have committed ACK before the reply was lost. |
| “Reconnection plus visibility guarantees no loss” | The measured Redis pop-before-registration window leaves nothing for visibility recovery. |

Use durable job status and business operation IDs. An outbox helps publication;
reconciliation is also needed for lost delivery/uncertain settlement. A cache flag
or process-local lock does not provide cross-worker exactly-once effects.

## 9. Acceptance checklist

- [ ] Verify CLI early patching and actual cooperative behavior of every dependency,
  including DNS, TLS, DB drivers, locks, logging, result I/O, and cleanup.
- [ ] Load-test intended C: memory, descriptors, connection pools, downstream limits,
  hub lag, task progress, and shared-state correctness.
- [ ] Verify resolved C, task overrides, prefetch/ETA guard, visibility, result
  retention, and broker/backend timeout settings.
- [ ] TERM during maximum-duration work and backend delays; verify body/result/ACK
  and exit separately within supervisor grace.
- [ ] If enabling a hard limit, test a busy body and delayed result I/O; account for
  SUCCESS-plus-timeout observations and deliberate timeout recovery policy.
- [ ] Compare cancel-on-loss False/True with representative partial side effects
  before selecting True. Verify actual restored records and safe repeated effects.
- [ ] Reproduce known SystemExit and signal-context defects in isolated tests when
  validating upgrades; do not exercise destructive shutdown probes in production.
- [ ] Test silent connections, explicit cuts, hub starvation, backend outage, and
  combined outages. Earlier connection faults kept backend connectivity healthy.
- [ ] Saturate short ETA/retry backlog and test visibility recovery with another
  scanning worker; no high-C duplicate/cap interaction matrix was established.
- [ ] Test whole-process crash/restart, missing-delivery reconciliation, result expiry,
  and the deployment's Redis persistence/failover/security assumptions.

## Evidence and maintenance

- [Shutdown report](shutdown-results.md): cooperative warm drain, signal-context
  errors, and ACK of incomplete gevent work.
- [Task-policy report](task-policy-results.md): unenforced soft limits, cooperative
  hard limits, late timeout after SUCCESS, requeue duplication, and SystemExit defect.
- [Connection report](redis-connection-results.md): cooperative vs busy monitoring,
  gevent BRPOP health recovery, cancellation comparison, ACK ambiguity, and delivery gap.
- [ETA/prefetch report](eta-prefetch-results.md): shared scheduled-message/credit behavior.
- [Visibility report](redis-visibility-results.md): source wiring and common recovery
  mechanism; exact solo visibility duplicate counts are not new gevent measurements.

Source checks include `celery/__init__.py` early patching,
`celery/concurrency/gevent.py` scheduling/timeout/cancellation wrappers, request
callbacks, and RedisBackend retry handling. Production tuning and library
compatibility recommendations are not a new integration test. Maintain this guide
with the reports and revalidate the exact sources after upgrades.
