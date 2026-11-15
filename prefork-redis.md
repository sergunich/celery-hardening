# Recommended prefork + Redis setup

> **Recommended profile, not a newly validated production configuration.** Based
> on our reports for Celery **5.7.0**, Kombu **5.7.0**, Redis **8.2.3**, CPython
> **3.13.9**, macOS ARM64. The measured prefork children used **spawn**; Linux fork,
> production concurrency, failover, and the complete configuration below require
> deployment tests. Several settings here are specific to the tested 5.7 code.
>
> Other profiles: [solo](solo-redis.md),
> [threads](threads-redis.md), [gevent](gevent-redis.md).
> These are alternative worker profiles, not settings to combine on one worker.

## 1. Recommended design and assumptions

Prefer prefork for CPU-bound work, independent task-process isolation, or workloads
requiring supported Celery soft/hard time limits. The parent can observe a child
crash, replace the child, and execute recovery callbacks while other children work.
It cannot do that after the entire worker/parent has died.

This profile assumes **idempotent, restart-safe jobs** whose partial effects can be
reconciled. It deliberately chooses:

- Late ACK, retained SUCCESS results, and successful-task deduplication.
- Ordinary prefetch multiplier 1: approximately **concurrency C**, not one message
  for the entire worker. Avoid extra local reservations unless benchmarks justify them.
- Application I/O deadlines plus a soft limit followed by a hard limit.
- **Bounded task retry on child loss**, rather than unlimited raw requeue.
- **Cancellation of active late-ACK tasks on broker connection loss**, to reduce
  continued original execution after recovery restores another copy. This requires
  tasks safe to interrupt; it does not undo prior effects or guarantee redelivery.
- TERM for normal deployments; externally supervised parent lifecycle.

If interrupted work cannot be made safe, do not enable cancellation or hard-kill
policies merely to improve responsiveness. Turning cancellation off lets originals
continue but can produce duplicate execution after reconnect. Resolve that tradeoff
at the application level rather than claiming a configuration provides exactly once.

## 2. Budgets and process sizing

| Budget | Recommendation |
| --- | --- |
| Concurrency C | Choose against actual CPU quota, per-child RSS, downstream capacity, and connections. The suite mostly used C=1; it is not a sizing benchmark. |
| Soft/hard task deadlines | Application deadlines should normally fire first; choose soft < hard, with a cleanup/checkpoint margin between them. |
| Visibility | Cover delivery-to-actual-ACK time: permitted ETA/retry delays, pool/rate waiting, execution, result handling, and margin. A hard execution limit alone does not bound that whole interval. |
| Result retention | Cover the expected recovery/redelivery horizon, including outages and scan delays. Needed for backend-based completed-task deduplication and the success-before-child-loss guard. |
| Shutdown grace | Cover remaining task execution/deadlines, retries and result/ACK work, pool draining, and teardown—not only the task body. |
| ETA guard | Positive and at least initial ordinary prefetch C. Size from memory/backlog requirements; saturation can block immediate work. |
| Child-loss retry budget | Finite `Task.max_retries` and deliberate `Task.default_retry_delay`. Delay creates ETA work; include it in visibility planning. |

Process recycling can help with cumulative leaks: consider
`worker_max_tasks_per_child` and `worker_max_memory_per_child` after profiling.
The memory setting is in **KiB** and replaces an over-limit child **after its task
finishes**; it is not a per-task peak-memory kill limit. Use OS/container resource
budgets too. Recycling/load behavior was not benchmarked in our suites.

Initialize process-local clients/resources safely in children. Do not assume that
fork-inherited sockets, locks, or threads are usable, or that spawn inherits
application globals. Test the start method actually used by your deployment.

## 3. Configuration template

For **TCP Redis**, preferably private authenticated endpoints with verified TLS
where required. Use a dedicated broker database or a consistent broker prefix
across publishers, workers, and other recovery scanners. Prefix changes do not
migrate existing messages; result prefixes are separate.

Required values come from your workload. The 5 s connection/socket timeouts,
25 s health interval, and three backend retries are **illustrative starting
values**, not production measurements. Configure before task/backend initialization.

```python
import os

from celery import Celery, Task


def integer(name, minimum=1):
    value = int(os.environ[name])
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value


concurrency = integer("CELERY_WORKER_CONCURRENCY")
visibility_seconds = integer("CELERY_REDIS_VISIBILITY_SECONDS")
result_retention_seconds = integer("CELERY_RESULT_RETENTION_SECONDS")
eta_limit = integer("CELERY_ETA_TASK_LIMIT")
soft_limit = integer("CELERY_TASK_SOFT_LIMIT_SECONDS")
hard_limit = integer("CELERY_TASK_HARD_LIMIT_SECONDS")
if eta_limit < concurrency:
    raise ValueError("ETA guard must be >= initial ordinary prefetch")
if soft_limit >= hard_limit:
    raise ValueError("Soft task limit must be below hard task limit")


class RecoveryTask(Task):
    max_retries = integer("CELERY_TASK_MAX_RETRIES", minimum=0)
    default_retry_delay = integer("CELERY_TASK_RETRY_DELAY_SECONDS")


app = Celery(
    "jobs",
    broker=os.environ["CELERY_BROKER_URL"],
    backend=os.environ["CELERY_RESULT_BACKEND"],  # Persistent Redis backend.
    task_cls=RecoveryTask,
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
    task_acks_on_timeout=True,       # Do not raw-requeue endless hard timeouts.
    task_retry_on_worker_lost=True,  # 5.7: bounded by task attributes above.
    task_reject_on_worker_lost=False,
    task_soft_time_limit=soft_limit,
    task_time_limit=hard_limit,
    task_ignore_result=False,
    worker_deduplicate_successful_tasks=True,
    result_expires=result_retention_seconds,
    worker_cancel_long_running_tasks_on_connection_loss=True,

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

Merge deliberately with existing settings and task classes: task-level overrides
can change ACKs, deadlines, result storage, and retry budgets. Do not change
serialization or queue routing without handling existing messages. For Unix sockets,
omit TCP keepalive fields and validate that transport separately.

Run the app using the same concurrency value used to validate the ETA guard:

```bash
uv run celery -A myapp.celery_app:app worker \
  --pool=prefork \
  --concurrency="${CELERY_WORKER_CONCURRENCY:?set concurrency}" \
  --prefetch-multiplier=1 \
  --queues=jobs \
  --hostname="${WORKER_ID:?set a unique worker id}@%h" \
  --loglevel=INFO
```

No monitoring-disabling flags are required. Redis does not negotiate AMQP
heartbeats. Monitoring events and Redis health checks serve different purposes.

## 4. Failure, timeout, and recovery policy

| Event | Selected policy / limitation |
| --- | --- |
| Ordinary application failure | ACK the handled failed attempt; narrowly targeted application retries decide whether to publish another attempt. |
| Unhandled soft-limit exception | Ordinary **failure** policy applies, not hard-timeout ACK policy. Catch only if you can checkpoint/clean up/retry deliberately. |
| Soft limit caught and normal return | Celery records SUCCESS even if only part of the intended business work was done. Return success only when semantically correct. |
| Hard deadline | Parent terminates execution; timeout policy ACKs the failed attempt. This is not classified as WorkerLostError for lost-task retry. Reconcile/retry explicitly if required. |
| Unexpected child loss | Retry-on-lost can publish the same task ID with incremented retries, then ACK the original. The task's finite budget limits this retry path. |
| Lost retry budget exhausted | With this profile, failure is settled/ACKed. Alert or reconcile; do not silently assume another delivery will appear. |
| Retry publication fails | Source falls through to ordinary lost policy; with reject-on-lost False and failure ACK True it can settle the original without replacement. Publication-failure injection was not measured. |
| Entire worker/parent dies | No surviving controller to run those policies. External restart plus remaining Redis records/visibility and application reconciliation are needed. |
| Broker connection lost | This profile attempts to stop eligible active late-ACK work. Recovery and task retry are different operations; cancellation RETRY state does not prove publication of a retry. |

Measured active-cut results: cancellation off produced **two starts/two body
completions**; cancellation on produced **two starts/one completion**. This supports
the interruption-safe profile, not a promise that every outage restores the message.
Partial side effects from the canceled execution still need idempotency.

Avoid enabling raw `task_reject_on_worker_lost=True` as if it were a bounded retry.
Measured raw requeue retained retries=0 and continued even with max_retries=0.
Likewise, `task_acks_on_timeout=False` raw-requeued hard timeouts; it did not use
the bounded retry budget. Choosing True here favors a terminal, observable failure
rather than an uncontrolled retry loop; business reconciliation supplies intentional recovery.

The measured success-before-child-loss guard avoided retry after SUCCESS was
already stored. It depends on available result information and differs for ignored
results/RPC/unavailable backends. It is not an exactly-once transaction with ACK.

## 5. Shutdown and supervision

**Use TERM for ordinary deployment.** Warm shutdown waits for running work without
its own deadline. Task deadlines and supervisor grace are separate. Repeated TERM
does not escalate shutdown, and `worker_soft_shutdown_timeout` does not start a
countdown after TERM.

Prefork also supported a deliberate soft-to-cold path in the tests: after QUIT,
short work finished during grace; overrunning late-ACK work was canceled and
restored. If your deployment needs this escalation, choose and test
`worker_soft_shutdown_timeout` and possibly `worker_enable_soft_shutdown_on_idle`
for ETA-only workers. This is an **optional cold-shutdown policy**, not part of
the normal TERM profile. An idle grace did not execute newly due ETA work in our tests.

- Do not remap TERM to QUIT merely to make deployments faster.
- A hard child deadline can prevent task cleanup/finally from completing; remote
  operations and separately spawned subprocesses may outlive the child.
- Supervise the entire worker process group/container. Killing only the parent
  must not leave children executing untracked work. Our whole-worker SIGKILL tests
  killed the owned process group, not just the parent PID.
- Keep a final external deadline for genuinely stuck teardown. Forced termination
  is a crash, with uncertain side effects and possible delayed redelivery.
- A worker-offline monitoring event arrived before a prefork task finished during
  warm shutdown. It is not proof that all task execution has stopped.
- The healthy and selected outage warm cases completed and ACKed, but longer or
  different outages can still leave uncertain settlement. Exit 0 is not an ACK proof.

## 6. Common Redis safety and sizing rules

- Business idempotency is mandatory for this recovery profile. Use durable operation
  IDs/transactions or external API idempotency keys, not an expiring cache flag.
  Retained SUCCESS deduplication only suppresses eligible completed redeliveries;
  it does not prevent concurrent copies before SUCCESS or all repeated publications.
- Record important jobs durably and reconcile unfinished work. A transactional
  outbox covers publication, not the measured Redis BRPOP-reply loss before unacked
  registration. Broker settings cannot recover a nonexistent unacked record.
- Use only the broker transport visibility option for this built-in Redis stack.
  Backend/top-level visibility entries have no independent effect; do not alias
  broker/backend dictionaries. A short-timeout scanner on another queue can restore
  your work. Audit monitoring consumers as well as task workers.
- Keep distant ETA in an external durable scheduler. Prefetch 1 and
  `worker_disable_prefetch=True` are not ETA-memory bounds. A positive ETA guard
  can block immediate work when saturated; initial credit above the guard is not
  clamped in this checkout. Revalidate if autoscaling raises C beyond the guard.
- Parent-loop progress means visibility scans can restore **still-running** or
  **reserved-but-not-started** tasks. This was measured, including overlap with
  completed-task deduplication enabled. Pool execution slots do not own the Redis lease.
- Do not rely on reconnect prefetch reduction: this checkout recorded
  `qos_global=False` and skipped reduction for Redis, even when enabled.
- Keep backend and publication retry budgets finite. Consumer retry None means
  keep reconnecting after detected failures, not replay every failed ACK. The
  chosen RedisBackend retry helper does not wrap every read; enabling generic
  backend retries requires a separate finite budget and combined-layer tests.
- Use noeviction, capacity alerts, appropriate persistence, private ACL-protected
  endpoints, and verified TLS. These are infrastructure recommendations, not results
  of persistence/failover tests. Broker/backend DB numbers do not isolate failures
  when the same Redis process serves both.
- Budget Redis/database/HTTP connections across the parent, all children, all
  worker instances, and reconnects. Concurrency is not a connection-count ceiling.

## 7. Gotchas to avoid

| Mistake | Safer interpretation |
| --- | --- |
| “A hard timeout retries the job because lost-worker retry is on” | Hard timeout and WorkerLostError are different branches. |
| “Turning failure ACK off keeps it recoverable” | Ordinary failure instead rejects without requeue. |
| “All processes are killed, so child-loss recovery runs” | It requires a surviving parent. |
| “A retry cap bounds all duplicates” | Visibility restoration, raw requeue, and publication duplicates are separate. |
| “Cancellation rolls back work” | External effects can already be committed; cancellation is interruption, not rollback. |
| “SUCCESS means settled; PENDING means never executed” | Separate body effects, backend retention/state, ACK observations, and broker records. |
| “More prefetch always improves throughput” | It increases outstanding work and pool-wait age; benchmark the tradeoff. |
| “Child memory recycling caps peak job memory” | Replacement happens after the task, not when its allocation crosses the threshold. |
| “A short soft-shutdown setting bounds TERM” | It applies before cold cancellation, not normal warm shutdown. |
| “Same version means identical pool behavior everywhere” | Our platform used spawn; test the actual OS, start method, libraries, and source versions. |

## 8. Acceptance checklist

- [ ] Validate resolved C, actual start method, ETA guard >= initial prefetch,
  deadlines, retry attributes, task-level overrides, and channel visibility.
- [ ] Load-test C with realistic task mix, per-child RSS, and downstream limits;
  validate process-local resource initialization/recycling if configured.
- [ ] TERM during maximum-duration work; verify body/result/ACK/exit separately
  and ensure supervisor grace is adequate.
- [ ] Exercise soft exception handling, hard termination, actual child crash,
  whole-parent/group crash, bounded loss-retry exhaustion, and failed retry publication.
- [ ] Test the **combination** of loss retry, cancellation-on-loss, and deadlines;
  the report cases establish individual paths, not every cross-product.
- [ ] Cut/blackhole broker connections during side effects and verify idempotency,
  cancellation, recovery, and subsequent worker usability.
- [ ] Fail the result backend independently and together with the broker. Prior
  connection experiments kept backend connectivity healthy.
- [ ] Saturate permitted ETA/retry backlog and test visibility against maximum
  unacked age, including other scanning applications.
- [ ] Verify durable reconciliation for uncertain ACK, missing result, lost delivery,
  terminal timeout, and exhausted retry. Test Redis persistence/failover separately.

## Evidence and maintenance

- [Shutdown report](shutdown.md): warm/soft/cold/hard behavior and platform scope.
- [Task-policy report](task-policy.md): prefork deadline enforcement,
  bounded loss retries, raw requeue, exhaustion, and success-before-loss guard.
- [ETA/prefetch report](eta-prefetch.md): waiting work, credit limits, and cap caveats.
- [Connection report](redis-connection.md): cancel-on-loss comparison,
  ACK ambiguity, delivery gap, prefetch reduction, and warm-outage observations.
- [Visibility report](redis-visibility.md): shared scanners, reserved work
  expiry, repeated execution, option wiring, and result TTL.

Maintain this guide separately from generated results. The configuration template
is checked for syntax/resolved values, not certified by a new worker matrix.
No production settings or services were changed to prepare it.
