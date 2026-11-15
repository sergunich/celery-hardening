# Recommended solo + Redis setup

> **Recommended starting profile, not a newly validated production configuration.**
> Synthesizes our shutdown, ETA/prefetch, task-policy, Redis-connection, and
> visibility reports. Findings apply to the recorded Celery **5.7.0** checkout and
> installed Kombu **5.7.0** sources; version numbers alone do not identify fixes.
>
> The intended workload is **finite, idempotent jobs with explicit application
> deadlines**, mostly dispatched for immediate execution. Choose the lifecycle
> budgets below before deploying. This guide does not change an application,
> service, Redis instance, or supervisor configuration.

## 1. The recommended design

1. **One execution per solo worker process.** Use multiple separately supervised
   workers for capacity, not a larger `--concurrency` value on one solo worker.
2. **Late ACK + successful-task deduplication + durable business idempotency.**
   Keep successful results long enough for the expected recovery horizon.
3. **Prefetch multiplier 1.** Avoid holding unnecessary immediate work locally.
   Add a positive, workload-sized ETA guard if short scheduled retries are allowed;
   do not mistake it for a strict memory bound or permission to preload distant ETA work.
4. **TERM for routine shutdown**, with sufficient supervisor grace. Use application
   I/O deadlines and cooperative total-runtime limits; solo's task time limits do
   not enforce them.
5. **Redis health checks and bounded socket operations**, configured separately for
   broker and result backend. Consumer reconnection is not failed-ACK replay.
6. **One compatible visibility policy per shared broker namespace**, including
   other workers and monitoring consumers that actually run recovery scans.
7. **Durable job tracking and reconciliation for important work.** Redis transport
   has a measured pop-before-unacked-registration loss window. No combination of
   the settings below guarantees exactly-once execution or unconditional delivery.

### When not to choose solo

Use prefork or a separately supervised execution process if you need independently
interruptible execution, enforced Celery task time limits, responsive worker
control while a task runs, or isolation from task crashes. Threads are not a fix
for independently killing running work either.

If a missed job is unacceptable, do not rely solely on Redis queue state: record
jobs in durable application storage and reconcile unfinished work. An outbox helps
with **publication**, but an outbox alone does not repair a delivery lost after it
was successfully published.

## 2. Choose the budgets first

| Budget | How to choose it |
| --- | --- |
| Application execution deadline | Bound all remote calls, loops, retries, and subprocesses. A timeout on one HTTP request is not a bound on the whole task. |
| Maximum unacked interval | Include ETA/countdown wait, rate/pool wait, execution, result handling, and time until an actual ACK—not just the body duration. |
| Visibility timeout | Greater than the expected maximum unacked interval, plus an operational margin. A longer value delays abandoned-work recovery. |
| Supervisor shutdown grace | Allow the worst expected remaining task work, result/ACK processing and retries, teardown, and margin after TERM. |
| Result retention | Cover the expected redelivery/recovery horizon if successful-task deduplication depends on the backend. Account for outages and scan delays, not only visibility. |
| ETA guard | Choose a positive value from permitted scheduled backlog and memory budget; keep initial ordinary prefetch no greater than it. Test saturation behavior. |

There is no finite timeout that both recovers abandoned work immediately and
prevents duplicates during arbitrarily long execution/outages. If the required
visibility delay conflicts with your recovery objective, shorten/checkpoint jobs
or reconsider the execution/queue design rather than looking for another flag.

For the intended immediate-work profile, keep long-term scheduling in a durable
scheduler and publish near execution time. Short `self.retry(countdown=...)` and
backoff delays are ETA work too: include their maximum delay in the budget.

## 3. Configuration template

This is a **TCP Redis profile**. Use private authenticated endpoints and verified
TLS where appropriate. Set the required environment values from the budgets
above; no arbitrary visibility, retention, or ETA-count defaults are supplied.

The **5 s socket/connect timeouts, 25 s health interval, and three backend retries
are illustrative starting values**, not values optimized by our experiments.
Tune them against healthy latency, payload sizes, retry budgets, and deployment
failure tests. Socket timeouts bound operations, not entire retry sequences.

Configure the app before tasks/backends/connections are initialized. Merge this
policy deliberately into an existing application; do not silently replace its
routing, serializers, prefixes, or task-level overrides. The template assumes a
dedicated broker database without unrelated applications. If sharing that database,
choose a consistent broker `global_keyprefix` for every participating publisher,
worker, and monitoring consumer before publishing work; plan migration of old
messages separately.

```python
import os

from celery import Celery


def positive_int(name):
    value = int(os.environ[name])
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


visibility_seconds = positive_int("CELERY_REDIS_VISIBILITY_SECONDS")
result_retention_seconds = positive_int("CELERY_RESULT_RETENTION_SECONDS")
eta_limit = positive_int("CELERY_ETA_TASK_LIMIT")

app = Celery(
    "jobs",
    broker=os.environ["CELERY_BROKER_URL"],
    backend=os.environ["CELERY_RESULT_BACKEND"],  # Persistent Redis result backend.
)

app.conf.update(
    task_default_queue="jobs",
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],

    task_acks_late=True,
    task_acks_on_failure_or_timeout=True,
    task_ignore_result=False,
    worker_deduplicate_successful_tasks=True,
    result_expires=result_retention_seconds,

    worker_prefetch_multiplier=1,
    worker_eta_task_limit=eta_limit,
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
    # Do not add an unlimited generic retry layer around backend operations.
    result_backend_always_retry=False,
)
```

### Why these choices—and their limits

| Choice | Reason / limitation |
| --- | --- |
| Late ACK | Preserves a recovery opportunity after many crashes; duplicates remain possible and not every Redis failure leaves a recoverable record. |
| ACK ordinary failures | Failed attempts should be terminal unless the application explicitly retries/reconciles them. Disabling failure ACK discards ordinary failures; it does not automatically requeue them. |
| Retain SUCCESS + dedup | Measured to skip a completed body's redelivered copy. Requires matching task ID/redelivery conditions and available success information; not a lock against concurrent execution or independently republished messages. |
| Prefetch 1 | Minimizes ordinary reservations and pool-wait exposure. It neither bounds all ETA intake nor prevents visibility duplicates/repeated execution. |
| Positive ETA guard | Limits admission in the supported positive-QoS setup. Saturation can block immediate work, and this checkout has cap/accounting caveats. It is a guard, not a hard memory guarantee. |
| Cancel-on-loss False | Solo has no independently killable task executor. The measured loss detection occurred after the task finished; enabling the option did not provide active-task protection. |
| Connection retries enabled, maximum None | Keeps trying after detected connection failures instead of giving up because of a temporary outage. Requires outage alerting; it can wait indefinitely and does not replay every failed ACK. |
| Channel-error retry False | Do not automatically restart for arbitrary channel errors that may be permanent configuration/data errors. Enable deliberately only after characterizing the relevant errors; it does not fix the measured ACK failure. |
| Backend retry policy finite | RedisBackend's own retry helper can retry writes and other operations using that helper. Not every read uses it. This policy is distinct from broker reconnect and task retry. |
| Generic backend retry layer off | Avoid stacking an unlimited generic layer on top of Redis retries. If enabling it for recoverable reads/writes, set a finite `result_backend_max_retries`, budget both layers, and test the combined behavior. |

Additional details:

- Leave the 5.7 split `task_acks_on_failure` and `task_acks_on_timeout` settings at
  their default `None` to inherit the True fallback, or explicitly set both True
  if your existing app overrides them. Review per-task ACK/result overrides too.
- Do not set `result_backend_transport_options['visibility_timeout']` or a
  standalone `app.conf.visibility_timeout` expecting another recovery timer.
  Our complete configuration matrix and source/history audit found no independent
  effect from those entries. Do not alias the broker/backend option dictionaries.
- For Unix-socket Redis, omit the TCP keepalive options and validate the socket
  transport separately. TCP keepalive timing itself was not exercised in our suite;
  its OS defaults can be much slower than the desired detection time.
- `result_backend_always_retry=False` does **not** mean RedisBackend never retries:
  its internal `ensure`/retry policy is separate. Conversely, three retries are
  not a three-second total deadline.
- The connection tests deliberately kept the result backend reachable. The exact
  bounded-backend-retry profile above is source-backed guidance, **not a measured
  combined broker/backend-outage result**. Backend outages require acceptance tests.
- Keep task-publication retries finite and handle exhausted/ambiguous publication
  in the submitting application. Publication retry is another layer, not consumer
  reconnection. A repeated publish can create duplicates even with the same ID.

### Worker command

Example for an application exporting this app as `myapp.celery_app:app`:

```bash
uv run celery -A myapp.celery_app:app worker \
  --pool=solo \
  --concurrency=1 \
  --prefetch-multiplier=1 \
  --queues=jobs \
  --hostname="${WORKER_ID:?set a unique worker id}@%h" \
  --loglevel=INFO
```

Each separately supervised worker needs a unique name. Route intended tasks to
`jobs`; a worker restricted to that queue will not consume other queues.

No `--without-heartbeat` is prescribed here. Keep monitoring events if you use
them; disable them only as an intentional monitoring choice. Gossip/mingle can
also be disabled when their coordination behavior is unnecessary—not merely
because the experiment CLI disabled them to reduce noise.

## 4. Task design: the part configuration cannot replace

### Make duplicate effects safe

Use a **business operation ID**, not merely the assumption that a Celery task ID
runs once. Prefer an atomic transaction combining the operation record and its
database effects; use external services' idempotency keys for remote effects.
Do not mark an operation complete before its effects are durably committed.
A simple cache flag or expiring lock is not a universal exactly-once mechanism.

Maintain enough durable status to reconcile incomplete jobs after a lost delivery,
failed publication, process death, or uncertain ACK. A Redis result key is useful
for Celery deduplication but is not automatically your durable business ledger.

### Bound work and retry explicitly

- Set connect/read/operation timeouts on **every application dependency**. Broker
  socket settings do not bound your HTTP, database, filesystem, or native calls.
- Use a monotonic total deadline and checkpoints for cooperative work. Calls into
  uninterruptible native code need a different execution boundary if termination
  must be enforceable.
- Retry narrowly identified transient failures with bounded attempts and bounded
  backoff/jitter. Re-raise programming errors instead of blindly retrying them.
  Avoid `autoretry_for=(Exception,)` as a blanket policy, and never swallow
  `BaseException`/shutdown exceptions in a retry-forever loop. Keep cleanup bounded too.
- Account for retry countdowns as ETA. Keep delayed retries small enough for the
  visibility budget or schedule them outside the worker's in-memory ETA queue.
- A task's `max_retries` limits Celery task retries, **not raw Redis redeliveries**.
  Add business-level attempt/reconciliation policy if repeated delivery must be bounded.
- Do not use `SystemExit`, `os._exit`, synthetic `WorkerLostError`, or synthetic
  `TimeLimitExceeded` as an application retry API.

## 5. Shutdown and supervision

**Normal sequence:** send one TERM, allow work to finish, then allow teardown.
Configure the supervisor/container to deliver TERM to the actual Celery worker
and to wait for the chosen grace period. Avoid a shell wrapper that swallows the
signal; use signal-forwarding supervision or an appropriate `exec` entrypoint.
A clean exit is not proof of settlement: a solo task stored SUCCESS and exited 0
while its failed ACK left an unacked record. More shutdown grace does not by itself
replay a failed ACK; deduplication and reconciliation still matter.

- TERM sets a stop flag; solo returns to the current body. Warm shutdown has no
  intrinsic deadline, and repeated TERM does not escalate it.
- Do not set `REMAP_SIGTERM=SIGQUIT`. Avoid QUIT or repeated Ctrl+C escalation for
  ordinary deployments: solo's active cancellation raises unsupported `kill_job`
  errors and can settle interrupted work as an ordinary failure.
- `worker_soft_shutdown_timeout` is not a warm-shutdown deadline. On solo, its
  measured main-thread wait paused the body rather than giving it useful progress.
- Do not make the private-state/`NotImplementedError` → `Reject(requeue=True)`
  workaround the standard lifecycle. It worked only in the tested guarded late-ACK
  case, relies on private behavior, and cannot undo side effects.
- If a task exceeds the agreed external grace, a final forced stop may be necessary.
  Treat it as an unclean crash: possible incomplete effects, unacked work, delayed
  recovery, and duplicates. SIGKILL is not a safe cancellation primitive.
- Restart an unexpectedly exited solo process externally, with backoff. There is
  no surviving controller in that process to execute lost-worker retry callbacks.

Do not set supervisor liveness to “Celery ping responds within a few seconds” for
long-running solo tasks. The control/consumer loop cannot respond during normal
inline execution. Monitor process existence, queue age, durable task progress,
and operation deadlines separately; restart on evidence of a stuck job, not merely
an expected monitoring gap.

## 6. Redis operation and visibility scope

Infrastructure recommendations below are **not Redis persistence/failover test
results**; our experiments used a disposable, healthy Redis with persistence off.

- Use a dedicated broker instance where feasible, or deliberate database/key-prefix
  isolation. Different queue names alone do **not** isolate unacked recovery.
- Use **`maxmemory-policy noeviction`**, sufficient memory, and alerts. Broker keys
  must not be silently evicted. A full noeviction instance can reject writes, so
  capacity/backpressure and error handling remain necessary.
- Enable persistence appropriate to the required recovery point objective. AOF
  `everysec` can lose recent writes; stricter fsync policies trade performance for
  durability. Neither persistence nor replicas remove the measured delivery/ACK gaps.
- Use private network access, appropriate Redis ACLs/authentication, and verified
  TLS where required. Do not expose an unauthenticated broker publicly.
- If broker and backend share a Redis process, separate DB numbers do not isolate
  outages, memory pressure, eviction policy, or persistence failures.
- Use consistent broker visibility and prefix settings across **all recovery
  scanners**, including monitoring/inspection processes that start transport
  consumers. A pure publisher or passive GET client does not itself run such scans.
- Changing a broker prefix does not migrate old messages. Coordinate deployment,
  draining, and recovery of the old namespace; configure backend prefixes separately.
- Visibility expiry is not exact-time delivery. A live scanner, scan cadence,
  mutex availability, and consumer capacity matter. In our installed async path,
  default scan interval/throttling can yield roughly 100 s between scans after
  the initial scan. Do not blindly copy that cadence to another Kombu version.

Do not copy the experiments' **2 s visibility**, **0.25/0.5 s scan intervals**, or
**1 s network timeouts** into production as reliability improvements. They were
chosen to expose failure mechanisms quickly.

## 7. Gotchas and things to avoid

| Avoid assuming / doing | What to do instead |
| --- | --- |
| “Late ACK means exactly once / guaranteed no loss” | Design idempotency and reconciliation; the Redis pop/registration loss gap is real. |
| “SUCCESS proves ACK” | Track business completion, backend state, and delivery recovery separately. SUCCESS coexisted with an unacked message. |
| “PENDING means the job never ran; republish immediately” | It can also mean waiting/running work or an expired/missing result. Reconcile against durable business status rather than blindly republishing. |
| “An ACK error proves the record remains” | Treat the outcome as ambiguous; Redis may already have committed it. |
| “Reconnecting will finish every failed ACK” | Use deduplication/reconciliation; no configuration-only retry-until-confirmed-ACK guarantee was established. |
| Early ACK as a solo duplicate-prevention switch | Solo can queue that ACK until after the body; another worker can restore its delivery first. |
| A tiny visibility timeout for fast failover | It can duplicate live or waiting work. Balance the full unacked interval against recovery delay. |
| Different queues protect different visibility policies | Separate namespaces or align policies; a worker can restore another queue's unacked delivery. |
| Prefetch 0 for throughput | It is unlimited and bypassed the tested ETA cap. Keep ordinary prefetch positive and modest. |
| Prefetch 1 or `worker_disable_prefetch=True` as an ETA cap | Neither prevented all future-message intake in the earlier tests. Avoid distant ETA; use a tested ETA guard/scheduling design. |
| An ETA cap guarantees immediate-job responsiveness | Saturation blocked immediate delivery even with idle execution capacity. Separate scheduling/workload routes where needed. |
| Local ETA timer disappears when Redis restores a message | It does not. One solo worker executed a single short-visibility ETA task four times. |
| `task_time_limit` / `task_soft_time_limit` enforce solo deadlines | Use application deadlines or a separately supervised execution process. |
| `task_reject_on_worker_lost` / `task_retry_on_worker_lost` protect a dead solo worker | Neither can run without a surviving process. Use external restart and broker/application recovery. |
| `task_acks_on_failure=False` automatically retries failures | The measured ordinary-failure path rejects **without requeue**. Retry explicitly. |
| `sys.exit()` safely requests recovery | This checkout exposed a non-prefork callback defect; use normal application outcomes and retry APIs. |
| Larger `--concurrency`, autoscaling, or child recycling controls improve solo execution isolation | Solo has one inline execution slot, no child execution pool, and no enforced pool timeouts. Scale separately supervised processes. |
| Missing monitoring heartbeat means Redis killed the task | Monitoring heartbeats pause during solo work; Redis does not negotiate AMQP protocol heartbeats. |
| `broker_channel_error_retry=True` solves all connectivity problems | It widens recoverable error categories, can repeat permanent errors, and does not replay every failed ACK. |
| Indefinite backend/publication retries are always safer | They can hold the only execution/control thread indefinitely. Bound and test each layer. |

## 8. Deployment acceptance checklist

The combined profile, actual workload, OS/supervisor, TLS/network path, and backend
availability must be tested before production. The previous passing matrices do
not certify this exact deployment.

- [ ] Inspect resolved worker channel visibility, ACK policy, prefetch, ETA guard,
  result retention, and socket options—not only the publisher's configuration.
- [ ] Verify all intended task routes and unique worker names; audit other scanners
  sharing the broker namespace.
- [ ] Run the longest expected job while monitoring heartbeats/control responses;
  ensure normal solo blocking does not trigger an automatic restart.
- [ ] TERM during the longest job: body completes, backend stores the result,
  delivery settles, and worker exits before supervisor grace expires.
- [ ] Deliberately crash a test worker: replacement recovers eligible work within
  the accepted operational window; repeated effects are safe.
- [ ] Exercise both explicit disconnects and silent sockets, not only Redis ping
  failure. Verify automatic progress before any manual rescue.
- [ ] Exercise result-backend outage and simultaneous broker/backend outage. These
  were not established by the earlier backend-bypassing connection experiments.
- [ ] Test failure, bounded explicit retry, exhausted publication retry, and
  successful-task redelivery. Verify business-level attempt policy separately.
- [ ] Saturate permitted short ETA/retry backlog; verify memory and immediate-job
  responsiveness. Re-check ETA-cap interactions with duplicates rather than assuming them.
- [ ] Test reconciliation of a job with no result and no queued/unacked record,
  plus a completed job whose ACK outcome is uncertain.
- [ ] Test operational Redis persistence/failover and the selected timeout values
  under realistic load; verify alerting for OOM, unacked age, and recovery delays.

## Evidence and maintenance

| Report | Decisions grounded in it |
| --- | --- |
| [Shutdown report](shutdown.md) and [maintained appendix](shutdown/shutdown-appendix.md) | TERM, external grace, unsupported solo cancellation, delayed ACK, limits of the guarded requeue workaround |
| [ETA/prefetch report](eta-prefetch.md) | Positive modest prefetch; ETA memory/credit behavior; cap saturation and implementation caveats |
| [Task-policy report](task-policy.md) | No enforced solo time limits; failure ACK/discard; explicit retry; lost-worker limitations and SystemExit defect |
| [Redis connection report](redis-connection.md) | Monitoring vs health checks; bounded socket operations; ACK ambiguity; deduplication; loss window and backend-outage scope |
| [Redis visibility report](redis-visibility.md) | Effective B-only configuration; scanner scope; solo early-ACK duplicates; repeated ETA execution; result TTL vs visibility |
| Visibility report A5–A7 and [wiring tests](redis-visibility/test-visibility-config-wiring.py) | Other backend options remain meaningful; no independent R/T visibility timer; dictionary aliasing and explicit forwarding |

Source checks additionally cover `celery/concurrency/solo.py`, RedisBackend's
retry policy/`ensure`, and the configuration reference for backend socket settings.
Task deadlines, business idempotency, supervisor policy, Redis persistence/security,
and the illustrative production tuning values are recommendations, not new runtime
measurements. No new worker matrix was run to produce this synthesis.

Keep this guide as maintained text alongside the reports. On dependency upgrades,
review the measured limitations and re-run relevant cases; do not silently carry
private-workaround or checkout-specific assumptions into a different stack.
