# Solo + Redis hardening without source changes

> **Best-effort application hardening, not guaranteed delivery or safe arbitrary
> cancellation.** Builds on [solo-redis.md](solo-redis.md) for the recorded Celery
> **5.7.0** / Kombu **5.7.0** sources. Keep that guide's configuration and operational
> budgets unless this document explicitly changes them. Do not fork/patch Celery,
> replace its Redis transport, or quietly substitute another execution pool.
>
> New checks: **14 unit tests and seven real solo-worker cases passed**. They verify
> the supplied wrappers and selected composition paths, not a production deployment
> or every outage scenario. Existing wire-fault/visibility evidence is reused.

## 1. What this hardening does—and does not—fix

| Baseline gap | Selected measure | Protection / remaining limit |
| --- | --- | --- |
| QUIT becomes ordinary failure + ACK | Narrow task-body guard converts the recognized solo `kill_job` error to `Reject(requeue=True)` | Measured recovery with late ACK; no protection outside the guard or if rejection fails |
| Waiting work during shutdown | Preserve built-in restoration; test mixed legacy ETA + active work | Waiting ETA was not lost in the earlier test or the new guarded test; cleanup failure still needs later recovery |
| ETA saturation, QoS drift, immediate-work starvation | Immediate-only publication policy; application scheduling for delays | Prevents native ETA from participating publishers; old messages/other publishers bypass it |
| Completed body, failed ACK | Retained SUCCESS + stock deduplication + durable business completion | Later delivery can settle without repeating effects; no original-ACK replay guarantee |
| Ambiguous ACK / reused tag | Leave settlement to stock delivery handling; do not replay old tags manually | Cannot prove settlement from an ACK exception; duplicate effects must be safe |
| Message lost after pop, before registration | Durable job record and reconciliation; track Kombu PR #2831 | Application recovery, not a present transport fix; an outbox alone is insufficient |
| Visibility duplicates and stale ETA timers | Remove native delays, align scanner policy, bound work, make repeats safe | Visibility can still restore active work; no renewable ownership lease is added |
| Missing/expired SUCCESS or backend outage | Business completion receipt independent of Celery result state | Requires application storage and atomic/idempotent effects; concurrent copies remain possible |
| Unenforced limits / blocked controller / task `SystemExit` | Bounded dependencies, cooperative deadlines, progress-aware supervisor; guard plain `SystemExit` | No preemption of arbitrary native work; no recovery callback after whole-process death |
| Redelivery loops, infrastructure and version drift | Durable attempt/quarantine policy, backpressure, resolved-config checks, regression gates | Redis durability and operational assumptions remain necessary |

**Default selection:** use the small guard and immediate-only publisher below,
keep normal TERM shutdown, and retain the baseline Redis settings. For important
jobs, add business idempotency and a small reconciler backed by existing application
storage. Do not add a custom ACK queue, broker lease protocol, or timer manipulation.

### Keep these states distinct

```text
Application job recorded durably
  -> publication attempted (success can be ambiguous)
  -> queued
  -> Redis pop -> unacked registration -> receipt/optional local ETA timer
             ^ current transport has a loss gap here
  -> task body -> business effects + durable completion
  -> Celery result storage
  -> ACK requested/scheduled -> Redis settlement -> reply observed

Any later delivery:
  Celery can skip an eligible retained SUCCESS;
  otherwise task code consults business completion before repeating effects.
```

Business completion must be committed with the effects it describes, or reconciled
against the external system applying them. An exception, PENDING, SUCCESS, a local
ACK flag, a logged ACK return, and worker exit are not interchangeable observations.

## 2. Small opt-in application module and integration

Review and copy [solo_hardening.py](solo-redis-hardening/solo_hardening.py) into your
application, for example `myapp/hardening.py`. It contains only:

- `solo_task_guard`: a task-body decorator for the known cold-shutdown error and
  accidental **plain** `SystemExit`.
- `ImmediateCelery`: a `Celery` subclass enforcing immediate-only publication through
  that application's `send_task` path.
- Two explicit exception types, `TaskProcessExit` and `SchedulingForbidden`.

There are no global monkey patches or replacement worker/transport implementations
in this application module. The separate probe files are test instrumentation,
not deployment code.

In the existing application factory, replace the `Celery(...)` constructor with
`ImmediateCelery(...)`, then apply the configuration from the baseline guide. Do
not create a second unrelated application next to the real worker app. All intended
publishers—including the dispatcher and worker retry/callback paths—must use it.
Start the worker with the existing stock CLI: `--pool=solo --concurrency=1`.

Example task registration, after constructing/configuring that application:

```python
from celery.exceptions import Reject
from myapp.celery_app import app
from myapp.hardening import (
    SchedulingForbidden, TaskProcessExit, solo_task_guard,
)
from myapp.jobs import execute_business_operation, TransientDependencyError


@app.task(
    bind=True,
    acks_late=True,
    ignore_result=False,
    default_retry_delay=0,
    max_retries=2,  # Illustrative finite budget; choose for this operation.
    autoretry_for=(),
    dont_autoretry_for=(Reject, TaskProcessExit, SchedulingForbidden),
)
@solo_task_guard
def run_job(self, operation_id):
    try:
        # Must check durable completion and make repeated effects safe.
        return execute_business_operation(operation_id, attempt_id=self.request.id)
    except TransientDependencyError as exc:
        # Only for failures where a small immediate retry budget is useful.
        raise self.retry(exc=exc, countdown=0)
```

Use the decorator **inside** `@app.task`, so it wraps the function Celery executes.
Apply it consistently to relevant task bodies; importing the module alone does
nothing. Do not rely on overriding `Task.__call__` to protect all trace/result/ACK
phases: those phases are not all inside the task body's execution boundary.

### Baseline settings to retain or make explicit

| Setting/policy | Selection |
| --- | --- |
| `task_acks_late`, task-level `acks_late` | True; prohibit early-ACK overrides for protected jobs |
| Failure/timeout ACK policy | Effective True; disabling failure ACK discards, not requeues |
| Results | Keep successful results and `worker_deduplicate_successful_tasks=True`; size retention for recovery |
| `worker_prefetch_multiplier` | 1, never 0 |
| `worker_eta_task_limit` | Keep a positive, workload-sized secondary guard; initial credit must not exceed it |
| `worker_cancel_long_running_tasks_on_connection_loss` | False; not an independently killable solo executor |
| `worker_soft_shutdown_timeout` | 0 in this cold-workaround profile; waiting in solo's signal handler pauses its task |
| Application retry policy | Explicit/narrow, finite; no native delayed retries in the immediate-only profile |
| Broker/backend I/O | Separately bounded sockets, health checks, deliberate retry budgets as in the baseline |

Inspect resolved **task** attributes, not just global defaults. A task override can
undo the ACK/result policy. Do these checks during application initialization/CI
and deployment acceptance, not by raising in `worker_ready`: Celery signal dispatch
can log/capture receiver exceptions rather than aborting startup/publication.

## 3. Cold shutdown: useful guard, not a cancellation guarantee

The decorator recognizes the exact recorded error:

```text
<class 'celery.concurrency.solo.TaskPool'> does not implement kill_job
```

It converts that error to `Reject(requeue=True)` **only** when private
`celery.worker.state.should_terminate` is neither `None` nor `False`. The identity
checks deliberately treat exit code **0** as a shutdown request. Other
`NotImplementedError` instances pass through unchanged. This is narrower than
converting every unsupported-operation error into requeue.

### Operating procedure

1. Use TERM for normal deployments. Let bounded work, result handling, and ACK
   processing finish within supervisor grace.
2. If aborting the body with QUIT is required, use it only for these guarded,
   late-ACK, interruption-safe jobs. Send one deliberate signal; do not flood the
   worker with Ctrl+C or automatically remap TERM to QUIT.
3. Allow bounded cleanup/restoration time. If an external deadline finally requires
   killing the process, treat it as a crash, not a successful reject/ACK.
4. Restart under supervision and inspect durable job progress. A successful process
   exit alone is not proof that restoration or settlement succeeded.

The guard is opt-in and version-specific. It depends on private state and exception
text; require regression tests before upgrading. An exception-text match is not a
new cancellation API. Side effects already performed are not rolled back by Reject.

### Exact coverage and failure boundaries

Covered: an untransformed matching exception that escapes the decorated function.
Not covered: task setup/before-start hooks, result serialization/storage, later
success/after-return hooks, ACK callbacks, code swallowing/translating the error,
or death of the process. Rejection itself can fail when Redis is down or another
signal interrupts cleanup. Native code can delay the Python signal handler.

Avoid broad `autoretry_for=(Exception,)`. In this checkout autoretry explicitly
excludes Ignore/Retry but **not automatically Reject**. If autoretry is used, keep
it narrow and include `Reject` in `dont_autoretry_for` so the guard's requeue signal
is not converted into a newly published task retry. Also exclude the policy/process
exit exceptions. Inner application catches can still defeat the outer guard.

A custom Worker/Request class that rewrites shutdown internals is not required or
recommended here. More code would not make arbitrary inline interruption safe.

### Running tasks versus waiting ETA/reserved tasks

**Waiting ETA tasks were not lost** in the cited mixed-shutdown experiment. Three
waiting tasks were restored without ACK; the unguarded active solo task became
FAILURE, was ACKed, and disappeared from the broker.

The new guard reproduced the useful combination: the active delivery was rejected
with requeue and all three waiting ETA deliveries were restored untouched. Preserve
that stock cleanup path—do not loop over all requests and ACK/reject them as if they
were the interrupted body. A timer becoming due during shutdown does not guarantee
execution during grace. After a forced kill or failed restore, remaining unacked
records may need a replacement/live scanner and visibility expiry before recovery.

Early ACK remains unsupported for this workaround: the local acknowledged flag can
already be True even if the actual ACK was deferred. The new negative control still
had **no body completion, one ACK, and no recoverable active delivery**.

## 4. Prevent ETA saturation at the producer boundary

A small wrapper cannot repair Kombu's QoS accounting or create independent ETA and
immediate-work credit. The reliable low-complexity policy is therefore:

**Do not publish native ETA/countdown work to these workers. Persist scheduling in
application storage and publish immediate tasks when due.**

`ImmediateCelery.send_task` rejects any non-None `eta`, and any `countdown` other
than None/0, before invoking the normal publication path. It even rejects a past
ETA or negative countdown to keep the contract simple. Ordinary `.delay()`, task
`.apply_async()`, and tested signatures/groups reach this path in the recorded stack.
Do not use a `before_task_publish` signal receiver as a veto: receiver exceptions
are captured by Celery's signal dispatch, not a reliable publication barrier.

### Apply the policy without losing work

1. Inventory **all** producers and saved signatures/canvas options. Use this app
   class consistently; restrict direct broker credentials to trusted publishers.
2. Before enabling it, migrate pending schedules to the application's durable due
   list. Stop legacy delayed publication and drain or account for already queued/
   locally scheduled messages. Merely deploying the guard does not remove them.
3. Dispatch bounded batches whose `not_before <= now`, using ordinary immediate
   publication. Keep future records in the database, not local worker timers.
4. On exhausted or ambiguous publication, retain the due/job record and retry
   with backoff. Never mark a job completed because publication succeeded.
5. Rate-limit dispatcher throughput and monitor oldest due age and queue backlog.
   Moving timers out of workers does not eliminate downstream capacity limits.

For genuinely periodic work, a scheduler that publishes immediate tasks can fit
this policy. Merely moving ETA publication into another producer does not.

### Delayed retries are a particularly important migration trap

`Task.retry()` defaults to `default_retry_delay` (normally 180 s). Autoretry backoff
also produces countdowns. Those are native ETA messages even if no user called
`apply_async(eta=...)` directly.

**The guard prevents publication, but stock `Task.retry()` catches publication
exceptions and raises `Reject(requeue=False)` in this checkout.** A rejected delayed
retry can therefore discard the current delivery. This was verified in unit tests.
Do not install the guard and assume existing delayed retries will safely wait.

- For a small, genuinely immediate retry budget, explicitly use `countdown=0`,
  set `default_retry_delay=0`, and keep `max_retries` finite. The seven-case worker
  check includes an immediate retry with two receipts/executions, one completion,
  and two ACKs. Immediate retries are not a suitable sustained-outage backoff policy.
- For delayed retries, atomically record a later `not_before` and retry reason in
  application storage, then let the dispatcher publish later. Do **not** also call
  `self.retry(countdown=...)`. Treat the current Celery attempt and the business
  operation as different lifecycles; an accepted reschedule is not business success.
- Audit retry backoff, explicit retry options, callbacks/errbacks, replacement tasks,
  and workflows before enabling the restriction. `max_retries` does not bound raw
  broker redelivery or independent application republication.

A callback with forbidden scheduling options may fail only when the parent task
tries to publish it—possibly after parent effects have committed. The policy does
not prevalidate an entire canvas. Avoid such workflows until tested. Eager `.apply()`
executes locally rather than exercising broker admission; it is not a substitute
for these checks.

### If native ETA must temporarily remain

Use a **different worker process/capacity allocation** for delayed work; a different
queue consumed by the same worker does not isolate QoS. Keep initial prefetch positive
and <= the ETA guard, enforce a short maximum scheduling horizon at trusted producers,
and bound outstanding schedule count/payload size upstream. Include retry delays.

This is a weaker transitional profile, not a proven hard cap: saturation can still
block that worker, discarded QoS increments can later lower its baseline, and visibility
scans can leave duplicate local timers. The existing `worker_eta_task_limit` remains
secondary protection, not a substitute for a publication/scheduling policy.

## 5. Completion and ACK: recover safely instead of adding a blind retry loop

### Why the obvious wrappers do not solve the actual problem

| Candidate | Why it is not the selected fix |
| --- | --- |
| Catch an ACK error around the task body / `after_return` | Actual ACK occurs later in the request/consumer callback path, outside the body wrapper |
| Retry `super().acknowledge()` in a Request subclass | It schedules the callback and marks `Request.acknowledged=True`; the later error is not necessarily raised there |
| Retry the consumer's `ack_log_error` call whenever it returns | It catches/logs supported connection errors; a normal return is not confirmation of settlement |
| Repeatedly call the old message's `ack()` directly | Bypasses the normal callback policy, blocks solo if unbounded, and can remove bookkeeping for a restored copy sharing that tag |
| Delete the Redis unacked hash/index entries manually | Not a delivery-aware acknowledgment; can destroy another execution's recovery record |
| Call `self.retry()` after uncertain settlement | Can publish duplicate work even when Redis already committed ACK; it is not an ACK retry |

Source path inspected: `Request.acknowledge` → consumer ACK promise →
`call_soon_ack`/hub → `Message.ack_log_error` → Redis `QoS.ack`. `Task.on_success`,
`after_return`, and `task_postrun` are not confirmed-ACK hooks. In the measured path
an error can be handled/logged after the local acknowledged flag was set;
`BrokenPipeError` also has a distinct re-raising path. Source:
[Request](repos/celery/celery/worker/request.py),
[consumer callback scheduling](repos/celery/celery/worker/consumer/consumer.py), and installed
Kombu `message.py` / `transport/redis.py` (hashed in the validation metadata).

A lower-level subclass/wrapper could intercept the socket operation, but safely
retaining/retrying old tags across restoration, consumer restart, and shutdown is
not a small general-purpose fix. **No ACK retry wrapper is shipped in this guide.**
The stock transport has no application-visible delivery-generation fence making
that retry universally safe. A finite retry count reduces blocking, not this race.

### Selected best-effort recovery path

1. Commit business effects/completion before returning, using the pattern in section 7.
2. Let Celery store the result and execute its normal late ACK. Keep network and
   backend retries bounded; allow consumer reconnect and external restart.
3. If ACK failed before commit, allow restoration/redelivery. With retained SUCCESS,
   late ACK, and eligible redelivery metadata, Celery skips the body and ACKs the
   new receipt. This was measured in the existing connection suite.
4. If SUCCESS is unavailable/expired or the copy is independently republished, the
   body may run. Its business-completion check returns the stored outcome without
   applying effects again; the normal completion path attempts settlement again.
5. If ACK committed but its reply was lost, there may be nothing to restore.
   Business completion is still valid. Do not republish completed work to make an
   ACK-return counter look correct.

This can eventually settle work after connectivity and scanning recover, but does
not replay or confirm the original ACK. It does not promise a recovery deadline
under unlimited outages. If “confirmed ACK before exit for every completed task”
is mandatory, it remains **unsupported under this low-complexity stock-transport
profile**. Do not represent exit 0 or backend SUCCESS as satisfying that requirement.

Monitor ACK/reject errors and recovery age. Avoid synchronous network logging that
can itself block solo. A log alert is observability, not a new ACK queue. More warm
shutdown grace alone does not replay an ACK callback that has already failed.

## 6. Pop-before-unacked loss and the upstream upgrade path

The current measured transport can remove a message with BRPOP and lose its reply
before unacked registration. Neither a task decorator (the task never started),
visibility timeout, nor successful-task deduplication can recover that absent record.
For important work, use application reconciliation in section 7 now.

### Kombu PR #2831

[PR #2831 — Record a Redis message in unacked in the step that pops it](https://github.com/celery/kombu/pull/2831)
was **open and unmerged when checked on 2026-11-16**, at head
`7515088f6f52717c62dcdd97c62a4d3e0f7b4ee4`. Retained evidence:
[PR metadata](solo-redis-hardening/kombu-pr-2831.json) and
[file diff snapshot](solo-redis-hardening/kombu-pr-2831-files.json).

The inspected proposal uses a Redis Lua operation to pop and record eligible messages
in the same server operation, and BLPOP wake lists when queues are empty. If the
script's reply is lost after recording, visibility recovery has an unacked record
instead of the old pop/registration gap. The draft also changes publisher wake-up
behavior; its current description recommends **upgrading publishers first** and notes
older publishers can incur a wait-timeout delay.

This is a proposed upstream fix, **not installed or tested here**. Its draft has
conditions/fallbacks—for example no-ACK operation does not record deliveries and
unrecognized message formats can fall back to the old recording path. Its reported
benchmarks/tests are the PR author's evidence, not our measurements. A draft
`versionchanged:: 5.7.1` note is not proof that a release contains the change.

If merged and released, upgrade to a supported Celery/Kombu combination that
actually includes the fix, verify the resolved Kombu source/version, follow the final
migration instructions, and rerun the lost-reply test. The old proxy's BRPOP-specific
injection will need adaptation to target the new message-bearing response rather
than a wake token. Recheck ordering, priorities, prefixes, ACL/Lua permissions,
noeviction, mixed versions, and realistic payload/load costs.

A Celery upgrade alone does not prove the relevant Kombu dependency changed. Even
a successful upgrade would not eliminate ACK ambiguity, live-work visibility
redelivery, duplicate side effects, or Redis persistence/failover loss. No local
backport, fork, or transport replacement is proposed here.

## 7. Minimal application patterns for the gaps wrappers cannot close

These are design patterns using existing application storage, not a supplied database
implementation or a requirement to build a new distributed recovery framework.
They are necessary only to the extent the business requires recovery beyond the
stock broker's guarantees. Without durable job intent, explicitly accept possible
missing work; there is no equivalent configuration flag.

### Durable intent + bounded dispatcher/reconciler

A small job table can hold `operation_id`, payload/reference, business status,
`not_before`, publication/progress timestamps, execution-attempt count, stored outcome,
and failure/quarantine reason. Keep the durable store in a failure domain suitable
for the business recovery objective—not merely another DB number on the same Redis.

```text
Submitter: transactionally record business intent and pending publication.
Dispatcher: select bounded due batches; publish immediate tasks with operation_id.
            Record publication progress, but not business completion.
Worker: check durable completion; apply effects safely; commit completion; return.
Reconciler: find due/incomplete jobs with overdue progress; republish with backoff
            and a finite business attempt policy; alert/quarantine exhausted jobs.
```

An outbox closes the application-transaction/publication gap, **not** the subsequent
broker-delivery gap. Keep incomplete jobs eligible for reconciliation even after
publication succeeded. A timeout/old progress timestamp is a reason to investigate
or safely republish, not proof the previous worker stopped. Concurrent duplicates
must be harmless independently of that timer.

Use the **business operation ID** for idempotency. Celery task IDs may be retained
for correlation but reusing one does not turn arbitrary republication into stock
successful-task deduplication. Do not decide to republish solely from PENDING or
queue inspection. Deleted/expired result keys and active bodies can look identical.

### Same-database effects: transaction, not a cache flag

Illustrative transaction structure; adapt locking, isolation, and statement/lock
budgets to your database:

```text
BEGIN
  load existing job by operation_id FOR UPDATE with a bounded lock wait
  if job is completed: return its stored outcome after ending the transaction
  if job is quarantined/cancelled/unknown: do not perform effects
  if job is not yet due: do not perform effects; preserve its scheduled retry
  if this publication attempt already has an outcome: return that receipt
  apply the bounded database effects
  store business completion and outcome in this SAME transaction
COMMIT
return the stored outcome
```

If the worker dies before commit, the transaction rolls back; after commit, a
redelivered body finds completion. Unknown job IDs must not silently recreate a
missing completion record and repeat effects. Retain identity/completion information
through the allowed replay horizon, or reject stale work after that horizon.

Attempt accounting needs its own deliberate durable semantics: an attempt counter
rolled back with a failed transaction does not count crashes. Record attempt starts
separately if crash attempts must count. Distinguish duplicate receipts of a completed
job from new business execution attempts so harmless recovery does not exhaust budgets.

Do not hold database locks through unbounded remote calls. A process-local lock or
expiring Redis lock does not provide cross-worker exactly-once effects or stop an
old owner after expiry. The small pattern above is for effects covered by the same
transaction; it is not a generic lease implementation.

### External effects: use the remote service's idempotency contract

Use a stable operation key supported by the remote API, store request intent and
returned receipt, and query/reconcile ambiguous outcomes where the API permits.
If a timeout occurs after the remote effect but before local recording, repeating
with the same key must be safe under that API's retention/parameter rules.

If the external system cannot deduplicate or expose the operation's outcome, no
wrapper can safely infer whether retrying repeats the effect. Quarantine ambiguous
jobs for reconciliation/manual handling rather than declaring exactly once.

### Backend outage and explicit rescheduling

Commit the business receipt before returning. Celery's result write can then fail
without erasing the business fact; a later body can return the stored outcome.
Keep result/backend retries finite and test the generic and Redis-specific layers
separately. Do not add a task-level retry around the entire body merely because
result storage failed.

For delayed business retry, persist `not_before`/reason in the job table before
ending the current attempt. Whether that attempt returns a “rescheduled” receipt
or records an ordinary failure is an explicit API decision; neither means the
business operation completed. Celery SUCCESS dedup can suppress a later eligible
redelivery with the same task ID, so **a new scheduled business attempt must get a
new Celery task ID**, while retaining the same business operation ID. Otherwise an
old “rescheduled” SUCCESS could prevent needed execution. Keep attempt identity
and completion identity distinct. Persist a per-attempt rescheduling receipt so a
duplicate of that attempt does not advance the schedule repeatedly. Check the
business `not_before` in the worker too: publication timing alone does not prevent
an old in-flight copy from arriving before the new due time.

## 8. Visibility, execution budgets, and operation restrictions

### Visibility and scanning

Keep one compatible visibility policy per shared unacked namespace, including
monitoring consumers. Only broker transport visibility independently controls
recovery in this stack; queue names do not isolate scanners. Size it for full
receipt-to-settlement age, including result/ACK handling, not just body runtime.
Removing native ETA makes that budget easier to bound, not immune to outages.

A lone busy solo worker may not scan, but another scanner can restore its active
work. Increasing visibility reduces premature restoration while delaying recovery;
there is no setting that gives both immediate recovery and no duplicate during
arbitrarily long execution. Do not mutate Redis unacked timestamps to invent a lease
or cancel local timers by reaching into consumer internals. Drain legacy scheduled
work or reconcile it explicitly during policy migration.

### Deadlines and supervisor behavior

- Every remote operation needs connect/read/total-operation bounds; lock/pool
  acquisition and cleanup need bounds too. Redis broker settings do not cover HTTP/SQL.
- For cooperative loops, track a monotonic deadline and check remaining budget at
  bounded checkpoints; cap each dependency timeout against the remaining budget.
  Checking a deadline after an unbounded call is not preemption.
- Celery soft/hard task limits do not enforce solo runtime. Do not use a timeout
  wrapper that merely abandons an executing thread and then pretend the effects stopped.
- Forbid unbounded or non-cooperative workloads if a strict cancellation deadline
  is required. That requirement is unsupported by this inline-solo profile.
- Monitor process existence, durable progress age, queue/due-job age, and deadlines.
  A missed Celery ping/heartbeat during a legitimate inline task is not sufficient
  evidence to restart it. An auxiliary heartbeat alone cannot prove body progress.
- Deliver TERM to the real worker, allow a separately chosen grace, and use a final
  externally enforced crash deadline where necessary. Repeated TERM does not escalate;
  repeated SIGINT can coalesce during native code. Whole-process death cannot invoke
  surviving-controller loss retries or child recycling.

### Plain SystemExit guard and failure policy

The supplied guard converts **exactly plain `SystemExit`**, as raised by `sys.exit`,
into `TaskProcessExit`, an ordinary task failure. The real-worker check stored
FAILURE, ACKed once, and executed a follow-up ping task instead of hitting the known
non-prefork callback defect. This is terminal failure handling, not automatic retry.

`WorkerShutdown` and `WorkerTerminate` inherit `SystemExit`; the exact-type check
lets them escape unchanged. So do other SystemExit subclasses, KeyboardInterrupt,
and GeneratorExit. Do not broadly swallow BaseException or process-control signals.
`os._exit`, SIGKILL, native crashes, and interpreter death cannot be caught here.
Ban intentional process exits from task libraries; the guard is a fallback, not an
invitation to use them as control flow.

### Operational hardening that remains mandatory

- Keep broker and backend option dictionaries independent. Socket/health settings
  for the backend use their dedicated top-level names; see connection appendix A8.
- Bound publication/backend retry layers and log exhaustion; indefinite consumer
  reconnect is a different policy and needs outage alerts. Do not enable channel-error
  retry as a cure for persistent WRONGTYPE/data/configuration errors.
- Set Redis noeviction only with memory headroom/backpressure and write-rejection
  handling. Choose persistence/failover RPO explicitly, secure/authenticate private
  endpoints, and verify TLS. Neither replicas nor noeviction remove the measured gap.
- Before changing namespaces, stop old publication and account for old queued/unacked
  work; a new prefix does not migrate it. Separate backend prefixes independently.
- Enforce a business attempt/quarantine policy for poison jobs. Do not raw-requeue
  forever or assume `max_retries` counts visibility recovery or cold requeues.
- Record source/dependency versions and revalidate private-state wrappers on upgrades.
  Do not apply broker maintenance or namespace deletion as an ACK repair technique.

## 9. Verification, remaining tests, and rollout

### Evidence added with this guide

- [Application example module](solo-redis-hardening/solo_hardening.py).
- [14 unit tests](solo-redis-hardening/test-solo-hardening.py): exact cold guard,
  ordinary-error preservation, SystemExit/control-exception separation, Reject
  exclusion from autoretry, immediate APIs/retry, forbidden scheduling, retry-discard
  trap, and bypass through another app. Publication uses a mock sender/memory broker;
  these unit tests do not claim broker-level enforcement.
- [Seven-case real-worker summary](solo-redis-hardening/runs/20261116T075739.167859Z/summary.json)
  and [console](solo-redis-hardening/validation.log). Private Redis, stock solo CLI,
  separate broker/backend DBs, late ACK except the explicit early-ACK negative control.

| Case | Body starts / completions | Actual ACK / reject | Backend / delivery after exit |
| --- | --- | --- | --- |
| Warm late ACK | 1 / 1 | 1 / none | SUCCESS; settled |
| Guarded cold late ACK | 1 / 0 | 0 / requeue | PENDING; active delivery restored |
| Cold early-ACK negative control | 1 / 0 | 1 / none | PENDING; active delivery absent—not safe |
| Cold with three legacy waiting ETA tasks | 1 / 0 for active | 0 / requeue for active | Active restored; all three waiting tasks restored without execution/ACK |
| Unrelated NotImplementedError | 1 / 0 | 1 / none | FAILURE; original exception preserved |
| Guarded plain SystemExit | 1 / 0 | 1 / none | FAILURE; worker remained usable |
| Explicit immediate retry | 2 / 1 | 2 / none | SUCCESS; settled |

All seven workers exited 0 with **no safety kills**. The mixed case deliberately
used an ordinary Celery publisher to model pre-policy ETA messages, demonstrating
both cleanup and the publisher guard's scope limitation. No source patches were
applied. Probe-only observation hooks delegate the original operations.

The existing [shutdown](shutdown.md), [ETA](eta-prefetch.md),
[connection](redis-connection.md), [visibility](redis-visibility.md), and
[task-policy](task-policy.md) suites support the other failure descriptions. This
new suite did not repeat their complete matrices or certify an application ledger.
The 29 existing unit/layout regressions also passed: **43 tests total**, retained in
[unit-validation.log](solo-redis-hardening/unit-validation.log). The guide's task
registration/body/immediate-retry example was exercised with a mocked business
operation; [example validation](solo-redis-hardening/example-validation.log) also
checks syntax, table formatting, and recorded source hashes.

```bash
mise exec -- uv run --locked --no-sync python solo-redis-hardening/test-solo-hardening.py
mise run solo-hardening
# Focused selection:
mise run solo-hardening \
  --case cold-mixed-legacy-eta --case immediate-retry
```

Runners write timestamped artifacts under the hardening suite; they do not regenerate
this maintained guide. See [validation metadata](solo-redis-hardening/validation-metadata.json)
for the environment/source inventory. PR snapshots describe their capture, not a
permanent merge-status assertion.

### Deployment acceptance still required

- [ ] QUIT at task setup, body, result handling, and ACK boundaries; repeated signals,
  Redis unavailable during rejection/restore, and forced kill during cleanup. These
  uncovered guard phases must not be claimed protected by the happy-path test.
- [ ] Long/native work with real supervisor grace; control-plane stalls, blocked
  dependencies/logging, bounded cleanup, and process-loss reconciliation.
- [ ] Publication-policy migration across every producer/retry/canvas path; legacy
  ETA backlog, short/large payload bursts, application scheduler backlog and fairness.
- [ ] ACK-before-commit and lost-reply faults, redelivery while original work runs,
  visibility restoration by another scanner, and expired/unavailable SUCCESS.
- [ ] Business idempotency under concurrent execution and remote ambiguous effects;
  crash before/after durable commit, durable attempt exhaustion, and quarantine.
- [ ] Backend-only and combined broker/backend outage, exhausted/ambiguous publication,
  Redis write rejection, persistence/failover, and namespace migration. Prior broker
  fault experiments kept the backend reachable; the new wrapper tests use healthy Redis.
- [ ] Lost-delivery reconciliation with no queue/unacked/result record; future PR
  upgrade regression testing against its actual message-bearing response path.

Roll out first to a small, explicitly routed set of interruption-safe jobs. Audit
ACK/result/retry overrides and producer policy before enabling restrictions. Keep
old namespace/job intent until recovery is accounted for. Roll back by removing
opt-in decorators/app-class selection only after reconciling outstanding work; do
not delete broker records or durable completion receipts to make dashboards green.
